"""QUERY-FOR-UPDATE task type handler.

Checks if a drone's DB field needs updating and sends a reminder if so.
"""

import json
import boto3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
lambda_client = boto3.client('lambda', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Map agent names to their Lambda functions
AGENT_LAMBDAS = {
    'void-mother': 'void-mother-chat',
    '0xf100': 'greeter-drone',
    '0xf101': 'propaganda-drone',
}

# Drone data table
DRONE_TABLE = 'cpuDrones'


def handle_query_for_update(task, target, channel_id):
    """
    Handle a QUERY-FOR-UPDATE task.

    Checks the drone's DB field and sends a reminder if:
    - The field is empty/null
    - The field hasn't been updated within timeout_hours

    Args:
        task: The task object from DynamoDB
        target: The resolved target (drone ID)
        channel_id: The Discord channel ID to post in

    Returns:
        dict with success status and any response data
    """
    payload = task.get('payload', {})
    db_field = payload.get('db_field', '')
    timeout_hours = payload.get('timeout_hours', 24)
    reminder_message = payload.get('reminder_message', '')
    agent_params = task.get('agent_params', {})
    assignee = task.get('assignee', 'void-mother')

    if not db_field:
        return {
            'success': False,
            'error': 'No db_field specified'
        }

    # Skip role targets - can only query individual drones
    if target.startswith('role:'):
        return {
            'success': False,
            'error': 'QUERY-FOR-UPDATE requires individual drone targets, not roles'
        }

    # Get drone data
    drone_data = get_drone_data(target)
    if not drone_data:
        logger.warning(f"Drone not found: {target}")
        return {
            'success': False,
            'error': f'Drone not found: {target}'
        }

    # Check if reminder is needed
    should_remind, reason = check_needs_update(drone_data, db_field, timeout_hours)

    if not should_remind:
        logger.info(f"No reminder needed for {target}.{db_field}: {reason}")
        return {
            'success': True,
            'reminded': False,
            'reason': reason
        }

    # Build reminder message
    if not reminder_message:
        reminder_message = f"Reminder: Please update your {db_field}."

    # Get the agent Lambda
    agent_lambda = AGENT_LAMBDAS.get(assignee)
    if not agent_lambda:
        return {
            'success': False,
            'error': f'Unknown agent: {assignee}'
        }

    # Build the payload for the agent Lambda
    agent_payload = {
        'action': 'send_reminder',
        'channel_id': channel_id,
        'content': reminder_message,
        'target': target,
        'db_field': db_field,
        'task_id': task['task_id'],
        'agent_params': agent_params,
        'reason': reason
    }

    try:
        response = lambda_client.invoke(
            FunctionName=agent_lambda,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(agent_payload)
        )

        logger.info(f"Sent reminder via {agent_lambda} for {target}.{db_field}, status: {response['StatusCode']}")

        return {
            'success': True,
            'reminded': True,
            'reason': reason,
            'agent': assignee,
            'lambda': agent_lambda,
            'status_code': response['StatusCode']
        }

    except Exception as e:
        logger.error(f"Failed to invoke {agent_lambda} for reminder: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def get_drone_data(drone_id):
    """Get drone data from DynamoDB."""
    try:
        table = dynamodb.Table(DRONE_TABLE)
        response = table.get_item(
            Key={'visitorId': drone_id}
        )
        return response.get('Item')
    except Exception as e:
        logger.error(f"Failed to get drone data for {drone_id}: {e}")
        return None


def check_needs_update(drone_data, db_field, timeout_hours):
    """
    Check if the drone needs to update a field.

    Returns:
        tuple: (should_remind: bool, reason: str)
    """
    field_value = drone_data.get(db_field)
    updated_at_field = f'{db_field}_updated_at'
    last_updated = drone_data.get(updated_at_field)

    # Check if field is empty
    if field_value is None or field_value == '':
        return True, 'field_empty'

    # Check if field hasn't been updated within timeout
    if last_updated:
        try:
            last_updated_dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
            cutoff = datetime.utcnow().replace(tzinfo=last_updated_dt.tzinfo) - timedelta(hours=timeout_hours)

            if last_updated_dt < cutoff:
                return True, 'timeout_exceeded'
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse {updated_at_field}: {last_updated}, error: {e}")

    return False, 'up_to_date'
