"""POLL task type handler.

Creates a Discord poll in the specified channel.
"""

import json
import boto3
import logging

logger = logging.getLogger(__name__)
lambda_client = boto3.client('lambda', region_name='us-east-1')

# Map agent names to their Lambda functions
AGENT_LAMBDAS = {
    'void-mother': 'void-mother-chat',
    '0xf100': 'greeter-drone',
    '0xf101': 'propaganda-drone',
}


def handle_poll(task, target, channel_id):
    """
    Handle a POLL task.

    Args:
        task: The task object from DynamoDB
        target: The resolved target (drone ID or user ID)
        channel_id: The Discord channel ID to post in

    Returns:
        dict with success status and any response data
    """
    payload = task.get('payload', {})
    question = payload.get('question', '')
    options = payload.get('options', [])
    duration_minutes = payload.get('duration_minutes', 60)
    agent_params = task.get('agent_params', {})
    assignee = task.get('assignee', 'void-mother')

    if not question:
        return {
            'success': False,
            'error': 'No poll question provided'
        }

    if not options or len(options) < 2:
        return {
            'success': False,
            'error': 'Poll requires at least 2 options'
        }

    # Polls can't be created in DMs
    channel_type = task.get('channel', 'dm')
    if channel_type in ('dm', 'group-dm'):
        return {
            'success': False,
            'error': 'Cannot create poll in DM channels'
        }

    # Get the agent Lambda
    agent_lambda = AGENT_LAMBDAS.get(assignee)
    if not agent_lambda:
        return {
            'success': False,
            'error': f'Unknown agent: {assignee}'
        }

    # Build the payload for the agent Lambda
    agent_payload = {
        'action': 'create_poll',
        'channel_id': channel_id,
        'question': question,
        'options': options,
        'duration_minutes': duration_minutes,
        'target': target,
        'task_id': task['task_id'],
        'agent_params': agent_params
    }

    try:
        response = lambda_client.invoke(
            FunctionName=agent_lambda,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(agent_payload)
        )

        logger.info(f"Invoked {agent_lambda} for poll task {task['task_id']}, status: {response['StatusCode']}")

        return {
            'success': True,
            'agent': assignee,
            'lambda': agent_lambda,
            'status_code': response['StatusCode']
        }

    except Exception as e:
        logger.error(f"Failed to invoke {agent_lambda} for poll: {e}")
        return {
            'success': False,
            'error': str(e)
        }
