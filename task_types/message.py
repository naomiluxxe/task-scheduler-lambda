"""MESSAGE task type handler.

Sends a message to the target(s) via the assigned agent.
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


def handle_message(task, target, channel_id):
    """
    Handle a MESSAGE task.

    Args:
        task: The task object from DynamoDB
        target: The resolved target (drone ID or user ID)
        channel_id: The Discord channel ID to post in

    Returns:
        dict with success status and any response data
    """
    payload = task.get('payload', {})
    content = payload.get('content', '')
    agent_params = task.get('agent_params', {})
    assignee = task.get('assignee', 'void-mother')

    if not content:
        return {
            'success': False,
            'error': 'No message content provided'
        }

    # Interpolate variables in content
    content = interpolate_content(content, target, task)

    # Get the agent Lambda
    agent_lambda = AGENT_LAMBDAS.get(assignee)
    if not agent_lambda:
        return {
            'success': False,
            'error': f'Unknown agent: {assignee}'
        }

    # Build the payload for the agent Lambda
    agent_payload = {
        'action': 'send_message',
        'channel_id': channel_id,
        'content': content,
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

        logger.info(f"Invoked {agent_lambda} for task {task['task_id']}, status: {response['StatusCode']}")

        return {
            'success': True,
            'agent': assignee,
            'lambda': agent_lambda,
            'status_code': response['StatusCode']
        }

    except Exception as e:
        logger.error(f"Failed to invoke {agent_lambda}: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def interpolate_content(content, target, task):
    """Replace variables in message content."""
    from datetime import datetime

    replacements = {
        '{target}': f'<@{target}>' if not target.startswith('role:') else f'<@&{target[5:]}>',
        '{date}': datetime.utcnow().strftime('%Y-%m-%d'),
        '{time}': datetime.utcnow().strftime('%H:%M'),
        '{title}': task.get('title', ''),
        '{task_id}': task.get('task_id', ''),
    }

    for key, value in replacements.items():
        content = content.replace(key, value)

    return content
