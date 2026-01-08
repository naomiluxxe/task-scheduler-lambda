"""POLL task type handler.

Creates a Discord poll in the specified channel via dronebot HTTP endpoint.
"""

import json
import os
import logging
import urllib.request
import urllib.error
from decimal import Decimal

logger = logging.getLogger(__name__)

# Configuration from environment
DRONEBOT_URL = os.environ.get('DRONEBOT_URL', 'http://localhost:3000')
DRONEBOT_TOKEN = os.environ.get('DRONEBOT_API_TOKEN', '')


def convert_decimals(obj):
    """Convert DynamoDB Decimal types to Python native types."""
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    return obj


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
    duration_hours = payload.get('duration_hours', 24)
    assignee = task.get('assignee', 'void-mother')
    task_id = task.get('task_id', 'unknown')

    # Convert any Decimals
    duration_hours = int(duration_hours) if duration_hours else 24
    options = convert_decimals(options)

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

    if len(options) > 10:
        return {
            'success': False,
            'error': 'Poll cannot have more than 10 options'
        }

    # Polls can't be created in DMs
    channel_type = task.get('channel', 'dm')
    if channel_type in ('dm', 'group-dm'):
        return {
            'success': False,
            'error': 'Cannot create poll in DM channels'
        }

    if not channel_id:
        return {
            'success': False,
            'error': 'No channel_id available'
        }

    # Send to dronebot HTTP endpoint
    try:
        result = send_poll_to_dronebot(
            channel_id=channel_id,
            question=question,
            options=options,
            duration_hours=duration_hours,
            agent_id=assignee,
            task_id=task_id,
            target=target
        )

        if result.get('success'):
            logger.info(f"Poll created for task {task_id}, message_id={result.get('message_id')}")
            return {
                'success': True,
                'agent': assignee,
                'message_id': result.get('message_id'),
                'question': question,
                'option_count': len(options)
            }
        else:
            error_msg = result.get('error', 'Unknown poll error')
            logger.error(f"Failed to create poll: {error_msg}")
            return {'success': False, 'error': f'Poll creation error: {error_msg}'}

    except Exception as e:
        logger.error(f"Failed to send poll to dronebot: {e}")
        return {'success': False, 'error': f'HTTP error: {str(e)}'}


def send_poll_to_dronebot(channel_id, question, options, duration_hours, agent_id, task_id, target):
    """
    Send poll request to dronebot HTTP endpoint.

    Args:
        channel_id: Discord channel ID
        question: Poll question
        options: List of poll options
        duration_hours: Poll duration in hours
        agent_id: Agent that creates the poll
        task_id: Task ID for logging
        target: Target user

    Returns:
        dict with success status and message_id if successful
    """
    if not DRONEBOT_URL:
        return {'success': False, 'error': 'DRONEBOT_URL not configured'}

    if not DRONEBOT_TOKEN:
        return {'success': False, 'error': 'DRONEBOT_API_TOKEN not configured'}

    payload = json.dumps({
        'agent_id': agent_id,
        'channel_id': channel_id,
        'question': question,
        'options': options,
        'duration_hours': duration_hours,
        'task_id': task_id,
        'target': target
    }).encode('utf-8')

    url = f"{DRONEBOT_URL}/task/poll"
    logger.info(f"Creating poll via {url}")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            'Content-Type': 'application/json',
            'X-Dronebot-Token': DRONEBOT_TOKEN
        },
        method='POST'
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8') if e.fp else str(e)
        logger.error(f"HTTP error {e.code}: {error_body}")
        return {'success': False, 'error': f'HTTP {e.code}: {error_body}'}
    except urllib.error.URLError as e:
        logger.error(f"URL error: {e.reason}")
        return {'success': False, 'error': f'Connection error: {e.reason}'}
