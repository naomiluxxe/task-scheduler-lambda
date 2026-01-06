"""Task Scheduler Lambda Handler.

Triggered by EventBridge on a schedule (every 15 minutes).
Queries due tasks and fires them to the appropriate agent.
"""

import json
import random
import logging
import boto3
from datetime import datetime

import dynamo
from task_types import handle_message, handle_poll, handle_query_for_update

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Discord bot Lambda for channel resolution
DISCORD_BOT_LAMBDA = 'dronebot'

# Error channel for alerts
CPU_ERRORS_CHANNEL_ID = None  # Set via environment or lookup

lambda_client = boto3.client('lambda', region_name='us-east-1')


def handler(event, context):
    """
    Main Lambda handler.

    Triggered by EventBridge schedule.
    """
    logger.info(f"Task scheduler triggered: {json.dumps(event)[:200]}")

    # Get all due tasks
    due_tasks = dynamo.get_due_tasks()
    logger.info(f"Found {len(due_tasks)} due tasks")

    results = {
        'processed': 0,
        'fired': 0,
        'skipped': 0,
        'errors': 0,
        'tasks': []
    }

    for task in due_tasks:
        task_id = task['task_id']
        task_result = process_task(task)
        results['tasks'].append({
            'task_id': task_id,
            'result': task_result
        })

        if task_result.get('fired'):
            results['fired'] += 1
        elif task_result.get('skipped'):
            results['skipped'] += 1
        elif task_result.get('error'):
            results['errors'] += 1

        results['processed'] += 1

    logger.info(f"Scheduler complete: {results['fired']} fired, {results['skipped']} skipped, {results['errors']} errors")

    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }


def process_task(task):
    """Process a single task."""
    task_id = task['task_id']
    task_type = task.get('type', 'MESSAGE')
    scheduler_params = task.get('scheduler_params', {})

    logger.info(f"Processing task {task_id} (type: {task_type})")

    # Check execution_rate probability
    execution_rate = scheduler_params.get('execution_rate', 100)
    if random.randint(0, 100) > execution_rate:
        logger.info(f"Task {task_id} skipped due to execution_rate ({execution_rate}%)")
        return {'skipped': True, 'reason': 'execution_rate'}

    # Expand targets (resolve roles to members)
    targets = expand_targets(task.get('targets', []))

    if not targets:
        logger.warning(f"Task {task_id} has no valid targets")
        return {'skipped': True, 'reason': 'no_targets'}

    # Process each target
    target_results = []
    all_success = True

    for target in targets:
        try:
            # Resolve channel for this target
            channel_id = resolve_channel(task.get('channel', 'dm'), target, task)

            if not channel_id:
                logger.warning(f"Could not resolve channel for task {task_id}, target {target}")
                target_results.append({
                    'target': target,
                    'success': False,
                    'error': 'channel_resolution_failed'
                })
                all_success = False
                continue

            # Fire the task based on type
            if task_type == 'MESSAGE':
                result = handle_message(task, target, channel_id)
            elif task_type == 'POLL':
                result = handle_poll(task, target, channel_id)
            elif task_type == 'QUERY-FOR-UPDATE':
                result = handle_query_for_update(task, target, channel_id)
            else:
                result = {'success': False, 'error': f'Unknown task type: {task_type}'}

            target_results.append({
                'target': target,
                **result
            })

            if not result.get('success'):
                all_success = False

        except Exception as e:
            logger.error(f"Error processing task {task_id} for target {target}: {e}")
            target_results.append({
                'target': target,
                'success': False,
                'error': str(e)
            })
            all_success = False

    # Update task state
    if all_success:
        dynamo.mark_task_fired(task)
        logger.info(f"Task {task_id} fired successfully")
    else:
        # Record error but don't prevent retry
        error_msg = '; '.join([
            f"{r['target']}: {r.get('error', 'unknown')}"
            for r in target_results if not r.get('success')
        ])
        dynamo.record_task_error(task, error_msg)
        alert_cpu_errors(task, error_msg)
        logger.error(f"Task {task_id} had errors: {error_msg}")

    return {
        'fired': all_success,
        'error': not all_success,
        'targets': target_results
    }


def expand_targets(targets):
    """
    Expand target list, resolving roles to member IDs.

    Args:
        targets: List of targets (e.g., ['0x0001', 'role:hive-drone'])

    Returns:
        List of individual target IDs
    """
    expanded = []

    for target in targets:
        if target.startswith('role:'):
            # Resolve role to members via Discord bot
            role_name = target[5:]
            members = resolve_role_members(role_name)
            expanded.extend(members)
        else:
            # Individual target
            expanded.append(target)

    return list(set(expanded))  # Dedupe


def resolve_role_members(role_name):
    """
    Resolve a role name to list of member IDs.

    Calls the Discord bot Lambda to get role members.
    """
    try:
        response = lambda_client.invoke(
            FunctionName=DISCORD_BOT_LAMBDA,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'action': 'get_role_members',
                'role_name': role_name
            })
        )

        result = json.loads(response['Payload'].read())
        if result.get('statusCode') == 200:
            body = json.loads(result.get('body', '{}'))
            return body.get('members', [])

    except Exception as e:
        logger.error(f"Failed to resolve role {role_name}: {e}")

    return []


def resolve_channel(channel_type, target, task):
    """
    Resolve channel type to actual channel ID.

    Args:
        channel_type: 'dm', 'group-dm', 'priv-chan', 'priv-chan-group', or channel ID
        target: Target drone ID
        task: Full task object

    Returns:
        Discord channel ID or None
    """
    # If channel_id is already stored in task (resolved at creation time), use it
    if task.get('channel_id'):
        return task['channel_id']

    # If it's already a channel ID (numeric string), return it
    if channel_type and channel_type.isdigit():
        return channel_type

    # For special channel types (dm, group-dm, priv-chan, priv-chan-group),
    # the dronebot HTTP endpoint will need to handle these
    # For now, return None and let the message handler deal with it
    if channel_type in ('dm', 'group-dm', 'priv-chan', 'priv-chan-group'):
        logger.warning(f"Special channel type '{channel_type}' requires bot-side handling")
        return None

    # If we get here, we can't resolve - this shouldn't happen with new tasks
    logger.warning(f"Cannot resolve channel '{channel_type}' - no channel_id stored")
    return None


def alert_cpu_errors(task, error_message):
    """Post error to #cpu-errors channel."""
    try:
        # Build error message
        content = (
            f"**Task Execution Error**\n"
            f"Task: `{task['task_id']}`\n"
            f"Title: {task.get('title', 'Untitled')}\n"
            f"Type: {task.get('type', 'Unknown')}\n"
            f"Error: {error_message}\n"
            f"Will retry on next scheduled cycle."
        )

        # Invoke Discord bot to post error
        lambda_client.invoke(
            FunctionName=DISCORD_BOT_LAMBDA,
            InvocationType='Event',
            Payload=json.dumps({
                'action': 'post_error',
                'channel': 'cpu-errors',
                'content': content
            })
        )

    except Exception as e:
        logger.error(f"Failed to alert cpu-errors: {e}")


# For local testing
if __name__ == '__main__':
    result = handler({}, None)
    print(json.dumps(result, indent=2))
