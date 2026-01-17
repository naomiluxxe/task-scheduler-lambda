"""Task Scheduler Lambda Handler.

Triggered by EventBridge on a schedule (every 15 minutes).
Queries due tasks and fires them to the appropriate agent.
"""

import json
import random
import logging
import boto3
import os
import urllib.request
import urllib.error
from datetime import datetime

import dynamo
from task_types import handle_message, handle_poll, handle_query_for_update

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Dronebot HTTP endpoint for error reporting
DRONEBOT_URL = os.environ.get('DRONEBOT_URL', 'http://localhost:3000')
DRONEBOT_TOKEN = os.environ.get('DRONEBOT_API_TOKEN', '')

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

    # Process targets
    target_results = []
    all_success = True

    # For MESSAGE tasks to a public channel, send ONE message mentioning all targets
    # For DM tasks, send separate messages to each target
    channel_type = task.get('channel', 'dm')
    is_public_channel = channel_type not in ('dm', 'group-dm') and task.get('channel_id')

    if task_type == 'MESSAGE' and is_public_channel:
        # Public channel message - send ONE message with all targets
        try:
            channel_id = task.get('channel_id')
            # Pass all targets as a combined string for the message
            all_targets = ' '.join(targets) if targets else ''
            result = handle_message(task, all_targets, channel_id, None)
            target_results.append({
                'target': all_targets,
                **result
            })
            if not result.get('success'):
                all_success = False
        except Exception as e:
            logger.error(f"Error processing task {task_id}: {e}")
            target_results.append({
                'target': 'all',
                'success': False,
                'error': str(e)
            })
            all_success = False
    else:
        # DM or other task types - process each target individually
        for target in targets:
            try:
                # Resolve channel for this target
                channel_id, user_id = resolve_channel(task.get('channel', 'dm'), target, task)

                if not channel_id and not user_id:
                    logger.warning(f"Could not resolve channel/user for task {task_id}, target {target}")
                    target_results.append({
                        'target': target,
                        'success': False,
                        'error': 'channel_resolution_failed'
                    })
                    all_success = False
                    continue

                # Fire the task based on type
                if task_type == 'MESSAGE':
                    result = handle_message(task, target, channel_id, user_id)
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
    Resolve channel type to actual channel ID or drone ID for DMs.

    Args:
        channel_type: 'dm', 'group-dm', 'priv-chan', 'priv-chan-group', or channel ID
        target: Target drone ID
        task: Full task object

    Returns:
        tuple of (channel_id, drone_id) - one will be set, other will be None
        For DMs: (None, drone_id) - dronebot will resolve to Discord user
        For channels: (channel_id, None)
    """
    # If channel_id is already stored in task (resolved at creation time), use it
    if task.get('channel_id'):
        return (task['channel_id'], None)

    # If it's already a channel ID (numeric string), return it
    if channel_type and channel_type.isdigit():
        return (channel_type, None)

    # Handle DM channel type - pass drone_id for dronebot to resolve
    if channel_type == 'dm':
        logger.info(f"DM task for drone {target} - dronebot will resolve")
        return (None, target)

    # For other special channel types (group-dm, priv-chan, priv-chan-group),
    # not yet implemented
    if channel_type in ('group-dm', 'priv-chan', 'priv-chan-group'):
        logger.warning(f"Special channel type '{channel_type}' not yet implemented")
        return (None, None)

    # If we get here, we can't resolve - this shouldn't happen with new tasks
    logger.warning(f"Cannot resolve channel '{channel_type}' - no channel_id stored")
    return (None, None)


def alert_cpu_errors(task, error_message):
    """Post error to #cpu-errors channel via dronebot HTTP endpoint."""
    if not DRONEBOT_URL or not DRONEBOT_TOKEN:
        logger.error("DRONEBOT_URL or DRONEBOT_API_TOKEN not configured for error alerts")
        return

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

        # Post error via dronebot HTTP endpoint
        payload = json.dumps({
            'content': content,
            'source': 'task-scheduler'
        }).encode('utf-8')

        url = f"{DRONEBOT_URL}/post/error"
        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                'Content-Type': 'application/json',
                'X-Dronebot-Token': DRONEBOT_TOKEN
            },
            method='POST'
        )

        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            if result.get('success'):
                logger.info(f"Posted error to cpu-errors: {result.get('message_id')}")
            else:
                logger.error(f"Failed to post error: {result.get('error')}")

    except urllib.error.HTTPError as e:
        logger.error(f"HTTP error posting to cpu-errors: {e.code}")
    except urllib.error.URLError as e:
        logger.error(f"URL error posting to cpu-errors: {e.reason}")
    except Exception as e:
        logger.error(f"Failed to alert cpu-errors: {e}")


# For local testing
if __name__ == '__main__':
    result = handler({}, None)
    print(json.dumps(result, indent=2))
