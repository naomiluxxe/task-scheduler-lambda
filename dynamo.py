"""DynamoDB operations for cpu-tasks table."""

import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('cpu-tasks')


def get_due_tasks():
    """Query active tasks where next_fire <= now."""
    now = datetime.utcnow().isoformat()

    response = table.query(
        IndexName='status-next_fire-index',
        KeyConditionExpression=Key('status').eq('active') & Key('next_fire').lte(now)
    )

    return response.get('Items', [])


def get_task(task_id, target):
    """Get a task by ID and target."""
    response = table.get_item(
        Key={
            'task_id': task_id,
            'target': target
        }
    )
    return response.get('Item')


def update_task(task_id, target, updates):
    """Update a task with the given fields."""
    update_parts = []
    expression_names = {}
    expression_values = {}

    for key, value in updates.items():
        if key in ('task_id', 'target'):
            continue

        attr_name = f'#{key}'
        attr_value = f':{key}'

        update_parts.append(f'{attr_name} = {attr_value}')
        expression_names[attr_name] = key
        expression_values[attr_value] = value

    if not update_parts:
        return None

    response = table.update_item(
        Key={
            'task_id': task_id,
            'target': target
        },
        UpdateExpression=f'SET {", ".join(update_parts)}',
        ExpressionAttributeNames=expression_names,
        ExpressionAttributeValues=expression_values,
        ReturnValues='ALL_NEW'
    )

    return response.get('Attributes')


def mark_task_fired(task):
    """Mark a task as fired and update state."""
    now = datetime.utcnow().isoformat()

    # Increment repeats_executed
    scheduler_params = task.get('scheduler_params', {})
    repeats_executed = scheduler_params.get('repeats_executed', 0) + 1
    num_repeats = scheduler_params.get('num_repeats', 0)

    # Check if task should be deactivated
    new_status = task['status']
    if num_repeats > 0 and repeats_executed >= num_repeats:
        new_status = 'inactive'

    # Calculate new next_fire
    next_fire = calculate_next_fire(task) if new_status == 'active' else None

    updated_scheduler_params = {
        **scheduler_params,
        'repeats_executed': repeats_executed
    }

    return update_task(task['task_id'], task['target'], {
        'last_fired': now,
        'next_fire': next_fire,
        'status': new_status,
        'scheduler_params': updated_scheduler_params,
        'error_count': 0,
        'last_error': None
    })


def record_task_error(task, error_message):
    """Record a task execution error."""
    error_count = task.get('error_count', 0) + 1

    return update_task(task['task_id'], task['target'], {
        'error_count': error_count,
        'last_error': str(error_message)
    })


def calculate_next_fire(task):
    """Calculate next fire time based on schedule settings."""
    from datetime import datetime, timedelta

    now = datetime.utcnow()

    # If recurring is set, calculate based on pattern
    recurring = task.get('recurring')
    schedule_time = task.get('schedule_time')

    if recurring:
        return calculate_recurring_next_fire(now, recurring, schedule_time)

    # If repeat_interval is set, calculate from now
    scheduler_params = task.get('scheduler_params', {})
    repeat_interval = scheduler_params.get('repeat_interval', 60)

    if repeat_interval:
        next_time = now + timedelta(minutes=repeat_interval)
        return next_time.isoformat()

    return None


def calculate_recurring_next_fire(from_date, recurring, preferred_time):
    """Calculate next fire time based on recurring pattern."""
    from datetime import datetime, timedelta

    next_time = from_date

    # Set preferred time if provided
    if preferred_time:
        hours, minutes = map(int, preferred_time.split(':'))
        next_time = next_time.replace(hour=hours, minute=minutes, second=0, microsecond=0)

    # Ensure we're in the future
    if next_time <= from_date:
        if recurring == 'hourly':
            next_time += timedelta(hours=1)
        elif recurring == 'daily':
            next_time += timedelta(days=1)
        elif recurring.startswith('weekly:'):
            target_day = recurring.split(':')[1].lower()
            days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
            try:
                target_day_index = days.index(target_day)
                current_day = next_time.weekday()
                # Convert to Sunday=0 format
                current_day = (current_day + 1) % 7
                days_to_add = target_day_index - current_day
                if days_to_add <= 0:
                    days_to_add += 7
                next_time += timedelta(days=days_to_add)
            except ValueError:
                next_time += timedelta(days=7)

    return next_time.isoformat()
