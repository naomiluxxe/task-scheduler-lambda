"""QUERY-FOR-UPDATE task type handler with agentic tool loop.

Enables LLM-driven query flows where the agent can:
1. Query DynamoDB tables
2. Reason about results
3. Decide whether to send a message
4. Loop up to max_iterations times
"""

import json
import os
import random
import boto3
import logging
import urllib.request
import urllib.error
from decimal import Decimal

logger = logging.getLogger(__name__)
lambda_client = boto3.client('lambda', region_name='us-east-1')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Configuration
DRONE_DATA_TABLE = 'cpu-drone-data'
DRONEBOT_URL = os.environ.get('DRONEBOT_URL', 'http://localhost:3000')
DRONEBOT_TOKEN = os.environ.get('DRONEBOT_API_TOKEN', '')
BEDROCK_MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'us.anthropic.claude-haiku-4-5-20251001-v1:0')

# Tool definitions for the LLM (Bedrock Converse API format)
AVAILABLE_TOOLS = [
    {
        "toolSpec": {
            "name": "list_drones",
            "description": "Get list of all drone IDs in the hive",
            "inputSchema": {
                "json": {"type": "object", "properties": {}}
            }
        }
    },
    {
        "toolSpec": {
            "name": "pick_random_drone",
            "description": "Select a random drone from the hive",
            "inputSchema": {
                "json": {"type": "object", "properties": {}}
            }
        }
    },
    {
        "toolSpec": {
            "name": "get_drone_config",
            "description": "Get full configuration for a specific drone",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "drone_id": {"type": "string", "description": "The drone ID (e.g., 0x1d31)"}
                    },
                    "required": ["drone_id"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "check_stale_config",
            "description": "Check which config fields are stale/empty for a drone. Returns list of fields that need attention.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "drone_id": {"type": "string", "description": "The drone ID to check"}
                    },
                    "required": ["drone_id"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "send_message",
            "description": "Send a message to a drone. Call this when you've decided what to say. This ends the loop.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "content": {"type": "string", "description": "The message to send"},
                        "drone_id": {"type": "string", "description": "The drone to message"}
                    },
                    "required": ["content", "drone_id"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "skip_message",
            "description": "Decide not to send any message. Call this if the drone's config is complete or no action needed.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "reason": {"type": "string", "description": "Why no message is needed"}
                    },
                    "required": ["reason"]
                }
            }
        }
    }
]


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


def handle_query_for_update(task, target, channel_id):
    """
    Agentic QUERY-FOR-UPDATE that lets the LLM drive the query flow.

    Loop:
    1. Call LLM with available tools
    2. If LLM requests tool call, execute and return result
    3. Repeat until LLM calls send_message/skip_message or max iterations
    """
    max_iterations_raw = task.get('agent_params', {}).get('max_iterations', 5)
    max_iterations = int(max_iterations_raw) if max_iterations_raw else 5
    assignee = task.get('assignee', 'void-mother')
    content_prompt = task.get('payload', {}).get('content', '')
    task_id = task.get('task_id', 'unknown')

    if not content_prompt:
        return {'success': False, 'error': 'No content prompt provided'}

    # Build system prompt for the agent
    system_prompt = f"""You are {assignee}, a helpful drone assistant. You have access to tools to query drone data and send messages.

Your task is described below. Use the tools to accomplish it. Be efficient - don't make unnecessary queries.

When you've gathered enough information, either:
- Call send_message to send a message to the drone
- Call skip_message if no message is needed (e.g., their config is already complete)

IMPORTANT: When sending a message, you MUST address the drone by their ID (e.g., "Hey 0x3604, ...") at the start of your message. This is crucial as the message goes to a shared channel and the drone needs to know it's meant for them.

Be specific and helpful in your messages. Reference the actual field names that need attention."""

    messages = [{"role": "user", "content": [{"text": content_prompt}]}]

    for i in range(max_iterations):
        logger.info(f"[{task_id}] Agentic loop iteration {i+1}/{max_iterations}")

        try:
            # Call Bedrock with tools
            response = bedrock.converse(
                modelId=BEDROCK_MODEL_ID,
                system=[{"text": system_prompt}],
                messages=messages,
                toolConfig={"tools": AVAILABLE_TOOLS}
            )

            stop_reason = response.get('stopReason')
            output = response.get('output', {})
            message = output.get('message', {})

            logger.info(f"[{task_id}] LLM stop_reason: {stop_reason}")

            if stop_reason == 'tool_use':
                # Process tool calls
                assistant_content = message.get('content', [])
                messages.append({"role": "assistant", "content": assistant_content})

                tool_results = []
                for block in assistant_content:
                    if 'toolUse' in block:
                        tool_use = block['toolUse']
                        tool_name = tool_use['name']
                        tool_input = tool_use.get('input', {})
                        tool_id = tool_use['toolUseId']

                        logger.info(f"[{task_id}] Executing tool: {tool_name}")
                        result = execute_tool(tool_name, tool_input, channel_id, assignee, task_id)

                        tool_results.append({
                            "toolResult": {
                                "toolUseId": tool_id,
                                "content": [{"json": result}]
                            }
                        })

                        # Check for terminal tools
                        if tool_name == 'send_message' and result.get('success'):
                            return {
                                'success': True,
                                'message_sent': True,
                                'iterations': i + 1,
                                'message_id': result.get('message_id'),
                                'drone_id': tool_input.get('drone_id')
                            }
                        elif tool_name == 'skip_message':
                            return {
                                'success': True,
                                'message_sent': False,
                                'reason': tool_input.get('reason', 'no_action_needed'),
                                'iterations': i + 1
                            }

                # Add tool results to conversation
                messages.append({"role": "user", "content": tool_results})

            elif stop_reason == 'end_turn':
                # LLM finished without calling terminal tool
                text_content = ''
                for block in message.get('content', []):
                    if 'text' in block:
                        text_content = block['text']
                        break

                logger.info(f"[{task_id}] LLM ended without action: {text_content[:100]}")
                return {
                    'success': True,
                    'message_sent': False,
                    'reason': 'llm_ended_without_action',
                    'llm_response': text_content[:500],
                    'iterations': i + 1
                }

            else:
                logger.warning(f"[{task_id}] Unexpected stop_reason: {stop_reason}")
                return {
                    'success': False,
                    'error': f'Unexpected stop_reason: {stop_reason}',
                    'iterations': i + 1
                }

        except Exception as e:
            logger.error(f"[{task_id}] Error in agentic loop: {e}")
            return {
                'success': False,
                'error': str(e),
                'iterations': i + 1
            }

    return {
        'success': False,
        'error': 'max_iterations_reached',
        'iterations': max_iterations
    }


def execute_tool(tool_name, tool_input, channel_id, assignee, task_id):
    """Execute a tool call and return the result."""
    try:
        if tool_name == 'list_drones':
            return tool_list_drones()
        elif tool_name == 'pick_random_drone':
            return tool_pick_random_drone()
        elif tool_name == 'get_drone_config':
            return tool_get_drone_config(tool_input.get('drone_id'))
        elif tool_name == 'check_stale_config':
            return tool_check_stale_config(tool_input.get('drone_id'))
        elif tool_name == 'send_message':
            return tool_send_message(
                tool_input.get('content'),
                tool_input.get('drone_id'),
                channel_id,
                assignee,
                task_id
            )
        elif tool_name == 'skip_message':
            return {'success': True, 'skipped': True, 'reason': tool_input.get('reason')}
        else:
            return {'error': f'Unknown tool: {tool_name}'}
    except Exception as e:
        logger.error(f"Tool {tool_name} error: {e}")
        return {'error': str(e)}


def tool_list_drones():
    """List all drones from cpu-drone-data table."""
    table = dynamodb.Table(DRONE_DATA_TABLE)
    response = table.scan(ProjectionExpression='droneid', Limit=100)
    drones = [item['droneid'] for item in response.get('Items', [])]
    return {'drones': drones, 'count': len(drones)}


def tool_pick_random_drone():
    """Pick a random drone from the hive."""
    result = tool_list_drones()
    drones = result.get('drones', [])
    if not drones:
        return {'error': 'No drones found in hive'}
    selected = random.choice(drones)
    return {'drone_id': selected, 'total_drones': len(drones)}


def tool_get_drone_config(drone_id):
    """Get a drone's full configuration."""
    if not drone_id:
        return {'error': 'drone_id is required'}

    table = dynamodb.Table(DRONE_DATA_TABLE)
    response = table.get_item(Key={'droneid': drone_id})
    item = response.get('Item')

    if not item:
        return {'error': f'Drone {drone_id} not found'}

    # Convert Decimals and return
    config = convert_decimals(item.get('configuration', {}))
    return {
        'drone_id': drone_id,
        'configuration': config
    }


def tool_check_stale_config(drone_id):
    """Check which config fields are stale/empty for a drone."""
    config_result = tool_get_drone_config(drone_id)
    if config_result.get('error'):
        return config_result

    config = config_result.get('configuration', {})
    stale_fields = []

    # Check behavioral_matrices for default values (50)
    bm = config.get('behavioral_matrices', {})
    behavioral_fields = [
        'sadistic_kind_tolerance',
        'control_autonomy_balance',
        'punishment_reward_perception',
        'degradation_pleasure_threshold',
        'emptiness_presence_spectrum'
    ]
    for field in behavioral_fields:
        value = bm.get(field)
        if value == 50 or value is None:
            stale_fields.append({
                'category': 'behavioral_matrices',
                'field': field,
                'current_value': value,
                'reason': 'default_value' if value == 50 else 'missing'
            })

    # Check boundary_mapping for empty arrays
    bounds = config.get('boundary_mapping', {})
    boundary_fields = ['red_limits', 'green_triggers', 'yellow_cautions']
    for field in boundary_fields:
        value = bounds.get(field, [])
        if not value:
            stale_fields.append({
                'category': 'boundary_mapping',
                'field': field,
                'current_value': value,
                'reason': 'empty'
            })

    # Check programming_metrics
    pm = config.get('programming_metrics', {})
    if not pm.get('recovery_requirements'):
        stale_fields.append({
            'category': 'programming_metrics',
            'field': 'recovery_requirements',
            'current_value': pm.get('recovery_requirements', ''),
            'reason': 'empty'
        })

    return {
        'drone_id': drone_id,
        'stale_fields': stale_fields,
        'total_stale': len(stale_fields),
        'needs_attention': len(stale_fields) > 0
    }


def tool_send_message(content, drone_id, channel_id, assignee, task_id):
    """Send message via dronebot HTTP endpoint."""
    if not content:
        return {'success': False, 'error': 'Message content is required'}
    if not drone_id:
        return {'success': False, 'error': 'drone_id is required'}
    if not channel_id:
        return {'success': False, 'error': 'channel_id not available'}
    if not DRONEBOT_URL:
        return {'success': False, 'error': 'DRONEBOT_URL not configured'}
    if not DRONEBOT_TOKEN:
        return {'success': False, 'error': 'DRONEBOT_API_TOKEN not configured'}

    payload = json.dumps({
        'agent_id': assignee,
        'channel_id': channel_id,
        'content': content,
        'task_id': task_id,
        'target': drone_id
    }).encode('utf-8')

    url = f"{DRONEBOT_URL}/task/execute"
    logger.info(f"Sending message to {url} for drone {drone_id}")

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
