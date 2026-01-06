#!/bin/bash
# Deploy Task Scheduler Lambda

set -e

LAMBDA_NAME="task-scheduler"
REGION="us-east-1"
ROLE_NAME="task-scheduler-role"

echo "=== Deploying Task Scheduler Lambda ==="

# Get AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "Account ID: $ACCOUNT_ID"

# Create IAM role if it doesn't exist
echo "Checking IAM role..."
if ! aws iam get-role --role-name $ROLE_NAME 2>/dev/null; then
    echo "Creating IAM role: $ROLE_NAME"

    TRUST_POLICY=$(cat << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
)

    aws iam create-role \
        --role-name $ROLE_NAME \
        --assume-role-policy-document "$TRUST_POLICY"

    echo "Waiting for role to propagate..."
    sleep 10
fi

# Attach policies
echo "Attaching policies..."
LAMBDA_POLICY=$(cat << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudWatchLogs",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Sid": "DynamoDB",
            "Effect": "Allow",
            "Action": [
                "dynamodb:Query",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem",
                "dynamodb:Scan"
            ],
            "Resource": [
                "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/cpu-tasks",
                "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/cpu-tasks/index/*",
                "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/cpuDrones"
            ]
        },
        {
            "Sid": "InvokeLambda",
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": [
                "arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:dronebot",
                "arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:void-mother-chat",
                "arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:void-mother-response-generator",
                "arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:greeter-drone",
                "arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:propaganda-drone"
            ]
        }
    ]
}
EOF
)

aws iam put-role-policy \
    --role-name $ROLE_NAME \
    --policy-name "${ROLE_NAME}-policy" \
    --policy-document "$LAMBDA_POLICY"

ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

# Package Lambda
echo "Packaging Lambda..."
cd "$(dirname "$0")"
rm -f lambda.zip
zip -r lambda.zip handler.py dynamo.py task_types/

# Deploy Lambda
echo "Deploying Lambda..."
if aws lambda get-function --function-name $LAMBDA_NAME --region $REGION 2>/dev/null; then
    aws lambda update-function-code \
        --function-name $LAMBDA_NAME \
        --zip-file fileb://lambda.zip \
        --region $REGION > /dev/null

    echo "Waiting for update..."
    sleep 5

    aws lambda update-function-configuration \
        --function-name $LAMBDA_NAME \
        --timeout 300 \
        --memory-size 256 \
        --environment "Variables={DRONEBOT_URL=${DRONEBOT_URL:-},DRONEBOT_API_TOKEN=${DRONEBOT_API_TOKEN:-},RESPONSE_GENERATOR_LAMBDA=${RESPONSE_GENERATOR_LAMBDA:-void-mother-response-generator}}" \
        --region $REGION > /dev/null

    echo "Updated Lambda: $LAMBDA_NAME"
else
    aws lambda create-function \
        --function-name $LAMBDA_NAME \
        --runtime python3.12 \
        --role "$ROLE_ARN" \
        --handler handler.handler \
        --zip-file fileb://lambda.zip \
        --timeout 300 \
        --memory-size 256 \
        --environment "Variables={DRONEBOT_URL=${DRONEBOT_URL:-},DRONEBOT_API_TOKEN=${DRONEBOT_API_TOKEN:-},RESPONSE_GENERATOR_LAMBDA=${RESPONSE_GENERATOR_LAMBDA:-void-mother-response-generator}}" \
        --region $REGION > /dev/null

    echo "Created Lambda: $LAMBDA_NAME"
fi

rm lambda.zip

# Create EventBridge rule
echo "Setting up EventBridge schedule..."
RULE_NAME="task-scheduler"

if ! aws events describe-rule --name $RULE_NAME --region $REGION 2>/dev/null; then
    aws events put-rule \
        --name $RULE_NAME \
        --schedule-expression "rate(15 minutes)" \
        --state ENABLED \
        --description "Trigger task scheduler every 15 minutes" \
        --region $REGION

    aws lambda add-permission \
        --function-name $LAMBDA_NAME \
        --statement-id eventbridge-invoke \
        --action lambda:InvokeFunction \
        --principal events.amazonaws.com \
        --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
        --region $REGION 2>/dev/null || true

    aws events put-targets \
        --rule $RULE_NAME \
        --targets "Id"="task-scheduler","Arn"="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${LAMBDA_NAME}" \
        --region $REGION

    echo "Created EventBridge rule: $RULE_NAME"
else
    echo "EventBridge rule already exists: $RULE_NAME"
fi

echo ""
echo "=== Deployment Complete ==="
echo "Lambda: $LAMBDA_NAME"
echo "Schedule: Every 15 minutes"
echo ""
echo "To test manually:"
echo "  aws lambda invoke --function-name $LAMBDA_NAME --region $REGION /dev/stdout"
