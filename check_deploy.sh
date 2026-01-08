#!/bin/bash
# Check deployment status of task-scheduler Lambda
# Compares local file modification times against Lambda deployment time.
#
# Usage:
#     ./check_deploy.sh         # Check status
#     ./check_deploy.sh update  # Deploy if out of sync

set -e

LAMBDA_NAME="task-scheduler"
REGION="us-east-1"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Get the most recent modification time of local files
get_local_mtime() {
    # Find most recently modified file
    local latest=0
    for file in handler.py dynamo.py task_types/*.py; do
        if [ -f "$file" ]; then
            mtime=$(stat -f %m "$file" 2>/dev/null || stat -c %Y "$file" 2>/dev/null)
            if [ "$mtime" -gt "$latest" ]; then
                latest=$mtime
            fi
        fi
    done
    echo $latest
}

# Get Lambda deployment time
get_deployed_time() {
    local deployed=$(aws lambda get-function-configuration \
        --function-name "$LAMBDA_NAME" \
        --query "LastModified" \
        --output text \
        --region "$REGION" 2>/dev/null)

    if [ -z "$deployed" ] || [ "$deployed" == "None" ]; then
        echo "0"
        return
    fi

    # Convert ISO time to epoch (macOS compatible)
    # Format: 2026-01-08T21:12:19.000+0000
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS date command
        echo $(date -j -f "%Y-%m-%dT%H:%M:%S" "${deployed%%.*}" "+%s" 2>/dev/null || echo "0")
    else
        # Linux date command
        echo $(date -d "${deployed}" "+%s" 2>/dev/null || echo "0")
    fi
}

echo "Task Scheduler Lambda Deployment Status"
echo "========================================"
echo ""

cd "$(dirname "$0")"

LOCAL_TIME=$(get_local_mtime)
DEPLOYED_TIME=$(get_deployed_time)

echo "Lambda: $LAMBDA_NAME"
echo "Files: handler.py, dynamo.py, task_types/*.py"
echo ""

if [ "$DEPLOYED_TIME" == "0" ]; then
    echo -e "Status: ${YELLOW}NOT DEPLOYED${NC}"
    NEEDS_DEPLOY=true
elif [ "$LOCAL_TIME" -gt "$DEPLOYED_TIME" ]; then
    echo -e "Status: ${RED}NEEDS DEPLOY${NC}"
    NEEDS_DEPLOY=true
else
    echo -e "Status: ${GREEN}UP TO DATE${NC}"
    NEEDS_DEPLOY=false
fi

echo "Local modified:  $(date -r $LOCAL_TIME '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -d @$LOCAL_TIME '+%Y-%m-%d %H:%M:%S')"
if [ "$DEPLOYED_TIME" != "0" ]; then
    echo "Last deployed:   $(date -r $DEPLOYED_TIME '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -d @$DEPLOYED_TIME '+%Y-%m-%d %H:%M:%S')"
else
    echo "Last deployed:   never"
fi
echo ""

# If update argument passed, deploy
if [ "$1" == "update" ] && [ "$NEEDS_DEPLOY" == "true" ]; then
    echo "========================================"
    echo "Deploying..."
    ./deploy.sh
    echo ""
    echo -e "${GREEN}Deployed successfully${NC}"
elif [ "$1" == "update" ]; then
    echo "========================================"
    echo -e "${GREEN}Already up to date, nothing to deploy${NC}"
fi
