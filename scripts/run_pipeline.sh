#!/bin/bash
# Paracelsus - Manual pipeline trigger script
# Triggers the full ELT pipeline via Step Functions

set -e

echo "=========================================="
echo "Paracelsus ELT Pipeline - Manual Trigger"
echo "=========================================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Get the state machine ARN
STATE_MACHINE_ARN=$(docker-compose exec -T localstack awslocal stepfunctions list-state-machines \
    --query 'stateMachines[?contains(name, `paracelsus`)].stateMachineArn' \
    --output text 2>/dev/null)

if [ -z "$STATE_MACHINE_ARN" ]; then
    echo "Error: State machine not found. Run init.sh first."
    exit 1
fi

echo -e "${YELLOW}Found state machine: $STATE_MACHINE_ARN${NC}"

# Start execution
echo ""
echo "Starting pipeline execution..."
EXECUTION_ARN=$(docker-compose exec -T localstack awslocal stepfunctions start-execution \
    --state-machine-arn "$STATE_MACHINE_ARN" \
    --input '{"job": "el-all", "environment": "dev", "triggered_by": "manual"}' \
    --query 'executionArn' \
    --output text)

echo -e "${GREEN}Execution started: $EXECUTION_ARN${NC}"

# Monitor execution
echo ""
echo "Monitoring execution status..."
while true; do
    STATUS=$(docker-compose exec -T localstack awslocal stepfunctions describe-execution \
        --execution-arn "$EXECUTION_ARN" \
        --query 'status' \
        --output text)

    echo "  Status: $STATUS"

    if [ "$STATUS" = "SUCCEEDED" ]; then
        echo -e "${GREEN}Pipeline completed successfully!${NC}"
        break
    elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "TIMED_OUT" ] || [ "$STATUS" = "ABORTED" ]; then
        echo "Pipeline failed with status: $STATUS"
        docker-compose exec -T localstack awslocal stepfunctions describe-execution \
            --execution-arn "$EXECUTION_ARN"
        exit 1
    fi

    sleep 5
done

echo ""
echo "Pipeline execution complete. Check the OLAP database for results:"
echo "  docker-compose exec olap-db psql -U warehouse -d analytics"
echo ""
echo "Example query:"
echo "  SELECT * FROM marts.fact_provider_case_load LIMIT 10;"
