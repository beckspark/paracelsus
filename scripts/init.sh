#!/bin/bash
# Paracelsus - Full initialization script
# Run this after docker-compose up to set up the entire pipeline

set -euo pipefail

cleanup_all() {
    echo -e "${YELLOW}Tearing down for clean retry...${NC}"
    docker-compose down -v 2>/dev/null || true
    # Nuke any local state that could block retries
    find . -name "*.lock*" -type f -delete 2>/dev/null || true
    find . -name ".terraform.tfstate*" -type f -delete 2>/dev/null || true
}
trap 'echo -e "${RED}Error on line $LINENO: $BASH_COMMAND${NC}"; cleanup_all; exit 1' ERR

echo "=========================================="
echo "Paracelsus ELT Pipeline Initialization"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Wait for services to be ready
wait_for_service() {
    local service=$1
    local url=$2
    local max_attempts=30
    local attempt=1

    echo -e "${YELLOW}Waiting for $service...${NC}"
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}$service is ready!${NC}"
            return 0
        fi
        echo "  Attempt $attempt/$max_attempts..."
        sleep 2
        ((attempt++))
    done
    echo -e "${RED}$service failed to start${NC}"
    return 1
}

# Step 1: Wait for core services
echo ""
echo "Step 1: Waiting for services to be ready..."
wait_for_service "LocalStack" "http://localhost:4566/_localstack/health"
wait_for_service "Mock HubSpot" "http://localhost:8001/health"

# Step 2: Run Terraform
echo ""
echo "Step 2: Provisioning LocalStack resources with Terraform..."
docker-compose exec -T terraform sh -c "
    cd /terraform && \
    terraform init && \
    terraform apply -auto-approve
"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Terraform provisioning complete!${NC}"
else
    echo -e "${RED}Terraform provisioning failed${NC}"
    exit 1
fi

# Step 3: Verify LocalStack resources
echo ""
echo "Step 3: Verifying LocalStack resources..."
docker-compose exec -T localstack awslocal s3 ls
docker-compose exec -T localstack awslocal events list-rules
docker-compose exec -T localstack awslocal lambda list-functions

# Step 4: Verify data seeding
echo ""
echo "Step 4: Checking data seeding status..."

# Check OLTP database
OLTP_COUNT=$(docker-compose exec -T oltp-db psql -U admin -d supervision -t -c "SELECT COUNT(*) FROM physicians;" 2>/dev/null | tr -d ' ')
if [ "$OLTP_COUNT" -gt 0 ] 2>/dev/null; then
    echo -e "${GREEN}OLTP database seeded: $OLTP_COUNT physicians${NC}"
else
    echo -e "${YELLOW}OLTP database not yet seeded (seeder container may still be running)${NC}"
fi

# Check S3
S3_FILES=$(docker-compose exec -T localstack awslocal s3 ls s3://paracelsus-landing/ --recursive 2>/dev/null | wc -l)
if [ "$S3_FILES" -gt 0 ]; then
    echo -e "${GREEN}S3 bucket seeded: $S3_FILES files${NC}"
else
    echo -e "${YELLOW}S3 bucket not yet seeded${NC}"
fi

# Step 5: Install Meltano plugins (including dbt-postgres)
echo ""
echo "Step 5: Installing Meltano plugins (may take several minutes)..."
docker-compose exec -T meltano meltano lock --update --all
docker-compose exec -T meltano meltano install
echo -e "${GREEN}Meltano plugins installed (including dbt-postgres utility)!${NC}"

# Step 6: Summary
echo ""
echo "=========================================="
echo "Initialization Summary"
echo "=========================================="
echo ""
echo "Services:"
echo "  - LocalStack:    http://localhost:4566"
echo "  - Mock HubSpot:  http://localhost:8001"
echo "  - OLTP DB:       localhost:5433 (admin/oltp_secret)"
echo "  - OLAP DB:       localhost:5435 (warehouse/olap_secret)"
echo ""
echo "Architecture (Phase 2 - Simplified):"
echo "  - Meltano handles EL + dbt Transform in single container"
echo "  - EventBridge Scheduler triggers Lambda on schedule"
echo "  - Lambda invokes Meltano container via docker exec"
echo ""
echo "Next steps - Run unified ELT pipeline:"
echo ""
echo "  # Option 1: Run EL only (no transform)"
echo "  docker-compose exec meltano meltano run el-postgres"
echo ""
echo "  # Option 2: Run unified ELT (Extract, Load, Transform)"
echo "  docker-compose exec meltano meltano run elt-postgres"
echo ""
echo "  # Option 3: Run individual dbt commands"
echo "  docker-compose exec meltano meltano invoke dbt-postgres run"
echo "  docker-compose exec meltano meltano invoke dbt-postgres test"
echo ""
echo "  # Option 4: Trigger via Lambda (simulates EventBridge)"
echo "  awslocal lambda invoke --function-name paracelsus-trigger-meltano /tmp/response.json"
echo ""
echo "Query the mart tables:"
echo "  docker-compose exec olap-db psql -U warehouse -d analytics -c \\"
echo "    \"SELECT COUNT(*) FROM public_marts.fact_provider_case_load\""
echo ""
echo -e "${GREEN}Initialization complete!${NC}"
