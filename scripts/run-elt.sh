#!/bin/bash
# Run ELT pipeline via act (GitHub Actions locally)
#
# Usage:
#   ./scripts/run-elt.sh              # Run default job (elt-all)
#   ./scripts/run-elt.sh el-postgres  # Run specific job
#   ./scripts/run-elt.sh elt-postgres # Run unified ELT for postgres
#
# Prerequisites:
#   - act installed: brew install act (macOS) or see https://github.com/nektos/act
#   - Docker Compose stack running: docker compose up -d
#   - Meltano installed in ./meltano directory

set -e

JOB=${1:-elt-all}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Validate job name
VALID_JOBS=("elt-all" "elt-postgres" "el-all" "el-postgres")
JOB_VALID=false
for valid in "${VALID_JOBS[@]}"; do
    if [[ "$JOB" == "$valid" ]]; then
        JOB_VALID=true
        break
    fi
done

if [[ "$JOB_VALID" == "false" ]]; then
    echo "Error: Invalid job '$JOB'"
    echo "Valid jobs: ${VALID_JOBS[*]}"
    exit 1
fi

# Check if act is installed
if ! command -v act &> /dev/null; then
    echo "Error: 'act' is not installed."
    echo "Install with: brew install act (macOS) or see https://github.com/nektos/act"
    exit 1
fi

# Check if docker network exists
if ! docker network inspect paracelsus-network &> /dev/null; then
    echo "Error: Docker network 'paracelsus-network' not found."
    echo "Start the stack first: docker compose up -d"
    exit 1
fi

# Check if .github/act/.env exists
if [[ ! -f ".github/act/.env" ]]; then
    echo "Error: .github/act/.env not found."
    echo "Create it with local secrets (see .github/act/.env.example)"
    exit 1
fi

echo "Running ELT job: $JOB"
echo "Using network: paracelsus-network"
echo ""

# Run the workflow with act
act workflow_dispatch \
    --input job="$JOB" \
    -j run-elt \
    --network paracelsus-network \
    --secret-file .github/act/.env \
    --env-file .github/act/.env \
    --env MELTANO_ENVIRONMENT=dev

echo ""
echo "ELT job '$JOB' completed"
