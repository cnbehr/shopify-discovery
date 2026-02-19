#!/bin/bash
# Deploy discovery worker as a Cloud Run Job
#
# Job definition:
#   discovery-worker - Discovers new Shopify stores (200 tasks, 100 parallelism)
#
# Build from repo root for consistent Docker context

set -e

PROJECT_ID="${PROJECT_ID:-shopifydb}"
REGION="${REGION:-us-central1}"
IMAGE="gcr.io/${PROJECT_ID}/discovery-worker"

# Parse optional --build-only or --skip-build flags
BUILD=true
DEPLOY=true
for arg in "$@"; do
    case $arg in
        --build-only) DEPLOY=false ;;
        --skip-build) BUILD=false ;;
    esac
done

echo "=== Discovery Worker Deployment ==="
echo ""

# Build from repo root for consistent Docker context
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

if [ "$BUILD" = true ]; then
    echo "Building Docker image from ${REPO_ROOT}..."
    docker build --platform linux/amd64 -t ${IMAGE} -f "$(dirname "$0")/Dockerfile" "${REPO_ROOT}"

    echo "Pushing to GCR..."
    docker push ${IMAGE}
    echo ""
fi

if [ "$DEPLOY" = false ]; then
    echo "Build complete (--build-only). Skipping deployment."
    exit 0
fi

# Helper: create or update a Cloud Run Job
deploy_job() {
    local job_name=$1
    local task_count=$2
    local parallelism=$3
    local timeout=$4
    local memory=$5
    local cpu=$6

    echo "--- Deploying: ${job_name} (${task_count} tasks, parallelism=${parallelism}) ---"

    # Delete existing job if present (can't update task count on existing jobs)
    gcloud run jobs delete ${job_name} --region=${REGION} --quiet 2>/dev/null || true

    gcloud run jobs create ${job_name} \
        --image=${IMAGE} \
        --region=${REGION} \
        --tasks=${task_count} \
        --parallelism=${parallelism} \
        --max-retries=1 \
        --task-timeout=${timeout} \
        --memory=${memory} \
        --cpu=${cpu} \
        --set-env-vars="PROJECT_ID=${PROJECT_ID},DATASET_ID=shopify_intelligence"

    echo "  -> ${job_name} deployed"
    echo ""
}

# -------------------------------------------------------
# Discovery worker: find new Shopify stores
# -------------------------------------------------------
deploy_job "discovery-worker" 200 100 3600s 2Gi 1

echo "=== Discovery Worker Deployed ==="
echo ""
echo "Manual execution:"
echo "  gcloud run jobs execute discovery-worker --region=${REGION}"
echo ""
echo "Monitor:"
echo "  gcloud run jobs executions list --job=discovery-worker --region=${REGION}"
echo ""
echo "Logs:"
echo "  gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=discovery-worker' --limit=100 --format='table(timestamp,textPayload)'"
