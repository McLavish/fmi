#!/usr/bin/env bash
set -euo pipefail

RUNBOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$RUNBOOK_DIR/../.." && pwd)"
ENDPOINT_URL="http://localhost:4566"
BUILD_IMAGE_TAG="${BUILD_IMAGE_TAG:-fmi-localstack-build:redis-gcc10}"
BUNDLE_DIR="$RUNBOOK_DIR/build/bundle"
FUNCTION_ZIP="$RUNBOOK_DIR/build/function.zip"
FUNCTION_CONFIG_JSON='{"FunctionName":"fmi-migration-worker","Environment":{"Variables":{"LD_LIBRARY_PATH":"/var/task/lib"}},"Layers":[]}'

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=true

cd "$RUNBOOK_DIR"

docker compose up -d

for attempt in $(seq 1 30); do
  if curl -sf "$ENDPOINT_URL/_localstack/health" >/dev/null; then
    break
  fi
  if [ "$attempt" -eq 30 ]; then
    echo "LocalStack health check did not pass after 60 seconds" >&2
    exit 1
  fi
  sleep 2
done

echo "Building Redis-only FMI Lambda bundle with $BUILD_IMAGE_TAG"
docker build -t "$BUILD_IMAGE_TAG" -f "$RUNBOOK_DIR/Dockerfile.build" "$REPO_ROOT"
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  --mount type=bind,source="$REPO_ROOT",target=/opt/fmi \
  "$BUILD_IMAGE_TAG" \
  /opt/fmi/runbooks/localstack-python311-redis/make-fmi-bundle.sh

rm -f "$FUNCTION_ZIP"
mkdir -p "$RUNBOOK_DIR/build"

STAGING_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGING_DIR"' EXIT
cp lambda_function.py worker_core.py fmi-worker.json "$STAGING_DIR/"
cp "$BUNDLE_DIR/fmi.so" "$STAGING_DIR/"
mkdir -p "$STAGING_DIR/lib"
if compgen -G "$BUNDLE_DIR/lib/*" >/dev/null; then
  cp -L "$BUNDLE_DIR"/lib/* "$STAGING_DIR/lib/"
fi
(cd "$STAGING_DIR" && zip -qr "$FUNCTION_ZIP" .)

if ! aws --endpoint-url="$ENDPOINT_URL" lambda create-function \
  --function-name fmi-migration-worker \
  --runtime python3.11 \
  --handler lambda_function.lambda_handler \
  --timeout 120 \
  --memory-size 1024 \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --zip-file "fileb://$FUNCTION_ZIP" \
  --environment Variables={LD_LIBRARY_PATH=/var/task/lib}; then
  aws --endpoint-url="$ENDPOINT_URL" lambda update-function-code \
    --function-name fmi-migration-worker \
    --zip-file "fileb://$FUNCTION_ZIP"
  aws --endpoint-url="$ENDPOINT_URL" lambda update-function-configuration \
    --cli-input-json "$FUNCTION_CONFIG_JSON" >/dev/null
fi

for attempt in $(seq 1 30); do
  read -r state update_status < <(aws --endpoint-url="$ENDPOINT_URL" lambda get-function-configuration \
    --function-name fmi-migration-worker \
    --query '[State,LastUpdateStatus]' \
    --output text)
  if [ "$state" = "Active" ] && [ "$update_status" = "Successful" ]; then
    break
  fi
  if [ "$attempt" -eq 30 ]; then
    echo "Function did not become active after 30 seconds: state=$state update=$update_status" >&2
    exit 1
  fi
  sleep 1
done

echo "Deployed fmi-migration-worker with Redis-only fat function zip"
