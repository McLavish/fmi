#!/usr/bin/env bash
set -euo pipefail

RUNBOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$RUNBOOK_DIR/../.." && pwd)"
LAYER_BUILD_DIR="$REPO_ROOT/python/aws/python311/.aws-sam/build"
ENDPOINT_URL="http://localhost:4566"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=true

echo "Prerequisites: build the FMI Python 3.11 image and SAM layer first:"
echo "  docker build -t fmi-build-python311 -f runbooks/aws-python311-s3/Dockerfile.python3.11 ."
echo "  (cd python/aws/python311 && sam build)"

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

LAYER_DIR="$(find "$LAYER_BUILD_DIR" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
if [ -z "$LAYER_DIR" ]; then
  echo "No SAM layer build directory found under $LAYER_BUILD_DIR" >&2
  exit 1
fi

rm -f layer.zip function.zip
(cd "$LAYER_DIR" && zip -qr "$RUNBOOK_DIR/layer.zip" .)

LAYER_JSON="$(aws --endpoint-url="$ENDPOINT_URL" lambda publish-layer-version \
  --layer-name fmi-layer \
  --zip-file fileb://layer.zip \
  --compatible-runtimes python3.11)"
LAYER_ARN="$(printf '%s' "$LAYER_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["LayerVersionArn"])')"

STAGING_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGING_DIR"' EXIT
cp lambda_function.py fmi-worker.json "$STAGING_DIR/"
cp "$LAYER_DIR/python/fmi.so" "$STAGING_DIR/"
mkdir -p "$STAGING_DIR/lib"
cp -L "$LAYER_DIR"/lib/* "$STAGING_DIR/lib/"
(cd "$STAGING_DIR" && zip -qr "$RUNBOOK_DIR/function.zip" .)

if ! aws --endpoint-url="$ENDPOINT_URL" lambda create-function \
  --function-name fmi-migration-worker \
  --runtime python3.11 \
  --handler lambda_function.lambda_handler \
  --timeout 120 \
  --memory-size 1024 \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --zip-file fileb://function.zip \
  --environment Variables={LD_LIBRARY_PATH=/var/task/lib:/opt/lib} \
  --layers "$LAYER_ARN"; then
  aws --endpoint-url="$ENDPOINT_URL" lambda update-function-code \
    --function-name fmi-migration-worker \
    --zip-file fileb://function.zip
  aws --endpoint-url="$ENDPOINT_URL" lambda update-function-configuration \
    --function-name fmi-migration-worker \
    --environment Variables={LD_LIBRARY_PATH=/var/task/lib:/opt/lib} \
    --layers "$LAYER_ARN" >/dev/null
fi

echo "Deployed fmi-migration-worker with layer $LAYER_ARN and fat function zip"
