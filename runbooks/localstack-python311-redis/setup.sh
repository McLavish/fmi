#!/usr/bin/env bash
set -euo pipefail

RUNBOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$RUNBOOK_DIR/../.." && pwd)"
LAYER_BUILD_DIR="$REPO_ROOT/python/aws/python311/.aws-sam/build"
ENDPOINT_URL="http://localhost:4566"

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

zip -q function.zip lambda_function.py fmi-worker.json

if ! aws --endpoint-url="$ENDPOINT_URL" lambda create-function \
  --function-name fmi-migration-worker \
  --runtime python3.11 \
  --handler lambda_function.lambda_handler \
  --timeout 120 \
  --memory-size 1024 \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --zip-file fileb://function.zip \
  --layers "$LAYER_ARN"; then
  aws --endpoint-url="$ENDPOINT_URL" lambda update-function-code \
    --function-name fmi-migration-worker \
    --zip-file fileb://function.zip
fi

# Fat-zip fallback: if the LocalStack layer path fails, bundle fmi.so and
# runtime-lib/ into function.zip, then create/update the function with:
# --environment Variables={LD_LIBRARY_PATH=/var/task/lib}

echo "Deployed fmi-migration-worker with layer $LAYER_ARN"
