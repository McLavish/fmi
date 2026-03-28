# FMI on AWS Lambda: Python 3.11 + S3 Runbook

## 1. Install missing tools

```bash
aws --version
docker --version
sam --version
```

## 2. Initialize the repo correctly

The actual Git repo is the `fmi/` subdirectory.

```bash
cd fmi
git submodule set-url extern/TCPunch https://github.com/OpenCoreCH/TCPunch.git
git submodule update --init --recursive
```

## 3. Set the variables used by the rest of the commands

```bash
export AWS_REGION=eu-central-1
export STACK_PREFIX=fmi-demo
export FMI_LAYER_STACK=${STACK_PREFIX}-layer
export FMI_APP_STACK=${STACK_PREFIX}-app
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export FMI_BUCKET=${STACK_PREFIX}-${AWS_ACCOUNT_ID}-${AWS_REGION}
export FMI_COMM_NAME=fmi-demo-$(date +%s)
```

## 4. Create the S3 bucket FMI will use for communication

```bash
aws s3api create-bucket \
  --bucket "$FMI_BUCKET" \
  --region "$AWS_REGION" \
  --create-bucket-configuration LocationConstraint="$AWS_REGION"
```

## 5. Build the Python 3.11 FMI build image

```bash
cd /home/luca/fmi-original
docker build -t fmi-build-python311:latest -f fmi/runbooks/aws-python311-s3/Dockerfile.python3.11 .
```

## 6. Build and deploy the FMI Lambda layer

```bash
cd fmi/python/aws/python311
sam build
sam deploy \
  --stack-name "$FMI_LAYER_STACK" \
  --region "$AWS_REGION" \
  --resolve-s3 \
  --no-confirm-changeset \
  --no-fail-on-empty-changeset
```

Capture the published layer ARN:

```bash
export FMI_LAYER_ARN=$(aws cloudformation describe-stacks \
  --stack-name "$FMI_LAYER_STACK" \
  --region "$AWS_REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`LayerVersionArn`].OutputValue' \
  --output text)
echo "$FMI_LAYER_ARN"
```

## 7. Generate the example FMI config for the Lambda function

```bash
cd fmi/runbooks/aws-python311-s3/function
sed \
  -e "s#__FMI_BUCKET__#${FMI_BUCKET}#g" \
  -e "s#__AWS_REGION__#${AWS_REGION}#g" \
  fmi.json.template > fmi.json
```

## 8. Build and deploy the example Lambda function

```bash
cd fmi/runbooks/aws-python311-s3/function
sam build
sam deploy \
  --stack-name "$FMI_APP_STACK" \
  --region "$AWS_REGION" \
  --resolve-s3 \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides FmiLayerArn="$FMI_LAYER_ARN" FmiBucketName="$FMI_BUCKET" \
  --no-confirm-changeset \
  --no-fail-on-empty-changeset
```

Capture the deployed function name:

```bash
export FMI_FUNCTION_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$FMI_APP_STACK" \
  --region "$AWS_REGION" \
  --query 'Stacks[0].Outputs[?OutputKey==`FunctionName`].OutputValue' \
  --output text)
echo "$FMI_FUNCTION_NAME"
```

## 9. Invoke two Lambda peers at the same time

Both invocations must share the same `comm_name` and `num_peers`.

```bash
export FMI_COMM_NAME=fmi-demo-$(date +%s)
aws lambda invoke \
  --function-name "$FMI_FUNCTION_NAME" \
  --region "$AWS_REGION" \
  --cli-binary-format raw-in-base64-out \
  --payload "{\"peer_id\":0,\"num_peers\":2,\"comm_name\":\"$FMI_COMM_NAME\"}" \
  /tmp/fmi-peer0.json &
PID0=$!

aws lambda invoke \
  --function-name "$FMI_FUNCTION_NAME" \
  --region "$AWS_REGION" \
  --cli-binary-format raw-in-base64-out \
  --payload "{\"peer_id\":1,\"num_peers\":2,\"comm_name\":\"$FMI_COMM_NAME\"}" \
  /tmp/fmi-peer1.json &
PID1=$!

wait $PID0 $PID1
cat /tmp/fmi-peer0.json
cat /tmp/fmi-peer1.json
```

Expected result: both responses should report the same `sum_of_peer_ids`, which should be `1` for a two-peer run with peer IDs `0` and `1`.

## 10. Watch the logs if the first run hangs

```bash
sam logs --stack-name "$FMI_APP_STACK" --region "$AWS_REGION" --tail
```

## 11. Clean up

```bash
aws cloudformation delete-stack --stack-name "$FMI_APP_STACK" --region "$AWS_REGION"
aws cloudformation delete-stack --stack-name "$FMI_LAYER_STACK" --region "$AWS_REGION"
aws s3 rm "s3://${FMI_BUCKET}" --recursive
aws s3api delete-bucket --bucket "$FMI_BUCKET" --region "$AWS_REGION"
```
