# FMI on AWS Lambda: Python 3.11 + S3 Runbook

This folder is the shortest path I could make from this repo to a working FMI deployment on AWS Lambda.

Assumptions:

- Linux or WSL on `x86_64`
- Docker is installed and your user can access the Docker daemon
- AWS CLI credentials are already configured, or you will run `aws configure`
- You want the least operationally painful backend first, so this runbook uses `S3`, not `Direct`

## 1. Install missing tools

Skip the `aws` install if you already have it. This repo already sees `aws`, but not `sam`.

```bash
aws --version
docker --version
sam --version
```

If `sam` is missing on Linux `x86_64`, install it:

```bash
cd /tmp
curl -L -o aws-sam-cli-linux-x86_64.zip https://github.com/aws/aws-sam-cli/releases/latest/download/aws-sam-cli-linux-x86_64.zip
rm -rf sam-installation
unzip -q aws-sam-cli-linux-x86_64.zip -d sam-installation
sudo ./sam-installation/install
sam --version
```

If your AWS CLI is not configured yet:

```bash
aws configure
```

## 2. Initialize the repo correctly

The actual Git repo is the `fmi/` subdirectory.

```bash
cd /home/luca/fmi-original/fmi
git submodule set-url extern/TCPunch https://github.com/OpenCoreCH/TCPunch.git
git submodule update --init --recursive
```

## 3. Set the variables used by the rest of the commands

This runbook uses `us-east-2` as the default region to avoid the special `us-east-1` S3 bucket creation case.

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

This Dockerfile now uses the AWS SAM Python 3.11 build image as its base instead of the raw Lambda runtime image. That avoids the current Amazon Linux 2 `openssl-snapsafe-libs` conflict that breaks `yum` installs on some Lambda base image revisions.

```bash
cd /home/luca/fmi-original
docker build -t fmi-build-python311:latest -f fmi/runbooks/aws-python311-s3/Dockerfile.python3.11 .
```

## 6. Build and deploy the FMI Lambda layer

```bash
cd /home/luca/fmi-original/fmi/python/aws/python311
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
cd /home/luca/fmi-original/fmi/runbooks/aws-python311-s3/function
sed \
  -e "s#__FMI_BUCKET__#${FMI_BUCKET}#g" \
  -e "s#__AWS_REGION__#${AWS_REGION}#g" \
  fmi.json.template > fmi.json
```

## 8. Build and deploy the example Lambda function

```bash
cd /home/luca/fmi-original/fmi/runbooks/aws-python311-s3/function
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

## 11. Clean up when you are done

```bash
aws cloudformation delete-stack --stack-name "$FMI_APP_STACK" --region "$AWS_REGION"
aws cloudformation delete-stack --stack-name "$FMI_LAYER_STACK" --region "$AWS_REGION"
aws s3 rm "s3://${FMI_BUCKET}" --recursive
aws s3api delete-bucket --bucket "$FMI_BUCKET" --region "$AWS_REGION"
```
