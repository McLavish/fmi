# Fresh System Replication: FMI Migration On EKS + Knative

This runbook recreates the FMI migration demo on a fresh Linux machine:

- EKS cluster in `eu-central-1`
- Knative Serving with Kourier
- Redis in Kubernetes
- FMI worker image in ECR
- VM-style ranks as Kubernetes Jobs
- replacement serverless rank as a Knative Service
- in-cluster orchestrator Job that verifies migration and allreduce result `3.0`

The commands assume:

- AWS CLI is already authenticated.
- Docker is installed and can build linux/amd64 images.
- The FMI repo is at `/home/luca/fmi`.
- The EKS workspace is at `/home/luca/knativecluster`.
- AWS account is `323756936843`.
- AWS region is `eu-central-1`.

Kourier creates an AWS Load Balancer. That may cost money.

## 1. Install Local Tools

```bash
mkdir -p "$HOME/.local/bin"
export PATH="$HOME/.local/bin:$PATH"

curl -sSL "https://github.com/eksctl-io/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz" \
  -o /tmp/eksctl.tar.gz
tar -xzf /tmp/eksctl.tar.gz -C /tmp
install /tmp/eksctl "$HOME/.local/bin/eksctl"

KVER="$(curl -sSL https://dl.k8s.io/release/stable.txt)"
curl -sSL "https://dl.k8s.io/release/${KVER}/bin/linux/amd64/kubectl" -o /tmp/kubectl
install /tmp/kubectl "$HOME/.local/bin/kubectl"

eksctl version
kubectl version --client=true
aws sts get-caller-identity
docker --version
```

Install `kn` and the `kn-operator` plugin from the Knative GitHub releases, then verify:

```bash
KN_TAG="$(curl -sSL https://api.github.com/repos/knative/client/releases/latest | sed -n 's/.*"tag_name": "\(.*\)".*/\1/p')"
curl -sSL "https://github.com/knative/client/releases/download/${KN_TAG}/kn-linux-amd64" -o /tmp/kn
install /tmp/kn "$HOME/.local/bin/kn"

KN_OPERATOR_TAG="$(curl -sSL https://api.github.com/repos/knative-extensions/kn-plugin-operator/releases/latest | sed -n 's/.*"tag_name": "\(.*\)".*/\1/p')"
curl -sSL "https://github.com/knative-extensions/kn-plugin-operator/releases/download/${KN_OPERATOR_TAG}/kn-operator-linux-amd64" -o /tmp/kn-operator
install /tmp/kn-operator "$HOME/.local/bin/kn-operator"

kn version
kn operator --help
```

## 2. Create The EKS Cluster

```bash
mkdir -p /home/luca/knativecluster
cd /home/luca/knativecluster
```

Create `/home/luca/knativecluster/cluster.yaml`:

```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

metadata:
  name: knative
  region: eu-central-1

managedNodeGroups:
  - name: knodes-small
    instanceType: t3.small
    desiredCapacity: 2
    minSize: 1
    maxSize: 3
    volumeSize: 30
    privateNetworking: false
    labels:
      workload: knative
    tags:
      project: knativecluster
```

Create the cluster and set kubeconfig:

```bash
eksctl create cluster --kubeconfig ./eksknative.yaml -f ./cluster.yaml
export KUBECONFIG=/home/luca/knativecluster/eksknative.yaml

kubectl get nodes -o wide
```

## 3. Install Knative Serving

```bash
export KUBECONFIG=/home/luca/knativecluster/eksknative.yaml

kn operator install -n knative-operator
kn operator install --component serving -n knative-serving --kourier
kn operator enable ingress --kourier -n knative-serving
```

If the second command times out, inspect the cluster before retrying:

```bash
kubectl get knativeserving -A
kubectl -n knative-serving get pods
kubectl -n knative-serving get svc kourier
kubectl get events -A --sort-by=.lastTimestamp | tail -80
```

For external test domains:

```bash
kubectl -n knative-serving patch configmap config-domain \
  --type merge \
  -p '{"data":{"example.com":""}}'
```

Delete any sample service if you created one earlier:

```bash
kn service delete hello || true
```

## 4. Build The FMI Bundle

The Knative image needs the prebuilt FMI Python extension at:

```text
/home/luca/fmi/runbooks/localstack-python311-redis/build/bundle/fmi.so
```

Build it without starting LocalStack:

```bash
cd /home/luca/fmi/runbooks/localstack-python311-redis

docker build \
  -t fmi-localstack-build:redis-gcc10 \
  -f Dockerfile.build \
  /home/luca/fmi

docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  --mount type=bind,source=/home/luca/fmi,target=/opt/fmi \
  fmi-localstack-build:redis-gcc10 \
  /opt/fmi/runbooks/localstack-python311-redis/make-fmi-bundle.sh

find build/bundle -maxdepth 2 -type f -print
```

Expected files include:

```text
build/bundle/fmi.so
build/bundle/lib/libpython3.11.so.1.0
build/bundle/lib/libstdc++.so.6
build/bundle/lib/libgcc_s.so.1
```

## 5. Build And Push The Knative Image

Create the ECR repository if it does not exist:

```bash
aws ecr describe-repositories \
  --repository-names fmi-knative-migration \
  --region eu-central-1 \
  >/dev/null 2>&1 || \
aws ecr create-repository \
  --repository-name fmi-knative-migration \
  --region eu-central-1
```

Login and push:

```bash
aws ecr get-login-password --region eu-central-1 | \
  docker login --username AWS --password-stdin \
  323756936843.dkr.ecr.eu-central-1.amazonaws.com

cd /home/luca/fmi/runbooks/localstack-python311-redis

docker buildx build \
  --platform linux/amd64 \
  -f knative-migration/Dockerfile \
  -t 323756936843.dkr.ecr.eu-central-1.amazonaws.com/fmi-knative-migration:v1 \
  --push \
  .
```

The Dockerfile runs this smoke test during build:

```bash
python3.11 -c "import fmi; assert hasattr(fmi, 'FTCoordinator'); print('fmi import smoke test ok')"
```

If that fails, rebuild the FMI bundle for the container base before continuing.

## 6. Deploy Redis, RBAC, And Knative Service

The `knative-service.yaml` and `orchestrator-job.yaml` manifests are shared with
the private-cluster runbook and rendered with `envsubst`; on AWS the image is the
ECR tag and the pull policy is `Always`.

```bash
export KUBECONFIG=/home/luca/knativecluster/eksknative.yaml
export FMI_IMAGE=323756936843.dkr.ecr.eu-central-1.amazonaws.com/fmi-knative-migration:v1
export IMAGE_PULL_POLICY=Always
cd /home/luca/fmi/runbooks/localstack-python311-redis

kubectl apply -f knative-migration/k8s/namespace.yaml
kubectl apply -f knative-migration/k8s/redis.yaml
kubectl -n fmi rollout status deploy/fmi-redis --timeout=180s

kubectl apply -f knative-migration/k8s/rbac.yaml
envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY}' < knative-migration/k8s/knative-service.yaml | kubectl apply -f -
kubectl -n fmi wait ksvc/fmi-serverless-rank --for=condition=Ready --timeout=300s
```

Confirm:

```bash
kubectl -n fmi get pods,ksvc
```

Expected:

- `fmi-redis` pod is `Running`
- `fmi-serverless-rank` is `Ready=True`
- one Knative revision pod is warm because `min-scale: "1"`

## 7. Run The Migration Test

Start a fresh orchestrator Job:

```bash
export KUBECONFIG=/home/luca/knativecluster/eksknative.yaml
export FMI_IMAGE=323756936843.dkr.ecr.eu-central-1.amazonaws.com/fmi-knative-migration:v1
export IMAGE_PULL_POLICY=Always
cd /home/luca/fmi/runbooks/localstack-python311-redis

kubectl -n fmi delete job fmi-orchestrator --ignore-not-found=true
envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY}' < knative-migration/k8s/orchestrator-job.yaml | kubectl apply -f -
kubectl -n fmi logs -f job/fmi-orchestrator
```

Expected pass output includes:

```text
--- Epoch 0 directory ---
rank 0: worker_id=rank0-vm-... placement=vm state=QUIESCED
rank 1: worker_id=rank1-vm-... placement=vm state=ACTIVE

--- Epoch 1 directory ---
rank 0: worker_id=rank0-serverless-... placement=serverless state=ACTIVE
rank 1: worker_id=rank1-vm-... placement=vm state=ACTIVE

[orchestrator] HTTP /invoke response {'peer_id': 0, 'placement': 'serverless', 'status': 'ok', 'result': 3.0}
[orchestrator] post-migration allreduce result = 3.0 (expected 3.0)

PASSED: rank directory flip verified AND post-migration allreduce result == 3.0
```

Verify Kubernetes status:

```bash
kubectl -n fmi get jobs,pods,ksvc
kubectl -n fmi get jobs -l fmi-role=vm-rank
```

Expected:

- `job.batch/fmi-orchestrator` is `Complete`
- no `fmi-role=vm-rank` Jobs remain
- Redis and the Knative serverless rank pod are still running

## 8. Useful Debugging Commands

Watch workload state:

```bash
kubectl -n fmi get jobs,pods -w
```

Show orchestrator logs:

```bash
kubectl -n fmi logs job/fmi-orchestrator
```

Show serverless HTTP adapter logs:

```bash
kubectl -n fmi logs -l serving.knative.dev/service=fmi-serverless-rank --tail=100
```

Show Redis keys:

```bash
kubectl -n fmi exec deploy/fmi-redis -- redis-cli KEYS '*'
```

Show recent namespace events:

```bash
kubectl -n fmi get events --sort-by=.lastTimestamp
```

Check pod capacity on the small `t3.small` nodes:

```bash
kubectl describe nodes | grep -E 'Name:|MemoryPressure|DiskPressure|PIDPressure|Ready|pods:' -A4
```

If pods stay Pending because of pod capacity, either delete unused workloads or scale the nodegroup to 3:

```bash
eksctl scale nodegroup \
  --cluster knative \
  --region eu-central-1 \
  --name knodes-small \
  --nodes 3
```

## 9. Rerun After Code Changes

After changing Python code or manifests:

```bash
export FMI_IMAGE=323756936843.dkr.ecr.eu-central-1.amazonaws.com/fmi-knative-migration:v1
export IMAGE_PULL_POLICY=Always
cd /home/luca/fmi/runbooks/localstack-python311-redis

python3 -m unittest discover -s knative-migration/tests -p 'test_*.py'
python3 -m py_compile \
  knative-migration/orchestrator.py \
  knative-migration/knative_http.py \
  lambda_function.py \
  worker_core.py \
  vm_worker.py

docker buildx build \
  --platform linux/amd64 \
  -f knative-migration/Dockerfile \
  -t "$FMI_IMAGE" \
  --push \
  .

envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY}' < knative-migration/k8s/knative-service.yaml | kubectl apply -f -
kubectl -n fmi wait ksvc/fmi-serverless-rank --for=condition=Ready --timeout=300s

kubectl -n fmi delete job fmi-orchestrator --ignore-not-found=true
envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY}' < knative-migration/k8s/orchestrator-job.yaml | kubectl apply -f -
kubectl -n fmi logs -f job/fmi-orchestrator
```

## 10. Cleanup

Delete only the FMI demo resources:

```bash
export KUBECONFIG=/home/luca/knativecluster/eksknative.yaml

kubectl delete namespace fmi
aws ecr delete-repository \
  --repository-name fmi-knative-migration \
  --region eu-central-1 \
  --force
```

Delete the whole EKS cluster:

```bash
eksctl delete cluster --region eu-central-1 --name knative --wait
```

If deletion hangs while waiting for `eksctl-knative-nodegroup-knodes-small`,
the backing EKS managed-nodegroup ASG may be stuck in the termination lifecycle
hook. Check and release stuck instances:

```bash
ASG="$(aws eks describe-nodegroup \
  --region eu-central-1 \
  --cluster-name knative \
  --nodegroup-name knodes-small \
  --query 'nodegroup.resources.autoScalingGroups[0].name' \
  --output text)"

aws autoscaling describe-auto-scaling-groups \
  --region eu-central-1 \
  --auto-scaling-group-names "$ASG" \
  --query 'AutoScalingGroups[0].Instances[].{Id:InstanceId,State:LifecycleState}' \
  --output table

for id in $(aws autoscaling describe-auto-scaling-groups \
  --region eu-central-1 \
  --auto-scaling-group-names "$ASG" \
  --query 'AutoScalingGroups[0].Instances[?LifecycleState==`Terminating:Wait`].InstanceId' \
  --output text); do
  aws autoscaling complete-lifecycle-action \
    --region eu-central-1 \
    --auto-scaling-group-name "$ASG" \
    --lifecycle-hook-name Terminate-LC-Hook \
    --lifecycle-action-result CONTINUE \
    --instance-id "$id"
done
```

Then rerun:

```bash
eksctl delete cluster --region eu-central-1 --name knative --wait
```

Check for leftover AWS resources:

```bash
aws eks list-clusters --region eu-central-1

aws cloudformation describe-stacks \
  --region eu-central-1 \
  --query 'Stacks[?contains(StackName, `knative`)].{Name:StackName,Status:StackStatus}' \
  --output table

aws elb describe-load-balancers \
  --region eu-central-1 \
  --query 'LoadBalancerDescriptions[].{Name:LoadBalancerName,DNS:DNSName}' \
  --output table
```
