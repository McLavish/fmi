# Private-Cluster Replication: FMI Migration On An On-Prem Kubernetes Cluster

This is the private-cluster counterpart of `setup.md`. It runs the **same** FMI
migration demo — VM ranks as Kubernetes Jobs, a replacement serverless rank as a
Knative Service, Redis control + data plane, an in-cluster orchestrator — on an
existing multi-node cluster with **no AWS account and no external registry**.

The FMI workload has no AWS runtime dependency (Redis-only backend, in-cluster),
so the only real differences from the EKS runbook are image delivery and a few
node-level registry settings. Image distribution uses a lightweight **in-cluster
`registry:2` (ephemeral)**: build once on the control plane, push once, and all
nodes pull over the cluster network.

Assumptions:

- You are SSH'd into the **control plane** node.
- `kubectl`, `kn`, and `podman` are installed and `kubectl` targets this cluster
  (on RHEL: `sudo dnf install -y podman`).
- The cluster is multi-node, **x86_64/amd64**.
- Nodes can pull public images (`registry:2`, the Knative images). Only the
  control plane needs to pull the build base image.
- The FMI repo is checked out at `$REPO` (set in §1; examples assume `$HOME/fmi`).

No external LoadBalancer is needed: the orchestrator reaches the serverless rank
over the in-cluster address `http://fmi-serverless-rank.fmi.svc.cluster.local/invoke`,
which Knative routes internally through `kourier-internal`.

## 1. Set Variables And Sanity-Check The Cluster

`REGISTRY` must be an address reachable from the control plane **and** every
node — the control-plane node IP plus the registry NodePort works well.

The shared `k8s/` manifests are rendered with `envsubst`; `FMI_IMAGE` points at
the in-cluster registry and the pull policy is `IfNotPresent`.

```bash
export REGISTRY="$(hostname -I | awk '{print $1}'):30500"
export TAG=v1
export FMI_IMAGE="${REGISTRY}/fmi-knative-migration:${TAG}"
export IMAGE_PULL_POLICY=IfNotPresent
export REPO="$HOME/fmi"          # adjust to your checkout location
echo "FMI_IMAGE=$FMI_IMAGE  REPO=$REPO"

cd "$REPO/runbooks/localstack-python311-redis"

kubectl get nodes -o wide
```

## 3. Deploy The In-Cluster Registry

```bash
kubectl apply -f knative-migration/k8s/namespace.yaml
kubectl apply -f knative-migration/k8s/registry.yaml
kubectl -n fmi rollout status deploy/fmi-registry --timeout=180s
```

The registry is ephemeral: if its pod restarts, re-push the image (§6) before
deploying.

## 5. Tell Knative To Skip Tag Resolution For This Registry

Knative resolves image tags to digests by contacting the registry from its
controller. For an insecure in-cluster registry, skip that resolution so the
node-side pull is the only thing that touches the registry:

```bash
kubectl -n knative-serving patch configmap config-deployment \
  --type merge \
  -p "{\"data\":{\"registries-skipping-tag-resolving\":\"${REGISTRY}\"}}"
```

If your Knative version uses a different key, check
`kubectl -n knative-serving get cm config-deployment -o yaml`.

## 6. Build The FMI Bundle And Image, Then Push

The bundle build mirrors `setup.md` §4 (amd64), using `podman`. Run podman as
**root** (`sudo`): on HPC/LDAP hosts your user usually lacks the `/etc/subuid`
ranges rootless podman needs, and rootful matches the original Docker behavior.
Use `sudo` consistently so `build` and `run` share one image store.

```bash
cd "$REPO/runbooks/localstack-python311-redis"

# The Direct backend builds from the TCPunch submodule; initialize it on the host
# (the bind mount below exposes it to the build container). TCPunch's URL is SSH;
# with no GitHub SSH key, rewrite GitHub SSH -> HTTPS first (reversible):
#   git config --global url."https://github.com/".insteadOf "git@github.com:"
git -C "$REPO" submodule update --init --recursive

sudo podman build -t fmi-localstack-build:redis-gcc10 -f Dockerfile.build "$REPO"

# --user expands in your shell before sudo, so the bundle stays owned by you
sudo podman run --rm \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  --mount type=bind,source="$REPO",target=/opt/fmi \
  fmi-localstack-build:redis-gcc10 \
  /opt/fmi/runbooks/localstack-python311-redis/make-fmi-bundle.sh

sudo podman build -f knative-migration/Dockerfile -t "$FMI_IMAGE" .
sudo podman push --tls-verify=false "$FMI_IMAGE"   # --tls-verify=false: plain-HTTP registry
```

The Dockerfile runs the import smoke test during build:

```text
python3.11 -c "import fmi; assert hasattr(fmi, 'FTControlPlane'); print('fmi import smoke test ok')"
```

## 7. Deploy Redis, RBAC, And The Knative Service

The shared manifests are rendered with `envsubst` (`FMI_IMAGE`/`IMAGE_PULL_POLICY`
from §1).

```bash
cd "$REPO/runbooks/localstack-python311-redis"

kubectl apply -f knative-migration/k8s/redis.yaml
kubectl -n fmi rollout status deploy/fmi-redis --timeout=180s

kubectl apply -f knative-migration/k8s/rbac.yaml
envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY}' < knative-migration/k8s/knative-service.yaml | kubectl apply -f -
kubectl -n fmi wait ksvc/fmi-serverless-rank --for=condition=Ready --timeout=300s

kubectl -n fmi get pods,ksvc
```

Expected:

- `fmi-redis` and `fmi-registry` pods are `Running`
- `fmi-serverless-rank` is `Ready=True`
- one warm Knative revision pod (`min-scale: "1"`)

## 8. Run The Migration Test

```bash
cd "$REPO/runbooks/localstack-python311-redis"

kubectl -n fmi delete job fmi-orchestrator --ignore-not-found=true
envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY}' < knative-migration/k8s/orchestrator-job.yaml | kubectl apply -f -
kubectl -n fmi logs -f job/fmi-orchestrator
```

Expected pass output is identical to the EKS runbook:

```text
--- Epoch 0 directory ---
rank 0: worker_id=rank0-vm-... placement=vm state=QUIESCED
rank 1: worker_id=rank1-vm-... placement=vm state=ACTIVE

--- Epoch 1 directory ---
rank 0: worker_id=rank0-serverless-... placement=serverless state=ACTIVE
rank 1: worker_id=rank1-vm-... placement=vm state=ACTIVE

[orchestrator] post-migration allreduce result = 3.0 (expected 3.0)

PASSED: rank directory flip verified AND post-migration allreduce result == 3.0
```

## 9. Useful Debugging Commands

```bash
kubectl -n fmi get jobs,pods -w
kubectl -n fmi logs job/fmi-orchestrator
kubectl -n fmi logs -l serving.knative.dev/service=fmi-serverless-rank --tail=100
kubectl -n fmi exec deploy/fmi-redis -- redis-cli KEYS '*'
kubectl -n fmi get events --sort-by=.lastTimestamp

# Confirm a node can pull from the registry:
curl -s "http://${REGISTRY}/v2/_catalog"
```

If a pod is stuck `ImagePullBackOff`, the node's containerd does not trust the
registry yet — recheck §4 on that node (`kubectl get pod <p> -o wide` shows which
node).

## 10. Rerun After Code Changes

Validate locally, then push a **new tag** so Knative makes a fresh revision and
nodes re-pull (this is cleaner than reusing a tag with `IfNotPresent`):

```bash
cd "$REPO/runbooks/localstack-python311-redis"

python3 -m unittest discover -s knative-migration/tests -p 'test_*.py'
python3 -m py_compile \
  knative-migration/orchestrator.py \
  knative-migration/knative_http.py \
  lambda_function.py worker_core.py vm_worker.py

export TAG=v2   # bump on every rebuild
export FMI_IMAGE="${REGISTRY}/fmi-knative-migration:${TAG}"
sudo podman build -f knative-migration/Dockerfile -t "$FMI_IMAGE" .
sudo podman push --tls-verify=false "$FMI_IMAGE"

envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY}' < knative-migration/k8s/knative-service.yaml | kubectl apply -f -
kubectl -n fmi wait ksvc/fmi-serverless-rank --for=condition=Ready --timeout=300s

kubectl -n fmi delete job fmi-orchestrator --ignore-not-found=true
envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY}' < knative-migration/k8s/orchestrator-job.yaml | kubectl apply -f -
kubectl -n fmi logs -f job/fmi-orchestrator
```

## 11. Cleanup

Delete only the FMI demo resources (keeps the cluster and Knative):

```bash
kubectl delete namespace fmi
```

The Knative install and the per-node containerd registry trust from §4 remain;
remove them manually only if you no longer need the registry.
