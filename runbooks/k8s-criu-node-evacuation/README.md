# CRIU node-evacuation demo (multi-machine, Kubernetes)

Migrate **all the ranks co-located on one machine, in a single consistent cut, with their
in-memory state preserved** — the multi-machine application of FMI's `migrate-local` + CRIU
state transfer.

The scenario: an 8-rank job runs **4 ranks on each of two machines** (nodes). An orchestrator
**evacuates one machine** — `fmi-rank-agent migrate-local` discovers that machine's 4 ranks,
quiesces them together, `criu` checkpoint/restores them in parallel, and promotes the epoch once.
The 4 ranks on the other machine wait at the barrier and rejoin at the new epoch. The whole job
continues and every rank proves its memory survived (a post-migration allreduce that can only be
correct if each rank's pre-migration state was preserved).

## Important: what "evacuation" means here (and what it does not)

CRIU state transfer in FMI is **same-host only**: `fmi-rank-agent` restores each checkpointed
process **in place, on the same node, at the same PID**. There is no image shipping / remote
restore, so this demo does **not** relocate ranks onto a different node or into a serverless
runtime — "migrate 4 ranks to serverless" and "CRIU state transfer" are different mechanisms that
do not compose today. What this demo faithfully shows is a **stateful node evacuation / consistent
checkpoint**: a node under pressure (a drain, a maintenance blip) has all its co-located ranks
checkpointed and restored together with zero application checkpoint code, while the rest of the
job continues. (For the cross-substrate, *stateless* VM→serverless migration, see the sibling
`../localstack-python311-redis/knative-migration/` demo.)

Two facts that shape the deployment:

- **The Direct data plane is mandatory under CRIU.** `TransparentMigrationRuntime` pins
  `preferred_data_backend=Direct` (CRIU must release sockets before the dump), so the ranks talk
  over TCP via a `tcpunchd` rendezvous — deployed here as the `fmi-tcpunch` Service — and need
  pod-to-pod TCP across nodes.
- **criu must run privileged in the pod.** The machine pods run `privileged: true` so the rank
  agent can ptrace-seize, freeze, and recreate the rank process trees. The agent runs in the
  *same container* as its ranks (via `kubectl exec`), sharing their PID + mount namespace.

## Components

| File | Role |
|------|------|
| `Dockerfile` | One image for every role: builds FMI (`fmi-rank-agent`, the `transparent_state_transfer_demo` worker), a robust `tcpunchd`, and `criu` 4.2 from source. |
| `supervisor.py` | Per-machine pod entrypoint: renders the config, launches this machine's rank slice to restorable log files, stays alive. |
| `orchestrator.py` | Kubernetes Job: waits for ACTIVE, `exec`s `migrate-local` into machine A, verifies epoch 1 + preserved state. |
| `fmi-machine.json.tmpl` | FT config template (Direct data plane, per-machine `criu.host_id`). |
| `k8s/*.yaml` | namespace, redis, tcpunch (headless), templated machine Deployment, RBAC, orchestrator Job. |
| `patches/0001-tcpunchd-robustness.patch` | tcpunchd ignores SIGPIPE + skips dead probes (no crash under N-rank churn); TCPunch client resolves **hostnames** (so `Direct.host` can be a Service name). Applied at image build. |
| `local-two-machines.sh` | Cluster-free baseline: 8 ranks, two simulated machines on one host (loopback). |
| `local-two-containers.sh` | Cluster-free, higher fidelity: two machines as two containers on a bridge network (distinct IPs, DNS, `docker exec` ≈ `kubectl exec`). |
| `tests/test_orchestrator.py` | Unit tests for the orchestrator's verification logic. |

## Prerequisites

- A working `criu` and `redis` + `tcpunchd` for the local paths (the repo's
  `build-unified-criu` build, a Redis on `127.0.0.1:6379`, and
  `extern/TCPunch/server/build*/tcpunchd 10000`). See `../local-criu-state-transfer/README.md`.
- For the cluster path: a multi-node cluster whose nodes **allow privileged pods** and can run
  `criu` (recent kernel), plus pod-to-pod TCP between nodes, and a registry your nodes can pull
  from. `docker` and `kubectl` on your workstation.

## 1. Verify locally first (no cluster)

These are the fast gates; run them before touching the cluster.

```bash
# (a) single host, two simulated machines (rootless criu)
FMI_CRIU_EXTRA_ARGS="--unprivileged" bash runbooks/k8s-criu-node-evacuation/local-two-machines.sh

# (b) orchestrator logic unit tests
python3 runbooks/k8s-criu-node-evacuation/tests/test_orchestrator.py

# (c) build the image, then run the two machines as two containers (distinct IPs, DNS,
#     privileged criu, docker exec ≈ kubectl exec — the closest cluster-free proxy)
docker build -f runbooks/k8s-criu-node-evacuation/Dockerfile -t fmi-criu-evac:dev .
bash runbooks/k8s-criu-node-evacuation/local-two-containers.sh
```

Each ends with `PASS: ... all 8 ranks preserved state at epoch 1`.

## 2. Run on the cluster

Set the shared variables once. `FMI_IMAGE` must be reachable from your nodes (push the image
built above to your registry, e.g. an in-cluster registry or your cloud registry).

```bash
export FMI_IMAGE=<your-registry>/fmi-criu-evac:v1
export IMAGE_PULL_POLICY=Always
export COMM_NAME=criu-evac-1
export NUM_PEERS=8
export WINDOW_MS=20000
```

### 2a. Label the two nodes that will host the machines

```bash
kubectl get nodes
kubectl label node <NODE-A> fmi-machine=a --overwrite
kubectl label node <NODE-B> fmi-machine=b --overwrite
```

### 2b. Base infra

```bash
cd runbooks/k8s-criu-node-evacuation
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/redis.yaml
kubectl -n fmi-criu rollout status deploy/fmi-redis --timeout=120s
envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY}' < k8s/tcpunch.yaml | kubectl apply -f -
kubectl -n fmi-criu rollout status deploy/fmi-tcpunch --timeout=120s
kubectl apply -f k8s/rbac.yaml
```

### 2c. The two machines (4 ranks each, node-pinned, privileged)

```bash
MACHINE_ID=a NODE_VALUE=a BASE_PEER_ID=0 RANKS_HERE=4 \
  envsubst < k8s/machine-deployment.yaml | kubectl apply -f -
MACHINE_ID=b NODE_VALUE=b BASE_PEER_ID=4 RANKS_HERE=4 \
  envsubst < k8s/machine-deployment.yaml | kubectl apply -f -
kubectl -n fmi-criu rollout status deploy/fmi-machine-a --timeout=180s
kubectl -n fmi-criu rollout status deploy/fmi-machine-b --timeout=180s
```

### 2d. Preflights (before the orchestrator)

```bash
# criu can actually checkpoint inside a machine pod
kubectl -n fmi-criu exec deploy/fmi-machine-a -- criu check --all

# the rendezvous is reachable and pod-to-pod TCP works across the two nodes
kubectl -n fmi-criu exec deploy/fmi-machine-a -- sh -c \
  'getent hosts fmi-tcpunch && nc -z -w3 fmi-tcpunch 10000 && echo tcpunch-ok'
B_IP=$(kubectl -n fmi-criu get pod -l fmi-machine=b -o jsonpath='{.items[0].status.podIP}')
kubectl -n fmi-criu exec deploy/fmi-machine-a -- ping -c1 -W2 "$B_IP"

# all 8 ranks reached ACTIVE at epoch 0
kubectl -n fmi-criu exec deploy/fmi-redis -- sh -c \
  "for r in 0 1 2 3 4 5 6 7; do redis-cli hget fmi:ft:${COMM_NAME}:epoch:0:states \$r; done"
```

If `criu check --all` fails, your nodes don't permit checkpointing (kernel/policy). As a fallback
you can run rootless criu by adding `- name: FMI_CRIU_EXTRA_ARGS` / `value: "--unprivileged"` to
the machine Deployment env (then `securityContext` still needs ptrace, but not full privilege).
If the cross-node `ping`/`nc` fails, a NetworkPolicy or CNI is blocking pod-to-pod traffic — the
Direct data plane needs it.

### 2e. Evacuate machine A and verify

```bash
envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY}' < k8s/orchestrator-job.yaml | kubectl apply -f -
kubectl -n fmi-criu logs -f job/fmi-orchestrator
```

Expected:

```
[orchestrator] --- epoch 0 directory ---
[orchestrator]   rank 0..7: state=ACTIVE placement=machine-a|machine-b
[orchestrator] evacuating machine-A pod 'fmi-machine-a-...' via fmi-rank-agent migrate-local
[orchestrator]   [agent] promoted_epoch=1
[orchestrator] --- epoch 1 directory ---
[orchestrator]   rank 0..7: state=ACTIVE ...
[orchestrator] PASSED: machine A evacuated in one criu cut; all 8 ranks preserved state at epoch 1
```

`migrate-local` only touches machine A's ranks (host_id `machine-a`); machine B's ranks are
survivors that park at the barrier and rejoin at epoch 1. The PASS proves every rank's memory
survived the checkpoint/restore (post-migration allreduce == `num_peers*(num_peers+1)/2 +
100*num_peers` = 836 for 8 ranks).

## Cleanup

```bash
kubectl delete namespace fmi-criu
kubectl label node <NODE-A> fmi-machine- ; kubectl label node <NODE-B> fmi-machine-
```

## Verification status

Verified on this host: `local-two-machines.sh` (rootless, 3/3), the in-container privileged-criu
gate, and `local-two-containers.sh` (distinct IPs + DNS + exec-driven evacuation, 3/3), plus the
orchestrator unit tests. The cluster steps (§2) are the deployment procedure for a real multi-node
cluster; the two highest-risk pieces they add over the local runs are **privileged criu on your
nodes' kernels** and **cross-node pod-to-pod Direct/TCPunch** — both covered by the §2d preflights.

## Scaling

The layout generalizes to N machines × `RANKS_HERE` ranks: apply `machine-deployment.yaml` per
machine with the right `MACHINE_ID`/`NODE_VALUE`/`BASE_PEER_ID`, set `NUM_PEERS` to the total, and
keep `EVACUATE_SELECTOR` pointed at the machine you want to evacuate.
