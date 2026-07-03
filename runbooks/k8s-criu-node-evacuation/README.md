# CRIU node evacuation → serverless (multi-machine, Kubernetes + Knative)

Evacuate **all the ranks co-located on one machine, in a single consistent cut, onto serverless
instances — with their in-memory state preserved**. This composes FMI's two migration mechanisms
that were previously separate demos: CRIU state transfer (`state_transfer="criu"`) and
cross-substrate relocation to Knative (the stateless
`../localstack-python311-redis/knative-migration/` demo).

The scenario: an 8-rank job runs **4 ranks on each of two machines** (nodes). An orchestrator
evacuates machine A:

1. `fmi-rank-agent evacuate-local` (exec'd in the machine-A pod) discovers that machine's 4
   ranks, quiesces them together, `criu dump`s them in parallel, and **stages each rank's packed
   image in Redis** — no restore, no epoch promotion;
2. machine A's Deployment is scaled to **zero** — the node is gone;
3. the orchestrator POSTs one `/restore` per rank to the **`fmi-restore` Knative Service**
   (scale-from-zero, `containerConcurrency: 1`): four cold-started pods each fetch a staged
   image, `criu restore` their rank, and hold the request while the restored worker runs;
4. once the CRIU registry shows every rank RUNNING on a `fmi-restore-*` host, the orchestrator
   promotes the epoch **once**; survivors on machine B (parked at the barrier) and the restored
   ranks rebuild channels at epoch 1;
5. the job finishes and every rank proves its memory survived (a post-migration allreduce that
   is only correct if each rank's pre-migration state was preserved — 836 for 8 ranks).

## How a rank crosses hosts (the mechanics that make this work)

- **Staged images travel through Redis.** `evacuate-local` tars each rank's criu image dir plus
  the files criu re-opens on restore (its log file, templated via
  `FMI_CRIU_EXTRA_FILES=/tmp/rank-{rank}.log`) and stores the archive in the control plane;
  `restore-remote` unpacks it at `/` so every dumped path is back in place.
- **One image for every role — mandatory, not convenience.** criu restore re-opens the worker's
  executable and shared libraries by path; dump and restore side must be byte-identical.
- **Restored pids can't collide.** The supervisor starts ranks in a high pid band
  (`ns_last_pid`, `PID_BASE=3000`); a fresh restore pod only holds single-digit pids.
- **Cgroups are ignored** (`--manage-cgroups=ignore` on dump and restore): the dumped kubepods
  cgroup paths don't exist in the restore pod; the restored task stays in the restorer's cgroup.
- **The Direct data plane is mandatory under CRIU.** `TransparentMigrationRuntime` pins
  `preferred_data_backend=Direct` (sockets are released before the dump), so ranks re-pair over
  TCP via the `fmi-tcpunch` rendezvous from wherever they run — this is exactly what makes the
  restore host-agnostic. Redis control connections are `--tcp-close`d and reconnect lazily.
- **The restore pod's identity is the proof of relocation.** Restore agents run with an empty
  `criu.host_id`, so `resolve_host_id` falls back to the pod hostname; the orchestrator verifies
  each evacuated rank is RUNNING on a host that is *not* `machine-a` before promoting.

## Components

| File | Role |
|------|------|
| `Dockerfile` | One image for every role: FMI (`fmi-rank-agent`, the `transparent_state_transfer_demo` worker), a robust `tcpunchd`, `criu` 4.2 from source, and the python entrypoints. |
| `supervisor.py` | Per-machine pod entrypoint: renders the config, sets the pid band, launches this machine's rank slice to restorable log files, stays alive. |
| `restore_server.py` | Knative container entrypoint: `POST /restore {"rank": R}` runs `fmi-rank-agent restore-remote` and holds the request while the restored worker runs, answering with its outcome. |
| `orchestrator.py` | Kubernetes Job: evacuate-local → scale machine A to 0 → 4 parallel `/restore` → verify relocation in the CRIU registry → promote → verify all 8 ranks preserved state. |
| `fmi-machine.json.tmpl` | FT config template (Direct data plane, per-role `criu.host_id`). |
| `k8s/*.yaml` | namespace, redis, tcpunch (headless), templated machine Deployment, RBAC, the `fmi-restore` Knative Service, orchestrator Job. |
| `local-two-machines.sh` | Cluster-free baseline: the full evacuate→stage→wipe→restore-remote→promote pipeline on one host (fake serverless host identities). |
| `local-two-containers.sh` | Cluster-free dress rehearsal: machine A is a container that gets **stopped** after staging; four fresh containers restore its ranks via `restore_server.py` over HTTP. |
| `tests/test_orchestrator.py` | Unit tests for the orchestrator's and restore server's parsing/verification logic. |

## Prerequisites

- For the local paths: a working `criu`, a Redis on `127.0.0.1:6379`, and
  `extern/TCPunch/server/build*/tcpunchd 10000` (see `../local-criu-state-transfer/README.md`),
  plus the repo's `build-unified-criu` build. `docker` for (c).
- For the cluster path: a multi-node cluster with **Knative Serving**, nodes that allow
  privileged pods (the machine pods) and can run `criu` (recent kernel), pod-to-pod TCP between
  nodes, a registry your nodes can pull from, and **cluster-admin on the `knative-serving`
  namespace** (one ConfigMap patch, below). `docker` and `kubectl` on your workstation.

## 1. Verify locally first (no cluster)

These are the fast gates; run them before touching the cluster.

```bash
# (a) single host: dump → stage in Redis → wipe local state → restore under fake serverless
#     identities → promote (rootless criu)
FMI_CRIU_EXTRA_ARGS="--unprivileged" bash runbooks/k8s-criu-node-evacuation/local-two-machines.sh

# (b) orchestrator + restore-server logic unit tests
python3 runbooks/k8s-criu-node-evacuation/tests/test_orchestrator.py

# (c) build the image, then the dress rehearsal: machine-a is STOPPED after staging and its
#     ranks restored over HTTP into four fresh containers (distinct hostnames/IPs, fresh pid ns)
docker build -f runbooks/k8s-criu-node-evacuation/Dockerfile -t fmi-criu-evac:dev .
bash runbooks/k8s-criu-node-evacuation/local-two-containers.sh
```

(a) and (c) end with `PASS: ... all 8 ranks preserved state at epoch 1`.

## 2. Run on the cluster

Set the shared variables once. `FMI_IMAGE` must be reachable from your nodes (push the image
built above to your registry).

```bash
export FMI_IMAGE=<your-registry>/fmi-criu-evac:v1
export IMAGE_PULL_POLICY=Always
export COMM_NAME=criu-evac-1
export NUM_PEERS=8
export WINDOW_MS=20000
export RESTORE_MAX_SCALE=4   # = ranks per machine
```

### 2a. Enable the Knative feature gates (once, cluster-admin)

criu restore needs root + ptrace/admin capabilities inside the restore pods; Knative blocks
securityContext fields, added capabilities, and emptyDir volumes unless these gates are on:

```bash
kubectl patch configmap/config-features -n knative-serving --type merge -p \
  '{"data":{"kubernetes.podspec-securitycontext":"enabled",
            "kubernetes.containerspec-addcapabilities":"enabled",
            "kubernetes.podspec-volumes-emptydir":"enabled"}}'
```

### 2b. Label the two nodes that will host the machines

```bash
kubectl get nodes
kubectl label node <NODE-A> fmi-machine=a --overwrite
kubectl label node <NODE-B> fmi-machine=b --overwrite
```

### 2c. Base infra

```bash
cd runbooks/k8s-criu-node-evacuation
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/redis.yaml
kubectl -n fmi-criu rollout status deploy/fmi-redis --timeout=120s
envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY}' < k8s/tcpunch.yaml | kubectl apply -f -
kubectl -n fmi-criu rollout status deploy/fmi-tcpunch --timeout=120s
kubectl apply -f k8s/rbac.yaml
```

### 2d. The restore Knative Service and the two machines

```bash
envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY} ${COMM_NAME} ${NUM_PEERS} ${RESTORE_MAX_SCALE}' \
  < k8s/knative-restore-service.yaml | kubectl apply -f -
kubectl -n fmi-criu wait ksvc/fmi-restore --for=condition=Ready --timeout=180s

MACHINE_ID=a NODE_VALUE=a BASE_PEER_ID=0 RANKS_HERE=4 \
  envsubst < k8s/machine-deployment.yaml | kubectl apply -f -
MACHINE_ID=b NODE_VALUE=b BASE_PEER_ID=4 RANKS_HERE=4 \
  envsubst < k8s/machine-deployment.yaml | kubectl apply -f -
kubectl -n fmi-criu rollout status deploy/fmi-machine-a --timeout=180s
kubectl -n fmi-criu rollout status deploy/fmi-machine-b --timeout=180s
```

### 2e. Preflights (before the orchestrator)

```bash
# (1) criu can checkpoint inside a machine pod
kubectl -n fmi-criu exec deploy/fmi-machine-a -- criu check --all

# (2) criu can RESTORE inside a Knative pod — the highest-risk piece of the whole demo.
#     Temporarily scale the service to one warm instance and run criu check in it:
kubectl -n fmi-criu patch ksvc fmi-restore --type merge -p \
  '{"spec":{"template":{"metadata":{"annotations":{"autoscaling.knative.dev/min-scale":"1"}}}}}'
kubectl -n fmi-criu wait ksvc/fmi-restore --for=condition=Ready --timeout=120s
POD=$(kubectl -n fmi-criu get pod -l serving.knative.dev/service=fmi-restore \
      -o jsonpath='{.items[0].metadata.name}')
kubectl -n fmi-criu exec "$POD" -c user-container -- criu check --all
kubectl -n fmi-criu patch ksvc fmi-restore --type merge -p \
  '{"spec":{"template":{"metadata":{"annotations":{"autoscaling.knative.dev/min-scale":"0"}}}}}'

# (3) the rendezvous is reachable and pod-to-pod TCP works across the two nodes
kubectl -n fmi-criu exec deploy/fmi-machine-a -- sh -c \
  'getent hosts fmi-tcpunch && nc -z -w3 fmi-tcpunch 10000 && echo tcpunch-ok'
B_IP=$(kubectl -n fmi-criu get pod -l fmi-machine=b -o jsonpath='{.items[0].status.podIP}')
kubectl -n fmi-criu exec deploy/fmi-machine-a -- ping -c1 -W2 "$B_IP"

# (4) all 8 ranks reached ACTIVE at epoch 0
kubectl -n fmi-criu exec deploy/fmi-redis -- sh -c \
  "for r in 0 1 2 3 4 5 6 7; do redis-cli hget fmi:ft:${COMM_NAME}:epoch:0:states \$r; done"
```

If preflight (2) fails: the ksvc pod could not get its capabilities — check that the 2a patch
took effect (`kubectl get cm config-features -n knative-serving -o yaml`), that no PodSecurity
admission policy on the namespace blocks it, and that your container runtime accepts the
`CHECKPOINT_RESTORE` capability (drop it from `k8s/knative-restore-service.yaml` on kernels/
runtimes that predate it — `SYS_ADMIN` covers it there). If the cross-node `ping`/`nc` fails, a
NetworkPolicy or CNI is blocking pod-to-pod traffic — the Direct data plane needs it.

### 2f. Evacuate machine A to serverless and verify

```bash
envsubst '${FMI_IMAGE} ${IMAGE_PULL_POLICY} ${COMM_NAME} ${NUM_PEERS}' \
  < k8s/orchestrator-job.yaml | kubectl apply -f -
kubectl -n fmi-criu logs -f job/fmi-orchestrator
```

Expected:

```
[orchestrator] --- epoch 0 directory ---
[orchestrator]   rank 0..7: state=ACTIVE placement=machine-a|machine-b
[orchestrator] staging machine-A ranks: fmi-rank-agent evacuate-local in pod 'fmi-machine-a-...'
[orchestrator]   [agent] staged_epoch=1 ranks=0,1,2,3
[orchestrator] scaling deploy/fmi-machine-a to 0 (machine A is evacuated)
[orchestrator] POSTing 4 parallel /restore requests to http://fmi-restore...
[orchestrator]   rank 0: machine-a -> fmi-restore-00001-deployment-...
[orchestrator]   [agent] promoted_epoch=1
[orchestrator] --- epoch 1 directory ---
[orchestrator]   rank 0..7: state=ACTIVE ...
[orchestrator]   rank 0 on fmi-restore-...: status=ok (state preserved)
[orchestrator] PASSED: machine A evacuated to serverless in one criu cut — 4 ranks relocated ...
```

You can watch the four Knative pods cold-start during step 3:
`kubectl -n fmi-criu get pods -l serving.knative.dev/service=fmi-restore -w`.

## Cleanup

```bash
kubectl delete namespace fmi-criu
kubectl label node <NODE-A> fmi-machine- ; kubectl label node <NODE-B> fmi-machine-
# optionally revert the knative-serving feature gates from 2a
```

## Verification status

Verified on this host: `local-two-machines.sh` (rootless: dump → Redis staging → wiped local
state → restore-remote under fake serverless identities → promote), the orchestrator/restore-
server unit tests, and `local-two-containers.sh` (machine-a container stopped after staging;
four fresh privileged containers each restored one rank via `restore_server.py` over HTTP;
epoch 1 with all 8 ranks OK). The cluster steps (§2) are the deployment procedure for a real
multi-node Knative cluster; the highest-risk piece they add over the local runs is **criu
restore inside a Knative pod** (capabilities via the 2a feature gates) — covered first by
preflight 2e(2).

## Scaling

The layout generalizes to N machines × `RANKS_HERE` ranks: apply `machine-deployment.yaml` per
machine with the right `MACHINE_ID`/`NODE_VALUE`/`BASE_PEER_ID`, set `NUM_PEERS` to the total,
`RESTORE_MAX_SCALE` to the largest per-machine rank count, and point `EVACUATE_SELECTOR` /
`EVACUATE_DEPLOYMENT` at the machine you want to evacuate. Evacuate one machine at a time (one
epoch cut at a time).
