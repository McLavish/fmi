# Transparent Rank Migration Demo (local, Direct/TCP)

Demonstrates FMI's `transparent_migration` FT mode: two ranks run a plain
`fmi.Communicator` allreduce loop with **no FT API calls**. One rank is migrated
mid-run — transparently, at an operation boundary — and replaced by a fresh process
with a new placement label. The rank directory flip is verified via
`FTCoordinator.directory_snapshot()`.

This is the transparent comms-migration skeleton that the production CRIU path
builds on: CRIU later adds memory capture in the same quiesce window. Switching
demo→production is a config-flag change with zero app-code change.

---

## What happens

```
Epoch 0:
  rank0 (placement=vm)  ──┐
                           ├── allreduce loop over Direct/TCP (@epoch=0 names)
  rank1 (placement=vm)  ──┘

  orchestrator: coordinator.request_migration(0)

At the next enter_operation() boundary:
  rank0: sees is_rank_pending(0) → marks Quiesced → exits
  rank1: sees pending → joins epoch 1, waits for full membership

orchestrator: detects rank0 Quiesced → relaunches as replacement (placement=serverless)
  replacement rank0: constructor reads epoch=1, builds @epoch=1 channels, registers

Epoch promotion:
  once both ranks are registered in epoch 1 → promote_epoch(1)
  rank1: reconfigure_to_epoch(@epoch=1) → channels rebuilt → loop resumes
  replacement rank0: begins loop (iteration 0; pre-migration results not preserved)

Post-migration directory (epoch 1):
  rank0: new worker_id, placement=serverless
  rank1: same worker_id,  placement=vm
```

**Known limitation:** the consistent cut (both ranks stopping epoch 0 at the same
boundary) is not enforced by a protocol — the demo relies on the migration flag
being observed between collectives. The residual race (flag set mid-collective on
one rank) is resolved in production by CRIU's coordinated cut.

---

## Prerequisites

Redis 7, tcpunchd, and the Python FMI module (Direct+Redis, no S3).

### 1 — Build tcpunchd (once)

```bash
cmake -S extern/TCPunch/server -B extern/TCPunch/server/build-debug
cmake --build extern/TCPunch/server/build-debug -j"$(nproc)"
```

### 2 — Build Python module

```bash
cmake -S python -B python/build-native-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DPython3_EXECUTABLE="$(command -v python3)" \
  -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_REDIS=ON -DFMI_USE_STATIC_BOOST=OFF
cmake --build python/build-native-debug -j"$(nproc)"
```

---

## Running the demo

### Start infrastructure (each in its own terminal or background)

```bash
# Terminal 1 — TCPunch rendezvous server
./extern/TCPunch/server/build-debug/tcpunchd 10000

# Terminal 2 — Redis
docker run --rm --name fmi-redis -p 127.0.0.1:6379:6379 redis:7
```

### Run the demo

```bash
python3 runbooks/local-python311-direct/transparent_migration_demo.py run \
  --comm-name demo-$(date +%s)
```

Expected output (abbreviated):

```
[rank 0] starting worker_id=rank0-vm-... placement=vm
[rank 1] starting worker_id=rank1-vm-... placement=vm
[rank 0] iter 0: allreduce=3.0
[rank 1] iter 0: allreduce=3.0
...
[orchestrator] requesting migration of rank 0
[orchestrator] waiting for rank 0 to quiesce ...
[orchestrator] rank 0 quiesced — launching replacement (placement=serverless)
[rank 0] starting worker_id=rank0-serverless-... placement=serverless
[rank 0] iter 0: allreduce=3.0
...

--- Epoch 0 directory ---
  rank 0: worker_id=rank0-vm-...       placement=vm         state=QUIESCED
  rank 1: worker_id=rank1-vm-...       placement=vm         state=ACTIVE

--- Epoch 1 directory ---
  rank 0: worker_id=rank0-serverless-... placement=serverless state=ACTIVE
  rank 1: worker_id=rank1-vm-...         placement=vm         state=ACTIVE

PASSED: rank directory flip verified
```

---

## Config

`fmi-transparent-migration.json` in this directory. Key setting:

```json
"fault_tolerance": {
  "enabled": true,
  "mode": "transparent_migration",
  "preferred_data_backend": "Direct"
}
```

Redis is used only as the FT control plane (membership, leases, placement
directory); the data plane is Direct/TCP.

---

## Cleanup

If the demo is interrupted before it finishes, clear Redis state manually:

```bash
python3 runbooks/local-python311-direct/transparent_migration_demo.py cleanup \
  --comm-name <your-comm-name>
```

---

## Relationship to production CRIU mode

The `transparent_migration` mode is the comms-migration skeleton. When
`criu_coordinated` mode is used, CRIU adds memory capture into the same quiesce
window:

| | `transparent_migration` (this demo) | `criu_coordinated` (production) |
|---|---|---|
| App code | Plain `Communicator`, no FT calls | Plain `Communicator`, no FT calls |
| Quiesce trigger | `enter_operation()` boundary | `enter_operation()` boundary |
| Epoch re-pairing | Yes — Direct sockets re-paired under `@epoch=N` names | Yes — same mechanism |
| Memory capture | No — work lost on migrated rank | Yes — `criu dump`/restore |
| Consistent cut | Informal (operation-boundary gap) | Coordinated barrier + `criu dump` |

Switching this demo to production: change `"mode"` to `"criu_coordinated"` in
the config. Zero app-code change.
