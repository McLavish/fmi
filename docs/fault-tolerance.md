# FMI Fault Tolerance

FMI has a **single** fault-tolerance protocol: **transparent migration**. Applications use a
plain `FMI::Communicator`; there is no separate FT API or mode selector. Fault tolerance is
turned on with `fault_tolerance.enabled` in the JSON config. Redis is the control plane and
`Direct`/TCP is the preferred data plane.

A migration moves one logical rank across a communicator **epoch cut**: the targeted rank
quiesces at an operation boundary, surviving ranks rebuild their channels under an
epoch-qualified communicator name, and a replacement rejoins with the same logical rank ID at
epoch `N+1`. Every backend-visible name (Direct pairing names, Redis/S3 object names, operation
counters) is epoch-qualified, so no stale message or object from epoch `N` can be consumed after
reconfiguration.

What it does:

- preserves logical rank IDs across migration
- triggers migration externally (an orchestrator calls `request_migration`)
- cuts over at FMI operation boundaries (via `Communicator`'s `OperationGuard`)
- uses `Direct`/TCP as the data plane and Redis for FT control

What it does not do:

- preserve in-flight collectives (cutover is only between operations)
- recover arbitrary crashes in the middle of an FMI operation

## Application-state handling: `fault_tolerance.state_transfer`

The protocol above always reconfigures *communication*. What happens to the migrated rank's
*application memory* is selected by `fault_tolerance.state_transfer` — a mechanism toggle within
the one protocol, **not** a second protocol:

| value            | what happens to the targeted rank | application work |
|------------------|-----------------------------------|------------------|
| `"none"` (default) | quiesces and `exit`s; a fresh replacement rejoins at epoch `N+1` | must recompute or restore its own state |
| `"criu"`           | its process image is checkpointed and restored (same-host v1) | none — memory is preserved transparently |

`"criu"` requires a build with `FMI_ENABLE_CRIU=ON`.

### `state_transfer = "none"`

The default. The migrated rank discards its memory; an orchestrator launches a replacement with
the same logical rank, which starts fresh at epoch `N+1`. This is appropriate for stateless or
recomputable workloads. The heterogeneous LocalStack EC2→Lambda runbook
(`runbooks/localstack-python311-redis/`) exercises this path: the replacement Lambda runs with
`resume=True` and recomputes phase 1.

### `state_transfer = "criu"` — transparent state transfer (same-host v1)

With CRIU, the migrated rank keeps its in-memory application state with **no application
checkpoint code**. The rank stays a plain `FMI::Communicator`; it never calls criu itself.

At the migration quiesce point (`TransparentMigrationRuntime`), the targeted rank:

1. opts in to ptrace from a non-parent supervisor via `prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY)`
   (needed under the default `yama ptrace_scope=1`),
2. releases its `Direct` sockets (`prepare_for_checkpoint`),
3. publishes a restorable image entry (pid + host + `QUIESCED@epoch N+1`) into the Redis CRIU
   rank registry,
4. blocks in the promotion-wait loop.

A host-local supervisor (`FMI::FT::MigrationSupervisor`, CLI `fmi-migration-supervisor`) then:

1. waits for that ready entry,
2. `criu dump`s the rank's process image (which kills and reaps the original),
3. `criu restore`s it — the restored image resumes inside the same wait loop, carrying all
   application memory,
4. promotes the epoch to `N+1`.

The restored rank and the survivors both observe epoch `N+1` and reconfigure their channels under
the new epoch-qualified name; `Direct` re-pairs lazily. The Redis control connection is closed by
criu's `--tcp-close` and the `Coordinator` reconnects lazily (SIGPIPE is ignored so the first
post-restore write is not fatal).

#### Configuration

```json
{
  "backends": {
    "Direct": { "enabled": true, "host": "127.0.0.1", "port": 10000, "max_timeout": 30000 },
    "Redis":  { "enabled": false, "host": "127.0.0.1", "port": 6379 }
  },
  "fault_tolerance": {
    "enabled": true,
    "control_backend": "Redis",
    "control_host": "127.0.0.1",
    "control_port": 6379,
    "poll_interval_ms": 50,
    "preferred_data_backend": "Direct",
    "state_transfer": "criu",
    "images_dir": "/tmp/fmi-criu-images",
    "poll_ms": 50,
    "quiesce_timeout_ms": 60000,
    "reconfigure_timeout_ms": 60000,
    "host_id": "local-criu-host"
  }
}
```

#### Supervisor CLI

```text
fmi-migration-supervisor migrate <comm_name> <num_peers> <config> <rank>
fmi-migration-supervisor watch   <comm_name> <num_peers> <config>
```

`migrate` performs one migration of an explicit rank; `watch` migrates the first rank marked via
`request_migration`. Per-deployment criu flags can be injected with the `FMI_CRIU_EXTRA_ARGS`
environment variable (e.g. `--unprivileged` for rootless criu). It is split on whitespace with
no shell quoting, so individual flags must not contain spaces.

#### Demo and runbook

- C++ demo: `tests/transparent_state_transfer_demo.cpp` (a plain `FMI::Communicator` whose
  post-migration `allreduce` proves state survived).
- Verified end-to-end runbook (rootless criu 4.2): `runbooks/local-criu-state-transfer/`.

#### v1 limitations

- same host only (criu restores on the dumping host)
- `Direct` is the only supported data backend; Redis is the control plane
- one targeted rank per migration; no in-flight-collective preservation
- no Python binding for the CRIU path (the demo is C++)
- the supervisor is the migration authority and must complete `migrate_rank` (dump → restore →
  `promote_epoch`). v1 has no supervisor-failure recovery: if it dies between restore and epoch
  promotion, survivors eventually hit `reconfigure_timeout_ms` and fail. Run the supervisor under
  process supervision for planned migrations.
- `reconfigure_timeout_ms` (the survivor wait) must comfortably exceed the dump + restore time;
  size it for your largest process image (the runbook uses 60 s). The migrated rank itself waits
  unbounded across the checkpoint, so only survivors are exposed to this deadline.

## Triggering a migration

An orchestrator drives migration through the `Coordinator` control plane.

C++:

```cpp
#include <fmi.h>

FMI::FT::Coordinator coordinator("config/fmi.json", comm_name, world_size);
coordinator.request_migration(rank_to_move);   // mark the rank for migration
// ... for state_transfer="criu" a fmi-migration-supervisor promotes the epoch;
//     for state_transfer="none" the orchestrator launches a replacement and calls
//     coordinator.promote_epoch(coordinator.epoch() + 1);
```

Python (`fmi.FTCoordinator`):

```python
import fmi

coordinator = fmi.FTCoordinator("config/fmi.json", comm_name, world_size)
coordinator.request_migration(rank_to_move)
# coordinator.promote_epoch()  # advance to the next epoch
# coordinator.directory_snapshot(epoch)  # inspect rank states/placements
```

## Operational requirements

For the CRIU path you need, on the target Linux host:

- a running Redis instance for the control plane
- a running `tcpunchd` instance for the `Direct` backend
- a working `criu` (`criu check --unprivileged` should report "Looks good" for rootless use)

In CI / environments without CRIU capabilities, the supervisor flow is exercised against a mock
`criu` binary — see the `CriuFaultTolerance` suite in `tests/criu_fault_tolerance.cpp`.

## Experimental whole-job CRIU rollback

A separate, experimental whole-job checkpoint/rollback path also exists under
`FMI_ENABLE_CRIU=ON` (`FMI::FT::CriuRuntime`, `fmi-criu-supervisor`,
`tests/criu_checkpoint_demo.cpp`). It dumps and restores *all* ranks together to the same
generation (no epoch change, no single-rank relocation) and is quarantined WIP raw material —
not part of the transparent-migration protocol described above.
