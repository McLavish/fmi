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
- agrees on **one consensus cut boundary** per migration: the first rank that observes the
  pending request fixes a `cut_index` (atomically, in the same per-operation control-plane
  round-trip), chosen past any operation another rank may already be inside. Every rank —
  the target included — keeps executing operations below the cut and parks exactly at it,
  so no rank is ever left blocked inside an operation the target never joins. The same
  round-trip publishes each rank's operation boundary (readable via
  `ControlPlane::operation_boundaries()` for progress/diagnostics).
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

1. opts in to ptrace from a non-parent rank agent via `prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY)`
   (needed under the default `yama ptrace_scope=1`),
2. releases its data-plane transport (`prepare_for_checkpoint`: `Direct` closes its peer
   sockets, `Redis` drops its client connection),
3. publishes a restorable image entry (pid + host + `QUIESCED@epoch N+1`) into the Redis CRIU
   rank registry,
4. blocks in a **socket-free** promotion-wait loop: the control-plane connection is dropped
   after every poll, so outside brief poll instants the process holds no TCP socket at all.

A host-local rank agent (`FMI::FT::LocalRankAgent`, CLI `fmi-rank-agent`) then:

1. waits for that ready entry,
2. `criu dump`s the rank's process image (which kills and reaps the original),
3. `criu restore`s it — the restored image resumes inside the same wait loop, carrying all
   application memory,
4. promotes the epoch to `N+1`.

The restored rank and the survivors both observe epoch `N+1` and reconfigure their channels under
the new epoch-qualified name; `Direct` re-pairs lazily. Because the dumped image contains no
established TCP socket, criu needs neither `--tcp-established` nor `--tcp-close`, the image is
host-agnostic, and the restored rank's first control-plane call simply opens a fresh Redis
connection (no dead-socket write, hence no SIGPIPE handling anywhere). The rank agent retries a
dump that happens to land during a poll instant.

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
    "criu": {
      "images_dir": "/tmp/fmi-criu-images",
      "poll_ms": 50,
      "quiesce_timeout_ms": 60000,
      "host_id": "local-criu-host"
    }
  }
}
```

#### Rank agent CLI

```text
fmi-rank-agent migrate <comm_name> <num_peers> <config> <rank>
fmi-rank-agent watch   <comm_name> <num_peers> <config>
```

`migrate` performs one migration of an explicit rank; `watch` migrates the first rank marked via
`request_migration`. Per-deployment criu flags can be injected with the `FMI_CRIU_EXTRA_ARGS`
environment variable (e.g. `--unprivileged` for rootless criu). It is split on whitespace with
no shell quoting, so individual flags must not contain spaces.

#### Demo and runbook

- C++ demo: `runbooks/local-criu-state-transfer/transparent_state_transfer_demo.cpp` (a plain
  `FMI::Communicator` whose post-migration `allreduce` proves state survived).
- Verified end-to-end runbook (rootless criu 4.2): `runbooks/local-criu-state-transfer/`.

#### v1 limitations

- same host only (criu restores on the dumping host)
- `Direct` and `Redis` are the supported (checkpoint-safe) data backends; S3 is rejected
  (live AWS SDK sockets/threads would be captured in the image). Redis is the control plane
- one targeted rank per migration; no in-flight-collective preservation
- no Python binding for the CRIU path (the demo is C++)
- the rank agent is the migration authority and must complete `migrate_rank` (dump → restore →
  `promote_epoch`). The library does **not** time this out: survivors (and a joining replacement)
  wait for epoch promotion indefinitely — see "Liveness" below. If the agent dies between restore
  and promotion the job will wait forever, so run the rank agent under process supervision and let
  the orchestrator act on its non-zero exit (abort the job, or promote a replacement).

## Liveness: who owns a stuck migration

The library never promotes its own epoch — an external actor always does (the orchestrator for
`state_transfer="none"`, the rank agent for `"criu"`). So a rank waiting at an operation boundary,
or a replacement waiting to join, **waits for epoch promotion indefinitely**; there is no
`reconfigure_timeout_ms` or any other library-side deadline on it. This is deliberate: a flat
wall-clock timeout cannot distinguish a slow-but-healthy migration (a large CRIU image) from a
dead one, and the library is not the actor that can resolve either.

Detecting and resolving a migration that never completes is the **orchestrator's** responsibility.
It already has the signals: `migrate_rank` / the `fmi-rank-agent` CLI exit non-zero on a failed
dump/restore, and the orchestrator can read epoch/rank state from the control plane. On failure it
chooses the policy — abort the job, retry, or promote a fresh (stateless) replacement to unblock
survivors. (Redis connectivity failures are unaffected: those still surface as exceptions from the
control-plane client; only the "promotion never arrives" case waits.)

## Triggering a migration

An orchestrator drives migration through the `ControlPlane` control plane.

C++:

```cpp
#include <fmi.h>

FMI::FT::ControlPlane control_plane("config/fmi.json", comm_name, world_size);
control_plane.request_migration(rank_to_move);   // mark the rank for migration
// ... for state_transfer="criu" a fmi-rank-agent promotes the epoch;
//     for state_transfer="none" the orchestrator waits until the target shows
//     QUIESCED in directory_snapshot(epoch()), launches a replacement, and calls
//     control_plane.promote_epoch(control_plane.epoch() + 1);
```

Python (`fmi.FTControlPlane`):

```python
import fmi

control_plane = fmi.FTControlPlane("config/fmi.json", comm_name, world_size)
control_plane.request_migration(rank_to_move)
# ... wait until directory_snapshot(control_plane.epoch()) shows the target QUIESCED ...
# control_plane.promote_epoch()  # advance to the next epoch (returns False if already promoted)
# control_plane.directory_snapshot(epoch)  # inspect rank states/placements
```

Promotion is **gated on quiescence**: `promote_epoch` throws (and changes nothing) while any
pending rank has not marked itself `QUIESCED` in the current epoch. An orchestrator that promotes
too early therefore gets a loud, retryable error instead of the silent alternative — clearing the
pending set before the target reached its quiesce point, after which the target's next operation
would rejoin the new epoch as a survivor right next to its replacement.

Migration requests are scoped to **one cut at a time**: `request_migration(s)` throws while a
rank outside the requested set is already pending. Promotion releases one global cut, so two
overlapping cuts (e.g. two hosts evacuating concurrently) would drop each other's migrations.
Serialize cuts in the orchestrator: request, quiesce, promote — then start the next cut.

## Operational requirements

For the CRIU path you need, on the target Linux host:

- a running Redis instance for the control plane
- a running `tcpunchd` instance for the `Direct` backend
- a working `criu` (`criu check --unprivileged` should report "Looks good" for rootless use)

In CI / environments without CRIU capabilities, the rank agent flow is exercised against a mock
`criu` binary — see the `CriuFaultTolerance` suite in `tests/criu_fault_tolerance.cpp`.
