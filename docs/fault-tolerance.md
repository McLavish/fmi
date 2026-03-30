# FMI Fault Tolerance

FMI currently supports two distinct fault-tolerance modes:

- `safe_point_restart`
- `criu_coordinated`

Both modes use Redis as the control plane. They differ in what happens to application state.

## `safe_point_restart`

This is the epoch-based migration mode that preserves logical rank IDs across communicator rebuilds.

What it does:

- preserves logical rank IDs
- uses `Direct`/TCP as the preferred data plane
- uses Redis as the FT control plane
- switches the communicator from epoch `N` to epoch `N+1`
- lets a replacement worker rejoin with the same logical rank

What it does not do:

- migrate process memory automatically
- preserve in-flight collectives
- recover arbitrary crashes in the middle of an FMI operation

### Programming model

The restart mode adds two APIs:

- `FMI::FT::Session` / `fmi.FTSession`
- `FMI::FT::Coordinator` / `fmi.FTCoordinator`

Applications use a session for communication and call `safe_point()` between communication phases.

`safe_point()` can return:

- `None` / `fmi.ft_events.none`
- `MigrateSelf` / `fmi.ft_events.migrate_self`
- `Reconfigured` / `fmi.ft_events.reconfigured`

The intended flow is:

1. Run a communication phase.
2. Call `safe_point()`.
3. If the result is `MigrateSelf`, checkpoint application state and exit.
4. Start a replacement worker with the same logical rank.
5. Restore the application checkpoint.
6. Call `safe_point()` on the replacement worker.
7. Continue on the rebuilt communicator for the new epoch.

### Configuration

```json
{
  "fault_tolerance": {
    "enabled": true,
    "mode": "safe_point_restart",
    "control_backend": "Redis",
    "control_host": "127.0.0.1",
    "control_port": 6379,
    "heartbeat_ms": 50,
    "lease_ms": 5000,
    "safe_point_only": true,
    "preferred_data_backend": "Direct"
  }
}
```

`preferred_data_backend = "Direct"` makes FT-enabled communicators fail fast if the TCP backend is not available.

### C++ usage

```cpp
#include <fmi.h>

FMI::FT::Session session(rank, world_size, "config/fmi.json", comm_name, worker_id);

FMI::Comm::Data<int> local(rank + 1);
FMI::Comm::Data<int> phase1_sum;
session.comm().allreduce(local, phase1_sum, sum_fn());

auto event = session.safe_point();
if (event == FMI::FT::Event::MigrateSelf) {
    save_checkpoint(app_state);
    return;
}

if (event == FMI::FT::Event::Reconfigured) {
    restore_checkpoint_if_needed(app_state);
}

FMI::Comm::Data<int> next_value(app_state.next_value);
FMI::Comm::Data<int> phase2_sum;
session.comm().allreduce(next_value, phase2_sum, sum_fn());
```

To trigger migration externally:

```cpp
FMI::FT::Coordinator coordinator("config/fmi.json", comm_name, world_size);
coordinator.request_migration(rank_to_move);
```

### Python usage

The Python bindings mirror the restart model:

- `fmi.FTSession`
- `fmi.FTCoordinator`
- `fmi.ft_events`

`FTSession` exposes the same collective methods as `Communicator`, plus:

- `safe_point()`
- `epoch()`

Example:

```python
import fmi

session = fmi.FTSession(rank, world_size, "config/fmi-ft.json", comm_name, worker_id, 1024)
session.hint(fmi.hints.fast)

phase1_sum = session.allreduce(rank + 1, fmi.func(fmi.op.sum), fmi.types(fmi.datatypes.int))

event = session.safe_point()
if event == fmi.ft_events.migrate_self:
    save_checkpoint(state)
    raise SystemExit(100)

if event == fmi.ft_events.reconfigured:
    state = load_checkpoint()

phase2_sum = session.allreduce(state["phase2_value"], fmi.func(fmi.op.sum), fmi.types(fmi.datatypes.int))
```

The local Python demo that exercises this flow lives in [ft_migration_demo.py](/home/luca/fmi-original/fmi/runbooks/local-python311-direct/ft_migration_demo.py).

### Checkpointing responsibility

The restart mode only reconfigures communication. It does not serialize application memory for you.

That means the application is responsible for:

- deciding what state must survive migration
- writing that state on `MigrateSelf`
- restoring that state in the replacement worker before continuing

## `criu_coordinated`

This is a second FT mode that uses CRIU for coordinated rollback/restart on the same host. It is transparent at the FMI API layer: applications continue to use plain `FMI::Communicator`.

What it does:

- uses CRIU to capture process state instead of application-managed serialization
- coordinates checkpoint and restore through Redis
- quiesces only at FMI operation boundaries
- keeps the `Communicator` API unchanged for C++ applications

Current v1 limits:

- same-host only
- `Direct` is the only supported FMI data backend
- Redis is required for control-plane coordination
- no Python CRIU binding
- no single-rank transparent migration
- no cross-machine restore

### Programming model

In CRIU mode, the application does not use `FMI::FT::Session`. It uses `FMI::Communicator` directly.

Internally, FMI:

- registers each rank in Redis
- counts active FMI operations
- stops admitting new FMI operations after a checkpoint request
- waits until the current operation drains
- closes `Direct` sockets before the checkpoint
- blocks until the supervisor publishes the restore generation

Because CRIU captures memory, the application does not need explicit serialize/restore hooks for in-memory state.

### Configuration

```json
{
  "backends": {
    "Direct": {
      "enabled": true,
      "host": "127.0.0.1",
      "port": 10000,
      "max_timeout": 1000
    },
    "Redis": {
      "enabled": false,
      "host": "127.0.0.1",
      "port": 6379
    }
  },
  "fault_tolerance": {
    "enabled": true,
    "mode": "criu_coordinated",
    "control_backend": "Redis",
    "control_host": "127.0.0.1",
    "control_port": 6379,
    "preferred_data_backend": "Direct",
    "images_dir": "/tmp/fmi-criu-images",
    "poll_ms": 25,
    "quiesce_timeout_ms": 2000
  }
}
```

The checked-in example is [fmi_criu_test.json](/home/luca/fmi-original/fmi/config/fmi_criu_test.json).

### C++ usage

```cpp
#include <fmi.h>

FMI::Communicator comm(rank, world_size, "config/fmi_criu_test.json", comm_name);

int state = rank + 1;

FMI::Comm::Data<int> phase1_value(state);
FMI::Comm::Data<int> phase1_sum;
comm.allreduce(phase1_value, phase1_sum, sum_fn());

state += 100;

comm.barrier();

FMI::Comm::Data<int> phase2_value(state);
FMI::Comm::Data<int> phase2_sum;
comm.allreduce(phase2_value, phase2_sum, sum_fn());
```

The plain-`Communicator` demo for this mode lives in [criu_checkpoint_demo.cpp](/home/luca/fmi-original/fmi/tests/criu_checkpoint_demo.cpp).

### Supervisor flow

CRIU mode is driven by `fmi-criu-supervisor`:

```text
fmi-criu-supervisor checkpoint <comm_name> <num_peers> <config>
fmi-criu-supervisor restore <comm_name> <num_peers> <config> [generation]
fmi-criu-supervisor status <comm_name> <num_peers> <config>
fmi-criu-supervisor cleanup <comm_name> <num_peers> <config>
```

Checkpoint flow:

1. The supervisor requests a new checkpoint generation in Redis.
2. FMI ranks stop entering new communication operations.
3. Once all local ranks report `QUIESCED`, the supervisor runs `criu dump`.
4. Images are stored under `images_dir/<comm_name>/generation-<N>/rank-<r>/`.

Restore flow:

1. The supervisor chooses a completed generation.
2. It publishes the restore generation and launches `criu restore` for each rank image directory.
3. Restored processes resume from the memory state captured at checkpoint time.

### Operational requirements

You need all of the following on the target Linux host:

- a running Redis instance for the control plane
- a running `tcpunchd` instance for the `Direct` backend
- a working CRIU installation
- the required kernel capabilities for checkpoint/restore

On a typical system, `criu check` must succeed before live checkpoint/restore will work. In this development environment, CRIU is installed but the required capabilities are not available, so the repository test suite uses a mock `criu` binary to exercise the supervisor flow without performing a real dump/restore.

## Local demos

Restart-mode Python demo:

- [runbooks/local-python311-direct/README.md](/home/luca/fmi-original/fmi/runbooks/local-python311-direct/README.md)
- [runbooks/local-python311-direct/fmi-ft.json](/home/luca/fmi-original/fmi/runbooks/local-python311-direct/fmi-ft.json)
- [runbooks/local-python311-direct/ft_migration_demo.py](/home/luca/fmi-original/fmi/runbooks/local-python311-direct/ft_migration_demo.py)

CRIU-mode C++ demo:

- [fmi_criu_test.json](/home/luca/fmi-original/fmi/config/fmi_criu_test.json)
- [criu_checkpoint_demo.cpp](/home/luca/fmi-original/fmi/tests/criu_checkpoint_demo.cpp)
