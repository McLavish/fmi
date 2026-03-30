# FMI Fault Tolerance

FMI fault tolerance v1 is coordinated rank migration across communicator epochs. It is designed for planned migration and restart, not transparent live process migration.

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

## Programming Model

The FT layer adds two APIs:

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

## Configuration

Enable FT in the FMI JSON config and point the control plane at Redis.

```json
{
  "backends": {
    "Redis": {
      "enabled": true,
      "host": "127.0.0.1",
      "port": 6379
    },
    "Direct": {
      "enabled": true,
      "host": "127.0.0.1",
      "port": 10000
    }
  },
  "fault_tolerance": {
    "enabled": true,
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

## C++ Usage

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

## Python Usage

The Python bindings mirror the same model:

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

## Checkpointing Responsibility

The FT layer only reconfigures communication. It does not serialize application memory for you.

That means the application is responsible for:

- deciding what state must survive migration
- writing that state on `MigrateSelf`
- restoring that state in the replacement worker before continuing

For small Python services this can be as simple as writing a JSON file. For larger C++ applications it may be a custom binary checkpoint or an external durable store.

## Local Demo

For a runnable local Python example, see:

- [runbooks/local-python311-direct/README.md](/home/luca/fmi-original/fmi/runbooks/local-python311-direct/README.md)
- [runbooks/local-python311-direct/fmi-ft.json](/home/luca/fmi-original/fmi/runbooks/local-python311-direct/fmi-ft.json)
- [runbooks/local-python311-direct/ft_migration_demo.py](/home/luca/fmi-original/fmi/runbooks/local-python311-direct/ft_migration_demo.py)
