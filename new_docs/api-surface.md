# API Surface

The current FT implementation is mostly wired through `FMI::Communicator`.

## C++ Communicator

The communicator constructor has optional FT-aware identity arguments:

```cpp
FMI::Communicator(
    peer_id,
    num_peers,
    config_path,
    comm_name,
    faas_memory,
    worker_id,
    placement
);
```

`worker_id` identifies the concrete worker process for a logical rank.
`placement` is stored in the coordinator directory and can be used by external
orchestrators. FMI does not interpret placement.

When FT is disabled, these arguments have no FT effect.

`Communicator::get_comm_name()` returns the effective channel communicator
name. In `transparent_migration`, this includes the epoch suffix. In
`criu_coordinated` and non-FT mode, it is the base communicator name.

## C++ Coordinator

`FMI::FT::Coordinator` is available through `include/fmi.h`.

Transparent migration methods intended for external orchestration include:

- `request_migration(rank)`
- `promote_epoch(next_epoch)`
- `epoch()`
- `directory_snapshot(epoch)`
- `placement_for_rank(epoch, rank)`
- `clear_job_state()`

CRIU methods (the single-rank registry) are used by the host-local migration
supervisor and the migration runtime, but are public in the header:

- rank registration and state updates (`criu_register_rank`,
  `criu_mark_rank_running`, `criu_mark_rank_quiesced`)
- rank status reads (`criu_rank_info`)
- `clear_criu_state()`

## Python Binding

Python exposes:

- `fmi.Communicator`
- `fmi.FTCoordinator`
- `fmi.RankDirectoryEntry`

`fmi.Communicator` accepts optional `faas_memory`, `worker_id`, and `placement`
arguments.

`fmi.FTCoordinator` exposes:

- `request_migration(rank)`
- `promote_epoch()`
- `clear_job_state()`
- `epoch()`
- `placement_for_rank(epoch, rank)`
- `directory_snapshot(epoch)`

There is no Python `FTSession` in the current binding, and there is no Python
binding for CRIU supervisor operations.

## CRIU Migration Supervisor CLI

The host-local migration supervisor is a C++ tool:

```text
fmi-migration-supervisor <migrate|watch|cleanup> <comm_name> <num_peers> <config> [rank]
```

It is built from `tools/migration_supervisor.cpp` when tools and CRIU support are
enabled.

## Operation Runtime Hook

All communicator operations are wrapped by `OperationGuard`, which delegates to
an optional `FMI::FT::OperationRuntime`.

Implemented runtimes:

- `TransparentMigrationRuntime`

This hook is the common point where migration enforces operation-boundary
coordination.
