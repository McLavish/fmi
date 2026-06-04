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

CRIU methods are used by `CriuRuntime` and `CriuSupervisor`, but are public in
the header:

- rank registration and state updates
- checkpoint and restore generation requests
- job and rank status reads
- `clear_criu_job_state()`

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

## CRIU Supervisor CLI

The CRIU supervisor is a C++ tool:

```text
fmi-criu-supervisor <checkpoint|restore|status|cleanup> <comm_name> <num_peers> <config> [generation]
```

It is built from `tools/criu_supervisor.cpp` when tools and CRIU support are
enabled.

## Operation Runtime Hook

All communicator operations are wrapped by `OperationGuard`, which delegates to
an optional `FMI::FT::OperationRuntime`.

Implemented runtimes:

- `TransparentMigrationRuntime`
- `CriuRuntime`

This hook is the common point where both modes enforce operation-boundary
coordination.
