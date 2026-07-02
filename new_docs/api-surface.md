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
`placement` is stored in the control-plane directory and can be used by external
orchestrators. FMI does not interpret placement.

When FT is disabled, these arguments have no FT effect.

`Communicator::get_comm_name()` returns the effective channel communicator
name. Under transparent migration this includes the epoch suffix; in non-FT
mode it is the base communicator name.

## C++ ControlPlane

`FMI::FT::ControlPlane` is available through `include/fmi.h`.

Transparent migration methods intended for external orchestration include:

- `request_migration(rank)`
- `promote_epoch(next_epoch)`
- `epoch()`
- `directory_snapshot(epoch)`
- `placement_for_rank(epoch, rank)`
- `clear_job_state()`

CRIU methods (the single-rank registry) are used by the host-local rank agent
and the migration runtime, but are public in the header:

- rank registration and state updates (`criu_register_rank`,
  `criu_mark_rank_running`, `criu_mark_rank_quiesced`)
- rank status reads (`criu_rank_info`)
- `clear_criu_state()`

## Python Binding

Python exposes:

- `fmi.Communicator`
- `fmi.FTControlPlane`
- `fmi.RankDirectoryEntry`

`fmi.Communicator` accepts optional `faas_memory`, `worker_id`, and `placement`
arguments.

`fmi.FTControlPlane` exposes:

- `request_migration(rank)`
- `promote_epoch()`
- `clear_job_state()`
- `epoch()`
- `placement_for_rank(epoch, rank)`
- `directory_snapshot(epoch)`

There is no Python `FTSession` in the current binding, and there is no Python
binding for CRIU rank agent operations.

## CRIU Rank Agent CLI

The host-local rank agent is a C++ tool:

```text
fmi-rank-agent <migrate|migrate-local|watch|cleanup> <comm_name> <num_peers> <config> [rank]
```

`migrate-local` migrates every rank advertised on this host in one epoch cut
(see `LocalRankAgent::migrate_local`); the other subcommands operate on a
single rank.

It is built from `tools/rank_agent.cpp` when tools and CRIU support are
enabled.

## Operation Runtime Hook

All communicator operations are wrapped by `OperationGuard`, which delegates to
an optional `FMI::FT::OperationRuntime`.

Implemented runtimes:

- `TransparentMigrationRuntime`

This hook is the common point where migration enforces operation-boundary
coordination.
