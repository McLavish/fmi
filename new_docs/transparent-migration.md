# Transparent Migration Mode

`transparent_migration` is the current non-CRIU migration mode. It is
"transparent" only in the communication API sense: user code keeps using a
plain `FMI::Communicator`, and FMI rebuilds channels across epochs at operation
boundaries.

## What This Is / Is Not

This mode is orchestrator-driven rank migration and communication
reconfiguration. It is not crash-recovery fault tolerance.

FMI does not checkpoint application memory, replay user work, or realign an
application's operation sequence after relaunch. The orchestrator and
application own state checkpoint/restore and step alignment. The demos do this
explicitly, for example by relaunching a replacement with `resume=True` after
the original rank has completed the first phase.

## Construction

When `fault_tolerance.enabled = true` and
`fault_tolerance.mode = "transparent_migration"`, the `Communicator`
constructor performs FT setup before building channels:

1. parse the FT config
2. validate `preferred_data_backend`, if one is configured
3. resolve a worker ID, either from the constructor argument or by generating
   `rank-<rank>-pid-<pid>-ts-<millis>`
4. create a Redis-backed `Coordinator`
5. read the active Redis epoch
6. if this rank is pending, wait until the orchestrator promotes and clears
   pending, then re-read the epoch
7. register rank membership, placement, and state in the selected epoch
8. build channels with `<base_comm_name>@epoch=<active_epoch>`
9. install `TransparentMigrationRuntime` as the operation runtime

The constructor accepts optional `worker_id` and `placement` strings. Python
exposes the same trailing constructor arguments on `fmi.Communicator`.

## Operation Boundary Checks

Every public communicator operation creates an `OperationGuard`. Its
constructor calls `enter_operation()` and its destructor calls
`exit_operation()`.

For transparent migration, `enter_operation()`:

1. reads the current Redis epoch
2. reconfigures immediately if the epoch is greater than this communicator's
   active epoch
3. returns immediately if no migration is pending
4. if this rank is pending, marks itself `QUIESCED` and calls `std::exit(0)`
5. otherwise parks until the orchestrator promotes the epoch, then rebuilds
   channels with `<base_comm_name>@epoch=<observed_epoch>`

`exit_operation()` is currently a no-op for this mode. There is no in-flight
counter because migration is only checked before each FMI operation starts.

## Centralized Promotion

Workers are followers. They never call `promote_epoch()`.

The external orchestrator owns the migration sequence:

1. `request_migration(rank)`
2. wait for the old rank to reach an operation boundary and mark `QUIESCED`
3. relaunch or otherwise arrange the replacement rank
4. call `promote_epoch()`

Promotion clears the pending set and advances `current_epoch` only if the new
epoch is greater than the stored epoch. Survivors blocked in
`enter_operation()` and replacements blocked in construction then join the
promoted epoch. The data-plane rendezvous on epoch-qualified names is the
barrier that proves all ranks reached the same communication generation.

If promotion is not observed within `reconfigure_timeout_ms`, the waiting
process throws `FMI::Utils::Timeout`. Polling uses `poll_interval_ms`.

## Migrating Rank Behavior

When a rank sees itself in the pending set at an operation boundary, it exits
the process. FMI does not run application checkpoint hooks and does not return a
migration event to user code.

Any application state transfer must be handled by an external mechanism or by
using CRIU mode.

## Channel Policy

If `preferred_data_backend` is configured, construction fails unless that
backend is active in the config. The value is then passed to `ChannelPolicy`,
which makes the preferred backend win channel selection.

Most FT configs set:

```json
"preferred_data_backend": "Direct"
```

This keeps normal data traffic on Direct/TCP while Redis is used only for FT
coordination.

## Observability

Use `FTCoordinator` to inspect migration state:

- `epoch()` returns the active epoch
- `directory_snapshot(epoch)` returns rank, worker ID, placement, and state
- `placement_for_rank(epoch, rank)` returns the stored placement string
- `promote_epoch()` advances to the next epoch from Python orchestrators

The Python binding exposes these methods through `fmi.FTCoordinator`.
