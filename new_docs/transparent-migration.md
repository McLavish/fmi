# Transparent Migration Mode

`transparent_migration` is the current non-CRIU migration mode. It is
"transparent" in the sense that user code keeps using `FMI::Communicator`
directly. The application does not call `safe_point()`, and the current branch
does not expose an `FTSession` wrapper.

## Construction

When `fault_tolerance.enabled = true` and
`fault_tolerance.mode = "transparent_migration"`, the `Communicator`
constructor performs FT setup before building channels:

1. parse the FT config
2. validate `preferred_data_backend`, if one is configured
3. resolve a worker ID, either from the constructor argument or by generating
   `rank-<rank>-pid-<pid>-ts-<millis>`
4. create a Redis-backed `Coordinator`
5. choose the active epoch
6. register rank membership, placement, state, and lease
7. build channels with `<base_comm_name>@epoch=<active_epoch>`
8. install `TransparentMigrationRuntime` as the operation runtime

The constructor accepts optional `worker_id` and `placement` strings. Python
exposes the same trailing constructor arguments on `fmi.Communicator`.

## Operation Boundary Checks

Every public communicator operation creates an `OperationGuard`. Its
constructor calls `enter_operation()` and its destructor calls
`exit_operation()`.

For transparent migration, `enter_operation()`:

1. refreshes this rank's lease in the current epoch
2. returns immediately if no migration is pending
3. if this rank is pending, marks itself `QUIESCED` and calls `std::exit(0)`
4. otherwise treats this rank as a survivor and starts epoch promotion

`exit_operation()` is currently a no-op for this mode. There is no in-flight
counter because migration is only checked before each FMI operation starts.

## Migrating Rank Behavior

When a rank sees itself in the pending set at an operation boundary, it exits
the process. FMI does not run application checkpoint hooks and does not return a
migration event to user code.

This means the current mode is best understood as a communication
reconfiguration skeleton. Any application state transfer must be handled by an
external mechanism or avoided by using CRIU mode.

## Survivor Behavior

Survivors call `promote_and_reconfigure()`:

1. set `next_epoch = active_epoch + 1`
2. repeatedly register themselves as `ACTIVE` in `next_epoch`
3. refresh their next-epoch lease
4. promote the epoch once all logical ranks have live next-epoch leases
5. when Redis reports an epoch greater than the old active epoch, rebuild
   channels with `<base_comm_name>@epoch=<observed_epoch>`

Reconfiguration is in-place. The `Communicator` object identity is preserved,
but its channels are finalized, cleared, and rebuilt.

## Replacement Behavior

A replacement process starts with the same logical rank as the old process but
a different worker ID.

In the constructor, replacement workers:

1. detect the pending migration against the previous worker ID
2. register into `current_epoch + 1` as `REPLACED`
3. refresh a next-epoch lease
4. promote the next epoch if all ranks are live
5. wait until Redis `current_epoch` advances
6. register as `ACTIVE` in the promoted epoch
7. build channels for the promoted epoch

The replacement constructor can block until the epoch is promoted. If promotion
does not complete within `lease_ms`, it throws `FMI::Utils::Timeout`.

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

The Python binding exposes these methods through `fmi.FTCoordinator`.

