# Redis Control Plane

Both FT modes use Redis as the control plane. The Redis data backend can still
be disabled for normal FMI traffic; the control plane uses its own
`fault_tolerance.control_host` and `fault_tolerance.control_port` settings.

`FMI::FT::Coordinator` is the only in-tree control-plane client. It validates
that FT is enabled and that `control_backend` is `Redis`.

## Client Behavior

The coordinator owns one persistent `redisContext` per `Coordinator::Impl`.
Commands are sent with `redisCommandArgv`, so communicator names, worker IDs,
and placement strings are always command arguments rather than printf-format
strings. On a broken connection, the client reconnects and retries the command
once. Access to the hiredis context is serialized inside the coordinator so
CRIU runtime helper threads can share the same control client.

The higher-level communicator and channel runtime still assumes one FMI
operation at a time per communicator.

## Transparent Migration Keys

All keys for a communicator start with:

```text
fmi:ft:<comm_name>:
```

`<comm_name>` is the logical base communicator name supplied by the user, not
the epoch-qualified transport name.

The transparent migration keys are:

```text
fmi:ft:<comm>:meta
fmi:ft:<comm>:pending
fmi:ft:<comm>:epoch:<N>:members
fmi:ft:<comm>:epoch:<N>:states
fmi:ft:<comm>:epoch:<N>:placement
```

`meta` is a Redis hash. It stores:

- `world_size`: expected number of logical ranks
- `current_epoch`: active communicator epoch, initialized to `0`

`pending` is a Redis set of logical rank IDs requested for migration.

`epoch:<N>:members` is a hash from logical rank to worker ID.

`epoch:<N>:states` is a hash from logical rank to state. The current state
strings are:

- `ACTIVE`
- `MIGRATION_PENDING`
- `QUIESCED`
- `REPLACED`

`epoch:<N>:placement` is a hash from logical rank to an optional placement
string. FMI stores this string but does not interpret it.

## Migration Request

`Coordinator::request_migration(rank)` does two things:

- adds `rank` to the `pending` set
- sets that rank's state in the current epoch to `MIGRATION_PENDING`

There is no scheduler or failure detector in the library. An external
orchestrator is expected to request migration, arrange replacement execution,
and promote the epoch.

## Epoch Promotion

`Coordinator::promote_epoch(next_epoch)` is the single writer path for
`current_epoch`. Workers never call it.

Promotion runs as one Lua `EVAL` script:

- read `meta[current_epoch]`
- if `next_epoch > current_epoch`, write `current_epoch = next_epoch`
- delete the `pending` set in the same script
- otherwise leave Redis unchanged

This prevents stale orchestrator calls from moving the epoch backward while
still making repeated promotion requests harmless.

## CRIU Keys

CRIU coordination uses a separate subtree:

```text
fmi:ft:<comm>:criu:meta
fmi:ft:<comm>:criu:ranks
fmi:ft:<comm>:criu:rank:<rank>
```

`criu:meta` is a hash. It stores:

- `world_size`

`criu:ranks` is a set of registered ranks.

`criu:rank:<rank>` is a hash. It stores:

- `pid`
- `host_id`
- `backend`
- `state`: `RUNNING` or `QUIESCED`
- `quiesced_generation`
- `last_heartbeat_ms`

CRIU rank heartbeats are timestamps in a hash. The current supervisor logic
does not expire old CRIU rank entries by timestamp.

## Cleanup

`Coordinator::clear_job_state()` deletes the transparent-migration metadata and
all `epoch:*` keys for the communicator.

`Coordinator::clear_criu_state()` deletes all keys under the communicator's
`criu:*` subtree.

The migration supervisor's `cleanup()` also removes
`images_dir/<comm_name>/` before clearing CRIU Redis state.
