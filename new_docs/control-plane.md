# Redis Control Plane

Both FT modes use Redis as the control plane. The Redis data backend can still
be disabled for normal FMI traffic; the control plane uses its own
`fault_tolerance.control_host` and `fault_tolerance.control_port` settings.

`FMI::FT::ControlPlane` is the only in-tree control-plane client. It validates
that FT is enabled and that `control_backend` is `Redis`.

## Client Behavior

The control plane owns one persistent `redisContext` per `ControlPlane::Impl`.
Commands are sent with `redisCommandArgv`, so communicator names, worker IDs,
and placement strings are always command arguments rather than printf-format
strings. On a broken connection, the client reconnects and retries the command
once. Access to the hiredis context is serialized inside the control plane so
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

`epoch:<N>:placement` is a hash from logical rank to an optional placement
string. FMI stores this string but does not interpret it.

## Migration Request

`ControlPlane::request_migration(rank)` does two things:

- adds `rank` to the `pending` set
- sets that rank's state in the current epoch to `MIGRATION_PENDING` — unless
  the rank already reached `QUIESCED`, which is preserved (re-requesting a rank
  that already quiesced must not knock it back to pending)

Requests are scoped to **one migration cut at a time**: while a rank outside
the requested set is pending, the request fails with an error. Promotion
releases one global cut, so two overlapping cuts (say, two hosts evacuating
concurrently) would silently drop each other's migrations — the first
promotion clears the whole pending set. Re-requesting already-pending ranks,
or a superset of them, is idempotent; a rejected requester retries after the
in-flight cut promotes.

There is no scheduler or failure detector in the library. An external
orchestrator is expected to request migration, arrange replacement execution,
and promote the epoch.

## Epoch Promotion

`ControlPlane::promote_epoch(next_epoch)` is the single writer path for
`current_epoch`. Workers never call it. It returns `true` when the call
performed the promotion and `false` when the epoch was already `>= next_epoch`.

Promotion runs as one Lua `EVAL` script:

- read `meta[current_epoch]`
- if `next_epoch <= current_epoch`, leave Redis unchanged and return `false`
- **quiescence gate**: if any rank in the `pending` set is not `QUIESCED` in
  the current epoch's state hash, fail with an error and change nothing
- write `current_epoch = next_epoch`
- delete the `pending` set and the previous epoch's hashes in the same script

This prevents stale orchestrator calls from moving the epoch backward, makes
repeated promotion requests harmless, and makes the protocol's ordering
requirement impossible to violate: promoting past an un-quiesced target would
clear the pending set — the only signal telling that rank it is a migration
target — so its next operation would rejoin the new epoch as a survivor next to
its freshly launched replacement (two live processes owning one logical rank).
The gate turns that silent corruption into a loud, retryable error; the
orchestrator promotes after observing `QUIESCED` in `directory_snapshot`.

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

CRIU rank heartbeats are timestamps in a hash. The current rank agent logic
does not expire old CRIU rank entries by timestamp.

## Cleanup

`ControlPlane::clear_job_state()` deletes the transparent-migration metadata and
all `epoch:*` keys for the communicator.

`ControlPlane::clear_criu_state()` deletes all keys under the communicator's
`criu:*` subtree.

The rank agent's `cleanup()` also removes
`images_dir/<comm_name>/` before clearing CRIU Redis state.
