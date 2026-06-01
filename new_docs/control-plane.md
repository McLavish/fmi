# Redis Control Plane

Both FT modes use Redis as the control plane. The Redis data backend can still
be disabled for normal FMI traffic; the control plane uses its own
`fault_tolerance.control_host` and `fault_tolerance.control_port` settings.

`FMI::FT::Coordinator` is the only in-tree control-plane client. It validates
that FT is enabled and that `control_backend` is `Redis`.

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
fmi:ft:<comm>:epoch:<N>:lease:<rank>
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

`epoch:<N>:lease:<rank>` is a TTL key containing the worker ID. It is refreshed
with `PSETEX` and expires after `fault_tolerance.lease_ms`.

## Leases And Liveness

`live_member_count(epoch)` scans the ranks registered in
`epoch:<N>:members` and counts only ranks whose lease key still exists.

Transparent migration uses this count as the epoch-promotion condition:

```text
live_member_count(next_epoch) >= num_peers
```

That means membership hashes can retain stale entries, but stale entries do not
advance an epoch unless their leases are also live.

## Migration Request

`Coordinator::request_migration(rank)` does two things:

- adds `rank` to the `pending` set
- sets that rank's state in the current epoch to `MIGRATION_PENDING`

There is no separate scheduler in the library. An external orchestrator is
expected to request migration and launch the replacement worker.

## Epoch Promotion

`Coordinator::promote_epoch(next_epoch)` writes `current_epoch = next_epoch`
and deletes the `pending` set.

The write is intentionally simple and idempotent, but it is not a compare-and-
set operation. The runtime assumes all participants are trying to promote the
same `active_epoch + 1` value.

## CRIU Keys

CRIU coordination uses a separate subtree:

```text
fmi:ft:<comm>:criu:meta
fmi:ft:<comm>:criu:ranks
fmi:ft:<comm>:criu:rank:<rank>
```

`criu:meta` is a hash. It stores:

- `world_size`
- `state`
- `requested_generation`
- `completed_generation`
- `restore_generation`
- `supervisor`

CRIU job state strings are:

- `RUNNING`
- `CHECKPOINT_REQUESTED`
- `QUIESCED`
- `CHECKPOINT_COMPLETE`
- `RESTORE_REQUESTED`
- `RESTORED`

`criu:ranks` is a set of registered ranks.

`criu:rank:<rank>` is a hash. It stores:

- `pid`
- `host_id`
- `backend`
- `state`: `RUNNING` or `QUIESCED`
- `quiesced_generation`
- `last_heartbeat_ms`

Unlike transparent migration leases, CRIU rank heartbeats are timestamps in a
hash. The current supervisor logic does not expire old CRIU rank entries by
timestamp.

## Cleanup

`Coordinator::clear_job_state()` deletes the transparent-migration metadata and
all `epoch:*` keys for the communicator.

`Coordinator::clear_criu_job_state()` deletes all keys under the communicator's
`criu:*` subtree.

`CriuSupervisor::cleanup()` also removes
`images_dir/<comm_name>/` before clearing CRIU Redis state.

