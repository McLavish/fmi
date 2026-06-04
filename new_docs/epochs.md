# Epoch System

Epochs are the central fencing mechanism for `transparent_migration`.

An epoch is a monotonically increasing communicator generation stored in Redis
as `fmi:ft:<comm_name>:meta[current_epoch]`. Epoch `0` is created when the
coordinator first initializes a job.

## Why Epochs Exist

The transport backends identify messages and resources using `comm_name`.
Without fencing, a replacement rank could accidentally consume stale data or
connect to an old Direct pairing name from before migration.

The implementation avoids this by changing the communicator name used by all
channels:

```text
<base_comm_name>@epoch=<N>
```

For example, a logical communicator named `job-a` uses transport name
`job-a@epoch=0` in epoch 0 and `job-a@epoch=1` after the first migration.

## What Gets Fenced

The epoch-qualified communicator name is passed to every channel through
`Channel::set_comm_name()`.

That affects:

- Direct pairing names:
  `<epoch_comm_name><sender>_<receiver>`
- Redis and S3 object names created by `ClientServer`:
  `<epoch_comm_name>...`
- per-channel operation counters, because reconfiguration constructs fresh
  channel objects with fresh `num_operations` maps

Old Direct sockets are finalized and closed before a survivor rebuilds channels
for the new epoch. Old Redis/S3 objects may still exist until finalized or
deleted, but their names no longer match the new epoch.

## Startup Epoch Selection

When `fault_tolerance.mode` is `transparent_migration`, the `Communicator`
constructor:

1. creates a `Coordinator` for the base communicator name
2. reads `current_epoch`
3. if this rank is in the pending set, waits until the orchestrator promotes
   and clears pending, then re-reads `current_epoch`
4. registers as `ACTIVE` in the selected epoch
5. builds channels with `<base>@epoch=<active_epoch>`

Normal workers join the current epoch directly.

Replacement workers are identified by the pending set, not by comparing worker
IDs. They wait in the constructor until the orchestrator clears pending by
promoting the epoch.

## Promotion

The orchestrator is the only promoter. Workers never call `promote_epoch()`;
they only observe that Redis `current_epoch` has advanced and then rebuild
their channels with the new epoch-qualified name.

Promotion uses a Redis Lua compare-and-set script. It advances
`current_epoch` only when the requested next epoch is greater than the stored
epoch, and it clears the pending set in the same script.

If promotion is not observed within `reconfigure_timeout_ms`, the waiting
process throws `FMI::Utils::Timeout`. Waiting workers poll Redis every
`poll_interval_ms`.

## What Epochs Do Not Solve

Epochs fence backend-visible communication names. They do not:

- serialize or restore application memory
- preserve an FMI operation already in flight
- migrate a process blocked inside an operation before it reaches the next
  operation boundary
- recover from arbitrary crashes without an external replacement/orchestrator
- delete every old data-plane object immediately
