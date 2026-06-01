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
3. determines whether this process is a replacement for its logical rank
4. registers into either the current epoch or the next epoch
5. builds channels with `<base>@epoch=<active_epoch>`

Normal workers register in the current epoch as `ACTIVE`.

Replacement workers register in `current_epoch + 1` as `REPLACED` while waiting
for the new epoch to be promoted. Once promotion is observed, they re-register
as `ACTIVE` in the promoted epoch and build channels for that epoch.

## Replacement Detection

A process is treated as a replacement when all of these are true:

- its rank is in the pending migration set
- the current epoch already has a worker ID for the same logical rank
- that existing worker ID differs from the new process's worker ID

If no worker ID is supplied, FMI auto-generates one using rank, PID, and current
time. For orchestrated migration, passing explicit worker IDs makes replacement
detection predictable.

## Promotion Loop

Surviving ranks and replacement ranks both participate in promotion. They
register themselves in `next_epoch`, refresh their leases, and check whether
all logical ranks have live leases in `next_epoch`.

The first process that sees enough live members calls `promote_epoch()`. Other
participants observe that Redis `current_epoch` has advanced and then rebuild
their channels with the new epoch-qualified name.

If promotion does not happen within `lease_ms`, the waiting process throws
`FMI::Utils::Timeout`.

## What Epochs Do Not Solve

Epochs fence backend-visible communication names. They do not:

- serialize or restore application memory
- preserve an FMI operation already in flight
- guarantee atomic multi-key Redis state transitions
- recover from arbitrary crashes without an external replacement/orchestrator
- delete every old data-plane object immediately

