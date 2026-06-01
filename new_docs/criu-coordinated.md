# CRIU-Coordinated Mode

`criu_coordinated` adds process checkpoint/restore around a plain
`FMI::Communicator`. Applications keep using the normal C++ communicator API.
The runtime handles operation-boundary quiescence, and an external supervisor
drives `criu dump` and `criu restore`.

## Build And Backend Requirements

The communicator constructor enforces these constraints:

- FMI must be built with `FMI_ENABLE_CRIU`
- the config mode must be `criu_coordinated`
- `Direct` must be enabled
- `Direct` must be the only active data backend
- `preferred_data_backend` must be empty or `Direct`

The supervisor also requires:

- Redis as the FT control backend
- all ranks registered on the supervisor host
- all ranks using the `Direct` backend

This is a same-host v1 design. Cross-host CRIU restore is not implemented.

## Runtime Registration

`CriuRuntime` is created by `Communicator` in CRIU mode. On construction it:

1. parses the FT config
2. resolves `host_id` from config or `gethostname()`
3. creates a `Coordinator`
4. registers the rank with PID, host ID, and backend name

The runtime updates rank state and heartbeat timestamp when entering or exiting
FMI operations.

## Operation Accounting

`CriuRuntime` maintains:

- `active_operations`
- `quiescing`
- `last_completed_generation`

`enter_operation()` admits a new operation only when there is no newer
checkpoint request. If a checkpoint is pending, new operation entry blocks
while the runtime quiesces.

`exit_operation()` decrements the active-operation count. If the last active
operation drains while a checkpoint request is pending, the runtime starts
quiescence.

An accounting underflow in `exit_operation()` throws a runtime error.

## Quiescence

Quiescence for generation `N` does the following:

1. calls the communicator's `prepare_channels_for_checkpoint()` callback
2. each channel receives `prepare_for_checkpoint()`
3. `Direct::prepare_for_checkpoint()` closes all Direct sockets
4. marks the rank `QUIESCED` with `quiesced_generation = N`
5. waits until Redis `restore_generation >= N`
6. marks the rank `RUNNING`
7. records `last_completed_generation = N`
8. wakes blocked operation-entry threads

The runtime blocks inside quiescence until the supervisor requests restore for
that generation. This makes the checkpoint boundary visible to all ranks before
normal communication resumes.

## Supervisor Checkpoint Flow

`CriuSupervisor::checkpoint()`:

1. requests the next checkpoint generation in Redis
2. waits until all local ranks are `QUIESCED` for that generation
3. marks the job `QUIESCED`
4. verifies all ranks are local and Direct-backed
5. runs `criu dump` once per rank
6. marks the checkpoint generation complete
7. returns the generation number

Images are written under:

```text
<images_dir>/<comm_name>/generation-<N>/rank-<rank>/
```

The dump command is:

```text
criu dump -t <pid> -D <rank_dir> -o dump.log --shell-job --leave-stopped
```

## Supervisor Restore Flow

`CriuSupervisor::restore(generation)`:

1. uses `completed_generation` if `generation` is `0`
2. verifies a generation is available
3. loads local rank metadata
4. verifies all ranks are local and Direct-backed
5. publishes `restore_generation = generation`
6. sends `SIGKILL` to registered PIDs greater than zero
7. runs `criu restore` once per rank
8. marks the job `RESTORED`
9. returns the restored generation

The restore command is:

```text
criu restore -D <rank_dir> -o restore.log --shell-job --restore-detached
```

## CLI

The `fmi-criu-supervisor` tool exposes:

```text
fmi-criu-supervisor checkpoint <comm_name> <num_peers> <config>
fmi-criu-supervisor restore <comm_name> <num_peers> <config> [generation]
fmi-criu-supervisor status <comm_name> <num_peers> <config>
fmi-criu-supervisor cleanup <comm_name> <num_peers> <config>
```

`status` prints job state and one line per registered rank.

