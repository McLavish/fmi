# FMI Fault Tolerance With TCP Data Plane and Redis Coordination

## Summary

Implement fault tolerance for FMI as coordinated rank migration across communicator epochs, not transparent live process migration. The default data plane should be `Direct`/TCP, while the first FT coordination plane should be Redis-backed.

Chosen semantics:

- logical rank IDs are preserved across migration
- migration is triggered externally
- cutover happens at FMI operation boundaries via `Communicator`'s `OperationGuard`
- in-flight collectives are not preserved
- application-state continuity is available via CRIU same-host state transfer
  (`fault_tolerance.state_transfer="criu"`); the default (`"none"`) still discards state and
  relies on a recomputing replacement
- `Redis` is used for FT membership, epochs, leases, and migration state
- `Direct` is the preferred/default FMI transport for rank-to-rank communication
- `Redis` remains available as an alternate data backend, but not the default

## Key Changes

### 1. Split fault tolerance into two planes

Keep FMI data transport and FT coordination separate.

- Data plane: existing FMI backends, with `Direct` as the default/preferred backend in FT-enabled deployments.
- Coordination plane: new Redis-backed FT metadata service that tracks communicator epoch, logical membership, worker-instance mapping, and migration state.

This avoids trying to build a distributed membership service on top of ad hoc TCP connections in v1.

### 2. Use transparent migration at operation boundaries

Fault tolerance is a single protocol: transparent migration. Applications keep using plain
`FMI::Communicator`. Every FMI operation is wrapped in `OperationGuard`, which calls the
configured `OperationRuntime` before and after the operation.

Current behavior:

- `TransparentMigrationRuntime` observes Redis migration requests at operation boundaries
- the targeted rank marks itself `QUIESCED` and exits so an external orchestrator can launch a replacement
- surviving ranks wait for the orchestrator to promote epoch `N+1`, then rebuild channels in place
- the replacement rank rejoins with the same logical rank ID under the new epoch-qualified communicator name

### 3. Make TCP the preferred data backend

Add FT-aware backend preference to configuration and channel selection.

Config additions:

- `fault_tolerance.enabled`
- `fault_tolerance.control_backend = "Redis"`
- `fault_tolerance.control_host`
- `fault_tolerance.control_port`
- `fault_tolerance.preferred_data_backend = "Direct"`

Policy changes:

- when FT is enabled, prefer `Direct` if it is enabled and healthy
- if `Direct` is unavailable at session construction, fail fast by default rather than silently switching transports
- allow an explicit optional fallback mode later, where `Redis` can be used as the data backend if configured

This keeps “TCP default” explicit instead of relying on the cost model to choose it accidentally.

### 4. Add epoch fencing to every backend-visible name

Every communicator instance used under FT must be epoch-qualified.

Apply epoch suffixing to:

- `comm_name` used by `Direct` pairing names
- object names for `Redis`
- object names for `S3`
- operation counters scoped to a communicator instance

On epoch change:

- destroy the old channel instances
- rebuild a fresh `Communicator`
- recreate channels with the new epoch-qualified communicator name
- ignore all old-epoch transport state

This is required to prevent stale messages or objects from leaking across migration.

Application state continuity depends on `fault_tolerance.state_transfer`:

- `"none"` (default): transparent migration only fences and rebuilds communication; the
  targeted rank exits and a fresh replacement recomputes.
- `"criu"` (same-host v1, requires `FMI_ENABLE_CRIU=ON`): the documented seam is now
  implemented. The outgoing rank's channel-release hook (`prepare_channels_for_checkpoint`) runs
  at the quiesce point, and a host-local `LocalRankAgent` (`fmi-rank-agent`)
  `criu dump`s the process before it leaves and `criu restore`s it before epoch `N+1` user work
  resumes — so process memory is preserved with no application checkpoint code. See
  `docs/fault-tolerance.md` and `runbooks/local-criu-state-transfer/`.

### 5. Define the migration state machine

Use Redis to manage these states per logical rank:

- `ACTIVE`
- `MIGRATION_PENDING`
- `QUIESCED`

Flow:

1. external daemon marks logical rank `r` as `MIGRATION_PENDING`
2. ranks observe the request at the next FMI operation boundary
3. rank `r` marks itself `QUIESCED`. With `state_transfer="none"` it exits; with
   `state_transfer="criu"` it instead publishes a restorable image entry and blocks while the
   rank agent checkpoints its process state.
4. the replacement takes over logical rank `r` at epoch `N+1`: with `"none"` a fresh worker
   registers and recomputes; with `"criu"` the rank agent restores the checkpointed process
   image, which resumes with its memory intact
5. once the orchestrator promotes epoch `N+1`, surviving communicators rebuild channels and resume

V1 defers any migration request that arrives while a rank is inside an FMI operation until the next operation boundary.

## Test Plan

### Unit and component tests

- Redis control-plane membership registration, heartbeat, lease expiry, and epoch advancement
- targeted ranks quiesce only at operation boundaries
- non-target ranks reconfigure exactly once per epoch transition
- `preferred_data_backend = Direct` is honored when FT is enabled
- communicator rebuild produces fresh epoch-qualified names for `Direct`, `Redis`, and `S3`

### Integration scenarios

- migrate one logical rank between two collective phases while preserving rank ID
- replacement worker rejoins and all peers continue on epoch `N+1`
- verify `Direct` reconnects cleanly after migration
- verify stale Redis/S3 transport objects from old epochs are ignored
- verify FT-enabled run fails fast if `Direct` is configured as required but cannot be established
- verify optional Redis data backend still works when explicitly selected for experiments

### Acceptance criteria

- logical rank numbering is unchanged after migration
- no old-epoch message or object is consumed after reconfiguration
- migration works with `Direct` as the default data backend
- Redis is only required for FT coordination, not for the normal data path
- FT-disabled applications keep today’s FMI behavior unchanged

## Assumptions and Defaults

- target runtime is long-lived processes on VMs or containers
- external daemon is responsible for eviction detection and replacement-worker launch
- applications already provide explicit checkpoint/restore hooks
- v1 targets planned migration, not arbitrary crash recovery or live process migration
- ranks issue guarded FMI operations in identical order (SPMD collectives): the consensus cut
  identifies operations across ranks by per-rank index, so divergent point-to-point schedules
  are out of scope — they fail-stop at the promotion gate (see docs/consensus-cut.md, "Scope:
  aligned operation streams")
- TCP means FMI `Direct` backend for rank-to-rank communication
- Redis is accepted for the first FT control plane even though the default data backend is TCP
