# FMI Fault Tolerance With TCP Data Plane and Redis Coordination

## Summary

Implement fault tolerance for FMI as coordinated rank migration across communicator epochs, not transparent live process migration. The default data plane should be `Direct`/TCP, while the first FT coordination plane should be Redis-backed.

Chosen semantics:

- logical rank IDs are preserved across migration
- migration is triggered externally
- cutover happens only at explicit safe points
- in-flight collectives are not preserved
- applications recover from explicit checkpoints
- `Redis` is used for FT membership, epochs, leases, and migration state
- `Direct` is the preferred/default FMI transport for rank-to-rank communication
- `Redis` remains available as an alternate data backend, but not the default

## Key Changes

### 1. Split fault tolerance into two planes

Keep FMI data transport and FT coordination separate.

- Data plane: existing FMI backends, with `Direct` as the default/preferred backend in FT-enabled deployments.
- Coordination plane: new Redis-backed FT metadata service that tracks communicator epoch, logical membership, worker-instance mapping, and migration state.

This avoids trying to build a distributed membership service on top of ad hoc TCP connections in v1.

### 2. Add an FT session wrapper

Introduce `FMI::FT::Session` above `Communicator`.

Public interface:

- `Communicator& comm()`
- `Event safe_point()`
- `std::uint64_t epoch() const`

New event type:

- `None`
- `MigrateSelf`
- `Reconfigured`

Behavior:

- application code uses `Session::comm()` for all FMI calls
- application calls `safe_point()` at checkpoint boundaries
- if the current logical rank is marked for migration, `safe_point()` returns `MigrateSelf`
- the replacement worker restores app state, rejoins with the same logical rank, and the session rebuilds the communicator for epoch `N+1`
- other ranks observe `Reconfigured` once and continue with the rebuilt communicator

### 3. Make TCP the preferred data backend

Add FT-aware backend preference to configuration and channel selection.

Config additions:

- `fault_tolerance.enabled`
- `fault_tolerance.control_backend = "Redis"`
- `fault_tolerance.control_host`
- `fault_tolerance.control_port`
- `fault_tolerance.heartbeat_ms`
- `fault_tolerance.lease_ms`
- `fault_tolerance.safe_point_only = true`
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

### 5. Define the migration state machine

Use Redis to manage these states per logical rank:

- `ACTIVE`
- `MIGRATION_PENDING`
- `QUIESCED`
- `REPLACED`

Flow:

1. external daemon marks logical rank `r` as `MIGRATION_PENDING`
2. all ranks reach `safe_point()` before starting the next communication phase
3. rank `r` checkpoints application state, unregisters its old worker instance, and exits
4. replacement worker starts with the same logical rank `r`, restores checkpointed state, and registers into epoch `N+1`
5. once all logical ranks are present for epoch `N+1`, all sessions rebuild their communicator and resume

V1 defers any migration request that arrives while a rank is inside an FMI operation until the next safe point.

## Test Plan

### Unit and component tests

- Redis control-plane membership registration, heartbeat, lease expiry, and epoch advancement
- `safe_point()` returns `MigrateSelf` only for the targeted logical rank
- non-target ranks receive `Reconfigured` exactly once per epoch transition
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
- TCP means FMI `Direct` backend for rank-to-rank communication
- Redis is accepted for the first FT control plane even though the default data backend is TCP
