# Limits And Failure Modes

This page records important current constraints. These are not all bugs; many
are deliberate v1 design boundaries.

## General

- Redis is required for FT control.
- `FMI_ENABLE_REDIS` must be enabled at build time for coordinator operations.
- FT metadata is scoped by base `comm_name`; concurrent unrelated jobs must use
  distinct communicator names.
- Redis commands are issued through short-lived connections and command strings.
  There are no Redis transactions or Lua scripts around multi-key state
  changes.
- World-size mismatches are rejected if existing Redis metadata records a
  different `world_size`.

## Transparent Migration

- The application is not notified before the migrating process exits.
- The migrating rank exits with `std::exit(0)` when it reaches an operation
  boundary and sees itself pending.
- Application memory is not checkpointed or restored by this mode.
- In-flight FMI operations are not migrated. Checks happen before operations
  start, not while a channel operation is running.
- Replacement construction can block until epoch promotion.
- Epoch promotion times out after `lease_ms`.
- `promote_epoch()` is idempotent but not compare-and-set protected.
- Old transport objects can remain in Redis/S3 until cleanup/finalize, but epoch
  naming prevents new-epoch operations from matching them.
- `safe_point_only` is parsed but does not currently change runtime behavior.

## CRIU-Coordinated

- CRIU support is C++ only in the current bindings.
- The v1 supervisor supports same-host jobs only.
- Only the Direct data backend is supported.
- Redis data backend may be disabled, but Redis control-plane service is still
  required.
- CRIU rank metadata does not currently expire stale ranks by heartbeat time.
- `CriuSupervisor::restore()` kills registered old PIDs greater than zero before
  restoring.
- Successful checkpoint/restore requires a working `criu` executable and the
  kernel/container privileges CRIU needs. Tests use a mock `criu` binary for
  supervisor behavior.

## Operational Guidance

- Use stable, unique `comm_name` values per job.
- In transparent migration, pass explicit worker IDs from the orchestrator.
- Keep `lease_ms` comfortably above expected Redis and scheduling jitter.
- Clear Redis job state between manual test runs if reusing a communicator name.
- For CRIU, set a stable `host_id` in config when hostname identity could change
  across containers or supervisor processes.

