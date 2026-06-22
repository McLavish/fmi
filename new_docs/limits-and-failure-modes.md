# Limits And Failure Modes

This page records important current constraints. These are not all bugs; many
are deliberate v1 design boundaries.

## General

- Redis is required for FT control.
- `FMI_ENABLE_REDIS` must be enabled at build time for control-plane operations.
- FT metadata is scoped by base `comm_name`; concurrent unrelated jobs must use
  distinct communicator names.
- ControlPlane commands use one persistent Redis connection per control plane and
  argv-form command calls. Access to that connection is serialized.
- The current threading assumption is one operation at a time per
  communicator at the channel/runtime level.
- World-size mismatches are rejected if existing Redis metadata records a
  different `world_size`.

## Transparent Migration

- This mode is orchestrator-driven migration and communication
  reconfiguration, not crash-recovery fault tolerance.
- The application is not notified before the migrating process exits.
- The migrating rank exits with `std::exit(0)` when it reaches an operation
  boundary and sees itself pending.
- Application memory is not checkpointed or restored by this mode.
- The application or orchestrator must realign operation steps after relaunch.
- In-flight FMI operations are not migrated. Checks happen before operations
  start, not while a channel operation is running.
- A rank blocked inside a channel receive or collective cannot migrate until it
  returns to an operation boundary.
- Replacement construction can block until epoch promotion.
- Promotion waiting times out after `reconfigure_timeout_ms`.
- `promote_epoch()` is compare-and-set guarded and only advances the epoch.
- There is no automatic failure detection or liveness-triggered recovery.
- Old transport objects can remain in Redis/S3 until cleanup/finalize, but
  epoch naming prevents new-epoch operations from matching them.

## CRIU-Coordinated

- CRIU support is C++ only in the current bindings.
- The v1 rank agent supports same-host jobs only.
- Only the Direct data backend is supported.
- Redis data backend may be disabled, but Redis control-plane service is still
  required.
- CRIU rank metadata does not currently expire stale ranks by heartbeat time.
- The rank agent `criu dump`s the target (reaping its pid) and then
  `criu restore`s it; a restore failure after the dump is unrecoverable.
- Successful checkpoint/restore requires a working `criu` executable and the
  kernel/container privileges CRIU needs. Tests use a mock `criu` binary for
  rank agent behavior.

## Operational Guidance

- Use stable, unique `comm_name` values per job.
- In transparent migration, pass explicit worker IDs from the orchestrator.
- Keep `reconfigure_timeout_ms` comfortably above expected Redis and
  scheduling jitter.
- Keep `poll_interval_ms` low enough for the desired migration responsiveness
  without creating excessive control-plane traffic.
- Clear Redis job state between manual test runs if reusing a communicator
  name.
- For CRIU, set a stable `host_id` in config when hostname identity could
  change across containers or rank agent processes.
