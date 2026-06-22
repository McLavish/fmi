# Fault-Tolerance Configuration

FT configuration lives in the top-level `fault_tolerance` JSON block. If the
block is missing, FT is disabled.

## Parsed Fields

`include/utils/Configuration.h` defines these defaults:

```text
enabled = false
control_backend = Redis
control_host = 127.0.0.1
control_port = 6379
poll_interval_ms = 1000
preferred_data_backend = ""
state_transfer = none
criu.images_dir = /tmp/fmi-criu-images   # nested under "criu"
criu.poll_ms = 100
criu.quiesce_timeout_ms = 10000
criu.host_id = ""
```

There is no FT `mode` selector. FT is turned on with `enabled`; the single
transparent-migration protocol is always used. `state_transfer` selects how the
migrated rank's application state is handled and accepts only:

- `none` (default) — the rank exits and a fresh replacement recomputes
- `criu` — the rank's process image is checkpointed/restored (needs
  `FMI_ENABLE_CRIU=ON` and a `Direct` data plane)

Older documentation may mention a `mode` field or `safe_point_restart`; neither
is read by the current parser.

## Transparent Migration Example

```json
{
  "fault_tolerance": {
    "enabled": true,
    "control_backend": "Redis",
    "control_host": "127.0.0.1",
    "control_port": 6379,
    "poll_interval_ms": 25,
    "preferred_data_backend": "Direct",
    "state_transfer": "none"
  }
}
```

`poll_interval_ms` controls how often a worker polls Redis while waiting for
orchestrator promotion. There is no library-side deadline on that wait — it is
unbounded by design (see `limits-and-failure-modes.md`); the orchestrator owns
detecting and resolving a migration that never completes.

## CRIU Example

```json
{
  "fault_tolerance": {
    "enabled": true,
    "state_transfer": "criu",
    "control_backend": "Redis",
    "control_host": "127.0.0.1",
    "control_port": 6379,
    "preferred_data_backend": "Direct",
    "criu": {
      "images_dir": "/tmp/fmi-criu-images",
      "poll_ms": 25,
      "quiesce_timeout_ms": 2000,
      "host_id": "optional-stable-host-id"
    }
  }
}
```

If `host_id` is empty, the migration runtime and rank agent use `gethostname()`.

## Backend Validation

For any FT config, if `preferred_data_backend` is non-empty, the communicator
constructor verifies that the named backend is active in `backends`.

For `state_transfer = "criu"`, validation is stricter:

- `Direct` must be active
- no other data backend may be active
- `preferred_data_backend` must be empty or `Direct`

Redis control-plane support is separate from the Redis data backend. A config
can disable the `Redis` data backend and still use Redis for FT control.
