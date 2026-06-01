# Fault-Tolerance Configuration

FT configuration lives in the top-level `fault_tolerance` JSON block. If the
block is missing, FT is disabled.

## Parsed Fields

`include/utils/Configuration.h` defines these defaults:

```text
enabled = false
mode = transparent_migration
control_backend = Redis
control_host = 127.0.0.1
control_port = 6379
heartbeat_ms = 1000
lease_ms = 10000
safe_point_only = true
preferred_data_backend = ""
images_dir = /tmp/fmi-criu-images
poll_ms = 100
quiesce_timeout_ms = 10000
host_id = ""
```

The mode parser accepts only:

- `transparent_migration`
- `criu_coordinated`

Older documentation may mention `safe_point_restart`; that string is not
accepted by the current parser.

## Transparent Migration Example

```json
{
  "fault_tolerance": {
    "enabled": true,
    "mode": "transparent_migration",
    "control_backend": "Redis",
    "control_host": "127.0.0.1",
    "control_port": 6379,
    "heartbeat_ms": 25,
    "lease_ms": 250,
    "safe_point_only": true,
    "preferred_data_backend": "Direct"
  }
}
```

`safe_point_only` is parsed but not currently used by
`TransparentMigrationRuntime`. Operation-boundary checking happens through
`OperationGuard` on every communicator call.

## CRIU Example

```json
{
  "fault_tolerance": {
    "enabled": true,
    "mode": "criu_coordinated",
    "control_backend": "Redis",
    "control_host": "127.0.0.1",
    "control_port": 6379,
    "preferred_data_backend": "Direct",
    "images_dir": "/tmp/fmi-criu-images",
    "poll_ms": 25,
    "quiesce_timeout_ms": 2000,
    "host_id": "optional-stable-host-id"
  }
}
```

If `host_id` is empty, CRIU runtime and supervisor use `gethostname()`.

## Backend Validation

For all FT modes, if `preferred_data_backend` is non-empty, the communicator
constructor verifies that the named backend is active in `backends`.

For `criu_coordinated`, validation is stricter:

- `Direct` must be active
- no other data backend may be active
- `preferred_data_backend` must be empty or `Direct`

Redis control-plane support is separate from the Redis data backend. A config
can disable the `Redis` data backend and still use Redis for FT control.

