# Fault Tolerance Design Notes

This folder documents the fault-tolerance implementation in the current branch.
It is intentionally implementation-facing: the goal is to describe what the code
does today, which invariants it relies on, and where the boundaries are.

## Current FT Model

FMI has one implemented fault-tolerance protocol, transparent migration, with a
mechanism toggle for application-state continuity:

- `transparent_migration`: a plain `FMI::Communicator` is made migration-aware.
  FMI checks Redis at operation boundaries, lets a replacement rank join the
  orchestrator-promoted epoch, and rebuilds surviving communicators under an
  epoch-qualified transport name.
- `fault_tolerance.state_transfer` selects what happens to the migrated rank's
  in-memory state: `"none"` (default) exits the rank and a fresh replacement
  recomputes; `"criu"` (same-host v1, `FMI_ENABLE_CRIU=ON`) checkpoints and
  restores the rank's process image so memory survives, driven by the host-local
  rank agent.

The old cooperative `Session` / `safe_point()` model is not part of the current
code path. The `Event` enum still exists in `include/ft/Common.h`, and some old
docs and runbooks still mention `FTSession`, but no `Session` class or
`safe_point()` API is exposed by the current headers or Python binding.

## File Map

- [control-plane.md](control-plane.md): Redis key layout, rank state, epoch
  promotion, CRIU state, and cleanup behavior.
- [epochs.md](epochs.md): how communicator epochs work and why every
  backend-visible name is epoch-qualified.
- [transparent-migration.md](transparent-migration.md): construction,
  centralized promotion, operation-boundary behavior, reconfiguration, and the
  CRIU single-rank state-transfer mechanism.
- [configuration.md](configuration.md): JSON fields parsed by the current
  implementation and mode-specific validation.
- [api-surface.md](api-surface.md): C++, Python, and CLI entry points.
- [limits-and-failure-modes.md](limits-and-failure-modes.md): current
  assumptions, limitations, and known sharp edges.

## Primary Implementation Entry Points

- `include/Communicator.h` and `src/Communicator.cpp`
- `include/ft/Common.h`
- `include/ft/ControlPlane.h` and `src/ft/ControlPlane.cpp`
- `include/ft/TransparentMigrationRuntime.h` and
  `src/ft/TransparentMigrationRuntime.cpp`
- `include/ft/experimental/LocalRankAgent.h` and
  `src/ft/experimental/LocalRankAgent.cpp`
- `tools/rank_agent.cpp`
- `python/PythonFT.cpp` and `python/fmi_python.cpp`
