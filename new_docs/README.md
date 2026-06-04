# Fault Tolerance Design Notes

This folder documents the fault-tolerance implementation in the current branch.
It is intentionally implementation-facing: the goal is to describe what the code
does today, which invariants it relies on, and where the boundaries are.

## Current FT Modes

FMI has two implemented fault-tolerance modes:

- `transparent_migration`: a plain `FMI::Communicator` is made migration-aware.
  FMI checks Redis at operation boundaries, exits the migrating rank, lets a
  replacement rank join the orchestrator-promoted epoch, and rebuilds surviving
  communicators under an epoch-qualified transport name.
- `criu_coordinated`: a plain `FMI::Communicator` is paired with a CRIU runtime.
  FMI drains active operations, closes Direct sockets, reports quiescence to
  Redis, and blocks until an external supervisor restores a checkpoint
  generation.

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
  centralized promotion, operation-boundary behavior, and reconfiguration.
- [criu-coordinated.md](criu-coordinated.md): runtime/supervisor behavior,
  checkpoint generations, and same-host Direct-only constraints.
- [configuration.md](configuration.md): JSON fields parsed by the current
  implementation and mode-specific validation.
- [api-surface.md](api-surface.md): C++, Python, and CLI entry points.
- [limits-and-failure-modes.md](limits-and-failure-modes.md): current
  assumptions, limitations, and known sharp edges.

## Primary Implementation Entry Points

- `include/Communicator.h` and `src/Communicator.cpp`
- `include/ft/Common.h`
- `include/ft/Coordinator.h` and `src/ft/Coordinator.cpp`
- `include/ft/TransparentMigrationRuntime.h` and
  `src/ft/TransparentMigrationRuntime.cpp`
- `include/ft/CriuRuntime.h` and `src/ft/CriuRuntime.cpp`
- `include/ft/CriuSupervisor.h` and `src/ft/CriuSupervisor.cpp`
- `tools/criu_supervisor.cpp`
- `python/PythonFT.cpp` and `python/fmi_python.cpp`
