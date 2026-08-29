# Design documents

The design contracts behind the library's checkpoint and migration support, in the order they
were written. They are records: each describes the mechanism as designed at the time, and the
code and `CLAUDE.md` are authoritative where they have since moved on.

- `2026-07-27-sequenced-incarnation-links-design.md` — the sequenced link layer: message
  identity, incarnations, retain-and-replay. Machine-checked models in `../tla/`.
- `2026-07-30-criu-mechanics-findings.md` — what criu does to a process with open sockets, measured.
- `2026-07-30-sequenced-links-implementation.md` — what was built against the design, and the
  rules (R1, …) the implementation settled on.
- `2026-08-11-neighborhood-drain-protocol.md` — `DrainTCP`, `MigrationTrigger`,
  `DrainCoordinator`: the socketless migration.

Paths under `runbooks/` and `example_programs/` in these documents refer to directories that
have since moved to the `fmi-spot-migration` repository (`benchmarks/migration/`, `apps/`,
`runtime/protocol/`).
