# The redesign in this library (branch `redesign`)

On 2026-11-11 the migration system was designed again. The design record is `runtime/DESIGN.md`
in [fmi-spot-migration](https://github.com/McLavish/fmi-spot-migration); this file says what
that means for the library.

**The first system's documents are unchanged here.** `README.md`, `TODO.md`, `CLAUDE.md`,
`docs/design/` and the TLA+ models in `docs/tla/` are as they were before the redesign (the tag
`pre-redesign`) and describe the library of the first system. Part of the code they describe is
no longer on this branch; it is at the tag. (The one exception is a three-line pointer at the
top of `CLAUDE.md`.)

## What the library has on this branch

- **One migration mechanism, Local Drain**, in a new channel, `LocalDrain`
  (`include/comm/LocalDrain.h`). A rank that has to leave its host closes its connections and
  reads them to EOF, so that every byte in flight is in process memory. It then stops without
  a socket, is checkpointed from outside, and continues on another host, where its peers
  reconnect. The application makes no checkpoint call.
- **`DirectTCP` is the same wire without any of this**: the baseline of the overhead
  experiments. It builds its link name only when a socket is missing.
- **Gone from the code**: Retain-and-Replay (the sequenced links that retained messages and
  replayed them on a rebuilt connection, `framed` and `recover_links`), the first Local Drain
  channel (`DrainTCP`, coordinated through Redis, with its trigger thread), and DirectTCP's
  relocation logic. `README.md`, `docs/design/` and `docs/tla/` describe them.
- Local Drain moves a rank that was told to leave. It does not reconstruct a lost process: a
  rank that dies ends the job. The host manager and the coordinator that drive a move are part
  of fmi-spot-migration.
- The build configuration that `README.md` gives now builds the Redis, DirectTCP and
  LocalDrain backends (the README says DrainTCP, which is gone).

## The redesign's documents

| Document | What it holds |
|---|---|
| `REDESIGN.md` | This file |
| `CLAUDE-redesign.md` | The developer guide for this branch |
| `TODO-redesign.md` | Open work: what is not measured yet, defects, performance, structure |
| `runtime/DESIGN.md` in fmi-spot-migration | The design: the control link and its messages, the rules of rank state, what a rank and its peers do when a rank leaves |
| `benchmarks/overhead/RESULTS-redesign.md` in fmi-spot-migration | What `LocalDrain` costs against `DirectTCP` while nothing moves (2026-11-11) |
