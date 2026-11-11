# Protocol design

FMI has one migration mechanism, **Local Drain**: a rank that has to leave its
host empties its sockets into process memory, stops without a socket, and
continues on another host. The channel is `LocalDrain`
(`include/comm/LocalDrain.h`). Store recovery uses the values already held in
Redis or S3; its configuration is described in the
[developer guide](../../CLAUDE.md).

| Document | Read it for |
|---|---|
| `runtime/DESIGN.md` in [fmi-spot-migration](https://github.com/McLavish/fmi-spot-migration) | The design: the control link and its messages, the rules of rank state, what a rank and its peers do when it leaves, the host manager and the coordinator |
| [CRIU experiments](2026-07-30-criu-mechanics-findings.md) | The initial process and socket experiments, with their scope |

Until 2026-11-11 the library had a second mechanism, Retain-and-Replay
(sequenced links that retained messages and replayed them on a rebuilt
connection), and an earlier Local Drain channel, `DrainTCP`, coordinated through
Redis. Both were removed. Their code, tests, design documents and TLA+ models are
at the tag `pre-redesign`. [TODO.md](../../TODO.md) tracks what is open.

Runnable campaigns and measured overheads live in fmi-spot-migration.
