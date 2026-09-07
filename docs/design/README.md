# Protocol design

FMI supports two TCP migration mechanisms. **Retain-and-Replay** reconstructs
broken connections from retained messages. **Local Drain** empties sockets into
process memory before a coordinated checkpoint. Store recovery uses the values
already held in Redis or S3; its configuration is described in the
[developer guide](../../CLAUDE.md).

| Document | Read it for |
|---|---|
| [Sequenced-link design](2026-07-27-sequenced-incarnation-links-design.md) | Message identity, retention, acknowledgment, and the limits of the original proposal |
| [Sequenced-link implementation](2026-07-30-sequenced-links-implementation.md) | Rules the socket implementation must preserve |
| [CRIU experiments](2026-07-30-criu-mechanics-findings.md) | The initial process and socket experiments, with their scope |
| [Local Drain](2026-08-11-neighborhood-drain-protocol.md) | Draining, migration control, reconnect, and failure handling |
| [TLA+ models](../tla/README.md) | Recorded model-checking results and what they do not establish |

The dated filenames preserve existing references. Superseded implementation plans
have been removed from the text. These documents describe the current mechanism
or explicitly identify historical experiments; [TODO.md](../../TODO.md) tracks
known implementation gaps.

Runnable campaigns and measured overheads live in
[fmi-spot-migration](https://github.com/McLavish/fmi-spot-migration), under
`benchmarks/migration/` and `benchmarks/overhead/`. Older references to this
library's `runbooks/` and `example_programs/` refer to content now maintained there.
