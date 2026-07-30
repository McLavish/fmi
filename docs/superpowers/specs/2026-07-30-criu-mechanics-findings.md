# CRIU checkpoint mechanics — measured findings

**Status:** experimental results, not design. Every line below was observed on this host.
**Relates to:** normative contract 4 in `2026-07-27-sequenced-incarnation-links-design.md`.

## Environment

criu **4.2**, `/usr/local/sbin/criu`, carrying
`cap_net_admin,cap_sys_ptrace,cap_sys_admin,cap_sys_resource,cap_checkpoint_restore=eip`.
Run as uid 1000 — rootless, and therefore requiring `--unprivileged` on every invocation
(`criu check` refuses without it). Kernel 7.0.0-28-generic, Ubuntu 24.04.

Subject: a ~60-line non-FMI program that increments a counter to a file, optionally holding a
mutex on a second thread and an established loopback TCP connection with both ends inside the
process. Deliberately not linked against FMI, so nothing here depends on the library.

## Results

| # | Question | Result |
|---|---|---|
| 1 | Does rootless dump/restore work at all? | **Yes**, dump rc=0, restore rc=0, with `--unprivileged` |
| 2 | Does `--leave-stopped` permit same-host restore? | **No** — see below |
| 3 | Two threads with a mutex held across the freeze? | **Works** — both threads restored, execution continues |
| 4 | Is `--tcp-close` required at dump? | **Yes** for an established connection |
| 5 | Is it required again at restore? | **Yes** — criu 4.2 records it in the image |
| 6 | Can the same process be migrated twice? | **Yes** — two full cycles, state continuous |

### 2. `--leave-stopped` is incompatible with same-host restore

```
criu dump --unprivileged -t <pid> --leave-stopped     rc=0
/proc/<pid>/stat state                                 T          (stopped, pid still held)
criu restore --unprivileged                            rc=1
  Error (criu/cr-restore.c:1230): Can't fork for <pid>: File exists
  Error (criu/cr-restore.c:2324): Restoring FAILED.
kill -9 <pid>; criu restore --unprivileged            rc=0        (same pid reclaimed)
```

This confirms the discipline `src/ft/experimental/LocalRankAgent.cpp:417` documents, and it
settles the design's open question: **frozen-original abort recovery is cross-host only.**
On the same host the original must be killed before its replacement can be restored, so the
"resume the original in place" path cannot exist for `migrate`/`migrate-local` without a pid
namespace or pid remapping.

### 3. Two-thread dump with a lock held

Dump and restore both rc=0 with a second thread blocked forever holding a `std::mutex` taken
before the freeze. The restored process has both threads and the counter keeps advancing
(79 → 99 across a one-second observation). This is the mechanism residual limitation 1 depends
on: the progress engine adds a background thread per rank, and CRIU must carry both.

### 4–5. `--tcp-close` on both legs

```
dump, no TCP flag        rc=1  inet: Connected TCP socket, consider using --tcp-established
dump --tcp-close         rc=0
restore, no TCP flag     rc=1  Error (criu/image.c:94): Need to set the --tcp-close options.
restore --tcp-close      rc=0
established connections after restore: 0
```

The flag is recorded in the image and demanded again at restore, exactly as the contract
states. The restored process resumes (counter 320 → 340) with its connections **gone**, which
is precisely the semantics the link layer is built to absorb: everything unacknowledged is
replayed from peer retention.

### 6. Repeated migration

Two complete dump/restore cycles of a process holding both a second thread and an established
TCP connection. The counter runs 39 → 79 → 119 → 139 without a gap and the thread count stays
at 2. Migration is therefore repeatable, not a one-shot.

## What these results do NOT establish

- **`PR_SET_PTRACER` survival is untested here, and is masked on this host.** criu carries
  `cap_sys_ptrace`, so a second dump succeeds whether or not the prctl survived restore. On a
  host relying on the prctl instead of the capability, "migrated once and never again" remains
  an open risk.
- **No FMI process was checkpointed.** The subject is deliberately a toy; nothing here says the
  progress engine, a hiredis connection, or a mid-`pair()` rank survives a freeze.
- **Freeze duration versus RSS was not measured**, so the design's W-sizing feedback loop
  (bigger window → bigger image → longer dump) is still unquantified.
- Single host. Nothing about cross-host staging, image transfer, or the restore lease.
