# Migrating a rank with the neighborhood drain: a real CRIU move, socketless

This runbook takes one rank of a **running, unmodified FMI application**, tells it to migrate,
and moves it with `criu` — with **no `--tcp-close` on either leg**. That flag is the whole
point of the exercise. The incumbent transparent-checkpoint story
(`../criu-transparent-checkpoint/`) freezes a rank with its TCP connections inside the image
and needs `--tcp-close` to drop them on restore; the neighborhood-drain protocol
(`DrainTCP` + `MigrationTrigger` + `DrainCoordinator`, described in the CLAUDE.md paragraph on
`DrainTCP`) instead quiesces the rank's links, half-closes them, and pulls every in-flight byte
into user memory *before* the freeze, so that the process owns **zero sockets** when it stops.
A criu image of such a process is host-agnostic and needs no connection-repair flag at all —
and a dump that criu accepts without `--tcp-close` **is** the assertion, measured rather than
argued.

The application does not participate. `drain_driver.py` runs the same
`fmi_checkpoint_subject` and the same app shapes as the criu-transparent-checkpoint runbook,
over a config that enables `DrainTCP` and nothing else. There is no checkpoint API in the
subject, no migration hook, no annotation; it never learns it was moved.

## The acceptance criterion

```
criu dump    --unprivileged -t PID -D img -v4 -o dump.log     # no --tcp-close, no --tcp-established
criu restore --unprivileged        -D img -d -v4 -o restore.log
```

A trial passes only if **all** of these hold:

1. `criu dump` returns 0 without being told about sockets;
2. `dump.log` contains no `Connected TCP socket`, no `External socket is used`, no `inetsk`;
3. the image contains no socket image file (`inetsk.img`, `unixsk.img`, `tcp-stream.img`,
   `sk-queues.img`, …) — `crit show img/files.img` lists `REG` and `PIPE` entries and nothing
   else;
4. `criu restore` returns 0, again with no socket flag;
5. every rank reaches `DONE` with the checksum a clean baseline of the same shape produced;
6. the migrated rank logs a round strictly past the one it was frozen on.

A failure of 1–4 is a **FAIL of the protocol**, not a criu skip: a socket in the image means
the drain did not do what it claims. `drain_driver.py` distinguishes the two anyway — a dump
that fails with *no* socket evidence in its log is reported as
`FAIL (criu itself, no socket evidence — triage the environment)` and counted separately in
the summary line, because that is an environment question and the other is a protocol verdict.

**Two strings that look like evidence and are not**, both tried here and both of which scored a
perfectly clean migration as a protocol failure:

- `unix: Dumping external sockets` — an unconditional stage header every criu dump prints.
- `inet: Collected …` — criu enumerates the **whole host's** TCP table out of `/proc/net/tcp`
  before it looks at the task. A clean drained dump on this machine carries 29 such lines,
  naming sshd and the Redis server. On criu 3.19 `grep -c inetsk dump.log` is therefore **0
  even for a rank that is full of sockets** — 3.19 tags those lines `inet:`, not `inetsk`.
  The strings that do discriminate are the *error* criu fails the dump with, and the image
  contents.

## Requirements

- `criu` (verified on 3.19) usable rootless — file caps
  `cap_sys_ptrace,cap_checkpoint_restore=eip`, and `--unprivileged` on both legs
- a Redis on `127.0.0.1:6379` — the DrainTCP peer registry **and** the drain coordinator
- a Redis-enabled build of this repo, which builds the subject:

```bash
cmake -S . -B build -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_TCPUNCH=OFF -DFMI_BUILD_TESTS=ON
cmake --build build -j"$(nproc)"
```

- `python3` (no third-party packages; `sigqueue` is reached through `ctypes` because `os.kill`
  cannot carry the epoch payload) and `redis-cli`
- optional: `crit`, for reading the image directly. It is **not** part of the `criu` package on
  Rocky 9 (`criu-3.19-5.el9` ships no `crit`), so it was assembled here without root from
  `crit`, `python3-criu` and `python3-protobuf` rpms unpacked into a temp prefix; see
  "Reading the image with crit" below.

## `fmi_drain.json`

Mirrors `../criu-transparent-checkpoint/fmi.json` — loopback registry, `max_timeout` 60000,
`127.0.0.1` for both `bind_host` and `advertise_host` — and enables **`DrainTCP` and nothing
else**. That is not tidiness: with a second backend enabled the cost model routes every
operation this subject issues to whichever is cheapest, and a migration sweep against such a
config would exercise no `DrainTCP` code at all and report green. The drain block on top:

```jsonc
"drain": true,                  // arms MigrationTrigger; without it the signal kills the rank
"trigger": "both",              // signal (the rank a driver addresses) + control (every peer
                                //   learns from the stream to half-close; without it the
                                //   migrator's drain never reaches EOF)
"drain_signal_offset": 3,       // SIGRTMIN+3, resolved at run time — glibc offsets SIGRTMIN
"drain_grace_ms": 5000,
"migration_max_ms": 120000,     // what a survivor waits instead of max_timeout while a peer
                                //   migrates; generous, since a dump+restore is the bound
"batch_lease_ms": 120000,
"control_poll_interval_ms": 20
```

`drain_rehearsal_only` is **absent on purpose** (default false). With it on, the migrator
sequence ends in an in-place restore instead of `SIGSTOP` and there would be nothing for criu
to dump; the driver refuses such a config at startup rather than reporting a hollow pass.

## One migration by hand

```bash
cd runbooks/drain-migration
SUBJECT=../../build/runbooks/criu-transparent-checkpoint/fmi_checkpoint_subject
COMM=hand-$$
redis-cli DEL "fmi:drain:$COMM" "fmi:drain:$COMM:members" \
              "fmi:drain:$COMM:events" "fmi:drain:$COMM:batch"

for r in 0 1; do
    setsid "$SUBJECT" $r 2 fmi_drain.json "$COMM" 20000 1 100 1 --shape baseline \
        < /dev/null > /tmp/rank$r.log 2>&1 &
done
sleep 3

PID=$(pgrep -f "fmi_checkpoint_subject 1 2 .* $COMM ")
SIG=$(python3 -c "import signal; print(int(signal.SIGRTMIN) + 3)")     # 37 here
python3 - "$PID" "$SIG" 0 <<'EOF'
import ctypes, os, sys
class sigval(ctypes.Union):
    _fields_ = [("sival_int", ctypes.c_int), ("sival_ptr", ctypes.c_void_p)]
libc = ctypes.CDLL("libc.so.6", use_errno=True)
libc.sigqueue.argtypes = [ctypes.c_int, ctypes.c_int, sigval]
pid, sig, epoch = (int(a) for a in sys.argv[1:4])
if libc.sigqueue(pid, sig, sigval(sival_int=epoch)):
    sys.exit(os.strerror(ctypes.get_errno()))
EOF

redis-cli XRANGE "fmi:drain:$COMM:events" - +        # leaving, then sealed
awk '{print $3}' /proc/$PID/stat                     # T
ls -l /proc/$PID/fd                                  # 0,1,2 files + one pipe pair. No sockets.

mkdir -p /tmp/img
criu dump    --unprivileged -t "$PID" -D /tmp/img -v4 -o dump.log     # NO --tcp-close
criu restore --unprivileged           -D /tmp/img -d -v4 -o restore.log
kill -CONT "$PID"                                    # see "the restored rank is stopped"

redis-cli XRANGE "fmi:drain:$COMM:events" - +        # ... restored, epoch 1, incarnation 1
tail -2 /tmp/rank0.log /tmp/rank1.log
```

The `sigqueue` payload is the target's **current epoch** — its restore count, 0 for a rank that
has never been migrated. `MigrationTrigger` drops a request whose epoch is behind the process's
own, so that a request queued before a restore cannot be acted on after it; `os.kill` cannot
carry that value, which is why the snippet is not one line of shell.

## The driver

```bash
python3 drain_driver.py --peers 2 --trials 3 --rounds 20000 --ms 1 --seed 1
python3 drain_driver.py --peers 4 --trials 8 --rounds 20000 --ms 1 --seed 11
python3 drain_driver.py --peers 4 --trials 3 --rounds 20000 --ms 1 --seed 3 \
        --shape mixed_p2p_collective
python3 drain_driver.py --peers 2 --trials 2 --rounds 30000 --ms 1 --seed 5 --migrations 2

# pin the binary under test — see "the one failure"
FMI_CHECKPOINT_SUBJECT=/tmp/fmi-pristine/build/runbooks/criu-transparent-checkpoint/fmi_checkpoint_subject \
    python3 drain_driver.py --peers 4 --trials 6 --seed 7
```

It takes a clean baseline per rank count first, then per trial: launches the job with
`sweep.py`'s launch hygiene *verbatim* (stdin from `/dev/null`, **no `env=`**, `setsid`, stdout
to a regular file, `--shape` last), waits until every rank is past the warm-up round, picks a
target (`--target` to fix it), `sigqueue`s it, waits for **both** its `sealed` event and
`/proc/<pid>/stat` state `T`, dumps, reaps, restores, continues, waits for `restored`, and
scores the finished job against the baseline. `--keep` keeps the images, rank logs and the
trial's whole event stream; every trial gets a unique `comm_name` and its four Redis keys are
deleted around it.

`--ms 1` matters. With `--ms 0` a 20000-round job at 2 ranks finishes in about six seconds and
the migration lands after the job is over — reported as a `SKIP`, which is the honest verdict
but proves nothing.

## Evidence

All single-host, this machine, criu 3.19 rootless, Redis on `127.0.0.1:6379`, `--rounds 20000
--ms 1` unless stated. **43 real `criu dump`/`criu restore` migrations over 40 driver trials
plus one by hand; every dump and every restore returned 0 with no `--tcp-close` and no
`--shell-job`, and no image contained a socket. 42 of the 43 ended in a job that finished with
the baseline checksums** — the one exception is the reconnect failure below, whose dump and
restore were themselves clean.

The last block of nine was taken against a **pristine build of `HEAD`** in `/tmp` reached
through `FMI_CHECKPOINT_SUBJECT`, because this working tree was being edited and rebuilt by
another session while the campaign ran; see "the one failure".

| run | result |
| --- | --- |
| the arithmetic agrees with DirectTCP | `sweep.py --config ../../config/fmi_drain_tcp.json --max-checkpoints 0 --trials 2 --peers 2 4 --rounds 3000 --seed 1` → baselines `{0: 61531500, 1: 49798500}` at 2 peers and `{0: 85756500, 1: 58810500, 2: 62119500, 3: 65431500}` at 4, **identical** to the same invocation on `fmi.json` (DirectTCP). Both trials `SKIP`, which is what a 0-checkpoint sweep must report |
| 2 peers, `baseline`, `--trials 3 --seed 1` | **3 passed, 0 failed, 0 skipped**; targets rank 0, 1, 0 at rounds 625/300/700 |
| 2 peers, `baseline`, `--trials 5 --seed 23` | **5 passed, 0 failed, 0 skipped** |
| 2 peers, `baseline`, `--trials 2 --seed 5 --migrations 2 --rounds 30000` | **2 passed, 0 failed** — the same rank migrated **twice** in one job, the second request addressed at the epoch the first restore produced: `epoch=0 → restored epoch=1 incarnation=1`, then `epoch=1 → restored epoch=2 incarnation=2`. The link counters carry across both cuts (`sent.1` 16317 → 32844) |
| 4 peers, `baseline`, `--trials 8 --seed 11` | **8 passed, 0 failed, 0 skipped**; all four ranks used as targets across the eight trials |
| 4 peers, `baseline`, `--trials 3 --seed 7` | **2 passed, 1 failed** — the one failure of the campaign; see "the one failure" below. Re-run with the same seed: **3 passed, 0 failed** |
| 4 peers, `baseline`, `--trials 3 --seed 31` | **3 passed, 0 failed, 0 skipped** |
| 4 peers, `mixed_p2p_collective`, `--trials 3 --seed 3` | **3 passed, 0 failed, 0 skipped**; targets rank 1, 3, 3 |
| **pristine build of `HEAD`**, 4 peers, `--trials 6 --seed 7` | **6 passed, 0 failed, 0 skipped** — including the trial that failed above, same seed, same target (rank 1), same round (625) |
| **pristine build of `HEAD`**, 2 peers, `--trials 3 --seed 1` | **3 passed, 0 failed, 0 skipped** |
| by hand, 2 ranks, rank 1 frozen at round 8300 of 20000 | dump rc=0, restore rc=0, `restored` at epoch 1 / incarnation 1; both ranks `DONE` with the driver's own baseline checksums **2620210000 / 2201990000**; rank 1 logged rounds 8400 through 20000 after its restore. The freeze itself was 32.8 s of wall clock (my typing), and the job absorbed it |
| what the migrating rank holds when criu looks at it | `ls -l /proc/PID/fd`: `0 -> /dev/null`, `1,2 -> r1.log`, `4,5 -> pipe:[…]`. **Five fds, zero sockets.** The pipe is the control thread's self-pipe, both ends in the same process |
| the dump log | `grep -c inetsk dump.log` → **0**; `grep -ci "connected tcp socket"` → **0**. The only socket-shaped line is the stage header `(00.020309) unix: Dumping external sockets`, which every criu dump prints. `Dumping finished successfully` at `(00.020483)` |
| the image itself, read with `crit` | `crit show img0/files.img` → `{'REG': 14, 'PIPE': 2}` — **no socket entry of any kind**. `crit show img0/fdinfo-2.img` → `0 REG, 1 REG, 2 REG, 4 PIPE, 5 PIPE`. The image directory contains no `inetsk.img`, `unixsk.img`, `tcp-stream.img` or `sk-queues.img` at all |
| **the control: the same dump against an *undrained* rank** | same binary, same config, same criu invocation, on a rank that was never asked to migrate: 4 socket fds, and `criu dump` **fails rc=1** with `(00.003155) Error (criu/sk-inet.c:191): inet: Connected TCP socket, consider using --tcp-established option.` That is what this runbook's acceptance criterion is measuring the absence of |
| what a migration costs the job | 22.2 s clean at 2 peers vs 22.2–22.3 s with a migration; 22.8 s clean at 4 peers vs 22.9 s. Inside one rank: seal in ~1 ms (the peers' data paths see the FIN and half-close before the leave notice reaches them), dump 4.5–20 ms, restore 1.8–4 ms, `MigrationTrigger … is back at epoch 1 after 42 ms` — measured `begins a real migration` 15:43:17.131349 → `resumed at epoch 1, incarnation 1, listening on 127.0.0.1:44513` 15:43:17.172818, with `dump.log` written at .167 and `restore.log` at .171 |

### The one failure

4 peers, seed 7, trial 2: rank 1 was migrated at round 625. The dump and the restore were
clean (`dump rc=0 socket-lines=0`, `restore rc=0`, image socket-free), the `sealed`/`restored`
events were correct, and the rank came back at epoch 1 / incarnation 1 after 16 ms. It then
died on its first reconnect to peer 0:

```
DrainTCP: rank 1 <-> peer 0: stream counters disagree at reconnect
  (peer sent 13692, we received 13692; peer received 8476, we sent 8476)
```

**The four counters that message names are pairwise equal** — it reports a disagreement that
is not in its own numbers. The comparison it comes from is
`theirs.bytes_sent != mine.bytes_received || theirs.bytes_received != mine.bytes_sent`
(`src/comm/DrainTCP.cpp:546`), and the shipped object's disassembly reads the same four stack
slots for the comparison and for the message, so the message is not printing different state
from what was compared. Ranks 0, 2 and 3 then died in cascade, correctly, from the protocol's
own unplanned-death detector (`the connection died 5015 ms ago and no migration notice
followed`) once rank 1 was gone.

Not reproduced, and there is a specific reason to distrust the binary it happened in.
`src/comm/DrainTCP.cpp` and `tests/drain_migration.cpp` in this working tree were being edited
and rebuilt by **another session while the campaign ran** — `tests/drain_migration.cpp` was
carrying a case marked `TEMPORARY PROBE (verification only — revert)`, and `DrainTCP.cpp` was
rewritten twice (15:27:13 and 15:47:33) with content that matched `HEAD` again afterwards, each
time triggering a rebuild of the object the subject links. The failure landed at 15:26:39, in a
subject binary linked at 15:02 from whatever that file held then. Which is exactly the kind of
thing that produces an error message whose own numbers contradict it.

So the run was repeated against a **pristine `git archive HEAD` tree built in `/tmp`**, reached
through `FMI_CHECKPOINT_SUBJECT`: the same seed, the same rank, the same round — 6 passed, 0
failed, and 3 more at 2 peers on top. Taken together with 31 further migrations on the shared
tree across two shapes and a twice-migrated rank, **the failure is not reproducible against
`HEAD`.** The evidence is kept anyway, under `runs/run213767/fmidr213767-t2p4/` (rank logs,
`events.log`, `img0/dump.log`, `img0/restore.log`), because it is not *explained* — only
un-reproduced against a build we can name.

The lesson for anyone re-running this: **verify which binary the ranks are executing.** A
migration campaign is long enough for a working tree to change underneath it, and the subject
is relinked without the driver noticing. `FMI_CHECKPOINT_SUBJECT=/path/to/pristine/build/...`
is how to pin it.

## Wrinkles worth knowing

**The restored rank comes back stopped, and something outside has to continue it.** The
migrator sequence ends in `raise(SIGSTOP)`; criu records the task as `TASK_STOPPED` and
faithfully restores it that way, so `/proc/<pid>/stat` reads `T` after a successful
`criu restore -d`. Nothing inside the library can undo this — the thread that would call
`SIGCONT` is the stopped one. Whoever asked for the migration is who resumes it; the driver
does it and logs `SIGCONT (restored in group-stop, as expected)`. This is expected behaviour,
not a defect, but a driver that forgets it hangs forever with a healthy image.

**Wait for the event *and* the stop.** `sealed` is emitted at the end of step 5 of the migrator
sequence; the registry and coordinator connections are dropped, the heap is trimmed and stdio
is flushed *after* it, and only then does the process stop. Dumping on the event alone races
those three steps — and one of the things they do is close the last two sockets.

**Neither `--tcp-close` nor `--shell-job` is needed.** `--shell-job` is what the
criu-transparent-checkpoint runbook passes; with `setsid`, stdin from `/dev/null` and stdout to
a regular file, this driver's ranks need neither, and both legs were run without them for every
row above. Leaving `--tcp-close` off is the point; leaving `--shell-job` off just keeps the
invocation honest about what the image contains.

**criu restores into a time namespace.** The image directory carries `timens-0.img`, so
`CLOCK_MONOTONIC` is virtualised across the move. The library measures its own deadlines with
`steady_clock`; that is the right choice here, but it means a rank's `migration_max_ms` clock
and a driver's wall clock are not the same clock across a restore.

**`pgrep -f` matches the process that runs it.** A pattern containing the binary name and the
comm name appears in the command line of any shell that types it, so a by-hand
`pgrep -f "fmi_checkpoint_subject 1 .* $COMM "` from an interactive shell matches that shell.
`find_rank_pid` is sweep.py's and is safe because the pattern is an argv element of a
`subprocess.run(["pgrep", …])` — the driver's own command line does not contain it, and pgrep
never reports itself. Observed for real while writing this runbook.

**Clean all four Redis keys per run, especially the event stream.** A rank starts reading the
stream from the tail it captured at construction, so it is immune to a previous run's events —
but a *driver* reads from the beginning, and a leftover `sealed` under a reused `comm_name`
would satisfy its wait before the rank it just signalled has done anything. `sweep.py`'s
`clean_comm` gained a `DrainTCP` branch that deletes `fmi:drain:<comm>` plus `:members`,
`:events` and `:batch`, and `--max-checkpoints 0` now means "clean run, score it anyway"
instead of a `ValueError`.

**`redis-cli` 6.2 has no `--json`,** and an `XRANGE` printed flat cannot be parsed: an event's
`last_stream_id` *value* looks exactly like an entry id. The driver reads the stream through a
six-line `EVAL` that joins fields with `\x1e` and entries with `\x1d`.

**`trigger` must include `control` on every rank.** The migrating rank's drain reaches EOF only
once each peer has closed its side, and a peer learns to from the coordinator stream. `both` is
what `fmi_drain.json` sets. A survivor's data path often notices the FIN first and handles it
without waiting for the notice — which is why a seal completes in about a millisecond here —
but that is an optimisation of the ordering, not a replacement for the notice.

### Reading the image with crit

`crit` is packaged separately from `criu` on Rocky 9 and is not installed on this machine.
Without root:

```bash
cd /tmp && mkdir critpkg && cd critpkg
dnf download crit python3-criu python3-protobuf
for f in *.rpm; do rpm2cpio "$f" | cpio -idm --quiet; done
export PYTHONPATH=/tmp/critpkg/usr/lib/python3.9/site-packages:/tmp/critpkg/usr/lib64/python3.9/site-packages
python3 /tmp/critpkg/usr/bin/crit show <img>/files.img | grep -c inetsk    # 0
```

## Interpreting failures

| symptom | meaning |
| --- | --- |
| `FAIL (A SOCKET SURVIVED THE DRAIN)` | the acceptance criterion. Either the dump log names a connected/external socket, or a socket image file exists, or `/proc/<pid>/fd` still had one at the stop. A protocol verdict |
| `FAIL (criu itself, no socket evidence)` | criu refused for its own reasons — caps, a kernel feature, a pid still taken. Read `img<k>/dump.log`; triage the environment, not the protocol |
| `FAIL (the drain protocol)`, no `sealed` | the rank never sealed within `--seal-timeout`. Usually a peer that never half-closed: check that every rank's `trigger` includes `control`, and that the coordinator's Redis is reachable |
| `sealed but never reached state T` | the drain finished and the process did not stop — `drain_rehearsal_only` left on, or the request was dropped as stale (its epoch was behind the rank's restore count) |
| `stream counters disagree at reconnect` | a byte was lost or duplicated across the cut. Loud by design; the counters in the message name all four sides. See "the one failure" for the one occurrence here that its own numbers do not explain |
| `the connection died N ms ago and no migration notice followed` | an *unplanned* death: a peer exited or was killed rather than migrating. Not recoverable in this protocol, by design — it handles planned migration only |
| a rank never reaches `DONE` after a clean restore | check `migration_max_ms` against how long the dump+restore actually took; a survivor's `max_timeout` is suspended while a peer migrates but that bound is not |

## Scope

Single host, one rank per migration, `SIGRTMIN+3` as the request. Batches (several ranks in one
cut), multi-host moves and the steady-state cost measurement are the next stages and are not
covered by anything here. Cross-host restore is *expected* to work by construction — the
restore leg binds a fresh listener and re-runs `resolve_advertise_ip` before re-advertising, so
nothing in the image pins the old machine, and the image has no socket to repair — but it has
**not** been run, and until it has it is a design claim, not evidence.

The in-place rehearsal in `tests/drain_migration.cpp` (`drain_rehearsal_only`, the whole
sequence with the `SIGSTOP` replaced by an immediate restore) is the regression net that runs
without criu; this runbook is the part of the claim that only a real dump can make.
