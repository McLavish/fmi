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

## Multi-host: four machines, cross-host restore and single-cut batches

Everything above is one machine. `multihost_drain.py` is the same acceptance criterion across a
cluster: ranks spread over several machines, a rank dumped on one and restored on **another**,
and — behind `--allow-batch` — several ranks drained in **one cut** under a driver-held batch
lease. Both are run and both are in the evidence tables at the end of this section.

Four files, all in this directory:

| file | what it is |
| --- | --- |
| `cluster.py` | the reusable layer: `Cluster` (ssh, no ControlMaster), `preflight`, `launch_ranks`, `criu_dump`/`criu_restore`, `sigqueue_remote`/`cont_remote`, `wait_pid_gone`, `kill_all`, and the control-plane producers `emit_migrate` + `batch_lease_take/release/wait_free` |
| `node_helper.py` | the per-node agent, stdlib-only python3.9, run from the shared tree over ssh. Every poll loop lives inside the node: `sigqueue`, `wait-state`, `seal-wait`, `wait-gone`, `cont-if-stopped`, `pidband`, `nvra` |
| `multihost_drain.py` | the campaign driver. Imports `drain_driver.py`'s scoring, verdicts, socket-evidence rules and stream reader, so a multi-host result means exactly what a single-host one means |
| `campaign.json` | the scenario matrix (B0–B8 sequential, C0–C8 batch), as a small DSL |
| `fmi_drain_multihost.json` | `fmi_drain.json` with a cluster-reachable registry (`10.164.0.3:6380`), `bind_host 0.0.0.0` and **`advertise_host ""`** |

### Setup prerequisites

All environmental, all of them load-bearing:

- **One shared tree at the same absolute path on every node** (here `/scratch/fmi`), holding the
  checkout, the build tree, this directory and the run directory. criu reopens the subject
  binary, the process's cwd and its log file *by path* on the restore host. Rule for the
  campaign: develop and commit in `/home/luca/fmi`, refresh `/scratch/fmi` only **between**
  phase blocks, and record `git rev-parse HEAD` plus the subject's sha256 with every run —
  `preflight` does both and writes them to `preflight.json`.
- **One Redis every node can reach**, named in the config, **never loopback**. It is both the
  DrainTCP registry and the drain coordinator. A loopback `registry_host` is a hard refusal in
  the driver: a restored rank re-resolves its advertise address against the registry, so it
  would publish the loopback of whatever machine it woke up on.
- **`advertise_host: ""`**. The restore leg binds a fresh listener and re-runs
  `resolve_advertise_ip` before re-advertising, which is what makes a cross-host restore correct
  by construction — but only if nothing pins an address in the config.
- **`trigger: "both"` on every rank.** The migrating rank's drain finishes only once its peers
  have half-closed, and they learn to from the coordinator stream.
- **Disjoint pid bands per node**, seeded into `/proc/sys/kernel/ns_last_pid` (defaults in
  `cluster.DEFAULT_PID_BANDS`: `.3`→1.0M, `.4`→1.5M, `.5`→2.0M, `.6`→2.5M, span 500000). criu
  restores the dumped pid verbatim, so a pid already taken on the destination is a restore that
  fails with "File exists". The driver asserts every **launched** pid is in its node's band the
  moment it is captured, and after a move asserts the restored pid still belongs to its home
  band and is *outside* the destination's — a rebooted node becomes a refused setup error at
  second zero instead of a mid-campaign restore failure. `--pid-bands none` disables it.
- **Identical runtime closure on every node**: `ldd` of the subject → `rpm -qf`, plus `criu -V`
  and `uname -r`. Not `rpm -qa` — criu remaps shared libraries by path and content, so what
  must match is what the subject actually loads. (Boost is static here and correctly absent.)
- criu with `cap_sys_ptrace,cap_checkpoint_restore` (no `cap_net_admin`: the images are
  socketless), `--unprivileged` on both legs, and passwordless ssh to every node.

### Invocations

```bash
cd /scratch/fmi/runbooks/drain-migration
NODES="10.164.0.3 10.164.0.4 10.164.0.5 10.164.0.6"      # T, N2, N1, N3 — campaign node order

# review the plan for any scenario without touching the cluster
python3 multihost_drain.py --dry-run --nodes $NODES --only B2
python3 multihost_drain.py --dry-run --nodes $NODES --only C2

# phase B — sequential cross-host migration (works today, driver only)
python3 multihost_drain.py --nodes $NODES --config /scratch/fmi/runbooks/drain-migration/fmi_drain_multihost.json \
        --phase B --seed 1 --keep

# one scenario, three seeds
for s in 1 2 3; do python3 multihost_drain.py --nodes $NODES --only B5 --seed $s; done

# phase C — single-cut batches. --allow-batch is required: the C-1..C-3 library fixes they
# need landed at 63382da, and the flag now acknowledges batch mode rather than overriding a
# known-red path. One scenario at a time is how the evidence below was taken, so that a
# verdict stops exactly one block.
python3 multihost_drain.py --nodes $NODES --only C4 --allow-batch --seed 2 --keep
```

The driver takes a **four-machine clean baseline per (shape, peers, rounds, payload_ints)**
first and scores every trial against it — "the same checksums as a single-host run" and "the
same checksums as a four-machine run" are different claims, and only the second separates a
migration bug from a cluster that is wired wrong. `preflight` runs once per invocation
(`--skip-preflight` to reuse a verified session). Images and logs live under
`runs/mhd<pid>/<comm>/`, on the shared filesystem: nothing is copied, and the dump strictly
precedes the restore, which is what gives NFS close-to-open consistency for the image.

### Scenario DSL

```jsonc
{"name": "B2", "phase": "B", "kind": "seq",       // clean | seq | cut
 "peers": 4, "shape": "baseline", "trials": 3,    // shape may be a list: once per shape
 "place": "rr",                                   // rr | block | {rank: node} explicit map
 "ranks": [1, 1, 1], "to": [3, 2, 1]}             // or "evacuate": <node>|[nodes],
                                                  // "to": next|spread|one|<node>|[nodes],
                                                  // "cuts": single|per-node
```

A node designator is an **index into `--nodes`** (so the file is cluster-agnostic) or a
hostname. `place: "rr"` puts rank *r* and rank *r+4* on one machine and therefore never puts
ring neighbours together — C6 uses an explicit map for exactly that reason.

### Acceptance criteria (per trial, all phases)

The six single-host criteria at the top of this README, plus:

7. the rank **actually changed machine** (a plan whose destination equals the source is scored
   `void`, never `pass`);
8. the restored pid keeps its home band and is outside the destination's;
9. the event trail reads `leaving(e) → sealed(e) → restored(e+1)` for every migration, with the
   expected batch id — the library's own `comm|rank@epoch` for a signalled migration, the
   driver's `comm|cutN` for a batch;
10. for a cut: **every** member sealed before **any** member restored, and the sealed counters
    agree pairwise, `sealed[a].sent.b == sealed[b].received.a`. That is byte-exactness evidence
    taken before anything is dumped, and it is available even for a link that is never
    re-established;
11. for the C8 negative: a second batch lease taken while a cut is in flight must be **refused**
    and the job must still finish with the baseline checksums.

Failure policy, from the campaign plan and enforced by the driver: any `socket`, `criu`,
`drain`, counters-disagree or checksum verdict **keeps the whole trial directory and stops**
(`--continue-on-fail` overrides, deliberately not the default). Widening a timeout, retrying
until green, pinning `advertise_host` or adding a TCP flag are not responses to a verdict. A
setup violation — a pid out of band, a HEAD or subject sha that differs between nodes — aborts
as a **setup error** and is never reported as a protocol verdict.

### Evidence

**Phases B and C are both done and are the evidence below** — sequential cross-host migration
first, then single-cut batch evacuation, the two claims this cluster existed to settle.

Cluster: four Rocky 9.8 machines — criu-testing `10.164.0.3` (T, 8c), criu-node-2 `.4` (N2),
criu-node-1 `.5` (N1), criu-node-3 `.6` (N3), all 4c except T — one shared `/scratch/fmi` (xfs
on T, NFS on the other three), cluster Redis on `10.164.0.3:6380`, criu 3.19 under `sudo`,
kernel `5.14.0-687.24.1+2.1.el9_8`, verified identical on all four nodes by `preflight` before
every invocation. 2026-08-14. **Phase B ran against tree `571df5f`, subject sha256
`314ce035…d660`; phase C against `63382da`, subject `e762cd9e…0c92`** — the C-1..C-3 library
fixes landed between the two blocks, so the subject sha changes on purpose and each block's runs
record their own.

**Phase B: 32 trials, 32 passed, 0 failed, 0 skipped, 0 void — and 47 real cross-host `criu
dump` / `criu restore` pairs (46 with a complete `leaving → sealed → restored` trail), every one
of them dumped on one machine and restored on another.
Phase C: 26 trials, 26 passed, 0 failed, 0 skipped, 0 void — 28 cuts, 60 migrated ranks, every
one of the 60 dumped on one machine and restored on another, and 41 pairwise sealed-counter
cross-checks inside batches with 0 disagreements.
Across both, every dump and every restore returned 0 with no `--tcp-close`, no
`--tcp-established` and no `--shell-job`; no image contained a socket; every rank of every trial
finished with the four-machine baseline checksums.** The four-machine baselines are themselves
bit-identical to single-host runs of the same (shape, peers, rounds, payload) — see the B0 row.

| scenario | what it moves | trials | migrations | dump/restore rc | socket verdicts | checksums | max(leaving→restored) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| B0 clean, 8 shapes | nothing | 8/8 clean | — | — | — | all 8 shapes **identical to the single-host oracle**, rank for rank | — |
| B1 one rank N2→N3 | 1 rank | 6/6 | 6 | 0 / 0 | none | == 4-machine baseline | 954 ms |
| B2 chained N2→N3→N1→N2 | 1 rank ×3, epochs 0,1,2 | 3/3 | 10 | 0 / 0 | none | == 4-machine baseline | 914 ms |
| B3 two ranks in sequence (r0 T→N3, r2 N1→T) | 2 ranks | 4/4 | 8 | 0 / 0 | none | == 4-machine baseline | 1030 ms |
| B4 rank 0 — the rank everyone dials (baseline, p2p_ring) | 1 rank | 6/6 | 6 | 0 / 0 | none | == 4-machine baseline | **1036 ms** |
| B5 8 ranks, N1 evacuated sequentially onto two survivors | 2 ranks | 3/3 | 6 | 0 / 0 | none | == 4-machine baseline | 976 ms |
| B6 mid-message, 8 ranks (variable_payloads, 4096 ints) | 1 rank | 3/3 | 3 | 0 / 0 | none | == 4-machine baseline | 940 ms |
| B7 deep_rounds / uneven_participation / mixed_p2p_collective | 1 rank ×3 shapes | 6/6 | 6 | 0 / 0 | none | == 4-machine baseline | 948 ms |
| B8 12 ranks, 3 per machine (optional) | 1 rank | 1/1 | 1 | 0 / 0 | none | == 4-machine baseline | 935 ms |
| C0 batch of one | 1 rank ×3 | 3/3 | 3 | 0 / 0 | none | == 4-machine baseline | 949 ms |
| C1 evacuate N1 k=2 → one survivor | 2 ranks ×3 | 3/3 | 6 | 0 / 0 | none | == 4-machine baseline | 1921 ms |
| C2 evacuate N1 k=2 → spread | 2 ranks ×5 | 5/5 | 10 | 0 / 0 | none | == 4-machine baseline | 1918 ms |
| C3 12 ranks, k=3 spread | 3 ranks ×3 | 3/3 | 9 | 0 / 0 | none | == 4-machine baseline | 2845 ms |
| C4 two machines, k=4, one cut | 4 ranks ×2 | 2/2 | 8 | 0 / 0 | none | == 4-machine baseline | **3744 ms** |
| C5 k=2 mid-message (variable_payloads, 4096 ints) | 2 ranks ×3 | 3/3 | 6 | 0 / 0 | none | == 4-machine baseline | 1926 ms |
| C6 ring neighbours, mutual seal (p2p_ring / mixed) | 2 ranks ×2 shapes ×2 | 4/4 | 8 | 0 / 0 | none | == 4-machine baseline | 1938 ms |
| C7 two cuts back to back (N1, then N3) | 2 ranks ×2 cuts ×2 | 2/2 | 8 | 0 / 0 | none | == 4-machine baseline | 1902 ms |
| C8 negative: second batch refused | 2 ranks | 1/1 | 2 | 0 / 0 | none | == 4-machine baseline | 1906 ms |

Phase C ran C0–C8 in order, one invocation per scenario, seeds rotating 1‑2‑3 across the block
(C0/C3/C6 seed 1, C1/C4/C7 seed 2, C2/C5/C8 seed 3) — the trial counts in `campaign.json` are
the repetition, and one pass of them is the plan's 26 trials / 60 migrated ranks exactly. Every
migrated rank passed the state-`T` fd scan at `fds=5 sockets=0` (60 of 60), every `criu dump`
and `criu restore` wrote `… finished successfully`, and the sweep for `inetsk.img`,
`unixsk.img`, `tcp-stream.img`, `sk-queues.img`, `packetsk.img` and `netlinksk.img` over all 60
image directories found **zero** of any of the six.

### What the batch path adds, measured

| | |
| --- | --- |
| **all sealed before any dump** | structural in `evacuate()` and confirmed from the stream in all 28 cuts: `max(sealed) < min(restored)` per batch, every time |
| **pairwise counters inside the batch** | `sealed[a].sent.b == sealed[b].received.a` — **41 pairs across 28 cuts, 0 disagreements.** C4's k=4 contributes 6 pairs per cut; C5 cross-checks 380205080 bytes per direction between two migrators, mid-message |
| **the driver's batch id, end to end** | every `leaving`, `sealed` and `restored` of all 60 migrations carried the driver's `<comm>\|cutN`, not the library's `comm\|rank@epoch` — the ranks adopted the batch and **skipped self-leasing** |
| **one lease per cut, driver-held** | `MONITOR` on the campaign Redis shows exactly one `SET <comm>:batch <driver-owner> NX PX 120000` per cut and one compare-and-delete release; **no rank ever issued a `SET` on the batch key** |
| **C7's lease-clear guard** | cut0's release (lua `GET`+`DEL`) at *t*, the driver's `EXISTS` poll 1 ms later, cut1's `SET NX` 1.5 s after that — the second cut never raced the first's release |
| **C8's refusal** | the intruder's `SET … NX` landed 6.4 ms after the holder's and was refused; the job finished with the baseline checksums |

### The C-1 fix, observed rather than argued

C-1 is the one whose absence a green run would not by itself prove: a member that restores first
must not dial a co-member that is still frozen. Its co-member's registry entry still names the
address it left — on the **evacuated** node, where nothing listens any more — so such a dial is
answered by that node's kernel with a RST. `Tcp: OutRsts` sampled on the evacuated node across
the interval between the batch's *first* `restored` and its *last* is therefore a direct count of
exactly the dials the fix must prevent; on a node whose ranks have all left, that interval is
otherwise silent.

**Measured over 30 such windows (C1–C7; C4 and C7 counted on both evacuated nodes), 17.6 s of
total exposure with 1 to 3 co-members still frozen: `OutRsts` +0, every window.** The exposure
per cut is 417–447 ms at k=2, 857–871 ms at k=3 and 1281–1286 ms at k=4 — the batch restores
serially, so the wider the batch the longer the first member waits, which is precisely why C-1
mattered.

The rank logs agree from the other side. Not one of the 26 trials logged a `Timeout`, a
`stream counters disagree`, an unplanned-death notice or any error at all; survivors logged
*nothing* about the batches passing under them. And the first-restored member's link to the
co-member is picked up only once that co-member is back — C1 trial 0, where rank 2 restored
439 ms before rank 6:

```
17:55:46.016  DrainTCP: rank 6 … resumed at epoch 1, incarnation 1, listening on 10.164.0.3:39339
17:55:46.036  DrainTCP: rank 6 dropped a leave notice from peer 2 written by incarnation 0;
                        it is already connected to incarnation 1
```

20 ms after rank 6 comes back, rank 2 — restored, incarnation 1 — is already connected to it, and
rank 2's own late leave notice is fenced by the incarnation rather than tearing that link down.
Both halves of the design in two lines, cross-host, inside a batch.

One honest observation: a single `OutRsts` was seen on N1 in one C7 trial, in the wider
`[last sealed, last restored]` window of cut1 — **not** in any C-1 exposure window (those were
+0), on a node that at that moment held no rank of the job and was also being talked to over
ssh by the driver. One event in 28 cuts, unattributable to a dial at a frozen co-member, and
that trial met every acceptance criterion.

B2's ten and B3's eight are the honest counts: B2 ran one further trial whose first leg
migrated cleanly and whose second leg the driver then aborted as a **setup error** — its own pid-band
assertion was wrong, not the cluster (see "What the first run of this driver got wrong"), so
that trial is excluded from the trial column and its completed migration is not. B3's fourth
trial is a re-verification after the `dump.log` permission fix below.

### Wall clock

Phase B, over its 46 migrations:

| | |
| --- | --- |
| `leaving → restored`, the migration window | **min 812 ms, median 940 ms, max 1036 ms** over all 46 |
| `leaving → sealed`, the drain itself | 0–20 ms; the median is 13 ms |
| `criu dump` | 0.2 s, every one of the 46 |
| `criu restore` | 0.2–0.3 s |
| the rank's own view | `MigrationTrigger … is back at epoch 1 after 622 ms` (B1 trial 0) |
| what a migration costs the job | clean 72.7 s vs 73.0–74.1 s with one migration at 4 ranks; B2's three migrations in one job cost 76 s against the same 72.7 s baseline |

Phase C, over its 60 migrated ranks in 28 cuts. Two numbers matter here and only one of them
existed in phase B: a rank's own window, and the **whole cut** — first `sealed` to last
`restored`, the interval in which the job is missing at least one rank:

| | |
| --- | --- |
| `leaving → restored`, per rank | min 932 ms, median 1891 ms, **max 3744 ms** over all 60 |
| `leaving → sealed`, the drain itself | 0–20 ms, median 14 ms — **unchanged by batching**, and unchanged by k |
| the whole cut, first sealed → last restored | 930–937 ms at k=1, 1856–1926 ms at k=2, 2812–2831 ms at k=3, **3705–3736 ms at k=4** |
| `criu dump` | 0.2 s, every one of the 60 (9–34 ms on criu's own clock) |
| `criu restore` | 0.2–0.3 s (2–132 ms on criu's own clock) |

The whole cut is **linear in k at ~930 ms per additional rank** on top of a ~930 ms floor — 930,
1890, 2820, 3720 at k = 1, 2, 3, 4. That is the driver's serial ordering showing through, not the
protocol's: all k ranks seal concurrently inside the same 0–20 ms, and then the dumps, the reaps,
the restores and the SIGCONTs are each done one rank at a time over ssh. Each extra rank
therefore buys one more serialized dump leg **and** one more serialized restore leg, ~430–470 ms
apiece — which is also why the interval in which one member is back and another is still frozen
grows with k (417–447 ms at k=2, 857–871 ms at k=3, 1281–1286 ms at k=4). The drain does not care
how many ranks are in the batch; the harness does, and a driver that dumped and restored the
batch in parallel would collapse most of this.

`migration_max_ms` is 120000 in `fmi_drain_multihost.json` against an observed maximum of
**3744 ms** across both phases — a factor of 32, still comfortably past the plan's rule of 10×,
and the k=4 row is the number to size it against rather than phase B's 1036 ms.

### The cross-host claim, measured

The thing this phase existed to settle. Rank 1 of B1 trial 0 was dumped on criu-node-2 and
restored on criu-node-3, and came back saying so:

```
[15:59:04.500138] MigrationTrigger: rank 1 … begins a real migration at epoch 0
[15:59:05.434393] DrainTCP: rank 1 … resumed at epoch 1, incarnation 1, listening on 10.164.0.6:37713
[15:59:05.434432] MigrationTrigger: rank 1 … is back at epoch 1 after 622 ms
```

`10.164.0.6` is criu-node-3 — the machine it woke up on, not the one whose image it carries.
`resolve_advertise_ip` re-ran on the restore leg and re-advertised into the shared registry, and
the three survivors re-established to the new address with **nothing in their logs at all**: no
drain notice handling, no error, no reconnect chatter. Rank 1 logged round 23750 (the round it
was frozen on) and then 23775 through 50000, and all four ranks finished with the four-machine
baseline checksums. That was `advertise_host: ""` doing exactly what the design said it would;
until this run it had never been executed.

Its image, read on the destination side: `files.img` and the directory listing carry `REG` and
`PIPE` entries, `timens-0.img`, and **no `inetsk.img`, `unixsk.img`, `tcp-stream.img`,
`sk-queues.img`, `packetsk.img` or `netlinksk.img`** — swept across all 47 image directories of
the phase, zero hits of any of the six. Every migration also passed the state-`T` fd scan, which
is what `node_helper.py seal-wait` exits 4 on and the driver turns into an immediate socket
verdict; the 40 scans recorded in the matrix logs all read exactly `fds=5 sockets=0` —
`0 → /dev/null`, `1,2 →` the rank log, and the control thread's self-pipe pair.

One image-level curiosity worth knowing before someone audits a directory and worries: seven of
the 47 images carry no `timens-0.img`, and they are exactly the **second and third legs of B2's
chained migrations** — a rank that is already inside a time namespace criu made for it on the
previous restore. All seven restored rc 0, and B2's windows (812–914 ms) and checksums are
indistinguishable from every other scenario's, so it is a criu detail about repeated time
namespaces and not a clock problem. The clock problem the `sudo` in `cluster.CRIU` exists to
avoid looks completely different: a `Timeout` thrown immediately after a migration that
succeeded. It did not occur once in this phase.

### What the first run of this driver got wrong

This was the driver's first real execution and it had five defects; all five are harness bugs,
none is in the library, and the campaign was re-run past each. Recorded because the next person
to stand this up will hit them in the same order.

1. **`git` is not installed on the worker nodes**, so the shared-tree HEAD gate failed on three
   of four machines. `node_helper.py head` now resolves `.git/HEAD` and the ref it names by
   reading them, which is what the gate actually asserts — every node seeing the same tree at
   the same path — without putting an unpinned package on the machines the gate is about.
2. **The pid-band assertion used the wrong band.** `assert_restored_pid` compared a restored pid
   against the band of the machine the move started FROM. criu restores a pid verbatim, so the
   pid belongs to the band of the machine the rank was **launched** on, for its whole life —
   after one migration those differ, and B2's second leg aborted as a setup error on a cluster
   that was fine. The same function also refused a rank returning to its launch node, where the
   pid is inside the destination's band by construction. Both fixed; the assertion still catches
   a genuinely out-of-band pid.
3. **An unreadable `dump.log` read as a clean one.** criu runs under `sudo` and writes its log
   mode 0600 as root. On the three nodes where `/scratch` is NFS the export's
   `all_squash anonuid=1000` turns that into `luca` and the log is readable; on criu-testing,
   which is the NFS *server* and sees `/scratch` as local xfs, it stays root-only. `socket_lines`
   answers `([], 0)` for a log it cannot open — indistinguishable from a clean one — so
   acceptance criterion 2 passed **vacuously** for the nine migrations dumped on that one
   machine. The criu legs now `chmod 0644` their logs (criu's exit status preserved across it)
   and `dump_all` refuses an unreadable log as a setup error rather than scoring it. The nine
   affected logs were re-read as root afterwards and are clean, and a fresh B3 trial confirms the
   check now actually runs: 1444 lines read, 0 socket hits.
4. **`--trials` and `--rounds` were inert.** They were folded into the campaign's `defaults`,
   which `field()` reads *after* the scenario — and every scenario names its own `trials`, so
   `--trials 1` ran the scenario's three. Command-line sizing now outranks the file.
5. **Two smaller ones**: a baseline's directory name did not include `rounds`/`payload_ints`
   while its cache key did, so two baselines of one shape at different sizes would have shared a
   directory and the second would have read the first's stale `DONE` lines; and the setup-error
   abort path saved the event stream but left the trial's four Redis keys behind.

A sixth thing that is not a bug: `--dry-run` printed the criu invocation without the `sudo` it
actually uses. The printed plan is reviewed as if it were the invocation, so it now is one.

### Sizing

The shapes differ ~70× in cost per round cross-host, so one `rounds` cannot size a scenario that
names a list of shapes; `campaign.json` grew a `by_shape` block and the measured per-round costs
are recorded in its `_sizing` note. Measured here at 4 ranks, one per machine, `ms=1`: a job
costs ~0.7 s of fixed overhead plus, in ms/round, baseline 1.44, p2p_ring 1.10,
collectives_sweep 1.55, mixed_p2p_collective 1.35, noncommutative 1.23, deep_rounds 41.8,
variable_payloads 44, uneven_participation 102. Cross-host is 5.5× single-host on `deep_rounds`
and 1.1× on `baseline` — the latency-bound shapes pay the network and the throughput-bound ones
barely notice, which is why the single-host round counts could not simply be reused. Every
scenario is sized for a ~75 s job; the eight B0 runs came in at 70.2–76.5 s.

### Multi-host wrinkles

**A restored rank comes back stopped, on the other machine.** Same as single-host, one ssh call
further away: `node_helper.py cont-if-stopped` is what resumes it.

**`;` not `&&` before `setsid` in the launch string.** With `cd X && cmd &` the whole list is
backgrounded as a subshell, `$!` names the subshell (criu would dump the wrong process) and that
subshell holds the ssh channel open forever. The restore call uses `&&` precisely because
nothing there is backgrounded.

**No ControlMaster on any ssh call.** A mux master that wedges silently hangs every later call
to that node; one plain handshake per call costs ~0.2 s and has no shared failure state.

**Never poll over ssh.** At ~0.15 s per handshake a remote `/proc` poll cannot resolve a state
that lasts a millisecond. Every wait loop is a `node_helper.py` verb, and one migration costs
about six round trips.

**The epoch payload is mandatory.** `sigqueue` carries the target's current epoch in
`si_value.sival_int` and `MigrationTrigger` drops a request whose epoch is behind the process's
restore count — which is exactly what B2's chained migrations produce.

**Rank logs are read over NFS.** A log's server-side view can lag while the file is open. Every
verdict reads logs after the ranks exited; the one softer read is the pre-migration round
snapshot, which can undercount and makes the progressed-after-restore check conservative in the
trial's favour.

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

Backed by real `criu` dumps and restores: single host, one rank per migration by `SIGRTMIN+3`
(43 migrations, the first half of this file); **cross-host restore**, one rank at a time (phase
B, 47 migrations, dumped on one machine and restored on another); and **single-cut batch
evacuation** by `migrate` event under a driver-held lease (phase C, 60 migrations in 28 cuts, up
to k=4 and up to two machines emptied at once). Cross-host restore and batches are no longer
design claims — they are the two evidence tables above.

Still not covered here: more than one rank per *process* (a second armed drain channel in one
process is a `std::logic_error` by design), unplanned failure of any kind (this protocol handles
planned migration only), and the steady-state cost measurement — what the drain-armed data path
costs a job that never migrates, which is the benchmark stage and not a correctness question.
The cuts here are all driven by one driver holding one lease; nothing has yet exercised two
independent drivers racing for the same communicator beyond C8's single-shot refusal.

The in-place rehearsal in `tests/drain_migration.cpp` (`drain_rehearsal_only`, the whole
sequence with the `SIGSTOP` replaced by an immediate restore) is the regression net that runs
without criu; this runbook is the part of the claim that only a real dump can make.
