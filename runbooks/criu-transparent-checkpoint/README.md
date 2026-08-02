# Transparent CRIU checkpoint/restore of an unmodified FMI program

This runbook checkpoints one rank of a **running, unmodified FMI application** with `criu`,
restores it, and verifies that the job finishes with exactly the results a clean run produced.

The application does not participate. `checkpoint_subject.cpp` constructs a `FMI::Communicator`
and hands it to one named **app shape** — the default, `baseline`, calls `barrier`, `bcast`,
`send`/`recv`, `allreduce`, `reduce`, `gather`, `scatter` and `scan` in a loop. There is no
checkpoint API in it, no migration hook, no annotation, and it never learns that it was frozen.
Everything that makes the freeze survivable happens inside the library, below `Channel`.

## What has to be true for this to work

A checkpoint lands at an arbitrary instant and takes every socket with it (`criu --tcp-close`
drops each connection on restore). Four separate things therefore have to hold, and each was
broken at some point in getting this to run:

1. **The restored process has to stay alive.** hiredis writes without `MSG_NOSIGNAL`, so the
   first registry command after a restore — issued on the context the image captured, whose
   socket the restore dropped — delivers `SIGPIPE`. With the default disposition the rank dies
   there, before it can re-establish anything. `FMI::Utils::suppress_sigpipe` fixes this.
2. **A break anywhere in a frame has to be recoverable.** The receive watermark may only
   advance once the payload is in the application's buffer. Committing at the header instead
   lets the peer prune a message whose bytes were still in a kernel socket buffer, and nothing
   reports the loss. See `SequencedLink::commit_inline`.
3. **Retention has to be released on links the peer never writes to.** Otherwise the sender's
   window fills and the job stops — which is what happens from three ranks upward, where a
   binomial tree leaves some directed links with no return traffic to carry an ack.
4. **The restored process must be recognised as the same lineage**, not as a replacement, so
   its peers reconcile with its sequences instead of resetting them. That is what the
   incarnation in the handshake is for; a restored process keeps the one its image was taken
   with, because it never re-runs the constructor that claims a new one.

## App shapes

One shape is one application workload. Each lives in its own file under `shapes/`, registers
itself, and is selected by name:

```bash
$SUBJECT --list-shapes                     # what is compiled in
$SUBJECT 0 2 fmi.json "$COMM" 20000 0 500 --shape p2p_ring
FMI_SHAPE=p2p_ring $SUBJECT 0 2 ...        # or from the environment
python3 sweep.py --shape p2p_ring ...      # the sweep passes it through
```

`--shape` may appear anywhere on the command line and never moves a positional argument, so the
`pgrep` patterns below keep working. The default is `baseline`.

### Adding one

Write `shapes/shape_<name>.cpp`, implement one function, register it. Nothing else — no shared
list to edit, and CMake globs `shapes/*.cpp`:

```cpp
#include "../shapes.h"

namespace FMI::Runbooks::Checkpoint {
namespace {

unsigned long long run_my_shape(FMI::Communicator& comm, FMI::Utils::peer_num rank,
                                FMI::Utils::peer_num num_peers, const ShapeParams& params) {
    unsigned long long checksum = 0;
    // ...operations; check every result inline; fold received values into checksum...
    return checksum;   // throw ShapeFailure{N} after printing a MISMATCH line instead
}

FMI_REGISTER_SHAPE("my_shape", "one line about what it exercises", run_my_shape)

} // namespace
} // namespace FMI::Runbooks::Checkpoint
```

**The checksum must depend only on the shape, the rank, the rank count and `ShapeParams`** —
never on timing, pids, addresses or how many times the rank was checkpointed. The sweep compares
a checkpointed run against a clean baseline run of the same shape, so a checksum that varies
between two clean runs fails every trial, and one that does not fold in *received* data passes
every trial while proving nothing. `shapes.h` documents the full contract and every field of
`ShapeParams`; `shapes/shape_p2p_ring.cpp` is a short worked example.

## Requirements

- `criu` (verified on 4.2) with `--unprivileged`, or run as root
- a Redis on `127.0.0.1:6379` — the DirectTCP peer registry, not the data plane
- a build of this repo with Redis enabled

```bash
cmake -S . -B build -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_TCPUNCH=OFF
cmake --build build -j"$(nproc)"
```

The subject lands at `build/runbooks/criu-transparent-checkpoint/fmi_checkpoint_subject`.

## One checkpoint by hand

```bash
cd runbooks/criu-transparent-checkpoint
SUBJECT=../../build/runbooks/criu-transparent-checkpoint/fmi_checkpoint_subject
COMM=demo-$$
redis-cli DEL "fmi:direct:$COMM"

for r in 0 1; do
    setsid "$SUBJECT" $r 2 fmi.json "$COMM" 20000 0 500 > /tmp/rank$r.log 2>&1 &
done
sleep 1

PID=$(pgrep -f "fmi_checkpoint_subject 1 2 .* $COMM ")
mkdir -p /tmp/img
criu dump    --unprivileged -t "$PID" -D /tmp/img --tcp-close --shell-job -v4 -o dump.log
criu restore --unprivileged        -D /tmp/img --tcp-close --shell-job -v4 -o restore.log -d

tail -2 /tmp/rank0.log /tmp/rank1.log
```

Both ranks must reach `DONE`, with the same checksums as a run that was never checkpointed.
Note that the argument list must be quoted into `pgrep` exactly as above: the rank number is
the first argument, so `fmi_checkpoint_subject 1 2` is rank 1 of 2.

## The sweep

`sweep.py` runs the same thing many times over, choosing the rank and the instant at random,
and comparing every rank's final checksum against a clean baseline of the same shape.

```bash
python3 sweep.py --trials 20 --peers 2 4 --rounds 40000 --max-checkpoints 3
python3 sweep.py --trials 20 --peers 2 4 --rounds 40000 --shape p2p_ring
```

A trial passes only if every rank reaches `DONE` with the baseline checksum **and** the
checkpointed rank logged a round after its restore. A trial whose job finished before the
checkpoint fired is reported as `SKIP` rather than a pass — such a trial proves nothing, and
early versions of this sweep reported a clean 12/12 while checkpointing nothing at all. (It
was once scored a FAIL, which several readers misread as a protocol failure; a skip says what
it means.)

**Give it enough rounds.** The default `--rounds 40000` leaves ample time for the 0.25–1.2 s
the sweep waits before it freezes anything. If you shorten it, a run that finishes before the
freeze lands ends `ALREADY FINISHED (trial is void)` and is skipped; lengthen a round with
`--ms` if you want fewer, slower rounds instead.

`--max-checkpoints N` checkpoints up to N times per run, choosing a fresh rank each time, which
exercises a rank being frozen while a peer is itself mid-repair.

## ClientServer (Redis) data plane

Everything above runs over `DirectTCP`. The store-backed family checkpoints too, by an entirely
different mechanism, and the same sweep drives it — `--config` chooses the plane:

```bash
python3 sweep.py --config fmi_redis.json --trials 20 --peers 2 4 8 --max-checkpoints 3 \
    --seed 1 --rounds 6000
```

The config must enable **exactly one** backend, and `fmi_redis.json` enables Redis only. That is
not tidiness: with DirectTCP also enabled the cost model picks DirectTCP for every operation this
subject issues, so a sweep against a mixed config exercises no Redis code and reports green.
Cleanup follows the plane the config names — DirectTCP `redis-cli DEL fmi:direct:<comm>`, Redis a
scan-and-delete of the comm's own keys, S3 an `aws s3 rm --recursive` of its prefix.

**Rounds are much more expensive here.** Measured on loopback Redis: 6.7 ms per round at 2 ranks,
8.7 ms at 4, 10.2 ms at 8, against microseconds for DirectTCP. `--rounds 6000` is a ~40 s clean
run at 2 ranks, which is the window the sweep's 0.25–1.2 s freeze delay needs; `p2p_ring` costs
1.17 ms per round at 4 ranks, hence `--rounds 30000` for that shape.

### `max_timeout` counts iterations, not milliseconds

A peer waiting for an object polls `max_timeout / timeout` times, sleeping `timeout` ms and paying
one store round trip on each pass. Its patience is therefore
`(max_timeout / timeout) × (sleep + RTT)`, not `max_timeout` milliseconds. `fmi_redis.json`'s
`60000 / 1` is 60000 passes — about 66 s of wall clock at 2 ranks on loopback, and more at higher
rank counts, where each pass buys more RTT. Work the formula out for your store's latency; do not
read the number as a wall clock.

### What the recovery relies on

Not the sequenced link. No frames, no sequence numbers, no retention, no replay, no incarnation:
points 2, 3 and 4 at the top of this runbook are properties of the TCP transport, and none of them
applies here. A store rank keeps all of its protocol state in process-local memory — the
per-operation counters, the poll loop's own locals — which criu restores byte-exact, and the store
holds durable whole values written by idempotent `SET`/`GET`/`DEL`. A restored process resumes on
the instruction it was frozen on, with the same key it was about to write or read, so nothing can
be re-executed into a collision.

The only casualty of the freeze is the TCP connection to the store, and `"recover": "true"` is
about exactly that. Point 1 applies verbatim — same hiredis, same `SIGPIPE` on the first write
after a restore — and past it, the next command notices the dead context, frees it, dials again
within `connect_timeout_ms` and re-issues within `io_timeout_ms`. Re-issuing blind is safe because
every command is idempotent. A write that cannot be completed within the poll budget throws
instead of being logged and dropped, so a lost message surfaces at the sender rather than as an
unexplained `Timeout` at the peer.

### Store cleanup and `comm_name`

Under `recover`, `finalize()` deletes nothing: a rank that leaves cannot know whether a peer still
needs what it wrote, and deleting at that moment is what makes a staggered finalize deadlock.
Instead every write carries `EX object_ttl_s` (3600 in `fmi_redis.json`; `0` means no expiry at
all, which under this flag means no cleanup at all) and the store expires the objects.

That makes **a `comm_name` unique per run** a precondition rather than a convention. `sweep.py`
already names each run `fmisw<pid>-…` and deletes the comm's keys around every trial, but by hand
do not reuse a name inside the TTL window: the barrier markers are not deleted either, so the
second run's first barrier is satisfied instantly by the first run's markers.

### Evidence

All on `fmi_redis.json`, single host, live Redis on `127.0.0.1:6379`, criu 4.2.1:

| run | result |
| --- | --- |
| `--trials 20 --peers 2 4 8 --max-checkpoints 3 --seed 1 --rounds 6000` | 20 passed, 0 failed, 0 skipped |
| `--shape p2p_ring --trials 5 --peers 4 --max-checkpoints 3 --seed 1 --rounds 30000` | 5 passed, 0 failed, 0 skipped |
| `--shape mixed_p2p_collective --trials 5 --peers 4 --max-checkpoints 3 --seed 1 --rounds 8000` | 5 passed, 0 failed, 0 skipped |
| by hand, 2 ranks, rank 1 dumped at round 1000 of 6000 | both `DONE` with the baseline checksums; rank 1 logged round 5500 after its restore |

The DirectTCP profile was re-verified against the same `sweep.py` after the `--config` refactor —
`--trials 3 --peers 2 --rounds 8000 --seed 7` on the unchanged `fmi.json`, 3 passed, 0 failed.

`--shape collectives_sweep` is **not** covered, and the reason is not the store: its *clean*
baseline fails at 4 ranks before any trial runs, on a pre-existing `ClientServer::reduce` bug. For
an ordered (non-commutative) reduction with a root other than 0, the root seeds the accumulator
with its own contribution and then folds the remaining ranks in ascending order, computing
`f(v_root, v_0, …)` instead of `f(v_0, …, v_n-1)`; `ClientServer::scan` already parks its own
contribution in its own slot and is correct. Reproduced on the commit this work branched from,
unrelated to the recovery flag, and not fixed here. `--shape noncommutative` rotates its root the
same way and is affected identically.

## Interpreting failures

| symptom | meaning |
| --- | --- |
| `dump rc=1`, `Running as non-root requires '--unprivileged'` | pass `--unprivileged`, or run as root |
| restored rank dies with no output at all | `SIGPIPE` on the registry socket — see point 1 above |
| `sequence gap from peer N` | a frame was pruned before it was delivered — points 2 or 4 |
| `is at its retention limit` | acks are not reaching the sender — point 3 |
| `identity mismatch` | the ranks are executing different operations; a genuine protocol violation, not a checkpoint artefact |

## Scope

**Verified at 2, 3, 5, 7, 8 and 16 ranks — 48 randomized trials, 48 passed**, all of them on the
`baseline` shape, whose workload is unchanged since that run. Non-powers of two are covered
deliberately: FMI's collectives are binomial trees and take a different shape when the rank
count is not a power of two. Other shapes have their own, much smaller, evidence: `p2p_ring` has
3/3 at 4 ranks.

If a rank ever does stall, it says so: after three seconds of waiting it prints what it believes
it holds on every link — descriptor, bytes queued, whether the link still owes a handshake —
which is how the last of the deadlocks here was found.

`sweep.py` is single host. The restored rank re-uses the listening socket and advertised
address the image captured, which is correct on the same machine and wrong on any other;
cross-host restore needs the rank to re-publish its registry entry under the address of the
machine it actually woke up on. Nothing in the library does that any more — the
`prepare_for_checkpoint` hook that dropped the listener, the cached advertised address and the
registry client was removed together with the epoch migration runtime that called it.
`multihost_sweep.py` below still offers `--restore next|random`; until that hook has a
replacement, treat a cross-host restore as unbacked and keep `--restore same`.

## Multi-host

`multihost_sweep.py` is the multi-machine sibling: ranks spread round-robin over `--nodes` via
ssh, criu dump/restore driven on whichever host holds the target rank, and `--restore
same|next|random` choosing whether a dumped rank wakes up on the machine it left or a different
one. Its module docstring lists the environmental prerequisites (shared checkout at one absolute
path, identical library versions, criu file caps, disjoint PID bands per node, a
cluster-reachable registry, `advertise_host` left empty). `--max-checkpoints 0` turns it into a
pure multi-host liveness/correctness harness — no freezes, every rank must still reach DONE with
the single-host baseline checksum.

Taking the job multi-host is what exposed the END-OF-JOB teardown race (a finished rank's
abrupt close RST-destroys final-round frames a lagging peer had not yet consumed; the peer
re-establishes toward the exited process and dies of Timeout) — loopback consumes everything
before the window opens, so a single-host sweep can never see it. The fix is the sequenced
transport's graceful goodbye (`TcpChannelBase::drain_links_for_shutdown`, run from
`finalize()`): drain retention with tail-ack flushing, half-close, keep accepting and servicing
until the peers' own FINs. Multi-host evidence on the fixed transport: **all 8 shapes at
4, 7, 8 and 16 ranks across 4 EC2 nodes — 32/32 clean-run cells passed** (before the fix, 10+
cells failed, up to 9 ranks cascading in one trial).

Two environmental rules governed the cross-host path, and both bit before they were understood.
The restored process keeps its dumped PID, so each node's `/proc/sys/kernel/ns_last_pid` must be
seeded into a disjoint band or the restore fails with `File exists`. And `advertise_host` must
be left empty rather than pinned, so that a rank resolves the address of the machine it is
actually on — necessary, but on its own no longer sufficient: with the checkpoint hook gone a
restored process never re-runs that resolution and keeps advertising the address it was dumped
on. See the last paragraph of [Scope](#scope).

`multihost_ft_migration.py` is left over from the deleted epoch migration protocol: it drives
`fmi-rank-agent evacuate-local` / `restore-remote` / `promote`, none of which exist any more.
It cannot run against this tree.
