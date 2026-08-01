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

This file's sweep is single host. The restored rank re-uses the listening socket and advertised
address the image captured, which is correct on the same machine and wrong on any other;
cross-host restore needs the rank to re-publish its registry entry, which the migration runtime
does and this sweep does not exercise.

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

`multihost_ft_migration.py` drives the FT-managed cross-host path over ssh (the plain-VM mirror
of `runbooks/k8s-criu-node-evacuation`): `evacuate-local` on the source node, `restore-remote`
on the destination, one `promote`, DirectTCP data plane throughout.
