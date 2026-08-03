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

## ClientServer (Redis, S3) data plane

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
Re-checked after the S3 cost guard landed: `--trials 3 --peers 2 --rounds 4000 --seed 3`, 2
passed, 0 failed, 1 skipped (a freeze that landed after the job had finished).

`--shape collectives_sweep` and `--shape noncommutative` are covered since the ordered-reduce
fix. `ClientServer::reduce` used to seed the accumulator with the root's own contribution and
fold the remaining ranks in ascending order — `f(v_root, v_0, …)` instead of `f(v_0, …, v_n-1)`
— correct only for root 0, the only root the `baseline` shape uses, and reproduced on the commit
this work branched from. Both of these shapes assert the fold in absolute rank order under a
rotating root, so their *clean* baselines failed at 4 ranks before any trial ran. The fix parks
the root's contribution in its own slot, exactly as `scan` always did, and brings the store
family into agreement with `PeerToPeer`'s rank-indexed fold — a multi-backend config can no
longer get a different ordered-reduce result depending on which channel the cost model picked.
Verified on the Redis plane after the fix, `--trials 3 --peers 4 --max-checkpoints 2 --seed 1`:
`collectives_sweep` (`--rounds 1000`) 3 passed, 0 failed; `noncommutative` (`--rounds 2000`)
3 passed, 0 failed — clean baselines and mid-collective freezes included.

### The S3 plane

`fmi_s3.json` is the same shape as `fmi_redis.json` — S3 enabled, everything else off, for the
same reason — with `timeout: 100`, `max_timeout: 60000`, `recover: true`. Two values are yours:

```jsonc
"bucket_name": "fmi-criu-sweep-CHANGE-ME",   // a bucket you own; the placeholder fails loudly
"s3_region":   "eu-central-1"
```

Before the first run:

1. **Put a lifecycle expiration rule on the bucket.** This is not optional and it is not a
   nicety: under `recover` nothing is ever deleted by a rank, and S3 has no per-object expiry, so
   `object_ttl_s` cannot do here what it does for Redis. Without a rule every object of every run
   is kept and billed forever. One day is plenty:
   ```bash
   aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
     --lifecycle-configuration '{"Rules":[{"ID":"fmi-sweep","Status":"Enabled",
       "Filter":{"Prefix":""},"Expiration":{"Days":1},
       "AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
   ```
   The channel says the same thing at `warning` every time a recovering S3 channel is built.
2. **Grant `s3:ListBucket`** on the bucket to whatever the ranks run as, not just `GetObject` and
   `PutObject`. Without it S3 answers a GET for a key that does not exist with 403, not 404 — by
   design, so that a missing key cannot be told apart from one you may not read — and under
   `recover` the channel raises on the first poll of the first collective instead of waiting.
3. **Give the ranks a credential provider that refreshes — not a snapshot.** The sweep inherits its
   environment, and a rank that is checkpointed keeps the environment its image captured, forever,
   so a session token that expires mid-run cannot be refreshed from there. `aws configure
   export-credentials --format env` is the shortest route and it is **the wrong one for a sweep**:
   measured here, that snapshot was valid for about 15 minutes, while the invocation below takes
   about 40, so trials began dying at
   `S3: request refused (... HTTP 400, ExpiredToken ...)` a third of the way in and every later
   trial failed instantly. Use a provider that re-resolves instead. The AWS SDK for C++ honours
   `credential_process`, and it can be pointed somewhere harmless rather than at `~/.aws/config`:
   ```bash
   cat > /tmp/fmi-aws-config <<'EOF'
   [profile fmisweep]
   region = eu-central-1
   credential_process = env -u AWS_PROFILE -u AWS_CONFIG_FILE aws --profile default configure export-credentials --format process
   EOF
   export AWS_CONFIG_FILE=/tmp/fmi-aws-config AWS_PROFILE=fmisweep
   unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
   ```
   The `env -u` matters: the helper inherits `AWS_PROFILE`, and without it the process resolves
   into itself and the CLI stops with `credential process resolution detected an infinite loop`.
   Unsetting the three environment variables matters too — the SDK's chain checks the environment
   first, so a stale snapshot there wins over the refreshing profile. **criu images of an S3 rank
   contain the credentials in plain text** — the `sweep/` directory is gitignored, and it is not
   somewhere to leave a session token lying around.

```bash
python3 sweep.py --config fmi_s3.json --trials 12 --peers 2 4 --rounds 100 \
    --print-every 1 --delay-range 0.5 20 --max-checkpoints 2
```

`--print-every 1` is mandatory: the progress check reads a round line logged *after* the freeze,
and at S3 latencies a run of 100 rounds prints too rarely otherwise. `--delay-range 0.5 20` is
likewise sized for a plane where a round is two round trips to a region, not two syscalls.

**`max_timeout` means two different things here**, and the section above is only half the story on
S3. For a peer that is merely late it is still iterations — `max_timeout / timeout` polls, each
paying a round trip — so a rank frozen for two minutes costs its peers nothing but patience. But a
*failing* request on this backend can cost far more wall clock than the interval it is charged:
15 s against an address that drops packets, 65 s against one that accepts the connection and then
says nothing. So a run of consecutive failures is bounded by `max_timeout` **milliseconds** as
well, measured from the request that opened the run and restarted by any answer from the store,
absence included. `60000` is therefore about a minute of a store that is not working, and about
66 s × the poll count of a peer that is simply slow.

**A run costs money, and the sweep says how much before it starts.** It prints the PUT and GET
counts it expects and stops above `--max-cost` (default $1) unless `--yes` is given — the default
`--rounds 40000` is sized for a free plane and is about $52 at eight peers. The invocation above
estimates $0.03.

**Read that estimate as a lower bound, not a budget.** It is low on GETs (it counts one per rank
per round per peer, and the poll loops ask repeatedly), and it is low on PUTs too, despite what
this file used to claim: it models one PUT per rank per round, while a round of the `baseline`
shape issues one write per rank per *collective*. Measured against a real bucket, a 2-rank run
writes 11 objects per round — 5.5 per rank, not 1 — and the count is exactly linear in rounds
with no fixed overhead. (Under `recover` nothing is deleted at finalize, so the objects a run
leaves behind *are* its PUT count, which is how that was measured.) The run above printed
$0.03 and actually spent about $0.16 in PUTs; the two errors do not cancel, since the estimate
also charges every trial at `max(--peers)`.

An S3-compatible endpoint costs nothing and exercises the same code: add `"endpoint_url":
"http://127.0.0.1:9000"` and `"use_path_style": true` to the backend block and point it at MinIO
(`docker run --rm -d -p 9000:9000 minio/minio server /data`, credentials `minioadmin`). Not
evidence for AWS — throttling, redirects and expiring tokens are what a real bucket adds — but it
is where to iterate.

### What is verified for S3, and what is not

Two tiers, and they are not interchangeable. **MinIO on loopback** is where this was iterated and
is where most of the rows come from; **a real bucket in `eu-central-1`** is the evidence that the
plane works against AWS itself, and that is the last block. Baselines for the MinIO rows:
84030/25770 at 60 rounds and 1444200/919800 at 400, 2 ranks.

#### Tier 1 — MinIO on loopback (iteration)

| | |
| --- | --- |
| MinIO, by hand, 2 ranks, rank 1 `criu dump --unprivileged --tcp-close --shell-job` at round 10 of 60 | 3/3 restored; both ranks `DONE` with the baseline checksums, and the frozen rank logged rounds 15 through 55 after its restore |
| MinIO, `sweep.py --config <minio>.json --trials 6 --peers 2 --rounds 120 --print-every 1 --delay-range 0.5 8 --max-checkpoints 2 --seed 5` | 6 passed, 0 failed, 0 skipped |
| MinIO, the same at `--trials 3 --peers 4 --rounds 60 --max-checkpoints 1 --seed 21` | 3 passed, 0 failed, 0 skipped |
| MinIO, store cleanup | after `--trials 4 --peers 2 --rounds 100`, the bucket is empty. The `aws s3 rm --recursive` this replaced deleted **nothing** — 36528 objects left behind by a run that reported clean |
| MinIO, the SDK's threads and sockets across a freeze | the `AwsEventLoop` CRT threads and the curl connection pool survive dump/restore; the SDK's own retry dials a fresh connection, which is what makes the recovery transparent |
| loopback fake endpoint, the failure bounds | `tests/s3_backend.cpp`: a write throttled with 503 is retried rather than abandoned; an operation against a store that never answers ends in `BackendFailure` inside its budget; a refusal and a wrong region are raised at once. Against a blackholed address a `download` gives up in 61.8 s on a 60 s budget (one failing request overshoots, by design), where the same code without those bounds took 120 s against a 3 s budget in the test and would have taken 51 minutes against a 60 s one |

#### Tier 2 — a real bucket in `eu-central-1` (2026-08-03)

All of it in one pass, account `323756936843`, against a bucket that was empty at the start
(`list-objects-v2` → `KeyCount 0`) and carried the lifecycle rule step 1 asks for
(`expire-sweep-objects`, `Expiration` 1 day, `AbortIncompleteMultipartUpload` 1 day).

| | |
| --- | --- |
| **the `S3Backend` suite** | run from `build-s3/tests` with `FMI_S3_TEST_BUCKET=fmi-criu-sweep-323756936843-eu-central-1`, `FMI_S3_TEST_REGION=eu-central-1`, `FMI_S3_TEST_SLOW` unset: **12 cases, 10 run, `*** No errors detected`, 14.3 s**. The 2 not run are the `SLOW`-gated `listing_crosses_page_boundaries` and `batched_delete_beats_one_request_per_object`, MinIO-only by design. The bucket is empty again afterwards — the suite tidies up after itself |
| **by hand, a freeze** | 2 ranks, 40 rounds. Timing first: a 10-round calibration took 13.49 s (1349 ms/round) and the 40-round clean baseline 54.08 s (1352 ms/round) — against a region a round is ~1.35 s, not the milliseconds MinIO on loopback gives. Baseline checksums **50820** (rank 0) / **12780** (rank 1). The freeze run used this file's exact invocation — `criu dump --unprivileged -t PID -D img --tcp-close --shell-job`, reap, `criu restore --unprivileged -D img --tcp-close --shell-job -d` — after round 10: **dump rc=0, restore rc=0**, both ranks `DONE` with the baseline checksums, rank 1 logged rounds **11 through 39** after its restore, 52.8 s wall. The restored rank re-issued its writes to the *same* keys: the checkpointed run left **440 objects, exactly what the un-checkpointed baseline left** |
| **the sweep** | `--trials 12 --peers 2 4 --rounds 100 --print-every 1 --delay-range 0.5 20 --max-checkpoints 2 --seed 1 --max-cost 0.60`. The cost guard printed `S3 request estimate: 4800 PUT + 19200 GET, about $0.03 (12 trials x 4 peers x 100 rounds)` and `--max-cost 0.60` cleared it with no `--yes`. Result **12 passed, 0 failed, 0 skipped, exit 0**, 12:29:40Z → 13:09:28Z, **39 min 48 s**. 7 two-peer and 5 four-peer trials, 20 freezes in total, including the same rank frozen twice (trials 6, 8, 9) and a freeze landing while a peer was itself mid-repair (trials 1, 2, 7, 10, 11). Credentials refreshed transparently throughout, restores included |
| **what the pass cost** | counting every request conservatively: ~29,700 PUT/LIST-class at $0.0054/1k = **$0.160**, ~89,000 GET at $0.00043/1k = $0.038, DELETE free — **about $0.20** for everything (the sweep, the suite, the calibration, the by-hand runs, an aborted first sweep attempt and the listings), against the printed $0.03. Read the estimate paragraph above before sizing a longer run |
| **bucket end state** | `list-objects-v2` → `KeyCount 0`, no `Contents`. Nothing stray left behind |
| what a real bucket still has not shown | no request in any run above was throttled, redirected, or answered with a retryable 5xx — the classification and retry paths are still covered only by the fake endpoint in the suite. Expiring credentials, on the other hand, are now covered the hard way: an earlier attempt at this same sweep died at `S3: request refused (... HTTP 400, ExpiredToken ...)` a third of the way in; see the credential note above |

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
