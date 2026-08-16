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

`sweep.py` is single host. Cross-host restore needs more than the image: the rank must
re-publish its registry entry under the address of the machine it actually woke up on, and
the transport state the image carried — listener, in-flight connects, registry client — all
referred to the machine it left. The library now detects the relocation itself: DirectTCP
remembers the kernel boot id of the machine its transport was built on, and an establishment
that finds a DIFFERENT boot id performs exactly the reset the deleted `prepare_for_checkpoint`
hook used to perform (listener, pending links, peer sockets, registry client, cached
advertised address — all dropped and rebuilt), plants a reconcile debt on every severed link
so the re-formed connections replay, and republishes. Same-host restores see the same boot id
and stay on the proven no-reset path. `--restore next|random` in `multihost_sweep.py` is
backed by this; see the multi-host evidence below.

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
actually on. The resolution is re-run at every registry publish (never downgrading a working
address to the loopback fallback), and the boot-id relocation reset re-triggers the whole
listener/advertisement rebuild on a machine change — see the last paragraph of
[Scope](#scope).

`--evacuate` moves whole machines: each checkpoint event picks a node, dumps EVERY live rank
it hosts, and restores all of them on the `--restore` destination. Cross-host evidence on
this tree, all with `--restore next`: the traced reproducing schedule 6/6, then whole-node
evacuations across six blocks — `baseline` at 4 ranks (4/4), at 8 ranks with two ranks moved
per cut (6/6), double evacuations in one trial (4/4), `deep_rounds` (4/4),
`variable_payloads` with 2 MiB frames mid-flight (4/4), `p2p_ring` (4/4) — **32/32, with the
same-host sweep (12/12) and the clean-run matrix re-verified on the same build**. Getting
there surfaced and fixed, in order: dialers parked in a SYN backlog nobody serviced, a
publish-once latch a frozen establishment restored as already-published, orphaned links
(dead fd with unpaid debts) invisible to the aged redialer, redial budgets shorter than a
replaying listener's accept latency, and handshake deaths escaping to the application —
each one traced, fixed, and re-verified against its own failing schedule.

`multihost_ft_migration.py` is left over from the deleted epoch migration protocol: it drives
`fmi-rank-agent evacuate-local` / `restore-remote` / `promote`, none of which exist any more.
It cannot run against this tree.

## Sequenced-protocol parity campaign: four machines, scenario for scenario against the drain

`runbooks/drain-migration/README.md` records a cluster campaign for the neighborhood-drain
protocol — 130 migrations across sequential moves, single-cut batch evacuations and LULESH.
This section is that **same scenario matrix run against the sequenced protocol**: `DirectTCP`
with `"framed": true` and `"recover_links": true`, the retention/replay transport this runbook
has always been about. The question it settles is one sentence: *both protocols should be able
to pass the same tests.*

The scenarios transfer. **The acceptance criteria do not, and must not.** The drain campaign's
central criterion is that a migrating rank's criu image contains **zero sockets** — it drains
and half-closes every link before it is frozen, so a socket in the image is a protocol failure.
The sequenced protocol is the opposite trade-off by construction: it makes no arrangement with
anybody, its image **carries its sockets by design**, `criu --tcp-close` drops them on restore,
and correctness comes from the frames the peers had not yet acknowledged being retained and
replayed across the break. So this campaign runs criu **with** `--tcp-close --shell-job` where
the drain campaign forbids exactly those flags, and asserts nothing about sockets, seals,
leases, epochs or event trails — none of which exist here.

Both campaigns run criu **privileged** (`sudo`, never `--unprivileged`), and for the same
reason: only a privileged restore lands in a time namespace that preserves `CLOCK_MONOTONIC`.
The sequenced protocol's deadlines are `steady_clock` too, so an unprivileged cross-host
restore onto a machine with a different uptime expires all of them at once. This campaign is
the first evidence for that on this transport: at the time it ran, the host had been up 54
minutes longer than the three nodes, and not one of the 103 migrated ranks saw a `Timeout`.

### Acceptance criteria, per trial

1. every rank reaches `DONE` with **exactly** the four-machine clean-run baseline checksum for
   that (shape, peers, rounds, ms, payload) — the replay working *is* the correctness
   mechanism, and the checksum is its oracle;
2. every checkpointed rank logs a round **past** the round it had reached before its dump,
   that round read on the machine that owns the log rather than over NFS;
3. both criu legs return **0**, privileged, with `--tcp-close --shell-job
   --manage-cgroups=ignore`;
4. the rank **actually changed machine** — a restore whose destination came out equal to the
   source is a trial failure, not a quiet pass;
5. the restored pid still belongs to the band of the machine the rank was **launched** on
   (criu restores the pid verbatim and keeps it for the life of the process, so after a hop the
   pid no longer matches the band of the machine it is sitting on — which is what makes it
   evidence the process was *moved* rather than started there);
6. no `MISMATCH`, `terminate called`, `Segmentation` or `stack smashing` in any rank's log.

There is deliberately **no socketless assertion and no counter cross-check**. The drain's
pairwise sealed counters are evidence available only because a drain seals; inventing an
analogue here would be inventing evidence.

### Setup

Four Rocky 9.8 machines — criu-testing `10.164.0.3` (T, 8c), criu-node-2 `.4` (N2), criu-node-1
`.5` (N1), criu-node-3 `.6` (N3) — one shared `/scratch/fmi` (xfs on T, NFS on the other
three), cluster Redis on `10.164.0.3:6380`, criu 3.19 under `sudo`, kernel
`5.14.0-687.31.1+2.1.el9_8_ciq`, pid bands 1.0M/1.5M/2.0M/2.5M. 2026-08-16, tree `68287e3`,
subject sha256 `46054d3e…`, config `/scratch/cfg/fmi_sequenced_multihost.json` (DirectTCP alone,
`framed` and `recover_links` true, registry `10.164.0.3:6380`, `bind_host 0.0.0.0`,
**`advertise_host ""`**, `max_timeout` 60000). Node order for round-robin is T, N2, N1, N3, so
4 ranks is one per machine, 8 is two, 12 is three.

### Evidence — phases B and C

**65 trials, 65 passed, 0 failed, 0 skipped, 0 void — 103 cross-host migrations: 45 sequential
and 58 inside 29 cuts. Every `criu dump` and every `criu restore` returned 0; every migrated
rank was dumped on one machine and restored on another; every rank of every trial finished with
the four-machine baseline checksum.** The four-machine baselines are themselves bit-identical to
single-host runs of the same parameters — the B0 row.

| scenario | what it moves | trials | migrations | dump/restore rc | checksums | window / cut |
| --- | --- | --- | --- | --- | --- | --- |
| B0 clean, 8 shapes | nothing | 8/8 clean | — | — | all 8 shapes **identical to the single-host oracle**, rank for rank | — |
| B1 one rank N2→N3 | 1 rank | 6/6 | 6 | 0 / 0 | == 4-machine baseline | 0.73 s |
| B2 same rank chained N2→N3→N1→N2 | 1 rank ×3 | 3/3 | 9 | 0 / 0 | == 4-machine baseline | 0.73 s |
| B3 two ranks in sequence (r0 T→N3, r2 N1→T) | 2 ranks | 4/4 | 8 | 0 / 0 | == 4-machine baseline | 0.75 s |
| B4 rank 0 — the rank everyone dials (baseline, p2p_ring) | 1 rank | 6/6 | 6 | 0 / 0 | == 4-machine baseline | 0.75 s |
| B5 8 ranks, N1 evacuated sequentially onto T and N2 | 2 ranks | 3/3 | 6 | 0 / 0 | == 4-machine baseline | 0.75 s |
| B6 mid-message, 8 ranks (variable_payloads, 4096 ints) | 1 rank | 3/3 | 3 | 0 / 0 | == 4-machine baseline | **0.86 s** |
| B7 deep_rounds / uneven_participation / mixed_p2p_collective | 1 rank ×3 shapes | 6/6 | 6 | 0 / 0 | == 4-machine baseline | 0.74 s |
| B8 12 ranks, 3 per machine | 1 rank | 1/1 | 1 | 0 / 0 | == 4-machine baseline | 0.72 s |
| C0 one-rank-node evacuation (k=1) | 1 rank ×3 | 3/3 | 3 | 0 / 0 | == 4-machine baseline | 0.86 s |
| C1 evacuate N1 k=2 → one survivor (T) | 2 ranks ×3 | 3/3 | 6 | 0 / 0 | == 4-machine baseline | 1.77 s |
| C2 evacuate N1 k=2 → spread (T, N2) | 2 ranks ×5 | 5/5 | 10 | 0 / 0 | == 4-machine baseline | 1.74 s |
| C3 12 ranks, k=3 spread (T, N2, N3) | 3 ranks ×3 | 3/3 | 9 | 0 / 0 | == 4-machine baseline | 2.58 s |
| C4 **two machines (N1+N3), k=4, one cut** → T, N2 | 4 ranks ×2 | 2/2 | 8 | 0 / 0 | == 4-machine baseline | **3.42 s** |
| C5 k=2 mid-message (variable_payloads, 4096 ints) | 2 ranks ×3 | 3/3 | 6 | 0 / 0 | == 4-machine baseline | 2.02 s |
| C6 ring neighbours co-evacuated (p2p_ring / mixed) | 2 ranks ×2 shapes ×2 | 4/4 | 8 | 0 / 0 | == 4-machine baseline | 1.76 s |
| C7 two cuts back to back (N1, then N3) | 2 ranks ×2 cuts ×2 | 2/2 | 8 | 0 / 0 | == 4-machine baseline | 1.73 s |
| C8 negative: second batch refused | — | **skipped** | — | — | — | no sequenced equivalent |

**C6 and C4 are where the sequenced protocol is genuinely stressed.** Round-robin places rank
*r* with rank *r+4* and therefore never co-locates ring neighbours; C6 uses an explicit
placement map to put ranks 2 and 3 on the evacuated machine, so the two migrators hold unacked
frames **to each other** and both images are taken before either is restored. The replay has to
reconcile both directions of a link whose two ends were frozen at different instants. C4 does
the same at k=4 across two machines at once.

### What the reconnect actually does, traced

`FMI_LINK_TRACE=1` on a B2-shaped run (rank 1 cut three times, N2→N3→N1→N2, 4 ranks, 2000
rounds) — the sequenced answer to the drain's `leaving → sealed → restored` event trail:

| | |
| --- | --- |
| **every reconnect is the same lineage** | **18 `reconcile same-lineage` handshakes, 0 `RESET_STREAM`.** A peer that had been replaced rather than restored would reset the stream; none did. The incarnation is `0` throughout, which is what "a restored process keeps the one its image was taken with" means when nothing has yet claimed a second one |
| **the replay is not vacuous** | 6 of the 18 handshakes found the peer's expected sequence *behind* what this side had sent, and replayed the difference — **6 frames replayed** across the three cuts. The rest had nothing outstanding, which is the other half of point 3 at the top of this runbook: retention is released on links the peer never writes back to |
| **the sequences carry across all three cuts** | one link reconciled three times, `my_send` 610 → 1340 → 1728 and `my_recv` 915 → 2010 → 2592. The counters are cumulative across every hop, exactly as the drain's `sent.1` is |
| **nothing else fired** | 0 `sequence gap`, 0 `is at its retention limit`, 0 `identity mismatch`, 0 incarnation-fence errors across all four ranks |
| the run itself | all four ranks `DONE` with the baseline checksums `40171000 / 26207000 / 28413000 / 30621000` |

### Wall clock

| | |
| --- | --- |
| one rank: dump | **0.15–0.26 s** over all 48 single-rank migrations (ssh round trip included) |
| one rank: restore | 0.15–0.31 s |
| one rank: the whole window, dump start → restored and running | min 0.59 s, **median 0.72 s**, max 0.86 s |
| a cut, first dump → last rank running | 0.84–0.86 s at k=1 (3), 1.67–2.02 s at k=2 (19), 2.56–2.58 s at k=3 (3), **3.41–3.42 s at k=4** (2) |
| what a migration costs the job | 4-machine clean 75.0 s at 4 ranks vs 75–76 s with one migration; B2's three chained cuts in one job cost about 2 s in total |
| the whole B+C matrix | 22 invocations, 65 trials, **1 h 36 min** of measured block time (B0's 16 runs a further 28 min) |

Two things this table says that the drain's does not. A cut costs **~0.85 s per rank, strictly
linear in k**, because the sequenced cut is nothing but *k* dumps then *k* restores — there is
no seal to wait for and no lease to take, so the constant the drain pays per migration
(`leaving → sealed`, 0–20 ms, plus the coordinator round trips) is simply absent. And the
window is **a quarter shorter than the drain's**: 0.72 s median against 0.94 s for a single
rank, and 3.41-3.42 s at k=4 against 3.71-3.74 s.

The other side of that ledger is per-message cost, and it is the reason the two protocols exist.
Measured on this cluster at 4 ranks, one per machine, `ms=1`, in ms per round — sequenced
`DirectTCP` against the drain campaign's `DrainTCP` figures for the same shapes:

| shape | DrainTCP | sequenced | ratio |
| --- | --- | --- | --- |
| baseline | 1.44 | 1.86 | 1.3× |
| p2p_ring | 1.10 | 1.21 | 1.1× |
| collectives_sweep | 1.55 | 2.07 | 1.3× |
| mixed_p2p_collective | 1.35 | 1.73 | 1.3× |
| noncommutative | 1.23 | 1.57 | 1.3× |
| **deep_rounds** | 41.8 | **124.6** | **3.0×** |
| variable_payloads | 44 | 40.0 | 0.9× |
| uneven_participation | 102 | 93.3 | 0.9× |

`deep_rounds` is hundreds of very short messages per round, and it is where the per-frame header
and the retention bookkeeping stop being amortised — three times the cost. The shapes that move
large payloads or spend their time idle (`variable_payloads`, `uneven_participation`) do not
notice the difference at all. That is the trade-off stated numerically: the drain pays nothing
per message and everything at the migration event; the sequenced protocol pays per message and
almost nothing at the event. (The two columns were measured in different sessions on the same
cluster, so read the ratios rather than the absolute difference.)

### Harness extensions

`multihost_sweep.py` chose its target rank, its instant and its destination at random, which is
right for a sweep and cannot express a scenario matrix. Twelve changes, all additive — every
previously valid invocation behaves exactly as before. **They are working-tree changes, logged
here and synced to the shared tree, not committed.**

| change | why the matrix needed it |
| --- | --- |
| `--target-rank R [R ...]` | scripted target per event, positionally; also fixes the event count. `--target-rank 1 1 1` is the same rank cut three times (B2); `--target-rank 0` is rank 0, the rank everyone dials (B4) |
| `--restore-to NODE [NODE ...]` | explicit destination per event (index into `--nodes`, or a hostname), so a scenario can say "N2 → N3" rather than "somewhere else" |
| `--evacuate-node NODE [NODE ...]` | which machine each cut empties, positionally. **An element may be comma-joined (`2,3`)**, which empties TWO machines in ONE cut — every rank of both dumped before any is restored. The biggest addition, and what C4 is |
| `--restore spread` | a fourth destination policy: each rank of one event lands on a **different** survivor. `next`/`random` send a whole cut to one place |
| `--place R:NODE,...` | explicit placement. Round-robin places rank *r* with rank *r+len(nodes)* and therefore **never** co-locates ring neighbours, which is exactly what C6 needs |
| `--exact-checkpoints` | perform exactly `--max-checkpoints` events rather than a random 1..N |
| pid-band assertion (`--pid-bands`) | every launched pid must fall in its node's `ns_last_pid` band. A rebooted node becomes a refused setup error at second zero instead of a mid-campaign restore failure |
| cross-host move check | a cross-host policy whose destination came out equal to the source is now a trial **failure**; it used to log `(same host)` and pass. The restored pid is checked against its **launch** host's band |
| `last_round_remote` | the pre-dump round is read on the machine that owns the log. Over NFS it read `-1` while the file was open, which made "logged a round past that one" a tautology |
| `evacuate_node` generalised | it computed one destination for the whole set from a single source node; it now carries each rank's own source, which is what makes both multi-node cuts and `spread` possible |
| `registry_del` port fix | it ran `redis-cli` with **no `-p`**, so with a campaign registry on 6380 every "cleanup" deleted a key on 6379 and left the real one behind |
| `SetupError` → exit 3 | a setup violation aborts the invocation and is never scored as a protocol verdict |

Plus one instrumentation line: each migration logs `[timing dump=… restore=… window=…]` and
each cut `[timing k=… dumps=… cut=…]`, which is where the wall-clock table comes from.

Two of these were found by the campaign failing, and both were the driver rather than the
library. The pid-band check first used the *previous* host as "home", so B2's chained hops
reported three false failures — criu keeps the pid, so after one hop it belongs to the launch
host's band and to no other, and a rank chained back to where it started legitimately has a pid
inside its destination's band. And the pre-dump round read over NFS returned `-1` for every
trial of the first B1 block. Both were fixed and the whole of phase B re-run from the top, so
the table above is one harness throughout.

### What was skipped, and why

**C0 (batch of one) — replaced, not skipped.** The drain's C0 isolates the *migrate-event* path
from concurrency: a batch containing a single rank, under the driver-held lease. There is no
event and no lease here, so a "batch of one" is not a thing that exists. The nearest sequenced
shape is a **one-rank-node evacuation** — the cut code path at k=1 — and that is what the C0 row
runs, three times.

**C8 (lease refusal) — genuinely skipped, no sequenced equivalent.** The drain's C8 asserts that
a second batch lease taken while a cut is in flight is refused and the job still finishes. The
sequenced protocol has no lease, no batch identity and no coordinator to refuse anything; a
second concurrent cut is simply two more dumps. There is nothing to assert that C7 (two cuts
back to back) does not already cover, and inventing an assertion here would be inventing
evidence.

### Invocations

```bash
cd /scratch/fmi/runbooks/criu-transparent-checkpoint
NODES="10.164.0.3 10.164.0.4 10.164.0.5 10.164.0.6"      # T, N2, N1, N3
CFG=/scratch/cfg/fmi_sequenced_multihost.json

# B0 — the four-machine clean oracle for one shape (and the single-host one to compare it to)
python3 multihost_sweep.py --nodes $NODES --config $CFG --trials 1 --max-checkpoints 0 \
        --peers 4 --rounds 40000 --ms 1 --shape baseline
python3 multihost_sweep.py --nodes 10.164.0.3 --config $CFG --trials 0 \
        --peers 4 --rounds 40000 --ms 1 --shape baseline

# B2 — the same rank cut three times, N2 -> N3 -> N1 -> N2
python3 multihost_sweep.py --nodes $NODES --config $CFG --trials 3 --peers 4 --rounds 40000 \
        --ms 1 --restore next --target-rank 1 1 1 --restore-to 3 2 1 --delay-range 2 8

# C4 — two machines emptied in ONE cut, k=4, onto the other two
python3 multihost_sweep.py --nodes $NODES --config $CFG --trials 2 --peers 8 --rounds 35000 \
        --ms 1 --evacuate-node 2,3 --restore spread --seed 2

# C6 — ring neighbours co-evacuated (round-robin never produces this placement)
python3 multihost_sweep.py --nodes $NODES --config $CFG --trials 2 --peers 8 --rounds 53000 \
        --ms 1 --shape p2p_ring --place 0:0,1:1,2:2,3:2,4:3,5:0,6:1,7:3 \
        --evacuate-node 2 --restore spread
```

Rounds are sized for a ~75 s job, which is the window a randomly-timed cut needs to land well
inside the run. Measured here at 4 ranks, `ms=1`: baseline 40000, p2p_ring 62000,
collectives_sweep 36000, mixed_p2p_collective 43000, noncommutative 48000, deep_rounds 600,
variable_payloads 1850, uneven_participation 800; at 8 ranks baseline 35000 and at 12 ranks
32000.

### Phase D — LULESH across four machines

The capstone, on a **real application** instead of the synthetic subject, and the direct
counterpart of the drain campaign's phase D. LULESH 2.0 ported to FMI (`McLavish/LULESH_FMI`,
upstream sources unchanged, all integration in the `lulesh-fmi.{h,cc}` MPI shim), 8 ranks,
2 per machine in a `block` placement — r0,r1@T r2,r3@N2 r4,r5@N1 r6,r7@N3 — `-s 30`, run to
completion at 2031 cycles. Its 2×2×2 decomposition makes every rank every other's neighbour, so
24 of the 28 links cross a machine boundary, and every cycle is one global `dt` allreduce plus
three 26-neighbour halo exchanges: the traffic being cut is a real application's, in the tens of
megabytes per link.

Its oracle is stronger than a checksum: rank 0's `Final Origin Energy` is bit-deterministic for
a given (binary, size, rank count), so a migration that lost or duplicated a single halo byte
moves it. The number to match is the single-host golden **`7.130703e+05`**.

The driver is `LULESH_FMI/lulesh-multihost-sequenced.py`, and like its drain sibling it
**imports** the campaign machinery rather than restating it — `multihost_sweep.py`'s `Cluster`,
`checkpoint_restore`, `evacuate_node`, `destinations`, the pid-band assertions and the
cross-host move check — so a phase-D verdict is produced by the same code as a phase-B or
phase-C one, criu flags included. What it adds is only LULESH: the four FMI parameters arrive
through the environment (`FMI_RANK`, `FMI_WORLD_SIZE`, `FMI_CONFIG`, `FMI_COMM_NAME`) rather
than argv, `stdbuf -oL` so the per-cycle counter is readable while the job runs, rank 0's
`cycle = N` replaces the subject's per-rank round counter as the progress reading, and the
finish test is every rank's *process* being gone (ranks other than 0 print nothing at all in a
healthy run).

**11 trials, 11 passed, 0 failed — 23 cross-host migrations, 9 of them single and 14 inside 5
cuts, every one dumped on one machine and restored on another. All eleven runs printed
`Final Origin Energy = 7.130703e+05` and `Iteration count = 2031`.** 2026-08-16, FMI tree
`68287e3`, `lulesh2.0` sha256 `3645f753…` (Release, `-O3`, `WITH_OPENMP=OFF`), config
`fmi-lulesh-seq-multihost.json` (DirectTCP alone, `framed` and `recover_links` true, registry
`10.164.0.3:6380`, `advertise_host ""`, `max_timeout` 60000).

| scenario | what it moves | energy | cycles | migrations | dump/restore rc | job |
| --- | --- | --- | --- | --- | --- | --- |
| D0 clean, 4 machines | nothing | `7.130703e+05` **== single-host golden** | 2031 | — | — | 31 s |
| D1a one rank, N2→N3 | r3 | `7.130703e+05` | 2031 | 1 | 0 / 0 | 39 s |
| D1b one rank, N3→T | r6 | `7.130703e+05` | 2031 | 1 | 0 / 0 | 32 s |
| D1c one rank, T→N1 — **rank 0 itself** | r0 | `7.130703e+05` | 2031 | 1 | 0 / 0 | 39 s |
| D2a three sequential, three ranks (r2 N2→T, r5 N1→N2, r7 N3→N1) | 3 ranks | `7.130703e+05` | 2031 | 3 | 0 / 0 | 33 s |
| D2b three sequential, rank 0 first (r0 T→N3, r3 N2→N1, r6 N3→T) | 3 ranks | `7.130703e+05` | 2031 | 3 | 0 / 0 | 39 s |
| D3a single cut, N2 evacuated → spread (T, N1) | 2 ranks | `7.130703e+05` | 2031 | 2 | 0 / 0 | 42 s |
| D3b single cut, N1 evacuated → spread (T, N2) | 2 ranks | `7.130703e+05` | 2031 | 2 | 0 / 0 | 39 s |
| D3c single cut, **T** evacuated → spread (N2, N1) | 2 ranks | `7.130703e+05` | 2031 | 2 | 0 / 0 | 42 s |
| D4a **two machines, k=4, one cut**: N1+N3 → T, N2 | 4 ranks | `7.130703e+05` | 2031 | 4 | 0 / 0 | 41 s |
| D4b **two machines, k=4, one cut**: T+N2 → N1, N3 | 4 ranks | `7.130703e+05` | 2031 | 4 | 0 / 0 | 47 s |

Timings on a real application's traffic: a single migration's window 0.66–0.78 s (dump
0.18–0.25 s, restore 0.17–0.29 s); a cut 1.78–1.95 s at k=2 and **3.54–3.61 s at k=4**. The
whole phase, eleven jobs, took 8 minutes. Against the drain campaign's LULESH figures — 0.93–1.04 s
per single migration, 3.79–3.86 s at k=4 — the sequenced protocol is again the faster event, and its
cut still scales at about 0.88 s per rank.

### Drain vs sequenced, scenario by scenario

Both protocols were put through the same matrix on the same four machines with the same subject
and the same application. Neither failed a scenario it was asked to run.

| scenario | drain: what it asserts | drain | sequenced: what it asserts | sequenced |
| --- | --- | --- | --- | --- |
| clean, 8 shapes | checksums == single-host oracle | 8/8 | same | 8/8 |
| one cross-host cut | socketless image, `leaving→sealed→restored`, checksums | 6/6 | image **carries** its sockets, replay reconciles, checksums | 6/6 |
| same rank chained ×3 | epochs 0,1,2; counters carry across the hops | 3/3 (10 migr) | lineage stays the same at every hop, sequences carry, replay reconciles | 3/3 (9 migr) |
| two ranks in sequence | as above, second migrator's peer has already moved | 4/4 | same, minus the event trail | 4/4 |
| rank 0 (baseline, p2p_ring) | as above | 6/6 | as above | 6/6 |
| sequential whole-node evacuation | as above, 2 ranks one at a time | 3/3 | as above | 3/3 |
| mid-message (variable_payloads) | seal cuts between chunks | 3/3 | freeze lands mid-message; retention/replay covers it | 3/3 |
| deep / uneven / mixed shapes | as above | 6/6 | as above | 6/6 |
| 12 ranks | scale | 1/1 | scale | 1/1 |
| batch of one / k=1 cut | the migrate-event path in isolation, under the lease | 3/3 | **replaced**: a one-rank-node evacuation (there is no event or lease) | 3/3 |
| evacuate one node k=2 → one survivor | all sealed before any restore; pairwise counters | 3/3 | all dumped before any restore; checksums | 3/3 |
| evacuate one node k=2 → spread | as above | 5/5 | as above | 5/5 |
| 12 ranks, k=3 | as above | 3/3 | as above | 3/3 |
| **two machines, k=4, one cut** | as above at full width | 2/2 | as above at full width | 2/2 |
| k=2 mid-message | counter cross-check is the byte-exactness oracle | 3/3 | checksums are the byte-exactness oracle | 3/3 |
| ring neighbours co-evacuated | the mutual seal, cross-host | 4/4 | **mutual retention**: both images hold unacked frames for each other; replay reconciles both directions | 4/4 |
| two cuts back to back | the lease race guard between them | 2/2 | no lease to race; the second cut is simply two more dumps | 2/2 |
| negative: second batch refused | the lease is refused and the job survives | 1/1 | **no equivalent — skipped** (no lease, no batch identity, nothing to refuse) | — |
| LULESH, 4 machines | `Final Origin Energy` bit-identical + socketless | 11/11, 23 migr | `Final Origin Energy` bit-identical | 11/11, 23 migr |
| **totals** | | **69 trials, 130 migrations** | | **76 trials, 126 migrations** |

Where they differ is not pass or fail but cost and what each demands of its environment.

| | drain (`DrainTCP`) | sequenced (`DirectTCP` framed + recover_links) |
| --- | --- | --- |
| criu flags | **no** `--tcp-close`, `--tcp-established` or `--shell-job` — the image is socketless and that is the acceptance criterion | `--tcp-close --shell-job` — the image carries its sockets **by design** |
| per-message cost | zero added bytes; a raw byte stream | a frame header per message plus retention until acknowledged: 1.1–1.3× on most shapes, **3.0× on `deep_rounds`** |
| cost at the event | a drain, a seal, coordinator round trips and a lease; window median **0.94 s** | *k* dumps and *k* restores; window median **0.72 s**, and a cut is strictly ~0.85 s per rank |
| what the application must arrange | `trigger: control` on every rank, a Redis stream coordinator, a batch lease for concurrent cuts | nothing whatsoever — the migration is unannounced and external |
| the survivor's patience | `max_timeout` is **suspended** per link for the migration; `migration_max_ms` bounds it instead | no suspension: the cut must complete inside plain `max_timeout` (60 s here against cuts of 0.85–3.6 s) |
| unplanned death | a loud error by design — planned migration only, never fault tolerance | the same: a break with no reconnect is a loud error, not a recovery |
| what it needs privileged criu for | a time namespace preserving `CLOCK_MONOTONIC` across hosts | the same, for the same reason |

The short version: **both protocols pass the same tests.** The drain buys a socketless image and
a survivor bound that a long freeze cannot exhaust, and pays for it with a control plane the
application has to participate in and a per-event cost. The sequenced protocol buys total
transparency — nothing outside the process needs to know a migration is happening — and pays for
it in bytes on every message and in images that carry sockets.
