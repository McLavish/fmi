# FMI example programs

Standalone, locally runnable FMI applications. Each example is one self-contained `.cpp` file
that builds into its own executable, links directly against FMI, and does exactly what an
ordinary FMI program does: `main()` parses its command line, constructs an `FMI::Communicator`
for one rank, runs an algorithm, checks its own result and exits.

The kernels are ports of the FMI example applications that live in GapRunner
(`/home/luca/GapRunner/functions/cpp`) as rFaaS functions, but nothing of that execution model
survives here: no rFaaS, no cereal, no gRPC, no hook contract and no central driver — just FMI
plus a small shared header (`common/launcher.hpp`) for flag parsing, local multi-rank launching
and the teardown handshake. The one kernel with a different origin is `lulesh`: LLNL's LULESH 2.0
as ported in [McLavish/LULESH_FMI](https://github.com/McLavish/LULESH_FMI), where the unchanged
physics runs behind a small MPI-compatible layer over FMI; here it is squashed into one file.

## Prerequisites

- **Redis** on `127.0.0.1:6379` for the `Redis` backend (and for anything using fault
  tolerance): `redis-server` or `docker run -p 6379:6379 redis`.
- **tcpunchd** on port `10000` for the `Direct` (TCP) backend:

  ```bash
  cmake -S extern/TCPunch/server -B extern/TCPunch/server/build
  cmake --build extern/TCPunch/server/build -j"$(nproc)"
  ./extern/TCPunch/server/build/tcpunchd 10000
  ```

## Building

```bash
cmake -S . -B build-examples -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_REDIS=ON -DFMI_USE_STATIC_BOOST=OFF \
  -DFMI_BUILD_EXAMPLES=ON
cmake --build build-examples -j"$(nproc)"
```

Binaries land in `build-examples/example_programs/<name>` (CMake target
`fmi_example_<name>`). Run them from the repository root so the default config path resolves.

## Program structure

Every example follows the same shape. Abridged from `ring.cpp`:

```cpp
int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);      // line-buffered, so rank output interleaves readably

    fmi_examples::Flags flags;                     // declare the example-specific flags first
    flags.add_int("num-iterations", 1, "Number of ring iterations");

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "ring", flags, opts, exit_code)) {
        return exit_code;                          // --help prints usage (0), a usage error exits 2
    }

    if (opts.ranks < 2) {                          // example-specific guards, before anything forks
        std::cerr << "ring requires at least 2 ranks (rank 0 sends to peer 1)" << std::endl;
        return 2;
    }

    if (!opts.single_rank()) {                     // no --rank given: be the launcher, not a rank
        return fmi_examples::spawn_local(argc, argv, opts);
    }

    bool ok = true;                                // from here on this process is exactly one rank
    try {
        FMI::Communicator comm(opts.rank, opts.ranks, fmi_examples::config_path(),
                               fmi_examples::comm_name(), fmi_examples::faas_memory());
        /* ... the algorithm ... */
        ok = compare(opts.rank, "last_recvd", expected, got);   // in-rank self-check
        fmi_examples::teardown_sync(comm, opts);   // rendezvous + grace, Communicator still alive
    } catch (const std::exception &e) { /* rank R crashed: ... */ ok = false; }
      catch (const std::string &s)    { /* TCPunch throws bare strings */ ok = false; }
      catch (...)                     { ok = false; }

    std::cout << "rank " << opts.rank << ": " << (ok ? "PASS" : "FAIL") << std::endl;
    return ok ? 0 : 1;
}
```

The order matters and is the same everywhere: parse, then guards, then either launch or run,
and the rank body always ends with `teardown_sync` inside the `Communicator`'s scope. Result
checking is in-rank, using `compare(...)` from `common/util.hpp` (`ok &= compare(...)` when an
example checks several fields).

## Examples

Extra flags are example-specific; the defaults below are the ones GapRunner used. All
examples additionally accept the common flags listed further down.

| Example | What it demonstrates | Extra flags (defaults) |
| --- | --- | --- |
| `avg` | `scatter` + `gather` of random numbers, global average | – (seed 42, 1000 elements per rank) |
| `communicating` | Full collective sweep: ring `send`/`recv`, `bcast`, `gather`, `scatter`, `reduce`, `allreduce`, `scan` | – |
| `crashing` | Random rank failure (1% per iteration) during repeated `allreduce` | `--num-iterations` (1000) |
| `ring` | Repeated ring `send`/`recv` around all ranks | `--num-iterations` (1) |
| `jacobi` | 2D Jacobi stencil with halo exchange over a 200x200 grid, 30 timesteps | – (grid `N=200`, `T=30` compiled in) |
| `npb_ep` | NAS Parallel Benchmarks EP kernel with a double `allreduce` | `--m` (28; use 24 locally) |
| `mantevo_hpccg` | Mantevo HPCCG conjugate gradient solver | `--nx` `--ny` `--nz` (64), `--max-iter` (150), `--tolerance` (0.0) |
| `lulesh` | LULESH 2.0 Sedov shock hydrodynamics: 26-neighbour halo exchange (`send`/`recv`) plus a `dt` `allreduce` every cycle | `--size` (30), `--iters` (9999999, i.e. to completion), `--regions` (11), `--balance` (1), `--cost` (1), `--progress` (0), `--expected-energy` (0 = unchecked; 8 ranks at size 30 give `7.130703e+05`) |
| `mixed_workload` | Alternating sleep phases and collectives | `--num-iterations` (5), `--sleep-min` (1), `--sleep-max` (3) |
| `checkpoint_workload` | Iterations of sleep + collectives, sized for checkpoint experiments | `--num-iterations` (1), `--sleep-seconds` (0), `--num-collectives` (0) |
| `long_communicating_checkpoint` | Long `allreduce` loop that validates every iteration | `--num-iterations` (1000) |
| `fixed_size_ckpt_comm` | Fixed-size resident payload plus a long `allreduce` loop | `--size-mb` (0), `--num-all-reduces` (1000000) |
| `fixed_size_ckpt_sleep` | Fixed-size resident payload plus a long sleep | `--size-mb` (0), `--sleep-minutes` (10) |

### Rank-count restrictions

Examples that cannot run at an arbitrary `--ranks` reject the value up front — after parsing but
before any rank is launched — with a message naming the constraint and exit code `2`. The same
guard also rejects an explicit `--rank` process, so a cross-machine run cannot slip past it.

- `jacobi` needs a rank count that factorises into `h * w` with both factors dividing the 200x200
  grid: 1, 2, 4, 5, 8, 10, 16, 20, 25, 32, 40, 50, 64. Other counts (3, 6, 7, …) abort with the
  supported list. Tiles are heap vectors, so 1 and 2 ranks are supported.
- `communicating` and `ring` need at least 2 ranks: their ring phase sends to a hardcoded peer 1,
  which does not exist in a single-rank communicator. Neither has an upper bound beyond the
  launcher's `--ranks` cap of 64.
- `mixed_workload` needs an **even** rank count of at least 2: it pairs rank `i` with rank
  `comm_size-1-i`, so an odd count would make the middle rank its own peer. It also requires
  `0 <= --sleep-min <= --sleep-max`.
- `mantevo_hpccg` requires `--nx`, `--ny`, `--nz` and `--max-iter` to be at least 1, and rejects
  sizes whose row or nonzero counts would overflow an `int`.
- `lulesh` decomposes onto a cubic grid of ranks, so it needs a perfect-cube rank count (1, 8, 27
  or 64 within the launcher's cap) and presets `--ranks` to 8 instead of the usual 4. `--size`,
  `--iters` and `--regions` must be at least 1, `--balance` and `--cost` non-negative.
- `avg`, `crashing`, `checkpoint_workload`, `long_communicating_checkpoint` and the
  `fixed_size_ckpt_*` pair run at any rank count from 1 up.

`npb_ep` requires `--m` greater than 16 (`MK`) and rejects smaller values up front; reference sums
exist only for 24, 25, 28, 30, 32, 36 and 40, so `--m 24` is the cheapest value that still
verifies its result. The kernel itself is cheap on this machine (`RelWithDebInfo`, 4 ranks:
0.28 s at `--m 24`, 4.35 s at the `--m 28` default), but a run that involves the `Direct` backend
can spend far longer in TCPunch rendezvous than in compute — the live-test lane saw `--m 24` on 4
ranks over the mixed config need about `--timeout 360`. Pass a generous `--timeout` for any run
that is not Redis-only; `run_examples.sh` already gives `npb_ep` `--timeout 360`.

`fixed_size_ckpt_*`, `long_communicating_checkpoint` and `crashing` run for a long time with
their GapRunner defaults — always pass smaller values (and `--timeout`) for a local smoke run.

## Common flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--ranks N` | 4 | Number of ranks to launch (hard cap 64) |
| `--rank I` | – | Run only rank `I` in this process, no launcher (cross-machine mode) |
| `--config PATH` | `example_programs/config/fmi_examples.json` | FMI JSON config |
| `--comm-name NAME` | `<example>-<hex>` | FMI communicator name, must agree across ranks |
| `--timeout SECONDS` | 180 | Wall-clock limit the launcher gives the rank processes |
| `--memory MIB` | 128 | `faas_memory` hint, passed to every example's `Communicator`; it scales the cost model's price-per-latency term and can flip which backend an operation uses |
| `--teardown-grace MS` | 1000 | How long each rank holds its channels open after the final rendezvous (see below); `0` disables |
| `--help` | – | Usage |

Values can be written `--flag value` or `--flag=value`, and parsing is last-wins. Declaring an
example flag that collides with one of these names is rejected at startup. Every numeric flag has
a declared range that is enforced while parsing, before the value is narrowed to the type it is
stored in, so an out-of-range value is always a usage error and never wraps into a plausible one.
Exit code is `0` on success (or `--help`), `1` when a rank failed, crashed, timed out or was
signalled, `2` on a usage error or a failed example-specific guard.

## Multi-rank local run

```bash
./build-examples/example_programs/communicating --ranks 4 \
  --config example_programs/config/fmi_examples_redis.json
```

Without `--rank`, the process becomes a launcher rather than a rank: it re-execs
`/proc/self/exe` once per rank with the original argv plus `--rank I --comm-name <name>`
appended, then polls for the children until they all exit or the `--timeout` deadline passes, at
which point survivors are `SIGKILL`ed and reaped. Because the child command line carries
`--rank`, a child always takes the single-rank path — children can never launch further ranks.

Each child prints its own progress (`rank <R>: ...`) and ends with `rank <R>: PASS` or
`rank <R>: FAIL`; a crash inside a rank prints `rank <R> crashed: <detail>` first. The launcher
then prints one wait-status line per rank — `rank <i>: exit=<C>`, `rank <i>: signal=<S>` or
`rank <i>: TIMEOUT (killed)` — and the verdict `EXAMPLE <name>: PASS|FAIL`. It exits `0` only if
every rank exited `0`.

## Single-rank / cross-machine run

Start one process per rank, each with the same `--ranks`, the same `--comm-name` and its own
`--rank`:

```bash
# machine A
./build-examples/example_programs/communicating --ranks 2 --rank 0 --comm-name demo-run \
  --config example_programs/config/fmi_examples_redis.json
# machine B
./build-examples/example_programs/communicating --ranks 2 --rank 1 --comm-name demo-run \
  --config example_programs/config/fmi_examples_redis.json
```

There is no launcher and no fork in this mode: validation is in-rank either way, so each process
checks its own results exactly as it would as a forked child, and its exit code is that rank's
verdict.

A few checks are rank-conditional, and in this mode they run only in the process that owns them
— so the run's verdict is complete only once the owning rank's exit code is included:

- `avg` — rank 0 owns the end-to-end verdict: it compares the average of the gathered per-rank
  averages against the average over the array it scattered. The other ranks only report that
  their own part did not fail.
- `npb_ep` and `mantevo_hpccg` — every rank verifies its own result (EP sums against the
  reference table; a finite residual and the expected iteration count), but the *cross-rank
  agreement* check over the gathered values is evaluated on rank 0 only.
- `lulesh` — every rank checks that it advanced and holds a finite origin energy; the plane-0
  symmetry check, the `--expected-energy` check and the cross-rank agreement of cycle count and
  simulated time are evaluated on rank 0 only.
- `communicating` — the gather result only exists on rank 0, so only rank 0 checks it. Every
  other collective in that example is checked on every rank.

Everywhere else every rank checks its own result, so a single process's exit code is that rank's
full verdict.

## Configs

- `config/fmi_examples.json` (the built-in `--config` default) — Direct (127.0.0.1:10000) and
  Redis (127.0.0.1:6379) enabled, S3 present but disabled, fault tolerance disabled. The cost
  model decides per operation, and with the default hint (`cheap`) and `--memory 128` it picks
  **Direct** for small point-to-point messages — so this config **needs both Redis and tcpunchd
  on port 10000**. It does *not* fall back when a backend is unreachable: without tcpunchd,
  `communicating`, `ring`, `jacobi`, `mantevo_hpccg`, `lulesh` and `mixed_workload` fail with
  `Connection with the rendezvous server failed`.
- `config/fmi_examples_redis.json` — only Redis enabled (no tcpunchd needed).
- `config/fmi_examples_direct.json` — only Direct enabled (needs tcpunchd on port 10000).

### Direct backend flakiness

TCPunch pairing occasionally fails to complete even with a healthy `tcpunchd` (a known trait of
the library, also visible in FMI's own test suite). It shows up as a rank dying in channel setup,
usually reported as a rendezvous or connection error. Rerun the example — the ported kernels
themselves are unaffected once the transport is established. If a rerun keeps failing, restart
`tcpunchd` before looking for a bug in the example.

## Smoke test

```bash
./example_programs/run_examples.sh                                       # Redis config
./example_programs/run_examples.sh example_programs/config/fmi_examples_direct.json
```

Runs the fast examples with small parameters, prints a per-example summary and exits nonzero
if anything failed. `RANKS` (default 4) and `TIMEOUT` (default 120 s, overridden per example
where needed) can be set in the environment.

One built example is deliberately excluded from the suite: `crashing`, because a rank aborts at
random by design and the outcome is therefore non-deterministic. Run it by hand.

## Teardown: why every example ends with `teardown_sync`

`FMI::Comm::ClientServer::finalize()` (`src/comm/ClientServer.cpp`), called from
`~Communicator`, deletes every object the peer uploaded. The Redis and S3 download path is a
plain `GET` that does not consume, so a message has to survive in the store until the receiver
polls it. A rank whose last FMI operation is a send therefore deletes that message microseconds
after writing it, while the receiver polls at millisecond granularity — the receiver then dies
with `Timeout was reached`. For `ring` this is deterministic (the last rank's final act is a
send to rank 0); for the collectives it shows up as intermittent failures.

`fmi_examples::teardown_sync(comm, opts)` is therefore the last statement inside every
`Communicator`'s scope, after validation. It does two things, and both are needed over
`ClientServer`:

1. **A final rendezvous over all ranks**, so no rank starts tearing down before every peer has
   finished its own operations. It is a 1-element sum-allreduce of the token `1`, and
   deliberately *not* `comm.barrier()`. `ClientServer::barrier()` counts arrivals by listing the
   whole store and matching object names by **suffix** only (`_barrier_<n>`), with no
   communicator prefix — and `Redis::get_object_names()` is a literal `KEYS *` over the entire
   database. Any object left behind by an unrelated run or test that died before `finalize()`
   therefore counts as an arrived peer, and on a store holding such leftovers (this machine's
   dev Redis had 63 stale keys ending in `_barrier_0`) the barrier returns on its first poll
   without a single peer having arrived. Measured with a probe that is rank 0 of 4 and the only
   process running: `comm.barrier()` returned after 7 ms, the allreduce rendezvous correctly
   blocked until the backend timeout. Same result end to end, with `checkpoint_workload` in
   cross-machine mode over Redis, ranks 0–2 started immediately and rank 3 only after 8 s: with
   the barrier ranks 0–2 exited after 1.02 s (the grace alone) while rank 3 had not even started;
   with the rendezvous they exited after 9.01 s, i.e. only once every peer had arrived. Allreduce
   is reduce-then-bcast and fetches every object by its exact, communicator-qualified name, so
   only this communicator's own peers can satisfy it.
2. **A `--teardown-grace` sleep** (default 1000 ms) after that rendezvous. The rendezvous alone
   does not quite close the window: its last step is a broadcast from root, and root returns as
   soon as it has uploaded that object, so it can delete the object in `finalize()` while slower
   peers are still polling for it. The grace period covers exactly that race — this last leg is
   irreducible over `ClientServer` (a barrier has the mirror-image version of it), which is why
   the grace is not redundant. Over `Direct` the rendezvous is a real handshake and the grace is
   merely harmless.

   Consequently `--teardown-grace 0` is not free over Redis/S3: losing that race makes each
   affected rank poll for the vanished object until the backend's `max_timeout` (30 s in the
   shipped configs) before giving up. Measured with `ring --num-iterations 2 --teardown-grace 0`
   over Redis: every run still passed, but most ranks printed `note: teardown rendezvous failed:
   Timeout was reached` after ~30 s. Leave the grace at its default unless the run is `Direct`
   only.

The rendezvous is best effort: it runs after validation, and a failure there prints
`note: teardown rendezvous failed: ...` without failing a rank whose actual work succeeded. Its
result is the number of participating ranks, so a count other than `--ranks` prints
`note: teardown rendezvous saw N of M ranks`.

This is a workaround in the examples, not a fix: the underlying defect is in the core library and
also causes flakiness in FMI's own test suite. Anything else built on top of `ClientServer` needs
the same end-of-run synchronization.
