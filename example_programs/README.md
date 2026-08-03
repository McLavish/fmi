# FMI example programs

Standalone, locally runnable ports of the FMI example applications that live in GapRunner
(`/home/luca/GapRunner/functions/cpp`) as rFaaS functions. GapRunner runs each function on a
remote rFaaS executor and validates the results in a central driver; here the same functions
run as forked local processes, one per rank, driven by a small shared harness
(`common/harness.hpp`) that keeps the GapRunner hook contract
(`get_context` / `free_context` / `initialize_input` / `check_output`) intact.

Each example is a single self-contained executable: no rFaaS, no cereal, no gRPC — just the
FMI library plus the harness.

## Prerequisites

- **Redis** on `127.0.0.1:6379` for the `Redis` backend (and for anything using fault
  tolerance): `redis-server` or `docker run -p 6379:6379 redis`.
- **tcpunchd** on port `10000` for the `Direct` (TCP) backend:

  ```bash
  cmake -S extern/TCPunch/server -B extern/TCPunch/server/build
  cmake --build extern/TCPunch/server/build -j"$(nproc)"
  ./extern/TCPunch/server/build/tcpunchd 10000
  ```

- **OpenCV** (`core`, `imgproc`, `imgcodecs`) only for `apply_blur`; without it that target is
  silently skipped.

## Building

```bash
cmake -S . -B build-examples -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_REDIS=ON -DFMI_USE_STATIC_BOOST=OFF \
  -DFMI_BUILD_EXAMPLES=ON
cmake --build build-examples -j"$(nproc)"
```

Binaries land in `build-examples/example_programs/<name>` (CMake target
`fmi_example_<name>`). Run them from the repository root so the default config path resolves.

## Examples

Extra flags are example-specific; the defaults below are the ones GapRunner used. All
examples additionally accept the common flags listed further down.

| Example | What it demonstrates | Extra flags (defaults) |
| --- | --- | --- |
| `minimal` | Pure compute, no FMI calls — proves the harness itself | – |
| `avg` | `scatter` + `reduce` of random numbers, global average | – (seed 42, 1000 elements per rank) |
| `communicating` | Full collective sweep: ring `send`/`recv`, `bcast`, `gather`, `scatter`, `reduce`, `allreduce`, `scan` | – |
| `crashing` | Random rank failure (1% per iteration) during repeated `allreduce` | `--num-iterations` (1000) |
| `ring` | Repeated ring `send`/`recv` around all ranks | `--num-iterations` (1) |
| `jacobi` | 2D Jacobi stencil with halo exchange over a 200x200 grid, 30 timesteps | – (grid `N=200`, `T=30` compiled in) |
| `npb_ep` | NAS Parallel Benchmarks EP kernel with a double `allreduce` | `--m` (28; use 24 locally) |
| `mantevo_hpccg` | Mantevo HPCCG conjugate gradient solver | `--nx` `--ny` `--nz` (64), `--max-iter` (150), `--tolerance` (0.0) |
| `mixed_workload` | Alternating sleep phases and collectives | `--num-iterations` (5), `--sleep-min` (1), `--sleep-max` (3) |
| `checkpoint_workload` | Iterations of sleep + collectives, sized for checkpoint experiments | `--num-iterations` (1), `--sleep-seconds` (0), `--num-collectives` (0) |
| `long_communicating_checkpoint` | Long `allreduce` loop that validates every iteration | `--num-iterations` (1000) |
| `fixed_size_ckpt_comm` | Fixed-size resident payload plus a long `allreduce` loop | `--size-mb` (0), `--num-all-reduces` (1000000) |
| `fixed_size_ckpt_sleep` | Fixed-size resident payload plus a long sleep | `--size-mb` (0), `--sleep-minutes` (10) |
| `apply_blur` | OpenCV Gaussian blur per rank (needs OpenCV + per-rank input JPGs) | `--input-dir` (`example_programs/apply_blur_inputs`), `--output-dir` (`example_programs/apply_blur_outputs`) |

### Rank-count restrictions

Examples that cannot run at an arbitrary `--ranks` reject the value up front, before any rank is
forked, with a message naming the constraint:

- `jacobi` only accepts rank counts that tile the 200x200 grid into tiles of at most 10000
  doubles: 4, 5, 8, 10, 16, 20, 25, 32, 40, 50, 64. Other counts (including 1, 2 and 3) abort
  with a message listing the supported values.
- `communicating` and `ring` need at least 2 ranks: their ring phase sends to a hardcoded peer 1,
  which does not exist in a single-rank communicator.
- `mixed_workload` needs an **even** rank count of at least 2: it pairs rank `i` with rank
  `comm_size-1-i`, so an odd count would make the middle rank its own peer.
- `communicating` also caps out at 50 ranks (its output buffer holds `2 * ranks` ints).

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
| `--ranks N` | 4 | Number of ranks to fork (hard cap 64) |
| `--rank I` | – | Run only rank `I` in this process, no fork (cross-machine mode) |
| `--config PATH` | `example_programs/config/fmi_examples.json` | FMI JSON config |
| `--comm-name NAME` | `<example>-<hex>` | FMI communicator name, must agree across ranks |
| `--timeout SECONDS` | 180 | Wall-clock limit for the rank processes |
| `--memory MIB` | 128 | `faas_memory` hint, passed to every example's `Communicator`; it scales the cost model's price-per-latency term and can flip which backend an operation uses |
| `--teardown-grace MS` | 1000 | In `--rank` mode only: hold channels open this long before teardown (see below) |
| `--help` | – | Usage |

Values can be written `--flag value` or `--flag=value`. Exit code is `0` on success, `1` when
a rank or a check failed, `2` on a usage error.

## Multi-rank local run

```bash
./build-examples/example_programs/communicating --ranks 4 \
  --config example_programs/config/fmi_examples_redis.json
```

The harness allocates shared (`mmap`) input/output buffers, initializes every rank's input in
the parent, forks one child per rank, waits for all of them (killing stragglers at the
timeout), then validates every rank's output **in the parent** — some examples share mutable
validation context across ranks, so validation has to happen in one process. Output ends with
per-rank status lines and `EXAMPLE <name>: PASS|FAIL`.

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

In this mode the harness does not fork and validation is best effort: only the local rank's
output is checked (checks that need other ranks' buffers will report `FAIL` even when the run
is fine).

## Configs

- `config/fmi_examples.json` (the built-in `--config` default) — Direct (127.0.0.1:10000) and
  Redis (127.0.0.1:6379) enabled, S3 present but disabled, fault tolerance disabled. The cost
  model decides per operation, and with the default hint (`cheap`) and `--memory 128` it picks
  **Direct** for small point-to-point messages — so this config **needs both Redis and tcpunchd
  on port 10000**. It does *not* fall back when a backend is unreachable: without tcpunchd,
  `communicating`, `ring`, `jacobi`, `mantevo_hpccg` and `mixed_workload` fail with
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

Two built examples are deliberately excluded from the suite: `crashing`, because a rank aborts
at random by design and the outcome is therefore non-deterministic, and `apply_blur`, because it
needs OpenCV at build time plus one `input_<rank>.jpg` per rank on disk. Run those by hand.

## Why every example ends with `fmi_examples::rank_barrier()`

`FMI::Comm::ClientServer::finalize()` (`src/comm/ClientServer.cpp`), called from
`~Communicator`, deletes every object the peer uploaded. The Redis and S3 download path is a
plain `GET` that does not consume, so a message has to survive in the store until the receiver
polls it. A rank whose last FMI operation is a send therefore deletes that message microseconds
after writing it, while the receiver polls at millisecond granularity — the receiver then dies
with `Timeout was reached`. For `ring` this is deterministic (the last rank's final act is a
send to rank 0); for the collectives it shows up as intermittent failures.

Every example that builds a `Communicator` therefore calls `fmi_examples::rank_barrier()` as its
last statement, while the `Communicator` is still alive. It is *not* an FMI collective — in
multi-rank mode it is a shared-memory barrier across the forked ranks, bounded by the `--timeout`
deadline so a crashed peer can never hang the run. In `--rank` mode there is no shared memory, so
it instead holds the channels open for `--teardown-grace` milliseconds.

This is a workaround in the examples, not a fix: the underlying defect is in the core library and
also causes flakiness in FMI's own test suite. Anything else built on top of `ClientServer` needs
the same end-of-run synchronization.

## apply_blur

`apply_blur` needs OpenCV at build time and one input JPG per rank on disk. Place them where
the ported `initialize_input` expects them (per-rank `input_<rank>.jpg`) and make sure the
output directory exists; without OpenCV the target is not built at all.
