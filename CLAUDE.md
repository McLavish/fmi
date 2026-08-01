# CLAUDE.md

This file provides guidance to agents when working with code in this repository.

## What this is

FMI (FaaS Message Interface) is a C++17 library (plus a Boost.Python binding) that gives
serverless/distributed applications an MPI-like API for point-to-point and collective
communication. Its distinguishing feature is **model-driven channel selection**: the same
logical operation can run over different transport backends (direct TCP, S3, Redis), and a
cost/performance model picks the backend per operation based on message size and an
optimization hint (`fast` vs `cheap`). It also adds one Redis-coordinated transparent
migration protocol on top of the base messaging layer.

The canonical working tree on this machine is `/home/luca/fmi`. Note that some checked-in
runbooks (e.g. `runbooks/aws-python311-s3/README.md`) reference paths like
`/home/luca/fmi-original/fmi/...` or instruct `cd fmi` — those reflect other deployment
layouts; here the repo root *is* `/home/luca/fmi`.

## Building

Always init submodules first — `extern/TCPunch` (the `Direct` backend depends on it) and
`docs/doxygen-awesome-css` are git submodules:

```bash
git submodule update --init --recursive
```

`extern/TCPunch` points at the `McLavish/TCPunch` fork, which carries two fixes the FT
runbooks rely on: `tcpunchd` ignores SIGPIPE instead of crashing, and the client resolves
the rendezvous host via `getaddrinfo` (upstream accepted only literal IPs, so K8s Service
DNS names failed).

Standard full build (with tests):

```bash
cmake -S . -B build -DFMI_BUILD_TESTS=ON
cmake --build build -j"$(nproc)"
```

CMake options (defaults in `CMakeLists.txt`): `FMI_ENABLE_S3` (ON, pulls AWS SDK for C++),
`FMI_ENABLE_REDIS` (ON, pulls hiredis), `FMI_ENABLE_CRIU` (OFF, experimental CRIU
checkpoint/restore raw material), `FMI_ENABLE_TCPUNCH` (ON, builds the `Direct` backend and
links the TCPunch submodule — turn it OFF on platforms with direct peer connectivity and the
submodule is not needed at all), `FMI_USE_STATIC_BOOST` (ON), `FMI_BUILD_TESTS` (OFF),
`FMI_BUILD_TOOLS` (ON when top-level, OFF when consumed via `add_subdirectory`). The library
target is `FMI` (alias `FMI::FMI`), built STATIC, and publishes its public headers via
`include/` (exported to a parent scope as `FMI_INCLUDE_DIRS`).

### Local Direct-only debug build (no AWS/Redis)

This is the fastest dev loop and the path verified on Ubuntu 24.04. It disables the S3 and
Redis backends so you don't need AWS SDK or hiredis, and builds the Python module too:

```bash
cmake -S extern/TCPunch/server -B extern/TCPunch/server/build-debug
cmake --build extern/TCPunch/server/build-debug -j"$(nproc)"

cmake -S python -B python/build-native-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DPython3_EXECUTABLE="$(command -v python3)" \
  -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_REDIS=OFF -DFMI_USE_STATIC_BOOST=OFF
cmake --build python/build-native-debug -j"$(nproc)"
```

### Python package management

Use **`uv`** for all Python package management and interpreter handling — `uv pip ...`,
`uv venv`, `uv python install/find` — rather than bare `pip`/`venv`/`virtualenv`. When a
build or runbook needs a specific interpreter, resolve it through uv (e.g.
`uv python find 3.12`) and pass it to CMake via `-DPython3_EXECUTABLE=...`. The one
exception is the Boost.Python version-matching constraint below: the interpreter uv selects
must still match the Boost.Python build (see the gotcha).

### Python binding gotcha

`python/` is its own CMake project that `add_subdirectory`'s the repo root. It produces a
module named `fmi.so` (the shared-module prefix is cleared on purpose). **The Boost.Python
component must match the target Python version.** On Ubuntu 24.04 use the system `python3`
(3.12) with the distro `libboost-python-dev`; do **not** pair a `uv` Python 3.11 with the
distro Boost.Python (it targets 3.12). Override the library explicitly with
`-DFMI_BOOST_PYTHON_LIBRARY=...` if auto-detection picks the wrong one. For Python 3.11
parity, build inside the Docker image (`runbooks/aws-python311-s3/Dockerfile.python3.11`).

## Testing

Tests use the Boost.Test framework and only build with `-DFMI_BUILD_TESTS=ON`. The single
test binary is `build/tests/Boost_Tests_run`.

```bash
./build/tests/Boost_Tests_run                              # all tests
./build/tests/Boost_Tests_run --run_test=Communicator/reduce   # one case
./build/tests/Boost_Tests_run --run_test=FaultTolerance    # one suite
./build/tests/Boost_Tests_run --list_content               # list all suites/cases
```

Suites (note the names differ from the file names): `Channels` (`channels.cpp`),
`Communicator` (`communicator.cpp`), `FaultTolerance` (`fault_tolerance.cpp`), and, only
with `FMI_ENABLE_CRIU=ON`, `CriuFaultTolerance` (`criu_fault_tolerance.cpp`). **Many tests
need live infrastructure:** Redis-backed cases need a running Redis; `Direct` cases need a
`tcpunchd` rendezvous server on port 10000
(`./extern/TCPunch/server/build*/tcpunchd 10000`); S3 cases need AWS credentials + a bucket.
`DirectTCP` cases need only a running Redis — no `tcpunchd`.
A `Direct` case that logs `ACTIVE` and then throws a `std::string` exception before pairing
means the TCPunch client could not reach the rendezvous server. Note that bare `std::string`
throw is uncaught inside the OpenMP two-thread cases and terminates the process, so a
`Direct` failure there shows up as `terminate called` / `stack smashing`, not a test failure.

The cheapest way to exercise the `Channels` suite is a `-DFMI_ENABLE_TCPUNCH=OFF` build: only
`DirectTCP` is registered, so the suite runs against a single backend with no rendezvous
server and none of TCPunch's flakiness (it has been green 5/5 that way, against `Direct`'s
0–33 failures per run).

The CRIU tests are designed to run against a *mock* `criu` binary because real
checkpoint/restore needs kernel capabilities usually unavailable in dev environments.

The one standalone demo is the experimental `transparent_state_transfer_demo`, which ships with
its runbook (`runbooks/local-criu-state-transfer/transparent_state_transfer_demo.cpp`, built via
that directory's own `CMakeLists.txt`) rather than under `tests/`. It is built only as a
top-level project with `FMI_ENABLE_CRIU=ON`, and is driven by that directory's
`run-demo.sh` (one rank) or `run-demo-all.sh` (checkpoints all local ranks in parallel).

## Running things

Every peer in a communicator must agree on `comm_name` and `num_peers`; `peer_id` is in
`[0, num_peers)`. The end-to-end `transparent_migration` flow is in
`runbooks/localstack-python311-redis/` (heterogeneous LocalStack EC2 → Lambda ranks, Redis
control + data plane), and the AWS Lambda + S3 flow is in `runbooks/aws-python311-s3/`; both
READMEs are verified step-by-step runbooks. The same migration demo on real AWS
infrastructure (EKS + Knative, ranks as K8s Jobs, replacement rank as a Knative Service)
lives in `runbooks/localstack-python311-redis/knative-migration/` (`setup.md`, plus
`setup-local.md` for a local-cluster variant). JSON config templates live in `config/`.

The experimental CRIU rank agent CLI is built only with `FMI_ENABLE_CRIU=ON`:

```text
fmi-rank-agent {migrate|migrate-local|evacuate-local|restore-remote|promote|watch|cleanup} \
    <comm_name> <num_peers> <config> [rank]
```

## Architecture

The dependency direction is: user API → channel policy → channel → transport backend, with
fault tolerance layered around the user API.

- **`FMI::Communicator`** (`include/Communicator.h`) is the user-facing MPI-like API:
  templated `send/recv/bcast/scatter/gather/reduce/allreduce/scan/barrier` over
  `FMI::Comm::Data<T>`. It owns a map of named channels plus one `ChannelPolicy`. For each
  call it asks the policy for a channel name, then dispatches to that channel. Every
  operation is wrapped in an `OperationGuard` (RAII) that calls `enter_operation`/
  `exit_operation` — this is the hook transparent migration uses to observe Redis
  migration requests and reconfigure at operation
  boundaries. Most of the real logic is header-only templates; `src/Communicator.cpp` is
  thin (construction, channel creation, FT runtime wiring).

- **`FMI::Utils::ChannelPolicy`** (`include/utils/ChannelPolicy.h`) is the cost model — the
  paper's core idea. Given an operation descriptor `{op, size, left_to_right}`, the per-
  backend `Model` parameters from config, and the `Hint`, it returns which backend to use.

- **Channel hierarchy** (`include/comm/`): abstract `Channel` is the backend interface, with
  a static `get_channel(name, ...)` factory keyed by backend name. Two abstract families
  implement the collectives differently:
  - `PeerToPeer` derives collectives (binomial trees, etc.) from `send_routine`/
    `recv_routine` primitives. Concrete backends: `Direct` (TCP via TCPunch NAT hole-punch)
    and `DirectTCP` (plain TCP, no rendezvous server). Both sit on `TcpChannelBase`, which
    owns everything about moving bytes over a socket; a subclass supplies only `establish()`.
  - `ClientServer` derives collectives over a shared medium via `upload`/`download`.
    Concrete backends: `S3`, `Redis`.

- **`DirectTCP`** (`include/comm/DirectTCP.h`) is the TCPunch-free peer-to-peer transport, for
  platforms where ranks can reach each other directly (VMs, containers on a shared network,
  pods, bare metal). Instead of a rendezvous server, each rank binds an ephemeral port and
  publishes `ip:port:nonce` to a Redis hash `fmi:direct:<comm_name>`; the lower rank listens,
  the higher connects, and the listener acknowledges a 32-byte hello so a connect that lands on
  a recycled port is detected rather than silently used. Discovery is one `HGETALL`, publishing
  is a pipelined `HSET`+`EXPIRE`, and links are built lazily as collectives touch them. Config
  keys are deliberately distinct from `Direct`'s (`registry_host`/`registry_port`, `bind_host`,
  `advertise_host`) so a `Direct` block cannot be silently reinterpreted. Example config:
  `config/fmi_direct_tcp.json`. Reset a stale registry with
  `redis-cli DEL fmi:direct:<comm_name>`.
  - Establishment is dramatically cheaper than TCPunch — measured first-barrier time on
    loopback: 107 ms → 0.5 ms at 2 peers, 315 ms → 6.8 ms at 8, and it completes in ~10 ms at
    32 peers where `Direct` fails outright.
  - **Not usable behind NAT**, or anywhere ranks cannot accept inbound connections (Lambda,
    Knative scale-from-zero). Those are exactly what `Direct` is for.
  - On multi-homed hosts and in containers, set `advertise_host` explicitly (in K8s, the pod IP
    via the downward API — never a Service VIP, which load-balances to an arbitrary pod).
    Otherwise the rank advertises whichever local address routes to the registry.
  - Registered as checkpoint-safe (`include/ft/experimental/CriuRequirements.h`), and for a
    stronger reason than `Direct`: `prepare_for_checkpoint` drops the listener, the cached
    advertised address and the registry client, so a restored rank re-binds, re-resolves the
    address peers must dial on whichever host it woke up on, and re-publishes it.
    (`prepare_for_checkpoint` runs only on the FT-managed path; `runbooks/criu-transparent-checkpoint/`
    freezes an unmodified DirectTCP job raw — no hooks — and survives on the sequenced-link
    recovery alone, which is why that runbook is single-host: a raw restore keeps the old
    listener and advertised address.)
  - Note `ChannelPolicy` breaks ties by `std::map` order, so `Direct` beats `DirectTCP`
    alphabetically at equal modelled cost — `model.DirectTCP.overhead` is set below `Direct`'s
    to reflect its cheaper connection setup, which is also what makes the policy pick it.

- **Data & reductions**: `FMI::Comm::Data<T>` (`include/comm/Data.h`) flattens scalars or
  vectors into a raw byte buffer (`data()`, `size_in_bytes()`). `FMI::Utils::Function<T>`
  (`include/utils/Function.h`) wraps a reduction op plus associativity/commutativity flags;
  `Communicator` type-erases it into a `raw_func` (`std::function<void(char*,char*)>`).
  Those flags drive both algorithm choice and evaluation order (non-assoc/non-comm forces a
  fixed left-to-right order).

- **Configuration** (`include/utils/Configuration.h`, `src/utils/Configuration.cpp`): one
  JSON file parses into `Config { channels, models, fault_tolerance }`. The `backends` block
  enables/configures each channel; the `model` block holds cost-model parameters; the
  `fault_tolerance` block maps to `FaultToleranceConfig`. If `fault_tolerance.enabled` is
  true, `Communicator` uses transparent migration; there is no FT mode selector.

- **Fault tolerance** (`include/ft/`, `src/ft/`, design in `PLANS.md`, usage in
  `docs/fault-tolerance.md`): transparent migration is the single FT protocol. It uses Redis
  as the control plane and `Direct`/TCP as the preferred data plane. `FMI::FT::ControlPlane`
  tracks epochs, membership, placement, and migration state in Redis; migration is triggered
  externally via `request_migration()`. `TransparentMigrationRuntime` checks for migration at
  `OperationGuard` boundaries. The targeted rank marks itself `QUIESCED`, a replacement takes
  over the same logical rank at epoch N+1, and surviving ranks rebuild channels under the new
  epoch-qualified communicator name. **Application-state continuity** is selected by
  `fault_tolerance.state_transfer` (a mechanism toggle within the one protocol, not a second
  mode): `"none"` (default) — the rank exits and a fresh replacement recomputes; `"criu"`
  (needs `FMI_ENABLE_CRIU=ON`) — the rank's process image is CRIU checkpointed/restored so
  memory is preserved transparently. The CRIU path is driven by the host-local
  `LocalRankAgent` (`fmi-rank-agent`), reuses the `prepare_channels_for_checkpoint` hook + the
  survivor reconfigure path, and supports two shapes: **same-host in-place**
  (`migrate`/`migrate-local`, verified in `runbooks/local-criu-state-transfer/`, rootless criu)
  and **cross-host** (`evacuate-local` dumps a host's ranks in one cut and stages packed images
  in Redis via `ControlPlane::criu_image_put`; `restore-remote` fetches + restores one rank on
  another host; the orchestrator `promote`s once — verified in
  `runbooks/k8s-criu-node-evacuation/`, node evacuation onto Knative).
    Shared criu-invocation code lives in `ft/experimental/CriuExec`.
  - **Epoch fencing invariant** (`PLANS.md`): under FT, every backend-visible name —
    `Direct` pairing names, `Redis`/`S3` object names, per-instance operation counters — is
    epoch-qualified, so stale messages/objects from an old epoch can never be consumed after
    reconfiguration.

- **Python bindings** (`python/`): a Boost.Python module. `fmi_python.cpp` is the module
  entry point; `PythonCommunicator.cpp` exposes `Communicator`; `PythonFT.cpp` exposes the
  FT control-plane surface (`FTControlPlane`) plus the type/op helpers (`hints`, `func`, `op`,
  `datatypes`, `types`). Because Python is dynamically typed, collective calls
  take an explicit `fmi.types(...)` descriptor and return results directly rather than
  filling a receive buffer.

`include/fmi.h` is the umbrella header (Communicator + the FT types). The ICS'23 paper and
the thesis linked from `README.md` are the authoritative design references; technical docs
are generated with Doxygen (`docs/Doxyfile`, output gitignored).

## Git Commits

- Commit after every meaningful, self-contained change — don't batch unrelated work.
- Each commit should leave the codebase in a working state.
- Write short, clear commit messages that describe what changed.
- If a task involves multiple logical steps, make a separate commit for each step.
- Stage only the files relevant to the current change.
