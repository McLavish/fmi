# CLAUDE.md

This file provides guidance to agents when working with code in this repository.

## What this is

FMI (FaaS Message Interface) is a C++17 library (plus a Boost.Python binding) that gives
serverless/distributed applications an MPI-like API for point-to-point and collective
communication. Its distinguishing feature is **model-driven channel selection**: the same
logical operation can run over different transport backends (direct TCP, S3, Redis), and a
cost/performance model picks the backend per operation based on message size and an
optimization hint (`fast` vs `cheap`). The TCP backends can additionally be configured to
carry every message inside a **sequenced link** — frames are numbered, retained until the peer
acknowledges them, and replayed after a connection is lost and rebuilt — so a rank can be
`criu` checkpointed and restored mid-job by an external tool without the application taking
part.

The canonical working tree on this machine is `/home/luca/fmi`. This repository is the library
alone; the applications, runbooks, benchmarks and the migration runtime built on it live in
`fmi-spot-migration`, which pins this repository as a submodule.

## Building

Always init submodules first — `extern/TCPunch` (the `Direct` backend depends on it) and
`docs/doxygen-awesome-css` are git submodules:

```bash
git submodule update --init --recursive
```

`extern/TCPunch` points at the `McLavish/TCPunch` fork, which carries two fixes upstream
lacks: `tcpunchd` ignores SIGPIPE instead of crashing, and the client resolves
the rendezvous host via `getaddrinfo` (upstream accepted only literal IPs, so K8s Service
DNS names failed).

Standard full build (with tests):

```bash
cmake -S . -B build -DFMI_BUILD_TESTS=ON
cmake --build build -j"$(nproc)"
```

CMake options (defaults in `CMakeLists.txt`): `FMI_ENABLE_S3` (ON, pulls AWS SDK for C++),
`FMI_ENABLE_REDIS` (ON, pulls hiredis), `FMI_ENABLE_TCPUNCH` (ON, builds the `Direct` backend
and links the TCPunch submodule — turn it OFF on platforms with direct peer connectivity and
the submodule is not needed at all), `FMI_USE_STATIC_BOOST` (ON), `FMI_BUILD_TESTS` (OFF).
The library target is `FMI` (alias `FMI::FMI`), built STATIC, and publishes its public headers
via `include/` (exported to a parent scope as `FMI_INCLUDE_DIRS`). The transparent-checkpoint
runbook's subject program is built as well whenever `FMI_ENABLE_REDIS` is ON and FMI is the
top-level project; it is an ordinary FMI application, so it needs no option of its own.

### S3-enabled build

The AWS SDK is not packaged on Ubuntu; build the one component the `S3` backend needs and point
CMake at it. Verified against **aws-sdk-cpp 1.11.861** (the version the S3 channel's error
classification was measured against):

```bash
git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp -b 1.11.861 /tmp/aws-sdk-cpp
cmake -S /tmp/aws-sdk-cpp -B /tmp/aws-sdk-cpp/build -GNinja \
  -DBUILD_ONLY=s3 -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTING=OFF \
  -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_INSTALL_PREFIX="$HOME/.local/aws-sdk-cpp"
cmake --build /tmp/aws-sdk-cpp/build --target install   # 8-20 min

cmake -S . -B build-s3 -DFMI_ENABLE_S3=ON -DFMI_ENABLE_REDIS=ON -DFMI_ENABLE_TCPUNCH=OFF \
  -DFMI_BUILD_TESTS=ON -DCMAKE_PREFIX_PATH="$HOME/.local/aws-sdk-cpp"
cmake --build build-s3 -j"$(nproc)"
export LD_LIBRARY_PATH="$HOME/.local/aws-sdk-cpp/lib"   # if ldd on the binaries misses aws libs
```

Needs `libcurl4-openssl-dev libssl-dev ninja-build`. The suite runs from `build-s3/tests` exactly
as from `build/tests`, and is green without AWS credentials.

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
parity, build inside the Docker image (`deploy/aws-lambda-python311-s3/Dockerfile.python3.11`
in fmi-spot-migration).

## Testing

Tests use the Boost.Test framework and only build with `-DFMI_BUILD_TESTS=ON`. There is one
test binary, `build/tests/Boost_Tests_run`, and **it must be run from its own build
directory**: every suite that constructs a `Communicator` loads its config by the relative
path `../../config/<name>.json`, so from anywhere else those cases fail at construction.

```bash
cd build/tests
./Boost_Tests_run                                  # all tests
./Boost_Tests_run --run_test=Communicator/reduce   # one case
./Boost_Tests_run --run_test=LinkLayer             # one suite
./Boost_Tests_run --list_content                   # list all suites/cases
```

Sixteen suites, one per file under `tests/`, the suite name being the file name in CamelCase:
`Channels`, `Communicator`, `LinkLayer`, `LinkRecovery`, `LinkLiveness`,
`FramedTransport`, `TransportRecovery`, `OperationIdentity`, `ProtocolValidation`,
`ProtocolEdgeCases`, `ProtocolFuzz`, `CheckpointFreezePoints`, `ClientServerRecovery`,
`S3Backend`, `DrainTransport`, `DrainMigration`.
(`LinkIncarnations` was removed with `frame_wire_version` 4: all ten of its cases exercised the
incarnation lineage fence, which no longer exists.)
(`tests/client.cpp` is a standalone sample, not a suite, and is not compiled into the binary.)
`S3Backend` only exists in an `FMI_ENABLE_S3=ON` build, and splits in two: the cases that need a
working store skip green unless `FMI_S3_TEST_BUCKET` names one (plus optional
`FMI_S3_TEST_REGION`/`_ENDPOINT`/`_PATH_STYLE`/`_SLOW`), while the cases about failure — a
throttled write that is retried, an operation that gives up inside its budget, a refusal that is
not waited out — always run, against a fake S3 endpoint the suite serves on loopback. So the
S3-enabled build is green with no credentials and no bucket, and still covers the recovery
bounds.

**Most tests need live infrastructure:** everything that talks `DirectTCP` needs a running
Redis, which is its peer registry, and `config/fmi_identity_test.json` drives the `Redis`
backend as an actual data plane; `Direct` cases need a `tcpunchd` rendezvous server on port
10000 (`./extern/TCPunch/server/build*/tcpunchd 10000`). Note the `Channels` suite
parameterises over the `backends` map at the top of `tests/channels.cpp`, in which `S3` and
`Redis` are commented out — only `Direct` and `DirectTCP` run there today, and re-enabling
`S3` means AWS credentials + a bucket.
A `Direct` case that logs `ACTIVE` and then throws a `std::string` exception before pairing
means the TCPunch client could not reach the rendezvous server. Note that bare `std::string`
throw is uncaught inside the OpenMP two-thread cases and terminates the process, so a
`Direct` failure there shows up as `terminate called` / `stack smashing`, not a test failure.

The cheapest way to run the whole binary is a `-DFMI_ENABLE_TCPUNCH=OFF` build. Only
`DirectTCP` is registered, so `Channels` runs against a single backend with no rendezvous
server and none of TCPunch's flakiness (green 5/5 that way, against `Direct`'s 0–33 failures
per run), and a `#if FMI_ENABLE_TCPUNCH` at the top of `tests/communicator.cpp` switches that
suite from `config/fmi_test.json` — which enables `Direct` and nothing else, so it needs
`tcpunchd` — to `config/fmi_directtcp_test.json`. Both then need only a running Redis.

The library builds nothing but `FMI`, `tests/` and the Python binding. The applications —
including the 8-shape checkpoint subject, an ordinary FMI program with no checkpoint API in it
that the criu sweeps freeze at random instants and compare against a clean run — and the
checkpoint/migration runbooks that drive them live in the `fmi-spot-migration` repository
(`apps/`, `benchmarks/migration/`, `runtime/protocol/`), which takes this library as a
submodule and pins the commit.

## Running things

Every peer in a communicator must agree on `comm_name` and `num_peers`; `peer_id` is in
`[0, num_peers)`. No runbooks ship with the library. In fmi-spot-migration: the AWS Lambda + S3
flow is `deploy/aws-lambda-python311-s3/` (historical), and the criu-transparent checkpoint and
drain migration runbooks — which checkpoint, move and restore one rank of an unmodified job
with `criu` driven entirely from outside the process — are
`benchmarks/migration/criu-transparent/` and `benchmarks/migration/drain/`. JSON config
templates live in `config/`.

## Architecture

The dependency direction is: user API → channel policy → channel → transport backend.

- **`FMI::Communicator`** (`include/Communicator.h`) is the user-facing MPI-like API:
  templated `send/recv/bcast/scatter/gather/reduce/allreduce/scan/barrier` over
  `FMI::Comm::Data<T>`. It owns a map of named channels plus one `ChannelPolicy`. For each
  call it asks the policy for a channel name, then dispatches to that channel. Every
  operation is wrapped in an `OperationScope` (RAII, `include/comm/OperationScope.h`) that
  publishes the active operation's identity — lane, op kind, collective index, root,
  reduction flags — in a thread-local for the duration of one logical call. Channels read it
  to stamp frames, which is how a receiver detects a peer executing a *different* operation
  without the `Channel` interface growing an argument. Scopes nest, and the outermost wins, so
  the index counts user-visible collectives rather than the internal fragments a collective
  decomposes into. Most of the real logic is header-only templates; `src/Communicator.cpp` is
  thin (construction, channel creation).

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
  - The criu-transparent runbook (fmi-spot-migration `benchmarks/migration/criu-transparent/`)
    freezes an unmodified DirectTCP job raw — no hooks
    — and survives on the sequenced-link recovery alone. **Cross-host restore is backed** (commit
    `dc43beb`), by three mechanisms that replaced the deleted `Channel::prepare_for_checkpoint`
    hook. `reset_transport_if_relocated()` (`src/comm/DirectTCP.cpp:75`) remembers the kernel boot
    id the transport was built on; an establishment that finds a different one drops listener,
    links, registry client and cached advertisement wholesale, and plants a reconcile debt on
    every severed link so each re-formed connection pays handshake and replay — without the debts
    the peer classifies the retained-frames hole as a FATAL sequence gap. `publish_self`
    (`src/comm/DirectTCP.cpp:196`) re-derives the advertised address on *every* publish rather
    than caching it once, and never downgrades a working address to the loopback fallback on a
    transient probe failure. Detection runs both at establishment entry and at the publish cadence
    inside the loop a freeze may land in. Same boot id — a same-host restore — never resets, so
    the proven path is untouched. Note this requires `advertise_host` to be left **empty**: an
    explicit one degenerates the probe to returning the configured string.
  - Note `ChannelPolicy` breaks ties by `std::map` order, so `Direct` beats `DirectTCP`
    alphabetically at equal modelled cost — `model.DirectTCP.overhead` is set below `Direct`'s
    to reflect its cheaper connection setup, which is also what makes the policy pick it.

- **`DrainTCP`** (`include/comm/DrainTCP.h`) is the third TCP backend and the steady-state half
  of the **neighborhood-drain migration protocol** — the coordinated alternative to the
  sequenced-link retention/replay above, with the opposite trade-off: a headerless raw byte
  stream (no frames, no acks, no retention — zero added bytes per message) whose per-link
  mutexes are held for exactly one non-blocking syscall per chunk with every wait outside the
  lock, so a migration-time drain can cut in *between* chunks and a criu image taken after a
  drain contains zero sockets. Discovery mirrors DirectTCP over its own registry hash
  `fmi:drain:<comm_name>` (a rank dials lower ids, accepts higher), but accepts run on a
  per-channel **control thread** — the library's only thread — so a compute-bound rank still
  answers connection attempts. The once-per-connection 56-byte hello
  (`include/comm/DrainProtocol.h`) carries the sender's incarnation plus cumulative
  per-direction byte counters, cross-checked at every (re)connect: a byte lost across a cut
  fails loudly at the reconnect instead of desyncing the stream silently.
  An *unplanned* connection death is a loud error by design — this protocol handles planned
  migration only, never fault tolerance. Example configs: `config/fmi_drain_tcp.json`
  (drain armed, one enabled backend on purpose), `config/fmi_draintcp_test.json`.
  - **The migration itself** is `"drain": true` plus three pieces. `FMI::Utils::MigrationTrigger`
    (`include/utils/MigrationTrigger.h`) is process-wide — one signal policy
    (`SIGRTMIN + drain_signal_offset`, blocked before the trigger thread exists, plus a handler
    as the safety net for threads that predate arming), one epoch (a restore counter that drops
    a request queued for a lineage this process has left), one thread, one `pthread_atfork`
    child handler that disarms. A **second armed drain channel in one process is a
    `std::logic_error`**, so the supported topology is one rank per process.
    `FMI::Comm::DrainCoordinator` (`include/comm/DrainCoordinator.h`) is the control plane: a
    narrow interface (so tests fake the orderings Redis will not produce) over a members hash,
    a Redis **stream** of `leaving`/`sealed`/`restored`/`migrate` events — a stream, because
    pub/sub loses exactly the events a frozen rank needed — and a `SET NX PX` batch lease.
  - The migrator sequence (`DrainTCP::quiesce_and_drain` + `resume_after_restore`): establish
    gate exclusively (bounded by `establish_yield_ms`, an in-flight dial letting go at its next
    retry boundary) → `leaving` → park the control thread where it owns no socket and close the
    listener → every link lock in ascending order, **half-close all, then one poll loop reading
    all to EOF** into each link's `inbound` → `sealed` with the per-peer counters → drop the
    registry and coordinator connections → `malloc_trim` → `SIGSTOP`. The restore leg bumps the
    epoch and the incarnation, binds a *fresh* listener and re-resolves the advertised address
    before re-advertising (cross-host correct by construction), emits `restored`, and clears
    every link — leaving fds closed, because re-establishment is lazy and belongs to whichever
    application thread next wants the link. Link locks are held across the whole stop and
    released last. `rehearse_migration_in_place([hold_ms])` runs all of it with the `SIGSTOP`
    replaced by an immediate restore; it is the regression net and what `tests/drain_migration.cpp`
    drives. `drain_hold_ms` is the same hold for a rehearsal nobody called that function for — a
    signalled or `migrate`-event one — and in a **batch** the restore leg keeps the window open on
    every link whose peer is itself still migrating (`LinkState::peer_migrating`, set by that
    peer's leave notice and cleared by its `restored`), because a rank that came back first would
    otherwise spend `max_timeout` dialling the address a frozen co-member left behind.
  - Two orderings this had to be built for. A survivor's data path often sees the migrator's
    FIN *before* the leave notice: with drain armed that is recorded as a tentative drain
    (`drain_unconfirmed`) rather than the immediate loud error it is with drain off, and the
    notice confirms it — or `drain_grace_ms` lapses and it becomes the loud unplanned-death
    error naming the peer. Conversely a leave notice can arrive *after* the migrating peer has
    restored and dialled back in, so every notice carries the leaver's incarnation and is
    dropped when the link already belongs to a later lineage. Without that fence a late notice
    tears down a healthy connection and both ends wait for each other forever (it did).
  - `max_timeout` is **suspended** for the duration of a migration; `migration_max_ms` bounds a
    survivor's wait instead and throws naming the peer. The suspension is accounted *per link*
    (`LinkState::migration_ms_total`, banked by whichever path ends the window, compared against
    the snapshot each operation takes at entry) rather than measured by the waiting thread —
    because most of the threads it is owed to never get to wait: the migrating rank's own drain
    holds every link lock from the seal to the end of the restore leg, so its application threads
    spend the whole window blocked on a plain mutex, and `draining` is set and cleared entirely
    inside the interval in which they cannot run. Credit the waiter and they wake onto an expired
    transport deadline — a `Timeout`, terminal for the communicator, thrown immediately after a
    migration that *succeeded*. A drain-armed job also needs `trigger` to include `control` on
    every rank: the migrator's drain finishes only once its peers have half-closed, and they
    learn to from the coordinator, not from the application.
  - **Cross-host restore is verified**, sequential and batch (whole-machine and two-machine
    single cuts), by the 4-machine campaign driven from
    `benchmarks/migration/drain/multihost_drain.py` in fmi-spot-migration — evidence in that
    runbook's README. One
    environmental requirement: cross-host criu must run privileged (sudo or
    `CAP_SYS_ADMIN`) so the restore lands in a time namespace preserving
    `CLOCK_MONOTONIC`; `--unprivileged` silently skips the namespace, the restored rank
    inherits the destination's clock, and a forward jump expires every absolute deadline at
    once. Same-host restores are unaffected; deadline re-basing that would lift the
    requirement is future work (see the spec doc §9).

- **Data & reductions**: `FMI::Comm::Data<T>` (`include/comm/Data.h`) flattens scalars or
  vectors into a raw byte buffer (`data()`, `size_in_bytes()`). `FMI::Utils::Function<T>`
  (`include/utils/Function.h`) wraps a reduction op plus associativity/commutativity flags;
  `Communicator` type-erases it into a `raw_func` (`std::function<void(char*,char*)>`).
  Those flags drive both algorithm choice and evaluation order (non-assoc/non-comm forces a
  fixed left-to-right order).

- **Configuration** (`include/utils/Configuration.h`, `src/utils/Configuration.cpp`): one
  JSON file with two blocks. `backends` enables and configures each channel — every key under
  an enabled backend is handed to that channel as an untyped `map<string,string>`, which is why
  backend-specific options like `framed` or `advertise_host` need no parser change; `model`
  holds the cost-model parameters `ChannelPolicy` reads, plus `FaaS.gib_second_price`.
  `get_active_channels()` returns exactly the enabled ones, so a backend disabled in the config
  is never constructed even if it was compiled in.

- **Sequenced link layer** (`include/comm/LinkFrame.h`, `include/comm/SequencedLink.h`,
  `src/comm/TcpChannelBase.cpp`; design in
  `docs/design/2026-07-27-sequenced-incarnation-links-design.md`, machine-checked
  models in `docs/tla/`): two independent opt-in flags on a TCP backend, both **off by
  default**, so an unconfigured `Direct`/`DirectTCP` byte stream is exactly what it was before
  this existed. `"framed": true` prefixes every message with a fixed-size `LinkFrame` header
  carrying the `OperationScope` identity, and the receiver *rejects* a frame belonging to a
  different logical operation instead of copying it into the application's buffer.
  `"recover_links": true` (requires `framed`) adds `SequencedLink`: per-peer send/receive
  sequence numbers, retention of every sent frame until the peer acknowledges it, and a
  handshake that replays the unacknowledged suffix after a connection dies and is
  re-established — so a completed `send()` stays a delivery obligation across the break. This
  is what lets a rank be criu-frozen mid-operation and restored with its peers none the wiser.
  Two rules the models and the runbook both turned out to depend on: the receive watermark may
  only advance once a payload is in the application's buffer (not at header parse), and
  retention must be released on links the peer never writes back to, or the sender's window
  fills and a binomial tree deadlocks from three ranks up.
  - **`frame_wire_version` is 4, and the header is 42 bytes** (was 72 at version 3). Version 4
    removed every field no production read consulted: `message_id` (8 B, written from the same
    `next_send_seq()` call as `transport_seq` and equal to it by construction), `fragment_index`
    (4 B, a placeholder for a fragmentation feature that does not exist — and one the design doc
    had rejected in favour of `fragment_offset`+`fragment_length`), two reserved holes and the
    tail padding (10 B, never written non-zero, never validated, and aligning nothing: the codec
    is byte-at-a-time and `write_frame` issues header and payload as separate writes), and
    `total_length` (8 B, written from the same `len` as `payload_length` on the next line, so
    equal on every frame a producer can emit). `total_length`'s identity role moved to
    `payload_length`, so `same_identity` compares the same quantity it always did. Every one of
    the 42 bytes now carries a field. The `payload_length > total_length` decode rule went with
    it — that state is now unrepresentable rather than merely rejected.
  - **The handshake is 30 bytes** (was 56). Version 4 also dropped `policy_fingerprint` (8 B,
    documented as a backend-policy mismatch detector, never computed and never compared) and the
    **incarnation pair** (16 B) — a lineage fence distinguishing "the same process, restored"
    from "a different process now serving this rank", which was correct code with no producer:
    permanently `0`, so all three lineage branches of `SequencedLink::reconcile()` were
    unreachable and `reset_stream()` was dead with them. `snapshot_version` went `2` → `3`
    accordingly. What remains is thinner than it looks: `reconcile()` decides from
    `next_expected_seq` alone, and the transmitted `next_send_seq`/`lowest_retained` pair is read
    only by a decode-time sanity check of each other (`LinkFrame.h`) — the *local* methods of
    those names are load-bearing for replay; only the wire copies decide nothing.
  - **The arming hook is `Channel::on_registered()`**, called last by
    `Communicator::register_channel` once `peer_id`, `num_peers` and `comm_name` are set; the
    default does nothing. It replaced `set_incarnation`, whose argument was always zero once the
    sequenced path lost its lineage fence in wire version 4 and whose name hid the one thing it
    still did. **`DrainTCP` overrides it to arm**: the override calls `ensure_started()`, which
    binds the listener, publishes to the registry, starts the control thread and attaches the
    `MigrationTrigger` (`src/comm/DrainTCP.cpp:353`). DrainTCP's incarnation is its own — starts
    at zero, bumped on every restore, carried in its once-per-connection hello
    (`DrainProtocol.h` `ResumeRecord`), and used to fence late leave notices — and nothing
    outside the channel assigns it. `Communicator` no longer carries an incarnation member.
  - `magic` and `wire_version` are deliberately **kept** although neither carries information
    between conforming peers: `magic` is the only detector of a raw/unframed peer and of a
    desynchronised stream, and `wire_version` is the fence that makes format changes safe. Both
    keep their offsets across every version so far (0..3 and 4..5 of each record) and both
    decoders check them before anything else.
    The fence is **not symmetric**, and shrinking a header is why. A version 4 rank reading a
    version 3 peer buffers 42 bytes, finds `wire_version` 3 and raises `BadVersion` at once. A
    version 3 rank reading a version 4 peer is waiting for **72** bytes before it decodes
    anything, so it raises `BadVersion` only once ≥72 bytes have arrived — and if the v4 side's
    traffic is smaller than that and then goes quiet, the old rank blocks instead. Mixing builds
    across a wire-version bump is a deployment error either way; the point is that in one
    direction it can present as a hang rather than an error, so **rebuild every rank together**.
  - `header_field_bytes`/`handshake_field_bytes` plus their `static_assert`s pin the encoders'
    field widths to the size constants. This matters now that the headers are exactly their field
    sums: the old `while (off < frame_header_bytes)` pad loop was silently absorbing any
    under-write, and without it a mistaken edit would overflow the fixed
    `char[frame_header_bytes]` every caller declares. `encode_header` returns the bytes written
    and `encode_header_checked` asserts on them.
  - Relatedly, `ack_safe_seq` is assigned `= next_recv` at both production write sites,
    so the separate "safe to prune" watermark `SequencedLink.h` documents is not maintained.

- **Store-family recovery** (`include/comm/RecoverableClientServer.h`,
  `src/comm/RecoverableClientServer.cpp`, `src/comm/Redis.cpp`, `src/comm/S3.cpp`): the same idea as the sequenced
  link layer for the other channel family, in the same place in the hierarchy —
  `RecoverableClientServer` sits between `ClientServer`, which keeps the collective algorithms,
  and the concrete backends, exactly as `TcpChannelBase` sits between `PeerToPeer` and
  `Direct`/`DirectTCP`. One opt-in flag, `"recover": "true"` on the backend's
  config block, **off by default**: unset, every override delegates to the base and the keys, the
  deletes and the byte copies are what the family always wrote. Recovery's entire footprint in
  the base is two hooks — `object_key_prefix()` and `delete_objects()`, both one-liners — so the
  collective algorithms are untouched *by recovery*. `ClientServer.cpp` did change (+141/-43),
  independently: see the vanilla bug fixes below. The only flag-off difference
  in the recovery work itself is that a NULL hiredis reply is now a named `std::runtime_error`
  instead of a null dereference. **Both backends implement the flag.** `Redis` was first; `S3` refused it at
  construction until it had its own half, because the family half (no deletion) without an S3 half
  is unbounded cost plus silent data loss.
  - Why the store family needs so much less machinery than TCP: all protocol state is
    process-local memory that criu restores byte-exact (the `num_operations` counters, the poll
    loops' locals), the store holds durable whole values, and every command is an idempotent
    SET/GET/DEL — so there is nothing to sequence, retain or replay. The only thing a freeze
    destroys is the connection to the store. Under `recover`, `Redis::command()` therefore
    reconnects and re-issues: `redisFree` plus a fresh dial, **never** clearing `err` in place,
    since a context frozen mid-command can hold a partial command in its output buffer.
    `connect_timeout_ms` and `io_timeout_ms` (both default 1000 ms, the latter clamped to
    [100, 60000] and never derived from the poll `timeout`, which is 1 ms in the shipped configs)
    bound a dial and an I/O against a store that has gone away.
  - A write is a delivery obligation — the analogue of sender retention — so `upload_object`
    retries under the poll budget and then **throws** rather than logging and dropping;
    a `-OOM`/`-READONLY`/`-MISCONF` reply throws immediately. `download_object` never throws for
    a connection reason (its callers are bounded poll loops whose budget is the failure
    detector, and they only re-enter on `false`), but a value whose length is not the length
    expected does throw instead of being silently truncated.
  - Cleanup moves to the store. Under `recover` `finalize()` deletes **nothing** — a rank that
    leaves cannot know whether a peer still needs what it wrote, and deleting there is what makes
    a staggered finalize deadlock — and every write carries `EX object_ttl_s` (default 3600; `0`
    means no expiry, which under this flag means no cleanup at all). `barrier()` correspondingly
    stops calling `get_object_names()` (a `KEYS *` per poll from every rank) and probes the N
    exact marker keys, which also ends the cross-communicator suffix false-arrivals; and
    `object_key_prefix()` gains a `|` separator, closing the `"job"` rank 11 vs `"job1"` rank 1
    key collision for recovered jobs.
  - **The S3 half.** The mechanism is the same and the client is the difference: a request costs a
    round trip and money, and one that fails can cost more wall clock than the whole budget it is
    charged against. So the channel is configured explicitly rather than by SDK default —
    `connect_timeout_ms` (3000), `request_timeout_ms` (15000, set on *both* `httpRequestTimeoutMs`
    and `requestTimeoutMs`, since the SDK field of that name is curl's low-speed time and not a
    cap), `max_attempts` (4, an explicit `StandardRetryStrategy`), `max_connections` (4),
    `transient_failure_limit` (200), `credentials_provider` (`default`|`environment`),
    `endpoint_url` + `use_path_style` for MinIO/LocalStack. A failure is classified by HTTP status
    and exception name, **never by the SDK's error enum** (1.11.861 maps `ExpiredToken` to
    `UNKNOWN`): absence is the silent hot path; a refusal, a missing bucket and a
    misrouted/missigned request (301 `PermanentRedirect`, 400 `AuthorizationHeaderMalformed`) are
    raised at once under `recover`; anything else is transient and bounded twice — by
    `transient_failure_limit` consecutive failures **and** by `max_timeout` milliseconds of wall
    clock across them, measured from the request that opened the run and restarted by any answer,
    absence included. Uploads retry at the poll interval under those same bounds before throwing.
    Also here and unconditional: `ListObjectsV2` with the communicator's prefix and pagination
    (one unpaginated whole-bucket page before), `DeleteObjects` in batches of 1000 at finalize,
    `std::call_once` SDK init that is never shut down (`Init`→`Shutdown`→`Init` is unsupported).
  - **S3 has no per-object expiry**, so the TTL half of the contract cannot be met by the channel:
    `object_ttl_s` is accepted and inert there, and the bucket **must carry a lifecycle expiration
    rule** or a recovered job's objects are kept and billed forever. The constructor says so at
    `warning` whenever `recover` is on. Grant `s3:ListBucket` to the ranks' role as well — without
    it S3 answers a GET for a missing key with 403, not 404, so every not-yet-written object looks
    like a refusal and `recover` raises on the first poll of the first collective.
  - Two preconditions this makes load-bearing. **`comm_name` must be unique per job run**: with
    nothing deleted, a same-named run inside the TTL window reads stale keys as live data, and
    its first barrier is satisfied instantly by the previous run's markers. And under a
    multi-backend config a send and its matching recv must size their buffers identically —
    `Communicator::recv` sizes the policy query with the *receiver's* buffer — which the
    exact-length check now reports rather than truncating past. `Utils::Timeout` remains
    **terminal for the communicator** (the counters are not retry-safe): `recover` makes timeouts
    rarer, not recoverable.
  - `fmi_redis.json` and `fmi_s3.json` of the criu-transparent runbook (fmi-spot-migration
    `benchmarks/migration/criu-transparent/`) are the checkpointing configs for this data
    plane, and each enables its one backend and nothing else on purpose —
    with DirectTCP also enabled the cost model routes every operation of that subject to
    DirectTCP, so a sweep would exercise no store code at all and report green. `sweep.py
    --config` picks the plane and cleans up after it; on S3 it also prints the requests and
    dollars a run will spend and refuses one above `--max-cost` (default $1) without `--yes`,
    because the sweep's default `--rounds 40000` is sized for a free plane and is $52 at eight
    peers. That runbook's README documents the invocations and the evidence.

- **Python bindings** (`python/`): a Boost.Python module. `fmi_python.cpp` is the module entry
  point — it registers the `Communicator` class and the type/op helpers (`hints`, `func`, `op`,
  `datatypes`, `types`); `PythonCommunicator.cpp` implements the wrapper the class binds to.
  Because Python is dynamically typed, collective calls take an explicit `fmi.types(...)`
  descriptor and return results directly rather than filling a receive buffer.

`include/fmi.h` is the umbrella header and includes `Communicator.h` and nothing else — it is
byte-identical to the pre-checkpointing version. The one checkpoint-era header reachable through
it is `comm/OperationScope.h`, which defines the `Lane`/`OpKind` identity enums and the
thread-local scope; `LinkFrame.h` includes that header, not the other way round, so the wire codec
is not pulled into an application's translation units. The ICS'23 paper and the thesis linked from `README.md` are the
authoritative design references; technical docs are generated with Doxygen (`docs/Doxyfile`,
output gitignored).

## Correctness fixes carried on this branch

These are repairs to defects that predate the checkpointing work and are unrelated to it. They
explain diffs in files that otherwise look untouched by the feature, and they must not be
reverted when reasoning about "what the checkpointing work cost":

- `ClientServer::scan` indexed `received`/`applied` (sized `peer_id + 1`) with a loop to
  `num_peers`, and folded past the end of the buffer — an out-of-bounds read feeding the
  reduction result for every rank except the last.
- `ClientServer::reduce`/`scan` seeded the accumulator with the local value, computing
  `f(v_root, v0, v1, ...)` instead of `f(v0, ..., v_{n-1})` — wrong at every root ≠ 0, which is
  exactly the case `left_to_right` exists to serve.
- `scan` advanced `num_operations["scan"]` *after* its `Timeout` throw, so a timed-out scan left
  the counter unadvanced and the next scan rewrote the abandoned generation.
- `Direct::send_object` issued one non-looping `::send` and silently truncated on a partial
  write; `recv_object` logged and returned with a partly-filled buffer on a short read. Both are
  fixed by `TcpChannelBase`'s `write_all`/`read_all`.
- `S3::get_object_names` issued one unpaginated, unprefixed `ListObjectsV2`: silently truncated
  at 1000 keys and listed every other job's objects in the bucket.
- The Python binding's `get_vec_function` CUSTOM branch hardcoded commutativity/associativity to
  `true`, ignoring the user's declared flags — which drive both algorithm choice and evaluation
  order.

## Known defects

Confirmed by review, unfixed as of 2026-08-17. Do not rediscover these. `TODO.md` at the repo root
carries the full work list this is drawn from — including the structural duplication, the dead
wire fields, the performance work, the doc corrections, and the items a same-day second-pass
review added (marked *(2nd pass)* there). **Both checkpoint-survival mechanisms are being kept and
benchmarked against each other**, so nothing in `TODO.md` proposes dropping either.

- `src/comm/TcpChannelBase.cpp:1088` — `maybe_send_ack` sits outside the enclosing `try` in
  `service_established_links`, and calls `write_all`. An ack whose socket had 1..41 bytes of room
  throws while the application waits on a *different* peer, violating the "servicing must not
  throw" contract stated at `TcpChannelBase.h:89`.
- `src/comm/DrainTCP.cpp:1610` — if `take_establish_gate()` throws, the catch clears
  `drain_pending` but not the `draining` flags already set on links that yielded, and rethrows
  before the outer handler that would `break_every_link`. The parked thread escapes only when
  `migration_max_ms` (120 s) expires, naming a peer that never migrated.
- `src/comm/DrainTCP.cpp:1320` — `finalize()` closes peer sockets abruptly and emits no leave
  event, so a drain-armed peer converts a normal shutdown into a tentative drain, waits
  `drain_grace_ms`, and dies with "an unplanned death is not recoverable". The same job with
  `drain: false` ends cleanly. There is no `DrainTCP` equivalent of
  `TcpChannelBase::drain_links_for_shutdown`.
- `src/utils/MigrationTrigger.cpp:112` — `forget_in_child()` documents assigning over a
  possibly inherited-locked mutex but never reconstructs or resets it; `attach`, `detach` and
  `drain_signal` later lock it. Fork-child deadlock risk.
- `src/utils/Signals.cpp:6` vs `src/comm/S3.cpp:32` — `suppress_sigpipe()` treats `SIG_IGN` as
  already owned and leaves it alone; S3 treats both `SIG_DFL` and `SIG_IGN` as unclaimed and
  installs the SDK handler, replacing a process-global disposition an application chose.
- `src/comm/Redis.cpp:273` and `:347` — `Utils::BackendFailure` is half-adopted. S3 throws it
  five times; Redis throws it zero, using plain `std::runtime_error` for the identical
  conditions, including a length mismatch where S3's matching check *does* throw
  `BackendFailure`.
- `src/comm/SequencedLink.cpp:61` — `admit` returns `false` for both "over
  `max_frame_bytes`" and "window full", so an oversized message burns the full `max_timeout` in
  `drain_acks` and then throws "at its retention limit with 0 bytes outstanding". Note
  `link_max_frame_bytes` (16 MiB) is also the one link parameter `parse_tcp_params` does not read
  from config.
- `src/comm/TcpChannelBase.cpp:384` — the stuck-rank beacon derives `waited` from a deadline two
  of its four call sites do not compute that way, so the value is constant and the guard is
  effectively never satisfiable under the shipped `max_timeout` values.
- `src/comm/TcpChannelBase.cpp:331` — the orphaned-link redial rescue requires
  `link_suspect_since.size() == num_peers`, but that vector is only ever sized by the
  live-fd peek path the rescue exists to substitute for. `ensure_link_state` sizes `links` and
  three siblings but not this one.
- `src/comm/DrainTCP.cpp:1149` — two comments assert a `LinkState::generation` check that does
  not exist. The counter is incremented four times and never read; correctness actually comes
  from re-reading `l.fd` under the lock.
- `src/comm/PeerRegistry.cpp:216`, `:273`, `:284` — every failure is reported as
  `"DirectTCP: registry error: ..."`, including when the caller is `DrainTCP` or
  `RedisDrainCoordinator`.
- `src/comm/DrainCoordinator.cpp:96`/`:101` — the members hash is write-only in production:
  `publish_member` runs at arm (`DrainTCP.cpp:311`) and after every restore (`:1814`), but
  nothing in `src/` calls `members()` — discovery goes through the transport registry via
  `lookup_peer`. Its only reader is a test fake, so `MemberRecord::decode` and `members()` are
  dead and each publish is a wasted Redis round trip.

## Structural notes for anyone extending this

- **Two mechanisms solve the same problem.** Sequenced links (retention/replay, survives an
  unplanned death, costs a 42-byte header and a retention copy per message) and neighborhood
  drain (coordinated quiesce, zero added bytes, any unplanned death is terminal) are independent
  solutions to surviving a criu freeze. Nothing in the tree yet benchmarks one against the other,
  so the choice between them is currently an argument rather than a measurement.
- **`DrainTCP` derives from `PeerToPeer`, not `TcpChannelBase`**, and re-implements listener
  bind, registry publish/parse, non-blocking dial, socket options and the cost model — the
  listener bind block exists three times in the tree, twice inside `DrainTCP.cpp` alone
  (`ensure_started` and `rebind_listener`). Do not blame the parenting choice for most of that:
  `TcpChannelBase` owns none of the establishment plumbing (no listener, registry, dial or
  accept — all of it lives in the sibling `DirectTCP`, behind the pure-virtual `establish()`),
  so deriving from the base would have recovered only `apply_socket_options`, the cost-model
  getters and the model-param parse, ~40 lines. The ~160–240 genuinely duplicated lines exist
  because establishment was never extracted into a shared helper (TODO §2); reparenting is not
  the fix. Of the two reasons given at `DrainTCP.h:26`, only the first holds: the base throws
  `LinkReplaced` and restarts at a frame boundary where drain must resume at a byte offset,
  which is a genuine conflict — but it constrains only the data path. The second reason
  (framing/retention cost) is **false as written**: both flags default off and `send_object`
  branches to `write_all` before any frame work, so an unconfigured `TcpChannelBase` already has
  a zero-overhead hot path.
- **A vanilla user links the whole migration stack.** `Channel::get_channel` references every
  compiled-in backend, so `DrainTCP.cpp.o`, `MigrationTrigger.cpp.o` and `DrainCoordinator.cpp.o`
  are pulled into any application regardless of config — those three TUs measure ~129 KB of
  `.text` (~160 KB of allocated sections) at `-O2`. (The +448 KB figure previously quoted here is
  the *whole branch's* static-link `.text` growth — sequenced links, DirectTCP and store recovery
  included — most of which no config or drain-only build option would remove.) The only way to
  avoid it is `FMI_ENABLE_REDIS=OFF`, which also removes the `Redis` channel; note that option
  gates hiredis and *everything* depending on it, not just the Redis backend.
- Constructing a `DrainTCP` channel starts its control thread even when `drain` is `false`,
  because `register_channel` calls `on_registered`, which calls `ensure_started`.

## Git Commits

- Commit after every meaningful, self-contained change — don't batch unrelated work.
- Each commit should leave the codebase in a working state.
- Write short, clear commit messages that describe what changed.
- If a task involves multiple logical steps, make a separate commit for each step.
- Stage only the files relevant to the current change.
