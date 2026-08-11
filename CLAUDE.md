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
parity, build inside the Docker image (`runbooks/aws-python311-s3/Dockerfile.python3.11`).

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
`Channels`, `Communicator`, `LinkLayer`, `LinkRecovery`, `LinkLiveness`, `LinkIncarnations`,
`FramedTransport`, `TransportRecovery`, `OperationIdentity`, `ProtocolValidation`,
`ProtocolEdgeCases`, `ProtocolFuzz`, `CheckpointFreezePoints`, `ClientServerRecovery`,
`S3Backend`, `DrainTransport`.
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

The one standalone program outside `tests/` is the transparent-checkpoint subject
(`runbooks/criu-transparent-checkpoint/`), built to
`build/runbooks/criu-transparent-checkpoint/fmi_checkpoint_subject` by any top-level
Redis-enabled build. It is an ordinary FMI application with no checkpoint API in it; that
directory's `sweep.py` (single host) and `multihost_sweep.py` (over ssh across nodes) freeze it
with criu at random instants and compare every rank's final checksums against a clean run.

## Running things

Every peer in a communicator must agree on `comm_name` and `num_peers`; `peer_id` is in
`[0, num_peers)`. Two verified step-by-step runbooks ship with the repo: the AWS Lambda + S3
flow in `runbooks/aws-python311-s3/`, and `runbooks/criu-transparent-checkpoint/`, which
checkpoints and restores one rank of an unmodified `DirectTCP` job with `criu` driven entirely
from outside the process. JSON config templates live in `config/`.

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
  - `runbooks/criu-transparent-checkpoint/` freezes an unmodified DirectTCP job raw — no hooks
    — and survives on the sequenced-link recovery alone. `advertised_ip` and the listener are
    set once in `ensure_listener()` and only torn down by `close_transport_state()` at
    finalize/destruction, so a restored process keeps the address and port its image captured.
    Correct on the machine it was dumped on, wrong on any other, and there is no longer a
    pre-checkpoint hook that drops them — `Channel::prepare_for_checkpoint` went with the epoch
    migration runtime that was its only caller. **Treat cross-host restore as unbacked** until
    something re-establishes it.
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
  fails loudly at the reconnect instead of desyncing the stream silently. Migration config keys
  (`drain`, `trigger`, `drain_grace_ms`, `migration_max_ms`, ...) are parsed and stored; the
  migration runtime itself (trigger, coordinator, drain/restore legs) is the next stage.
  An *unplanned* connection death is a loud error by design — this protocol handles planned
  migration only, never fault tolerance. Example configs: `config/fmi_drain_tcp.json`
  (drain armed, one enabled backend on purpose), `config/fmi_draintcp_test.json`.

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
  `docs/superpowers/specs/2026-07-27-sequenced-incarnation-links-design.md`, machine-checked
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
  - `LinkFrame`/`SequencedLink` also carry an **incarnation**, a lineage fence distinguishing
    "the same process, restored" from "a different process now serving this rank". It is
    permanently `0` today: its producer was the deleted control plane and nothing has replaced
    it, so the field is inert rather than absent — see the comment on
    `Communicator::incarnation`.

- **Store-family recovery** (`include/comm/RecoverableClientServer.h`,
  `src/comm/RecoverableClientServer.cpp`, `src/comm/Redis.cpp`, `src/comm/S3.cpp`): the same idea as the sequenced
  link layer for the other channel family, in the same place in the hierarchy —
  `RecoverableClientServer` sits between `ClientServer`, which keeps the collective algorithms
  and is untouched, and the concrete backends, exactly as `TcpChannelBase` sits between
  `PeerToPeer` and `Direct`/`DirectTCP`. One opt-in flag, `"recover": "true"` on the backend's
  config block, **off by default**: unset, every override delegates to the base and the keys, the
  deletes and the byte copies are what the family always wrote. The only flag-off difference
  anywhere is that a NULL hiredis reply is now a named `std::runtime_error` instead of a null
  dereference. **Both backends implement the flag.** `Redis` was first; `S3` refused it at
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
  - `runbooks/criu-transparent-checkpoint/fmi_redis.json` and `fmi_s3.json` are the checkpointing
    configs for this data plane, and each enables its one backend and nothing else on purpose —
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

`include/fmi.h` is the umbrella header and includes `Communicator.h` and nothing else. The
ICS'23 paper and the thesis linked from `README.md` are the authoritative design references;
technical docs are generated with Doxygen (`docs/Doxyfile`, output gitignored).

## Git Commits

- Commit after every meaningful, self-contained change — don't batch unrelated work.
- Each commit should leave the codebase in a working state.
- Write short, clear commit messages that describe what changed.
- If a task involves multiple logical steps, make a separate commit for each step.
- Stage only the files relevant to the current change.
