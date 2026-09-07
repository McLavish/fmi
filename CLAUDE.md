# Developer guide

Read [README.md](README.md) for the public API and project overview. FMI is a
C++17 library with a Boost.Python binding. This repository contains the library;
applications, deployment tools, benchmarks, and migration drivers are maintained
in [fmi-spot-migration](https://github.com/McLavish/fmi-spot-migration), which pins
FMI as a submodule. Make library changes in its own checkout.

## Building

Initialize the submodules before a full build:

```bash
git submodule update --init --recursive
cmake -S . -B build -DFMI_BUILD_TESTS=ON
cmake --build build -j"$(nproc)"
```

`extern/TCPunch` uses the McLavish fork. Its rendezvous server handles SIGPIPE,
and its client resolves hostnames with `getaddrinfo`. The documentation theme is
also a submodule.

| CMake option | Default | Effect |
|---|---|---|
| `FMI_ENABLE_S3` | `ON` | Builds S3; requires the AWS SDK for C++ |
| `FMI_ENABLE_REDIS` | `ON` | Builds Redis, DirectTCP, DrainTCP, and their hiredis-based helpers |
| `FMI_ENABLE_TCPUNCH` | `ON` | Builds Direct and links the TCPunch submodule |
| `FMI_USE_STATIC_BOOST` | `ON` | Requests static Boost libraries |
| `FMI_BUILD_TESTS` | `OFF` | Builds the Boost.Test executable |

The static library target is `FMI`, with alias `FMI::FMI`. It publishes the
`include/` directory; `FMI_INCLUDE_DIRS` is also exported to a parent project.
The root project builds the library and optional tests. Build the Python binding
through `python/`; build applications through fmi-spot-migration.

For a local build using DirectTCP and Redis, disable S3 and TCPunch:

```bash
cmake -S . -B build-local -DCMAKE_BUILD_TYPE=Debug \
  -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_TCPUNCH=OFF -DFMI_BUILD_TESTS=ON
cmake --build build-local -j"$(nproc)"
```

### S3 dependencies

The S3 backend was tested with AWS SDK for C++ **1.11.861**. To build that version,
install the build dependencies, including `libcurl4-openssl-dev`, `libssl-dev`,
and `ninja-build`, then run:

```bash
git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp -b 1.11.861 /tmp/aws-sdk-cpp
cmake -S /tmp/aws-sdk-cpp -B /tmp/aws-sdk-cpp/build -GNinja \
  -DBUILD_ONLY=s3 -DCMAKE_BUILD_TYPE=Release -DENABLE_TESTING=OFF \
  -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_INSTALL_PREFIX="$HOME/.local/aws-sdk-cpp"
cmake --build /tmp/aws-sdk-cpp/build --target install

cmake -S . -B build-s3 -DFMI_ENABLE_S3=ON -DFMI_ENABLE_REDIS=ON -DFMI_ENABLE_TCPUNCH=OFF \
  -DFMI_BUILD_TESTS=ON -DCMAKE_PREFIX_PATH="$HOME/.local/aws-sdk-cpp"
cmake --build build-s3 -j"$(nproc)"
```

If the loader cannot find the AWS libraries, add their installation directory to
`LD_LIBRARY_PATH`.

### Python

Use **uv** for Python package and interpreter management: `uv pip`, `uv venv`,
and `uv python install/find`, rather than bare `pip`, `venv`, or `virtualenv`.
Pass a selected interpreter to CMake with `-DPython3_EXECUTABLE=...`.

The interpreter must match Boost.Python. On Ubuntu 24.04, the distribution's
`libboost-python-dev` targets the system Python 3.12; it cannot be used with a
uv-managed Python 3.11. Use the matching system interpreter in that case, or
build a matching Boost.Python. Override detection with
`-DFMI_BOOST_PYTHON_LIBRARY=...` if necessary. The Python 3.11 Lambda build is in
fmi-spot-migration's `deploy/aws-lambda-python311-s3/`.

```bash
cmake -S python -B python/build-native-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DPython3_EXECUTABLE="$(command -v python3)" \
  -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_TCPUNCH=OFF -DFMI_USE_STATIC_BOOST=OFF
cmake --build python/build-native-debug -j"$(nproc)"
```

This produces `python/build-native-debug/fmi.so`. Add that directory to
`PYTHONPATH` or install the module on your Python module path.

## Testing

Tests require `FMI_BUILD_TESTS=ON`. Run the binary **from its own build directory**:
communicator tests load `../../config/<name>.json` relative to that directory.
For the full build above:

```bash
cd build/tests
./Boost_Tests_run
./Boost_Tests_run --run_test=Communicator/reduce
./Boost_Tests_run --run_test=LinkLayer
./Boost_Tests_run --list_content
```

Most tests require services. DirectTCP uses Redis for discovery, and the identity
tests also use Redis as the data plane. Direct requires `tcpunchd` on port 10000.
With `FMI_ENABLE_TCPUNCH=OFF`, the communicator tests select DirectTCP and need no
rendezvous server. Adjust the directory above for `build-local` or `build-s3`.

S3 failure tests use a local fake endpoint. Tests that require a real bucket skip
unless `FMI_S3_TEST_BUCKET` is set; optional settings are
`FMI_S3_TEST_REGION`, `FMI_S3_TEST_ENDPOINT`, `FMI_S3_TEST_PATH_STYLE`, and
`FMI_S3_TEST_SLOW`. Passing without bucket credentials therefore does not mean the
real-store integration tests ran. `tests/client.cpp` is a standalone example,
not a test suite.

The application and CRIU integration campaigns live in fmi-spot-migration under
`benchmarks/migration/`. Their READMEs document prerequisites and recorded results.

## Architecture

The call path is **Communicator → ChannelPolicy → Channel → backend**.

| Component | Responsibility |
|---|---|
| `Communicator` | Public point-to-point and collective API over `Data<T>` |
| `ChannelPolicy` | Chooses a backend using the operation, message size, model parameters, and `fast` or `cheap` hint |
| `PeerToPeer` | Implements collectives in terms of send and receive primitives |
| `TcpChannelBase` | Shared raw and framed socket data paths for Direct and DirectTCP |
| `DrainTCP` | Raw TCP data path with coordinated draining; derives from PeerToPeer |
| `ClientServer` | Implements collectives through shared-store upload and download |
| `RecoverableClientServer` | Store recovery behavior shared by Redis and S3 |

Most communicator operations are header-only templates. `Data<T>` exposes scalar
or vector storage as bytes. `Function<T>` carries the reduction function and its
associativity and commutativity flags; those flags determine valid algorithms
and evaluation order. Python wrappers return results directly and use explicit
`fmi.types(...)` descriptors.

`OperationScope` publishes operation identity in thread-local state so frames can
be checked against the receive that consumes them. A scope saves and replaces the
previous identity, then restores it on exit. Normal collective implementations
call channel primitives within the communicator's scope. The implementation does
**not** enforce the old comment's claim that the outermost nested scope wins.

Configuration has two main blocks: `backends` enables and configures transports;
`model` provides their cost parameters and `FaaS.gib_second_price`. Only enabled
backends are constructed. A build option and a configuration flag have different
effects: disabling a backend in JSON does not remove its code from the binary.

### DirectTCP discovery and relocation

Each rank publishes an ephemeral listening address and nonce in the Redis hash
`fmi:direct:<comm_name>`. Higher rank IDs dial lower IDs. A 32-byte hello checks
that the connection reached the intended listener. Connections are established
as operations need them. Use [config/fmi_direct_tcp.json](config/fmi_direct_tcp.json)
as a starting point.

DirectTCP requires inbound connectivity to each rank. For fixed hosts with
multiple interfaces, set `advertise_host` to an address peers can reach; a
load-balanced Kubernetes Service address cannot identify a particular rank.
For cross-host checkpoint restore, **leave `advertise_host` empty** so the
transport can discover its new address.

`reset_transport_if_relocated()` detects a changed kernel boot ID, discards stale
socket and discovery state, and marks each severed link for reconciliation. Each
rebuilt link then performs its handshake and replay. Publication also refreshes
the advertised address. A same-host restore does not trigger the boot-ID reset.

### Retain-and-Replay

On Direct or DirectTCP, `framed: true` adds operation identity and sequence fields.
`recover_links: true` also retains sent payloads until acknowledged and replays
the unacknowledged suffix after reconnecting. Both flags default to false, and
recovery requires framing.

The current wire format is version 4: a **42-byte frame header** and **30-byte
handshake payload**. Serialized link snapshots use version 3. Rebuild every rank
together when changing wire versions; mixed versions can fail during the initial
read, before a version error is reported. The old sequenced-link incarnation
fence was removed in version 4; DrainTCP still uses incarnations separately.

A receiver acknowledges data only after the whole payload is in application
memory or a receive queue. Queued frames must be consumed in order. One-way
traffic needs standalone ACKs to free sender retention. Progress comes from
application calls that service sockets; there is no background replay engine
that runs through arbitrary compute phases. See the
[implementation rules](docs/design/2026-07-30-sequenced-links-implementation.md)
and [model limitations](docs/tla/README.md).

### Local Drain

DrainTCP uses a raw byte stream, with no frame header or retained copy per
message. Per-link locks protect individual nonblocking I/O steps; waits happen
outside the lock so a drain can interrupt an operation between chunks. A
56-byte connection record checks incarnations and byte counters after reconnect.
A control thread accepts connections even while the application is computing.

With `drain: true`, `MigrationTrigger` handles process-wide migration requests and
`DrainCoordinator` exchanges events through Redis. Only **one armed drain channel
per process** is supported. Every rank must include `control` in its `trigger`
setting so peers can cooperate with a migration; `both` includes signal and
control requests.

Migration closes the establishment gate, publishes `leaving`, parks the control
thread, and locks the links. It half-closes all peer sockets before draining all
of them to EOF into process memory. It then publishes `sealed`, closes remaining
service connections, trims the heap, and stops the process. After restore it
advances the epoch and incarnation, binds and advertises a fresh listener,
publishes `restored`, and releases the locks. Peer connections reopen lazily.
Rehearsal uses the same sequence without an external checkpoint.

The intended timeout policy excludes migration time from normal I/O budgets and
uses `migration_max_ms` to bound migration waits. **Two defects reproduced on
2026-09-07 remain open:** operations entered during a held drain lock can lose
that time credit, and stale queued control requests can trigger another migration
after restore. See [TODO.md](TODO.md). A larger `max_timeout` does not fix the
stale-request defect.

Local Drain handles planned migration. An unexpected connection loss becomes a
terminal error if no matching leave notice arrives within `drain_grace_ms`.
Cross-host CRIU restore requires privileges that preserve the source's
`CLOCK_MONOTONIC` through a time namespace; `--unprivileged` can inherit a different
host clock and expire deadlines immediately. See the
[drain protocol](docs/design/2026-08-11-neighborhood-drain-protocol.md).

### Redis and S3 recovery

`recover: true` enables connection recovery and retries. Protocol counters remain
in the checkpointed process, while whole values remain in the store. Redis
recovery frees a failed hiredis context and creates a new one; clearing `err` in
place could resend a partial buffered command.

A write must succeed within its retry budget or throw. Redis download connection
failures return `false` so the caller's bounded poll loop can retry; a wrong-sized
value throws. Permanent store errors must not be treated as temporary absence.
Redis's `connect_timeout_ms` and `io_timeout_ms` default to 1000 ms; the latter is
clamped to 100–60000 ms independently of the poll interval.

Under recovery, finalize does not delete objects that a peer may still need.
Redis writes use `object_ttl_s` (default 3600 seconds; zero disables expiry).
**S3 ignores that setting: configure bucket lifecycle expiration.** Grant
`s3:ListBucket` as well as object permissions; otherwise a missing object can
produce 403 instead of 404 and be treated as an access failure.

S3 defaults are `connect_timeout_ms=3000`, `request_timeout_ms=15000`,
`max_attempts=4`, `max_connections=4`, and `transient_failure_limit=200`.
The request timeout sets both AWS SDK timeout fields. Transient failures are
bounded by consecutive failure count and `max_timeout` wall time; successful
responses, including absence, reset that sequence. Error classification uses
HTTP status and exception name because the tested SDK can map `ExpiredToken` to
`UNKNOWN`. Configuration also supports `credentials_provider`, `endpoint_url`,
and `use_path_style`.

Use a unique `comm_name` for every job run to avoid reading retained objects from
an earlier run. Sender and receiver must agree on buffer sizes, particularly
when the policy can choose among several backends. A `Utils::Timeout` is
**terminal for the communicator**: operation counters do not support retrying
the same application call after a timeout.

## Maintenance constraints

Keep both migration mechanisms; their different overheads are measured in
fmi-spot-migration. Do not treat the absence of a per-message drain header as the
absence of thread, discovery, or code-size costs.

DrainTCP and DirectTCP duplicate some connection-establishment code.
`TcpChannelBase` does not own their listeners, registries, or dial loops, so
reparenting DrainTCP would not remove most of that duplication. Its raw stream
must also resume at a byte offset rather than restart at a frame boundary.

Preserve the independent correctness fixes already carried by this fork:
ClientServer scan bounds and reduction order, scan counter advancement on
timeout, complete Direct socket reads and writes, paginated and prefixed S3
listing, and Python custom reduction flags. These fix pre-existing behavior;
they are not optional migration features.

[TODO.md](TODO.md) is the detailed list of known defects and maintenance work.
Check a symbol's users in fmi-spot-migration before calling a library-local API
unused: benchmark probes also consume library instrumentation and control data.

## Git commits

- Commit after every meaningful, self-contained change; do not batch unrelated work.
- Each commit must leave the codebase in a working state.
- Use short, clear messages that describe the change.
- Make separate commits for unrelated logical steps.
- Stage only files relevant to the current change.
- Do not add `Co-Authored-By` or Claude trailers.
