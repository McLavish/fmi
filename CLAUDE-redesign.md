# Developer guide for the `redesign` branch

`CLAUDE.md` is the first system's guide and is kept unchanged; on this branch this file
replaces it. Documents and results that existed before the redesign (the tag `pre-redesign`)
are never edited, moved or deleted here (Luca, 2026-11-11: "All the old docs and old results
should be kept. Just make new docs!"); what the redesign has to say goes into a new file, and
[REDESIGN.md](REDESIGN.md) lists them.

Read [README.md](README.md) for the public API and [REDESIGN.md](REDESIGN.md) for what differs
from it on this branch. FMI is a
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
| `FMI_ENABLE_REDIS` | `ON` | Builds Redis and DirectTCP, with their hiredis-based helpers |
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
./Boost_Tests_run --run_test=LocalDrainChannel
./Boost_Tests_run --list_content
```

Most tests require services. DirectTCP uses Redis for discovery. Direct requires
`tcpunchd` on port 10000. With `FMI_ENABLE_TCPUNCH=OFF`, the communicator tests
select DirectTCP and need no rendezvous server. The Local Drain tests need no
service: they start their own test host manager (`tests/test_host.h`). Adjust the
directory above for `build-local` or `build-s3`.

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
| `TcpChannelBase` | Shared plain socket data path for Direct and DirectTCP |
| `LocalDrain` | Plain TCP data path of a rank that can leave its host and come back; derives from PeerToPeer |
| `ClientServer` | Implements collectives through shared-store upload and download |
| `RecoverableClientServer` | Store recovery behavior shared by Redis and S3 |

Most communicator operations are header-only templates. `Data<T>` exposes scalar
or vector storage as bytes. `Function<T>` carries the reduction function and its
associativity and commutativity flags; those flags determine valid algorithms
and evaluation order. Python wrappers return results directly and use explicit
`fmi.types(...)` descriptors.

Configuration has two main blocks: `backends` enables and configures transports;
`model` provides their cost parameters and `FaaS.gib_second_price`. Only enabled
backends are constructed. A build option and a configuration flag have different
effects: disabling a backend in JSON does not remove its code from the binary.

### DirectTCP

DirectTCP is the plain channel: the wire carries payload and nothing else, a rank
stays where it started, and a link that breaks ends the job. It is the baseline
that Local Drain is measured against.

Each rank publishes an ephemeral listening address and nonce in the Redis hash
`fmi:direct:<comm_name>`. Higher rank IDs dial lower IDs. A 32-byte hello checks
that the connection reached the intended listener. Connections are established
as operations need them, and a rank keeps accepting while it waits in a read or a
write. Use [config/fmi_direct_tcp.json](config/fmi_direct_tcp.json) as a starting
point.

DirectTCP requires inbound connectivity to each rank. For hosts with multiple
interfaces, set `advertise_host` to an address peers can reach; a load-balanced
Kubernetes Service address cannot identify a particular rank.

`framed` and `recover_links` are refused with an error: sequenced links
(Retain-and-Replay) were removed on 2026-11-11.

### Local Drain

`LocalDrain` uses a plain byte stream, with no header and no retained copy per
message. It is the one channel whose ranks can be moved. The design, the messages
of the control link and the rules of rank state are in fmi-spot-migration's
`runtime/DESIGN.md`; read it before changing this channel.

**Control link.** A rank connects to the host manager of its host over a Unix
socket (`host_link` in the config, or `FMI_HOST_LINK` in the environment, which
wins). It registers there (`hello`), asks there where a peer listens (`where`),
gets rows of the rank table (`rank`), and is told there to leave (`leave`). The
channel uses no Redis and polls nothing.

**Service thread.** One thread per channel sleeps in epoll on the listener, the
control link and every peer link. It accepts connections while the application
computes, notices a peer that closes, and runs the whole leave sequence.

**Rank state only moves forward.** A row is `here` or `leaving` at a generation,
or `gone`. A rank applies a row only if it is newer than the one it holds
(`RankTable`), so repeated, late and reordered rows change nothing.

**Leaving.** On `leave`, the service thread waits for connections that are being
built, closes the listener, takes every link lock, half-closes every peer socket
and reads each to EOF into process memory. It reports `drained` with the byte
counters of every link, closes the control link last, and stops the process with
no socket left (`stop: sigstop`; `stop: hold` waits in place and exists for
tests). After the restore it opens a new listener, registers the next generation,
asks again for every peer it knew, waits for `synced`, and releases the locks.
Links reopen when an operation needs them, and the 56-byte hello compares the
byte counters of both ends. A rank that cannot drain reports `failed` and breaks
its links: a move that fails ends the job.

**A peer that leaves.** The FIN is the signal: the peer of a leaving rank closes
its side as soon as it reads EOF, without waiting for a message. The row confirms
that the close was a move. A close that no row explains within `drain_grace_ms`
is a death and ends the job. A peer that finalized (`gone`) is a timeout for an
operation that still waited for it.

**Timeouts.** Time an operation spends waiting out a move is not charged to
`max_timeout`, whether the operation began before the move or during it;
`migration_max_ms` bounds the wait for a peer to come back.

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

There is one migration mechanism, Local Drain. Retain-and-Replay (sequenced
links: frames, retention, acknowledgments, replay), the first Local Drain channel
(`DrainTCP` with its Redis coordinator and trigger thread) and the relocation
logic of DirectTCP were removed on 2026-11-11. Their last version is the tag
`pre-redesign`. Do not bring a second mechanism back without a decision recorded
in fmi-spot-migration's `runtime/DESIGN.md`.

`LocalDrain` does not derive from `TcpChannelBase`: its links are closed and
reopened while operations are in flight, so every step of its data path runs
under a per-link lock and resumes at a byte offset. `TcpChannelBase` has neither.

Preserve the independent correctness fixes already carried by this fork:
ClientServer scan bounds and reduction order, scan counter advancement on
timeout, complete Direct socket reads and writes, paginated and prefixed S3
listing, and Python custom reduction flags. These fix pre-existing behavior;
they are not optional migration features.

[TODO-redesign.md](TODO-redesign.md) is the detailed list of known defects and maintenance work.
Check a symbol's users in fmi-spot-migration before calling a library-local API
unused: benchmark probes also consume library instrumentation and control data.

## Git commits

- Commit after every meaningful, self-contained change; do not batch unrelated work.
- Each commit must leave the codebase in a working state.
- Use short, clear messages that describe the change.
- Make separate commits for unrelated logical steps.
- Stage only files relevant to the current change.
- Do not add `Co-Authored-By` or Claude trailers.
