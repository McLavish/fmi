<img src="docs/fmi.svg" width="100%" alt="FMI">

# FaaS Message Interface

FMI is a C++17 communication library with Python bindings. It provides MPI-like
point-to-point operations and collectives for serverless and distributed
applications. A cost model selects a transport for each operation according to
message size and the application's preference for speed or cost.

This fork adds checkpoint and migration support to the original FMI library.
Applications, benchmark results, and migration drivers live in
[fmi-spot-migration](https://github.com/McLavish/fmi-spot-migration).

## Transports and migration

| Backend | How ranks communicate | Requirements |
|---|---|---|
| `Direct` | TCP connections established through TCPunch NAT traversal | A TCPunch rendezvous server |
| `DirectTCP` | Direct TCP connections | Reachable rank addresses and Redis for discovery |
| `DrainTCP` | Direct TCP with coordinated socket draining | Reachable rank addresses and Redis for discovery and migration control |
| `Redis` | Values in a shared Redis instance | A Redis server |
| `S3` | Objects in a shared bucket | An S3-compatible service |

`Direct` and `DirectTCP` support **Retain-and-Replay**: set `framed` and
`recover_links` to `true` to retain messages until acknowledged and replay them
when a connection is rebuilt. **Local Drain** uses `DrainTCP` with `drain: true`
to move socket data into process memory before a planned checkpoint. Store
backends use `recover: true` to reconnect and retry store operations.

These mechanisms support externally driven checkpoint and restore without
application checkpoint calls. They do not reconstruct a lost process: restoring
a rank requires its checkpoint image. Local Drain also requires coordination
before the connection is lost. See the [design guide](docs/design/README.md) for
protocol details and limitations.

## Build and use the C++ library

Dependencies are a C++17 compiler, CMake, and Boost, plus hiredis for Redis-based
backends, the AWS SDK for S3, and TCPunch for `Direct`. Optional backends can be
disabled at build time. From the repository root:

```bash
git submodule update --init --recursive
cmake -S . -B build -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_TCPUNCH=OFF
cmake --build build -j"$(nproc)"
```

This configuration builds the Redis, DirectTCP, and DrainTCP backends. For the
full build, test setup, and dependency options, see [CLAUDE.md](CLAUDE.md).

To use FMI from another CMake project:

```cmake
add_subdirectory(path/to/fmi)
target_link_libraries(your_application PRIVATE FMI::FMI)
```

The target supplies its public include path. Construct one communicator per rank:

```cpp
#include <Communicator.h>

FMI::Communicator comm(peer_id, num_peers, "config/fmi.json", "MyApp", 512);
```

Ranks must agree on the communicator name and size, and use distinct IDs in
`[0, num_peers)`. Use a unique communicator name for each job run. The final
argument is the memory allocation in MiB used by the cost model. Choose a
configuration from [config/](config/) and set its service addresses for your
installation.

## Build and use the Python binding

The binding requires Python development headers and a Boost.Python library built
for the same Python version. `python/` is a separate CMake project:

```bash
cmake -S python -B python/build -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_TCPUNCH=OFF
cmake --build python/build -j"$(nproc)"
export PYTHONPATH="$PWD/python/build${PYTHONPATH:+:$PYTHONPATH}"
```

Then create a communicator:

```python
import fmi

comm = fmi.Communicator(peer_id, num_peers, "config/fmi.json", "MyApp", 512)
```

Collective calls take explicit type descriptors such as
`fmi.types(fmi.datatypes.int)`. See [the examples](docs/mainpage.md),
[the Python sample](python/tests/client.py), and the Python version guidance in
[CLAUDE.md](CLAUDE.md).

## Documentation and citation

- [Developer guide](CLAUDE.md): builds, tests, architecture, and configuration.
- [Protocol design](docs/design/README.md): Retain-and-Replay and Local Drain.
- [TLA+ models](docs/tla/README.md): checked properties and model limitations.
- [Open work](TODO.md): known defects and proposed improvements.
- [Original ICS 2023 paper](https://spcl.inf.ethz.ch/Publications/.pdf/2023_ics_fmi.pdf)
  and [original thesis](https://doi.org/10.3929/ethz-b-000532425): the original FMI
  interface, transport selection, and evaluation.

The upstream API documentation is at [fmi.opencore.ch](https://fmi.opencore.ch).
Generate documentation for this fork with [docs/Doxyfile](docs/Doxyfile).

If you use FMI in your work, cite the original paper:

```
@inproceedings{10.1145/3577193.3593718,
author = {Copik, Marcin and B\"{o}hringer, Roman and Calotoiu, Alexandru and Hoefler, Torsten},
title = {FMI: Fast and Cheap Message Passing for Serverless Functions},
year = {2023},
isbn = {9798400700569},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3577193.3593718},
doi = {10.1145/3577193.3593718},
abstract = {Serverless functions provide elastic scaling and a fine-grained billing model, making Function-as-a-Service (FaaS) an attractive programming model. However, for distributed jobs that benefit from large-scale and dynamic parallelism, the lack of fast and cheap communication is a major limitation. Individual functions cannot communicate directly, group operations do not exist, and users resort to manual implementations of storage-based communication. This results in communication times multiple orders of magnitude slower than those found in HPC systems. We overcome this limitation and present the FaaS Message Interface (FMI). FMI is an easy-to-use, high-performance framework for general-purpose point-to-point and collective communication in FaaS applications. We support different communication channels and offer a model-driven channel selection according to performance and cost expectations. We model the interface after MPI and show that message passing can be integrated into serverless applications with minor changes, providing portable communication closer to that offered by high-performance systems. In our experiments, FMI can speed up communication for a distributed machine learning FaaS application by up to 162x, while simultaneously reducing cost by up to 397 times.},
booktitle = {Proceedings of the 37th International Conference on Supercomputing},
pages = {373–385},
numpages = {13},
keywords = {high-performance computing, I/O, serverless, function-as-a-service, faas},
location = {Orlando, FL, USA},
series = {ICS '23}
}
```

## Original authors

- [Marcin Copik](https://github.com/mcopik/), ETH Zurich.
- [Roman Böhringer](https://github.com/OpenCoreCH), OpenCoreCH.
