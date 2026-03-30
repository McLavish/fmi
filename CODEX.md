# FMI (FaaS Message Interface)

FMI is a communication library for serverless and Function-as-a-Service workloads. It gives FaaS applications an MPI-like interface for point-to-point and collective communication, while selecting a concrete transport backend according to a latency and cost model.

The main user-facing abstractions are:

- `FMI::Communicator` for C++
- `fmi.Communicator` for Python

The built-in operations are:

- `send` / `recv`
- `bcast`
- `barrier`
- `gather`
- `scatter`
- `reduce`
- `allreduce`
- `scan`

FMI currently ships with three bundled backends:

- `S3`, implemented as a client-server channel on top of the AWS SDK for C++
- `Redis`, implemented as a client-server channel on top of `hiredis`
- `Direct`, implemented as a peer-to-peer channel on top of the `TCPunch` submodule

## Quick Start

FMI is configured with a JSON file such as [`config/fmi.json`](/home/luca/fmi-original/fmi/config/fmi.json) or [`config/fmi_test.json`](/home/luca/fmi-original/fmi/config/fmi_test.json). The file enables backends and defines the performance and cost model used by channel selection.

Minimal C++ usage:

```cpp
#include <fmi.h>

FMI::Communicator comm(peer_id, num_peers, "config/fmi.json", "MyApp", 512);
FMI::Comm::Data<int> value = peer_id;
FMI::Comm::Data<int> total;
FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);

comm.allreduce(value, total, sum);
```

To consume FMI from another CMake project:

```cmake
add_subdirectory(path/to/fmi)
target_link_libraries(${PROJECT_NAME} PRIVATE FMI)
target_include_directories(${PROJECT_NAME} PRIVATE ${FMI_INCLUDE_DIRS})
```

To build the Python extension from this repository:

```bash
cd python
mkdir -p build
cd build
cmake ..
make
```

This produces `python/build/fmi.so`.

Examples:

- C++ sample usage: [`tests/communicator.cpp`](/home/luca/fmi-original/fmi/tests/communicator.cpp)
- Python sample usage: [`python/tests/client.py`](/home/luca/fmi-original/fmi/python/tests/client.py)

## For Developers and Contributors

### Runtime Architecture

The `Communicator` constructor in [`src/Communicator.cpp`](/home/luca/fmi-original/fmi/src/Communicator.cpp) is the entry point for FMI runtime setup:

1. It loads JSON configuration through `FMI::Utils::Configuration`.
2. It discovers enabled backends from the `backends` section.
3. It instantiates channel objects through `FMI::Comm::Channel::get_channel(...)`.
4. It builds a `ChannelPolicy` from the enabled channels plus the FaaS pricing model.
5. Every operation delegates to the channel chosen for that operation and payload size.

The default policy implementation lives in [`src/utils/ChannelPolicy.cpp`](/home/luca/fmi-original/fmi/src/utils/ChannelPolicy.cpp). It compares the modeled latency and total price of each enabled backend and chooses either the fastest or cheapest option based on the current hint.

### Channel Hierarchy

The transport abstraction is rooted at [`include/comm/Channel.h`](/home/luca/fmi-original/fmi/include/comm/Channel.h).

- `Channel` defines the common API and default implementations for some collectives.
- `ClientServer` in [`include/comm/ClientServer.h`](/home/luca/fmi-original/fmi/include/comm/ClientServer.h) implements collectives for storage-backed channels where peers communicate through named objects or keys.
- `PeerToPeer` in [`include/comm/PeerToPeer.h`](/home/luca/fmi-original/fmi/include/comm/PeerToPeer.h) implements collectives for directly addressable channels.

Bundled channel implementations:

- [`src/comm/S3.cpp`](/home/luca/fmi-original/fmi/src/comm/S3.cpp): AWS S3 object-store backend
- [`src/comm/Redis.cpp`](/home/luca/fmi-original/fmi/src/comm/Redis.cpp): Redis backend
- [`src/comm/Direct.cpp`](/home/luca/fmi-original/fmi/src/comm/Direct.cpp): direct TCP backend using `TCPunch`

### Python Binding

The Python module is a thin wrapper around the C++ communicator:

- [`python/fmi_python.cpp`](/home/luca/fmi-original/fmi/python/fmi_python.cpp) defines the Boost.Python module named `fmi`
- [`python/PythonCommunicator.cpp`](/home/luca/fmi-original/fmi/python/PythonCommunicator.cpp) adapts Python objects, lists, and reduction functions to the typed C++ API
- [`python/CMakeLists.txt`](/home/luca/fmi-original/fmi/python/CMakeLists.txt) builds the shared module and links it against the core FMI library

### Deployment Assets

This repository includes AWS-oriented packaging and runbook material in addition to the core library:

- [`python/aws/`](/home/luca/fmi-original/fmi/python/aws) contains versioned AWS SAM templates and Makefiles for Lambda layer packaging
- [`runbooks/aws-python311-s3/`](/home/luca/fmi-original/fmi/runbooks/aws-python311-s3) contains a practical deployment runbook plus an example Lambda function configured for the `S3` backend

## Project Structure

The codebase is organized as follows:

- [`CMakeLists.txt`](/home/luca/fmi-original/fmi/CMakeLists.txt): root build entry point for the C++ static library
- [`cmake/`](/home/luca/fmi-original/fmi/cmake): custom CMake modules and package discovery helpers
- [`config/`](/home/luca/fmi-original/fmi/config): sample FMI configuration files and backend model parameters
- [`include/`](/home/luca/fmi-original/fmi/include): public headers
- [`include/comm/`](/home/luca/fmi-original/fmi/include/comm): backend and transport abstractions, channel APIs, and data wrappers
- [`include/utils/`](/home/luca/fmi-original/fmi/include/utils): configuration loading, policy logic, operation metadata, and helper types
- [`src/`](/home/luca/fmi-original/fmi/src): C++ implementation of the public library
- [`src/comm/`](/home/luca/fmi-original/fmi/src/comm): concrete backend implementations and collective algorithms
- [`src/utils/`](/home/luca/fmi-original/fmi/src/utils): configuration parsing and channel-selection policy
- [`python/`](/home/luca/fmi-original/fmi/python): Boost.Python binding layer and Python-side build
- [`python/aws/`](/home/luca/fmi-original/fmi/python/aws): AWS Lambda layer packaging templates by Python version
- [`tests/`](/home/luca/fmi-original/fmi/tests): Boost.Test-based C++ tests and example programs
- [`docs/`](/home/luca/fmi-original/fmi/docs): Doxygen configuration, landing page content, and static documentation assets
- [`extern/TCPunch/`](/home/luca/fmi-original/fmi/extern/TCPunch): git submodule used by the `Direct` backend for TCP NAT hole punching
- [`runbooks/`](/home/luca/fmi-original/fmi/runbooks): practical deployment instructions and end-to-end setup notes

## Build and Test Notes

Core library build:

```bash
cmake -S . -B build
cmake --build build
```

Important dependencies from the root build:

- C++17
- Boost
- Zlib
- AWS SDK for C++
- `hiredis`
- `TCPunch`

The root CMake file currently builds the `FMI` static library and the `TCPunch` client subproject. The `tests/` directory exists and has its own [`tests/CMakeLists.txt`](/home/luca/fmi-original/fmi/tests/CMakeLists.txt), but it is not enabled from the root `CMakeLists.txt` at the moment.

Current test and example files include:

- [`tests/channels.cpp`](/home/luca/fmi-original/fmi/tests/channels.cpp)
- [`tests/communicator.cpp`](/home/luca/fmi-original/fmi/tests/communicator.cpp)
- [`tests/client.cpp`](/home/luca/fmi-original/fmi/tests/client.cpp)
- [`python/tests/client.py`](/home/luca/fmi-original/fmi/python/tests/client.py)

Documentation can be generated from [`docs/Doxyfile`](/home/luca/fmi-original/fmi/docs/Doxyfile).

## Coding Conventions

This repository does not currently ship a separate `CONTRIBUTING.md` or formal in-tree style guide. Follow the conventions already present in the code:

- Keep public declarations in `include/` and matching implementations in `src/`
- Preserve the namespace split between `FMI`, `FMI::Comm`, and `FMI::Utils`
- Keep header guards and file-local structure consistent with nearby files
- Match the existing formatting style in the file you are editing instead of reformatting unrelated code
- When adding a new backend, implement the channel interface first and then wire it into `Channel::get_channel(...)`
