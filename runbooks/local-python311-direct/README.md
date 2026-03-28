# FMI Local Runbook: Direct Backend

This runbook mirrors the Lambda example, but runs the same two-peer FMI program locally without deploying to Lambda.

There are two useful local paths:

- native host debugging on Ubuntu 24.04 with Python `3.12` and distro Boost.Python
- Docker-based local execution with Python `3.11` when you want closer Lambda parity

It uses the `Direct` backend from FMI's config, which means:

- no Lambda deployment
- no S3 bucket
- one local TCPunch rendezvous server on port `10000`
- two local Python processes that share the same `comm_name`

## Native path for this machine

The native instructions below were verified on this host:

- Ubuntu `24.04`
- system `python3` = `3.12.3`
- distro `libboost-python-dev`

If you are on this same machine, prefer the native path below and use the system `python3`.

## 1. Initialize the repo correctly

The actual Git repo is the `fmi/` subdirectory.

```bash
cd fmi
git submodule set-url extern/TCPunch https://github.com/OpenCoreCH/TCPunch.git
git submodule update --init --recursive
```

## 2. Install the native build prerequisites

On Ubuntu 24.04, a `Direct`-only native FMI build needs the host toolchain and Boost headers/libs, but not AWS SDK or `hiredis`.

```bash
sudo apt update
sudo apt install -y \
  build-essential \
  cmake \
  zlib1g-dev \
  libboost-dev \
  libboost-system-dev \
  libboost-log-dev \
  libboost-python-dev
```

## 3. Select a native Python that matches Ubuntu's Boost.Python

```bash
cd fmi
export FMI_PYTHON=$(command -v python3)
"$FMI_PYTHON" -V
```

On Ubuntu 24.04, the simplest native choice is the distro `python3`, which is `3.12.x`.

If you prefer `uv`, use a `3.12` interpreter for the native build:

```bash
cd fmi
uv python install 3.12
export FMI_PYTHON=$(uv python find 3.12)
"$FMI_PYTHON" -V
```

Do not use `uv` Python `3.11` with the distro `libboost-python-dev` package on Ubuntu 24.04. That combination fails because the packaged Boost.Python build targets Python `3.12`.

## 4. Quick start on this machine

If you want the shortest native path for this Ubuntu 24.04 host, these are the exact commands:

```bash
cd /home/luca/fmi
export FMI_PYTHON=$(command -v python3)

sudo apt update
sudo apt install -y \
  build-essential \
  cmake \
  zlib1g-dev \
  libboost-dev \
  libboost-system-dev \
  libboost-log-dev \
  libboost-python-dev

cmake -S extern/TCPunch/server -B extern/TCPunch/server/build-debug
cmake --build extern/TCPunch/server/build-debug -j"$(nproc)"

cmake -S python -B python/build-native-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DPython3_EXECUTABLE="$FMI_PYTHON" \
  -DFMI_ENABLE_S3=OFF \
  -DFMI_ENABLE_REDIS=OFF \
  -DFMI_USE_STATIC_BOOST=OFF

cmake --build python/build-native-debug -j"$(nproc)"
```

## 5. Build FMI natively in debug mode

The CMake flags below intentionally disable the `S3` and `Redis` backends for this local debug build, so you do not need AWS SDK for C++ or `hiredis` on the host.

```bash
cd fmi
cmake -S extern/TCPunch/server -B extern/TCPunch/server/build-debug
cmake --build extern/TCPunch/server/build-debug -j"$(nproc)"

cmake -S python -B python/build-native-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DPython3_EXECUTABLE="$FMI_PYTHON" \
  -DFMI_ENABLE_S3=OFF \
  -DFMI_ENABLE_REDIS=OFF \
  -DFMI_USE_STATIC_BOOST=OFF

cmake --build python/build-native-debug -j"$(nproc)"
```

The Python extension gets built at `python/build-native-debug/fmi.so`.

## 6. Verify that the native module imports

```bash
cd fmi
PYTHONPATH="$PWD/python/build-native-debug" \
  "$FMI_PYTHON" -c 'import fmi; print(fmi.__file__)'
```

Expected result: it should print a path ending in `python/build-native-debug/fmi.so`.

## 7. Start the local TCPunch rendezvous server

Keep this running in a separate terminal:

```bash
cd fmi
./extern/TCPunch/server/build-debug/tcpunchd 10000
```

The local FMI config in this runbook already points to `127.0.0.1:10000`.

## 8. Run two local peers at the same time

Both processes must use the same `comm_name` and `num_peers`.

```bash
cd fmi
export FMI_COMM_NAME=fmi-local-$(date +%s)

PYTHONPATH="$PWD/python/build-native-debug" \
  "$FMI_PYTHON" runbooks/local-python311-direct/peer.py \
  --peer-id 0 \
  --num-peers 2 \
  --comm-name "$FMI_COMM_NAME" \
  > /tmp/fmi-local-peer0.json &
PID0=$!

PYTHONPATH="$PWD/python/build-native-debug" \
  "$FMI_PYTHON" runbooks/local-python311-direct/peer.py \
  --peer-id 1 \
  --num-peers 2 \
  --comm-name "$FMI_COMM_NAME" \
  > /tmp/fmi-local-peer1.json &
PID1=$!

wait $PID0 $PID1
cat /tmp/fmi-local-peer0.json
cat /tmp/fmi-local-peer1.json
```

Expected result:

- both outputs should report `sum_of_peer_ids: 1`
- both outputs should report `bcast_value: 42`

This exact flow was verified successfully on this machine with host Python `3.12`.

## 9. Debugging notes

Useful native debugging entry points:

- attach your debugger to `"$FMI_PYTHON"` while it runs `runbooks/local-python311-direct/peer.py`
- rebuild after changes with `cmake --build python/build-native-debug -j"$(nproc)"`
- keep `CMAKE_BUILD_TYPE=Debug` for symbols

## 10. Troubleshooting

If CMake fails while looking for Boost.Python:

- FMI now tries to resolve a Boost.Python component that matches the selected Python version first
- if CMake warns that it is falling back to generic `python3`, verify that the resolved Boost.Python library really matches the Python version you selected
- on Ubuntu 24.04, a native build with the distro Boost packages should use Python `3.12`
- if you specifically need a native Python `3.11` build, you must provide a matching Boost.Python `3.11` build yourself instead of using the distro package

If a run hangs, check these first:

- `tcpunchd` is still running and listening on port `10000`
- both peers use the same `--comm-name`
- `PYTHONPATH` points to `python/build-native-debug`
- no other process is already bound to port `10000`

If you want a different port, update both:

- `runbooks/local-python311-direct/fmi.json`
- the `tcpunchd` command line

## 11. Python 3.11 parity fallback

If you specifically want local execution with Python `3.11`, keep that path containerized. The Docker image already provides a compatible Python `3.11` and matching shared libraries.

```bash
cd fmi
docker build -t fmi-build-python311:latest -f runbooks/aws-python311-s3/Dockerfile.python3.11 .

docker run --rm \
  -v "$PWD":/workspace \
  -w /workspace \
  fmi-build-python311:latest \
  bash -lc '
    cmake -S extern/TCPunch/server -B extern/TCPunch/server/build &&
    cmake -S python -B python/build &&
    cmake --build extern/TCPunch/server/build -j"$(nproc)" &&
    cmake --build python/build -j"$(nproc)" &&
    export PYTHONPATH=/workspace/python/build &&
    export FMI_COMM_NAME=fmi-local-$(date +%s) &&
    ./extern/TCPunch/server/build/tcpunchd 10000 &
    TCPUNCH_PID=$! &&
    sleep 1 &&
    python3 runbooks/local-python311-direct/peer.py --peer-id 0 --num-peers 2 --comm-name "$FMI_COMM_NAME" > /tmp/fmi-local-peer0.json &
    PID0=$! &&
    python3 runbooks/local-python311-direct/peer.py --peer-id 1 --num-peers 2 --comm-name "$FMI_COMM_NAME" > /tmp/fmi-local-peer1.json &
    PID1=$! &&
    wait $PID0 $PID1 &&
    cat /tmp/fmi-local-peer0.json &&
    cat /tmp/fmi-local-peer1.json &&
    kill $TCPUNCH_PID
  '
```

For debugging on the host, the native Python `3.12` path above is the better default on Ubuntu 24.04.
