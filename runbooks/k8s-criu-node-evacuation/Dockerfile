# CRIU node-evacuation demo image.
#
# Ships the FMI rank agent (fmi-rank-agent), the transparent_state_transfer_demo worker, a
# robust tcpunchd, and criu, so one image can serve every role in the demo: the per-machine
# pods (run workers + are exec'd into to drive `migrate-local`), the tcpunchd rendezvous, and
# the orchestrator Job. CRIU forces the Direct data plane, hence tcpunchd ships here too.
#
# Build context is the repo root:
#   docker build -f runbooks/k8s-criu-node-evacuation/Dockerfile -t fmi-criu-evac:dev .
#
# Single stage on Ubuntu: criu/restore reliability depends on matching the build and run
# userspace, and keeping it one image avoids any ABI/shared-lib mismatch between stages.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Build deps (gcc/cmake/boost/hiredis), criu build deps, and runtime deps (python control libs,
# envsubst, net tools used by the runbook preflights). Kept in one layer so the produced binaries
# always find their shared libraries at runtime. criu is NOT packaged for Ubuntu 24.04 and the
# host/cluster kernels are recent, so it is built from source (v4.2) below.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake git patch ca-certificates \
        libboost-all-dev libhiredis-dev \
        libprotobuf-dev libprotobuf-c-dev protobuf-c-compiler protobuf-compiler python3-protobuf \
        libnl-3-dev libnet-dev libcap-dev libbsd-dev libnftables-dev uuid-dev pkg-config \
        python3 python3-redis python3-kubernetes \
        gettext-base iproute2 iputils-ping netcat-openbsd procps redis-tools \
    && rm -rf /var/lib/apt/lists/*

# Build criu from source (pinned to v4.2, matching the version the demo was verified against).
RUN git clone --depth 1 --branch v4.2 https://github.com/checkpoint-restore/criu.git /criu-src \
    && make -C /criu-src -j"$(nproc)" \
    && install -D -m0755 /criu-src/criu/criu /usr/local/sbin/criu \
    && rm -rf /criu-src \
    && criu --version

# Build FMI (library + tools + the local-criu worker) and tcpunchd from the source tree.
COPY . /src
WORKDIR /src
# Apply the tcpunchd robustness patch. `--forward` exits non-zero when the working tree copied
# into the image already has it applied, so accept that case by verifying the patched marker is
# actually present — but fail the build on any other patch failure (e.g. a submodule bump that
# makes the patch stop applying would otherwise silently ship an unpatched, SIGPIPE-crashing
# tcpunchd).
RUN { patch -p1 -d /src/extern/TCPunch --forward \
        < /src/runbooks/k8s-criu-node-evacuation/patches/0001-tcpunchd-robustness.patch \
      || grep -q "SIG_IGN" /src/extern/TCPunch/server/hole_punching_server.cpp; } \
    && grep -q "getaddrinfo" /src/extern/TCPunch/client/tcpunch.cpp \
    && cmake -S /src -B /src/build-img \
        -DFMI_ENABLE_CRIU=ON -DFMI_ENABLE_REDIS=ON -DFMI_ENABLE_S3=OFF \
        -DFMI_BUILD_TOOLS=ON -DFMI_BUILD_TESTS=OFF -DFMI_USE_STATIC_BOOST=OFF \
    && cmake --build /src/build-img -j"$(nproc)" \
    && cmake -S /src/extern/TCPunch/server -B /src/extern/TCPunch/server/build-img \
    && cmake --build /src/extern/TCPunch/server/build-img -j"$(nproc)"

# Stage the binaries and demo assets, then drop the source/build tree.
RUN mkdir -p /opt/fmi/bin /opt/fmi/app \
    && cp /src/build-img/tools/fmi-rank-agent /opt/fmi/bin/ \
    && cp /src/build-img/runbooks/local-criu-state-transfer/transparent_state_transfer_demo /opt/fmi/bin/ \
    && cp /src/extern/TCPunch/server/build-img/tcpunchd /opt/fmi/bin/ \
    && cp /src/runbooks/k8s-criu-node-evacuation/fmi-machine.json.tmpl /opt/fmi/app/ \
    && cp /src/runbooks/k8s-criu-node-evacuation/local-two-machines.sh /opt/fmi/app/ \
    && cp /src/runbooks/k8s-criu-node-evacuation/supervisor.py /opt/fmi/app/ \
    && cp /src/runbooks/k8s-criu-node-evacuation/orchestrator.py /opt/fmi/app/ \
    && rm -rf /src

ENV PATH=/opt/fmi/bin:${PATH}
ENV FMI_DEMO_BIN=/opt/fmi/bin/transparent_state_transfer_demo
ENV FMI_AGENT_BIN=/opt/fmi/bin/fmi-rank-agent
WORKDIR /opt/fmi/app

# Default entrypoint is the per-machine supervisor; the orchestrator Job and tcpunchd
# Deployment override the command.
CMD ["python3", "-u", "/opt/fmi/app/supervisor.py"]
