# FMI LocalStack Transparent Migration Demo — Heterogeneous Topology

This runbook runs FMI's `transparent_migration` fault-tolerance mode on genuinely
heterogeneous execution substrates.  Epoch-0 ranks run as **real long-lived Docker
containers** (a VM/instance analog — a persistent process holding state in memory), while
the migrated replacement in epoch 1 is a **LocalStack Lambda invocation** (ephemeral,
serverless).  The migration therefore crosses two distinct execution models, observable via
`docker ps`.

LocalStack Community EC2 is a non-executing mock, so a plain Docker container is the
correct VM stand-in; no Pro license is required.  Note that state is not transferred on
migration (work from epoch 0 is lost on quiesce); CRIU is the path to a true stateful cut.

## Architecture

```
Epoch 0                          Epoch 1
--------                         --------
rank 0: fmi-vm-rank0             rank 0: localstack lambda container
        (fmi-localstack-build:           (public.ecr.aws/lambda/python:3.11)
         redis-gcc10)                     placement=serverless
         placement=vm
                        MIGRATION
rank 1: fmi-vm-rank1  ---------->  rank 1: fmi-vm-rank1 (unchanged survivor)
        placement=vm                        placement=vm
```

The VM containers join `fmi-net` (same Docker network as the Lambda) so `fmi-redis`
resolves identically from both substrates.  The bind-mounted repo root exposes the same
`build/bundle/fmi.so` (GCC 10, AL2 ABI) that the Lambda zip contains, giving both
substrates identical library behaviour.

## Prerequisites

Docker must be available.  The setup script builds a demo-local Lambda-compatible image
tagged `fmi-localstack-build:redis-gcc10`, then builds a Redis-only `fmi.so` bundle from it.

The host orchestrator imports the native debug module from `python/build-native-debug/fmi.so`.
Build that first if it is not already present (see the top-level CLAUDE.md for the exact
cmake flags; use `-DFMI_ENABLE_REDIS=ON -DFMI_ENABLE_CRIU=OFF`).

## Start Services

```bash
cd runbooks/localstack-python311-redis
docker compose up -d
```

This starts LocalStack on `localhost:4566` and Redis as `fmi-redis` on the Docker network
`fmi-net`, with Redis also published to `127.0.0.1:6379` for the host orchestrator.

## Deploy the Function

```bash
./setup.sh
```

`setup.sh` waits for LocalStack health, builds the GCC 10 Redis-only function bundle, and
creates or updates the `fmi-migration-worker` function.  The function zip contains
`lambda_function.py`, `worker_core.py`, `fmi-worker.json`, `fmi.so`, and `lib/`;
`LD_LIBRARY_PATH` is set to `/var/task/lib`.  The script uses the AWS CLI with
`--endpoint-url=http://localhost:4566`; it does not require `awslocal`.

The same `fmi-localstack-build:redis-gcc10` image and `build/bundle/` are reused when
launching the VM containers, so no additional image build is needed.

## Run the Demo

```bash
python3 orchestrator.py run
```

You can also choose a stable communication name:

```bash
python3 orchestrator.py run --comm-name demo-$(date +%s)
```

Expected output includes epoch directory dumps like:

```text
[orchestrator] started VM container fmi-vm-rank0 (...)
[orchestrator] started VM container fmi-vm-rank1 (...)
[orchestrator] requesting migration of rank 0

--- Epoch 0 directory ---
rank 0: worker_id=rank0-vm-... placement=vm state=QUIESCED
rank 1: worker_id=rank1-vm-... placement=vm state=ACTIVE

--- Epoch 1 directory ---
rank 0: worker_id=rank0-serverless-... placement=serverless state=ACTIVE
rank 1: worker_id=rank1-vm-... placement=vm state=ACTIVE

PASSED: rank directory flip verified
```

## Heterogeneity Evidence

Run `docker ps` during the epoch-0 → epoch-1 transition to see three different containers
at the same time:

```
NAMES                     IMAGE                              STATUS
fmi-vm-rank0              fmi-localstack-build:redis-gcc10   Up 2 seconds
fmi-vm-rank1              fmi-localstack-build:redis-gcc10   Up 2 seconds
...-lambda-fmi-...-...    public.ecr.aws/lambda/python:3.11  Up 4 seconds
fmi-redis                 redis:7                            Up 55 minutes
localstack-...            localstack/localstack:3.8.1        Up 55 minutes
```

After migration completes:

- `fmi-vm-rank0` is gone (quiesced, exited via `--rm`)
- `fmi-vm-rank1` persists (VM survivor, now in epoch 1)
- A `public.ecr.aws/lambda/python:3.11` container replaces rank 0 (serverless)

The two epoch-1 peers (`fmi-vm-rank1` and the Lambda replacement) communicate over
different execution substrates sharing a single Redis data plane.

## Cleanup

Clear the Redis FT state for a specific run:

```bash
python3 orchestrator.py cleanup --comm-name <name>
```

Stop LocalStack and Redis:

```bash
docker compose down
```

## Code Layout

| File | Role |
|------|------|
| `worker_core.py` | Shared FMI collective body (`run_worker`), used by both substrates |
| `lambda_function.py` | Thin Lambda handler — parses event, calls `run_worker` |
| `vm_worker.py` | VM container entrypoint — reads env vars, calls `run_worker` |
| `orchestrator.py` | Host driver: launches VM containers + Lambda replacement, asserts flip |
| `fmi-worker.json` | FMI config for workers (Redis on `fmi-net`) |
| `fmi-host.json` | FMI config for host orchestrator (Redis on `127.0.0.1`) |
| `setup.sh` | Builds bundle, deploys Lambda function |
| `Dockerfile.build` | GCC 10 / Python 3.11 AL2 build environment |
| `make-fmi-bundle.sh` | Builds `fmi.so` + runtime libs inside the build image |

## Known Limitations

- No state transfer on quiesce: work from epoch 0 is lost at migration. CRIU is the
  production path for a stronger transparent checkpoint/restore cut.
- The Lambda replacement can re-run phase-1 collectives after the VM survivor has moved on.
  The worker catches that exception; the directory flip is the success criterion.
- Consistent cut is informal: migration is observed at an operation boundary.
- No library code changes are included. If Redis epoch re-pairing exposes a library issue,
  treat that as a separate FMI fix rather than changing this runbook.
