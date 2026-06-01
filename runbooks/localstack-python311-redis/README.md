# FMI LocalStack Transparent Migration Demo — Heterogeneous Topology

This runbook runs FMI's `transparent_migration` fault-tolerance mode on genuinely
heterogeneous execution substrates.  Epoch-0 ranks run as **real LocalStack EC2 instances**
(Docker VM Manager — each instance is a real `localstack-ec2.i-*` Docker container launched
from a custom AMI via `aws ec2 run-instances`), while the migrated replacement in epoch 1 is
a **LocalStack Lambda invocation** (ephemeral, serverless).  The migration therefore crosses
two distinct execution models, observable via `docker ps` and `aws ec2 describe-instances`.

Both the EC2 and Lambda paths run through a single LocalStack Pro endpoint at
`http://localhost:4566`.  EC2 VM Manager requires LocalStack Pro >= 4.4.0; the compose file
pins `localstack/localstack-pro:latest` (consider a concrete 4.x tag for reproducibility).

Note: state is not transferred on migration (work from epoch 0 is lost on quiesce); CRIU is
the path to a true stateful cut.

## Architecture

```
Epoch 0                                 Epoch 1
--------                                --------
rank 0: localstack-ec2.i-<id0>          rank 0: localstack lambda container
        (localstack-ec2/fmi-vm:                 (public.ecr.aws/lambda/python:3.11)
         ami-00000001)                           placement=serverless
         placement=vm
                           MIGRATION
rank 1: localstack-ec2.i-<id1>  ------> rank 1: localstack-ec2.i-<id1> (unchanged)
        (localstack-ec2/fmi-vm:                  placement=vm
         ami-00000001)
         placement=vm
```

All containers join `fmi-net` (via `EC2_DOCKER_FLAGS=--network fmi-net` and
`LAMBDA_DOCKER_NETWORK=fmi-net`) so `fmi-redis` resolves identically from every substrate.

## Prerequisites

- Docker with the Docker socket accessible at `/var/run/docker.sock`.
- LocalStack Pro auth token in `runbooks/localstack-python311-redis/.env`:
  ```
  LOCALSTACK_AUTH_TOKEN=ls-...
  ```
  This file is gitignored.  `docker compose` reads it automatically for `${VAR}` expansion.
- AWS CLI (`aws`) available on `PATH`.
- The host orchestrator imports the native debug module from `python/build-native-debug/fmi.so`.
  Build that first if it is not already present (see CLAUDE.md; use
  `-DFMI_ENABLE_REDIS=ON -DFMI_ENABLE_CRIU=OFF`).

## Start Services

```bash
cd runbooks/localstack-python311-redis
docker compose up -d
```

This starts LocalStack Pro on `localhost:4566` and Redis as `fmi-redis` on `fmi-net`.
Verify that the Pro license activates:

```bash
docker compose logs localstack | grep "Successfully requested and activated new license"
```

## Deploy the Function and AMI

```bash
./setup.sh
```

`setup.sh`:
1. Waits for the LocalStack health endpoint and Pro license activation.
2. Builds `fmi-localstack-build:redis-gcc10` (GCC 10 / Python 3.11 AL2 image).
3. Runs `make-fmi-bundle.sh` inside that image to produce `build/bundle/fmi.so` + `lib/`.
4. Builds the AMI image `localstack-ec2/fmi-vm:ami-00000001` from `Dockerfile.ami` — this
   bakes `fmi.so`, the shared libs, `worker_core.py`, `vm_worker.py`, and `fmi-worker.json`
   into a self-contained image that LocalStack EC2 launches directly as a container.
5. Creates or updates the `fmi-migration-worker` Lambda function.

## Run the Demo

```bash
python3 orchestrator.py run --comm-name demo-$(date +%s)
```

Expected output:

```text
[orchestrator] launched EC2 instance i-<id0> for rank 0
[orchestrator] launched EC2 instance i-<id1> for rank 1
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

During epoch 0, two real EC2 instances are visible:

```bash
aws --endpoint-url=http://localhost:4566 ec2 describe-instances \
    --query 'Reservations[].Instances[].[InstanceId,State.Name,ImageId]' \
    --output table
```

Expected output (two `running` instances on `ami-00000001`):
```
--------------------------------------------------
|             DescribeInstances                  |
+----------------+----------+-------------------+
|  i-<id0>       | running  | ami-00000001      |
|  i-<id1>       | running  | ami-00000001      |
+----------------+----------+-------------------+
```

The matching Docker containers:

```bash
docker ps --filter "name=localstack-ec2.i-"
```

```
CONTAINER ID  IMAGE                                 NAMES
...           localstack-ec2/fmi-vm:ami-00000001    localstack-ec2.i-<id0>
...           localstack-ec2/fmi-vm:ami-00000001    localstack-ec2.i-<id1>
```

During epoch 1 (after migration), the Lambda replacement appears:

```bash
docker ps
```

```
NAMES                                    IMAGE
localstack-ec2.i-<id1>                  localstack-ec2/fmi-vm:ami-00000001
...-lambda-fmi-migration-worker-...     public.ecr.aws/lambda/python:3.11
fmi-redis                               redis:7
localstack-...                          localstack/localstack-pro:latest
```

After the demo completes orchestrator.py terminates the EC2 instances and removes any
leftover `localstack-ec2.i-*` containers automatically.

## Cleanup

Clear FT state for a specific run:

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
| `vm_worker.py` | VM instance entrypoint — reads env vars, calls `run_worker` |
| `orchestrator.py` | Host driver: launches EC2 instances + Lambda replacement, asserts flip |
| `fmi-worker.json` | FMI config for workers (Redis on `fmi-net`) |
| `fmi-host.json` | FMI config for host orchestrator (Redis on `127.0.0.1`) |
| `setup.sh` | Builds bundle, builds AMI image, deploys Lambda function |
| `Dockerfile.build` | GCC 10 / Python 3.11 AL2 build environment |
| `Dockerfile.ami` | AMI image for LocalStack EC2 VM Manager (bakes bundle in) |
| `make-fmi-bundle.sh` | Builds `fmi.so` + runtime libs inside the build image |
| `.env` | `LOCALSTACK_AUTH_TOKEN=ls-...` (gitignored, required for Pro) |

## Known Limitations

- LocalStack Pro >= 4.4.0 is required for EC2 Docker VM Manager.  Pin a concrete version
  tag (e.g. `localstack-pro:4.4.0`) instead of `:latest` for reproducible builds.
- No state transfer on quiesce: work from epoch 0 is lost at migration. CRIU is the
  production path for a stronger transparent checkpoint/restore cut.
- The Lambda replacement can re-run phase-1 collectives after the VM survivor has moved on.
  The worker catches that exception; the directory flip is the success criterion.
- Consistent cut is informal: migration is observed at an operation boundary.
- No library code changes are included. If Redis epoch re-pairing exposes a library issue,
  treat that as a separate FMI fix rather than changing this runbook.
