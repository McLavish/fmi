# FMI LocalStack Transparent Migration Demo

This runbook runs FMI's `transparent_migration` fault-tolerance mode on local AWS Lambda
emulation. LocalStack provides concurrent Lambda containers, and Redis is used for both the
FMI data plane and the FT control plane.

## Prerequisites

Docker must be available. The setup script builds a demo-local Lambda-compatible image
tagged `fmi-localstack-build:redis-gcc10`, then builds a Redis-only `fmi.so` bundle from it.

The host orchestrator imports the native debug module from `python/build-native-debug/fmi.so`.
Build that first if it is not already present.

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
creates or updates the `fmi-migration-worker` function. The function zip contains
`lambda_function.py`, `fmi-worker.json`, `fmi.so`, and `lib/`; `LD_LIBRARY_PATH` is set to
`/var/task/lib`. The script uses the AWS CLI with `--endpoint-url=http://localhost:4566`;
it does not require `awslocal`.

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
--- Epoch 0 directory ---
rank 0: worker_id=rank0-vm-... placement=vm state=QUIESCED
rank 1: worker_id=rank1-vm-... placement=vm state=ACTIVE

--- Epoch 1 directory ---
rank 0: worker_id=rank0-serverless-... placement=serverless state=ACTIVE
rank 1: worker_id=rank1-vm-... placement=vm state=ACTIVE

PASSED: rank directory flip verified
```

During active migration, a concurrency sanity check should show two LocalStack function
containers at the same time:

```bash
docker ps
```

## Cleanup

Clear the Redis FT state for a specific run:

```bash
python3 orchestrator.py cleanup --comm-name <name>
```

Stop LocalStack and Redis:

```bash
docker compose down
```

## Known Limitations

- Partner-less tail on the replacement: the cold replacement can re-run phase-1 collectives
  after the survivor has moved on. The worker catches that exception; the directory flip is
  the success criterion, not collective values.
- Consistent cut is still informal: migration is observed at an operation boundary. CRIU is
  the production path for a stronger transparent checkpoint/restore cut.
- No library code changes are included. If Redis epoch re-pairing exposes a library issue,
  treat that as a separate FMI fix rather than changing this runbook.
