# Experiment: Serverless → VM stateless rank migration

This runbook documents `serverless_to_vm_demo.py`: a local, single-host experiment that
shows a logical rank surviving a move from one "environment" to another **while a collective
keeps working** — without the application persisting any state of its own.

It builds on the base Direct-backend runbook in [`README.md`](README.md) (toolchain, TCPunch,
how to build). Read that first if you have not built FMI on this machine yet. The only extra
requirement here is a **Redis** control plane (see [Prerequisites](#prerequisites)).

---

## 1. The story

Two ranks run an `allreduce` together, but they live in different "places":

- **rank 0** is labelled `serverless` (think: an AWS Lambda invocation)
- **rank 1** is labelled `vm` (think: a long-lived EC2/VM worker)

Then rank 0 is **evicted** from serverless (a simulated Lambda timeout/return). The whole
point: rank 0 *migrates to the VM* and the job continues. The migration is **stateless** —
nothing of the application survives the move except the **logical rank identity** (rank 0 is
still rank 0). A brand-new worker process picks up that identity on the VM side and the
collective resumes.

```
            epoch 0                         migration                 epoch 1
   ┌───────────────────────┐                                ┌───────────────────────┐
   │ rank0  @ serverless    │ ── evicted (MigrateSelf) ──▶ ✗ │ rank0  @ vm  (NEW pid) │
   │ rank1  @ vm            │ ───────── stays ────────────▶  │ rank1  @ vm  (same pid)│
   └───────────────────────┘                                └───────────────────────┘
        allreduce  ⇒ 3                                            allreduce ⇒ 201
```

### Topology (all on `localhost`)

| Piece                 | Role                                                   | Address              |
| --------------------- | ------------------------------------------------------ | -------------------- |
| Redis                 | FT **control plane** + placement directory             | `127.0.0.1:6379`     |
| `tcpunchd`            | Direct/TCP **rendezvous** (NAT hole-punch)             | `127.0.0.1:10000`    |
| rank 0 worker         | `serverless`-labelled process (then evicted)           | local process        |
| rank 1 worker         | `vm`-labelled process (stays)                          | local process        |
| replacement rank 0    | `vm`-labelled process, **new** worker id               | local process        |

Config is [`fmi-ft.json`](fmi-ft.json) reused as-is: `Direct` enabled and preferred as the
data backend, Redis as the FT control backend, `safe_point_only` (the `SafePointRestart` FT
mode).

---

## 2. How it works

### Two planes

FMI's fault tolerance splits responsibilities into two planes:

- **Control plane (Redis).** Tracks the *epoch* (a monotonically increasing reconfiguration
  generation), *membership* (`rank → worker_id`), per-rank *leases* (liveness), the set of
  *pending migrations*, and — added for this experiment — the *placement directory*
  (`rank → "serverless" | "vm"`). Keys live under `fmi:ft:<comm>:…`.
- **Data plane (Direct/TCP).** The actual `allreduce` traffic. Peers find each other through
  the `tcpunchd` rendezvous and open direct TCP connections; the cost model in `fmi-ft.json`
  picks `Direct` for these small messages.

### Epoch fencing

Everything backend-visible is **qualified by epoch**. The communicator name becomes
`<comm>@epoch=N`, so Direct pairing names, Redis/S3 object names, and per-instance operation
counters all carry the epoch. That guarantees a stale message or half-open socket from
epoch 0 can never be consumed in epoch 1 after reconfiguration.

### The migration protocol

The application drives reconfiguration by calling `session.safe_point()` between
communication phases. Walking the demo timeline:

1. Both workers register into **epoch 0** (membership + lease + placement) and run the
   phase-1 `allreduce` → `1 + 2 = 3`.
2. The orchestrator calls `coordinator.request_migration(0)` — this just sets a `pending`
   flag for rank 0 in Redis (it is an *external* trigger; the workers are not interrupted).
3. At the next `safe_point()`:
   - **rank 0** sees it is the pending rank → returns `MigrateSelf`. In this stateless demo
     the worker simply **exits** (like a Lambda returning — no checkpoint).
   - **rank 1** is not pending → it registers itself into **epoch 1** and blocks inside
     `safe_point()`, waiting for the epoch to fill.
4. The orchestrator launches a **replacement** rank 0 with a *new* `worker_id` and
   `--placement vm`. Because rank 0 is pending and the stored worker differs, the new process
   joins as a *replacement candidate*: it does **not** touch epoch 0, it goes straight to
   `safe_point()` and registers into **epoch 1**.
5. Once epoch 1 has a live member for every rank (`live_member_count ≥ num_peers`), the
   epoch is **promoted** (`current_epoch = 1`). Every session observes the new epoch, rebuilds
   its `Communicator` for `<comm>@epoch=1` (fresh, epoch-fenced Direct re-pairing), and
   `safe_point()` returns `Reconfigured`.
6. Phase-2 `allreduce` runs on epoch 1 → `100 + 101 = 201`. The rank survived the move.

### The placement directory (the FMI change this experiment added)

Placement is a **sibling Redis hash**, `fmi:ft:<comm>:epoch:<N>:placement`, holding
`rank → placement`. It is written next to the existing membership whenever a session
registers itself, and exposed for inspection via the coordinator:

- `Coordinator::set_placement(epoch, rank, placement)`
- `Coordinator::placement_for_rank(epoch, rank)`
- `Coordinator::directory_snapshot(epoch) → [RankDirectoryEntry{rank, worker_id, placement, state}]`

It is purely **descriptive / observational metadata** — a "who is where" directory. It does
**not** influence channel selection or routing; the cost model still chooses the backend.
Comparing the epoch-0 vs epoch-1 snapshots is what lets the demo *prove* that rank 0 flipped
`serverless → vm` while keeping the same logical id.

---

## 3. Is this transparent to the application?

**Partly. It is transparent at the communication layer, but cooperative at the application
layer.** Be precise about which.

**What FMI handles for you (transparent):**
- The **logical rank identity** is preserved across the move (rank 0 stays rank 0).
- The **collective keeps working** after migration: FMI rebuilds the messaging layer and
  re-pairs Direct/TCP sockets through the rendezvous automatically. The app never manages
  addresses, NAT hole-punching, membership, or epochs.
- **Epoch fencing** prevents stale cross-epoch messages — you do not reason about that.

**What the application must do (cooperative — *not* transparent):**
- Use `fmi.FTSession` instead of a plain `Communicator`.
- Call `safe_point()` at safe boundaries (between phases) — *the app* decides where it is
  legal to reconfigure.
- React to the returned event: `None` (carry on), `MigrateSelf` (you are being evicted —
  return/exit, after checkpointing if you are stateful), `Reconfigured` (the communicator was
  rebuilt for a new epoch — carry on).
- **Own its own state.** `SafePointRestart` reconfigures *communication only*; it does not
  move application memory. This demo is deliberately **stateless** (only the rank identity
  survives), so the replacement just re-runs the `allreduce`. A stateful app would
  save/restore its data around `MigrateSelf`/`Reconfigured` (see `ft_migration_demo.py`,
  which checkpoints to disk).

**The fully-transparent alternative.** FMI also has a `CriuCoordinated` mode where the app
keeps using a *plain* `Communicator` (no `safe_point()` calls) and the whole process is
checkpoint/restored with CRIU. That one is genuinely transparent — but it is **same-host
only, `Direct`-only, and has no Python binding**, because real checkpoint/restore needs
kernel capabilities. This experiment uses `SafePointRestart` precisely because it works
*across* environments (serverless ↔ VM), which is the scenario we care about.

---

## Prerequisites

1. **Build `fmi.so` with Redis enabled** (FT needs the Redis control plane — so, unlike the
   base Direct-only build in `README.md`, do **not** disable Redis):

   ```bash
   cmake -S python -B python/build-native-debug -DCMAKE_BUILD_TYPE=Debug \
     -DPython3_EXECUTABLE="$(command -v python3)" \
     -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_REDIS=ON -DFMI_USE_STATIC_BOOST=OFF
   cmake --build python/build-native-debug -j"$(nproc)"
   ```

   This needs `hiredis` development files (`find_package(hiredis)`; the repo ships
   `cmake/Findhiredis.cmake`, which honours `-DHIREDIS_ROOT_DIR=<prefix>`). If you cannot
   install it system-wide, extract the distro package into a local prefix and pass
   `-DHIREDIS_ROOT_DIR=<prefix> -DCMAKE_LIBRARY_PATH=<prefix>/lib`.

   The demo script bootstraps `sys.path` to find `fmi.so` under
   `python/build-native-debug/`, so you do **not** need to set `PYTHONPATH`.

2. **Start the rendezvous server** (built per `README.md`):

   ```bash
   ./extern/TCPunch/server/build/tcpunchd 10000
   ```

3. **Start Redis** on `127.0.0.1:6379`. If you have Docker:

   ```bash
   docker run -d --name fmi-redis -p 127.0.0.1:6379:6379 redis:7
   ```

   (or `redis-server` from a system/package install). Stop it later with
   `docker rm -f fmi-redis`.

---

## Running it

One self-contained orchestrator. Use a fresh `comm_name` each run so stale Redis state can
never interfere:

```bash
python3 runbooks/local-python311-direct/serverless_to_vm_demo.py run --comm-name demo-$(date +%s)
```

The `run` subcommand clears stale state, spawns the two workers, waits for phase 1, triggers
`request_migration(0)`, spawns the replacement, waits for phase 2, then prints both epoch
directories and the acceptance checks. (The script also has a `worker` subcommand — that is
the per-process entry point the orchestrator spawns; you normally do not call it directly.)

---

## Expected output

```
Epoch 0 directory:
  rank=0 worker_id=demo-…-rank0-serverless-XXXXXXXX placement=serverless state=QUIESCED
  rank=1 worker_id=demo-…-rank1-vm                  placement=vm         state=QUIESCED
Epoch 1 directory:
  rank=0 worker_id=demo-…-rank0-vm-replacement-YYYYYYYY placement=vm     state=ACTIVE
  rank=1 worker_id=demo-…-rank1-vm                      placement=vm     state=ACTIVE
Phase 1 sum: 3
Phase 2 sum: 201
PASS phase1 sum == sum(p+1 for p in range(num_peers))
PASS final_epoch == 1
PASS phase2 sum == sum(100+p for p in range(num_peers))
PASS dir0 placements are rank0=serverless and rank1=vm
PASS dir1 placements are rank0=vm and rank1=vm
PASS logical rank ids unchanged across the move
PASS rank0 survived on a NEW worker
PASS rank1 stayed the same worker
OVERALL: PASS
```

### Acceptance criteria

- **Phase-1** `allreduce` returns the correct sum with rank0=`serverless`, rank1=`vm`
  (mixed serverless↔VM comms work over Direct/TCP).
- After migration the **epoch advances 0 → 1** and **phase-2** `allreduce` returns the
  correct sum (the rank survived the move).
- The directory shows rank 0 `serverless` (epoch 0) → `vm` (epoch 1); rank 1 stays `vm`;
  **logical rank ids are unchanged**.
- rank 0's original `worker_id` ≠ the replacement `worker_id` (a genuinely new worker).

> Note: rank states read `QUIESCED` in the epoch-0 snapshot because both ranks pass through
> the quiesce step of `safe_point()` during reconfiguration; placement and rank identity are
> what this experiment asserts on.

---

## Out of scope (the real-Lambda follow-up)

This is a **local** stand-in. A real serverless↔VM deployment is the genuinely hard part and
is intentionally **not** covered here:

- A **publicly reachable `tcpunchd`** and working **NAT traversal** so a Lambda and a VM can
  actually hole-punch a direct connection.
- A **launcher** that performs `aws lambda invoke` for the serverless side (instead of
  spawning a local subprocess) and re-launches a replacement on the VM.

The control-plane/epoch/placement mechanics demonstrated here are exactly what that launcher
would build on; only the *transport reachability* and the *invocation seam* change.
