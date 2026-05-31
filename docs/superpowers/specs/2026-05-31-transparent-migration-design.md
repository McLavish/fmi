# Transparent Stateless Rank Migration — Design Spec

**Date:** 2026-05-31  
**Branch:** `test-migration`  
**Status:** Approved for implementation

---

## Context

FMI already has two FT modes:

- **`CriuCoordinated`** — transparent to the app (no `safe_point()` calls), but uses CRIU for process checkpoint/restore; same-host only, Direct-only, no Python binding.
- **`SafePointRestart`** — cooperative: the app must call `session.safe_point()` between phases and branch on `Event` values. Works across hosts, has a Python binding, but the app code is *not* identical to a non-migration program.

The gap: a mode that is **transparent like CRIU** (app code byte-identical to non-migration) but **reconfigures-only like SafePointRestart** (no memory capture). The immediate need is a demo showing that the rank-directory / migration mechanism is viable — ranks hold a `vm` or `serverless` placement label, one gets migrated, the directory reflects the flip, comms continue. Work loss on the migrated rank is acceptable (no checkpoint means the fresh replacement process restarts from `main()`).

The design is explicitly the **transparent comms-migration skeleton** that the real product uses: CRIU later plugs memory capture into the same quiesce window, and the epoch re-pairing built here is exactly what cross-host VM→serverless CRIU needs. Switching demo→production is a config-flag change; zero app-code change.

The cooperative `SafePointRestart` / `Session` / `safe_point()` path is **replaced** by this new path. CRIU mode is left intact.

---

## Architecture

### New FT mode: `transparent_migration`

Declared in `include/ft/Common.h` alongside the existing `CriuCoordinated`. Enabled in the `fault_tolerance` config block with `"mode": "transparent_migration"`.

### Abstract operation-boundary runtime interface

Replace the concrete `std::shared_ptr<FMI::FT::CriuRuntime> criu_runtime` in `Communicator` with an abstract pointer to a new interface:

```cpp
// include/ft/OperationRuntime.h  (new file)
class OperationRuntime {
public:
    virtual ~OperationRuntime() = default;
    virtual void enter_operation() = 0;
    virtual void exit_operation() = 0;
};
```

`Communicator` holds `std::shared_ptr<FMI::FT::OperationRuntime> operation_runtime` instead of the current concrete `criu_runtime`. `CriuRuntime` implements the interface unchanged. The new `TransparentMigrationRuntime` also implements it (see below).

`enter_operation()` and `exit_operation()` in `src/Communicator.cpp` delegate to `operation_runtime` instead of `criu_runtime`. No change to `OperationGuard`.

### `TransparentMigrationRuntime`

New class in `include/ft/TransparentMigrationRuntime.h` + `src/ft/TransparentMigrationRuntime.cpp`.

Holds: a reference to the owning `Communicator` (to call `reconfigure_to_epoch`), a `shared_ptr<Coordinator>`, the rank's own `peer_id`, `placement`, in-flight counter + mutex/CV (reusing the pattern from `CriuRuntime`).

**`enter_operation()`**:
1. Check `Coordinator::epoch()` — if it has advanced past the local active epoch, reconfigure (see below). This covers the *replacement* process case: it lands at a higher epoch than it started.
2. Check `Coordinator::has_pending_migration()` + `is_rank_pending(peer_id)`.
3. If this rank is the migration target: mark self `Quiesced` in Redis, then block/exit (see quiesce semantics below).
4. If a migration is pending but for another rank: participate in epoch promotion (join epoch N+1, wait for `live_member_count >= num_peers`, call `promote_epoch`, then reconfigure). The thread blocks here — this IS the survivor's quiesce at the operation boundary; the collective has not started, so blocking is correct.
5. Otherwise: increment in-flight counter, admit the operation.

**`exit_operation()`**: decrement in-flight counter, notify waiters.

**Reconfigure path** (`reconfigure_to_epoch(N)`):
- Called when this rank has joined epoch N and all peers are present.
- Calls `Communicator::reconfigure_to_epoch(epoch_comm_name(N))` (in-place channel rebuild).
- Updates local `active_epoch = N`.

**Quiesce semantics for the migrated rank**: mark state `Quiesced` in Redis, then call `std::exit(0)` (or a configurable quiesce callback — see note). The orchestrator detects the quiesced state and relaunches the rank as a new process. This mirrors `CriuRuntime::quiesce()` — it blocks/terminates at the same boundary where CRIU would freeze the process.

*Note: using `std::exit(0)` from inside a runtime is blunt but correct for a demo where work loss is acceptable. A future CRIU integration replaces this exit with actual `criu dump` — the quiesce hook is the integration point.*

### `Communicator` changes

**`build_channels(comm_name)`** (new private method, `src/Communicator.cpp`): extract the channel-creation loop from the constructor into a reusable method — iterates `config.channels`, calls the channel factory, invokes `register_channel`. The constructor calls this.

**`reconfigure_to_epoch(new_comm_name)`** (new public method, `src/Communicator.cpp`):
1. Call `channel->finalize()` on all existing channels (closes Direct sockets, flushes ClientServer state).
2. Clear the `channels` map.
3. Call `build_channels(new_comm_name)` — rebuilds all channels under the epoch-fenced name.
4. Refresh `comm_name` on the communicator.

Object identity of `Communicator` is preserved (the app's reference remains valid). The existing `ChannelPolicy` is unchanged (it doesn't embed `comm_name`).

**Migration-aware constructor**: when FT is enabled in config and mode is `transparent_migration`:
1. Read current epoch from `Coordinator::epoch()`.
2. Form the epoch-fenced comm_name: `base_name + "@epoch=" + epoch` (reusing `Session::epoch_comm_name` logic).
3. Call `build_channels(epoch_fenced_name)`.
4. Register self into the epoch's membership + placement via `Coordinator`.
5. Construct `TransparentMigrationRuntime` and assign to `operation_runtime`.

The survivor's constructor registers into epoch 0. The replacement process's constructor reads the current epoch (e.g. 1) and registers into epoch 1 with its `placement` label.

**`placement` constructor parameter**: add an optional `std::string placement = ""` parameter to `Communicator` (mirroring what `Session` has), forwarded into the `Coordinator` registration.

### Removal of the cooperative path

- `include/ft/Session.h` + `src/ft/Session.cpp`: remove (or deprecate with a compile-time warning). The `Event` enum and `RankState` in `include/ft/Common.h` may stay (they're still used by `Coordinator` and `directory_snapshot`), but `MigrateSelf` / `Reconfigured` become internal-only.
- `FMI::FT::Mode::SafePointRestart`: repurpose the string to be an alias or remove from the mode enum; add `Mode::TransparentMigration`.
- Python: remove `FTSession` from `python/PythonFT.cpp` + `python/fmi_python.cpp`. `FTCoordinator` and `RankDirectoryEntry` remain (orchestrator still uses them). `fmi.Communicator` gains an optional trailing `placement` string in its Python binding.

---

## Data flow: 2-rank VM→serverless migration

```
Epoch 0:
  rank0 (vm)  ─┐
               ├── allreduce loop (Direct/TCP, @epoch=0 names)
  rank1 (vm)  ─┘

  orchestrator: coordinator.request_migration(0)
                → Redis: fmi:ft:<comm>:pending = {0}
                         fmi:ft:<comm>:epoch:0:states[0] = MIGRATION_PENDING

At next enter_operation() boundary:
  rank0: sees is_rank_pending(0)=true → marks Quiesced → exits
  rank1: sees pending → joins epoch 1, refreshes lease, waits for membership

orchestrator: detects rank0 Quiesced → launches replacement rank0 (placement=serverless)
  replacement rank0: constructor reads epoch=1, builds @epoch=1 channels,
                     registers Active into epoch 1

Epoch promotion:
  once live_member_count(1) == 2: promote_epoch(1)
  rank1: reconfigure_to_epoch(@epoch=1) → channels rebuilt → allreduce loop resumes
  replacement rank0: begins allreduce loop (from main(), iteration 0)

Post-migration directory:
  epoch 0: {rank0: worker_id=A, placement=vm, state=QUIESCED}
           {rank1: worker_id=B, placement=vm, state=ACTIVE}
  epoch 1: {rank0: worker_id=C, placement=serverless, state=ACTIVE}
           {rank1: worker_id=B, placement=vm, state=ACTIVE}
```

---

## Known limitation (documented, not fixed)

**Consistent cut is not enforced by a protocol.** The demo relies on the migration flag being observed between collectives. If `request_migration` arrives while rank0 is mid-collective, rank1 may already be blocked in `recv` for the next collective before rank0 checks Redis — this is a residual race. In production, CRIU's coordinated cut (quiesce at a barrier, then `criu dump`) eliminates it. For the demo, the orchestrator triggers migration at a natural phase gap (e.g. after observing phase 1 complete in Redis), making the race unlikely in practice. It is not retried or recovered.

---

## Files modified / created

| File | Change |
|---|---|
| `include/ft/OperationRuntime.h` | **NEW** — abstract interface |
| `include/ft/TransparentMigrationRuntime.h` | **NEW** — header |
| `src/ft/TransparentMigrationRuntime.cpp` | **NEW** — implementation |
| `include/ft/Common.h` | Add `Mode::TransparentMigration`; keep `RankState`/`RankDirectoryEntry` |
| `include/Communicator.h` | `operation_runtime` replaces `criu_runtime`; add `reconfigure_to_epoch`, `placement` param |
| `src/Communicator.cpp` | Extract `build_channels`; add `reconfigure_to_epoch`; migration-aware constructor; delegate to `operation_runtime` |
| `include/ft/CriuRuntime.h` | Inherit from `OperationRuntime` |
| `src/ft/CriuRuntime.cpp` | Implement interface methods (rename wrappers if needed) |
| `include/ft/Session.h` | **REMOVE** |
| `src/ft/Session.cpp` | **REMOVE** |
| `python/PythonFT.cpp` | Remove `PythonFTSession`; keep `PythonFTCoordinator` + `PythonRankDirectoryEntry` |
| `python/PythonCommunicator.cpp` | Add optional `placement` param forwarding |
| `python/fmi_python.cpp` | Remove `FTSession` bindings; add `placement` to `Communicator` ctor |
| `tests/fault_tolerance.cpp` | Rewrite `FaultTolerance` suite to use plain Communicator; remove Session/safe_point tests |
| `tests/ft_migration_demo.cpp` | **REMOVE** or repurpose |
| `runbooks/local-python311-direct/transparent_migration_demo.py` | **NEW** — worker: plain `fmi.Communicator` loop; orchestrator: trigger + verify |
| `runbooks/local-python311-direct/transparent-migration-demo.md` | **NEW** — runbook |
| `runbooks/local-python311-direct/serverless_to_vm_demo.py` | **REMOVE** (replaced) |
| `runbooks/local-python311-direct/serverless-to-vm-demo.md` | **REMOVE** (replaced) |
| `docs/fault-tolerance.md` | Replace SafePointRestart section with TransparentMigration |
| `PLANS.md` | Update to reflect new mode |

---

## Implementation order (each step leaves the build green)

1. **Abstract interface** (`OperationRuntime.h`): new header, no other changes. Build verifies it compiles.
2. **CriuRuntime implements interface**: add inheritance, thin wrappers if needed. Tests pass.
3. **Communicator: swap concrete→interface**: `operation_runtime` replaces `criu_runtime`; delegate `enter/exit_operation`. All existing CRIU tests pass.
4. **`build_channels` + `reconfigure_to_epoch`** (riskiest step): extract constructor logic into `build_channels`; add `reconfigure_to_epoch`. Write a focused unit test that constructs a Communicator, calls `reconfigure_to_epoch`, and verifies channels are rebuilt under the new name.
5. **Migration-aware constructor + `TransparentMigrationRuntime`**: implement `enter_operation` polling logic, quiesce/epoch-advance paths, construct in `transparent_migration` config mode.
6. **Python binding updates**: remove `FTSession`, add `placement` to `fmi.Communicator`, verify `FTCoordinator` still works.
7. **New demo + runbook**: `transparent_migration_demo.py` with worker loop + orchestrator.
8. **Remove old cooperative path**: delete `Session`, remove old demo, update docs.
9. **Test updates**: rewrite `fault_tolerance.cpp` suite.

De-risk step 4 by landing and testing it independently before step 5 wires the runtime. The in-place rebuild is the piece most likely to break the Direct backend's lazy socket re-pairing — verify with a local `tcpunchd` + 2-rank communicator before proceeding.

---

## Verification

**Build:**
```bash
# Build tcpunchd
cmake -S extern/TCPunch/server -B extern/TCPunch/server/build-debug
cmake --build extern/TCPunch/server/build-debug -j"$(nproc)"

# Build Python module (Direct + Redis, no S3)
cmake -S python -B python/build-native-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DPython3_EXECUTABLE="$(command -v python3)" \
  -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_REDIS=ON -DFMI_USE_STATIC_BOOST=OFF
cmake --build python/build-native-debug -j"$(nproc)"
```

**Infrastructure:**
```bash
./extern/TCPunch/server/build-debug/tcpunchd 10000 &
docker run -d --name fmi-redis -p 127.0.0.1:6379:6379 redis:7
```

**Demo:**
```bash
python3 runbooks/local-python311-direct/transparent_migration_demo.py run \
  --comm-name demo-$(date +%s)
```

**Success criteria:**
- All collective iterations complete on both ranks (with work loss on the migrated rank acceptable — replacement starts from iteration 0).
- `directory_snapshot(0)`: rank0=vm, rank1=vm.
- `directory_snapshot(1)`: rank0=serverless, rank1=vm; rank0 has a *new* `worker_id`; rank1's `worker_id` is unchanged.
- No deadlock; orchestrator exits 0.

**Boost tests:**
```bash
cmake -S . -B build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_S3=OFF -DFMI_ENABLE_REDIS=ON
cmake --build build -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=FaultTolerance
```
