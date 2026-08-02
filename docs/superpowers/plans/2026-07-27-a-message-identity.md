# Plan A — Message Identity: Correctness Without Rewrite

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement stages A1, A2 and A3 of
`docs/superpowers/specs/2026-07-27-sequenced-incarnation-links-design.md` — give `Direct`'s
rendezvous an order-independent name, give the ClientServer data plane `run_id`-fenced job-lifetime
message identity, and install the operation-identity seam above the `Channel` interface — so that the
migration counterexample corpus flips from silent substitution to correct delivery or a loud,
diagnosable failure, **on the existing epoch protocol**.

**Architecture:** No new architecture, no new threads, no change to the epoch machinery. Three
additive layers, in order: (A1) the `Direct` rendezvous name stops being built at the call site and
becomes a fixed-size hash over the canonical rank-ordered tuple
`(run_id, wire_version, low_rank, low_incarnation, high_rank, high_incarnation)`, so both ends
compute it identically regardless of who registers first; (A2) `ClientServer` derives its data-plane
keys from a `run_id`-prefixed *stable* communicator name plus job-lifetime per-directed-pair
sequences, while the epoch-qualified name is retained **only** for `Direct` pairing; (A3) an RAII
thread-local operation scope, set by `Communicator`'s entry points, publishes
`(lane, op_kind, collective_index, root)` plus the reduction function's commutative/associative
flags, **and owns the per-directed-pair message ordinal for both channel families**, which
`ClientServer` folds into its keys and into a validated per-object header. Per the design spec,
contract 1, the `Channel` virtuals do **not** gain a lane parameter.

**Already landed upstream — not re-planned here.** The TCPunch concurrency work that stage A1
originally carried is done: `extern/TCPunch` commits `b938efd` ("client: fix stack-use-after-return
and thread/fd leak in `pair()`"), `3a6e986` ("client: decide the pairing before stopping the
listener") and `7460d68` ("server: sweep dead and idle pairing registrations"). The two file-scope
atomics were replaced by a per-pairing `ListenContext` struct — see the explanatory comment at
`extern/TCPunch/client/tcpunch.cpp:22-34` — and the server registry already stamps `registered_at`
and calls `sweep_registry` once per `accept()` iteration
(`extern/TCPunch/server/hole_punching_server.cpp:21,39-40,128`). On the FMI side, `9c7e533` ("fix:
ClientServer reconfigures in place with job-lifetime data keys") landed the `data_comm_name` /
`data_plane_name()` split and `ClientServer::reconfigure_for_epoch`, and `1aa9beb` landed
`Channels/direct_concurrent_pairings_one_timeout`. What remains under those headings is naming and
fencing, not concurrency.

**Tech Stack:** C++17, CMake, Boost.Test, hiredis/Redis, TCPunch (git submodule, `McLavish` fork),
POSIX sockets/threads/`/proc`.

## Global Constraints

- The design spec (`docs/superpowers/specs/2026-07-27-sequenced-incarnation-links-design.md`) is
  normative. Where this plan and the spec disagree, the spec wins; raise the conflict rather than
  diverging silently.
- **What is machine-checked, and what this plan must still earn.** `docs/tla/README.md` (three
  modules, 36 TLC 2.19 configurations, all exhausted) is the results document. For Plan A's slice —
  contract 1 — `MessageIdentity.tla` establishes over *all* divergent program pairs of length ≤ 2
  (≤ 3 in two configs) that today's unframed `Direct` admits silent substitution, that lane alone and
  lane + `collective_index` are each insufficient, that `op_kind` **and** the
  commutative/associative flags are each **independently** necessary (`reduce` vs `reduce_nc` collide
  on every other field), and that the full envelope never rejects a well-formed pair. Two things it
  does **not** cover and this plan must not claim: `root`'s necessity needs `N ≥ 3` and is not
  exhibited at `N = 2`; and **`ClientServer` is not modelled at all**, so every claim in Tasks 3–6 and
  8 about job-lifetime keys, consumer-delete namespaces, `GET`/commit/`DEL`, and the barrier probe
  rests on this plan's own Boost cases alone.
- Every task ends with the tree building **and** `Boost_Tests_run` passing. No task may leave a
  broken intermediate state.
- One task = one commit (repo commit discipline, `CLAUDE.md`). A1 tasks produce **two** commits: one
  inside the `extern/TCPunch` submodule and one in FMI that bumps the gitlink together with the new
  FMI-side test. Task 2 is the one deliberate exception: it is a numbering anchor for work that has
  already landed and produces no commit.
- Stage and commit only files belonging to the current task. The working tree carries unrelated
  modifications — verified on `exp/checkpoint-mechanics`: modified
  `runbooks/localstack-python311-redis/orchestrator.py`,
  `runbooks/localstack-python311-redis/knative-migration/orchestrator.py`,
  `tests/communicator.cpp`; untracked `gate`, `.superpowers/`, `tests/forked_rank_guard.h`,
  `tests/migration_p2p_cut_counterexample.cpp`. Preserve them. Re-run `git status --short` before
  each task rather than trusting this list.
- **Check the submodule working tree before touching `extern/TCPunch`.** As of this writing
  `git -C /home/luca/fmi/extern/TCPunch status --short` is clean and the parent repo shows the
  gitlink ahead of the recorded commit. If that has changed, rebase onto whatever is there — never
  overwrite `client/tcpunch.cpp` or `server/hole_punching_server.cpp` wholesale, both carry landed
  fixes (`b938efd`, `3a6e986`, `7460d68`) this plan depends on.
- Do **not** delete, rename, or gate the epoch machinery. `epoch_comm_name`
  (`include/ft/Common.h:55-57`), `ControlPlane`, and `TransparentMigrationRuntime` are Plan C's
  territory. Per the spec's acceptance oracles, no stage may loudly reject ClientServer-under-FT
  while `fmi_migration_p2p_cut_counterexample` remains the headline gate.
- **Cross-plan file ownership** (read this before Tasks 1, 3, 6, 8 and 9; index:
  `docs/superpowers/plans/2026-07-27-migration-v2-README.md`):
  - `tests/channels.cpp` / `tests/communicator.cpp` are **shared with Plan C Task 7**, which routes
    every `get_channel` call through a transport-variant overlay and diffs the case list before and
    after. **Plan A Task 1 lands first**; Plan C Task 7 then records the post-A1 case count as its
    baseline. Verified today: `Channels` holds **17** cases, of which
    `direct_concurrent_pairings_one_timeout` (`tests/channels.cpp:756`, from `1aa9beb`) and
    `scan_ltr_client_server_ordering` (`tests/channels.cpp:812`, from `523128a`) are the two added
    since the 15-case baseline. Task 1 adds two more, so Plan C's baseline is **19 in `Channels`**.
    `direct_pair_timeout_is_clean` calls `pair()` directly and is outside the variant loop;
    `direct_pair_both_ends_send_first` constructs two `Direct` channels directly and is likewise
    outside it. Do not restructure the suite beyond adding those two cases.
  - `include/ft/ControlPlane.h` / `src/ft/ControlPlane.cpp` (Tasks 3, 6 and 8) are the **one
    exception** to the spec's staging-map claim that A2/A3 and B touch disjoint file sets. All three
    edits are additive: the `run_id` mint plus the `fmi:ft:name:<comm_name> → run_id` index (Task 3),
    a defaulted parameter on `clear_job_state` (Task 6), and the policy-fingerprint key (Task 8).
    Plan B is forbidden from touching those files, so there is no A↔B collision. Plan C stage C3
    rewrites `ControlPlane` and must carry all three forward — they are listed in that stage's port
    list.
  - Plan A **solely owns** the `--group` selector and the `tests/migration_cut_model.cpp` disposition
    (Task 9). `Classification::LoudFail` and the `aggregate_exit` contract already landed with
    `6967723` (`tests/migration_counterexample_scenarios.h:97`,
    `tests/migration_counterexample_runner.cpp:1967-1993`) — Plan A verifies and extends them rather
    than adding them. Plan C stage C4 re-verifies all of it against the incarnation protocol but must
    not re-implement it.
  - `CMakeLists.txt` and `tests/CMakeLists.txt` are appended to by all three plans (`FMI_SOURCES`
    near `CMakeLists.txt:42-51`, `FMI_TEST_SOURCES` at `tests/CMakeLists.txt:6`). Expect trivial
    textual conflicts; resolve by union, never by overwrite.
- Both counterexample binaries only existed under `-DFMI_ENABLE_CRIU=ON`
  (`tests/CMakeLists.txt`, two `if(FMI_ENABLE_CRIU)` blocks), and every verification recipe
  below passed it. **The option, those blocks and both binaries were removed together with the
  epoch migration protocol**; the flag has been dropped from the recipes here, and the steps
  that build or run a counterexample binary can no longer be re-run as written.
- Build once per task with:
  `cmake -S /home/luca/fmi -B /home/luca/fmi/build -DFMI_BUILD_TESTS=ON`
  then `cmake --build /home/luca/fmi/build -j"$(nproc)"`.
- **`Boost_Tests_run` must be run with `cwd = build/tests`.** `tests/communicator.cpp:12` hardcodes
  `config_path = "../../config/fmi_test.json"`, which only resolves from
  `/home/luca/fmi/build/tests`. Running it from the repo root resolves to `/home/config/…` and every
  `Communicator` case fails at construction.
- **Direct preflight.** `config/fmi_test.json:19` sets `backends.Direct.host` to the stale LAN
  address `192.168.0.166`, and Direct is the only backend enabled in that file. Before running
  `Boost_Tests_run`, apply the local override and **never stage it**:
  ```bash
  sed -i 's/192\.168\.0\.166/127.0.0.1/' /home/luca/fmi/config/fmi_test.json
  # ... run tests ...
  git -C /home/luca/fmi checkout -- config/fmi_test.json
  ```
- Infrastructure on this machine: Redis native at `127.0.0.1:6379` (`redis-cli ping` → `PONG`);
  rendezvous server `/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000` (the
  `build/` and `build-debug/` binaries are stale pre-fix builds — do not use them); `criu` 4.2 at
  `/usr/local/sbin/criu` with `cap_sys_ptrace=eip`.
- Use `uv`, never bare `pip`, for any Python. This plan requires no Python packages.
- **Coverage warning that shapes every A2/A3 task — thin, but not absent.** The `backends` *matrix*
  at `tests/channels.cpp:68-71` has `S3` and `Redis` commented out (`:69-70`), so the data-driven part
  of the `Channels` suite exercises `Direct` only; and `config/fmi_test.json` disables Redis (`:11`)
  and enables only `Direct` (`:18`), so the `Communicator` suite is `Direct`-only too. **But the
  ClientServer data plane does not have zero `Boost_Tests_run` coverage.** Two cases construct a real
  Redis channel outside the matrix: `scan_ltr_client_server_ordering` (`tests/channels.cpp:812`,
  guarded `#if FMI_ENABLE_REDIS`, which is what caught the ClientServer scan fold-order bug fixed in
  `523128a`) and the `ClientServerKeys` suite (`tests/clientserver_keys.cpp:14`, already in
  `FMI_TEST_SOURCES` at `tests/CMakeLists.txt:6`). The correct statement is therefore: **`scan` is the
  sole covered ClientServer *collective*; `send`/`recv`/`bcast`/`barrier`/`gather`/`scatter`/`reduce`
  over ClientServer are uncovered**, so ClientServer collective coverage is thin rather than missing,
  and the model to copy already exists in-tree. Every A2/A3 task must still add its own explicitly
  Redis-backed Boost case; "Boost_Tests_run stays green" remains a no-regression gate, not evidence
  that the new code works. Model checking does not fill the hole either — `docs/tla/README.md` records
  that `ClientServer` is **not modelled at all**, which matters precisely because the headline
  acceptance oracle fails through the ClientServer path.
- New Boost test translation units must **not** define `BOOST_TEST_MODULE` — `tests/communicator.cpp:1`
  already owns it. New files are added to `FMI_TEST_SOURCES` in `tests/CMakeLists.txt:6`.
- Redis-touching Boost cases must be guarded with `#if FMI_ENABLE_REDIS` and must clean up their own
  keys, because `Redis::get_object_names` (`src/comm/Redis.cpp:85-94`) issues an **unscoped
  `KEYS *`** over the whole database.

---

### Task 1: Order-independent hashed rendezvous key

**Deleted from this task, because upstream already did the work.** The original A1 scope — "rewrite
pairing as per-call state", "joinable/cancellable listener", "remove the process-global pairing
state" — is **done**: `extern/TCPunch` commits `b938efd` ("client: fix stack-use-after-return and
thread/fd leak in `pair()`") and `3a6e986` ("client: decide the pairing before stopping the
listener") replaced the two file-scope atomics with a per-pairing `ListenContext` struct; the
explanatory comment at `extern/TCPunch/client/tcpunch.cpp:22-34` records why. `peer_listen` now takes
a `ListenContext*` and never touches `pair()`'s frame, and its `accept()` loop runs under
`SO_RCVTIMEO` so it observes `ctx->stop`. Nothing of that rewrite remains to plan. Likewise the
idle-TTL sweep of the pairing registry (old Task 2) landed as `7460d68`; see Task 2 below.

What is left under the A1 heading is a **naming** change, not a concurrency change: the pairing name
is still built at the **call site** by each end independently, and the two ends do not agree on it.
`Direct::send_object` registers `comm_name + peer_id + "_" + rcpt_id`
(`src/comm/Direct.cpp:29`) while `Direct::recv_object` registers
`comm_name + sender_id + "_" + peer_id` (`src/comm/Direct.cpp:49`), and `check_socket`
(`src/comm/Direct.cpp:82-106`) pairs under whichever arrives first. A legal divergent program in
which A's first touch of B is a `send` **and** B's first touch of A is also a `send` registers
`comm A_B` against `comm B_A`: the names never meet, both endpoints block to `max_timeout` and both
throw. Today's collectives happen to agree on first-touch direction on every pair, which is why this
is invisible — and it becomes reachable under exactly the divergent schedules v2 exists to support.

**Files:**
- Modify: `extern/TCPunch/client/tcpunch.h`
- Modify: `extern/TCPunch/client/tcpunch.cpp`
- Modify: `extern/TCPunch/server/hole_punching_server.cpp`
- Modify: `include/comm/Channel.h`
- Modify: `include/comm/Direct.h`
- Modify: `src/comm/Direct.cpp`
- Modify: `src/Communicator.cpp`
- Modify: `tests/channels.cpp`

**Interfaces:**
- Produces, in `extern/TCPunch/client/tcpunch.h` (replacing the signature at `:14`). The `side`
  argument is **required, not defaulted**: a default would put both ends on the same side and they
  would never meet, which is the very defect this task removes. The fork has exactly two call sites —
  `Direct::check_socket` (`src/comm/Direct.cpp:88`) and the `pair_fails` helper
  (`tests/channels.cpp:732-745`) used by `direct_concurrent_pairings_one_timeout` — and both are
  updated in this task.

```cpp
//! Which end of a rank-ordered pair is registering. The rendezvous name is identical on both
//! ends by construction, so the server needs the side to tell two endpoints apart — and so a
//! retrying endpoint can never be paired with its own abandoned registration.
enum class PairSide : int { Low = 0, High = 1 };

int pair(const std::string& pairing_name, const std::string& server_address,
         int port, int timeout_ms, PairSide side);
```

- Produces, on `Channel` (`include/comm/Channel.h`, alongside `set_data_comm_name` and the
  `data_plane_name()` helper at `:160-170`):

```cpp
//! Universal name fence (design spec, contract 1, "run_id"). A fresh random value minted once
//! at job creation, prefixed to every Direct rendezvous tuple and every ClientServer data key.
void set_run_id(std::string id) { run_id = std::move(id); }

//! The generation that qualifies a link's endpoints. Plan A: the current epoch, which under the
//! global cut is identical on both ends of every pair. Plan C stage C3 replaces it with the two
//! ranks' per-rank incarnations, which are then independent.
void set_link_generation(std::uint64_t g) { link_generation = g; }

protected:
    std::string   run_id;
    std::uint64_t link_generation = 0;
```

- Produces, on `Direct` (`include/comm/Direct.h`, private; `check_socket` **loses** its `pair_name`
  parameter — the name is no longer a call-site input):

```cpp
//! Fixed-width, order-independent rendezvous name: 32 lowercase hex characters over the
//! canonical rank-ordered tuple
//!   (run_id, wire_version, low_rank, low_generation, high_rank, high_generation)
//! with a field separator that cannot occur inside any field. Both ends compute the identical
//! string regardless of which one registers first.
std::string rendezvous_name(FMI::Utils::peer_num partner_id) const;

//! Low end is min(peer_id, partner_id); high end is max.
PairSide rendezvous_side(FMI::Utils::peer_num partner_id) const;

void check_socket(FMI::Utils::peer_num partner_id);
```

This subsumes three naming defects at once, per the design spec's "Rendezvous naming (Direct)":

1. The direction disagreement above, because the name no longer depends on who calls first.
2. The `@epoch=` digit-merge collision. `epoch_comm_name` produces `base + "@epoch=" + N`
   (`include/ft/Common.h:55-57`) and `Direct::send_object` concatenates ids onto it with **no
   separator** (`src/comm/Direct.cpp:29`), so `X@epoch=1` with `(peer=10, rcpt=1)` and `X@epoch=11`
   with `(peer=0, rcpt=1)` both yield `X@epoch=110_1` — two different epochs pairing under one name.
3. The `MAX_PAIRING_NAME = 100` truncation (`extern/TCPunch/server/hole_punching_server.cpp:24`)
   stops being a silent correctness cliff for long communicator names, because a hash is fixed-width
   by construction.

The **full tuple**, not only its hash, is what a future `HANDSHAKE` authenticates (Plan C). Plan A
carries the hash only; keep `rendezvous_name`'s tuple construction in one function so Plan C can
serialize the same tuple without re-deriving it.

- [ ] **Step 1: Lock in the landed leak fix, and reproduce the divergent-pair hang (RED)**

Two cases in `tests/channels.cpp`, both added inside the existing `Channels` suite (`:19`).

`direct_pair_timeout_is_clean` — a **regression guard for work that already landed**, so it is
expected to pass the moment it is written. It is required anyway: nothing in the suite currently
asserts the `b938efd`/`3a6e986` invariant, and Plan C's engine will re-enter this code. With
`tcpunchd` deliberately **not** reachable (reuse the `rendezvous_reachable()` probe at
`tests/channels.cpp:713-726`, inverted — skip when the server *is* up):

1. record the baseline thread count (entries in `/proc/self/task`) and the baseline count of
   listening fds (for every entry in `/proc/self/fd`, `getsockopt(fd, SOL_SOCKET, SO_ACCEPTCONN)`
   returning 1);
2. call `pair("leak-probe-<pid>-<n>", "127.0.0.1", 10000, 300, PairSide::Low)` inside
   `BOOST_CHECK_THROW(..., Timeout)` — catching `Timeout` *and* `std::string`, since
   `extern/TCPunch/common/utils.h` throws a bare `std::string` from `error_exit`;
3. sleep 500 ms and re-read both counts;
4. `BOOST_CHECK_EQUAL` them against the baseline.

`direct_pair_both_ends_send_first` — the genuine RED. Guarded by `rendezvous_reachable()`. Two
in-process threads over two `Direct` channels built from `direct_test_params` /
`direct_test_model_params` (`tests/channels.cpp:53-66`), `num_peers = 2`, a shared
`comm_name = "bothsend-<pid>-<n>"`:

- thread 0: `ch0.send(payload0, 1)` then `ch0.recv(got0, 1)`;
- thread 1: `ch1.send(payload1, 0)` then `ch1.recv(got1, 0)`;
- assert both payloads arrive intact.

Every rank's **first touch of its partner is a send**, which is legal and which no collective in
`src/comm/PeerToPeer.cpp` produces. Bound it with `direct_test_params`' `max_timeout` of 1000 ms
(`tests/channels.cpp:55`) so today's failure is a pair of `Utils::Timeout` throws within ~1 s rather
than a hang — `Boost_Tests_run` runs these peers as threads in one process, so a hang would take the
whole binary down instead of failing it.

```bash
cmake -S /home/luca/fmi -B /home/luca/fmi/build -DFMI_BUILD_TESTS=ON
cmake --build /home/luca/fmi/build --target Boost_Tests_run -j"$(nproc)"
/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000 &
TCPUNCHD=$!; sleep 1
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=Channels/direct_pair_both_ends_send_first
kill "$TCPUNCHD"
```

Expected: `direct_pair_both_ends_send_first` **fails** with `Utils::Timeout` on both ranks —
`send_object` registered `…0_1` while its partner registered `…1_0`.

- [ ] **Step 2: Compute the name in one place**

- Add `run_id` / `link_generation` and their setters to `Channel`, next to the existing
  `data_comm_name` / `data_plane_name()` members (`include/comm/Channel.h:160-170`).
- `Communicator::register_channel` (`src/Communicator.cpp:129-134`) propagates both, alongside the
  `set_data_comm_name` call already there at `:133`. The epoch is what `src/Communicator.cpp:69`
  already folds into `comm_name` via `FMI::FT::epoch_comm_name`; pass the same `active_epoch` to
  `set_link_generation`, and `0` on the non-FT path. `reconfigure_to_epoch`
  (`src/Communicator.cpp:101-127`) updates it for kept channels too, so a selective re-pair
  (`Direct::reconfigure_for_epoch`) re-pairs a moved link under the new generation.
- Implement `Direct::rendezvous_name` / `rendezvous_side`. Hash with a 128-bit FNV-1a over the
  separator-joined tuple, rendered as 32 lowercase hex characters. Do **not** hash the raw
  concatenation: join with a byte that cannot occur in any field so `("ab","c")` and `("a","bc")`
  cannot collide.
- Delete the `pair_name` argument from `check_socket` (`include/comm/Direct.h`,
  `src/comm/Direct.cpp:82`) and both call sites (`:29`, `:49`), which then read identically.
- `check_socket` passes `rendezvous_side(partner_id)` to `pair()` (`src/comm/Direct.cpp:87`).

- [ ] **Step 3: The rendezvous server records which side registered**

In `extern/TCPunch/server/hole_punching_server.cpp`:

- The client's registration message carries the fixed-width name **plus a one-byte side tag**. Key
  the registry on `(name, side)` and pair an arriving endpoint only against the **opposite** side.
- An arriving endpoint whose `(name, side)` is already present **replaces** the stored entry (the
  existing dead-client replacement path at `:175-212` already has this shape) rather than pairing
  with it. This is the normative rule from the spec: *a retrying endpoint must never be paired with
  its own abandoned registration.*
- Leave `sweep_registry` (`:39-40`, called at `:128`) and the `registered_at` stamps (`:178`, `:203`,
  `:227`) alone — they landed in `7460d68` and only need their map key widened to include the side.
- `MAX_PAIRING_NAME` (`:24`) stays as a bound, but the name is now 32 bytes by construction; assert
  the received name length rather than silently truncating.

Client and server change together, so the FMI-side gitlink bump in Step 5 is what pins them.

- [ ] **Step 4: Verify (GREEN)**

```bash
cmake --build /home/luca/fmi/extern/TCPunch/server/build-fresh -j"$(nproc)"
pkill -f 'tcpunchd 10000' || true
/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000 &
TCPUNCHD=$!; sleep 1
cmake --build /home/luca/fmi/build --target Boost_Tests_run -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=Channels/direct_pair_both_ends_send_first
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=Channels/direct_concurrent_pairings_one_timeout
kill "$TCPUNCHD"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=Channels/direct_pair_timeout_is_clean

/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000 &
TCPUNCHD=$!; sleep 1
sed -i 's/192\.168\.0\.166/127.0.0.1/' /home/luca/fmi/config/fmi_test.json
cd /home/luca/fmi/build/tests && ./Boost_Tests_run
git -C /home/luca/fmi checkout -- config/fmi_test.json
kill "$TCPUNCHD"
```

Expected: both new cases pass; `direct_concurrent_pairings_one_timeout` (from `1aa9beb`) still
passes, i.e. the side tag did not break the idle-TTL sweep; the full suite passes with **19** cases
in `Channels`; `ss -ltnp | grep Boost_Tests_run` is empty after the run.

- [ ] **Step 5: Commit (two commits)**

```bash
git -C /home/luca/fmi/extern/TCPunch add client/tcpunch.h client/tcpunch.cpp \
  server/hole_punching_server.cpp
git -C /home/luca/fmi/extern/TCPunch commit -m "rendezvous: fixed-width sided pairing names"

git -C /home/luca/fmi add extern/TCPunch include/comm/Channel.h include/comm/Direct.h \
  src/comm/Direct.cpp src/Communicator.cpp tests/channels.cpp
git -C /home/luca/fmi commit -m "fix: order-independent hashed rendezvous name for Direct"
```

---

### Task 2: tcpunchd idle-TTL sweep — LANDED UPSTREAM, no work, no commit

Retained as a numbering anchor so the cross-plan references to "Plan A Task 6", "Task 9" and Plan C's
task list stay valid. **Do not renumber the tasks below.**

The whole of this task shipped before the plan was written:

- `extern/TCPunch` `7460d68` ("server: sweep dead and idle pairing registrations") added
  `registered_at` to `ConnectionData` (`server/hole_punching_server.cpp:21`), `sweep_registry`
  (`:39-40`) called once per `accept()` iteration (`:128`), the `registered_at` stamps at the three
  insertion sites (`:178`, `:203`, `:227`), and the optional
  `tcpunchd <port> [idle_ttl_seconds]` argument (`:60-82`, default at `DEFAULT_IDLE_TTL_SECONDS`).
- FMI `1aa9beb` ("test: concurrent pairings survive one timed-out partner") added
  `Channels/direct_concurrent_pairings_one_timeout` (`tests/channels.cpp:756`) and its
  `rendezvous_reachable()` skip probe (`:713-726`).

Task 1 Step 3 widens the registry key to `(name, side)`; Task 1 Step 4 re-runs
`direct_concurrent_pairings_one_timeout` to confirm the sweep still behaves. There is nothing else to
do here.

---

### Task 3: `run_id` — the universal name fence, and the name index that makes it reachable

**Deleted from this task, because upstream already did the work.** The stable/epoch-qualified name
split this task originally carried landed as FMI `9c7e533` ("fix: ClientServer reconfigures in place
with job-lifetime data keys"): `Channel::set_data_comm_name` + the `data_plane_name()` fallback
helper (`include/comm/Channel.h:160-170`), `Communicator::data_comm_name`
(`include/Communicator.h:195`, assigned at `src/Communicator.cpp:68` and `:82`, propagated at
`src/Communicator.cpp:133`), `ClientServer::reconfigure_for_epoch` returning `true`
(`src/comm/ClientServer.cpp:67-77`), and `tests/clientserver_keys.cpp` with
`survives_epoch_reconfigure` (`:46`) and `a_rebuilt_channel_reuses_keys` (`:99`), already wired into
`FMI_TEST_SOURCES` (`tests/CMakeLists.txt:6`). Do not re-plan any of it.

What remains is the fence *in front of* that name. `grep -rn run_id include/ src/ tools/ tests/`
returns **nothing** today (verified), so `comm_name` is the sole fence and it is reused verbatim
across four independent namespaces: the control-plane key prefix
(`ControlPlane::Impl::prefix()`, `src/ft/ControlPlane.cpp:199-201`), the CRIU image directory
(`src/ft/experimental/LocalRankAgent.cpp:91`), every ClientServer data key
(`data_plane_name()`, `src/comm/ClientServer.cpp:14`) and every `Direct` rendezvous tuple (Task 1).
Two runs of the same job name share all four.

**Files:**
- Modify: `include/ft/ControlPlane.h`
- Modify: `src/ft/ControlPlane.cpp`
- Modify: `include/comm/Channel.h`
- Modify: `include/Communicator.h`
- Modify: `src/Communicator.cpp`
- Modify: `tests/clientserver_keys.cpp`

**Interfaces:**
- Produces, on `ControlPlane` (`include/ft/ControlPlane.h`, next to `clear_job_state` at `:48`):

```cpp
//! Mint-once-and-publish. SET NX on fmi:ft:name:<comm_name> and return the value that won, so a
//! second caller for the same comm_name gets the existing run_id back rather than a new one.
//! Every rank of a job calls this; exactly one mint survives.
std::string ensure_run_id(const std::string& comm_name) const;

//! Resolve a job addressed BY NAME to its run_id. Throws when the index entry is absent, which
//! is a stale-name error and must never fall back to treating comm_name as the fence.
std::string resolve_run_id(const std::string& comm_name) const;
```

> **The index is not optional.** Every tool that drives a migration addresses the job by **name**:
> `fmi-rank-agent <verb> <comm_name> <num_peers> <config> [rank]`
> (`tools/rank_agent.cpp:38`, parsed at `:44`), both shell demos
> (`runbooks/local-criu-state-transfer/run-demo.sh:24`,
> `run-demo-all.sh:29,91`, which even reconstructs the control-plane prefix by hand at
> `run-demo-all.sh:39`: `PREFIX="fmi:ft:${COMM_NAME}:"`), and both Python orchestrators
> (`runbooks/localstack-python311-redis/orchestrator.py`,
> `runbooks/localstack-python311-redis/knative-migration/orchestrator.py:270-271`, which construct
> `fmi.FTControlPlane(config, comm_name, num_peers)`). Without
> `fmi:ft:name:<comm_name> → run_id`, `run_id` is unreachable from the entire CLI surface that
> actually performs migrations. The index key deliberately sits **outside** the
> `fmi:ft:<run_id>:<comm>:` prefix — it is the entry point *to* that prefix — so `clear_job_state`
> must delete it explicitly and last.

- Produces, on `Channel`: `data_plane_name()` (`include/comm/Channel.h:167-170`) gains a `run_id`
  prefix, using the same fallback idiom it already uses for `data_comm_name`:

```cpp
//! run_id-fenced name data-plane keys are built from. Falls back to the un-fenced name when no
//! run_id was set (no fault tolerance ⇒ no cross-run reuse to fence), so the non-FT path keeps
//! today's keys byte-for-byte.
std::string data_plane_name() const {
    const std::string& base = data_comm_name.empty() ? comm_name : data_comm_name;
    return run_id.empty() ? base : run_id + ":" + base;
}
```

`run_id` and its setter were added to `Channel` in Task 1 (the `Direct` rendezvous tuple is its other
consumer); this task only mints it, indexes it, propagates it, and puts it in front of the keys.

- `Communicator::register_channel` (`src/Communicator.cpp:129-134`) already propagates
  `set_data_comm_name` at `:133`; the `set_run_id` call added in Task 1 now carries a real value.
  On the FT path the value comes from `control_plane->ensure_run_id(comm_name)`, called next to the
  existing `join_epoch` (`src/Communicator.cpp:64`) and **before** `build_channels`. On the non-FT
  path it stays empty.

- Produces the demotion of `clear_job_state`'s role, to be recorded in its doc comment in Task 6:
  with `run_id` in place a previous run's keys are **unreachable** rather than merely unlikely to be
  hit, so `clear_job_state`'s cross-run collision duty drops from a correctness precondition to
  **hygiene** (reclaiming space, keeping `redis-cli dbsize` stable across the acceptance runs). The
  defaulted `data_comm_name` parameter Task 6 adds is therefore a cleanup convenience, not a fence.

- [ ] **Step 1: Prove a reused communicator name is not fenced (RED)**

Add `ClientServerKeys/run_id_fences_a_reused_comm_name` to `tests/clientserver_keys.cpp`, guarded by
`#if FMI_ENABLE_REDIS`, following the shape of `survives_epoch_reconfigure` (`:46`) and reusing
`redis_test_params` / `redis_test_model_params` (`tests/channels.cpp:36-51`):

- build a `Redis` channel with `peer_id=0`, `num_peers=2`, `comm_name = "runid-<pid>-<ts>"`, and
  `send` one 4-byte payload to peer 1 that is never received — this models a run that died holding an
  undelivered message;
- build a **second** channel pair under the **identical** `comm_name`, standing in for a re-launched
  job, with a different `run_id`;
- assert the second run's `recv(…, 0)` on peer 1 throws `Utils::Timeout` — it must not see the dead
  run's payload;
- clean up the test's own keys (`Redis::get_object_names` issues an unscoped `KEYS *`,
  `src/comm/Redis.cpp:88`).

Add `ClientServerKeys/name_index_resolves_run_id`, also `#if FMI_ENABLE_REDIS`: two `ensure_run_id`
calls for one `comm_name` return the same value; `resolve_run_id` returns that value; `resolve_run_id`
on an unknown name throws.

Expected today: compilation fails (`ensure_run_id`/`resolve_run_id` do not exist), and once stubbed,
`run_id_fences_a_reused_comm_name` **fails** because the second run reads the first run's key —
`data_plane_name()` returns only the communicator name.

- [ ] **Step 2: Mint and index the `run_id`**

- Implement `ensure_run_id` as a single `SET fmi:ft:name:<comm_name> <candidate> NX GET` (or `SET NX`
  followed by `GET` in one Lua execution) so concurrent ranks converge on one value in one round
  trip. Mint the candidate from a CSPRNG, not from a timestamp or a PID.
- Implement `resolve_run_id` as a `GET` that throws `std::runtime_error` naming the communicator when
  the key is absent. It must never silently fall back to `comm_name`.
- Leave `ControlPlane::Impl::prefix()` (`src/ft/ControlPlane.cpp:199-201`) **unchanged in this task**.
  Re-prefixing the control plane to `fmi:ft:<run_id>:<comm>:` is a directory-schema change that Plan C
  stage C3 owns together with the incarnation rewrite; doing it here would break every runbook and
  both orchestrators mid-plan. Record it in C3's port list.

- [ ] **Step 3: Thread it through to the data plane**

- Call `ensure_run_id` on the FT branch of the `Communicator` constructor, next to `join_epoch`
  (`src/Communicator.cpp:64`), store it in a `std::string run_id;` member beside `data_comm_name`
  (`include/Communicator.h:195`), and let `register_channel` (`:129-134`) push it into every channel.
- Change `data_plane_name()` (`include/comm/Channel.h:167-170`) to the fenced form above. This alone
  re-prefixes every ClientServer key, because all eight key constructions already route through it
  (`src/comm/ClientServer.cpp:14,28,35,47,123,147,155,219`). **Do not change the key grammar** — that
  is Tasks 4, 5 and 8.
- Confirm the `Direct` rendezvous tuple from Task 1 now hashes a non-empty `run_id`.

- [ ] **Step 4: Verify (GREEN)**

```bash
redis-cli -h 127.0.0.1 -p 6379 ping
cmake -S /home/luca/fmi -B /home/luca/fmi/build -DFMI_BUILD_TESTS=ON
cmake --build /home/luca/fmi/build -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=ClientServerKeys
/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000 &
TCPUNCHD=$!; sleep 1
sed -i 's/192\.168\.0\.166/127.0.0.1/' /home/luca/fmi/config/fmi_test.json
cd /home/luca/fmi/build/tests && ./Boost_Tests_run
git -C /home/luca/fmi checkout -- config/fmi_test.json
kill "$TCPUNCHD"
redis-cli -h 127.0.0.1 -p 6379 --scan --pattern 'runid-*' | wc -l
```

Expected: the two new cases pass alongside the two that `9c7e533` already added; the full suite
passes — in particular the `Communicator` suite, which runs **without** fault tolerance
(`config/fmi_test.json:54` sets `fault_tolerance.enabled=false`) and therefore exercises the empty-
`run_id` fallback, proving the non-FT key shape is unchanged; the pattern count is `0`.

- [ ] **Step 5: Commit**

```bash
git -C /home/luca/fmi add include/ft/ControlPlane.h src/ft/ControlPlane.cpp \
  include/comm/Channel.h include/Communicator.h src/Communicator.cpp \
  tests/clientserver_keys.cpp
git -C /home/luca/fmi commit -m "feat: run_id fence with a name index for name-addressed tools"
```

---

### Task 4: Job-lifetime p2p keys, consumer-delete, exact-length validation

**Files:**
- Modify: `include/comm/ClientServer.h`
- Modify: `src/comm/ClientServer.cpp`
- Modify: `src/comm/Redis.cpp`
- Modify: `src/comm/S3.cpp`
- Modify: `tests/clientserver_keys.cpp`

**Interfaces:**
- Produces, on `ClientServer` (replacing the string-keyed `num_operations` map at
  `include/comm/ClientServer.h:73-78` for the p2p lane):

```cpp
//! Job-lifetime, per-directed-pair message ordinals. Never reset — not by epoch
//! reconfiguration, not by finalize. Key: (src, dst).
//!
//! TEMPORARY OWNER. Task 7 moves this counter onto the RAII operation scope, which is the
//! single owner for BOTH channel families (design spec, contract 1). Keep it a thin pair of
//! maps with no other state so Task 7's move is a deletion plus a read, not a rewrite; do NOT
//! let anything outside send()/recv() come to depend on it.
std::map<std::pair<FMI::Utils::peer_num, FMI::Utils::peer_num>, std::uint64_t> p2p_send_seq;
std::map<std::pair<FMI::Utils::peer_num, FMI::Utils::peer_num>, std::uint64_t> p2p_recv_seq;
```

- Produces, changing the `download_object` contract (`include/comm/ClientServer.h:50`, with
  `upload_object` at `:47`):

```cpp
//! Returns true only when an object of EXACTLY buf.len bytes was fetched into buf. An object
//! that exists with a different length is a protocol violation: throw, never truncate.
virtual bool download_object(channel_data buf, std::string name) = 0;

//! Create-if-absent. If the key already exists with an identical length and digest, this is a
//! retried upload and must succeed silently; a different length or digest throws.
virtual void upload_object(channel_data buf, std::string name) = 0;
```

- Produces the p2p key grammar. `<data>` is `data_plane_name()` (`include/comm/Channel.h:167-170`),
  which as of Task 3 expands to `<run_id>:<data_comm_name>` under FT and to `<data_comm_name>` alone
  without it. **This is mechanical and applies to every key grammar in Tasks 4, 5, 6 and 8:** the
  `run_id` prefix arrives for free through the one helper, and no task below re-derives it. Never
  build a key from `comm_name` or `data_comm_name` directly.

```text
<data>:p2p:<src>:<dst>:<message_id>
```

- Produces, in `FMI::Utils` (`include/utils/Common.h`, next to `Timeout` at `:12-17`):

```cpp
//! A message was found under its identity key but does not match the identity the consumer
//! expected. Loud by construction: never truncate, never substitute.
struct IdentityMismatch : public std::runtime_error {
    using std::runtime_error::runtime_error;
};
```

- [ ] **Step 1: Reproduce silent truncation and the counter reset (RED)**

Add two cases to `tests/clientserver_keys.cpp`:

- `exact_length_is_enforced`: upload a 12-byte object, then `download` into a 4-byte buffer.
  Expect `IdentityMismatch`. Today `src/comm/Redis.cpp:72` does
  `std::memcpy(buf.buf, reply->str, std::min(buf.len, reply->len))` and returns `true` — silent
  partial delivery. (S3's `download_object`, `src/comm/S3.cpp:44-55`, has the mirror-image defect:
  `s.read(buf.buf, buf.len)` ignores `gcount()`, leaving the buffer tail untouched on a short
  object.)
- `p2p_sequence_never_resets`: send three messages 0→1, `reconfigure_for_epoch` twice between them,
  and assert the keys are `…:p2p:0:1:0`, `…:p2p:0:1:1`, `…:p2p:0:1:2`.

Run: `cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=ClientServerKeys`. Expected:
both new cases fail.

- [ ] **Step 2: Rewrite the p2p path**

In `src/comm/ClientServer.cpp`, replace `send` (`:6-18`) and `recv` (`:20-32`) — note the message
ordinal itself moves onto the RAII scope in Task 7, so keep the maps thin:

- key from `p2p_send_seq[{peer_id, dest}]++` / `p2p_recv_seq[{src, peer_id}]++` in the grammar
  above;
- `recv` is **consumer-delete** — p2p is a single-consumer namespace per the spec's contract 1 —
  and follows the checkpoint-consistent discipline: repeatable `download_object` (`GET`) → commit
  the bytes into `buf` → idempotent `delete_object` (`DEL`). **Never `GETDEL`**; a freeze between
  the server-side delete and the process-side copy destroys the only copy.
- `send` no longer pushes the key into `created_objects` (`src/comm/ClientServer.cpp:102`) for the
  p2p lane, since the
  consumer reclaims it; `finalize()` retains its sweep for keys the consumer never reached.

- [ ] **Step 3: Make length and retry contracts real**

- `Redis::download_object` (`src/comm/Redis.cpp:64-76`): on a length mismatch, free the reply and
  throw `IdentityMismatch` naming the key, expected and actual lengths. Keep returning `false` only
  for `REDIS_REPLY_NIL`.
- `Redis::upload_object` (`:54-62`): issue `SET <key> <payload> NX`. On a nil reply (key present),
  `GET` it and compare length **and** bytes; equal → return, different → throw `IdentityMismatch`.
  Report the existing `redisCommand` error branch rather than only logging it.
- `Redis::get_object_names` (`:85-94`) additionally leaks its reply — add the missing
  `freeReplyObject`. Leave the `KEYS *` scoping to Task 6.
- `S3::download_object` (`src/comm/S3.cpp:44-55`): check `outcome.GetResult().GetContentLength()`
  and `s.gcount()` against `buf.len`; throw on mismatch. `S3::upload_object` (`:57-67`): use a
  `HeadObject` precondition for the retry-safe path and throw on a mismatching length.

- [ ] **Step 4: Verify (GREEN)**

```bash
cmake --build /home/luca/fmi/build -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=ClientServerKeys

redis-cli -h 127.0.0.1 -p 6379 flushall
/home/luca/fmi/build/tests/fmi_migration_p2p_cut_counterexample; echo "p2p counterexample exit=$?"

for c in queued_message_lost queued_message_substituted partially_drained_fifo_suffix \
         multi_message_backlog_shift variable_size_partial_overwrite bidirectional_backlog \
         three_rank_ring_backlog unrelated_rank_migration single_rank_self_send; do
  /home/luca/fmi/build/tests/fmi_migration_counterexamples --case "$c"; echo "$c exit=$?"
done
```

Expected: `ClientServerKeys` passes; **`fmi_migration_p2p_cut_counterexample` exits `0`** — this is
the headline flip, and per the spec's acceptance oracles it lands here at A2, not at any Direct
milestone (that binary hardcodes `config/fmi_ft_stress_redis_test.json` at
`tests/migration_p2p_cut_counterexample.cpp:267-268`, which sets `Direct.enabled=false`,
`Redis.enabled=true`, `preferred_data_backend="Redis"`). Of the nine Redis cases, the seven pure
p2p ones report `migrated=preserved` and exit `0`; `partially_drained_fifo_suffix` and
`unrelated_rank_migration` use `barrier()` and remain non-zero until Task 6. Then run the full
Boost suite per the Global Constraints preflight and confirm it is green.

- [ ] **Step 5: Commit**

```bash
git -C /home/luca/fmi add include/utils/Common.h include/comm/ClientServer.h \
  src/comm/ClientServer.cpp src/comm/Redis.cpp src/comm/S3.cpp tests/clientserver_keys.cpp
git -C /home/luca/fmi commit -m "fix: job-lifetime p2p keys with consumer-delete and exact lengths"
```

---

### Task 5: Collective namespaces — gather/scatter, per-consumer bcast/scan, reduce

**Files:**
- Modify: `include/comm/ClientServer.h`
- Modify: `src/comm/ClientServer.cpp`
- Modify: `tests/clientserver_keys.cpp`

**Interfaces:**
- Produces, on `ClientServer` (new overrides of the `Channel` defaults at
  `include/comm/Channel.h:60` and `:68`):

```cpp
//! ClientServer no longer inherits Channel::gather / Channel::scatter. The base
//! implementations (src/comm/Channel.cpp:32-62) route every fragment through the *p2p*
//! send/recv virtuals, so a gather fragment and an application send on the same directed pair
//! share one ordinal sequence and can silently swap payloads.
void gather(channel_data sendbuf, channel_data recvbuf, FMI::Utils::peer_num root) override;
void scatter(channel_data sendbuf, channel_data recvbuf, FMI::Utils::peer_num root) override;
```

- Produces the collective key grammar. `<data>` is `data_plane_name()`, i.e. `run_id`-prefixed under
  FT exactly as in Task 4 — mechanical, and the reason no key below mentions `run_id` explicitly:

```text
<data>:gather:<n>:<contributor>
<data>:scatter:<n>:<consumer>
<data>:bcast:<n>:<root>:for:<consumer>
<data>:reduce:<n>:<contributor>
<data>:scan:<n>:<producer>:for:<consumer>
```

`<n>` is the existing per-namespace ordinal from `num_operations`
(`include/comm/ClientServer.h:73-78`), extended with `gather` and `scatter` entries. Task 8
replaces `<n>` with the Communicator-scoped collective index.

- [ ] **Step 1: Reproduce the base-class counter leak and the scan OOB (RED)**

Add to `tests/clientserver_keys.cpp`:

- `gather_does_not_consume_p2p_slots`: on a Redis channel with `num_peers=3`, rank 0 issues
  `send(payload_A, 0)`-style application traffic from rank 1 to rank 0, and separately a
  `gather(root=0)`. Assert that rank 0's application `recv` returns `payload_A` and the gathered
  slice returns the gather contribution. Today `src/comm/Channel.cpp:34` and `:42` call the same
  `send`/`recv` virtuals as the application, so the two streams share
  `p2p_send_seq[{1,0}]` and cross over.
- `scan_ordinal_survives_a_timed_out_scan`: run `scan` with `num_peers = 3` where one contributor
  never uploads, so the poll loop at `src/comm/ClientServer.cpp:213` expires and `:229-231` throws
  `Utils::Timeout`; assert that a subsequent successful `scan` on the surviving ranks uses ordinal
  `1`, not `0`. Today `num_operations["scan"]++` sits at `:232`, **below** the throw, so a timed-out
  scan leaves the ordinal desynchronized against peers that succeeded.

  **Not planned: the scan out-of-bounds read.** It was fixed upstream in `523128a` ("comm: fix
  ClientServer::scan fold order and out-of-bounds apply loop"): every loop is now bounded by
  `num_data = peer_id + 1` (`src/comm/ClientServer.cpp:159`, `:190-204`, `:214`) with the reason
  recorded in a comment at `:189-191`, and `scan_ltr_client_server_ordering`
  (`tests/channels.cpp:812`) covers the fold order. Likewise `num_operations["reduce"]++` is already
  above its throw (`src/comm/ClientServer.cpp:142` versus `:143-145`), so only the `scan` ordinal
  needs moving.

- [ ] **Step 2: Give gather and scatter their own namespace**

Implement `ClientServer::gather` / `ClientServer::scatter` mirroring the shape of
`src/comm/Channel.cpp:32-62` but uploading/downloading under the `gather`/`scatter` keys and using
`upload`/`download` directly instead of `send`/`recv`. Both are single-consumer namespaces per the
spec's contract 1, so the consumer deletes: the root deletes each `…:gather:<n>:<i>` after
committing it; each non-root consumer deletes its own `…:scatter:<n>:<j>`.

- [ ] **Step 3: Per-consumer bcast and scan keys; consumer-delete for reduce**

- `bcast` (`src/comm/ClientServer.cpp:34-42`): the root uploads `N-1` per-consumer objects `…:bcast:<n>:<root>:for:<j>`;
  each consumer downloads and deletes only its own. This replaces the single shared object, which
  no consumer can safely delete.
- `scan` (`src/comm/ClientServer.cpp:153-233`): peer `i` uploads one object per downstream consumer,
  `…:scan:<n>:<i>:for:<j>`, for `j > i`; each consumer deletes only its own copies. Hoist
  `num_operations["scan"]++` (`:232`) above the `throw` at `:229-231` so a timed-out scan does not
  desynchronize the ordinal against peers that succeeded. The loop bounds are already `num_data`
  (`523128a`) — leave them alone.
- `reduce` (`src/comm/ClientServer.cpp:106-151`): contributions `…:reduce:<n>:<i>` are single-consumer
  (the root); the root deletes each after committing it. `num_operations["reduce"]++` (`:142`) is
  already above the timeout throw at `:143-145`; no change needed there.

- [ ] **Step 4: Verify (GREEN)**

```bash
cmake --build /home/luca/fmi/build -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=ClientServerKeys
```

Then a targeted collectives sweep against Redis. Temporarily (uncommitted) re-enable Redis in the
`backends` map at `tests/channels.cpp:68-72` (uncomment `:70`), rebuild, and run:

```bash
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=Channels
git -C /home/luca/fmi checkout -- tests/channels.cpp   # only if the re-enable was temporary
```

Expected: `ClientServerKeys` green including the two new cases; the whole `Channels` suite green
with Redis enabled — which is the first coverage `bcast`/`barrier`/`gather`/`scatter`/`reduce` over
ClientServer have had (`scan` already had `scan_ltr_client_server_ordering`,
`tests/channels.cpp:812`). Then the full suite per the preflight.

- [ ] **Step 5: Commit**

```bash
git -C /home/luca/fmi add include/comm/ClientServer.h src/comm/ClientServer.cpp \
  tests/clientserver_keys.cpp
git -C /home/luca/fmi commit -m "fix: per-namespace ClientServer collectives with consumer-delete"
```

---

### Task 6: Barrier by direct key probe, the deadline-suspension seam, and job-state cleanup

**Files:**
- Modify: `src/comm/ClientServer.cpp`
- Modify: `include/comm/ClientServer.h`
- Modify: `src/comm/Redis.cpp`
- Modify: `include/comm/Redis.h`
- Modify: `src/comm/S3.cpp`
- Modify: `include/comm/S3.h`
- Modify: `include/ft/ControlPlane.h`
- Modify: `src/ft/ControlPlane.cpp`
- Modify: `tests/clientserver_keys.cpp`

**Interfaces:**
- Produces, **replacing** the suffix scan over an unscoped listing (`include/comm/ClientServer.h:58`,
  `src/comm/ClientServer.cpp:52-64`) with a direct probe of the `N` names the barrier already knows:

```cpp
//! Existence probe over a KNOWN set of keys, in one round trip. Returns a parallel vector:
//! result[i] is true iff names[i] exists. Redis: MGET / EXISTS. S3: a HeadObject fan-out.
//! This is NOT a listing: nothing is discovered, so nothing foreign can be observed, there is
//! no cursor to duplicate keys, and there is no pagination bound to honour.
virtual std::vector<bool> probe_objects(const std::vector<std::string>& names) = 0;
```

`get_object_names()` (`include/comm/ClientServer.h:58`) is **retained unchanged** — `finalize()`
(`src/comm/ClientServer.cpp:78-85`) is its only remaining caller after this task, and it is not on any
correctness path.

> **Why a probe and not a prefix-scoped listing.** The spec's contract 1 makes the direct probe
> normative, and the reason is that it deletes a class rather than patching one. The barrier's key
> names are **fully determined** by `(comm, generation, rank)` — already true today
> (`src/comm/ClientServer.cpp:46-47`) — so there is nothing to discover. The defect that exists today
> is *scope*: arrival is a `count_if` over a **suffix** match (`_barrier_<n>`,
> `src/comm/ClientServer.cpp:54-56`) applied to an unscoped listing of the entire store —
> `Redis::get_object_names` issues `KEYS *` (`src/comm/Redis.cpp:88`) and `S3::get_object_names` a
> whole-bucket `ListObjects` (`src/comm/S3.cpp:79-93`) — so a foreign communicator's or a dead
> epoch's key counts as an arrival. The defect that *would appear* if the listing were merely
> prefix-scoped is duplication: scoping forces `KEYS` to become `SCAN`, `SCAN` may return the same key
> more than once, and the count would then have to become a deduplicated rank set. **Neither problem
> exists for an `O(N)` `MGET`/`EXISTS` over the known names**, and the S3 `ListObjects`
> pagination requirement (`IsTruncated`/`NextMarker`, which today's implementation silently ignores)
> disappears with them. The earlier draft of this task specified `list_objects(prefix)` plus a
> deduplicated `std::set<peer_num>`; that requirement is **dropped**, not weakened.

> **Explicitly rejected: a two-phase arrived/departed barrier.** It was proposed to let barrier keys
> be reclaimed. It does not work here: the departure phase is a second rendezvous over the same medium
> with the same termination requirement, so it merely moves the hazard — a rank dumped mid-barrier
> lets rank 0 observe all departures and reclaim, while rank 1 polls late and blocks to `max_timeout`.
> It also buys no correctness that is missing, because the keys are already generation-qualified
> (`_barrier_<n>`, `src/comm/ClientServer.cpp:46-47`), so a stale generation's keys can never be
> miscounted as this generation's arrivals. **Retention until job cleanup is kept**: deleting after
> one rank's pass strands slower ranks.

- Produces the barrier key grammar and arrival rule (`<data>` is `data_plane_name()`, i.e.
  `run_id`-prefixed under FT — see Task 4):

```text
<data>:barrier:<n>:<rank>            for rank in [0, num_peers)
```

Arrival is `probe_objects` over exactly those `N` names, returning when every entry is true. No
parsing of key names, no set, no count over a listing.

- Produces, on `ClientServer` (`include/comm/ClientServer.h`, next to the `timeout` / `max_timeout`
  members) — the seam the spec's contract 3 assigns to Plan A: *"ClientServer's four poll loops also
  expire at `max_timeout` with no suspension hook; Plan A records the hook, Plan C wires it."*

```cpp
//! Deadline-suspension seam (design spec, contract 3, "Deadline suspension"). Plan A only
//! RECORDS this hook; Plan C stage C3 replaces the default with per-link suspension keyed on
//! link state, so a poll loop whose peer is mid-migration does not expire at max_timeout.
//! Returning true means "this wait does not count against the deadline".
//! The four poll loops are src/comm/ClientServer.cpp:52 (barrier, deadline advanced at :60),
//! :89 (download, at :94), :117 (reduce, at :139) and :213 (scan, at :226).
virtual bool deadline_suspended() const { return false; }
```

- Produces, on `ControlPlane` (`include/ft/ControlPlane.h:48`):

```cpp
//! Clears the control-plane prefix, the fmi:ft:name:<comm_name> index entry from Task 3, and
//! the data-plane namespaces of `data_comm_name`.
//!
//! HYGIENE, NOT A FENCE. Before Task 3 this call was the only thing standing between a reused
//! communicator name and a previous run's messages. With run_id in front of every data key and
//! every rendezvous tuple, a previous run's keys are UNREACHABLE rather than merely unlikely to
//! be hit, so what this reclaims is space and dbsize stability -- not correctness.
void clear_job_state(const std::string& data_comm_name = "");
```

- [ ] **Step 1: Reproduce the cross-scope barrier match (RED)**

Add `ClientServerKeys/barrier_ignores_foreign_keys` to `tests/clientserver_keys.cpp`: with
`num_peers = 2`, pre-seed Redis with a key ending in `_barrier_0` under a **different** communicator
name, then run `barrier()` from a single rank. Assert it throws `Utils::Timeout` (its partner never
arrived).

Expected today: it returns immediately. `src/comm/ClientServer.cpp:54-56` matches on the **suffix**
`_barrier_<n>` only, and `Redis::get_object_names` (`src/comm/Redis.cpp:85-94`) lists the whole
database with `KEYS *`. This is precisely the `stale_barrier_object` corpus mechanism: a target that
exits at epoch 0 without finalizing leaves `…0_barrier_0`, and the survivor's epoch-1 barrier 0 counts
it as an arrival and returns before the replacement exists.

- [ ] **Step 2: Add the existence probe**

- `Redis::probe_objects(names)`: one `MGET` (or `EXISTS key…` when only presence is needed) over the
  whole name vector; map a nil element to `false`. Free the reply — note that
  `Redis::get_object_names` (`src/comm/Redis.cpp:85-94`) currently leaks its reply, which Task 4
  already fixes; do not reintroduce the pattern here.
- `S3::probe_objects(names)`: a `HeadObject` per name, `404` → `false`. `N` is the peer count, so the
  fan-out is bounded by the job size and needs no pagination.
- Both are additive: `get_object_names()` keeps its signature and its `finalize()` caller.

- [ ] **Step 3: Rewrite barrier, and route the four poll loops through the seam**

Replace `src/comm/ClientServer.cpp:44-65`:

```text
upload  <data>:barrier:<n>:<peer_id>                    (write-once, retry-safe, from Task 4)
poll    probe_objects({<data>:barrier:<n>:0 … <data>:barrier:<n>:num_peers-1})
        return when every entry is true
        throw Utils::Timeout() at max_timeout
```

Increment `num_operations["barrier"]` (`src/comm/ClientServer.cpp:48`) **before** the poll loop so a
timed-out barrier does not reuse `<n>`. Do not delete barrier keys on return.

Add `deadline_suspended()` and route all four loops through it —
`if (!deadline_suspended()) { elapsed_time += timeout; }` at `src/comm/ClientServer.cpp:60`, `:94`,
`:139` and `:226` — so the default (`false`) is byte-for-byte today's behaviour and C3 has exactly one
override to write. Add `ClientServerKeys/deadline_seam_defaults_to_unsuspended`: a subclass returning
`true` makes a `barrier()` with an absent partner loop past `max_timeout` (bound it with a test-side
cancel), while the base class still throws `Utils::Timeout`.

> **The suspension-ordering gap, closed explicitly.** The hook is recorded here and **not wired**, so
> for the whole of Plan A a ClientServer poll loop still expires at `max_timeout` while its peer is
> being dumped. That makes every A2/A3 acceptance result partly a function of dump latency unless the
> margin is pinned. **Requirement, not advice:** every acceptance run in Tasks 6, 8 and 9 uses a
> `max_timeout` that exceeds the *measured* dump duration for the scenario, and the measured duration
> and the configured `max_timeout` are recorded next to the result.
> `config/fmi_ft_stress_redis_test.json` is the file that sets it for both counterexample binaries
> (`tests/migration_p2p_cut_counterexample.cpp:268` hardcodes that path). Take the dump duration from
> Plan B's `O(RSS)` measurement (~0.4 s/GB) or measure it directly; a green corpus with an unrecorded
> margin does not count as a pass. If Plan C stage C3 lands the per-link override before Plan A
> finishes, drop the margin requirement and say so.

- [ ] **Step 4: Extend job cleanup to the data plane and the name index**

`ControlPlane::clear_job_state` (`src/ft/ControlPlane.cpp:431-447`) currently deletes only the
`fmi:ft:<comm>:` control-plane keys (`meta_key`, `pending_key`, `boundaries_key`, then a `KEYS`
sweep of `prefix() + "epoch:*"`). Add:

- when `data_comm_name` is non-empty, a sweep of `<run_id>:<data_comm_name>:*` — resolve `run_id`
  through `resolve_run_id` (Task 3), and match the existing `clear_criu_state` shape
  (`src/ft/ControlPlane.cpp:450-462`);
- a final `DEL fmi:ft:name:<comm_name>`, **last**, because it is the entry point every other lookup
  goes through: deleting it first would strand the data-plane sweep with no way to resolve `run_id`.

Update the call sites that already exist in the corpus
(`tests/migration_p2p_cut_counterexample.cpp:104`, `:248`) and in `tests/fault_tolerance.cpp:41`,
`:81`, `:89`, `:101`, `:115`, `:127`, `:158`, `:172` to pass the data name where they know it; the
defaulted parameter keeps every other call compiling.

- [ ] **Step 5: Verify (GREEN)**

```bash
redis-cli -h 127.0.0.1 -p 6379 flushall
cmake --build /home/luca/fmi/build -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=ClientServerKeys

grep -n 'max_timeout' /home/luca/fmi/config/fmi_ft_stress_redis_test.json   # record this
/home/luca/fmi/build/tests/fmi_migration_counterexamples --case stale_barrier_object; echo "exit=$?"
for c in queued_message_lost queued_message_substituted partially_drained_fifo_suffix \
         multi_message_backlog_shift variable_size_partial_overwrite bidirectional_backlog \
         three_rank_ring_backlog unrelated_rank_migration single_rank_self_send; do
  /home/luca/fmi/build/tests/fmi_migration_counterexamples --case "$c"; echo "$c exit=$?"
done
/home/luca/fmi/build/tests/fmi_migration_p2p_cut_counterexample; echo "p2p exit=$?"
redis-cli -h 127.0.0.1 -p 6379 dbsize
```

Expected: **all nine Redis program scenarios report `migrated=preserved` and exit `0`**,
`stale_barrier_object` exits `0`, `fmi_migration_p2p_cut_counterexample` exits `0`, and `dbsize`
returns to its pre-run value. Record the `max_timeout` from the `grep` above together with the
observed per-scenario wall time, per the margin requirement in Step 3. Then the full Boost suite per
the preflight.

Note deliberately: `--backend redis` **still exits `2`** at this point, because
`future_matching_send_after_cut`, `pending_set_expands_after_park`,
`failed_operation_advances_boundary` and `promotion_before_full_membership` are epoch/control-plane
defects that Plan A does not fix (Plan C stage C3 owns them). Task 9 introduces the scoped selector
that makes the Plan A gate a single pasteable command.

- [ ] **Step 6: Commit**

```bash
git -C /home/luca/fmi add include/comm/ClientServer.h src/comm/ClientServer.cpp \
  include/comm/Redis.h src/comm/Redis.cpp include/comm/S3.h src/comm/S3.cpp \
  include/ft/ControlPlane.h src/ft/ControlPlane.cpp tests/clientserver_keys.cpp
git -C /home/luca/fmi commit -m "fix: barrier arrival by direct key probe; data-plane job cleanup"
```

---

### Task 7: The operation identity scope

**Files:**
- Create: `include/utils/OperationIdentity.h`
- Create: `src/utils/OperationIdentity.cpp`
- Modify: `include/Communicator.h`
- Modify: `include/comm/Channel.h`
- Modify: `include/comm/ClientServer.h`
- Modify: `src/comm/ClientServer.cpp`
- Modify: `CMakeLists.txt`
- Create: `tests/operation_identity.cpp`
- Modify: `tests/clientserver_keys.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces, per the design spec, contract 1 ("Where the identity is produced"):

```cpp
namespace FMI::Utils {
    enum class Lane : std::uint8_t { P2P = 0, Collective = 1 };

    struct OperationIdentity {
        Lane          lane             = Lane::P2P;
        Operation     op_kind          = Operation::send;
        std::uint64_t collective_index = 0;   // per-Communicator; 0 and unused on the P2P lane
        peer_num      root             = 0;   // collective root, or the peer id for p2p
        bool          associative      = true;
        bool          commutative      = true;

        [[nodiscard]] bool left_to_right() const { return !(commutative && associative); }
    };

    //! The identity in force for the current logical operation on THIS thread.
    //! Thread-local, not process-global: tests/communicator.cpp:21-32 runs several ranks as
    //! OpenMP threads inside one process, and a process-global scope would cross-contaminate them.
    const OperationIdentity& active_operation_identity();

    //! RAII. Installs `identity` for the duration of one logical operation and restores the
    //! previous value on scope exit, including on the unwind path.
    class OperationScope {
    public:
        explicit OperationScope(const OperationIdentity& identity);
        ~OperationScope();
        OperationScope(const OperationScope&) = delete;
        OperationScope& operator=(const OperationScope&) = delete;
    private:
        OperationIdentity previous_;
    };
}
```

- `Communicator::OperationGuard` (`include/Communicator.h:223-239`) gains an `OperationIdentity`
  parameter and owns an `OperationScope` member. Every entry point supplies its own identity:
  `send` (`:32`), `recv` (`:41`), `bcast` (`:50`), `barrier` (`:58`), `gather` (`:70`),
  `scatter` (`:84`), `reduce` (`:99`), `allreduce` (`:124`), `scan` (`:149`).
- `Communicator` gains `std::uint64_t collective_index_ = 0;`, incremented by the guard for every
  `Lane::Collective` operation and **not** by `send`/`recv`. Per the spec, the counter lives on the
  `Communicator`, not the channel: with a per-channel counter, `bcast → Redis; barrier → Direct`
  yields collective count 0 on both ranks in either issue order and detects nothing.
- **The scope is also the single owner of the per-lane, per-directed-pair `message_id`, for BOTH
  channel families.** `Communicator` gains

```cpp
//! Job-lifetime, per-lane, per-directed-pair message ordinals. Never reset -- not by epoch
//! reconfiguration, not by finalize, not by a channel rebuild. Read through the active
//! OperationScope by BOTH channel families, so there is exactly one counter per concept.
std::map<std::tuple<Lane, peer_num, peer_num>, std::uint64_t> message_ids_;
```

  and `OperationIdentity` gains a `std::uint64_t message_id` that the guard stamps when the operation
  is issued. This is normative in the spec ("that same RAII scope is the **single owner** of the
  per-lane, per-directed-pair `message_id` counter, for **both** channel families") and it changes
  Task 4's shape: the `p2p_send_seq` / `p2p_recv_seq` maps Task 4 introduces on `ClientServer` become
  **reads of the scope**, not a second copy. `ClientServer` already maintains exactly this counter
  today, in `num_operations["send"+dest]` / `["recv"+dest]` (`src/comm/ClientServer.cpp:7,16,21,30`;
  declared `include/comm/ClientServer.h:73`), and a framed `Direct` would otherwise grow a second,
  independent copy inside Plan C's link layer. Two counters for one concept is exactly the divergence
  this plan exists to prevent.

  > **Rejected: a separate "logical message" layer above the channels owning the identity.** The
  > collectives are implemented *inside* the `Channel` subclasses — `PeerToPeer` implements
  > `bcast`/`barrier`/`reduce`/`allreduce`/`scan`/`gather`/`scatter`
  > (`src/comm/PeerToPeer.cpp:14,29,35,86,132,186,241`) and `ClientServer` implements its own
  > (`src/comm/ClientServer.cpp:34,44,106,153`) — i.e. *below* where such a layer would sit, with
  > `include/comm/Channel.h:56` ("Child classes may create more optimized implementations") and
  > `:64` explicitly inviting subclasses to supply optimized versions, and
  > `Communicator::register_channel` (`include/Communicator.h:166`) as the documented out-of-tree
  > extension point. A layer owning identity for both families would have to change the pure-virtual
  > signatures or hoist the collectives out of both subclasses — strictly larger breakage than the
  > lane parameter this plan already forbids. The RAII scope owns the counter instead.

- **Normative invariant this task depends on, to be recorded in the header's doc comment:** *no FMI
  collective places two messages on one directed pair within a single logical operation.* That is what
  makes `(collective_index, op_kind, root)` plus per-directed-pair FIFO sufficient, and it is why no
  algorithm-phase or substep field is needed. A future optimized collective that violated it would
  silently break identity, so any new or overridden collective must be checked against it.
- **Only `Communicator` sets the scope.** Channels never nest a scope. `PeerToPeer::barrier()`
  (`src/comm/PeerToPeer.cpp:29-33`) delegates to `allreduce`, and `Channel::allreduce`
  (`src/comm/Channel.cpp:64-67`) delegates to `reduce` + `bcast`; those are fragments of one logical
  `barrier` and must keep `op_kind == barrier`. A nested scope would relabel them and destroy the
  identity.
- `include/comm/Channel.h` includes the new header and documents that channels read the active
  scope. **No `Channel` virtual signature changes** — that would break custom channels satisfying
  the existing interface.

- [ ] **Step 1: Assert the seam is absent (RED)**

Create `tests/operation_identity.cpp`, suite `OperationIdentity`, and add it to
`FMI_TEST_SOURCES`. Case `guard_publishes_identity` uses a `RecordingChannel : FMI::Comm::Channel`
that snapshots `FMI::Utils::active_operation_identity()` inside every virtual, then:

- constructs `FMI::Communicator(0, 1, "../../config/fmi_test.json", name)` — `fault_tolerance` is
  disabled there (`config/fmi_test.json:54`), so no control plane is needed;
- calls the public `register_channel("Direct", recording)` to replace the sole enabled backend
  before it is ever used (`Direct`'s constructor only parses params, `src/comm/Direct.cpp:12-26`;
  pairing is lazy in `check_socket`, `:82-106`);
- issues `bcast(root=1)`, `barrier()`, `reduce(root=2, f)` with a non-commutative `f`, and `send`;
- asserts the recorded tuples are
  `{Collective, bcast, 0, 1, …}`, `{Collective, barrier, 1, 0, …}`,
  `{Collective, reduce, 2, 2, associative=false, commutative=false}`, `{P2P, send, 0, dest, …}`.

Run: `cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=OperationIdentity`. Expected:
compilation fails — `include/utils/OperationIdentity.h` does not exist.

- [ ] **Step 2: Implement the scope**

Add the header and `src/utils/OperationIdentity.cpp` (a `thread_local OperationIdentity` plus the
two accessors). Register `src/utils/OperationIdentity.cpp` in `FMI_SOURCES` (`CMakeLists.txt:42-51`).

- [ ] **Step 3: Wire the Communicator entry points**

Extend `OperationGuard` and pass an identity from each of the nine entry points. `reduce`
(`include/Communicator.h:104`) and `allreduce` (`:129`) already compute
`left_to_right = !(f.commutative && f.associative)`; pass `f.associative` / `f.commutative` into
the identity rather than the derived boolean, since the spec puts the **flags** in the envelope and
validates them per collective. `scan` (`:148-166`) must publish them too even though it does not
currently compute `left_to_right` at the Communicator level.

**`Communicator::scan` is flag-blind, and this task must not silently paper over it.**
`include/Communicator.h:153` calls `policy->get_channel({Utils::scan, sendbuf.size_in_bytes()})`
with no `left_to_right` argument, unlike `reduce` (`:104`) and `allreduce` (`:129`). Consequence: a
`commutative`/`associative` divergence on `scan` produces an *algorithm* mismatch — which the
envelope's flags catch in Task 8 — but can never split the two ranks onto different **backends**,
because the backend choice does not see the flag. Publishing the flags in the scope is correct and
sufficient for identity; whether to also make the *policy call* consistent with `reduce`/`allreduce`
is an open item in the design spec, and this plan does not depend on the answer. Do **not** change
`:153` here. Record the asymmetry in a comment at the `scan` entry point so it is not rediscovered by
accident.

- [ ] **Step 4: Move the per-directed-pair ordinal onto the scope**

`OperationGuard` stamps `identity.message_id` from `Communicator::message_ids_` keyed on
`(lane, src, dst)` — `(peer_id, dest)` for `send`, `(src, peer_id)` for `recv`, and on the collective
lane the pair each fragment actually traverses — incrementing after the read, never resetting.

Then delete `ClientServer::p2p_send_seq` / `p2p_recv_seq` (introduced in Task 4) and have
`ClientServer::send` / `recv` (`src/comm/ClientServer.cpp:6-32`) read
`active_operation_identity().message_id` instead. This is the C6 single-owner rule: after this step
there is exactly one per-directed-pair counter in the tree, and Plan C's link layer reads the same
one rather than growing its own.

Add `OperationIdentity/message_id_is_scope_owned`: with two `RecordingChannel`s registered under
different backend names, a `send(0→1)` on one and a `send(0→1)` on the other must see message ids
`0` then `1` — a per-channel counter would report `0` twice. Add
`ClientServerKeys/p2p_sequence_never_resets` from Task 4 to the same run and confirm it still passes
against the moved counter.

- [ ] **Step 5: Verify (GREEN)**

```bash
cmake -S /home/luca/fmi -B /home/luca/fmi/build -DFMI_BUILD_TESTS=ON
cmake --build /home/luca/fmi/build -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=OperationIdentity
/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000 &
TCPUNCHD=$!; sleep 1
sed -i 's/192\.168\.0\.166/127.0.0.1/' /home/luca/fmi/config/fmi_test.json
cd /home/luca/fmi/build/tests && ./Boost_Tests_run
git -C /home/luca/fmi checkout -- config/fmi_test.json
kill "$TCPUNCHD"
```

Expected: `OperationIdentity` green (it needs no Redis and no rendezvous server); `ClientServerKeys`
green, in particular `p2p_sequence_never_resets` against the moved counter; full suite green. The only
behaviour change is where the p2p ordinal is stored — the emitted keys are byte-identical, which is
what `p2p_sequence_never_resets` asserts.

- [ ] **Step 6: Commit**

```bash
git -C /home/luca/fmi add include/utils/OperationIdentity.h src/utils/OperationIdentity.cpp \
  include/Communicator.h include/comm/Channel.h include/comm/ClientServer.h \
  src/comm/ClientServer.cpp CMakeLists.txt \
  tests/operation_identity.cpp tests/clientserver_keys.cpp tests/CMakeLists.txt
git -C /home/luca/fmi commit -m "feat: RAII operation identity scope owns the per-pair ordinal"
```

---

### Task 8: Land the identity on ClientServer keys and make mismatches loud

**Files:**
- Modify: `include/comm/ClientServer.h`
- Modify: `src/comm/ClientServer.cpp`
- Modify: `src/comm/PeerToPeer.cpp`
- Modify: `src/comm/Channel.cpp`
- Modify: `include/Communicator.h`
- Modify: `src/Communicator.cpp`
- Modify: `include/ft/ControlPlane.h`
- Modify: `src/ft/ControlPlane.cpp`
- Modify: `tests/operation_identity.cpp`
- Modify: `tests/clientserver_keys.cpp`

**Interfaces:**
- Produces the validated object header, prefixed to every ClientServer object:

```cpp
namespace FMI::Comm {
    struct ObjectHeader {                 // 32 bytes, little-endian, fixed layout
        std::uint16_t wire_version;       // 1
        std::uint8_t  lane;
        std::uint8_t  op_kind;
        std::uint8_t  flags;              // bit0 associative, bit1 commutative
        std::uint8_t  reserved[3];
        std::uint32_t root;
        std::uint32_t reserved2;
        std::uint64_t collective_index;
        std::uint64_t total_length;
    };
    static_assert(sizeof(ObjectHeader) == 32, "ObjectHeader layout is wire-visible");
}
```

Rationale, and why the flags are **not** in the key: `op_kind`, `collective_index` and `root` go in
both the key and the header (defense in depth). The commutative/associative flags go in the
**header only** — if they were in the key, a flag divergence would produce a key that never appears
and the consumer would sit until `max_timeout` and report an ambiguous `Utils::Timeout`
(classified `OperationStuck`). In the header, the consumer downloads the object it was going to
download anyway, sees the mismatch immediately, and throws a bounded, diagnosable
`Utils::IdentityMismatch` naming both sides — which is the `LoudFail` outcome the corpus can gate
on.

- Produces the A3 key grammar, replacing the Task 5 grammar's `<n>` with the Communicator-scoped
  ordinal `<ci>` from `active_operation_identity().collective_index`:

```text
<data>:p2p:<src>:<dst>:<message_id>                         (unchanged; lane P2P)
<data>:coll:<op>:<ci>:<root>:gather:<contributor>
<data>:coll:<op>:<ci>:<root>:scatter:<consumer>
<data>:coll:<op>:<ci>:<root>:bcast:for:<consumer>
<data>:coll:<op>:<ci>:<root>:reduce:<contributor>
<data>:coll:<op>:<ci>:<root>:scan:<producer>:for:<consumer>
<data>:barrier:<ci>:<rank>
```

`<op>` is the numeric `FMI::Utils::Operation`. `num_operations`
(`include/comm/ClientServer.h:73-78`) is deleted; the `Communicator` is now the sole source of the
collective ordinal.

- [ ] **Step 1: Reproduce the silent flag divergence (RED)**

Add `ClientServerKeys/reduce_flag_divergence_is_loud`: on a Redis channel with `num_peers = 2`,
run `reduce(root=0)` where rank 0 supplies `raw_function{f, associative=false, commutative=false}`
and rank 1 supplies `{f, true, true}`, with an `f` whose result is order-dependent. Assert
`Utils::IdentityMismatch`.

Expected today: it **completes successfully with a silently order-dependent result**. Both branches
of `src/comm/ClientServer.cpp:106-151` upload contributions under the identical key
`…_reduce_<n>`; only the root's apply order (`:131-132`, via `left_to_right` at `:108`) differs, so a
divergence is entirely invisible. On `PeerToPeer` the same divergence selects wholly different
algorithms (`reduce_ltr` vs `reduce_no_order`, `src/comm/PeerToPeer.cpp:35-42`) and hangs instead.
This is the concrete instance of the spec's "policy uniformity" clause: the flags belong in the
envelope and are validated per collective.

Add `OperationIdentity/collective_index_is_communicator_scoped` to `tests/operation_identity.cpp`:
with two `RecordingChannel`s registered under different backend names, assert the collective index
advances across the pair rather than per channel.

- [ ] **Step 2: Key the collectives on the active scope**

In `src/comm/ClientServer.cpp`, replace every `num_operations[…]` lookup with
`FMI::Utils::active_operation_identity()` and emit the grammar above. Delete `num_operations` from
the header. `barrier` keeps its own top-level namespace (the arrival set from Task 6 is unchanged
apart from `<n>` becoming `<ci>`).

- [ ] **Step 3: Prefix and validate the header**

- `ClientServer::upload` (`src/comm/ClientServer.cpp:101-104`) builds a staging buffer of
  `sizeof(ObjectHeader) + buf.len`, fills the header from the active scope, and hands the whole
  thing to `upload_object`.
- `ClientServer::download` (`src/comm/ClientServer.cpp:87-99`) fetches into a staging buffer of
  `sizeof(ObjectHeader) + buf.len` — this supersedes the Task 4 exact-length check, which now
  compares against the header-inclusive size — validates `wire_version`, `lane`, `op_kind`,
  `root`, `collective_index`, `total_length` and both flags against the active scope, throws
  `Utils::IdentityMismatch` on any disagreement, and only then commits the payload into `buf`.
  Commit-then-delete ordering from Task 4 is preserved.

**Also in this step: the job-wide policy fingerprint — frozen at the FIRST GUARDED OPERATION, and
published to the CONTROL PLANE.** The spec's contract 1, "Policy uniformity", requires `run_id`,
`faas_price`, the model-parameter hash, `num_peers`, `wire_version` and the **effective** policy
fingerprint to be validated **once, job-wide** — "handshake fingerprint under Axis C; **startup
assertion before that**". Plan A owns the *before that* half; Plan C's `HandshakeState.fingerprint`
consumes the same `u64` later.

Two placement decisions, both normative in the spec, both different from this plan's earlier draft:

1. **Not at construction — at the first `OperationGuard`.** `hint()` (`src/Communicator.cpp:154-157`)
   and `set_channel_policy()` (`:150-152`) are **public runtime mutators**, so a digest taken during
   construction records the *default* `Hint` (`include/Communicator.h:198`,
   `channel_hint = Hint::cheap`) and never the effective one. Compute the digest lazily inside the
   first guarded operation, over the **effective** `hint` and `preferred_data_backend`, and make both
   setters `throw` once it is frozen.

   Rejecting mutation "after the rank has joined" is the right hazard named at the wrong point: the
   rank joins inside the `Communicator` constructor (`src/Communicator.cpp:64`) and **every** FT
   application in this repo calls `hint()` *after* construction —
   `runbooks/localstack-python311-redis/worker_core.py:38`,
   `runbooks/local-criu-state-transfer/transparent_state_transfer_demo.cpp:42`,
   `runbooks/aws-python311-s3/function/lambda_function.py:16`. A join-time freeze would reject all of
   them. First-guarded-operation is the earliest point that is both after configuration and before
   any message.

2. **Published to the control plane, not to a ClientServer data key.** A ClientServer-key mechanism is
   **inert on exactly the configurations that matter**: `config/fmi_test.json` enables only `Direct`
   (`:18`, with Redis disabled at `:11`), and so does the CRIU runbook config
   (`runbooks/local-criu-state-transfer/fmi.json:18`, Redis disabled at `:11`,
   `preferred_data_backend: "Direct"` at `:59`) — i.e. the checkpoint-verified path has no
   ClientServer channel to publish through at all. The control plane exists on every FT
   configuration by definition.

```cpp
//! FNV-1a over: wire_version, num_peers, the EFFECTIVE Hint, the effective
//! preferred_data_backend, faas_price (bit pattern), and the canonicalized model-parameter map --
//! the values that must be identical across ranks or the job is ill-formed (design spec,
//! contract 1, "Policy uniformity"). Computed lazily inside the FIRST OperationGuard and frozen
//! there; hint() and set_channel_policy() throw afterwards. Plan C carries the same u64 in the
//! link handshake.
std::uint64_t FMI::Communicator::policy_fingerprint();
```

- Produces, on `ControlPlane` (`include/ft/ControlPlane.h`, next to the Task 3 additions):

```cpp
//! Write-once per (run_id, rank) and compare. Returns the ranks whose recorded fingerprint
//! disagrees; empty means uniform so far. Never blocks on absent ranks: this is an assertion
//! over the ranks that have arrived, not a barrier.
std::vector<FMI::Utils::peer_num> publish_policy_fingerprint(FMI::Utils::peer_num rank,
                                                             std::uint64_t fingerprint) const;
```

Throw `Utils::IdentityMismatch` naming both ranks and both fingerprints on any disagreement. Under a
non-FT configuration there is no control plane and the assertion is skipped — that is the
`Communicator` suite's situation (`config/fmi_test.json:54`) and it must stay green.

Add `ClientServerKeys/hint_after_first_operation_throws` (or an `OperationIdentity` case if no Redis
is needed): `hint()` before the first collective succeeds and is reflected in the fingerprint;
`hint()` after it throws.

- [ ] **Step 4: Make algorithm selection read the same flags**

The channels currently recompute `left_to_right` locally from `raw_function`, which can disagree
with the published identity: `src/comm/PeerToPeer.cpp:36`, `:87`, `:133`, and
`src/comm/ClientServer.cpp:108`, `:158`. Replace all five with
`FMI::Utils::active_operation_identity().left_to_right()`, and assert in debug builds that the
`raw_function` flags match the scope. `src/comm/Channel.cpp:64-67` (`Channel::allreduce`) keeps
delegating to `reduce(root 0)` + `bcast(0)` — document that both fragments correctly share the
enclosing `allreduce` identity and are separated only by their key namespace. `Channel::gather` and
`Channel::scatter` (`src/comm/Channel.cpp:32-62`) remain the `PeerToPeer` default and are now
unambiguous, because the enclosing scope already carries `op_kind = gather|scatter` for a
`Communicator::gather`/`scatter` call and `op_kind = reduce` when `reduce_ltr`
(`src/comm/PeerToPeer.cpp:44-57`) calls `gather` internally.

- [ ] **Step 5: Verify (GREEN)**

```bash
redis-cli -h 127.0.0.1 -p 6379 flushall
cmake --build /home/luca/fmi/build -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=ClientServerKeys
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=OperationIdentity

/home/luca/fmi/build/tests/fmi_migration_p2p_cut_counterexample; echo "p2p exit=$?"
for c in queued_message_lost queued_message_substituted partially_drained_fifo_suffix \
         multi_message_backlog_shift variable_size_partial_overwrite bidirectional_backlog \
         three_rank_ring_backlog unrelated_rank_migration single_rank_self_send \
         stale_barrier_object; do
  /home/luca/fmi/build/tests/fmi_migration_counterexamples --case "$c"; echo "$c exit=$?"
done
```

Expected: `reduce_flag_divergence_is_loud` passes with a bounded `IdentityMismatch` (well under the
5 s `max_timeout` in `config/fmi_ft_stress_redis_test.json`); every Task 6 result is unchanged;
full Boost suite green per the preflight.

- [ ] **Step 6: Commit**

```bash
git -C /home/luca/fmi add include/comm/ClientServer.h src/comm/ClientServer.cpp \
  src/comm/PeerToPeer.cpp src/comm/Channel.cpp include/Communicator.h src/Communicator.cpp \
  include/ft/ControlPlane.h src/ft/ControlPlane.cpp \
  tests/operation_identity.cpp tests/clientserver_keys.cpp
git -C /home/luca/fmi commit -m "feat: identity-qualified ClientServer keys with a validated header"
```

---

### Task 9: The Plan A gate, `IdentityMismatch` classification, and the model's disposition

**Files:**
- Modify: `tests/migration_counterexample_scenarios.h`
- Modify: `tests/migration_counterexample_scenarios.cpp`
- Modify: `tests/migration_counterexample_runner.cpp`
- Modify: `tests/migration_cut_model.cpp`
- Create: `tests/migration_cut_model_expected.txt`
- Modify: `tests/MIGRATION_COUNTEREXAMPLES.md`

**Deleted from this task, because upstream already did the work.** `Classification::LoudFail` exists
and is wired end to end, landed with `6967723` ("test: extend the migration counterexample harness for
the acceptance corpus"): the enum member (`tests/migration_counterexample_scenarios.h:97`), its
`to_string` (`tests/migration_counterexample_scenarios.cpp:355`), the classification decision
(`tests/migration_counterexample_runner.cpp:825-826`), the exit contract
(`outcome_is_acceptable`, `:1967-1979`, where `LoudFail` passes **only** for a scenario whose
`expected_v1` is `LoudFail`, feeding `aggregate_exit` at `:1982-1993`), and the baseline comparison
(`:2036-2042`). The spec's noted contradiction — loud-fail scenarios versus an "aggregate exit 0"
gate — is therefore **already reconciled**; do not re-plan it, and do not append the enum member a
second time.

**Interfaces:**
- Produces a scoped selector so the Plan A gate is one pasteable command — this is what is still
  missing (`grep -n '"--group"' tests/migration_counterexample_runner.cpp` returns nothing):

```text
--group message-identity   # the scenarios stages A1-A3 are responsible for
```

The group is exactly: the nine Redis `ScenarioKind::Program` cases, `stale_barrier_object`, and the
new `collective_flag_divergence_is_loud`. It deliberately **excludes**
`future_matching_send_after_cut`, `pending_set_expands_after_park`,
`failed_operation_advances_boundary` and `promotion_before_full_membership`, which are epoch and
control-plane defects owned by Plan C (axes C and D), and the two `Backend::Direct` cases, which need the framed
transport from Plan C.

- [ ] **Step 1: Show the gate does not exist (RED)**

```bash
/home/luca/fmi/build/tests/fmi_migration_counterexamples --group message-identity
```

Expected: exit `1` with the usage banner — `--group` is not a recognised option.

- [ ] **Step 2: Confirm the exit contract, and route `IdentityMismatch` into it**

Most of this step is a **verification**, not a change — see the deleted-work note above. Confirm, do
not rewrite:

- `Classification::LoudFail` is present (`tests/migration_counterexample_scenarios.h:97`) and has a
  `to_string` (`tests/migration_counterexample_scenarios.cpp:355`);
- `outcome_is_acceptable` (`tests/migration_counterexample_runner.cpp:1967-1979`) passes `LoudFail`
  **only** when `scenario->expected_v1 == LoudFail`, and `aggregate_exit` (`:1982-1993`) consumes it;
- `compare_with_baseline`'s `LoudFail` short-circuit is in place (`:2036-2042`), so a loud refusal is
  a terminal observation rather than a payload comparison.

The one genuine change: in the rank executor, catch `FMI::Utils::IdentityMismatch` (from Task 4)
**before** the existing `FMI::Utils::Timeout` handler and record `LoudFail` with the exception text in
`detail`, so the new failure mode reaches the classification path at `:825-826` instead of being
reported as `OperationStuck`.

- [ ] **Step 3: Add the divergent-schedule scenario and the group selector**

Add `collective_flag_divergence_is_loud` to the catalog: `Backend::Redis`, world 2, no migration
request, in which rank 0 issues `reduce(root=0)` with `{associative=false, commutative=false}` and
rank 1 with `{true, true}`. `expected_current_failure = Classification::LoudFail`. This is the
runtime counterpart of the spec's `bcast; barrier` / `barrier; bcast` reordering argument, adapted
to the namespace-separated ClientServer keys where flag divergence — not op reordering — is the
silent path.

Add `--group <name>` to the option parser (rejecting combination with `--case`/`--backend`,
matching the existing mutual-exclusion handling) and wire the membership list above.

- [ ] **Step 4: Give `tests/migration_cut_model.cpp` an explicit disposition**

The spec is right that the explorer can never be green as written: it exits `2` while it reports
counterexamples and `1` when it reports none (`run_explorer`,
`tests/migration_counterexample_runner.cpp:2063-2082`). Disposition, in two parts:

1. **Retarget the model to post-A2 semantics.** The model rule "Redis promotion discards all
   old queues" *was* the counter reset that `9c7e533` and Task 4 removed. Change the ClientServer
   transition to preserve queues across promotion. What survives is the genuinely unfixed set: the
   `Direct` moved-link shapes and the divergent-schedule shapes, which Plan C owns.
2. **Decouple its exit code from the corpus gate.** `--explore` keeps exit `2` as a diagnostic.
   Add `--explore --expect <file>`, which exits `0` when the rendered output matches the file
   byte-for-byte and `2` otherwise. Generate `tests/migration_cut_model_expected.txt` from the
   retargeted model and check it in. The explorer becomes a **change detector** for the abstract
   protocol semantics rather than a pass/fail oracle, which is the only role it can honestly hold
   after Plan A.

- [ ] **Step 5: Verify (GREEN)**

```bash
redis-cli -h 127.0.0.1 -p 6379 flushall
cmake -S /home/luca/fmi -B /home/luca/fmi/build -DFMI_BUILD_TESTS=ON
cmake --build /home/luca/fmi/build -j"$(nproc)"

/home/luca/fmi/build/tests/fmi_migration_counterexamples --list
/home/luca/fmi/build/tests/fmi_migration_counterexamples --group message-identity; echo "group exit=$?"
/home/luca/fmi/build/tests/fmi_migration_p2p_cut_counterexample; echo "p2p exit=$?"

/home/luca/fmi/build/tests/fmi_migration_counterexamples --explore \
  --expect /home/luca/fmi/tests/migration_cut_model_expected.txt; echo "explore exit=$?"
/home/luca/fmi/build/tests/fmi_migration_counterexamples --explore > /tmp/fmi-explore-1.txt
/home/luca/fmi/build/tests/fmi_migration_counterexamples --explore > /tmp/fmi-explore-2.txt
diff -u /tmp/fmi-explore-1.txt /tmp/fmi-explore-2.txt

/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000 &
TCPUNCHD=$!; sleep 1
sed -i 's/192\.168\.0\.166/127.0.0.1/' /home/luca/fmi/config/fmi_test.json
cd /home/luca/fmi/build/tests && ./Boost_Tests_run
git -C /home/luca/fmi checkout -- config/fmi_test.json
kill "$TCPUNCHD"

git -C /home/luca/fmi diff --check
git -C /home/luca/fmi status --short
redis-cli -h 127.0.0.1 -p 6379 dbsize
```

Expected — **this is Plan A's acceptance**:

- `--group message-identity` exits `0`, with `migrated=preserved` on the nine Redis program
  scenarios and on `stale_barrier_object`, and `migrated=loud_fail` on
  `collective_flag_divergence_is_loud`;
- `fmi_migration_p2p_cut_counterexample` exits `0`;
- `--explore --expect …` exits `0` and bare `--explore` is byte-stable;
- `Boost_Tests_run` is fully green, including the new `ClientServerKeys` and `OperationIdentity`
  suites and the two new `Channels` cases from A1;
- `git status --short` still shows the pre-existing unrelated modifications and nothing staged
  beyond this task's files;
- `dbsize` returns to its pre-run value.

- [ ] **Step 6: Update the usage document and commit**

Record in `tests/MIGRATION_COUNTEREXAMPLES.md`: the new `LoudFail` classification and why it is a
pass; the `--group message-identity` gate and exactly which scenarios it excludes and why; the
explorer's new `--expect` mode and its status as a change detector rather than an oracle; and the
`cwd = build/tests` plus `config/fmi_test.json` Direct-host preflight.

```bash
git -C /home/luca/fmi add tests/migration_counterexample_scenarios.h \
  tests/migration_counterexample_scenarios.cpp tests/migration_counterexample_runner.cpp \
  tests/migration_cut_model.cpp tests/migration_cut_model_expected.txt \
  tests/MIGRATION_COUNTEREXAMPLES.md
git -C /home/luca/fmi commit -m "test: add LoudFail, the message-identity gate, and the model's disposition"
```
