# Plan A — Message Identity: Correctness Without Rewrite

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement stages A1, A2 and A3 of
`docs/superpowers/specs/2026-07-27-sequenced-incarnation-links-design.md` — harden TCPunch pairing,
give the ClientServer data plane job-lifetime message identity, and install the operation-identity
seam above the `Channel` interface — so that the migration counterexample corpus flips from silent
substitution to correct delivery or a loud, diagnosable failure, **on the existing epoch protocol**.

**Architecture:** No new architecture, no new threads, no change to the epoch machinery. Three
additive layers, in order: (A1) `extern/TCPunch` pairing becomes per-call state with a joined
listener and a TTL-swept server registry; (A2) `ClientServer` reconfigures **in place** across an
epoch and derives its data-plane keys from a *stable* communicator name plus job-lifetime
per-directed-pair sequences, while the epoch-qualified name is retained **only** for `Direct`
pairing; (A3) an RAII thread-local operation scope, set by `Communicator`'s entry points, publishes
`(lane, op_kind, collective_index, root)` plus the reduction function's commutative/associative
flags, which `ClientServer` folds into its keys and into a validated per-object header. Per the
design spec, contract 1, the `Channel` virtuals do **not** gain a lane parameter.

**Tech Stack:** C++17, CMake, Boost.Test, hiredis/Redis, TCPunch (git submodule, `McLavish` fork),
POSIX sockets/threads/`/proc`.

## Global Constraints

- The design spec (`docs/superpowers/specs/2026-07-27-sequenced-incarnation-links-design.md`) is
  normative. Where this plan and the spec disagree, the spec wins; raise the conflict rather than
  diverging silently.
- Every task ends with the tree building **and** `Boost_Tests_run` passing. No task may leave a
  broken intermediate state.
- One task = one commit (repo commit discipline, `CLAUDE.md`). A1 tasks produce **two** commits: one
  inside the `extern/TCPunch` submodule and one in FMI that bumps the gitlink together with the new
  FMI-side test.
- Stage and commit only files belonging to the current task. The working tree already carries
  unrelated modifications (`runbooks/…/orchestrator.py`, `tests/channels.cpp`,
  `tests/communicator.cpp`, `tests/CMakeLists.txt`, untracked `gate`, `.superpowers/`,
  `tests/forked_rank_guard.h`, `tests/migration_p2p_cut_counterexample.cpp`) — preserve them.
- Do **not** delete, rename, or gate the epoch machinery. `epoch_comm_name`
  (`include/ft/Common.h:55-57`), `ControlPlane`, and `TransparentMigrationRuntime` are Plan C's
  territory. Per the spec's acceptance oracles, no stage may loudly reject ClientServer-under-FT
  while `fmi_migration_p2p_cut_counterexample` remains the headline gate.
- **Cross-plan file ownership** (read this before Tasks 1, 2, 6 and 9; index:
  `docs/superpowers/plans/2026-07-27-migration-v2-README.md`):
  - `tests/channels.cpp` / `tests/communicator.cpp` are **shared with Plan C Task 7**, which routes
    every `get_channel` call through a transport-variant overlay and diffs the case list before and
    after. **Plan A Tasks 1 and 2 land first**; Plan C Task 7 then records the post-A1 case count
    (17 in `Channels`, not 15) as its baseline. The two A1 cases call `pair()` directly and are
    outside the variant loop. Do not restructure the suite beyond adding the two cases.
  - `include/ft/ControlPlane.h` / `src/ft/ControlPlane.cpp` (Task 6) are the **one exception** to
    the spec's staging-map claim that A2/A3 and B touch disjoint file sets. The edit is additive
    (a defaulted parameter on `clear_job_state`); Plan B is forbidden from touching those files, so
    there is no A↔B collision. Plan C stage C3 rewrites `ControlPlane` and must carry this change
    forward — it is listed in that stage's port list.
  - Plan A **solely owns** `Classification::LoudFail`, `aggregate_exit`, the `--group` selector and
    the `tests/migration_cut_model.cpp` disposition (Task 9). Plan C stage C4 re-verifies them
    against the incarnation protocol but must not re-implement them.
  - `CMakeLists.txt` and `tests/CMakeLists.txt` are appended to by all three plans (`FMI_SOURCES`
    near `CMakeLists.txt:42-51`, `FMI_TEST_SOURCES` at `tests/CMakeLists.txt:6`). Expect trivial
    textual conflicts; resolve by union, never by overwrite.
- Both counterexample binaries only exist under `-DFMI_ENABLE_CRIU=ON`
  (`tests/CMakeLists.txt`, two `if(FMI_ENABLE_CRIU)` blocks). Every verification recipe passes it.
- Build once per task with:
  `cmake -S /home/luca/fmi -B /home/luca/fmi/build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON`
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
- **Coverage warning that shapes every A2/A3 task.** The `backends` map at `tests/channels.cpp:62-66`
  (already so in `HEAD`) has `S3` and `Redis` commented out at `:63-64`, so the `Channels` suite
  exercises `Direct` only; and `config/fmi_test.json` disables Redis, so the `Communicator` suite does too.
  **The ClientServer collectives this plan rewrites have zero coverage in `Boost_Tests_run` today.**
  Every A2/A3 task must therefore add its own explicitly Redis-backed Boost case; "Boost_Tests_run
  stays green" is a no-regression gate, not evidence that the new code works.
- New Boost test translation units must **not** define `BOOST_TEST_MODULE` — `tests/communicator.cpp:1`
  already owns it. New files are added to `FMI_TEST_SOURCES` in `tests/CMakeLists.txt:6`.
- Redis-touching Boost cases must be guarded with `#if FMI_ENABLE_REDIS` and must clean up their own
  keys, because `Redis::get_object_names` (`src/comm/Redis.cpp:85-94`) issues an **unscoped
  `KEYS *`** over the whole database.

---

### Task 1: TCPunch — per-call pairing state with a joined listener

**Files:**
- Modify: `extern/TCPunch/client/tcpunch.cpp`
- Modify: `tests/channels.cpp`
- Modify: `tests/CMakeLists.txt` (only if a new source file is added; prefer extending `channels.cpp`)

**Interfaces:**
- `pair()`'s public signature in `extern/TCPunch/client/tcpunch.h:14` is **unchanged**:

```cpp
int pair(const std::string& pairing_name, const std::string& server_address,
         int port = 10000, int timeout_ms = 0);
```

- Produces, file-local to `tcpunch.cpp` (replacing the file-scope globals at
  `extern/TCPunch/client/tcpunch.cpp:22-23`):

```cpp
namespace {
    struct PairingAttempt {
        PeerConnectionData  local;                  // bound port handed to the listener
        std::atomic<int>    accepted_socket{-1};    // set by the listener on success
        std::atomic<int>    listen_socket{-1};      // published so pair() can cancel accept()
        std::atomic<bool>   established{false};
        std::atomic<bool>   cancelled{false};
    };

    void* peer_listen(void* attempt);               // takes PairingAttempt*, never a global
}
```

- [ ] **Step 1: Reproduce the leak (RED)**

Add `Channels/direct_pair_timeout_is_clean` to `tests/channels.cpp`. It runs **before** any other
Direct case touches the rendezvous server, with `tcpunchd` deliberately **not** started, and:

1. records the baseline thread count (entries in `/proc/self/task`) and the baseline count of
   listening fds (for every entry in `/proc/self/fd`, `getsockopt(fd, SOL_SOCKET, SO_ACCEPTCONN)`
   returning 1);
2. calls `pair("leak-probe-<pid>-<n>", "127.0.0.1", 10000, 300)` inside
   `BOOST_CHECK_THROW(..., Timeout)` — catching `Timeout` *and* `std::string`, since
   `extern/TCPunch/common/utils.h` throws a bare `std::string` from `error_exit`;
3. sleeps 500 ms and re-reads both counts;
4. `BOOST_CHECK_EQUAL`s them against the baseline.

Build and run:

```bash
cmake -S /home/luca/fmi -B /home/luca/fmi/build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON
cmake --build /home/luca/fmi/build --target Boost_Tests_run -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=Channels/direct_pair_timeout_is_clean
```

Expected: the case **fails** on the thread count and/or the listening-fd count. That is the leak
described in the spec's staging table for A1: the listener created at
`extern/TCPunch/client/tcpunch.cpp:147-151` is never joined on the `Timeout` throw at
`:199-202`.

- [ ] **Step 2: Replace the globals with per-call state**

In `extern/TCPunch/client/tcpunch.cpp`:

- Delete `connection_established` and `accepting_socket` (`:22-23`) and their reset at `:89-90`.
- Stack-allocate one `PairingAttempt` per `pair()` call; pass `&attempt` to `pthread_create`
  (`:148`). `peer_listen` publishes its `listen_socket` into the attempt **before** entering
  `accept()`, and writes `accepted_socket` + `established` on success.
- `peer_listen`'s `accept()` loop (`:55-71`) exits when `attempt->cancelled` is set or `accept()`
  fails with `EBADF`/`EINVAL`; it closes `listen_socket` on **every** exit path, not only on
  success.
- `peer_listen` must not call `error_exit_errno` (`:31`, `:36`, `:45`, `:49`) — throwing from a
  `pthread` entry point calls `std::terminate`. Record the failure in the attempt, close what it
  owns, and return.

- [ ] **Step 3: Make every exit path join**

Introduce a scope guard next to the existing `ScopedFd` (`:79-86`) that, on **any** unwind out of
`pair()`:

1. sets `attempt.cancelled`;
2. `shutdown(listen_socket, SHUT_RDWR)` then `close()`s it, which wakes a blocked `accept()`;
3. `pthread_join`s the listener;
4. closes whichever of `{peer_socket, accepted_socket}` is **not** returned.

Then:
- The timeout path (`:198-202`) throws through the guard instead of leaking.
- The connect-wins path (`:216-222`) falls into the guard's join instead of the current
  `if(connection_established.load())`-only join at `:224`.
- `:226`'s `peer_socket = accepting_socket.load()` reads `attempt.accepted_socket`, which cannot
  belong to a different pairing name because the attempt is per call.
- Both sockets exist on the race path; close the loser explicitly and return the winner.

- [ ] **Step 4: Verify (GREEN)**

```bash
cmake --build /home/luca/fmi/build --target Boost_Tests_run -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=Channels/direct_pair_timeout_is_clean

/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000 &
TCPUNCHD=$!
sleep 1
sed -i 's/192\.168\.0\.166/127.0.0.1/' /home/luca/fmi/config/fmi_test.json
cd /home/luca/fmi/build/tests && ./Boost_Tests_run
git -C /home/luca/fmi checkout -- config/fmi_test.json
kill "$TCPUNCHD"
```

Expected: the new case passes (thread count and listening-fd count both return to baseline after a
timed-out `pair()`); the full suite passes; `ss -ltnp | grep Boost_Tests_run` is empty after the
run.

- [ ] **Step 5: Commit (two commits)**

```bash
git -C /home/luca/fmi/extern/TCPunch add client/tcpunch.cpp
git -C /home/luca/fmi/extern/TCPunch commit -m "client: per-call pairing state; always join the listener"

git -C /home/luca/fmi add extern/TCPunch tests/channels.cpp
git -C /home/luca/fmi commit -m "fix: no thread or listening-socket leak on a timed-out pair()"
```

---

### Task 2: tcpunchd — idle-TTL sweep of the pairing registry

**Files:**
- Modify: `extern/TCPunch/server/hole_punching_server.cpp`
- Modify: `tests/channels.cpp`

**Interfaces:**
- Produces, in `hole_punching_server.cpp` (extending the `ConnectionData` at `:17-20`):

```cpp
typedef struct {
    int                                   socket;
    struct sockaddr_in                    client_info;
    std::chrono::steady_clock::time_point registered_at;
} ConnectionData;

// Drops registry entries whose socket is at EOF or whose age exceeds idle_ttl.
static void sweep_registry(std::map<std::string, ConnectionData>& clients,
                           std::chrono::seconds idle_ttl);
```

- New optional second argument: `tcpunchd <port> [idle_ttl_seconds]`, default 120.

- [ ] **Step 1: Reproduce the stale-entry hazard (RED)**

Add `Channels/direct_concurrent_pairings_one_timeout` to `tests/channels.cpp`, guarded by a probe
that `connect()`s to `127.0.0.1:10000` and returns `BOOST_TEST_MESSAGE` + early return when the
rendezvous server is absent. The case starts `N = 4` in-process threads:

- three pairs of threads pair successfully under distinct names (`concurrent-<pid>-<k>`);
- one thread pairs under a name whose partner never appears, with `timeout_ms = 500`, and must
  observe `Timeout` within ~1 s;
- afterwards, a **fresh** `pair()` under that same abandoned name, with a real partner, must succeed
  within its own timeout.

Run against a `tcpunchd` that has been fed the abandoned name:

```bash
/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000 &
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=Channels/direct_concurrent_pairings_one_timeout
```

Expected: the re-pair leg fails or hangs to its timeout, because
`hole_punching_server.cpp:112-176` only reclaims a registry entry when a **second client registers
under the same name**; an abandoned entry otherwise lives for the process lifetime.

- [ ] **Step 2: Add the sweep**

- Stamp `registered_at` at both insertion sites (`:125-128`, `:149-152`, `:172-175`).
- Implement `sweep_registry`: for each entry, `recv(fd, &probe, 1, MSG_DONTWAIT | MSG_PEEK)` — the
  same liveness probe already used at `:119-120` — and drop it when the peek returns `0`, or when
  `now - registered_at > idle_ttl`. Close the fd on every drop.
- Call `sweep_registry` once per `accept()` iteration, immediately after `accept()` returns
  (`:74`). The server is single-threaded and the registry is small; no timer thread is needed and
  none may be added.
- Parse the optional TTL argument alongside the existing port parsing (`:27-38`).

- [ ] **Step 3: Verify (GREEN)**

```bash
cmake --build /home/luca/fmi/extern/TCPunch/server/build-fresh -j"$(nproc)"
pkill -f 'tcpunchd 10000' || true
/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000 5 &
TCPUNCHD=$!
sleep 1
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=Channels/direct_concurrent_pairings_one_timeout
sed -i 's/192\.168\.0\.166/127.0.0.1/' /home/luca/fmi/config/fmi_test.json
cd /home/luca/fmi/build/tests && ./Boost_Tests_run
git -C /home/luca/fmi checkout -- config/fmi_test.json
kill "$TCPUNCHD"
```

Expected: the concurrency case passes with a 5 s TTL; the three successful pairings are unaffected
by the one that times out; the full suite passes.

- [ ] **Step 4: Commit (two commits)**

```bash
git -C /home/luca/fmi/extern/TCPunch add server/hole_punching_server.cpp
git -C /home/luca/fmi/extern/TCPunch commit -m "server: sweep dead and idle pairing registrations"

git -C /home/luca/fmi add extern/TCPunch tests/channels.cpp
git -C /home/luca/fmi commit -m "test: concurrent pairings survive one timed-out partner"
```

---

### Task 3: ClientServer reconfigures in place; data keys leave the epoch

**Files:**
- Modify: `include/comm/Channel.h`
- Modify: `include/comm/ClientServer.h`
- Modify: `src/comm/ClientServer.cpp`
- Modify: `src/Communicator.cpp`
- Create: `tests/clientserver_keys.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**
- Produces, on `Channel` (`include/comm/Channel.h`, alongside `set_comm_name` at `:100`):

```cpp
//! Job-lifetime communicator name. Never epoch-qualified. Backends key their *data plane* on
//! this; only Direct's TCPunch pairing names use the epoch-qualified comm_name.
void set_data_comm_name(std::string name) { data_comm_name = std::move(name); }

protected:
    std::string data_comm_name;
```

- Produces, on `ClientServer` (`include/comm/ClientServer.h`, replacing the default at
  `include/comm/Channel.h:118-123` for this family only):

```cpp
//! Adopt the epoch-qualified name for diagnostics only. Data-plane keys and the job-lifetime
//! sequence counters are NOT reset: per the design spec, contract 1, ClientServer keys are
//! never epoch-qualified. Always returns true, so Communicator::reconfigure_to_epoch keeps this
//! channel object instead of finalizing and rebuilding it.
bool reconfigure_for_epoch(const std::string& new_comm_name,
                           const std::vector<FMI::Utils::peer_num>& moved_ranks) override;
```

- `Communicator::register_channel` (`src/Communicator.cpp:125-130`) gains one line:
  `c->set_data_comm_name(data_comm_name);` where `data_comm_name` is the **un-qualified** name
  passed to the constructor. `src/Communicator.cpp:66` keeps assigning
  `FMI::FT::epoch_comm_name(comm_name, active_epoch)` to `this->comm_name`; store the raw
  `comm_name` in a new `data_comm_name` member alongside it. `reconfigure_to_epoch`
  (`:97-123`) leaves `data_comm_name` untouched.

- [ ] **Step 1: Prove the counter reset (RED)**

Create `tests/clientserver_keys.cpp` with suite `ClientServerKeys`, and add
`clientserver_keys.cpp` to `FMI_TEST_SOURCES` (`tests/CMakeLists.txt:6`). First case,
`survives_epoch_reconfigure`, guarded by `#if FMI_ENABLE_REDIS`:

- build a `Redis` channel via `FMI::Comm::Channel::get_channel("Redis", …)` using the same param
  maps as `tests/channels.cpp:30-45` (`redis_test_params` `:30-35`, `redis_test_model_params`
  `:37-45`), with `peer_id=0`, `num_peers=2`,
  `comm_name = "cskeys-" + pid + "-" + timestamp`;
- `send` two 4-byte payloads to peer 1 without any matching `recv`;
- call `reconfigure_for_epoch(epoch_comm_name(base, 1), {1})`;
- `send` a third payload;
- assert (over `get_object_names()` filtered by the test's own prefix) that **three** distinct
  objects exist and that no two share a key.

Expected today: the case cannot even be written against `reconfigure_for_epoch` returning `false`
without a rebuild — so assert instead against the current behaviour and watch the third `send`
collide with the first. Record the observed collision in the failure message.

- [ ] **Step 2: Split the two names**

- Add `data_comm_name` to `Channel` (protected) with its setter, defaulting to `comm_name` when
  never set, so custom channels and the non-FT path are unaffected.
- Add `std::string data_comm_name;` to `Communicator`'s private members
  (`include/Communicator.h:184-193`) and set it from the constructor argument in **both** branches
  of `src/Communicator.cpp:42-80`.
- `register_channel` propagates it (`src/Communicator.cpp:125-130`).
- Rewrite every key construction in `src/comm/ClientServer.cpp` to use `data_comm_name` instead of
  `comm_name`: `:14`, `:28`, `:35`, `:47`, `:109`, `:133`, `:141`, `:160`. Do **not** change the
  key *grammar* yet — that is Task 4.

- [ ] **Step 3: Reconfigure in place**

Implement `ClientServer::reconfigure_for_epoch` to store the new epoch-qualified name in
`comm_name` and `return true`. Do not touch `num_operations` (`include/comm/ClientServer.h:62-67`),
`created_objects` (`:69`), or call `finalize()`. `src/Communicator.cpp:105-112` then keeps the
channel instead of finalizing and rebuilding it, which is the mechanism the spec identifies as the
proven silent-substitution path.

Note the consequence and state it in a comment: `finalize()` (`src/comm/ClientServer.cpp:67-71`)
is now called only at communicator destruction, so barrier and collective objects are retained for
the whole job — which is what the spec's contract-1 barrier clause requires.

- [ ] **Step 4: Verify (GREEN)**

```bash
redis-cli -h 127.0.0.1 -p 6379 ping
cmake -S /home/luca/fmi -B /home/luca/fmi/build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON
cmake --build /home/luca/fmi/build -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=ClientServerKeys
/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000 &
TCPUNCHD=$!; sleep 1
sed -i 's/192\.168\.0\.166/127.0.0.1/' /home/luca/fmi/config/fmi_test.json
cd /home/luca/fmi/build/tests && ./Boost_Tests_run
git -C /home/luca/fmi checkout -- config/fmi_test.json
kill "$TCPUNCHD"
```

Expected: `ClientServerKeys/survives_epoch_reconfigure` passes with three distinct keys; the full
suite passes; `redis-cli --scan --pattern 'cskeys-*' | wc -l` is `0` after the run.

- [ ] **Step 5: Commit**

```bash
git -C /home/luca/fmi add include/comm/Channel.h include/comm/ClientServer.h \
  src/comm/ClientServer.cpp include/Communicator.h src/Communicator.cpp \
  tests/clientserver_keys.cpp tests/CMakeLists.txt
git -C /home/luca/fmi commit -m "fix: ClientServer reconfigures in place with job-lifetime data keys"
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
  `include/comm/ClientServer.h:62-67` for the p2p lane):

```cpp
//! Job-lifetime, per-directed-pair message ordinals. Never reset — not by epoch
//! reconfiguration, not by finalize. Key: (src, dst).
std::map<std::pair<FMI::Utils::peer_num, FMI::Utils::peer_num>, std::uint64_t> p2p_send_seq;
std::map<std::pair<FMI::Utils::peer_num, FMI::Utils::peer_num>, std::uint64_t> p2p_recv_seq;
```

- Produces, changing the `download_object` contract (`include/comm/ClientServer.h:39`):

```cpp
//! Returns true only when an object of EXACTLY buf.len bytes was fetched into buf. An object
//! that exists with a different length is a protocol violation: throw, never truncate.
virtual bool download_object(channel_data buf, std::string name) = 0;

//! Create-if-absent. If the key already exists with an identical length and digest, this is a
//! retried upload and must succeed silently; a different length or digest throws.
virtual void upload_object(channel_data buf, std::string name) = 0;
```

- Produces the p2p key grammar (`<data>` is `data_comm_name`):

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

In `src/comm/ClientServer.cpp`, replace `send` (`:6-18`) and `recv` (`:20-32`):

- key from `p2p_send_seq[{peer_id, dest}]++` / `p2p_recv_seq[{src, peer_id}]++` in the grammar
  above;
- `recv` is **consumer-delete** — p2p is a single-consumer namespace per the spec's contract 1 —
  and follows the checkpoint-consistent discipline: repeatable `download_object` (`GET`) → commit
  the bytes into `buf` → idempotent `delete_object` (`DEL`). **Never `GETDEL`**; a freeze between
  the server-side delete and the process-side copy destroys the only copy.
- `send` no longer pushes the key into `created_objects` (`:88`) for the p2p lane, since the
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

- Produces the collective key grammar:

```text
<data>:gather:<n>:<contributor>
<data>:scatter:<n>:<consumer>
<data>:bcast:<n>:<root>:for:<consumer>
<data>:reduce:<n>:<contributor>
<data>:scan:<n>:<producer>:for:<consumer>
```

`<n>` is the existing per-namespace ordinal from `num_operations`
(`include/comm/ClientServer.h:62-67`), extended with `gather` and `scatter` entries. Task 8
replaces `<n>` with the Communicator-scoped collective index.

- [ ] **Step 1: Reproduce the base-class counter leak and the scan OOB (RED)**

Add to `tests/clientserver_keys.cpp`:

- `gather_does_not_consume_p2p_slots`: on a Redis channel with `num_peers=3`, rank 0 issues
  `send(payload_A, 0)`-style application traffic from rank 1 to rank 0, and separately a
  `gather(root=0)`. Assert that rank 0's application `recv` returns `payload_A` and the gathered
  slice returns the gather contribution. Today `src/comm/Channel.cpp:34` and `:42` call the same
  `send`/`recv` virtuals as the application, so the two streams share
  `p2p_send_seq[{1,0}]` and cross over.
- `scan_reads_only_its_own_contributions`: run `scan` with `num_peers=4` from `peer_id=1` under
  ASan (or with an explicit bounds assertion). `src/comm/ClientServer.cpp:167` loops
  `for (int i = 0; i < num_peers; i++)` while `received` and `applied` are sized `peer_id + 1`
  (`:145-147`) — an out-of-bounds read on every peer with `peer_id + 1 < num_peers`.

- [ ] **Step 2: Give gather and scatter their own namespace**

Implement `ClientServer::gather` / `ClientServer::scatter` mirroring the shape of
`src/comm/Channel.cpp:32-62` but uploading/downloading under the `gather`/`scatter` keys and using
`upload`/`download` directly instead of `send`/`recv`. Both are single-consumer namespaces per the
spec's contract 1, so the consumer deletes: the root deletes each `…:gather:<n>:<i>` after
committing it; each non-root consumer deletes its own `…:scatter:<n>:<j>`.

- [ ] **Step 3: Per-consumer bcast and scan keys; consumer-delete for reduce**

- `bcast` (`:34-42`): the root uploads `N-1` per-consumer objects `…:bcast:<n>:<root>:for:<j>`;
  each consumer downloads and deletes only its own. This replaces the single shared object, which
  no consumer can safely delete.
- `scan` (`:139-183`): peer `i` uploads one object per downstream consumer,
  `…:scan:<n>:<i>:for:<j>`, for `j > i`; each consumer deletes only its own copies. Fix the loop
  bound at `:167` to `num_data` and hoist `num_operations["scan"]++` above the early `throw` at
  `:179-181` so a timed-out scan does not desynchronize the ordinal against peers that succeeded.
- `reduce` (`:92-137`): contributions `…:reduce:<n>:<i>` are single-consumer (the root); the root
  deletes each after committing it. Move `num_operations["reduce"]++` (`:128`) above the timeout
  throw at `:129-131` for the same reason.

- [ ] **Step 4: Verify (GREEN)**

```bash
cmake --build /home/luca/fmi/build -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=ClientServerKeys
```

Then a targeted collectives sweep against Redis. Temporarily (uncommitted) re-enable Redis in the
`backends` map at `tests/channels.cpp:62-66` (uncomment `:64`), rebuild, and run:

```bash
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=Channels
git -C /home/luca/fmi checkout -- tests/channels.cpp   # only if the re-enable was temporary
```

Expected: `ClientServerKeys` green including the two new cases; the whole `Channels` suite green
with Redis enabled — that is the first real coverage the ClientServer collectives have had. Then
the full suite per the preflight.

- [ ] **Step 5: Commit**

```bash
git -C /home/luca/fmi add include/comm/ClientServer.h src/comm/ClientServer.cpp \
  tests/clientserver_keys.cpp
git -C /home/luca/fmi commit -m "fix: per-namespace ClientServer collectives with consumer-delete"
```

---

### Task 6: Barrier rework and job-state cleanup of the data namespaces

**Files:**
- Modify: `src/comm/ClientServer.cpp`
- Modify: `include/comm/ClientServer.h`
- Modify: `src/comm/Redis.cpp`
- Modify: `include/comm/Redis.h`
- Modify: `src/comm/S3.cpp`
- Modify: `include/ft/ControlPlane.h`
- Modify: `src/ft/ControlPlane.cpp`
- Modify: `tests/clientserver_keys.cpp`

**Interfaces:**
- Produces, replacing the suffix scan (`include/comm/ClientServer.h:48`):

```cpp
//! List objects whose key starts with `prefix`. Prefix scoping is mandatory: the barrier
//! arrival set must never observe keys from another communicator or another epoch.
virtual std::vector<std::string> list_objects(const std::string& prefix) = 0;
```

`get_object_names()` becomes `list_objects("")` and is retained only for `finalize`.

- Produces the barrier key grammar and arrival rule:

```text
<data>:barrier:<n>:<rank>
```

Arrival is the **deduplicated set of `<rank>` values** parsed out of the listed keys, never a raw
count. Keys are retained until job cleanup.

- Produces, on `ControlPlane` (`include/ft/ControlPlane.h:48`):

```cpp
//! Clears the control-plane prefix AND the data-plane namespaces of `data_comm_name`, so a
//! reused communicator name cannot inherit a previous job's messages.
void clear_job_state(const std::string& data_comm_name = "");
```

- [ ] **Step 1: Reproduce the cross-scope barrier match (RED)**

Add `barrier_ignores_foreign_keys` to `tests/clientserver_keys.cpp`: with `num_peers = 2`,
pre-seed Redis with a key ending in `_barrier_0` under a **different** communicator name, then run
`barrier()` from a single rank. Assert it throws `Utils::Timeout` (its partner never arrived).

Expected today: it returns immediately. `src/comm/ClientServer.cpp:54-55` matches on the
**suffix** `_barrier_<n>` only, and `Redis::get_object_names` (`src/comm/Redis.cpp:85-94`) lists
the whole database with `KEYS *`. This is precisely the `stale_barrier_object` corpus mechanism: a
target that exits at epoch 0 without finalizing leaves `…0_barrier_0`, and the survivor's epoch-1
barrier 0 counts it as an arrival and returns before the replacement exists.

- [ ] **Step 2: Add prefix-scoped listing**

- `Redis::list_objects(prefix)`: `SCAN 0 MATCH <prefix>* COUNT 512`, looping on the returned
  cursor. Free every reply. **`SCAN` may return the same key more than once** — that is why the
  arrival rule must be a set, not a count. (`KEYS` did not have this property; the dedup
  requirement is a consequence of moving to `SCAN`, which prefix scoping requires.)
- `S3::list_objects(prefix)`: `ListObjectsRequest().WithPrefix(prefix)`, honouring
  `IsTruncated`/`NextMarker` — the current `S3::get_object_names` (`src/comm/S3.cpp:79-92`)
  silently stops at the first 1000 keys.

- [ ] **Step 3: Rewrite barrier**

Replace `src/comm/ClientServer.cpp:44-65`:

```text
upload  <data>:barrier:<n>:<peer_id>          (write-once, retry-safe, from Task 4)
poll    list_objects("<data>:barrier:<n>:")
        parse the trailing <rank>, insert into std::set<peer_num>
        return when set.size() == num_peers
        throw Utils::Timeout() at max_timeout
```

Increment `num_operations["barrier"]` **before** the poll loop so a timed-out barrier does not
reuse `<n>`. Do not delete barrier keys on return — deleting after one rank's pass strands slower
ranks, per the spec's contract 1.

**Also in this step: record the deadline-suspension seam.** The spec's contract 3 assigns this to
Plan A explicitly — "ClientServer's four poll loops also expire at `max_timeout` with no suspension
hook; Plan A records the hook, Plan C wires it." Add to `include/comm/ClientServer.h`, next to
`timeout`/`max_timeout`:

```cpp
//! Deadline-suspension seam (design spec, contract 3, "Deadline suspension"). Plan A only
//! RECORDS this hook; Plan C stage C3 replaces the default with per-link suspension keyed on
//! link state, so a poll loop whose peer is mid-migration does not expire at max_timeout.
//! Returning true means "this wait does not count against the deadline".
//! The four call sites are the poll loops at src/comm/ClientServer.cpp:52 (barrier), :75
//! (download), :103 (reduce) and :154 (scan).
virtual bool deadline_suspended() const { return false; }
```

Route all four loops through it — `if (!deadline_suspended()) { elapsed_time += timeout; }` — so the
default (`false`) is byte-for-byte today's behaviour and C3 has exactly one override to write. Add
`ClientServerKeys/deadline_seam_defaults_to_unsuspended`: a subclass returning `true` makes a
`barrier()` with an absent partner loop past `max_timeout` (bound it with a test-side cancel), while
the base class still throws `Utils::Timeout`.

- [ ] **Step 4: Extend job cleanup to the data plane**

`ControlPlane::clear_job_state` (`src/ft/ControlPlane.cpp:424-440`) currently deletes only the
`fmi:ft:<comm>:` control-plane keys. Add, when `data_comm_name` is non-empty, a `SCAN`-based sweep
of `<data_comm_name>:*`, matching the existing `clear_criu_state` shape (`:443-457`). Update the
call sites that already exist in the corpus and in `tests/fault_tolerance.cpp:41,81,89,101` to pass
the data name where they know it; the defaulted parameter keeps every other call compiling.

- [ ] **Step 5: Verify (GREEN)**

```bash
redis-cli -h 127.0.0.1 -p 6379 flushall
cmake --build /home/luca/fmi/build -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=ClientServerKeys

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
returns to its pre-run value. Then the full Boost suite per the preflight.

Note deliberately: `--backend redis` **still exits `2`** at this point, because
`future_matching_send_after_cut`, `pending_set_expands_after_park`,
`failed_operation_advances_boundary` and `promotion_before_full_membership` are epoch/control-plane
defects that Plan A does not fix (Plan C stage C3 owns them). Task 9 introduces the scoped selector
that makes the Plan A gate a single pasteable command.

- [ ] **Step 6: Commit**

```bash
git -C /home/luca/fmi add include/comm/ClientServer.h src/comm/ClientServer.cpp \
  include/comm/Redis.h src/comm/Redis.cpp src/comm/S3.cpp \
  include/ft/ControlPlane.h src/ft/ControlPlane.cpp tests/clientserver_keys.cpp
git -C /home/luca/fmi commit -m "fix: prefix-scoped deduplicated barrier and data-plane job cleanup"
```

---

### Task 7: The operation identity scope

**Files:**
- Create: `include/utils/OperationIdentity.h`
- Create: `src/utils/OperationIdentity.cpp`
- Modify: `include/Communicator.h`
- Modify: `include/comm/Channel.h`
- Modify: `CMakeLists.txt`
- Create: `tests/operation_identity.cpp`
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
    //! Thread-local, not process-global: tests/communicator.cpp:21-31 runs several ranks as
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

- `Communicator::OperationGuard` (`include/Communicator.h:217-233`) gains an `OperationIdentity`
  parameter and owns an `OperationScope` member. Every entry point supplies its own identity:
  `send` (`:32`), `recv` (`:41`), `bcast` (`:50`), `barrier` (`:58`), `gather` (`:70`),
  `scatter` (`:84`), `reduce` (`:99`), `allreduce` (`:124`), `scan` (`:149`).
- `Communicator` gains `std::uint64_t collective_index_ = 0;`, incremented by the guard for every
  `Lane::Collective` operation and **not** by `send`/`recv`. Per the spec, the counter lives on the
  `Communicator`, not the channel: with a per-channel counter, `bcast → Redis; barrier → Direct`
  yields collective count 0 on both ranks in either issue order and detects nothing.
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
  pairing is lazy in `check_socket`, `:80-104`);
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
(`include/Communicator.h:103`) and `allreduce` (`:128`) already compute
`left_to_right = !(f.commutative && f.associative)`; pass `f.associative` / `f.commutative` into
the identity rather than the derived boolean, since the spec puts the **flags** in the envelope and
validates them per collective. `scan` (`:148-163`) must publish them too even though it does not
currently compute `left_to_right` at the Communicator level.

- [ ] **Step 4: Verify (GREEN)**

```bash
cmake -S /home/luca/fmi -B /home/luca/fmi/build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON
cmake --build /home/luca/fmi/build -j"$(nproc)"
cd /home/luca/fmi/build/tests && ./Boost_Tests_run --run_test=OperationIdentity
/home/luca/fmi/extern/TCPunch/server/build-fresh/tcpunchd 10000 &
TCPUNCHD=$!; sleep 1
sed -i 's/192\.168\.0\.166/127.0.0.1/' /home/luca/fmi/config/fmi_test.json
cd /home/luca/fmi/build/tests && ./Boost_Tests_run
git -C /home/luca/fmi checkout -- config/fmi_test.json
kill "$TCPUNCHD"
```

Expected: `OperationIdentity` green (it needs no Redis and no rendezvous server); full suite green;
no behaviour change anywhere, because nothing reads the scope yet.

- [ ] **Step 5: Commit**

```bash
git -C /home/luca/fmi add include/utils/OperationIdentity.h src/utils/OperationIdentity.cpp \
  include/Communicator.h include/comm/Channel.h CMakeLists.txt \
  tests/operation_identity.cpp tests/CMakeLists.txt
git -C /home/luca/fmi commit -m "feat: RAII operation identity scope above the channel interface"
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
(`include/comm/ClientServer.h:62-67`) is deleted; the `Communicator` is now the sole source of the
collective ordinal.

- [ ] **Step 1: Reproduce the silent flag divergence (RED)**

Add `ClientServerKeys/reduce_flag_divergence_is_loud`: on a Redis channel with `num_peers = 2`,
run `reduce(root=0)` where rank 0 supplies `raw_function{f, associative=false, commutative=false}`
and rank 1 supplies `{f, true, true}`, with an `f` whose result is order-dependent. Assert
`Utils::IdentityMismatch`.

Expected today: it **completes successfully with a silently order-dependent result**. Both branches
of `src/comm/ClientServer.cpp:92-137` upload contributions under the identical key
`…_reduce_<n>`; only the root's apply order (`:117`, via `left_to_right` at `:94`) differs, so a
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

- `ClientServer::upload` (`:87-90`) builds a staging buffer of
  `sizeof(ObjectHeader) + buf.len`, fills the header from the active scope, and hands the whole
  thing to `upload_object`.
- `ClientServer::download` (`:73-85`) fetches into a staging buffer of
  `sizeof(ObjectHeader) + buf.len` — this supersedes the Task 4 exact-length check, which now
  compares against the header-inclusive size — validates `wire_version`, `lane`, `op_kind`,
  `root`, `collective_index`, `total_length` and both flags against the active scope, throws
  `Utils::IdentityMismatch` on any disagreement, and only then commits the payload into `buf`.
  Commit-then-delete ordering from Task 4 is preserved.

**Also in this step: the job-wide policy fingerprint (startup assertion).** The spec's contract 1,
"Policy uniformity", requires `hint`, `faas_price`, the model-parameter hash, `num_peers` and
`wire_version` to be validated **once, job-wide** — "handshake fingerprint under Axis C; **startup
assertion before that**". Plan A owns the *before that* half; Plan C's `HandshakeState.fingerprint`
consumes the same value later. Implement:

```cpp
//! FNV-1a over: wire_version, num_peers, the Hint, faas_price (bit pattern), and the
//! canonicalized model-parameter map — the values that must be identical across ranks or the
//! job is ill-formed (design spec, contract 1, "Policy uniformity"). Computed once at
//! construction; Plan C carries the same u64 in the link handshake.
std::uint64_t FMI::Communicator::policy_fingerprint() const;
```

Publish it once per rank from the `Communicator` constructor via the active ClientServer channel
under `<data>:fingerprint:<rank>` (write-once, retry-safe, from Task 4), and validate it lazily on
the first collective: list `<data>:fingerprint:`, and throw `Utils::IdentityMismatch` naming both
ranks and both fingerprints on any disagreement. Do **not** block waiting for absent ranks — this is
an assertion over ranks that have arrived, not a barrier. Under `Direct`-only configs the assertion
is inert; that is expected and is exactly the hole Plan C's handshake closes.

- [ ] **Step 4: Make algorithm selection read the same flags**

The channels currently recompute `left_to_right` locally from `raw_function`, which can disagree
with the published identity: `src/comm/PeerToPeer.cpp:36`, `:87`, `:133`, and
`src/comm/ClientServer.cpp:94`, `:144`. Replace all five with
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
  tests/operation_identity.cpp tests/clientserver_keys.cpp
git -C /home/luca/fmi commit -m "feat: identity-qualified ClientServer keys with a validated header"
```

---

### Task 9: LoudFail classification, the Plan A gate, and the model's disposition

**Files:**
- Modify: `tests/migration_counterexample_scenarios.h`
- Modify: `tests/migration_counterexample_scenarios.cpp`
- Modify: `tests/migration_counterexample_runner.cpp`
- Modify: `tests/migration_cut_model.cpp`
- Create: `tests/migration_cut_model_expected.txt`
- Modify: `tests/MIGRATION_COUNTEREXAMPLES.md`

**Interfaces:**
- Produces, extending `Classification`
  (`tests/migration_counterexample_scenarios.h:22-33`) — **appended last** so the pipe encoding's
  range check at `tests/migration_counterexample_runner.cpp:1504` stays valid after its bound is
  updated:

```cpp
enum class Classification {
    Preserved, WrongPayload, LostMessage, ReorderedOrDuplicated, PartialPayload,
    PrematureCollective, PromotionStuck, OperationStuck, SetupError, InfrastructureSkip,
    //! The protocol refused the operation with a bounded, diagnosable error
    //! (FMI::Utils::IdentityMismatch) instead of delivering the wrong bytes. A PASS.
    LoudFail
};
```

- Produces the reconciliation of the spec's noted contradiction — the corpus specifies loud-fail
  scenarios *and* an "aggregate exit 0" gate, but `aggregate_exit`
  (`tests/migration_counterexample_runner.cpp:1825-1837`) passes only `Preserved` and
  `InfrastructureSkip`:

```cpp
// LoudFail joins Preserved and InfrastructureSkip as a passing outcome.
int aggregate_exit(const std::vector<Classification>& classifications);
```

- Produces a scoped selector so the Plan A gate is one pasteable command:

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

- [ ] **Step 2: Add LoudFail and reconcile the exit contract**

- Append `LoudFail` to the enum and to `to_string`
  (`tests/migration_counterexample_scenarios.cpp:221-241`).
- Update the pipe-decoder bound at `tests/migration_counterexample_runner.cpp:1504`.
- Update `aggregate_exit` (`:1825-1837`) so `LoudFail` does not set `semantic_counterexample`.
- In `compare_with_baseline` (`:1576-1610`), when the migrated child reported `LoudFail` do **not**
  overwrite it from `scenario.expected_current_failure` (`:1596`) — a loud refusal is a terminal
  observation, not a payload comparison.
- In the rank executor, catch `FMI::Utils::IdentityMismatch` **before** the existing
  `FMI::Utils::Timeout` handler and record `LoudFail` with the exception text in `detail`.

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
`tests/migration_counterexample_runner.cpp:1870-1889`). Disposition, in two parts:

1. **Retarget the model to post-A2 semantics.** The Task 2 model rule "Redis promotion discards all
   old queues" *was* the counter reset that Task 3 and Task 4 removed. Change the ClientServer
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
cmake -S /home/luca/fmi -B /home/luca/fmi/build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON
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
