# Plan C — Sequenced Link Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> ## ⚠️ THIS PLAN IS WRITTEN JUST-IN-TIME. READ THIS BEFORE TASK 1.
>
> **Only Stage C1 (Tasks 1–7) is specified as executable tasks.** Stages C2, C3, and C4 are
> deliberately left as scoped outlines with entry criteria, named work items, and the decisions
> that must be resolved before they can be expanded.
>
> This is not laziness — it is the staging discipline the design spec mandates. C2 is the
> commitment point ("Socket ownership is not divisible — once the engine owns the fds there is no
> partial retreat", design spec, *Staging map*), and the spec explicitly defers the C2 decision
> "until B has reported what CRIU actually does on the target kernel." Two facts that C2–C4 task
> detail would have to encode do not exist yet:
>
> 1. **Plan B's measured CRIU findings** — whether `criu dump` handles a process with a live
>    second (engine) thread cleanly on this kernel, the observed frozen duration as a function of
>    RSS, and the EINTR/SIGPIPE fallout enumerated in design spec contract 4. These determine
>    whether the progress engine is a thread at all, and how `W` must be sized.
> 2. **Plan A's landed identity seam** — the RAII lane-and-instance scope that publishes
>    `(lane, op_kind, collective_index, root)` (design spec contract 1, *Where the identity is
>    produced*). `LinkFrame` has nothing to read until that seam exists, and its exact shape
>    changes what C2's framed `Direct` reads at send time.
>
> **Writing C2–C4 tasks now would mean fabricating detail that depends on unmeasured facts.**
> Where a decision is needed, this plan names the decision and its owner instead of guessing.
> Expand C2 into tasks only after its entry criteria (below) are all met; expand C3 only after
> C2's exit criterion is demonstrated; expand C4 only after C3 lands.
>
> **C1 is fully reversible and independently valuable.** It adds new files only, wires nothing
> into production paths, and can be abandoned without touching a single existing code path.

**Goal:** Build the offline, socket-free sequenced link layer (`LinkFrame` codec + `SequencedLink`
state machine + `ProgressHooks` seam + the `fault_tolerance.link` config block) that design spec
contracts 1 and 2 require, property-tested to the point where the only remaining risk in the
transport is socket ownership itself — then hand off to a staged, entry-criteria-gated rollout.

**Architecture:** `LinkFrame` is a header-only, endianness-explicit codec for the envelope in
design spec contract 1. `SequencedLink` is a pure state machine over that codec: it owns retention,
both watermarks, per-lane drain queues, dedup, credit, handshake reconciliation, and
snapshot/seed — and it never touches a file descriptor, a thread, or a clock. `ProgressHooks` is
the abstract seam that a C2 progress engine implements; in C1 it is driven only by a test double
that asserts design spec contract 2's three ordering invariants. Production code is untouched in
C1 apart from additive config parsing.

**Tech Stack:** C++17, Boost.Test, CMake, hiredis/Redis (control plane, C3+), TCPunch Direct
transport (C2+), CRIU 4.2 (Plan B / C2+), POSIX threads and processes.

## Global Constraints

- The design spec `docs/superpowers/specs/2026-07-27-sequenced-incarnation-links-design.md` is
  normative. This plan says HOW and IN WHAT ORDER; it does not restate WHAT or WHY. Every task
  references the spec by contract number.
- Every task must end with the tree building and `./build/tests/Boost_Tests_run` passing. Each
  task names its own new test explicitly; a task with no new test is not a task.
- One task = one commit, per `CLAUDE.md` ("Commit after every meaningful, self-contained change";
  "Each commit should leave the codebase in a working state").
- Stage and commit **only** files belonging to the current task. The working tree carries
  pre-existing uncommitted changes (`runbooks/localstack-python311-redis/orchestrator.py`,
  `runbooks/localstack-python311-redis/knative-migration/orchestrator.py`, `tests/CMakeLists.txt`,
  `tests/channels.cpp`, `tests/communicator.cpp`, plus untracked `tests/forked_rank_guard.h` and
  `tests/migration_p2p_cut_counterexample.cpp`). Preserve all of them. Never `git add -A`.
- **C1 adds no production wiring.** No existing source file changes behavior. The one permitted
  production edit in C1 is additive config parsing (Task 1), which is inert until C2 reads it.
- **Cross-plan file ownership** (index: `docs/superpowers/plans/2026-07-27-migration-v2-README.md`):
  - `tests/channels.cpp` and `tests/communicator.cpp` (Task 7) are **shared with Plan A tasks 1–2**,
    which add two cases to the `Channels` suite. **Task 7 must land after Plan A tasks 1–2** —
    running it first guarantees a merge conflict and invalidates its case-list diff. Task 7 step 1
    records whatever count it observes; it must not hardcode one.
  - `include/utils/Configuration.h` / `src/utils/Configuration.cpp` (Task 1) are Plan C's alone.
    Plan B explicitly forbids itself any configuration-schema change.
  - Plan C does **not** own `Classification::LoudFail`, `aggregate_exit`, the `--group` selector, or
    the `tests/migration_cut_model.cpp` disposition. **Plan A task 9 delivers all four**; stage C4
    only re-verifies them against the incarnation protocol.
  - Stage C3 rewrites `ControlPlane`. Plan A task 6 lands one additive change there first (a
    defaulted `data_comm_name` parameter on `clear_job_state`, plus a `<data_comm_name>:*` sweep);
    it is in C3's port list below and must be carried forward, not dropped.
  - Plan B hands Plan C two items it measures but may not fix, because they sit outside Plan B's
    file set: the `PR_SET_PTRACER` re-arm (`src/ft/TransparentMigrationRuntime.cpp:155`) and the
    stale `--tcp-close` claim in `include/ft/ControlPlane.h:86-92`. Both are named work items below.
- Canonical configure for this machine:
  `cmake -S . -B build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON -DFMI_ENABLE_S3=OFF`.
  `-DFMI_ENABLE_CRIU=ON` is **required** — without it neither counterexample binary is built
  (`tests/CMakeLists.txt:19` and `:33` both gate on `FMI_ENABLE_CRIU`).
- Infrastructure on this machine: Redis native at `127.0.0.1:6379` (verify with
  `redis-cli -h 127.0.0.1 -p 6379 ping` → `PONG`); `tcpunchd` from
  `extern/TCPunch/server/build-fresh/tcpunchd` on port 10000 (the `server/build/` binary is stale
  pre-fix — do not use it); criu 4.2 at `/usr/local/sbin/criu` with `cap_sys_ptrace=eip`.
- Use `uv` for any Python work, never bare `pip`. C1 requires no Python packages.
- Wire format is little-endian and field-by-field encoded. Never `memcpy` a struct to the wire and
  never rely on struct packing or padding.
- `SequencedLink` must remain free of sockets, threads, sleeps, and wall-clock reads for the whole
  of C1. Every test in `tests/link_layer.cpp` must run deterministically in well under a second
  with no external infrastructure.

---

# Stage C1 — Offline link layer (fully specified)

Seven tasks. Reversible throughout. Delivers `LinkFrame`, `SequencedLink`, `ProgressHooks`, the
`fault_tolerance.link` config block, and the `LinkLayer` Boost.Test suite.

### Task 1: Pin the two blocking open items and add the `fault_tolerance.link` config block

The design spec's *Open items to resolve before Plan C task 1* lists two items (1: CREDIT initial
value and `retention_limit_bytes` cap behavior; 2: the quiesce-snapshot schema) that must be
decided before any code. This task records the decisions and lands the config surface they imply.

**Files:**
- Create: `docs/superpowers/decisions/2026-07-27-link-layer-open-items.md`
- Modify: `include/utils/Configuration.h`
- Modify: `src/utils/Configuration.cpp`
- Create: `tests/link_layer.cpp`
- Modify: `tests/CMakeLists.txt`

**Interfaces:**

Nested inside the existing `FMI::Utils::FaultToleranceConfig` (`include/utils/Configuration.h:11-40`),
alongside the existing `Criu` sub-struct at `:34-39` — same shape, same parse pattern:

```cpp
namespace FMI::Utils {
    struct FaultToleranceConfig {
        // ... existing members (enabled, control_backend, ..., state_transfer, criu) ...

        //! Sequenced link layer parameters (design spec, "Config surface"). Inert until the
        //! progress engine reads them (Stage C2). Parsed from a nested "link" object under
        //! "fault_tolerance"; absent -> defaults.
        struct Link {
            std::uint32_t window_frames = 64;              // W
            std::uint32_t max_frame_bytes = 1u << 20;      // 1 MiB
            std::uint64_t retention_limit_bytes = 0;       // 0 => W * max_frame_bytes
            std::uint32_t slice_ms = 5;
            std::uint32_t drain_reserve_frames = 64;       // must be >= window_frames
        } link;
    };
}
```

`Configuration::get_fault_tolerance_config()` (`src/utils/Configuration.cpp:44-66`) gains a
`get_child_optional("link")` block mirroring the `criu` block at `:59-64`, followed by **validation
at parse** that throws `std::runtime_error` with a field-naming message on any violation:

- `window_frames >= 1`
- `max_frame_bytes >= 64` (one header, see Task 2) and `max_frame_bytes <= 1u << 30`
- `drain_reserve_frames >= window_frames` — design spec contract 2, "Drain reserve per source ≥ the
  peer's window `W`, otherwise ACKs queue behind undrainable data (ack-behind-data deadlock)"
- effective `retention_limit_bytes >= window_frames * max_frame_bytes` (see the decision below)

- [ ] **Step 1: Verify the config block is unrecognized (RED)**

Write the failing test first in `tests/link_layer.cpp` (suite `LinkLayer`), then run:

```bash
cmake -S . -B build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON -DFMI_ENABLE_S3=OFF
cmake --build build --target Boost_Tests_run -j"$(nproc)"
```

Expected: compile error — `FaultToleranceConfig` has no member `link`.

- [ ] **Step 2: Write the decision record**

Create `docs/superpowers/decisions/2026-07-27-link-layer-open-items.md`. Do **not** edit the design
spec; it is normative and this is a downstream resolution of items it left open. Record:

**Open item 1 — CREDIT initial value and `retention_limit_bytes` cap behavior.** The spec offers
two options: derive retention from the window and reject configs that cannot honor the delivery
obligation, *or* never discard retention and let a cap hit block admission of new sends only.
**Resolution (requires sign-off before Task 3):** take *both halves* of the second option plus the
first option's parse-time rejection —

  - Retention is **never** discarded except on a validated cumulative ACK (design spec contract 2,
    ordering invariant 3). `retention_limit_bytes` is an **admission threshold, not an eviction
    threshold**: `SequencedLink::admit()` refuses new sends at the cap and the caller must retry.
    Eviction would break the delivery obligation outright.
  - Parse-time rejection of `retention_limit_bytes < window_frames * max_frame_bytes`, because
    below that the cap — not `W` — is the effective window, and a config whose real window is not
    the configured one is a trap. `0` means "derive as `window_frames * max_frame_bytes`".
  - **Initial CREDIT = `drain_reserve_frames` frames and `drain_reserve_frames * max_frame_bytes`
    bytes.** Combined with the `drain_reserve_frames >= window_frames` constraint this makes the
    initial credit ≥ `W` by construction, which is exactly the ack-behind-data-deadlock condition
    the spec states. Credit is returned as the application consumes (contract 2, *ACK versus
    CREDIT*). Control frames — ACK, CREDIT, HANDSHAKE — are credit-exempt.

  State explicitly in the record that this makes `W` a *hard* bound on in-flight bytes where TCP
  socket buffers are effectively unbounded today, and cross-reference the backpressure regression
  risk (Task 7 and C2 work item 7).

**Open item 2 — quiesce-snapshot schema.** The spec directs: "an opaque versioned per-**channel**
blob produced by each channel's `quiesce_links()`, and enumerate the required contents." Enumerate
them:

  1. `schema_version` (u16), so a restored image from an older build fails loudly.
  2. `run_id`, base `comm_name` (the *un*-qualified one — contract 1 decouples data keys from the
     pairing name), `rank`, and `incarnation` (`epoch` until C3).
  3. The per-**Communicator** `collective_index` counter (contract 1: "The counter must live on
     the `Communicator`, not the channel"), carried into the channel blob because the channel blob
     is what the image round-trips.
  4. Per-directed-pair `message_id` counters, both directions, for both lanes.
  5. Per-peer `SequencedLink::snapshot()` blobs (Task 5), keyed by peer id.
  6. The policy fingerprint: `hint`, `faas_price`, model-parameter hash, `num_peers`,
     `wire_version` (contract 1, *Policy uniformity*).
  7. The `AckPolicy` in force (`OnReceive` for `criu`, `OnConsume` for `none` — contract 2, *Two
     watermarks*), so a restored rank cannot silently change its ack discipline.

  Note that `quiesce_links()` is a `Channel` virtual and therefore production wiring: it is
  **out of scope for C1** and lands in C2. C1 delivers only item 5's per-link blob.

- [ ] **Step 3: Add the `Link` struct, the parser, and parse-time validation**

Implement the interface above. Keep the parser shape identical to the existing `criu` block so the
two read the same way. Validation throws from `get_fault_tolerance_config()`, not from a later
consumer, so a bad config fails at `Communicator` construction rather than mid-run.

- [ ] **Step 4: Add `tests/link_layer.cpp` and wire it into the test binary**

Create the file with `BOOST_AUTO_TEST_SUITE(LinkLayer)` and these cases:

```text
link_config_defaults_when_block_absent
link_config_parses_every_field
link_config_rejects_drain_reserve_below_window
link_config_rejects_retention_below_one_window
link_config_zero_retention_derives_from_window
```

Each writes a small JSON to a unique path under `/tmp` and constructs
`FMI::Utils::Configuration` on it — the same pattern the existing FT config tests use. Do **not**
edit the checked-in `config/*.json` files in this task; C2 adds the `link` block to
`config/fmi_ft_test.json` when something actually reads it.

In `tests/CMakeLists.txt:6`, extend the source list:

```cmake
set(FMI_TEST_SOURCES channels.cpp communicator.cpp fault_tolerance.cpp link_layer.cpp)
```

- [ ] **Step 5: Build and verify (GREEN)**

```bash
cmake --build build --target Boost_Tests_run -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=LinkLayer
./build/tests/Boost_Tests_run --list_content | head -30
```

Expected: `LinkLayer` reports 5 cases, all passing. Then confirm nothing regressed with live
infrastructure up:

```bash
redis-cli -h 127.0.0.1 -p 6379 ping
(./extern/TCPunch/server/build-fresh/tcpunchd 10000 &) ; sleep 1
./build/tests/Boost_Tests_run
pkill -f 'build-fresh/tcpunchd 10000'
```

Expected: `*** No errors detected`.

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/decisions/2026-07-27-link-layer-open-items.md \
  include/utils/Configuration.h src/utils/Configuration.cpp \
  tests/link_layer.cpp tests/CMakeLists.txt
git commit -m "feat: add fault_tolerance.link config block and pin link-layer open items"
```

### Task 2: `LinkFrame` — header-only envelope codec

**Files:**
- Create: `include/comm/LinkFrame.h`
- Modify: `tests/link_layer.cpp`

**Interfaces:**

Every field of design spec contract 1's envelope table, plus the reduction associativity/
commutativity flags the spec places "in the **envelope**, alongside `op_kind`" (contract 1,
*Policy uniformity*). `op_kind` reuses the existing `FMI::Utils::Operation` enum
(`include/utils/Common.h:47-49`), which already has exactly the eight kinds the contract names —
`send, bcast, barrier, gather, scatter, reduce, allreduce, scan`. Do not define a parallel enum.

```cpp
#ifndef FMI_LINK_FRAME_H
#define FMI_LINK_FRAME_H

#include "../utils/Common.h"
#include <cstddef>
#include <cstdint>

namespace FMI::Comm::Link {

    //! Protocol generation. A framed rank must never pair with a raw one
    //! (design spec contract 1).
    inline constexpr std::uint16_t k_wire_version = 2;

    //! Fixed on-wire header size. Encoded field-by-field, little-endian; never memcpy'd.
    inline constexpr std::size_t k_header_bytes = 64;

    enum class Lane : std::uint8_t { P2P = 0, Collective = 1 };

    enum class FrameType : std::uint8_t { Data = 0, Ack = 1, Credit = 2, Handshake = 3 };

    //! Reduction-function properties, per collective, not job-wide: left_to_right =
    //! !(commutative && associative) selects wholly different algorithms
    //! (computed at include/Communicator.h:103 in reduce and :128 in allreduce).
    enum FrameFlag : std::uint8_t { Associative = 1u << 0, Commutative = 1u << 1 };

    struct FrameHeader {
        std::uint16_t wire_version   = k_wire_version;
        FrameType     type           = FrameType::Data;
        Lane          lane           = Lane::P2P;
        std::uint8_t  op_kind        = 0;   //!< FMI::Utils::Operation, widened
        std::uint8_t  flags          = 0;   //!< FrameFlag bitset
        std::uint32_t root           = 0;   //!< collective root, or peer id on the P2P lane
        std::uint32_t fragment_index = 0;
        std::uint64_t collective_index = 0; //!< per-Communicator; zero and unused on P2P
        std::uint64_t message_id     = 0;   //!< per-lane, per-directed-pair, never reset
        std::uint64_t total_length   = 0;   //!< logical message length in bytes
        std::uint64_t transport_seq  = 0;   //!< link-scoped, job-lifetime
        std::uint32_t payload_length = 0;   //!< bytes of this fragment
        std::uint32_t header_crc     = 0;   //!< CRC32 over the preceding bytes
    };

    enum class Decode {
        Ok,
        NeedMore,      //!< fewer than k_header_bytes available: a torn header, not an error
        BadVersion,    //!< wire_version mismatch -> loud abort, never a silent downgrade
        BadHeader,     //!< CRC or field-domain violation
        TooLarge       //!< payload_length > max_frame_bytes
    };

    //! Returns k_header_bytes on success, 0 if out_len is too small.
    std::size_t encode_header(const FrameHeader& hdr, char* out, std::size_t out_len);

    //! Decodes exactly k_header_bytes. Never reads past in_len.
    Decode decode_header(const char* in, std::size_t in_len,
                         std::size_t max_frame_bytes, FrameHeader& out);

    //! True when both headers describe the same logical operation. This is the check that
    //! catches the bcast-vs-barrier collision in design spec contract 1.
    bool same_operation(const FrameHeader& a, const FrameHeader& b);
}

#endif // FMI_LINK_FRAME_H
```

All functions are `inline` — the header is standalone, so nothing is added to `FMI_SOURCES` in
`CMakeLists.txt:42-51` for this task.

- [ ] **Step 1: Verify the header is absent (RED)**

Add the codec cases to `tests/link_layer.cpp` with `#include "../include/comm/LinkFrame.h"` and run:

```bash
cmake --build build --target Boost_Tests_run -j"$(nproc)"
```

Expected: `fatal error: ../include/comm/LinkFrame.h: No such file or directory`.

- [ ] **Step 2: Implement the codec**

Encode/decode little-endian, byte by byte, into a fixed 64-byte layout with explicit reserved
padding. Validate on decode: `wire_version == k_wire_version`, `type` and `lane` in domain,
`op_kind` within `FMI::Utils::Operation`'s range, `payload_length <= max_frame_bytes`,
`fragment_index * 1ull <= total_length`, and CRC. Reject `collective_index != 0` on the `P2P`
lane and `root >= num_peers` is *not* checkable here (no `num_peers`) — leave that to
`SequencedLink`.

- [ ] **Step 3: Verify the codec (GREEN)**

Cases in `LinkLayer`:

```text
frame_codec_round_trips_every_op_kind_and_lane
frame_codec_round_trips_assoc_comm_flag_matrix     # all four combinations
frame_codec_torn_header_needs_more                 # every prefix length 0..63 -> NeedMore
frame_codec_rejects_foreign_wire_version
frame_codec_rejects_corrupt_header_crc             # flip one bit in each field's byte range
frame_codec_rejects_oversized_payload
frame_codec_header_is_exactly_64_bytes
same_operation_separates_bcast_from_barrier        # the contract-1 N=2 collision, header-only
```

The `same_operation_separates_bcast_from_barrier` case builds the two 1-byte frames the spec
derives at `src/comm/PeerToPeer.cpp:14-27` (`bcast`, `rounds=1`) and `:29-33` (`barrier` as a
1-byte `allreduce` with a `{commutative=true, associative=true}` nop) and asserts `same_operation`
is **false** even though `lane`, `total_length`, and `message_id` are identical on both.

```bash
cmake --build build --target Boost_Tests_run -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=LinkLayer
```

- [ ] **Step 4: Commit**

```bash
git add include/comm/LinkFrame.h tests/link_layer.cpp
git commit -m "feat: add LinkFrame envelope codec"
```

### Task 3: `SequencedLink` send side — retention ring, window, admission

**Files:**
- Create: `include/comm/SequencedLink.h`
- Create: `src/comm/SequencedLink.cpp`
- Modify: `CMakeLists.txt`
- Modify: `tests/link_layer.cpp`

**Interfaces:**

```cpp
namespace FMI::Comm::Link {

    //! Contract 2: CRIU mode acks on receive (drain buffers are inside the image);
    //! `none` mode acks only on application consumption.
    enum class AckPolicy { OnReceive, OnConsume };

    struct Watermarks {
        std::uint64_t next_send       = 0;  //!< next transport_seq this side will assign
        std::uint64_t next_received   = 0;  //!< highest contiguous seq accepted, +1
        std::uint64_t ack_safe        = 0;  //!< highest seq the peer may prune, +1
        std::uint64_t lowest_retained = 0;  //!< oldest seq still in retention
    };

    class SequencedLink {
    public:
        SequencedLink(FMI::Utils::FaultToleranceConfig::Link cfg, AckPolicy policy);

        // --- send side ---------------------------------------------------
        //! Retain an immutable copy and assign transport_seq (ordering invariant 1: the
        //! retained copy exists before any byte may reach a socket). Returns false when the
        //! window or the retention cap refuses admission; the caller MUST retry, never drop.
        bool admit(FrameHeader hdr, const char* payload, std::size_t len);

        //! Retained frames not yet handed to a socket, oldest first, encoded and ready.
        std::vector<std::uint64_t> pending_egress() const;
        const std::string& encoded_frame(std::uint64_t transport_seq) const;
        void on_egress_written(std::uint64_t transport_seq);

        //! Validated cumulative ACK. Reclaims retention (ordering invariant 3). Throws
        //! std::runtime_error on an impossible cumulative value (> next_send).
        void on_ack(std::uint64_t cumulative);

        [[nodiscard]] std::size_t retained_bytes() const;
        [[nodiscard]] Watermarks watermarks() const;
    };
}
```

Add `src/comm/SequencedLink.cpp` to `FMI_SOURCES` in `CMakeLists.txt` next to
`src/comm/Direct.cpp` (currently `CMakeLists.txt:49`). It is unconditional — the link layer does
not depend on `FMI_ENABLE_S3` / `FMI_ENABLE_REDIS` / `FMI_ENABLE_CRIU`.

- [ ] **Step 1: Verify admission and retention are absent (RED)**

Add the send-side cases, build, expect a missing-header compile failure.

- [ ] **Step 2: Implement retention, window, and reclamation**

`transport_seq` is assigned inside `admit`, monotonically, never reset. Retention is a ring
indexed by seq. Admission fails when either `next_send - lowest_retained >= window_frames` or
`retained_bytes() + frame_size > effective retention_limit_bytes` (Task 1's decision: an
admission threshold, never an eviction threshold). `on_ack` reclaims strictly below the cumulative
value and advances `lowest_retained`; a cumulative ACK above `next_send` is the spec's "impossible
state → loud abort".

- [ ] **Step 3: Verify the send side (GREEN)**

Cases in `LinkLayer`:

```text
retention_holds_every_admitted_frame_until_acked
retention_trims_exactly_on_cumulative_ack
retention_ignores_a_regressive_cumulative_ack
retention_aborts_loudly_on_ack_above_next_send
admit_refuses_at_window_full_and_resumes_after_ack
admit_refuses_at_retention_cap_without_evicting     # retained_bytes never decreases here
transport_seq_is_monotonic_and_never_reset
```

```bash
cmake --build build --target Boost_Tests_run -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=LinkLayer
./build/tests/Boost_Tests_run
```

- [ ] **Step 4: Commit**

```bash
git add include/comm/SequencedLink.h src/comm/SequencedLink.cpp CMakeLists.txt tests/link_layer.cpp
git commit -m "feat: add SequencedLink retention ring and send window"
```

### Task 4: `SequencedLink` receive side — dedup, watermarks, per-lane drain, credit

**Files:**
- Modify: `include/comm/SequencedLink.h`
- Modify: `src/comm/SequencedLink.cpp`
- Modify: `tests/link_layer.cpp`

**Interfaces:**

```cpp
namespace FMI::Comm::Link {

    enum class Accept {
        Delivered,   //!< in-order, committed to the lane's drain queue
        Duplicate,   //!< transport_seq < next_received: a replay, silently correct to drop
        FatalGap     //!< transport_seq > next_received: the sender pruned what we never got
    };

    class SequencedLink {
    public:
        // ... send side from Task 3 ...

        // --- receive side ------------------------------------------------
        //! Ordering invariant 2: a complete frame is committed to the drain queue before its
        //! ACK becomes writable. Returns FatalGap rather than buffering out-of-order frames —
        //! per-directed-pair FIFO plus replay makes a gap impossible unless an invariant broke.
        Accept on_frame(const FrameHeader& hdr, const char* payload, std::size_t len);

        //! Pop the next complete logical message on `lane`, FIFO within the lane. Returns
        //! false when the lane's queue is empty or its head message is still fragmented.
        bool drain(Lane lane, FrameHeader& out_hdr, std::string& out_payload);

        //! What this side may currently advertise as ack_safe. Under OnReceive this tracks
        //! next_received; under OnConsume it only advances as drain() consumes.
        [[nodiscard]] std::uint64_t ackable() const;

        [[nodiscard]] std::uint32_t credit_frames(Lane lane) const;
        [[nodiscard]] std::size_t   credit_bytes(Lane lane) const;
    };
}
```

- [ ] **Step 1: Verify the receive side is absent (RED)**

- [ ] **Step 2: Implement dedup, the two watermarks, per-lane queues, and credit**

Two independent lanes (`P2P`, `Collective`), each with its own FIFO drain queue, per design spec
contract 1 ("Separate FIFO and drain queue per lane"). `transport_seq` remains **link-scoped**, not
per-lane — dedup and replay operate on the link. Fragment reassembly keys on
`(lane, message_id, fragment_index)` against `total_length`; a message becomes drainable only when
every fragment has arrived. Credit is decremented on `on_frame` for `FrameType::Data` and returned
on `drain`; `Ack`, `Credit`, and `Handshake` frames are credit-exempt.

- [ ] **Step 3: Verify the receive side (GREEN)**

Cases in `LinkLayer`:

```text
dedup_returns_duplicate_for_replayed_frames
dedup_is_idempotent_across_a_full_replayed_suffix
gap_returns_fatal_gap_and_does_not_advance_next_received
next_received_and_ack_safe_diverge_under_on_consume
next_received_equals_ack_safe_under_on_receive
lane_demux_separates_bcast_from_barrier_at_one_byte
drain_is_fifo_within_a_lane
drain_across_lanes_is_independent
fragmented_message_drains_only_when_complete
credit_returns_only_as_the_application_consumes
control_frames_are_credit_exempt
both_directions_full_still_admits_acks              # drain_reserve >= W, contract 2
```

`lane_demux_separates_bcast_from_barrier_at_one_byte` is the end-to-end form of the contract-1
counterexample: feed a link the 0→1 stream `[bcast(root=0)][barrier]` while the receiver expects
barrier-then-bcast, and assert the mismatch is reported rather than silently delivered — the
failure class the protocol exists to eliminate.

`both_directions_full_still_admits_acks` is the ack-behind-data-deadlock guard: fill both
directions to `window_frames` with undrained data and assert that an ACK frame is still accepted
and still reclaims retention on each side.

```bash
cmake --build build --target Boost_Tests_run -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=LinkLayer
./build/tests/Boost_Tests_run
```

- [ ] **Step 4: Commit**

```bash
git add include/comm/SequencedLink.h src/comm/SequencedLink.cpp tests/link_layer.cpp
git commit -m "feat: add SequencedLink dedup, per-lane drain queues, and credit"
```

### Task 5: Handshake reconciliation and `snapshot()` / `seed()`

**Files:**
- Modify: `include/comm/SequencedLink.h`
- Modify: `src/comm/SequencedLink.cpp`
- Modify: `tests/link_layer.cpp`

**Interfaces:**

```cpp
namespace FMI::Comm::Link {

    //! Carried per direction on reconnect (design spec contract 2, "Two watermarks").
    struct HandshakeState {
        std::uint16_t wire_version   = k_wire_version;
        std::uint64_t next_send      = 0;
        std::uint64_t next_received  = 0;
        std::uint64_t ack_safe       = 0;
        std::uint64_t lowest_retained = 0;
        std::uint64_t fingerprint    = 0;  //!< policy fingerprint, contract 1
    };

    class SequencedLink {
    public:
        // ... Tasks 3 and 4 ...

        [[nodiscard]] HandshakeState local_handshake(std::uint64_t fingerprint) const;

        //! Idempotent: repeated calls with the same peer state return the same replay set and
        //! mutate nothing. Returns the exact unacked suffix to replay, oldest first.
        //! Throws std::runtime_error on an impossible peer state:
        //!   - peer.next_received > this->next_send
        //!   - peer requesting frames older than this->lowest_retained
        //!   - wire_version or fingerprint mismatch
        std::vector<std::uint64_t> reconcile(const HandshakeState& peer);

        //! Opaque, versioned. Item 5 of the quiesce-snapshot schema (Task 1 decision record).
        [[nodiscard]] std::string snapshot() const;
        void seed(const std::string& blob);   //!< throws on schema_version mismatch
    };
}
```

- [ ] **Step 1: Verify reconciliation and snapshot are absent (RED)**

- [ ] **Step 2: Implement reconciliation and snapshot/seed**

`reconcile` is pure with respect to the link's own watermarks except for adopting the peer's
cumulative ack (which is an `on_ack`). The replay set is `[peer.next_received, next_send)`, which
is exactly the unacked suffix — the property that makes `criu dump --tcp-close` sound, because
"everything in a kernel socket buffer is unacked by construction" (contract 2). `snapshot()`
serializes `schema_version`, both watermarks, the retention ring contents, every lane's drain queue
and partial-reassembly state, and the `AckPolicy`.

- [ ] **Step 3: Verify reconciliation and snapshot (GREEN)**

Cases in `LinkLayer`:

```text
reconcile_replays_exactly_the_unacked_suffix
reconcile_is_idempotent_when_called_twice
reconcile_replays_nothing_when_peer_is_fully_caught_up
reconcile_aborts_when_peer_next_received_exceeds_next_send
reconcile_aborts_when_peer_requests_below_lowest_retained
reconcile_aborts_on_wire_version_mismatch
reconcile_aborts_on_fingerprint_mismatch
snapshot_seed_round_trips_watermarks_and_retention
snapshot_seed_round_trips_partial_fragment_reassembly
seed_then_reconcile_then_replay_delivers_exactly_once
seed_rejects_a_foreign_schema_version
```

`seed_then_reconcile_then_replay_delivers_exactly_once` is the whole C1 thesis in one case: snapshot
a mid-stream link, seed a fresh one from the blob, reconcile against a peer that lost its socket
buffer, replay the returned suffix, and assert the receiving side reports `Delivered` for every
frame it had not seen and `Duplicate` for every frame it had — no loss, no duplication, across an
arbitrary-instant freeze.

- [ ] **Step 4: Commit**

```bash
git add include/comm/SequencedLink.h src/comm/SequencedLink.cpp tests/link_layer.cpp
git commit -m "feat: add SequencedLink handshake reconciliation and snapshot/seed"
```

### Task 6: `ProgressHooks` seam and the ordering-invariant proof

**Files:**
- Create: `include/comm/ProgressHooks.h`
- Modify: `include/comm/SequencedLink.h`
- Modify: `src/comm/SequencedLink.cpp`
- Modify: `tests/link_layer.cpp`

**Interfaces:**

```cpp
#ifndef FMI_PROGRESS_HOOKS_H
#define FMI_PROGRESS_HOOKS_H

#include "LinkFrame.h"
#include <cstddef>
#include <cstdint>

namespace FMI::Comm::Link {

    //! Seam between the socket-free SequencedLink (Stage C1) and the progress engine that
    //! will own the file descriptors (Stage C2). SequencedLink calls these at exactly the
    //! points design spec contract 2's three ordering invariants name; C1 wires only a test
    //! double, C2 wires the engine. Implementations must not call back into the link.
    class ProgressHooks {
    public:
        virtual ~ProgressHooks() = default;

        //! Invariant 1: after the immutable retained copy exists, before any DATA byte may
        //! reach a socket.
        virtual void on_retained(std::uint64_t transport_seq, std::size_t bytes) = 0;

        //! Invariant 2: after a complete frame is committed to the drain queue, before its
        //! ACK becomes writable.
        virtual void on_drain_committed(Lane lane, std::uint64_t transport_seq) = 0;

        //! Invariant 3: only from a validated cumulative ACK.
        virtual void on_retention_reclaimed(std::uint64_t through_seq, std::size_t bytes) = 0;

        //! Credit returned as the application consumes.
        virtual void on_credit_returned(Lane lane, std::uint32_t frames, std::size_t bytes) = 0;
    };
}

#endif // FMI_PROGRESS_HOOKS_H
```

`SequencedLink` takes an optional `ProgressHooks*` (nullable, defaulting to `nullptr`) — C1 must
not make the hooks mandatory, because C2 has not decided whether the engine or the app thread owns
the send (design spec *Decision record*, open alternative; C2 entry criterion 3 below).

- [ ] **Step 1: Verify the seam is absent (RED)**

- [ ] **Step 2: Add the seam and its call sites**

Invoke each hook at exactly the point its comment names. No hook may be invoked from inside a
`SequencedLink` mutex the implementation could re-enter; document the calling context in the
header.

- [ ] **Step 3: Verify the ordering invariants (GREEN)**

Add a `RecordingHooks` test double to `tests/link_layer.cpp` that appends every callback to a
sequence log, then assert the invariants directly. Cases:

```text
hook_order_retain_precedes_egress
hook_order_drain_commit_precedes_ackable_advance
hook_order_reclaim_only_follows_a_validated_ack
freeze_between_retain_and_egress_loses_nothing
freeze_between_egress_and_drain_commit_loses_nothing
freeze_between_drain_commit_and_ack_write_loses_nothing
freeze_after_ack_write_before_ack_receipt_loses_nothing
```

The four `freeze_*` cases enumerate the spec's four freeze positions relative to drain-commit and
ACK-write: at each position, `snapshot()` both sides, seed fresh links, reconcile, replay, and
assert the delivered set is exactly the sent set with no duplicates surfaced to the application.
This is the C1-level evidence for contract 2's claim that "none produces loss or duplication".

```bash
cmake --build build --target Boost_Tests_run -j"$(nproc)"
./build/tests/Boost_Tests_run --run_test=LinkLayer
./build/tests/Boost_Tests_run
```

- [ ] **Step 4: Commit**

```bash
git add include/comm/ProgressHooks.h include/comm/SequencedLink.h src/comm/SequencedLink.cpp \
  tests/link_layer.cpp
git commit -m "feat: add ProgressHooks seam and prove link ordering invariants"
```

### Task 7: Transport-variant parameterization of the existing matrices, and the unframed backpressure baseline

This task is **test-only** and lands in C1 on purpose. It pre-builds the mechanism C2 needs and
buys the flag-off byte-identical evidence for free.

**Why it is needed** (this is the framed-collective coverage gap): the `Channels` and `Communicator`
suites are the design spec's "genuinely untouched controls", and they will *never* exercise framed
transport by accident. `tests/channels.cpp` builds its backends from a hardcoded table at
`tests/channels.cpp:62-66` — in the current working tree only `Direct` is live, S3 and Redis are
commented out — and every case calls `FMI::Comm::Channel::get_channel(channel_name, test_params,
model_params)` (e.g. `tests/channels.cpp:83`). Neither suite ever constructs FT. So before rollout
the framing flag is off there, and after rollout, if framing keys on FT being enabled, it is still
off there. `Channel::get_channel` (`src/comm/Channel.cpp:11-30`) already takes the `params` map that
`Direct`'s constructor reads (`src/comm/Direct.cpp:12-26`), so the parameterization hook already
exists — this task only exploits it.

**Files:**
- Create: `tests/transport_variants.h`
- Modify: `tests/channels.cpp`
- Modify: `tests/communicator.cpp`
- Modify: `tests/link_layer.cpp`

**Interfaces:**

```cpp
#ifndef FMI_TESTS_TRANSPORT_VARIANTS_H
#define FMI_TESTS_TRANSPORT_VARIANTS_H

#include <map>
#include <string>
#include <vector>

namespace FMI::Tests {
    //! One entry per (backend, transport-variant) pair. `overlay` is merged into the backend's
    //! params map before Channel::get_channel, so a variant is one extra key and nothing else
    //! about a case changes. In C1 there is exactly one variant per backend ("unframed", empty
    //! overlay); C2 adds {"framed", {{"framed","true"}}} and every case in the Channels and
    //! Communicator matrices runs twice.
    struct TransportVariant {
        std::string name;
        std::map<std::string, std::string> overlay;
    };

    const std::vector<TransportVariant>& transport_variants();

    //! Backend params with the variant overlay applied.
    std::map<std::string, std::string> with_variant(std::map<std::string, std::string> params,
                                                    const TransportVariant& variant);
}

#endif // FMI_TESTS_TRANSPORT_VARIANTS_H
```

- [ ] **Step 1: Record the current matrix as the baseline (RED-equivalent)**

There is no missing symbol to fail on here, so capture the pre-change result instead:

```bash
(./extern/TCPunch/server/build-fresh/tcpunchd 10000 &) ; sleep 1
./build/tests/Boost_Tests_run --run_test=Channels,Communicator --log_level=test_suite \
  > /tmp/fmi-matrix-before.txt 2>&1
grep -c "Leaving test case" /tmp/fmi-matrix-before.txt
```

**Record the number this command prints; do not assert a literal.** At plan-authoring time it is 25
— 15 cases in `Channels` (`tests/channels.cpp`, verified with
`./build/tests/Boost_Tests_run --list_content`) plus 10 in `Communicator`. **Note:** the parent
brief describes this as "`tests/channels.cpp`'s 25-case matrix"; the verified split is 15 + 10
across the two files, and both must be parameterized for the coverage argument to hold.

> **Ordering dependency, non-negotiable.** Plan A tasks 1 and 2 add
> `Channels/direct_pair_timeout_is_clean` and `Channels/direct_concurrent_pairings_one_timeout` to
> this same file. **This task lands after them**, so the observed baseline will be 27 (17 + 10), not
> 25. Those two cases drive `pair()` directly rather than `Channel::get_channel`, so they stay
> **outside** the variant loop — the overlay has nothing to apply to them. If the baseline is 25,
> stop and confirm Plan A's A1 tasks have not yet landed rather than proceeding and colliding later.

- [ ] **Step 2: Add the variant table and route both suites through it**

Replace the direct `test_params` use in every `get_channel` call in `tests/channels.cpp` and the
config selection in `tests/communicator.cpp` with `with_variant(...)` under a loop over
`transport_variants()`. In C1 the table has one entry with an empty overlay, so behavior is
unchanged by construction. Keep the case names stable — a variant loop *inside* each case, not
`BOOST_DATA_TEST_CASE`, so every existing case name and the existing `--run_test=` invocations in
the runbooks keep working. Leave Plan A's two pairing cases alone; they do not call `get_channel`.

- [ ] **Step 3: Add the unframed backpressure baseline**

Add to `tests/link_layer.cpp`:

```text
backpressure_baseline_unframed_completes
```

Two ranks over `Direct`, rank 0 sends `B` bytes in `k` messages with **no** interleaved receive on
rank 1 until all sends have returned, where `B` comfortably exceeds the Task 1 default
`window_frames * max_frame_bytes`. It must pass today (kernel socket buffers are effectively
unbounded) and it is the exact regression the framed path must not break — the design spec's
"Known regression risk": bounding buffering at `W` means programs that work now can deadlock.

**Structural requirement, do not skip:** this case must fork, not thread. Most `Channels` cases
already fork with `ForkedRankGuard` (`tests/forked_rank_guard.h`; used at `tests/channels.cpp:141`,
`:177`, `:221`, and ten more — thirteen sites in total), but `sending_receiving` and `sending_receiving_mult_times`
(`tests/channels.cpp:70-128`) run their two peers as OpenMP threads in one process. A
backpressure deadlock in a thread-based case would hang `Boost_Tests_run` itself rather than fail
it. Use `ForkedRankGuard` plus a hard `alarm()` in the child so a future framed regression reports
a timeout instead of wedging the suite. Skip cleanly (not fail) when `127.0.0.1:10000` is not
listening, matching the existing TCPunch-probe convention.

- [ ] **Step 4: Verify the matrix is unchanged (GREEN)**

```bash
cmake --build build --target Boost_Tests_run -j"$(nproc)"
(./extern/TCPunch/server/build-fresh/tcpunchd 10000 &) ; sleep 1
./build/tests/Boost_Tests_run --run_test=Channels,Communicator --log_level=test_suite \
  > /tmp/fmi-matrix-after.txt 2>&1
diff <(grep -o 'Entering test case "[^"]*"' /tmp/fmi-matrix-before.txt) \
     <(grep -o 'Entering test case "[^"]*"' /tmp/fmi-matrix-after.txt)
./build/tests/Boost_Tests_run --run_test=LinkLayer/backpressure_baseline_unframed_completes
./build/tests/Boost_Tests_run
pkill -f 'build-fresh/tcpunchd 10000'
```

Expected: `diff` exits 0 (identical case names and order), the backpressure baseline passes, the
whole suite reports `*** No errors detected`.

- [ ] **Step 5: Commit**

```bash
git add tests/transport_variants.h tests/channels.cpp tests/communicator.cpp tests/link_layer.cpp
git commit -m "test: parameterize channel matrices over transport variants"
```

> **Working-tree caution for this task.** `tests/channels.cpp` and `tests/communicator.cpp` both
> carry pre-existing uncommitted changes (the `ForkedRankGuard` conversion). Read
> `git diff tests/channels.cpp tests/communicator.cpp` before editing and commit only the
> variant-parameterization hunks alongside those changes as they stand — do not revert or
> restructure them.

---

# Stage C2 — Progress engine + framed `Direct`, still under epochs (OUTLINE)

> ⚠️ **COMMITMENT POINT.** Everything through C1 is additive and independently valuable even if
> the engine is never built. C2 takes ownership of the file descriptors, and per the design spec's
> staging map there is no partial retreat from that. Do not expand this outline into tasks until
> every entry criterion below is satisfied and recorded.

**Scope.** The progress engine (or the inline-send alternative — see open question 1) plus a framed
`Direct`, **deliberately still under epoch-qualified pairing names**. The spec is explicit about
why: "The epoch acts as a coarse incarnation, which lets the threading model and retention/replay
be proven without simultaneously swapping the membership authority." Do not touch `ControlPlane`
membership in C2.

### Entry criteria (all required, all recorded in writing before task expansion)

1. **Plan B has reported two-thread dump behavior** on this kernel with criu 4.2: does
   `criu dump` of a process carrying a live engine thread succeed, and what does the restored
   process see? Plan B must also report frozen duration as a function of RSS (the spec's ~0.4 s/GB
   figure needs local confirmation), because survivors keep sending during a dump and frozen
   duration is charged directly against each link's window — the `W`/RSS/dump-duration feedback
   loop in the spec's *Config surface*.
2. **Plan A's identity seam has landed on `main`-track** — the RAII lane-and-instance scope on
   `OperationGuard` (`include/Communicator.h:217-233`, entered from all nine primitives at
   `include/Communicator.h:31, 40, 49, 57, 69, 83, 98, 123, 148`) publishing
   `(lane, op_kind, collective_index, root)`. Without it, framed `Direct` has no identity to write
   into `LinkFrame`, and the per-**Communicator** collective counter (contract 1: it must not live
   on the channel) does not exist.
3. **The inline-send-vs-full-engine benchmark has been run and decided.** The design spec's
   *Decision record* carries an explicitly unevaluated alternative: "app thread performs the send
   inline under a per-link mutex *after* retaining its copy, with the engine owning only ACK
   consumption, replay, repair, and pairing." The spec requires this be benchmarked "**before**
   freezing the request interface", not after the engine is built. Deliverable: p50/p99 latency for
   8 B / 1 KiB / 1 MiB messages, N=2 on loopback with `tcpunchd` on `build-fresh`, both designs,
   plus the CRIU-image size delta. This is C2's first work item, before any engine code.
4. **Task 1's open-item decisions are signed off** and have survived Plan B's measurements —
   specifically, `W` sized against dump duration, not only memory.
5. C1 is fully green: `./build/tests/Boost_Tests_run --run_test=LinkLayer` passes and the
   `Channels`/`Communicator` matrices are byte-identical to their pre-C1 case list (Task 7 Step 4).

### Exit criterion (the vertical slice — this is the whole point of C2)

Two ranks. Rank 0 calls `send()` once; `send()` returns. The connection is then killed. The message
is replayed and delivered correctly to rank 1 **with rank 0 making no further FMI calls at all.**
This is the design spec's delivery obligation stated as a test: "Replay to a restored peer proceeds
while the application computes, with no dependence on the application's next FMI call — a completed
send may be the last time the application ever touches that link." If this test cannot be written,
C2 is not done regardless of how much code exists.

Plus, non-negotiable: **flag-off must be byte-identical to today.** With
`backends.Direct.framed` absent or false, `Direct` must emit exactly the raw byte stream
`src/comm/Direct.cpp:28-72` emits now. Task 7's variant table makes this checkable by running the
full `Channels` + `Communicator` matrix (the count Task 7 step 1 recorded) in both variants.

### Named work items (not yet tasks)

1. Run and record the entry-criterion-3 benchmark. Freeze the request interface only after.
2. Plumb `backends.Direct.framed`. **A blocker was found while writing this plan:** there is no
   path from `fault_tolerance` config to a channel constructor.
   `Channel::get_channel(name, params, model_params)` (`src/comm/Channel.cpp:11-30`) receives only
   the two maps that `Configuration::get_active_channels()` (`src/utils/Configuration.cpp:12-38`)
   flattens out of the backend JSON block, and `Communicator::register_channel`
   (`src/Communicator.cpp:125-130`) then sets only `peer_id`, `num_peers`, and `comm_name` (Plan A
   task 3 adds one more line there, `set_data_comm_name`, but still no link config). So
   `fault_tolerance.link` cannot reach `Direct` today. **Decision required:** inject the link
   parameters through the `params` map (cheap, matches Task 7's variant overlay, but duplicates
   config), or add a `Channel::set_link_config(...)` setter called from `register_channel` (cleaner,
   but touches the `Channel` interface that custom channels implement — the spec already requires
   custom channels to "advertise sequencing, snapshot, retry, and progress capabilities to be
   enabled under FT", so this may be the natural home).
3. Framed `Direct`: `send_object`/`recv_object` (`src/comm/Direct.cpp:28-46`, `:48-72`) route
   through `SequencedLink::admit`/`drain`. Fix EINTR while here — both loops currently treat it as
   fatal, handling only `EAGAIN`/`EWOULDBLOCK` (`src/comm/Direct.cpp:36-43` and `:62-69`) — because
   freeze delivers EINTR to blocking socket calls (spec contract 4). `MSG_NOSIGNAL` is already
   present at `src/comm/Direct.cpp:35`.
4. The progress component itself, per the entry-criterion-3 decision. If it is a thread:
   `pthread_sigmask(SIG_BLOCK, {SIGPIPE})` on that thread only — the repo deliberately avoids a
   process-global `SIG_IGN` (spec contract 4), and `CriuFaultTolerance/
   criu_runtime_leaves_sigpipe_disposition_untouched` already asserts that.
5. Directory-driven repair, still on epochs: the engine polls the epoch directory and proactively
   closes a link on observing a peer's epoch change or a state leaving `ACTIVE`. It must **never**
   wait for EOF, RST, or a timeout (spec contract 2, *Repair is directory-driven*). Required test:
   a survivor repairs while its old socket is still `ESTABLISHED` — the leave-stopped case where
   the peer's sockets stay open and silent.
6. Flip Task 7's variant table to two entries and run the whole `Channels` + `Communicator` matrix
   framed. This is the framed-collective coverage the suites can otherwise never get.
7. Flip `LinkLayer/backpressure_baseline_unframed_completes` to a framed variant. If it deadlocks,
   the window/credit sizing decision from Task 1 is wrong and must be revisited before C3 — this is
   the spec's known regression risk materializing.
8. Decide the disposition of `prepare_for_checkpoint()`. Today `Direct::prepare_for_checkpoint`
   (`include/comm/Direct.h:24`, `src/comm/Direct.cpp:128-130`) just calls `close_sockets()`. Under
   framing it must stop the progress component, snapshot every link, and leave zero sockets — and
   it is the natural implementation site for `quiesce_links()` and the Task 1 quiesce-snapshot
   schema.
9. Record the disposition (do not yet execute) of the three FT tests that assert on the epoch and
   pairing surface: `transparent_migration_cut_timing_stress` (`tests/fault_tolerance.cpp:482`),
   `transparent_migration_cut_timing_stress_redis` (`:490`), and
   `transparent_migration_selective_repair_keeps_survivor_links` (`:501`). The last asserts on
   `Direct::pairing_count()` (`include/comm/Direct.h:37`), whose meaning changes the moment the
   engine owns pairing. Per the design spec these are **rewrites, not controls** — they are built
   on the epoch/promotion surface C3 deletes. Execute the rewrites in C3.
10. **Re-arm `PR_SET_PTRACER` after every restore.** Handed over by Plan B experiment 4, which
   measures the behaviour but may not fix it (`src/ft/TransparentMigrationRuntime.cpp` is outside
   Plan B's file set). Today the `prctl` is issued once, at quiesce time
   (`src/ft/TransparentMigrationRuntime.cpp:155`), so on the rootless path a rank can be migrated
   once and never again. It is currently masked on this host because `/usr/local/sbin/criu` carries
   `cap_sys_ptrace=eip`. Implement per Plan B's recorded verdict: re-arm from the restored process's
   first poll, or from the agent through `/proc`. Required test: two consecutive migrations of the
   same rank under `kernel.yama.ptrace_scope=1` with an uncapped criu.

### Open questions to resolve during C2

- Engine-owns-fd versus inline-send-under-per-link-mutex (entry criterion 3). Everything else in
  C2's shape follows from this answer.
- One progress component per process, or one per `Communicator`? A process may hold several
  communicators; `Direct::sockets` (`include/comm/Direct.h:41`) is per channel instance, and
  `tests/channels.cpp:83` constructs a separate channel object per peer *in the same process*.
- Does the engine share `Direct`'s existing `SO_RCVTIMEO`/`SO_SNDTIMEO` settings
  (`src/comm/Direct.cpp:94-98`), or must those be removed because deadline suspension moves to the
  link state machine (spec contract 3, *Deadline suspension*)?
- The `@epoch=` naming collision. `epoch_comm_name` (`include/ft/Common.h:55-57`) produces
  `base + "@epoch=" + N`, and `Direct::send_object` concatenates it with peer ids without a
  separator (`src/comm/Direct.cpp:29`), so `X@epoch=1` with `(1,0)` and `X@epoch=11` with `(0,1)`
  both yield the pair name `X@epoch=110_1`. Decide whether C2 fixes this in place or leaves it to
  die with epochs in C3 — it is a live bug either way and must not be silently carried.

---

# Stage C3 — Epochs → incarnations (OUTLINE)

> **Irreversible.** This stage deletes the cut machinery. It cannot begin until C2's exit criterion
> is demonstrated and stable across the full framed matrix.

**Scope.** `ControlPlane` v2 schema and Lua scripts implementing design spec contract 3 in full,
per-link deadline suspension, and directory-driven repair keyed on incarnations instead of epochs.

### Entry criteria

- C2's vertical slice passes repeatedly (define the run count when expanding), and the full
  `Channels` + `Communicator` matrix is green in the framed variant.
- Task 1's quiesce-snapshot schema has survived C2 unchanged, or has been amended in the decision
  record.
- Plan B's checkpoint lifecycle is landed, since contract 3's `PAUSING → CHECKPOINTED` transition
  is gated on "image durably staged **and** digest-verified" — an agent-side property. C3 absorbs
  Plan B's shadow `ckpt:` directory (named work item 8); expanding C3 before Plan B lands would mean
  writing the agent-driven edges twice.

### Named work items

1. **Schema v2 and the state machine.** Contract 3's states — `STARTING`, `ACTIVE`, `PAUSING`,
   `CHECKPOINTED`, `RESTORE_RESERVED`, `ACTIVATING` — are **disjoint** from today's
   `FMI::FT::RankState { Active, MigrationPending, Quiesced }` (`include/ft/Common.h:12-16`). Both
   `to_string` (`:20-30`) and `rank_state_from_string` (`:32-43`) throw on an unrecognized value,
   and the header comment states they are "Shared by the C++ control plane and the Python binding
   so the strings can never drift apart" — so `python/PythonFT.cpp` must move in the same commit.
2. **Every Lua script, including the failure edges.** The spec is emphatic: "every state must have
   at least one outbound edge that an external actor can drive", and names `abort_pause`,
   `abort_restore`, and `abort_activation` as non-optional — without them a failed `criu dump`,
   which today's code treats as routine (three attempts then rethrow,
   `src/ft/experimental/LocalRankAgent.cpp:420-437`), wedges the job. `abort_pause` is idempotent
   by `migration_id`; `acquire_restore_lease`'s CAS must accept `RESTORE_RESERVED`-with-expired-lease
   as well as `CHECKPOINTED`. The existing scripts to model on are at `src/ft/ControlPlane.cpp:339`
   (request), `:489` (`observe_operation`), and `:643` (promotion).
3. **Per-link deadline suspension** replacing the global flag. A link in `DEAD`/`REPAIRING`
   suspends its own clock until handshake completes and replay drains. The pause entry clears at
   `ACTIVATING → ACTIVE`, **not** at `commit_restore` — the spec derives this from a real failure:
   releasing suspension at `commit_restore` makes a survivor mid-operation throw `Utils::Timeout`,
   and an exception unwinding a sliced operation poisons the communicator, so a textbook-clean
   migration would intermittently poison the job. Also wire the hook into ClientServer's four poll
   loops (`src/comm/ClientServer.cpp:52` barrier, `:75` download, `:103` reduce, `:154` scan).
   Per the spec's contract 3 ("Plan A records the hook, Plan C wires it"), **Plan A task 6 already
   landed the seam** — a `ClientServer::deadline_suspended()` virtual defaulting to `false`, with
   all four loops routed through it. C3 supplies the override; it does not have to touch the loops.
4. **Directory-driven repair on incarnations**, replacing C2's epoch-keyed version.
5. **Rewrite** `transparent_migration_cut_timing_stress` (`tests/fault_tolerance.cpp:482`),
   `transparent_migration_cut_timing_stress_redis` (`:490`), and
   `transparent_migration_selective_repair_keeps_survivor_links` (`:501`). Per the design spec these
   are rewrites, not controls — do not attempt to keep them passing unchanged.
6. **Carry Plan A's `ControlPlane` change forward.** Plan A task 6 adds an optional
   `data_comm_name` parameter to `clear_job_state` (`include/ft/ControlPlane.h:48`,
   `src/ft/ControlPlane.cpp:424-440`) plus a `<data_comm_name>:*` data-plane sweep, and a
   `data_comm_name` member on `Channel`/`Communicator` that decouples data keys from the
   epoch-qualified pairing name. Under incarnations the epoch qualification disappears entirely, so
   `data_comm_name` becomes the only name — **collapse the two, do not delete the data one.** The
   v2 schema must keep an equivalent job-scoped data-plane cleanup; a rewrite that silently drops it
   reintroduces the reused-communicator-name bug Plan A fixed.
7. **Correct the stale `--tcp-close` claim** in `include/ft/ControlPlane.h:86-92`
   ("which removes the need for criu's `--tcp-close` (and the SIGPIPE hazard …)"). Handed over by
   Plan B task 4, which found it but may not edit that file. It is true only under the
   quiesce-point-dump discipline the spec retires; under contract 4 a rank is frozen mid-syscall and
   both legs carry `--tcp-close`. Rewrite it against Plan B's recorded experiment-2 matrix. If
   `disconnect()` itself becomes dead under per-link deadline suspension, delete it instead.
8. **Absorb Plan B's `AgentDirectory` — this is the one genuine double-implementation in the set,
   and C3 is where it is resolved.** Plan B task 8 implements contract 3's five agent-driven edges
   (`PAUSING → CHECKPOINTED`, `abort_pause`, `acquire_restore_lease`, `commit_restore`,
   `abort_restore`) as CAS'd Lua scripts under a shadow `fmi:ft:<comm>:ckpt:` namespace, because
   `ControlPlane` v2 does not exist when stage B runs. Contract 3 specifies **one** directory under
   `fmi:ft:<comm>:`, so two authorities over the same rank's state must not survive C3. Delete the
   `ckpt:` namespace, move its keys into the v2 schema, and reduce `AgentDirectory` to a thin client
   of the v2 scripts. Plan B's state-machine cases are written against the `AgentDirectory`
   *interface* rather than its Redis keys precisely so they survive this swap — **adopt them as the
   acceptance tests for C3's Lua scripts** instead of writing new ones. The single exception is
   `ckpt_namespace_is_disjoint_from_criu_state`, which asserts on raw key names and dies here.
   Until this item lands, `ControlPlane` v2 must not write the five agent-driven edges itself.
9. **Custom-channel capability advertisement.** The spec's config surface requires that "custom
   channels must advertise sequencing, snapshot, retry, and progress capabilities to be enabled
   under FT". No such surface exists — `Channel` (`include/comm/Channel.h`) has no capability query,
   and `Communicator` enables FT without consulting the channels at all. Add the query and make
   `Communicator` construction reject an FT-enabled config whose active channels do not advertise
   the full set. This is the natural home for the C2 work-item-2 `set_link_config` decision.

### Deleted-symbol inventory (measured, not estimated)

Re-run these before expanding C3; the numbers below are the state at plan-authoring time.

```bash
grep -rl "promote_epoch" . | grep -v '^./build' | grep -v '^./.git' | sort   # 24 files
grep -rl "current_epoch" . | grep -v '^./build' | grep -v '^./.git' | sort   # 11 files
```

**`promote_epoch` — 24 files.**
Production: `include/ft/ControlPlane.h:67`, `src/ft/ControlPlane.cpp`,
`src/ft/TransparentMigrationRuntime.cpp`, `src/ft/experimental/LocalRankAgent.cpp`.
Python binding: `python/fmi_python.cpp`, `python/PythonFT.h`, `python/PythonFT.cpp`.
Tests: `tests/fault_tolerance.cpp`, `tests/criu_fault_tolerance.cpp`,
`tests/migration_counterexample_runner.cpp`, `tests/migration_p2p_cut_counterexample.cpp`.
Runbooks: `runbooks/localstack-python311-redis/orchestrator.py`,
`runbooks/localstack-python311-redis/knative-migration/orchestrator.py`,
`runbooks/local-criu-state-transfer/README.md`.
Docs: `docs/consensus-cut.md`, `docs/fault-tolerance.md`, `new_docs/api-surface.md`,
`new_docs/control-plane.md`, `new_docs/epochs.md`, `new_docs/limits-and-failure-modes.md`,
`new_docs/transparent-migration.md`, `docs/superpowers/specs/2026-05-31-transparent-migration-design.md`,
`docs/superpowers/specs/2026-07-22-migration-counterexamples-design.md`,
`docs/superpowers/plans/2026-07-22-migration-counterexamples.md`.

**`current_epoch` (the Redis meta hash field) — 11 files.**
Production: `src/Communicator.cpp`, `src/ft/ControlPlane.cpp`,
`src/ft/experimental/LocalRankAgent.cpp`.
Shell and orchestration: `runbooks/k8s-criu-node-evacuation/local-two-containers.sh`,
`runbooks/k8s-criu-node-evacuation/local-two-machines.sh`,
`runbooks/k8s-criu-node-evacuation/orchestrator.py`, **`runbooks/local-criu-state-transfer/run-demo.sh:101`**,
**`runbooks/local-criu-state-transfer/run-demo-all.sh:113`** — the last two read
`redis hget "${PREFIX}meta" current_epoch` directly and were **omitted from the superseded plan's
port list**. Both must be in C3's port list.
Docs: `new_docs/control-plane.md`, `new_docs/epochs.md`, `new_docs/transparent-migration.md`.

**Also deleted, also omitted by the superseded plan:**
- `include/ft/Common.h` in its entirety as an epoch surface: `RankState` and its two string
  converters (`:12-43`) and `epoch_comm_name` (`:55-57`). Every backend-visible name is epoch-fenced
  through that one function.
- The rest of the epoch API on `include/ft/ControlPlane.h`: `epoch()` `:84`, `join_epoch` `:80`,
  `mark_rank_quiesced` `:72`, `set_placement` `:75`, `placement_for_rank` `:44`,
  `directory_snapshot` `:45`, `moved_ranks` `:108`, `observe_operation` and its `OperationSnapshot`
  `:136-161`, `register_rank` / `set_rank_state` `:165-166`, and the CRIU blob pair
  `criu_image_put` / `criu_image_get` `:123-125` (which the spec keeps "as a small-image fallback
  for local runbooks" — re-keyed on incarnation, not deleted).
- `Channel::reconfigure_for_epoch` (`include/comm/Channel.h:118-123`, default `false`),
  `Direct::reconfigure_for_epoch` (`include/comm/Direct.h:32-33`, `src/comm/Direct.cpp:132-142`),
  and `Communicator::reconfigure_to_epoch` (`src/Communicator.cpp:97-123`) — the mechanism the
  spec identifies as the ClientServer counter-reset bug. **Re-measure this one after Plan A:** task 3
  adds a third override, `ClientServer::reconfigure_for_epoch` returning `true`, so the "rebuild the
  channel" branch at `src/Communicator.cpp:105-112` is already dead by the time C3 runs. Deleting the
  whole family is then a pure removal rather than a behaviour change — but only if Plan A landed.

### Open questions to resolve during C3

- Does the Python `FTControlPlane` surface keep a compatibility shim for one release, or version in
  the same commit? The header comment on `include/ft/Common.h:18-19` argues for the latter.
- Who owns `dirgen` consumption? The spec says every mutating script bumps it, but not which reader
  is required to observe it.
- Does `RESTORE_RESERVED` lease TTL come from config or is it fixed? The spec specifies SET-NX-EX
  but not the value.

---

# Stage C4 — Integration, `none` contract, fuzz, docs (OUTLINE)

**Scope.** Everything that makes C3 shippable rather than merely landed.

### Entry criteria

- C3 landed; the counterexample corpus runs against the incarnation protocol.

### Named work items

1. **Cooperative `none`-mode contract.** `none` is relabeled: it is not transparent. Per the spec:
   quiesce only at guard boundaries with the guard certifying no-operation-in-flight; a **bounded**
   flush that aborts loudly rather than hanging (ack-on-consume means a `CLOSING` flush could
   otherwise wait on a receive that occurs later in a divergent program); aligned-collective
   applications only; batch-evacuating mutually-communicating `none` ranks is rejected loudly. The
   application-side resume point already exists in the LocalStack flow —
   `runbooks/localstack-python311-redis/worker_core.py:19-23` documents `resume=True` — and the
   docs must stop implying transparency for this mode.
2. **Randomized divergent-schedule fuzz over all nine primitives.** The nine are the
   `OperationGuard`-wrapped entry points on `include/Communicator.h`: `send` `:31`, `recv` `:40`,
   `bcast` `:49`, `barrier` `:57`, `gather` `:69`, `scatter` `:83`, `reduce` `:98`, `allreduce`
   `:123`, `scan` `:148`. The fuzzer must generate genuinely divergent per-rank streams — the
   whole point of the redesign — including the `bcast`/`barrier` inversion from contract 1, and
   must vary the reduction `associative`/`commutative` flags per operation, since
   `left_to_right = !(f.commutative && f.associative)` selects wholly different algorithms
   (`src/comm/PeerToPeer.cpp:36`, `:87`, `:133`).
3. **Rollout-flag removal.** Delete `backends.Direct.framed` and collapse Task 7's variant table
   back to one entry. Decide at this point what governs framing in its absence — note the C2 work
   item 2 blocker: there is currently no plumbing from `fault_tolerance` to a channel constructor,
   so "framing keys on `fault_tolerance.enabled`" requires the setter or the params injection to
   have landed in C2.
4. **Docs.** `docs/consensus-cut.md` → `docs/link-layer.md`, per the spec's *Supersedes* line.
   Inbound links to fix: `docs/fault-tolerance.md:34` and `:50`, and `PLANS.md:152`. The
   `new_docs/` set (`api-surface.md`, `control-plane.md`, `epochs.md`,
   `limits-and-failure-modes.md`, `transparent-migration.md`) all describe epochs and needs a
   disposition decision — rewrite or retire.
5. **Remaining runbook orchestrators.** `runbooks/localstack-python311-redis/orchestrator.py`,
   `runbooks/localstack-python311-redis/knative-migration/orchestrator.py`, and
   `runbooks/k8s-criu-node-evacuation/orchestrator.py`. **Two of these three carry pre-existing
   uncommitted working-tree changes** — rebase the port onto whatever they contain at the time, do
   not overwrite. Plus the two shell demos, `runbooks/local-criu-state-transfer/run-demo.sh` and
   `run-demo-all.sh`. Use `uv` for all Python work.
6. **Corpus acceptance wiring — verify, do not re-implement.** `Classification::LoudFail` in
   `tests/migration_counterexample_scenarios.h`, `aggregate_exit`
   (`tests/migration_counterexample_runner.cpp:1825-1836`) treating it as a pass, and the
   `--group message-identity` selector are **delivered by Plan A task 9**. C4's job is to confirm
   they still hold once membership moves to incarnations, and to add the incarnation-era scenarios
   the group selector does not cover (`future_matching_send_after_cut`,
   `pending_set_expands_after_park`, `failed_operation_advances_boundary`,
   `promotion_before_full_membership`, and the two `Backend::Direct` cases — the exact set Plan A
   deliberately excluded because they are Plan C defects). Re-implementing `LoudFail` here would
   collide with Plan A. Note that the headline gate,
   `tests/migration_p2p_cut_counterexample.cpp`, hardcodes `config/fmi_ft_stress_redis_test.json`
   at `:267-268` (verified: `backends.Direct.enabled=false`, `backends.Redis.enabled=true`,
   `preferred_data_backend="Redis"`), so its mechanism is the **ClientServer** counter reset and it
   flips at **Plan A stage A2**, not at any Plan C milestone. No Plan C stage may gate on it, and
   no Plan C stage may loudly reject ClientServer-under-FT while it remains the headline gate.
7. **`tests/migration_cut_model.cpp` — re-target, disposition already decided.** Plan A task 9
   step 4 settled the spec's open disposition: the model is retargeted to post-A2 semantics and its
   exit code is decoupled from the corpus gate via `--explore --expect
   tests/migration_cut_model_expected.txt`, making it a **change detector** rather than a pass/fail
   oracle. C4 does not re-decide this. C4's job is to re-target the model's transition rules a
   second time, from epochs to incarnations, and regenerate the checked-in expected file — the
   surviving counterexample classes at that point are precisely the ones C2/C3 were built to
   eliminate, so a shrinking expected file is the evidence.

### Final verification recipe (to be expanded when C4 is written)

```bash
cmake -S . -B build -DFMI_BUILD_TESTS=ON -DFMI_ENABLE_CRIU=ON -DFMI_ENABLE_S3=OFF
cmake --build build --target Boost_Tests_run fmi_migration_counterexamples \
  fmi_migration_p2p_cut_counterexample -j"$(nproc)"
redis-cli -h 127.0.0.1 -p 6379 ping
(./extern/TCPunch/server/build-fresh/tcpunchd 10000 &) ; sleep 1
./build/tests/Boost_Tests_run
./build/tests/fmi_migration_counterexamples --list
./build/tests/fmi_migration_counterexamples --backend redis
./build/tests/fmi_migration_counterexamples --backend direct
./build/tests/fmi_migration_p2p_cut_counterexample
pkill -f 'build-fresh/tcpunchd 10000'
git diff --check
git status --short
```
