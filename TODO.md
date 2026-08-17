# TODO

Work items from the `dev` vs `main` growth review of 2026-08-17 (six independent reviews: five
Claude subsystem passes plus a Codex `gpt-5.6-sol` cross-family pass). Line counts are estimates
unless marked *measured*. A second pass later the same day (15 agents: five subsystem
accountants, three cross-cutting analysts, six adversarial verifiers, plus another Codex
`gpt-5.6-sol` sweep) confirmed the totals, added the items marked *(2nd pass)*, and corrected two
attributions in place: the §2 intro (reparenting `DrainTCP` was not the cause of the duplication)
and §5's `.text` figure (129 KB is the drain stack's share, not 448 KB).

**Standing decision: both checkpoint-survival mechanisms stay.** The sequenced-link layer
(retention/replay) and the neighborhood-drain protocol are being kept and benchmarked against each
other under different scenarios. Every "drop one of them" recommendation from the review is
therefore **out of scope** and is not listed below. What *is* listed is everything that makes
carrying both cheaper and more correct — chiefly §2, which is the dedup that the two-mechanism
decision makes load-bearing rather than optional.

Nothing here is a regression introduced by the review; these are pre-existing findings.

---

## 0. The measurement that the two-mechanism decision rests on

- [ ] **Benchmark retention overhead against drain overhead.** Nothing in the tree measures one
  against the other, so the choice between them is currently an argument rather than a number.
  This is now the highest-value item in the file: it is the evidence that justifies carrying both.
  Wants at minimum: per-message wire overhead and latency at small message sizes (binomial-tree
  collectives fragment down to 4–8 bytes), throughput at large sizes, establishment cost, and
  migration wall-clock. Read §4's first item before interpreting small-message results: the
  sequenced path spends an extra TCP segment per message on a header/payload write split that is
  deliberate and not trivially removable, which inflates its small-message cost against drain's.
  That makes an unfixed run an upper bound on sequenced-path cost rather than a wrong one.

---

## 1. Correctness bugs

Fix regardless of any structural decision. Ordered by severity.

- [ ] **Servicing can throw out of `pump`, naming the wrong peer.**
  `src/comm/TcpChannelBase.cpp:1088` — `maybe_send_ack` sits outside the enclosing `try` and calls
  `write_all`. An ack whose socket had 1..71 bytes of room raises while the application is waiting
  on a *different* peer, violating the "servicing must not throw" contract stated at
  `include/comm/TcpChannelBase.h:89`. Fix: `try{}catch(...){}` around the call.

- [ ] **A failed establish-gate acquisition leaves links permanently marked migrating.**
  `src/comm/DrainTCP.cpp:1610` — if `take_establish_gate()` throws, the catch clears
  `drain_pending` but not the `draining` flags already set on links that yielded, and rethrows
  *before* the outer handler that would `break_every_link`. The parked thread escapes only when
  `migration_max_ms` (default 120 s) expires, reporting a peer that never migrated. The two
  failure paths of one function disagree about cleanup.

- [ ] **A drain-armed job treats a normal `finalize()` as an unplanned death.**
  `src/comm/DrainTCP.cpp:1320` — sockets close abruptly with no leave event, so a peer still in
  `recv_object` records a tentative drain, waits `drain_grace_ms` (5 s), then dies with "an
  unplanned death is not recoverable". The same job with `drain: false` ends cleanly. Any
  staggered or asymmetric rank exit pays this per link. `TcpChannelBase` has
  `drain_links_for_shutdown` for exactly this; `DrainTCP` has no equivalent and no "leaving for
  good" event type.

- [ ] **Fork-child mutex is never reset.**
  `src/utils/MigrationTrigger.cpp:112` — `forget_in_child()` documents assigning over a possibly
  inherited-locked mutex but never reconstructs or resets it; `attach`, `detach` and
  `drain_signal` later lock it. A child forked while another thread held it can deadlock.

- [ ] **SIGPIPE ownership contradiction.**
  `src/utils/Signals.cpp:6` vs `src/comm/S3.cpp:32` — `suppress_sigpipe()` treats `SIG_IGN` as
  already owned and leaves it alone; S3 treats both `SIG_DFL` and `SIG_IGN` as unclaimed and
  installs the SDK handler. An application that deliberately set `SIG_IGN` has that process-global
  choice replaced. Pick one owner rule and apply it in both places.

- [ ] **`Utils::BackendFailure` is half-adopted.**
  `src/comm/Redis.cpp:273` and `:347` — the type means "the store answered and the answer is no".
  S3 throws it five times; Redis throws it zero, using plain `std::runtime_error` for the
  identical conditions — including a stored-length mismatch where S3's matching check *does* throw
  `BackendFailure`. A caller catching permanent store refusals catches them on S3 and misses them
  on Redis. ~4 lines.

- [ ] **An oversized message is misreported after burning the full timeout.**
  `src/comm/SequencedLink.cpp:61` — `admit` returns `false` for both "larger than
  `max_frame_bytes`" and "window full". `send_object` reads that as "window full", spends
  `max_timeout` in `drain_acks`, retries, and throws "at its retention limit with 0 bytes
  outstanding and 0 frames unacknowledged". Distinguish the two rejection reasons.

- [ ] **`link_max_frame_bytes` is not configurable.**
  `include/comm/TcpChannelBase.h:224` (16 MiB) — `parse_tcp_params` reads `link_window_frames`,
  `link_retention_limit_bytes` and `link_ack_interval` but not this one, so the cap above cannot
  be raised from config. Same for `max_link_repairs` (`:217`), settable only from a test subclass.

- [ ] **Stuck-rank beacon cannot fire.**
  `src/comm/TcpChannelBase.cpp:384` — `waited` is derived from `deadline_ms - max_timeout`, but two
  of the four call sites pass `min(deadline, now + 100)`, making the value the constant
  `max_timeout - 100`. Under every shipped `max_timeout` the guard is satisfiable at most once per
  operation and never at all at 1000. Its comment calls it "the one thing that makes a mesh
  deadlock diagnosable"; as written it diagnoses nothing. Fix by passing the operation start time
  explicitly, or delete it.

- [ ] **Orphaned-link redial rescue cannot arm.**
  `src/comm/TcpChannelBase.cpp:331` — entry requires `link_suspect_since.size() == num_peers`, but
  that vector is only ever sized inside `service_established_links`' dead-observation branches,
  i.e. by a peek on a *live* fd. The orphan branch exists precisely for links whose fd is already
  `-1`. One line in `ensure_link_state` (`:54`, which sizes `links` and three siblings but not
  this one, nor `link_needs_reconcile`, `link_generation`, `decode_fail_marks`) fixes it.

- [ ] **Latent out-of-bounds: `maybe_send_ack` guards the wrong vector.**
  `src/comm/TcpChannelBase.cpp:88` indexes `sockets[partner_id]` guarded only by
  `partner_id >= links.size()`. `recv_object` calls `ensure_link_state()` (which sizes `links`) and
  can reach `maybe_send_ack` before `check_socket` (which sizes `sockets`). Unreachable today —
  needs a non-empty drain queue with no socket ever established — but the guard tests the wrong
  thing. Same shape at `check_socket:1415`.

- [ ] **`reconcile_if_needed` called from inside a catch handler can escape to the application.**
  `src/comm/TcpChannelBase.cpp:650` — `reconcile_or_repair` (`:1367`) documents this as a confirmed
  adversarial-review finding and guards its second attempt; `send_object` repeats the unguarded
  pattern.

- [ ] **`PeerRegistry` reports every failure as a DirectTCP failure.**
  `src/comm/PeerRegistry.cpp:216`, `:273`, `:284` — hard-coded `"DirectTCP: registry error: ..."`
  even when the caller is `DrainTCP` or `RedisDrainCoordinator`. Take the channel name as a
  constructor argument.

- [ ] **`#define SOL_TCP` inside a function body.**
  `src/comm/TcpChannelBase.cpp:1447` and again in `src/comm/DrainTCP.cpp` — leaks to the rest of
  the translation unit. Move to a header.

- [ ] **`OperationScope` nesting: the comment and the implementation disagree.**
  `include/comm/OperationScope.h:49` (and CLAUDE.md) say nesting is by design and "the outermost
  wins". `src/comm/OperationScope.cpp:12` is a plain save/restore in which the **innermost** wins.
  The stated invariant holds only because no inner scope is ever pushed — it is enforced by
  absence, not by the mechanism. A future `Communicator`-level re-entry would silently make
  `collective_index` count internal fragments. Add an assertion rather than a comment.

---

## 2. Structural duplication

**This section is what the keep-both decision makes mandatory rather than optional.** Two full TCP
transports are being maintained; the establishment machinery underneath them does not have to be
duplicated to achieve that. Measured duplication across the whole core is ~450–650 code lines
(648 implicated, ~447 realistically removable — 2nd-pass count).

**Correction (2nd pass):** an earlier draft blamed ~70% of the duplication on the single decision
that `DrainTCP` derives from `PeerToPeer` rather than `TcpChannelBase`. That attribution was
adversarially checked and **refuted**: `TcpChannelBase` contains no listener, registry, dial or
accept code — all of that lives in the *sibling* `DirectTCP`, behind the pure-virtual
`establish()`, and `DirectTCP` derives from the base yet still writes every line of it. Reparenting
`DrainTCP` would recover only `apply_socket_options`, `get_latency`/`get_price` and the model-param
parse — ~40 lines. The duplication exists because the establishment machinery was never extracted
into a shared helper; the first item below is the fix regardless of parentage.

- [ ] **Extract a shared registered-TCP endpoint.** ~250–360 lines. The biggest single win.
  A `TcpMeshEndpoint` sibling of the existing `TcpEndpoint`/`PeerRegistry` extraction:
  `bind_listener(bind_host) -> {fd, port, nonce, advertised_ip}`, `publish_self`, `lookup_peer`,
  `connect_to`, plus a free `tcp_cost_model()`. Both `DirectTCP` and `DrainTCP` consume it;
  neither data path is touched. Verified duplicate pairs:

  | responsibility | DrainTCP | counterpart |
  |---|---|---|
  | listener socket/bind/listen/getsockname | `DrainTCP.cpp:244` | `DirectTCP.cpp:132` |
  | re-bind listener (a third copy) | `DrainTCP.cpp:1756` | same block as above |
  | `apply_socket_options` — **byte-identical** | `DrainTCP.cpp:426` | `TcpChannelBase.cpp:1438` |
  | `get_latency`/`get_price` — **byte-identical** | `DrainTCP.cpp:1265` | `TcpChannelBase.cpp:1453` |
  | publish `ip:port:nonce` | `DrainTCP.cpp:366` | `DirectTCP.cpp:179` |
  | registry entry parse | `DrainTCP.cpp:395` | `DirectTCP.cpp:720` |
  | non-blocking connect + `POLLOUT` + `SO_ERROR` | `DrainTCP.cpp:441` | `DirectTCP.cpp:300` |
  | accept + hello + refuse | `DrainTCP.cpp:678` | `DirectTCP.cpp:405` |
  | `link_name` | `DrainTCP.cpp:183` | `TcpChannelBase.cpp:265` |
  | `registry_key` | `DrainTCP.cpp:177` | `DirectTCP.cpp:128` |
  | `monotonic_ms` (third copy in tree) | `DrainTCP.cpp:27` | `DirectTCP.cpp:28` |

  **What must NOT be shared:** the data path. `TcpChannelBase`'s `write_all`/`read_all` throw
  `LinkReplaced` and restart at a frame boundary; drain must resume a parked message at its byte
  offset on the replacement connection. Those contracts are genuinely incompatible (see §6.1).

- [ ] **Collapse `DrainTCP::rebind_listener` into the `ensure_started` block.** ~30 lines.
  `src/comm/DrainTCP.cpp:1756` is a near-verbatim copy of `src/comm/DrainTCP.cpp:244` differing
  only in error strings and the pipe setup — and the two copies have *already* diverged in their
  `SO_REUSEADDR` comment. Independently actionable from the item above, zero risk.

- [ ] **One hiredis client, not two.** ~55 lines. `src/comm/PeerRegistry.cpp:203` and
  `src/comm/Redis.cpp:85` both hand-roll `timeval` conversion, `redisConnectWithTimeout` +
  `redisSetTimeout`, the pid/fork guard, free-and-redial, argv marshalling and reply ownership.
  A ~70-line `HiredisConnection` under both. Note the stated justification at
  `include/comm/PeerRegistry.h:18` — "deliberately NOT built like the Redis channel, which
  concatenates commands into a string and passes it as a printf format" — is **stale**:
  `src/comm/Redis.cpp:209` uses `redisCommandArgv`. The real remaining differences (pipelining,
  fatal-vs-tolerant error replies) are policy, not client.

- [ ] **Consolidate the `FMI_LINK_TRACE` scaffolding.** ~240 physical → ~60. Twenty-one blocks in
  `TcpChannelBase.cpp` and ten in `DirectTCP.cpp`, each open-coding its own `getenv`, its own
  `static thread_local` throttle and its own `fprintf`. One `LINK_TRACE(tag, fmt, ...)` macro with
  a shared throttle. Note `DirectTCP.cpp` re-reads `getenv` inline at six sites rather than using
  `TcpChannelBase`'s cached `link_trace()`, because that helper is in an anonymous namespace in
  the `.cpp` and unreachable — itself a sign the extraction stopped one step early.
  *(2nd pass)* Add `SequencedLink.cpp` to the sweep: its trace predicate (`:12`) is identical to
  `TcpChannelBase.cpp:73` but for the function name, and its blocks plus the `first_int` helper
  are another ~42 lines. `DirectTCP::redial_dead_link` is the extreme case — 31 of its 55 code
  lines are trace. Route the two *unconditional* `DRAIN GAP` `fprintf` blocks
  (`TcpChannelBase.cpp:1034`, `:1288`) through the same gate while there.

- [ ] **Five config-value accessors, four near-identical.** ~38 lines.
  `TcpEndpoint::param_or` (`src/comm/TcpEndpoint.cpp:81`), `RecoverableClientServer::configured`
  (`:18`), `Redis::timeout_param` (`:15`), `S3::required` (`:183`), `S3::numeric_param` (`:192`).
  `timeout_param` and `numeric_param` differ only in clamp-vs-throw and the message prefix;
  `required` and `configured` only in return type.

- [ ] **Intra-file and intra-family clones.** *(2nd pass)* ~250 lines across five files, each pair
  verified by diffing the spans:
  - `SequencedLink::accept` re-implements `classify` instead of calling it —
    `src/comm/SequencedLink.cpp:128-137` is **byte-identical** (comments included) to `:157-166`.
  - `DrainTCP::send_object`/`recv_object` (`src/comm/DrainTCP.cpp:1068`/`:1157`) are the same
    per-chunk loop written twice: 58 of send's 61 code lines have a counterpart in recv (draining
    check, `fd < 0` rebuild with `clock.absorb`, one non-blocking syscall, EINTR/EAGAIN,
    drain-armed EPIPE/ECONNRESET, `wait_fd`), differing in `::send` vs `::recv` and the EOF
    branch. One direction-parameterised `move_chunk` helper. ~75 lines.
  - `TcpChannelBase.cpp` internal: `finish_stage`'s dispatch tail (`:1027`) vs the whole-frame
    tail in `service_established_links` (`:1281`); `drain_acks`' stage-advance loop (`:132`) vs
    `read_header_yielding`'s (`:770`); the suspect-mark idiom ×3 (`:1106`, `:1139`, `:1185`); the
    repair-budget throw ×3 (`:719`, `:782`, `:829`). ~53 excess lines.
  - `ClientServer::scan`'s fold + `apply_ready` lambdas (`src/comm/ClientServer.cpp:226-252`) are
    byte-identical to `reduce`'s (`:135-161`) apart from `num_data` vs `num_peers`. ~27 lines.
  - The retry-under-poll-budget upload loop is written once per store backend
    (`src/comm/Redis.cpp:268`, `src/comm/S3.cpp:584`) — it belongs next to `poll_attempts` in
    `RecoverableClientServer` (see the item above). ~15 lines.

- [ ] **Two byte-order helper families with opposite endianness.** ~30 lines.
  `TcpEndpoint::put32/get32/put64/get64` (`src/comm/TcpEndpoint.cpp:12`, big-endian) and
  `LinkFrame::detail::put_u8/16/32/64` + `get_*` (`include/comm/LinkFrame.h:145`, little-endian).
  Same eight functions, same loop shape. One endianness-parameterised family; wire compatibility
  means each protocol keeps its own order.

- [ ] **Duplicated `poll_attempts`.** ~30 lines. `src/comm/Redis.cpp:143` (+ decl
  `include/comm/Redis.h:64`) and `src/comm/S3.cpp:224` — identical `ceil(max_timeout/timeout)`
  arithmetic, identical zero-guard, near-identical justifying comment.
  `RecoverableClientServer` already owns `timeout` and `max_timeout`; this is a 7-line protected
  method there.

- [ ] **Header/implementation comment duplication in `DrainTCP`.** ~150 lines, zero information
  loss. The same rationale appears in full prose in both places: `DrainTCP.h:341` restated at
  `DrainTCP.cpp:655`; `DrainTCP.h:182` at `DrainTCP.cpp:1828`; `DrainTCP.h:298` at
  `DrainTCP.cpp:1091` *and* `:1201`; `DrainTCP.h:318` at `DrainTCP.cpp:1029`; `DrainTCP.h:447` at
  `DrainTCP.cpp:1497`; `DrainTCP.h:520` at `DrainTCP.cpp:1410`. Keep the header copy (it is the
  interface contract), reduce the `.cpp` copy to a one-line pointer.

- [ ] **Merge the two Python reduction dispatchers; reconsider the support-header split.**
  *(2nd pass)* ~46 lines: `get_vec_function` (`python/PythonBindingSupport.h:103-150`) mirrors
  `get_function` (`:83-101`) branch-for-branch (SUM/PROD/MAX/MIN/CUSTOM), differing only by a
  `std::transform` wrapper — one dispatch parameterised on the transform collapses it. The header
  itself is a 154-line verbatim move-and-reformat of 116 lines deleted from
  `PythonCommunicator.h`, has exactly one includer (the file it came from), and carries one
  2-token semantic change (`:146`, the CUSTOM-flags fix). Either fold it back (~40 net lines, one
  fewer file) or give the split a second consumer that justifies it.

---

## 3. Dead and inert surface

Under 1% of the added code *(measured)*, but concentrated and load-bearing on the wire.

- [ ] **The incarnation lineage fence on the sequenced path.** ~124 lines + **16 wire bytes** of
  the 56-byte handshake. Permanently `0` — its producer was the deleted control plane. All three
  lineage branches of `SequencedLink::reconcile()` are unreachable (`0 > 0`), which makes
  `reset_stream()` (`src/comm/SequencedLink.cpp:98`) dead with them. Spans
  `include/comm/LinkFrame.h:324`, `include/comm/SequencedLink.h:171`,
  `src/comm/SequencedLink.cpp:98,228,238,307`, `include/comm/TcpChannelBase.h:258`,
  `src/comm/TcpChannelBase.cpp:45`, `include/comm/Channel.h:103`, `include/Communicator.h:190`,
  `src/Communicator.cpp:31`.
  **Keep `Channel::set_incarnation`** — `DrainTCP` overrides it (`src/comm/DrainTCP.cpp:353`) and
  its incarnation is live, fencing late leave notices against a restored link. Only the
  Communicator→Channel plumbing and the sequenced-side machinery go. A future lineage coordinator
  should reintroduce a complete feature rather than keep half of a removed one alive indefinitely.

- [ ] **`message_id` and `fragment_index`.** ~10 lines, **12 wire bytes**, header **72 → 56**
  (still 8-aligned, no padding needed) — a **22% per-message header reduction** for about ten
  lines of work. `message_id` (`include/comm/LinkFrame.h:96`) is written twice to the same value
  as `transport_seq` and read by nothing; `fragment_index` (`:97`) is never set to a non-zero
  value anywhere and implies a fragmentation feature the library does not have.

- [ ] **`HandshakePayload::policy_fingerprint`.** ~5 lines, **8 wire bytes**.
  `include/comm/LinkFrame.h:323` — documented as a backend-policy mismatch detector; the only
  production caller (`src/comm/TcpChannelBase.cpp:538`) passes the default `0` and `reconcile()`
  never compares it. The protection is nonfunctional as shipped.

- [ ] **`HandshakePayload::next_send_seq` and `lowest_retained` decide nothing.** *(2nd pass)*
  **16 wire bytes** of the 56-byte handshake. The *transmitted* copies are read only by the
  decode-time sanity check of each other (`include/comm/LinkFrame.h:390`, reject if
  `lowest_retained > next_send_seq`) and a trace line (`src/comm/SequencedLink.cpp:273`);
  `reconcile()` decides everything from `peer.next_expected_seq` alone. (The *local* methods of
  the same names are load-bearing for replay — `src/comm/TcpChannelBase.cpp:559` — it is only the
  wire copies that carry no decision.) With this pair, the incarnation pair and
  `policy_fingerprint`, 40 of the 56 handshake bytes are dead: only 14 influence any outcome
  *(measured)*. Either cross-check them against the replay bounds for real, or stop sending them.

- [ ] **`ack_safe_seq` is a shadow of `next_recv`.** *(2nd pass)* ~12 lines plus a wrong class
  doc. Assigned `= next_recv` at both production write sites (`src/comm/SequencedLink.cpp:147`,
  `:182`) and zeroed with it (`:106`); the only distinct assignment is the test-only `seed()`
  (`:306`). Read only by `snapshot()` and the `ack_safe()` accessor — no production caller. The
  class doc (`include/comm/SequencedLink.h:22`) presents it as an independent "highest seq the
  peer may prune" watermark, i.e. it documents state the code does not maintain separately.
  Either maintain it for real (it becomes meaningful the day delivery and durable custody
  diverge) or delete the member and fix the doc.

- [ ] **The drain coordinator's members hash is write-only in production.** *(2nd pass)* ~74
  lines plus one Redis round trip per arm and per restore. `publish_member` runs at arm
  (`src/comm/DrainTCP.cpp:311`) and after every restore (`:1814`), `remove_member` at finalize
  (`:1337`) — but nothing in `src/` ever calls `DrainCoordinator::members()`; the only reader is
  a test fake (`tests/drain_migration.cpp:530`), and real peer discovery goes through the
  transport registry (`lookup_peer` on `fmi:drain:<comm>` — a *different* key from
  `...:members`). `MemberRecord::encode`/`decode`, `members()` and `members_key()` are dead in
  production. Either make batch planning read it (the plausible intended consumer) or delete the
  hash and its three interface slots.

- [ ] **`DrainTCP::LinkState::generation`.** ~20 lines. `include/comm/DrainTCP.h:163` — incremented
  at `src/comm/DrainTCP.cpp:669,1394,1564,1934`, **never read**. Two comments
  (`DrainTCP.h:165`, `DrainTCP.cpp:1149`) assert a generation check that does not exist;
  correctness actually comes from re-reading `l.fd` under the lock. Either delete the counter and
  correct both comments, or implement the check they describe. Inherited from
  `TcpChannelBase::link_generation`, where it *is* read and *is* load-bearing.

- [ ] **Symbols with zero references anywhere, tests included.** ~40 lines total.
  `SequencedLink::accept_inline` (`include/comm/SequencedLink.h:153`), `::last_ack_sent` (`:98`),
  `::configuration` (`:218`); `Channel::current_operation` (`include/comm/Channel.h:144`);
  `DirectTCP::connection_count` (`include/comm/DirectTCP.h:37`) and its backing
  `total_connections` atomic; `MigrationTrigger::running` (`include/utils/MigrationTrigger.h:133`);
  `DrainTCP::drained_peers` (`include/comm/DrainTCP.h:653`);
  `DrainParticipant::peer_is_leaving` (`include/comm/DrainProtocol.h:112`);
  `OperationIdentity::valid` (`include/comm/OperationScope.h:38`);
  `S3::delete_object` (`src/comm/S3.cpp:609`, unreachable — `ClientServer::delete_objects` is
  overridden at `S3.cpp:628`); `MigrationTrigger::drain_signal()` (no C++ caller).

- [ ] **Never-exercised `targets` selectivity.** ~20 lines. `include/comm/DrainProtocol.h:95` and
  the `to_seal` filtering at `src/comm/DrainTCP.cpp:1671` — every caller passes `{}`. Either drive
  it from somewhere or drop the parameter.

- [ ] **`attachments` refcounting.** ~10 lines. `src/utils/MigrationTrigger.cpp:126,148,206` —
  `attach` is reachable exactly once per process by construction (a second armed drain channel is
  already a `std::logic_error`).

- [ ] **Decide on the test-only API surface.** ~27 lines shipped in production headers and used
  only by `tests/`: `SequencedLink::snapshot`/`seed`, `::ack_safe`, `::known_peer_incarnation`,
  `LinkFrame::encode_header_checked`, `DrainTCP::set_coordinator_for_testing` (the
  `RedisDrainCoordinator::members`/`MemberRecord` pair has moved to its own item above). Not
  deletions — a decision about whether the production header should advertise them.

- [ ] **`S3::note_transient_failure` keeps counters the flag-off path never prints.** *(2nd
  pass)* ~6 lines. Under `!recover` it increments `consecutive_transient_failures` and stamps
  `failing_since` "for the log line" (`src/comm/S3.cpp:430`), but the only flag-off transient log
  prints `describe()` and neither counter (`:475`). Print the count in that warning or stop
  maintaining the counters flag-off.

---

## 4. Performance

- [ ] **Every framed message costs two TCP segments.** `src/comm/TcpChannelBase.cpp:520` —
  `write_frame` issues two `write_all` calls, header then payload, with `TCP_NODELAY` set
  unconditionally, so the 72-byte header leaves as its own segment. For the 4–8 byte fragments
  binomial-tree collectives are made of, this roughly doubles packet count.

  **The split is deliberate and the obvious fix is wrong.** Do not assemble header and payload into
  one buffer:
  - The header is **re-encoded per send**. On replay, `src/comm/TcpChannelBase.cpp:563` rewrites
    `frame.header.cumulative_ack` to the *current* receive watermark before handing the frame to
    `write_frame`. `SequencedLink::admit` (`src/comm/SequencedLink.cpp:70`) therefore retains the
    header as a struct and the payload as bytes, deliberately unencoded. A pre-encoded blob would
    replay a stale ack — under-reporting the watermark, stalling the peer's retention release, and
    walking into the binomial-tree deadlock that releasing retention on write-only links exists to
    prevent.
  - The first send is **zero-copy on the payload**: `write_frame(rcpt_id, header, buf.buf)`
    (`:621`, `:646`) sends straight from the application's buffer, not from the retained copy.
    Assembling into a contiguous buffer would add a full-message memcpy to every send to save one
    syscall — worse at any size that matters.

  The remedy that preserves both properties is a vectored or coalesced write: `MSG_MORE` on the
  header write (smallest change — a flag threaded through the existing scalar `write_all` loop) or
  `writev` with two iovecs. `writev` is the tidier shape but not trivial: `write_all` is a
  partial-write loop carrying generation and freeze checks, and a vectored version has to advance
  across an iovec boundary correctly.

  **Weigh this against §0 rather than blocking on it.** It inflates the sequenced path's small-message
  cost relative to drain's, so a benchmark run before the fix overstates the gap — but it is a fixed
  per-message overhead, so a run *with* it is still a valid upper bound on sequenced-path cost, and
  the measurement is worth having either way.

- [ ] **Retention copy is unconditional and unpooled.** `src/comm/SequencedLink.cpp:74` —
  `r.payload.assign(...)` is one heap allocation plus a full payload memcpy into a
  `std::vector<char>` inside a `std::deque` node. Unavoidable in principle (the delivery obligation
  must outlive the caller's buffer), but a 256-frame window at MiB scale churns the allocator on
  every collective.

- [ ] **The serviced receive path copies three times.** `src/comm/TcpChannelBase.cpp:1277` allocates
  and reads into a vector, `SequencedLink::accept` (`:141`) copies it into `Committed.payload`, and
  `deliver_into` (`:209`) memcpys out. The inline path is correctly zero-copy; only frames arriving
  while the app is elsewhere pay this.

- [ ] **A duplicate payload is discarded through a full-size allocation.**
  `src/comm/TcpChannelBase.cpp:910` — drain into a fixed scratch buffer instead.

- [ ] **`pump` slices unconditionally at 20 ms.** `src/comm/TcpChannelBase.cpp:307` — for `Direct`
  (no `service_transport` override, `recover_links` off by default) both serviced calls are no-ops,
  so a rank waiting on a recv wakes ~50×/s where vanilla issued one blocking `recv`. Guard the
  slicing on "there is servicing to do". Also `pump` runs a `steady_now_ms()` + modulo per
  iteration for the stuck-rank diagnostic that §1 shows cannot fire.

- [ ] **`DirectTCP::service_transport` issues an unconditional `accept()` per pump iteration.**
  `src/comm/DirectTCP.cpp:496` — regardless of flags, making three syscalls per blocked wait where
  vanilla made one.

---

## 5. Config and build

- [ ] **Move the six test-only configs under `tests/`.** ~440 lines *(measured)*. They have no
  production reference: `config/fmi_directtcp_test.json`, `fmi_draintcp_test.json`,
  `fmi_framed_test.json`, `fmi_framed_fast_test.json`, `fmi_raw_fast_test.json`,
  `fmi_identity_test.json`. `fmi_framed_test.json` and `fmi_framed_fast_test.json` are 73-line
  files differing on **exactly one line** (`max_timeout` 8000 vs 1500). One base file plus per-suite
  overrides, or build the map in test setup.

- [ ] **Strip dead `model` blocks from configs.** ~140 lines. `src/utils/Configuration.cpp:24`
  `continue`s on a disabled backend *before* `:28` reads its model sub-tree, yet every new config
  carries full `model` entries for backends it never enables — ~20 dead lines × 7 files.

- [ ] **Let a build drop the migration stack while keeping Redis.** `src/comm/Channel.cpp:15`'s
  factory references every compiled-in backend, so `DrainTCP.cpp.o` (244 KB on disk),
  `MigrationTrigger.cpp.o` (66 KB) and `DrainCoordinator.cpp.o` (73 KB) link into any application
  regardless of config, and drag `Threads::Threads` with them. **Corrected numbers (2nd pass,
  measured):** those three TUs are ~129 KB of `.text` (~160 KB of allocated sections) at `-O2`;
  the earlier +448 KB figure is the *whole branch's* static-link `.text` growth, most of which
  (sequenced links, DirectTCP, store recovery) an `FMI_ENABLE_DRAIN` option would not remove. The
  only escape today is `FMI_ENABLE_REDIS=OFF`, which also removes the Redis channel. Wants either
  a separate `FMI_ENABLE_DRAIN` option (~15 lines of CMake plus one `#if`; benchmark builds keep
  it ON) or a registration scheme the linker can prune.

- [ ] **`FMI_ENABLE_REDIS`'s help string is wrong.** `CMakeLists.txt` — it says "Enable the Redis
  backend" but actually gates hiredis and everything depending on it: `Redis.cpp`, `DirectTCP.cpp`,
  `DrainTCP.cpp`, `DrainCoordinator.cpp`, `PeerRegistry.cpp`, `MigrationTrigger.cpp` — three of
  five backends. The coupling is real and correctly expressed; only the label misleads.

- [ ] **`object_ttl_s` on S3 logs where it would otherwise throw.** `src/comm/S3.cpp:207` —
  `numeric_param` deliberately throws on an out-of-range value, reasoning that a bad number is a
  typo more often than a request. `object_ttl_s` on an S3 `recover` channel is exactly that case
  and gets a `warning` instead — one a Lambda's stdout may discard, leaving objects billed forever.

---

## 6. Documentation

- [ ] **Trim the comment layer to its contracts.** *(2nd pass)* The single largest non-code cost
  in the diff: 3,172 of the ~10.7k added core lines are pure comment — 30%, *measured twice
  independently* (the Claude and Codex sweeps agree within 5 lines) against `main`'s baseline
  ratio of ~10% (headers: 0.41 comment lines per code line on `main`, 1.42 in the added code).
  Keep interface contracts and the notes that record measured failures (the 403-vs-404
  `ListBucket` trap, `requestTimeoutMs` being curl low-speed time, the R9 stall analysis); move
  incident narrative and rejected alternatives into `docs/superpowers/specs/`. Measured worst
  offenders: `DrainTCP.h` 424 of 693 lines (61% — and 79% of that prose documents *private*
  members, e.g. a 19-line essay on one bool parameter at `:338`); `TcpChannelBase.h` 295 comment
  vs 108 code (seven runs ≥ 12 lines, the longest 33 at `:128`); `TcpChannelBase.cpp` 351;
  `S3.cpp` 303 of 661; `DirectTCP.cpp` 209; `RecoverableClientServer.cpp` 101 of 226, including
  a 28-line rationale on a `finalize()` whose recover-path body is empty (`:152`). Realistic
  reduction: 900–1,200 lines with no information loss — more if §2's header/implementation
  comment-dedup item lands first. This also corrects the headline growth optics: code-only
  growth is ~4.6×, not the raw 5.7×.

- [ ] **`DrainTCP.h:26`'s rationale is half false.** It gives two reasons for not deriving from
  `TcpChannelBase`. The first is **true** and is the real constraint: the base's read/write loops
  throw `LinkReplaced` and restart at a frame boundary, where drain must resume at a byte offset —
  genuinely incompatible mid-message replacement contracts. The second — "its framing and retention
  machinery is also precisely the per-message cost this protocol exists to avoid" — is **false as
  written**: both flags default off (`include/comm/TcpChannelBase.h:183`) and `send_object`
  branches to `write_all` before any frame construction, so an unconfigured `TcpChannelBase`
  already has zero added wire bytes and no retention. Rewrite to keep the true half; it is what
  scopes §2's extraction to establishment only.

- [ ] **Stale comments in `TcpChannelBase`.** `src/comm/TcpChannelBase.cpp:459` says "MSG_WAITALL
  under SO_RCVTIMEO can legitimately return a partial chunk" — the code uses `MSG_DONTWAIT` and
  never `MSG_WAITALL`. `:111` says a partial ack's remainder is "bounded by SO_SNDTIMEO" —
  `write_all` is `MSG_DONTWAIT` with its own deadline, so `SO_SNDTIMEO` is dead for every
  peer-socket path except DirectTCP's blocking hello.

- [ ] **Stale API docs in `DirectTCP.h`.** `:145` documents an `expect_sender` parameter the
  signature does not have; `:121` has two leftover summary lines for `accept_one`.

- [ ] **`LinkFrame.h:72`** claims `encode_header_checked()` "asserts the two agree" — it contains
  no assertion (`:224`).

- [ ] **`PeerRegistry.h:18`** describes itself as "a minimal Redis client for the peer registry",
  but lines 74–190 (`xadd`, `tail_id`, `xread_after`, `set_nx_px`, `del_if_equal`, ~117 lines, 40%
  of the file) exist solely for `DrainCoordinator` and have no DirectTCP consumer. The class is
  60/40 two unrelated concerns under one name.

---

## Reference

Full review write-up: <https://claude.ai/code/artifact/cc15e794-d3f1-45bd-bbee-482c2cd230d7>

Vanilla-era defects found by the same review are **not** listed here — they were fixed on six
single-commit branches off `main` for upstreaming: `fix/direct-partial-io`,
`fix/clientserver-scan-oob`, `fix/clientserver-reduction-order`, `fix/s3-list-pagination`,
`fix/python-custom-reduction-flags`, `fix/scan-counter-advance`. See CLAUDE.md, "Correctness fixes
carried on this branch".
