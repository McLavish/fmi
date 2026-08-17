# TODO

Work items from the `dev` vs `main` growth review of 2026-08-17 (six independent reviews: five
Claude subsystem passes plus a Codex `gpt-5.6-sol` cross-family pass). Line counts are estimates
unless marked *measured*.

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
  migration wall-clock. Note §4.1 first — the missing `writev` costs the sequenced path a whole
  extra TCP segment per message, so benchmarking before that fix measures the bug, not the design.

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
duplicated to achieve that. Measured duplication across the whole core is ~520 code lines, and
roughly 70% of it comes from the single decision that `DrainTCP` derives from `PeerToPeer` rather
than `TcpChannelBase`.

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

- [ ] **Five config-value accessors, four near-identical.** ~38 lines.
  `TcpEndpoint::param_or` (`src/comm/TcpEndpoint.cpp:81`), `RecoverableClientServer::configured`
  (`:18`), `Redis::timeout_param` (`:15`), `S3::required` (`:183`), `S3::numeric_param` (`:192`).
  `timeout_param` and `numeric_param` differ only in clamp-vs-throw and the message prefix;
  `required` and `configured` only in return type.

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
  `LinkFrame::encode_header_checked`, `DrainTCP::set_coordinator_for_testing`,
  `RedisDrainCoordinator::members` + `MemberRecord::decode`. Not deletions — a decision about
  whether the production header should advertise them.

---

## 4. Performance

- [ ] **No `writev`: every framed message costs two TCP segments.** `src/comm/TcpChannelBase.cpp:520`
  issues two separate `write_all` calls, header then payload, with `TCP_NODELAY` set
  unconditionally — so the 72-byte header leaves as its own segment. For the 4–8 byte fragments
  binomial-tree collectives are made of this roughly doubles packet count. Cheap to fix *because*
  the retention copy already exists: assemble header+payload into the retained buffer once and
  issue one `write_all`, or `writev` for the non-retained case. **Do this before §0's benchmark**,
  or the benchmark measures this bug rather than the design.

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

- [ ] **Let a build drop the migration stack while keeping Redis.** *(measured: +448 KB of `.text`
  for a user who enables none of it.)* `src/comm/Channel.cpp:15`'s factory references every
  compiled-in backend, so `DrainTCP.cpp.o` (244 KB), `MigrationTrigger.cpp.o` (66 KB) and
  `DrainCoordinator.cpp.o` (73 KB) link into any application regardless of config. The only escape
  today is `FMI_ENABLE_REDIS=OFF`, which also removes the Redis channel. Wants either a separate
  `FMI_ENABLE_DRAIN` option or a registration scheme the linker can prune.

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
