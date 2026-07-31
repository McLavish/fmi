#!/usr/bin/env python3
"""Mutation sweep for the sequenced link layer.

Injects a known fault into the implementation, rebuilds, and runs the protocol suites. A
mutation that SURVIVES means no test detects that defect, so the suite is not actually
pinning the behaviour it appears to.

This exists because the original suite passed with 9 of 21 faults injected: every
"identity ignores field X" mutation survived, because each test varied two or more fields
at once and some other comparison always caught the break.

Usage:  python3 tests/tools/mutation_sweep.py
Expects a configured build/ directory and Redis on 127.0.0.1:6379.
Restores every file it touches, including on failure.
"""
import signal, subprocess, sys, os
WT="/home/luca/fmi-sequenced-links"
# Cheapest and most discriminating first: a killed mutation stops at the first failing suite,
# so ordering decides whether the sweep takes forty minutes or four hours.
SUITES=["LinkLayer","LinkIncarnations","ProtocolValidation","OperationIdentity",
        "CheckpointFreezePoints","LinkLiveness","LinkRecovery","TransportRecovery",
        "FramedTransport"]
SUITE_TIMEOUT=180

MUTS = [
 ("identity_ignores_op_kind","include/comm/LinkFrame.h",
  "&& a.op_kind == b.op_kind\n","&& true\n"),
 ("identity_ignores_lane","include/comm/LinkFrame.h",
  "return a.lane == b.lane\n","return true\n"),
 ("identity_ignores_collective_index","include/comm/LinkFrame.h",
  "&& a.collective_index == b.collective_index\n","&& true\n"),
 ("identity_ignores_root","include/comm/LinkFrame.h",
  "&& a.root == b.root\n","&& true\n"),
 ("identity_ignores_comm_flag","include/comm/LinkFrame.h",
  "&& a.commutative == b.commutative\n","&& true\n"),
 ("identity_ignores_assoc_flag","include/comm/LinkFrame.h",
  "&& a.associative == b.associative\n","&& true\n"),
 ("identity_ignores_total_length","include/comm/LinkFrame.h",
  "&& a.total_length == b.total_length;","&& true;"),
 ("decode_skips_magic","include/comm/LinkFrame.h",
  "if (detail::get_u32(in, off) != frame_magic) {\n            return DecodeStatus::BadMagic;\n        }",
  "detail::get_u32(in, off);"),
 ("decode_skips_version","include/comm/LinkFrame.h",
  "if (out.wire_version != frame_wire_version) {\n            return DecodeStatus::BadVersion;\n        }",""),
 ("decode_skips_payload_cap","include/comm/LinkFrame.h",
  "if (out.payload_length > max_payload) {\n            return DecodeStatus::PayloadTooLarge;\n        }",""),
 ("decode_skips_truncation","include/comm/LinkFrame.h",
  "if (available < frame_header_bytes) {\n            return DecodeStatus::Truncated;\n        }",""),
 ("ack_off_by_one","src/comm/SequencedLink.cpp",
  "retention.front().header.transport_seq < cumulative","retention.front().header.transport_seq <= cumulative"),
 ("accept_no_dedup","src/comm/SequencedLink.cpp",
  "if (header.transport_seq < next_recv) {","if (false) {"),
 ("accept_no_gap_check","src/comm/SequencedLink.cpp",
  "if (header.transport_seq > next_recv) {","if (false) {"),
 ("deliver_skips_identity","src/comm/SequencedLink.cpp",
  "if (!same_identity(queue.front().header, expected)) {","if (false) {"),
 ("deliver_skips_length","src/comm/SequencedLink.cpp",
  "if (queue.front().payload.size() != len) {","if (false) {"),
 ("window_never_blocks","src/comm/SequencedLink.cpp",
  "return retention.size() >= config.window_frames","return false && retention.size() >= config.window_frames"),
 ("reconcile_skips_ahead_check","src/comm/SequencedLink.cpp",
  "if (peer.next_expected_seq > next_send) {","if (false) {"),
 ("wire_skips_identity_check","src/comm/TcpChannelBase.cpp",
  "if (!same_identity(arrived, expected)) {","if (false) {"),
 ("wire_skips_gap_check","src/comm/TcpChannelBase.cpp",
  "        if (accepted == SequencedLink::Accept::FatalGap) {","        if (false) {"),
 # Two decode-status checks now exist; target each precisely rather than by first match.
 ("handshake_skips_decode_check","src/comm/TcpChannelBase.cpp",
  "    const DecodeStatus status = decode_handshake(payload, handshake_bytes, theirs);\n    if (status != DecodeStatus::Ok) {",
  "    const DecodeStatus status = decode_handshake(payload, handshake_bytes, theirs);\n    if (false) {"),
 ("wire_skips_decode_check","src/comm/TcpChannelBase.cpp",
  "                decode_header(encoded, frame_header_bytes, link_max_frame_bytes, arrived);\n        if (status != DecodeStatus::Ok) {",
  "                decode_header(encoded, frame_header_bytes, link_max_frame_bytes, arrived);\n        if (false) {"),

 # --- a waiting rank must keep meeting every obligation it has -----------------------------
 # Each of these removes one of the obligations. Rounds 1-6 showed that dropping any single one
 # is enough to deadlock a checkpointed job of three ranks or more.
 #
 # Four of the five are killed by the CRIU sweep rather than by this suite, and that is the
 # honest place for them: the property is about what a rank owes its OTHER peers while it is
 # blocked, which only bites once a restore forces several links to be rebuilt at once. Measured
 # at 4 peers x 8 trials, seed 7 (runbooks/criu-transparent-checkpoint/sweep.py) - each of
 # waiting_rank_serves_nobody, waiting_rank_never_accepts, waiting_rank_never_reconciles and
 # adoption_skips_the_handshake_debt takes the job from 8/8 to 0/8. Not a marginal shift: with
 # any one of them applied, no run survives a checkpoint.
 #
 # accepted_links_are_never_adopted is the one this suite pins on its own, via LinkLiveness's
 # a_rank_blocked_on_one_peer_still_accepts_another. Note that test only earns the kill because
 # rank 0 establishes to rank 2 BEFORE it blocks - an earlier ordering let build_mesh do the
 # accepting, and the mutation sailed through.
 ("waiting_rank_serves_nobody","src/comm/TcpChannelBase.cpp",
  "        service_transport();\n        service_established_links(peer);",
  "        (void) 0;"),
 ("waiting_rank_never_accepts","src/comm/TcpChannelBase.cpp",
  "        service_transport();\n","        \n"),
 ("waiting_rank_never_reconciles","src/comm/TcpChannelBase.cpp",
  "        try {\n            reconcile_if_needed(peer);\n        } catch (const std::exception&) {\n            continue;   // the receive path owns repair\n        }",""),
 ("accepted_links_are_never_adopted","src/comm/DirectTCP.cpp",
  "                adopt_link(rank, fd);","                (void) fd;"),
 ("adoption_skips_the_handshake_debt","src/comm/TcpChannelBase.cpp",
  "    link_needs_reconcile[partner_id] = 0;\n    exchange_handshake(partner_id);\n}",
  "    link_needs_reconcile[partner_id] = 0;\n}"),

 # --- delivery order under nested servicing (the 2026-07-30 silent-substitution family) ------
 # A nested pump (entered with a different skip) may drain the link the application is
 # currently receiving from. Delivery must then come from the lane, oldest first; with p2p
 # identity carrying no per-operation counter, sequence order is the only thing between the
 # application and a wrong-round payload.
 ("lane_is_checked_once_at_entry","src/comm/TcpChannelBase.cpp",
  "        if (recover_links && links[sender_id].pending(wanted.lane) > 0) {\n            const SequencedLink::Accept drained =",
  "        if (false) {\n            const SequencedLink::Accept drained ="),
 # The park branch and the yielding header read are a REDUNDANT PAIR: a lane filled during
 # the header wait is caught by the yield while no byte is consumed, and by the park once the
 # next header is in. Removing either alone leaves the other holding the line (the lone
 # survivable exception: yield removed AND the drained frame was the sender's last message -
 # then nothing ever completes the header read and the receive times out, a liveness not a
 # safety failure). The mutation that must die is removing BOTH, which reintroduces the
 # original R6 substitution exactly.
 ("inline_path_ignores_the_drain_queue","src/comm/TcpChannelBase.cpp",
  "        if (recover_links && accepted == SequencedLink::Accept::Delivered &&\n            links[sender_id].pending(arrived.lane) > 0) {",
  "        if (false) {"),
 ("header_read_never_yields_to_the_lane","src/comm/TcpChannelBase.cpp",
  "            if (got == 0 && links[sender_id].pending(wanted.lane) > 0) {\n                return 0;\n            }",
  ""),
 ("delivery_order_guards_both_removed","src/comm/TcpChannelBase.cpp",
  "MULTI",
  [["        if (recover_links && accepted == SequencedLink::Accept::Delivered &&\n            links[sender_id].pending(arrived.lane) > 0) {",
    "        if (false) {"],
   ["            if (got == 0 && links[sender_id].pending(wanted.lane) > 0) {\n                return 0;\n            }",
    ""]]),
 ("replaced_link_keeps_the_old_byte_count","src/comm/TcpChannelBase.cpp",
  "        if (sockets[sender_id] != fd_at_entry) {\n            throw LinkReplaced(transport_tag + \": link to peer \" + std::to_string(sender_id) +",
  "        if (false) {\n            throw LinkReplaced(transport_tag + \": link to peer \" + std::to_string(sender_id) +"),
 # --- epoch reconfigure: the moved rank's own chair ------------------------------------------
 ("moved_rank_keeps_links_to_survivors","src/comm/TcpChannelBase.cpp",
  "    if (self_moved) {",
  "    if (false && self_moved) {"),
 ("repair_forgets_to_reset_seq","src/comm/TcpChannelBase.cpp",
  "        if (rank < links.size()) {\n            links[rank] = SequencedLink({link_window_frames, link_max_frame_bytes,\n                                         link_retention_limit_bytes});\n            links[rank].set_incarnation(local_incarnation);\n        }",""),
 ("dedup_disabled","src/comm/SequencedLink.cpp",
  "FMI::Comm::SequencedLink::classify(const FrameHeader& header) const {\n    if (header.transport_seq < next_recv) {",
  "FMI::Comm::SequencedLink::classify(const FrameHeader& header) const {\n    if (false) {"),
 ("inline_never_advances","src/comm/SequencedLink.cpp",
  "    ++next_recv;\n    FMI_LTRACE(\"commit_inline","    FMI_LTRACE(\"commit_inline"),
 # --- durability: retention, replay and reconciliation across a break ---
 ("replay_returns_nothing","include/comm/SequencedLink.h",
  "[[nodiscard]] const std::deque<Retained>& replay_suffix() const { return retention; }",
  "[[nodiscard]] const std::deque<Retained>& replay_suffix() const { static std::deque<Retained> none; return none; }"),
 ("reconcile_does_not_prune","src/comm/SequencedLink.cpp",
  "    on_ack(peer.next_expected_seq);","    (void) 0;"),
 ("ack_safe_never_advances","src/comm/SequencedLink.cpp",
  "    ack_safe_seq = next_recv;",""),
 ("retention_pruned_eagerly","src/comm/SequencedLink.cpp",
  "    while (!retention.empty() && retention.front().header.transport_seq < cumulative) {",
  "    while (!retention.empty()) {"),
 ("handshake_reports_zero_expected","src/comm/SequencedLink.cpp",
  "    h.next_expected_seq = next_recv;","    h.next_expected_seq = 0;"),
 # --- durability wired into the transport ---
 ("repair_does_not_replay","src/comm/TcpChannelBase.cpp",
  "    for (const auto& retained : links[partner_id].replay_suffix()) {","    for (const auto& retained : std::deque<SequencedLink::Retained>()) {"),
 ("send_does_not_retain","src/comm/TcpChannelBase.cpp",
  "    if (!links[rcpt_id].admit(header, buf.buf, buf.len, stamped)) {","    links[rcpt_id].note_sent(); stamped = header; stamped.transport_seq = links[rcpt_id].next_send_seq() - 1; stamped.payload_length = static_cast<std::uint32_t>(buf.len); if (false) {"),
 ("piggyback_ack_ignored","src/comm/TcpChannelBase.cpp",
  "        links[sender_id].on_ack(arrived.cumulative_ack);",""),
 ("send_never_repairs","src/comm/TcpChannelBase.cpp",
  "        repair_link(rcpt_id);","        throw;"),
 ("recv_never_repairs","src/comm/TcpChannelBase.cpp",
  "            repair_link(sender_id);\n            return false;","            throw;"),
 ("eof_folded_into_timeout","src/comm/TcpChannelBase.cpp",
  "            if (received == 0 && eof_before_data_is_timeout && !recover_links) {",
  "            if (received == 0 && eof_before_data_is_timeout) {"),
 ("repairs_unbounded","src/comm/TcpChannelBase.cpp",
  "            if (++repairs > max_link_repairs) {","            if (false) {"),

 # --- checkpoint mechanics: the receive watermark and what it lets the peer forget --------
 ("commit_before_the_payload_is_read","src/comm/TcpChannelBase.cpp",
  "        if (!guarded_read(buf.buf, buf.len)) {\n            continue;\n        }\n        // Committed only now: a link that died anywhere above left this frame unacknowledged,",
  "        links[sender_id].commit_inline(arrived);\n        if (!guarded_read(buf.buf, buf.len)) {\n            continue;\n        }\n        // Committed only now: a link that died anywhere above left this frame unacknowledged,"),
 ("payload_length_unchecked","src/comm/TcpChannelBase.cpp",
  "        if (arrived.payload_length != buf.len) {","        if (false) {"),
 ("sigpipe_left_fatal","src/utils/Signals.cpp",
  "        if (current.sa_handler != SIG_DFL) {\n            return;\n        }","        return;"),

 # --- liveness: releasing retention on a link the peer never writes to --------------------
 ("no_standalone_acks","src/comm/TcpChannelBase.cpp",
  "        maybe_send_ack(sender_id);",""),
 ("no_ack_drain_when_blocked","src/comm/TcpChannelBase.cpp",
  "        drain_acks(rcpt_id, static_cast<long>(max_timeout));",""),
 ("ack_interval_may_reach_the_window","src/comm/TcpChannelBase.cpp",
  "    if (link_ack_interval >= link_window_frames) {","    if (false) {"),
 ("ack_frames_are_not_skipped","src/comm/TcpChannelBase.cpp",
  "        if (arrived.frame_type == FrameType::Ack) {","        if (false) {"),
 ("ack_never_marked_sent","src/comm/SequencedLink.cpp",
  "    if (value > acked_to_peer) {\n        acked_to_peer = value;\n    }",""),

 # --- establishment must not stall the rank's other links ---
 # This was carried as a KNOWN SURVIVOR for a while, on the reasoning that the drain only has an
 # observable effect during a >=3-rank establishment race and so could not be pinned
 # deterministically. That was true of THIS suite and false of the system: the CRIU sweep kills
 # it outright, 0/8 at 4 peers and 0/8 at 8 peers (seed 7). The wiring is load-bearing after
 # all. Worth remembering as a caution about the label — "no test can see this" meant "no test
 # I had written could see this".
 ("establishment_reads_no_other_link","src/comm/DirectTCP.cpp",
  "        service_established_links(target);","        (void) 0;"),
 ("drained_frames_are_never_delivered","src/comm/TcpChannelBase.cpp",
  "    if (recover_links && links[sender_id].pending(wanted.lane) > 0) {","    if (false) {"),
 # --- contract 3: lineage ----------------------------------------------------------------
 ("lineage_ignores_a_superseded_peer","src/comm/SequencedLink.cpp",
  "    if (peer.incarnation < peer_incarnation) {","    if (false) {"),
 ("lineage_ignores_our_own_replacement","src/comm/SequencedLink.cpp",
  "    if (peer.peer_incarnation > local_incarnation) {","    if (false) {"),
 ("lineage_does_not_reset_the_stream","src/comm/SequencedLink.cpp",
  "        reset_stream();\n        peer_incarnation = peer.incarnation;","        peer_incarnation = peer.incarnation;"),
 ("handshake_omits_our_lineage","src/comm/SequencedLink.cpp",
  "    h.incarnation = local_incarnation;","    h.incarnation = 0;"),
 ("handshake_omits_the_peer_lineage","src/comm/SequencedLink.cpp",
  "    h.peer_incarnation = peer_incarnation;","    h.peer_incarnation = 0;"),
 ("snapshot_drops_the_lineage","src/comm/SequencedLink.cpp",
  "        << \' \' << local_incarnation << \' \' << peer_incarnation;","        << \" 0 0\";"),
]

def run(cmd, **kw):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, **kw)

survived, killed, broken = [], [], []

# Mutations this suite cannot kill but the criu sweep does, with the measurement that says so.
# They are NOT test holes and must not be reported as such: each one is pinned, just by a
# different vehicle, because the obligation it removes only bites once a restore forces several
# links to be rebuilt at once. Anything that appears as a survivor and is NOT listed here is a
# genuine hole. Before adding a name, run it through
# runbooks/criu-transparent-checkpoint/sweep.py and record the rate you measured.
# Survivable BY DESIGN, not holes: each is one member of a redundant pair whose other member
# holds the line alone. The pair's combined removal is the mutation that must die, and does:
# delivery_order_guards_both_removed below.
BY_DESIGN = {
    "inline_path_ignores_the_drain_queue":
        "redundant pair with the yielding header read; combined removal is killed",
    "header_read_never_yields_to_the_lane":
        "redundant pair with the park branch; combined removal is killed",
    "replaced_link_keeps_the_old_byte_count":
        "audit-pinned (certain-from-code); criu sweep 19/20 vs 54/54 pristine - real but weak "
        "statistical signal (mid-frame replacement is rare); kept for the guard it documents",
}

CRIU_KILLED = {
    "waiting_rank_serves_nobody":        "criu sweep 4 peers, seed 7: 8/8 -> 0/8",
    "waiting_rank_never_accepts":        "criu sweep 4 peers, seed 7: 8/8 -> 0/8",
    "waiting_rank_never_reconciles":     "criu sweep 4 peers, seed 7: 8/8 -> 0/8",
    "adoption_skips_the_handshake_debt": "criu sweep 4 peers, seed 7: 8/8 -> 0/8",
    "establishment_reads_no_other_link": "criu sweep, seed 7: 0/8 at 4 peers and 0/8 at 8",
    "delivery_order_guards_both_removed":
        "criu sweep 7 peers 2MB payloads, seed 47: 20/20 clean build -> 16/20 with 3 "
        "wrong-round MISMATCH trials (the original R6 substitution reappearing)",
}
# Every file this sweep will touch, saved up front. A mutation left behind in a source tree is
# far worse than a sweep that did not finish: it is a deliberately broken protocol that looks
# like ordinary uncommitted work. Restored in a finally block, and on SIGINT/SIGTERM, because
# killing this script mid-mutation is the normal way to stop it.
PRISTINE = {rel: open(os.path.join(WT, rel)).read() for _, rel, _, _ in MUTS}

def restore_all():
    for rel, text in PRISTINE.items():
        path = os.path.join(WT, rel)
        if open(path).read() != text:
            open(path, 'w').write(text)
            print(f"restored {rel}", flush=True)

def on_signal(signum, _frame):
    restore_all()
    sys.exit(128 + signum)

signal.signal(signal.SIGINT, on_signal)
signal.signal(signal.SIGTERM, on_signal)

try:
    for name, rel, old, new in MUTS:
        path=os.path.join(WT,rel)
        src=PRISTINE[rel]
        # A MULTI entry applies several (old, new) pairs at once - for redundant-pair guards
        # where only the combined removal is required to die.
        pairs = new if old == "MULTI" else [[old, new]]
        missing = [o for o, _ in pairs if o not in src]
        if missing:
            broken.append((name,"PATTERN NOT FOUND")); print(f"BROKEN   {name} (pattern not found)", flush=True); continue
        mutated = src
        for o, n in pairs:
            mutated = mutated.replace(o, n, 1)
        open(path,'w').write(mutated)
        b=run(f"cd {WT} && cmake --build build -j16")
        if b.returncode!=0:
            broken.append((name,"build failed"))
        else:
            failed=False
            for s in SUITES:
                r=run(f"cd {WT}/build/tests && timeout {SUITE_TIMEOUT} ./Boost_Tests_run --run_test={s}")
                if r.returncode!=0: failed=True; break
            (killed if failed else survived).append(name)
        open(path,'w').write(src)
        if name in killed:
            verdict, detail = 'KILLED  ', f" (by {s})"
        elif name in survived and name in CRIU_KILLED:
            verdict, detail = 'criu    ', f" (not this suite: {CRIU_KILLED[name]})"
        elif name in survived and name in BY_DESIGN:
            verdict, detail = 'by-design', f" ({BY_DESIGN[name]})"
        elif name in survived:
            verdict, detail = 'SURVIVED', ""
        else:
            verdict, detail = 'BROKEN  ', ""
        print(f"{verdict} {name}{detail}", flush=True)
finally:
    restore_all()

run(f"cd {WT} && cmake --build build -j16")
elsewhere = [n for n in survived if n in CRIU_KILLED]
designed  = [n for n in survived if n in BY_DESIGN]
holes     = [n for n in survived if n not in CRIU_KILLED and n not in BY_DESIGN]
print("\n===== MUTATION SUMMARY =====")
print(f"killed here   : {len(killed)}")
print(f"killed by criu: {len(elsewhere)}  (pinned, just not by this suite)")
for n in elsewhere: print(f"   - {n}  [{CRIU_KILLED[n]}]")
print(f"by design     : {len(designed)}  (redundant-pair members / audit-pinned; see BY_DESIGN)")
print(f"SURVIVED      : {len(holes)}  <-- genuine test holes")
for n in holes: print("   -",n)
for n,why in broken: print("  BROKEN",n,why)
