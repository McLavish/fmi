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
SUITES=["LinkLayer","ProtocolValidation","OperationIdentity",
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
  "    HandshakePayload theirs;\n    const DecodeStatus status = decode_handshake(in, handshake_bytes, theirs);\n    if (status != DecodeStatus::Ok) {",
  "    HandshakePayload theirs;\n    const DecodeStatus status = decode_handshake(in, handshake_bytes, theirs);\n    if (false) {"),
 ("wire_skips_decode_check","src/comm/TcpChannelBase.cpp",
  "        const DecodeStatus status = decode_header(encoded, frame_header_bytes, buf.len, arrived);\n        if (status != DecodeStatus::Ok) {",
  "        const DecodeStatus status = decode_header(encoded, frame_header_bytes, buf.len, arrived);\n        if (false) {"),
 ("repair_forgets_to_reset_seq","src/comm/TcpChannelBase.cpp",
  "        if (rank < links.size()) {\n            links[rank] = SequencedLink({link_window_frames, link_max_frame_bytes,\n                                         link_retention_limit_bytes});\n        }",""),
 ("dedup_disabled","src/comm/SequencedLink.cpp",
  "FMI::Comm::SequencedLink::classify(const FrameHeader& header) const {\n    if (header.transport_seq < next_recv) {",
  "FMI::Comm::SequencedLink::classify(const FrameHeader& header) const {\n    if (false) {"),
 ("inline_never_advances","src/comm/SequencedLink.cpp",
  "    ++next_recv;\n    // Only reached once the payload is in","    // Only reached once the payload is in"),
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
  "        if (!guarded_read(buf.buf, buf.len)) {\n            continue;\n        }\n        // Committed only now: a link that died anywhere above left this frame unacknowledged,\n        // so the peer still holds it and replays it after the repair.\n        links[sender_id].commit_inline(arrived);",
  "        links[sender_id].commit_inline(arrived);\n        if (!guarded_read(buf.buf, buf.len)) {\n            continue;\n        }"),
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

]

def run(cmd, **kw):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, **kw)

survived, killed, broken = [], [], []
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
        if old not in src:
            broken.append((name,"PATTERN NOT FOUND")); print(f"BROKEN   {name} (pattern not found)", flush=True); continue
        open(path,'w').write(src.replace(old,new,1))
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
        verdict = 'KILLED  ' if name in killed else 'SURVIVED' if name in survived else 'BROKEN  '
        detail = f" (by {s})" if name in killed else ""
        print(f"{verdict} {name}{detail}", flush=True)
finally:
    restore_all()

run(f"cd {WT} && cmake --build build -j16")
print("\n===== MUTATION SUMMARY =====")
print(f"killed   : {len(killed)}")
print(f"SURVIVED : {len(survived)}  <-- test holes")
for s in survived: print("   -",s)
for n,why in broken: print("  BROKEN",n,why)
