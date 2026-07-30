#include "../../include/comm/SequencedLink.h"

#include <cstring>
#include <sstream>

namespace {
    //! Snapshot format tag. Bumped whenever the serialization below changes shape.
    constexpr char snapshot_version = '1';
}

bool FMI::Comm::SequencedLink::send_blocked() const {
    return retention.size() >= config.window_frames
           || retention_bytes >= config.retention_limit_bytes;
}

std::uint64_t FMI::Comm::SequencedLink::lowest_retained() const {
    // With nothing retained the peer may prune everything we have produced so far.
    return retention.empty() ? next_send : retention.front().header.transport_seq;
}

bool FMI::Comm::SequencedLink::admit(const FrameHeader& identity, const char* payload,
                                     std::size_t len, FrameHeader& stamped) {
    if (len > config.max_frame_bytes || send_blocked()) {
        return false;
    }

    stamped = identity;
    stamped.wire_version = frame_wire_version;
    stamped.payload_length = static_cast<std::uint32_t>(len);
    stamped.transport_seq = next_send;

    // The retained copy must exist before any byte reaches the socket: once send() returns,
    // the message is a delivery obligation even if the peer has not started its receive.
    Retained r;
    r.header = stamped;
    r.payload.assign(payload, payload + len);
    retention_bytes += len;
    retention.push_back(std::move(r));

    ++next_send;
    return true;
}

void FMI::Comm::SequencedLink::note_ack_sent(std::uint64_t value) {
    if (value > acked_to_peer) {
        acked_to_peer = value;
    }
}

bool FMI::Comm::SequencedLink::ack_due(std::uint64_t interval) const {
    if (interval == 0) {
        return next_recv > acked_to_peer;
    }
    return next_recv >= acked_to_peer + interval;
}

void FMI::Comm::SequencedLink::reset_stream() {
    next_send = 0;
    retention.clear();
    retention_bytes = 0;
    next_recv = 0;
    ack_safe_seq = 0;
    acked_to_peer = 0;
    lanes[0].clear();
    lanes[1].clear();
}

void FMI::Comm::SequencedLink::on_ack(std::uint64_t cumulative) {
    // Acks are cumulative: everything strictly below `cumulative` is durably held by the peer.
    // A stale or duplicated ack is simply a no-op rather than an error.
    while (!retention.empty() && retention.front().header.transport_seq < cumulative) {
        retention_bytes -= retention.front().payload.size();
        retention.pop_front();
    }
}

FMI::Comm::SequencedLink::Accept
FMI::Comm::SequencedLink::accept(const FrameHeader& header, const char* payload) {
    if (header.transport_seq < next_recv) {
        // Already accepted once. This is the normal outcome of a post-restore replay and must
        // never reach the application a second time.
        return Accept::Duplicate;
    }
    if (header.transport_seq > next_recv) {
        // A hole. The sender only ever transmits contiguously from its retention, so a gap
        // means bytes were lost in a way retention cannot repair.
        return Accept::FatalGap;
    }

    Committed c;
    c.header = header;
    c.payload.assign(payload, payload + header.payload_length);
    lanes[lane_index(header.lane)].push_back(std::move(c));

    ++next_recv;
    // Committed to a drain queue, so it is now durably held: the ack may be issued, and under
    // a checkpoint the queue travels inside the image.
    ack_safe_seq = next_recv;
    return Accept::Delivered;
}

FMI::Comm::SequencedLink::Accept
FMI::Comm::SequencedLink::classify(const FrameHeader& header) const {
    if (header.transport_seq < next_recv) {
        // Already accepted once. This is the normal outcome of a post-restore replay and must
        // never reach the application a second time.
        return Accept::Duplicate;
    }
    if (header.transport_seq > next_recv) {
        // A hole. The sender only ever transmits contiguously from its retention, so a gap
        // means bytes were lost in a way retention cannot repair.
        return Accept::FatalGap;
    }
    return Accept::Delivered;
}

void FMI::Comm::SequencedLink::commit_inline(const FrameHeader& header) {
    if (classify(header) != Accept::Delivered) {
        return;
    }
    ++next_recv;
    // Only reached once the payload is in the application's buffer, so the frame really is
    // durably held and the ack may be issued. Committing any earlier would let a freeze
    // between the header and the payload advance the watermark past a message whose bytes
    // were still in a kernel buffer the checkpoint does not capture — the peer would then
    // prune it on the next handshake and never replay it.
    ack_safe_seq = next_recv;
}

FMI::Comm::SequencedLink::Accept
FMI::Comm::SequencedLink::accept_inline(const FrameHeader& header) {
    const Accept verdict = classify(header);
    if (verdict == Accept::Delivered) {
        commit_inline(header);
    }
    return verdict;
}

FMI::Comm::SequencedLink::Accept
FMI::Comm::SequencedLink::deliver_into(const FrameHeader& expected, char* dst, std::size_t len) {
    auto& queue = lanes[lane_index(expected.lane)];
    if (queue.empty()) {
        return Accept::FatalGap;
    }
    if (!same_identity(queue.front().header, expected)) {
        // The head of this lane belongs to a different logical operation. Under the old
        // protocol this is exactly where a divergent schedule silently consumed the wrong
        // payload; here it is a loud, recoverable-by-nobody error.
        return Accept::IdentityMismatch;
    }
    if (queue.front().payload.size() != len) {
        return Accept::IdentityMismatch;
    }
    std::memcpy(dst, queue.front().payload.data(), len);
    queue.pop_front();
    return Accept::Delivered;
}

std::size_t FMI::Comm::SequencedLink::pending(Lane lane) const {
    return lanes[lane_index(lane)].size();
}

FMI::Comm::HandshakePayload
FMI::Comm::SequencedLink::local_handshake(std::uint64_t policy_fingerprint) const {
    HandshakePayload h;
    h.next_send_seq = next_send;
    h.next_expected_seq = next_recv;
    h.lowest_retained = lowest_retained();
    h.policy_fingerprint = policy_fingerprint;
    return h;
}

bool FMI::Comm::SequencedLink::reconcile(const HandshakePayload& peer, std::string& error) {
    if (peer.wire_version != frame_wire_version) {
        error = "handshake wire version mismatch";
        return false;
    }
    if (peer.next_expected_seq > next_send) {
        error = "peer expects sequence " + std::to_string(peer.next_expected_seq)
                + " but we have only produced " + std::to_string(next_send);
        return false;
    }
    if (peer.next_expected_seq < lowest_retained()) {
        error = "peer expects sequence " + std::to_string(peer.next_expected_seq)
                + " which is below our lowest retained " + std::to_string(lowest_retained());
        return false;
    }
    // The peer's next_expected doubles as a cumulative ack, which is what reconciles two ranks
    // whose final acks crossed while the link was down.
    on_ack(peer.next_expected_seq);
    // The peer starts again from what it told us it expects, so anything we believed it had
    // been told about our own watermark no longer holds on the new connection.
    acked_to_peer = 0;
    return true;
}

std::string FMI::Comm::SequencedLink::snapshot() const {
    std::ostringstream out;
    out << snapshot_version << ' ' << next_send << ' ' << next_recv << ' ' << ack_safe_seq;
    return out.str();
}

bool FMI::Comm::SequencedLink::seed(const std::string& blob) {
    std::istringstream in(blob);
    char version = 0;
    in >> version;
    if (!in || version != snapshot_version) {
        return false;
    }
    std::uint64_t send = 0, recv = 0, safe = 0;
    in >> send >> recv >> safe;
    if (!in) {
        return false;
    }
    next_send = send;
    next_recv = recv;
    ack_safe_seq = safe;
    // Nothing has been told to the peer over the link this state is being seeded onto.
    acked_to_peer = 0;
    return true;
}
