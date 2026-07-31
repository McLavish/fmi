#include "../../include/comm/SequencedLink.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sstream>

namespace {
    //! Env-gated tracing (FMI_LINK_TRACE=1): every event that moves link state, one line each,
    //! with the payload's first int where one exists — the checkpoint shapes encode the round
    //! in it, so a trace shows exactly which seq carried which application message.
    bool link_trace_enabled() {
        static const bool on = [] {
            const char* v = std::getenv("FMI_LINK_TRACE");
            return v != nullptr && v[0] == '1';
        }();
        return on;
    }
    int first_int(const char* payload, std::size_t len) {
        if (payload == nullptr || len < sizeof(int)) return -1;
        int v; std::memcpy(&v, payload, sizeof(int)); return v;
    }
}
#define FMI_LTRACE(...) do { if (link_trace_enabled()) { \
        std::fprintf(stderr, "[lt %p] ", static_cast<const void*>(this)); \
        std::fprintf(stderr, __VA_ARGS__); std::fputc('\n', stderr); } } while (0)

namespace {
    //! Snapshot format tag. Bumped whenever the serialization below changes shape.
    //! '2' added the incarnation pair.
    constexpr char snapshot_version = '2';
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
    FMI_LTRACE("admit seq=%llu first=%d retained=%zu",
               (unsigned long long) stamped.transport_seq,
               first_int(payload, len), retention.size());
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
    FMI_LTRACE("RESET_STREAM send=%llu recv=%llu retained=%zu lanes=%zu/%zu",
               (unsigned long long) next_send, (unsigned long long) next_recv,
               retention.size(), lanes[0].size(), lanes[1].size());
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
    const std::size_t before = retention.size();
    while (!retention.empty() && retention.front().header.transport_seq < cumulative) {
        retention_bytes -= retention.front().payload.size();
        retention.pop_front();
    }
    if (retention.size() != before) {
        FMI_LTRACE("on_ack cum=%llu pruned=%zu left=%zu",
                   (unsigned long long) cumulative, before - retention.size(), retention.size());
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
    FMI_LTRACE("accept->lane seq=%llu first=%d depth=%zu",
               (unsigned long long) header.transport_seq,
               first_int(payload, header.payload_length),
               lanes[lane_index(header.lane)].size());
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
    FMI_LTRACE("commit_inline seq=%llu pending=%zu/%zu",
               (unsigned long long) header.transport_seq, lanes[0].size(), lanes[1].size());
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
    FMI_LTRACE("deliver_from_lane seq=%llu first=%d left=%zu",
               (unsigned long long) queue.front().header.transport_seq,
               first_int(queue.front().payload.data(), len), queue.size() - 1);
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
    h.incarnation = local_incarnation;
    h.peer_incarnation = peer_incarnation;
    return h;
}

bool FMI::Comm::SequencedLink::reconcile(const HandshakePayload& peer, std::string& error) {
    if (peer.wire_version != frame_wire_version) {
        error = "handshake wire version mismatch";
        return false;
    }
    // Lineage before sequences: a fresh incarnation's counters are legitimately zero, and the
    // sequence checks below would read that as a peer which had forgotten what we still owe.
    if (peer.incarnation < peer_incarnation) {
        error = "peer claims incarnation " + std::to_string(peer.incarnation)
                + " which incarnation " + std::to_string(peer_incarnation)
                + " has already superseded";
        return false;
    }
    if (peer.peer_incarnation > local_incarnation) {
        // The peer has already reconciled with a later incarnation of *this* rank, so this
        // process is the zombie. It must not be allowed to serve the rank alongside its
        // replacement.
        error = "peer is talking to incarnation " + std::to_string(peer.peer_incarnation)
                + " of this rank, but this process is incarnation "
                + std::to_string(local_incarnation);
        return false;
    }
    if (peer.incarnation > peer_incarnation) {
        // The peer restarted from nothing. A delivery obligation is owed to a lineage, not to
        // a rank number, so what we retained for its predecessor is discharged, not replayed.
        reset_stream();
        peer_incarnation = peer.incarnation;
        return true;
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
    FMI_LTRACE("reconcile same-lineage peer_expects=%llu peer_send=%llu my_send=%llu my_recv=%llu low_ret=%llu",
               (unsigned long long) peer.next_expected_seq, (unsigned long long) peer.next_send_seq,
               (unsigned long long) next_send, (unsigned long long) next_recv,
               (unsigned long long) lowest_retained());
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
    out << snapshot_version << ' ' << next_send << ' ' << next_recv << ' ' << ack_safe_seq
        << ' ' << local_incarnation << ' ' << peer_incarnation;
    return out.str();
}

bool FMI::Comm::SequencedLink::seed(const std::string& blob) {
    std::istringstream in(blob);
    char version = 0;
    in >> version;
    if (!in || version != snapshot_version) {
        return false;
    }
    std::uint64_t send = 0, recv = 0, safe = 0, mine = 0, theirs = 0;
    in >> send >> recv >> safe >> mine >> theirs;
    if (!in) {
        return false;
    }
    next_send = send;
    next_recv = recv;
    ack_safe_seq = safe;
    local_incarnation = mine;
    peer_incarnation = theirs;
    // Nothing has been told to the peer over the link this state is being seeded onto.
    acked_to_peer = 0;
    return true;
}
