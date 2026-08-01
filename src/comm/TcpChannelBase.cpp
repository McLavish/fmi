#include "../../include/comm/TcpChannelBase.h"

#include "../../include/comm/OperationScope.h"

#include <poll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/tcp.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

void FMI::Comm::TcpChannelBase::parse_tcp_params(std::map<std::string, std::string>& params) {
    max_timeout = std::stoi(params["max_timeout"]);
    framed = params.count("framed") > 0 && params["framed"] == "true";
    recover_links = params.count("recover_links") > 0 && params["recover_links"] == "true";
    if (recover_links && !framed) {
        // The recovery protocol is defined over frames; without them there are no sequences,
        // no acks and no replay, and none of the mid-frame guards apply to the raw stream.
        throw std::runtime_error("recover_links requires framed");
    }
    if (params.count("link_window_frames") > 0) {
        link_window_frames = static_cast<std::uint32_t>(std::stoul(params["link_window_frames"]));
    }
    if (params.count("link_retention_limit_bytes") > 0) {
        link_retention_limit_bytes = static_cast<std::size_t>(
                std::stoull(params["link_retention_limit_bytes"]));
    }
    if (params.count("link_ack_interval") > 0) {
        link_ack_interval = static_cast<std::uint32_t>(std::stoul(params["link_ack_interval"]));
    }
    // An ack interval at or above the window would let the window fill before the peer is ever
    // told anything, which is the deadlock this exists to prevent.
    if (link_ack_interval >= link_window_frames) {
        link_ack_interval = std::max<std::uint32_t>(1, link_window_frames / 4);
    }
}

void FMI::Comm::TcpChannelBase::set_incarnation(std::uint64_t value) {
    local_incarnation = value;
    // Links built before the incarnation arrived (none in normal use, since the Communicator
    // sets it at registration) must not keep claiming to be lineage zero.
    for (auto& link : links) {
        link.set_incarnation(value);
    }
}

void FMI::Comm::TcpChannelBase::ensure_link_state() {
    if (links.size() != num_peers) {
        links.assign(num_peers, SequencedLink({link_window_frames, link_max_frame_bytes,
                                               link_retention_limit_bytes}));
        for (auto& link : links) {
            link.set_incarnation(local_incarnation);
        }
    }
    if (inbound_stage.size() != num_peers) {
        inbound_stage.assign(num_peers, InboundStage{});
        app_owns_stream.assign(num_peers, 0);
    }
    if (outbound_frozen.size() < num_peers) {
        outbound_frozen.resize(num_peers, 0);
    }
}

namespace {
    //! Env-gated tracing (FMI_LINK_TRACE=1), cached: one getenv for the process lifetime.
    bool link_trace() {
        static const bool on = [] {
            const char* v = std::getenv("FMI_LINK_TRACE");
            return v != nullptr && v[0] == '1';
        }();
        return on;
    }

    long steady_now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
    }
}

void FMI::Comm::TcpChannelBase::maybe_send_ack(Utils::peer_num partner_id, bool flush_tail) {
    if (!recover_links || partner_id >= links.size() || sockets[partner_id] < 0) {
        return;
    }
    if (frozen(partner_id)) {
        // A write_all is mid-frame toward this peer; an ack sent now would splice into that
        // frame. Best-effort by contract: re-offered from the next servicing pass and the
        // next commit.
        return;
    }
    if (!links[partner_id].ack_due(flush_tail ? 1 : link_ack_interval)) {
        return;
    }
    const std::uint64_t watermark = links[partner_id].next_received();
    char encoded[frame_header_bytes];
    encode_header(make_ack(watermark), encoded);
    long n = ::send(sockets[partner_id], encoded, frame_header_bytes, MSG_NOSIGNAL | MSG_DONTWAIT);
    if (n < 0) {
        // No room, or the link is broken. Either way this ack is not owed: the receive path
        // discovers a broken link on its own, and a dropped ack is re-offered next commit.
        return;
    }
    if (static_cast<std::size_t>(n) < frame_header_bytes) {
        // A partial ack would desynchronise the peer's frame boundaries, so the remainder has
        // to go out even though that means blocking. Bounded by SO_SNDTIMEO, and reachable
        // only when the socket had 1..frame_header_bytes-1 bytes of room, which needs the peer
        // to have stopped reading entirely.
        write_all(partner_id, encoded + n, frame_header_bytes - static_cast<std::size_t>(n));
    }
    links[partner_id].note_ack_sent(watermark);
}

void FMI::Comm::TcpChannelBase::drain_acks(Utils::peer_num partner_id, long budget_ms) {
    if (!recover_links || partner_id >= links.size() || sockets[partner_id] < 0) {
        return;
    }
    if (partner_id < app_owns_stream.size() && app_owns_stream[partner_id]) {
        // Unreachable today — nothing under recv_object sends — but the peek below would
        // read mid-frame bytes if a future path got here with the claim armed, so the
        // invariant is enforced rather than assumed.
        return;
    }
    const long deadline = steady_now_ms() + std::max<long>(budget_ms, 0);
    char encoded[frame_header_bytes];
    while (true) {
        if (partner_id < inbound_stage.size() && inbound_stage[partner_id].active) {
            // The stream is mid-frame at a position the stage records; a peek here would
            // decode payload bytes as a header. Advance the stage instead — its header's ack
            // was applied at activation, and its completed frame commits to the lane, which
            // is exactly the progress this wait exists to collect.
            const int filled = fill_stage(partner_id);
            if (filled > 0) {
                try {
                    finish_stage(partner_id);
                } catch (const std::exception&) {
                    return;   // let the receive path report it properly
                }
                if (!links[partner_id].send_blocked()) {
                    return;
                }
                continue;
            }
            if (filled < 0) {
                return;   // peer gone; the receive path turns that into a repair
            }
            const long remaining = deadline - steady_now_ms();
            if (remaining <= 0) {
                return;
            }
            struct pollfd pfd{sockets[partner_id], POLLIN, 0};
            ::poll(&pfd, 1, static_cast<int>(std::min<long>(remaining, 1000)));
            continue;
        }
        long n = ::recv(sockets[partner_id], encoded, frame_header_bytes,
                        MSG_PEEK | MSG_DONTWAIT);
        if (n < 0 || static_cast<std::size_t>(n) < frame_header_bytes) {
            if (n == 0) {
                return;   // peer closed; the receive path turns that into a repair
            }
            const long remaining = deadline - steady_now_ms();
            if (remaining <= 0) {
                return;
            }
            if (link_trace()) {
                static thread_local long last_da_note = 0;
                const long now = steady_now_ms();
                if (now - last_da_note > 3000) {
                    last_da_note = now;
                    std::fprintf(stderr, "[lt] DRAIN-ACKS-WAIT peer=%u fd=%d remaining=%ld\n",
                                 static_cast<unsigned>(partner_id), sockets[partner_id], remaining);
                }
            }
            struct pollfd pfd{sockets[partner_id], POLLIN, 0};
            if (::poll(&pfd, 1, static_cast<int>(std::min<long>(remaining, 1000))) <= 0) {
                if (steady_now_ms() >= deadline) {
                    return;
                }
                continue;
            }
            continue;
        }
        FrameHeader peeked;
        if (decode_header(encoded, frame_header_bytes, link_max_frame_bytes, peeked)
            != DecodeStatus::Ok) {
            return;   // let the receive path report it properly, with its own diagnostics
        }
        links[partner_id].on_ack(peeked.cumulative_ack);
        if (peeked.frame_type == FrameType::Handshake) {
            int available = 0;
            if (::ioctl(sockets[partner_id], FIONREAD, &available) != 0 ||
                static_cast<std::size_t>(available) < frame_header_bytes + handshake_bytes) {
                return;
            }
            read_all(partner_id, encoded, frame_header_bytes);
            char offered[handshake_bytes];
            read_all(partner_id, offered, handshake_bytes);
            reconcile_handshake(partner_id, offered);
            continue;
        }
        if (peeked.frame_type != FrameType::Ack) {
            // A data frame belongs to recv_object, so it stays in the stream untouched. Its
            // ack has already been applied, which is the only thing needed here.
            return;
        }
        try {
            read_all(partner_id, encoded, frame_header_bytes);   // consume the ack just peeked
        } catch (const LinkReplaced&) {
            return;   // the fresh link's acks arrive on their own schedule
        }
        if (!links[partner_id].send_blocked()) {
            return;
        }
    }
}

namespace {
    //! Identity the local rank believes it is exchanging, for a message of @p len bytes.
    /*!
     * Built from the operation the Communicator published, so a collective fragment and an
     * application send of the same size are different identities even though the bytes on the
     * wire are indistinguishable.
     */
    FMI::Comm::FrameHeader expected_identity(std::size_t len) {
        const auto& op = FMI::Comm::active_operation();
        FMI::Comm::FrameHeader h;
        h.lane = op.lane;
        h.op_kind = op.op_kind;
        h.collective_index = op.collective_index;
        h.root = op.root;
        h.commutative = op.commutative;
        h.associative = op.associative;
        h.total_length = len;
        h.payload_length = static_cast<std::uint32_t>(len);
        return h;
    }

    std::string describe(const FMI::Comm::FrameHeader& h) {
        return "lane=" + std::to_string(static_cast<int>(h.lane))
               + " op=" + std::to_string(static_cast<int>(h.op_kind))
               + " collective=" + std::to_string(h.collective_index)
               + " root=" + std::to_string(h.root)
               + " len=" + std::to_string(h.total_length);
    }
}

void FMI::Comm::TcpChannelBase::parse_tcp_model_params(std::map<std::string, std::string>& model_params) {
    bandwidth = std::stod(model_params["bandwidth"]);
    overhead = std::stod(model_params["overhead"]);
    transfer_price = std::stod(model_params["transfer_price"]);
    vm_price = std::stod(model_params["vm_price"]);
    requests_per_hour = std::stoi(model_params["requests_per_hour"]);
    if (model_params["include_infrastructure_costs"] == "true") {
        include_infrastructure_costs = true;
    } else {
        include_infrastructure_costs = false;
    }
}

std::string FMI::Comm::TcpChannelBase::link_name(Utils::peer_num partner_id, bool) const {
    auto low = std::min(peer_id, partner_id);
    auto high = std::max(peer_id, partner_id);
    return comm_name + "|" + std::to_string(low) + "-" + std::to_string(high);
}

bool FMI::Comm::TcpChannelBase::pump(Utils::peer_num peer, short events, long deadline_ms) {
    // Bound the nesting rather than forbid it: servicing reads and writes, and those wait, and
    // waiting pumps. Two levels are needed, not one — servicing reconciles links, and that
    // sends a handshake and a replay, which waits in turn. Below that, fall back to this
    // descriptor alone, and only in short slices: a nested wait that consumed the whole
    // deadline would leave the rank serving nobody for as long as it lasted.
    if (pump_depth >= 3) {
        const long remaining = std::max<long>(deadline_ms - steady_now_ms(), 0);
        if (link_trace()) {
            static thread_local long last_solo_note = 0;
            const long now = steady_now_ms();
            if (now - last_solo_note > 3000) {
                last_solo_note = now;
                std::fprintf(stderr, "[lt] SOLO-POLL depth=%d peer=%u fd=%d events=%d remaining=%ld\n",
                             pump_depth, static_cast<unsigned>(peer),
                             peer < sockets.size() ? sockets[peer] : -2, events, remaining);
            }
        }
        struct pollfd solo{sockets[peer], events, 0};
        return ::poll(&solo, 1, static_cast<int>(std::min<long>(remaining, 20))) > 0;
    }
    ++pump_depth;
    struct Guard {
        int& depth;
        ~Guard() { --depth; }
    } guard{pump_depth};

    while (true) {
        struct pollfd want{sockets[peer], events, 0};
        // A short slice, not the whole deadline: the listener and the other links have to be
        // looked at even while this descriptor stays quiet, and they are not pollable from
        // here (the listener belongs to the subclass).
        const long remaining = deadline_ms - steady_now_ms();
        if (remaining <= 0) {
            return false;
        }
        const int slice = static_cast<int>(std::min<long>(remaining, 20));
        const int ready = ::poll(&want, 1, slice);
        if (ready > 0) {
            return true;
        }
        if (ready < 0 && errno != EINTR) {
            return false;
        }
        // Nothing yet on the descriptor we need. Pay what we owe everyone else.
        service_transport();
        // The skip protects the caller's INBOUND cursor, which only a POLLIN wait owns. A
        // POLLOUT wait is a writer, and reading the peer while writing to it is full-duplex
        // safe — refusing to was itself a deadlock: two ranks replaying oversized suffixes to
        // each other both block writing, each skipping the other, and neither ever stages the
        // other's frames. The application's mid-frame claim (app_owns_stream) is what
        // protects the inbound cursor now, on every path including this one.
        service_established_links((events & POLLIN) ? peer : num_peers);
        // One more obligation, the aged kind: a link servicing has seen dead for over a
        // second, whose DIALER this rank is, gets one bounded re-dial attempt per slice. The
        // dead end cannot fix itself - its owner only touches the link when the application
        // does, and the application may be blocked right here in a wait that transitively
        // needs that very peer. Guarded against establishment reentrancy, aged so ordinary
        // teardown never triggers it, and skipped for the peer this pump is already about.
        if (recover_links && establishment_depth == 0 && !tearing_down &&
            link_suspect_since.size() == num_peers) {
            const long now_ms = steady_now_ms();
            for (Utils::peer_num q = 0; q < num_peers; q++) {
                if (q == peer || q == peer_id || link_suspect_since[q] == 0 ||
                    now_ms - link_suspect_since[q] < 1000) {
                    continue;
                }
                if (q < app_owns_stream.size() && app_owns_stream[q]) {
                    continue;   // the application is mid-frame on it; it is not dead
                }
                if (redial_dead_link(q, 250)) {
                    link_suspect_since[q] = 0;
                    if (link_trace()) {
                        std::fprintf(stderr, "[lt] REDIALED peer=%u fd=%d\n",
                                     static_cast<unsigned>(q),
                                     q < sockets.size() ? sockets[q] : -2);
                    }
                }
            }
        }
        {
            // A rank that has been waiting seconds says what it believes it holds on every
            // link. Silent in a healthy run - nothing waits this long - and the one thing that
            // makes a mesh deadlock diagnosable at all when it does happen.
            const long waited = steady_now_ms() - (deadline_ms - static_cast<long>(max_timeout));
            if (waited > 3000 && waited % 3000 < 25) {
                std::fprintf(stderr, "[FMI] rank %u has waited %ldms for peer %u;", peer_id, waited, peer);
                for (Utils::peer_num q = 0; q < num_peers; q++) {
                    if (q == peer_id) { continue; }
                    int avail = -1;
                    if (q < sockets.size() && sockets[q] >= 0) {
                        ::ioctl(sockets[q], FIONREAD, &avail);
                    }
                    std::fprintf(stderr, " p%u{fd=%d rq=%d rec=%d}", q,
                                 q < sockets.size() ? sockets[q] : -2, avail,
                                 q < link_needs_reconcile.size() ? link_needs_reconcile[q] : -1);
                }
                std::fprintf(stderr, " %s\n", transport_state_note().c_str());
            }
        }
    }
}

void FMI::Comm::TcpChannelBase::write_all(Utils::peer_num rcpt_id, const char* data, std::size_t len) {
    // MSG_NOSIGNAL: a peer that closed mid-migration must surface as EPIPE, not kill the
    // process with SIGPIPE. Loop: a blocking send under SO_SNDTIMEO may accept only part of
    // a large buffer, and a silent short send would desynchronize the byte stream.
    // While this write is in flight nothing else may write to this peer — the pump below
    // services links, and a serviced frame's ack (or a reconcile's handshake) emitted here
    // would land in the MIDDLE of the frame this write is part of. A per-peer depth, not a
    // scalar: writes nest across peers, and each frame in flight needs its own freeze for
    // its own peer, at every nesting level.
    if (outbound_frozen.size() <= rcpt_id) {
        outbound_frozen.resize(num_peers > rcpt_id ? num_peers : rcpt_id + 1, 0);
    }
    ++outbound_frozen[rcpt_id];
    // By index, not by reference: a nested write to a not-yet-sized peer can reallocate the
    // vector, and a reference held across that is dangling.
    struct FreezeGuard {
        std::vector<int>& depths;
        const Utils::peer_num peer;
        ~FreezeGuard() { --depths[peer]; }
    } freeze_guard{outbound_frozen, rcpt_id};
    std::size_t sent = 0;
    const long deadline = steady_now_ms() + static_cast<long>(max_timeout);
    const int fd_at_entry = sockets[rcpt_id];
    const std::uint64_t gen_at_entry = generation(rcpt_id);
    while (sent < len) {
        if (sockets[rcpt_id] != fd_at_entry || generation(rcpt_id) != gen_at_entry) {
            throw LinkReplaced(transport_tag + ": link to peer " + std::to_string(rcpt_id) +
                               " replaced after " + std::to_string(sent) + "/" +
                               std::to_string(len) + " bytes written");
        }
        long n = ::send(sockets[rcpt_id], data + sent, len - sent,
                        MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n == -1) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // The socket is full. Wait for room while still serving everyone else; a rank
                // that stopped accepting here would deadlock a peer trying to re-establish.
                if (pump(rcpt_id, POLLOUT, deadline)) {
                    continue;
                }
                throw Utils::Timeout();
            }
            throw std::runtime_error(transport_tag + ": send to peer " + std::to_string(rcpt_id) +
                                     " failed after " + std::to_string(sent) + "/" +
                                     std::to_string(len) + " bytes: " + strerror(errno));
        }
        sent += static_cast<std::size_t>(n);
    }
}

void FMI::Comm::TcpChannelBase::read_all(Utils::peer_num sender_id, char* data, std::size_t len) {
    // Never return with a partially filled buffer: EOF or an error mid-message must be loud.
    // A silent short read here hands garbage to the collective and corrupts the reduction.
    // MSG_WAITALL under SO_RCVTIMEO can legitimately return a partial chunk on timeout, so
    // loop while progress is made and only throw Timeout when a call yields nothing.
    std::size_t received = 0;
    const long deadline = steady_now_ms() + static_cast<long>(max_timeout);
    const int fd_at_entry = sockets[sender_id];
    const std::uint64_t gen_at_entry = generation(sender_id);
    while (received < len) {
        if (sockets[sender_id] != fd_at_entry || generation(sender_id) != gen_at_entry) {
            throw LinkReplaced(transport_tag + ": link to peer " + std::to_string(sender_id) +
                               " replaced after " + std::to_string(received) + "/" +
                               std::to_string(len) + " bytes read");
        }
        long n = ::recv(sockets[sender_id], data + received, len - received, MSG_DONTWAIT);
        if (n == 0) {
            if (received == 0 && eof_before_data_is_timeout && !recover_links) {
                throw Utils::Timeout();
            }
            throw std::runtime_error(transport_tag + ": connection to peer " + std::to_string(sender_id) +
                                     " closed after " + std::to_string(received) + "/" +
                                     std::to_string(len) + " bytes");
        }
        if (n == -1) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Nothing here yet. Wait for it while serving every other obligation, which is
                // what stops a rank waiting on one link from starving the rest of the mesh.
                if (pump(sender_id, POLLIN, deadline)) {
                    continue;
                }
                if (received > 0) {
                    // Half a message came and the rest did not. "Nothing arrived in time" is
                    // both the wrong thing to say and the wrong thing to do: the bytes already
                    // taken are gone from the stream, so every frame boundary after this one
                    // is off by however many were read. Report a broken link instead, which is
                    // true and recoverable — a re-established connection starts clean and the
                    // peer replays whatever this end never took delivery of.
                    throw std::runtime_error(transport_tag + ": message from peer " +
                                             std::to_string(sender_id) + " stalled after " +
                                             std::to_string(received) + "/" +
                                             std::to_string(len) + " bytes");
                }
                throw Utils::Timeout();
            }
            // A peer that tears the connection down before sending any of this message has
            // abandoned the collective; it reaches us as ECONNRESET rather than EOF whenever it
            // closed with data of ours still unread. Same condition as the n == 0 case above.
            if (received == 0 && errno == ECONNRESET && eof_before_data_is_timeout
                && !recover_links) {
                throw Utils::Timeout();
            }
            throw std::runtime_error(transport_tag + ": recv from peer " + std::to_string(sender_id) +
                                     " failed after " + std::to_string(received) + "/" +
                                     std::to_string(len) + " bytes: " + strerror(errno));
        }
        received += static_cast<std::size_t>(n);
    }
}

void FMI::Comm::TcpChannelBase::write_frame(Utils::peer_num rcpt_id, const FrameHeader& header,
                                            const char* payload) {
    char encoded[frame_header_bytes];
    encode_header(header, encoded);
    // Header immediately precedes its payload with nothing interleaved, which holds because a
    // channel is only ever driven by one application thread at a time.
    write_all(rcpt_id, encoded, frame_header_bytes);
    if (header.payload_length > 0) {
        write_all(rcpt_id, payload, header.payload_length);
    }
    if (rcpt_id < links.size()) {
        // The piggybacked ack has reached the wire, so the standalone path need not repeat it.
        links[rcpt_id].note_ack_sent(header.cumulative_ack);
    }
}

void FMI::Comm::TcpChannelBase::exchange_handshake(Utils::peer_num partner_id) {
    // One way; nothing is read here. The peer's handshake arrives as an ordinary frame.
    const HandshakePayload mine = links[partner_id].local_handshake();
    if (link_trace()) {
        const auto& suffix = links[partner_id].replay_suffix();
        std::fprintf(stderr, "[lt] handshake-out peer=%u send=%llu recv=%llu replay=%zu[%llu..%llu]\n",
                     static_cast<unsigned>(partner_id),
                     (unsigned long long) mine.next_send_seq,
                     (unsigned long long) mine.next_expected_seq, suffix.size(),
                     suffix.empty() ? 0ull : (unsigned long long) suffix.front().header.transport_seq,
                     suffix.empty() ? 0ull : (unsigned long long) suffix.back().header.transport_seq);
    }
    char payload[handshake_bytes];
    encode_handshake(mine, payload);
    write_frame(partner_id, make_handshake_frame(), payload);

    // Replay by sequence, one re-validated COPY at a time — never by iterating the live
    // retention. Every write below pumps, the pump services other links, and an inbound
    // frame's piggybacked ack (or the peer's own handshake, reconciled from a nested pass)
    // legitimately prunes this very retention mid-loop. A reference into the deque dangles
    // at that instant — observed as freed-heap bytes on the wire, a silent stream
    // corruption. A frame pruned before its turn is one the peer just declared it holds:
    // skipping it is correct, not just safe.
    const std::uint64_t replay_end = links[partner_id].next_send_seq();
    for (std::uint64_t seq = links[partner_id].lowest_retained(); seq < replay_end; seq++) {
        SequencedLink::Retained frame;
        if (!links[partner_id].copy_retained(seq, frame)) {
            continue;
        }
        frame.header.cumulative_ack = links[partner_id].next_received();
        write_frame(partner_id, frame.header, frame.payload.data());
    }
}

void FMI::Comm::TcpChannelBase::reconcile_handshake(Utils::peer_num partner_id,
                                                    const char* payload) {
    HandshakePayload theirs;
    const DecodeStatus status = decode_handshake(payload, handshake_bytes, theirs);
    if (status != DecodeStatus::Ok) {
        throw LinkProtocolError(transport_tag + ": malformed handshake from peer " +
                                std::to_string(partner_id));
    }
    std::string error;
    if (!links[partner_id].reconcile(theirs, error)) {
        throw LinkProtocolError(transport_tag + ": irreconcilable link to peer " +
                                std::to_string(partner_id) + ": " + error);
    }
}

void FMI::Comm::TcpChannelBase::repair_link(Utils::peer_num partner_id) {
    if (link_trace()) {
        std::fprintf(stderr, "[lt] repair_link peer=%u fd=%d\n",
                     static_cast<unsigned>(partner_id),
                     partner_id < sockets.size() ? sockets[partner_id] : -2);
    }
    if (partner_id < sockets.size() && sockets[partner_id] >= 0) {
        close(sockets[partner_id]);
        sockets[partner_id] = -1;
    }
    // One path reconciles a re-established link, whether this end initiated the repair or the
    // transport replaced the connection under it. check_socket handshakes and replays.
    note_link_replaced(partner_id);
    check_socket(partner_id, link_name(partner_id, true));
}

void FMI::Comm::TcpChannelBase::send_object(channel_data buf, Utils::peer_num rcpt_id) {
    check_socket(rcpt_id, link_name(rcpt_id, true));
    reconcile_if_needed(rcpt_id);
    if (!framed) {
        write_all(rcpt_id, buf.buf, buf.len);
        return;
    }

    ensure_link_state();
    if (buf.len > 0xFFFFFFFFull) {
        throw std::runtime_error(transport_tag + ": message of " + std::to_string(buf.len) +
                                 " bytes exceeds the framed maximum");
    }

    FrameHeader header = expected_identity(buf.len);
    if (!recover_links) {
        header.message_id = links[rcpt_id].next_send_seq();
        header.transport_seq = links[rcpt_id].next_send_seq();
        header.cumulative_ack = links[rcpt_id].next_received();
        links[rcpt_id].note_sent();
        write_frame(rcpt_id, header, buf.buf);
        return;
    }

    // The retained copy exists before a single byte reaches the socket: once this returns the
    // message is a delivery obligation even if the peer has not started its receive.
    header.message_id = links[rcpt_id].next_send_seq();
    FrameHeader stamped;
    if (!links[rcpt_id].admit(header, buf.buf, buf.len, stamped)) {
        // The window is full. On a link that carries traffic both ways this never happens,
        // because every frame the peer sends prunes retention; on a one-way link — which a
        // binomial tree produces as soon as there are three ranks — nothing prunes it but the
        // peer's standalone acks, and those have to be collected explicitly.
        drain_acks(rcpt_id, static_cast<long>(max_timeout));
        if (!links[rcpt_id].admit(header, buf.buf, buf.len, stamped)) {
            throw std::runtime_error(transport_tag + ": link to peer " + std::to_string(rcpt_id) +
                                     " is at its retention limit with " +
                                     std::to_string(links[rcpt_id].retained_bytes()) +
                                     " bytes outstanding and " +
                                     std::to_string(links[rcpt_id].replay_suffix().size()) +
                                     " frames unacknowledged");
        }
    }
    stamped.cumulative_ack = links[rcpt_id].next_received();
    try {
        write_frame(rcpt_id, stamped, buf.buf);
    } catch (const LinkReplaced&) {
        // The transport already re-established this link under us; the frame is retained, and
        // paying the handshake debt replays it on the new connection. Nothing to repair.
        reconcile_if_needed(rcpt_id);
    } catch (const std::exception&) {
        // The frame is retained, so the repair replays it; the send obligation is intact.
        repair_link(rcpt_id);
    }
}

void FMI::Comm::TcpChannelBase::recv_object(channel_data buf, Utils::peer_num sender_id) {
    if (framed && recover_links) {
        // The drain queue BEFORE any socket work. The message may already be in local memory
        // — drained while some other operation waited — and the link itself may be gone for a
        // reason that needs no repair at all: a peer that finalized after its last send is
        // not an error, but insisting on a live socket here would try to re-establish to a
        // process that no longer exists and time out with the answer already in hand.
        ensure_link_state();
        const FrameHeader early = expected_identity(buf.len);
        if (links[sender_id].pending(early.lane) > 0) {
            const SequencedLink::Accept drained =
                    links[sender_id].deliver_into(early, buf.buf, buf.len);
            if (drained == SequencedLink::Accept::Delivered) {
                // A lane delivery must offer the ack the drain-time offer may have had
                // suppressed (a write toward this peer was mid-frame then).
                maybe_send_ack(sender_id);
                return;
            }
            if (drained == SequencedLink::Accept::IdentityMismatch) {
                throw std::runtime_error(transport_tag + ": message identity mismatch from peer " +
                                         std::to_string(sender_id) + " - expected [" +
                                         describe(early) + "] at the head of the drain queue");
            }
        }
    }
    check_socket(sender_id, link_name(sender_id, false));
    reconcile_if_needed(sender_id);
    if (!framed) {
        read_all(sender_id, buf.buf, buf.len);
        return;
    }

    ensure_link_state();

    const FrameHeader wanted = expected_identity(buf.len);

    int repairs = 0;
    // Read that survives the link dying under it. Returns false once the link has been
    // repaired, meaning the caller must start again from the frame header: the replacement
    // connection carries the peer's replay from the last sequence it saw acknowledged, and
    // whatever was half-read on the old connection is gone with it.
    auto guarded_read = [&](char* dst, std::size_t len) -> bool {
        if (!recover_links) {
            read_all(sender_id, dst, len);
            return true;
        }
        try {
            read_all(sender_id, dst, len);
            return true;
        } catch (const Utils::Timeout&) {
            throw;   // nothing arrived in time; that is not the same as a dead link
        } catch (const LinkProtocolError&) {
            throw;
        } catch (const LinkReplaced&) {
            // The transport already holds a fresh connection for this peer; the peer replays
            // from its retention on it. Start again at the header - no repair to do, and no
            // charge against the repair budget for a link that is already back.
            return false;
        } catch (const std::exception&) {
            // The connection died — one end sees a failed write, the other EOF or a reset —
            // so both re-establish, reconcile and replay. Bounded, because a peer that has
            // genuinely gone away would otherwise be repaired forever.
            if (++repairs > max_link_repairs) {
                throw std::runtime_error(transport_tag + ": link to peer " +
                                         std::to_string(sender_id) + " could not be repaired after " +
                                         std::to_string(max_link_repairs) + " attempts");
            }
            repair_link(sender_id);
            return false;
        }
    };

    // While this receive is mid-frame on the sender's link, nested servicing — reached
    // through pumps whose skip is some OTHER peer — must not consume the bytes that finish
    // the frame. Armed at the first header byte, released at every frame boundary and on
    // unwind.
    struct StreamClaim {
        std::vector<char>& flags;
        const Utils::peer_num peer;
        void arm() { if (peer < flags.size()) flags[peer] = 1; }
        void release() { if (peer < flags.size()) flags[peer] = 0; }
        ~StreamClaim() { release(); }
    } claim{app_owns_stream, sender_id};

    // Header read that yields to the drain queue. Any pump below this frame's wait services
    // the OTHER links with their own skip — so a wait inside, say, a handshake replay to a
    // third rank legitimately drains THIS link into its lanes. Once that has happened the
    // socket's next frame is no longer the oldest undelivered one, and a read that kept
    // blocking here could wait forever on a sender with nothing more to say while the wanted
    // frame sits in local memory. Yield only while no header byte has been consumed: from the
    // first byte on, the frame boundary is this read's to finish.
    //   1 = header in `dst`, 0 = the lane gained frames (deliver from it), -1 = link repaired.
    auto read_header_yielding = [&](char* dst) -> int {
        if (!recover_links) {
            read_all(sender_id, dst, frame_header_bytes);
            return 1;
        }
        std::size_t got = 0;
        const long deadline = steady_now_ms() + static_cast<long>(max_timeout);
        int fd_at_entry = sockets[sender_id];
        std::uint64_t gen_at_entry = generation(sender_id);
        while (got < frame_header_bytes) {
            if (sockets[sender_id] != fd_at_entry || generation(sender_id) != gen_at_entry) {
                // Replaced mid-wait. Anything partially read died with the old stream; the
                // new one starts at a frame boundary, so simply restart the header.
                fd_at_entry = sockets[sender_id];
                gen_at_entry = generation(sender_id);
                got = 0;
                claim.release();
            }
            if (got == 0 && links[sender_id].pending(wanted.lane) > 0) {
                return 0;
            }
            if (got == 0 && sender_id < inbound_stage.size() && inbound_stage[sender_id].active) {
                // Servicing consumed this frame's header and part of its payload before this
                // receive arrived; the stream position is mid-frame, where the stage records
                // it. Finish the stage with the blocking machinery this path owns; the frame
                // lands in its lane (or reconciles a handshake) and the loop re-dispatches.
                while (true) {
                    const int filled = fill_stage(sender_id);
                    if (filled > 0) {
                        finish_stage(sender_id);
                        return 0;
                    }
                    if (filled < 0) {
                        if (++repairs > max_link_repairs) {
                            throw std::runtime_error(transport_tag + ": link to peer " +
                                                     std::to_string(sender_id) +
                                                     " could not be repaired after " +
                                                     std::to_string(max_link_repairs) +
                                                     " attempts");
                        }
                        repair_link(sender_id);
                        return -1;
                    }
                    if (steady_now_ms() >= deadline) {
                        throw Utils::Timeout();
                    }
                    maybe_send_ack(sender_id);   // same obligation as the raw header wait
                    pump(sender_id, POLLIN,
                         std::min<long>(deadline, steady_now_ms() + 100));
                    if (!inbound_stage[sender_id].active) {
                        return 0;   // a nested pass completed it meanwhile
                    }
                }
            }
            long n = ::recv(sockets[sender_id], dst + got, frame_header_bytes - got,
                            MSG_DONTWAIT);
            if (n > 0) {
                got += static_cast<std::size_t>(n);
                claim.arm();
                continue;
            }
            if (n == -1 && errno == EINTR) {
                continue;
            }
            if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                if (steady_now_ms() >= deadline) {
                    throw Utils::Timeout();
                }
                // A receiver about to wait on a link first tells it what it holds: the
                // pump's servicing skips this very peer (POLLIN owns its inbound cursor),
                // so an ack this link is owed has no other sender for as long as the wait
                // lasts — and the peer's window may be exactly what the wait is for.
                maybe_send_ack(sender_id);
                // Short slices, so the lane check above re-runs promptly after any nested
                // servicing; pump itself keeps meeting this rank's obligations meanwhile.
                pump(sender_id, POLLIN,
                     std::min<long>(deadline, steady_now_ms() + 100));
                continue;
            }
            // EOF or a hard error: same repair contract as guarded_read.
            if (++repairs > max_link_repairs) {
                throw std::runtime_error(transport_tag + ": link to peer " +
                                         std::to_string(sender_id) + " could not be repaired after " +
                                         std::to_string(max_link_repairs) + " attempts");
            }
            repair_link(sender_id);
            return -1;
        }
        return 1;
    };

    while (true) {
        claim.release();   // every iteration starts at a frame boundary
        // The drain queue first, EVERY iteration — not once at entry. A frame already taken
        // off this link by a nested pump is OLDER than anything still on the socket, so it
        // must go first or the stream is silently reordered; with p2p identity carrying no
        // per-operation counter, sequence order here is the only thing standing between the
        // application and a wrong-round payload. deliver_into validates identity exactly as
        // the inline path does.
        if (recover_links && links[sender_id].pending(wanted.lane) > 0) {
            const SequencedLink::Accept drained =
                    links[sender_id].deliver_into(wanted, buf.buf, buf.len);
            if (drained == SequencedLink::Accept::Delivered) {
                maybe_send_ack(sender_id);
                return;
            }
            if (drained == SequencedLink::Accept::IdentityMismatch) {
                throw std::runtime_error(transport_tag + ": message identity mismatch from peer " +
                                         std::to_string(sender_id) + " - expected [" +
                                         describe(wanted) + "] at the head of the drain queue");
            }
        }

        char encoded[frame_header_bytes];
        {
            const int header_state = read_header_yielding(encoded);
            if (header_state <= 0) {
                continue;   // 0: serve the lane; -1: repaired, start over
            }
        }

        FrameHeader arrived;
        // The cap is the LINK's maximum frame, not the application's buffer. Using buf.len
        // here conflates two different limits and rejects any frame larger than the message
        // being waited for — including a handshake frame, which is 56 bytes and arrives while
        // the application is waiting for four. What the application's length actually
        // constrains is checked below, once the frame is known to be data.
        const DecodeStatus status =
                decode_header(encoded, frame_header_bytes, link_max_frame_bytes, arrived);
        if (status != DecodeStatus::Ok) {
            throw std::runtime_error(transport_tag + ": malformed frame from peer " +
                                     std::to_string(sender_id) + " (decode status " +
                                     std::to_string(static_cast<int>(status)) +
                                     ", declared payload " + std::to_string(arrived.payload_length) +
                                     ", declared total " + std::to_string(arrived.total_length) +
                                     ", expecting " + std::to_string(buf.len) + " bytes for [" +
                                     describe(expected_identity(buf.len)) + "])");
        }

        // Any traffic from the peer carries its cumulative ack, which is what prunes retention
        // whenever the link is busy in both directions.
        links[sender_id].on_ack(arrived.cumulative_ack);

        if (arrived.frame_type == FrameType::Ack) {
            continue;
        }
        if (arrived.frame_type == FrameType::Handshake) {
            char offered[handshake_bytes];
            if (!guarded_read(offered, handshake_bytes)) {
                continue;
            }
            reconcile_handshake(sender_id, offered);
            continue;
        }

        // Classify without committing. The watermark may not move until the payload is in the
        // application's buffer — see SequencedLink::commit_inline.
        const SequencedLink::Accept accepted = links[sender_id].classify(arrived);
        if (accepted == SequencedLink::Accept::Duplicate) {
            // A replayed frame the peer had not seen acknowledged. Consume and discard its
            // payload, or the following frames would be parsed from the wrong offset.
            std::vector<char> discard(arrived.payload_length);
            if (arrived.payload_length > 0 && !guarded_read(discard.data(), discard.size())) {
                continue;
            }
            continue;
        }
        if (accepted == SequencedLink::Accept::FatalGap) {
            throw std::runtime_error(transport_tag + ": sequence gap from peer " +
                                     std::to_string(sender_id) + " - expected " +
                                     std::to_string(links[sender_id].next_received()) +
                                     " but received " + std::to_string(arrived.transport_seq));
        }

        if (recover_links && accepted == SequencedLink::Accept::Delivered &&
            links[sender_id].pending(arrived.lane) > 0) {
            // Older frames entered the drain queue while this one's header was being read —
            // the yielding read closes that window before the first byte, but not after it.
            // Deliver strictly in sequence: park this frame behind the queued ones and loop;
            // the lane check at the top serves the true head. Identity is judged at drain
            // time, exactly as for any other queued frame.
            std::vector<char> park(arrived.payload_length);
            if (arrived.payload_length > 0 && !guarded_read(park.data(), park.size())) {
                continue;
            }
            links[sender_id].accept(arrived, park.data());
            // The frame is fully consumed: the stream is at a boundary again, so the claim
            // is released BEFORE the ack goes out — the ack's own write may pump, and a
            // pump that still saw the claim would refuse to drain this very peer.
            claim.release();
            maybe_send_ack(sender_id);
            continue;
        }

        const FrameHeader expected = expected_identity(buf.len);
        if (!same_identity(arrived, expected)) {
            // The peer is in a different logical operation. Under the unframed protocol this is
            // exactly where a divergent schedule silently consumed the wrong payload.
            throw std::runtime_error(transport_tag + ": message identity mismatch from peer " +
                                     std::to_string(sender_id) + " - expected [" +
                                     describe(expected) + "] but received [" +
                                     describe(arrived) + "]");
        }
        // Identity matched on total_length, so an unfragmented frame must carry exactly the
        // bytes the application is waiting for. A frame that claims fewer would leave the rest
        // of the buffer holding whatever the next frame's header happens to be.
        if (arrived.payload_length != buf.len) {
            throw std::runtime_error(transport_tag + ": frame from peer " + std::to_string(sender_id) +
                                     " declares " + std::to_string(arrived.payload_length) +
                                     " payload bytes for a " + std::to_string(buf.len) +
                                     "-byte message");
        }
        if (!guarded_read(buf.buf, buf.len)) {
            continue;
        }
        // The payload is fully consumed — frame boundary again — so the claim drops before
        // the commit's ack can pump.
        claim.release();
        // Committed only now: a link that died anywhere above left this frame unacknowledged,
        // so the peer still holds it and replays it after the repair.
        if (link_trace() && buf.len >= sizeof(int)) {
            int fi; std::memcpy(&fi, buf.buf, sizeof(int));
            std::fprintf(stderr, "[lt] inline-deliver peer=%u seq=%llu first=%d\n",
                         static_cast<unsigned>(sender_id),
                         (unsigned long long) arrived.transport_seq, fi);
        }
        links[sender_id].commit_inline(arrived);
        // On a link the peer never receives on, this is the only thing that will ever let it
        // release what it is holding for us.
        maybe_send_ack(sender_id);
        return;
    }
}

int FMI::Comm::TcpChannelBase::fill_stage(Utils::peer_num peer) {
    if (peer >= sockets.size() || sockets[peer] < 0) {
        return -1;
    }
    InboundStage& stage = inbound_stage[peer];
    // Bounded per call: against a blocked writer on loopback the sender refills as fast as
    // this drains, so an until-EAGAIN loop could move the whole frame — minutes of somebody
    // else's wait slice, the aged redial's entire 250 ms budget, and the stuck-rank beacon's
    // 25 ms window — in one visit. The outer loops all re-enter promptly.
    std::size_t budget = 256u << 10;
    while (stage.got < stage.payload.size()) {
        if (budget == 0) {
            return 0;
        }
        long n = ::recv(sockets[peer], stage.payload.data() + stage.got,
                        std::min(stage.payload.size() - stage.got, budget), MSG_DONTWAIT);
        if (n > 0) {
            stage.got += static_cast<std::size_t>(n);
            budget -= std::min(static_cast<std::size_t>(n), budget);
            continue;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        }
        if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return 0;
        }
        return -1;
    }
    return 1;
}

void FMI::Comm::TcpChannelBase::finish_stage(Utils::peer_num peer) {
    // Cleared before dispatch: reconcile_handshake can throw, and an unwound stage must not
    // be resumed — the stream position has already moved past the frame.
    InboundStage& stage = inbound_stage[peer];
    const FrameHeader arrived = stage.header;
    std::vector<char> payload = std::move(stage.payload);
    stage = InboundStage{};
    if (link_trace()) {
        std::fprintf(stderr, "[lt] STAGE-DONE peer=%u seq=%llu len=%zu\n",
                     static_cast<unsigned>(peer),
                     (unsigned long long) arrived.transport_seq, payload.size());
    }
    if (arrived.frame_type == FrameType::Handshake) {
        reconcile_handshake(peer, payload.data());
        return;
    }
    const auto drained = links[peer].accept(arrived, payload.data());
    if (drained == SequencedLink::Accept::Delivered) {
        maybe_send_ack(peer);
    } else if (drained == SequencedLink::Accept::FatalGap) {
        std::fprintf(stderr,
                     "[FMI] DRAIN GAP peer %u: expected %llu received %llu\n",
                     static_cast<unsigned>(peer),
                     static_cast<unsigned long long>(links[peer].next_received()),
                     static_cast<unsigned long long>(arrived.transport_seq));
    }
}

bool FMI::Comm::TcpChannelBase::service_established_links(Utils::peer_num skip) {
    if (!recover_links || links.size() != num_peers) {
        return false;
    }
    bool progress = false;
    for (Utils::peer_num peer = 0; peer < num_peers; peer++) {
        if (peer == skip || peer == peer_id || peer >= sockets.size() || sockets[peer] < 0) {
            continue;
        }
        if (peer < app_owns_stream.size() && app_owns_stream[peer]) {
            // The application is mid-frame on this link (reachable through a nested pump: the
            // direct pump's skip protects only its own peer). The bytes on the socket belong
            // to the frame the application is reading; touching them here would splice the
            // stream. The application actively consuming the link is also stronger liveness
            // evidence than any peek, so standing suspicion is dropped — without this, the
            // clear at the bottom of this loop is unreachable for exactly as long as the app
            // holds the claim, and the aged redial would kill a healthy link.
            if (peer < link_suspect_since.size()) {
                link_suspect_since[peer] = 0;
            }
            if (link_trace()) {
                static thread_local long last_own_note = 0;
                const long now = steady_now_ms();
                if (now - last_own_note > 3000) {
                    last_own_note = now;
                    std::fprintf(stderr, "[lt] SERVICING-APP-OWNS peer=%u\n",
                                 static_cast<unsigned>(peer));
                }
            }
            continue;
        }
        // Pay off any reconciliation this link is owed. Deferring it to the application's next
        // send or receive on this peer is not good enough: the peer is waiting for the replay
        // that goes out with the handshake, and this rank may be blocked on somebody else for
        // as long as the job lasts. Measured directly - a rank sat 39 seconds holding a link
        // marked for reconciliation while its peer waited for exactly that.
        try {
            reconcile_if_needed(peer);
        } catch (const std::exception&) {
            continue;   // the receive path owns repair
        }
        // Re-offer any ack this link is owed. An ack suppressed while a write toward this
        // peer was mid-frame has no other reliable retry: the commit-time offers only fire
        // when another frame arrives, and when the ack is what opens the peer's window,
        // nothing more ever does. Self-gating — ack_due and the freeze both no-op it.
        maybe_send_ack(peer);
        // Bounded: drain what is already there and return. This runs inside somebody else's
        // establishment loop and must not become one itself.
        for (int taken = 0; taken < 64; taken++) {
            if (inbound_stage[peer].active) {
                const std::size_t got_before = inbound_stage[peer].got;
                const int filled = fill_stage(peer);
                if (filled >= 0 && inbound_stage[peer].got > got_before) {
                    progress = true;
                }
                if (filled < 0) {
                    // Dead mid-frame: observe exactly as the dead-peek path below does. The
                    // stage stays — the frame was never committed, so it was never acked, and
                    // the replacement's replay delivers it whole after the stage is discarded.
                    if (link_trace()) {
                        std::fprintf(stderr, "[lt] SERVICING-SAW-DEAD peer=%u fd=%d n=-1 errno=%d\n",
                                     static_cast<unsigned>(peer), sockets[peer], errno);
                    }
                    if (link_suspect_since.size() != num_peers) {
                        link_suspect_since.assign(num_peers, 0);
                    }
                    if (link_suspect_since[peer] == 0) {
                        link_suspect_since[peer] = steady_now_ms();
                    }
                    break;
                }
                if (filled == 0) {
                    break;   // no more bytes buffered; later passes continue the stage
                }
                if (peer < link_suspect_since.size()) {
                    link_suspect_since[peer] = 0;
                }
                try {
                    finish_stage(peer);
                } catch (const std::exception&) {
                    break;   // the receive path owns repair; leave this link to it
                }
                continue;
            }
            char encoded[frame_header_bytes];
            long n = ::recv(sockets[peer], encoded, frame_header_bytes, MSG_PEEK | MSG_DONTWAIT);
            if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
                // The peer's end is gone. A first fix closed and marked the link here and gave
                // servicing a bounded re-dial; it made the post-restore establishment wedge
                // WORSE (1/8 vs 4/6 on variable_payloads at 7 ranks) - build_mesh re-entered
                // from inside the servicing loop it itself drives was never proven safe. Left
                // as a trace-visible observation until that is designed properly.
                if (link_trace()) {
                    std::fprintf(stderr, "[lt] SERVICING-SAW-DEAD peer=%u fd=%d n=%ld errno=%d\n",
                                 static_cast<unsigned>(peer), sockets[peer], n, errno);
                }
                if (link_suspect_since.size() != num_peers) {
                    link_suspect_since.assign(num_peers, 0);
                }
                if (link_suspect_since[peer] == 0) {
                    link_suspect_since[peer] = steady_now_ms();
                }
                break;
            }
            if (n < 0 || static_cast<std::size_t>(n) < frame_header_bytes) {
                break;
            }
            if (peer < link_suspect_since.size()) {
                link_suspect_since[peer] = 0;   // bytes arrived; the link is alive after all
            }
            FrameHeader arrived;
            const DecodeStatus svc_status =
                    decode_header(encoded, frame_header_bytes, link_max_frame_bytes, arrived);
            if (svc_status != DecodeStatus::Ok) {
                if (link_trace()) {
                    static thread_local long last_dec_note = 0;
                    const long now = steady_now_ms();
                    if (now - last_dec_note > 3000) {
                        last_dec_note = now;
                        char hex[3 * frame_header_bytes + 1];
                        for (std::size_t b = 0; b < frame_header_bytes; b++) {
                            std::snprintf(hex + 3 * b, 4, "%02x ",
                                          static_cast<unsigned char>(encoded[b]));
                        }
                        std::fprintf(stderr, "[lt] SERVICING-DECODE-FAIL peer=%u status=%d bytes=%s\n",
                                     static_cast<unsigned>(peer),
                                     static_cast<int>(svc_status), hex);
                    }
                }
                // A full 72-byte peek that does not decode is a desynchronised stream, and
                // waiting cannot fix it — the same bytes will be there forever. Mark the
                // link suspect exactly as a dead peek would: the dialer's aged redial tears
                // it down and the replay restores alignment, and if this side is the
                // listener, the application's own read of the same bytes throws loudly and
                // repairs. Both paths self-heal; a silent break here wedged a job for its
                // whole deadline with 400 KB sitting unread. CAPPED per connection: a
                // content-level rejection reproduces identically after every replay, and an
                // uncapped mark turns one loud error into an endless redial storm.
                if (decode_fail_marks.size() != num_peers) {
                    decode_fail_marks.assign(num_peers, 0);
                }
                if (++decode_fail_marks[peer] <= 2) {
                    if (link_suspect_since.size() != num_peers) {
                        link_suspect_since.assign(num_peers, 0);
                    }
                    if (link_suspect_since[peer] == 0) {
                        link_suspect_since[peer] = steady_now_ms();
                    }
                }
                break;
            }
            // Take the frame whole when all of it has arrived; otherwise stage it. The old
            // rule — only ever take complete frames — kept the link from being left half-read,
            // but it also made a frame larger than the socket receive buffer undrainable by
            // anyone except the application receiving on this exact link, and after a restore
            // the replay carrying such a frame routinely meets a receiver parked on a
            // different peer for good. The stage records the mid-frame position, so the link
            // is never *ambiguously* half-read: every later pass resumes exactly where this
            // one stopped, and nothing is committed until the last byte is in local memory.
            int available = 0;
            if (::ioctl(sockets[peer], FIONREAD, &available) != 0) {
                break;
            }
            if (static_cast<std::size_t>(available) < frame_header_bytes + arrived.payload_length) {
                // A frame that is still arriving shows a growing byte count from pass to
                // pass, and the fast path takes it whole once the last byte lands —
                // byte-for-byte as before staging existed. A frame that has STOPPED growing
                // while incomplete is jammed: the peer's TCP window closed against a reader
                // that is not reading, which for a frame larger than the buffer is
                // permanent. Progress is judged empirically rather than against SO_RCVBUF —
                // the kernel's reported buffer size is inflated by receive autotuning while
                // a reader drains fast, and trusting it once left a 2 MiB frame stranded
                // behind a ~512 KB closed window forever, silently.
                const long now_stall = steady_now_ms();
                if (available > inbound_stage[peer].stalled_available) {
                    inbound_stage[peer].stalled_available = available;
                    inbound_stage[peer].stalled_at_ms = now_stall;
                    if (link_trace()) {
                        static thread_local long last_gr_note = 0;
                        if (now_stall - last_gr_note > 3000) {
                            last_gr_note = now_stall;
                            std::fprintf(stderr,
                                         "[lt] SERVICING-GROWING peer=%u avail=%d need=%zu\n",
                                         static_cast<unsigned>(peer), available,
                                         frame_header_bytes + arrived.payload_length);
                        }
                    }
                    break;   // still arriving; a later pass re-judges
                }
                if (now_stall - inbound_stage[peer].stalled_at_ms < 20) {
                    break;   // unchanged, but not for long enough to call it jammed
                }
                char consumed[frame_header_bytes];
                std::size_t off = 0;
                while (off < frame_header_bytes) {
                    long taken_bytes = ::recv(sockets[peer], consumed + off,
                                              frame_header_bytes - off, MSG_DONTWAIT);
                    if (taken_bytes <= 0) {
                        if (taken_bytes == -1 && errno == EINTR) {
                            continue;
                        }
                        break;   // cannot happen — the peek just saw the full header
                    }
                    off += static_cast<std::size_t>(taken_bytes);
                }
                if (off < frame_header_bytes) {
                    break;
                }
                links[peer].on_ack(arrived.cumulative_ack);
                progress = true;   // the header is consumed; the stream advanced
                InboundStage& stage = inbound_stage[peer];
                stage.active = true;
                stage.header = arrived;
                stage.payload.resize(arrived.payload_length);
                stage.got = 0;
                stage.stalled_available = -1;
                if (link_trace()) {
                    std::fprintf(stderr, "[lt] STAGE peer=%u seq=%llu len=%u\n",
                                 static_cast<unsigned>(peer),
                                 (unsigned long long) arrived.transport_seq,
                                 arrived.payload_length);
                }
                continue;   // the next pass fills it from what is already buffered
            }
            // The whole frame is here; the stream advances past it, so the next head frame
            // starts its progress judgement fresh.
            inbound_stage[peer].stalled_available = -1;
            try {
                read_all(peer, encoded, frame_header_bytes);
                progress = true;
                links[peer].on_ack(arrived.cumulative_ack);
                if (arrived.frame_type == FrameType::Ack) {
                    continue;
                }
                std::vector<char> payload(arrived.payload_length);
                if (arrived.payload_length > 0) {
                    read_all(peer, payload.data(), payload.size());
                }
                if (arrived.frame_type == FrameType::Handshake) {
                    reconcile_handshake(peer, payload.data());
                    continue;
                }
                const auto drained = links[peer].accept(arrived, payload.data());
                if (drained == SequencedLink::Accept::Delivered) {
                    maybe_send_ack(peer);
                } else if (drained == SequencedLink::Accept::FatalGap) {
                    std::fprintf(stderr,
                                 "[FMI] DRAIN GAP peer %u: expected %llu received %llu\n",
                                 static_cast<unsigned>(peer),
                                 static_cast<unsigned long long>(links[peer].next_received()),
                                 static_cast<unsigned long long>(arrived.transport_seq));
                }
            } catch (const std::exception&) {
                break;   // the receive path owns repair; leave this link to it
            }
        }
    }
    return progress;
}

void FMI::Comm::TcpChannelBase::adopt_link(Utils::peer_num partner_id, int fd) {
    if (sockets.empty()) {
        sockets = std::vector<int>(num_peers, -1);
    }
    if (partner_id >= sockets.size() || sockets[partner_id] >= 0) {
        return;   // already hold one; the caller keeps its descriptor
    }
    sockets[partner_id] = fd;
    apply_socket_options(fd);
    bump_generation(partner_id);
    if (!recover_links) {
        return;
    }
    ensure_link_state();
    // Deliberately does NOT hand over the handshake and replay here. Adoption runs inside
    // servicing, and replay can be large: a write that blocks there stalls this rank on one
    // peer while every other link goes unread, which is the deadlock the servicing exists to
    // prevent. The mark stays set and reconcile_if_needed pays it off from the application's
    // own thread. Nothing is waiting on it meanwhile — the handshake is one way.
}

void FMI::Comm::TcpChannelBase::reconcile_if_needed(Utils::peer_num partner_id) {
    if (!recover_links || partner_id >= link_needs_reconcile.size() ||
        !link_needs_reconcile[partner_id]) {
        return;
    }
    if (frozen(partner_id)) {
        // A write_all is mid-frame toward this peer; a handshake and replay written now
        // would splice into that frame. The mark stays set and is paid once the frame
        // completes.
        return;
    }
    ensure_link_state();
    link_needs_reconcile[partner_id] = 0;
    try {
        exchange_handshake(partner_id);
    } catch (...) {
        // The debt is NOT paid: a replay that threw partway may have left a partial frame
        // on the wire, and clearing the mark on that path meant this end never replayed
        // again while the link itself was never replaced. Restore it and let the caller's
        // error handling decide.
        link_needs_reconcile[partner_id] = 1;
        throw;
    }
}

void FMI::Comm::TcpChannelBase::note_link_replaced(FMI::Utils::peer_num partner_id) {
    if (!recover_links) {
        return;
    }
    if (link_needs_reconcile.size() != num_peers) {
        link_needs_reconcile.assign(num_peers, 0);
    }
    link_needs_reconcile[partner_id] = 1;
    bump_generation(partner_id);
    if (partner_id < inbound_stage.size()) {
        // Whatever was mid-arrival died with the old stream. The staged frame was never
        // committed, so it was never acked: the peer still retains it, and the replay that
        // follows the replacement delivers it whole.
        inbound_stage[partner_id] = InboundStage{};
    }
    if (partner_id < link_suspect_since.size()) {
        // The suspicion belonged to the connection that just died. Carrying it onto the
        // replacement lets the aged redial tear down a healthy link the moment nothing
        // happens to clear it — and the application being mid-frame on it (which suppresses
        // servicing's peek, the usual clearer) is exactly such a moment.
        link_suspect_since[partner_id] = 0;
    }
    if (partner_id < decode_fail_marks.size()) {
        decode_fail_marks[partner_id] = 0;   // the marks belonged to the replaced stream
    }
}

void FMI::Comm::TcpChannelBase::check_socket(FMI::Utils::peer_num partner_id, const std::string& name) {
    if (sockets.empty()) {
        sockets = std::vector<int>(num_peers, -1);
    }
    if (sockets[partner_id] != -1) {
        return;
    }
    sockets[partner_id] = establish(partner_id, name);
    apply_socket_options(sockets[partner_id]);
    bump_generation(partner_id);
    // A link the transport replaced under us belongs to a peer that is repairing, so this end
    // owes it the same handshake and replay a local repair would have produced. Establishing
    // for the first time still costs nothing.
    if (partner_id < link_needs_reconcile.size() && link_needs_reconcile[partner_id]) {
        link_needs_reconcile[partner_id] = 0;
        ensure_link_state();
        exchange_handshake(partner_id);
    }
}

void FMI::Comm::TcpChannelBase::apply_socket_options(int fd) const {
    struct timeval timeout;
    timeout.tv_sec = max_timeout / 1000;
    timeout.tv_usec = (max_timeout % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof timeout);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, sizeof timeout);
    // Disable Nagle algorithm to avoid 40ms TCP ack delays
    int one = 1;
    // SOL_TCP not defined on macOS
    #if !defined(SOL_TCP) && defined(IPPROTO_TCP)
    #define SOL_TCP IPPROTO_TCP
    #endif
    setsockopt(fd, SOL_TCP, TCP_NODELAY, &one, sizeof(one));
}

double FMI::Comm::TcpChannelBase::get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) {
    double agg_bandwidth = bandwidth;
    double trans_time = producer * consumer * ((double) size_in_bytes / 1000000.) / agg_bandwidth;
    return log2(producer + consumer) * overhead + trans_time;
}

double FMI::Comm::TcpChannelBase::get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) {
    double transfer_costs = 2 * consumer * producer * ((double) size_in_bytes / 1000000000.) * transfer_price;
    double total_costs = transfer_costs;
    if (include_infrastructure_costs) {
        total_costs += 1. / requests_per_hour * vm_price;
    }
    return total_costs;
}

void FMI::Comm::TcpChannelBase::finalize() {
    // The goodbye must happen HERE, not (only) in the subclass destructor: the Communicator
    // finalizes every channel explicitly before destruction, so by destructor time the
    // sockets are long closed and a destructor-side drain is a no-op over dead fds. Traced
    // on baseline@16p: the finisher's TEARDOWN-BEGIN showed every fd already -1 with 960
    // bytes still retained for one peer — retention this close() then destroyed, wedging
    // that peer in establishment for its whole deadline.
    drain_links_for_shutdown();
    close_sockets();
    close_transport_state();
}

void FMI::Comm::TcpChannelBase::prepare_for_checkpoint() {
    close_sockets();
    close_transport_state();
}

bool FMI::Comm::TcpChannelBase::reconfigure_for_epoch(const std::string& new_comm_name,
                                                      const std::vector<Utils::peer_num>& moved_ranks) {
    const bool self_moved =
            std::find(moved_ranks.begin(), moved_ranks.end(), peer_id) != moved_ranks.end();
    if (self_moved) {
        // This rank IS the one that moved. Every survivor is about to reset its link to this
        // rank to zero, so every link this side holds must reset too — resetting only
        // links[self] (which the loop below would do) is meaningless, and keeping the others
        // makes this rank's first framed send report a gap that never happened on the wire.
        // A fresh replacement gets this for free (its links are zero by construction); the
        // in-place and CRIU-restored shapes re-enter here with the old process's counters
        // still in memory, which is exactly what must not survive the epoch.
        close_sockets();
        for (auto& link : links) {
            link = SequencedLink({link_window_frames, link_max_frame_bytes,
                                  link_retention_limit_bytes});
            link.set_incarnation(local_incarnation);
        }
        set_comm_name(new_comm_name);
        return true;
    }
    for (auto rank : moved_ranks) {
        if (rank < sockets.size() && sockets[rank] >= 0) {
            close(sockets[rank]);
            sockets[rank] = -1;
        }
        if (rank < inbound_stage.size()) {
            inbound_stage[rank] = InboundStage{};
        }
        // The transport sequence is scoped to a link, and re-pairing to a migrated rank
        // creates a new one: the replacement's counters start at zero whether it is a fresh
        // process or the migrated rank re-entering under the branch above. Carrying the old
        // link's counters over would make the survivor's first framed exchange with the
        // replacement report a gap that never happened.
        if (rank < links.size()) {
            links[rank] = SequencedLink({link_window_frames, link_max_frame_bytes,
                                         link_retention_limit_bytes});
            links[rank].set_incarnation(local_incarnation);
        }
    }
    set_comm_name(new_comm_name);
    return true;
}

void FMI::Comm::TcpChannelBase::drain_links_for_shutdown() {
    // Only the framed, recoverable transport has retention and acks to settle; the legacy
    // modes keep their abrupt close. Contract and rationale on the declaration.
    if (!framed || !recover_links || links.empty() || sockets.empty()) {
        return;
    }
    // Idle deadline, not total: a peer 25 rounds behind (deep_rounds' pipelined drift)
    // legitimately takes many seconds to work through its lanes, and it only advances our
    // retention when its own consumption reaches the socket again — its ack watermark is
    // its committed prefix, so no fixed grace can cover it without covering every possible
    // backlog. Progress (retention shrinking, a link resolving) re-arms the grace; only a
    // QUIET grace — no progress at all — gives up. The absolute cap is the transport's own
    // patience, so teardown can never outwait what an operation would.
    tearing_down = true;
    struct TearingGuard {
        bool& flag;
        ~TearingGuard() { flag = false; }
    } tearing_guard{tearing_down};
    const long absolute_cap = steady_now_ms() + static_cast<long>(max_timeout);
    long quiet_deadline = steady_now_ms() + teardown_grace_ms;
    if (link_trace()) {
        std::string held;
        for (Utils::peer_num q = 0; q < num_peers && q < sockets.size(); q++) {
            if (q == peer_id || q >= links.size()) { continue; }
            held += " p" + std::to_string(q) + "{fd=" + std::to_string(sockets[q]) +
                    " ret=" + std::to_string(links[q].retained_bytes()) + "}";
        }
        std::fprintf(stderr, "[lt] TEARDOWN-BEGIN%s\n", held.c_str());
    }
    // Any exception here means the goodbye cannot be completed on some link; falling through
    // to the caller's abrupt close is exactly the pre-drain behaviour, and a destructor must
    // not let anything propagate.
    try {
        // Phase 1: stay until every live link's retention is empty. An unacked frame is one
        // the peer has not committed yet; those bytes may still be in flight, and only the
        // peer's ack proves them safe from the close below. Links whose socket is already
        // dead cannot drain and are not waited for.
        std::size_t last_outstanding_bytes = SIZE_MAX;
        while (steady_now_ms() < quiet_deadline && steady_now_ms() < absolute_cap) {
            // The FULL transport, not just established links: a peer still mid-job may be
            // re-dialing us right now, and its connect sits in the listener backlog until
            // accept_one/adopt_pending answer it. A teardown that stops accepting turns
            // that peer's recoverable redial into its whole establishment deadline.
            service_transport();
            service_established_links(num_peers);
            // Flush the sub-interval ack tail on every live link. Without this the drain
            // CANNOT converge: standalone acks are interval-gated, no data frame will ever
            // piggyback again, so the last (interval-1) committed frames of every link
            // would go unacknowledged on both sides forever and both peers would burn
            // their whole grace (adversarial-review finding, confirmed).
            for (Utils::peer_num q = 0; q < num_peers && q < sockets.size(); q++) {
                if (q != peer_id && sockets[q] >= 0) {
                    maybe_send_ack(q, /*flush_tail=*/true);
                }
            }
            std::size_t outstanding_bytes = 0;
            bool outstanding = false;
            for (Utils::peer_num q = 0; q < num_peers && q < sockets.size(); q++) {
                if (q == peer_id || sockets[q] < 0 || q >= links.size()) {
                    continue;
                }
                if (links[q].replay_suffix().empty()) {
                    continue;
                }
                // A link whose inbound is already EOF or error can never drain: acks are
                // writes from the peer, and a peer that half-closed (or died) sends no more
                // of them. Waiting for it would burn the whole grace on a ghost. Close it
                // and move on — its retention is lost exactly as it would have been at the
                // grace deadline, no later.
                char probe;
                const long n = ::recv(sockets[q], &probe, 1, MSG_PEEK | MSG_DONTWAIT);
                if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK &&
                               errno != EINTR)) {
                    ::close(sockets[q]);
                    sockets[q] = -1;
                    continue;
                }
                outstanding = true;
                outstanding_bytes += links[q].retained_bytes();
            }
            if (!outstanding) {
                break;
            }
            if (outstanding_bytes < last_outstanding_bytes) {
                last_outstanding_bytes = outstanding_bytes;
                quiet_deadline = steady_now_ms() + teardown_grace_ms;
            }
            poll_live_sockets(20);
        }
        // Phases 2+3, merged into one pass: half-close every link whose current connection
        // has not had its FIN yet, then wait for the peers' own FINs, still servicing the
        // whole transport — a peer repairing its last link may redial (the listener must
        // answer), replay frames at us, and our acks are what release its phase 1. FINs are
        // tracked per link GENERATION, not once up front: an adoption during this wait hands
        // the peer a fresh connection whose replay has already been written by the adopt,
        // and that connection needs its own goodbye. EOF is only reported once the stream
        // before it is drained, so a close here never outraces data. Same idle-not-total
        // regime: every link that resolves re-arms the quiet grace, because a still-working
        // peer FINs only when it finishes.
        std::vector<std::uint64_t> finned_gen(sockets.size(), UINT64_MAX);
        int last_open = -1;
        quiet_deadline = std::max(quiet_deadline, steady_now_ms() + teardown_grace_ms);
        while (steady_now_ms() < quiet_deadline && steady_now_ms() < absolute_cap) {
            service_transport();
            service_established_links(num_peers);
            int open = 0;
            for (Utils::peer_num q = 0; q < sockets.size(); q++) {
                if (q == peer_id || sockets[q] < 0) {
                    continue;
                }
                // Settle acks BEFORE the FIN: shutdown(WR) closes our ack channel, and an
                // unsent tail ack would leave the peer's phase 1 waiting its whole grace
                // for an acknowledgment that can never come.
                maybe_send_ack(q, /*flush_tail=*/true);
                if (finned_gen[q] != generation(q) &&
                    (q >= links.size() || !links[q].ack_due(1))) {
                    ::shutdown(sockets[q], SHUT_WR);
                    finned_gen[q] = generation(q);
                }
                char probe;
                const long n = ::recv(sockets[q], &probe, 1, MSG_PEEK | MSG_DONTWAIT);
                if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK &&
                               errno != EINTR)) {
                    ::close(sockets[q]);
                    sockets[q] = -1;
                } else {
                    open++;
                }
            }
            if (open == 0) {
                if (link_trace()) {
                    std::fprintf(stderr, "[lt] TEARDOWN-DONE all-fins\n");
                }
                return;
            }
            if (last_open < 0 || open < last_open) {
                last_open = open;
                quiet_deadline = steady_now_ms() + teardown_grace_ms;
            }
            poll_live_sockets(20);
        }
        if (link_trace()) {
            std::fprintf(stderr, "[lt] TEARDOWN-GAVE-UP quiet=%d cap=%d\n",
                         steady_now_ms() >= quiet_deadline,
                         steady_now_ms() >= absolute_cap);
        }
    } catch (...) {
        // Fall through to the abrupt close the caller performs anyway.
        if (link_trace()) {
            std::fprintf(stderr, "[lt] TEARDOWN-THREW\n");
        }
    }
}

void FMI::Comm::TcpChannelBase::poll_live_sockets(int slice_ms) {
    std::vector<pollfd> fds;
    for (Utils::peer_num q = 0; q < sockets.size(); q++) {
        if (q != peer_id && sockets[q] >= 0) {
            fds.push_back(pollfd{sockets[q], POLLIN, 0});
        }
    }
    if (!fds.empty()) {
        ::poll(fds.data(), fds.size(), slice_ms);
    }
}

void FMI::Comm::TcpChannelBase::close_sockets() {
    for (auto& socket_fd : sockets) {
        if (socket_fd >= 0) {
            close(socket_fd);
            socket_fd = -1;
        }
    }
    for (auto& stage : inbound_stage) {
        stage = InboundStage{};
    }
    std::fill(app_owns_stream.begin(), app_owns_stream.end(), 0);
}
