#include <boost/test/unit_test.hpp>

#include "../include/comm/LinkFrame.h"
#include "../include/comm/SequencedLink.h"

#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <vector>

//! Durability of a link across a real connection break.
/*!
 * SequencedLink carries the retention ring, the two watermarks and the handshake, but until
 * now nothing drove them over an actual socket that dies mid-stream. These cases do: frames
 * are serialised through a socketpair, the socket is destroyed at a chosen instant, a fresh
 * one is created, both ends exchange handshakes, and the sender replays.
 *
 * The properties under test are the ones the design contract calls normative:
 *   no loss        — every message whose send completed is eventually delivered
 *   no duplication — a replayed frame is delivered at most once
 *   FIFO           — delivery order matches send order
 *   retention safety — the sender never prunes a frame the receiver does not hold
 *
 * Deliberately NOT covered: any of this happening concurrently with application progress.
 * That needs the progress engine, which does not exist yet.
 */
BOOST_AUTO_TEST_SUITE(LinkRecovery)

using namespace FMI::Comm;
using Accept = SequencedLink::Accept;

namespace {
    //! A pair of endpoints joined by a socket that can be destroyed and remade at will.
    struct Endpoints {
        SequencedLink sender;
        SequencedLink receiver;
        int fds[2] = {-1, -1};

        //! Everything the receiving application has consumed, in order.
        std::vector<std::string> consumed;
        //! Everything the sending application handed to the link, in order.
        std::vector<std::string> posted;

        explicit Endpoints(SequencedLink::Config cfg = {16, 1u << 20, 1u << 20})
                : sender(cfg), receiver(cfg) {
            connect();
        }

        ~Endpoints() { disconnect(); }

        void connect() {
            BOOST_REQUIRE_EQUAL(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
        }

        void disconnect() {
            if (fds[0] >= 0) { ::close(fds[0]); fds[0] = -1; }
            if (fds[1] >= 0) { ::close(fds[1]); fds[1] = -1; }
        }

        //! Kill the connection. Anything still in the socket buffers is lost, which is exactly
        //! what a checkpoint taken with --tcp-close does to a live link.
        void break_link() { disconnect(); }

        void reconnect() { connect(); }

        static FrameHeader identity(std::size_t len) {
            FrameHeader h;
            h.lane = Lane::P2P;
            h.op_kind = OpKind::Send;
            h.root = 1;
            h.payload_length = static_cast<std::uint32_t>(len);
            return h;
        }

        //! Application send: retain first, then put the bytes on the wire.
        bool post(const std::string& body) {
            FrameHeader stamped;
            if (!sender.admit(identity(body.size()), body.data(), body.size(),
                              stamped)) {
                return false;   // window or retention cap
            }
            posted.push_back(body);
            write_frame(stamped, body);
            return true;
        }

        void write_frame(const FrameHeader& h, const std::string& body) {
            if (fds[0] < 0) { return; }   // link is down; the frame stays in retention
            char hdr[frame_header_bytes];
            encode_header(h, hdr);
            if (::write(fds[0], hdr, sizeof hdr) != static_cast<ssize_t>(sizeof hdr)) { return; }
            if (!body.empty()) {
                if (::write(fds[0], body.data(), body.size()) !=
                    static_cast<ssize_t>(body.size())) { return; }
            }
        }

        //! Drain whatever is currently readable, offering each complete frame to the receiver.
        void pump() {
            if (fds[1] < 0) { return; }
            while (true) {
                char hdr[frame_header_bytes];
                ssize_t n = ::recv(fds[1], hdr, sizeof hdr, MSG_DONTWAIT);
                if (n <= 0) { return; }
                BOOST_REQUIRE_EQUAL(n, static_cast<ssize_t>(sizeof hdr));

                FrameHeader h;
                BOOST_REQUIRE(decode_header(hdr, sizeof hdr, 1u << 20, h) == DecodeStatus::Ok);
                std::string body(h.payload_length, '\0');
                if (h.payload_length > 0) {
                    ssize_t got = ::recv(fds[1], body.data(), h.payload_length, MSG_WAITALL);
                    BOOST_REQUIRE_EQUAL(got, static_cast<ssize_t>(h.payload_length));
                }

                const Accept a = receiver.accept(h, body.data());
                if (a == Accept::Delivered) {
                    // The application consumes it immediately in this harness.
                    std::string dst(h.payload_length, '\0');
                    FrameHeader expected = identity(h.payload_length);
                    BOOST_REQUIRE(receiver.deliver_into(expected, dst.data(), dst.size())
                                  == Accept::Delivered);
                    consumed.push_back(dst);
                }
                // Duplicate is the expected outcome for a replayed frame and is simply dropped.
            }
        }

        //! What a re-established link does before any data flows again.
        void handshake_and_replay() {
            const HandshakePayload from_receiver = receiver.local_handshake();
            std::string error;
            BOOST_REQUIRE_MESSAGE(sender.reconcile(from_receiver, error), error);
            for (const auto& r : sender.replay_suffix()) {
                write_frame(r.header, std::string(r.payload.begin(), r.payload.end()));
            }
            pump();
        }
    };

    std::string body_for(int i) { return "message-" + std::to_string(i); }
}

BOOST_AUTO_TEST_CASE(nothing_is_lost_when_the_link_dies_with_frames_in_flight) {
    Endpoints e;
    for (int i = 0; i < 5; i++) { BOOST_REQUIRE(e.post(body_for(i))); }
    e.pump();
    const std::size_t before = e.consumed.size();
    BOOST_REQUIRE_EQUAL(before, 5u);

    // More frames, then the link dies before the receiver ever reads them.
    for (int i = 5; i < 9; i++) { BOOST_REQUIRE(e.post(body_for(i))); }
    e.break_link();
    BOOST_CHECK_EQUAL(e.consumed.size(), before);   // they never arrived

    e.reconnect();
    e.handshake_and_replay();

    BOOST_REQUIRE_EQUAL(e.consumed.size(), 9u);
    for (int i = 0; i < 9; i++) {
        BOOST_CHECK_EQUAL(e.consumed[i], body_for(i));
    }
}

BOOST_AUTO_TEST_CASE(a_frame_the_receiver_already_held_is_not_delivered_twice) {
    Endpoints e;
    for (int i = 0; i < 4; i++) { BOOST_REQUIRE(e.post(body_for(i))); }
    e.pump();
    BOOST_REQUIRE_EQUAL(e.consumed.size(), 4u);

    // The receiver holds all four, but its ack never reached the sender before the break, so
    // the sender still has every one of them in retention and will replay them all.
    BOOST_CHECK_EQUAL(e.sender.replay_suffix().size(), 4u);
    e.break_link();
    e.reconnect();
    e.handshake_and_replay();

    // The handshake carried the receiver's next_expected, which prunes retention; anything
    // that still gets replayed must dedup rather than reach the application again.
    BOOST_CHECK_EQUAL(e.consumed.size(), 4u);
    for (int i = 0; i < 4; i++) {
        BOOST_CHECK_EQUAL(e.consumed[i], body_for(i));
    }
}

BOOST_AUTO_TEST_CASE(the_handshake_prunes_exactly_what_the_receiver_holds) {
    Endpoints e;
    for (int i = 0; i < 6; i++) { BOOST_REQUIRE(e.post(body_for(i))); }
    e.pump();                       // receiver takes all six
    BOOST_REQUIRE_EQUAL(e.consumed.size(), 6u);
    BOOST_CHECK_EQUAL(e.sender.replay_suffix().size(), 6u);

    e.break_link();
    e.reconnect();
    std::string error;
    BOOST_REQUIRE(e.sender.reconcile(e.receiver.local_handshake(), error));
    // Retention safety: the sender may drop only what the receiver durably holds, which here
    // is everything, so nothing needs replaying.
    BOOST_CHECK_EQUAL(e.sender.replay_suffix().size(), 0u);
    BOOST_CHECK_EQUAL(e.sender.lowest_retained(), e.sender.next_send_seq());
}

BOOST_AUTO_TEST_CASE(repeated_breaks_still_deliver_exactly_once_in_order) {
    // Breaking at several different points exercises the reconciliation from a range of
    // watermark combinations rather than one convenient instant.
    Endpoints e;
    int next = 0;
    for (int round = 0; round < 5; round++) {
        for (int k = 0; k < 3; k++) { BOOST_REQUIRE(e.post(body_for(next++))); }
        if (round % 2 == 0) { e.pump(); }    // sometimes the receiver has read, sometimes not
        e.break_link();
        e.reconnect();
        e.handshake_and_replay();
    }
    e.pump();

    BOOST_REQUIRE_EQUAL(e.consumed.size(), static_cast<std::size_t>(next));
    for (int i = 0; i < next; i++) {
        BOOST_CHECK_EQUAL(e.consumed[i], body_for(i));
    }
}

BOOST_AUTO_TEST_CASE(a_break_between_commit_and_ack_still_delivers_exactly_once) {
    // The freeze position the design calls out as load-bearing: the receiver has committed a
    // frame and is therefore obliged to keep it, but the sender has not learned that yet. The
    // frame must survive in the image AND be deduplicated when the sender replays it.
    Endpoints e;
    BOOST_REQUIRE(e.post(body_for(0)));
    e.pump();                                   // committed and consumed
    BOOST_REQUIRE_EQUAL(e.consumed.size(), 1u);
    BOOST_CHECK_EQUAL(e.receiver.ack_safe(), 1u);
    BOOST_CHECK_EQUAL(e.sender.replay_suffix().size(), 1u);   // sender has not been told

    e.break_link();
    e.reconnect();
    e.handshake_and_replay();

    BOOST_CHECK_EQUAL(e.consumed.size(), 1u);   // not delivered a second time
    BOOST_CHECK_EQUAL(e.consumed[0], body_for(0));
}

BOOST_AUTO_TEST_CASE(the_window_bounds_retention_while_the_link_is_down) {
    // With the link down nothing can be acked, so the window is the only thing keeping
    // retention finite. Once it is full the send must be refused rather than silently
    // dropping an obligation the sender has already accepted.
    Endpoints e({4, 1u << 20, 1u << 20});
    e.break_link();
    int accepted = 0;
    for (int i = 0; i < 10; i++) {
        if (e.post(body_for(i))) { accepted++; } else { break; }
    }
    BOOST_CHECK_EQUAL(accepted, 4);
    BOOST_CHECK(e.sender.send_blocked());
    BOOST_CHECK_EQUAL(e.sender.replay_suffix().size(), 4u);

    e.reconnect();
    e.handshake_and_replay();
    BOOST_REQUIRE_EQUAL(e.consumed.size(), 4u);
    for (int i = 0; i < 4; i++) {
        BOOST_CHECK_EQUAL(e.consumed[i], body_for(i));
    }
    // With the backlog delivered and acked, the window reopens.
    std::string error;
    BOOST_REQUIRE(e.sender.reconcile(e.receiver.local_handshake(), error));
    BOOST_CHECK(!e.sender.send_blocked());
}

BOOST_AUTO_TEST_SUITE_END()
