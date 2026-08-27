#include <boost/test/unit_test.hpp>

#include "../include/comm/LinkFrame.h"
#include "../include/comm/SequencedLink.h"

#include <cstring>
#include <string>
#include <vector>


BOOST_AUTO_TEST_SUITE(LinkLayer)

using namespace FMI::Comm;
using Accept = SequencedLink::Accept;

namespace {
    //! Identity of an application-level point-to-point message.
    FrameHeader p2p(std::uint32_t dest, std::uint64_t len) {
        FrameHeader h;
        h.lane = Lane::P2P;
        h.op_kind = OpKind::Send;
        h.collective_index = 0;
        h.root = dest;
        h.payload_length = static_cast<std::uint32_t>(len);
        return h;
    }

    //! Identity of a fragment belonging to collective number @p index.
    FrameHeader collective(OpKind op, std::uint64_t index, std::uint32_t root,
                           std::uint64_t len, bool commutative = true, bool associative = true) {
        FrameHeader h;
        h.lane = Lane::Collective;
        h.op_kind = op;
        h.collective_index = index;
        h.root = root;
        h.commutative = commutative;
        h.associative = associative;
        h.payload_length = static_cast<std::uint32_t>(len);
        return h;
    }

    //! Push one frame through a link, as the transport would.
    Accept offer(SequencedLink& link, const FrameHeader& stamped, const std::string& payload) {
        return link.accept(stamped, payload.data());
    }
}

// ---------------------------------------------------------------- codec

BOOST_AUTO_TEST_CASE(codec_roundtrip_preserves_every_field) {
    FrameHeader h = collective(OpKind::Reduce, 42, 3, 64, false, true);
    h.transport_seq = 77;

    std::vector<char> buf(frame_header_bytes);
    BOOST_CHECK_EQUAL(encode_header_checked(h, buf.data()), frame_header_bytes);

    FrameHeader d;
    BOOST_REQUIRE(decode_header(buf.data(), buf.size(), 1u << 20, d) == DecodeStatus::Ok);
    BOOST_CHECK(same_identity(h, d));
    BOOST_CHECK_EQUAL(d.payload_length, 64u);
    BOOST_CHECK_EQUAL(d.transport_seq, 77u);
    BOOST_CHECK_EQUAL(d.commutative, false);
    BOOST_CHECK_EQUAL(d.associative, true);
}

BOOST_AUTO_TEST_CASE(a_truncated_header_is_rejected_not_read) {
    FrameHeader h = p2p(1, 8);
    std::vector<char> buf(frame_header_bytes);
    encode_header(h, buf.data());

    FrameHeader d;
    for (std::size_t partial = 0; partial < frame_header_bytes; ++partial) {
        BOOST_CHECK(decode_header(buf.data(), partial, 1u << 20, d) == DecodeStatus::Truncated);
    }
}

BOOST_AUTO_TEST_CASE(bad_magic_and_bad_version_are_rejected) {
    FrameHeader h = p2p(1, 8);
    std::vector<char> buf(frame_header_bytes);
    encode_header(h, buf.data());
    FrameHeader d;

    std::vector<char> bad_magic = buf;
    bad_magic[0] = static_cast<char>(bad_magic[0] ^ 0xFF);
    BOOST_CHECK(decode_header(bad_magic.data(), bad_magic.size(), 1u << 20, d)
                == DecodeStatus::BadMagic);

    // A raw (unframed) peer's first bytes are application payload, which will not carry our
    // magic. This is what stops a framed rank from pairing with a raw one.
    std::vector<char> raw(frame_header_bytes, 'x');
    BOOST_CHECK(decode_header(raw.data(), raw.size(), 1u << 20, d) == DecodeStatus::BadMagic);

    std::vector<char> bad_version = buf;
    bad_version[4] = static_cast<char>(0x7F);
    BOOST_CHECK(decode_header(bad_version.data(), bad_version.size(), 1u << 20, d)
                == DecodeStatus::BadVersion);
}

BOOST_AUTO_TEST_CASE(an_oversized_payload_is_rejected_before_allocation) {
    FrameHeader h = p2p(1, 1u << 30);
    h.payload_length = 1u << 30;
    std::vector<char> buf(frame_header_bytes);
    encode_header(h, buf.data());

    FrameHeader d;
    BOOST_CHECK(decode_header(buf.data(), buf.size(), 4096, d) == DecodeStatus::PayloadTooLarge);
}

// There is no "fragment larger than its message" case any more. It checked
// payload_length > total_length, and with wire version 4 carrying a single length field a frame
// can no longer contradict itself about its own size — the state is unrepresentable rather than
// merely rejected. A peer that lies about payload_length is still caught, one layer up, by
// same_identity and by the receiver's own buffer-length check; see
// ProtocolValidation/a_frame_claiming_{more,less}_payload_than_expected_is_rejected_before_it_desynchronises.

// ------------------------------------------------------- retention and replay

BOOST_AUTO_TEST_CASE(a_cumulative_ack_trims_exactly_the_acked_prefix) {
    SequencedLink link{{8, 1u << 20, 1u << 20}};
    const std::string body = "abcd";
    FrameHeader stamped;
    for (int i = 0; i < 4; ++i) {
        BOOST_REQUIRE(link.admit(p2p(1, body.size()), body.data(), body.size(), stamped));
    }
    BOOST_CHECK_EQUAL(link.replay_suffix().size(), 4u);
    BOOST_CHECK_EQUAL(link.lowest_retained(), 0u);

    link.on_ack(2);   // peer durably holds seqs 0 and 1
    BOOST_CHECK_EQUAL(link.replay_suffix().size(), 2u);
    BOOST_CHECK_EQUAL(link.lowest_retained(), 2u);

    link.on_ack(1);   // a stale ack must not resurrect or double-trim
    BOOST_CHECK_EQUAL(link.replay_suffix().size(), 2u);
}

BOOST_AUTO_TEST_CASE(replay_yields_exactly_the_unacked_suffix_in_order) {
    SequencedLink link{{8, 1u << 20, 1u << 20}};
    FrameHeader stamped;
    for (int i = 0; i < 5; ++i) {
        const std::string body(1, static_cast<char>('a' + i));
        BOOST_REQUIRE(link.admit(p2p(1, body.size()), body.data(), body.size(), stamped));
    }
    link.on_ack(3);

    const auto& suffix = link.replay_suffix();
    BOOST_REQUIRE_EQUAL(suffix.size(), 2u);
    BOOST_CHECK_EQUAL(suffix[0].header.transport_seq, 3u);
    BOOST_CHECK_EQUAL(suffix[1].header.transport_seq, 4u);
    BOOST_CHECK_EQUAL(suffix[0].payload[0], 'd');
    BOOST_CHECK_EQUAL(suffix[1].payload[0], 'e');
}

BOOST_AUTO_TEST_CASE(a_replayed_frame_dedups_instead_of_delivering_twice) {
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    const std::string body = "payload";
    FrameHeader stamped;
    BOOST_REQUIRE(sender.admit(p2p(1, body.size()), body.data(), body.size(), stamped));

    BOOST_CHECK(offer(receiver, stamped, body) == Accept::Delivered);
    BOOST_CHECK_EQUAL(receiver.pending(Lane::P2P), 1u);

    // The sender never saw the ack and replays after the link came back.
    BOOST_CHECK(offer(receiver, stamped, body) == Accept::Duplicate);
    BOOST_CHECK_EQUAL(receiver.pending(Lane::P2P), 1u);
}

BOOST_AUTO_TEST_CASE(a_true_gap_is_fatal_rather_than_silently_accepted) {
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    FrameHeader h = p2p(1, 4);
    h.payload_length = 4;
    h.transport_seq = 3;   // 0..2 never arrived
    BOOST_CHECK(offer(receiver, h, "abcd") == Accept::FatalGap);
}

BOOST_AUTO_TEST_CASE(the_ack_watermark_only_covers_committed_frames) {
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    BOOST_CHECK_EQUAL(receiver.ack_safe(), 0u);

    FrameHeader h = p2p(1, 1);
    h.payload_length = 1;
    h.transport_seq = 0;
    BOOST_REQUIRE(offer(receiver, h, "z") == Accept::Delivered);
    // Committed to the drain queue, therefore durably held, therefore ackable.
    BOOST_CHECK_EQUAL(receiver.ack_safe(), 1u);
    BOOST_CHECK_EQUAL(receiver.next_received(), 1u);
}

// ------------------------------------------------------------- lane demux

BOOST_AUTO_TEST_CASE(a_one_byte_p2p_send_cannot_satisfy_a_collective_receive) {
    // The design's motivating collision: at two peers a bcast fragment and a barrier fragment
    // are both a single byte. Without lanes they are indistinguishable from an application
    // send of the same size.
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    const std::string one = "A";
    FrameHeader stamped;
    BOOST_REQUIRE(sender.admit(p2p(1, 1), one.data(), 1, stamped));
    BOOST_REQUIRE(offer(receiver, stamped, one) == Accept::Delivered);

    // The application is inside a barrier, waiting on the collective lane.
    char dst = 0;
    BOOST_CHECK(receiver.deliver_into(collective(OpKind::Barrier, 0, 0, 1), &dst, 1)
                == Accept::FatalGap);
    // The p2p frame is untouched and still available to the receive it actually belongs to.
    BOOST_CHECK_EQUAL(receiver.pending(Lane::P2P), 1u);
    BOOST_CHECK(receiver.deliver_into(p2p(1, 1), &dst, 1) == Accept::Delivered);
    BOOST_CHECK_EQUAL(dst, 'A');
}

BOOST_AUTO_TEST_CASE(collectives_issued_in_different_orders_are_caught_not_substituted) {
    // rank0: bcast(root=0); barrier()      rank1: barrier(); bcast(root=0)
    // Both frames are one byte on the Collective lane with equal ordinals. TLC finds this
    // silently substituting when the envelope carries only lane + ordinal.
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    const std::string one = "B";
    FrameHeader stamped;
    BOOST_REQUIRE(sender.admit(collective(OpKind::Bcast, 0, 0, 1), one.data(), 1, stamped));
    BOOST_REQUIRE(offer(receiver, stamped, one) == Accept::Delivered);

    char dst = 0;
    // The receiver believes its collective #0 is a barrier.
    BOOST_CHECK(receiver.deliver_into(collective(OpKind::Barrier, 0, 0, 1), &dst, 1)
                == Accept::IdentityMismatch);
    BOOST_CHECK_EQUAL(dst, 0);   // nothing was written into the application buffer
}

BOOST_AUTO_TEST_CASE(an_equal_index_collective_still_needs_op_kind_to_disambiguate) {
    // Machine-checked separately: an envelope carrying collective_index but not op_kind is
    // violated at depth 3, because each rank's first collective is its own #0.
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    const std::string one = "C";
    FrameHeader stamped;
    BOOST_REQUIRE(sender.admit(collective(OpKind::Reduce, 0, 0, 1), one.data(), 1, stamped));
    BOOST_REQUIRE(offer(receiver, stamped, one) == Accept::Delivered);

    char dst = 0;
    BOOST_CHECK(receiver.deliver_into(collective(OpKind::Barrier, 0, 0, 1), &dst, 1)
                == Accept::IdentityMismatch);
}

BOOST_AUTO_TEST_CASE(diverging_reduction_flags_are_an_identity_mismatch) {
    // commutative/associative select entirely different algorithms, so two ranks disagreeing
    // on them are in different logical operations even though op_kind and root agree.
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    const std::string one = "D";
    FrameHeader stamped;
    BOOST_REQUIRE(sender.admit(collective(OpKind::Reduce, 0, 0, 1, true, true),
                               one.data(), 1, stamped));
    BOOST_REQUIRE(offer(receiver, stamped, one) == Accept::Delivered);

    char dst = 0;
    BOOST_CHECK(receiver.deliver_into(collective(OpKind::Reduce, 0, 0, 1, false, false), &dst, 1)
                == Accept::IdentityMismatch);
}

BOOST_AUTO_TEST_CASE(a_matching_receive_on_each_lane_drains_independently) {
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    SequencedLink receiver{{8, 1u << 20, 1u << 20}};
    FrameHeader stamped;
    const std::string p = "P", c = "C";
    BOOST_REQUIRE(sender.admit(p2p(1, 1), p.data(), 1, stamped));
    BOOST_REQUIRE(offer(receiver, stamped, p) == Accept::Delivered);
    BOOST_REQUIRE(sender.admit(collective(OpKind::Barrier, 0, 0, 1), c.data(), 1, stamped));
    BOOST_REQUIRE(offer(receiver, stamped, c) == Accept::Delivered);

    // Interleaved on the wire, but each lane keeps its own FIFO, so the collective receive
    // does not have to wait behind the p2p one.
    char dst = 0;
    BOOST_CHECK(receiver.deliver_into(collective(OpKind::Barrier, 0, 0, 1), &dst, 1)
                == Accept::Delivered);
    BOOST_CHECK_EQUAL(dst, 'C');
    BOOST_CHECK(receiver.deliver_into(p2p(1, 1), &dst, 1) == Accept::Delivered);
    BOOST_CHECK_EQUAL(dst, 'P');
}

// --------------------------------------------------------- flow control

BOOST_AUTO_TEST_CASE(the_window_bounds_outstanding_frames) {
    SequencedLink link{{2, 1u << 20, 1u << 20}};
    const std::string body = "x";
    FrameHeader stamped;
    BOOST_CHECK(link.admit(p2p(1, 1), body.data(), 1, stamped));
    BOOST_CHECK(link.admit(p2p(1, 1), body.data(), 1, stamped));
    BOOST_CHECK(link.send_blocked());
    BOOST_CHECK(!link.admit(p2p(1, 1), body.data(), 1, stamped));

    link.on_ack(1);
    BOOST_CHECK(!link.send_blocked());
    BOOST_CHECK(link.admit(p2p(1, 1), body.data(), 1, stamped));
}

BOOST_AUTO_TEST_CASE(the_retention_cap_blocks_admission_without_discarding) {
    SequencedLink link{{64, 1u << 20, 8}};
    const std::string body(8, 'y');
    FrameHeader stamped;
    BOOST_REQUIRE(link.admit(p2p(1, body.size()), body.data(), body.size(), stamped));
    BOOST_CHECK(link.send_blocked());
    BOOST_CHECK(!link.admit(p2p(1, body.size()), body.data(), body.size(), stamped));
    // Crucially the retained frame is still owed to the peer; the cap never drops it.
    BOOST_CHECK_EQUAL(link.replay_suffix().size(), 1u);
    BOOST_CHECK_EQUAL(link.retained_bytes(), 8u);
}

BOOST_AUTO_TEST_CASE(a_frame_larger_than_the_configured_maximum_is_refused) {
    SequencedLink link{{8, 4, 1u << 20}};
    const std::string body(16, 'z');
    FrameHeader stamped;
    BOOST_CHECK(!link.admit(p2p(1, body.size()), body.data(), body.size(), stamped));
}

// ------------------------------------------------------------ handshake

BOOST_AUTO_TEST_CASE(a_handshake_prunes_retention_the_peer_already_holds) {
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    const std::string body = "q";
    FrameHeader stamped;
    for (int i = 0; i < 3; ++i) {
        BOOST_REQUIRE(sender.admit(p2p(1, 1), body.data(), 1, stamped));
    }
    BOOST_CHECK_EQUAL(sender.replay_suffix().size(), 3u);

    // The peer durably held two frames before the link died; its ack never got back to us.
    HandshakePayload peer;
    peer.next_expected_seq = 2;
    peer.next_send_seq = 0;
    peer.lowest_retained = 0;
    std::string error;
    BOOST_REQUIRE(sender.reconcile(peer, error));
    BOOST_CHECK_EQUAL(sender.replay_suffix().size(), 1u);
}

BOOST_AUTO_TEST_CASE(an_impossible_handshake_aborts_loudly) {
    SequencedLink sender{{8, 1u << 20, 1u << 20}};
    const std::string body = "q";
    FrameHeader stamped;
    BOOST_REQUIRE(sender.admit(p2p(1, 1), body.data(), 1, stamped));

    std::string error;
    HandshakePayload ahead;
    ahead.next_expected_seq = 99;   // expects what we never produced
    BOOST_CHECK(!sender.reconcile(ahead, error));
    BOOST_CHECK(!error.empty());

    sender.on_ack(1);               // we pruned seq 0
    HandshakePayload behind;
    behind.next_expected_seq = 0;   // still asks for it
    BOOST_CHECK(!sender.reconcile(behind, error));
}

BOOST_AUTO_TEST_CASE(handshake_payload_roundtrips) {
    HandshakePayload h;
    h.next_send_seq = 12;
    h.next_expected_seq = 7;
    h.lowest_retained = 5;
    h.policy_fingerprint = 0xDEADBEEFULL;

    std::vector<char> buf(handshake_bytes);
    encode_handshake(h, buf.data());
    HandshakePayload d;
    BOOST_REQUIRE(decode_handshake(buf.data(), buf.size(), d) == DecodeStatus::Ok);
    BOOST_CHECK_EQUAL(d.next_send_seq, 12u);
    BOOST_CHECK_EQUAL(d.next_expected_seq, 7u);
    BOOST_CHECK_EQUAL(d.lowest_retained, 5u);
    BOOST_CHECK_EQUAL(d.policy_fingerprint, 0xDEADBEEFULL);
}

// ------------------------------------------------------ snapshot / seed

BOOST_AUTO_TEST_CASE(snapshot_and_seed_roundtrip_the_watermarks) {
    SequencedLink link{{8, 1u << 20, 1u << 20}};
    const std::string body = "s";
    FrameHeader stamped;
    for (int i = 0; i < 3; ++i) {
        BOOST_REQUIRE(link.admit(p2p(1, 1), body.data(), 1, stamped));
    }
    FrameHeader in = p2p(0, 1);
    in.payload_length = 1;
    in.transport_seq = 0;
    BOOST_REQUIRE(link.accept(in, body.data()) == Accept::Delivered);

    SequencedLink restored;
    BOOST_REQUIRE(restored.seed(link.snapshot()));
    BOOST_CHECK_EQUAL(restored.next_send_seq(), link.next_send_seq());
    BOOST_CHECK_EQUAL(restored.next_received(), link.next_received());
    BOOST_CHECK_EQUAL(restored.ack_safe(), link.ack_safe());
}

BOOST_AUTO_TEST_CASE(a_malformed_snapshot_is_rejected) {
    SequencedLink link;
    BOOST_CHECK(!link.seed(""));
    BOOST_CHECK(!link.seed("9 1 2 3"));
    BOOST_CHECK(!link.seed("1 notanumber"));
}

BOOST_AUTO_TEST_SUITE_END()
