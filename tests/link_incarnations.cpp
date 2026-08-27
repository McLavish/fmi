//! Contract 3: which lineage of a logical rank a link is talking to.
/*!
 * A logical rank outlives the processes that serve it. Three situations are indistinguishable
 * on the wire without an incarnation, and each demands a different response:
 *
 *   - the same process reconnecting, or one restored from a checkpoint    -> reconcile
 *   - a fresh replacement whose counters start at zero                    -> start again
 *   - a process that has already been replaced and is still running       -> refuse
 *
 * The third is the one that matters. A zombie's handshake is internally consistent, so nothing
 * else in the protocol can tell it apart from the legitimate peer; without the incarnation it
 * would be accepted and would serve the rank alongside its own replacement.
 */
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "../include/comm/SequencedLink.h"

using namespace FMI::Comm;

namespace {
    SequencedLink::Config small_config() {
        return {8, 4096, 1u << 20};
    }

    //! A link that has sent @p sent frames and received @p received of the peer's.
    SequencedLink link_with_history(std::uint64_t incarnation, int sent, int received) {
        SequencedLink link(small_config());
        link.set_incarnation(incarnation);
        for (int i = 0; i < sent; i++) {
            FrameHeader identity;
            FrameHeader stamped;
            const int payload = i;
            BOOST_REQUIRE(link.admit(identity, reinterpret_cast<const char*>(&payload), 4, stamped));
        }
        for (int i = 0; i < received; i++) {
            FrameHeader arriving;
            arriving.transport_seq = static_cast<std::uint64_t>(i);
            arriving.payload_length = 4;
            BOOST_REQUIRE(link.classify(arriving) == SequencedLink::Accept::Delivered);
            link.commit_inline(arriving);
        }
        return link;
    }
}

BOOST_AUTO_TEST_SUITE(LinkIncarnations)

BOOST_AUTO_TEST_CASE(a_link_reports_its_own_lineage_and_the_one_it_knows) {
    SequencedLink link(small_config());
    link.set_incarnation(3);
    const HandshakePayload mine = link.local_handshake();
    BOOST_CHECK_EQUAL(mine.incarnation, 3u);
    BOOST_CHECK_EQUAL(mine.peer_incarnation, 0u);
}

BOOST_AUTO_TEST_CASE(the_same_lineage_reconciles_and_keeps_what_it_owes) {
    // The checkpoint/restore case: the peer comes back with the same incarnation because its
    // link state travelled inside the image. Nothing is discarded; the unacked suffix is still
    // owed and still replayable.
    SequencedLink link = link_with_history(0, 5, 2);
    BOOST_REQUIRE_EQUAL(link.replay_suffix().size(), 5u);

    HandshakePayload peer;
    peer.incarnation = 0;
    peer.peer_incarnation = 0;
    peer.next_send_seq = 2;
    peer.next_expected_seq = 3;   // it had seen three of ours
    peer.lowest_retained = 2;

    std::string error;
    BOOST_REQUIRE_MESSAGE(link.reconcile(peer, error), error);
    BOOST_CHECK_EQUAL(link.replay_suffix().size(), 2u);   // 3 and 4 are still owed
    BOOST_CHECK_EQUAL(link.next_send_seq(), 5u);
    BOOST_CHECK_EQUAL(link.next_received(), 2u);
}

BOOST_AUTO_TEST_CASE(a_newer_lineage_starts_the_stream_again) {
    // A replacement process: its counters are zero, so ours must be too. Anything we retained
    // was owed to a process that no longer exists.
    SequencedLink link = link_with_history(0, 5, 2);

    HandshakePayload replacement;
    replacement.incarnation = 1;
    replacement.peer_incarnation = 0;
    replacement.next_send_seq = 0;
    replacement.next_expected_seq = 0;
    replacement.lowest_retained = 0;

    std::string error;
    BOOST_REQUIRE_MESSAGE(link.reconcile(replacement, error), error);
    BOOST_CHECK_EQUAL(link.replay_suffix().size(), 0u);
    BOOST_CHECK_EQUAL(link.next_send_seq(), 0u);
    BOOST_CHECK_EQUAL(link.next_received(), 0u);
    BOOST_CHECK_EQUAL(link.known_peer_incarnation(), 1u);
    BOOST_CHECK_EQUAL(link.retained_bytes(), 0u);
}

BOOST_AUTO_TEST_CASE(a_replacements_zero_sequences_would_be_rejected_without_the_lineage_check) {
    // Why the lineage has to be judged BEFORE the sequences: a replacement legitimately
    // expects sequence 0 while this side has already pruned past it. Read as a same-lineage
    // handshake that is an impossible state, and the link would be refused.
    SequencedLink link = link_with_history(0, 5, 2);
    link.on_ack(5);   // peer had acknowledged everything before it died
    BOOST_REQUIRE_EQUAL(link.lowest_retained(), 5u);

    HandshakePayload same_lineage;
    same_lineage.incarnation = 0;
    same_lineage.peer_incarnation = 0;
    same_lineage.next_expected_seq = 0;
    std::string error;
    BOOST_CHECK_MESSAGE(!link.reconcile(same_lineage, error),
                        "a same-lineage peer asking for a pruned sequence must be refused");

    SequencedLink fresh = link_with_history(0, 5, 2);
    fresh.on_ack(5);
    HandshakePayload newer = same_lineage;
    newer.incarnation = 1;
    BOOST_CHECK_MESSAGE(fresh.reconcile(newer, error),
                        "the identical sequences from a NEW lineage must be accepted: " + error);
}

BOOST_AUTO_TEST_CASE(a_superseded_peer_is_refused) {
    // The zombie fence. This side has already re-paired with incarnation 2 of the peer; the
    // original process is still alive and tries to reconnect. Its handshake is perfectly
    // well-formed, and every sequence in it is consistent with the history it remembers.
    SequencedLink link(small_config());
    link.set_incarnation(0);
    HandshakePayload current;
    current.incarnation = 2;
    std::string error;
    BOOST_REQUIRE(link.reconcile(current, error));
    BOOST_REQUIRE_EQUAL(link.known_peer_incarnation(), 2u);

    HandshakePayload zombie;
    zombie.incarnation = 1;
    zombie.peer_incarnation = 0;
    BOOST_CHECK_MESSAGE(!link.reconcile(zombie, error), "a superseded peer must not be accepted");
    BOOST_CHECK_MESSAGE(error.find("superseded") != std::string::npos, error);
    BOOST_CHECK_EQUAL(link.known_peer_incarnation(), 2u);
}

BOOST_AUTO_TEST_CASE(a_zombie_recognises_itself_as_superseded) {
    // The other half of the fence, and the half that matters when the zombie is the one with
    // work to do: the peer reports which incarnation of THIS rank it is talking to. Hearing a
    // number ahead of its own is how a superseded process learns it has been replaced, without
    // any control-plane round trip.
    SequencedLink zombie(small_config());
    zombie.set_incarnation(0);

    HandshakePayload peer;
    peer.incarnation = 0;
    peer.peer_incarnation = 1;   // it has already reconciled with our replacement

    std::string error;
    BOOST_CHECK_MESSAGE(!zombie.reconcile(peer, error), "a superseded process must refuse to serve");
    BOOST_CHECK_MESSAGE(error.find("incarnation") != std::string::npos, error);
}

BOOST_AUTO_TEST_CASE(a_peer_that_has_not_noticed_our_replacement_yet_is_fine) {
    // The converse of the case above, and it must NOT be refused: a peer that still believes
    // it is talking to our previous incarnation simply has not handshaken with us yet. Our own
    // handshake is what tells it, and it resets on its side.
    SequencedLink replacement(small_config());
    replacement.set_incarnation(1);

    HandshakePayload peer;
    peer.incarnation = 0;
    peer.peer_incarnation = 0;

    std::string error;
    BOOST_CHECK_MESSAGE(replacement.reconcile(peer, error), error);
}

BOOST_AUTO_TEST_CASE(the_handshake_carries_both_incarnations_across_the_wire) {
    HandshakePayload sent;
    sent.next_send_seq = 9;
    sent.next_expected_seq = 4;
    sent.lowest_retained = 7;
    sent.incarnation = 6;
    sent.peer_incarnation = 5;

    char wire[handshake_bytes];
    encode_handshake(sent, wire);
    HandshakePayload got;
    BOOST_REQUIRE(decode_handshake(wire, handshake_bytes, got) == DecodeStatus::Ok);
    BOOST_CHECK_EQUAL(got.incarnation, 6u);
    BOOST_CHECK_EQUAL(got.peer_incarnation, 5u);
    BOOST_CHECK_EQUAL(got.next_send_seq, 9u);
}

BOOST_AUTO_TEST_CASE(a_snapshot_carries_the_lineage_it_was_taken_in) {
    // What a checkpoint image has to preserve for the restored process to be recognised as the
    // same lineage rather than a replacement.
    SequencedLink link = link_with_history(4, 3, 1);
    HandshakePayload peer;
    peer.incarnation = 2;
    std::string error;
    BOOST_REQUIRE(link.reconcile(peer, error));

    SequencedLink restored(small_config());
    BOOST_REQUIRE(restored.seed(link.snapshot()));
    BOOST_CHECK_EQUAL(restored.incarnation(), 4u);
    BOOST_CHECK_EQUAL(restored.known_peer_incarnation(), 2u);
    BOOST_CHECK_EQUAL(restored.next_send_seq(), link.next_send_seq());
    BOOST_CHECK_EQUAL(restored.next_received(), link.next_received());
}

BOOST_AUTO_TEST_CASE(the_handshake_a_survivor_offers_tells_a_zombie_it_has_been_replaced) {
    // Round trip of the field a superseded process learns its fate from. Every case above
    // hand-builds the peer's handshake, which checks how one is READ and nothing about how one
    // is WRITTEN -- and this field is only ever read by the other end, so omitting it is
    // invisible from either side alone.
    SequencedLink survivor(small_config());
    survivor.set_incarnation(7);
    HandshakePayload replacement;
    replacement.incarnation = 1;
    std::string error;
    BOOST_REQUIRE_MESSAGE(survivor.reconcile(replacement, error), error);

    const HandshakePayload offered = survivor.local_handshake();
    BOOST_CHECK_MESSAGE(offered.peer_incarnation == 1u,
                        "the survivor's handshake does not say which lineage it reconciled with");

    // The zombie -- incarnation 0 of the same rank, still running -- reads exactly that
    // handshake. Nothing else in it marks the zombie as stale: the survivor's own incarnation
    // is ahead of what the zombie recorded, which on its own reads as "my peer restarted".
    SequencedLink zombie(small_config());
    zombie.set_incarnation(0);
    BOOST_CHECK_MESSAGE(!zombie.reconcile(offered, error),
                        "a superseded process accepted a handshake that named its replacement");
}

BOOST_AUTO_TEST_SUITE_END()
