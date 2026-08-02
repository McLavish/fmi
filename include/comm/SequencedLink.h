#ifndef FMI_SEQUENCEDLINK_H
#define FMI_SEQUENCEDLINK_H

#include "LinkFrame.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <vector>

namespace FMI::Comm {

    //! Per-directed-link state machine for the sequenced link layer.
    /*!
     * Deliberately owns no socket, no file descriptor and no I/O. It is a pure function of
     * (state, event), which is what makes it exhaustively unit-testable and what lets the same
     * logic be driven by a blocking transport today and an event loop later.
     *
     * Two watermarks, per docs/tla/SequencedLink.tla:
     *   next_received — highest contiguous transport_seq accepted; drives dedup and replay.
     *   ack_safe      — highest seq the peer may prune. Under a CRIU-style checkpoint this is
     *                   "received and durably held", i.e. it tracks next_received, because the
     *                   drain queues live in memory that the checkpoint image captures.
     *
     * The invariant that makes an arbitrary-instant freeze safe is that an ack is only ever
     * issued for a frame already committed to a drain queue. Everything still sitting in a
     * kernel socket buffer is therefore unacked by construction, and is replayed from the
     * sender's retention after the link is re-established.
     */
    class SequencedLink {
    public:
        //! Outcome of offering a frame, or of draining one into an application buffer.
        enum class Accept {
            Delivered,         //!< accepted and committed, or drained into the caller's buffer
            Duplicate,         //!< already seen; a benign replay, must not be delivered twice
            FatalGap,          //!< a hole in the sequence that retention can no longer fill
            IdentityMismatch   //!< the frame belongs to a different logical operation
        };

        struct Config {
            //! Maximum unacked frames the sender may have outstanding.
            std::uint32_t window_frames = 64;
            //! Largest payload accepted or produced in a single frame.
            std::uint32_t max_frame_bytes = 1u << 20;
            //! Hard cap on retained bytes. Reaching it blocks admission of new sends; it never
            //! discards a retained frame, because that would break the delivery obligation.
            std::size_t retention_limit_bytes = 64u << 20;
        };

        //! A frame the sender still owes the peer, held until it is cumulatively acked.
        struct Retained {
            FrameHeader header;
            std::vector<char> payload;
        };

        SequencedLink() = default;
        explicit SequencedLink(Config cfg) : config(cfg) {}

        // ---- sender ------------------------------------------------------------------

        //! True when the window or the retention cap forbids admitting another frame.
        [[nodiscard]] bool send_blocked() const;

        //! Retain a copy of @p payload and stamp it with the next transport_seq.
        /*!
         * The retained copy exists before the caller may put a single byte on the wire — that
         * ordering is what makes the delivery obligation survive a freeze. Returns false and
         * writes nothing when send_blocked().
         */
        bool admit(const FrameHeader& identity, const char* payload, std::size_t len,
                   FrameHeader& stamped);

        //! Advance the send sequence without retaining anything.
        /*!
         * For framed-without-recovery, where frames carry a sequence so gaps are detected but
         * nothing is replayed and therefore nothing needs holding.
         */
        void note_sent() { ++next_send; }

        //! Release retention up to and including @p cumulative.
        void on_ack(std::uint64_t cumulative);

        //! Record that a cumulative ack of @p value has actually reached the wire.
        /*!
         * Piggybacked and standalone acks both land here, so the standalone path can tell
         * whether the peer already knows what we have received.
         */
        void note_ack_sent(std::uint64_t value);

        //! True when the peer is far enough behind our watermark to be worth telling.
        /*!
         * The peer prunes on what it hears, so silence on a one-way link is what fills its
         * window. @p interval trades ack traffic against how much slack the sender keeps.
         */
        [[nodiscard]] bool ack_due(std::uint64_t interval) const;

        [[nodiscard]] std::uint64_t last_ack_sent() const { return acked_to_peer; }

        //! Exactly the unacked suffix, in sequence order: what a re-established link replays.
        [[nodiscard]] const std::deque<Retained>& replay_suffix() const { return retention; }

        //! Copy the retained frame with @p seq into @p out; false if it is not retained.
        /*!
         * For replay loops that must survive the retention being pruned UNDER them: writing a
         * replay frame pumps, the pump services other links, and an inbound frame's
         * piggybacked ack legitimately prunes this very deque mid-iteration. A reference into
         * the deque dangles at that moment — observed as freed-heap bytes on the wire — where
         * a copy is immune, and a frame pruned before its turn is a frame the peer just
         * declared it holds, so skipping it is correct rather than merely safe.
         */
        [[nodiscard]] bool copy_retained(std::uint64_t seq, Retained& out) const;

        [[nodiscard]] std::uint64_t next_send_seq() const { return next_send; }
        [[nodiscard]] std::uint64_t lowest_retained() const;
        [[nodiscard]] std::size_t retained_bytes() const { return retention_bytes; }

        // ---- receiver ----------------------------------------------------------------

        //! Offer an arriving frame. Dedups by transport_seq and commits to the frame's lane.
        /*!
         * Commits before the caller may make the corresponding ack writable. Identity is NOT
         * checked here: a frame may legitimately arrive before the application posts the
         * matching receive. Identity is validated at drain time, in deliver_into().
         */
        Accept accept(const FrameHeader& header, const char* payload);

        //! Decide what an arriving frame is, changing nothing.
        /*!
         * Split out from accept_inline so a blocking transport can find out whether a frame is
         * new, a replay or a gap *before* it reads the payload, and only commit once those
         * bytes are safely in the application's buffer.
         */
        [[nodiscard]] Accept classify(const FrameHeader& header) const;

        //! Advance the watermarks for a frame whose payload the caller has already delivered.
        /*!
         * The ordering is a correctness requirement, not a style choice: this is the point at
         * which the peer becomes free to forget the frame, so it must not happen while the
         * payload is still somewhere the checkpoint does not capture (a kernel socket buffer).
         * A no-op unless classify() says Delivered, so a double commit cannot skip a sequence.
         */
        void commit_inline(const FrameHeader& header);

        //! Account for an arriving frame WITHOUT buffering its payload.
        /*!
         * classify() followed by commit_inline(). Correct only where the payload is already in
         * hand; a transport that still has to read it off a socket must use the two halves
         * separately. Dedup and gap detection are identical to accept(); on Duplicate the
         * caller must still consume the payload bytes off the wire and discard them, or the
         * stream desynchronises.
         */
        Accept accept_inline(const FrameHeader& header);

        //! Drain the head of @p expected's lane into @p dst, validating message identity.
        /*!
         * This is where a divergent schedule is caught: if the frame at the head of the lane
         * belongs to a different logical operation than the one the application is waiting
         * for, the result is IdentityMismatch rather than a silently wrong payload.
         */
        Accept deliver_into(const FrameHeader& expected, char* dst, std::size_t len);

        //! Frames committed to a lane but not yet drained by the application.
        [[nodiscard]] std::size_t pending(Lane lane) const;

        [[nodiscard]] std::uint64_t next_received() const { return next_recv; }
        [[nodiscard]] std::uint64_t ack_safe() const { return ack_safe_seq; }

        // ---- (re-)establishment ------------------------------------------------------

        //! Which lineage of the local rank owns this link state (contract 3).
        /*!
         * Set once, before the link carries anything, by whatever supplies the rank's lineage.
         * A process restored from a checkpoint never re-enters this path, so it keeps the
         * incarnation its image was taken with — which is precisely the property that lets its
         * peers tell "restored, reconcile" from "replaced, start again".
         *
         * NOTE: nothing supplies a non-zero lineage today. The epoch protocol that used to
         * claim one per process was removed and no coordinator has replaced it, so every
         * process presents 0 and only the same-lineage branch of reconcile() is reachable.
         * The field stays on the wire (frame_wire_version 3) so a decentralised coordinator
         * can populate it without a format change.
         */
        void set_incarnation(std::uint64_t value) { local_incarnation = value; }
        [[nodiscard]] std::uint64_t incarnation() const { return local_incarnation; }
        [[nodiscard]] std::uint64_t known_peer_incarnation() const { return peer_incarnation; }

        [[nodiscard]] HandshakePayload local_handshake(std::uint64_t policy_fingerprint = 0) const;

        //! Adopt the peer's cumulative ack and reject impossible states loudly.
        /*!
         * Also decides, from the incarnation pair, *which* peer this is:
         *   same lineage    — reconcile sequences, replay the unacked suffix (the CRIU case);
         *   newer lineage   — the peer restarted from nothing, so this side starts again too;
         *   older lineage   — the peer has already been replaced and is a zombie: refused.
         * Refusing the last one is the whole point of the incarnation. A zombie's sequences
         * are internally consistent, so nothing else in the handshake can tell it apart from
         * the legitimate peer.
         *
         * @param error set to a human-readable reason when the handshake is rejected.
         */
        bool reconcile(const HandshakePayload& peer, std::string& error);

        //! Forget everything about the stream, keeping the configuration and the incarnations.
        /*!
         * What "the peer restarted" means for link state: its counters are back at zero, so
         * ours must be too, and the frames we still hold for its predecessor are owed to a
         * process that no longer exists.
         */
        void reset_stream();

        // ---- checkpoint / replacement seeding ----------------------------------------

        //! Opaque, versioned serialization of the whole link state.
        [[nodiscard]] std::string snapshot() const;
        bool seed(const std::string& blob);

        [[nodiscard]] const Config& configuration() const { return config; }

    private:
        Config config;

        // sender
        std::uint64_t next_send = 0;
        std::deque<Retained> retention;
        std::size_t retention_bytes = 0;

        // receiver
        std::uint64_t next_recv = 0;
        std::uint64_t ack_safe_seq = 0;
        //! Highest cumulative ack this side has actually put on the wire.
        std::uint64_t acked_to_peer = 0;

        // identity (contract 3)
        std::uint64_t local_incarnation = 0;
        std::uint64_t peer_incarnation = 0;
        struct Committed {
            FrameHeader header;
            std::vector<char> payload;
        };
        //! One drain queue per lane; index is the Lane enumerator.
        std::deque<Committed> lanes[2];

        static std::size_t lane_index(Lane lane) { return static_cast<std::size_t>(lane); }
    };
}

#endif //FMI_SEQUENCEDLINK_H
