#ifndef FMI_DRAINPROTOCOL_H
#define FMI_DRAINPROTOCOL_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "../utils/Common.h"

namespace FMI::Comm {

    //! Magic for the DrainTCP per-connection hello; the wire bytes read "FMID".
    constexpr std::uint32_t drain_hello_magic = 0x464D4944;

    //! Version of the hello record. Counters and incarnation are carried from version 1,
    //! zero-valued on a first establishment, so the migration stages add meaning to the
    //! fields without a wire change.
    constexpr std::uint16_t drain_hello_version = 1;

    //! Size of the encoded hello record.
    constexpr std::size_t resume_record_bytes = 56;

    //! The once-per-connection hello of the DrainTCP channel.
    /*!
     * This is the direct analogue of DirectTCP's 32-byte hello/nonce frame, extended with
     * the sender's incarnation and the link's cumulative byte counters. It is exchanged in
     * both directions when a connection is established or re-established, and never again:
     * the data path itself is headerless raw bytes, deliberately — the drain protocol's
     * claim is that its steady state costs nothing per message, so every validation this
     * channel performs is priced at connection lifecycle and migration events only.
     *
     * The counters are what make a raw stream safe to cut: `bytes_sent` and
     * `bytes_received` are cumulative per directed link over the lifetime of the lineage,
     * never reset. At every reconnect each end asserts that the peer's `bytes_sent` equals
     * its own `bytes_received` and vice versa. A single byte lost or duplicated across a
     * migration cut therefore fails loudly at the reconnect, naming both counts — instead
     * of desynchronizing the stream silently and permanently, which is the documented
     * failure mode of the reference implementation's bare-int handshake.
     *
     * Wire layout, all integers big-endian (matching the DirectTCP hello helpers):
     *   0   u32  magic 'FMID'
     *   4   u16  wire_version
     *   6   u16  reserved (0)
     *   8   u32  sender_rank
     *   12  u32  receiver_rank
     *   16  u64  link_name_hash    fnv1a64 of the rank-ordered link name; the name embeds
     *                              comm_name, so a foreign communicator cannot pass
     *   24  u64  nonce             dialer -> listener: the nonce the listener advertised in
     *                              the registry (proves the dialer reached this listener
     *                              incarnation, not a recycled port); reply direction: 0
     *   32  u64  sender_incarnation  must be monotone per rank; lower than the last one
     *                              seen means a superseded lineage and is refused
     *   40  u64  bytes_sent
     *   48  u64  bytes_received
     */
    struct ResumeRecord {
        std::uint16_t wire_version = drain_hello_version;
        std::uint32_t sender_rank = 0;
        std::uint32_t receiver_rank = 0;
        std::uint64_t link_name_hash = 0;
        std::uint64_t nonce = 0;
        std::uint64_t sender_incarnation = 0;
        std::uint64_t bytes_sent = 0;
        std::uint64_t bytes_received = 0;
    };

    //! Encode @p record into exactly resume_record_bytes at @p out.
    void encode_resume(const ResumeRecord& record, char* out);

    //! Decode resume_record_bytes from @p in. Returns false on a magic or version
    //! mismatch; field validation (ranks, hash, nonce, counters) is the caller's, since
    //! only the caller knows which link this record arrived on.
    bool decode_resume(const char* in, ResumeRecord& out);

    //! What the migration runtime calls on a channel that participates in drain migration.
    /*!
     * Deliberately NOT part of the Channel interface: Channel is the surface user-supplied
     * backends implement, and OperationScope's rationale is the precedent for keeping new
     * transport concerns out of it. The process-wide migration trigger holds
     * DrainParticipant pointers, never Channel pointers.
     *
     * Threading: every method here runs on the migration runtime's control thread, never
     * on an application thread. Implementations synchronize with the data path through
     * their own per-link state.
     */
    class DrainParticipant {
    public:
        virtual ~DrainParticipant() = default;

        //! Quiesce and drain the links to @p targets into user memory. On return, every
        //! listed link's fd is closed and every byte the peer sent before its FIN sits in
        //! this rank's heap, positioned to be consumed before any future socket read.
        //! An empty @p targets means every live link (the migrating rank's own drain).
        virtual void quiesce_and_drain(const std::vector<Utils::peer_num>& targets) = 0;

        //! Release every socket the transport owns that is not a data link: listener,
        //! registry connection, coordinator stream. After this and a full drain the
        //! process owns zero sockets, which is what makes the CRIU image socket-free and
        //! host-agnostic.
        virtual void release_transport_sockets() = 0;

        //! Fresh listener, fresh advertised address, re-publish, restart the event stream,
        //! adopt the bumped incarnation. Runs after a restore (or an in-place rehearsal).
        //! noexcept: a throw here would terminate the process on the control thread while
        //! application threads are parked on link state; failures are recorded per link
        //! and thrown on the application thread instead.
        virtual void resume_after_restore() noexcept = 0;

        //! A peer announced it is migrating (epoch = the peer's pre-migration epoch).
        //! Survivor side: drain and park only the link to that peer.
        virtual void peer_is_leaving(Utils::peer_num peer, std::uint64_t epoch) = 0;

        //! Take whatever the control plane has said since the last call and act on it.
        /*!
         * The dispatch lives behind the participant rather than in the migration runtime for
         * the same reason the runtime lives in `utils`: the runtime owns the process — one
         * signal, one epoch, one thread — and knows nothing about streams, ranks or links. What
         * an event *means* is the channel's business.
         *
         * @return true when one of those events asks this rank to migrate. The caller runs the
         *         migrator sequence, so the dispatch never re-enters it from inside itself.
         *
         * Must be non-blocking: the caller owes its signal wait the next tick. Implementations
         * are idempotent and epoch-filtered, because a stream is at-least-once and a rank that
         * was frozen resumes reading from where it left off.
         */
        virtual bool poll_control_events() = 0;

        virtual Utils::peer_num local_rank() const = 0;
        virtual const std::string& channel_comm_name() const = 0;
    };
}

#endif //FMI_DRAINPROTOCOL_H
