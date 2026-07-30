#ifndef FMI_TCPCHANNELBASE_H
#define FMI_TCPCHANNELBASE_H

#include "PeerToPeer.h"
#include "LinkFrame.h"
#include "SequencedLink.h"

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

namespace FMI::Comm {
    //! A link said something the protocol does not allow; re-establishing cannot fix it.
    struct LinkProtocolError : public std::runtime_error {
        using std::runtime_error::runtime_error;
    };

    //! Peer-to-peer channel over one blocking TCP socket per peer.
    /*!
     * Holds everything about TCP byte-stream messaging that does not depend on *how* the
     * connection was established: the framing-free send/recv loops, the socket options, the
     * per-peer socket table and its lifecycle, and the shared cost model. Subclasses supply
     * connection establishment only, through establish().
     *
     * Deliberately declares no destructor. Channel documents why there is no virtual
     * destructor (see Channel::finalize), and a base destructor that closed sockets here would
     * close them a second time after finalize() — Communicator::reconfigure_to_epoch finalizes
     * a channel and then destroys it, by which point those fd numbers may already have been
     * handed out again.
     */
    class TcpChannelBase : public PeerToPeer {
    public:
        void send_object(channel_data buf, Utils::peer_num rcpt_id) override;

        void recv_object(channel_data buf, Utils::peer_num sender_id) override;

        double get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;

        double get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;

        void finalize() override;

        void prepare_for_checkpoint() override;

        //! Selective re-pair: close only the links to migrated ranks and adopt the new
        //! epoch-qualified name; surviving peer connections stay open. Safe because the
        //! consensus cut guarantees every operation below the cut completed everywhere before
        //! anyone reconfigures, so a kept stream is message-aligned with no epoch-N bytes in
        //! flight; the re-established (moved) links are rebuilt under epoch-qualified names,
        //! which preserves the fencing invariant for everything that is rebuilt.
        bool reconfigure_for_epoch(const std::string& new_comm_name,
                                   const std::vector<FMI::Utils::peer_num>& moved_ranks) override;

        void set_incarnation(std::uint64_t value) override;

    protected:
        //! Establish a connection to exactly @p partner_id.
        /*!
         * @param partner_id the peer to connect to; the returned fd must belong to that peer
         *                   and no other.
         * @param link_name  the name both ends of this link derive identically, for transports
         *                   that need a rendezvous or verification name.
         * @return a CONNECTED, BLOCKING socket fd. Throws FMI::Utils::Timeout when the
         *         transport's deadline expires.
         */
        virtual int establish(Utils::peer_num partner_id, const std::string& link_name) = 0;

        //! Name that both ends of a link derive identically.
        /*!
         * The default is rank-ordered, so it does not depend on which side touches the link
         * first. The explicit separators matter: an epoch-qualified comm_name ends in digits,
         * and concatenating bare rank numbers onto it lets distinct (epoch, rank pair)
         * combinations collide.
         */
        virtual std::string link_name(Utils::peer_num partner_id, bool outbound) const;

        //! Release subclass-owned transport state (listening sockets, registry connections).
        //! Called by finalize() and prepare_for_checkpoint(), after the peer sockets are closed.
        virtual void close_transport_state() {}

        //! Ensure a connection to @p partner_id exists, establishing it on first use.
        void check_socket(Utils::peer_num partner_id, const std::string& name);

        //! Timeouts plus TCP_NODELAY, applied to every peer socket before it carries traffic.
        void apply_socket_options(int fd) const;

        void close_sockets();

        //! Parse the parameters every TCP transport shares. Missing keys throw, as before.
        void parse_tcp_params(std::map<std::string, std::string>& params);

        //! Wrap every message in a LinkFrame carrying its operation identity.
        /*!
         * Off by default, in which case the byte stream is exactly what it was before framing
         * existed. On, each message is preceded by a fixed-size header and the receiver
         * rejects a frame belonging to a different logical operation instead of copying it
         * into the application's buffer.
         */
        bool framed = false;

        //! Retain every sent frame until the peer acknowledges it, and replay after a break.
        /*!
         * Off by default. On, a connection that dies is re-established and the unacknowledged
         * suffix is retransmitted, so a completed send remains a delivery obligation across
         * the break. Requires framed.
         */
        bool recover_links = false;

        //! How many times one receive may re-establish before giving up on the peer.
        int max_link_repairs = 4;

        //! Per-peer link state: sequences, retention ring and watermarks.
        std::vector<SequencedLink> links;

        //! Link-layer bounds, from the backend config block.
        std::uint32_t link_window_frames = 256;
        std::uint32_t link_max_frame_bytes = 1u << 24;
        std::size_t link_retention_limit_bytes = 256u << 20;

        //! Grow the per-peer link state to num_peers on first use.
        void ensure_link_state();

        //! Re-establish a dead link and retransmit whatever the peer has not acknowledged.
        void repair_link(Utils::peer_num partner_id);

        //! Tell the peer what we have received, when nothing else is going that way.
        /*!
         * Best effort: an ack that will not fit in the socket right now is simply re-offered
         * after the next commit. What is not optional is that the peer eventually hears
         * something, which is what drain_acks guarantees from the other side.
         */
        void maybe_send_ack(Utils::peer_num partner_id);

        //! Collect the peer's acks without disturbing the message stream.
        /*!
         * Called when the send window is full, which on a one-way link is the only thing that
         * can ever unblock it. Frames are examined with MSG_PEEK, so a data frame is left
         * exactly where it was for recv_object to read normally and only payload-free ack
         * frames are consumed. The peeked cumulative ack is applied either way, so even a data
         * frame left in the stream prunes retention.
         */
        void drain_acks(Utils::peer_num partner_id, long budget_ms);

        //! Commits between standalone acks. Zero means "ack whenever anything is outstanding".
        std::uint32_t link_ack_interval = 32;

        //! The lineage of this rank that owns the link state; see SequencedLink::set_incarnation.
        std::uint64_t local_incarnation = 0;

        //! Offer this end's link state on a fresh link and replay what it owes. Reads nothing.
        void exchange_handshake(Utils::peer_num partner_id);

        //! Adopt a handshake that arrived as a frame payload.
        void reconcile_handshake(Utils::peer_num partner_id, const char* payload);

        //! Take whole frames off every established link that has one waiting, without blocking.
        /*!
         * Called by a subclass while it is stuck establishing ONE link. Without it a rank
         * inside establishment reads nothing else, and after a restore — when several links
         * must be rebuilt at once, in an order each rank chooses for itself — the wait-for
         * relation that makes lazy establishment safe stops being acyclic and the job
         * deadlocks: every rank waits, and every rank holds a whole frame another needs.
         *
         * Only complete frames are taken (the header is peeked and the payload's arrival
         * confirmed before anything is consumed), so a link is never left half-read. They go
         * into the per-lane drain queues, and recv_object takes them from there in preference
         * to the socket. A link that fails here is left alone: the receive path owns repair.
         */
        void service_established_links(Utils::peer_num skip);

        //! Record that the connection to @p partner_id was replaced, not merely established.
        /*!
         * Called by a subclass that drops a dead socket in favour of an incoming connection.
         * The rank at the other end reached this link through repair and is waiting to
         * reconcile; without the mark this end would take the new connection as a first
         * establishment, skip the handshake, and read its peer's handshake as a frame.
         */
        void note_link_replaced(Utils::peer_num partner_id);

        //! Links whose next establishment must reconcile rather than start clean.
        std::vector<char> link_needs_reconcile;

        //! Serialize and write one frame.
        void write_frame(Utils::peer_num rcpt_id, const FrameHeader& header, const char* payload);

        //! Write exactly @p len bytes to @p rcpt_id, looping over partial writes.
        void write_all(Utils::peer_num rcpt_id, const char* data, std::size_t len);

        //! Read exactly @p len bytes from @p sender_id, looping over partial reads.
        void read_all(Utils::peer_num sender_id, char* data, std::size_t len);

        void parse_tcp_model_params(std::map<std::string, std::string>& model_params);

        //! Socket file descriptors for the communication with the peers, indexed by peer id.
        /*!
         * Sized lazily in check_socket rather than in a constructor: num_peers is pushed in by
         * Communicator::register_channel *after* construction, so at construction time it is
         * still uninitialized and resizing here would leave every index out of bounds.
         */
        std::vector<int> sockets;
        unsigned int max_timeout = 0;
        //! Report a peer that closes without sending a single byte as a Timeout rather than a
        //! transport error.
        /*!
         * Only meaningful for a transport that can hold an established link to a peer which
         * then abandons the collective — the peer's close is that abandonment, and "the
         * collective did not complete" is what FMI::Utils::Timeout means. An EOF *part way*
         * through a message stays a hard error either way: that one is truncation, and it must
         * never be quiet.
         *
         * Off for Direct, whose pairing never completes in the first place when a peer does not
         * participate, so it surfaces the same condition as a Timeout from pair().
         */
        bool eof_before_data_is_timeout = false;
        //! Prefix for error messages ("Direct", "DirectTCP"), so each transport reports as itself.
        std::string transport_tag;
        // Model params
        double bandwidth = 0.;
        double overhead = 0.;
        double transfer_price = 0.;
        double vm_price = 0.;
        unsigned int requests_per_hour = 0;
        bool include_infrastructure_costs = false;
    };
}

#endif //FMI_TCPCHANNELBASE_H
