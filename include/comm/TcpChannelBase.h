#ifndef FMI_TCPCHANNELBASE_H
#define FMI_TCPCHANNELBASE_H

#include "PeerToPeer.h"

#include <map>
#include <string>
#include <vector>

namespace FMI::Comm {
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
