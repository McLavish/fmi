#ifndef FMI_TCPCHANNELBASE_H
#define FMI_TCPCHANNELBASE_H

#include "PeerToPeer.h"

#include <map>
#include <string>
#include <vector>

namespace FMI::Comm {
    //! Peer-to-peer channel over one TCP socket per peer, with nothing on the wire but payload.
    /*!
     * Holds everything about TCP byte-stream messaging that does not depend on *how* the
     * connection was established: the send and receive loops, the socket options, the per-peer
     * socket table and its lifecycle, and the shared cost model. Subclasses supply connection
     * establishment only, through establish().
     *
     * A link that breaks is an error. This family has no recovery: a rank that must survive a
     * move uses LocalDrain, which closes its links on purpose and rebuilds them.
     *
     * Deliberately declares no destructor. Channel documents why there is no virtual
     * destructor (see Channel::finalize), and a base destructor that closed sockets here would
     * close them a second time after finalize() — ~Communicator finalizes every channel and
     * then destroys it, and any other rank in the process may have been handed those fd
     * numbers again in between.
     */
    class TcpChannelBase : public PeerToPeer {
    public:
        void send_object(channel_data buf, Utils::peer_num rcpt_id) override;

        void recv_object(channel_data buf, Utils::peer_num sender_id) override;

        double get_latency(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;

        double get_price(Utils::peer_num producer, Utils::peer_num consumer, std::size_t size_in_bytes) override;

        void finalize() override;

    protected:
        //! Establish a connection to exactly @p partner_id.
        /*!
         * @param partner_id the peer to connect to; the returned fd must belong to that peer
         *                   and no other.
         * @param link_name  the name both ends of this link derive identically, for transports
         *                   that need a rendezvous or verification name.
         * @return a CONNECTED socket fd. Throws FMI::Utils::Timeout when the transport's
         *         deadline expires.
         */
        virtual int establish(Utils::peer_num partner_id, const std::string& link_name) = 0;

        //! Name that both ends of a link derive identically.
        /*!
         * The default is rank-ordered, so it does not depend on which side touches the link
         * first. The explicit separators matter: comm names routinely end in digits — the
         * test suite builds one out of a timestamp, i.e. digits only — and concatenating bare
         * rank numbers onto such a name lets distinct (comm, rank pair) combinations collide.
         */
        virtual std::string link_name(Utils::peer_num partner_id, bool outbound) const;

        //! Release subclass-owned transport state (listening sockets, registry connections).
        //! Called by finalize(), after the peer sockets are closed.
        virtual void close_transport_state() {}

        //! Do whatever the transport owes its peers right now, without blocking.
        /*!
         * A rank waiting for one link must keep serving every other obligation it has. For a
         * listener-based transport that means accepting connections: a rank blocked in a read
         * accepts nothing otherwise, so a peer that dials it sits in the backlog unanswered
         * until this rank happens to establish a link of its own.
         *
         * Called from inside every wait of a read or a write. Must return promptly and must
         * not throw: it runs while another operation is in flight, and that operation owns the
         * error reporting.
         */
        virtual void service_transport() {}

        //! Wait for the socket of @p peer to become ready, serving the transport meanwhile.
        /*!
         * The single place this family ever waits on a link. Returns false when the deadline
         * passes with the socket still not ready.
         */
        bool pump(Utils::peer_num peer, short events, long deadline_ms);

        //! Ensure a connection to @p partner_id exists, establishing it on first use.
        void check_socket(Utils::peer_num partner_id, const std::string& name);

        //! Timeouts plus TCP_NODELAY, applied to every peer socket before it carries traffic.
        void apply_socket_options(int fd) const;

        void close_sockets();

        //! Parse the parameters every TCP transport shares. A missing max_timeout throws.
        void parse_tcp_params(std::map<std::string, std::string>& params);

        //! Take ownership of a connection the transport accepted on its own initiative.
        /*!
         * The descriptor becomes this rank's socket for that peer, unless it already holds one;
         * then the caller keeps its descriptor.
         */
        void adopt_link(Utils::peer_num partner_id, int fd);

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
