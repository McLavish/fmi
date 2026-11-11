#ifndef FMI_DIRECTTCP_H
#define FMI_DIRECTTCP_H

#include "TcpChannelBase.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace FMI::Comm {
    class PeerRegistry;

    //! Peer-to-peer TCP channel for platforms where peers can reach each other directly.
    /*!
     * Direct exists because some serverless platforms put ranks behind NAT, and it pays a
     * rendezvous round trip plus a retry-driven simultaneous open for every link. Where peers
     * are mutually routable — VMs, containers on a shared network, pods, bare metal — none of
     * that is needed, and this backend replaces it with a Redis registry of peer addresses and
     * ordinary connect/accept.
     *
     * Establishment costs one Redis round trip to publish, one to discover, and one TCP
     * handshake per link. Only the link actually asked for is built — building the full mesh up
     * front costs O(num_peers^2) sockets where a binomial-tree collective touches
     * O(log num_peers) of them, measured as ~1500 sockets per suite run left in TIME_WAIT.
     *
     * This is the plain channel: the wire carries payload and nothing else, a rank stays where
     * it started, and a link that breaks ends the job. It is the baseline the migrating channel
     * (LocalDrain) is measured against.
     *
     * Not usable behind NAT, or anywhere ranks cannot accept inbound connections (Lambda,
     * Knative scale-from-zero). Those are exactly the cases Direct is for.
     */
    class DirectTCP : public TcpChannelBase {
    public:
        explicit DirectTCP(std::map<std::string, std::string> params, std::map<std::string, std::string> model_params);

        ~DirectTCP();

        //! Process-wide count of established links. Cheap telemetry for measuring
        //! establishment cost; scoped to this backend so it counts only its own links.
        static unsigned int connection_count();

    protected:
        //! Hand out the link to @p partner_id, building it if no peer has built it already.
        /*!
         * The higher rank of a pair dials, the lower one accepts:
         *
         *   1. listen + publish this rank's address
         *   2. if the peer is LOWER, connect to it and send a hello
         *   3. drive one event loop that both accepts from HIGHER ranks (verifying their hello
         *      and acknowledging it) and collects the acknowledgement owed to step 2
         *
         * A connect completes in the kernel via the listen backlog whether or not the peer has
         * called accept(), and the 32-byte hello always fits the socket buffer, so neither the
         * connect nor either hello ever waits on peer application progress. A rank keeps
         * accepting for as long as it waits for anything (here and in every read and write, see
         * service_transport), so a rank waiting for an acknowledgement is never blocking the
         * peer that owes it.
         *
         * The acknowledgement is not ceremony: an ephemeral port is recycled aggressively, so a
         * connect can land on a live listener belonging to a different rank — or to this very
         * process, in the threaded tests. Only the peer can tell us it is who we wanted.
         */
        int establish(Utils::peer_num partner_id, const std::string& link_name) override;

        //! Accept whatever is waiting on the listener, right now, without blocking.
        /*!
         * Called from inside every wait of a read or a write. A rank that accepted only while
         * it established a link of its own would leave a dialing peer in the backlog for as
         * long as its application is busy elsewhere.
         */
        void service_transport() override;

        void close_transport_state() override;

    private:
        //! Address a peer published for itself.
        struct PeerAddr {
            std::string ip;
            int port = 0;
            std::uint64_t nonce = 0;
        };

        std::string registry_key() const;

        //! Bind an ephemeral port and listen, if this rank has no listener yet. Touches no Redis.
        void ensure_listener();

        //! Advertise this rank's address, refreshing the registry key's TTL.
        /*!
         * Called on every establishment, not once per listener. The registry key carries a
         * TTL, so publishing once would let a long-lived rank's entry expire and make it
         * undiscoverable to any peer that needs a link to it later — links are established
         * lazily, so "later" is normal. Re-publishing here is sufficient precisely because a
         * connector only ever needs the *listener's* entry, and the listener runs its own
         * establishment for that same link in order to hand it out. One pipelined round trip,
         * on the cold path only.
         */
        void publish_self(long deadline_ms);

        void build_mesh(Utils::peer_num target, long deadline_ms);

        //! Hand accepted connections to the socket table, except the one of @p exclude.
        void adopt_pending(Utils::peer_num exclude);

        //! Non-blocking connect to every named peer at once. Successful connects land in
        //! @p unconfirmed — they are not usable links until the listener acknowledges, since a
        //! connect that reaches a recycled port belonging to some other rank looks identical to
        //! a good one from this side. Returns the ranks that could not be connected at all.
        std::vector<Utils::peer_num> connect_batch(const std::map<Utils::peer_num, PeerAddr>& addrs,
                                                   const std::vector<Utils::peer_num>& wanted,
                                                   std::map<Utils::peer_num, int>& unconfirmed,
                                                   long deadline_ms);

        //! What one pass over the listener found.
        /*!
         * Three outcomes, not two: a caller that loops until "nothing waiting" must not stop at
         * the first connection it had to reject, or everything behind it stays unaccepted.
         */
        enum class AcceptResult {
            Accepted,   //!< a connection was taken and belongs to a peer
            Rejected,   //!< one was taken and dropped; there may be more behind it
            Empty       //!< the backlog is empty
        };

        //! Accept one inbound connection, verify its hello, acknowledge it, and file the link.
        AcceptResult accept_one();

        //! Builder shared by the hello (connector -> listener) and its acknowledgement.
        std::string hello_for(Utils::peer_num partner_id, std::uint64_t nonce) const;

        bool send_hello(int fd, Utils::peer_num partner_id, std::uint64_t nonce) const;

        //! Read one hello, waiting at most a short moment for it.
        //! @param sender        the rank the hello names, on success.
        //! @param expect_nonce  the value the nonce field must carry.
        bool read_hello(int fd, Utils::peer_num& sender, std::uint64_t expect_nonce) const;

        std::string registry_host;
        int registry_port = 6379;
        std::string bind_host;
        std::string advertise_host;
        unsigned int registry_poll_interval_ms = 5;
        unsigned int connect_retry_interval_ms = 10;
        unsigned int registry_ttl_s = 3600;

        int listen_fd = -1;
        int listen_port = 0;
        std::uint64_t listener_nonce = 0;
        std::string advertised_ip;

        //! Links that are established but not yet in the socket table of the base class.
        std::map<Utils::peer_num, int> pending_links;

        std::unique_ptr<PeerRegistry> registry;
    };
}

#endif //FMI_DIRECTTCP_H
