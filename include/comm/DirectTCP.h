#ifndef FMI_DIRECTTCP_H
#define FMI_DIRECTTCP_H

#include "TcpChannelBase.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>

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
     * handshake per link, all overlapped. Only the link actually asked for is built — building
     * the full mesh up front costs O(num_peers^2) sockets where a binomial-tree collective
     * touches O(log num_peers) of them, measured as ~1500 sockets per suite run left in
     * TIME_WAIT. See establish().
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
        //! Build the whole peer mesh on first use, then hand out one link per call.
        /*!
         * Establishment is phased rather than lazy per link, which is what makes it
         * deadlock-free:
         *
         *   1. listen + publish this rank's address
         *   2. connect to every LOWER rank, all concurrently, each sending a hello
         *   3. drive one event loop that both accepts from HIGHER ranks (verifying their hello
         *      and acknowledging it) and collects the acknowledgements owed to phase 2
         *
         * A connect completes in the kernel via the listen backlog whether or not the peer has
         * called accept(), and the 32-byte frames always fit the socket buffer, so neither the
         * connect nor either frame write ever waits on peer application progress. Because every
         * rank keeps servicing its listener for as long as it still needs anything, a rank
         * waiting for an acknowledgement is never blocking the peer that owes it. No rank can
         * wait on a rank that is waiting on it.
         *
         * The acknowledgement is not ceremony: an ephemeral port is recycled aggressively, so a
         * connect can land on a live listener belonging to a different rank — or to this very
         * process, in the threaded tests. Only the peer can tell us it is who we wanted.
         *
         * Establishing lazily — one link at the moment a collective first touches it — does
         * not have that property: it turns a low rank's send() to a high rank into a
         * synchronous rendezvous, and the resulting wait-for chains are acyclic for today's
         * collectives only by accident.
         */
        int establish(Utils::peer_num partner_id, const std::string& link_name) override;
        bool redial_dead_link(Utils::peer_num partner_id, long budget_ms) override;

    //! Accept whatever is waiting on the listener, right now, without blocking.
    /*!
     * Called from inside every blocking read and write, because accepting only from
     * build_mesh is what leaves a re-establishing peer stuck in the backlog while the rank
     * that would answer it is busy waiting on a link that peer is part of. That is the last
     * of the deadlocks a checkpoint at three ranks or more used to produce.
     */
    void service_transport() override;

    //! Listener and un-adopted connections, for stuck-rank diagnostics.
    std::string transport_state_note() const override;

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

        //! (Re)advertise this rank's address, refreshing the registry key's TTL.
        /*!
         * Called on every establishment attempt, not once per listener. The registry key
         * carries a TTL, so publishing once would let a long-lived rank's entry expire and
         * make it permanently undiscoverable to any peer that needs a link to it later — links
         * are established lazily, so "later" is normal. Re-publishing here is sufficient
         * precisely because a connector only ever needs the *listener's* entry, and the
         * listener necessarily runs its own establishment for that same link in order to accept
         * it. One pipelined round trip, on the cold path only.
         */
        void publish_self(long deadline_ms);

        void build_mesh(Utils::peer_num target, long deadline_ms);
        //! Adopt accepted-but-unheld pending connections, except @p exclude.
        void adopt_pending(Utils::peer_num exclude);

        //! Non-blocking connect to every named peer at once. Successful connects land in
        //! @p unconfirmed — they are not usable links until the listener acknowledges, since a
        //! connect that reaches a recycled port belonging to some other rank looks identical to
        //! a good one from this side. Returns the ranks that could not be connected at all.
        std::vector<Utils::peer_num> connect_batch(const std::map<Utils::peer_num, PeerAddr>& addrs,
                                                   const std::vector<Utils::peer_num>& wanted,
                                                   std::map<Utils::peer_num, int>& unconfirmed,
                                                   long deadline_ms);

        //! Accept one inbound connection, verify its hello, acknowledge it, and file the link.
        //! What one pass over the listener found.
        /*!
         * Three outcomes, not two. An earlier signature returned bool and conflated "nothing
         * waiting" with "took one and rejected it", so a caller looping until false stopped at
         * the first stale connection in the backlog and left everything behind it unaccepted.
         * That matters most for rank 0, which every other rank reconnects to at once after a
         * restore: one duplicate at the head would hold up the entire mesh.
         */
        enum class AcceptResult {
            Accepted,   //!< a connection was taken and belongs to a peer
            Rejected,   //!< one was taken and dropped; there may be more behind it
            Empty       //!< the backlog is empty
        };

        AcceptResult accept_one();

        //! Frame builder shared by the hello (connector -> listener) and its acknowledgement.
        std::string frame_for(Utils::peer_num partner_id, std::uint64_t nonce) const;

        bool send_frame(int fd, Utils::peer_num partner_id, std::uint64_t nonce) const;

        //! Read one frame without blocking past what is already buffered.
        //! @param expect_nonce  the value the frame's nonce field must carry.
        //! @param expect_sender when non-null, the only sender rank accepted; otherwise the
        //!                      sender is learned from the frame and returned through it.
        bool read_frame(int fd, Utils::peer_num& sender, std::uint64_t expect_nonce) const;

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

        //! The machine this process last (re)built its transport on, as the kernel's boot id.
        //! A criu image restored on a DIFFERENT machine resumes with transport state that is
        //! all subtly wrong at once — a listener criu re-bound but peers cannot be told about
        //! mid-frozen-establishment, in-flight connect fds to addresses that meant something
        //! on the old host, a registry context whose socket the restore dropped. Comparing
        //! the boot id at establishment time detects the relocation and triggers the same
        //! wholesale reset the FT-managed checkpoint hook used to perform, which is the
        //! recovery shape the migration runbooks verified. Same-host restores keep the same
        //! boot id and are deliberately left on their proven no-reset path.
        std::string birth_boot_id;

        //! Read /proc/sys/kernel/random/boot_id (empty on failure, which disables detection).
        static std::string read_boot_id();

        //! If the boot id changed since the transport was built, reset it (listener, pending
        //! links, peer sockets, registry client, advertised address) and rearm. Returns true
        //! when a relocation was detected and handled; the caller must abandon its current
        //! establishment attempt and let its own caller retry on clean state.
        bool reset_transport_if_relocated();

        //! Links built by the mesh pass but not yet handed to check_socket. Kept separate from
        //! the base's sockets vector, which check_socket alone owns.
        std::map<Utils::peer_num, int> pending_links;

        std::unique_ptr<PeerRegistry> registry;
    };
}

#endif //FMI_DIRECTTCP_H
