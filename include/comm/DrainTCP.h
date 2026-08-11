#ifndef FMI_DRAINTCP_H
#define FMI_DRAINTCP_H

#include "DrainProtocol.h"
#include "PeerToPeer.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

namespace FMI::Comm {
    class PeerRegistry;

    //! Raw-TCP peer-to-peer channel whose links can be evicted between chunks.
    /*!
     * The steady-state half of the neighborhood-drain migration protocol. Deliberately NOT a
     * TcpChannelBase subclass: that base's read and write loops abandon a partially moved
     * message when the connection underneath them changes (`LinkReplaced`, "restart at a frame
     * boundary"), which is the exact opposite of this protocol's contract — a message parked
     * mid-flight resumes at its byte offset on the connection that replaces the one it started
     * on. Its framing and retention machinery is also precisely the per-message cost this
     * protocol exists to avoid.
     *
     * **Data path: headerless raw bytes.** A message is `len` bytes on the wire and nothing
     * else — no header, no sequence number, no ack, no retained copy. Per message this channel
     * adds zero bytes, zero copies, zero allocations and zero syscalls over what a bare
     * `send()`/`recv()` loop would issue. All protocol cost sits at connection-lifecycle
     * events, where a 56-byte `ResumeRecord` is exchanged in both directions and the two ends'
     * cumulative byte counters are cross-checked (see DrainProtocol.h).
     *
     * **Concurrency.** One mutex per link, never one per channel: a channel-wide lock would
     * freeze a survivor's traffic to *every* peer while one neighbor migrates, which is
     * exactly the claim ("only the neighborhood stops") this backend exists to make. Under one
     * acquisition of a link's mutex the data path performs exactly one non-blocking syscall or
     * one buffer splice; every wait — poll, condition variable, sleep — happens with the mutex
     * released. That is the per-chunk evictability the drain depends on: a drain can cut in
     * between two chunks of a message, never inside a syscall.
     *
     * **Offsets, not restarts.** The `moved` counter of an in-flight message lives on the
     * application thread's stack, so a CRIU image captures it for free; on wake the loop
     * re-reads the link state and continues from `data + moved`.
     *
     * **Failure surface, stated honestly.** A connection that dies *unplanned* mid-message is
     * a loud error, not a replay: fault tolerance is out of scope for this protocol, whose
     * whole bargain is to pay for migration at migration time and nothing at all in between.
     *
     * **Threading of the control thread.** One thread per channel owns the listener and the
     * accept-side handshake. `OperationScope`'s identity is thread-local and therefore
     * invisible to it — by design: the control thread never touches a message payload nor an
     * operation identity, so there is nothing for it to misattribute.
     *
     * Stage 1 implements steady state only. The `DrainParticipant` methods that carry the
     * migration sequence are declared here and left to Stage 2.
     */
    class DrainTCP : public PeerToPeer, public DrainParticipant {
    public:
        DrainTCP(std::map<std::string, std::string> params,
                 std::map<std::string, std::string> model_params);

        //! Runs finalize() if the application did not. Never throws.
        ~DrainTCP() override;

        void send_object(channel_data buf, Utils::peer_num rcpt_id) override;

        void recv_object(channel_data buf, Utils::peer_num sender_id) override;

        double get_latency(Utils::peer_num producer, Utils::peer_num consumer,
                           std::size_t size_in_bytes) override;

        double get_price(Utils::peer_num producer, Utils::peer_num consumer,
                         std::size_t size_in_bytes) override;

        //! Adopt this rank's lineage and, if the ids are already valid, arm the channel.
        /*!
         * Communicator::register_channel calls this last, after peer_id, num_peers and
         * comm_name — it is therefore the first instant at which this channel can legally bind
         * a listener and publish itself, and the documented arming point of the drain family.
         * Arming here rather than at first use means a rank that is compute-bound for its first
         * seconds still answers a peer's dial. A failure to arm is not fatal here: the data
         * path arms again and reports the failure to the caller that actually needs the link.
         */
        void set_incarnation(std::uint64_t value) override;

        //! The whole teardown: control thread, listener, links, registry. Idempotent.
        /*!
         * Not split between finalize() and the destructor. Communicator finalizes every channel
         * and only then destroys it, so a destructor-side half of the teardown would run over
         * state finalize() had already released, and anything finalize() forgot would leak for
         * the lifetime of the process.
         */
        void finalize() override;

        //! @name DrainParticipant — the migration sequence, Stage 2.
        //! @{
        void quiesce_and_drain(const std::vector<Utils::peer_num>& targets) override;

        void release_transport_sockets() override;

        void resume_after_restore() noexcept override;

        void peer_is_leaving(Utils::peer_num peer, std::uint64_t epoch) override;

        Utils::peer_num local_rank() const override { return peer_id; }

        const std::string& channel_comm_name() const override { return comm_name; }
        //! @}

    private:
        //! Everything about this rank's link to one peer, and the only lock that guards it.
        /*!
         * Held for exactly one non-blocking syscall or one buffer splice at a time. A waiting
         * data path releases it and parks on `back_up`, which every event that could unblock it
         * — a fresh connection filed, a drain completing, an unrecoverable failure recorded —
         * notifies.
         */
        struct LinkState {
            std::mutex mu;
            //! Signalled whenever fd, draining or unrecoverable changes.
            std::condition_variable back_up;
            int fd = -1;
            //! Increments on every connection filed for this peer. A parked data path compares
            //! it rather than the descriptor number: the kernel hands the lowest free number
            //! back out, so an fd comparison can miss a replacement entirely (ABA).
            std::uint64_t generation = 0;
            //! Cumulative over the lineage of this directed link, never reset. These are what
            //! make a raw stream safe to cut: they are cross-checked at every reconnect.
            std::uint64_t bytes_sent = 0;
            std::uint64_t bytes_received = 0;
            //! Bytes the peer sent before its FIN, moved into user memory by a drain. The
            //! receive path consumes these before it ever reads the socket, and frees the
            //! vector the moment it is spent — an unpruned buffer per link is how the
            //! reference implementation leaked.
            std::vector<char> inbound;
            std::size_t inbound_at = 0;
            //! The peer is migrating: the application-visible timeout is suspended for this
            //! link and migration_max_ms applies instead. Stage 2 sets it.
            bool draining = false;
            long draining_since_ms = 0;
            //! Highest lineage seen from this peer. A hello claiming a lower one is a
            //! superseded process and is refused.
            std::uint64_t peer_incarnation = 0;
            //! Set by the control thread, thrown by the application thread. The control thread
            //! must never throw: it would terminate the process while application threads are
            //! parked on link state it holds.
            std::string unrecoverable;
        };

        //! What a single application-visible operation is allowed to spend waiting.
        /*!
         * Two clocks, not one. `max_timeout` measures the patience the job was configured with
         * for a link that is merely slow. Time in which the peer is *migrating* is not that:
         * a survivor must wait out a planned migration rather than fail it, so those intervals
         * are subtracted here and bounded by migration_max_ms per link instead.
         */
        struct OperationClock {
            long started_ms = 0;
            long budget_ms = 0;
            long suspended_ms = 0;

            [[nodiscard]] bool expired(long now_ms) const {
                return now_ms - started_ms - suspended_ms >= budget_ms;
            }
        };

        //! Address a peer published for itself.
        struct PeerAddr {
            std::string ip;
            int port = 0;
            std::uint64_t nonce = 0;
        };

        enum class AcceptResult {
            Accepted,   //!< a connection was taken and filed against a peer
            Rejected,   //!< one was taken and dropped; there may be more behind it
            Empty       //!< the backlog is empty
        };

        //! The link to @p p, bounds-checked. Throws rather than indexing past the vector: the
        //! reference implementation's fixed MAX_PEERS array is the defect this replaces.
        LinkState& link(Utils::peer_num p);

        //! Size the link table, bind the listener, start the control thread. Idempotent.
        /*!
         * Not the constructor's job: peer_id, num_peers and comm_name are pushed in after
         * construction, so at construction time there is no way to know how many links exist
         * or what to publish. On failure nothing is left half-built and the caller may retry.
         */
        void ensure_started();

        //! Bring up the link to @p peer if it is not up, blocking for at most max_timeout.
        void ensure_link(Utils::peer_num peer);

        //! Dial a LOWER-ranked peer: registry lookup, connect, hello exchange.
        void dial_peer(Utils::peer_num peer, long deadline_ms);

        //! Wait for a HIGHER-ranked peer to dial this rank's listener.
        void await_peer(Utils::peer_num peer, long deadline_ms);

        //! One bounded wait on the link's condition variable, with the lock released by the
        //! wait itself. Charges the interval to the right clock and throws when it runs out.
        void wait_for_link(std::unique_lock<std::mutex>& lock, LinkState& l,
                           Utils::peer_num peer, OperationClock& clock);

        //! Take the connection as this rank's link to @p peer, replacing whatever was there.
        /*!
         * Accept-replace, deliberately: a peer only dials a rank it already had a link to
         * after deciding that link is gone, and the hello it just passed proves it is that
         * peer on this communicator. Believing the local descriptor instead wedges both ends,
         * because a descriptor can still report ESTABLISHED with nothing on the far side.
         */
        void file_link(Utils::peer_num peer, int fd, std::uint64_t peer_incarnation);

        //! Throw whatever the control thread recorded against this link. Caller holds l.mu.
        static void throw_if_unrecoverable(const LinkState& l, Utils::peer_num peer);

        std::string registry_key() const;

        //! The name both ends of a link derive identically, embedding comm_name so a foreign
        //! communicator's hello cannot pass. Rank-ordered, so it does not depend on which side
        //! derives it. Separators are explicit: comm names routinely end in digits.
        std::string link_name(Utils::peer_num partner) const;

        //! Publish this rank's address, at most once per half TTL unless @p force.
        //! @return false when the registry could not be reached; the caller retries.
        bool try_publish(long deadline_ms, bool force = false);

        //! Current address of @p peer, from a FRESH registry snapshot.
        /*!
         * Fresh every attempt on purpose: a stale entry is only ever detected by failing
         * against it (a refused connect, a nonce the listener does not own), and the only
         * thing that can fix it is the entry as it stands now.
         */
        bool lookup_peer(Utils::peer_num peer, PeerAddr& out, long deadline_ms);

        //! Non-blocking connect bounded by the deadline. Returns a NON-blocking fd, or -1.
        int connect_to(const PeerAddr& addr, long deadline_ms);

        //! Dialer half of the hello: send ours carrying the listener's nonce, read theirs,
        //! validate, file the link. Returns false for a failure a retry can fix; throws for
        //! one it cannot (counters that do not agree, a superseded lineage).
        bool exchange_hello_as_dialer(Utils::peer_num peer, int fd, std::uint64_t nonce,
                                      long deadline_ms);

        //! Acceptor half of the hello, on the control thread. Never throws: a fatal disagreement
        //! is recorded in the link's `unrecoverable` and raised on the application thread.
        bool exchange_hello_as_acceptor(int fd);

        //! Timeouts plus TCP_NODELAY, on every socket before it carries a hello or a payload.
        void apply_socket_options(int fd) const;

        //! @name Control thread
        //! @{
        //! Blocks in poll() on the listener and the wake pipe; never spins. Accepts, completes
        //! the acceptor half of the hello, and republishes this rank's registry entry before
        //! its TTL can lapse. Every call it makes is bounded, so shutdown is bounded.
        void control_loop();

        AcceptResult accept_one();

        //! Republish if half the registry TTL has elapsed. Failures are retried next tick: a
        //! briefly unreachable registry must not take the channel down.
        void housekeeping();

        //! Set `stopping`, wake the poll through the pipe, join. Bounded by construction.
        void stop_control_thread();
        //! @}

        //! Slice length for every bounded wait in this channel: data-path polls, condition
        //! variable waits, and the control thread's handshake reads. One knob for how finely
        //! the channel chops its waits, which is what decides how promptly a drain can cut in.
        [[nodiscard]] int wait_slice_ms() const;

        // Transport configuration.
        std::string registry_host;
        int registry_port = 6379;
        std::string bind_host;
        std::string advertise_host;
        unsigned int max_timeout = 0;
        unsigned int registry_poll_interval_ms = 5;
        unsigned int connect_retry_interval_ms = 10;
        unsigned int registry_ttl_s = 3600;
        unsigned int control_poll_interval_ms = 20;

        // Migration configuration. Parsed and stored in Stage 1; only migration_max_ms has an
        // effect yet, and only because the wait helper is written for both clocks from the
        // start rather than retrofitted around a live migration.
        bool drain_enabled = false;
        std::string trigger = "both";
        int drain_signal_offset = 3;
        long drain_grace_ms = 5000;
        long migration_max_ms = 120000;

        // Model params, same shape and meaning as every other TCP backend's.
        double bandwidth = 0.;
        double overhead = 0.;
        double transfer_price = 0.;
        double vm_price = 0.;
        unsigned int requests_per_hour = 0;
        bool include_infrastructure_costs = false;

        //! Sized once by ensure_started(), before the control thread exists, and never resized:
        //! a vector that could reallocate under a reader is not a table two threads can share.
        std::vector<std::unique_ptr<LinkState>> links;

        //! Guards the whole start/stop lifecycle: the link table's one sizing, the listener,
        //! the wake pipe and the control thread's existence.
        std::mutex lifecycle_mutex;
        std::atomic<bool> started{false};
        bool finalized = false;

        //! Shared for the duration of an establishment, exclusive by a drain (Stage 2). The
        //! window in which establishment is unsafe, made mechanical rather than documented.
        std::shared_mutex establish_gate;

        int listen_fd = -1;
        int listen_port = 0;
        //! Fresh per listener incarnation. A dialer echoes it, which is what distinguishes this
        //! listener from whatever else has since been handed the same ephemeral port.
        std::uint64_t listener_nonce = 0;
        std::string advertised_ip;
        //! Self-pipe, so the control thread's poll() can be woken without a timeout to expire.
        int wake_pipe[2] = {-1, -1};
        std::atomic<bool> stopping{false};
        std::thread control_thread;

        //! Steady-clock ms of the last successful publish; -1 = never published.
        std::atomic<long> published_at_ms{-1};

        //! Atomic because two threads read it: the application thread stamps a dialer's hello
        //! with it and the control thread stamps an acceptor's. Stage 2's restore leg bumps it
        //! from a third, which is what turns a benign ordering question into a real one.
        std::atomic<std::uint64_t> local_incarnation{0};

        std::unique_ptr<PeerRegistry> registry;
    };
}

#endif //FMI_DRAINTCP_H
