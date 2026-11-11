#ifndef FMI_LOCALDRAIN_H
#define FMI_LOCALDRAIN_H

#include "PeerToPeer.h"
#include "RankTable.h"

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

    //! Local Drain: plain TCP between ranks, and a rank that can be moved while the job runs.
    /*!
     * In the steady state a message is its own bytes on a TCP connection: no header, no copy, no
     * acknowledgment. When a rank has to leave its host, the rank and the peers connected to it
     * close their connections and read them to EOF, so that every byte in flight ends up in
     * process memory. The rank then stops without a single socket, is checkpointed, and continues
     * on another host with the next generation. Peers reconnect when they need the link, and the
     * byte counters of both ends are compared at that moment.
     *
     * The channel talks to the host manager of its host over the control link, a Unix socket. It
     * registers there, asks there where a peer is, and is told there to leave. Nothing is polled:
     * one service thread sleeps in epoll until a peer connects, a peer closes, or a line arrives.
     *
     * The design, its messages and the rules of rank state are in fmi-spot-migration's
     * runtime/DESIGN.md.
     */
    class LocalDrain : public PeerToPeer {
    public:
        LocalDrain(std::map<std::string, std::string> params,
                   std::map<std::string, std::string> model_params);

        ~LocalDrain();

        void send_object(channel_data buf, Utils::peer_num rcpt_id) override;

        void recv_object(channel_data buf, Utils::peer_num sender_id) override;

        double get_latency(Utils::peer_num producer, Utils::peer_num consumer,
                           std::size_t size_in_bytes) override;

        double get_price(Utils::peer_num producer, Utils::peer_num consumer,
                         std::size_t size_in_bytes) override;

        void on_registered() override;

        void finalize() override;

        //! How many restores this rank has been through.
        [[nodiscard]] std::uint64_t generation() const {
            return own_generation.load(std::memory_order_acquire);
        }

        //! With `stop=hold`: true while the rank is drained and waits in place for release_hold().
        [[nodiscard]] bool is_held() const { return held.load(std::memory_order_acquire); }

        //! With `stop=hold`: let a drained rank continue in place, as a restore would.
        void release_hold();

        //! Sockets this channel owns right now: listener, control link and links. Zero while a
        //! rank is drained, which is what lets its image travel to any host.
        [[nodiscard]] int open_sockets();

    private:
        //! Everything the channel knows about its link to one peer.
        struct LinkState {
            //! Guards every field. Held for one non-blocking system call or one copy, never
            //! across a wait, so a drain can take it between two chunks of a message.
            std::mutex mu;
            //! Signalled when the link changes: filed, drained, closed, broken, or back.
            std::condition_variable changed;
            int fd = -1;
            //! Bytes the kernel accepted on this link since the job started.
            std::uint64_t bytes_sent = 0;
            //! Bytes that reached the application or the drained buffer.
            std::uint64_t bytes_received = 0;
            //! Bytes read off a socket by a drain and not yet consumed. A receive takes these
            //! before it reads a socket, which keeps the stream in order across a move.
            std::vector<char> drained;
            std::size_t drained_at = 0;
            //! True while the link waits out a move: of the peer, or of this rank.
            bool draining = false;
            long draining_since_ms = 0;
            //! Generation of the peer whose departure the link is waiting out. A row or a hello
            //! from a higher generation ends the wait.
            std::uint64_t away_generation = 0;
            //! The peer closed and no row has yet said why. Past drain_grace_ms this is a death.
            bool unconfirmed = false;
            //! Highest generation the table has reported as leaving for this peer.
            std::uint64_t leaving_generation = 0;
            bool leaving_known = false;
            //! Generation of the peer the current or last connection was built with.
            std::uint64_t peer_generation = 0;
            //! Total time this link spent in move windows, credited to waiting operations.
            std::uint64_t migration_ms_total = 0;
            //! The peer finalized its channel. An operation that still waits for it has been
            //! abandoned, which is what a timeout means.
            bool peer_gone = false;
            //! Set once the link can never be used again; every operation on it throws this.
            std::string broken;
        };

        //! The budget of one operation, with the time it waited out a move taken off.
        struct OperationClock {
            bool running = false;
            long started_ms = 0;
            long budget_ms = 0;
            long credited_ms = 0;
            std::uint64_t baseline = 0;

            //! Starts the budget, once: the first time the operation cannot go on at once. An
            //! operation whose system call takes the whole message never reads the time.
            //! Called with the link's lock held, like every other use of the clock, so an
            //! operation that had to wait for that lock while a drain held it has not started
            //! yet and is not charged for the wait.
            void start(long now_ms, long budget, std::uint64_t link_total) {
                if (!running) {
                    running = true;
                    started_ms = now_ms;
                    budget_ms = budget;
                    baseline = link_total;
                }
            }

            void absorb(std::uint64_t link_total) {
                credited_ms += static_cast<long>(link_total - baseline);
                baseline = link_total;
            }

            [[nodiscard]] bool expired(long now_ms) const {
                return now_ms - started_ms - credited_ms >= budget_ms;
            }

            [[nodiscard]] long deadline(long now_ms) const {
                const long remaining = budget_ms - (now_ms - started_ms - credited_ms);
                return now_ms + (remaining > 0 ? remaining : 0);
            }
        };

        struct PeerAddr {
            std::string ip;
            int port = 0;
            std::uint64_t nonce = 0;
        };

        // ---- start and stop ----
        void ensure_started();
        void open_listener();
        void open_control_link(long deadline_ms);
        void stop_service_thread();
        LinkState& link(Utils::peer_num peer);
        static void throw_if_broken(const LinkState& l, Utils::peer_num peer);
        //! The peer finalized and nothing is left of what it sent: the operation was abandoned.
        void throw_if_abandoned(const LinkState& l, Utils::peer_num peer, std::size_t moved,
                                std::size_t wanted) const;
        [[nodiscard]] std::string link_name(Utils::peer_num partner) const;

        // ---- control link ----
        void say(const std::string& line);
        //! Read what the control link holds and act on each line. False once it is closed.
        bool read_control_lines();
        void handle_line(const std::string& line);
        void ask_where(Utils::peer_num peer);
        void on_row(Utils::peer_num peer, const RankRow& row);
        void apply_row_locked(LinkState& l, Utils::peer_num peer, const RankRow& row, long now_ms);
        void lose_control_link(const std::string& why);
        [[nodiscard]] std::string lost_reason();

        // ---- the service thread ----
        void service_loop();
        void accept_pending();
        bool accept_one();
        void watch_link(Utils::peer_num peer, int fd);
        void peer_closed(Utils::peer_num peer);

        // ---- establishing a link ----
        void ensure_link(Utils::peer_num peer, long deadline_ms);
        void dial_peer(Utils::peer_num peer, long deadline_ms);
        void await_peer(Utils::peer_num peer, long deadline_ms);
        bool yield_to_drain(Utils::peer_num peer);
        bool lookup_peer(Utils::peer_num peer, PeerAddr& out, long wait_ms);
        int connect_to(const PeerAddr& addr, long deadline_ms);
        bool hello_as_dialer(Utils::peer_num peer, int fd, std::uint64_t nonce, long deadline_ms);
        bool hello_as_acceptor(int fd);
        [[nodiscard]] bool file_link(Utils::peer_num peer, int fd, std::uint64_t peer_generation);
        void apply_socket_options(int fd) const;
        void wait_for_link(std::unique_lock<std::mutex>& lock, LinkState& l, Utils::peer_num peer,
                           OperationClock& clock);

        // ---- draining ----
        //! Take every byte the socket holds right now. True when it ended in EOF.
        bool take_readable_locked(LinkState& l, int fd);
        //! The peer closed its side: keep what it sent, close ours, and wait for the reason.
        void close_after_peer_locked(LinkState& l, Utils::peer_num peer, long now_ms);
        static void end_window_locked(LinkState& l, long now_ms);
        void drain_all_links();
        void break_every_link(const std::string& reason, bool locks_held);
        [[nodiscard]] long drain_budget_ms() const;

        // ---- leaving and coming back ----
        void leave(const std::string& move);
        void come_back();

        // ---- settings ----
        std::string host_link;
        std::string bind_host;
        unsigned int max_timeout = 0;
        unsigned int connect_retry_interval_ms = 10;
        long drain_grace_ms = 5000;
        long migration_max_ms = 120000;
        long establish_yield_ms = 2000;
        bool stop_by_signal = true;
        bool trim_heap = true;

        double bandwidth = 0.;
        double overhead = 0.;
        double transfer_price = 0.;
        double vm_price = 0.;
        unsigned int requests_per_hour = 0;
        bool include_infrastructure_costs = false;

        // ---- state ----
        //! One entry per rank, allocated once at start and never resized: two threads read it.
        std::vector<std::unique_ptr<LinkState>> links;
        RankTable table;
        //! Peers whose row has been asked for since the control link was opened.
        std::vector<char> asked;
        std::mutex asked_mutex;

        std::mutex lifecycle_mutex;
        std::atomic<bool> started{false};
        bool finalized = false;

        //! Shared by a thread that builds a connection, exclusive for a drain: no connection is
        //! built while this rank is being cut.
        std::shared_timed_mutex establish_gate;
        //! Set while a drain waits for the gate; an establishment in flight lets go when it sees it.
        std::atomic<bool> drain_pending{false};

        std::atomic<int> listen_fd{-1};
        int listen_port = 0;
        std::uint64_t listener_nonce = 0;

        std::atomic<int> control_fd{-1};
        std::mutex control_write_mutex;
        std::string control_buffer;
        std::mutex control_wait_mutex;
        std::condition_variable control_wait;
        bool welcomed = false;
        std::uint64_t synced_token = 0;
        std::uint64_t next_sync_token = 1;
        std::atomic<bool> control_lost{false};
        std::string control_lost_reason;

        int epoll_fd = -1;
        int wake_pipe[2] = {-1, -1};
        std::atomic<bool> stopping{false};
        std::thread service_thread;
        //! True while the service thread holds every link lock: between the drain and the end of
        //! the way back. Rows that arrive then are applied without taking a lock again.
        std::atomic<bool> holding_all_links{false};
        std::vector<std::unique_lock<std::mutex>> all_link_locks;
        //! True from the request to leave until the rank is back. A second request that arrives
        //! in between is kept and acted on afterwards.
        bool in_transition = false;
        std::string deferred_leave_move;
        std::uint64_t deferred_leave_generation = 0;
        std::unique_lock<std::shared_timed_mutex> held_gate;

        std::atomic<std::uint64_t> own_generation{0};
        std::atomic<bool> held{false};
        std::mutex hold_mutex;
        std::condition_variable hold_released;
        bool hold_release_requested = false;
    };
}

#endif //FMI_LOCALDRAIN_H
