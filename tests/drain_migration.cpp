//! The neighborhood-drain migration protocol: the cut, and what survives it.
/*!
 * A migration in this protocol is a rank half-closing every link it owns, reading every byte
 * still in flight into its own heap, and stopping with no socket left in the process — while
 * its neighbours pause on those links alone and everybody else keeps working. What makes that
 * safe is a handful of properties that are easy to state and easy to break:
 *
 *  - the drained bytes are the ones the application had not consumed, and they are delivered
 *    before anything that arrives on the connection that replaces the closed one;
 *  - a message caught mid-flight resumes at its byte offset, on both sides;
 *  - the cumulative counters carried in the reconnect hello agree in both directions, which is
 *    the only thing standing between a headerless stream and a silent desynchronisation;
 *  - a peer that is *migrating* suspends its neighbours' patience, and a peer that has *died*
 *    does not;
 *  - the notice and the FIN race, and either order has to produce the same drain, exactly once.
 *
 * Two kinds of case cover those. The first drives the state machine directly against a fake
 * control plane and a hand-written far end on a real socket — that is where an event delivered
 * twice, an event from a superseded epoch and a FIN that overtakes its notice can be produced
 * on demand, which a healthy Redis will not do. The second forks real ranks over real loopback
 * and a real registry and runs `rehearse_migration_in_place()`, which is the whole migrator
 * sequence with the CRIU stop replaced by an immediate restore: everything the protocol does,
 * minus the part CRIU is responsible for.
 *
 * ForkedRankGuard is mandatory in every forking case; see its header for what happens without
 * it.
 */
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#if FMI_ENABLE_REDIS

#include "../include/comm/DrainCoordinator.h"
#include "../include/comm/DrainProtocol.h"
#include "../include/comm/DrainTCP.h"
#include "../include/comm/PeerRegistry.h"
#include "../include/comm/TcpEndpoint.h"
#include "../include/utils/MigrationTrigger.h"
#include "forked_rank_guard.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace FMI::Comm;
using FMI::Utils::MigrationTrigger;

namespace {

    long now_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                .count();
    }

    //! Nanosecond clock plus the pid: the registry and the event stream both persist in Redis,
    //! so two runs sharing a name means one reading the other's events as its own.
    std::string unique_comm(const char* tag) {
        return std::string("fmi-drainmig-") + tag + "-" + std::to_string(::getpid()) + "-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    }

    int* shared_flags(int n) {
        return static_cast<int*>(mmap(nullptr, n * sizeof(int), PROT_READ | PROT_WRITE,
                                      MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    }

    std::map<std::string, std::string> model_params() {
        return {{"bandwidth", "400"},   {"overhead", "0.2"}, {"transfer_price", "0"},
                {"vm_price", "0.0134"}, {"requests_per_hour", "1000"},
                {"include_infrastructure_costs", "false"}};
    }

    //! Drain-armed transport parameters. Every wait in these tests is bounded by one of them,
    //! so a regression shows up as a failure rather than as a suite that never finishes.
    std::map<std::string, std::string> drain_params(const std::string& trigger, long max_timeout,
                                                    long drain_grace, long migration_max) {
        return {{"registry_host", "127.0.0.1"},
                {"registry_port", "6379"},
                {"bind_host", "127.0.0.1"},
                {"advertise_host", "127.0.0.1"},
                {"max_timeout", std::to_string(max_timeout)},
                {"registry_poll_interval_ms", "5"},
                {"connect_retry_interval_ms", "10"},
                {"registry_ttl_s", "60"},
                {"control_poll_interval_ms", "10"},
                {"drain", "true"},
                {"trigger", trigger},
                {"drain_grace_ms", std::to_string(drain_grace)},
                {"migration_max_ms", std::to_string(migration_max)},
                {"establish_yield_ms", "2000"}};
    }

    std::shared_ptr<DrainTCP> make_drain_channel(int peer_id, int num_peers,
                                                 const std::string& name,
                                                 std::map<std::string, std::string> params) {
        auto ch = std::make_shared<DrainTCP>(params, model_params());
        ch->set_peer_id(static_cast<FMI::Utils::peer_num>(peer_id));
        ch->set_num_peers(static_cast<FMI::Utils::peer_num>(num_peers));
        ch->set_comm_name(name);
        return ch;
    }

    std::string link_name_of(const std::string& comm, unsigned int a, unsigned int b) {
        const unsigned int low = a < b ? a : b;
        const unsigned int high = a < b ? b : a;
        return comm + "|" + std::to_string(low) + "-" + std::to_string(high);
    }

    // ------------------------------------------------------------------------------------
    // A control plane that does what it is told, including the things a healthy one will not.
    // ------------------------------------------------------------------------------------
    class FakeDrainCoordinator : public DrainCoordinator {
    public:
        void publish_member(FMI::Utils::peer_num rank, const MemberRecord& record) override {
            std::lock_guard<std::mutex> lock(mutex);
            member_records[rank] = record;
        }

        std::map<FMI::Utils::peer_num, MemberRecord> members() override {
            std::lock_guard<std::mutex> lock(mutex);
            return member_records;
        }

        void remove_member(FMI::Utils::peer_num rank) override {
            std::lock_guard<std::mutex> lock(mutex);
            member_records.erase(rank);
        }

        std::string emit(const DrainEvent& event) override {
            std::lock_guard<std::mutex> lock(mutex);
            const std::string id = std::to_string(++sequence) + "-0";
            entries.emplace_back(id, event);
            return id;
        }

        std::string tail_id() override {
            std::lock_guard<std::mutex> lock(mutex);
            return entries.empty() ? "0-0" : entries.back().first;
        }

        std::vector<std::pair<std::string, DrainEvent>> read_after(const std::string& last_id,
                                                                   long) override {
            std::lock_guard<std::mutex> lock(mutex);
            std::vector<std::pair<std::string, DrainEvent>> out;
            bool taking = (last_id == "0-0");
            for (const auto& entry : entries) {
                if (taking) {
                    out.push_back(entry);
                } else if (entry.first == last_id) {
                    taking = true;
                }
            }
            return out;
        }

        bool try_batch_lock(const std::string& owner, long) override {
            std::lock_guard<std::mutex> lock(mutex);
            if (lease_held) {
                return false;
            }
            lease_held = true;
            lease_owner = owner;
            return true;
        }

        void release_batch_lock(const std::string& owner) override {
            std::lock_guard<std::mutex> lock(mutex);
            if (lease_held && lease_owner == owner) {
                lease_held = false;
                lease_owner.clear();
            }
        }

        void disconnect() override { disconnects++; }

        //! Append an event without pretending a rank emitted it. The test is the driver here.
        /*!
         * @param extra the annotations a real notice carries, under the names
         *        RedisDrainCoordinator writes them: `"incarnation"` is the lineage that emitted
         *        a `leaving`, and it is what the survivor's fence against a *late* notice reads.
         *        Omitted, the event carries no lineage at all, which the channel reads as "act
         *        on it" — the hand-driven control plane's reading.
         */
        void inject(DrainEvent::Type type, FMI::Utils::peer_num rank, std::uint64_t epoch,
                    std::map<std::string, std::string> extra = {}) {
            DrainEvent event;
            event.type = type;
            event.rank = rank;
            event.epoch = epoch;
            event.extra = std::move(extra);
            emit(event);
        }

        std::atomic<int> disconnects{0};

    private:
        std::mutex mutex;
        std::map<FMI::Utils::peer_num, MemberRecord> member_records;
        std::vector<std::pair<std::string, DrainEvent>> entries;
        std::uint64_t sequence = 0;
        bool lease_held = false;
        std::string lease_owner;
    };

    // ------------------------------------------------------------------------------------
    // The far end of one link, written by hand: the test speaks the hello itself, so it can
    // decide when to FIN, what incarnation to claim and what counters to assert.
    // ------------------------------------------------------------------------------------
    struct WiredPeer {
        int fd = -1;

        ~WiredPeer() { close_now(); }

        //! Dial the target's advertised listener and complete the hello as the dialer.
        //! @return the reply record, or throws — a failure here is a broken test, not a result.
        ResumeRecord greet(const MemberRecord& target, const std::string& comm,
                           unsigned int my_rank, unsigned int target_rank,
                           std::uint64_t incarnation, std::uint64_t sent, std::uint64_t received,
                           bool expect_refusal = false) {
            fd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (fd < 0) {
                throw std::runtime_error("WiredPeer: socket() failed");
            }
            struct sockaddr_in dest{};
            dest.sin_family = AF_INET;
            dest.sin_port = htons(static_cast<std::uint16_t>(target.port));
            if (::inet_pton(AF_INET, target.ip.c_str(), &dest.sin_addr) != 1 ||
                ::connect(fd, reinterpret_cast<struct sockaddr*>(&dest), sizeof(dest)) != 0) {
                throw std::runtime_error("WiredPeer: could not reach " + target.ip + ":" +
                                         std::to_string(target.port));
            }
            ResumeRecord mine;
            mine.sender_rank = my_rank;
            mine.receiver_rank = target_rank;
            mine.link_name_hash = TcpEndpoint::fnv1a64(link_name_of(comm, my_rank, target_rank));
            mine.nonce = target.nonce;
            mine.sender_incarnation = incarnation;
            mine.bytes_sent = sent;
            mine.bytes_received = received;
            char wire[resume_record_bytes];
            encode_resume(mine, wire);
            if (::send(fd, wire, resume_record_bytes, MSG_NOSIGNAL) !=
                static_cast<ssize_t>(resume_record_bytes)) {
                throw std::runtime_error("WiredPeer: could not write the hello");
            }
            char reply[resume_record_bytes];
            std::size_t moved = 0;
            while (moved < resume_record_bytes) {
                struct pollfd pfd{fd, POLLIN, 0};
                if (::poll(&pfd, 1, 3000) <= 0) {
                    break;
                }
                const ssize_t n = ::recv(fd, reply + moved, resume_record_bytes - moved, 0);
                if (n <= 0) {
                    break;
                }
                moved += static_cast<std::size_t>(n);
            }
            ResumeRecord theirs;
            if (expect_refusal) {
                // A refusal is a close: the dialer sees EOF instead of a record, which is
                // exactly what a listener that has nothing to say to it must produce.
                if (moved != 0) {
                    throw std::runtime_error("WiredPeer: expected a refusal, got " +
                                             std::to_string(moved) + " bytes");
                }
                return theirs;
            }
            if (moved != resume_record_bytes || !decode_resume(reply, theirs)) {
                throw std::runtime_error("WiredPeer: no valid hello came back");
            }
            return theirs;
        }

        void write_all(const char* data, std::size_t len) {
            std::size_t moved = 0;
            while (moved < len) {
                const ssize_t n = ::send(fd, data + moved, len - moved, MSG_NOSIGNAL);
                if (n <= 0) {
                    throw std::runtime_error("WiredPeer: write failed");
                }
                moved += static_cast<std::size_t>(n);
            }
        }

        void half_close() {
            if (fd >= 0) {
                ::shutdown(fd, SHUT_WR);
            }
        }

        void close_now() {
            if (fd >= 0) {
                ::close(fd);
                fd = -1;
            }
        }
    };

    // ------------------------------------------------------------------------------------
    // The far end of one link the other way round: a listener the channel under test dials.
    // Where WiredPeer plays a peer that outranks the channel, this plays one it outranks, so
    // the dialer half of the establishment is the half under test.
    // ------------------------------------------------------------------------------------
    struct WiredListener {
        int fd = -1;
        int conn = -1;
        int port = 0;
        std::uint64_t nonce = 0x5EA1EDull;

        ~WiredListener() {
            if (conn >= 0) {
                ::close(conn);
            }
            if (fd >= 0) {
                ::close(fd);
            }
        }

        //! Bind on loopback and advertise the result under @p rank in the transport registry,
        //! which is where a dialer looks and the only thing it believes.
        void bind_and_publish(const std::string& comm, unsigned int rank, PeerRegistry& registry) {
            fd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (fd < 0) {
                throw std::runtime_error("WiredListener: socket() failed");
            }
            int one = 1;
            ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
            struct sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(0);
            ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
            if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0 ||
                ::listen(fd, 8) != 0) {
                throw std::runtime_error("WiredListener: could not listen on loopback");
            }
            socklen_t len = sizeof(addr);
            if (::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &len) != 0) {
                throw std::runtime_error("WiredListener: getsockname failed");
            }
            port = ntohs(addr.sin_port);
            registry.publish("fmi:drain:" + comm, std::to_string(rank),
                             "127.0.0.1:" + std::to_string(port) + ":" + std::to_string(nonce),
                             60, 2000);
        }

        //! Take the dialer's connection and its hello, and nothing more: the reply is the
        //! caller's to send, whenever the test wants the dialer to move on.
        ResumeRecord accept_hello(long timeout_ms) {
            struct pollfd pfd{fd, POLLIN, 0};
            if (::poll(&pfd, 1, static_cast<int>(timeout_ms)) <= 0) {
                throw std::runtime_error("WiredListener: nobody dialled");
            }
            conn = ::accept(fd, nullptr, nullptr);
            if (conn < 0) {
                throw std::runtime_error("WiredListener: accept failed");
            }
            char wire[resume_record_bytes];
            std::size_t moved = 0;
            while (moved < resume_record_bytes) {
                struct pollfd on_conn{conn, POLLIN, 0};
                if (::poll(&on_conn, 1, static_cast<int>(timeout_ms)) <= 0) {
                    throw std::runtime_error("WiredListener: no hello arrived");
                }
                const ssize_t n = ::recv(conn, wire + moved, resume_record_bytes - moved, 0);
                if (n <= 0) {
                    throw std::runtime_error("WiredListener: the dialer went away mid-hello");
                }
                moved += static_cast<std::size_t>(n);
            }
            ResumeRecord theirs;
            if (!decode_resume(wire, theirs)) {
                throw std::runtime_error("WiredListener: the hello did not decode");
            }
            return theirs;
        }

        //! The acceptor's half of the hello: their hash, nonce zero, counters as given.
        void reply(const ResumeRecord& theirs, std::uint64_t incarnation, std::uint64_t sent,
                   std::uint64_t received) {
            ResumeRecord mine;
            mine.sender_rank = theirs.receiver_rank;
            mine.receiver_rank = theirs.sender_rank;
            mine.link_name_hash = theirs.link_name_hash;
            mine.nonce = 0;
            mine.sender_incarnation = incarnation;
            mine.bytes_sent = sent;
            mine.bytes_received = received;
            char wire[resume_record_bytes];
            encode_resume(mine, wire);
            if (::send(conn, wire, resume_record_bytes, MSG_NOSIGNAL) !=
                static_cast<ssize_t>(resume_record_bytes)) {
                throw std::runtime_error("WiredListener: could not write the reply");
            }
        }

        //! Whether the far end has closed its side within @p timeout_ms.
        bool saw_eof(long timeout_ms) {
            struct pollfd pfd{conn, POLLIN, 0};
            if (::poll(&pfd, 1, static_cast<int>(timeout_ms)) <= 0) {
                return false;
            }
            char byte = 0;
            return ::recv(conn, &byte, 1, MSG_DONTWAIT) == 0;
        }
    };

    //! Every descriptor of this process that a `stat` calls a socket.
    /*!
     * The socketless image is the whole prize of this protocol, and it is a property of the
     * process rather than of any object in it — so it is asserted the way a checkpointer would
     * see it, by reading /proc/self/fd, and not by asking the channel what it thinks it holds.
     */
    std::set<int> open_socket_fds() {
        std::set<int> out;
        DIR* dir = ::opendir("/proc/self/fd");
        if (dir == nullptr) {
            return out;
        }
        while (const struct dirent* entry = ::readdir(dir)) {
            const int fd = std::atoi(entry->d_name);
            if (fd <= 0 || fd == ::dirfd(dir)) {
                continue;
            }
            struct stat st{};
            if (::fstat(fd, &st) == 0 && S_ISSOCK(st.st_mode)) {
                out.insert(fd);
            }
        }
        ::closedir(dir);
        return out;
    }

    //! Sockets this process holds that it did not inherit from the fork.
    /*!
     * The inherited ones — whatever the test binary happened to have open when the rank was
     * forked — are counted out by descriptor number, which is safe precisely because they are
     * never closed: their numbers cannot be recycled for a socket of the channel's.
     */
    int sockets_beyond(const std::set<int>& inherited) {
        int extra = 0;
        for (const int fd : open_socket_fds()) {
            if (inherited.count(fd) == 0) {
                extra++;
            }
        }
        return extra;
    }

    //! Arm a channel and hand back the address it published, so the test can dial it.
    MemberRecord arm_and_locate(const std::shared_ptr<DrainTCP>& ch, FakeDrainCoordinator& fake,
                                FMI::Utils::peer_num rank) {
        ch->set_incarnation(0);   // the documented arming point
        auto members = fake.members();
        auto it = members.find(rank);
        if (it == members.end()) {
            throw std::runtime_error("the channel did not arm (is Redis reachable?)");
        }
        return it->second;
    }
}

BOOST_AUTO_TEST_SUITE(DrainMigration)

//! A leave notice drains the link into user memory, and saying it twice changes nothing.
/*!
 * Also the epoch fence, in the same case because it is the same handler: once the peer has
 * announced a restore at a higher epoch, a notice from below it describes a process that no
 * longer exists and must be dropped. The two outcomes are told apart by what the receive path
 * does next — a link the channel believes is migrating parks on the migration clock, and one it
 * believes is merely absent runs out its transport deadline.
 */
BOOST_AUTO_TEST_CASE(a_leave_notice_drains_once_and_a_stale_one_not_at_all) {
    const std::string name = unique_comm("notice");
    auto ch = make_drain_channel(0, 2, name, drain_params("none", 400, 2000, 20000));
    auto* fake = new FakeDrainCoordinator();
    ch->set_coordinator_for_testing(std::unique_ptr<DrainCoordinator>(fake));

    const MemberRecord me = arm_and_locate(ch, *fake, 0);
    WiredPeer peer;
    peer.greet(me, name, 1, 0, 0, 0, 0);

    // Eight bytes the application has not asked for yet, then a half-close: this is the state a
    // migrating peer leaves behind, and every one of those bytes has to survive the cut.
    const char payload[8] = {'d', 'r', 'a', 'i', 'n', 'm', 'e', '!'};
    peer.write_all(payload, sizeof(payload));
    peer.half_close();

    fake->inject(DrainEvent::Type::Leaving, 1, 0);
    BOOST_REQUIRE_NO_THROW(ch->poll_control_events());

    char got[8];
    std::memset(got, 0, sizeof(got));
    BOOST_REQUIRE_NO_THROW(ch->recv({got, sizeof(got)}, 1));
    BOOST_CHECK_EQUAL(std::string(got, sizeof(got)), std::string(payload, sizeof(payload)));

    // At-least-once delivery is the only kind a stream offers, so the handler has to be safe to
    // run again on a link it has already sealed.
    fake->inject(DrainEvent::Type::Leaving, 1, 0);
    BOOST_CHECK_NO_THROW(ch->poll_control_events());

    fake->inject(DrainEvent::Type::Restored, 1, 1);
    BOOST_REQUIRE_NO_THROW(ch->poll_control_events());

    // Queued before the restore, delivered after it. Dropped, so the link stays merely absent —
    // which the receive path reports as the transport deadline running out, not as a migration
    // that never ends.
    fake->inject(DrainEvent::Type::Leaving, 1, 0);
    BOOST_REQUIRE_NO_THROW(ch->poll_control_events());

    peer.close_now();
    char ignored[4];
    const long started = now_ms();
    BOOST_CHECK_THROW(ch->recv({ignored, sizeof(ignored)}, 1), FMI::Utils::Timeout);
    BOOST_CHECK_MESSAGE(now_ms() - started < 5000,
                        "a stale leave notice suspended the deadline it should not have");
    ch->finalize();
}

//! The data path may see the FIN before the notice. Until the notice confirms it, that is a
//! guess — and a guess nobody confirms is what an unplanned death looks like.
BOOST_AUTO_TEST_CASE(an_unconfirmed_fin_becomes_a_loud_failure) {
    const std::string name = unique_comm("unconfirmed");
    auto ch = make_drain_channel(0, 2, name, drain_params("none", 20000, 600, 20000));
    auto* fake = new FakeDrainCoordinator();
    ch->set_coordinator_for_testing(std::unique_ptr<DrainCoordinator>(fake));

    const MemberRecord me = arm_and_locate(ch, *fake, 0);
    WiredPeer peer;
    peer.greet(me, name, 1, 0, 0, 0, 0);
    peer.close_now();   // an unplanned death: a FIN, and nothing on the control plane

    char got[4];
    const long started = now_ms();
    // Not a Timeout, and not silence: the peer is named, and the error says the protocol does
    // not recover from this. max_timeout is 20 s here precisely so that the bound that fires is
    // the grace period and not the transport's patience.
    BOOST_CHECK_THROW(ch->recv({got, sizeof(got)}, 1), std::runtime_error);
    const long elapsed = now_ms() - started;
    BOOST_CHECK_MESSAGE(elapsed >= 500 && elapsed < 5000,
                        "the unconfirmed drain resolved after " << elapsed
                                                                << " ms, expected ~600 ms");
    ch->finalize();
}

//! The same FIN, with the notice arriving first: one drain, and the application never learns
//! the difference.
BOOST_AUTO_TEST_CASE(a_notice_that_arrives_before_the_fin_produces_the_same_drain) {
    const std::string name = unique_comm("noticefirst");
    auto ch = make_drain_channel(0, 2, name, drain_params("none", 1000, 3000, 20000));
    auto* fake = new FakeDrainCoordinator();
    ch->set_coordinator_for_testing(std::unique_ptr<DrainCoordinator>(fake));

    const MemberRecord me = arm_and_locate(ch, *fake, 0);
    WiredPeer peer;
    peer.greet(me, name, 1, 0, 0, 0, 0);
    const char payload[6] = {'l', 'a', 't', 'e', 'r', '.'};
    peer.write_all(payload, sizeof(payload));

    // The survivor's half of the handshake, as a real survivor performs it: it half-closes only
    // once it has seen the migrating rank's FIN.
    std::thread responder([&peer]() {
        struct pollfd pfd{peer.fd, POLLIN, 0};
        ::poll(&pfd, 1, 3000);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        peer.half_close();
    });

    fake->inject(DrainEvent::Type::Leaving, 1, 0);
    BOOST_REQUIRE_NO_THROW(ch->poll_control_events());
    responder.join();

    char got[6];
    std::memset(got, 0, sizeof(got));
    BOOST_REQUIRE_NO_THROW(ch->recv({got, sizeof(got)}, 1));
    BOOST_CHECK_EQUAL(std::string(got, sizeof(got)), std::string(payload, sizeof(payload)));
    peer.close_now();
    ch->finalize();
}

//! A message the drain already moved into user memory is delivered without a connection.
/*!
 * After a migration a survivor's link is deliberately left closed, and everything the peer sent
 * before its FIN is already in this rank's heap and already counted as received. Establishing a
 * connection before consuming it would gate a message that has *arrived* behind a peer that has
 * no reason to dial — and when the sender outranks the receiver, its dial is the only thing that
 * could ever end the wait, so the message times out with itself sitting in local memory.
 */
BOOST_AUTO_TEST_CASE(a_message_the_drain_already_moved_needs_no_reconnect) {
    const std::string name = unique_comm("drained");
    auto ch = make_drain_channel(0, 2, name, drain_params("none", 1500, 2000, 20000));
    auto* fake = new FakeDrainCoordinator();
    ch->set_coordinator_for_testing(std::unique_ptr<DrainCoordinator>(fake));

    const MemberRecord me = arm_and_locate(ch, *fake, 0);
    WiredPeer peer;
    peer.greet(me, name, 1, 0, 0, 0, 0);
    const char payload[8] = {'d', 'r', 'a', 'i', 'n', 'm', 'e', '!'};
    peer.write_all(payload, sizeof(payload));
    peer.half_close();

    // The migration, start to finish: the notice drains all eight bytes into user memory, and the
    // restore clears the pause while leaving the link closed, which is exactly the state the
    // protocol says a survivor comes out of a migration in.
    fake->inject(DrainEvent::Type::Leaving, 1, 0);
    BOOST_REQUIRE_NO_THROW(ch->poll_control_events());
    fake->inject(DrainEvent::Type::Restored, 1, 1);
    BOOST_REQUIRE_NO_THROW(ch->poll_control_events());

    // And now the peer is not there at all: it will not dial, and rank 0 outranked by it cannot
    // dial either. If the receive path asks for a connection first, nothing can answer.
    peer.close_now();

    char got[8];
    std::memset(got, 0, sizeof(got));
    const long started = now_ms();
    BOOST_REQUIRE_NO_THROW(ch->recv({got, sizeof(got)}, 1));
    BOOST_CHECK_EQUAL(std::string(got, sizeof(got)), std::string(payload, sizeof(payload)));
    BOOST_CHECK_MESSAGE(now_ms() - started < 400,
                        "the drained bytes took " << (now_ms() - started)
                                                  << " ms to be handed over; they were in memory "
                                                     "the whole time");
    ch->finalize();
}

//! A leave notice that lands while this rank is dialling the leaver must not be overwritten by
//! the connection it raced.
/*!
 * The dial runs with the link lock released for its whole length — registry lookup, connect, two
 * 56-byte moves — so the notice can be applied in the middle of it, take its "the fd is already
 * -1, nothing to close" branch, and never run again. A connection filed on top of that is a live
 * socket on a link nothing will ever half-close, while the migrating peer's seal polls it for an
 * EOF that will not come: the migration then fails and takes the job with it.
 *
 * Here the far end is hand-written, so the notice can be placed exactly in that window — after
 * the hello has been read, before the reply goes back.
 */
BOOST_AUTO_TEST_CASE(a_leave_notice_during_a_dial_is_not_overwritten_by_it) {
    const std::string name = unique_comm("dialrace");
    // The migration bound is what must fire: max_timeout is long enough that patience cannot be
    // what ends this, so an application thread that parks proves it parked on the drain.
    auto ch = make_drain_channel(1, 2, name, drain_params("none", 20000, 5000, 800));
    auto* fake = new FakeDrainCoordinator();
    ch->set_coordinator_for_testing(std::unique_ptr<DrainCoordinator>(fake));
    ch->set_incarnation(0);   // the arming point; rank 1 dials rank 0, so rank 0 must be findable

    PeerRegistry registry("127.0.0.1", 6379);
    WiredListener leaver;
    leaver.bind_and_publish(name, 0, registry);

    std::string failure;
    std::thread app([&ch, &failure]() {
        int value = 42;
        try {
            ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 0);
            failure = "the send completed; it should have parked on the migration";
        } catch (const std::exception& e) {
            failure = e.what();
        }
    });

    ResumeRecord theirs;
    BOOST_REQUIRE_NO_THROW(theirs = leaver.accept_hello(5000));
    // The dialer is now blocked reading a reply that has not been written. Deterministically
    // inside the window: the notice lands with the hello already sent and nothing filed yet.
    fake->inject(DrainEvent::Type::Leaving, 0, 0);
    BOOST_REQUIRE_NO_THROW(ch->poll_control_events());
    BOOST_REQUIRE_NO_THROW(leaver.reply(theirs, 0, 0, 0));

    // The link must come out of this with no descriptor on it: the dialer drops the connection
    // it just completed, and the close is what delivers the FIN the leaver's seal is waiting for.
    BOOST_CHECK_MESSAGE(leaver.saw_eof(4000),
                        "the connection was filed on a draining link instead of being dropped");
    app.join();
    // ...and the application thread is parked on the migration clock, not writing into a socket
    // the migration has already accounted for.
    BOOST_CHECK_MESSAGE(failure.find("migration_max_ms") != std::string::npos,
                        "the parked send failed with '" << failure
                                                        << "', expected the migration bound");
    ch->finalize();
}

//! A batch lease somebody else holds stops the migration before it touches a socket, and says so.
/*!
 * One batch in flight per communicator. What the code does with a refusal is not "queue and
 * serialise" but a loud failure that breaks the channel — the documented one-phase policy: a
 * migration that could not run must not look like one that did nothing.
 */
BOOST_AUTO_TEST_CASE(a_batch_lease_held_elsewhere_stops_the_migration_loudly) {
    const std::string name = unique_comm("lease");
    auto ch = make_drain_channel(0, 2, name, drain_params("none", 2000, 2000, 20000));
    auto* fake = new FakeDrainCoordinator();
    ch->set_coordinator_for_testing(std::unique_ptr<DrainCoordinator>(fake));

    const MemberRecord me = arm_and_locate(ch, *fake, 0);
    WiredPeer peer;
    peer.greet(me, name, 1, 0, 0, 0, 0);

    BOOST_REQUIRE(fake->try_batch_lock("a-batch-of-someone-else's", 60000));

    std::string reported;
    try {
        ch->rehearse_migration_in_place();
        reported = "(the migration ran)";
    } catch (const std::exception& e) {
        reported = e.what();
    }
    BOOST_CHECK_MESSAGE(reported.find("batch lease") != std::string::npos,
                        "the refused migration reported '" << reported
                                                           << "', expected the batch lease");

    // Nothing was sealed: the link never saw a half-close, because the lease is taken before the
    // notice goes out and long before a descriptor is touched.
    struct pollfd pfd{peer.fd, POLLIN, 0};
    BOOST_CHECK_MESSAGE(::poll(&pfd, 1, 100) == 0,
                        "a migration that never started still half-closed a link");

    // And the channel is broken on purpose, which is what the application finds out.
    char ignored[4];
    std::string raised;
    try {
        ch->recv({ignored, sizeof(ignored)}, 1);
    } catch (const std::exception& e) {
        raised = e.what();
    }
    BOOST_CHECK_MESSAGE(raised.find("migration that could not complete") != std::string::npos,
                        "the application was told '" << raised
                                                     << "', expected the broken-channel reason");
    peer.close_now();
    ch->finalize();
}

//! A leave notice written by a lineage this rank has already replaced is dropped, connection
//! intact.
/*!
 * The notice and the migrating rank's own reconnect travel over different media, so the notice
 * can arrive *after* the peer is back and has dialled in. Acting on it then would half-close and
 * tear down a link both ends believe in, in the name of a migration that is already over — and
 * since the peer is not migrating any more, nothing would ever send the FIN that ends the drain,
 * so the link would be marked unrecoverable and the job would die naming a migration that
 * finished long ago.
 */
BOOST_AUTO_TEST_CASE(a_leave_notice_from_a_superseded_lineage_is_dropped) {
    const std::string name = unique_comm("latenotice");
    // A small grace period on purpose: if the notice were acted on, the drain would wait for a
    // FIN this peer has no reason to send, and the case would fail in under a second.
    auto ch = make_drain_channel(0, 2, name, drain_params("none", 2000, 500, 20000));
    auto* fake = new FakeDrainCoordinator();
    ch->set_coordinator_for_testing(std::unique_ptr<DrainCoordinator>(fake));

    const MemberRecord me = arm_and_locate(ch, *fake, 0);
    WiredPeer peer;
    // The lineage that came back from the migration the notice below describes.
    peer.greet(me, name, 1, 0, /*incarnation=*/1, 0, 0);

    fake->inject(DrainEvent::Type::Leaving, 1, 0, {{"incarnation", "0"}});
    BOOST_REQUIRE_NO_THROW(ch->poll_control_events());

    // Untouched: no half-close reached the peer, so the socket has nothing to report.
    struct pollfd pfd{peer.fd, POLLIN, 0};
    BOOST_CHECK_MESSAGE(::poll(&pfd, 1, 100) == 0,
                        "a notice from a superseded lineage tore down the link its successor "
                        "built");

    // And traffic written after the notice still arrives over that same connection.
    const char payload[6] = {'a', 'l', 'i', 'v', 'e', '.'};
    peer.write_all(payload, sizeof(payload));
    char got[6];
    std::memset(got, 0, sizeof(got));
    BOOST_REQUIRE_NO_THROW(ch->recv({got, sizeof(got)}, 1));
    BOOST_CHECK_EQUAL(std::string(got, sizeof(got)), std::string(payload, sizeof(payload)));
    peer.close_now();
    ch->finalize();
}

//! A hello claiming a lineage below the one already seen is a superseded process, and adopting
//! its stream would rewind the link. Refused, loudly, and the link stays broken.
BOOST_AUTO_TEST_CASE(an_incarnation_below_the_one_already_seen_is_refused) {
    const std::string name = unique_comm("lineage");
    auto ch = make_drain_channel(0, 2, name, drain_params("none", 500, 2000, 20000));
    auto* fake = new FakeDrainCoordinator();
    ch->set_coordinator_for_testing(std::unique_ptr<DrainCoordinator>(fake));

    const MemberRecord me = arm_and_locate(ch, *fake, 0);
    {
        WiredPeer peer;
        peer.greet(me, name, 1, 0, /*incarnation=*/5, 0, 0);
        peer.close_now();
    }
    // Everything else about this second hello is impeccable: the right listener nonce, the
    // right link, counters that agree. Only the lineage is older.
    {
        WiredPeer stale;
        BOOST_CHECK_NO_THROW(
                stale.greet(me, name, 1, 0, /*incarnation=*/2, 0, 0, /*expect_refusal=*/true));
    }

    char ignored[4];
    // Recorded by the control thread, raised on the application thread: the refusal is not
    // something a retry can fix, so it must not look like patience running out.
    BOOST_CHECK_THROW(ch->send({ignored, sizeof(ignored)}, 1), std::runtime_error);
    ch->finalize();
}

//! Two ranks, rounds of exchanges, and one of them rehearses a whole migration in the middle.
//! Every value arrives, which means the counters agreed at the reconnect — a byte lost or
//! duplicated across the cut would have failed the hello instead.
BOOST_AUTO_TEST_CASE(two_ranks_survive_a_rehearsed_migration) {
    constexpr int num_peers = 2;
    constexpr int rounds = 12;
    const std::string name = unique_comm("rehearse2");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        auto ch = make_drain_channel(peer_id, num_peers, name,
                                     drain_params("control", 15000, 3000, 30000));
        int failures = 0;
        for (int r = 0; r < rounds; r++) {
            if (peer_id == 0) {
                int out = r;
                ch->send({reinterpret_cast<char*>(&out), sizeof(out)}, 1);
                int back = -1;
                ch->recv({reinterpret_cast<char*>(&back), sizeof(back)}, 1);
                if (back != r + 1000) {
                    failures++;
                }
            } else {
                int got = -1;
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                if (got != r) {
                    failures++;
                }
                if (r == rounds / 2) {
                    // Between two messages: the quiet case, and the one every other case is a
                    // harder version of.
                    ch->rehearse_migration_in_place();
                }
                int reply = got + 1000;
                ch->send({reinterpret_cast<char*>(&reply), sizeof(reply)}, 0);
            }
        }
        ok[peer_id] = (failures == 0);
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[0] == 1, "rank 0 did not complete its rounds across the migration");
    BOOST_CHECK_MESSAGE(ok[1] == 1, "rank 1 did not complete its rounds across the migration");
}

//! A survivor caught mid-message: the migrating rank drains the prefix its peer had already
//! handed to the kernel, and the peer resumes at its offset on the connection that replaces the
//! one it started on. The message arrives byte for byte or the case fails.
BOOST_AUTO_TEST_CASE(a_survivor_mid_send_resumes_at_its_offset) {
    constexpr int num_peers = 2;
    constexpr std::size_t payload_bytes = 8u << 20;   // far past any socket buffer
    const std::string name = unique_comm("midsend");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        auto ch = make_drain_channel(peer_id, num_peers, name,
                                     drain_params("control", 30000, 5000, 60000));
        std::vector<char> buffer(payload_bytes);
        if (peer_id == 0) {
            for (std::size_t i = 0; i < payload_bytes; i++) {
                buffer[i] = static_cast<char>((i * 31u + 7u) & 0xFF);
            }
            // The receiver is asleep, so this blocks with the socket buffers full and most of
            // the message still in the application's hands: mid-message, deterministically.
            ch->send({buffer.data(), buffer.size()}, 1);
            ok[0] = 1;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            ch->rehearse_migration_in_place();
            ch->recv({buffer.data(), buffer.size()}, 0);
            std::size_t wrong = 0;
            for (std::size_t i = 0; i < payload_bytes; i++) {
                if (buffer[i] != static_cast<char>((i * 31u + 7u) & 0xFF)) {
                    wrong++;
                }
            }
            ok[1] = (wrong == 0);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[0] == 1, "the sender did not finish its message across the migration");
    BOOST_CHECK_MESSAGE(ok[1] == 1, "the drained prefix and the resumed remainder did not join up");
}

//! The other direction: the *migrating* rank is the one mid-message. Its peer's drain captures
//! everything the kernel had already accepted, and the migrator resumes at its own offset.
/*!
 * This is the half of the counter invariant the previous case does not reach. There, the
 * survivor's `bytes_sent` had to equal the migrator's drained `bytes_received`; here it is the
 * migrator's `bytes_sent` against the survivor's, which advances both in the application's
 * receive loop and inside the survivor's own drain. Both have to agree at the reconnect, in
 * both directions at once, or the hello refuses the link.
 */
BOOST_AUTO_TEST_CASE(a_migrator_mid_send_resumes_at_its_offset) {
    constexpr int num_peers = 2;
    constexpr std::size_t payload_bytes = 8u << 20;
    const std::string name = unique_comm("migmidsend");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        auto ch = make_drain_channel(peer_id, num_peers, name,
                                     drain_params("control", 30000, 5000, 60000));
        std::vector<char> buffer(payload_bytes);
        if (peer_id == 1) {
            for (std::size_t i = 0; i < payload_bytes; i++) {
                buffer[i] = static_cast<char>((i * 17u + 3u) & 0xFF);
            }
            // The migration runs on a thread of its own, because the application thread this
            // rank owns is exactly the one that must be caught in the middle of a message.
            std::thread migrator([&ch]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
                try {
                    ch->rehearse_migration_in_place();
                } catch (const std::exception& e) {
                    std::fprintf(stderr, "[rank 1] rehearsal: %s\n", e.what());
                }
            });
            ch->send({buffer.data(), buffer.size()}, 0);
            migrator.join();
            ok[1] = 1;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(600));
            ch->recv({buffer.data(), buffer.size()}, 1);
            std::size_t wrong = 0;
            for (std::size_t i = 0; i < payload_bytes; i++) {
                if (buffer[i] != static_cast<char>((i * 17u + 3u) & 0xFF)) {
                    wrong++;
                }
            }
            ok[0] = (wrong == 0);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[1] == 1, "the migrating sender did not finish its message");
    BOOST_CHECK_MESSAGE(ok[0] == 1, "the receiver did not reassemble the migrated message");
}

//! The migrating rank's *own* application thread, caught mid-message, is paid for the window it
//! was held across.
/*!
 * The suspension of the application clock exists so that a migration is not charged to the
 * patience the job configured for a link that is merely slow. For a survivor that is easy: it
 * waits on the link's condition variable and can time itself. For the migrating rank's own
 * threads it is not: the drain takes every link lock and holds them from the seal to the end of
 * the restore leg, so a thread that was mid-message spends the whole migration blocked on a plain
 * mutex — never waiting on anything it could measure — and `draining` is set and cleared entirely
 * inside the interval in which it cannot run. Charged to the transport clock, that window is a
 * `Timeout` thrown out of `send()` immediately after a migration that *succeeded*, and a Timeout
 * is terminal for the communicator.
 *
 * The link is established by a handshake first, deliberately: an application thread still inside
 * its establishment takes an entirely different path (it yields the establish gate and parks on
 * the migration clock, where it is credited), and that is the accident by which every other case
 * here misses this.
 */
BOOST_AUTO_TEST_CASE(a_migrator_mid_send_is_paid_for_its_own_migration) {
    constexpr int num_peers = 2;
    constexpr std::size_t payload_bytes = 32u << 20;   // far past any socket buffer
    const std::string name = unique_comm("migclock");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        // rank 1's patience is 2000 ms and its own migration holds every link lock for 3000. rank
        // 0 is patient, so the only clock this case can fail on is the migrator's own.
        auto ch = make_drain_channel(peer_id, num_peers, name,
                                     drain_params("control", peer_id == 1 ? 2000 : 30000, 5000,
                                                  60000));
        std::vector<char> buffer(payload_bytes);
        if (peer_id == 1) {
            for (std::size_t i = 0; i < payload_bytes; i++) {
                buffer[i] = static_cast<char>((i * 29u + 11u) & 0xFF);
            }
            // The handshake is what makes this case the one it means to be: the link is
            // ESTABLISHED, so the big send below runs in the data loop from its first byte.
            int hello = 1;
            ch->send({reinterpret_cast<char*>(&hello), sizeof(hello)}, 0);
            int back = -1;
            ch->recv({reinterpret_cast<char*>(&back), sizeof(back)}, 0);

            std::thread migrator([&ch]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
                try {
                    ch->rehearse_migration_in_place(3000);
                } catch (const std::exception& e) {
                    std::fprintf(stderr, "[rank 1] rehearsal: %s\n", e.what());
                }
            });
            // The receiver is asleep, so this blocks with the socket buffers full and most of the
            // message still in the application's hands — mid-message, on a live link, when the
            // migration seizes the lock underneath it.
            //
            // Caught here rather than let out of the case: the failure this pins is precisely
            // "the send throws", and a throw past a joinable thread terminates the process
            // instead of reporting anything.
            std::string trouble;
            try {
                ch->send({buffer.data(), buffer.size()}, 0);
            } catch (const std::exception& e) {
                trouble = e.what();
                std::fprintf(stderr, "[rank 1] the migrator's own send: %s\n", e.what());
            }
            migrator.join();
            ok[1] = (trouble.empty() && back == 2);
        } else {
            int hello = -1;
            ch->recv({reinterpret_cast<char*>(&hello), sizeof(hello)}, 1);
            int reply = 2;
            ch->send({reinterpret_cast<char*>(&reply), sizeof(reply)}, 1);
            std::this_thread::sleep_for(std::chrono::milliseconds(1500));
            ch->recv({buffer.data(), buffer.size()}, 1);
            std::size_t wrong = 0;
            for (std::size_t i = 0; i < payload_bytes; i++) {
                if (buffer[i] != static_cast<char>((i * 29u + 11u) & 0xFF)) {
                    wrong++;
                }
            }
            ok[0] = (wrong == 0 && hello == 1);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[1] == 1,
                        "the migrating rank's own send failed across its own migration");
    BOOST_CHECK_MESSAGE(ok[0] == 1, "the receiver did not reassemble the migrated message");
}

//! At the stop, the process owns no socket at all. That is the prize, so it is an assertion.
/*!
 * A migration that leaves one connected socket in the image is a migration whose image is bound
 * to the host that took it, and whose CRIU legs need `--tcp-close` — which is the thing this
 * protocol exists not to need. Nothing about that is visible from the channel's own state, so it
 * is checked the way a checkpointer sees it: by reading /proc/self/fd from inside the rank while
 * the rehearsal holds it sealed.
 *
 * The minimum over the whole run is what is recorded, because the scan cannot know when the
 * sealed window is: outside it the rank holds a listener, a link and two registry connections, so
 * a zero can only have been sampled inside.
 */
BOOST_AUTO_TEST_CASE(a_sealed_rank_owns_no_socket) {
    constexpr int num_peers = 2;
    const std::string name = unique_comm("socketless");
    int* state = shared_flags(4);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    state[peer_id] = 0;
    if (peer_id == 0) {
        state[2] = -1;   // the fewest sockets rank 1 was ever seen holding
        state[3] = -1;   // and how many it inherited from the fork, for the failure message
    }

    try {
        auto ch = make_drain_channel(peer_id, num_peers, name,
                                     drain_params("control", 30000, 5000, 60000));
        if (peer_id == 0) {
            int value = 5;
            ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);
            int back = -1;
            ch->recv({reinterpret_cast<char*>(&back), sizeof(back)}, 1);
            state[0] = (back == 6);
        } else {
            // Whatever the test binary had open when this rank was forked is not this channel's,
            // and it is never closed, so its descriptor numbers cannot be recycled.
            const std::set<int> inherited = open_socket_fds();
            int value = 0;
            ch->recv({reinterpret_cast<char*>(&value), sizeof(value)}, 0);

            std::atomic<bool> scanning{true};
            std::atomic<int> fewest{std::numeric_limits<int>::max()};
            std::thread scanner([&scanning, &fewest, &inherited]() {
                while (scanning.load(std::memory_order_acquire)) {
                    const int seen = sockets_beyond(inherited);
                    int best = fewest.load(std::memory_order_acquire);
                    while (seen < best && !fewest.compare_exchange_weak(best, seen)) {
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }
            });
            ch->rehearse_migration_in_place(1000);
            scanning.store(false, std::memory_order_release);
            scanner.join();

            state[2] = fewest.load();
            state[3] = static_cast<int>(inherited.size());
            int reply = value + 1;
            ch->send({reinterpret_cast<char*>(&reply), sizeof(reply)}, 0);
            state[1] = (value == 5);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(state[1] == 1, "rank 1 did not complete its held migration");
    BOOST_CHECK_MESSAGE(state[0] == 1, "rank 0 did not get its reply after the migration");
    BOOST_CHECK_MESSAGE(state[2] == 0,
                        "at its quietest the sealed rank still held " << state[2]
                                << " socket(s) of its own (plus " << state[3]
                                << " inherited); a CRIU image of it would not be host-agnostic");
}

//! The same rank migrates twice in one job, which is the only thing that proves the batch lease
//! is given back.
/*!
 * The lease is taken before the leave notice and released at the end of the restore leg by a
 * check-and-delete. If that release ever stopped working the first migration would still look
 * perfect: only a second one finds the lease still held, and the refusal is a loud failure that
 * breaks a job which had already migrated successfully once.
 */
BOOST_AUTO_TEST_CASE(a_rank_migrates_twice_in_one_job) {
    constexpr int num_peers = 2;
    const std::string name = unique_comm("twice");
    int* state = shared_flags(3);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    state[peer_id] = 0;
    if (peer_id == 0) {
        state[2] = -1;   // rank 1's epoch after both migrations
    }

    try {
        auto ch = make_drain_channel(peer_id, num_peers, name,
                                     drain_params("control", 20000, 5000, 60000));
        int failures = 0;
        for (int round = 0; round < 2; round++) {
            if (peer_id == 0) {
                int out = round;
                ch->send({reinterpret_cast<char*>(&out), sizeof(out)}, 1);
                int back = -1;
                ch->recv({reinterpret_cast<char*>(&back), sizeof(back)}, 1);
                if (back != round + 100) {
                    failures++;
                }
            } else {
                int got = -1;
                ch->recv({reinterpret_cast<char*>(&got), sizeof(got)}, 0);
                if (got != round) {
                    failures++;
                }
                // Twice against a real coordinator, under the same communicator: the second
                // try_batch_lock can only succeed if the first migration released the lease.
                ch->rehearse_migration_in_place();
                int reply = got + 100;
                ch->send({reinterpret_cast<char*>(&reply), sizeof(reply)}, 0);
            }
        }
        if (peer_id == 1) {
            state[2] = static_cast<int>(MigrationTrigger::instance().epoch());
        }
        state[peer_id] = (failures == 0);
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(state[1] == 1, "rank 1 did not complete both of its migrations");
    BOOST_CHECK_MESSAGE(state[0] == 1, "rank 0 did not exchange across both migrations");
    BOOST_CHECK_MESSAGE(state[2] == 2,
                        "rank 1 ran " << state[2] << " migrations, expected two");
}

//! The objection this whole design answers, as a test: a peer that is busy computing and will
//! not enter the library for seconds still half-closes on time, because the thread that answers
//! is not the application's.
BOOST_AUTO_TEST_CASE(a_compute_bound_peer_does_not_delay_the_drain) {
    constexpr int num_peers = 2;
    constexpr long compute_ms = 3000;
    const std::string name = unique_comm("computebound");
    int* result = shared_flags(num_peers + 1);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    result[peer_id] = 0;
    if (peer_id == 0) {
        result[num_peers] = -1;   // the measured rehearsal, written by rank 1
    }

    try {
        auto ch = make_drain_channel(peer_id, num_peers, name,
                                     drain_params("control", 20000, 5000, 60000));
        int handshake = 0;
        if (peer_id == 0) {
            handshake = 1;
            ch->send({reinterpret_cast<char*>(&handshake), sizeof(handshake)}, 1);
            // Three seconds inside application code, nowhere near FMI.
            std::this_thread::sleep_for(std::chrono::milliseconds(compute_ms));
            int back = -1;
            ch->recv({reinterpret_cast<char*>(&back), sizeof(back)}, 1);
            result[0] = (back == 2);
        } else {
            ch->recv({reinterpret_cast<char*>(&handshake), sizeof(handshake)}, 0);
            const long started = now_ms();
            ch->rehearse_migration_in_place();
            result[num_peers] = static_cast<int>(now_ms() - started);
            int reply = 2;
            ch->send({reinterpret_cast<char*>(&reply), sizeof(reply)}, 0);
            result[1] = (handshake == 1);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(result[1] == 1, "rank 1 did not complete its migration");
    BOOST_CHECK_MESSAGE(result[0] == 1, "rank 0 did not get its reply after the migration");
    BOOST_CHECK_MESSAGE(result[num_peers] >= 0 && result[num_peers] < compute_ms / 2,
                        "the drain took " << result[num_peers]
                                          << " ms against a peer that was computing for "
                                          << compute_ms
                                          << " ms; it must not wait for an application thread");
}

//! A survivor blocked in recv does not time out across a migration longer than its own patience.
//! The deadline it was configured with measures a link that is slow, not one that is migrating.
BOOST_AUTO_TEST_CASE(a_blocked_survivor_waits_out_a_migration_longer_than_max_timeout) {
    constexpr int num_peers = 2;
    const std::string name = unique_comm("waitout");
    int* ok = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    ok[peer_id] = 0;

    try {
        // max_timeout 2000 against a migration that stays sealed for 3000: the transport clock
        // would have fired twice over, and must not have fired at all.
        auto ch = make_drain_channel(peer_id, num_peers, name,
                                     drain_params("control", 2000, 8000, 60000));
        int value = 0;
        if (peer_id == 0) {
            value = 7;
            ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);
            int back = -1;
            ch->recv({reinterpret_cast<char*>(&back), sizeof(back)}, 1);
            ok[0] = (back == 8);
        } else {
            ch->recv({reinterpret_cast<char*>(&value), sizeof(value)}, 0);
            ch->rehearse_migration_in_place(3000);
            int reply = value + 1;
            ch->send({reinterpret_cast<char*>(&reply), sizeof(reply)}, 0);
            ok[1] = (value == 7);
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(ok[1] == 1, "rank 1 did not complete its held migration");
    BOOST_CHECK_MESSAGE(ok[0] == 1,
                        "rank 0 failed across a migration it was supposed to wait out");
}

//! And the documented failure surface, as a test: waiting is not unconditional. Past
//! migration_max_ms the survivor gives up and names the peer it was waiting for.
BOOST_AUTO_TEST_CASE(a_survivor_gives_up_once_migration_max_ms_lapses) {
    constexpr int num_peers = 2;
    const std::string name = unique_comm("givesup");
    int* verdict = shared_flags(num_peers);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    verdict[peer_id] = 0;

    try {
        // The migration bound is 500 ms and the rehearsal holds for 2500: the survivor must
        // fail, and the failure must not be a Timeout, which would say the wrong thing about
        // why it happened.
        auto ch = make_drain_channel(peer_id, num_peers, name,
                                     drain_params("control", 20000, 8000, 500));
        int value = 0;
        if (peer_id == 0) {
            value = 5;
            ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);
            int back = -1;
            try {
                ch->recv({reinterpret_cast<char*>(&back), sizeof(back)}, 1);
                verdict[0] = 0;   // it should not have got here
            } catch (const FMI::Utils::Timeout&) {
                verdict[0] = 2;   // wrong failure: patience, not a migration that overran
            } catch (const std::runtime_error& e) {
                const std::string text = e.what();
                verdict[0] = (text.find("peer 1") != std::string::npos &&
                              text.find("migration_max_ms") != std::string::npos)
                                     ? 1
                                     : 3;
            }
        } else {
            ch->recv({reinterpret_cast<char*>(&value), sizeof(value)}, 0);
            ch->rehearse_migration_in_place(2500);
            verdict[1] = 1;
        }
        ch->finalize();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(verdict[1] == 1, "rank 1 did not complete its over-long migration");
    BOOST_CHECK_MESSAGE(verdict[0] == 1,
                        "rank 0's failure was " << verdict[0]
                                                << " (1 = named the peer and the bound, 0 = no "
                                                   "failure at all, 2 = a Timeout, 3 = something "
                                                   "else)");
}

//! The signal path end to end, including the fence that makes it safe to re-arm: a request
//! queued for an epoch this process has already left is dropped, and the next one is performed.
/*!
 * Under `drain_rehearsal_only` the sequence ends in an in-place restore rather than a
 * `raise(SIGSTOP)`, so the test can observe the far side of it without a checkpointer. What is
 * exercised is everything up to and including the stop's replacement: the signal is delivered
 * to a process whose application thread is asleep and whose trigger thread does the work, and
 * the epoch counter afterwards says exactly how many migrations ran.
 *
 * The third signal is the lifecycle half: it is delivered when the rank has *finalized*, so
 * nothing is armed to receive it, and it names the epoch the process is actually at — the
 * staleness fence cannot drop it. A request nobody is armed for belongs to nobody, and the next
 * communicator this process builds must not inherit it: honouring it there would seal every link
 * and stop the process with no checkpointer on the other side of the stop.
 */
BOOST_AUTO_TEST_CASE(a_queued_signal_migrates_once_and_a_stale_one_not_at_all) {
    constexpr int num_peers = 2;
    const std::string name = unique_comm("signal");
    // [0],[1] verdicts; [2] rank 1's epoch after the two armed signals; [3] rank 1 has finalized;
    // [4] rank 0 has signalled the unarmed process; [5] rank 1's epoch after arming again.
    int* state = shared_flags(6);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    state[peer_id] = 0;
    if (peer_id == 0) {
        state[num_peers] = -1;   // rank 1's epoch after the signals, written by rank 1
        state[3] = 0;
        state[4] = 0;
        state[5] = -1;
    }

    // The two ranks hand this test over to each other twice, so neither can run ahead of the
    // other's half of the lifecycle. Bounded, because a test that waits forever reports nothing.
    const auto wait_for_flag = [state](int slot, long budget_ms) {
        const long deadline = now_ms() + budget_ms;
        while (state[slot] != 1 && now_ms() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return state[slot] == 1;
    };

    try {
        auto params = drain_params("both", 20000, 8000, 60000);
        params["drain_rehearsal_only"] = "true";
        auto ch = make_drain_channel(peer_id, num_peers, name, params);
        int value = 0;
        if (peer_id == 0) {
            value = 3;
            ch->send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);
            int back = -1;
            ch->recv({reinterpret_cast<char*>(&back), sizeof(back)}, 1);   // rank 1 is now armed
            const int signal_number = MigrationTrigger::signal_for_offset(3);
            union sigval payload{};
            // Addressed to a lineage this process has already left. Dropped, with a line saying
            // so — never performed, and never silently ignored either.
            payload.sival_int = -1;
            ::sigqueue(rank_guard.children.at(0), signal_number, payload);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            payload.sival_int = 0;
            ::sigqueue(rank_guard.children.at(0), signal_number, payload);
            int last = -1;
            ch->recv({reinterpret_cast<char*>(&last), sizeof(last)}, 1);
            state[0] = (back == 4 && last == 9);
            ch->finalize();

            // Nothing is armed in rank 1 any more. This request names the epoch that process is
            // at, so the staleness fence has nothing to say about it — only the arm/disarm
            // hygiene can drop it.
            if (wait_for_flag(3, 15000)) {
                payload.sival_int = 1;
                ::sigqueue(rank_guard.children.at(0), signal_number, payload);
            }
            state[4] = 1;
        } else {
            ch->recv({reinterpret_cast<char*>(&value), sizeof(value)}, 0);
            int reply = value + 1;
            ch->send({reinterpret_cast<char*>(&reply), sizeof(reply)}, 0);
            // Asleep in application code while both signals arrive: whatever happens, happens
            // on the trigger thread.
            std::this_thread::sleep_for(std::chrono::milliseconds(2500));
            state[num_peers] = static_cast<int>(MigrationTrigger::instance().epoch());
            int last = 9;
            ch->send({reinterpret_cast<char*>(&last), sizeof(last)}, 0);
            state[1] = 1;
            ch->finalize();

            state[3] = 1;   // disarmed; rank 0 may now signal a process that is listening to
                            // nobody
            wait_for_flag(4, 15000);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));

            // A second communicator in the same process — the shape that turns a retained
            // request into an unrequested migration. It arms exactly as the first one did.
            auto again = make_drain_channel(peer_id, num_peers, unique_comm("signal-again"),
                                            params);
            again->set_incarnation(0);
            std::this_thread::sleep_for(std::chrono::milliseconds(400));   // many trigger ticks
            state[5] = static_cast<int>(MigrationTrigger::instance().epoch());
            again->finalize();
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[rank %d] %s\n", peer_id, e.what());
    }

    rank_guard.reap_ranks();
    BOOST_CHECK_MESSAGE(state[1] == 1, "rank 1 did not finish after its signalled migration");
    BOOST_CHECK_MESSAGE(state[0] == 1, "rank 0 did not exchange across the signalled migration");
    BOOST_CHECK_MESSAGE(state[num_peers] == 1,
                        "rank 1 ran " << state[num_peers]
                                      << " migrations; exactly one of the two signals was "
                                         "addressed to its current epoch");
    BOOST_CHECK_MESSAGE(state[5] == 1,
                        "rank 1 was at epoch " << state[5]
                                               << " after arming a second communicator; a request "
                                                  "delivered while nothing was armed was honoured "
                                                  "by the next arming");
}

BOOST_AUTO_TEST_SUITE_END()

#endif // FMI_ENABLE_REDIS
