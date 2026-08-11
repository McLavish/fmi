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
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
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
        void inject(DrainEvent::Type type, FMI::Utils::peer_num rank, std::uint64_t epoch) {
            DrainEvent event;
            event.type = type;
            event.rank = rank;
            event.epoch = epoch;
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
 */
BOOST_AUTO_TEST_CASE(a_queued_signal_migrates_once_and_a_stale_one_not_at_all) {
    constexpr int num_peers = 2;
    const std::string name = unique_comm("signal");
    int* state = shared_flags(num_peers + 1);

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);
    state[peer_id] = 0;
    if (peer_id == 0) {
        state[num_peers] = -1;   // rank 1's epoch after the signals, written by rank 1
    }

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
        }
        ch->finalize();
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
}

BOOST_AUTO_TEST_SUITE_END()

#endif // FMI_ENABLE_REDIS
