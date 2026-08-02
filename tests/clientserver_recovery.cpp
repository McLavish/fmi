#include <boost/test/unit_test.hpp>

// The whole suite talks to a live Redis as a ClientServer data plane; without the backend
// compiled in there is nothing here to test.
#if FMI_ENABLE_REDIS

#include "forked_rank_guard.h"

#include "../include/comm/Channel.h"
#include "../include/comm/ClientServer.h"
#include "../include/comm/Redis.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

//! Behaviour of the ClientServer (store-backed) channel family across connection loss.
/*!
 * The cases here are the pinning half: they assert what the channels do TODAY, so that the
 * recovery work that follows can only change behaviour where it means to. Anything that reads
 * odd below (a finalize that strands peers, a download that silently truncates, a barrier that
 * counts another communicator's markers) is pinned deliberately, not endorsed.
 */
BOOST_AUTO_TEST_SUITE(ClientServerRecovery);

namespace {

    // Same shape as tests/channels.cpp's redis_test_params/redis_test_model_params: this suite
    // drives the Redis channel directly rather than through a Communicator, so it needs the same
    // untyped maps the factory hands to the backend.
    std::map<std::string, std::string> redis_test_params = {
            {"host",        "127.0.0.1"},
            {"port",        "6379"},
            {"timeout",     "1"},
            {"max_timeout", "1000"}
    };

    std::map<std::string, std::string> redis_test_model_params = {
            {"bandwidth_single",             "100.0"},
            {"bandwidth_multiple",           "400.0"},
            {"overhead",                     "5.2"},
            {"transfer_price",               "0.0"},
            {"instance_price",               "0.0038"},
            {"requests_per_hour",            "1000"},
            {"include_infrastructure_costs", "true"}
    };

    //! Nanosecond clock plus the pid, as in tests/channels.cpp.
    /*!
     * Keys in the store outlive the process that wrote them (a crashed or timed-out case leaves
     * them behind), so two runs sharing a name would read each other's data. Per case, not per
     * suite: several cases here deliberately leave the store in a state the next one must not see.
     */
    std::string unique_comm_name(const std::string& tag) {
        return "csrec_" + tag + "_" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "_" +
               std::to_string(getpid()) + "_";
    }

    std::shared_ptr<FMI::Comm::ClientServer> make_redis(const std::string& comm_name,
                                                        FMI::Utils::peer_num peer_id,
                                                        FMI::Utils::peer_num num_peers,
                                                        std::map<std::string, std::string> params = redis_test_params) {
        auto channel = FMI::Comm::Channel::get_channel("Redis", params, redis_test_model_params);
        auto client_server = std::dynamic_pointer_cast<FMI::Comm::ClientServer>(channel);
        BOOST_REQUIRE(client_server != nullptr);
        client_server->set_peer_id(peer_id);
        client_server->set_num_peers(num_peers);
        client_server->set_comm_name(comm_name);
        return client_server;
    }

    //! redis_test_params with the recovery semantics on, and any extra keys the case needs.
    std::map<std::string, std::string> recover_params(std::map<std::string, std::string> extra = {}) {
        std::map<std::string, std::string> params = redis_test_params;
        params["recover"] = "true";
        // Short enough that a case which leaves keys behind does not pollute the store for an
        // hour, long enough that no case can outlive its own objects.
        params["object_ttl_s"] = "60";
        for (const auto& entry : extra) {
            params[entry.first] = entry.second;
        }
        return params;
    }

    //! A Redis channel whose connection a test can destroy the way a restore does.
    class BreakableRedis : public FMI::Comm::Redis {
    public:
        using FMI::Comm::Redis::Redis;

        //! Tear the connection down, leaving the channel holding a valid but dead descriptor.
        /*!
         * shutdown(), not close(): the channel owns the descriptor number and closes it itself
         * when it drops the context. Closing it from here would leave the channel holding a stale
         * number the kernel could hand straight back out to something else — the reasoning the
         * link-layer harness records at tests/transport_recovery.cpp:73-81, and the same trap.
         *
         * What remains is exactly what `criu restore --tcp-close` hands a restored rank: the
         * connection is gone, but the descriptor is valid, context->err is still 0 and nothing in
         * the process can tell until it writes.
         */
        void sever() {
            const int fd = connection_fd();
            if (fd >= 0) {
                ::shutdown(fd, SHUT_RDWR);
            }
        }

        bool connected() const { return connection_fd() >= 0; }
    };

    //! A channel that talks to a store where nothing ever appears.
    /*!
     * Both halves are overridden, so no command reaches Redis at all and every collective runs
     * its poll loop to exhaustion and throws. What it is for is what the counters look like
     * afterwards.
     */
    class StalledRedis : public FMI::Comm::Redis {
    public:
        using FMI::Comm::Redis::Redis;

        void upload_object(channel_data, std::string) override {}

        bool download_object(channel_data, std::string) override { return false; }

        unsigned int operations(const std::string& op) { return num_operations[op]; }
    };

    std::shared_ptr<BreakableRedis> make_breakable(const std::string& comm_name,
                                                   FMI::Utils::peer_num peer_id,
                                                   FMI::Utils::peer_num num_peers,
                                                   const std::map<std::string, std::string>& params) {
        auto channel = std::make_shared<BreakableRedis>(params, redis_test_model_params);
        channel->set_peer_id(peer_id);
        channel->set_num_peers(num_peers);
        channel->set_comm_name(comm_name);
        return channel;
    }

    //! Remove every object whose name starts with prefix.
    /*!
     * Recovered channels do not delete what they write — that is the point of the TTL — so the
     * cases that use the flag clean up after themselves instead of leaving a minute of debris in
     * a store other suites are also using.
     */
    void purge(FMI::Comm::ClientServer& channel, const std::string& prefix) {
        for (const auto& name : channel.get_object_names()) {
            if (name.rfind(prefix, 0) == 0) {
                channel.delete_object(name);
            }
        }
    }

    //! Count the objects whose name ends in suffix, the way ClientServer::barrier does.
    std::size_t count_with_suffix(const std::vector<std::string>& names, const std::string& suffix) {
        return static_cast<std::size_t>(std::count_if(names.begin(), names.end(),
                [&suffix] (const std::string& s) {
                    return s.size() > suffix.size() &&
                           s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
                }));
    }

}

//! A store that cannot be reached is reported by name, not by segfault.
/*!
 * Every command used to be issued on whatever context existed and its reply dereferenced without
 * a look: against a dead endpoint hiredis answers NULL and the process died on the spot, with no
 * clue as to which channel or which key was involved. Construction still only logs — a channel
 * whose store is down must be constructible, or a rank frozen inside its constructor could not be
 * restored — and the failure surfaces at the first command.
 */
BOOST_AUTO_TEST_CASE(dead_endpoint_is_reported_not_dereferenced) {
    std::map<std::string, std::string> params = redis_test_params;
    params["port"] = "1";   // nothing listens there, and connecting is refused immediately

    const std::string comm_name = unique_comm_name("dead");
    const std::string key = comm_name + "0_payload";

    std::shared_ptr<FMI::Comm::ClientServer> channel;
    BOOST_REQUIRE_NO_THROW(channel = make_redis(comm_name, 0, 1, params));

    int value = 42;
    BOOST_CHECK_EXCEPTION(channel->upload_object({reinterpret_cast<char*>(&value), sizeof(value)}, key),
                          std::runtime_error,
                          [&key] (const std::runtime_error& e) {
                              return std::string(e.what()).find(key) != std::string::npos;
                          });
}

//! finalize() deletes this channel's objects there and then, peers still reading or not.
/*!
 * That immediacy is the root of the staggered-shutdown deadlock documented at
 * tests/channels.cpp:836 — a rank that leaves first takes its objects with it. Pinned so a
 * change of deletion policy has to be a deliberate one.
 */
BOOST_AUTO_TEST_CASE(flag_off_finalize_deletes_immediately) {
    const std::string comm_name = unique_comm_name("finalize");
    const std::string key = comm_name + "0_payload";

    auto writer = make_redis(comm_name, 0, 2);
    auto reader = make_redis(comm_name, 1, 2);

    int value = 42;
    // upload(), not upload_object(): only upload() records the name, and only recorded names are
    // deleted by finalize().
    writer->upload({reinterpret_cast<char*>(&value), sizeof(value)}, key);

    int seen = 0;
    BOOST_REQUIRE(reader->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, key));
    BOOST_CHECK_EQUAL(seen, value);

    writer->finalize();

    seen = 0;
    BOOST_CHECK(!reader->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, key));

    // Belt and braces: if the check above ever fails, do not leave the key behind for the next run.
    reader->delete_object(key);
}

//! A stored value shorter than the receive buffer is reported as a complete download.
/*!
 * download_object copies min(buffer, stored) bytes and returns true, so the tail of the caller's
 * buffer keeps whatever was in it and nobody hears about the shortfall. Every ClientServer
 * collective sizes its buffer from its own expectation, so a size disagreement between two ranks
 * surfaces as silently wrong data rather than an error.
 */
BOOST_AUTO_TEST_CASE(flag_off_short_value_truncates) {
    const std::string comm_name = unique_comm_name("short");
    const std::string key = comm_name + "0_short";

    auto channel = make_redis(comm_name, 0, 1);

    char stored[2] = {'a', 'b'};
    channel->upload_object({stored, sizeof(stored)}, key);

    char buf[8];
    std::memset(buf, 0x5a, sizeof(buf));
    BOOST_CHECK(channel->download_object({buf, sizeof(buf)}, key));
    BOOST_CHECK_EQUAL(buf[0], 'a');
    BOOST_CHECK_EQUAL(buf[1], 'b');
    // Untouched tail: the copy was truncated to the stored length and the caller cannot tell.
    BOOST_CHECK_EQUAL(buf[sizeof(buf) - 1], static_cast<char>(0x5a));

    channel->delete_object(key);
}

//! barrier() counts every object in the store whose name ends in _barrier_<n>.
/*!
 * The suffix carries no communicator name, and get_object_names() lists the whole keyspace, so
 * markers left by another communicator — or by an earlier, unrelated run — are counted as
 * arrivals of this one. Here a barrier over N ranks is satisfied while only rank 0 of the
 * communicator has arrived.
 *
 * N is derived from the store's current contents rather than fixed at 2: this suite runs against
 * a shared development Redis that already holds barrier markers from other work, and a fixed N
 * would be satisfied by those alone — proving nothing. With N = stale + 3 the barrier can only
 * return if the two markers seeded under a foreign communicator name were counted.
 */
BOOST_AUTO_TEST_CASE(flag_off_barrier_counts_foreign_suffixes) {
    const std::string suffix = "_barrier_0";
    const std::string comm_name = unique_comm_name("barrier");
    const std::string foreign_comm_name = unique_comm_name("foreign");

    auto probe = make_redis(comm_name, 0, 1);
    const std::size_t stale = count_with_suffix(probe->get_object_names(), suffix);

    auto foreign = make_redis(foreign_comm_name, 0, 2);
    const std::string foreign_keys[] = {foreign_comm_name + "0" + suffix, foreign_comm_name + "1" + suffix};
    char marker = '1';
    for (const auto& foreign_key : foreign_keys) {
        foreign->upload_object({&marker, sizeof(marker)}, foreign_key);
    }

    // Short budget: a barrier that is NOT satisfied should fail this case quickly rather than
    // sit in its poll loop.
    std::map<std::string, std::string> params = redis_test_params;
    params["timeout"] = "5";
    params["max_timeout"] = "50";

    // stale markers + our own + the two foreign ones. One short of that and the barrier times out.
    auto channel = make_redis(comm_name, 0, static_cast<FMI::Utils::peer_num>(stale + 3), params);
    BOOST_CHECK_NO_THROW(channel->barrier());

    channel->delete_object(comm_name + "0" + suffix);
    for (const auto& foreign_key : foreign_keys) {
        foreign->delete_object(foreign_key);
    }
}

//! Asking for recovery means asking for a poll budget that can expire.
/*!
 * The ClientServer poll loops advance elapsed_time by timeout and sleep for timeout, so a
 * configured 0 neither waits nor gets any closer to max_timeout: the loop spins on the store at
 * full speed and never gives up. A channel that offers to survive a lost connection has to be able
 * to notice one, so that configuration is refused at construction — but only under the flag,
 * because it is a configuration the family accepts today.
 */
BOOST_AUTO_TEST_CASE(degenerate_recover_config_is_rejected) {
    const std::string comm_name = unique_comm_name("degenerate");

    std::map<std::string, std::string> spinning = redis_test_params;
    spinning["recover"] = "true";
    spinning["timeout"] = "0";
    BOOST_CHECK_EXCEPTION(make_redis(comm_name, 0, 1, spinning), std::runtime_error,
                          [] (const std::runtime_error& e) {
                              const std::string what = e.what();
                              return what.find("timeout") != std::string::npos &&
                                     what.find("0") != std::string::npos;
                          });

    // A budget shorter than one poll interval is the same complaint from the other side.
    std::map<std::string, std::string> upside_down = redis_test_params;
    upside_down["recover"] = "true";
    upside_down["timeout"] = "50";
    upside_down["max_timeout"] = "10";
    BOOST_CHECK_EXCEPTION(make_redis(comm_name, 0, 1, upside_down), std::runtime_error,
                          [] (const std::runtime_error& e) {
                              return std::string(e.what()).find("max_timeout") != std::string::npos;
                          });

    std::map<std::string, std::string> sound = redis_test_params;
    sound["recover"] = "true";
    BOOST_CHECK_NO_THROW(make_redis(comm_name, 0, 1, sound));

    // Same degenerate budget, flag absent: constructed exactly as before, no opinion offered.
    // Nothing is invoked on it — an operation with this budget is the infinite spin above.
    std::map<std::string, std::string> unflagged = redis_test_params;
    unflagged["timeout"] = "0";
    BOOST_CHECK_NO_THROW(make_redis(comm_name, 0, 1, unflagged));
}

//! Under recover, the communicator name and the rank number stop running together.
/*!
 * Object names are the communicator name with a rank number appended, so communicator "job" rank
 * 11 and communicator "job1" rank 1 both write "job11_2_0" — two unrelated jobs quietly reading
 * each other's messages whenever the names line up. Recovered jobs get a separator between the
 * two; flag-off names stay exactly what they were, which is what the second half pins.
 */
BOOST_AUTO_TEST_CASE(recover_keys_carry_a_separator) {
    const std::string comm_name = unique_comm_name("separator");

    // A plain channel to read raw keys with: download_object takes the name verbatim, so this one
    // can look for either spelling without being subject to the naming rule under test.
    auto probe = make_redis(comm_name, 0, 2);

    std::map<std::string, std::string> params = redis_test_params;
    params["recover"] = "true";
    auto recovering = make_redis(comm_name, 0, 2, params);

    int value = 42;
    recovering->send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);

    const std::string separated = comm_name + "|0_1_0";
    const std::string concatenated = comm_name + "0_1_0";

    int seen = 0;
    BOOST_CHECK(probe->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, separated));
    BOOST_CHECK_EQUAL(seen, value);
    BOOST_CHECK(!probe->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, concatenated));

    // And the same send from a channel without the flag lands under the old name.
    const std::string plain_comm_name = unique_comm_name("noseparator");
    auto plain = make_redis(plain_comm_name, 0, 2);
    plain->send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);

    const std::string plain_key = plain_comm_name + "0_1_0";
    seen = 0;
    BOOST_CHECK(probe->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, plain_key));
    BOOST_CHECK_EQUAL(seen, value);
    BOOST_CHECK(!probe->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)},
                                        plain_comm_name + "|0_1_0"));

    probe->delete_object(separated);
    probe->delete_object(plain_key);
}

//! A connection that dies between two commands is rebuilt, and the command that hit it is reissued.
/*!
 * This is the whole of what a restore needs from the client: the checkpoint captured a context
 * whose socket the restore then dropped, so the first command after it is issued on a connection
 * that no longer exists. Nothing can be pre-flighted — the descriptor is valid and context->err
 * is 0 — so that command has to fail, be recognised as a transport failure, and be reissued on a
 * fresh connection before the caller ever hears about it.
 */
BOOST_AUTO_TEST_CASE(dropped_connection_is_rebuilt_and_retried) {
    const std::string comm_name = unique_comm_name("dropped");
    const std::string key = comm_name + "payload";

    auto channel = make_breakable(comm_name, 0, 1, recover_params());
    int value = 42;
    channel->upload_object({reinterpret_cast<char*>(&value), sizeof(value)}, key);
    BOOST_REQUIRE(channel->connected());

    channel->sever();
    int seen = 0;
    BOOST_CHECK(channel->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, key));
    BOOST_CHECK_EQUAL(seen, value);

    // And the same for a write, which is the direction that also has to survive SIGPIPE.
    channel->sever();
    value = 43;
    BOOST_CHECK_NO_THROW(channel->upload_object({reinterpret_cast<char*>(&value), sizeof(value)}, key));

    channel->sever();
    seen = 0;
    BOOST_CHECK(channel->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, key));
    BOOST_CHECK_EQUAL(seen, 43);

    channel->delete_object(key);
}

//! The first write after a restore must not kill the process before the reconnect can happen.
/*!
 * hiredis writes without MSG_NOSIGNAL, and every socket a checkpointed process owned comes back
 * dropped (criu --tcp-close). With the default disposition that first write delivers SIGPIPE and
 * the rank dies on the spot — before the retry above can run, before it can say anything. All the
 * recovery machinery in this file is worth nothing if the process is not alive to run it.
 *
 * Sibling of tests/checkpoint_freeze_points.cpp:473, which pins the same property for the
 * DirectTCP peer registry; the two channels install the disposition independently, so each suite
 * asserts it for its own.
 */
BOOST_AUTO_TEST_CASE(restored_rank_survives_writing_to_a_dropped_connection) {
    const std::string comm_name = unique_comm_name("sigpipe");
    // Through the factory, because that is how an application gets one: constructing a channel at
    // all has to be what makes the write below survivable.
    auto factory_built = make_redis(comm_name, 0, 1);

    struct sigaction current {};
    BOOST_REQUIRE_EQUAL(::sigaction(SIGPIPE, nullptr, &current), 0);
    BOOST_CHECK_MESSAGE(current.sa_handler != SIG_DFL,
                        "a channel that owns a hiredis connection left SIGPIPE fatal");

    // And the disposition actually holds: writing to a closed peer returns EPIPE rather than
    // killing this process. If it does not, the test binary dies here and says so loudly.
    int fds[2];
    BOOST_REQUIRE_EQUAL(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
    ::close(fds[1]);
    const long n = ::send(fds[0], "x", 1, 0);
    BOOST_CHECK_MESSAGE(n < 0 && errno == EPIPE, "expected EPIPE, got " << n);
    ::close(fds[0]);

    // Now the real thing, on the channel's own connection: sever it and write.
    const std::string key = comm_name + "afterwards";
    auto channel = make_breakable(comm_name, 0, 1, recover_params());
    int value = 7;
    channel->upload_object({reinterpret_cast<char*>(&value), sizeof(value)}, key);
    channel->sever();
    value = 8;
    BOOST_CHECK_NO_THROW(channel->upload_object({reinterpret_cast<char*>(&value), sizeof(value)}, key));

    int seen = 0;
    BOOST_CHECK(channel->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, key));
    BOOST_CHECK_EQUAL(seen, 8);

    channel->delete_object(key);
    factory_built->finalize();
}

//! Losing the connection between the operations of a collective changes none of its results.
/*!
 * The single-channel cases above prove the client reconnects; this one proves the collectives do
 * not notice. Rank 2's connection is destroyed between every pair of operations, three rounds
 * running, while three other ranks are mid-job — the position a checkpoint of one rank puts the
 * job in, minus criu. Every rank checks its own results and the parent reports which one was
 * wrong.
 */
BOOST_AUTO_TEST_CASE(reconnect_survives_the_middle_of_a_collective) {
    constexpr int num_peers = 4;
    constexpr int rounds = 3;
    const std::string comm_name = unique_comm_name("collective");

    int* ok = static_cast<int*>(mmap(nullptr, sizeof(int) * num_peers, PROT_READ | PROT_WRITE,
                                     MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    BOOST_REQUIRE(ok != MAP_FAILED);
    for (int i = 0; i < num_peers; i++) {
        ok[i] = 0;
    }

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    // Four ranks polling a shared store are slower than one; the budget has to cover a loaded
    // machine, not just a quiet one.
    auto params = recover_params({{"max_timeout", "30000"}});

    bool all_good = true;
    try {
        BreakableRedis channel(params, redis_test_model_params);
        channel.set_peer_id(peer_id);
        channel.set_num_peers(num_peers);
        channel.set_comm_name(comm_name);

        auto add = [] (char* a, char* b) {
            *reinterpret_cast<int*>(a) = *reinterpret_cast<int*>(a) + *reinterpret_cast<int*>(b);
        };
        raw_function sum{add, true, true};
        auto cut = [&] { if (peer_id == 2) { channel.sever(); } };

        for (int round = 0; round < rounds; round++) {
            const int announced = 1000 * round + 7;
            int seen = peer_id == 0 ? announced : 0;
            channel.bcast({reinterpret_cast<char*>(&seen), sizeof(seen)}, 0);
            all_good = all_good && seen == announced;
            cut();

            int contribution = peer_id + 1;
            int total = 0;
            channel.reduce({reinterpret_cast<char*>(&contribution), sizeof(contribution)},
                           {reinterpret_cast<char*>(&total), sizeof(total)}, 0, sum);
            if (peer_id == 0) {
                all_good = all_good && total == num_peers * (num_peers + 1) / 2;
            }
            cut();

            int prefix = 0;
            channel.scan({reinterpret_cast<char*>(&contribution), sizeof(contribution)},
                         {reinterpret_cast<char*>(&prefix), sizeof(prefix)}, sum);
            all_good = all_good && prefix == (peer_id + 1) * (peer_id + 2) / 2;
            cut();

            channel.barrier();
            cut();
        }
    } catch (const std::exception& e) {
        all_good = false;
        BOOST_TEST_MESSAGE("rank " << peer_id << " failed: " << e.what());
    }
    ok[peer_id] = all_good ? 1 : 0;

    rank_guard.reap_ranks();
    for (int i = 0; i < num_peers; i++) {
        BOOST_TEST(ok[i] == 1, "rank " << i << " did not come through the cuts intact");
    }
    auto sweeper = make_redis(comm_name, 0, 1);
    purge(*sweeper, comm_name);
    munmap(ok, sizeof(int) * num_peers);
}

//! A store that refuses a write says so, and the write is not quietly dropped.
/*!
 * A full or read-only Redis answers SET with an error and stores nothing. Logging that and
 * returning — which is what happens with the flag off — loses the message: the rank that wrote it
 * carries on believing it was delivered, and the failure resurfaces on a different rank, seconds
 * later, as a Timeout with no cause attached. Waiting cannot fix a store that says no, so under
 * recover the refusal is raised where it happened.
 */
BOOST_AUTO_TEST_CASE(store_rejection_is_not_silently_lost) {
    constexpr int port = 6390;

    // fork + exec of a server, not of a rank: the child replaces itself with redis-server before
    // it can run a line of test code, so there is no forked-rank body here for ForkedRankGuard to
    // protect. If the exec fails the child leaves immediately by the same rule.
    const pid_t server = ::fork();
    BOOST_REQUIRE(server >= 0);
    if (server == 0) {
        // Its startup banner is not this suite's output.
        const int null_fd = ::open("/dev/null", O_WRONLY);
        if (null_fd >= 0) {
            ::dup2(null_fd, STDOUT_FILENO);
            ::dup2(null_fd, STDERR_FILENO);
        }
        ::execlp("redis-server", "redis-server", "--port", std::to_string(port).c_str(),
                 "--bind", "127.0.0.1", "--maxmemory", "1", "--maxmemory-policy", "noeviction",
                 "--save", "", "--appendonly", "no", static_cast<char*>(nullptr));
        std::_Exit(127);
    }

    // Wait for it to accept connections, or decide it is not there.
    bool up = false;
    for (int attempt = 0; attempt < 100 && !up; attempt++) {
        const int probe = ::socket(AF_INET, SOCK_STREAM, 0);
        BOOST_REQUIRE(probe >= 0);
        struct sockaddr_in addr {};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
        up = ::connect(probe, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0;
        ::close(probe);
        if (!up) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    if (!up) {
        ::kill(server, SIGKILL);
        ::waitpid(server, nullptr, 0);
        BOOST_TEST_MESSAGE("no usable redis-server binary: skipping the store-rejection case");
        return;
    }

    {
        const std::string comm_name = unique_comm_name("oom");
        // A short budget: an -OOM is a completed command and is raised at once, so anything that
        // does reach the retry loop is a different failure and should not sit in it.
        auto params = recover_params({{"port", std::to_string(port)}, {"max_timeout", "20"}});
        auto channel = make_breakable(comm_name, 0, 1, params);

        std::vector<char> payload(4096, 'x');
        BOOST_CHECK_EXCEPTION(channel->upload_object({payload.data(), payload.size()},
                                                     comm_name + "refused"),
                              std::runtime_error,
                              [] (const std::runtime_error& e) {
                                  return std::string(e.what()).find("OOM") != std::string::npos;
                              });
    }

    ::kill(server, SIGTERM);
    ::waitpid(server, nullptr, 0);
}

//! A communicator name is data, not a format string, and it survives being either.
/*!
 * Commands used to be built by concatenating the key into a printf format, so a '%' in a
 * communicator name made hiredis read arguments that were never passed. Names come from the
 * application and nothing forbids one.
 */
BOOST_AUTO_TEST_CASE(format_characters_in_comm_name_round_trip) {
    const std::string comm_name = unique_comm_name("fmt") + "%s %d_";

    auto params = recover_params();
    auto sender = make_redis(comm_name, 0, 2, params);
    auto receiver = make_redis(comm_name, 1, 2, params);

    int value = 4242;
    sender->send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);

    int seen = 0;
    BOOST_CHECK_NO_THROW(receiver->recv({reinterpret_cast<char*>(&seen), sizeof(seen)}, 0));
    BOOST_CHECK_EQUAL(seen, value);

    purge(*receiver, comm_name);
}

//! Two communicators whose names differ only by where the rank number starts stay apart.
/*!
 * With the flag off, communicator "job" rank 11 and communicator "job1" rank 1 both name their
 * first send to rank 2 "job11_2_0" and read each other's messages. The separator the recovery
 * layer inserts is what keeps the two names from meeting in the middle.
 */
BOOST_AUTO_TEST_CASE(ambiguous_concat_does_not_collide_under_recover) {
    const std::string base = unique_comm_name("ambiguous");
    const std::string wide = base + "job";       // rank 11 of 12
    const std::string narrow = base + "job1";    // rank 1 of 2

    auto params = recover_params();
    auto wide_rank = make_redis(wide, 11, 12, params);
    auto narrow_rank = make_redis(narrow, 1, 2, params);

    int from_wide = 11;
    int from_narrow = 1;
    wide_rank->send({reinterpret_cast<char*>(&from_wide), sizeof(from_wide)}, 2);
    narrow_rank->send({reinterpret_cast<char*>(&from_narrow), sizeof(from_narrow)}, 2);

    // Raw keys, read through a channel that names nothing itself.
    auto probe = make_redis(base, 0, 1);
    int seen = 0;
    BOOST_CHECK(probe->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, wide + "|11_2_0"));
    BOOST_CHECK_EQUAL(seen, from_wide);
    seen = 0;
    BOOST_CHECK(probe->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, narrow + "|1_2_0"));
    BOOST_CHECK_EQUAL(seen, from_narrow);
    // The name the two would have shared was never written.
    BOOST_CHECK(!probe->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, base + "job11_2_0"));

    purge(*probe, base);
}

//! An unreachable store is noticed in seconds, not in however long the kernel takes to give up.
/*!
 * A blackholed address answers nothing at all, and an unbounded connect sits through the whole
 * TCP SYN retry schedule — minutes — inside a single command, where no poll budget can see it.
 * The connect and I/O timeouts exist so that the failure lands back in the poll loop, which is the
 * only place that can decide what to do about it.
 */
BOOST_AUTO_TEST_CASE(unreachable_store_fails_within_the_io_timeout) {
    const std::string comm_name = unique_comm_name("blackhole");
    auto params = recover_params({{"host", "10.255.255.1"},
                                  {"connect_timeout_ms", "300"},
                                  {"io_timeout_ms", "300"}});

    const auto started = std::chrono::steady_clock::now();
    auto channel = make_redis(comm_name, 0, 1, params);
    int seen = 0;
    // False, not an exception: the callers are poll loops, and this is what "not yet" looks like
    // to them whatever the reason.
    BOOST_CHECK(!channel->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)},
                                          comm_name + "never"));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started).count();
    BOOST_CHECK_MESSAGE(elapsed < 3000, "an unreachable store took " << elapsed << " ms to be noticed");
}

//! However an operation ends, its counter has advanced exactly once.
/*!
 * The counters name the objects of the next operation of the same kind, so a rank that gave up and
 * a rank that did not must still agree on which generation comes next. reduce and scan used to
 * advance theirs after their poll loop, which the Timeout path never reaches — so a timed-out root
 * went on to reuse the name of the reduce it had just abandoned. Timeout remains terminal for the
 * communicator; this is about the counter being in one place rather than two.
 */
BOOST_AUTO_TEST_CASE(timeout_leaves_every_counter_advanced_once) {
    const std::string comm_name = unique_comm_name("counters");
    // Rank 1 of 3: not the root of the bcast, the root of the reduce, and in the middle of the
    // scan — so all three have something to wait for, and nothing will ever arrive.
    auto params = recover_params({{"max_timeout", "3"}});
    StalledRedis channel(params, redis_test_model_params);
    channel.set_peer_id(1);
    channel.set_num_peers(3);
    channel.set_comm_name(comm_name);

    auto add = [] (char* a, char* b) {
        *reinterpret_cast<int*>(a) = *reinterpret_cast<int*>(a) + *reinterpret_cast<int*>(b);
    };
    raw_function sum{add, true, true};
    int value = 1;
    int result = 0;

    BOOST_CHECK_THROW(channel.bcast({reinterpret_cast<char*>(&value), sizeof(value)}, 0),
                      FMI::Utils::Timeout);
    BOOST_CHECK_THROW(channel.reduce({reinterpret_cast<char*>(&value), sizeof(value)},
                                     {reinterpret_cast<char*>(&result), sizeof(result)}, 1, sum),
                      FMI::Utils::Timeout);
    BOOST_CHECK_THROW(channel.scan({reinterpret_cast<char*>(&value), sizeof(value)},
                                   {reinterpret_cast<char*>(&result), sizeof(result)}, sum),
                      FMI::Utils::Timeout);

    BOOST_CHECK_EQUAL(channel.operations("bcast"), 1u);
    BOOST_CHECK_EQUAL(channel.operations("reduce"), 1u);
    BOOST_CHECK_EQUAL(channel.operations("scan"), 1u);
}

BOOST_AUTO_TEST_SUITE_END();

#endif // FMI_ENABLE_REDIS
