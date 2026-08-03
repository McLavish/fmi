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

#include <boost/log/core.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <new>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <iostream>
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

    //! What the store says is left of a key's life: seconds, -1 for "no expiry", -2 for "gone".
    /*!
     * Read on a connection of the test's own, because the property under test is what the channel
     * left in the store, not what the channel believes it left there.
     */
    long key_ttl(const std::string& key) {
        redisContext* context = redisConnect("127.0.0.1", 6379);
        if (context == nullptr || context->err) {
            if (context != nullptr) {
                redisFree(context);
            }
            return -3;
        }
        const char* argv[] = {"TTL", key.c_str()};
        const std::size_t argvlen[] = {sizeof("TTL") - 1, key.size()};
        auto* reply = static_cast<redisReply*>(redisCommandArgv(context, 2, argv, argvlen));
        long ttl = -3;
        if (reply != nullptr && reply->type == REDIS_REPLY_INTEGER) {
            ttl = static_cast<long>(reply->integer);
        }
        freeReplyObject(reply);
        redisFree(context);
        return ttl;
    }

    //! A port nothing was listening on at the moment the kernel was asked, or 0.
    int free_port() {
        const int probe = ::socket(AF_INET, SOCK_STREAM, 0);
        if (probe < 0) {
            return 0;
        }
        struct sockaddr_in addr {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
        addr.sin_port = 0;
        int port = 0;
        socklen_t len = sizeof(addr);
        if (::bind(probe, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0 &&
            ::getsockname(probe, reinterpret_cast<struct sockaddr*>(&addr), &len) == 0) {
            port = ntohs(addr.sin_port);
        }
        ::close(probe);
        return port;
    }

    //! A redis-server of this case's own, on a port of its own, killed however the case leaves.
    /*!
     * Some properties can only be shown against a store that answers badly — one that is out of
     * memory, or that refuses every command — which the development Redis this suite otherwise
     * uses must not be turned into.
     *
     * fork + exec of a server, not of a rank: the child replaces itself with redis-server before it
     * can run a line of test code, so there is no forked-rank body for ForkedRankGuard to protect
     * (nothing here can fork again, which is what that guard is for). What it does need is an
     * owner. A case that leaves by an unexpected exception — a BOOST_REQUIRE, a throw from a
     * constructor — unwinds past any kill() written after it and leaves a server running for the
     * life of the machine; a leaked one on a fixed port then makes the next run's readiness probe
     * succeed against a server started with none of the settings the case depends on, and the
     * failure that follows names the wrong thing. So the pid is owned by an object and the port is
     * asked for rather than assumed.
     */
    class EphemeralRedisServer {
    public:
        explicit EphemeralRedisServer(const std::vector<std::string>& extra_args) {
            server_port = free_port();
            if (server_port == 0) {
                return;
            }
            std::vector<std::string> args = {"redis-server", "--port", std::to_string(server_port),
                                             "--bind", "127.0.0.1", "--save", "",
                                             "--appendonly", "no"};
            args.insert(args.end(), extra_args.begin(), extra_args.end());
            std::vector<char*> argv;
            argv.reserve(args.size() + 1);
            for (auto& arg : args) {
                argv.push_back(const_cast<char*>(arg.c_str()));
            }
            argv.push_back(nullptr);

            pid = ::fork();
            if (pid < 0) {
                return;
            }
            if (pid == 0) {
                // Its startup banner is not this suite's output.
                const int null_fd = ::open("/dev/null", O_WRONLY);
                if (null_fd >= 0) {
                    ::dup2(null_fd, STDOUT_FILENO);
                    ::dup2(null_fd, STDERR_FILENO);
                }
                ::execvp("redis-server", argv.data());
                std::_Exit(127);
            }
            ready = wait_until_accepting();
            if (!ready) {
                stop();
            }
        }

        EphemeralRedisServer(const EphemeralRedisServer&) = delete;
        EphemeralRedisServer& operator=(const EphemeralRedisServer&) = delete;

        ~EphemeralRedisServer() { stop(); }

        //! Whether there is a server to talk to; false means no usable redis-server binary.
        bool up() const { return ready; }

        std::string port() const { return std::to_string(server_port); }

    private:
        bool wait_until_accepting() {
            for (int attempt = 0; attempt < 100; attempt++) {
                const int probe = ::socket(AF_INET, SOCK_STREAM, 0);
                if (probe < 0) {
                    return false;
                }
                struct sockaddr_in addr {};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(static_cast<uint16_t>(server_port));
                addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
                const bool accepted =
                        ::connect(probe, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0;
                ::close(probe);
                if (accepted) {
                    return true;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            return false;
        }

        void stop() {
            if (pid > 0) {
                // SIGKILL: it was started with no persistence, so there is nothing for it to write
                // out, and a shutdown that can be ignored is one this destructor could hang on.
                ::kill(pid, SIGKILL);
                ::waitpid(pid, nullptr, 0);
            }
            pid = -1;
            ready = false;
        }

        pid_t pid = -1;
        int server_port = 0;
        bool ready = false;
    };

    //! Everything the library logs while this object is alive, counted in lines.
    /*!
     * At the descriptor, not at std::cout: with no sink configured BOOST_LOG_TRIVIAL goes through
     * the core's default sink, which writes to standard output without going through the stream
     * object — lending std::cout (or std::clog) another buffer captures nothing. Standard output
     * is redirected to a file of our own for the duration instead, which is true however the sink
     * gets there.
     *
     * What the case using it needs is the count, not the text: the property under test is how often
     * a poll loop repeats itself, and a thousand-line answer and a one-line answer differ by
     * nothing else.
     */
    class CapturedLog {
    public:
        CapturedLog() {
            flush_all();
            saved_stdout = ::dup(STDOUT_FILENO);
            char path[] = "/tmp/fmi_clientserver_log_XXXXXX";
            sink_fd = ::mkstemp(path);
            if (sink_fd >= 0) {
                ::unlink(path);   // it exists only as this descriptor
                ::dup2(sink_fd, STDOUT_FILENO);
            }
        }

        CapturedLog(const CapturedLog&) = delete;
        CapturedLog& operator=(const CapturedLog&) = delete;

        ~CapturedLog() { restore(); }

        std::size_t lines() {
            flush_all();
            if (sink_fd < 0) {
                return 0;
            }
            ::lseek(sink_fd, 0, SEEK_SET);
            char buf[4096];
            std::size_t count = 0;
            ssize_t n;
            while ((n = ::read(sink_fd, buf, sizeof(buf))) > 0) {
                count += static_cast<std::size_t>(std::count(buf, buf + n, '\n'));
            }
            return count;
        }

    private:
        static void flush_all() {
            boost::log::core::get()->flush();
            std::cout.flush();
            std::fflush(stdout);
        }

        void restore() {
            flush_all();
            if (saved_stdout >= 0) {
                ::dup2(saved_stdout, STDOUT_FILENO);
                ::close(saved_stdout);
                saved_stdout = -1;
            }
            if (sink_fd >= 0) {
                ::close(sink_fd);
                sink_fd = -1;
            }
        }

        int saved_stdout = -1;
        int sink_fd = -1;
    };

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
    EphemeralRedisServer server({"--maxmemory", "1", "--maxmemory-policy", "noeviction"});
    if (!server.up()) {
        BOOST_TEST_MESSAGE("no usable redis-server binary: skipping the store-rejection case");
        return;
    }

    const std::string comm_name = unique_comm_name("oom");
    // A short budget: an -OOM is a completed command and is raised at once, so anything that does
    // reach the retry loop is a different failure and should not sit in it.
    auto params = recover_params({{"port", server.port()}, {"max_timeout", "20"}});
    auto channel = make_breakable(comm_name, 0, 1, params);

    std::vector<char> payload(4096, 'x');
    BOOST_CHECK_EXCEPTION(channel->upload_object({payload.data(), payload.size()},
                                                 comm_name + "refused"),
                          std::runtime_error,
                          [] (const std::runtime_error& e) {
                              return std::string(e.what()).find("OOM") != std::string::npos;
                          });
}

//! A store that answers every read with an error is reported once, not once per poll.
/*!
 * The errors a GET can draw are states rather than events — -LOADING while a restarted Redis reads
 * its dump back, -MISCONF when background saves are failing, -READONLY after a failover, -NOAUTH
 * after a credential rotation — and each lasts as long as its condition does. Each is also a
 * completed command, which is what makes it different from an outage: the connection is fine, so
 * every poll draws another one, and a poll is a millisecond.
 *
 * A rank in that state wrote one console line per poll — 2000 of them from a single download()
 * against a password-protected store — each taking Boost.Log's console sink lock and doing blocking
 * I/O. Under the 60 s budget the sweep configuration asks for, that is a rank spending its failure
 * describing it, on every rank at once, onto the disk of the machine running the sweep.
 */
BOOST_AUTO_TEST_CASE(a_store_that_answers_with_errors_is_reported_once) {
    EphemeralRedisServer server({"--requirepass", "secret"});
    if (!server.up()) {
        BOOST_TEST_MESSAGE("no usable redis-server binary: skipping the error-flood case");
        return;
    }

    const std::string comm_name = unique_comm_name("noauth");
    // No password on the channel, so every command it sends is answered -NOAUTH: a store that is
    // reachable, answering, and of no use whatsoever.
    auto params = recover_params({{"port", server.port()}, {"max_timeout", "500"}});
    auto channel = make_redis(comm_name, 0, 1, params);

    std::size_t lines = 0;
    {
        CapturedLog log;
        int seen = 0;
        BOOST_CHECK_THROW(channel->download({reinterpret_cast<char*>(&seen), sizeof(seen)},
                                            comm_name + "unreadable"), FMI::Utils::Timeout);
        lines = log.lines();
    }
    // At least one, because a store that cannot be read must say so — and because a count of zero
    // would mean this case is measuring nothing rather than that nothing was written.
    BOOST_CHECK_MESSAGE(lines >= 1 && lines <= 3,
                        "expected the refusal to be reported once, got " << lines
                        << " log lines from a 500-poll download");
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

//! And the poll budget above that failure is spent in milliseconds, not in dials.
/*!
 * The budget is the failure detector under recover, which only means anything if a pass through
 * the poll loop costs about what the loop charges for it. A dial against an address that answers
 * nothing costs the whole connect timeout while the loop charges one `timeout` — 1 ms in every
 * shipped config — so a channel that dialled on every pass turned a 20 ms budget into twelve
 * seconds, and the 5000 ms of config/fmi_identity_test.json into the better part of an hour.
 *
 * The bound below is deliberately loose: what it has to catch is the ratio, not the constant. One
 * dial's worth of overrun is unavoidable (an unreachable store cannot be recognised faster than a
 * connect attempt takes to fail), six hundred is the bug.
 */
BOOST_AUTO_TEST_CASE(unreachable_store_gives_up_within_its_poll_budget) {
    const std::string comm_name = unique_comm_name("blackhole_budget");
    auto params = recover_params({{"host", "10.255.255.1"},
                                  {"max_timeout", "20"},
                                  {"connect_timeout_ms", "300"},
                                  {"io_timeout_ms", "300"}});

    auto channel = make_redis(comm_name, 0, 1, params);

    // download(), not download_object(): the poll loop is what carries the budget.
    const auto download_started = std::chrono::steady_clock::now();
    int seen = 0;
    BOOST_CHECK_THROW(channel->download({reinterpret_cast<char*>(&seen), sizeof(seen)},
                                        comm_name + "never"), FMI::Utils::Timeout);
    const auto download_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - download_started).count();
    BOOST_CHECK_MESSAGE(download_ms < 2000,
                        "a 20 ms download budget took " << download_ms << " ms against a blackholed store");

    // The write side carries the same budget, and had the same problem.
    const auto upload_started = std::chrono::steady_clock::now();
    int value = 7;
    BOOST_CHECK_THROW(channel->upload_object({reinterpret_cast<char*>(&value), sizeof(value)},
                                             comm_name + "unwritable"), std::runtime_error);
    const auto upload_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - upload_started).count();
    BOOST_CHECK_MESSAGE(upload_ms < 2000,
                        "a 20 ms upload budget took " << upload_ms << " ms against a blackholed store");
}

//! However an operation ends, its counter has advanced exactly once.
/*!
 * The counters name the objects of the next operation of the same kind, so a rank that gave up and
 * a rank that did not must still agree on which generation comes next. scan advanced its own after
 * the Timeout check, which the giving-up path never reaches, so a timed-out rank went on to rewrite
 * the generation it had just abandoned; reduce's sat between the poll loop and that check, where
 * any exception out of a download stepped over it instead. Both are now where barrier's always was,
 * and this case holds all three of them to the same rule rather than to the route each took to
 * break it. Timeout remains terminal for the communicator; this is about the counter being in one
 * place rather than two.
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

//! Ranks that finish at different moments do not take each other's messages with them.
/*!
 * This is the case tests/channels.cpp:836-840 documents and works around: it uses a shared-memory
 * rendezvous before finalize because, without one, the rank that finishes first deletes the
 * objects the ranks behind it are still polling for, and the run deadlocks about seven times in
 * ten. A scan is the sharpest form of it — rank 0 folds only its own value and is done
 * immediately, while rank 3 still needs what ranks 0 to 2 wrote.
 *
 * Here there is no rendezvous at all: every rank tears down the instant it has its own answer,
 * which is what a real application does and what a rank that is killed or checkpointed and never
 * restored does involuntarily. Under recover that is safe, because finalize deletes nothing.
 */
BOOST_AUTO_TEST_CASE(staggered_finalize_under_recover_loses_nothing) {
    constexpr int num_peers = 4;
    const std::string comm_name = unique_comm_name("staggered");

    int* res = static_cast<int*>(mmap(nullptr, sizeof(int) * num_peers, PROT_READ | PROT_WRITE,
                                      MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    BOOST_REQUIRE(res != MAP_FAILED);
    int* done = static_cast<int*>(mmap(nullptr, sizeof(int) * num_peers, PROT_READ | PROT_WRITE,
                                       MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    BOOST_REQUIRE(done != MAP_FAILED);
    for (int i = 0; i < num_peers; i++) {
        res[i] = 0;
        done[i] = 0;
    }

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    // Subtraction: neither commutative nor associative, so the fold order is pinned as well as the
    // delivery. Values from 1 up, because 0 is subtraction's identity and would hide a wrong order.
    auto subtract = [] (char* a, char* b) {
        *reinterpret_cast<int*>(a) = *reinterpret_cast<int*>(a) - *reinterpret_cast<int*>(b);
    };
    auto params = recover_params({{"max_timeout", "30000"}});

    try {
        auto channel = make_redis(comm_name, peer_id, num_peers, params);
        int value = peer_id + 1;
        channel->scan({reinterpret_cast<char*>(&value), sizeof(int)},
                      {reinterpret_cast<char*>(res + peer_id), sizeof(int)}, {subtract, false, false});
        // No rendezvous of any kind between the scan and the teardown: this rank leaves the moment
        // it is finished with the collective, peers or no peers.
        channel->finalize();
        done[peer_id] = 1;
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("rank " << peer_id << " failed: " << e.what());
    }

    rank_guard.reap_ranks();
    int expected = 1;                       // v0
    for (int i = 0; i < num_peers; i++) {
        BOOST_TEST(done[i] == 1, "rank " << i << " did not finish its scan");
        BOOST_CHECK_EQUAL(expected, res[i]);
        expected = expected - (i + 2);      // ... - v(i+1)
    }
    auto sweeper = make_redis(comm_name, 0, 1);
    purge(*sweeper, comm_name);
    munmap(res, sizeof(int) * num_peers);
    munmap(done, sizeof(int) * num_peers);
}

//! Nothing leaves finalize, whatever the state of the job or the store.
/*!
 * finalize runs from ~Communicator, which is noexcept: an exception that escapes it is not a
 * failed shutdown, it is std::terminate. Two ways in. A peer that leaves mid-job — killed,
 * evicted, checkpointed and never restored — must not turn the survivor's teardown into an abort;
 * and a backend whose own cleanup fails must be contained rather than propagated.
 *
 * The teardown below is marked noexcept on purpose, so that a propagation aborts this binary
 * exactly as it would abort a rank, loudly and at the right line, instead of being counted as a
 * test failure and moved on from.
 */
BOOST_AUTO_TEST_CASE(finalize_never_propagates) {
    constexpr int num_peers = 2;
    const std::string comm_name = unique_comm_name("orphan");

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    auto params = recover_params();
    if (peer_id != 0) {
        // The peer that never comes back: it builds its channel, joins the job, and vanishes
        // without finalizing anything.
        auto abandoned = make_redis(comm_name, peer_id, num_peers, params);
        int value = 1;
        abandoned->send({reinterpret_cast<char*>(&value), sizeof(value)}, 0);
        rank_guard.reap_ranks();
        return;
    }

    auto survivor = make_redis(comm_name, 0, num_peers, params);
    int value = 2;
    survivor->send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);
    auto teardown = [&] () noexcept {
        survivor->finalize();
        survivor.reset();
    };
    teardown();
    rank_guard.reap_ranks();

    // And the same guarantee for a backend whose cleanup itself fails: with the flag off the base
    // class deletes object by object, and one refusal from the store must not become an abort
    // either.
    struct ExplodingRedis : public FMI::Comm::Redis {
        using FMI::Comm::Redis::Redis;
        void delete_object(std::string) override { throw std::runtime_error("store said no"); }
    };
    ExplodingRedis flag_off(redis_test_params, redis_test_model_params);
    flag_off.set_peer_id(0);
    flag_off.set_num_peers(1);
    flag_off.set_comm_name(comm_name);
    int leftover = 3;
    flag_off.upload({reinterpret_cast<char*>(&leftover), sizeof(leftover)}, comm_name + "leftover");
    auto flag_off_teardown = [&] () noexcept { flag_off.finalize(); };
    flag_off_teardown();

    auto sweeper = make_redis(comm_name, 0, 1);
    purge(*sweeper, comm_name);
}

//! Under recover the store owns the cleanup: every object gets an expiry and finalize keeps its hands off.
/*!
 * The two halves are one decision. Nothing is deleted on the way out because a departing rank
 * cannot know what its peers still need, so something else has to reclaim the objects, and the
 * only thing that still works when a rank never reaches finalize at all is the store's own expiry.
 */
BOOST_AUTO_TEST_CASE(uploads_carry_ttls_and_finalize_deletes_nothing) {
    const std::string comm_name = unique_comm_name("ttl");
    auto channel = make_redis(comm_name, 0, 2, recover_params());

    int value = 42;
    channel->send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);
    const std::string key = comm_name + "|0_1_0";

    const long ttl = key_ttl(key);
    BOOST_CHECK_MESSAGE(ttl > 0 && ttl <= 60, "expected an expiry of at most 60 s, TTL says " << ttl);

    channel->finalize();
    int seen = 0;
    BOOST_CHECK_MESSAGE(channel->download_object({reinterpret_cast<char*>(&seen), sizeof(seen)}, key),
                        "finalize deleted an object a peer might still be waiting for");
    BOOST_CHECK_EQUAL(seen, value);

    // With the flag off the object has no expiry at all — the deletion in finalize is the whole
    // cleanup story, which is why it has to happen there.
    const std::string plain_comm_name = unique_comm_name("nottl");
    auto plain = make_redis(plain_comm_name, 0, 2);
    plain->send({reinterpret_cast<char*>(&value), sizeof(value)}, 1);
    BOOST_CHECK_EQUAL(key_ttl(plain_comm_name + "0_1_0"), -1);
    plain->finalize();
    BOOST_CHECK_EQUAL(key_ttl(plain_comm_name + "0_1_0"), -2);

    channel->delete_object(key);
}

//! Another communicator's markers are not arrivals, however their names end.
/*!
 * The inverse of flag_off_barrier_counts_foreign_suffixes above, which pins the same store state
 * satisfying a barrier today: the suffix carries no communicator name and the whole keyspace is
 * counted, so somebody else's markers finish this rank's wait. Under recover the barrier asks for
 * the names its own ranks will write, so the only thing that can end it is its own peer arriving —
 * and when the peer does not, it times out.
 */
BOOST_AUTO_TEST_CASE(foreign_markers_do_not_satisfy_a_recovered_barrier) {
    const std::string suffix = "_barrier_0";
    const std::string comm_name = unique_comm_name("ownbarrier");
    const std::string foreign_comm_name = unique_comm_name("foreignbarrier");

    // Markers of a communicator that has nothing to do with this one, spelled the way a recovered
    // job spells them, so that only the name — not the shape — distinguishes them.
    auto foreign = make_redis(foreign_comm_name, 0, 2);
    char marker = '1';
    const std::string foreign_keys[] = {foreign_comm_name + "|0" + suffix,
                                        foreign_comm_name + "|1" + suffix};
    for (const auto& foreign_key : foreign_keys) {
        foreign->upload_object({&marker, sizeof(marker)}, foreign_key);
    }

    // Rank 0 of 2, and rank 1 never comes. A short budget: this barrier is meant to fail, and the
    // case should not sit in it.
    auto params = recover_params({{"timeout", "5"}, {"max_timeout", "50"}});
    auto channel = make_redis(comm_name, 0, 2, params);
    BOOST_CHECK_THROW(channel->barrier(), FMI::Utils::Timeout);

    for (const auto& foreign_key : foreign_keys) {
        foreign->delete_object(foreign_key);
    }
    purge(*foreign, comm_name);
}

//! And a barrier whose ranks do arrive completes, generation after generation.
/*!
 * The markers of a recovered job are never deleted, so every generation has to be told apart by
 * its number alone: a second barrier must wait for the second generation's markers rather than
 * finding the first generation's still in the store and returning at once.
 */
BOOST_AUTO_TEST_CASE(recovered_barrier_completes) {
    constexpr int num_peers = 2;
    const std::string comm_name = unique_comm_name("barrierpass");

    constexpr int generations = 2;
    int* ok = static_cast<int*>(mmap(nullptr, sizeof(int) * num_peers, PROT_READ | PROT_WRITE,
                                     MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    BOOST_REQUIRE(ok != MAP_FAILED);
    // Set by the late rank immediately before it enters each barrier, so the early rank can say
    // whether it was actually held there.
    auto* entered = static_cast<std::atomic<int>*>(mmap(nullptr, sizeof(std::atomic<int>) * generations,
                                                        PROT_READ | PROT_WRITE,
                                                        MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    BOOST_REQUIRE(entered != MAP_FAILED);
    for (int i = 0; i < num_peers; i++) {
        ok[i] = 0;
    }
    for (int g = 0; g < generations; g++) {
        new (entered + g) std::atomic<int>(0);
    }

    ForkedRankGuard rank_guard;
    int& peer_id = rank_guard.peer_id;
    rank_guard.fork_ranks(num_peers);

    auto params = recover_params({{"max_timeout", "30000"}});
    try {
        auto channel = make_redis(comm_name, peer_id, num_peers, params);
        bool held = true;
        for (int generation = 0; generation < generations; generation++) {
            if (peer_id == 1) {
                // Arrive late, and announce it only on the way in: a barrier that lets rank 0 out
                // before this line has not waited for anything.
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                entered[generation].store(1, std::memory_order_release);
            }
            channel->barrier();
            if (peer_id == 0) {
                held = held && entered[generation].load(std::memory_order_acquire) == 1;
            }
        }
        ok[peer_id] = held ? 1 : 0;
    } catch (const std::exception& e) {
        BOOST_TEST_MESSAGE("rank " << peer_id << " failed: " << e.what());
    }

    rank_guard.reap_ranks();
    for (int i = 0; i < num_peers; i++) {
        BOOST_TEST(ok[i] == 1, "rank " << i << " did not come through both barriers");
    }
    auto sweeper = make_redis(comm_name, 0, 1);
    purge(*sweeper, comm_name);
    munmap(ok, sizeof(int) * num_peers);
    munmap(entered, sizeof(std::atomic<int>) * generations);
}


BOOST_AUTO_TEST_SUITE_END();

#endif // FMI_ENABLE_REDIS
