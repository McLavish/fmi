#include <boost/test/unit_test.hpp>

// The whole suite talks to a live Redis as a ClientServer data plane; without the backend
// compiled in there is nothing here to test.
#if FMI_ENABLE_REDIS

#include "../include/comm/Channel.h"
#include "../include/comm/ClientServer.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
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

BOOST_AUTO_TEST_SUITE_END();

#endif // FMI_ENABLE_REDIS
