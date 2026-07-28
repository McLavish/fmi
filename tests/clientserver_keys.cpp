#include <boost/test/unit_test.hpp>

#include "../include/comm/Channel.h"
#include "../include/comm/ClientServer.h"
#include "../include/ft/Common.h"

#include <algorithm>
#include <chrono>
#include <map>
#include <string>
#include <unistd.h>
#include <vector>

BOOST_AUTO_TEST_SUITE(ClientServerKeys)

#if FMI_ENABLE_REDIS

namespace {
    std::map<std::string, std::string> cs_params = {
            {"host",        "127.0.0.1"},
            {"port",        "6379"},
            {"timeout",     "1"},
            {"max_timeout", "30000"}
    };

    std::map<std::string, std::string> cs_model_params = {
            {"bandwidth_single",   "100.0"},
            {"bandwidth_multiple", "400.0"},
            {"overhead",           "5.2"},
            {"transfer_price",     "0.0"},
            {"instance_price",     "0.0038"},
            {"requests_per_hour",  "1000"}
    };
}

// An epoch change must not rewind the per-operation counters the data-plane keys are built from.
//
// Communicator::reconfigure_to_epoch used to finalize a ClientServer channel and build a fresh
// one, because reconfigure_for_epoch defaulted to false. The replacement started with
// num_operations back at zero, so the first send after a migration rebuilt the very key the first
// send before it had used. With the object still present, that is a silent substitution: the
// receiver takes whichever payload is under the key.
//
// The keys are also no longer epoch-qualified, so the check is a genuine one -- an epoch suffix
// would make every key distinct for a reason that has nothing to do with the counters.
BOOST_AUTO_TEST_CASE(survives_epoch_reconfigure) {
    const std::string base = "cskeys-" + std::to_string(getpid()) + "-" +
                             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());

    auto ch = FMI::Comm::Channel::get_channel("Redis", cs_params, cs_model_params);
    ch->set_peer_id(0);
    ch->set_num_peers(2);
    ch->set_comm_name(base);
    ch->set_data_comm_name(base);

    int payload = 1;
    ch->send({reinterpret_cast<char*>(&payload), sizeof(payload)}, 1);
    payload = 2;
    ch->send({reinterpret_cast<char*>(&payload), sizeof(payload)}, 1);

    const bool kept = ch->reconfigure_for_epoch(FMI::FT::epoch_comm_name(base, 1), {1});
    BOOST_CHECK_MESSAGE(kept, "ClientServer must reconfigure in place, not be rebuilt");

    payload = 3;
    ch->send({reinterpret_cast<char*>(&payload), sizeof(payload)}, 1);

    auto* cs = dynamic_cast<FMI::Comm::ClientServer*>(ch.get());
    BOOST_REQUIRE(cs != nullptr);
    auto names = cs->get_object_names();
    std::vector<std::string> mine;
    std::copy_if(names.begin(), names.end(), std::back_inserter(mine),
                 [&base](const std::string& n) { return n.rfind(base, 0) == 0; });

    std::sort(mine.begin(), mine.end());
    const auto duplicate = std::adjacent_find(mine.begin(), mine.end());
    BOOST_CHECK_MESSAGE(duplicate == mine.end(),
                        "two sends share a key after reconfigure: " +
                        (duplicate == mine.end() ? std::string() : *duplicate));
    BOOST_CHECK_EQUAL(mine.size(), 3u);

    ch->finalize();

    // finalize() only drops what this channel created; make sure nothing under our prefix leaks,
    // because Redis::get_object_names issues an unscoped KEYS * that every other case sees.
    auto leftover = cs->get_object_names();
    const auto still_ours = std::count_if(leftover.begin(), leftover.end(),
                                          [&base](const std::string& n) { return n.rfind(base, 0) == 0; });
    BOOST_CHECK_EQUAL(still_ours, 0);
}

// Why keeping the channel object matters, stated as an executable fact: a ClientServer built
// from scratch reuses the keys of the one it replaces.
//
// This is the substitution path itself. reconfigure_to_epoch used to finalize the channel and
// build a replacement, and because the key grammar carries only (comm_name, src, dest, counter),
// the replacement's first send addresses exactly the key the original's first send used. Nothing
// about the epoch appears in it, so the message is not merely misrouted -- it is indistinguishable
// from the earlier one, which is what makes the substitution silent.
BOOST_AUTO_TEST_CASE(a_rebuilt_channel_reuses_keys) {
    const std::string base = "cskeys-rebuild-" + std::to_string(getpid()) + "-" +
                             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());

    auto names_after_one_send = [&](int payload) {
        auto ch = FMI::Comm::Channel::get_channel("Redis", cs_params, cs_model_params);
        ch->set_peer_id(0);
        ch->set_num_peers(2);
        ch->set_comm_name(base);
        ch->set_data_comm_name(base);
        ch->send({reinterpret_cast<char*>(&payload), sizeof(payload)}, 1);

        auto* cs = dynamic_cast<FMI::Comm::ClientServer*>(ch.get());
        BOOST_REQUIRE(cs != nullptr);
        auto all = cs->get_object_names();
        std::vector<std::string> mine;
        std::copy_if(all.begin(), all.end(), std::back_inserter(mine),
                     [&base](const std::string& n) { return n.rfind(base, 0) == 0; });
        return mine;
    };

    const auto first = names_after_one_send(1);
    BOOST_REQUIRE_EQUAL(first.size(), 1u);

    // A second, independent channel over the same communicator -- what the rebuild produced.
    const auto second = names_after_one_send(2);
    BOOST_REQUIRE_EQUAL(second.size(), 1u);

    BOOST_CHECK_EQUAL(first.front(), second.front());

    auto ch = FMI::Comm::Channel::get_channel("Redis", cs_params, cs_model_params);
    ch->set_peer_id(0);
    ch->set_num_peers(2);
    ch->set_comm_name(base);
    ch->set_data_comm_name(base);
    auto* cs = dynamic_cast<FMI::Comm::ClientServer*>(ch.get());
    BOOST_REQUIRE(cs != nullptr);
    for (const auto& n : cs->get_object_names()) {
        if (n.rfind(base, 0) == 0) {
            cs->delete_object(n);
        }
    }
}

#endif

BOOST_AUTO_TEST_SUITE_END()
