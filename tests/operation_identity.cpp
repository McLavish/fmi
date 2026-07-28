#include <boost/test/unit_test.hpp>

#include "../include/fmi.h"
#include "../include/comm/OperationScope.h"

#include <memory>
#include <string>
#include <vector>

BOOST_AUTO_TEST_SUITE(OperationIdentity)

using namespace FMI::Comm;

namespace {
    std::string identity_config = "../../config/fmi_identity_test.json";

    //! A channel that performs no I/O and only records the identity in scope when it is called.
    /*!
     * Substituted for the real backend after construction, so these cases assert what the
     * Communicator publishes rather than what any particular transport does with it.
     */
    class RecordingChannel : public Channel {
    public:
        struct Observation {
            Lane lane;
            OpKind op_kind;
            std::uint64_t collective_index;
            std::uint32_t root;
            bool commutative;
            bool associative;
            bool valid;
        };

        std::vector<Observation> seen;

        void record() {
            const auto& id = current_operation();
            seen.push_back({id.lane, id.op_kind, id.collective_index, id.root,
                            id.commutative, id.associative, id.valid});
        }

        void send(channel_data, FMI::Utils::peer_num) override { record(); }
        void recv(channel_data, FMI::Utils::peer_num) override { record(); }
        void bcast(channel_data, FMI::Utils::peer_num) override { record(); }
        void barrier() override { record(); }
        void gather(channel_data, channel_data, FMI::Utils::peer_num) override { record(); }
        void scatter(channel_data, channel_data, FMI::Utils::peer_num) override { record(); }
        void reduce(channel_data, channel_data, FMI::Utils::peer_num, raw_function) override { record(); }
        void allreduce(channel_data, channel_data, raw_function) override { record(); }
        void scan(channel_data, channel_data, raw_function) override { record(); }

        double get_latency(FMI::Utils::peer_num, FMI::Utils::peer_num, std::size_t) override { return 0.; }
        double get_price(FMI::Utils::peer_num, FMI::Utils::peer_num, std::size_t) override { return 0.; }
        double get_operation_latency(FMI::Utils::OperationInfo) override { return 0.; }
        double get_operation_price(FMI::Utils::OperationInfo) override { return 0.; }
    };

    //! A Communicator whose only backend is a RecordingChannel.
    struct Harness {
        std::unique_ptr<FMI::Communicator> comm;
        std::shared_ptr<RecordingChannel> recorder = std::make_shared<RecordingChannel>();

        Harness(FMI::Utils::peer_num rank, FMI::Utils::peer_num peers, const std::string& name) {
            comm = std::make_unique<FMI::Communicator>(rank, peers, identity_config, name);
            // The config enables only Redis, so the policy always selects that name; swapping
            // the entry routes every operation into the recorder without any real I/O.
            comm->register_channel("Redis", recorder);
        }
    };

    std::string unique_name(const std::string& suffix) {
        return "identity-" + std::to_string(std::time(nullptr)) + "-" + suffix;
    }
}

BOOST_AUTO_TEST_CASE(every_operation_publishes_its_own_identity) {
    Harness h(0, 2, unique_name("all-ops"));
    FMI::Comm::Data<int> a = 1, b = 1;
    FMI::Comm::Data<std::vector<int>> small{{1, 2}};
    FMI::Comm::Data<std::vector<int>> wide(4);   // num_peers * small, for gather/scatter
    FMI::Utils::Function<int> sum([](int x, int y) { return x + y; }, true, true);

    h.comm->send(a, 1);
    h.comm->recv(b, 1);
    h.comm->bcast(a, 0);
    h.comm->barrier();
    h.comm->gather(small, wide, 0);
    h.comm->scatter(wide, small, 0);
    h.comm->reduce(a, b, 0, sum);
    h.comm->allreduce(a, b, sum);
    h.comm->scan(a, b, sum);

    BOOST_REQUIRE_EQUAL(h.recorder->seen.size(), 9u);
    const auto& s = h.recorder->seen;

    // Point-to-point: P2P lane, no collective index, root is the DESTINATION in both cases.
    BOOST_CHECK(s[0].lane == Lane::P2P);
    BOOST_CHECK(s[0].op_kind == OpKind::Send);
    BOOST_CHECK_EQUAL(s[0].collective_index, 0u);
    BOOST_CHECK_EQUAL(s[0].root, 1u);          // send to rank 1
    BOOST_CHECK(s[1].lane == Lane::P2P);
    BOOST_CHECK_EQUAL(s[1].root, 0u);          // recv by rank 0 => rank 0 is the destination

    // Collectives: Collective lane, one index each, in issue order.
    BOOST_CHECK(s[2].lane == Lane::Collective);
    BOOST_CHECK(s[2].op_kind == OpKind::Bcast);
    BOOST_CHECK_EQUAL(s[2].collective_index, 0u);
    BOOST_CHECK(s[3].op_kind == OpKind::Barrier);
    BOOST_CHECK_EQUAL(s[3].collective_index, 1u);
    BOOST_CHECK(s[4].op_kind == OpKind::Gather);
    BOOST_CHECK_EQUAL(s[4].collective_index, 2u);
    BOOST_CHECK(s[5].op_kind == OpKind::Scatter);
    BOOST_CHECK_EQUAL(s[5].collective_index, 3u);
    BOOST_CHECK(s[6].op_kind == OpKind::Reduce);
    BOOST_CHECK_EQUAL(s[6].collective_index, 4u);
    BOOST_CHECK(s[7].op_kind == OpKind::Allreduce);
    BOOST_CHECK_EQUAL(s[7].collective_index, 5u);
    BOOST_CHECK(s[8].op_kind == OpKind::Scan);
    BOOST_CHECK_EQUAL(s[8].collective_index, 6u);

    for (const auto& o : s) {
        BOOST_CHECK(o.valid);
    }
}

BOOST_AUTO_TEST_CASE(the_reduction_flags_reach_the_channel) {
    Harness h(0, 2, unique_name("flags"));
    FMI::Comm::Data<int> a = 1, b = 1;
    FMI::Utils::Function<int> commutative([](int x, int y) { return x + y; }, true, true);
    FMI::Utils::Function<int> ordered([](int x, int y) { return x - y; }, false, false);

    h.comm->reduce(a, b, 0, commutative);
    h.comm->reduce(a, b, 0, ordered);

    BOOST_REQUIRE_EQUAL(h.recorder->seen.size(), 2u);
    BOOST_CHECK(h.recorder->seen[0].commutative && h.recorder->seen[0].associative);
    BOOST_CHECK(!h.recorder->seen[1].commutative && !h.recorder->seen[1].associative);
    // Same op_kind, same root, same index — only the flags separate them, and they must,
    // because the flags select entirely different algorithms.
    BOOST_CHECK(h.recorder->seen[0].op_kind == h.recorder->seen[1].op_kind);
}

BOOST_AUTO_TEST_CASE(identical_programs_agree_on_every_collective_index) {
    Harness r0(0, 2, unique_name("agree-0"));
    Harness r1(1, 2, unique_name("agree-1"));
    FMI::Comm::Data<int> a = 1;

    for (auto* h : {&r0, &r1}) {
        h->comm->bcast(a, 0);
        h->comm->barrier();
        h->comm->bcast(a, 0);
    }

    BOOST_REQUIRE_EQUAL(r0.recorder->seen.size(), 3u);
    BOOST_REQUIRE_EQUAL(r1.recorder->seen.size(), 3u);
    for (std::size_t i = 0; i < 3; ++i) {
        BOOST_CHECK_EQUAL(r0.recorder->seen[i].collective_index,
                          r1.recorder->seen[i].collective_index);
        BOOST_CHECK(r0.recorder->seen[i].op_kind == r1.recorder->seen[i].op_kind);
    }
}

BOOST_AUTO_TEST_CASE(divergent_collective_orders_produce_different_identities) {
    // This is the detection the whole design turns on. Two ranks issue their collectives in
    // different orders; at the same ordinal they are in different logical operations, and the
    // identities must say so. Under the old protocol both were simply "the next 1-byte frame".
    Harness r0(0, 2, unique_name("diverge-0"));
    Harness r1(1, 2, unique_name("diverge-1"));
    FMI::Comm::Data<int> a = 1;

    r0.comm->bcast(a, 0);
    r0.comm->barrier();

    r1.comm->barrier();
    r1.comm->bcast(a, 0);

    BOOST_REQUIRE_EQUAL(r0.recorder->seen.size(), 2u);
    BOOST_REQUIRE_EQUAL(r1.recorder->seen.size(), 2u);

    // Same collective_index on both ranks at ordinal 0 ...
    BOOST_CHECK_EQUAL(r0.recorder->seen[0].collective_index,
                      r1.recorder->seen[0].collective_index);
    // ... but a different operation, which is precisely what op_kind exposes. A bare
    // collective counter would compare equal here and substitute silently.
    BOOST_CHECK(r0.recorder->seen[0].op_kind != r1.recorder->seen[0].op_kind);
    BOOST_CHECK(r0.recorder->seen[1].op_kind != r1.recorder->seen[1].op_kind);
}

BOOST_AUTO_TEST_CASE(no_identity_is_in_scope_outside_an_operation) {
    // Nothing should leak between operations: a channel consulted outside any FMI call must
    // see an invalid identity rather than a stale one.
    BOOST_CHECK(!active_operation().valid);
    {
        Harness h(0, 2, unique_name("scope"));
        FMI::Comm::Data<int> a = 1;
        h.comm->bcast(a, 0);
    }
    BOOST_CHECK(!active_operation().valid);
}

BOOST_AUTO_TEST_SUITE_END()
