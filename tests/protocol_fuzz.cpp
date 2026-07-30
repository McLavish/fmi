#include <boost/test/unit_test.hpp>

#include "forked_rank_guard.h"

#include "../include/fmi.h"

#include <sys/mman.h>
#include <sys/wait.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <unistd.h>
#include <vector>

//! Randomized differential testing of the framed transport.
/*!
 * Hand-written cases only cover what their author thought of. This generates random VALID
 * programs from a seed and runs each one twice — framed and unframed — requiring identical
 * observations. Because every rank derives the program from the same seed, all ranks issue the
 * same collectives in the same order with the same sizes, which is the well-formedness rule
 * the protocol assumes; the generator therefore explores only programs a correct
 * implementation must accept.
 *
 * Seed count is overridable with FMI_FUZZ_SEEDS for longer soak runs.
 */
BOOST_AUTO_TEST_SUITE(ProtocolFuzz)

#if FMI_ENABLE_REDIS

namespace {
    const std::string framed_config   = "../../config/fmi_framed_test.json";
    const std::string unframed_config = "../../config/fmi_direct_tcp.json";

    constexpr int max_values = 512;
    struct RankResult {
        int ok;
        int count;
        long long values[max_values];
    };

    RankResult* shared_results(int n) {
        return static_cast<RankResult*>(mmap(nullptr, n * sizeof(RankResult),
                                             PROT_READ | PROT_WRITE,
                                             MAP_SHARED | MAP_ANONYMOUS, -1, 0));
    }

    void push(RankResult& r, long long v) {
        if (r.count < max_values) { r.values[r.count++] = v; }
    }

    enum class Op { Barrier, Bcast, Reduce, Allreduce, AllreduceOrdered, Scan, Gather, Scatter, P2P };

    struct Step {
        bool operator==(const Step& o) const {
            return op == o.op && root == o.root && elements == o.elements && src == o.src && dst == o.dst;
        }
        Op op;
        int root;
        int elements;   //!< per-rank element count, identical on every rank
        int src, dst;   //!< for P2P
    };

    //! Derive a program from a seed. Every rank runs this and gets the same answer, which is
    //! what makes the generated program well-formed.
    std::vector<Step> make_program(unsigned seed, int num_peers, int length) {
        std::mt19937 rng(seed);
        std::vector<Step> steps;
        for (int i = 0; i < length; i++) {
            Step s{};
            s.op = static_cast<Op>(rng() % 9);
            s.root = static_cast<int>(rng() % num_peers);
            s.elements = 1 + static_cast<int>(rng() % 4);
            s.src = static_cast<int>(rng() % num_peers);
            s.dst = static_cast<int>(rng() % num_peers);
            if (s.dst == s.src) { s.dst = (s.src + 1) % num_peers; }
            steps.push_back(s);
        }
        return steps;
    }

    void execute(FMI::Communicator& comm, int rank, int num_peers,
                 const std::vector<Step>& steps, RankResult& r) {
        FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);
        FMI::Utils::Function<int> ordered([](int a, int b) { return a - b; }, false, false);

        for (const Step& s : steps) {
            switch (s.op) {
                case Op::Barrier: {
                    comm.barrier();
                    push(r, 1);
                    break;
                }
                case Op::Bcast: {
                    std::vector<int> v(s.elements, rank == s.root ? s.root + 50 : 0);
                    FMI::Comm::Data<std::vector<int>> d(v);
                    comm.bcast(d, s.root);
                    for (int x : d.get()) { push(r, x); }
                    break;
                }
                case Op::Reduce: {
                    FMI::Comm::Data<int> in = rank + 1, out = 0;
                    comm.reduce(in, out, s.root, sum);
                    if (rank == s.root) { push(r, out.get()); }
                    break;
                }
                case Op::Allreduce: {
                    FMI::Comm::Data<int> in = rank + 1, out = 0;
                    comm.allreduce(in, out, sum);
                    push(r, out.get());
                    break;
                }
                case Op::AllreduceOrdered: {
                    FMI::Comm::Data<int> in = rank + 1, out = 0;
                    comm.allreduce(in, out, ordered);
                    push(r, out.get());
                    break;
                }
                case Op::Scan: {
                    FMI::Comm::Data<int> in = rank + 1, out = 0;
                    comm.scan(in, out, sum);
                    push(r, out.get());
                    break;
                }
                case Op::Gather: {
                    std::vector<int> mine(s.elements, rank + 1);
                    FMI::Comm::Data<std::vector<int>> send(mine);
                    FMI::Comm::Data<std::vector<int>> all(s.elements * num_peers);
                    comm.gather(send, all, s.root);
                    if (rank == s.root) {
                        for (int x : all.get()) { push(r, x); }
                    }
                    break;
                }
                case Op::Scatter: {
                    std::vector<int> whole(s.elements * num_peers);
                    for (std::size_t i = 0; i < whole.size(); i++) {
                        whole[i] = static_cast<int>(i) + 1;
                    }
                    FMI::Comm::Data<std::vector<int>> send(whole);
                    FMI::Comm::Data<std::vector<int>> slice(s.elements);
                    comm.scatter(send, slice, s.root);
                    for (int x : slice.get()) { push(r, x); }
                    break;
                }
                case Op::P2P: {
                    FMI::Comm::Data<int> payload = s.src * 100 + s.dst, got = 0;
                    if (rank == s.src) {
                        comm.send(payload, s.dst);
                    } else if (rank == s.dst) {
                        comm.recv(got, s.src);
                        push(r, got.get());
                    }
                    break;
                }
            }
        }
    }

    void run(unsigned seed, int num_peers, const std::vector<Step>& steps,
             const std::string& config, const std::string& name, RankResult* out) {
        for (int i = 0; i < num_peers; i++) { out[i].ok = 0; out[i].count = 0; }

        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        rank_guard.fork_ranks(num_peers);
        try {
            FMI::Communicator comm(peer_id, num_peers, config, name);
            execute(comm, peer_id, num_peers, steps, out[peer_id]);
            out[peer_id].ok = 1;
        } catch (const std::exception& e) {
            std::fprintf(stderr, "[seed %u rank %d] %s\n", seed, peer_id, e.what());
            out[peer_id].ok = 0;
        }
        rank_guard.reap_ranks();
    }

    std::string comm_for(unsigned seed, const char* tag) {
        return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
               "_" + std::to_string(getpid()) + "_fz" + std::to_string(seed) + tag;
    }
}

BOOST_AUTO_TEST_CASE(random_valid_programs_are_identical_framed_and_unframed) {
    const char* env = std::getenv("FMI_FUZZ_SEEDS");
    const int seeds = env != nullptr ? std::atoi(env) : 12;
    constexpr int num_peers = 3;
    constexpr int length = 6;

    RankResult* framed   = shared_results(num_peers);
    RankResult* unframed = shared_results(num_peers);

    BOOST_TEST_MESSAGE("fuzz seeds=" << seeds);
    for (int s = 0; s < seeds; s++) {
        const unsigned seed = 1000u + static_cast<unsigned>(s);
        const auto steps = make_program(seed, num_peers, length);

        BOOST_TEST_MESSAGE("seed " << seed << " steps=" << steps.size());
        run(seed, num_peers, steps, unframed_config, comm_for(seed, "raw"), unframed);
        run(seed, num_peers, steps, framed_config,   comm_for(seed, "fr"),  framed);

        for (int i = 0; i < num_peers; i++) {
            BOOST_CHECK_MESSAGE(unframed[i].ok == 1, "seed " << seed << ": unframed rank " << i << " failed");
            BOOST_CHECK_MESSAGE(framed[i].ok == 1,   "seed " << seed << ": framed rank " << i << " failed");
            BOOST_CHECK_MESSAGE(framed[i].count == unframed[i].count,
                                "seed " << seed << ": rank " << i << " value count "
                                        << framed[i].count << " framed vs " << unframed[i].count);
            const int n = std::min(framed[i].count, unframed[i].count);
            for (int k = 0; k < n; k++) {
                BOOST_CHECK_MESSAGE(framed[i].values[k] == unframed[i].values[k],
                                    "seed " << seed << ": rank " << i << " value " << k << " = "
                                            << framed[i].values[k] << " framed vs "
                                            << unframed[i].values[k] << " unframed");
            }
        }
    }
}

// ------------------------------------------------------- adversarial (divergent) programs

namespace {
    const std::string framed_fast   = "../../config/fmi_framed_fast_test.json";
    const std::string unframed_fast = "../../config/fmi_raw_fast_test.json";

    //! Make one rank's program ill-formed by swapping two adjacent collectives of different
    //! kinds. Every rank must issue collectives in the same order, so this is exactly the
    //! divergence the envelope exists to catch, and it is guaranteed to mismatch on the wire.
    std::vector<Step> diverge(std::vector<Step> steps, unsigned seed) {
        std::mt19937 rng(seed ^ 0xD1E5u);
        for (std::size_t i = 0; i + 1 < steps.size(); i++) {
            const std::size_t j = i + 1;
            if (steps[i].op != steps[j].op &&
                steps[i].op != Op::P2P && steps[j].op != Op::P2P) {
                std::swap(steps[i], steps[j]);
                return steps;
            }
        }
        // No adjacent differing pair: force one by rewriting a step.
        if (!steps.empty()) {
            steps[rng() % steps.size()].op = Op::Barrier;
        }
        return steps;
    }

    //! Outcome of running an ill-formed program.
    struct Outcome {
        int completed_without_error = 0;   //!< ranks that returned success
        int identity_mismatch = 0;         //!< ranks that reported a mismatch loudly
        int other_error = 0;               //!< timeouts and the like
    };

    Outcome run_divergent(int num_peers, const std::vector<Step>& base,
                          const std::vector<Step>& skewed, const std::string& config,
                          const std::string& name) {
        struct Slot { int ok; int mismatch; int other; };
        Slot* slots = static_cast<Slot*>(mmap(nullptr, num_peers * sizeof(Slot),
                                              PROT_READ | PROT_WRITE,
                                              MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        for (int i = 0; i < num_peers; i++) { slots[i] = {0, 0, 0}; }

        ForkedRankGuard rank_guard;
        int& peer_id = rank_guard.peer_id;
        rank_guard.fork_ranks(num_peers);
        try {
            FMI::Communicator comm(peer_id, num_peers, config, name);
            RankResult sink{};
            // Rank 1 alone runs the skewed program.
            execute(comm, peer_id, num_peers, peer_id == 1 ? skewed : base, sink);
            slots[peer_id].ok = 1;
        } catch (const std::exception& e) {
            const std::string what = e.what();
            if (what.find("identity mismatch") != std::string::npos) {
                slots[peer_id].mismatch = 1;
            } else {
                slots[peer_id].other = 1;
            }
        }
        rank_guard.reap_ranks();

        Outcome o;
        for (int i = 0; i < num_peers; i++) {
            o.completed_without_error += slots[i].ok;
            o.identity_mismatch += slots[i].mismatch;
            o.other_error += slots[i].other;
        }
        return o;
    }
}

BOOST_AUTO_TEST_CASE(divergent_programs_never_complete_silently_when_framed) {
    // The protocol's central safety claim, fuzzed: an ill-formed program must not run to
    // completion as though it were valid. It may abort loudly or stall, but it may never
    // report success while ranks were in different logical operations.
    const char* env = std::getenv("FMI_FUZZ_SEEDS");
    const int seeds = env != nullptr ? std::atoi(env) : 8;
    constexpr int num_peers = 2;
    constexpr int length = 5;

    int framed_all_ok = 0, framed_caught = 0, unframed_all_ok = 0;

    for (int s = 0; s < seeds; s++) {
        const unsigned seed = 7000u + static_cast<unsigned>(s);
        const auto base = make_program(seed, num_peers, length);
        const auto skewed = diverge(base, seed);
        if (base == skewed) { continue; }

        const Outcome fr = run_divergent(num_peers, base, skewed, framed_fast,
                                         comm_for(seed, "dvfr"));
        const Outcome raw = run_divergent(num_peers, base, skewed, unframed_fast,
                                          comm_for(seed, "dvraw"));

        if (fr.completed_without_error == num_peers) { framed_all_ok++; }
        if (fr.identity_mismatch > 0) { framed_caught++; }
        if (raw.completed_without_error == num_peers) { unframed_all_ok++; }

        BOOST_CHECK_MESSAGE(fr.completed_without_error != num_peers,
                            "seed " << seed << ": framed run completed an ILL-FORMED program "
                                    << "with every rank reporting success");
    }

    BOOST_TEST_MESSAGE("framed: " << framed_caught << " runs caught a mismatch explicitly, "
                       << framed_all_ok << " completed silently");
    BOOST_TEST_MESSAGE("unframed: " << unframed_all_ok << " completed silently");
    // Framing must be strictly better than no framing at detecting this.
    BOOST_CHECK_MESSAGE(framed_all_ok <= unframed_all_ok,
                        "framing detected FEWER divergent programs than the raw transport");
}

#endif // FMI_ENABLE_REDIS

BOOST_AUTO_TEST_SUITE_END()
