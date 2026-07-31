// App shape: deep_rounds
//
// Depth rather than width: a very large number of very short operations, and repeated *identical*
// operations whose answers must not be swapped for one another.
//
// Two things it covers that the other shapes do not:
//
//   * Operation boundaries, in bulk. Every operation here is a handful of bytes, so a step costs
//     tens of microseconds and a run crosses tens of thousands of boundaries. The freeze the sweep
//     applies at a uniformly random instant is therefore overwhelmingly likely to land either
//     exactly between two operations or inside a very short one -- the case a workload built from
//     a few long messages almost never reaches.
//   * Repeated identical operations. Each step issues three bcasts from one root back to back,
//     then two allreduces with the same function and the same size, then two ring messages to the
//     same neighbour. The calls are indistinguishable by kind, root, size and function; only their
//     position in the sequence tells them apart. Each carries a different payload and is checked
//     against the payload *that call* was supposed to produce, so an answer delivered to the wrong
//     one of a repeated pair -- a duplicate, a reorder, a frame left over from the call before --
//     fails the run rather than looking plausible.
//
// A round is a batch of `steps_per_round` such steps; the round exists only so that the driver's
// progress line and --ms sleep stay per-round as in every other shape, and so that a default
// --rounds still buys thousands of operations.
//
// Deterministic by construction: every value is a function of (rank, num_peers, step index), and
// the checksum folds received data only, through an order-sensitive mix -- so a lost, duplicated,
// reordered or substituted message changes it. `payload_ints` is deliberately ignored: small
// messages are the entire point of this shape, and shape_baseline already covers bulk payloads.

#include "../shapes.h"

#include <chrono>
#include <cstdio>
#include <thread>

namespace FMI::Runbooks::Checkpoint {
namespace {

//! Steps in one round. Sized so that a default --rounds 60 is still ~7.7k steps (~77k operations)
//! and lasts several seconds -- long enough for the sweep's 0.25-1.2 s delay plus a dump and a
//! restore to land while the job is still running -- while 8 ranks stay under ten seconds.
constexpr int steps_per_round = 128;

//! Identical bcasts issued back to back per step, and identical allreduces / ring messages.
constexpr int repeated_bcasts = 3;
constexpr int repeated_allreduces = 2;
constexpr int repeated_ring_messages = 2;

//! Bounds every derived value inside int at any round count, while staying distinct across any
//! 65536 consecutive steps -- far more than a run ever spans between two repeats of a payload.
int step_tag(long long step) {
    return static_cast<int>(step & 0xFFFF);
}

//! Payload of copy `k` of the repeated bcast issued in `step`. Distinct per copy, so the three
//! calls cannot be confused with one another.
int bcast_token(long long step, int k) {
    return step_tag(step) * 8 + k * 3 + 1;
}

//! Rank `rank`'s contribution to copy `k` of the repeated allreduce issued in `step`.
int allreduce_contribution(FMI::Utils::peer_num rank, long long step, int k) {
    return static_cast<int>(rank) + step_tag(step) + k * 5;
}

//! What copy `k` of the repeated allreduce of `step` must produce on every rank.
int allreduce_expected(FMI::Utils::peer_num num_peers, long long step, int k) {
    const int n = static_cast<int>(num_peers);
    return n * (n - 1) / 2 + n * (step_tag(step) + k * 5);
}

//! Payload `sender` puts on the wire as copy `k` of the repeated ring message of `step`.
int ring_payload(FMI::Utils::peer_num sender, long long step, int k) {
    return static_cast<int>(sender) * 4 + step_tag(step) * 2 + k;
}

//! Order-sensitive fold: unlike a running sum it changes when two received values swap places, so
//! a reordering that preserves the multiset of messages still fails. Unsigned overflow wraps, and
//! wrapping is what makes it a fixed-width hash rather than a growing number.
void mix(unsigned long long& checksum, int value) {
    checksum = checksum * 1099511628211ull
             + static_cast<unsigned long long>(static_cast<unsigned int>(value));
}

unsigned long long run_deep_rounds(FMI::Communicator& comm, FMI::Utils::peer_num rank,
                                   FMI::Utils::peer_num num_peers, const ShapeParams& params) {
    if (num_peers < 2) {
        std::printf("rank %u: deep_rounds needs at least 2 peers\n", rank);
        throw ShapeFailure{9};
    }

    FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);

    const auto next = (rank + 1) % num_peers;
    const auto prev = (rank + num_peers - 1) % num_peers;

    unsigned long long checksum = 0;

    for (int round = 0; round < params.rounds; round++) {
        for (int s = 0; s < steps_per_round; s++) {
            const long long step = static_cast<long long>(round) * steps_per_round + s;
            // Rotate the root, so a run walks every tree the rank count admits instead of
            // re-running one. Nothing here assumes a power of two: with 3, 5 or 7 ranks the
            // binomial tree is lopsided and a different root gives it a different shape.
            const auto root = static_cast<FMI::Utils::peer_num>(step % num_peers);

            comm.barrier();

            // --- repeated bcasts, same root, same size, distinct payloads -------------------
            for (int k = 0; k < repeated_bcasts; k++) {
                const int want = bcast_token(step, k);
                // Non-roots start from a sentinel, so a bcast that delivered nothing at all is
                // caught here rather than folded in as a plausible value.
                FMI::Comm::Data<int> token(rank == root ? want : -1);
                comm.bcast(token, root);
                if (token.get() != want) {
                    std::printf("rank %u: BCAST MISMATCH step %lld copy %d root %u: "
                                "got %d want %d\n", rank, step, k, root, token.get(), want);
                    throw ShapeFailure{3};
                }
                mix(checksum, token.get());
            }

            // --- repeated allreduces, same function, same size, distinct contributions ------
            for (int k = 0; k < repeated_allreduces; k++) {
                FMI::Comm::Data<int> mine(allreduce_contribution(rank, step, k));
                FMI::Comm::Data<int> total(0);
                comm.allreduce(mine, total, sum);
                const int want = allreduce_expected(num_peers, step, k);
                if (total.get() != want) {
                    std::printf("rank %u: ALLREDUCE MISMATCH step %lld copy %d: got %d want %d\n",
                                rank, step, k, total.get(), want);
                    throw ShapeFailure{4};
                }
                mix(checksum, total.get());
            }

            // --- repeated ring messages, same neighbour, distinct payloads ------------------
            // Blocking sends, so fix an order: even ranks send first, odd ranks receive first.
            // With an odd rank count rank 0 and rank num_peers-1 are both "even" in that sense,
            // which is fine -- every sender still has a receiver ready for it.
            for (int k = 0; k < repeated_ring_messages; k++) {
                FMI::Comm::Data<int> out(ring_payload(rank, step, k));
                FMI::Comm::Data<int> in(-1);
                if (rank % 2 == 0) {
                    comm.send(out, next);
                    comm.recv(in, prev);
                } else {
                    comm.recv(in, prev);
                    comm.send(out, next);
                }
                const int want = ring_payload(prev, step, k);
                if (in.get() != want) {
                    std::printf("rank %u: RING MISMATCH step %lld copy %d from %u: "
                                "got %d want %d\n", rank, step, k, prev, in.get(), want);
                    throw ShapeFailure{5};
                }
                mix(checksum, in.get());
            }
        }

        if (params.print_every > 0 && round % params.print_every == 0) {
            std::printf("rank %u: round %d ok checksum=%llu\n", rank, round, checksum);
        }
        if (params.ms_per_round > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(params.ms_per_round));
        }
    }

    return checksum;
}

FMI_REGISTER_SHAPE("deep_rounds",
                   "hundreds of very short steps per round, each repeating one bcast, one "
                   "allreduce and one ring message several times over with distinct payloads, so "
                   "boundaries are dense and identical operations must not be confused",
                   run_deep_rounds)

} // namespace
} // namespace FMI::Runbooks::Checkpoint
