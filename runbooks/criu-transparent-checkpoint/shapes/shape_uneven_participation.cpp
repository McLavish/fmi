// App shape: uneven_participation
//
// Ranks that do wildly different amounts of work between collectives, so they arrive at each
// collective at very different times. What that buys over `baseline`, where every rank does the
// same thing and reaches every operation together:
//
//   * a collective spends most of its time waiting for one late rank, so a checkpoint taken at a
//     random instant usually lands with some ranks already blocked inside an operation and
//     others still computing outside one — not with everybody in the same place;
//   * the skew rotates twice per round (a rank is in tier T before the allreduce and in the
//     complementary tier before the gather), so the arrival ORDER changes within a single round
//     and no link settles into a steady rhythm;
//   * every STRAGGLER_EVERY rounds one rank stops for STRAGGLER_PAUSE_MS. Its peers sit in a
//     collective the whole time, which leaves directed links idle for the better part of a
//     second — long enough to look dead to anything that judges a link by its traffic.
//
// The skew has two parts. The local loop really is different work per rank (up to ~20x the
// iterations), and its result is what the rank contributes to the allreduce; the pause next to it
// is what makes the skew the same size on a fast host and a slow one. `work_value` is the closed
// form of that loop, so every rank can predict every peer's contribution in constant time while
// the peer really spent O(n) producing it — the allreduce is checked, not just summed.
//
// Determinism: every value sent, and every value expected, is a function of (rank, round, index,
// num_peers) alone. Nothing folded into the checksum comes from the clock, from how long a pause
// actually took, or from how many times the rank was checkpointed. The fold is a polynomial hash
// rather than a sum, so a reordered pair of received values changes the checksum too.
//
// Note on rounds: a round here costs tens of milliseconds by design, so this shape wants rounds
// in the tens or low hundreds -- not the 40000 the `baseline` shape's near-free rounds want. At
// the sweep's default of 60 rounds a run takes a few seconds, which is comfortably longer than
// the sweep's checkpoint delay and short enough to sweep quickly.

#include "../shapes.h"

#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

namespace FMI::Runbooks::Checkpoint {
namespace {

//! How many distinct amounts of work a rank can be given. Fixed rather than derived from
//! num_peers, so the wall-clock cost of a round does not grow with the rank count.
constexpr int SKEW_TIERS = 4;
//! Pause per tier before the round's first collective: 0, 12, 24, 36 ms.
constexpr int TIER_PAUSE_MS = 12;
//! Pause per complementary tier before the round's second collective: 18, 12, 6, 0 ms.
constexpr int TAIL_PAUSE_MS = 6;
//! One rank stops dead this often (in rounds) ...
constexpr int STRAGGLER_EVERY = 20;
//! ... for this long, which is what leaves a link with no traffic long enough to look dead.
constexpr int STRAGGLER_PAUSE_MS = 700;
//! Iterations of the local loop that every rank runs, plus one tier's worth on top.
constexpr long long WORK_BASE_ITERS = 150000;
constexpr long long WORK_TIER_ITERS = 600000;
//! Keeps the loop's accumulator small; the closed form below agrees with it because the
//! accumulator only ever adds and multiplies, and both distribute over the modulus.
constexpr long long WORK_MOD = 100003;

//! Which tier of work `rank` is given in `round`.
/*!
 * 3 is coprime with SKEW_TIERS, so consecutive ranks land in different tiers (with four or more
 * peers, all four tiers are occupied in every round), and adding the round makes each rank take
 * every tier in turn.
 */
int tier_of(FMI::Utils::peer_num rank, int round) {
    return static_cast<int>((rank * 3u + static_cast<unsigned>(round)) % SKEW_TIERS);
}

//! Whether `rank` is the one that stops dead in `round`. At most one rank per round, and the
//! choice walks the whole communicator, so every rank takes a turn at any peer count.
bool is_straggler(FMI::Utils::peer_num rank, int round, FMI::Utils::peer_num num_peers) {
    if (round % STRAGGLER_EVERY != STRAGGLER_EVERY - 1) {
        return false;
    }
    return rank == static_cast<FMI::Utils::peer_num>(round / STRAGGLER_EVERY) % num_peers;
}

//! Iterations of the local loop `rank` runs in `round`.
long long work_iters(FMI::Utils::peer_num rank, int round) {
    return WORK_BASE_ITERS + static_cast<long long>(tier_of(rank, round)) * WORK_TIER_ITERS;
}

//! The local loop: `n` iterations of real integer work, no clock and no allocation.
int run_local_work(long long n) {
    long long acc = 0;
    for (long long i = 0; i < n; i++) {
        acc = (acc + 3 * i + 1) % WORK_MOD;
    }
    return static_cast<int>(acc);
}

//! What run_local_work(n) returns, in constant time: sum of (3i + 1) for i < n, modulo WORK_MOD.
//! n stays in the low millions here, so the triangular number is nowhere near overflowing.
int work_value(long long n) {
    const long long triangular = n * (n - 1) / 2;
    return static_cast<int>((3 * triangular + n) % WORK_MOD);
}

//! Element `index` of the ring message `sender` sends in `round`.
int ring_element(FMI::Utils::peer_num sender, int round, int index) {
    return static_cast<int>(sender) * 100000 + round * 13 + index;
}

//! The value `rank` contributes to `round`'s gather.
int gather_element(FMI::Utils::peer_num rank, int round) {
    return static_cast<int>(rank) * 10 + round;
}

//! The token broadcast in `round`.
int bcast_token(FMI::Utils::peer_num root, int round) {
    return static_cast<int>(root) * 1000 + round * 7 + 1;
}

void pause_ms(int ms) {
    if (ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }
}

//! Folds one received value into the running checksum. A polynomial hash, not a sum: swapping
//! two received values changes the result, so the checksum catches reordering as well as loss,
//! duplication and substitution.
void mix(unsigned long long& checksum, long long value) {
    checksum = checksum * 1000003ULL + static_cast<unsigned long long>(value);
}

unsigned long long run_uneven_participation(FMI::Communicator& comm, FMI::Utils::peer_num rank,
                                            FMI::Utils::peer_num num_peers,
                                            const ShapeParams& params) {
    if (num_peers < 2) {
        std::printf("rank %u: uneven_participation needs at least 2 peers\n", rank);
        throw ShapeFailure{9};
    }
    const int width = params.payload_ints > 0 ? params.payload_ints : 1;

    FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);

    unsigned long long checksum = 0;

    for (int round = 0; round < params.rounds; round++) {
        const int tier = tier_of(rank, round);

        // --- skew, part one ----------------------------------------------------------
        // Different iteration counts per rank, then a pause of the same size on any host.
        const int my_work = run_local_work(work_iters(rank, round));
        pause_ms(tier * TIER_PAUSE_MS);
        if (is_straggler(rank, round, num_peers)) {
            pause_ms(STRAGGLER_PAUSE_MS);
        }

        // --- allreduce: every rank waits here for the slowest one --------------------
        // Contributed value is what the loop actually computed; the expected total is the
        // closed form for every rank, so a rank that skipped or repeated work is caught.
        FMI::Comm::Data<int> mine(my_work);
        FMI::Comm::Data<int> total(0);
        comm.allreduce(mine, total, sum);
        int want_total = 0;
        for (FMI::Utils::peer_num r = 0; r < num_peers; r++) {
            want_total += work_value(work_iters(r, round));
        }
        if (total.get() != want_total) {
            std::printf("rank %u: WORK ALLREDUCE MISMATCH round %d: got %d want %d\n",
                        rank, round, total.get(), want_total);
            throw ShapeFailure{4};
        }
        mix(checksum, total.get());

        // --- point-to-point into a peer that may still be computing -------------------
        // Blocking sends, so fix an order: even ranks send first, odd ranks receive first.
        const auto next = (rank + 1) % num_peers;
        const auto prev = (rank + num_peers - 1) % num_peers;
        std::vector<int> payload(width);
        for (int j = 0; j < width; j++) {
            payload[j] = ring_element(rank, round, j);
        }
        FMI::Comm::Data<std::vector<int>> out(payload);
        FMI::Comm::Data<std::vector<int>> in(std::vector<int>(width, -1));
        if (rank % 2 == 0) {
            comm.send(out, next);
            comm.recv(in, prev);
        } else {
            comm.recv(in, prev);
            comm.send(out, next);
        }
        const auto got_ring = in.get();
        for (int j = 0; j < width; j++) {
            const int want = ring_element(prev, round, j);
            if (got_ring[j] != want) {
                std::printf("rank %u: RING MISMATCH round %d slot %d: got %d want %d\n",
                            rank, round, j, got_ring[j], want);
                throw ShapeFailure{3};
            }
        }
        mix(checksum, got_ring[0]);
        mix(checksum, got_ring[width - 1]);

        // --- skew, part two ----------------------------------------------------------
        // The complementary tier: whoever was last to the allreduce is first to the gather,
        // so the arrival order flips inside one round.
        pause_ms((SKEW_TIERS - 1 - tier) * TAIL_PAUSE_MS);

        // --- gather to a rotating root -----------------------------------------------
        // A different root every round, so the binomial tree is a different one each time and
        // the idle links are not always the same ones.
        const auto gather_root = static_cast<FMI::Utils::peer_num>(round + 1) % num_peers;
        std::vector<int> send_vec{gather_element(rank, round)};
        FMI::Comm::Data<std::vector<int>> gsend(send_vec);
        FMI::Comm::Data<std::vector<int>> grecv(std::vector<int>(num_peers, -1));
        comm.gather(gsend, grecv, gather_root);
        if (rank == gather_root) {
            const auto got = grecv.get();
            for (FMI::Utils::peer_num r = 0; r < num_peers; r++) {
                if (got[r] != gather_element(r, round)) {
                    std::printf("rank %u: GATHER MISMATCH round %d slot %u: got %d want %d\n",
                                rank, round, r, got[r], gather_element(r, round));
                    throw ShapeFailure{5};
                }
                mix(checksum, got[r]);
            }
        }

        // --- bcast from another rotating root ----------------------------------------
        const auto bcast_root = static_cast<FMI::Utils::peer_num>(round) % num_peers;
        FMI::Comm::Data<int> token(rank == bcast_root ? bcast_token(bcast_root, round) : -1);
        comm.bcast(token, bcast_root);
        if (token.get() != bcast_token(bcast_root, round)) {
            std::printf("rank %u: BCAST MISMATCH round %d: got %d want %d\n",
                        rank, round, token.get(), bcast_token(bcast_root, round));
            throw ShapeFailure{6};
        }
        mix(checksum, token.get());

        // --- barrier: reached in a different order than the round's first collective ---
        comm.barrier();

        if (params.print_every > 0 && round % params.print_every == 0) {
            std::printf("rank %u: round %d ok checksum=%llu\n", rank, round, checksum);
        }
        if (params.ms_per_round > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(params.ms_per_round));
        }
    }

    return checksum;
}

FMI_REGISTER_SHAPE("uneven_participation",
                   "compute skew: per-rank work and pauses that rotate twice a round, plus a "
                   "periodic straggler, so ranks reach each collective far apart and links idle",
                   run_uneven_participation)

} // namespace
} // namespace FMI::Runbooks::Checkpoint
