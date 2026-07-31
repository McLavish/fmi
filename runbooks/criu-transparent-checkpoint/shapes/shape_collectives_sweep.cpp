// App shape: collectives_sweep
//
// The breadth shape. One round runs every collective the Communicator offers — barrier, bcast,
// scatter, gather, reduce, allreduce, scan — back to back, over scalars and over vectors, and
// with the root rotating through every rank. It exists to cover what `baseline` never reaches:
//
//   * a root other than 0. baseline roots everything at 0, so the binomial trees are never built
//     through PeerToPeer's root transform and gather/scatter never take their wrap-around path
//     (root 0 cannot wrap). Here root = round % num_peers, so every rank is root in turn.
//   * vector forms of bcast, gather, scatter, reduce and scan, not just of allreduce. Each moves
//     a multi-element slice per rank, so the messages a collective sends differ in size from the
//     scalar ones and a slice landing at the wrong offset is visible.
//   * the left-to-right paths of allreduce and scan. baseline uses a non-commutative function
//     only for reduce; a non-commutative allreduce is reduce-then-bcast and a non-commutative
//     scan is a linear chain (scan_ltr), both of which are entirely different traffic from the
//     recursive-doubling and binomial-tree versions, and neither is otherwise exercised here.
//
// It is an ordinary FMI program: no checkpoint API, no migration hook, nothing that knows it can
// be frozen.
//
// Determinism, which the sweep depends on: every value sent is a pure function of
// (rank, round, index), every value received is checked inline against what it must be, and the
// checksum is an order-sensitive rolling hash over received values only. A message that is lost,
// duplicated, reordered, or delivered into the wrong collective either fails a check outright or
// changes the hash.

#include "../shapes.h"

#include <chrono>
#include <cstddef>
#include <cstdio>
#include <thread>
#include <vector>

namespace FMI::Runbooks::Checkpoint {
namespace {

//! Elements per rank in the fixed-size vector collectives. Small on purpose: `payload_ints`
//! sizes the one bulk collective, and these are here for breadth, not for volume.
constexpr int kVec = 3;

//! Modulus of the non-commutative combiner. It keeps the left-to-right fold bounded, so the
//! expected value stays exact at any rank count rather than overflowing int the way a doubling
//! combiner would past ~30 ranks.
constexpr int kOrdMod = 1000003;

//! Neither commutative nor associative, so reduce/allreduce/scan take their left-to-right code
//! paths (reduce_ltr, reduce+bcast, scan_ltr) instead of their tree ones.
int ord_combine(int a, int b) { return (a * 31 + b) % kOrdMod; }

//! What rank `r` contributes to the ordered reductions in `round`. Bounded by kOrdMod's headroom.
int ord_input(FMI::Utils::peer_num r, int round) {
    return (static_cast<int>(r) * 7 + round) % 1000 + 1;
}

//! ord_combine folded over ranks 0..last in ascending rank order — which is exactly what the
//! library produces: reduce_ltr gathers into rank-indexed slots and folds them in slot order
//! (independent of the root), and scan_ltr's chain hands rank `last` the same fold over 0..last.
int ord_fold(FMI::Utils::peer_num last, int round) {
    int acc = ord_input(0, round);
    for (FMI::Utils::peer_num r = 1; r <= last; r++) {
        acc = ord_combine(acc, ord_input(r, round));
    }
    return acc;
}

// Value families, one per collective, so a message delivered into the wrong operation's buffer
// is a mismatch rather than a plausible number.
int bcast_scalar_value(FMI::Utils::peer_num root, int round) {
    return static_cast<int>(root) * 1000 + round * 7 + 1;
}
int bcast_vector_value(FMI::Utils::peer_num root, int round, int j) {
    return static_cast<int>(root) * 1000 + round * 7 + 100 + j;
}
int scatter_value(FMI::Utils::peer_num dest, int round, int j) {
    return static_cast<int>(dest) * 1000 + round * 7 + 200 + j;
}
int gather_value(FMI::Utils::peer_num owner, int round, int j) {
    return static_cast<int>(owner) * 1000 + round * 7 + 300 + j;
}

//! Order-sensitive checksum fold. A running sum would absorb a reordering or a value showing up
//! under a different collective; a rolling hash over (operation tag, value) does not.
void fold(unsigned long long& h, int tag, int value) {
    h = h * 1000003ULL + static_cast<unsigned long long>(static_cast<unsigned int>(tag));
    h = h * 1000003ULL + static_cast<unsigned long long>(static_cast<unsigned int>(value));
}

//! Prints the one diagnostic line the sweep classifies a trial from, then fails the run.
[[noreturn]] void mismatch(FMI::Utils::peer_num rank, const char* op, int round, int slot,
                           int got, int want, int exit_code) {
    std::printf("rank %u: %s MISMATCH round %d slot %d: got %d want %d\n",
                rank, op, round, slot, got, want);
    throw ShapeFailure{exit_code};
}

unsigned long long run_collectives_sweep(FMI::Communicator& comm, FMI::Utils::peer_num rank,
                                         FMI::Utils::peer_num num_peers,
                                         const ShapeParams& params) {
    if (num_peers < 2) {
        std::printf("rank %u: collectives_sweep needs at least 2 peers\n", rank);
        throw ShapeFailure{9};
    }
    const int n = static_cast<int>(num_peers);
    // Sum of the rank ids, the constant in every expected reduction below. Written out rather
    // than assumed a power of two: this shape must be exact at 3, 5 and 7 ranks too.
    const int rank_sum = n * (n - 1) / 2;
    const int bulk = params.payload_ints;

    FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);
    FMI::Utils::Function<int> ordered(ord_combine, false, false);
    FMI::Utils::Function<std::vector<int>> vsum([](std::vector<int> a, std::vector<int> b) {
        for (std::size_t j = 0; j < a.size(); j++) {
            a[j] += b[j];
        }
        return a;
    }, true, true);

    unsigned long long checksum = 0;

    for (int round = 0; round < params.rounds; round++) {
        const auto root = static_cast<FMI::Utils::peer_num>(round % n);

        // --- barrier -----------------------------------------------------------------
        comm.barrier();

        // --- bcast, scalar -----------------------------------------------------------
        const int bs_want = bcast_scalar_value(root, round);
        FMI::Comm::Data<int> bs(rank == root ? bs_want : -1);
        comm.bcast(bs, root);
        if (bs.get() != bs_want) {
            mismatch(rank, "BCAST", round, 0, bs.get(), bs_want, 3);
        }
        fold(checksum, 1, bs.get());

        // --- bcast, vector -----------------------------------------------------------
        std::vector<int> bv(kVec, -1);
        if (rank == root) {
            for (int j = 0; j < kVec; j++) {
                bv[j] = bcast_vector_value(root, round, j);
            }
        }
        FMI::Comm::Data<std::vector<int>> bvbuf(bv);
        comm.bcast(bvbuf, root);
        {
            const auto got = bvbuf.get();
            for (int j = 0; j < kVec; j++) {
                const int want = bcast_vector_value(root, round, j);
                if (got[j] != want) {
                    mismatch(rank, "BCAST_VEC", round, j, got[j], want, 4);
                }
                fold(checksum, 2, got[j]);
            }
        }

        // --- scatter -----------------------------------------------------------------
        // Only the root's sendbuf matters, but every rank builds it: that is what an ordinary
        // program does, and it keeps the buffer sizes identical on every rank.
        std::vector<int> spread(static_cast<std::size_t>(n) * kVec);
        for (int r = 0; r < n; r++) {
            for (int j = 0; j < kVec; j++) {
                spread[static_cast<std::size_t>(r) * kVec + j] =
                        scatter_value(static_cast<FMI::Utils::peer_num>(r), round, j);
            }
        }
        FMI::Comm::Data<std::vector<int>> ssend(spread);
        FMI::Comm::Data<std::vector<int>> srecv(std::vector<int>(kVec, -1));
        comm.scatter(ssend, srecv, root);
        {
            const auto got = srecv.get();
            for (int j = 0; j < kVec; j++) {
                const int want = scatter_value(rank, round, j);
                if (got[j] != want) {
                    mismatch(rank, "SCATTER", round, j, got[j], want, 5);
                }
                fold(checksum, 3, got[j]);
            }
        }

        // --- gather ------------------------------------------------------------------
        std::vector<int> slice(kVec);
        for (int j = 0; j < kVec; j++) {
            slice[j] = gather_value(rank, round, j);
        }
        FMI::Comm::Data<std::vector<int>> gsend(slice);
        FMI::Comm::Data<std::vector<int>> grecv(
                std::vector<int>(static_cast<std::size_t>(n) * kVec, -1));
        comm.gather(gsend, grecv, root);
        if (rank == root) {
            const auto got = grecv.get();
            for (int r = 0; r < n; r++) {
                for (int j = 0; j < kVec; j++) {
                    const int want = gather_value(static_cast<FMI::Utils::peer_num>(r), round, j);
                    const int slot = r * kVec + j;
                    if (got[slot] != want) {
                        mismatch(rank, "GATHER", round, slot, got[slot], want, 6);
                    }
                    fold(checksum, 4, got[slot]);
                }
            }
        }

        // --- reduce to a rotating root, commutative + associative ---------------------
        const int rs_term = (round % 97) + 1;
        FMI::Comm::Data<int> rs_in(static_cast<int>(rank) * 10 + rs_term);
        FMI::Comm::Data<int> rs_out(0);
        comm.reduce(rs_in, rs_out, root, sum);
        if (rank == root) {
            const int want = 10 * rank_sum + n * rs_term;
            if (rs_out.get() != want) {
                mismatch(rank, "REDUCE", round, 0, rs_out.get(), want, 7);
            }
            fold(checksum, 5, rs_out.get());
        }

        // --- reduce to a rotating root, left-to-right --------------------------------
        FMI::Comm::Data<int> ro_in(ord_input(rank, round));
        FMI::Comm::Data<int> ro_out(0);
        comm.reduce(ro_in, ro_out, root, ordered);
        if (rank == root) {
            const int want = ord_fold(num_peers - 1, round);
            if (ro_out.get() != want) {
                mismatch(rank, "REDUCE_LTR", round, 0, ro_out.get(), want, 8);
            }
            fold(checksum, 6, ro_out.get());
        }

        // --- reduce, vector ----------------------------------------------------------
        std::vector<int> rv(kVec);
        for (int j = 0; j < kVec; j++) {
            rv[j] = static_cast<int>(rank) + j;
        }
        FMI::Comm::Data<std::vector<int>> rvsend(rv);
        FMI::Comm::Data<std::vector<int>> rvrecv(std::vector<int>(kVec, 0));
        comm.reduce(rvsend, rvrecv, root, vsum);
        if (rank == root) {
            const auto got = rvrecv.get();
            for (int j = 0; j < kVec; j++) {
                const int want = n * j + rank_sum;
                if (got[j] != want) {
                    mismatch(rank, "REDUCE_VEC", round, j, got[j], want, 10);
                }
                fold(checksum, 7, got[j]);
            }
        }

        // --- allreduce, commutative + associative ------------------------------------
        FMI::Comm::Data<int> as_in(static_cast<int>(rank) + round);
        FMI::Comm::Data<int> as_out(0);
        comm.allreduce(as_in, as_out, sum);
        {
            const int want = n * round + rank_sum;
            if (as_out.get() != want) {
                mismatch(rank, "ALLREDUCE", round, 0, as_out.get(), want, 11);
            }
            fold(checksum, 8, as_out.get());
        }

        // --- allreduce, left-to-right (reduce to 0 then bcast) -----------------------
        FMI::Comm::Data<int> ao_in(ord_input(rank, round));
        FMI::Comm::Data<int> ao_out(0);
        comm.allreduce(ao_in, ao_out, ordered);
        {
            const int want = ord_fold(num_peers - 1, round);
            if (ao_out.get() != want) {
                mismatch(rank, "ALLREDUCE_LTR", round, 0, ao_out.get(), want, 12);
            }
            fold(checksum, 9, ao_out.get());
        }

        // --- allreduce, bulk vector --------------------------------------------------
        // The one payload sized by the caller: past a socket buffer a single message spans many
        // segments, so a freeze can land part way through one rather than only between messages.
        if (bulk > 1) {
            std::vector<int> chunk(bulk);
            for (int j = 0; j < bulk; j++) {
                chunk[j] = static_cast<int>(rank) + j;
            }
            FMI::Comm::Data<std::vector<int>> vsend(chunk);
            FMI::Comm::Data<std::vector<int>> vrecv(std::vector<int>(bulk, 0));
            comm.allreduce(vsend, vrecv, vsum);
            const auto got = vrecv.get();
            for (int j = 0; j < bulk; j++) {
                const int want = n * j + rank_sum;
                if (got[j] != want) {
                    mismatch(rank, "ALLREDUCE_BULK", round, j, got[j], want, 13);
                }
            }
            // Every element was checked; folding both ends keeps the checksum O(1) per round.
            fold(checksum, 10, got[0]);
            fold(checksum, 10, got[bulk - 1]);
        }

        // --- scan, commutative + associative -----------------------------------------
        FMI::Comm::Data<int> sc_in(static_cast<int>(rank) + 1);
        FMI::Comm::Data<int> sc_out(0);
        comm.scan(sc_in, sc_out, sum);
        {
            const int want = static_cast<int>((rank + 1) * (rank + 2) / 2);
            if (sc_out.get() != want) {
                mismatch(rank, "SCAN", round, 0, sc_out.get(), want, 14);
            }
            fold(checksum, 11, sc_out.get());
        }

        // --- scan, left-to-right ------------------------------------------------------
        FMI::Comm::Data<int> so_in(ord_input(rank, round));
        FMI::Comm::Data<int> so_out(0);
        comm.scan(so_in, so_out, ordered);
        {
            const int want = ord_fold(rank, round);
            if (so_out.get() != want) {
                mismatch(rank, "SCAN_LTR", round, 0, so_out.get(), want, 15);
            }
            fold(checksum, 12, so_out.get());
        }

        // --- scan, vector -------------------------------------------------------------
        std::vector<int> sv(kVec);
        for (int j = 0; j < kVec; j++) {
            sv[j] = (static_cast<int>(rank) + 1) * (j + 1);
        }
        FMI::Comm::Data<std::vector<int>> svsend(sv);
        FMI::Comm::Data<std::vector<int>> svrecv(std::vector<int>(kVec, 0));
        comm.scan(svsend, svrecv, vsum);
        {
            const auto got = svrecv.get();
            const int prefix = static_cast<int>((rank + 1) * (rank + 2) / 2);
            for (int j = 0; j < kVec; j++) {
                const int want = (j + 1) * prefix;
                if (got[j] != want) {
                    mismatch(rank, "SCAN_VEC", round, j, got[j], want, 16);
                }
                fold(checksum, 13, got[j]);
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

FMI_REGISTER_SHAPE("collectives_sweep",
                   "every collective back to back with a rotating root: barrier, bcast, scatter, "
                   "gather, reduce, allreduce and scan, over scalars and vectors, including the "
                   "left-to-right allreduce and scan paths",
                   run_collectives_sweep)

} // namespace
} // namespace FMI::Runbooks::Checkpoint
