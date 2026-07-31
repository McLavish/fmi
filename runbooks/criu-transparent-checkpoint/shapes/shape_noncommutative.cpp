// App shape: noncommutative
//
// Reductions whose operator is declared NEITHER commutative NOR associative. FMI reads those two
// flags off `FMI::Utils::Function` and takes a different algorithm because of them
// (`PeerToPeer::reduce` -> `reduce_ltr`, `allreduce` -> reduce-to-0 plus bcast,
// `scan` -> `scan_ltr`), so this shape drives traffic the baseline's commutative reductions never
// produce:
//
//   * `reduce_ltr` is a *gather* followed by n-1 local applications on the root, not a binomial
//     tree of partial results — every rank's contribution travels intact, and the root's buffer
//     grows with the rank count;
//   * `allreduce` becomes two collectives back to back, so a checkpoint can land between the
//     reduce and the bcast that publishes its result;
//   * `scan_ltr` is a strictly serial chain of n-1 messages, rank 0 -> 1 -> ... -> n-1, which is
//     the longest single dependency in the runbook: freezing any rank in it stalls every rank
//     above it.
//
// Order sensitivity is the point. Both operators are asymmetric (`mix(a,b) = 31a + b + 1`,
// `shift(a,b) = 3a + 2b + 7`, both mod a prime), so folding the same values in a different order,
// re-associating them, dropping one or applying one twice all produce a different int. Each rank
// recomputes the exact fold FMI must have performed and compares, and the checksum absorbs the
// received results positionally, so an order error cannot cancel out.
//
// Deterministic by construction: every contribution is a pure function of (rank, round, salt),
// the reduce's root rotates with the round rather than with anything observed, and no result is
// folded into the checksum on a rank that is not supposed to hold it.

#include "../shapes.h"

#include <chrono>
#include <cstddef>
#include <cstdio>
#include <thread>
#include <vector>

namespace FMI::Runbooks::Checkpoint {
namespace {

//! Everything is reduced modulo this prime, which keeps each operator's intermediates far inside
//! int (the largest is 31 * kMod + kMod + 1) so the arithmetic can never overflow and the
//! expected value is exactly reproducible on every rank.
constexpr int kMod = 1000003;

//! One salt per collective, so the payloads of the collectives within a round are all different.
//! A message delivered to the wrong collective is then a wrong *value*, not a plausible one.
constexpr int kSaltAllreduce = 1;
constexpr int kSaltReduce = 2;
constexpr int kSaltScan = 3;
constexpr int kSaltVector = 4;

//! Non-commutative and non-associative: mix(a,b) != mix(b,a), and mix(mix(a,b),c) != mix(a,mix(b,c)).
int mix(int a, int b) {
    return (31 * a + b + 1) % kMod;
}

//! A second asymmetric operator, so two collectives in the same round fold the same rank order
//! into different results.
int shift(int a, int b) {
    return (3 * a + 2 * b + 7) % kMod;
}

using IntOp = int (*)(int, int);

//! The value rank `peer` contributes to the collective identified by `salt` in `round`.
int value_of(FMI::Utils::peer_num peer, int round, int salt) {
    return (static_cast<int>(peer) * 7919 + round * 131 + salt * 104729 + 17) % kMod;
}

//! Element `index` of the bulk payload rank `peer` contributes in `round`.
int element_of(FMI::Utils::peer_num peer, int round, int index) {
    return (value_of(peer, round, kSaltVector) + index * 37) % kMod;
}

//! Left-to-right fold over ranks 0..upto, i.e. op(op(op(v0,v1),v2)...,v_upto).
/*!
 * This is exactly the order the library must produce: `reduce_ltr` gathers every contribution
 * into a buffer indexed by absolute rank id and then folds it from slot 0 upwards (so the root's
 * identity does not change the result), and `scan_ltr` passes the running prefix up the rank
 * chain. Recomputing it here is what turns a reordered or duplicated message into a failure.
 */
int fold_ltr(IntOp op, FMI::Utils::peer_num upto, int round, int salt) {
    int acc = value_of(0, round, salt);
    for (FMI::Utils::peer_num p = 1; p <= upto; p++) {
        acc = op(acc, value_of(p, round, salt));
    }
    return acc;
}

//! Absorbs one received value positionally, so both the values and their order are in the digest.
unsigned long long absorb(unsigned long long checksum, int observed) {
    return checksum * 1000003ULL + static_cast<unsigned long long>(static_cast<unsigned int>(observed));
}

unsigned long long run_noncommutative(FMI::Communicator& comm, FMI::Utils::peer_num rank,
                                      FMI::Utils::peer_num num_peers, const ShapeParams& params) {
    if (num_peers < 2) {
        std::printf("rank %u: noncommutative needs at least 2 peers\n", rank);
        throw ShapeFailure{9};
    }
    const int width = params.payload_ints > 0 ? params.payload_ints : 1;

    // Guard against the shape being weakened into something an order error could slip through:
    // if the operators ever became order-insensitive, every check below would still pass while
    // proving nothing. Folding the same values right-to-left has to give a different answer.
    {
        int rtl = value_of(num_peers - 1, 0, kSaltAllreduce);
        for (FMI::Utils::peer_num p = num_peers - 1; p > 0; p--) {
            rtl = mix(rtl, value_of(p - 1, 0, kSaltAllreduce));
        }
        if (rtl == fold_ltr(mix, num_peers - 1, 0, kSaltAllreduce)) {
            std::printf("rank %u: ORDER WITNESS MISMATCH: the operator is order-insensitive at "
                        "%u peers, so this shape would pass vacuously\n", rank, num_peers);
            throw ShapeFailure{10};
        }
    }

    FMI::Utils::Function<int> mix_fn(mix, false, false);
    FMI::Utils::Function<int> shift_fn(shift, false, false);
    FMI::Utils::Function<std::vector<int>> vmix_fn([](std::vector<int> a, std::vector<int> b) {
        for (std::size_t j = 0; j < a.size(); j++) {
            a[j] = mix(a[j], b[j]);
        }
        return a;
    }, false, false);

    unsigned long long checksum = 0;

    for (int round = 0; round < params.rounds; round++) {
        // --- allreduce: reduce-to-rank-0 followed by a bcast of its result ------------
        FMI::Comm::Data<int> ar_in(value_of(rank, round, kSaltAllreduce));
        FMI::Comm::Data<int> ar_out(0);
        comm.allreduce(ar_in, ar_out, mix_fn);
        const int want_ar = fold_ltr(mix, num_peers - 1, round, kSaltAllreduce);
        if (ar_out.get() != want_ar) {
            std::printf("rank %u: ALLREDUCE MISMATCH round %d: got %d want %d\n",
                        rank, round, ar_out.get(), want_ar);
            throw ShapeFailure{3};
        }
        checksum = absorb(checksum, ar_out.get());

        // --- reduce with a rotating root ---------------------------------------------
        // The root moves every round, so the gather underneath rotates its rank->slot mapping
        // and, past the wrap point, splits a peer's block across the ends of the buffer. The
        // result must still be the fold in absolute rank order, whoever the root is.
        const auto root = static_cast<FMI::Utils::peer_num>(round % static_cast<int>(num_peers));
        FMI::Comm::Data<int> rd_in(value_of(rank, round, kSaltReduce));
        FMI::Comm::Data<int> rd_out(0);
        comm.reduce(rd_in, rd_out, root, shift_fn);
        if (rank == root) {
            const int want_rd = fold_ltr(shift, num_peers - 1, round, kSaltReduce);
            if (rd_out.get() != want_rd) {
                std::printf("rank %u: REDUCE MISMATCH round %d root %u: got %d want %d\n",
                            rank, round, root, rd_out.get(), want_rd);
                throw ShapeFailure{4};
            }
            checksum = absorb(checksum, rd_out.get());
        }

        // --- scan: the serial prefix chain -------------------------------------------
        FMI::Comm::Data<int> sc_in(value_of(rank, round, kSaltScan));
        FMI::Comm::Data<int> sc_out(0);
        comm.scan(sc_in, sc_out, shift_fn);
        const int want_sc = fold_ltr(shift, rank, round, kSaltScan);
        if (sc_out.get() != want_sc) {
            std::printf("rank %u: SCAN MISMATCH round %d: got %d want %d\n",
                        rank, round, sc_out.get(), want_sc);
            throw ShapeFailure{5};
        }
        checksum = absorb(checksum, sc_out.get());

        // --- a bulk vector allreduce on the same ordered path -------------------------
        // Only worth doing when the payload is wide enough to span segments; the gather inside
        // an ordered reduce carries num_peers * width ints in one message on the last hop.
        if (width > 1) {
            std::vector<int> chunk(width);
            for (int j = 0; j < width; j++) {
                chunk[j] = element_of(rank, round, j);
            }
            FMI::Comm::Data<std::vector<int>> vsend(chunk);
            FMI::Comm::Data<std::vector<int>> vrecv(std::vector<int>(width, 0));
            comm.allreduce(vsend, vrecv, vmix_fn);
            const auto got = vrecv.get();
            for (int j = 0; j < width; j++) {
                int want = element_of(0, round, j);
                for (FMI::Utils::peer_num p = 1; p < num_peers; p++) {
                    want = mix(want, element_of(p, round, j));
                }
                if (got[j] != want) {
                    std::printf("rank %u: VECTOR MISMATCH round %d slot %d: got %d want %d\n",
                                rank, round, j, got[j], want);
                    throw ShapeFailure{6};
                }
            }
            checksum = absorb(checksum, got[0]);
            checksum = absorb(checksum, got[width - 1]);
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

FMI_REGISTER_SHAPE("noncommutative",
                   "reductions declared neither commutative nor associative: the left-to-right "
                   "gather-and-fold reduce with a rotating root, the reduce+bcast allreduce and "
                   "the serial scan chain, all checked against the exact fold order",
                   run_noncommutative)

} // namespace
} // namespace FMI::Runbooks::Checkpoint
