// App shape: mixed_p2p_collective
//
// Point-to-point traffic and collectives in the SAME round, which neither existing shape does:
// baseline separates its ring from its collectives by a barrier-fronted round structure, and
// p2p_ring has no collectives at all. Here every collective is bracketed by point-to-point
// messages, so a checkpoint can land with a rank part way through a binomial tree while its
// links also carry direct messages that belong to no collective.
//
// Three properties this shape adds:
//
//   * **non-neighbour traffic** — the hub phases make every rank talk to rank 0, which for ranks
//     2..n-2 is not a ring neighbour, and the skew exchange shifts by 2 instead of 1, so at 5
//     ranks and up its partner shares no ring edge with it. The links that exist are therefore
//     not the ring's, and differ from the ones the collectives' binomial trees build.
//   * **unbalanced traffic** — rank 0 receives n-1 + 4 messages and sends n-1 per round while a
//     rank other than 0 or 1 sends 2 and receives 2. Rank 1 sends 5. A per-link window or
//     retention rule that only holds when both directions carry similar volume fails here.
//   * **rotating roots** — bcast and gather use a different root each round, so the trees are
//     rebuilt in a different shape every round rather than always fanning out from rank 0.
//
// Deterministic by construction: every value is a pure function of (sender, round, index), and
// the checksum folds ONLY received values. The fold is a polynomial (multiply-then-add) rather
// than a sum, so it is sensitive to order as well as content: a lost, duplicated, reordered or
// substituted message changes the result, where a summing checksum would hide a reordering.
//
// Ordering rules that keep it deadlock-free at any rank count, powers of two or not:
//   * hub in: everyone but rank 0 sends, rank 0 receives in ascending rank order;
//   * hub out: rank 0 sends in ascending rank order to receivers that are already blocked on it;
//   * skew exchange: the lower-numbered end of each directed pair sends first.

#include "../shapes.h"

#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

namespace FMI::Runbooks::Checkpoint {
namespace {

//! Extra messages rank 1 pushes at rank 0 every round, on top of the one every rank sends. This
//! is the deliberate imbalance: rank 1 sends five messages per round where rank 3 sends two.
constexpr int kBurst = 4;

//! Elements in the skew exchange's message. Deliberately small and fixed: that exchange is the
//! one phase where both ends can be sending at the same time, and a payload that fits a socket
//! buffer keeps it deadlock-free whatever cycle structure the shift produces.
constexpr int kSkewWidth = 3;

//! The value rank `root` broadcasts in `round`. Known to every rank, so all of them can check it.
int bcast_value(FMI::Utils::peer_num root, int round) {
    return static_cast<int>(root) * 31 + round * 7 + 1;
}

//! Message `slot` of the ones rank `sender` sends to the hub in `round`. Slot 0 is the message
//! every rank sends; slots 1..kBurst are rank 1's burst.
int hub_value(FMI::Utils::peer_num sender, int round, int slot) {
    return static_cast<int>(sender) * 1000 + round * 11 + slot * 3;
}

//! Element `index` of the hub's reply to `dest` in `round`.
int reply_value(FMI::Utils::peer_num dest, int round, int index) {
    return static_cast<int>(dest) * 7 + round * 3 + index * 5 + 1;
}

//! Element `index` of the skew message rank `sender` sends in `round`.
int skew_value(FMI::Utils::peer_num sender, int round, int index) {
    return static_cast<int>(sender) * 97 + round * 13 + index;
}

//! Value rank `sender` contributes to `round`'s gather.
int gather_value(FMI::Utils::peer_num sender, int round) {
    return static_cast<int>(sender) * 10 + round % 1000;
}

//! Order- and content-sensitive fold. Dropping a value, seeing one twice, seeing two of them in
//! the other order or seeing a different value all change the result.
void mix(unsigned long long& checksum, int value) {
    checksum = checksum * 1000003ULL
             + static_cast<unsigned long long>(static_cast<unsigned int>(value));
}

unsigned long long run_mixed_p2p_collective(FMI::Communicator& comm, FMI::Utils::peer_num rank,
                                            FMI::Utils::peer_num num_peers,
                                            const ShapeParams& params) {
    if (num_peers < 2) {
        std::printf("rank %u: mixed_p2p_collective needs at least 2 peers\n", rank);
        throw ShapeFailure{9};
    }
    // Sizes the hub's reply. Anything past a socket buffer makes one reply span many segments,
    // so a freeze can land part way through a payload rather than only between whole messages.
    const int width = params.payload_ints > 0 ? params.payload_ints : 1;
    // A shift of 2 leaves the ring: at 5 ranks and up the partner is not a neighbour. At 2 ranks
    // there is no non-neighbour to pick, so it degenerates to the only other rank.
    const FMI::Utils::peer_num stride = num_peers >= 3 ? 2 : 1;
    const auto skew_next = (rank + stride) % num_peers;
    const auto skew_prev = (rank + num_peers - stride) % num_peers;

    FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);

    unsigned long long checksum = 0;

    for (int round = 0; round < params.rounds; round++) {
        // --- barrier: the round's only synchronisation point --------------------------
        comm.barrier();

        // --- bcast from a rotating root -----------------------------------------------
        const auto bcast_root = static_cast<FMI::Utils::peer_num>(
                static_cast<unsigned>(round) % num_peers);
        FMI::Comm::Data<int> token(rank == bcast_root ? bcast_value(bcast_root, round) : -1);
        comm.bcast(token, bcast_root);
        if (token.get() != bcast_value(bcast_root, round)) {
            std::printf("rank %u: BCAST MISMATCH round %d root %u: got %d want %d\n",
                        rank, round, bcast_root, token.get(), bcast_value(bcast_root, round));
            throw ShapeFailure{3};
        }
        mix(checksum, token.get());

        // --- hub in: point-to-point, straight after a collective -----------------------
        // Every rank but 0 sends one message to rank 0 -- a non-neighbour for ranks 2..n-2 --
        // and rank 1 sends kBurst more. Rank 0 receives all of them, per sender, in a fixed
        // order, so its checksum pins down both the contents and the sequence.
        if (rank == 0) {
            for (FMI::Utils::peer_num sender = 1; sender < num_peers; sender++) {
                FMI::Comm::Data<int> in(0);
                comm.recv(in, sender);
                if (in.get() != hub_value(sender, round, 0)) {
                    std::printf("rank %u: HUB MISMATCH round %d from %u: got %d want %d\n",
                                rank, round, sender, in.get(), hub_value(sender, round, 0));
                    throw ShapeFailure{4};
                }
                mix(checksum, in.get());
            }
            for (int slot = 1; slot <= kBurst; slot++) {
                FMI::Comm::Data<int> in(0);
                comm.recv(in, 1);
                if (in.get() != hub_value(1, round, slot)) {
                    std::printf("rank %u: BURST MISMATCH round %d slot %d: got %d want %d\n",
                                rank, round, slot, in.get(), hub_value(1, round, slot));
                    throw ShapeFailure{5};
                }
                mix(checksum, in.get());
            }
        } else {
            FMI::Comm::Data<int> out(hub_value(rank, round, 0));
            comm.send(out, 0);
            if (rank == 1) {
                for (int slot = 1; slot <= kBurst; slot++) {
                    FMI::Comm::Data<int> burst(hub_value(rank, round, slot));
                    comm.send(burst, 0);
                }
            }
        }

        // --- allreduce, with point-to-point traffic on either side of it ---------------
        FMI::Comm::Data<int> mine(static_cast<int>(rank) + round % 13);
        FMI::Comm::Data<int> total(0);
        comm.allreduce(mine, total, sum);
        const int want_total = static_cast<int>(num_peers) * (round % 13)
                             + static_cast<int>(num_peers * (num_peers - 1) / 2);
        if (total.get() != want_total) {
            std::printf("rank %u: ALLREDUCE MISMATCH round %d: got %d want %d\n",
                        rank, round, total.get(), want_total);
            throw ShapeFailure{6};
        }
        mix(checksum, total.get());

        // --- hub out: rank 0 replies to everyone, in ascending order -------------------
        // The receivers are already blocked on rank 0, so a reply wider than a socket buffer
        // still cannot deadlock. This is the second half of the imbalance: rank 0 sends n-1
        // messages per round, every other rank sends none here.
        if (rank == 0) {
            for (FMI::Utils::peer_num dest = 1; dest < num_peers; dest++) {
                std::vector<int> reply(width);
                for (int j = 0; j < width; j++) {
                    reply[j] = reply_value(dest, round, j);
                }
                FMI::Comm::Data<std::vector<int>> out(reply);
                comm.send(out, dest);
            }
        } else {
            FMI::Comm::Data<std::vector<int>> in(std::vector<int>(width, -1));
            comm.recv(in, 0);
            const auto got = in.get();
            for (int j = 0; j < width; j++) {
                if (got[j] != reply_value(rank, round, j)) {
                    std::printf("rank %u: REPLY MISMATCH round %d slot %d: got %d want %d\n",
                                rank, round, j, got[j], reply_value(rank, round, j));
                    throw ShapeFailure{7};
                }
            }
            mix(checksum, got[0]);
            mix(checksum, got[width - 1]);
        }

        // --- skew exchange: shift by 2, so the partner is off the ring -----------------
        // The lower end of each directed pair sends first. That is enough for every cycle the
        // shift produces at any rank count, and the message is small either way.
        std::vector<int> skew_out(kSkewWidth);
        for (int j = 0; j < kSkewWidth; j++) {
            skew_out[j] = skew_value(rank, round, j);
        }
        FMI::Comm::Data<std::vector<int>> sout(skew_out);
        FMI::Comm::Data<std::vector<int>> sin(std::vector<int>(kSkewWidth, -1));
        if (rank < skew_next) {
            comm.send(sout, skew_next);
            comm.recv(sin, skew_prev);
        } else {
            comm.recv(sin, skew_prev);
            comm.send(sout, skew_next);
        }
        const auto skew_got = sin.get();
        for (int j = 0; j < kSkewWidth; j++) {
            if (skew_got[j] != skew_value(skew_prev, round, j)) {
                std::printf("rank %u: SKEW MISMATCH round %d from %u slot %d: got %d want %d\n",
                            rank, round, skew_prev, j, skew_got[j],
                            skew_value(skew_prev, round, j));
                throw ShapeFailure{8};
            }
            mix(checksum, skew_got[j]);
        }

        // --- gather to a rotating root, closing the round with a collective ------------
        std::vector<int> gsend_vec{gather_value(rank, round)};
        FMI::Comm::Data<std::vector<int>> gsend(gsend_vec);
        FMI::Comm::Data<std::vector<int>> grecv(std::vector<int>(num_peers, -1));
        const auto gather_root = static_cast<FMI::Utils::peer_num>(
                static_cast<unsigned>(round + 1) % num_peers);
        comm.gather(gsend, grecv, gather_root);
        if (rank == gather_root) {
            const auto got = grecv.get();
            for (FMI::Utils::peer_num r = 0; r < num_peers; r++) {
                if (got[r] != gather_value(r, round)) {
                    std::printf("rank %u: GATHER MISMATCH round %d slot %u: got %d want %d\n",
                                rank, round, r, got[r], gather_value(r, round));
                    throw ShapeFailure{10};
                }
                mix(checksum, got[r]);
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

FMI_REGISTER_SHAPE("mixed_p2p_collective",
                   "point-to-point interleaved with collectives: a hub every rank sends to, a "
                   "burst from one rank, a shift-by-2 exchange off the ring, and bcast/gather "
                   "from rotating roots",
                   run_mixed_p2p_collective)

} // namespace
} // namespace FMI::Runbooks::Checkpoint
