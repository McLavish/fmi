// App shape: p2p_ring
//
// A second shape, written to show what an added shape file looks like and to cover traffic the
// baseline never produces:
//
//   * no collectives at all — so no binomial tree is ever built, and the only links that exist
//     are the ring's;
//   * strictly one-directional — each directed link carries payload one way and nothing the
//     other way, which is the case where an ack has no return traffic to ride on;
//   * no barrier — ranks are free to run ahead of each other, so a checkpoint lands with the
//     ring's in-flight messages spread over different rounds per rank.
//
// Deterministic by construction: every value is a function of (rank, round, index), and the
// checksum folds in only received data, in a fixed order.

#include "../shapes.h"

#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

namespace FMI::Runbooks::Checkpoint {
namespace {

//! Element `index` of the message rank `sender` sends in `round`.
int element_of(FMI::Utils::peer_num sender, int round, int index) {
    return static_cast<int>(sender) * 1000 + round * 7 + index;
}

unsigned long long run_p2p_ring(FMI::Communicator& comm, FMI::Utils::peer_num rank,
                                FMI::Utils::peer_num num_peers, const ShapeParams& params) {
    if (num_peers < 2) {
        std::printf("rank %u: p2p_ring needs at least 2 peers\n", rank);
        throw ShapeFailure{9};
    }
    const int width = params.payload_ints > 0 ? params.payload_ints : 1;
    const auto next = (rank + 1) % num_peers;
    const auto prev = (rank + num_peers - 1) % num_peers;

    long long checksum = 0;

    for (int round = 0; round < params.rounds; round++) {
        std::vector<int> payload(width);
        for (int j = 0; j < width; j++) {
            payload[j] = element_of(rank, round, j);
        }
        FMI::Comm::Data<std::vector<int>> out(payload);
        FMI::Comm::Data<std::vector<int>> in(std::vector<int>(width, -1));

        // Blocking sends, so fix an order: even ranks send first, odd ranks receive first.
        // With an odd rank count rank 0 and rank num_peers-1 are both "even" in this sense,
        // which is fine — the ring still has one receiver ready for every sender.
        if (rank % 2 == 0) {
            comm.send(out, next);
            comm.recv(in, prev);
        } else {
            comm.recv(in, prev);
            comm.send(out, next);
        }

        const auto got = in.get();
        for (int j = 0; j < width; j++) {
            const int want = element_of(prev, round, j);
            if (got[j] != want) {
                std::printf("rank %u: RING MISMATCH round %d slot %d: got %d want %d\n",
                            rank, round, j, got[j], want);
                throw ShapeFailure{3};
            }
        }
        checksum += got[0];
        checksum += got[width - 1];

        if (params.print_every > 0 && round % params.print_every == 0) {
            std::printf("rank %u: round %d ok checksum=%lld\n", rank, round, checksum);
        }
        if (params.ms_per_round > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(params.ms_per_round));
        }
    }

    return static_cast<unsigned long long>(checksum);
}

FMI_REGISTER_SHAPE("p2p_ring",
                   "point-to-point only: a one-directional ring with no collectives and no "
                   "barrier, so ranks drift apart and idle links carry no return traffic",
                   run_p2p_ring)

} // namespace
} // namespace FMI::Runbooks::Checkpoint
