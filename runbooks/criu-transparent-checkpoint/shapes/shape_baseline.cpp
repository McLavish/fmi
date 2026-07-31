// App shape: baseline
//
// The original workload of this runbook, moved here verbatim when the subject gained named
// shapes. Its per-round operation sequence, its payload values and its checksum arithmetic are
// unchanged, so the 48/48 result recorded in README.md still refers to this shape.
//
// Every collective checks its own result inline and fails the run on a mismatch, and the running
// checksum lets the sweep compare a checkpointed run against a clean one of the same shape. A
// lost, duplicated or substituted message therefore fails the run rather than being absorbed
// into a plausible-looking answer.

#include "../shapes.h"

#include <chrono>
#include <cstddef>
#include <cstdio>
#include <thread>
#include <vector>

namespace FMI::Runbooks::Checkpoint {
namespace {

unsigned long long run_baseline(FMI::Communicator& comm, FMI::Utils::peer_num peer_id,
                                FMI::Utils::peer_num num_peers, const ShapeParams& params) {
    const int rounds = params.rounds;
    const int ms = params.ms_per_round;
    const int print_every = params.print_every;
    // Sizes the vector collective below. Anything past a socket buffer makes a single message
    // span many segments, so a freeze can land part way through a payload rather than only
    // between whole messages — the case a small-message workload never reaches.
    const int payload_ints = params.payload_ints;

    FMI::Utils::Function<int> sum([](int a, int b) { return a + b; }, true, true);
    // Deliberately NOT commutative/associative: takes the other reduction algorithm, so the
    // run exercises both code paths through the framed transport.
    FMI::Utils::Function<int> ordered([](int a, int b) { return 2 * a + b; }, false, false);
    FMI::Utils::Function<std::vector<int>> vsum([](std::vector<int> a, std::vector<int> b) {
        for (std::size_t j = 0; j < a.size(); j++) {
            a[j] += b[j];
        }
        return a;
    }, true, true);

    long long checksum = 0;

    for (int round = 0; round < rounds; round++) {
        // --- barrier -----------------------------------------------------------------
        comm.barrier();

        // --- bcast -------------------------------------------------------------------
        FMI::Comm::Data<int> token(peer_id == 0 ? round * 7 + 1 : -1);
        comm.bcast(token, 0);
        checksum += token.get();

        // --- point-to-point ring -----------------------------------------------------
        // Blocking sends, so fix an order: even ranks send first, odd ranks receive first.
        const auto next = (peer_id + 1) % num_peers;
        const auto prev = (peer_id + num_peers - 1) % num_peers;
        FMI::Comm::Data<int> out(static_cast<int>(peer_id) * 1000 + round);
        FMI::Comm::Data<int> in(0);
        if (peer_id % 2 == 0) {
            comm.send(out, next);
            comm.recv(in, prev);
        } else {
            comm.recv(in, prev);
            comm.send(out, next);
        }
        if (in.get() != static_cast<int>(prev) * 1000 + round) {
            std::printf("rank %u: P2P MISMATCH round %d: got %d want %d\n",
                        peer_id, round, in.get(), static_cast<int>(prev) * 1000 + round);
            throw ShapeFailure{3};
        }
        checksum += in.get();

        // --- allreduce (commutative + associative) -----------------------------------
        FMI::Comm::Data<int> mine(static_cast<int>(peer_id) + round);
        FMI::Comm::Data<int> total(0);
        comm.allreduce(mine, total, sum);
        const int want_total = static_cast<int>(num_peers) * round
                             + static_cast<int>(num_peers * (num_peers - 1) / 2);
        if (total.get() != want_total) {
            std::printf("rank %u: ALLREDUCE MISMATCH round %d: got %d want %d\n",
                        peer_id, round, total.get(), want_total);
            throw ShapeFailure{4};
        }
        checksum += total.get();

        // --- reduce to root 0, non-commutative path ----------------------------------
        FMI::Comm::Data<int> ord_in(static_cast<int>(peer_id) + 1);
        FMI::Comm::Data<int> ord_out(0);
        comm.reduce(ord_in, ord_out, 0, ordered);
        if (peer_id == 0) {
            checksum += ord_out.get();
        }

        // --- gather / scatter on vectors ---------------------------------------------
        std::vector<int> send_vec{static_cast<int>(peer_id) * 10 + round};
        FMI::Comm::Data<std::vector<int>> gsend(send_vec);
        std::vector<int> recv_vec(num_peers, -1);
        FMI::Comm::Data<std::vector<int>> grecv(recv_vec);
        comm.gather(gsend, grecv, 0);
        if (peer_id == 0) {
            auto got = grecv.get();
            for (FMI::Utils::peer_num r = 0; r < num_peers; r++) {
                if (got[r] != static_cast<int>(r) * 10 + round) {
                    std::printf("rank %u: GATHER MISMATCH round %d slot %u: got %d want %d\n",
                                peer_id, round, r, got[r], static_cast<int>(r) * 10 + round);
                    throw ShapeFailure{5};
                }
                checksum += got[r];
            }
        }

        std::vector<int> spread(num_peers);
        for (FMI::Utils::peer_num r = 0; r < num_peers; r++) {
            spread[r] = static_cast<int>(r) * 100 + round;
        }
        FMI::Comm::Data<std::vector<int>> ssend(spread);
        std::vector<int> sslot(1, -1);
        FMI::Comm::Data<std::vector<int>> srecv(sslot);
        comm.scatter(ssend, srecv, 0);
        if (srecv.get()[0] != static_cast<int>(peer_id) * 100 + round) {
            std::printf("rank %u: SCATTER MISMATCH round %d: got %d want %d\n",
                        peer_id, round, srecv.get()[0], static_cast<int>(peer_id) * 100 + round);
            throw ShapeFailure{6};
        }
        checksum += srecv.get()[0];

        // --- a large vector allreduce ------------------------------------------------
        if (payload_ints > 1) {
            std::vector<int> chunk(payload_ints);
            for (int j = 0; j < payload_ints; j++) {
                chunk[j] = static_cast<int>(peer_id) + j;
            }
            FMI::Comm::Data<std::vector<int>> vsend(chunk);
            FMI::Comm::Data<std::vector<int>> vrecv(std::vector<int>(payload_ints, 0));
            comm.allreduce(vsend, vrecv, vsum);
            const auto got = vrecv.get();
            for (int j = 0; j < payload_ints; j++) {
                const int want = static_cast<int>(num_peers) * j
                               + static_cast<int>(num_peers * (num_peers - 1) / 2);
                if (got[j] != want) {
                    std::printf("rank %u: VECTOR MISMATCH round %d slot %d: got %d want %d\n",
                                peer_id, round, j, got[j], want);
                    throw ShapeFailure{8};
                }
            }
            checksum += got[0] + got[payload_ints - 1];
        }

        // --- scan --------------------------------------------------------------------
        FMI::Comm::Data<int> scan_in(static_cast<int>(peer_id) + 1);
        FMI::Comm::Data<int> scan_out(0);
        comm.scan(scan_in, scan_out, sum);
        const int want_scan = static_cast<int>((peer_id + 1) * (peer_id + 2) / 2);
        if (scan_out.get() != want_scan) {
            std::printf("rank %u: SCAN MISMATCH round %d: got %d want %d\n",
                        peer_id, round, scan_out.get(), want_scan);
            throw ShapeFailure{7};
        }
        checksum += scan_out.get();

        if (print_every > 0 && round % print_every == 0) {
            std::printf("rank %u: round %d ok checksum=%lld\n", peer_id, round, checksum);
        }
        if (ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        }
    }

    return static_cast<unsigned long long>(checksum);
}

FMI_REGISTER_SHAPE("baseline",
                   "every primitive once per round: barrier, bcast, p2p ring, allreduce, "
                   "ordered reduce, gather, scatter, bulk vector allreduce, scan",
                   run_baseline)

} // namespace
} // namespace FMI::Runbooks::Checkpoint
