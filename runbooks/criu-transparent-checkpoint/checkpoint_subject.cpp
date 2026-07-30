// An ORDINARY FMI program. It knows nothing about migration, checkpointing, epochs or the
// link layer: it constructs a Communicator and calls collectives and point-to-point sends,
// exactly as any FMI application would. Everything the checkpoint story needs happens below
// this file.
//
// argv: <peer_id> <num_peers> <config> <comm_name> <rounds> <ms_per_round> [print_every]
//
// Every collective checks its own result inline and exits non-zero on a mismatch, and the
// running checksum lets the sweep compare a checkpointed run against a clean one of the same
// shape. A lost, duplicated or substituted message therefore fails the run rather than being
// absorbed into a plausible-looking answer.
#include <fmi.h>

#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

int main(int argc, char** argv) {
    if (argc < 7) {
        std::fprintf(stderr, "usage: %s <peer_id> <num_peers> <config> <comm> <rounds> <ms>\n", argv[0]);
        return 2;
    }
    const auto peer_id = static_cast<FMI::Utils::peer_num>(std::stoul(argv[1]));
    const auto num_peers = static_cast<FMI::Utils::peer_num>(std::stoul(argv[2]));
    const std::string config = argv[3];
    const std::string comm_name = argv[4];
    const int rounds = std::stoi(argv[5]);
    const int ms = std::stoi(argv[6]);
    const int print_every = argc > 7 ? std::stoi(argv[7]) : 1;
    // Sizes the vector collective below. Anything past a socket buffer makes a single message
    // span many segments, so a freeze can land part way through a payload rather than only
    // between whole messages — the case a small-message workload never reaches.
    const int payload_ints = argc > 8 ? std::stoi(argv[8]) : 1;

    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    std::printf("rank %u: start pid=%d\n", peer_id, (int) ::getpid());

    FMI::Communicator comm(peer_id, num_peers, config, comm_name, 128);
    comm.hint(FMI::Utils::Hint::fast);

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
            return 3;
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
            return 4;
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
                    return 5;
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
            return 6;
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
                    return 8;
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
            return 7;
        }
        checksum += scan_out.get();

        if (print_every > 0 && round % print_every == 0) {
            std::printf("rank %u: round %d ok checksum=%lld\n", peer_id, round, checksum);
        }
        if (ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        }
    }

    std::printf("rank %u: DONE rounds=%d checksum=%lld\n", peer_id, rounds, checksum);
    return 0;
}
