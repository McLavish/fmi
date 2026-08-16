#include <Communicator.h>

#include <chrono>
#include <cstdio>
#include <exception>
#include <iostream>
#include <random>
#include <string>
#include <thread>

#include "launcher.hpp"
#include "util.hpp"

int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    fmi_examples::Flags flags;
    flags.add_int32("num-iterations", 5, "Number of mixed sleep/P2P/allreduce iterations");
    flags.add_int32("sleep-min", 1, "Minimum sleep duration per phase, in seconds");
    flags.add_int32("sleep-max", 3, "Maximum sleep duration per phase, in seconds");

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "mixed_workload", flags, opts, exit_code)) {
        return exit_code;
    }

    // The P2P phase pairs rank i with rank ranks-1-i (the GapRunner pairing), so an odd rank count
    // makes the middle rank its own peer: it would recv from itself before sending and every rank
    // would then die on the backend timeout. Checked before the launcher branch so both the
    // launcher and an explicit --rank process reject the run.
    if (opts.ranks < 2 || opts.ranks % 2 != 0) {
        std::cerr << "mixed_workload pairs rank i with rank ranks-1-i, so it needs an even rank count of at least 2 "
                     "(got "
                  << opts.ranks << ")" << std::endl;
        return 2;
    }

    // std::uniform_int_distribution is undefined for min > max, and a negative sleep bound is
    // meaningless, so reject both instead of letting the kernel run into UB.
    if (opts.flags.get_int("sleep-min") < 0 || opts.flags.get_int("sleep-max") < opts.flags.get_int("sleep-min")) {
        std::cerr << "mixed_workload requires 0 <= --sleep-min <= --sleep-max (got " << opts.flags.get_int("sleep-min")
                  << " and " << opts.flags.get_int("sleep-max") << ")" << std::endl;
        return 2;
    }

    if (!opts.single_rank()) {
        return fmi_examples::spawn_local(argc, argv, opts);
    }

    const int rank = opts.rank;
    const int ranks = opts.ranks;
    const int iterations = opts.flags.get_int("num-iterations");
    const int sleep_min = opts.flags.get_int("sleep-min");
    const int sleep_max = opts.flags.get_int("sleep-max");
    const int peer = ranks - 1 - rank;
    bool ok = true;

    try {
        FMI::Communicator comm(rank, ranks, fmi_examples::config_path(), fmi_examples::comm_name(),
                               fmi_examples::faas_memory());

        std::cout << "rank " << rank << ": established communicator" << std::endl;

        bool all_ok = true;
        int done_iterations = 0;

        std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<int> sleep_dist(sleep_min, sleep_max);

        for (int i = 0; i < iterations; i++) {
            std::cout << "rank " << rank << ": iteration " << i + 1 << "/" << iterations << std::endl;

            // sleep
            std::this_thread::sleep_for(std::chrono::seconds(sleep_dist(rng)));

            // P2P: paired exchange (0<->11, 1<->10, ...)
            {
                FMI::Comm::Data<int> send_data(rank);
                FMI::Comm::Data<int> recv_data(-1);
                if (rank < peer) {
                    comm.send(send_data, peer);
                    comm.recv(recv_data, peer);
                } else {
                    comm.recv(recv_data, peer);
                    comm.send(send_data, peer);
                }
                if (recv_data.get() != peer) {
                    std::cout << "rank " << rank << ": P2P wrong: expected " << peer << " got " << recv_data.get()
                              << std::endl;
                    all_ok = false;
                }
            }

            // sleep
            std::this_thread::sleep_for(std::chrono::seconds(sleep_dist(rng)));

            // allreduce
            {
                FMI::Comm::Data<int> sendbuf(rank);
                FMI::Comm::Data<int> recvbuf(-1);
                FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);
                comm.allreduce(sendbuf, recvbuf, fadd);

                int expected = ranks * (ranks - 1) / 2;
                if (recvbuf.get() != expected) {
                    std::cout << "rank " << rank << ": allreduce wrong: expected " << expected << " got "
                              << recvbuf.get() << std::endl;
                    all_ok = false;
                }
            }

            done_iterations++;
        }

        std::cout << "rank " << rank << ": done" << std::endl;

        ok = compare(rank, "done_iterations", iterations, done_iterations);
        ok &= compare(rank, "all_ok", true, all_ok);

        fmi_examples::teardown_sync(comm, opts);
    } catch (const std::exception &e) {
        std::cout << "rank " << rank << " crashed: " << e.what() << std::endl;
        ok = false;
    } catch (const std::string &s) {
        std::cout << "rank " << rank << " crashed: " << s << std::endl;
        ok = false;
    } catch (...) {
        std::cout << "rank " << rank << " crashed: unknown exception" << std::endl;
        ok = false;
    }

    std::cout << "rank " << rank << ": " << (ok ? "PASS" : "FAIL") << std::endl;
    return ok ? 0 : 1;
}
