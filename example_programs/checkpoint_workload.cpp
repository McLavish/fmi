#include <Communicator.h>

#include <chrono>
#include <cstdio>
#include <exception>
#include <iostream>
#include <string>
#include <thread>

#include "launcher.hpp"
#include "util.hpp"

int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    fmi_examples::Flags flags;
    flags.add_int("num-iterations", 1, "Number of workload iterations");
    flags.add_int("sleep-seconds", 0, "Seconds to sleep per iteration");
    flags.add_int("num-collectives", 0, "Number of allreduces per iteration");

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "checkpoint_workload", flags, opts, exit_code)) {
        return exit_code;
    }

    // No example-specific guard: the kernel only ever calls allreduce, which is well defined for a
    // single rank, and K/S/C of zero simply make the workload empty.

    if (!opts.single_rank()) {
        return fmi_examples::spawn_local(argc, argv, opts);
    }

    const int rank = opts.rank;
    const int comm_size = opts.ranks;
    const int K = opts.flags.get_int("num-iterations");
    const int S = opts.flags.get_int("sleep-seconds");
    const int C = opts.flags.get_int("num-collectives");
    bool ok = true;

    try {
        bool all_ok = true;
        int done_iterations = 0;

        FMI::Communicator comm(rank, comm_size, fmi_examples::config_path(), fmi_examples::comm_name(),
                               fmi_examples::faas_memory());

        std::cout << "rank " << rank << ": established communicator" << std::endl;

        for (int i = 0; i < K; i++) {
            std::cout << "rank " << rank << ": iteration " << i + 1 << "/" << K << ": sleeping " << S << "s"
                      << std::endl;

            std::this_thread::sleep_for(std::chrono::seconds(S));

            for (int c = 0; c < C; c++) {
                FMI::Comm::Data<int> sendbuf(rank);
                FMI::Comm::Data<int> recvbuf(-1);
                FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);
                comm.allreduce(sendbuf, recvbuf, fadd);

                int expected = comm_size * (comm_size - 1) / 2;
                if (recvbuf.get() != expected) {
                    std::cout << "rank " << rank << ": collective " << c + 1 << " wrong: expected " << expected
                              << " got " << recvbuf.get() << std::endl;
                    all_ok = false;
                }
            }

            done_iterations++;
            std::cout << "rank " << rank << ": completed iteration " << i + 1 << "/" << K << std::endl;
        }

        std::cout << "rank " << rank << ": done" << std::endl;

        ok = compare(rank, "all_ok", true, all_ok);
        ok &= compare(rank, "done_iterations", K, done_iterations);

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
