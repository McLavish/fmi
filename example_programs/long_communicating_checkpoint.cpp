#include <Communicator.h>

#include <cstdio>
#include <exception>
#include <iostream>
#include <string>

#include "launcher.hpp"
#include "util.hpp"

int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    fmi_examples::Flags flags;
    flags.add_int32("num-iterations", 1000, "Number of allreduce iterations");

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "long_communicating_checkpoint", flags, opts, exit_code)) {
        return exit_code;
    }

    if (!opts.single_rank()) {
        return fmi_examples::spawn_local(argc, argv, opts);
    }

    const int rank = opts.rank;
    const int ranks = opts.ranks;
    const int iterations = opts.flags.get_int("num-iterations");
    bool ok = true;

    try {
        FMI::Communicator comm(rank, ranks, fmi_examples::config_path(), fmi_examples::comm_name(),
                               fmi_examples::faas_memory());

        std::cout << "rank " << rank << ": established communicator!" << std::endl;

        bool all_ok = true;
        int done_iterations = 0;

        for (int i = 0; i < iterations; i++) {
            std::cout << "rank " << rank << ": preparing to run iteration " << i + 1 << std::endl;

            FMI::Comm::Data<int> sendbuf(rank);
            FMI::Comm::Data<int> recvbuf(-1);
            FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);

            comm.allreduce(sendbuf, recvbuf, fadd);

            int expected = ranks * (ranks - 1) / 2;
            if (recvbuf.get() != expected) {
                std::cout << "rank " << rank << ": got incorrect result: expected " << expected << ", got "
                          << recvbuf.get() << std::endl;
                all_ok = false;
            } else
                std::cout << "rank " << rank << ": got correct result" << std::endl;

            done_iterations++;
        }

        std::cout << "rank " << rank << ": is done communicating!" << std::endl;

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
