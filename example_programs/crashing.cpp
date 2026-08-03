#include <Communicator.h>

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>

#include "launcher.hpp"

// This example exists to fail: every iteration has a 1% chance of throwing. The rank's own
// catch block prints "rank R crashed: Random crash" and the rank reports FAIL (exit 1), which
// the launcher turns into "EXAMPLE crashing: FAIL". That FAIL is the designed observable, not
// a port bug. Note that a crashed rank also leaves the surviving ranks blocked in allreduce
// until the launcher timeout SIGKILLs them.
int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    fmi_examples::Flags flags;
    flags.add_int("num-iterations", 1000, "Number of allreduce iterations per rank");

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "crashing", flags, opts, exit_code)) {
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
        srand(time(nullptr));

        FMI::Communicator comm(rank, ranks, fmi_examples::config_path(), fmi_examples::comm_name(),
                               fmi_examples::faas_memory());

        std::cout << "rank " << rank << ": established communicator" << std::endl;

        for (int i = 0; i < iterations; i++) {
            std::cout << "rank " << rank << ": preparing to run iteration " << i + 1 << std::endl;

            // 1% chance to crash
            int rand_crash = rand() % 100;
            if (rand_crash == 0)
                throw std::runtime_error("Random crash");

            FMI::Comm::Data<int> sendbuf(rank);
            FMI::Comm::Data<int> recvbuf(-1);
            FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);

            comm.allreduce(sendbuf, recvbuf, fadd);

            int expected = ranks * (ranks - 1) / 2;
            if (recvbuf.get() != expected)
                std::cout << "rank " << rank << ": got incorrect result: expected " << expected << ", got "
                          << recvbuf.get() << std::endl;
            else
                std::cout << "rank " << rank << ": got correct result" << std::endl;
        }

        std::cout << "rank " << rank << ": done communicating" << std::endl;

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
