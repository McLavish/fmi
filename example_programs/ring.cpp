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
    flags.add_int32("num-iterations", 1, "Number of ring iterations");

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "ring", flags, opts, exit_code)) {
        return exit_code;
    }

    // Rank 0 sends to a hardcoded peer 1 and receives from ranks-1, so with a single rank it would
    // send to a peer that does not exist and then wait on a receive from itself until the backend
    // timeout fires. Checked before the launcher branch so both the launcher and an explicit
    // --rank process reject the run.
    if (opts.ranks < 2) {
        std::cerr << "ring requires at least 2 ranks (rank 0 sends to peer 1)" << std::endl;
        return 2;
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

        std::cout << "rank " << rank << ": established communicator, starting " << iterations << " ring iterations"
                  << std::endl;

        int last_recvd = -1;
        for (int i = 0; i < iterations; i++) {
            FMI::Comm::Data<int> sendbuf(rank);
            FMI::Comm::Data<int> recvbuf(-1);

            if (rank == 0) {
                comm.send(sendbuf, 1);
                comm.recv(recvbuf, ranks - 1);
            } else {
                comm.recv(recvbuf, (rank - 1 + ranks) % ranks);
                comm.send(sendbuf, (rank + 1) % ranks);
            }

            last_recvd = recvbuf.get();
            std::cout << "rank " << rank << ": iteration " << i << " recvd " << last_recvd << std::endl;
        }

        ok = compare(rank, "last_recvd", (rank - 1 + ranks) % ranks, last_recvd);

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
