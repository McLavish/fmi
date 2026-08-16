#include <Communicator.h>

#include <cstdio>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include "launcher.hpp"
#include "util.hpp"

namespace {

    /*!
     * Element-wise comparison against an expected vector.
     *
     * util.hpp's compare<T> streams both values on mismatch, so it cannot be instantiated for
     * std::vector<int> (no operator<<). The size is checked first so the element loop never
     * indexes past the received vector.
     */
    bool compare_vector(int rank, const std::string &field, const std::vector<int> &expected,
                        const std::vector<int> &got) {
        if (!compare(rank, field + ".size", static_cast<int>(expected.size()), static_cast<int>(got.size()))) {
            return false;
        }
        bool ok = true;
        for (std::size_t i = 0; i < expected.size(); i++) {
            ok &= compare(rank, field + "[" + std::to_string(i) + "]", expected[i], got[i]);
        }
        return ok;
    }

} // namespace

int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "communicating", opts, exit_code)) {
        return exit_code;
    }

    // The ring phase sends to a hardcoded peer 1 and receives from ranks-1, so with a single rank
    // it would send to a peer that does not exist and then wait on a receive from itself until the
    // backend timeout fires. Checked before the launcher branch so both the launcher and an
    // explicit --rank process reject the run.
    if (opts.ranks < 2) {
        std::cerr << "communicating requires at least 2 ranks (the ring phase sends to peer 1)" << std::endl;
        return 2;
    }

    if (!opts.single_rank()) {
        return fmi_examples::spawn_local(argc, argv, opts);
    }

    const int rank = opts.rank;
    const int ranks = opts.ranks;
    bool ok = true;

    try {
        FMI::Communicator comm(rank, ranks, fmi_examples::config_path(), fmi_examples::comm_name(),
                               fmi_examples::faas_memory());

        std::cout << "rank " << rank << ": established communicator" << std::endl;

        std::cout << "rank " << rank << ": preparing to run ring" << std::endl;

        // P2P: send, recv in a ring
        {
            FMI::Comm::Data<int> sendbuf(rank);
            FMI::Comm::Data<int> recvbuf(-1);
            if (rank == 0) {
                comm.send(sendbuf, 1);
                comm.recv(recvbuf, ranks - 1);
            } else {
                comm.recv(recvbuf, (rank - 1 + ranks) % ranks);
                comm.send(sendbuf, (rank + 1) % ranks);
            }

            ok &= compare(rank, "recvd_ring_msg", (rank - 1 + ranks) % ranks, recvbuf.get());
        }

        std::cout << "rank " << rank << ": preparing to run bcast" << std::endl;

        // bcast
        {
            std::vector<int> v(ranks, -1);
            if (rank == 0) {
                for (int i = 0; i < ranks; ++i)
                    v[i] = i;
            }

            FMI::Comm::Data<std::vector<int>> bcastbuf(std::move(v));
            comm.bcast(bcastbuf, 0);

            std::vector<int> expected(ranks);
            for (int i = 0; i < ranks; ++i)
                expected[i] = i;
            ok &= compare_vector(rank, "bcast", expected, bcastbuf.get());
        }

        std::cout << "rank " << rank << ": preparing to run gather" << std::endl;

        // gather
        {
            std::vector<int> v = {rank};
            FMI::Comm::Data<std::vector<int>> sendbuf(std::move(v));
            FMI::Comm::Data<std::vector<int>> recvbuf(std::vector<int>((rank == 0) ? ranks : 0));

            comm.gather(sendbuf, recvbuf, 0);

            if (rank == 0) {
                std::vector<int> expected(ranks);
                for (int i = 0; i < ranks; ++i)
                    expected[i] = i;
                ok &= compare_vector(rank, "gather", expected, recvbuf.get());
            }
        }

        std::cout << "rank " << rank << ": preparing to run scatter" << std::endl;

        // scatter
        {
            std::vector<int> v(ranks, -1);
            for (int i = 0; i < ranks; ++i)
                v[i] = i;

            FMI::Comm::Data<std::vector<int>> scatterbuf(std::move(v));
            FMI::Comm::Data<std::vector<int>> recvbuf(std::vector<int>(1, -1));

            comm.scatter(scatterbuf, recvbuf, 0);

            ok &= compare_vector(rank, "scatter", std::vector<int>(1, rank), recvbuf.get());
        }

        std::cout << "rank " << rank << ": preparing to run reduce, allreduce, scan" << std::endl;

        // reduce, allreduce and scan
        {
            FMI::Comm::Data<int> sendbuf_reduce(rank), sendbuf_allreduce(rank), sendbuf_scan(rank);
            FMI::Comm::Data<int> recvbuf_reduce(-1), recvbuf_allreduce(-1), recvbuf_scan(-1);

            FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);

            comm.reduce(sendbuf_reduce, recvbuf_reduce, 0, fadd);
            // Only the root receives the result; both backends leave a non-root recvbuf untouched.
            ok &= compare(rank, "result_reduce", (rank == 0) ? ((ranks - 1) * ranks / 2) : -1, recvbuf_reduce.get());

            comm.allreduce(sendbuf_allreduce, recvbuf_allreduce, fadd);
            ok &= compare(rank, "result_allreduce", (ranks - 1) * ranks / 2, recvbuf_allreduce.get());

            comm.scan(sendbuf_scan, recvbuf_scan, fadd);
            ok &= compare(rank, "result_scan", rank * (rank + 1) / 2, recvbuf_scan.get());
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
