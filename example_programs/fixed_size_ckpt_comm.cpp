#include <Communicator.h>

#include <cstddef>
#include <cstdio>
#include <exception>
#include <iostream>
#include <string>

#include "fixed_size_payload.hpp"
#include "launcher.hpp"
#include "util.hpp"

int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    fmi_examples::Flags flags;
    flags.add_int("size-mb", 0, "Size of the per-rank memory ballast in MiB");
    flags.add_int("num-all-reduces", 1000000, "Number of all_reduce iterations");

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "fixed_size_ckpt_comm", flags, opts, exit_code)) {
        return exit_code;
    }

    if (!opts.single_rank()) {
        return fmi_examples::spawn_local(argc, argv, opts);
    }

    const int rank = opts.rank;
    const int comm_size = opts.ranks;
    const long size_mb = static_cast<long>(opts.flags.get_long("size-mb"));
    const long long num_all_reduces = opts.flags.get_long("num-all-reduces");
    bool ok = true;

    try {
        bool all_ok = true;
        long long done_all_reduces = 0;

        FMI::Communicator comm(rank, comm_size, fmi_examples::config_path(), fmi_examples::comm_name(),
                               fmi_examples::faas_memory());
        std::cout << "rank " << rank << ": established communicator" << std::endl;

        std::size_t bytes = fixed_size_payload::mb_to_bytes(size_mb);
        void *payload = fixed_size_payload::alloc_and_fill(bytes);
        std::cout << "rank " << rank << ": allocated and filled " << size_mb << " MiB payload" << std::endl;

        const int expected = comm_size * (comm_size - 1) / 2;
        FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);

        std::cout << "rank " << rank << ": starting " << num_all_reduces << " all_reduces" << std::endl;
        for (long long i = 0; i < num_all_reduces; ++i) {
            FMI::Comm::Data<int> sendbuf(rank);
            FMI::Comm::Data<int> recvbuf(-1);
            comm.allreduce(sendbuf, recvbuf, fadd);

            if (recvbuf.get() != expected)
                all_ok = false;
            done_all_reduces++;

            if ((i + 1) % 100000 == 0)
                std::cout << "rank " << rank << ": completed " << (i + 1) << "/" << num_all_reduces << " all_reduces"
                          << std::endl;
        }

        fixed_size_payload::free_payload(payload, bytes);
        std::cout << "rank " << rank << ": done" << std::endl;

        ok &= compare(rank, "done_all_reduces", num_all_reduces, done_all_reduces);
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
