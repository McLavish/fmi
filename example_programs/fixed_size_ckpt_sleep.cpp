#include <Communicator.h>

#include <chrono>
#include <cstddef>
#include <cstdio>
#include <exception>
#include <iostream>
#include <string>
#include <thread>

#include "fixed_size_payload.hpp"
#include "launcher.hpp"

int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    fmi_examples::Flags flags;
    flags.add_int("size-mb", 0, "Size of the per-rank memory ballast in MiB");
    flags.add_int32("sleep-minutes", 10, "Number of minutes each rank sleeps");

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "fixed_size_ckpt_sleep", flags, opts, exit_code)) {
        return exit_code;
    }

    if (!opts.single_rank()) {
        return fmi_examples::spawn_local(argc, argv, opts);
    }

    const int rank = opts.rank;
    const int comm_size = opts.ranks;
    const long size_mb = static_cast<long>(opts.flags.get_long("size-mb"));
    const int sleep_minutes = opts.flags.get_int("sleep-minutes");
    bool ok = true;

    try {
        // The communicator is established but deliberately never used: this example exists to hold
        // a fixed-size resident process (communicator + ballast) alive for a checkpoint window.
        FMI::Communicator comm(rank, comm_size, fmi_examples::config_path(), fmi_examples::comm_name(),
                               fmi_examples::faas_memory());
        std::cout << "rank " << rank << ": established communicator" << std::endl;

        std::size_t bytes = fixed_size_payload::mb_to_bytes(size_mb);
        void *payload = fixed_size_payload::alloc_and_fill(bytes);
        std::cout << "rank " << rank << ": allocated and filled " << size_mb << " MiB payload" << std::endl;

        std::cout << "rank " << rank << ": sleeping for " << sleep_minutes << " minutes" << std::endl;
        std::this_thread::sleep_for(std::chrono::minutes(sleep_minutes));

        fixed_size_payload::free_payload(payload, bytes);
        std::cout << "rank " << rank << ": done" << std::endl;

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
