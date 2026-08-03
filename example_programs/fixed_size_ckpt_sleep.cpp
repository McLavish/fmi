#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <thread>

#include <Communicator.h>

#include "fixed_size_ckpt_sleep.hpp"
#include "fixed_size_payload.hpp"
#include "harness.hpp"

static uint32_t fixed_size_ckpt_sleep(void *args, uint32_t, void *res) {
    auto *in = static_cast<fixed_size_ckpt_sleep_input *>(args);
    auto *out = static_cast<fixed_size_ckpt_sleep_output *>(res);

    int rank = in->function_id;
    int comm_size = in->world_size;
    long size_mb = in->size_mb;
    int sleep_minutes = in->sleep_minutes;

    out->function_id = rank;
    out->all_ok = true;

    FMI::Communicator comm(rank, comm_size, fmi_examples::config_path(), fmi_examples::comm_name());
    // comm.barrier();
    std::cout << "Function " << rank << " established communicator" << std::endl;

    std::size_t bytes = fixed_size_payload::mb_to_bytes(size_mb);
    void *payload = fixed_size_payload::alloc_and_fill(bytes);
    std::cout << "Function " << rank << " allocated and filled " << size_mb << " MiB payload" << std::endl;

    std::cout << "Function " << rank << " sleeping for " << sleep_minutes << " minutes" << std::endl;
    std::this_thread::sleep_for(std::chrono::minutes(sleep_minutes));

    fixed_size_payload::free_payload(payload, bytes);
    std::cout << "Function " << rank << " done" << std::endl;
    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(fixed_size_ckpt_sleep_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;
    flags.add_int("size-mb", 0, "Size of the per-rank memory ballast in MiB");
    flags.add_int("sleep-minutes", 10, "Number of minutes each rank sleeps");

    return fmi_examples::run(argc, argv, "fixed_size_ckpt_sleep", flags, [](const fmi_examples::Options &opts) {
        fmi_examples::ExampleSpec spec;
        spec.name = "fixed_size_ckpt_sleep";
        spec.input_size = sizeof(fixed_size_ckpt_sleep_input);
        spec.output_size = sizeof(fixed_size_ckpt_sleep_output);
        spec.get_context = [] { return _fixed_size_ckpt_sleep::get_context(); };
        spec.free_context = [](void *ctx) { _fixed_size_ckpt_sleep::free_context(ctx); };

        long size_mb = static_cast<long>(opts.flags.get_long("size-mb"));
        int sleep_minutes = opts.flags.get_int("sleep-minutes");

        spec.initialize_input = [size_mb, sleep_minutes](int fn, int nc, void *, char *ptr) {
            _fixed_size_ckpt_sleep::fill_input(fn, nc, size_mb, sleep_minutes, ptr);
        };
        spec.check_output = [](int fn, int, void *, char *ptr) {
            return _fixed_size_ckpt_sleep::verify_output(fn, ptr);
        };
        spec.fn = fixed_size_ckpt_sleep;
        return spec;
    });
}
