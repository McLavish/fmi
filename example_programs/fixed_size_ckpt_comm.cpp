#include <cstddef>
#include <cstdint>
#include <iostream>

#include <Communicator.h>

#include "fixed_size_ckpt_comm.hpp"
#include "fixed_size_payload.hpp"
#include "harness.hpp"

static uint32_t fixed_size_ckpt_comm(void *args, uint32_t, void *res) {
    auto *in = static_cast<fixed_size_ckpt_comm_input *>(args);
    auto *out = static_cast<fixed_size_ckpt_comm_output *>(res);

    int rank = in->function_id;
    int comm_size = in->world_size;
    long size_mb = in->size_mb;
    long num_all_reduces = in->num_all_reduces;

    out->function_id = rank;
    out->all_ok = true;
    out->done_all_reduces = 0;

    FMI::Communicator comm(rank, comm_size, fmi_examples::config_path(), fmi_examples::comm_name());
    // comm.barrier();
    std::cout << "Function " << rank << " established communicator" << std::endl;

    std::size_t bytes = fixed_size_payload::mb_to_bytes(size_mb);
    void *payload = fixed_size_payload::alloc_and_fill(bytes);
    std::cout << "Function " << rank << " allocated and filled " << size_mb << " MiB payload" << std::endl;

    const int expected = comm_size * (comm_size - 1) / 2;
    FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);

    std::cout << "Function " << rank << " starting " << num_all_reduces << " all_reduces" << std::endl;
    for (long i = 0; i < num_all_reduces; ++i) {
        FMI::Comm::Data<int> sendbuf(rank);
        FMI::Comm::Data<int> recvbuf(-1);
        comm.allreduce(sendbuf, recvbuf, fadd);

        if (recvbuf.get() != expected)
            out->all_ok = false;
        out->done_all_reduces++;

        if ((i + 1) % 100000 == 0)
            std::cout << "Function " << rank << " completed " << (i + 1) << "/" << num_all_reduces << " all_reduces"
                      << std::endl;
    }

    fixed_size_payload::free_payload(payload, bytes);
    std::cout << "Function " << rank << " done" << std::endl;
    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(fixed_size_ckpt_comm_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;
    flags.add_int("size-mb", 0, "Size of the per-rank memory ballast in MiB");
    flags.add_int("num-all-reduces", 1000000, "Number of all_reduce iterations");

    return fmi_examples::run(argc, argv, "fixed_size_ckpt_comm", flags, [](const fmi_examples::Options &opts) {
        fmi_examples::ExampleSpec spec;
        spec.name = "fixed_size_ckpt_comm";
        spec.input_size = sizeof(fixed_size_ckpt_comm_input);
        spec.output_size = sizeof(fixed_size_ckpt_comm_output);
        spec.get_context = [] { return _fixed_size_ckpt_comm::get_context(); };
        spec.free_context = [](void *ctx) { _fixed_size_ckpt_comm::free_context(ctx); };

        long size_mb = static_cast<long>(opts.flags.get_long("size-mb"));
        long num_all_reduces = static_cast<long>(opts.flags.get_long("num-all-reduces"));

        spec.initialize_input = [size_mb, num_all_reduces](int fn, int nc, void *, char *ptr) {
            _fixed_size_ckpt_comm::fill_input(fn, nc, size_mb, num_all_reduces, ptr);
        };
        spec.check_output = [num_all_reduces](int fn, int, void *, char *ptr) {
            return _fixed_size_ckpt_comm::verify_output(fn, num_all_reduces, ptr);
        };
        spec.fn = fixed_size_ckpt_comm;
        return spec;
    });
}
