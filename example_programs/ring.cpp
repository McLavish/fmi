#include <Communicator.h>
#include <cstdint>
#include <iostream>

#include "harness.hpp"
#include "ring.hpp"

static uint32_t ring(void *args, uint32_t, void *res) {
    ring_input *in = static_cast<ring_input *>(args);
    ring_output *out = static_cast<ring_output *>(res);

    int rank = in->function_id;
    int comm_size = in->world_size;
    int k = in->num_iterations;

    out->function_id = rank;
    out->last_recvd = -1;

    FMI::Communicator comm(rank, comm_size, fmi_examples::config_path(), fmi_examples::comm_name());

    std::cout << "Function " << rank << " established communicator, starting " << k << " ring iterations" << std::endl;

    for (int i = 0; i < k; i++) {
        FMI::Comm::Data<int> sendbuf(rank);
        FMI::Comm::Data<int> recvbuf(-1);

        if (rank == 0) {
            comm.send(sendbuf, 1);
            comm.recv(recvbuf, comm_size - 1);
        } else {
            comm.recv(recvbuf, (rank - 1 + comm_size) % comm_size);
            comm.send(sendbuf, (rank + 1) % comm_size);
        }

        out->last_recvd = recvbuf.get();
        std::cout << "Function " << rank << " iteration " << i << ": recvd " << out->last_recvd << std::endl;
    }

    std::cout << "Function " << rank << " done with ring" << std::endl;

    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(ring_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;
    flags.add_int("num-iterations", 1, "Number of ring iterations");

    return fmi_examples::run(argc, argv, "ring", flags, [](const fmi_examples::Options &opts) {
        fmi_examples::ExampleSpec spec;
        spec.name = "ring";
        spec.input_size = sizeof(ring_input);
        spec.output_size = sizeof(ring_output);
        spec.get_context = [] { return _ring::get_context(); };
        spec.free_context = [](void *ctx) { _ring::free_context(ctx); };
        int K = opts.flags.get_int("num-iterations");
        spec.initialize_input = [K](int func_num, int numcores, void *, char *ptr) {
            _ring::fill_input(func_num, numcores, K, ptr);
        };
        spec.check_output = [](int func_num, int numcores, void *ctx, char *ptr) {
            return _ring::verify_output(func_num, numcores, ctx, ptr);
        };
        spec.fn = ring;
        return spec;
    });
}
