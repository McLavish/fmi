#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include <Communicator.h>

#include "harness.hpp"
#include "long_communicating_checkpoint.hpp"

static uint32_t long_communicating_checkpoint(void *args, uint32_t, void *res) {
    long_communicating_input *in = static_cast<long_communicating_input *>(args);
    long_communicating_output *out = static_cast<long_communicating_output *>(res);

    int rank = in->function_id;
    int comm_size = in->world_size;
    int num_iterations = in->num_iterations;

    out->function_id = rank;
    out->all_ok = true;
    out->done_iterations = 0;

    FMI::Communicator comm(rank, comm_size, fmi_examples::config_path(), fmi_examples::comm_name(),
                           fmi_examples::faas_memory());
    // comm.barrier();
    std::cout << "Function " << rank << " established communicator!" << std::endl;

    for (int i = 0; i < num_iterations; i++) {
        std::cout << "Function " << rank << " preparing to run iteration " << i + 1 << std::endl;

        FMI::Comm::Data<int> sendbuf(rank);
        FMI::Comm::Data<int> recvbuf(-1);
        FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);

        comm.allreduce(sendbuf, recvbuf, fadd);

        int expected = comm_size * (comm_size - 1) / 2;
        if (recvbuf.get() != expected) {
            std::cout << "Function " << rank << " got incorrect result: expected " << expected << ", got "
                      << recvbuf.get() << std::endl;
            out->all_ok = false;
        } else
            std::cout << "Function " << rank << " got correct result" << std::endl;

        out->done_iterations++;
    }

    std::cout << "Function " << rank << " is done communicating!" << std::endl;

    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(long_communicating_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;
    flags.add_int("num-iterations", _long_communicating::NUM_ITERATIONS, "Number of allreduce iterations");

    return fmi_examples::run(argc, argv, "long_communicating_checkpoint", flags,
                             [](const fmi_examples::Options &opts) {
                                 fmi_examples::ExampleSpec spec;
                                 spec.name = "long_communicating_checkpoint";
                                 spec.input_size = sizeof(long_communicating_input);
                                 spec.output_size = sizeof(long_communicating_output);
                                 spec.get_context = [] { return _long_communicating::get_context(); };
                                 spec.free_context = [](void *ctx) { _long_communicating::free_context(ctx); };

                                 int num_iterations = opts.flags.get_int("num-iterations");

                                 spec.initialize_input = [num_iterations](int func_num, int numcores, void *,
                                                                          char *ptr) {
                                     _long_communicating::fill_input(func_num, numcores, num_iterations, ptr);
                                 };
                                 spec.check_output = [num_iterations](int func_num, int numcores, void *ctx,
                                                                      char *ptr) {
                                     return _long_communicating::verify_output(func_num, numcores, num_iterations, ctx,
                                                                               ptr);
                                 };
                                 spec.fn = long_communicating_checkpoint;
                                 return spec;
                             });
}
