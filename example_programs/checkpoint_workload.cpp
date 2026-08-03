#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include <Communicator.h>

#include "checkpoint_workload.hpp"
#include "harness.hpp"

static uint32_t checkpoint_workload(void *args, uint32_t, void *res) {
    auto *in = static_cast<checkpoint_workload_input *>(args);
    auto *out = static_cast<checkpoint_workload_output *>(res);

    int rank = in->function_id;
    int comm_size = in->world_size;
    int K = in->num_iterations;
    int S = in->sleep_seconds;
    int C = in->num_collectives;

    out->function_id = rank;
    out->all_ok = true;
    out->done_iterations = 0;

    FMI::Communicator comm(rank, comm_size, fmi_examples::config_path(), fmi_examples::comm_name());
    // comm.barrier();
    std::cout << "Function " << rank << " established communicator" << std::endl;

    for (int i = 0; i < K; i++) {
        std::cout << "Function " << rank << " iteration " << i + 1 << "/" << K << ": sleeping " << S
                  << "s" << std::endl;

        std::this_thread::sleep_for(std::chrono::seconds(S));

        for (int c = 0; c < C; c++) {
            FMI::Comm::Data<int> sendbuf(rank);
            FMI::Comm::Data<int> recvbuf(-1);
            FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);
            comm.allreduce(sendbuf, recvbuf, fadd);

            int expected = comm_size * (comm_size - 1) / 2;
            if (recvbuf.get() != expected) {
                std::cout << "Function " << rank << " collective " << c + 1 << " wrong: expected "
                          << expected << " got " << recvbuf.get() << std::endl;
                out->all_ok = false;
            }
        }

        out->done_iterations++;
        std::cout << "Function " << rank << " completed iteration " << i + 1 << "/" << K
                  << std::endl;
    }

    std::cout << "Function " << rank << " done" << std::endl;
    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(checkpoint_workload_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;
    flags.add_int("num-iterations", 1, "Number of workload iterations");
    flags.add_int("sleep-seconds", 0, "Seconds to sleep per iteration");
    flags.add_int("num-collectives", 0, "Number of allreduces per iteration");

    return fmi_examples::run(argc, argv, "checkpoint_workload", flags,
                             [](const fmi_examples::Options &opts) {
                                 fmi_examples::ExampleSpec spec;
                                 spec.name = "checkpoint_workload";
                                 spec.input_size = sizeof(checkpoint_workload_input);
                                 spec.output_size = sizeof(checkpoint_workload_output);
                                 spec.get_context = [] { return _checkpoint_workload::get_context(); };
                                 spec.free_context = [](void *ctx) {
                                     _checkpoint_workload::free_context(ctx);
                                 };
                                 int K = opts.flags.get_int("num-iterations");
                                 int S = opts.flags.get_int("sleep-seconds");
                                 int C = opts.flags.get_int("num-collectives");
                                 spec.initialize_input = [K, S, C](int fn, int nc, void *, char *ptr) {
                                     _checkpoint_workload::fill_input(fn, nc, K, S, C, ptr);
                                 };
                                 spec.check_output = [K](int fn, int, void *, char *ptr) {
                                     return _checkpoint_workload::verify_output(fn, K, ptr);
                                 };
                                 spec.fn = checkpoint_workload;
                                 return spec;
                             });
}
