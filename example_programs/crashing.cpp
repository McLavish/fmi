#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <stdexcept>
#include <thread>

#include <Communicator.h>

#include "crashing.hpp"
#include "harness.hpp"

// This example exists to fail: every iteration has a 1% chance of throwing. The harness
// catches the throw in the forked rank, prints "Rank i crashed: Random crash" and reports
// EXAMPLE crashing: FAIL (exit 1). That FAIL is the designed observable, not a port bug.
// Note that a crashed rank also leaves the surviving ranks blocked in allreduce until the
// harness timeout SIGKILLs them.
static uint32_t crashing(void *args, uint32_t, void *res) {
    srand(time(nullptr));

    crashing_input *in = static_cast<crashing_input *>(args);
    crashing_output *out = static_cast<crashing_output *>(res);

    int rank = in->function_id;
    int comm_size = in->world_size;
    int num_iterations = in->num_iterations;

    out->function_id = rank;

    FMI::Communicator comm(rank, comm_size, fmi_examples::config_path(), fmi_examples::comm_name());
    // comm.barrier();
    std::cout << "Function " << rank << " established communicator!" << std::endl;

    for (int i = 0; i < num_iterations; i++) {
        std::cout << "Function " << rank << " preparing to run iteration " << i + 1 << std::endl;

        // 1% chance to crash
        int rand_crash = rand() % 100;
        if (rand_crash == 0)
            throw std::runtime_error("Random crash");

        FMI::Comm::Data<int> sendbuf(rank);
        FMI::Comm::Data<int> recvbuf(-1);
        FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);

        comm.allreduce(sendbuf, recvbuf, fadd);

        int expected = comm_size * (comm_size - 1) / 2;
        if (recvbuf.get() != expected)
            std::cout << "Function " << rank << " got incorrect result: expected " << expected << ", got "
                      << recvbuf.get() << std::endl;
        else
            std::cout << "Function " << rank << " got correct result" << std::endl;
    }

    std::cout << "Function " << rank << " is done communicating!" << std::endl;

    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(crashing_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;
    flags.add_int("num-iterations", _crashing::NUM_ITERATIONS, "Number of allreduce iterations per rank");

    return fmi_examples::run(argc, argv, "crashing", flags, [](const fmi_examples::Options &opts) {
        fmi_examples::ExampleSpec spec;
        spec.name = "crashing";
        spec.input_size = sizeof(crashing_input);
        spec.output_size = sizeof(crashing_output);
        spec.get_context = [] { return _crashing::get_context(); };
        spec.free_context = [](void *ctx) { _crashing::free_context(ctx); };
        int K = opts.flags.get_int("num-iterations");
        spec.initialize_input = [K](int func_num, int numcores, void *ctx, char *ptr) {
            _crashing::fill_input(func_num, numcores, K, ptr);
        };
        spec.check_output = [](int func_num, int numcores, void *ctx, char *ptr) {
            return _crashing::check_output(func_num, numcores, ctx, ptr);
        };
        spec.fn = crashing;
        return spec;
    });
}
