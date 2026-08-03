#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <thread>

#include <Communicator.h>

#include "harness.hpp"
#include "mixed_workload.hpp"

static uint32_t mixed_workload(void *args, uint32_t, void *res) {
    auto *in = static_cast<mixed_workload_input *>(args);
    auto *out = static_cast<mixed_workload_output *>(res);

    int rank = in->function_id;
    int comm_size = in->world_size;
    int K = in->num_iterations;
    int peer = comm_size - 1 - rank;

    out->function_id = rank;
    out->all_ok = true;
    out->done_iterations = 0;

    FMI::Communicator comm(rank, comm_size, fmi_examples::config_path(), fmi_examples::comm_name());
    // comm.barrier();
    std::cout << "Function " << rank << " established communicator" << std::endl;

    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> sleep_dist(in->sleep_min, in->sleep_max);

    for (int i = 0; i < K; i++) {
        std::cout << "Function " << rank << " iteration " << i + 1 << "/" << K << std::endl;

        // sleep
        std::this_thread::sleep_for(std::chrono::seconds(sleep_dist(rng)));

        // P2P: paired exchange (0<->11, 1<->10, ...)
        {
            FMI::Comm::Data<int> send_data(rank);
            FMI::Comm::Data<int> recv_data(-1);
            if (rank < peer) {
                comm.send(send_data, peer);
                comm.recv(recv_data, peer);
            } else {
                comm.recv(recv_data, peer);
                comm.send(send_data, peer);
            }
            if (recv_data.get() != peer) {
                std::cout << "Function " << rank << " P2P wrong: expected " << peer << " got " << recv_data.get()
                          << std::endl;
                out->all_ok = false;
            }
        }

        // sleep
        std::this_thread::sleep_for(std::chrono::seconds(sleep_dist(rng)));

        // allreduce
        {
            FMI::Comm::Data<int> sendbuf(rank);
            FMI::Comm::Data<int> recvbuf(-1);
            FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);
            comm.allreduce(sendbuf, recvbuf, fadd);

            int expected = comm_size * (comm_size - 1) / 2;
            if (recvbuf.get() != expected) {
                std::cout << "Function " << rank << " allreduce wrong: expected " << expected << " got "
                          << recvbuf.get() << std::endl;
                out->all_ok = false;
            }
        }

        out->done_iterations++;
    }

    std::cout << "Function " << rank << " done" << std::endl;
    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(mixed_workload_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;
    flags.add_int("num-iterations", 5, "Number of mixed sleep/P2P/allreduce iterations");
    flags.add_int("sleep-min", 1, "Minimum sleep duration per phase, in seconds");
    flags.add_int("sleep-max", 3, "Maximum sleep duration per phase, in seconds");

    return fmi_examples::run(argc, argv, "mixed_workload", flags, [](const fmi_examples::Options &opts) {
        fmi_examples::ExampleSpec spec;
        spec.name = "mixed_workload";
        spec.input_size = sizeof(mixed_workload_input);
        spec.output_size = sizeof(mixed_workload_output);
        spec.get_context = [] { return _mixed_workload::get_context(); };
        spec.free_context = [](void *ctx) { _mixed_workload::free_context(ctx); };

        int K = opts.flags.get_int("num-iterations");
        int lo = opts.flags.get_int("sleep-min");
        int hi = opts.flags.get_int("sleep-max");

        spec.initialize_input = [K, lo, hi](int func_num, int numcores, void *, char *ptr) {
            _mixed_workload::fill_input(func_num, numcores, K, lo, hi, ptr);
        };
        spec.check_output = [K](int func_num, int, void *, char *ptr) {
            return _mixed_workload::verify_output(func_num, K, ptr);
        };
        spec.fn = mixed_workload;
        return spec;
    });
}
