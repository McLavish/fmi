#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <Communicator.h>

#include "communicating.hpp"
#include "harness.hpp"

static uint32_t communicating(void *args, uint32_t, void *res) {
    communicating_input *in = static_cast<communicating_input *>(args);
    communicating_output *out = static_cast<communicating_output *>(res);

    int rank = in->function_id;
    int comm_size = in->world_size;

    out->function_id = rank;
    int offset = 0;
    memset(out->buffer, 0, sizeof(out->buffer));

    FMI::Communicator comm(rank, comm_size, fmi_examples::config_path(), fmi_examples::comm_name());
    // comm.barrier();

    std::cout << "Function " << rank << " established communicator!" << std::endl;

    std::cout << "Function " << rank << " preparing to run ring" << std::endl;

    // P2P: send, recv in a ring
    {
        FMI::Comm::Data<int> sendbuf(rank);
        FMI::Comm::Data<int> recvbuf(-1);
        if (rank == 0) {
            comm.send(sendbuf, 1);
            comm.recv(recvbuf, comm_size - 1);
        } else {
            comm.recv(recvbuf, (rank - 1 + comm_size) % comm_size);
            comm.send(sendbuf, (rank + 1) % comm_size);
        }

        out->recvd_ring_msg = recvbuf.get();
    }

    std::cout << "Function " << rank << " preparing to run bcast" << std::endl;

    // bcast
    {
        std::vector<int> v(comm_size, -1);
        if (rank == 0) {
            for (int i = 0; i < comm_size; ++i)
                v[i] = i;
        }

        FMI::Comm::Data<std::vector<int>> bcastbuf(std::move(v));
        comm.bcast(bcastbuf, 0);

        memcpy(out->buffer + offset, bcastbuf.data(), comm_size * sizeof(int));
        offset += comm_size;
    }

    std::cout << "Function " << rank << " preparing to run gather" << std::endl;

    // gather
    {
        std::vector<int> v = {rank};
        FMI::Comm::Data<std::vector<int>> sendbuf(std::move(v));
        FMI::Comm::Data<std::vector<int>> recvbuf(std::vector<int>((rank == 0) ? comm_size : 0));

        comm.gather(sendbuf, recvbuf, 0);

        if (rank == 0)
            memcpy(out->buffer + offset, recvbuf.data(), comm_size * sizeof(int));
        offset += comm_size;
    }

    std::cout << "Function " << rank << " preparing to run scatter" << std::endl;

    // scatter
    {
        std::vector<int> v(comm_size, -1);
        for (int i = 0; i < comm_size; ++i)
            v[i] = i;

        FMI::Comm::Data<std::vector<int>> scatterbuf(std::move(v));
        FMI::Comm::Data<std::vector<int>> recvbuf(std::vector<int>(1, -1));

        comm.scatter(scatterbuf, recvbuf, 0);

        out->recvd_scatter_msg = recvbuf.get()[0];
    }

    std::cout << "Function " << rank << " preparing to run reduce, allreduce, scan" << std::endl;

    // reduce, allreduce and scan
    {
        FMI::Comm::Data<int> sendbuf_reduce(rank), sendbuf_allreduce(rank), sendbuf_scan(rank);
        FMI::Comm::Data<int> recvbuf_reduce(-1), recvbuf_allreduce(-1), recvbuf_scan(-1);

        FMI::Utils::Function<int> fadd([](int a, int b) -> int { return a + b; }, true, true);

        comm.reduce(sendbuf_reduce, recvbuf_reduce, 0, fadd);
        out->result_reduce = recvbuf_reduce.get();

        comm.allreduce(sendbuf_allreduce, recvbuf_allreduce, fadd);
        out->result_allreduce = recvbuf_allreduce.get();

        comm.scan(sendbuf_scan, recvbuf_scan, fadd);
        out->result_scan = recvbuf_scan.get();
    }

    std::cout << "Function " << rank << " is done communicating!" << std::endl;

    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(communicating_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;

    return fmi_examples::run(argc, argv, "communicating", flags, [](const fmi_examples::Options &opts) {
        // communicating_output::buffer holds the bcast result followed by the gather result,
        // so more than 50 ranks would write past its 100 ints.
        if (opts.ranks > _communicating::MAX_COMMUNICATING_RANKS) {
            throw std::runtime_error("communicating supports at most " +
                                     std::to_string(_communicating::MAX_COMMUNICATING_RANKS) +
                                     " ranks (output buffer holds 2 * ranks ints)");
        }

        fmi_examples::ExampleSpec spec;
        spec.name = "communicating";
        spec.input_size = sizeof(communicating_input);
        spec.output_size = sizeof(communicating_output);
        spec.get_context = [] { return _communicating::get_context(); };
        spec.free_context = [](void *ctx) { _communicating::free_context(ctx); };
        spec.initialize_input = [](int func_num, int numcores, void *ctx, char *ptr) {
            _communicating::initialize_input(func_num, numcores, ctx, ptr);
        };
        spec.check_output = [](int func_num, int numcores, void *ctx, char *ptr) {
            return _communicating::check_output(func_num, numcores, ctx, ptr);
        };
        spec.fn = communicating;
        return spec;
    });
}
