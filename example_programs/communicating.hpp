#pragma once

#include <string>

#include "util.hpp"

struct communicating_input {
    int function_id;
    int world_size;
};

struct communicating_output {
    int function_id;
    int recvd_ring_msg;
    int recvd_scatter_msg;
    int result_reduce;
    int result_allreduce;
    int result_scan;
    int buffer[100];
};

namespace _communicating {

    //! The output buffer holds bcast and gather results side by side, so 2 * ranks <= 100.
    constexpr int MAX_COMMUNICATING_RANKS = 50;

    inline void *get_context() {
        return nullptr;
    }

    inline void free_context(void *context) {}

    inline void initialize_input(int func_num, int numcores, void *context, char *ptr) {
        communicating_input *args = reinterpret_cast<communicating_input *>(ptr);
        args->function_id = func_num;
        args->world_size = numcores;
    }

    inline bool check_output(int func_num, int numcores, void *context, char *ptr) {
        communicating_output *res = reinterpret_cast<communicating_output *>(ptr);

        bool correct = compare(func_num, "function_id", func_num, res->function_id);

        int expected_ring_msg = (func_num - 1 + numcores) % numcores;
        correct &= compare(func_num, "recvd_ring_msg", expected_ring_msg, res->recvd_ring_msg);

        int expected_scatter_msg = func_num;
        correct &= compare(func_num, "recvd_scatter_msg", expected_scatter_msg, res->recvd_scatter_msg);

        int expected_result_reduce = (func_num == 0) ? ((numcores - 1) * numcores / 2) : -1;
        correct &= compare(func_num, "result_reduce", expected_result_reduce, res->result_reduce);

        int expected_result_allreduce = (numcores - 1) * numcores / 2;
        correct &= compare(func_num, "result_allreduce", expected_result_allreduce, res->result_allreduce);

        int expected_result_scan = func_num * (func_num + 1) / 2;
        correct &= compare(func_num, "result_scan", expected_result_scan, res->result_scan);

        for (int i = 0; i < numcores; i++) {
            correct &= compare(func_num, "bcast[" + std::to_string(i) + "]", i, res->buffer[i]);
            correct &=
                compare(func_num, "gather[" + std::to_string(i) + "]", (func_num == 0) ? i : 0, res->buffer[numcores + i]);
        }

        return correct;
    }

} // namespace _communicating
