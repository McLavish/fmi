#pragma once

#include "util.hpp"

struct checkpoint_workload_input {
    int function_id;
    int world_size;
    int num_iterations;
    int sleep_seconds;
    int num_collectives;
};

struct checkpoint_workload_output {
    int function_id;
    int done_iterations;
    bool all_ok;
};

namespace _checkpoint_workload {

    inline void *get_context() {
        return nullptr;
    }

    inline void free_context(void *) {}

    inline void fill_input(int func_num, int numcores, int K, int S, int C, char *ptr) {
        auto *args = reinterpret_cast<checkpoint_workload_input *>(ptr);
        args->function_id = func_num;
        args->world_size = numcores;
        args->num_iterations = K;
        args->sleep_seconds = S;
        args->num_collectives = C;
    }

    inline bool verify_output(int func_num, int K, char *ptr) {
        auto *res = reinterpret_cast<checkpoint_workload_output *>(ptr);
        bool correct = compare(func_num, "function_id", func_num, res->function_id);
        correct &= compare(func_num, "done_iterations", K, res->done_iterations);
        correct &= compare(func_num, "all_ok", true, res->all_ok);
        return correct;
    }

} // namespace _checkpoint_workload
