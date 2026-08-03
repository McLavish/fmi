#pragma once

#include "util.hpp"

struct mixed_workload_input {
    int function_id;
    int world_size;
    int num_iterations;
    int sleep_min;
    int sleep_max;
};

struct mixed_workload_output {
    int function_id;
    int done_iterations;
    bool all_ok;
};

namespace _mixed_workload {

    inline void *get_context() {
        return nullptr;
    }

    inline void free_context(void *) {}

    inline void fill_input(int func_num, int numcores, int K, int sleep_min, int sleep_max, char *ptr) {
        auto *args = reinterpret_cast<mixed_workload_input *>(ptr);
        args->function_id = func_num;
        args->world_size = numcores;
        args->num_iterations = K;
        args->sleep_min = sleep_min;
        args->sleep_max = sleep_max;
    }

    inline bool verify_output(int func_num, int K, char *ptr) {
        auto *res = reinterpret_cast<mixed_workload_output *>(ptr);
        bool correct = compare(func_num, "function_id", func_num, res->function_id);
        correct &= compare(func_num, "done_iterations", K, res->done_iterations);
        correct &= compare(func_num, "all_ok", true, res->all_ok);
        return correct;
    }

} // namespace _mixed_workload
