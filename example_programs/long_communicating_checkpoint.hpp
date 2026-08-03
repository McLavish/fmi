#pragma once

#include "util.hpp"

struct long_communicating_input {
    int function_id;
    int world_size;
    int num_iterations;
};

struct long_communicating_output {
    int function_id;
    int done_iterations;
    bool all_ok;
};

namespace _long_communicating {

    constexpr int NUM_ITERATIONS = 1000;

    inline void *get_context() {
        return nullptr;
    }

    inline void free_context(void *context) {}

    inline void fill_input(int func_num, int numcores, int num_iterations, char *ptr) {
        long_communicating_input *args = reinterpret_cast<long_communicating_input *>(ptr);
        args->function_id = func_num;
        args->world_size = numcores;
        args->num_iterations = num_iterations;
    }

    inline bool verify_output(int func_num, int numcores, int num_iterations, void *context, char *ptr) {
        long_communicating_output *res = reinterpret_cast<long_communicating_output *>(ptr);

        bool correct = compare(func_num, "function_id", func_num, res->function_id);
        correct &= compare(func_num, "done_iterations", num_iterations, res->done_iterations);
        correct &= compare(func_num, "all_ok", true, res->all_ok);

        return correct;
    }

    inline void initialize_input(int func_num, int numcores, void *context, char *ptr) {
        fill_input(func_num, numcores, NUM_ITERATIONS, ptr);
    }

    inline bool check_output(int func_num, int numcores, void *context, char *ptr) {
        return verify_output(func_num, numcores, NUM_ITERATIONS, context, ptr);
    }

} // namespace _long_communicating
