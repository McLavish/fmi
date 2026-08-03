#pragma once

#include "util.hpp"

struct crashing_input {
    int function_id;
    int world_size;
    int num_iterations;
};

struct crashing_output {
    int function_id;
};

namespace _crashing {

    constexpr int NUM_ITERATIONS = 1000;

    inline void *get_context() {
        return nullptr;
    }

    inline void free_context(void *context) {}

    // Curried variant used by main() so that NUM_ITERATIONS can be overridden from the CLI.
    inline void fill_input(int func_num, int numcores, int num_iterations, char *ptr) {
        crashing_input *args = reinterpret_cast<crashing_input *>(ptr);
        args->function_id = func_num;
        args->world_size = numcores;
        args->num_iterations = num_iterations;
    }

    inline void initialize_input(int func_num, int numcores, void *context, char *ptr) {
        fill_input(func_num, numcores, NUM_ITERATIONS, ptr);
    }

    inline bool check_output(int func_num, int numcores, void *context, char *ptr) {
        // should have crashed anyway...
        return true;
    }

} // namespace _crashing
