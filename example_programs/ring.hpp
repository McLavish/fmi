#pragma once

#include "util.hpp"

struct ring_input {
    int function_id;
    int world_size;
    int num_iterations;
};

struct ring_output {
    int function_id;
    int last_recvd;
};

namespace _ring {

    inline void *get_context() {
        return nullptr;
    }

    inline void free_context(void *) {}

    inline void fill_input(int func_num, int numcores, int num_iterations, char *ptr) {
        ring_input *args = reinterpret_cast<ring_input *>(ptr);
        args->function_id = func_num;
        args->world_size = numcores;
        args->num_iterations = num_iterations;
    }

    inline bool verify_output(int func_num, int numcores, void *, char *ptr) {
        ring_output *res = reinterpret_cast<ring_output *>(ptr);
        bool correct = compare(func_num, "function_id", func_num, res->function_id);
        int expected = (func_num - 1 + numcores) % numcores;
        correct &= compare(func_num, "last_recvd", expected, res->last_recvd);
        return correct;
    }

} // namespace _ring
