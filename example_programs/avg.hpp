#pragma once

#include <cstdlib>

#include "util.hpp"

struct avg_input {
    int function_id;
    int world_size;
    int num_elements_per_proc;
    int random_seed;
};

struct avg_output {
    int function_id;
    double subset_avg;
    double total_avg;
    bool globally_correct;
};

constexpr int SEED = 42;
constexpr int NUM_ELEMENTS_PER_PROC = 1000;

struct avg_context {
    double expected_avg = 0;
};

namespace _avg {

    inline struct avg_context *get_context() {
        struct avg_context *context = new avg_context;

        srand(SEED);
        for (int i = 0; i < NUM_ELEMENTS_PER_PROC; i++)
            context->expected_avg += 100 * (rand() / (double)RAND_MAX);
        context->expected_avg /= NUM_ELEMENTS_PER_PROC;

        return context;
    }

    inline void free_context(void *context) {
        struct avg_context *ctx = static_cast<avg_context *>(context);
        delete ctx;
    }

    inline void initialize_input(int func_num, int numcores, void *context, char *ptr) {
        avg_input *args = reinterpret_cast<avg_input *>(ptr);
        args->function_id = func_num;
        args->world_size = numcores;
        args->num_elements_per_proc = NUM_ELEMENTS_PER_PROC;
        args->random_seed = SEED;
    }

    inline bool check_output(int func_num, int numcores, void *context, char *ptr) {
        avg_output *res = reinterpret_cast<avg_output *>(ptr);
        avg_context *ctx = reinterpret_cast<avg_context *>(context);

        bool correct = compare(func_num, "function_id", func_num, res->function_id);
        if (func_num == 0) correct &= compare(func_num, "subset_avg", ctx->expected_avg, res->subset_avg);
        correct &= compare(func_num, "globally_correct", (func_num == 0) ? true : false, res->globally_correct);

        return correct;
    }

} // namespace _avg
