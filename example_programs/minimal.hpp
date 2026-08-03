#pragma once

#include "util.hpp"

struct minimal_input {
    int function_id;
    int function_payload;
};

struct minimal_output {
    int function_id;
    long long function_result;
};

namespace _minimal {

    inline void *get_context() {
        return nullptr;
    }

    inline void free_context(void *context) {}

    inline void initialize_input(int func_num, int numcores, void *context, char *ptr) {
        minimal_input *args = reinterpret_cast<minimal_input *>(ptr);
        args->function_id = func_num;
        args->function_payload = func_num + 1;
    }

    inline bool check_output(int func_num, int numcores, void *context, char *ptr) {
        minimal_output *res = reinterpret_cast<minimal_output *>(ptr);

        bool correct = compare(func_num, "function_id", func_num, res->function_id);

        long long sum = (1ll * (func_num + 1) * (func_num + 2)) / 2;
        correct &= compare(func_num, "function_result", sum, res->function_result);

        return correct;
    }

} // namespace _minimal
