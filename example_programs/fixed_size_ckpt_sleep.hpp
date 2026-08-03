#pragma once

#include "util.hpp"

struct fixed_size_ckpt_sleep_input {
    int function_id;
    int world_size;
    long size_mb;
    int sleep_minutes;
};

struct fixed_size_ckpt_sleep_output {
    int function_id;
    bool all_ok;
};

namespace _fixed_size_ckpt_sleep {

    inline void *get_context() {
        return nullptr;
    }

    inline void free_context(void *) {}

    inline void fill_input(int func_num, int numcores, long size_mb, int sleep_minutes, char *ptr) {
        auto *args = reinterpret_cast<fixed_size_ckpt_sleep_input *>(ptr);
        args->function_id = func_num;
        args->world_size = numcores;
        args->size_mb = size_mb;
        args->sleep_minutes = sleep_minutes;
    }

    inline bool verify_output(int func_num, char *ptr) {
        auto *res = reinterpret_cast<fixed_size_ckpt_sleep_output *>(ptr);
        bool correct = compare(func_num, "function_id", func_num, res->function_id);
        correct &= compare(func_num, "all_ok", true, res->all_ok);
        return correct;
    }

} // namespace _fixed_size_ckpt_sleep
