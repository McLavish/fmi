#pragma once

#include "util.hpp"

struct fixed_size_ckpt_comm_input {
    int function_id;
    int world_size;
    long size_mb;
    long num_all_reduces;
};

struct fixed_size_ckpt_comm_output {
    int function_id;
    bool all_ok;
    long done_all_reduces;
};

namespace _fixed_size_ckpt_comm {

    inline void *get_context() {
        return nullptr;
    }

    inline void free_context(void *) {}

    inline void fill_input(int func_num, int numcores, long size_mb, long num_all_reduces, char *ptr) {
        auto *args = reinterpret_cast<fixed_size_ckpt_comm_input *>(ptr);
        args->function_id = func_num;
        args->world_size = numcores;
        args->size_mb = size_mb;
        args->num_all_reduces = num_all_reduces;
    }

    inline bool verify_output(int func_num, long num_all_reduces, char *ptr) {
        auto *res = reinterpret_cast<fixed_size_ckpt_comm_output *>(ptr);
        bool correct = compare(func_num, "function_id", func_num, res->function_id);
        correct &= compare(func_num, "done_all_reduces", num_all_reduces, res->done_all_reduces);
        correct &= compare(func_num, "all_ok", true, res->all_ok);
        return correct;
    }

} // namespace _fixed_size_ckpt_comm
