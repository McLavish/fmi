#pragma once

#include <cmath>

#include "util.hpp"

struct mantevo_hpccg_input {
    int size, rank;
    int nx;
    int ny;
    int nz;
    int max_iter;
    double tolerance;
};

struct mantevo_hpccg_output {
    int rank;
    int niters;
    double normr;
};

namespace _mantevo_hpccg {

    struct context {
        bool has_reference = false;
        int niters;
        double normr;
    };

    inline void *get_context() {
        return new context{};
    }

    inline void free_context(void *ctx) {
        delete static_cast<context *>(ctx);
    }

    inline void fill_input(int func_num, int numcores, int nx, int ny, int nz, int max_iter, double tolerance,
                           char *ptr) {
        mantevo_hpccg_input *args = reinterpret_cast<mantevo_hpccg_input *>(ptr);
        args->size = numcores;
        args->rank = func_num;
        args->nx = nx;
        args->ny = ny;
        args->nz = nz;
        args->max_iter = max_iter;
        args->tolerance = tolerance;
    }

    inline bool verify_output(int func_num, int numcores, int max_iter, double tolerance, void *ctx_, char *ptr) {
        mantevo_hpccg_output *res = reinterpret_cast<mantevo_hpccg_output *>(ptr);
        context *ctx = static_cast<context *>(ctx_);

        bool correct = compare(func_num, "rank", func_num, res->rank);

        if (tolerance == 0.0) {
            // With tolerance 0 every run does the full iteration count (max_iter - 1)
            correct &= compare(func_num, "niters", max_iter - 1, res->niters);
        }

        if (!std::isfinite(res->normr)) {
            std::cout << "Function " << func_num << " returned non-finite residual " << res->normr << std::endl;
            correct = false;
        }

        // The final residual comes out of an allreduce
        // Every rank must report the same niters/normr
        if (!ctx->has_reference) {
            ctx->has_reference = true;
            ctx->niters = res->niters;
            ctx->normr = res->normr;
        } else {
            correct &= compare(func_num, "niters", ctx->niters, res->niters);
            correct &= compare(func_num, "normr", ctx->normr, res->normr);
        }

        return correct;
    }

} // namespace _mantevo_hpccg
