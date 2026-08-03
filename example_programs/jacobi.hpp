#pragma once

#include <cassert>
#include <stdexcept>
#include <string>
#include <utility>

#include "util.hpp"

constexpr int N = 200;
constexpr int T = 30;

struct jacobi_input {
    int function_id;
    int world_size;
    int tile_h, tile_w;
    int tile_x, tile_y;
    double buf[2 * 100 * 100];
};

struct jacobi_output {
    int function_id;
    double buf[2 * 100 * 100];
};

constexpr int NOT_INITIALIZED = 0;
constexpr int INITIALIZED = 1;
constexpr int COMPUTED = 2;

struct jacobi_context {
    double A[N][N], B[N][N];
    int status = NOT_INITIALIZED;
};

namespace _jacobi {

    inline void init_array(double A[N][N], double B[N][N]) {
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                A[i][j] = 1. * (i * (j + 2) + 2) / N;
                B[i][j] = 1. * (i * (j + 3) + 3) / N;
            }
        }
    }

    inline void jacobi_2d(double A[N][N], double B[N][N]) {
        for (int t = 0; t < T; t++) {
            for (int i = 1; i < N - 1; i++)
                for (int j = 1; j < N - 1; j++)
                    B[i][j] = 0.2 * (A[i][j] + A[i][j - 1] + A[i][j + 1] + A[i + 1][j] + A[i - 1][j]);

            for (int i = 1; i < N - 1; i++)
                for (int j = 1; j < N - 1; j++)
                    A[i][j] = 0.2 * (B[i][j] + B[i][j - 1] + B[i][j + 1] + B[i + 1][j] + B[i - 1][j]);
        }
    }

    inline struct jacobi_context *get_context() {
        jacobi_context *context = new jacobi_context;

        init_array(context->A, context->B);
        context->status = INITIALIZED;

        return context;
    }

    inline void free_context(void *context) {
        jacobi_context *ctx = static_cast<jacobi_context *>(context);
        delete ctx;
    }

    inline std::pair<int, int> get_optimal_hw(int numcores) {
        std::pair<int, int> hw = {-1, -1};

        for (int d = 1; d * d <= numcores; d++) {
            if (numcores % d)
                continue;

            int h = d, w = numcores / d;
            if (N % h || N % w)
                continue;

            hw = {N / h, N / w};
        }

        return hw;
    }

    inline void initialize_input(int func_num, int numcores, void *context, char *ptr) {
        jacobi_input *args = reinterpret_cast<jacobi_input *>(ptr);
        args->function_id = func_num;
        args->world_size = numcores;

        jacobi_context *ctx = reinterpret_cast<jacobi_context *>(context);
        assert(ctx->status == INITIALIZED);

        auto [tile_h, tile_w] = get_optimal_hw(numcores);
        if (tile_h == -1 || tile_w == -1)
            throw std::runtime_error("Could not suitably divide grid for numcores");

        args->tile_h = tile_h;
        args->tile_w = tile_w;
        if (2 * tile_h * tile_w > 2 * 100 * 100)
            throw std::runtime_error("Too much memory needed per function");

        int nx = N / tile_w;
        args->tile_y = func_num / nx;
        args->tile_x = func_num % nx;

        double *A = args->buf, *B = args->buf + tile_h * tile_w;
        for (int y = 0; y < tile_h; y++) {
            for (int x = 0; x < tile_w; x++) {
                int orig_y = tile_h * args->tile_y + y;
                int orig_x = tile_w * args->tile_x + x;
                int idx_buf = y * tile_w + x;

                A[idx_buf] = ctx->A[orig_y][orig_x];
                B[idx_buf] = ctx->B[orig_y][orig_x];
            }
        }
    }

    inline bool check_output(int func_num, int numcores, void *context, char *ptr) {
        jacobi_output *res = reinterpret_cast<jacobi_output *>(ptr);

        jacobi_context *ctx = reinterpret_cast<jacobi_context *>(context);
        if (ctx->status != COMPUTED) {
            jacobi_2d(ctx->A, ctx->B);
            ctx->status = COMPUTED;
        }

        auto [tile_h, tile_w] = get_optimal_hw(numcores);
        int nx = N / tile_w;
        int tile_y = func_num / nx;
        int tile_x = func_num % nx;

        bool correct = compare(func_num, "function_id", func_num, res->function_id);

        double *A = res->buf, *B = res->buf + tile_h * tile_w;
        for (int y = 0; y < tile_h; y++) {
            for (int x = 0; x < tile_w; x++) {
                int orig_y = tile_h * tile_y + y;
                int orig_x = tile_w * tile_x + x;
                int idx_buf = y * tile_w + x;

                correct &= compare(func_num, "A[" + std::to_string(orig_y) + "][" + std::to_string(orig_x) + "]",
                                   ctx->A[orig_y][orig_x], A[idx_buf]);
                correct &= compare(func_num, "B[" + std::to_string(orig_y) + "][" + std::to_string(orig_x) + "]",
                                   ctx->B[orig_y][orig_x], B[idx_buf]);
            }
        }

        return correct;
    }

} // namespace _jacobi
