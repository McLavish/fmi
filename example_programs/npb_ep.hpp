#pragma once

#include <cmath>

#include "util.hpp"

struct npb_ep_input {
    int size, rank;
    int m;
};

struct npb_ep_output {
    int rank;
    double sx;
    double sy;
    double gc;
};

namespace _npb_ep {

    struct context {
        bool has_reference = false;
        double sx, sy, gc;
    };

    inline void *get_context() {
        return new context{};
    }

    inline void free_context(void *ctx) {
        delete static_cast<context *>(ctx);
    }

    inline void fill_input(int func_num, int numcores, int m, char *ptr) {
        npb_ep_input *args = reinterpret_cast<npb_ep_input *>(ptr);
        args->size = numcores;
        args->rank = func_num;
        args->m = m;
    }

    // Reference sums from the verification part of the original EP benchmark.
    // M = 24/25/28/30/32/36/40 correspond to classes S/W/A/B/C/D/E.
    inline bool reference_sums(int m, double &sx_verify_value, double &sy_verify_value) {
        if (m == 24) {
            sx_verify_value = -3.247834652034740e+3;
            sy_verify_value = -6.958407078382297e+3;
        } else if (m == 25) {
            sx_verify_value = -2.863319731645753e+3;
            sy_verify_value = -6.320053679109499e+3;
        } else if (m == 28) {
            sx_verify_value = -4.295875165629892e+3;
            sy_verify_value = -1.580732573678431e+4;
        } else if (m == 30) {
            sx_verify_value = 4.033815542441498e+4;
            sy_verify_value = -2.660669192809235e+4;
        } else if (m == 32) {
            sx_verify_value = 4.764367927995374e+4;
            sy_verify_value = -8.084072988043731e+4;
        } else if (m == 36) {
            sx_verify_value = 1.982481200946593e+5;
            sy_verify_value = -1.020596636361769e+5;
        } else if (m == 40) {
            sx_verify_value = -5.319717441530e+05;
            sy_verify_value = -3.688834557731e+05;
        } else {
            return false;
        }
        return true;
    }

    inline bool verify_output(int func_num, int numcores, int m, void *ctx_, char *ptr) {
        npb_ep_output *res = reinterpret_cast<npb_ep_output *>(ptr);
        context *ctx = static_cast<context *>(ctx_);

        bool correct = compare(func_num, "rank", func_num, res->rank);

        if (!std::isfinite(res->sx) || !std::isfinite(res->sy)) {
            std::cout << "Function " << func_num << " returned non-finite sums " << res->sx << " " << res->sy
                      << std::endl;
            correct = false;
        }

        double sx_ref, sy_ref;
        if (reference_sums(m, sx_ref, sy_ref)) {
            constexpr double epsilon = 1.0e-8;
            double sx_err = std::fabs((res->sx - sx_ref) / sx_ref);
            double sy_err = std::fabs((res->sy - sy_ref) / sy_ref);
            if (sx_err > epsilon) {
                std::cout << "Function " << func_num << " returned wrong sx: expected " << sx_ref << ", got " << res->sx
                          << std::endl;
                correct = false;
            }
            if (sy_err > epsilon) {
                std::cout << "Function " << func_num << " returned wrong sy: expected " << sy_ref << ", got " << res->sy
                          << std::endl;
                correct = false;
            }
        } else {
            std::cout << "No EP reference sums for M = " << m << ", skipping sum verification for function " << func_num
                      << std::endl;
        }

        // Every rank must report the same sx/sy/gc
        if (!ctx->has_reference) {
            ctx->has_reference = true;
            ctx->sx = res->sx;
            ctx->sy = res->sy;
            ctx->gc = res->gc;
        } else {
            correct &= compare(func_num, "sx", ctx->sx, res->sx);
            correct &= compare(func_num, "sy", ctx->sy, res->sy);
            correct &= compare(func_num, "gc", ctx->gc, res->gc);
        }

        return correct;
    }

} // namespace _npb_ep
