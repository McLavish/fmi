#include <Communicator.h>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "harness.hpp"
#include "jacobi.hpp"

static uint32_t jacobi(void *args, uint32_t size, void *res) {
    jacobi_input *input = static_cast<jacobi_input *>(args);
    jacobi_output *output = static_cast<jacobi_output *>(res);

    int rank = input->function_id;
    int world_size = input->world_size;

    FMI::Communicator comm(rank, world_size, fmi_examples::config_path(), fmi_examples::comm_name(),
                           fmi_examples::faas_memory());
    // comm.barrier();

    output->function_id = rank;
    int h = input->tile_h, w = input->tile_w;
    int y = input->tile_y, x = input->tile_x;
    int ny = N / h, nx = N / w;

    if (rank == 0)
        std::cout << "Info h=" << h << " w=" << w << " ny=" << ny << " nx=" << nx << std::endl;

    std::cout << "Rank " << rank << " y=" << y << " x=" << x << " starts work!" << std::endl;

    double *A = input->buf, *B = input->buf + h * w;
    auto idx = [&](int i, int j) { return i * w + j; };

    // indexes of neighbors
    int top_neighbor = (y > 0) ? rank - nx : -1;
    int bottom_neighbor = (y < ny - 1) ? rank + nx : -1;
    int left_neighbor = (x > 0) ? rank - 1 : -1;
    int right_neighbor = (x < nx - 1) ? rank + 1 : -1;

    std::cout << "Rank " << rank << " top_neighbor=" << top_neighbor << " bottom_neighbor=" << bottom_neighbor
              << " left_neighbor=" << left_neighbor << " right_neighbor=" << right_neighbor << std::endl;

    // buffers for halo exchange
    std::vector<double> recv_top(w), recv_bottom(w);
    std::vector<double> recv_left(h), recv_right(h);
    std::vector<double> send_top(w), send_bottom(w);
    std::vector<double> send_left(h), send_right(h);

    auto exchange_halos_top_bottom = [&](double *src) {
        for (int j = 0; j < w; j++) {
            send_top[j] = src[idx(0, j)];
            send_bottom[j] = src[idx(h - 1, j)];
        }

        FMI::Comm::Data<std::vector<double>> s_bottom(send_bottom);
        FMI::Comm::Data<std::vector<double>> r_bottom(recv_bottom);
        FMI::Comm::Data<std::vector<double>> s_top(send_top);
        FMI::Comm::Data<std::vector<double>> r_top(recv_top);
        // even ranks: send bottom, then recv bottom; send top, then recv top
        // odd ranks: recv top, then send top; recv bottom, then send bottom

        if (y % 2 == 0) {
            if (y < ny - 1) {
                comm.send(s_bottom, bottom_neighbor);
                comm.recv(r_bottom, bottom_neighbor);
                recv_bottom = r_bottom.get();
            }

            if (y > 0) {
                comm.send(s_top, top_neighbor);
                comm.recv(r_top, top_neighbor);
                recv_top = r_top.get();
            }
        } else {
            if (y > 0) {
                comm.recv(r_top, top_neighbor);
                comm.send(s_top, top_neighbor);
                recv_top = r_top.get();
            }

            if (y < ny - 1) {
                comm.recv(r_bottom, bottom_neighbor);
                comm.send(s_bottom, bottom_neighbor);
                recv_bottom = r_bottom.get();
            }
        }
    };

    auto exchange_halos_left_right = [&](double *src) {
        for (int i = 0; i < h; i++) {
            send_left[i] = src[idx(i, 0)];
            send_right[i] = src[idx(i, w - 1)];
        }

        FMI::Comm::Data<std::vector<double>> s_right(send_right);
        FMI::Comm::Data<std::vector<double>> r_right(recv_right);
        FMI::Comm::Data<std::vector<double>> s_left(send_left);
        FMI::Comm::Data<std::vector<double>> r_left(recv_left);
        // even ranks: send right, then recv right; send left, then recv left
        // odd ranks: recv left, then send left; recv right, then send right

        if (x % 2 == 0) {
            if (x < nx - 1) {
                comm.send(s_right, right_neighbor);
                comm.recv(r_right, right_neighbor);
                recv_right = r_right.get();
            }

            if (x > 0) {
                comm.send(s_left, left_neighbor);
                comm.recv(r_left, left_neighbor);
                recv_left = r_left.get();
            }
        } else {
            if (x > 0) {
                comm.recv(r_left, left_neighbor);
                comm.send(s_left, left_neighbor);
                recv_left = r_left.get();
            }

            if (x < nx - 1) {
                comm.recv(r_right, right_neighbor);
                comm.send(s_right, right_neighbor);
                recv_right = r_right.get();
            }
        }
    };

    auto compute_border_corners = [&](double *tg, double *org) {
        if (y > 0)
            for (int j = 1; j < w - 1; j++)
                tg[idx(0, j)] =
                    0.2 * (org[idx(0, j)] + org[idx(0, j - 1)] + org[idx(0, j + 1)] + org[idx(1, j)] + recv_top[j]);
        if (y < ny - 1)
            for (int j = 1; j < w - 1; j++)
                tg[idx(h - 1, j)] = 0.2 * (org[idx(h - 1, j)] + org[idx(h - 1, j - 1)] + org[idx(h - 1, j + 1)] +
                                           recv_bottom[j] + org[idx(h - 2, j)]);
        if (x > 0)
            for (int i = 1; i < h - 1; i++)
                tg[idx(i, 0)] =
                    0.2 * (org[idx(i, 0)] + org[idx(i - 1, 0)] + org[idx(i + 1, 0)] + recv_left[i] + org[idx(i, 1)]);
        if (x < nx - 1)
            for (int i = 1; i < h - 1; i++)
                tg[idx(i, w - 1)] = 0.2 * (org[idx(i, w - 1)] + org[idx(i - 1, w - 1)] + org[idx(i + 1, w - 1)] +
                                           org[idx(i, w - 2)] + recv_right[i]);

        if (y > 0 && x > 0)
            tg[idx(0, 0)] = 0.2 * (org[idx(0, 0)] + recv_top[0] + org[idx(1, 0)] + recv_left[0] + org[idx(0, 1)]);

        if (y > 0 && x < nx - 1)
            tg[idx(0, w - 1)] = 0.2 * (org[idx(0, w - 1)] + recv_top[w - 1] + org[idx(1, w - 1)] + org[idx(0, w - 2)] +
                                       recv_right[0]);

        if (y < ny - 1 && x > 0)
            tg[idx(h - 1, 0)] = 0.2 * (org[idx(h - 1, 0)] + org[idx(h - 2, 0)] + recv_bottom[0] + recv_left[h - 1] +
                                       org[idx(h - 1, 1)]);

        if (y < ny - 1 && x < nx - 1)
            tg[idx(h - 1, w - 1)] = 0.2 * (org[idx(h - 1, w - 1)] + org[idx(h - 2, w - 1)] + recv_bottom[w - 1] +
                                           org[idx(h - 1, w - 2)] + recv_right[h - 1]);
    };

    for (int t = 0; t < T; t++) {
        // compute inside B
        for (int i = 1; i < h - 1; i++)
            for (int j = 1; j < w - 1; j++)
                B[idx(i, j)] =
                    0.2 * (A[idx(i, j)] + A[idx(i, j - 1)] + A[idx(i, j + 1)] + A[idx(i + 1, j)] + A[idx(i - 1, j)]);

        // exchange halos of A
        exchange_halos_top_bottom(A);
        exchange_halos_left_right(A);

        // compute border and corners of B
        compute_border_corners(B, A);

        // compute inside A
        for (int i = 1; i < h - 1; i++)
            for (int j = 1; j < w - 1; j++)
                A[idx(i, j)] =
                    0.2 * (B[idx(i, j)] + B[idx(i, j - 1)] + B[idx(i, j + 1)] + B[idx(i + 1, j)] + B[idx(i - 1, j)]);

        // exchange halos of B
        exchange_halos_top_bottom(B);
        exchange_halos_left_right(B);

        // compute border of A
        compute_border_corners(A, B);

        if (rank == 0)
            std::cout << "Done iteration " << t + 1 << " / " << T << std::endl;
    }

    std::memcpy(output->buf, A, h * w * sizeof(double));
    std::memcpy(output->buf + h * w, B, h * w * sizeof(double));

    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(jacobi_output);
}

//! Rank counts that tile the N x N grid and still fit into the per-rank input buffer.
static std::string supported_rank_counts() {
    std::string counts;
    for (int n = 1; n <= fmi_examples::MAX_RANKS; n++) {
        auto [tile_h, tile_w] = _jacobi::get_optimal_hw(n);
        if (tile_h == -1 || tile_w == -1 || 2 * tile_h * tile_w > 2 * 100 * 100)
            continue;
        if (!counts.empty())
            counts += ", ";
        counts += std::to_string(n);
    }
    return counts;
}

/*!
 * Same conditions _jacobi::initialize_input enforces, checked in the spec builder so that an
 * unusable --ranks value fails with an actionable message before the harness forks any rank.
 */
static void validate_rank_count(int ranks) {
    auto [tile_h, tile_w] = _jacobi::get_optimal_hw(ranks);
    if (tile_h == -1 || tile_w == -1)
        throw std::runtime_error("jacobi cannot tile the " + std::to_string(N) + "x" + std::to_string(N) +
                                 " grid across " + std::to_string(ranks) +
                                 " ranks (needs ranks == h * w with h and w both dividing " + std::to_string(N) +
                                 "); supported --ranks values: " + supported_rank_counts());
    if (2 * tile_h * tile_w > 2 * 100 * 100)
        throw std::runtime_error("jacobi tile for " + std::to_string(ranks) + " ranks is " + std::to_string(tile_h) +
                                 "x" + std::to_string(tile_w) + ", which exceeds the " + std::to_string(100 * 100) +
                                 " doubles per grid the input buffer holds; supported --ranks values: " +
                                 supported_rank_counts());
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;

    return fmi_examples::run(argc, argv, "jacobi", flags, [](const fmi_examples::Options &opts) {
        validate_rank_count(opts.ranks);

        fmi_examples::ExampleSpec spec;
        spec.name = "jacobi";
        spec.input_size = sizeof(jacobi_input);
        spec.output_size = sizeof(jacobi_output);
        spec.get_context = [] { return _jacobi::get_context(); };
        spec.free_context = [](void *ctx) { _jacobi::free_context(ctx); };
        spec.initialize_input = [](int func_num, int numcores, void *ctx, char *ptr) {
            _jacobi::initialize_input(func_num, numcores, ctx, ptr);
        };
        spec.check_output = [](int func_num, int numcores, void *ctx, char *ptr) {
            return _jacobi::check_output(func_num, numcores, ctx, ptr);
        };
        spec.fn = jacobi;
        return spec;
    });
}
