#include <Communicator.h>

#include <cstdio>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "launcher.hpp"
#include "util.hpp"

namespace {

    constexpr int N = 200;
    constexpr int T = 30;

    //! Reference grid used for the per-rank self check; heap allocated (2 * 200 * 200 doubles).
    struct ReferenceGrid {
        double A[N][N];
        double B[N][N];
    };

    void init_array(double A[N][N], double B[N][N]) {
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                A[i][j] = 1. * (i * (j + 2) + 2) / N;
                B[i][j] = 1. * (i * (j + 3) + 3) / N;
            }
        }
    }

    //! Serial reference: the same stencil and the same A <-> B double buffering the ranks run.
    void jacobi_2d(double A[N][N], double B[N][N]) {
        for (int t = 0; t < T; t++) {
            for (int i = 1; i < N - 1; i++)
                for (int j = 1; j < N - 1; j++)
                    B[i][j] = 0.2 * (A[i][j] + A[i][j - 1] + A[i][j + 1] + A[i + 1][j] + A[i - 1][j]);

            for (int i = 1; i < N - 1; i++)
                for (int j = 1; j < N - 1; j++)
                    A[i][j] = 0.2 * (B[i][j] + B[i][j - 1] + B[i][j + 1] + B[i + 1][j] + B[i - 1][j]);
        }
    }

    /*!
     * Tile shape {tile_h, tile_w} for a numcores x-by-y decomposition of the N x N grid, or
     * {-1, -1} when no factorisation of numcores divides N in both dimensions.
     */
    std::pair<int, int> get_optimal_hw(int numcores) {
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

    /*!
     * Rank counts that tile the N x N grid. Tiles are heap vectors now, so the old fixed
     * "2 * 100 * 100 doubles per rank" POD-buffer cap is gone and 1 and 2 ranks are supported.
     */
    std::string supported_rank_counts() {
        std::string counts;
        for (int n = 1; n <= fmi_examples::MAX_RANKS; n++) {
            std::pair<int, int> hw = get_optimal_hw(n);
            if (hw.first == -1 || hw.second == -1)
                continue;
            if (!counts.empty())
                counts += ", ";
            counts += std::to_string(n);
        }
        return counts;
    }

} // namespace

int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    fmi_examples::Flags flags;

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "jacobi", flags, opts, exit_code)) {
        return exit_code;
    }

    // The decomposition only exists when ranks factorises into h * w with both factors dividing N.
    // Checked before the launcher branch so both the launcher and an explicit --rank process
    // reject the run identically, before any rank is forked.
    const std::pair<int, int> hw = get_optimal_hw(opts.ranks);
    if (hw.first == -1 || hw.second == -1) {
        std::cerr << "jacobi cannot tile the " << N << "x" << N << " grid across " << opts.ranks
                  << " ranks (needs ranks == h * w with h and w both dividing " << N
                  << "); supported --ranks values: " << supported_rank_counts() << std::endl;
        return 2;
    }

    if (!opts.single_rank()) {
        return fmi_examples::spawn_local(argc, argv, opts);
    }

    const int rank = opts.rank;
    const int ranks = opts.ranks;
    bool ok = true;

    try {
        FMI::Communicator comm(rank, ranks, fmi_examples::config_path(), fmi_examples::comm_name(),
                               fmi_examples::faas_memory());

        const int h = hw.first, w = hw.second;
        const int ny = N / h, nx = N / w;
        const int y = rank / nx, x = rank % nx;

        if (rank == 0)
            std::cout << "rank " << rank << ": info h=" << h << " w=" << w << " ny=" << ny << " nx=" << nx << std::endl;

        std::cout << "rank " << rank << ": y=" << y << " x=" << x << " starts work!" << std::endl;

        // Reference grid: seeds this rank's tile now, and after the run it holds the serial result
        // this rank checks itself against.
        std::unique_ptr<ReferenceGrid> ref(new ReferenceGrid());
        init_array(ref->A, ref->B);

        std::vector<double> tile_a(static_cast<size_t>(h) * w);
        std::vector<double> tile_b(static_cast<size_t>(h) * w);
        for (int i = 0; i < h; i++) {
            for (int j = 0; j < w; j++) {
                tile_a[i * w + j] = ref->A[h * y + i][w * x + j];
                tile_b[i * w + j] = ref->B[h * y + i][w * x + j];
            }
        }

        double *A = tile_a.data(), *B = tile_b.data();
        auto idx = [&](int i, int j) { return i * w + j; };

        // indexes of neighbors
        int top_neighbor = (y > 0) ? rank - nx : -1;
        int bottom_neighbor = (y < ny - 1) ? rank + nx : -1;
        int left_neighbor = (x > 0) ? rank - 1 : -1;
        int right_neighbor = (x < nx - 1) ? rank + 1 : -1;

        std::cout << "rank " << rank << ": top_neighbor=" << top_neighbor << " bottom_neighbor=" << bottom_neighbor
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
                    tg[idx(i, 0)] = 0.2 * (org[idx(i, 0)] + org[idx(i - 1, 0)] + org[idx(i + 1, 0)] + recv_left[i] +
                                           org[idx(i, 1)]);
            if (x < nx - 1)
                for (int i = 1; i < h - 1; i++)
                    tg[idx(i, w - 1)] = 0.2 * (org[idx(i, w - 1)] + org[idx(i - 1, w - 1)] + org[idx(i + 1, w - 1)] +
                                               org[idx(i, w - 2)] + recv_right[i]);

            if (y > 0 && x > 0)
                tg[idx(0, 0)] = 0.2 * (org[idx(0, 0)] + recv_top[0] + org[idx(1, 0)] + recv_left[0] + org[idx(0, 1)]);

            if (y > 0 && x < nx - 1)
                tg[idx(0, w - 1)] = 0.2 * (org[idx(0, w - 1)] + recv_top[w - 1] + org[idx(1, w - 1)] +
                                           org[idx(0, w - 2)] + recv_right[0]);

            if (y < ny - 1 && x > 0)
                tg[idx(h - 1, 0)] = 0.2 * (org[idx(h - 1, 0)] + org[idx(h - 2, 0)] + recv_bottom[0] +
                                           recv_left[h - 1] + org[idx(h - 1, 1)]);

            if (y < ny - 1 && x < nx - 1)
                tg[idx(h - 1, w - 1)] = 0.2 * (org[idx(h - 1, w - 1)] + org[idx(h - 2, w - 1)] + recv_bottom[w - 1] +
                                               org[idx(h - 1, w - 2)] + recv_right[h - 1]);
        };

        for (int t = 0; t < T; t++) {
            // compute inside B
            for (int i = 1; i < h - 1; i++)
                for (int j = 1; j < w - 1; j++)
                    B[idx(i, j)] = 0.2 * (A[idx(i, j)] + A[idx(i, j - 1)] + A[idx(i, j + 1)] + A[idx(i + 1, j)] +
                                          A[idx(i - 1, j)]);

            // exchange halos of A
            exchange_halos_top_bottom(A);
            exchange_halos_left_right(A);

            // compute border and corners of B
            compute_border_corners(B, A);

            // compute inside A
            for (int i = 1; i < h - 1; i++)
                for (int j = 1; j < w - 1; j++)
                    A[idx(i, j)] = 0.2 * (B[idx(i, j)] + B[idx(i, j - 1)] + B[idx(i, j + 1)] + B[idx(i + 1, j)] +
                                          B[idx(i - 1, j)]);

            // exchange halos of B
            exchange_halos_top_bottom(B);
            exchange_halos_left_right(B);

            // compute border of A
            compute_border_corners(A, B);

            if (rank == 0)
                std::cout << "rank " << rank << ": done iteration " << t + 1 << " / " << T << std::endl;
        }

        // Self check: the serial reference over the whole grid, compared on this rank's tile only.
        jacobi_2d(ref->A, ref->B);

        for (int i = 0; i < h; i++) {
            for (int j = 0; j < w; j++) {
                int orig_y = h * y + i;
                int orig_x = w * x + j;
                int idx_buf = i * w + j;

                ok &= compare(rank, "A[" + std::to_string(orig_y) + "][" + std::to_string(orig_x) + "]",
                              ref->A[orig_y][orig_x], A[idx_buf]);
                ok &= compare(rank, "B[" + std::to_string(orig_y) + "][" + std::to_string(orig_x) + "]",
                              ref->B[orig_y][orig_x], B[idx_buf]);
            }
        }

        fmi_examples::teardown_sync(comm, opts);
    } catch (const std::exception &e) {
        std::cout << "rank " << rank << " crashed: " << e.what() << std::endl;
        ok = false;
    } catch (const std::string &s) {
        std::cout << "rank " << rank << " crashed: " << s << std::endl;
        ok = false;
    } catch (...) {
        std::cout << "rank " << rank << " crashed: unknown exception" << std::endl;
        ok = false;
    }

    std::cout << "rank " << rank << ": " << (ok ? "PASS" : "FAIL") << std::endl;
    return ok ? 0 : 1;
}
