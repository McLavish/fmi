/*
MIT License

Copyright (c) 2021 Parallel Applications Modelling Group - GMAP
        GMAP website: https://gmap.pucrs.br

        Pontifical Catholic University of Rio Grande do Sul (PUCRS)
        Av. Ipiranga, 6681, Porto Alegre - Brazil, 90619-900

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

------------------------------------------------------------------------------

The original NPB 3.4.1 version was written in Fortran and belongs to:
        http://www.nas.nasa.gov/Software/NPB/

Authors of the Fortran code:
        P. O. Frederickson
        D. H. Bailey
        A. C. Woo

------------------------------------------------------------------------------

The serial C++ version is a translation of the original NPB 3.4.1
Serial C++ version: https://github.com/GMAP/NPB-CPP/tree/master/NPB-SER

Authors of the C++ code:
        Dalvan Griebler <dalvangriebler@gmail.com>
        Gabriell Araujo <hexenoften@gmail.com>
        Júnior Löff <loffjh@gmail.com>
*/

// This file adapts the NPB-CPP serial EP benchmark (ep.cpp, plus randlc and
// vranlc from common/c_randdp.cpp) to an FMI program:
//  - M is a runtime input instead of a compile-time npbparams.hpp constant,
//  - the batch loop is distributed round-robin over the FMI ranks,
//  - the three MPI_Allreduce calls of the NPB MPI version (sx, sy, q) are
//    fused into a single sum-allreduce at the end.

#include <Communicator.h>

#include <cmath>
#include <cstdio>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include <sys/time.h>

#include "launcher.hpp"
#include "util.hpp"

/*
 * --------------------------------------------------------------------
 * this is the serial version of the app benchmark 1,
 * the "embarassingly parallel" benchmark.
 * --------------------------------------------------------------------
 * M is the Log_2 of the number of complex pairs of uniform (0, 1) random
 * numbers. MK is the Log_2 of the size of each batch of uniform random
 * numbers.  MK can be set for convenience on a given system, since it does
 * not affect the results.
 * --------------------------------------------------------------------
 */
#define MK 16
#define NQ 10
#define EPSILON 1.0e-8
#define A 1220703125.0
#define S 271828183.0

#define pow2(a) ((a) * (a))
#define max(a, b) (((a) > (b)) ? (a) : (b))

#if defined(USE_POW)
#define r23 pow(0.5, 23.0)
#define r46 (r23 * r23)
#define t23 pow(2.0, 23.0)
#define t46 (t23 * t23)
#else
#define r23                                                                    \
  (0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 *     \
   0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5)
#define r46 (r23 * r23)
#define t23                                                                    \
  (2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 *     \
   2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0)
#define t46 (t23 * t23)
#endif

// Largest M the reference table below knows (class E).
#define MAX_M 40

/*
 * ---------------------------------------------------------------------
 *
 * this routine returns a uniform pseudorandom double precision number in the
 * range (0, 1) by using the linear congruential generator
 *
 * x_{k+1} = a x_k  (mod 2^46)
 *
 * where 0 < x_k < 2^46 and 0 < a < 2^46. this scheme generates 2^44 numbers
 * before repeating. the argument A is the same as 'a' in the above formula,
 * and X is the same as x_0.  A and X must be odd double precision integers
 * in the range (1, 2^46). the returned value RANDLC is normalized to be
 * between 0 and 1, i.e. RANDLC = 2^(-46) * x_1.  X is updated to contain
 * the new seed x_1, so that subsequent calls to RANDLC using the same
 * arguments will generate a continuous sequence.
 *
 * this routine should produce the same results on any computer with at least
 * 48 mantissa bits in double precision floating point data.  On 64 bit
 * systems, double precision should be disabled.
 *
 * David H. Bailey, October 26, 1990
 *
 * ---------------------------------------------------------------------
 */
static double randlc(double *x, double a) {
    double t1, t2, t3, t4, a1, a2, x1, x2, z;

    /*
     * ---------------------------------------------------------------------
     * break A into two parts such that A = 2^23 * A1 + A2.
     * ---------------------------------------------------------------------
     */
    t1 = r23 * a;
    a1 = (int)t1;
    a2 = a - t23 * a1;

    /*
     * ---------------------------------------------------------------------
     * break X into two parts such that X = 2^23 * X1 + X2, compute
     * Z = A1 * X2 + A2 * X1  (mod 2^23), and then
     * X = 2^23 * Z + A2 * X2  (mod 2^46).
     * ---------------------------------------------------------------------
     */
    t1 = r23 * (*x);
    x1 = (int)t1;
    x2 = (*x) - t23 * x1;
    t1 = a1 * x2 + a2 * x1;
    t2 = (int)(r23 * t1);
    z = t1 - t23 * t2;
    t3 = t23 * z + a2 * x2;
    t4 = (int)(r46 * t3);
    (*x) = t3 - t46 * t4;

    return (r46 * (*x));
}

/*
 * ---------------------------------------------------------------------
 *
 * this routine generates N uniform pseudorandom double precision numbers in
 * the range (0, 1) by using the linear congruential generator
 *
 * x_{k+1} = a x_k  (mod 2^46)
 *
 * where 0 < x_k < 2^46 and 0 < a < 2^46. this scheme generates 2^44 numbers
 * before repeating. the argument A is the same as 'a' in the above formula,
 * and X is the same as x_0. A and X must be odd double precision integers
 * in the range (1, 2^46). the N results are placed in Y and are normalized
 * to be between 0 and 1. X is updated to contain the new seed, so that
 * subsequent calls to VRANLC using the same arguments will generate a
 * continuous sequence.  if N is zero, only initialization is performed, and
 * the variables X, A and Y are ignored.
 *
 * this routine is the standard version designed for scalar or RISC systems.
 * however, it should produce the same results on any single processor
 * computer with at least 48 mantissa bits in double precision floating point
 * data. on 64 bit systems, double precision should be disabled.
 *
 * ---------------------------------------------------------------------
 */
static void vranlc(int n, double *x_seed, double a, double y[]) {
    int i;
    double x, t1, t2, t3, t4, a1, a2, x1, x2, z;

    /*
     * ---------------------------------------------------------------------
     * break A into two parts such that A = 2^23 * A1 + A2.
     * ---------------------------------------------------------------------
     */
    t1 = r23 * a;
    a1 = (int)t1;
    a2 = a - t23 * a1;
    x = *x_seed;

    /*
     * ---------------------------------------------------------------------
     * generate N results. this loop is not vectorizable.
     * ---------------------------------------------------------------------
     */
    for (i = 0; i < n; i++) {
        /*
         * ---------------------------------------------------------------------
         * break X into two parts such that X = 2^23 * X1 + X2, compute
         * Z = A1 * X2 + A2 * X1  (mod 2^23), and then
         * X = 2^23 * Z + A2 * X2  (mod 2^46).
         * ---------------------------------------------------------------------
         */
        t1 = r23 * x;
        x1 = (int)t1;
        x2 = x - t23 * x1;
        t1 = a1 * x2 + a2 * x1;
        t2 = (int)(r23 * t1);
        z = t1 - t23 * t2;
        t3 = t23 * z + a2 * x2;
        t4 = (int)(r46 * t3);
        x = t3 - t46 * t4;
        y[i] = r46 * x;
    }
    *x_seed = x;
}

static double wtime(void) {
    struct timeval tp;
    gettimeofday(&tp, NULL);
    return ((double)tp.tv_sec) + tp.tv_usec / 1000000.0;
}

// Reference sums from the verification part of the original EP benchmark.
// M = 24/25/28/30/32/36/40 correspond to classes S/W/A/B/C/D/E.
static bool reference_sums(int m, double &sx_verify_value, double &sy_verify_value) {
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

// FMI communication helper, replacing the MPI calls of the NPB MPI version
static void allreduce_sum_double(FMI::Communicator &comm, double *sendbuf, double *recvbuf, int count) {
    FMI::Utils::Function<std::vector<double>> f(
        [](std::vector<double> a, std::vector<double> b) {
            for (std::size_t i = 0; i < a.size(); i++) {
                a[i] += b[i];
            }
            return a;
        },
        true, true);
    FMI::Comm::Data<std::vector<double>> senddata = std::vector<double>(sendbuf, sendbuf + count);
    FMI::Comm::Data<std::vector<double>> recvdata = std::vector<double>(count);
    comm.allreduce(senddata, recvdata, f);
    std::vector<double> result = recvdata.get();
    std::memcpy(recvbuf, result.data(), count * sizeof(double));
}

int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    fmi_examples::Flags flags;
    flags.add_int("m", 28, "Log_2 of the number of random number pairs (24=class S, 28=class A)");

    fmi_examples::Options opts;
    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "npb_ep", flags, opts, exit_code)) {
        return exit_code;
    }

    // The kernel needs at least one batch of 2^MK pairs, so m has to sit above MK; the original
    // kernel only printed a diagnostic and returned in that case, which would exit 0 and score as
    // a clean run of a benchmark that had computed nothing. The upper bound keeps
    // "1 << (m - MK)" well inside int and matches the largest entry of the reference table.
    // Read as a long long so an out-of-int-range value is rejected here instead of throwing.
    const long long m_arg = opts.flags.get_long("m");
    if (m_arg <= MK || m_arg > MAX_M) {
        std::cerr << "npb_ep requires --m in (" << MK << ", " << MAX_M << "]; got " << m_arg
                  << " (reference sums exist for m in {24, 25, 28, 30, 32, 36, 40})" << std::endl;
        return 2;
    }

    if (!opts.single_rank()) {
        return fmi_examples::spawn_local(argc, argv, opts);
    }

    const int rank = opts.rank;
    const int size = opts.ranks;
    const int m = static_cast<int>(m_arg);
    bool ok = true;

    try {
        double Mops, t1, t2, t3, t4, x1, x2;
        double sx, sy, tm, an, gc;
        double sx_verify_value, sy_verify_value, sx_err, sy_err;
        int np;
        int i, ik, kk, l, k;
        int k_offset;
        bool verified;
        double dum[3] = {1.0, 1.0, 1.0};

        const int mm = m - MK;
        const int nn = 1 << mm;
        const int nk = 1 << MK;
        const int nk_plus = 2 * nk + 1;

        // std::vector instead of the original new[]/delete[] so an exception out of any FMI call
        // below cannot leak the buffer.
        std::vector<double> x(nk_plus);
        double q[NQ];

        FMI::Communicator comm(rank, size, fmi_examples::config_path(), fmi_examples::comm_name(),
                               fmi_examples::faas_memory());

        std::cout << "rank " << rank << ": established communicator, running EP with m = " << m << std::endl;

        printf("\n\n NAS Parallel Benchmarks - EP Benchmark (FMI version)\n\n");
        printf(" Number of random numbers generated: %15.0f\n", pow(2.0, m + 1));
        printf(" Rank %d of %d\n", rank, size);

        verified = false;

        /*
         * --------------------------------------------------------------------
         * compute the number of "batches" of random number pairs generated
         * per processor. Adjust if the number of processors does not evenly
         * divide the total number
         * --------------------------------------------------------------------
         */
        np = nn;

        /*
         * call the random number generator functions and initialize
         * the x-array to reduce the effects of paging on the timings.
         * also, call all mathematical functions that are used. make
         * sure these initializations cannot be eliminated as dead code.
         */
        vranlc(0, &dum[0], dum[1], &dum[2]);
        dum[0] = randlc(&dum[1], dum[2]);
        for (i = 0; i < nk_plus; i++) {
            x[i] = -1.0e99;
        }
        Mops = log(sqrt(fabs(max(1.0, 1.0))));

        double t_start = wtime();

        t1 = A;
        vranlc(0, &t1, A, x.data());

        /* compute AN = A ^ (2 * NK) (mod 2^46) */

        t1 = A;

        for (i = 0; i < MK + 1; i++) {
            t2 = randlc(&t1, t1);
        }

        an = t1;
        gc = 0.0;
        sx = 0.0;
        sy = 0.0;

        for (i = 0; i <= NQ - 1; i++) {
            q[i] = 0.0;
        }

        /*
         * each instance of this loop may be performed independently. we compute
         * the k offsets separately to take into account the fact that some nodes
         * have more numbers to generate than others.
         * the batches are distributed round-robin over the FMI ranks; the results
         * are independent of the number of ranks.
         */
        k_offset = -1;

        int batches_done = 0;
        int local_batches = (rank < np) ? ((np - 1 - rank) / size + 1) : 0;
        int print_freq = max(1, local_batches / 100);

        for (k = 1 + rank; k <= np; k += size) {
            kk = k_offset + k;
            t1 = S;
            t2 = an;

            /* find starting seed t1 for this kk */
            for (i = 1; i <= 100; i++) {
                ik = kk / 2;
                if ((2 * ik) != kk) {
                    t3 = randlc(&t1, t2);
                }
                if (ik == 0) {
                    break;
                }
                t3 = randlc(&t2, t2);
                kk = ik;
            }

            /* compute uniform pseudorandom numbers */
            vranlc(2 * nk, &t1, A, x.data());

            /*
             * compute gaussian deviates by acceptance-rejection method and
             * tally counts in concentric square annuli. this loop is not
             * vectorizable.
             */

            for (i = 0; i < nk; i++) {
                x1 = 2.0 * x[2 * i] - 1.0;
                x2 = 2.0 * x[2 * i + 1] - 1.0;
                t1 = pow2(x1) + pow2(x2);
                if (t1 <= 1.0) {
                    t2 = sqrt(-2.0 * log(t1) / t1);
                    t3 = (x1 * t2);
                    t4 = (x2 * t2);
                    l = max(fabs(t3), fabs(t4));
                    q[l] += 1.0;
                    sx = sx + t3;
                    sy = sy + t4;
                }
            }

            batches_done++;
            if (batches_done % print_freq == 0 || batches_done == local_batches) {
                printf("Rank %d: finished batch %d of %d\n", rank, batches_done, local_batches);
                fflush(stdout);
            }
        }

        // combine partial sums of all ranks
        double sums[NQ + 2], gsums[NQ + 2];
        sums[0] = sx;
        sums[1] = sy;
        memcpy(&sums[2], q, NQ * sizeof(double));

        allreduce_sum_double(comm, sums, gsums, NQ + 2);

        sx = gsums[0];
        sy = gsums[1];
        memcpy(q, &gsums[2], NQ * sizeof(double));

        for (i = 0; i <= NQ - 1; i++) {
            gc = gc + q[i];
        }

        tm = wtime() - t_start;

        const bool has_reference = reference_sums(m, sx_verify_value, sy_verify_value);
        verified = has_reference;
        if (verified) {
            sx_err = fabs((sx - sx_verify_value) / sx_verify_value);
            sy_err = fabs((sy - sy_verify_value) / sy_verify_value);
            verified = ((sx_err <= EPSILON) && (sy_err <= EPSILON));
        }
        Mops = pow(2.0, m + 1) / tm / 1000000.0;

        if (rank == 0) {
            printf("\n EP Benchmark Results:\n\n");
            printf(" CPU Time =%10.4f\n", tm);
            printf(" N = 2^%5d\n", m);
            printf(" No. Gaussian Pairs = %15.0f\n", gc);
            printf(" Sums = %25.15e %25.15e\n", sx, sy);
            printf(" Counts: \n");
            for (i = 0; i < NQ - 1; i++) {
                printf("%3d%15.0f\n", i, q[i]);
            }
            printf(" Mop/s total = %15.2f\n", Mops);
            printf(" Verification = %s\n", verified ? "SUCCESSFUL" : "UNSUCCESSFUL");
        } else {
            printf("Summary info on rank 0\n");
        }
        fflush(stdout);

        // Self validation: the allreduced sums must be finite and, when the reference table knows
        // this m, match the published values within a relative error of EPSILON. compare() from
        // util.hpp cannot be used here because its double specialization is an absolute tolerance.
        if (!std::isfinite(sx) || !std::isfinite(sy)) {
            std::cout << "rank " << rank << " returned non-finite sums " << sx << " " << sy << std::endl;
            ok = false;
        }
        if (has_reference) {
            if (sx_err > EPSILON) {
                std::cout << "rank " << rank << " returned wrong sx: expected " << sx_verify_value << ", got " << sx
                          << std::endl;
                ok = false;
            }
            if (sy_err > EPSILON) {
                std::cout << "rank " << rank << " returned wrong sy: expected " << sy_verify_value << ", got " << sy
                          << std::endl;
                ok = false;
            }
        } else {
            std::cout << "No EP reference sums for M = " << m << ", skipping sum verification for rank " << rank
                      << std::endl;
        }

        // Cross-rank check: the allreduce must have left every rank with the same sx/sy/gc. Collected
        // with a gather, i.e. a different collective than the one being verified, so a broken
        // allreduce cannot hide behind the check that inspects it.
        std::vector<double> triple = {sx, sy, gc};
        std::vector<double> all_triples;
        if (rank == 0) {
            all_triples = std::vector<double>(3 * size);
        }
        FMI::Comm::Data<std::vector<double>> _buf_triple(triple), _buf_all_triples(all_triples);
        comm.gather(_buf_triple, _buf_all_triples, 0);
        all_triples = _buf_all_triples.get();

        if (rank == 0) {
            for (int r = 0; r < size; r++) {
                ok &= compare<double>(r, "sx", sx, all_triples[3 * r]);
                ok &= compare<double>(r, "sy", sy, all_triples[3 * r + 1]);
                ok &= compare<double>(r, "gc", gc, all_triples[3 * r + 2]);
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

#undef max
#undef pow2
#undef A
#undef S
