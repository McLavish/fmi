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
// vranlc from common/c_randdp.cpp) to an FMI function:
//  - M is a runtime input instead of a compile-time npbparams.hpp constant,
//  - the batch loop is distributed round-robin over the FMI ranks,
//  - the three MPI_Allreduce calls of the NPB MPI version (sx, sy, q) are
//    fused into a single sum-allreduce at the end.

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>
#include <sys/time.h>

#include <Communicator.h>

#include "harness.hpp"
#include "npb_ep.hpp"

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

/* ep */
static uint32_t npb_ep(void *args, uint32_t, void *res) {
    npb_ep_input *input = static_cast<npb_ep_input *>(args);
    npb_ep_output *output = static_cast<npb_ep_output *>(res);

    int size = input->size;
    int rank = input->rank;
    int m = input->m;

    double Mops, t1, t2, t3, t4, x1, x2;
    double sx, sy, tm, an, gc;
    double sx_verify_value, sy_verify_value, sx_err, sy_err;
    int np;
    int i, ik, kk, l, k;
    int k_offset;
    bool verified;
    double dum[3] = {1.0, 1.0, 1.0};

    if (m <= MK) {
        printf("EP: M = %d must be greater than MK = %d\n", m, MK);
        return 0;
    }

    const int mm = m - MK;
    const int nn = 1 << mm;
    const int nk = 1 << MK;
    const int nk_plus = 2 * nk + 1;

    double *x = new double[nk_plus];
    double q[NQ];

    FMI::Communicator comm(rank, size, fmi_examples::config_path(), fmi_examples::comm_name(),
                           fmi_examples::faas_memory());
    // comm.barrier();

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
    vranlc(0, &t1, A, x);

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
        vranlc(2 * nk, &t1, A, x);

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

    verified = _npb_ep::reference_sums(m, sx_verify_value, sy_verify_value);
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

    output->rank = rank;
    output->sx = sx;
    output->sy = sy;
    output->gc = gc;

    delete[] x;

    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(npb_ep_output);
}

#undef max
#undef pow2
#undef A
#undef S

int main(int argc, char **argv) {
    fmi_examples::Flags flags;
    flags.add_int("m", 28, "Log_2 of the number of random number pairs (24=class S, 28=class A)");

    return fmi_examples::run(argc, argv, "npb_ep", flags, [](const fmi_examples::Options &opts) {
        fmi_examples::ExampleSpec spec;
        spec.name = "npb_ep";
        spec.input_size = sizeof(npb_ep_input);
        spec.output_size = sizeof(npb_ep_output);
        spec.get_context = [] { return _npb_ep::get_context(); };
        spec.free_context = [](void *ctx) { _npb_ep::free_context(ctx); };
        int m = opts.flags.get_int("m");
        // The ported kernel bails out with a diagnostic (and no output) when m <= MK, exactly like
        // the NPB original. Since it returns rather than throws, the harness would score that as a
        // clean exit and — for rank 0, whose zero-filled output happens to satisfy every check —
        // report PASS for a benchmark that computed nothing. Reject the value up front instead.
        if (m <= MK) {
            throw std::runtime_error("npb_ep requires --m greater than " + std::to_string(MK) + " (MK); got " +
                                     std::to_string(m));
        }
        spec.initialize_input = [m](int func_num, int numcores, void *, char *ptr) {
            _npb_ep::fill_input(func_num, numcores, m, ptr);
        };
        spec.check_output = [m](int func_num, int numcores, void *ctx, char *ptr) {
            return _npb_ep::verify_output(func_num, numcores, m, ctx, ptr);
        };
        spec.fn = npb_ep;
        return spec;
    });
}
