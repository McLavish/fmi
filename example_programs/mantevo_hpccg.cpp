//@HEADER
// ************************************************************************
//
//               HPCCG: Simple Conjugate Gradient Benchmark Code
//                 Copyright (2006) Sandia Corporation
//
// Under terms of Contract DE-AC04-94AL85000, there is a non-exclusive
// license for use of this work by or on behalf of the U.S. Government.
//
// BSD 3-Clause License
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// * Neither the name of the copyright holder nor the names of its
//   contributors may be used to endorse or promote products derived from
//   this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// Questions? Contact Michael A. Heroux (maherou@sandia.gov)
//
// ************************************************************************
//@HEADER

// Conjugate gradient benchmark on a synthetic 27-point stencil matrix.
// The MPI calls of the original Mantevo code are replaced by FMI collectives
// and point-to-point operations.

#include <iostream>
using std::cerr;
using std::cout;
using std::endl;
#include <algorithm>
#include <cassert>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <sys/resource.h>
#include <sys/time.h>
#include <vector>

#include <Communicator.h>

#include "harness.hpp"
#include "mantevo_hpccg.hpp"

// These constants are upper bounds that might need to be changes for
// pathological matrices, e.g., those with nearly dense rows/columns.

const int max_external = 100000;
const int max_num_messages = 500;
const int max_num_neighbors = max_num_messages;

struct HPC_Sparse_Matrix_STRUCT {
    char *title;
    int start_row;
    int stop_row;
    int total_nrow;
    long long total_nnz;
    int local_nrow;
    int local_ncol; // Must be defined in make_local_matrix
    int local_nnz;
    int *nnz_in_row;
    double **ptr_to_vals_in_row;
    int **ptr_to_inds_in_row;
    double **ptr_to_diags;

    int num_external;
    int num_send_neighbors;
    int *external_index;
    int *external_local_index;
    int total_to_be_sent;
    int *elements_to_send;
    int *neighbors;
    int *recv_length;
    int *send_length;
    double *send_buffer;

    double *list_of_vals; // needed for cleaning up memory
    int *list_of_inds;    // needed for cleaning up memory
};
typedef struct HPC_Sparse_Matrix_STRUCT HPC_Sparse_Matrix;

static void destroyMatrix(HPC_Sparse_Matrix *&A) {
    if (A->title) {
        delete[] A->title;
    }
    if (A->nnz_in_row) {
        delete[] A->nnz_in_row;
    }
    if (A->list_of_vals) {
        delete[] A->list_of_vals;
    }
    if (A->ptr_to_vals_in_row != 0) {
        delete[] A->ptr_to_vals_in_row;
    }
    if (A->list_of_inds) {
        delete[] A->list_of_inds;
    }
    if (A->ptr_to_inds_in_row != 0) {
        delete[] A->ptr_to_inds_in_row;
    }
    if (A->ptr_to_diags) {
        delete[] A->ptr_to_diags;
    }

    if (A->external_index) {
        delete[] A->external_index;
    }
    if (A->external_local_index) {
        delete[] A->external_local_index;
    }
    if (A->elements_to_send) {
        delete[] A->elements_to_send;
    }
    if (A->neighbors) {
        delete[] A->neighbors;
    }
    if (A->recv_length) {
        delete[] A->recv_length;
    }
    if (A->send_length) {
        delete[] A->send_length;
    }
    if (A->send_buffer) {
        delete[] A->send_buffer;
    }

    delete A;
    A = 0;
}

static double mytimer(void) {
    struct timeval tp;
    static long start = 0, startu;
    if (!start) {
        gettimeofday(&tp, NULL);
        start = tp.tv_sec;
        startu = tp.tv_usec;
        return (0.0);
    }
    gettimeofday(&tp, NULL);
    return (((double)(tp.tv_sec - start)) + (tp.tv_usec - startu) / 1000000.0);
}

/////////////////////////////////////////////////////////////////////////
// FMI communication helpers, replacing the MPI calls of the original
// Mantevo code.
/////////////////////////////////////////////////////////////////////////

// Element-wise sum-allreduce of an int array; the FMI equivalent of
// MPI_Allreduce(sendbuf, recvbuf, count, MPI_INT, MPI_SUM, MPI_COMM_WORLD).
static void allreduce_sum_int(FMI::Communicator &comm, int *sendbuf, int *recvbuf, int count) {
    static const FMI::Utils::Function<std::vector<int>> sum(
        [](std::vector<int> a, std::vector<int> b) {
            for (std::size_t i = 0; i < a.size(); i++) {
                a[i] += b[i];
            }
            return a;
        },
        true, true);

    FMI::Comm::Data<std::vector<int>> senddata(std::vector<int>(sendbuf, sendbuf + count));
    FMI::Comm::Data<std::vector<int>> recvdata(static_cast<std::size_t>(count));
    comm.allreduce(senddata, recvdata, sum);
    std::vector<int> result = recvdata.get();
    std::copy(result.begin(), result.end(), recvbuf);
}

// Blocking pairwise exchange with one neighbor. Lower rank sends first, higher
// rank receives first. Skip empty transfers.
static void exchange_with_neighbor(FMI::Communicator &comm, int rank, int neighbor, void *sendbuf,
                                   std::size_t send_bytes, void *recvbuf, std::size_t recv_bytes) {
    FMI::Comm::Data<void *> senddata(sendbuf, send_bytes);
    FMI::Comm::Data<void *> recvdata(recvbuf, recv_bytes);
    if (rank < neighbor) {
        if (send_bytes > 0)
            comm.send(senddata, neighbor);
        if (recv_bytes > 0)
            comm.recv(recvdata, neighbor);
    } else {
        if (recv_bytes > 0)
            comm.recv(recvdata, neighbor);
        if (send_bytes > 0)
            comm.send(senddata, neighbor);
    }
}

static void generate_matrix(int size, int rank, int nx, int ny, int nz, HPC_Sparse_Matrix **A, double **x, double **b,
                            double **xexact) {
    *A = new HPC_Sparse_Matrix; // Allocate matrix struct and fill it
    (*A)->title = 0;

    // Set this bool to true if you want a 7-pt stencil instead of a 27 pt stencil
    bool use_7pt_stencil = false;

    int local_nrow = nx * ny * nz;   // This is the size of our subblock
    assert(local_nrow > 0);          // Must have something to work with
    int local_nnz = 27 * local_nrow; // Approximately 27 nonzeros per row (except
                                     // for boundary nodes)

    int total_nrow = local_nrow * size;           // Total number of grid points in mesh
    long long total_nnz = 27 * (long long)total_nrow; // Approximately 27 nonzeros per row (except
                                                      // for boundary nodes)

    int start_row = local_nrow * rank; // Each processor gets a section of a chimney stack domain
    int stop_row = start_row + local_nrow - 1;

    // Allocate arrays that are of length local_nrow
    (*A)->nnz_in_row = new int[local_nrow];
    (*A)->ptr_to_vals_in_row = new double *[local_nrow];
    (*A)->ptr_to_inds_in_row = new int *[local_nrow];
    (*A)->ptr_to_diags = new double *[local_nrow];

    *x = new double[local_nrow];
    *b = new double[local_nrow];
    *xexact = new double[local_nrow];

    // Allocate arrays that are of length local_nnz
    (*A)->list_of_vals = new double[local_nnz];
    (*A)->list_of_inds = new int[local_nnz];

    double *curvalptr = (*A)->list_of_vals;
    int *curindptr = (*A)->list_of_inds;

    long long nnzglobal = 0;
    for (int iz = 0; iz < nz; iz++) {
        for (int iy = 0; iy < ny; iy++) {
            for (int ix = 0; ix < nx; ix++) {
                int curlocalrow = iz * nx * ny + iy * nx + ix;
                int currow = start_row + iz * nx * ny + iy * nx + ix;
                int nnzrow = 0;
                (*A)->ptr_to_vals_in_row[curlocalrow] = curvalptr;
                (*A)->ptr_to_inds_in_row[curlocalrow] = curindptr;
                for (int sz = -1; sz <= 1; sz++) {
                    for (int sy = -1; sy <= 1; sy++) {
                        for (int sx = -1; sx <= 1; sx++) {
                            int curcol = currow + sz * nx * ny + sy * nx + sx;
                            //            Since we have a stack of nx by ny by nz domains ,
                            //            stacking in the z direction, we check to see if sx
                            //            and sy are reaching outside of the domain, while the
                            //            check for the curcol being valid is sufficient to
                            //            check the z values
                            if ((ix + sx >= 0) && (ix + sx < nx) && (iy + sy >= 0) && (iy + sy < ny) &&
                                (curcol >= 0 && curcol < total_nrow)) {
                                if (!use_7pt_stencil ||
                                    (sz * sz + sy * sy + sx * sx <= 1)) { // This logic will skip over point that are not
                                                                          // part of a 7-pt stencil
                                    if (curcol == currow) {
                                        (*A)->ptr_to_diags[curlocalrow] = curvalptr;
                                        *curvalptr++ = 27.0;
                                    } else {
                                        *curvalptr++ = -1.0;
                                    }
                                    *curindptr++ = curcol;
                                    nnzrow++;
                                }
                            }
                        } // end sx loop
                    } // end sy loop
                } // end sz loop
                (*A)->nnz_in_row[curlocalrow] = nnzrow;
                nnzglobal += nnzrow;
                (*x)[curlocalrow] = 0.0;
                (*b)[curlocalrow] = 27.0 - ((double)(nnzrow - 1));
                (*xexact)[curlocalrow] = 1.0;
            } // end ix loop
        } // end iy loop
    } // end iz loop

    cout << "Process " << rank << " of " << size << " has " << local_nrow << " rows. Global rows " << start_row
         << " through " << stop_row << endl;

    cout << "Process " << rank << " of " << size << " has " << local_nnz << " nonzeros." << endl;

    (*A)->start_row = start_row;
    (*A)->stop_row = stop_row;
    (*A)->total_nrow = total_nrow;
    (*A)->total_nnz = total_nnz;
    (*A)->local_nrow = local_nrow;
    (*A)->local_ncol = local_nrow;
    (*A)->local_nnz = local_nnz;

    return;
}

static void make_local_matrix(FMI::Communicator &comm, HPC_Sparse_Matrix *A, int size, int rank) {
    std::map<int, int> externals;
    int i, j, k;
    int num_external = 0;
    double t0;

    // Extract Matrix pieces
    int start_row = A->start_row;
    int stop_row = A->stop_row;
    int total_nrow = A->total_nrow;
    long long total_nnz = A->total_nnz;
    int local_nrow = A->local_nrow;
    int local_nnz = A->local_nnz;
    int *nnz_in_row = A->nnz_in_row;
    double **ptr_to_vals_in_row = A->ptr_to_vals_in_row;
    int **ptr_to_inds_in_row = A->ptr_to_inds_in_row;

    // We need to convert the index values for the rows on this processor
    // to a local index space. We need to:
    // - Determine if each index reaches to a local value or external value
    // - If local, subtract start_row from index value to get local index
    // - If external, find out if it is already accounted for.
    //   - If so, then do nothing,
    //   - otherwise
    //     - add it to the list of external indices,
    //     - find out which processor owns the value.
    //     - Set up communication for sparse MV operation.

    ///////////////////////////////////////////
    // Scan the indices and transform to local
    ///////////////////////////////////////////

    t0 = mytimer();

    int *external_index = new int[max_external];
    int *external_local_index = new int[max_external];
    A->external_index = external_index;
    A->external_local_index = external_local_index;

    for (i = 0; i < local_nrow; i++) {
        for (j = 0; j < nnz_in_row[i]; j++) {
            int cur_ind = ptr_to_inds_in_row[i][j];
            if (start_row <= cur_ind && cur_ind <= stop_row) {
                ptr_to_inds_in_row[i][j] -= start_row;
            } else // Must find out if we have already set up this point
            {
                if (externals.find(cur_ind) == externals.end()) {
                    externals[cur_ind] = num_external++;
                    if (num_external <= max_external) {
                        external_index[num_external - 1] = cur_ind;
                        // Mark index as external by negating it
                        ptr_to_inds_in_row[i][j] = -(ptr_to_inds_in_row[i][j] + 1);
                    } else {
                        cerr << "Must increase max_external in HPC_Sparse_Matrix.hpp" << endl;
                        abort();
                    }
                } else {
                    // Mark index as external by adding 1 and negating it
                    ptr_to_inds_in_row[i][j] = -(ptr_to_inds_in_row[i][j] + 1);
                }
            }
        }
    }

    t0 = mytimer() - t0;
    cout << "            Time in transform to local phase = " << t0 << endl;
    cout << "Processor " << rank << " of " << size << ": Number of external equations = " << num_external << endl;

    ////////////////////////////////////////////////////////////////////////////
    // Go through list of externals to find out which processors must be accessed.
    ////////////////////////////////////////////////////////////////////////////

    t0 = mytimer();

    A->num_external = num_external;
    int *tmp_buffer = new int[size]; // Temp buffer space needed below

    // Build list of global index offset

    int *global_index_offsets = new int[size];
    for (i = 0; i < size; i++)
        tmp_buffer[i] = 0; // First zero out

    tmp_buffer[rank] = start_row; // This is my start row

    // This call sends the start_row of each ith processor to the ith
    // entry of global_index_offset on all processors.
    // Thus, each processor know the range of indices owned by all
    // other processors.
    // Note:  There might be a better algorithm for doing this, but this
    //        will work...

    allreduce_sum_int(comm, tmp_buffer, global_index_offsets, size);

    // Go through list of externals and find the processor that owns each
    int *external_processor = new int[num_external];
    int *new_external_processor = new int[num_external];

    for (i = 0; i < num_external; i++) {
        int cur_ind = external_index[i];
        for (int j = size - 1; j >= 0; j--)
            if (global_index_offsets[j] <= cur_ind) {
                external_processor[i] = j;
                break;
            }
    }

    t0 = mytimer() - t0;
    cout << "          Time in finding processors phase = " << t0 << endl;

    ////////////////////////////////////////////////////////////////////////////
    // Sift through the external elements. For each newly encountered external
    // point assign it the next index in the sequence. Then look for other
    // external elements who are update by the same node and assign them the next
    // set of index numbers in the sequence (ie. elements updated by the same node
    // have consecutive indices).
    ////////////////////////////////////////////////////////////////////////////

    t0 = mytimer();

    int count = local_nrow;
    for (i = 0; i < num_external; i++)
        external_local_index[i] = -1;

    for (i = 0; i < num_external; i++) {
        if (external_local_index[i] == -1) {
            external_local_index[i] = count++;

            for (j = i + 1; j < num_external; j++) {
                if (external_processor[j] == external_processor[i])
                    external_local_index[j] = count++;
            }
        }
    }

    t0 = mytimer() - t0;
    cout << "           Time in scanning external indices phase = " << t0 << endl;

    t0 = mytimer();

    for (i = 0; i < local_nrow; i++) {
        for (j = 0; j < nnz_in_row[i]; j++) {
            if (ptr_to_inds_in_row[i][j] < 0) // Change index values of externals
            {
                int cur_ind = -ptr_to_inds_in_row[i][j] - 1;
                ptr_to_inds_in_row[i][j] = external_local_index[externals[cur_ind]];
            }
        }
    }

    for (i = 0; i < num_external; i++)
        new_external_processor[i] = 0;

    for (i = 0; i < num_external; i++)
        new_external_processor[external_local_index[i] - local_nrow] = external_processor[i];

    t0 = mytimer() - t0;
    cout << "           Time in assigning external indices phase = " << t0 << endl;

    for (i = 0; i < num_external; i++) {
        cout << "Processor " << rank << " of " << size << ": external processor[" << i
             << "] = " << external_processor[i] << endl;
        cout << "Processor " << rank << " of " << size << ": new external processor[" << i
             << "] = " << new_external_processor[i] << endl;
    }

    ////////////////////////////////////////////////////////////////////////////
    ///
    // Count the number of neighbors from which we receive information to update
    // our external elements. Additionally, fill the array tmp_neighbors in the
    // following way:
    //      tmp_neighbors[i] = 0   ==>  No external elements are updated by
    //                              processor i.
    //      tmp_neighbors[i] = x   ==>  (x-1)/size elements are updated from
    //                              processor i.
    ///
    ////////////////////////////////////////////////////////////////////////////

    t0 = mytimer();
    int *tmp_neighbors = new int[size];
    for (i = 0; i < size; i++)
        tmp_neighbors[i] = 0;

    int num_recv_neighbors = 0;

    for (i = 0; i < num_external; i++) {
        if (tmp_neighbors[new_external_processor[i]] == 0) {
            num_recv_neighbors++;
            tmp_neighbors[new_external_processor[i]] = 1;
        }
        tmp_neighbors[new_external_processor[i]] += size;
    }

    /// sum over all processors all the tmp_neighbors arrays ///

    allreduce_sum_int(comm, tmp_neighbors, tmp_buffer, size);

    /// decode the combined 'tmp_neighbors' (stored in tmp_buffer)
    //  array from all the processors

    int num_send_neighbors = tmp_buffer[rank] % size;

    /// decode 'tmp_buffer[rank] to deduce total number of elements
    //  we must send

    int total_to_be_sent = (tmp_buffer[rank] - num_send_neighbors) / size;

    //
    // Check to see if we have enough workspace allocated.  This could be
    // dynamically modified, but let's keep it simple for now...
    //

    if (num_send_neighbors > max_num_messages) {
        cerr << "Must increase max_num_messages in HPC_Sparse_Matrix.hpp" << endl;
        cerr << "Must be at least " << num_send_neighbors << endl;
        abort();
    }

    if (total_to_be_sent > max_external) {
        cerr << "Must increase max_external in HPC_Sparse_Matrix.hpp" << endl;
        cerr << "Must be at least " << total_to_be_sent << endl;
        abort();
    }
    delete[] tmp_neighbors;

    t0 = mytimer() - t0;
    cout << "           Time in finding neighbors phase = " << t0 << endl;

    cout << "Processor " << rank << " of " << size << ": Number of send neighbors = " << num_send_neighbors << endl;

    cout << "Processor " << rank << " of " << size << ": Number of receive neighbors = " << num_recv_neighbors << endl;

    cout << "Processor " << rank << " of " << size << ": Total number of elements to send = " << total_to_be_sent
         << endl;

    /////////////////////////////////////////////////////////////////////////
    ///
    // Make a list of the neighbors that will send information to update our
    // external elements (in the order that we will receive this information).
    ///
    /////////////////////////////////////////////////////////////////////////

    int *recv_list = new int[max_external];

    j = 0;
    if (num_external > 0) {
        recv_list[j++] = new_external_processor[0];
        for (i = 1; i < num_external; i++) {
            if (new_external_processor[i - 1] != new_external_processor[i]) {
                recv_list[j++] = new_external_processor[i];
            }
        }
    }

    //
    // Construct 'send_list', the list of processors we must send data to. The
    // original code discovers this by sending a 0 length message to each of its
    // recv neighbors and receiving with MPI_ANY_SOURCE. FMI has no wildcard
    // receives, so instead we sum-allreduce a (size x size) connectivity matrix
    // in which row r holds the recv-neighbor flags of processor r; column
    // 'rank' then tells us exactly who receives data from us.
    //

    int *send_list = new int[num_send_neighbors];
    for (i = 0; i < num_send_neighbors; i++)
        send_list[i] = 0;

    int *connectivity = new int[size * size];
    int *global_connectivity = new int[size * size];
    for (i = 0; i < size * size; i++)
        connectivity[i] = 0;
    for (i = 0; i < num_recv_neighbors; i++)
        connectivity[rank * size + recv_list[i]] = 1;

    allreduce_sum_int(comm, connectivity, global_connectivity, size * size);

    j = 0;
    for (i = 0; i < size; i++) {
        if (global_connectivity[i * size + rank]) {
            if (j == num_send_neighbors) {
                cerr << "Connectivity matrix disagrees with neighbor counts" << endl;
                abort();
            }
            send_list[j++] = i;
        }
    }
    if (j != num_send_neighbors) {
        cerr << "Connectivity matrix disagrees with neighbor counts" << endl;
        abort();
    }

    delete[] connectivity;
    delete[] global_connectivity;

    /////////////////////////////////////////////////////////////////////////
    ///
    //  Compare the two lists. In most cases they should be the same.
    //  However, if they are not then add new entries to the recv list
    //  that are in the send list (but not already in the recv list).
    ///
    /////////////////////////////////////////////////////////////////////////

    for (j = 0; j < num_send_neighbors; j++) {
        int found = 0;
        for (i = 0; i < num_recv_neighbors; i++) {
            if (recv_list[i] == send_list[j])
                found = 1;
        }

        if (found == 0) {
            cout << "Processor " << rank << " of " << size << ": recv_list[" << num_recv_neighbors
                 << "] = " << send_list[j] << endl;
            recv_list[num_recv_neighbors] = send_list[j];
            (num_recv_neighbors)++;
        }
    }

    delete[] send_list;
    num_send_neighbors = num_recv_neighbors;

    if (num_send_neighbors > max_num_messages) {
        cerr << "Must increase max_external in HPC_Sparse_Matrix.hpp" << endl;
        abort();
    }

    /////////////////////////////////////////////////////////////////////////
    /// Start filling HPC_Sparse_Matrix struct
    /////////////////////////////////////////////////////////////////////////

    A->total_to_be_sent = total_to_be_sent;
    int *elements_to_send = new int[total_to_be_sent];
    A->elements_to_send = elements_to_send;

    for (i = 0; i < total_to_be_sent; i++)
        elements_to_send[i] = 0;

    //
    // Create 'new_external' which explicitly put the external elements in the
    // order given by 'external_local_index'
    //

    int *new_external = new int[num_external];
    for (i = 0; i < num_external; i++) {
        new_external[external_local_index[i] - local_nrow] = external_index[i];
    }

    /////////////////////////////////////////////////////////////////////////
    //
    // Send each processor the global index list of the external elements in the
    // order that I will want to receive them when updating my external elements
    //
    /////////////////////////////////////////////////////////////////////////

    int *neighbors = new int[max_num_neighbors];
    int *recv_length = new int[max_num_neighbors];
    int *send_length = new int[max_num_neighbors];

    A->neighbors = neighbors;
    A->recv_length = recv_length;
    A->send_length = send_length;

    j = 0;
    for (i = 0; i < num_recv_neighbors; i++) {
        int newlength = 0;

        // go through list of external elements until updating
        // processor changes

        while ((j < num_external) && (new_external_processor[j] == recv_list[i])) {
            newlength++;
            j++;
            if (j == num_external)
                break;
        }

        recv_length[i] = newlength;
        neighbors[i] = recv_list[i];
    }

    // Tell each neighbor how many externals we expect from it and learn how
    // many elements we must send to it. Walk the neighbors in ascending rank
    // order so the blocking pairwise exchanges cannot deadlock.

    for (int p = 0; p < size; p++) {
        int idx = -1;
        for (i = 0; i < num_recv_neighbors; i++) {
            if (neighbors[i] == p) {
                idx = i;
                break;
            }
        }
        if (idx < 0)
            continue;
        exchange_with_neighbor(comm, rank, p, recv_length + idx, sizeof(int), send_length + idx, sizeof(int));
    }

    ///////////////////////////////////////////////////////////////////
    // Build "elements_to_send" list.  These are the x elements I own
    // that need to be sent to other processors.
    ///////////////////////////////////////////////////////////////////

    // Send each neighbor the chunk of 'new_external' it must update us with
    // (grouped by neighbor, in neighbors[] order) and receive from it the list
    // of our own elements we must send.
    // Send in asceding order.

    for (int p = 0; p < size; p++) {
        int idx = -1;
        int send_offset = 0; // into new_external, grows by recv_length
        int recv_offset = 0; // into elements_to_send, grows by send_length
        for (i = 0; i < num_recv_neighbors; i++) {
            if (neighbors[i] == p) {
                idx = i;
                break;
            }
            send_offset += recv_length[i];
            recv_offset += send_length[i];
        }
        if (idx < 0)
            continue;
        exchange_with_neighbor(comm, rank, p, new_external + send_offset, recv_length[idx] * sizeof(int),
                               elements_to_send + recv_offset, send_length[idx] * sizeof(int));
    }

    /// replace global indices by local indices ///

    for (i = 0; i < total_to_be_sent; i++)
        elements_to_send[i] -= start_row;

    ////////////////
    // Finish up !!
    ////////////////

    A->num_send_neighbors = num_send_neighbors;
    A->local_ncol = A->local_nrow + num_external;

    // Used in exchange_externals
    double *send_buffer = new double[total_to_be_sent];
    A->send_buffer = send_buffer;

    delete[] tmp_buffer;
    delete[] global_index_offsets;
    delete[] recv_list;
    delete[] external_processor;
    delete[] new_external;
    delete[] new_external_processor;

    return;
}

#define TICK() t0 = mytimer() // Use TICK and TOCK to time a code section
#define TOCK(t) t += mytimer() - t0

static void exchange_externals(FMI::Communicator &comm, HPC_Sparse_Matrix *A, const double *x, int rank, int size) {
    int i;

    // Extract Matrix pieces

    int local_nrow = A->local_nrow;
    int num_neighbors = A->num_send_neighbors;
    int *recv_length = A->recv_length;
    int *send_length = A->send_length;
    int *neighbors = A->neighbors;
    double *send_buffer = A->send_buffer;
    int total_to_be_sent = A->total_to_be_sent;
    int *elements_to_send = A->elements_to_send;

    //
    // Externals are at end of locals
    //
    double *x_external = (double *)x + local_nrow;

    //
    // Fill up send buffer
    //

    for (i = 0; i < total_to_be_sent; i++)
        send_buffer[i] = x[elements_to_send[i]];

    //
    // Exchange with each neighbor.
    // Pairwise exchange with every neighbor in ascending rank order.
    //

    for (int p = 0; p < size; p++) {
        int idx = -1;
        int send_offset = 0;
        int recv_offset = 0;
        for (i = 0; i < num_neighbors; i++) {
            if (neighbors[i] == p) {
                idx = i;
                break;
            }
            send_offset += send_length[i];
            recv_offset += recv_length[i];
        }
        if (idx < 0)
            continue;
        exchange_with_neighbor(comm, rank, p, send_buffer + send_offset, send_length[idx] * sizeof(double),
                               x_external + recv_offset, recv_length[idx] * sizeof(double));
    }

    return;
}

static int HPC_sparsemv(HPC_Sparse_Matrix *A, const double *const x, double *const y) {
    const int nrow = (const int)A->local_nrow;

    for (int i = 0; i < nrow; i++) {
        double sum = 0.0;
        const double *const cur_vals = (const double *const)A->ptr_to_vals_in_row[i];

        const int *const cur_inds = (const int *const)A->ptr_to_inds_in_row[i];

        const int cur_nnz = (const int)A->nnz_in_row[i];

        for (int j = 0; j < cur_nnz; j++)
            sum += cur_vals[j] * x[cur_inds[j]];
        y[i] = sum;
    }
    return (0);
}

static int ddot(FMI::Communicator &comm, const int n, const double *const x, const double *const y,
                double *const result, double &time_allreduce) {
    double local_result = 0.0;
    if (y == x)
        for (int i = 0; i < n; i++)
            local_result += x[i] * x[i];
    else
        for (int i = 0; i < n; i++)
            local_result += x[i] * y[i];

    static const FMI::Utils::Function<double> sum([](double a, double b) { return a + b; }, true, true);
    double t0 = mytimer();
    FMI::Comm::Data<double> senddata(local_result);
    FMI::Comm::Data<double> recvdata(0.0);
    comm.allreduce(senddata, recvdata, sum);
    *result = recvdata.get();
    time_allreduce += mytimer() - t0;

    return (0);
}

static int waxpby(const int n, const double alpha, const double *const x, const double beta, const double *const y,
                  double *const w) {
    if (alpha == 1.0) {
        for (int i = 0; i < n; i++)
            w[i] = x[i] + beta * y[i];
    } else if (beta == 1.0) {
        for (int i = 0; i < n; i++)
            w[i] = alpha * x[i] + y[i];
    } else {
        for (int i = 0; i < n; i++)
            w[i] = alpha * x[i] + beta * y[i];
    }

    return (0);
}

static int HPCCG(FMI::Communicator &comm, HPC_Sparse_Matrix *A, const double *const b, double *const x,
                 const int max_iter, const double tolerance, int &niters, double &normr, double *times, int rank,
                 int size) {
    double t_begin = mytimer(); // Start timing right away

    double t0 = 0.0, t1 = 0.0, t2 = 0.0, t3 = 0.0, t4 = 0.0;
    double t5 = 0.0;

    int nrow = A->local_nrow;
    int ncol = A->local_ncol;

    double *r = new double[nrow];
    double *p = new double[ncol]; // In parallel case, A is rectangular
    double *Ap = new double[nrow];

    normr = 0.0;
    double rtrans = 0.0;
    double oldrtrans = 0.0;

    int print_freq = max_iter / 10;
    if (print_freq > 50)
        print_freq = 50;
    if (print_freq < 1)
        print_freq = 1;

    // p is of length ncols, copy x to p for sparse MV operation
    TICK();
    waxpby(nrow, 1.0, x, 0.0, x, p);
    TOCK(t2);
    TICK();
    exchange_externals(comm, A, p, rank, size);
    TOCK(t5);
    TICK();
    HPC_sparsemv(A, p, Ap);
    TOCK(t3);
    TICK();
    waxpby(nrow, 1.0, b, -1.0, Ap, r);
    TOCK(t2);
    TICK();
    ddot(comm, nrow, r, r, &rtrans, t4);
    TOCK(t1);
    normr = sqrt(rtrans);

    if (rank == 0)
        cout << "Initial Residual = " << normr << endl;

    for (int k = 1; k < max_iter && normr > tolerance; k++) {
        if (k == 1) {
            TICK();
            waxpby(nrow, 1.0, r, 0.0, r, p);
            TOCK(t2);
        } else {
            oldrtrans = rtrans;
            TICK();
            ddot(comm, nrow, r, r, &rtrans, t4);
            TOCK(t1); // 2*nrow ops
            double beta = rtrans / oldrtrans;
            TICK();
            waxpby(nrow, 1.0, r, beta, p, p);
            TOCK(t2); // 2*nrow ops
        }
        normr = sqrt(rtrans);
        if (rank == 0 && (k % print_freq == 0 || k + 1 == max_iter))
            cout << "Iteration = " << k << "   Residual = " << normr << endl;

        TICK();
        exchange_externals(comm, A, p, rank, size);
        TOCK(t5);
        TICK();
        HPC_sparsemv(A, p, Ap);
        TOCK(t3); // 2*nnz ops
        double alpha = 0.0;
        TICK();
        ddot(comm, nrow, p, Ap, &alpha, t4);
        TOCK(t1); // 2*nrow ops
        alpha = rtrans / alpha;
        TICK();
        waxpby(nrow, 1.0, x, alpha, p, x); // 2*nrow ops
        waxpby(nrow, 1.0, r, -alpha, Ap, r);
        TOCK(t2); // 2*nrow ops
        niters = k;
        cout << "Rank " << rank << ": finished iteration " << k << ", residual = " << normr << endl;
    }

    // Store times
    times[1] = t1; // ddot time
    times[2] = t2; // waxpby time
    times[3] = t3; // sparsemv time
    times[4] = t4; // AllReduce time
    times[5] = t5; // exchange boundary time
    delete[] p;
    delete[] Ap;
    delete[] r;
    times[0] = mytimer() - t_begin; // Total time. All done...
    return (0);
}

static uint32_t mantevo_hpccg(void *args, uint32_t, void *res) {
    mantevo_hpccg_input *input = static_cast<mantevo_hpccg_input *>(args);
    mantevo_hpccg_output *output = static_cast<mantevo_hpccg_output *>(res);

    HPC_Sparse_Matrix *A;
    double *x, *b, *xexact;
    int ierr = 0;
    double times[7];
    double t6 = 0.0;
    int nx = input->nx;
    int ny = input->ny;
    int nz = input->nz;

    int size = input->size;
    int rank = input->rank;

    FMI::Communicator comm(rank, size, fmi_examples::config_path(), fmi_examples::comm_name());
    // comm.barrier();

    generate_matrix(size, rank, nx, ny, nz, &A, &x, &b, &xexact);

    // Transform matrix indices from global to local values.
    // Define number of columns for the local matrix.

    t6 = mytimer();
    make_local_matrix(comm, A, size, rank);
    t6 = mytimer() - t6;
    times[6] = t6;

    int niters = 0;
    double normr = 0.0;
    int max_iter = input->max_iter;
    double tolerance = input->tolerance; // Set tolerance to zero to make all runs
                                         // do max_iter iterations
    ierr = HPCCG(comm, A, b, x, max_iter, tolerance, niters, normr, times, rank, size);

    if (ierr)
        cerr << "Error in call to CG: " << ierr << ".\n" << endl;

    if (rank == 0) {
        double fniters = niters;
        double fnrow = A->total_nrow;
        double fnnz = A->total_nnz;
        double fnops_ddot = fniters * 4 * fnrow;
        double fnops_waxpby = fniters * 6 * fnrow;
        double fnops_sparsemv = fniters * 2 * fnnz;
        double fnops = fnops_ddot + fnops_waxpby + fnops_sparsemv;

        cout << "Number of iterations " << niters << endl;
        cout << "Final residual " << normr << endl;

        cout << "Time Total " << times[0] << endl;
        cout << "Time DDOT " << times[1] << endl;
        cout << "Time WAXPBY " << times[2] << endl;
        cout << "Time SPARSEMV " << times[3] << endl;

        cout << "FLOPS Total" << fnops << endl;
        cout << "FLOPS DDOT" << fnops_ddot << endl;
        cout << "FLOPS WAXPBY" << fnops_waxpby << endl;
        cout << "FLOPS SPARSEMV" << fnops_sparsemv << endl;

        cout << "MFLOPS Total" << fnops / times[0] / 1.0E6 << endl;
        cout << "MFLOPS DDOT" << fnops_ddot / times[1] / 1.0E6 << endl;
        cout << "MFLOPS WAXPBY" << fnops_waxpby / times[2] / 1.0E6 << endl;
        cout << "MFLOPS SPARSEMV" << fnops_sparsemv / (times[3]) / 1.0E6 << endl;
    } else {
        cout << "Summary info on rank 0" << endl;
    }

    output->rank = rank;
    output->niters = niters;
    output->normr = normr;

    destroyMatrix(A);
    delete[] x;
    delete[] b;
    delete[] xexact;

    // Hold every channel open until all ranks are finished: FMI's ClientServer
    // teardown deletes objects peers may not have downloaded yet.
    fmi_examples::rank_barrier();

    return sizeof(mantevo_hpccg_output);
}

int main(int argc, char **argv) {
    fmi_examples::Flags flags;
    flags.add_int("nx", 64, "Local grid points in x per rank");
    flags.add_int("ny", 64, "Local grid points in y per rank");
    flags.add_int("nz", 64, "Local grid points in z per rank");
    flags.add_int("max-iter", 150, "Maximum number of CG iterations");
    flags.add_double("tolerance", 0.0, "Residual tolerance, 0 runs the full max_iter - 1 iterations");

    return fmi_examples::run(argc, argv, "mantevo_hpccg", flags, [](const fmi_examples::Options &opts) {
        fmi_examples::ExampleSpec spec;
        spec.name = "mantevo_hpccg";
        spec.input_size = sizeof(mantevo_hpccg_input);
        spec.output_size = sizeof(mantevo_hpccg_output);
        spec.get_context = [] { return _mantevo_hpccg::get_context(); };
        spec.free_context = [](void *ctx) { _mantevo_hpccg::free_context(ctx); };

        int nx = opts.flags.get_int("nx");
        int ny = opts.flags.get_int("ny");
        int nz = opts.flags.get_int("nz");
        int max_iter = opts.flags.get_int("max-iter");
        double tolerance = opts.flags.get_double("tolerance");

        spec.initialize_input = [nx, ny, nz, max_iter, tolerance](int func_num, int numcores, void *, char *ptr) {
            _mantevo_hpccg::fill_input(func_num, numcores, nx, ny, nz, max_iter, tolerance, ptr);
        };
        spec.check_output = [max_iter, tolerance](int func_num, int numcores, void *ctx, char *ptr) {
            return _mantevo_hpccg::verify_output(func_num, numcores, max_iter, tolerance, ctx, ptr);
        };
        spec.fn = mantevo_hpccg;
        return spec;
    });
}
