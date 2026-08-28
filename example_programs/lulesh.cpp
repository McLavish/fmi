/*
  This is a Version 2.0 MPI + OpenMP implementation of LULESH

                 Copyright (c) 2010-2013.
      Lawrence Livermore National Security, LLC.
Produced at the Lawrence Livermore National Laboratory.
                  LLNL-CODE-461231
                All rights reserved.

This file is part of LULESH, Version 2.0.
Please also read this link -- http://www.opensource.org/licenses/index.php

//////////////
DIFFERENCES BETWEEN THIS VERSION (2.x) AND EARLIER VERSIONS:
* Addition of regions to make work more representative of multi-material codes
* Default size of each domain is 30^3 (27000 elem) instead of 45^3. This is
  more representative of our actual working set sizes
* Single source distribution supports pure serial, pure OpenMP, MPI-only, 
  and MPI+OpenMP
* Addition of ability to visualize the mesh using VisIt 
  https://wci.llnl.gov/codes/visit/download.html
* Various command line options (see ./lulesh2.0 -h)
 -q              : quiet mode - suppress stdout
 -i <iterations> : number of cycles to run
 -s <size>       : length of cube mesh along side
 -r <numregions> : Number of distinct regions (def: 11)
 -b <balance>    : Load balance between regions of a domain (def: 1)
 -c <cost>       : Extra cost of more expensive regions (def: 1)
 -f <filepieces> : Number of file parts for viz output (def: np/9)
 -p              : Print out progress
 -v              : Output viz file (requires compiling with -DVIZ_MESH
 -h              : This message

 printf("Usage: %s [opts]\n", execname);
      printf(" where [opts] is one or more of:\n");
      printf(" -q              : quiet mode - suppress all stdout\n");
      printf(" -i <iterations> : number of cycles to run\n");
      printf(" -s <size>       : length of cube mesh along side\n");
      printf(" -r <numregions> : Number of distinct regions (def: 11)\n");
      printf(" -b <balance>    : Load balance between regions of a domain (def: 1)\n");
      printf(" -c <cost>       : Extra cost of more expensive regions (def: 1)\n");
      printf(" -f <numfiles>   : Number of files to split viz dump into (def: (np+10)/9)\n");
      printf(" -p              : Print out progress\n");
      printf(" -v              : Output viz file (requires compiling with -DVIZ_MESH\n");
      printf(" -h              : This message\n");
      printf("\n\n");

*Notable changes in LULESH 2.0

* Split functionality into different files
lulesh.cc - where most (all?) of the timed functionality lies
lulesh-comm.cc - MPI functionality
lulesh-init.cc - Setup code
lulesh-viz.cc  - Support for visualization option
lulesh-util.cc - Non-timed functions
*
* The concept of "regions" was added, although every region is the same ideal
*    gas material, and the same sedov blast wave problem is still the only
*    problem its hardcoded to solve.
* Regions allow two things important to making this proxy app more representative:
*   Four of the LULESH routines are now performed on a region-by-region basis,
*     making the memory access patterns non-unit stride
*   Artificial load imbalances can be easily introduced that could impact
*     parallelization strategies.  
* The load balance flag changes region assignment.  Region number is raised to
*   the power entered for assignment probability.  Most likely regions changes
*   with MPI process id.
* The cost flag raises the cost of ~45% of the regions to evaluate EOS by the
*   entered multiple. The cost of 5% is 10x the entered multiple.
* MPI and OpenMP were added, and coalesced into a single version of the source
*   that can support serial builds, MPI-only, OpenMP-only, and MPI+OpenMP
* Added support to write plot files using "poor mans parallel I/O" when linked
*   with the silo library, which in turn can be read by VisIt.
* Enabled variable timestep calculation by default (courant condition), which
*   results in an additional reduction.
* Default domain (mesh) size reduced from 45^3 to 30^3
* Command line options to allow numerous test cases without needing to recompile
* Performance optimizations and code cleanup beyond LULESH 1.0
* Added a "Figure of Merit" calculation (elements solved per microsecond) and
*   output in support of using LULESH 2.0 for the 2017 CORAL procurement
*
* Possible Differences in Final Release (other changes possible)
*
* High Level mesh structure to allow data structure transformations
* Different default parameters
* Minor code performance changes and cleanup

TODO in future versions
* Add reader for (truly) unstructured meshes, probably serial only
* CMake based build system

//////////////

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

   * Redistributions of source code must retain the above copyright
     notice, this list of conditions and the disclaimer below.

   * Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the disclaimer (as noted below)
     in the documentation and/or other materials provided with the
     distribution.

   * Neither the name of the LLNS/LLNL nor the names of its contributors
     may be used to endorse or promote products derived from this software
     without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LAWRENCE LIVERMORE NATIONAL SECURITY, LLC,
THE U.S. DEPARTMENT OF ENERGY OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


Additional BSD Notice

1. This notice is required to be provided under our contract with the U.S.
   Department of Energy (DOE). This work was produced at Lawrence Livermore
   National Laboratory under Contract No. DE-AC52-07NA27344 with the DOE.

2. Neither the United States Government nor Lawrence Livermore National
   Security, LLC nor any of their employees, makes any warranty, express
   or implied, or assumes any liability or responsibility for the accuracy,
   completeness, or usefulness of any information, apparatus, product, or
   process disclosed, or represents that its use would not infringe
   privately-owned rights.

3. Also, reference herein to any specific commercial products, process, or
   services by trade name, trademark, manufacturer or otherwise does not
   necessarily constitute or imply its endorsement, recommendation, or
   favoring by the United States Government or Lawrence Livermore National
   Security, LLC. The views and opinions of authors expressed herein do not
   necessarily state or reflect those of the United States Government or
   Lawrence Livermore National Security, LLC, and shall not be used for
   advertising or product endorsement purposes.

*/

// Sedov blast-wave shock hydrodynamics on a 3D hexahedral mesh: LULESH 2.0, the LLNL proxy
// application (LLNL-CODE-461231). The physics is the unmodified upstream code; its MPI calls
// go through the small MPI-compatible layer below, implemented on FMI collectives and blocking
// point-to-point operations. The non-blocking 26-neighbour halo exchange is emulated by
// recording the Irecv/Isend requests and executing the batch at the first Wait in a
// rank-ordered, deadlock-free sequence.
//
// Assembled from McLavish/LULESH_FMI @ a57e9bc (lulesh.h, lulesh-fmi.{h,cc}, lulesh-init.cc,
// lulesh-comm.cc, lulesh.cc). The OpenMP pragmas, the silo writer and the argv/environment
// plumbing are dropped in favour of the shared example launcher; main() and its result checks
// are this port's.

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <Communicator.h>

#include "launcher.hpp"
#include "util.hpp"

// ===================== MPI subset over FMI: declarations =====================
//
// Exactly the MPI API LULESH uses, so the physics sources compile unchanged. LULESH only ever
// passes MPI_COMM_WORLD around and never inspects a status, so the handle types are trivial.
// MPI_Datatype encodes the element size in its value so that count * datatype is the message
// size in bytes. The implementation follows the LULESH header.

#define USE_MPI 0
#define USE_FMI 1
#define USE_DISTRIBUTED 1

typedef int MPI_Comm;
typedef int MPI_Request;
typedef int MPI_Status;

enum MPI_Datatype { MPI_DATATYPE_NULL = 0, MPI_FLOAT = 4, MPI_DOUBLE = 8 };
enum MPI_Op { MPI_OP_NULL = 0, MPI_MIN, MPI_MAX, MPI_SUM };

inline constexpr MPI_Comm    MPI_COMM_WORLD   = 0;
inline constexpr MPI_Request MPI_REQUEST_NULL = 0;
inline constexpr int         MPI_SUCCESS      = 0;

int    MPI_Comm_rank(MPI_Comm comm, int *rank);
int    MPI_Comm_size(MPI_Comm comm, int *size);
int    MPI_Abort(MPI_Comm comm, int errorcode);
double MPI_Wtime();
int    MPI_Barrier(MPI_Comm comm);
int    MPI_Allreduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op,
                     MPI_Comm comm);
int    MPI_Reduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root,
                  MPI_Comm comm);
int    MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int src, int tag, MPI_Comm comm, MPI_Request *request);
int    MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm,
                 MPI_Request *request);
int    MPI_Wait(MPI_Request *request, MPI_Status *status);
int    MPI_Waitall(int count, MPI_Request *requests, MPI_Status *statuses);

// ===================== lulesh.h (its backend-selection block is replaced by the defines above) =====================


#if USE_DISTRIBUTED
/*
   define one of these three symbols:

   SEDOV_SYNC_POS_VEL_NONE
   SEDOV_SYNC_POS_VEL_EARLY
   SEDOV_SYNC_POS_VEL_LATE
*/

#define SEDOV_SYNC_POS_VEL_EARLY 1
#endif

#include <math.h>
#include <stdlib.h>
#include <stdint.h>
#include <vector>

//**************************************************
// Allow flexibility for arithmetic representations 
//**************************************************

#define MAX(a, b) ( ((a) > (b)) ? (a) : (b))


// Precision specification
typedef float        real4 ;
typedef double       real8 ;
typedef long double  real10 ;  // 10 bytes on x86

typedef int32_t Int4_t ;
typedef int64_t Int8_t ;
typedef Int4_t  Index_t ; // array subscript and loop index
typedef real8   Real_t ;  // floating point representation
typedef Int4_t  Int_t ;   // integer representation

enum { VolumeError = -1, QStopError = -2 } ;

inline real4  SQRT(real4  arg) { return sqrtf(arg) ; }
inline real8  SQRT(real8  arg) { return sqrt(arg) ; }
inline real10 SQRT(real10 arg) { return sqrtl(arg) ; }

inline real4  CBRT(real4  arg) { return cbrtf(arg) ; }
inline real8  CBRT(real8  arg) { return cbrt(arg) ; }
inline real10 CBRT(real10 arg) { return cbrtl(arg) ; }

inline real4  FABS(real4  arg) { return fabsf(arg) ; }
inline real8  FABS(real8  arg) { return fabs(arg) ; }
inline real10 FABS(real10 arg) { return fabsl(arg) ; }


// Stuff needed for boundary conditions
// 2 BCs on each of 6 hexahedral faces (12 bits)
#define XI_M        0x00007
#define XI_M_SYMM   0x00001
#define XI_M_FREE   0x00002
#define XI_M_COMM   0x00004

#define XI_P        0x00038
#define XI_P_SYMM   0x00008
#define XI_P_FREE   0x00010
#define XI_P_COMM   0x00020

#define ETA_M       0x001c0
#define ETA_M_SYMM  0x00040
#define ETA_M_FREE  0x00080
#define ETA_M_COMM  0x00100

#define ETA_P       0x00e00
#define ETA_P_SYMM  0x00200
#define ETA_P_FREE  0x00400
#define ETA_P_COMM  0x00800

#define ZETA_M      0x07000
#define ZETA_M_SYMM 0x01000
#define ZETA_M_FREE 0x02000
#define ZETA_M_COMM 0x04000

#define ZETA_P      0x38000
#define ZETA_P_SYMM 0x08000
#define ZETA_P_FREE 0x10000
#define ZETA_P_COMM 0x20000

// MPI Message Tags
#define MSG_COMM_SBN      1024
#define MSG_SYNC_POS_VEL  2048
#define MSG_MONOQ         3072

#define MAX_FIELDS_PER_MPI_COMM 6

// Assume 128 byte coherence
// Assume Real_t is an "integral power of 2" bytes wide
#define CACHE_COHERENCE_PAD_REAL (128 / sizeof(Real_t))

#define CACHE_ALIGN_REAL(n) \
   (((n) + (CACHE_COHERENCE_PAD_REAL - 1)) & ~(CACHE_COHERENCE_PAD_REAL-1))

/*********************************/
/* Data structure implementation */
/*********************************/

/* might want to add access methods so that memory can be */
/* better managed, as in luleshFT */

template <typename T>
T *Allocate(size_t size)
{
   return static_cast<T *>(malloc(sizeof(T)*size)) ;
}

template <typename T>
void Release(T **ptr)
{
   if (*ptr != NULL) {
      free(*ptr) ;
      *ptr = NULL ;
   }
}

//////////////////////////////////////////////////////
// Primary data structure
//////////////////////////////////////////////////////

/*
 * The implementation of the data abstraction used for lulesh
 * resides entirely in the Domain class below.  You can change
 * grouping and interleaving of fields here to maximize data layout
 * efficiency for your underlying architecture or compiler.
 *
 * For example, fields can be implemented as STL objects or
 * raw array pointers.  As another example, individual fields
 * m_x, m_y, m_z could be budled into
 *
 *    struct { Real_t x, y, z ; } *m_coord ;
 *
 * allowing accessor functions such as
 *
 *  "Real_t &x(Index_t idx) { return m_coord[idx].x ; }"
 *  "Real_t &y(Index_t idx) { return m_coord[idx].y ; }"
 *  "Real_t &z(Index_t idx) { return m_coord[idx].z ; }"
 */

class Domain {

   public:

   // Constructor
   Domain(Int_t numRanks, Index_t colLoc,
          Index_t rowLoc, Index_t planeLoc,
          Index_t nx, Int_t tp, Int_t nr, Int_t balance, Int_t cost);

   // Destructor
   ~Domain();

   //
   // ALLOCATION
   //

   void AllocateNodePersistent(Int_t numNode) // Node-centered
   {
      m_x.resize(numNode);  // coordinates
      m_y.resize(numNode);
      m_z.resize(numNode);

      m_xd.resize(numNode); // velocities
      m_yd.resize(numNode);
      m_zd.resize(numNode);

      m_xdd.resize(numNode); // accelerations
      m_ydd.resize(numNode);
      m_zdd.resize(numNode);

      m_fx.resize(numNode);  // forces
      m_fy.resize(numNode);
      m_fz.resize(numNode);

      m_nodalMass.resize(numNode);  // mass
   }

   void AllocateElemPersistent(Int_t numElem) // Elem-centered
   {
      m_nodelist.resize(8*numElem);

      // elem connectivities through face
      m_lxim.resize(numElem);
      m_lxip.resize(numElem);
      m_letam.resize(numElem);
      m_letap.resize(numElem);
      m_lzetam.resize(numElem);
      m_lzetap.resize(numElem);

      m_elemBC.resize(numElem);

      m_e.resize(numElem);
      m_p.resize(numElem);

      m_q.resize(numElem);
      m_ql.resize(numElem);
      m_qq.resize(numElem);

      m_v.resize(numElem);

      m_volo.resize(numElem);
      m_delv.resize(numElem);
      m_vdov.resize(numElem);

      m_arealg.resize(numElem);

      m_ss.resize(numElem);

      m_elemMass.resize(numElem);

      m_vnew.resize(numElem) ;
   }

   void AllocateGradients(Int_t numElem, Int_t allElem)
   {
      // Position gradients
      m_delx_xi   = Allocate<Real_t>(numElem) ;
      m_delx_eta  = Allocate<Real_t>(numElem) ;
      m_delx_zeta = Allocate<Real_t>(numElem) ;

      // Velocity gradients
      m_delv_xi   = Allocate<Real_t>(allElem) ;
      m_delv_eta  = Allocate<Real_t>(allElem);
      m_delv_zeta = Allocate<Real_t>(allElem) ;
   }

   void DeallocateGradients()
   {
      Release(&m_delx_zeta);
      Release(&m_delx_eta) ;
      Release(&m_delx_xi)  ;

      Release(&m_delv_zeta);
      Release(&m_delv_eta) ;
      Release(&m_delv_xi)  ;
   }

   void AllocateStrains(Int_t numElem)
   {
      m_dxx = Allocate<Real_t>(numElem) ;
      m_dyy = Allocate<Real_t>(numElem) ;
      m_dzz = Allocate<Real_t>(numElem) ;
   }

   void DeallocateStrains()
   {
      Release(&m_dzz) ;
      Release(&m_dyy) ;
      Release(&m_dxx) ;
   }
   
   //
   // ACCESSORS
   //

   // Node-centered

   // Nodal coordinates
   Real_t& x(Index_t idx)    { return m_x[idx] ; }
   Real_t& y(Index_t idx)    { return m_y[idx] ; }
   Real_t& z(Index_t idx)    { return m_z[idx] ; }

   // Nodal velocities
   Real_t& xd(Index_t idx)   { return m_xd[idx] ; }
   Real_t& yd(Index_t idx)   { return m_yd[idx] ; }
   Real_t& zd(Index_t idx)   { return m_zd[idx] ; }

   // Nodal accelerations
   Real_t& xdd(Index_t idx)  { return m_xdd[idx] ; }
   Real_t& ydd(Index_t idx)  { return m_ydd[idx] ; }
   Real_t& zdd(Index_t idx)  { return m_zdd[idx] ; }

   // Nodal forces
   Real_t& fx(Index_t idx)   { return m_fx[idx] ; }
   Real_t& fy(Index_t idx)   { return m_fy[idx] ; }
   Real_t& fz(Index_t idx)   { return m_fz[idx] ; }

   // Nodal mass
   Real_t& nodalMass(Index_t idx) { return m_nodalMass[idx] ; }

   // Nodes on symmertry planes
   Index_t symmX(Index_t idx) { return m_symmX[idx] ; }
   Index_t symmY(Index_t idx) { return m_symmY[idx] ; }
   Index_t symmZ(Index_t idx) { return m_symmZ[idx] ; }
   bool symmXempty()          { return m_symmX.empty(); }
   bool symmYempty()          { return m_symmY.empty(); }
   bool symmZempty()          { return m_symmZ.empty(); }

   //
   // Element-centered
   //
   Index_t&  regElemSize(Index_t idx) { return m_regElemSize[idx] ; }
   Index_t&  regNumList(Index_t idx) { return m_regNumList[idx] ; }
   Index_t*  regNumList()            { return &m_regNumList[0] ; }
   Index_t*  regElemlist(Int_t r)    { return m_regElemlist[r] ; }
   Index_t&  regElemlist(Int_t r, Index_t idx) { return m_regElemlist[r][idx] ; }

   Index_t*  nodelist(Index_t idx)    { return &m_nodelist[Index_t(8)*idx] ; }

   // elem connectivities through face
   Index_t&  lxim(Index_t idx) { return m_lxim[idx] ; }
   Index_t&  lxip(Index_t idx) { return m_lxip[idx] ; }
   Index_t&  letam(Index_t idx) { return m_letam[idx] ; }
   Index_t&  letap(Index_t idx) { return m_letap[idx] ; }
   Index_t&  lzetam(Index_t idx) { return m_lzetam[idx] ; }
   Index_t&  lzetap(Index_t idx) { return m_lzetap[idx] ; }

   // elem face symm/free-surface flag
   Int_t&  elemBC(Index_t idx) { return m_elemBC[idx] ; }

   // Principal strains - temporary
   Real_t& dxx(Index_t idx)  { return m_dxx[idx] ; }
   Real_t& dyy(Index_t idx)  { return m_dyy[idx] ; }
   Real_t& dzz(Index_t idx)  { return m_dzz[idx] ; }

   // New relative volume - temporary
   Real_t& vnew(Index_t idx)  { return m_vnew[idx] ; }

   // Velocity gradient - temporary
   Real_t& delv_xi(Index_t idx)    { return m_delv_xi[idx] ; }
   Real_t& delv_eta(Index_t idx)   { return m_delv_eta[idx] ; }
   Real_t& delv_zeta(Index_t idx)  { return m_delv_zeta[idx] ; }

   // Position gradient - temporary
   Real_t& delx_xi(Index_t idx)    { return m_delx_xi[idx] ; }
   Real_t& delx_eta(Index_t idx)   { return m_delx_eta[idx] ; }
   Real_t& delx_zeta(Index_t idx)  { return m_delx_zeta[idx] ; }

   // Energy
   Real_t& e(Index_t idx)          { return m_e[idx] ; }

   // Pressure
   Real_t& p(Index_t idx)          { return m_p[idx] ; }

   // Artificial viscosity
   Real_t& q(Index_t idx)          { return m_q[idx] ; }

   // Linear term for q
   Real_t& ql(Index_t idx)         { return m_ql[idx] ; }
   // Quadratic term for q
   Real_t& qq(Index_t idx)         { return m_qq[idx] ; }

   // Relative volume
   Real_t& v(Index_t idx)          { return m_v[idx] ; }
   Real_t& delv(Index_t idx)       { return m_delv[idx] ; }

   // Reference volume
   Real_t& volo(Index_t idx)       { return m_volo[idx] ; }

   // volume derivative over volume
   Real_t& vdov(Index_t idx)       { return m_vdov[idx] ; }

   // Element characteristic length
   Real_t& arealg(Index_t idx)     { return m_arealg[idx] ; }

   // Sound speed
   Real_t& ss(Index_t idx)         { return m_ss[idx] ; }

   // Element mass
   Real_t& elemMass(Index_t idx)  { return m_elemMass[idx] ; }

   Index_t nodeElemCount(Index_t idx)
   { return m_nodeElemStart[idx+1] - m_nodeElemStart[idx] ; }

   Index_t *nodeElemCornerList(Index_t idx)
   { return &m_nodeElemCornerList[m_nodeElemStart[idx]] ; }

   // Parameters 

   // Cutoffs
   Real_t u_cut() const               { return m_u_cut ; }
   Real_t e_cut() const               { return m_e_cut ; }
   Real_t p_cut() const               { return m_p_cut ; }
   Real_t q_cut() const               { return m_q_cut ; }
   Real_t v_cut() const               { return m_v_cut ; }

   // Other constants (usually are settable via input file in real codes)
   Real_t hgcoef() const              { return m_hgcoef ; }
   Real_t qstop() const               { return m_qstop ; }
   Real_t monoq_max_slope() const     { return m_monoq_max_slope ; }
   Real_t monoq_limiter_mult() const  { return m_monoq_limiter_mult ; }
   Real_t ss4o3() const               { return m_ss4o3 ; }
   Real_t qlc_monoq() const           { return m_qlc_monoq ; }
   Real_t qqc_monoq() const           { return m_qqc_monoq ; }
   Real_t qqc() const                 { return m_qqc ; }

   Real_t eosvmax() const             { return m_eosvmax ; }
   Real_t eosvmin() const             { return m_eosvmin ; }
   Real_t pmin() const                { return m_pmin ; }
   Real_t emin() const                { return m_emin ; }
   Real_t dvovmax() const             { return m_dvovmax ; }
   Real_t refdens() const             { return m_refdens ; }

   // Timestep controls, etc...
   Real_t& time()                 { return m_time ; }
   Real_t& deltatime()            { return m_deltatime ; }
   Real_t& deltatimemultlb()      { return m_deltatimemultlb ; }
   Real_t& deltatimemultub()      { return m_deltatimemultub ; }
   Real_t& stoptime()             { return m_stoptime ; }
   Real_t& dtcourant()            { return m_dtcourant ; }
   Real_t& dthydro()              { return m_dthydro ; }
   Real_t& dtmax()                { return m_dtmax ; }
   Real_t& dtfixed()              { return m_dtfixed ; }

   Int_t&  cycle()                { return m_cycle ; }
   Index_t&  numRanks()           { return m_numRanks ; }

   Index_t&  colLoc()             { return m_colLoc ; }
   Index_t&  rowLoc()             { return m_rowLoc ; }
   Index_t&  planeLoc()           { return m_planeLoc ; }
   Index_t&  tp()                 { return m_tp ; }

   Index_t&  sizeX()              { return m_sizeX ; }
   Index_t&  sizeY()              { return m_sizeY ; }
   Index_t&  sizeZ()              { return m_sizeZ ; }
   Index_t&  numReg()             { return m_numReg ; }
   Int_t&  cost()             { return m_cost ; }
   Index_t&  numElem()            { return m_numElem ; }
   Index_t&  numNode()            { return m_numNode ; }
   
   Index_t&  maxPlaneSize()       { return m_maxPlaneSize ; }
   Index_t&  maxEdgeSize()        { return m_maxEdgeSize ; }
   
   //
   // MPI-Related additional data
   //

#if USE_DISTRIBUTED
   // Communication Work space
   Real_t *commDataSend ;
   Real_t *commDataRecv ;
   
   // Maximum number of block neighbors 
   MPI_Request recvRequest[26] ; // 6 faces + 12 edges + 8 corners 
   MPI_Request sendRequest[26] ; // 6 faces + 12 edges + 8 corners 
#endif

  private:

   void BuildMesh(Int_t nx, Int_t edgeNodes, Int_t edgeElems);
   void SetupThreadSupportStructures();
   void CreateRegionIndexSets(Int_t nreg, Int_t balance);
   void SetupCommBuffers(Int_t edgeNodes);
   void SetupSymmetryPlanes(Int_t edgeNodes);
   void SetupElementConnectivities(Int_t edgeElems);
   void SetupBoundaryConditions(Int_t edgeElems);

   //
   // IMPLEMENTATION
   //

   /* Node-centered */
   std::vector<Real_t> m_x ;  /* coordinates */
   std::vector<Real_t> m_y ;
   std::vector<Real_t> m_z ;

   std::vector<Real_t> m_xd ; /* velocities */
   std::vector<Real_t> m_yd ;
   std::vector<Real_t> m_zd ;

   std::vector<Real_t> m_xdd ; /* accelerations */
   std::vector<Real_t> m_ydd ;
   std::vector<Real_t> m_zdd ;

   std::vector<Real_t> m_fx ;  /* forces */
   std::vector<Real_t> m_fy ;
   std::vector<Real_t> m_fz ;

   std::vector<Real_t> m_nodalMass ;  /* mass */

   std::vector<Index_t> m_symmX ;  /* symmetry plane nodesets */
   std::vector<Index_t> m_symmY ;
   std::vector<Index_t> m_symmZ ;

   // Element-centered

   // Region information
   Int_t    m_numReg ;
   Int_t    m_cost; //imbalance cost
   Index_t *m_regElemSize ;   // Size of region sets
   Index_t *m_regNumList ;    // Region number per domain element
   Index_t **m_regElemlist ;  // region indexset 

   std::vector<Index_t>  m_nodelist ;     /* elemToNode connectivity */

   std::vector<Index_t>  m_lxim ;  /* element connectivity across each face */
   std::vector<Index_t>  m_lxip ;
   std::vector<Index_t>  m_letam ;
   std::vector<Index_t>  m_letap ;
   std::vector<Index_t>  m_lzetam ;
   std::vector<Index_t>  m_lzetap ;

   std::vector<Int_t>    m_elemBC ;  /* symmetry/free-surface flags for each elem face */

   Real_t             *m_dxx ;  /* principal strains -- temporary */
   Real_t             *m_dyy ;
   Real_t             *m_dzz ;

   Real_t             *m_delv_xi ;    /* velocity gradient -- temporary */
   Real_t             *m_delv_eta ;
   Real_t             *m_delv_zeta ;

   Real_t             *m_delx_xi ;    /* coordinate gradient -- temporary */
   Real_t             *m_delx_eta ;
   Real_t             *m_delx_zeta ;
   
   std::vector<Real_t> m_e ;   /* energy */

   std::vector<Real_t> m_p ;   /* pressure */
   std::vector<Real_t> m_q ;   /* q */
   std::vector<Real_t> m_ql ;  /* linear term for q */
   std::vector<Real_t> m_qq ;  /* quadratic term for q */

   std::vector<Real_t> m_v ;     /* relative volume */
   std::vector<Real_t> m_volo ;  /* reference volume */
   std::vector<Real_t> m_vnew ;  /* new relative volume -- temporary */
   std::vector<Real_t> m_delv ;  /* m_vnew - m_v */
   std::vector<Real_t> m_vdov ;  /* volume derivative over volume */

   std::vector<Real_t> m_arealg ;  /* characteristic length of an element */
   
   std::vector<Real_t> m_ss ;      /* "sound speed" */

   std::vector<Real_t> m_elemMass ;  /* mass */

   // Cutoffs (treat as constants)
   const Real_t  m_e_cut ;             // energy tolerance 
   const Real_t  m_p_cut ;             // pressure tolerance 
   const Real_t  m_q_cut ;             // q tolerance 
   const Real_t  m_v_cut ;             // relative volume tolerance 
   const Real_t  m_u_cut ;             // velocity tolerance 

   // Other constants (usually setable, but hardcoded in this proxy app)

   const Real_t  m_hgcoef ;            // hourglass control 
   const Real_t  m_ss4o3 ;
   const Real_t  m_qstop ;             // excessive q indicator 
   const Real_t  m_monoq_max_slope ;
   const Real_t  m_monoq_limiter_mult ;
   const Real_t  m_qlc_monoq ;         // linear term coef for q 
   const Real_t  m_qqc_monoq ;         // quadratic term coef for q 
   const Real_t  m_qqc ;
   const Real_t  m_eosvmax ;
   const Real_t  m_eosvmin ;
   const Real_t  m_pmin ;              // pressure floor 
   const Real_t  m_emin ;              // energy floor 
   const Real_t  m_dvovmax ;           // maximum allowable volume change 
   const Real_t  m_refdens ;           // reference density 

   // Variables to keep track of timestep, simulation time, and cycle
   Real_t  m_dtcourant ;         // courant constraint 
   Real_t  m_dthydro ;           // volume change constraint 
   Int_t   m_cycle ;             // iteration count for simulation 
   Real_t  m_dtfixed ;           // fixed time increment 
   Real_t  m_time ;              // current time 
   Real_t  m_deltatime ;         // variable time increment 
   Real_t  m_deltatimemultlb ;
   Real_t  m_deltatimemultub ;
   Real_t  m_dtmax ;             // maximum allowable time increment 
   Real_t  m_stoptime ;          // end time for simulation 


   Int_t   m_numRanks ;

   Index_t m_colLoc ;
   Index_t m_rowLoc ;
   Index_t m_planeLoc ;
   Index_t m_tp ;

   Index_t m_sizeX ;
   Index_t m_sizeY ;
   Index_t m_sizeZ ;
   Index_t m_numElem ;
   Index_t m_numNode ;

   Index_t m_maxPlaneSize ;
   Index_t m_maxEdgeSize ;

   // OMP hack 
   Index_t *m_nodeElemStart ;
   Index_t *m_nodeElemCornerList ;

   // Used in setup
   Index_t m_rowMin, m_rowMax;
   Index_t m_colMin, m_colMax;
   Index_t m_planeMin, m_planeMax ;

} ;

typedef Real_t &(Domain::* Domain_member )(Index_t) ;

struct cmdLineOpts {
   Int_t its; // -i 
   Int_t nx;  // -s 
   Int_t numReg; // -r 
   Int_t numFiles; // -f
   Int_t showProg; // -p
   Int_t quiet; // -q
   Int_t viz; // -v 
   Int_t cost; // -c
   Int_t balance; // -b
};



// Function Prototypes

// lulesh-par
Real_t CalcElemVolume( const Real_t x[8],
                       const Real_t y[8],
                       const Real_t z[8]);

// lulesh-util
void ParseCommandLineOptions(int argc, char *argv[],
                             Int_t myRank, struct cmdLineOpts *opts);
void VerifyAndWriteFinalOutput(Real_t elapsed_time,
                               Domain& locDom,
                               Int_t nx,
                               Int_t numRanks);

// lulesh-viz
void DumpToVisit(Domain& domain, int numFiles, int myRank, int numRanks);

// lulesh-comm
void CommRecv(Domain& domain, Int_t msgType, Index_t xferFields,
              Index_t dx, Index_t dy, Index_t dz,
              bool doRecv, bool planeOnly);
void CommSend(Domain& domain, Int_t msgType,
              Index_t xferFields, Domain_member *fieldData,
              Index_t dx, Index_t dy, Index_t dz,
              bool doSend, bool planeOnly);
void CommSBN(Domain& domain, Int_t xferFields, Domain_member *fieldData);
void CommSyncPosVel(Domain& domain);
void CommMonoQ(Domain& domain);

// lulesh-init
void InitMeshDecomp(Int_t numRanks, Int_t myRank,
                    Int_t *col, Int_t *row, Int_t *plane, Int_t *side);

// ===================== MPI subset over FMI: implementation =====================
//
// FMI only offers BLOCKING send/recv and is not safe to drive concurrently on one
// Communicator, so LULESH's non-blocking 26-neighbour halo exchange (Irecv/Isend ...
// Wait/Waitall) is emulated by RECORDING each request and executing the whole batch at the
// first Wait/Waitall, in a deadlock-free order.
//
// Deadlock freedom: every send has a matching recv on the peer (MPI semantics). We iterate
// the peers we exchange with in ascending rank order; for peer P, the lower-ranked endpoint
// of the pair sends first, the higher-ranked one receives first. The globally-lowest
// unfinished rank therefore never blocks on a send before it reaches a recv that drains a
// higher peer, so progress is guaranteed up the ranks. (This is the rule
// exchange_with_neighbor in mantevo_hpccg.cpp follows, applied to a whole batch.)
//
// Everything runs on the calling thread against the communicator main() built for this
// rank, attached with lulesh_fmi::bind() before the first MPI_* call.

namespace {

FMI::Communicator *g_comm = nullptr;
int g_rank = 0;
int g_size = 1;

// One recorded, not-yet-executed point-to-point request.
struct PendingOp {
    bool is_send;
    int peer;
    char *buf;
    size_t bytes;
};

std::vector<PendingOp> g_pending;
bool g_flushed = false; // the current batch has already been executed

// Every failure surfaces as an exception out of the MPI_* call, so main()'s catch reports the
// rank as crashed and it exits 1 like any other example rank.
[[noreturn]] void fail(const std::string &what) {
    throw std::runtime_error("[FMI rank " + std::to_string(g_rank) + "] " + what);
}

// A new exchange phase always starts with Irecv/Isend; clear the just-finished batch lazily
// when the first request of the next phase is recorded.
void reset_batch_if_flushed() {
    if (g_flushed) {
        g_pending.clear();
        g_flushed = false;
    }
}

// Record one point-to-point request for the current batch. LULESH's 26-neighbour stencil
// posts at most one message per peer per direction per phase, and that invariant is exactly
// what makes the tag-less, FIFO-matched flush correct: with two same-direction messages to
// one peer in a single phase, FMI's FIFO socket could pair them by arrival order and silently
// mismatch their sizes. MPI does not enforce this, so guard it here -- turn a future
// regression into a loud failure rather than silent data corruption.
void record_pending(bool is_send, int peer, char *buf, size_t bytes) {
    reset_batch_if_flushed();
    for (const PendingOp &op : g_pending) {
        if (op.peer == peer && op.is_send == is_send) {
            fail(std::string(is_send ? "MPI_Isend" : "MPI_Irecv") + ": a second " +
                 (is_send ? "send to" : "recv from") + " peer " + std::to_string(peer) +
                 " in one exchange phase breaks the one-message-per-peer-per-direction "
                 "assumption (tag-less FIFO matching would mispair them)");
        }
    }
    g_pending.push_back(PendingOp{is_send, peer, buf, bytes});
}

void send_one(const PendingOp &op) {
    FMI::Comm::Data<void *> d(static_cast<void *>(op.buf), op.bytes);
    g_comm->send(d, static_cast<FMI::Utils::peer_num>(op.peer));
}

void recv_one(const PendingOp &op) {
    FMI::Comm::Data<void *> d(static_cast<void *>(op.buf), op.bytes);
    g_comm->recv(d, static_cast<FMI::Utils::peer_num>(op.peer));
}

void flush_batch() {
    if (g_flushed)
        return;

    // Distinct peers we exchange with this phase, in ascending rank order.
    std::vector<int> peers;
    for (const PendingOp &op : g_pending) {
        if (std::find(peers.begin(), peers.end(), op.peer) == peers.end()) {
            peers.push_back(op.peer);
        }
    }
    std::sort(peers.begin(), peers.end());

    try {
        for (int p : peers) {
            const bool send_first = (g_rank < p);
            if (send_first) {
                for (const PendingOp &op : g_pending)
                    if (op.peer == p && op.is_send)
                        send_one(op);
                for (const PendingOp &op : g_pending)
                    if (op.peer == p && !op.is_send)
                        recv_one(op);
            } else {
                for (const PendingOp &op : g_pending)
                    if (op.peer == p && !op.is_send)
                        recv_one(op);
                for (const PendingOp &op : g_pending)
                    if (op.peer == p && op.is_send)
                        send_one(op);
            }
        }
    } catch (const std::exception &e) {
        fail(std::string("halo exchange failed: ") + e.what());
    } catch (...) {
        fail("halo exchange failed: unknown exception");
    }

    g_flushed = true;
}

// Collectives must never interleave with a half-flushed P2P batch. LULESH never does this,
// but fail loudly if a future change ever violates it.
void assert_no_pending(const char *who) {
    if (!g_pending.empty() && !g_flushed) {
        fail(std::string(who) + " called with an unflushed point-to-point batch");
    }
}

template <typename T>
FMI::Utils::Function<T> make_reduce_fn(MPI_Op op) {
    switch (op) {
    case MPI_MIN:
        return FMI::Utils::Function<T>([](T a, T b) { return a < b ? a : b; }, true, true);
    case MPI_MAX:
        return FMI::Utils::Function<T>([](T a, T b) { return a > b ? a : b; }, true, true);
    case MPI_SUM:
        return FMI::Utils::Function<T>([](T a, T b) { return a + b; }, true, true);
    default:
        fail("unsupported MPI_Op in reduction");
    }
}

// Scalar (count==1) allreduce/reduce for double or float, the only forms LULESH uses.
template <typename T>
void scalar_collective(const void *sendbuf, void *recvbuf, MPI_Op op, int root, bool allreduce) {
    T s = *static_cast<const T *>(sendbuf);
    FMI::Comm::Data<T> sd(s), rd;
    auto fn = make_reduce_fn<T>(op);
    if (allreduce) {
        g_comm->allreduce(sd, rd, fn);
        *static_cast<T *>(recvbuf) = rd.get();
    } else {
        g_comm->reduce(sd, rd, static_cast<FMI::Utils::peer_num>(root), fn);
        if (g_rank == root)
            *static_cast<T *>(recvbuf) = rd.get();
    }
}

} // namespace

namespace lulesh_fmi {

//! Route the MPI_* calls to the communicator main() built for this rank.
void bind(FMI::Communicator &comm, int rank, int size) {
    g_comm = &comm;
    g_rank = rank;
    g_size = size;
    g_pending.clear();
    g_flushed = false;
}

} // namespace lulesh_fmi

int MPI_Comm_rank(MPI_Comm /*comm*/, int *rank) {
    *rank = g_rank;
    return MPI_SUCCESS;
}

int MPI_Comm_size(MPI_Comm /*comm*/, int *size) {
    *size = g_size;
    return MPI_SUCCESS;
}

int MPI_Abort(MPI_Comm /*comm*/, int errorcode) {
    fail("MPI_Abort(code=" + std::to_string(errorcode) + ")");
}

double MPI_Wtime() {
    using clock = std::chrono::steady_clock;
    return std::chrono::duration<double>(clock::now().time_since_epoch()).count();
}

int MPI_Barrier(MPI_Comm /*comm*/) {
    assert_no_pending("MPI_Barrier");
    try {
        g_comm->barrier();
    } catch (const std::exception &e) {
        fail(std::string("barrier failed: ") + e.what());
    } catch (...) {
        fail("barrier failed: unknown exception");
    }
    return MPI_SUCCESS;
}

int MPI_Allreduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op,
                  MPI_Comm /*comm*/) {
    if (count != 1)
        fail("MPI_Allreduce shim supports count==1 only");
    assert_no_pending("MPI_Allreduce");
    try {
        if (datatype == MPI_DOUBLE)
            scalar_collective<double>(sendbuf, recvbuf, op, /*root*/ 0, /*allreduce*/ true);
        else if (datatype == MPI_FLOAT)
            scalar_collective<float>(sendbuf, recvbuf, op, /*root*/ 0, /*allreduce*/ true);
        else
            fail("MPI_Allreduce shim supports MPI_DOUBLE/MPI_FLOAT only");
    } catch (const std::exception &e) {
        fail(std::string("allreduce failed: ") + e.what());
    } catch (...) {
        fail("allreduce failed: unknown exception");
    }
    return MPI_SUCCESS;
}

int MPI_Reduce(const void *sendbuf, void *recvbuf, int count, MPI_Datatype datatype, MPI_Op op, int root,
               MPI_Comm /*comm*/) {
    if (count != 1)
        fail("MPI_Reduce shim supports count==1 only");
    assert_no_pending("MPI_Reduce");
    try {
        if (datatype == MPI_DOUBLE)
            scalar_collective<double>(sendbuf, recvbuf, op, root, /*allreduce*/ false);
        else if (datatype == MPI_FLOAT)
            scalar_collective<float>(sendbuf, recvbuf, op, root, /*allreduce*/ false);
        else
            fail("MPI_Reduce shim supports MPI_DOUBLE/MPI_FLOAT only");
    } catch (const std::exception &e) {
        fail(std::string("reduce failed: ") + e.what());
    } catch (...) {
        fail("reduce failed: unknown exception");
    }
    return MPI_SUCCESS;
}

// Point-to-point: recorded now, executed at the first Wait/Waitall.

int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int src, int /*tag*/, MPI_Comm /*comm*/,
              MPI_Request *request) {
    record_pending(/*is_send*/ false, src, static_cast<char *>(buf),
                   static_cast<size_t>(count) * static_cast<size_t>(datatype));
    if (request)
        *request = 1;
    return MPI_SUCCESS;
}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int /*tag*/, MPI_Comm /*comm*/,
              MPI_Request *request) {
    record_pending(/*is_send*/ true, dest, static_cast<char *>(const_cast<void *>(buf)),
                   static_cast<size_t>(count) * static_cast<size_t>(datatype));
    if (request)
        *request = 1;
    return MPI_SUCCESS;
}

int MPI_Wait(MPI_Request *request, MPI_Status * /*status*/) {
    flush_batch();
    if (request)
        *request = MPI_REQUEST_NULL;
    return MPI_SUCCESS;
}

int MPI_Waitall(int count, MPI_Request *requests, MPI_Status * /*statuses*/) {
    flush_batch();
    if (requests)
        for (int i = 0; i < count; ++i)
            requests[i] = MPI_REQUEST_NULL;
    return MPI_SUCCESS;
}

// ===================== lulesh-init.cc =====================

#include <math.h>
#if _OPENMP
#include <omp.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <cstdlib>

/////////////////////////////////////////////////////////////////////
Domain::Domain(Int_t numRanks, Index_t colLoc,
               Index_t rowLoc, Index_t planeLoc,
               Index_t nx, Int_t tp, Int_t nr, Int_t balance, Int_t cost)
   :
   m_e_cut(Real_t(1.0e-7)),
   m_p_cut(Real_t(1.0e-7)),
   m_q_cut(Real_t(1.0e-7)),
   m_v_cut(Real_t(1.0e-10)),
   m_u_cut(Real_t(1.0e-7)),
   m_hgcoef(Real_t(3.0)),
   m_ss4o3(Real_t(4.0)/Real_t(3.0)),
   m_qstop(Real_t(1.0e+12)),
   m_monoq_max_slope(Real_t(1.0)),
   m_monoq_limiter_mult(Real_t(2.0)),
   m_qlc_monoq(Real_t(0.5)),
   m_qqc_monoq(Real_t(2.0)/Real_t(3.0)),
   m_qqc(Real_t(2.0)),
   m_eosvmax(Real_t(1.0e+9)),
   m_eosvmin(Real_t(1.0e-9)),
   m_pmin(Real_t(0.)),
   m_emin(Real_t(-1.0e+15)),
   m_dvovmax(Real_t(0.1)),
   m_refdens(Real_t(1.0)),
//
// set pointers to (potentially) "new'd" arrays to null to 
// simplify deallocation.
//
   m_regNumList(0),
   m_nodeElemStart(0),
   m_nodeElemCornerList(0),
   m_regElemSize(0),
   m_regElemlist(0)
#if USE_DISTRIBUTED
   , 
   commDataSend(0),
   commDataRecv(0)
#endif
{

   Index_t edgeElems = nx ;
   Index_t edgeNodes = edgeElems+1 ;
   this->cost() = cost;

   m_tp       = tp ;
   m_numRanks = numRanks ;

   ///////////////////////////////
   //   Initialize Sedov Mesh
   ///////////////////////////////

   // construct a uniform box for this processor

   m_colLoc   =   colLoc ;
   m_rowLoc   =   rowLoc ;
   m_planeLoc = planeLoc ;
   
   m_sizeX = edgeElems ;
   m_sizeY = edgeElems ;
   m_sizeZ = edgeElems ;
   m_numElem = edgeElems*edgeElems*edgeElems ;

   m_numNode = edgeNodes*edgeNodes*edgeNodes ;

   m_regNumList = new Index_t[numElem()] ;  // material indexset

   // Elem-centered 
   AllocateElemPersistent(numElem()) ;

   // Node-centered 
   AllocateNodePersistent(numNode()) ;

   SetupCommBuffers(edgeNodes);

   // Basic Field Initialization 
   for (Index_t i=0; i<numElem(); ++i) {
      e(i) =  Real_t(0.0) ;
      p(i) =  Real_t(0.0) ;
      q(i) =  Real_t(0.0) ;
      ss(i) = Real_t(0.0) ;
   }

   // Note - v initializes to 1.0, not 0.0!
   for (Index_t i=0; i<numElem(); ++i) {
      v(i) = Real_t(1.0) ;
   }

   for (Index_t i=0; i<numNode(); ++i) {
      xd(i) = Real_t(0.0) ;
      yd(i) = Real_t(0.0) ;
      zd(i) = Real_t(0.0) ;
   }

   for (Index_t i=0; i<numNode(); ++i) {
      xdd(i) = Real_t(0.0) ;
      ydd(i) = Real_t(0.0) ;
      zdd(i) = Real_t(0.0) ;
   }

   for (Index_t i=0; i<numNode(); ++i) {
      nodalMass(i) = Real_t(0.0) ;
   }

   BuildMesh(nx, edgeNodes, edgeElems);

#if _OPENMP
   SetupThreadSupportStructures();
#endif

   // Setup region index sets. For now, these are constant sized
   // throughout the run, but could be changed every cycle to 
   // simulate effects of ALE on the lagrange solver
   CreateRegionIndexSets(nr, balance);

   // Setup symmetry nodesets
   SetupSymmetryPlanes(edgeNodes);

   // Setup element connectivities
   SetupElementConnectivities(edgeElems);

   // Setup symmetry planes and free surface boundary arrays
   SetupBoundaryConditions(edgeElems);


   // Setup defaults

   // These can be changed (requires recompile) if you want to run
   // with a fixed timestep, or to a different end time, but it's
   // probably easier/better to just run a fixed number of timesteps
   // using the -i flag in 2.x

   dtfixed() = Real_t(-1.0e-6) ; // Negative means use courant condition
   stoptime()  = Real_t(1.0e-2); // *Real_t(edgeElems*tp/45.0) ;

   // Initial conditions
   deltatimemultlb() = Real_t(1.1) ;
   deltatimemultub() = Real_t(1.2) ;
   dtcourant() = Real_t(1.0e+20) ;
   dthydro()   = Real_t(1.0e+20) ;
   dtmax()     = Real_t(1.0e-2) ;
   time()    = Real_t(0.) ;
   cycle()   = Int_t(0) ;

   // initialize field data 
   for (Index_t i=0; i<numElem(); ++i) {
      Real_t x_local[8], y_local[8], z_local[8] ;
      Index_t *elemToNode = nodelist(i) ;
      for( Index_t lnode=0 ; lnode<8 ; ++lnode )
      {
        Index_t gnode = elemToNode[lnode];
        x_local[lnode] = x(gnode);
        y_local[lnode] = y(gnode);
        z_local[lnode] = z(gnode);
      }

      // volume calculations
      Real_t volume = CalcElemVolume(x_local, y_local, z_local );
      volo(i) = volume ;
      elemMass(i) = volume ;
      for (Index_t j=0; j<8; ++j) {
         Index_t idx = elemToNode[j] ;
         nodalMass(idx) += volume / Real_t(8.0) ;
      }
   }

   // deposit initial energy
   // An energy of 3.948746e+7 is correct for a problem with
   // 45 zones along a side - we need to scale it
   const Real_t ebase = Real_t(3.948746e+7);
   Real_t scale = (nx*m_tp)/Real_t(45.0);
   Real_t einit = ebase*scale*scale*scale;
   if (m_rowLoc + m_colLoc + m_planeLoc == 0) {
      // Dump into the first zone (which we know is in the corner)
      // of the domain that sits at the origin
      e(0) = einit;
   }
   //set initial deltatime base on analytic CFL calculation
   deltatime() = (Real_t(.5)*cbrt(volo(0)))/sqrt(Real_t(2.0)*einit);

} // End constructor


////////////////////////////////////////////////////////////////////////////////
Domain::~Domain()
{
   delete [] m_regNumList;
   delete [] m_nodeElemStart;
   delete [] m_nodeElemCornerList;
   delete [] m_regElemSize;
   for (Index_t i=0 ; i<numReg() ; ++i) {
     delete [] m_regElemlist[i];
   }
   delete [] m_regElemlist;
   
#if USE_DISTRIBUTED
   delete [] commDataSend;
   delete [] commDataRecv;
#endif
} // End destructor


////////////////////////////////////////////////////////////////////////////////
void
Domain::BuildMesh(Int_t nx, Int_t edgeNodes, Int_t edgeElems)
{
  Index_t meshEdgeElems = m_tp*nx ;

  // initialize nodal coordinates 
  Index_t nidx = 0 ;
  Real_t tz = Real_t(1.125)*Real_t(m_planeLoc*nx)/Real_t(meshEdgeElems) ;
  for (Index_t plane=0; plane<edgeNodes; ++plane) {
    Real_t ty = Real_t(1.125)*Real_t(m_rowLoc*nx)/Real_t(meshEdgeElems) ;
    for (Index_t row=0; row<edgeNodes; ++row) {
      Real_t tx = Real_t(1.125)*Real_t(m_colLoc*nx)/Real_t(meshEdgeElems) ;
      for (Index_t col=0; col<edgeNodes; ++col) {
	x(nidx) = tx ;
	y(nidx) = ty ;
	z(nidx) = tz ;
	++nidx ;
	// tx += ds ; // may accumulate roundoff... 
	tx = Real_t(1.125)*Real_t(m_colLoc*nx+col+1)/Real_t(meshEdgeElems) ;
      }
      // ty += ds ;  // may accumulate roundoff... 
      ty = Real_t(1.125)*Real_t(m_rowLoc*nx+row+1)/Real_t(meshEdgeElems) ;
    }
    // tz += ds ;  // may accumulate roundoff... 
    tz = Real_t(1.125)*Real_t(m_planeLoc*nx+plane+1)/Real_t(meshEdgeElems) ;
  }


  // embed hexehedral elements in nodal point lattice 
  Index_t zidx = 0 ;
  nidx = 0 ;
  for (Index_t plane=0; plane<edgeElems; ++plane) {
    for (Index_t row=0; row<edgeElems; ++row) {
      for (Index_t col=0; col<edgeElems; ++col) {
	Index_t *localNode = nodelist(zidx) ;
	localNode[0] = nidx                                       ;
	localNode[1] = nidx                                   + 1 ;
	localNode[2] = nidx                       + edgeNodes + 1 ;
	localNode[3] = nidx                       + edgeNodes     ;
	localNode[4] = nidx + edgeNodes*edgeNodes                 ;
	localNode[5] = nidx + edgeNodes*edgeNodes             + 1 ;
	localNode[6] = nidx + edgeNodes*edgeNodes + edgeNodes + 1 ;
	localNode[7] = nidx + edgeNodes*edgeNodes + edgeNodes     ;
	++zidx ;
	++nidx ;
      }
      ++nidx ;
    }
    nidx += edgeNodes ;
  }
}


////////////////////////////////////////////////////////////////////////////////
void
Domain::SetupThreadSupportStructures()
{
#if _OPENMP
   Index_t numthreads = omp_get_max_threads();
#else
   Index_t numthreads = 1;
#endif

  if (numthreads > 1) {
    // set up node-centered indexing of elements 
    Index_t *nodeElemCount = new Index_t[numNode()] ;

    for (Index_t i=0; i<numNode(); ++i) {
      nodeElemCount[i] = 0 ;
    }

    for (Index_t i=0; i<numElem(); ++i) {
      Index_t *nl = nodelist(i) ;
      for (Index_t j=0; j < 8; ++j) {
	++(nodeElemCount[nl[j]] );
      }
    }

    m_nodeElemStart = new Index_t[numNode()+1] ;

    m_nodeElemStart[0] = 0;

    for (Index_t i=1; i <= numNode(); ++i) {
      m_nodeElemStart[i] =
	m_nodeElemStart[i-1] + nodeElemCount[i-1] ;
    }
       
    m_nodeElemCornerList = new Index_t[m_nodeElemStart[numNode()]];

    for (Index_t i=0; i < numNode(); ++i) {
      nodeElemCount[i] = 0;
    }

    for (Index_t i=0; i < numElem(); ++i) {
      Index_t *nl = nodelist(i) ;
      for (Index_t j=0; j < 8; ++j) {
	Index_t m = nl[j];
	Index_t k = i*8 + j ;
	Index_t offset = m_nodeElemStart[m] + nodeElemCount[m] ;
	m_nodeElemCornerList[offset] = k;
	++(nodeElemCount[m]) ;
      }
    }

    Index_t clSize = m_nodeElemStart[numNode()] ;
    for (Index_t i=0; i < clSize; ++i) {
      Index_t clv = m_nodeElemCornerList[i] ;
      if ((clv < 0) || (clv > numElem()*8)) {
	fprintf(stderr,
		"AllocateNodeElemIndexes(): nodeElemCornerList entry out of range!\n");
#if USE_DISTRIBUTED
	MPI_Abort(MPI_COMM_WORLD, -1);
#else
	exit(-1);
#endif
      }
    }

    delete [] nodeElemCount ;
  }
}


////////////////////////////////////////////////////////////////////////////////
void
Domain::SetupCommBuffers(Int_t edgeNodes)
{
  // allocate a buffer large enough for nodal ghost data 
  Index_t maxEdgeSize = MAX(this->sizeX(), MAX(this->sizeY(), this->sizeZ()))+1 ;
  m_maxPlaneSize = CACHE_ALIGN_REAL(maxEdgeSize*maxEdgeSize) ;
  m_maxEdgeSize = CACHE_ALIGN_REAL(maxEdgeSize) ;

  // assume communication to 6 neighbors by default 
  m_rowMin = (m_rowLoc == 0)        ? 0 : 1;
  m_rowMax = (m_rowLoc == m_tp-1)     ? 0 : 1;
  m_colMin = (m_colLoc == 0)        ? 0 : 1;
  m_colMax = (m_colLoc == m_tp-1)     ? 0 : 1;
  m_planeMin = (m_planeLoc == 0)    ? 0 : 1;
  m_planeMax = (m_planeLoc == m_tp-1) ? 0 : 1;

#if USE_DISTRIBUTED
  // account for face communication 
  Index_t comBufSize =
    (m_rowMin + m_rowMax + m_colMin + m_colMax + m_planeMin + m_planeMax) *
    m_maxPlaneSize * MAX_FIELDS_PER_MPI_COMM ;

  // account for edge communication 
  comBufSize +=
    ((m_rowMin & m_colMin) + (m_rowMin & m_planeMin) + (m_colMin & m_planeMin) +
     (m_rowMax & m_colMax) + (m_rowMax & m_planeMax) + (m_colMax & m_planeMax) +
     (m_rowMax & m_colMin) + (m_rowMin & m_planeMax) + (m_colMin & m_planeMax) +
     (m_rowMin & m_colMax) + (m_rowMax & m_planeMin) + (m_colMax & m_planeMin)) *
    m_maxEdgeSize * MAX_FIELDS_PER_MPI_COMM ;

  // account for corner communication 
  // factor of 16 is so each buffer has its own cache line 
  comBufSize += ((m_rowMin & m_colMin & m_planeMin) +
		 (m_rowMin & m_colMin & m_planeMax) +
		 (m_rowMin & m_colMax & m_planeMin) +
		 (m_rowMin & m_colMax & m_planeMax) +
		 (m_rowMax & m_colMin & m_planeMin) +
		 (m_rowMax & m_colMin & m_planeMax) +
		 (m_rowMax & m_colMax & m_planeMin) +
		 (m_rowMax & m_colMax & m_planeMax)) * CACHE_COHERENCE_PAD_REAL ;

  this->commDataSend = new Real_t[comBufSize] ;
  this->commDataRecv = new Real_t[comBufSize] ;
  // prevent floating point exceptions 
  memset(this->commDataSend, 0, comBufSize*sizeof(Real_t)) ;
  memset(this->commDataRecv, 0, comBufSize*sizeof(Real_t)) ;
#endif   

  // Boundary nodesets
  if (m_colLoc == 0)
    m_symmX.resize(edgeNodes*edgeNodes);
  if (m_rowLoc == 0)
    m_symmY.resize(edgeNodes*edgeNodes);
  if (m_planeLoc == 0)
    m_symmZ.resize(edgeNodes*edgeNodes);
}


////////////////////////////////////////////////////////////////////////////////
void
Domain::CreateRegionIndexSets(Int_t nr, Int_t balance)
{
#if USE_DISTRIBUTED
   int myRank;
   MPI_Comm_rank(MPI_COMM_WORLD, &myRank) ;
   srand(myRank);
#else
   srand(0);
   Index_t myRank = 0;
#endif
   this->numReg() = nr;
   m_regElemSize = new Index_t[numReg()];
   m_regElemlist = new Index_t*[numReg()];
   Index_t nextIndex = 0;
   //if we only have one region just fill it
   // Fill out the regNumList with material numbers, which are always
   // the region index plus one 
   if(numReg() == 1) {
      while (nextIndex < numElem()) {
	 this->regNumList(nextIndex) = 1;
         nextIndex++;
      }
      regElemSize(0) = 0;
   }
   //If we have more than one region distribute the elements.
   else {
      Int_t regionNum;
      Int_t regionVar;
      Int_t lastReg = -1;
      Int_t binSize;
      Index_t elements;
      Index_t runto = 0;
      Int_t costDenominator = 0;
      Int_t* regBinEnd = new Int_t[numReg()];
      //Determine the relative weights of all the regions.  This is based off the -b flag.  Balance is the value passed into b.  
      for (Index_t i=0 ; i<numReg() ; ++i) {
         regElemSize(i) = 0;
	 costDenominator += pow((i+1), balance);  //Total sum of all regions weights
	 regBinEnd[i] = costDenominator;  //Chance of hitting a given region is (regBinEnd[i] - regBinEdn[i-1])/costDenominator
      }
      //Until all elements are assigned
      while (nextIndex < numElem()) {
	 //pick the region
	 regionVar = rand() % costDenominator;
	 Index_t i = 0;
         while(regionVar >= regBinEnd[i])
	    i++;
         //rotate the regions based on MPI rank.  Rotation is Rank % NumRegions this makes each domain have a different region with 
         //the highest representation
	 regionNum = ((i + myRank) % numReg()) + 1;
	 // make sure we don't pick the same region twice in a row
         while(regionNum == lastReg) {
	    regionVar = rand() % costDenominator;
	    i = 0;
            while(regionVar >= regBinEnd[i])
	       i++;
	    regionNum = ((i + myRank) % numReg()) + 1;
         }
	 //Pick the bin size of the region and determine the number of elements.
         binSize = rand() % 1000;
	 if(binSize < 773) {
	   elements = rand() % 15 + 1;
	 }
	 else if(binSize < 937) {
	   elements = rand() % 16 + 16;
	 }
	 else if(binSize < 970) {
	   elements = rand() % 32 + 32;
	 }
	 else if(binSize < 974) {
	   elements = rand() % 64 + 64;
	 } 
	 else if(binSize < 978) {
	   elements = rand() % 128 + 128;
	 }
	 else if(binSize < 981) {
	   elements = rand() % 256 + 256;
	 }
	 else
	    elements = rand() % 1537 + 512;
	 runto = elements + nextIndex;
	 //Store the elements.  If we hit the end before we run out of elements then just stop.
         while (nextIndex < runto && nextIndex < numElem()) {
	    this->regNumList(nextIndex) = regionNum;
	    nextIndex++;
	 }
	 lastReg = regionNum;
      }

      delete [] regBinEnd; 
   }
   // Convert regNumList to region index sets
   // First, count size of each region 
   for (Index_t i=0 ; i<numElem() ; ++i) {
      int r = this->regNumList(i)-1; // region index == regnum-1
      regElemSize(r)++;
   }
   // Second, allocate each region index set
   for (Index_t i=0 ; i<numReg() ; ++i) {
      m_regElemlist[i] = new Index_t[regElemSize(i)];
      regElemSize(i) = 0;
   }
   // Third, fill index sets
   for (Index_t i=0 ; i<numElem() ; ++i) {
      Index_t r = regNumList(i)-1;       // region index == regnum-1
      Index_t regndx = regElemSize(r)++; // Note increment
      regElemlist(r,regndx) = i;
   }
   
}

/////////////////////////////////////////////////////////////
void 
Domain::SetupSymmetryPlanes(Int_t edgeNodes)
{
  Index_t nidx = 0 ;
  for (Index_t i=0; i<edgeNodes; ++i) {
    Index_t planeInc = i*edgeNodes*edgeNodes ;
    Index_t rowInc   = i*edgeNodes ;
    for (Index_t j=0; j<edgeNodes; ++j) {
      if (m_planeLoc == 0) {
	m_symmZ[nidx] = rowInc   + j ;
      }
      if (m_rowLoc == 0) {
	m_symmY[nidx] = planeInc + j ;
      }
      if (m_colLoc == 0) {
	m_symmX[nidx] = planeInc + j*edgeNodes ;
      }
      ++nidx ;
    }
  }
}



/////////////////////////////////////////////////////////////
void
Domain::SetupElementConnectivities(Int_t edgeElems)
{
   lxim(0) = 0 ;
   for (Index_t i=1; i<numElem(); ++i) {
      lxim(i)   = i-1 ;
      lxip(i-1) = i ;
   }
   lxip(numElem()-1) = numElem()-1 ;

   for (Index_t i=0; i<edgeElems; ++i) {
      letam(i) = i ; 
      letap(numElem()-edgeElems+i) = numElem()-edgeElems+i ;
   }
   for (Index_t i=edgeElems; i<numElem(); ++i) {
      letam(i) = i-edgeElems ;
      letap(i-edgeElems) = i ;
   }

   for (Index_t i=0; i<edgeElems*edgeElems; ++i) {
      lzetam(i) = i ;
      lzetap(numElem()-edgeElems*edgeElems+i) = numElem()-edgeElems*edgeElems+i ;
   }
   for (Index_t i=edgeElems*edgeElems; i<numElem(); ++i) {
      lzetam(i) = i - edgeElems*edgeElems ;
      lzetap(i-edgeElems*edgeElems) = i ;
   }
}

/////////////////////////////////////////////////////////////
void
Domain::SetupBoundaryConditions(Int_t edgeElems) 
{
  Index_t ghostIdx[6] ;  // offsets to ghost locations

  // set up boundary condition information
  for (Index_t i=0; i<numElem(); ++i) {
     elemBC(i) = Int_t(0) ;
  }

  for (Index_t i=0; i<6; ++i) {
    ghostIdx[i] = INT_MIN ;
  }

  Int_t pidx = numElem() ;
  if (m_planeMin != 0) {
    ghostIdx[0] = pidx ;
    pidx += sizeX()*sizeY() ;
  }

  if (m_planeMax != 0) {
    ghostIdx[1] = pidx ;
    pidx += sizeX()*sizeY() ;
  }

  if (m_rowMin != 0) {
    ghostIdx[2] = pidx ;
    pidx += sizeX()*sizeZ() ;
  }

  if (m_rowMax != 0) {
    ghostIdx[3] = pidx ;
    pidx += sizeX()*sizeZ() ;
  }

  if (m_colMin != 0) {
    ghostIdx[4] = pidx ;
    pidx += sizeY()*sizeZ() ;
  }

  if (m_colMax != 0) {
    ghostIdx[5] = pidx ;
  }

  // symmetry plane or free surface BCs 
  for (Index_t i=0; i<edgeElems; ++i) {
    Index_t planeInc = i*edgeElems*edgeElems ;
    Index_t rowInc   = i*edgeElems ;
    for (Index_t j=0; j<edgeElems; ++j) {
      if (m_planeLoc == 0) {
	elemBC(rowInc+j) |= ZETA_M_SYMM ;
      }
      else {
	elemBC(rowInc+j) |= ZETA_M_COMM ;
	lzetam(rowInc+j) = ghostIdx[0] + rowInc + j ;
      }

      if (m_planeLoc == m_tp-1) {
	elemBC(rowInc+j+numElem()-edgeElems*edgeElems) |=
	  ZETA_P_FREE;
      }
      else {
	elemBC(rowInc+j+numElem()-edgeElems*edgeElems) |=
	  ZETA_P_COMM ;
	lzetap(rowInc+j+numElem()-edgeElems*edgeElems) =
	  ghostIdx[1] + rowInc + j ;
      }

      if (m_rowLoc == 0) {
	elemBC(planeInc+j) |= ETA_M_SYMM ;
      }
      else {
	elemBC(planeInc+j) |= ETA_M_COMM ;
	letam(planeInc+j) = ghostIdx[2] + rowInc + j ;
      }

      if (m_rowLoc == m_tp-1) {
	elemBC(planeInc+j+edgeElems*edgeElems-edgeElems) |= 
	  ETA_P_FREE ;
      }
      else {
	elemBC(planeInc+j+edgeElems*edgeElems-edgeElems) |= 
	  ETA_P_COMM ;
	letap(planeInc+j+edgeElems*edgeElems-edgeElems) =
	  ghostIdx[3] +  rowInc + j ;
      }

      if (m_colLoc == 0) {
	elemBC(planeInc+j*edgeElems) |= XI_M_SYMM ;
      }
      else {
	elemBC(planeInc+j*edgeElems) |= XI_M_COMM ;
	lxim(planeInc+j*edgeElems) = ghostIdx[4] + rowInc + j ;
      }

      if (m_colLoc == m_tp-1) {
	elemBC(planeInc+j*edgeElems+edgeElems-1) |= XI_P_FREE ;
      }
      else {
	elemBC(planeInc+j*edgeElems+edgeElems-1) |= XI_P_COMM ;
	lxip(planeInc+j*edgeElems+edgeElems-1) =
	  ghostIdx[5] + rowInc + j ;
      }
    }
  }
}

///////////////////////////////////////////////////////////////////////////
void InitMeshDecomp(Int_t numRanks, Int_t myRank,
                    Int_t *col, Int_t *row, Int_t *plane, Int_t *side)
{
   Int_t testProcs;
   Int_t dx, dy, dz;
   Int_t myDom;
   
   // Assume cube processor layout for now 
   testProcs = Int_t(cbrt(Real_t(numRanks))+0.5) ;
   if (testProcs*testProcs*testProcs != numRanks) {
      printf("Num processors must be a cube of an integer (1, 8, 27, ...)\n") ;
#if USE_DISTRIBUTED
      MPI_Abort(MPI_COMM_WORLD, -1) ;
#else
      exit(-1);
#endif
   }
   if (sizeof(Real_t) != 4 && sizeof(Real_t) != 8) {
      printf("MPI operations only support float and double right now...\n");
#if USE_DISTRIBUTED
      MPI_Abort(MPI_COMM_WORLD, -1) ;
#else
      exit(-1);
#endif
   }
   if (MAX_FIELDS_PER_MPI_COMM > CACHE_COHERENCE_PAD_REAL) {
      printf("corner element comm buffers too small.  Fix code.\n") ;
#if USE_DISTRIBUTED
      MPI_Abort(MPI_COMM_WORLD, -1) ;
#else
      exit(-1);
#endif
   }

   dx = testProcs ;
   dy = testProcs ;
   dz = testProcs ;

   // temporary test
   if (dx*dy*dz != numRanks) {
      printf("error -- must have as many domains as procs\n") ;
#if USE_DISTRIBUTED
      MPI_Abort(MPI_COMM_WORLD, -1) ;
#else
      exit(-1);
#endif
   }
   Int_t remainder = dx*dy*dz % numRanks ;
   if (myRank < remainder) {
      myDom = myRank*( 1+ (dx*dy*dz / numRanks)) ;
   }
   else {
      myDom = remainder*( 1+ (dx*dy*dz / numRanks)) +
         (myRank - remainder)*(dx*dy*dz/numRanks) ;
   }

   *col = myDom % dx ;
   *row = (myDom / dx) % dy ;
   *plane = myDom / (dx*dy) ;
   *side = testProcs;

   return;
}


// ===================== lulesh-util.cc (VerifyAndWriteFinalOutput only; the argv parser is replaced by the launcher flags) =====================

void VerifyAndWriteFinalOutput(Real_t elapsed_time,
                               Domain& locDom,
                               Int_t nx,
                               Int_t numRanks)
{
   // GrindTime1 only takes a single domain into account, and is thus a good way to measure
   // processor speed indepdendent of MPI parallelism.
   // GrindTime2 takes into account speedups from MPI parallelism.
   // Cast to 64-bit integer to avoid overflows.
   Int8_t nx8 = nx;
   Real_t grindTime1 = ((elapsed_time*1e6)/locDom.cycle())/(nx8*nx8*nx8);
   Real_t grindTime2 = ((elapsed_time*1e6)/locDom.cycle())/(nx8*nx8*nx8*numRanks);

   Index_t ElemId = 0;
   std::cout << "Run completed:\n";
   std::cout << "   Problem size        =  " << nx       << "\n";
   std::cout << "   MPI tasks           =  " << numRanks << "\n";
   std::cout << "   Iteration count     =  " << locDom.cycle() << "\n";
   std::cout << "   Final Origin Energy =  ";
   std::cout << std::scientific << std::setprecision(6);
   std::cout << std::setw(12) << locDom.e(ElemId) << "\n";

   Real_t   MaxAbsDiff = Real_t(0.0);
   Real_t TotalAbsDiff = Real_t(0.0);
   Real_t   MaxRelDiff = Real_t(0.0);

   for (Index_t j=0; j<nx; ++j) {
      for (Index_t k=j+1; k<nx; ++k) {
         Real_t AbsDiff = FABS(locDom.e(j*nx+k)-locDom.e(k*nx+j));
         TotalAbsDiff  += AbsDiff;

         if (MaxAbsDiff <AbsDiff) MaxAbsDiff = AbsDiff;

         Real_t RelDiff = AbsDiff / locDom.e(k*nx+j);

         if (MaxRelDiff <RelDiff)  MaxRelDiff = RelDiff;
      }
   }

   // Quick symmetry check
   std::cout << "   Testing Plane 0 of Energy Array on rank 0:\n";
   std::cout << "        MaxAbsDiff   = " << std::setw(12) << MaxAbsDiff   << "\n";
   std::cout << "        TotalAbsDiff = " << std::setw(12) << TotalAbsDiff << "\n";
   std::cout << "        MaxRelDiff   = " << std::setw(12) << MaxRelDiff   << "\n";

   // Timing information
   std::cout.unsetf(std::ios_base::floatfield);
   std::cout << std::setprecision(2);
   std::cout << "\nElapsed time         = " << std::setw(10) << elapsed_time << " (s)\n";
   std::cout << std::setprecision(8);
   std::cout << "Grind time (us/z/c)  = "  << std::setw(10) << grindTime1 << " (per dom)  ("
             << std::setw(10) << elapsed_time << " overall)\n";
   std::cout << "FOM                  = " << std::setw(10) << 1000.0/grindTime2 << " (z/s)\n\n";

   return ;
}

// ===================== lulesh-comm.cc =====================


// If serial (no communication backend), then this whole file is stubbed out
#if USE_DISTRIBUTED

#include <string.h>

/* Comm Routines */

#define ALLOW_UNPACKED_PLANE false
#define ALLOW_UNPACKED_ROW   false
#define ALLOW_UNPACKED_COL   false

/*
   There are coherence issues for packing and unpacking message
   buffers.  Ideally, you would like a lot of threads to 
   cooperate in the assembly/dissassembly of each message.
   To do that, each thread should really be operating in a
   different coherence zone.

   Let's assume we have three fields, f1 through f3, defined on
   a 61x61x61 cube.  If we want to send the block boundary
   information for each field to each neighbor processor across
   each cube face, then we have three cases for the
   memory layout/coherence of data on each of the six cube
   boundaries:

      (a) Two of the faces will be in contiguous memory blocks
      (b) Two of the faces will be comprised of pencils of
          contiguous memory.
      (c) Two of the faces will have large strides between
          every value living on the face.

   How do you pack and unpack this data in buffers to
   simultaneous achieve the best memory efficiency and
   the most thread independence?

   Do do you pack field f1 through f3 tighly to reduce message
   size?  Do you align each field on a cache coherence boundary
   within the message so that threads can pack and unpack each
   field independently?  For case (b), do you align each
   boundary pencil of each field separately?  This increases
   the message size, but could improve cache coherence so
   each pencil could be processed independently by a separate
   thread with no conflicts.

   Also, memory access for case (c) would best be done without
   going through the cache (the stride is so large it just causes
   a lot of useless cache evictions).  Is it worth creating
   a special case version of the packing algorithm that uses
   non-coherent load/store opcodes?
*/

/******************************************/


/* doRecv flag only works with regular block structure */
void CommRecv(Domain& domain, Int_t msgType, Index_t xferFields,
              Index_t dx, Index_t dy, Index_t dz, bool doRecv, bool planeOnly) {

   if (domain.numRanks() == 1)
      return ;

   /* post recieve buffers for all incoming messages */
   int myRank ;
   Index_t maxPlaneComm = xferFields * domain.maxPlaneSize() ;
   Index_t maxEdgeComm  = xferFields * domain.maxEdgeSize() ;
   Index_t pmsg = 0 ; /* plane comm msg */
   Index_t emsg = 0 ; /* edge comm msg */
   Index_t cmsg = 0 ; /* corner comm msg */
   MPI_Datatype baseType = ((sizeof(Real_t) == 4) ? MPI_FLOAT : MPI_DOUBLE) ;
   bool rowMin, rowMax, colMin, colMax, planeMin, planeMax ;

   /* assume communication to 6 neighbors by default */
   rowMin = rowMax = colMin = colMax = planeMin = planeMax = true ;

   if (domain.rowLoc() == 0) {
      rowMin = false ;
   }
   if (domain.rowLoc() == (domain.tp()-1)) {
      rowMax = false ;
   }
   if (domain.colLoc() == 0) {
      colMin = false ;
   }
   if (domain.colLoc() == (domain.tp()-1)) {
      colMax = false ;
   }
   if (domain.planeLoc() == 0) {
      planeMin = false ;
   }
   if (domain.planeLoc() == (domain.tp()-1)) {
      planeMax = false ;
   }

   for (Index_t i=0; i<26; ++i) {
      domain.recvRequest[i] = MPI_REQUEST_NULL ;
   }

   MPI_Comm_rank(MPI_COMM_WORLD, &myRank) ;

   /* post receives */

   /* receive data from neighboring domain faces */
   if (planeMin && doRecv) {
      /* contiguous memory */
      int fromRank = myRank - domain.tp()*domain.tp() ;
      int recvCount = dx * dy * xferFields ;
      MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm],
                recvCount, baseType, fromRank, msgType,
                MPI_COMM_WORLD, &domain.recvRequest[pmsg]) ;
      ++pmsg ;
   }
   if (planeMax) {
      /* contiguous memory */
      int fromRank = myRank + domain.tp()*domain.tp() ;
      int recvCount = dx * dy * xferFields ;
      MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm],
                recvCount, baseType, fromRank, msgType,
                MPI_COMM_WORLD, &domain.recvRequest[pmsg]) ;
      ++pmsg ;
   }
   if (rowMin && doRecv) {
      /* semi-contiguous memory */
      int fromRank = myRank - domain.tp() ;
      int recvCount = dx * dz * xferFields ;
      MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm],
                recvCount, baseType, fromRank, msgType,
                MPI_COMM_WORLD, &domain.recvRequest[pmsg]) ;
      ++pmsg ;
   }
   if (rowMax) {
      /* semi-contiguous memory */
      int fromRank = myRank + domain.tp() ;
      int recvCount = dx * dz * xferFields ;
      MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm],
                recvCount, baseType, fromRank, msgType,
                MPI_COMM_WORLD, &domain.recvRequest[pmsg]) ;
      ++pmsg ;
   }
   if (colMin && doRecv) {
      /* scattered memory */
      int fromRank = myRank - 1 ;
      int recvCount = dy * dz * xferFields ;
      MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm],
                recvCount, baseType, fromRank, msgType,
                MPI_COMM_WORLD, &domain.recvRequest[pmsg]) ;
      ++pmsg ;
   }
   if (colMax) {
      /* scattered memory */
      int fromRank = myRank + 1 ;
      int recvCount = dy * dz * xferFields ;
      MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm],
                recvCount, baseType, fromRank, msgType,
                MPI_COMM_WORLD, &domain.recvRequest[pmsg]) ;
      ++pmsg ;
   }

   if (!planeOnly) {
      /* receive data from domains connected only by an edge */
      if (rowMin && colMin && doRecv) {
         int fromRank = myRank - domain.tp() - 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dz * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMin && planeMin && doRecv) {
         int fromRank = myRank - domain.tp()*domain.tp() - domain.tp() ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dx * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (colMin && planeMin && doRecv) {
         int fromRank = myRank - domain.tp()*domain.tp() - 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dy * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMax && colMax) {
         int fromRank = myRank + domain.tp() + 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dz * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMax && planeMax) {
         int fromRank = myRank + domain.tp()*domain.tp() + domain.tp() ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dx * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (colMax && planeMax) {
         int fromRank = myRank + domain.tp()*domain.tp() + 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dy * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMax && colMin) {
         int fromRank = myRank + domain.tp() - 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dz * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMin && planeMax) {
         int fromRank = myRank + domain.tp()*domain.tp() - domain.tp() ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dx * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (colMin && planeMax) {
         int fromRank = myRank + domain.tp()*domain.tp() - 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dy * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMin && colMax && doRecv) {
         int fromRank = myRank - domain.tp() + 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dz * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMax && planeMin && doRecv) {
         int fromRank = myRank - domain.tp()*domain.tp() + domain.tp() ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dx * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (colMax && planeMin && doRecv) {
         int fromRank = myRank - domain.tp()*domain.tp() + 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm],
                   dy * xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      /* receive data from domains connected only by a corner */
      if (rowMin && colMin && planeMin && doRecv) {
         /* corner at domain logical coord (0, 0, 0) */
         int fromRank = myRank - domain.tp()*domain.tp() - domain.tp() - 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL],
                   xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMin && colMin && planeMax) {
         /* corner at domain logical coord (0, 0, 1) */
         int fromRank = myRank + domain.tp()*domain.tp() - domain.tp() - 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL],
                   xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMin && colMax && planeMin && doRecv) {
         /* corner at domain logical coord (1, 0, 0) */
         int fromRank = myRank - domain.tp()*domain.tp() - domain.tp() + 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL],
                   xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMin && colMax && planeMax) {
         /* corner at domain logical coord (1, 0, 1) */
         int fromRank = myRank + domain.tp()*domain.tp() - domain.tp() + 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL],
                   xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMax && colMin && planeMin && doRecv) {
         /* corner at domain logical coord (0, 1, 0) */
         int fromRank = myRank - domain.tp()*domain.tp() + domain.tp() - 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL],
                   xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMax && colMin && planeMax) {
         /* corner at domain logical coord (0, 1, 1) */
         int fromRank = myRank + domain.tp()*domain.tp() + domain.tp() - 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL],
                   xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMax && colMax && planeMin && doRecv) {
         /* corner at domain logical coord (1, 1, 0) */
         int fromRank = myRank - domain.tp()*domain.tp() + domain.tp() + 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL],
                   xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMax && colMax && planeMax) {
         /* corner at domain logical coord (1, 1, 1) */
         int fromRank = myRank + domain.tp()*domain.tp() + domain.tp() + 1 ;
         MPI_Irecv(&domain.commDataRecv[pmsg * maxPlaneComm +
                                         emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL],
                   xferFields, baseType, fromRank, msgType,
                   MPI_COMM_WORLD, &domain.recvRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
   }
}

/******************************************/

void CommSend(Domain& domain, Int_t msgType,
              Index_t xferFields, Domain_member *fieldData,
              Index_t dx, Index_t dy, Index_t dz, bool doSend, bool planeOnly)
{

   if (domain.numRanks() == 1)
      return ;

   /* post recieve buffers for all incoming messages */
   int myRank ;
   Index_t maxPlaneComm = xferFields * domain.maxPlaneSize() ;
   Index_t maxEdgeComm  = xferFields * domain.maxEdgeSize() ;
   Index_t pmsg = 0 ; /* plane comm msg */
   Index_t emsg = 0 ; /* edge comm msg */
   Index_t cmsg = 0 ; /* corner comm msg */
   MPI_Datatype baseType = ((sizeof(Real_t) == 4) ? MPI_FLOAT : MPI_DOUBLE) ;
   MPI_Status status[26] ;
   Real_t *destAddr ;
   bool rowMin, rowMax, colMin, colMax, planeMin, planeMax ;
   /* assume communication to 6 neighbors by default */
   rowMin = rowMax = colMin = colMax = planeMin = planeMax = true ;
   if (domain.rowLoc() == 0) {
      rowMin = false ;
   }
   if (domain.rowLoc() == (domain.tp()-1)) {
      rowMax = false ;
   }
   if (domain.colLoc() == 0) {
      colMin = false ;
   }
   if (domain.colLoc() == (domain.tp()-1)) {
      colMax = false ;
   }
   if (domain.planeLoc() == 0) {
      planeMin = false ;
   }
   if (domain.planeLoc() == (domain.tp()-1)) {
      planeMax = false ;
   }

   for (Index_t i=0; i<26; ++i) {
      domain.sendRequest[i] = MPI_REQUEST_NULL ;
   }

   MPI_Comm_rank(MPI_COMM_WORLD, &myRank) ;

   /* post sends */

   if (planeMin | planeMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      int sendCount = dx * dy ;

      if (planeMin) {
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm] ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<sendCount; ++i) {
               destAddr[i] = (domain.*src)(i) ;
            }
            destAddr += sendCount ;
         }
         destAddr -= xferFields*sendCount ;

         MPI_Isend(destAddr, xferFields*sendCount, baseType,
                   myRank - domain.tp()*domain.tp(), msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg]) ;
         ++pmsg ;
      }
      if (planeMax && doSend) {
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm] ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<sendCount; ++i) {
               destAddr[i] = (domain.*src)(dx*dy*(dz - 1) + i) ;
            }
            destAddr += sendCount ;
         }
         destAddr -= xferFields*sendCount ;

         MPI_Isend(destAddr, xferFields*sendCount, baseType,
                   myRank + domain.tp()*domain.tp(), msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg]) ;
         ++pmsg ;
      }
   }
   if (rowMin | rowMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      int sendCount = dx * dz ;

      if (rowMin) {
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dx; ++j) {
                  destAddr[i*dx+j] = (domain.*src)(i*dx*dy + j) ;
               }
            }
            destAddr += sendCount ;
         }
         destAddr -= xferFields*sendCount ;

         MPI_Isend(destAddr, xferFields*sendCount, baseType,
                   myRank - domain.tp(), msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg]) ;
         ++pmsg ;
      }
      if (rowMax && doSend) {
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dx; ++j) {
                  destAddr[i*dx+j] = (domain.*src)(dx*(dy - 1) + i*dx*dy + j) ;
               }
            }
            destAddr += sendCount ;
         }
         destAddr -= xferFields*sendCount ;

         MPI_Isend(destAddr, xferFields*sendCount, baseType,
                   myRank + domain.tp(), msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg]) ;
         ++pmsg ;
      }
   }
   if (colMin | colMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      int sendCount = dy * dz ;

      if (colMin) {
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dy; ++j) {
                  destAddr[i*dy + j] = (domain.*src)(i*dx*dy + j*dx) ;
               }
            }
            destAddr += sendCount ;
         }
         destAddr -= xferFields*sendCount ;

         MPI_Isend(destAddr, xferFields*sendCount, baseType,
                   myRank - 1, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg]) ;
         ++pmsg ;
      }
      if (colMax && doSend) {
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dy; ++j) {
                  destAddr[i*dy + j] = (domain.*src)(dx - 1 + i*dx*dy + j*dx) ;
               }
            }
            destAddr += sendCount ;
         }
         destAddr -= xferFields*sendCount ;

         MPI_Isend(destAddr, xferFields*sendCount, baseType,
                   myRank + 1, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg]) ;
         ++pmsg ;
      }
   }

   if (!planeOnly) {
      if (rowMin && colMin) {
         int toRank = myRank - domain.tp() - 1 ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               destAddr[i] = (domain.*src)(i*dx*dy) ;
            }
            destAddr += dz ;
         }
         destAddr -= xferFields*dz ;
         MPI_Isend(destAddr, xferFields*dz, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMin && planeMin) {
         int toRank = myRank - domain.tp()*domain.tp() - domain.tp() ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dx; ++i) {
               destAddr[i] = (domain.*src)(i) ;
            }
            destAddr += dx ;
         }
         destAddr -= xferFields*dx ;
         MPI_Isend(destAddr, xferFields*dx, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (colMin && planeMin) {
         int toRank = myRank - domain.tp()*domain.tp() - 1 ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dy; ++i) {
               destAddr[i] = (domain.*src)(i*dx) ;
            }
            destAddr += dy ;
         }
         destAddr -= xferFields*dy ;
         MPI_Isend(destAddr, xferFields*dy, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMax && colMax && doSend) {
         int toRank = myRank + domain.tp() + 1 ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               destAddr[i] = (domain.*src)(dx*dy - 1 + i*dx*dy) ;
            }
            destAddr += dz ;
         }
         destAddr -= xferFields*dz ;
         MPI_Isend(destAddr, xferFields*dz, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMax && planeMax && doSend) {
         int toRank = myRank + domain.tp()*domain.tp() + domain.tp() ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dx; ++i) {
              destAddr[i] = (domain.*src)(dx*(dy-1) + dx*dy*(dz-1) + i) ;
            }
            destAddr += dx ;
         }
         destAddr -= xferFields*dx ;
         MPI_Isend(destAddr, xferFields*dx, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (colMax && planeMax && doSend) {
         int toRank = myRank + domain.tp()*domain.tp() + 1 ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dy; ++i) {
               destAddr[i] = (domain.*src)(dx*dy*(dz-1) + dx - 1 + i*dx) ;
            }
            destAddr += dy ;
         }
         destAddr -= xferFields*dy ;
         MPI_Isend(destAddr, xferFields*dy, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMax && colMin && doSend) {
         int toRank = myRank + domain.tp() - 1 ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               destAddr[i] = (domain.*src)(dx*(dy-1) + i*dx*dy) ;
            }
            destAddr += dz ;
         }
         destAddr -= xferFields*dz ;
         MPI_Isend(destAddr, xferFields*dz, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMin && planeMax && doSend) {
         int toRank = myRank + domain.tp()*domain.tp() - domain.tp() ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dx; ++i) {
               destAddr[i] = (domain.*src)(dx*dy*(dz-1) + i) ;
            }
            destAddr += dx ;
         }
         destAddr -= xferFields*dx ;
         MPI_Isend(destAddr, xferFields*dx, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (colMin && planeMax && doSend) {
         int toRank = myRank + domain.tp()*domain.tp() - 1 ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dy; ++i) {
               destAddr[i] = (domain.*src)(dx*dy*(dz-1) + i*dx) ;
            }
            destAddr += dy ;
         }
         destAddr -= xferFields*dy ;
         MPI_Isend(destAddr, xferFields*dy, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMin && colMax) {
         int toRank = myRank - domain.tp() + 1 ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               destAddr[i] = (domain.*src)(dx - 1 + i*dx*dy) ;
            }
            destAddr += dz ;
         }
         destAddr -= xferFields*dz ;
         MPI_Isend(destAddr, xferFields*dz, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMax && planeMin) {
         int toRank = myRank - domain.tp()*domain.tp() + domain.tp() ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dx; ++i) {
               destAddr[i] = (domain.*src)(dx*(dy - 1) + i) ;
            }
            destAddr += dx ;
         }
         destAddr -= xferFields*dx ;
         MPI_Isend(destAddr, xferFields*dx, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (colMax && planeMin) {
         int toRank = myRank - domain.tp()*domain.tp() + 1 ;
         destAddr = &domain.commDataSend[pmsg * maxPlaneComm +
                                          emsg * maxEdgeComm] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            Domain_member src = fieldData[fi] ;
            for (Index_t i=0; i<dy; ++i) {
               destAddr[i] = (domain.*src)(dx - 1 + i*dx) ;
            }
            destAddr += dy ;
         }
         destAddr -= xferFields*dy ;
         MPI_Isend(destAddr, xferFields*dy, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg]) ;
         ++emsg ;
      }

      if (rowMin && colMin && planeMin) {
         /* corner at domain logical coord (0, 0, 0) */
         int toRank = myRank - domain.tp()*domain.tp() - domain.tp() - 1 ;
         Real_t *comBuf = &domain.commDataSend[pmsg * maxPlaneComm +
                                                emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            comBuf[fi] = (domain.*fieldData[fi])(0) ;
         }
         MPI_Isend(comBuf, xferFields, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMin && colMin && planeMax && doSend) {
         /* corner at domain logical coord (0, 0, 1) */
         int toRank = myRank + domain.tp()*domain.tp() - domain.tp() - 1 ;
         Real_t *comBuf = &domain.commDataSend[pmsg * maxPlaneComm +
                                                emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL] ;
         Index_t idx = dx*dy*(dz - 1) ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            comBuf[fi] = (domain.*fieldData[fi])(idx) ;
         }
         MPI_Isend(comBuf, xferFields, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMin && colMax && planeMin) {
         /* corner at domain logical coord (1, 0, 0) */
         int toRank = myRank - domain.tp()*domain.tp() - domain.tp() + 1 ;
         Real_t *comBuf = &domain.commDataSend[pmsg * maxPlaneComm +
                                                emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL] ;
         Index_t idx = dx - 1 ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            comBuf[fi] = (domain.*fieldData[fi])(idx) ;
         }
         MPI_Isend(comBuf, xferFields, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMin && colMax && planeMax && doSend) {
         /* corner at domain logical coord (1, 0, 1) */
         int toRank = myRank + domain.tp()*domain.tp() - domain.tp() + 1 ;
         Real_t *comBuf = &domain.commDataSend[pmsg * maxPlaneComm +
                                                emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL] ;
         Index_t idx = dx*dy*(dz - 1) + (dx - 1) ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            comBuf[fi] = (domain.*fieldData[fi])(idx) ;
         }
         MPI_Isend(comBuf, xferFields, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMax && colMin && planeMin) {
         /* corner at domain logical coord (0, 1, 0) */
         int toRank = myRank - domain.tp()*domain.tp() + domain.tp() - 1 ;
         Real_t *comBuf = &domain.commDataSend[pmsg * maxPlaneComm +
                                                emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL] ;
         Index_t idx = dx*(dy - 1) ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            comBuf[fi] = (domain.*fieldData[fi])(idx) ;
         }
         MPI_Isend(comBuf, xferFields, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMax && colMin && planeMax && doSend) {
         /* corner at domain logical coord (0, 1, 1) */
         int toRank = myRank + domain.tp()*domain.tp() + domain.tp() - 1 ;
         Real_t *comBuf = &domain.commDataSend[pmsg * maxPlaneComm +
                                                emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL] ;
         Index_t idx = dx*dy*(dz - 1) + dx*(dy - 1) ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            comBuf[fi] = (domain.*fieldData[fi])(idx) ;
         }
         MPI_Isend(comBuf, xferFields, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMax && colMax && planeMin) {
         /* corner at domain logical coord (1, 1, 0) */
         int toRank = myRank - domain.tp()*domain.tp() + domain.tp() + 1 ;
         Real_t *comBuf = &domain.commDataSend[pmsg * maxPlaneComm +
                                                emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL] ;
         Index_t idx = dx*dy - 1 ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            comBuf[fi] = (domain.*fieldData[fi])(idx) ;
         }
         MPI_Isend(comBuf, xferFields, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
      if (rowMax && colMax && planeMax && doSend) {
         /* corner at domain logical coord (1, 1, 1) */
         int toRank = myRank + domain.tp()*domain.tp() + domain.tp() + 1 ;
         Real_t *comBuf = &domain.commDataSend[pmsg * maxPlaneComm +
                                                emsg * maxEdgeComm +
                                         cmsg * CACHE_COHERENCE_PAD_REAL] ;
         Index_t idx = dx*dy*dz - 1 ;
         for (Index_t fi=0; fi<xferFields; ++fi) {
            comBuf[fi] = (domain.*fieldData[fi])(idx) ;
         }
         MPI_Isend(comBuf, xferFields, baseType, toRank, msgType,
                   MPI_COMM_WORLD, &domain.sendRequest[pmsg+emsg+cmsg]) ;
         ++cmsg ;
      }
   }

   MPI_Waitall(26, domain.sendRequest, status) ;
}

/******************************************/

void CommSBN(Domain& domain, Int_t xferFields, Domain_member *fieldData) {

   if (domain.numRanks() == 1)
      return ;

   /* summation order should be from smallest value to largest */
   /* or we could try out kahan summation! */

   int myRank ;
   Index_t maxPlaneComm = xferFields * domain.maxPlaneSize() ;
   Index_t maxEdgeComm  = xferFields * domain.maxEdgeSize() ;
   Index_t pmsg = 0 ; /* plane comm msg */
   Index_t emsg = 0 ; /* edge comm msg */
   Index_t cmsg = 0 ; /* corner comm msg */
   Index_t dx = domain.sizeX() + 1 ;
   Index_t dy = domain.sizeY() + 1 ;
   Index_t dz = domain.sizeZ() + 1 ;
   MPI_Status status ;
   Real_t *srcAddr ;
   Index_t rowMin, rowMax, colMin, colMax, planeMin, planeMax ;
   /* assume communication to 6 neighbors by default */
   rowMin = rowMax = colMin = colMax = planeMin = planeMax = 1 ;
   if (domain.rowLoc() == 0) {
      rowMin = 0 ;
   }
   if (domain.rowLoc() == (domain.tp()-1)) {
      rowMax = 0 ;
   }
   if (domain.colLoc() == 0) {
      colMin = 0 ;
   }
   if (domain.colLoc() == (domain.tp()-1)) {
      colMax = 0 ;
   }
   if (domain.planeLoc() == 0) {
      planeMin = 0 ;
   }
   if (domain.planeLoc() == (domain.tp()-1)) {
      planeMax = 0 ;
   }

   MPI_Comm_rank(MPI_COMM_WORLD, &myRank) ;

   if (planeMin | planeMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      Index_t opCount = dx * dy ;

      if (planeMin) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<opCount; ++i) {
               (domain.*dest)(i) += srcAddr[i] ;
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
      if (planeMax) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<opCount; ++i) {
               (domain.*dest)(dx*dy*(dz - 1) + i) += srcAddr[i] ;
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
   }

   if (rowMin | rowMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      Index_t opCount = dx * dz ;

      if (rowMin) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dx; ++j) {
                  (domain.*dest)(i*dx*dy + j) += srcAddr[i*dx + j] ;
               }
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
      if (rowMax) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dx; ++j) {
                  (domain.*dest)(dx*(dy - 1) + i*dx*dy + j) += srcAddr[i*dx + j] ;
               }
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
   }
   if (colMin | colMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      Index_t opCount = dy * dz ;

      if (colMin) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dy; ++j) {
                  (domain.*dest)(i*dx*dy + j*dx) += srcAddr[i*dy + j] ;
               }
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
      if (colMax) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dy; ++j) {
                  (domain.*dest)(dx - 1 + i*dx*dy + j*dx) += srcAddr[i*dy + j] ;
               }
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
   }

   if (rowMin & colMin) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dz; ++i) {
            (domain.*dest)(i*dx*dy) += srcAddr[i] ;
         }
         srcAddr += dz ;
      }
      ++emsg ;
   }

   if (rowMin & planeMin) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dx; ++i) {
            (domain.*dest)(i) += srcAddr[i] ;
         }
         srcAddr += dx ;
      }
      ++emsg ;
   }

   if (colMin & planeMin) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dy; ++i) {
            (domain.*dest)(i*dx) += srcAddr[i] ;
         }
         srcAddr += dy ;
      }
      ++emsg ;
   }

   if (rowMax & colMax) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dz; ++i) {
            (domain.*dest)(dx*dy - 1 + i*dx*dy) += srcAddr[i] ;
         }
         srcAddr += dz ;
      }
      ++emsg ;
   }

   if (rowMax & planeMax) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dx; ++i) {
            (domain.*dest)(dx*(dy-1) + dx*dy*(dz-1) + i) += srcAddr[i] ;
         }
         srcAddr += dx ;
      }
      ++emsg ;
   }

   if (colMax & planeMax) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dy; ++i) {
            (domain.*dest)(dx*dy*(dz-1) + dx - 1 + i*dx) += srcAddr[i] ;
         }
         srcAddr += dy ;
      }
      ++emsg ;
   }

   if (rowMax & colMin) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dz; ++i) {
            (domain.*dest)(dx*(dy-1) + i*dx*dy) += srcAddr[i] ;
         }
         srcAddr += dz ;
      }
      ++emsg ;
   }

   if (rowMin & planeMax) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dx; ++i) {
            (domain.*dest)(dx*dy*(dz-1) + i) += srcAddr[i] ;
         }
         srcAddr += dx ;
      }
      ++emsg ;
   }

   if (colMin & planeMax) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dy; ++i) {
            (domain.*dest)(dx*dy*(dz-1) + i*dx) += srcAddr[i] ;
         }
         srcAddr += dy ;
      }
      ++emsg ;
   }

   if (rowMin & colMax) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dz; ++i) {
            (domain.*dest)(dx - 1 + i*dx*dy) += srcAddr[i] ;
         }
         srcAddr += dz ;
      }
      ++emsg ;
   }

   if (rowMax & planeMin) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dx; ++i) {
            (domain.*dest)(dx*(dy - 1) + i) += srcAddr[i] ;
         }
         srcAddr += dx ;
      }
      ++emsg ;
   }

   if (colMax & planeMin) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dy; ++i) {
            (domain.*dest)(dx - 1 + i*dx) += srcAddr[i] ;
         }
         srcAddr += dy ;
      }
      ++emsg ;
   }

   if (rowMin & colMin & planeMin) {
      /* corner at domain logical coord (0, 0, 0) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(0) += comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMin & colMin & planeMax) {
      /* corner at domain logical coord (0, 0, 1) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*dy*(dz - 1) ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) += comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMin & colMax & planeMin) {
      /* corner at domain logical coord (1, 0, 0) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx - 1 ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) += comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMin & colMax & planeMax) {
      /* corner at domain logical coord (1, 0, 1) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*dy*(dz - 1) + (dx - 1) ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) += comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMax & colMin & planeMin) {
      /* corner at domain logical coord (0, 1, 0) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*(dy - 1) ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) += comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMax & colMin & planeMax) {
      /* corner at domain logical coord (0, 1, 1) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*dy*(dz - 1) + dx*(dy - 1) ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) += comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMax & colMax & planeMin) {
      /* corner at domain logical coord (1, 1, 0) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*dy - 1 ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) += comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMax & colMax & planeMax) {
      /* corner at domain logical coord (1, 1, 1) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*dy*dz - 1 ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) += comBuf[fi] ;
      }
      ++cmsg ;
   }
}

/******************************************/

void CommSyncPosVel(Domain& domain) {

   if (domain.numRanks() == 1)
      return ;

   int myRank ;
   bool doRecv = false ;
   Index_t xferFields = 6 ; /* x, y, z, xd, yd, zd */
   Domain_member fieldData[6] ;
   Index_t maxPlaneComm = xferFields * domain.maxPlaneSize() ;
   Index_t maxEdgeComm  = xferFields * domain.maxEdgeSize() ;
   Index_t pmsg = 0 ; /* plane comm msg */
   Index_t emsg = 0 ; /* edge comm msg */
   Index_t cmsg = 0 ; /* corner comm msg */
   Index_t dx = domain.sizeX() + 1 ;
   Index_t dy = domain.sizeY() + 1 ;
   Index_t dz = domain.sizeZ() + 1 ;
   MPI_Status status ;
   Real_t *srcAddr ;
   bool rowMin, rowMax, colMin, colMax, planeMin, planeMax ;

   /* assume communication to 6 neighbors by default */
   rowMin = rowMax = colMin = colMax = planeMin = planeMax = true ;
   if (domain.rowLoc() == 0) {
      rowMin = false ;
   }
   if (domain.rowLoc() == (domain.tp()-1)) {
      rowMax = false ;
   }
   if (domain.colLoc() == 0) {
      colMin = false ;
   }
   if (domain.colLoc() == (domain.tp()-1)) {
      colMax = false ;
   }
   if (domain.planeLoc() == 0) {
      planeMin = false ;
   }
   if (domain.planeLoc() == (domain.tp()-1)) {
      planeMax = false ;
   }

   fieldData[0] = &Domain::x ;
   fieldData[1] = &Domain::y ;
   fieldData[2] = &Domain::z ;
   fieldData[3] = &Domain::xd ;
   fieldData[4] = &Domain::yd ;
   fieldData[5] = &Domain::zd ;

   MPI_Comm_rank(MPI_COMM_WORLD, &myRank) ;

   if (planeMin | planeMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      Index_t opCount = dx * dy ;

      if (planeMin && doRecv) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<opCount; ++i) {
               (domain.*dest)(i) = srcAddr[i] ;
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
      if (planeMax) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<opCount; ++i) {
               (domain.*dest)(dx*dy*(dz - 1) + i) = srcAddr[i] ;
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
   }

   if (rowMin | rowMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      Index_t opCount = dx * dz ;

      if (rowMin && doRecv) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dx; ++j) {
                  (domain.*dest)(i*dx*dy + j) = srcAddr[i*dx + j] ;
               }
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
      if (rowMax) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dx; ++j) {
                  (domain.*dest)(dx*(dy - 1) + i*dx*dy + j) = srcAddr[i*dx + j] ;
               }
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
   }

   if (colMin | colMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      Index_t opCount = dy * dz ;

      if (colMin && doRecv) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dy; ++j) {
                  (domain.*dest)(i*dx*dy + j*dx) = srcAddr[i*dy + j] ;
               }
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
      if (colMax) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<dz; ++i) {
               for (Index_t j=0; j<dy; ++j) {
                  (domain.*dest)(dx - 1 + i*dx*dy + j*dx) = srcAddr[i*dy + j] ;
               }
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
   }

   if (rowMin && colMin && doRecv) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dz; ++i) {
            (domain.*dest)(i*dx*dy) = srcAddr[i] ;
         }
         srcAddr += dz ;
      }
      ++emsg ;
   }

   if (rowMin && planeMin && doRecv) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dx; ++i) {
            (domain.*dest)(i) = srcAddr[i] ;
         }
         srcAddr += dx ;
      }
      ++emsg ;
   }

   if (colMin && planeMin && doRecv) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dy; ++i) {
            (domain.*dest)(i*dx) = srcAddr[i] ;
         }
         srcAddr += dy ;
      }
      ++emsg ;
   }

   if (rowMax && colMax) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dz; ++i) {
            (domain.*dest)(dx*dy - 1 + i*dx*dy) = srcAddr[i] ;
         }
         srcAddr += dz ;
      }
      ++emsg ;
   }

   if (rowMax && planeMax) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dx; ++i) {
            (domain.*dest)(dx*(dy-1) + dx*dy*(dz-1) + i) = srcAddr[i] ;
         }
         srcAddr += dx ;
      }
      ++emsg ;
   }

   if (colMax && planeMax) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dy; ++i) {
            (domain.*dest)(dx*dy*(dz-1) + dx - 1 + i*dx) = srcAddr[i] ;
         }
         srcAddr += dy ;
      }
      ++emsg ;
   }

   if (rowMax && colMin) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dz; ++i) {
            (domain.*dest)(dx*(dy-1) + i*dx*dy) = srcAddr[i] ;
         }
         srcAddr += dz ;
      }
      ++emsg ;
   }

   if (rowMin && planeMax) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dx; ++i) {
            (domain.*dest)(dx*dy*(dz-1) + i) = srcAddr[i] ;
         }
         srcAddr += dx ;
      }
      ++emsg ;
   }

   if (colMin && planeMax) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dy; ++i) {
            (domain.*dest)(dx*dy*(dz-1) + i*dx) = srcAddr[i] ;
         }
         srcAddr += dy ;
      }
      ++emsg ;
   }

   if (rowMin && colMax && doRecv) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dz; ++i) {
            (domain.*dest)(dx - 1 + i*dx*dy) = srcAddr[i] ;
         }
         srcAddr += dz ;
      }
      ++emsg ;
   }

   if (rowMax && planeMin && doRecv) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dx; ++i) {
            (domain.*dest)(dx*(dy - 1) + i) = srcAddr[i] ;
         }
         srcAddr += dx ;
      }
      ++emsg ;
   }

   if (colMax && planeMin && doRecv) {
      srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm +
                                       emsg * maxEdgeComm] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg], &status) ;
      for (Index_t fi=0 ; fi<xferFields; ++fi) {
         Domain_member dest = fieldData[fi] ;
         for (Index_t i=0; i<dy; ++i) {
            (domain.*dest)(dx - 1 + i*dx) = srcAddr[i] ;
         }
         srcAddr += dy ;
      }
      ++emsg ;
   }


   if (rowMin && colMin && planeMin && doRecv) {
      /* corner at domain logical coord (0, 0, 0) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(0) = comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMin && colMin && planeMax) {
      /* corner at domain logical coord (0, 0, 1) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*dy*(dz - 1) ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) = comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMin && colMax && planeMin && doRecv) {
      /* corner at domain logical coord (1, 0, 0) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx - 1 ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) = comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMin && colMax && planeMax) {
      /* corner at domain logical coord (1, 0, 1) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*dy*(dz - 1) + (dx - 1) ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) = comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMax && colMin && planeMin && doRecv) {
      /* corner at domain logical coord (0, 1, 0) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*(dy - 1) ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) = comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMax && colMin && planeMax) {
      /* corner at domain logical coord (0, 1, 1) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*dy*(dz - 1) + dx*(dy - 1) ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) = comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMax && colMax && planeMin && doRecv) {
      /* corner at domain logical coord (1, 1, 0) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*dy - 1 ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) = comBuf[fi] ;
      }
      ++cmsg ;
   }
   if (rowMax && colMax && planeMax) {
      /* corner at domain logical coord (1, 1, 1) */
      Real_t *comBuf = &domain.commDataRecv[pmsg * maxPlaneComm +
                                             emsg * maxEdgeComm +
                                      cmsg * CACHE_COHERENCE_PAD_REAL] ;
      Index_t idx = dx*dy*dz - 1 ;
      MPI_Wait(&domain.recvRequest[pmsg+emsg+cmsg], &status) ;
      for (Index_t fi=0; fi<xferFields; ++fi) {
         (domain.*fieldData[fi])(idx) = comBuf[fi] ;
      }
      ++cmsg ;
   }
}

/******************************************/

void CommMonoQ(Domain& domain)
{
   if (domain.numRanks() == 1)
      return ;

   int myRank ;
   Index_t xferFields = 3 ; /* delv_xi, delv_eta, delv_zeta */
   Domain_member fieldData[3] ;
   Index_t fieldOffset[3] ;
   Index_t maxPlaneComm = xferFields * domain.maxPlaneSize() ;
   Index_t pmsg = 0 ; /* plane comm msg */
   Index_t dx = domain.sizeX() ;
   Index_t dy = domain.sizeY() ;
   Index_t dz = domain.sizeZ() ;
   MPI_Status status ;
   Real_t *srcAddr ;
   bool rowMin, rowMax, colMin, colMax, planeMin, planeMax ;
   /* assume communication to 6 neighbors by default */
   rowMin = rowMax = colMin = colMax = planeMin = planeMax = true ;
   if (domain.rowLoc() == 0) {
      rowMin = false ;
   }
   if (domain.rowLoc() == (domain.tp()-1)) {
      rowMax = false ;
   }
   if (domain.colLoc() == 0) {
      colMin = false ;
   }
   if (domain.colLoc() == (domain.tp()-1)) {
      colMax = false ;
   }
   if (domain.planeLoc() == 0) {
      planeMin = false ;
   }
   if (domain.planeLoc() == (domain.tp()-1)) {
      planeMax = false ;
   }

   /* point into ghost data area */
   // fieldData[0] = &(domain.delv_xi(domain.numElem())) ;
   // fieldData[1] = &(domain.delv_eta(domain.numElem())) ;
   // fieldData[2] = &(domain.delv_zeta(domain.numElem())) ;
   fieldData[0] = &Domain::delv_xi ;
   fieldData[1] = &Domain::delv_eta ;
   fieldData[2] = &Domain::delv_zeta ;
   fieldOffset[0] = domain.numElem() ;
   fieldOffset[1] = domain.numElem() ;
   fieldOffset[2] = domain.numElem() ;


   MPI_Comm_rank(MPI_COMM_WORLD, &myRank) ;

   if (planeMin | planeMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      Index_t opCount = dx * dy ;

      if (planeMin) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<opCount; ++i) {
               (domain.*dest)(fieldOffset[fi] + i) = srcAddr[i] ;
            }
            srcAddr += opCount ;
            fieldOffset[fi] += opCount ;
         }
         ++pmsg ;
      }
      if (planeMax) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<opCount; ++i) {
               (domain.*dest)(fieldOffset[fi] + i) = srcAddr[i] ;
            }
            srcAddr += opCount ;
            fieldOffset[fi] += opCount ;
         }
         ++pmsg ;
      }
   }

   if (rowMin | rowMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      Index_t opCount = dx * dz ;

      if (rowMin) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<opCount; ++i) {
               (domain.*dest)(fieldOffset[fi] + i) = srcAddr[i] ;
            }
            srcAddr += opCount ;
            fieldOffset[fi] += opCount ;
         }
         ++pmsg ;
      }
      if (rowMax) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<opCount; ++i) {
               (domain.*dest)(fieldOffset[fi] + i) = srcAddr[i] ;
            }
            srcAddr += opCount ;
            fieldOffset[fi] += opCount ;
         }
         ++pmsg ;
      }
   }
   if (colMin | colMax) {
      /* ASSUMING ONE DOMAIN PER RANK, CONSTANT BLOCK SIZE HERE */
      Index_t opCount = dy * dz ;

      if (colMin) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<opCount; ++i) {
               (domain.*dest)(fieldOffset[fi] + i) = srcAddr[i] ;
            }
            srcAddr += opCount ;
            fieldOffset[fi] += opCount ;
         }
         ++pmsg ;
      }
      if (colMax) {
         /* contiguous memory */
         srcAddr = &domain.commDataRecv[pmsg * maxPlaneComm] ;
         MPI_Wait(&domain.recvRequest[pmsg], &status) ;
         for (Index_t fi=0 ; fi<xferFields; ++fi) {
            Domain_member dest = fieldData[fi] ;
            for (Index_t i=0; i<opCount; ++i) {
               (domain.*dest)(fieldOffset[fi] + i) = srcAddr[i] ;
            }
            srcAddr += opCount ;
         }
         ++pmsg ;
      }
   }
}

#endif

// ===================== lulesh.cc (license block at the top of this file; main() replaced by the one at the end) =====================


#include <climits>
#include <vector>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <sys/time.h>
#include <iostream>
#include <unistd.h>

#if _OPENMP
# include <omp.h>
#endif


/* Work Routines */

static inline
void TimeIncrement(Domain& domain)
{
   Real_t targetdt = domain.stoptime() - domain.time() ;

   if ((domain.dtfixed() <= Real_t(0.0)) && (domain.cycle() != Int_t(0))) {
      Real_t ratio ;
      Real_t olddt = domain.deltatime() ;

      /* This will require a reduction in parallel */
      Real_t gnewdt = Real_t(1.0e+20) ;
      Real_t newdt ;
      if (domain.dtcourant() < gnewdt) {
         gnewdt = domain.dtcourant() / Real_t(2.0) ;
      }
      if (domain.dthydro() < gnewdt) {
         gnewdt = domain.dthydro() * Real_t(2.0) / Real_t(3.0) ;
      }

#if USE_DISTRIBUTED      
      MPI_Allreduce(&gnewdt, &newdt, 1,
                    ((sizeof(Real_t) == 4) ? MPI_FLOAT : MPI_DOUBLE),
                    MPI_MIN, MPI_COMM_WORLD) ;
#else
      newdt = gnewdt;
#endif
      
      ratio = newdt / olddt ;
      if (ratio >= Real_t(1.0)) {
         if (ratio < domain.deltatimemultlb()) {
            newdt = olddt ;
         }
         else if (ratio > domain.deltatimemultub()) {
            newdt = olddt*domain.deltatimemultub() ;
         }
      }

      if (newdt > domain.dtmax()) {
         newdt = domain.dtmax() ;
      }
      domain.deltatime() = newdt ;
   }

   /* TRY TO PREVENT VERY SMALL SCALING ON THE NEXT CYCLE */
   if ((targetdt > domain.deltatime()) &&
       (targetdt < (Real_t(4.0) * domain.deltatime() / Real_t(3.0))) ) {
      targetdt = Real_t(2.0) * domain.deltatime() / Real_t(3.0) ;
   }

   if (targetdt < domain.deltatime()) {
      domain.deltatime() = targetdt ;
   }

   domain.time() += domain.deltatime() ;

   ++domain.cycle() ;
}

/******************************************/

static inline
void CollectDomainNodesToElemNodes(Domain &domain,
                                   const Index_t* elemToNode,
                                   Real_t elemX[8],
                                   Real_t elemY[8],
                                   Real_t elemZ[8])
{
   Index_t nd0i = elemToNode[0] ;
   Index_t nd1i = elemToNode[1] ;
   Index_t nd2i = elemToNode[2] ;
   Index_t nd3i = elemToNode[3] ;
   Index_t nd4i = elemToNode[4] ;
   Index_t nd5i = elemToNode[5] ;
   Index_t nd6i = elemToNode[6] ;
   Index_t nd7i = elemToNode[7] ;

   elemX[0] = domain.x(nd0i);
   elemX[1] = domain.x(nd1i);
   elemX[2] = domain.x(nd2i);
   elemX[3] = domain.x(nd3i);
   elemX[4] = domain.x(nd4i);
   elemX[5] = domain.x(nd5i);
   elemX[6] = domain.x(nd6i);
   elemX[7] = domain.x(nd7i);

   elemY[0] = domain.y(nd0i);
   elemY[1] = domain.y(nd1i);
   elemY[2] = domain.y(nd2i);
   elemY[3] = domain.y(nd3i);
   elemY[4] = domain.y(nd4i);
   elemY[5] = domain.y(nd5i);
   elemY[6] = domain.y(nd6i);
   elemY[7] = domain.y(nd7i);

   elemZ[0] = domain.z(nd0i);
   elemZ[1] = domain.z(nd1i);
   elemZ[2] = domain.z(nd2i);
   elemZ[3] = domain.z(nd3i);
   elemZ[4] = domain.z(nd4i);
   elemZ[5] = domain.z(nd5i);
   elemZ[6] = domain.z(nd6i);
   elemZ[7] = domain.z(nd7i);

}

/******************************************/

static inline
void InitStressTermsForElems(Domain &domain,
                             Real_t *sigxx, Real_t *sigyy, Real_t *sigzz,
                             Index_t numElem)
{
   //
   // pull in the stresses appropriate to the hydro integration
   //

   for (Index_t i = 0 ; i < numElem ; ++i){
      sigxx[i] = sigyy[i] = sigzz[i] =  - domain.p(i) - domain.q(i) ;
   }
}

/******************************************/

static inline
void CalcElemShapeFunctionDerivatives( Real_t const x[],
                                       Real_t const y[],
                                       Real_t const z[],
                                       Real_t b[][8],
                                       Real_t* const volume )
{
  const Real_t x0 = x[0] ;   const Real_t x1 = x[1] ;
  const Real_t x2 = x[2] ;   const Real_t x3 = x[3] ;
  const Real_t x4 = x[4] ;   const Real_t x5 = x[5] ;
  const Real_t x6 = x[6] ;   const Real_t x7 = x[7] ;

  const Real_t y0 = y[0] ;   const Real_t y1 = y[1] ;
  const Real_t y2 = y[2] ;   const Real_t y3 = y[3] ;
  const Real_t y4 = y[4] ;   const Real_t y5 = y[5] ;
  const Real_t y6 = y[6] ;   const Real_t y7 = y[7] ;

  const Real_t z0 = z[0] ;   const Real_t z1 = z[1] ;
  const Real_t z2 = z[2] ;   const Real_t z3 = z[3] ;
  const Real_t z4 = z[4] ;   const Real_t z5 = z[5] ;
  const Real_t z6 = z[6] ;   const Real_t z7 = z[7] ;

  Real_t fjxxi, fjxet, fjxze;
  Real_t fjyxi, fjyet, fjyze;
  Real_t fjzxi, fjzet, fjzze;
  Real_t cjxxi, cjxet, cjxze;
  Real_t cjyxi, cjyet, cjyze;
  Real_t cjzxi, cjzet, cjzze;

  fjxxi = Real_t(.125) * ( (x6-x0) + (x5-x3) - (x7-x1) - (x4-x2) );
  fjxet = Real_t(.125) * ( (x6-x0) - (x5-x3) + (x7-x1) - (x4-x2) );
  fjxze = Real_t(.125) * ( (x6-x0) + (x5-x3) + (x7-x1) + (x4-x2) );

  fjyxi = Real_t(.125) * ( (y6-y0) + (y5-y3) - (y7-y1) - (y4-y2) );
  fjyet = Real_t(.125) * ( (y6-y0) - (y5-y3) + (y7-y1) - (y4-y2) );
  fjyze = Real_t(.125) * ( (y6-y0) + (y5-y3) + (y7-y1) + (y4-y2) );

  fjzxi = Real_t(.125) * ( (z6-z0) + (z5-z3) - (z7-z1) - (z4-z2) );
  fjzet = Real_t(.125) * ( (z6-z0) - (z5-z3) + (z7-z1) - (z4-z2) );
  fjzze = Real_t(.125) * ( (z6-z0) + (z5-z3) + (z7-z1) + (z4-z2) );

  /* compute cofactors */
  cjxxi =    (fjyet * fjzze) - (fjzet * fjyze);
  cjxet =  - (fjyxi * fjzze) + (fjzxi * fjyze);
  cjxze =    (fjyxi * fjzet) - (fjzxi * fjyet);

  cjyxi =  - (fjxet * fjzze) + (fjzet * fjxze);
  cjyet =    (fjxxi * fjzze) - (fjzxi * fjxze);
  cjyze =  - (fjxxi * fjzet) + (fjzxi * fjxet);

  cjzxi =    (fjxet * fjyze) - (fjyet * fjxze);
  cjzet =  - (fjxxi * fjyze) + (fjyxi * fjxze);
  cjzze =    (fjxxi * fjyet) - (fjyxi * fjxet);

  /* calculate partials :
     this need only be done for l = 0,1,2,3   since , by symmetry ,
     (6,7,4,5) = - (0,1,2,3) .
  */
  b[0][0] =   -  cjxxi  -  cjxet  -  cjxze;
  b[0][1] =      cjxxi  -  cjxet  -  cjxze;
  b[0][2] =      cjxxi  +  cjxet  -  cjxze;
  b[0][3] =   -  cjxxi  +  cjxet  -  cjxze;
  b[0][4] = -b[0][2];
  b[0][5] = -b[0][3];
  b[0][6] = -b[0][0];
  b[0][7] = -b[0][1];

  b[1][0] =   -  cjyxi  -  cjyet  -  cjyze;
  b[1][1] =      cjyxi  -  cjyet  -  cjyze;
  b[1][2] =      cjyxi  +  cjyet  -  cjyze;
  b[1][3] =   -  cjyxi  +  cjyet  -  cjyze;
  b[1][4] = -b[1][2];
  b[1][5] = -b[1][3];
  b[1][6] = -b[1][0];
  b[1][7] = -b[1][1];

  b[2][0] =   -  cjzxi  -  cjzet  -  cjzze;
  b[2][1] =      cjzxi  -  cjzet  -  cjzze;
  b[2][2] =      cjzxi  +  cjzet  -  cjzze;
  b[2][3] =   -  cjzxi  +  cjzet  -  cjzze;
  b[2][4] = -b[2][2];
  b[2][5] = -b[2][3];
  b[2][6] = -b[2][0];
  b[2][7] = -b[2][1];

  /* calculate jacobian determinant (volume) */
  *volume = Real_t(8.) * ( fjxet * cjxet + fjyet * cjyet + fjzet * cjzet);
}

/******************************************/

static inline
void SumElemFaceNormal(Real_t *normalX0, Real_t *normalY0, Real_t *normalZ0,
                       Real_t *normalX1, Real_t *normalY1, Real_t *normalZ1,
                       Real_t *normalX2, Real_t *normalY2, Real_t *normalZ2,
                       Real_t *normalX3, Real_t *normalY3, Real_t *normalZ3,
                       const Real_t x0, const Real_t y0, const Real_t z0,
                       const Real_t x1, const Real_t y1, const Real_t z1,
                       const Real_t x2, const Real_t y2, const Real_t z2,
                       const Real_t x3, const Real_t y3, const Real_t z3)
{
   Real_t bisectX0 = Real_t(0.5) * (x3 + x2 - x1 - x0);
   Real_t bisectY0 = Real_t(0.5) * (y3 + y2 - y1 - y0);
   Real_t bisectZ0 = Real_t(0.5) * (z3 + z2 - z1 - z0);
   Real_t bisectX1 = Real_t(0.5) * (x2 + x1 - x3 - x0);
   Real_t bisectY1 = Real_t(0.5) * (y2 + y1 - y3 - y0);
   Real_t bisectZ1 = Real_t(0.5) * (z2 + z1 - z3 - z0);
   Real_t areaX = Real_t(0.25) * (bisectY0 * bisectZ1 - bisectZ0 * bisectY1);
   Real_t areaY = Real_t(0.25) * (bisectZ0 * bisectX1 - bisectX0 * bisectZ1);
   Real_t areaZ = Real_t(0.25) * (bisectX0 * bisectY1 - bisectY0 * bisectX1);

   *normalX0 += areaX;
   *normalX1 += areaX;
   *normalX2 += areaX;
   *normalX3 += areaX;

   *normalY0 += areaY;
   *normalY1 += areaY;
   *normalY2 += areaY;
   *normalY3 += areaY;

   *normalZ0 += areaZ;
   *normalZ1 += areaZ;
   *normalZ2 += areaZ;
   *normalZ3 += areaZ;
}

/******************************************/

static inline
void CalcElemNodeNormals(Real_t pfx[8],
                         Real_t pfy[8],
                         Real_t pfz[8],
                         const Real_t x[8],
                         const Real_t y[8],
                         const Real_t z[8])
{
   for (Index_t i = 0 ; i < 8 ; ++i) {
      pfx[i] = Real_t(0.0);
      pfy[i] = Real_t(0.0);
      pfz[i] = Real_t(0.0);
   }
   /* evaluate face one: nodes 0, 1, 2, 3 */
   SumElemFaceNormal(&pfx[0], &pfy[0], &pfz[0],
                  &pfx[1], &pfy[1], &pfz[1],
                  &pfx[2], &pfy[2], &pfz[2],
                  &pfx[3], &pfy[3], &pfz[3],
                  x[0], y[0], z[0], x[1], y[1], z[1],
                  x[2], y[2], z[2], x[3], y[3], z[3]);
   /* evaluate face two: nodes 0, 4, 5, 1 */
   SumElemFaceNormal(&pfx[0], &pfy[0], &pfz[0],
                  &pfx[4], &pfy[4], &pfz[4],
                  &pfx[5], &pfy[5], &pfz[5],
                  &pfx[1], &pfy[1], &pfz[1],
                  x[0], y[0], z[0], x[4], y[4], z[4],
                  x[5], y[5], z[5], x[1], y[1], z[1]);
   /* evaluate face three: nodes 1, 5, 6, 2 */
   SumElemFaceNormal(&pfx[1], &pfy[1], &pfz[1],
                  &pfx[5], &pfy[5], &pfz[5],
                  &pfx[6], &pfy[6], &pfz[6],
                  &pfx[2], &pfy[2], &pfz[2],
                  x[1], y[1], z[1], x[5], y[5], z[5],
                  x[6], y[6], z[6], x[2], y[2], z[2]);
   /* evaluate face four: nodes 2, 6, 7, 3 */
   SumElemFaceNormal(&pfx[2], &pfy[2], &pfz[2],
                  &pfx[6], &pfy[6], &pfz[6],
                  &pfx[7], &pfy[7], &pfz[7],
                  &pfx[3], &pfy[3], &pfz[3],
                  x[2], y[2], z[2], x[6], y[6], z[6],
                  x[7], y[7], z[7], x[3], y[3], z[3]);
   /* evaluate face five: nodes 3, 7, 4, 0 */
   SumElemFaceNormal(&pfx[3], &pfy[3], &pfz[3],
                  &pfx[7], &pfy[7], &pfz[7],
                  &pfx[4], &pfy[4], &pfz[4],
                  &pfx[0], &pfy[0], &pfz[0],
                  x[3], y[3], z[3], x[7], y[7], z[7],
                  x[4], y[4], z[4], x[0], y[0], z[0]);
   /* evaluate face six: nodes 4, 7, 6, 5 */
   SumElemFaceNormal(&pfx[4], &pfy[4], &pfz[4],
                  &pfx[7], &pfy[7], &pfz[7],
                  &pfx[6], &pfy[6], &pfz[6],
                  &pfx[5], &pfy[5], &pfz[5],
                  x[4], y[4], z[4], x[7], y[7], z[7],
                  x[6], y[6], z[6], x[5], y[5], z[5]);
}

/******************************************/

static inline
void SumElemStressesToNodeForces( const Real_t B[][8],
                                  const Real_t stress_xx,
                                  const Real_t stress_yy,
                                  const Real_t stress_zz,
                                  Real_t fx[], Real_t fy[], Real_t fz[] )
{
   for(Index_t i = 0; i < 8; i++) {
      fx[i] = -( stress_xx * B[0][i] );
      fy[i] = -( stress_yy * B[1][i]  );
      fz[i] = -( stress_zz * B[2][i] );
   }
}

/******************************************/

static inline
void IntegrateStressForElems( Domain &domain,
                              Real_t *sigxx, Real_t *sigyy, Real_t *sigzz,
                              Real_t *determ, Index_t numElem, Index_t numNode)
{
#if _OPENMP
   Index_t numthreads = omp_get_max_threads();
#else
   Index_t numthreads = 1;
#endif

   Index_t numElem8 = numElem * 8 ;
   Real_t *fx_elem;
   Real_t *fy_elem;
   Real_t *fz_elem;
   Real_t fx_local[8] ;
   Real_t fy_local[8] ;
   Real_t fz_local[8] ;


  if (numthreads > 1) {
     fx_elem = Allocate<Real_t>(numElem8) ;
     fy_elem = Allocate<Real_t>(numElem8) ;
     fz_elem = Allocate<Real_t>(numElem8) ;
  }
  // loop over all elements

  for( Index_t k=0 ; k<numElem ; ++k )
  {
    const Index_t* const elemToNode = domain.nodelist(k);
    Real_t B[3][8] ;// shape function derivatives
    Real_t x_local[8] ;
    Real_t y_local[8] ;
    Real_t z_local[8] ;

    // get nodal coordinates from global arrays and copy into local arrays.
    CollectDomainNodesToElemNodes(domain, elemToNode, x_local, y_local, z_local);

    // Volume calculation involves extra work for numerical consistency
    CalcElemShapeFunctionDerivatives(x_local, y_local, z_local,
                                         B, &determ[k]);

    CalcElemNodeNormals( B[0] , B[1], B[2],
                          x_local, y_local, z_local );

    if (numthreads > 1) {
       // Eliminate thread writing conflicts at the nodes by giving
       // each element its own copy to write to
       SumElemStressesToNodeForces( B, sigxx[k], sigyy[k], sigzz[k],
                                    &fx_elem[k*8],
                                    &fy_elem[k*8],
                                    &fz_elem[k*8] ) ;
    }
    else {
       SumElemStressesToNodeForces( B, sigxx[k], sigyy[k], sigzz[k],
                                    fx_local, fy_local, fz_local ) ;

       // copy nodal force contributions to global force arrray.
       for( Index_t lnode=0 ; lnode<8 ; ++lnode ) {
          Index_t gnode = elemToNode[lnode];
          domain.fx(gnode) += fx_local[lnode];
          domain.fy(gnode) += fy_local[lnode];
          domain.fz(gnode) += fz_local[lnode];
       }
    }
  }

  if (numthreads > 1) {
     // If threaded, then we need to copy the data out of the temporary
     // arrays used above into the final forces field
     for( Index_t gnode=0 ; gnode<numNode ; ++gnode )
     {
        Index_t count = domain.nodeElemCount(gnode) ;
        Index_t *cornerList = domain.nodeElemCornerList(gnode) ;
        Real_t fx_tmp = Real_t(0.0) ;
        Real_t fy_tmp = Real_t(0.0) ;
        Real_t fz_tmp = Real_t(0.0) ;
        for (Index_t i=0 ; i < count ; ++i) {
           Index_t ielem = cornerList[i] ;
           fx_tmp += fx_elem[ielem] ;
           fy_tmp += fy_elem[ielem] ;
           fz_tmp += fz_elem[ielem] ;
        }
        domain.fx(gnode) = fx_tmp ;
        domain.fy(gnode) = fy_tmp ;
        domain.fz(gnode) = fz_tmp ;
     }
     Release(&fz_elem) ;
     Release(&fy_elem) ;
     Release(&fx_elem) ;
  }
}

/******************************************/

static inline
void VoluDer(const Real_t x0, const Real_t x1, const Real_t x2,
             const Real_t x3, const Real_t x4, const Real_t x5,
             const Real_t y0, const Real_t y1, const Real_t y2,
             const Real_t y3, const Real_t y4, const Real_t y5,
             const Real_t z0, const Real_t z1, const Real_t z2,
             const Real_t z3, const Real_t z4, const Real_t z5,
             Real_t* dvdx, Real_t* dvdy, Real_t* dvdz)
{
   const Real_t twelfth = Real_t(1.0) / Real_t(12.0) ;

   *dvdx =
      (y1 + y2) * (z0 + z1) - (y0 + y1) * (z1 + z2) +
      (y0 + y4) * (z3 + z4) - (y3 + y4) * (z0 + z4) -
      (y2 + y5) * (z3 + z5) + (y3 + y5) * (z2 + z5);
   *dvdy =
      - (x1 + x2) * (z0 + z1) + (x0 + x1) * (z1 + z2) -
      (x0 + x4) * (z3 + z4) + (x3 + x4) * (z0 + z4) +
      (x2 + x5) * (z3 + z5) - (x3 + x5) * (z2 + z5);

   *dvdz =
      - (y1 + y2) * (x0 + x1) + (y0 + y1) * (x1 + x2) -
      (y0 + y4) * (x3 + x4) + (y3 + y4) * (x0 + x4) +
      (y2 + y5) * (x3 + x5) - (y3 + y5) * (x2 + x5);

   *dvdx *= twelfth;
   *dvdy *= twelfth;
   *dvdz *= twelfth;
}

/******************************************/

static inline
void CalcElemVolumeDerivative(Real_t dvdx[8],
                              Real_t dvdy[8],
                              Real_t dvdz[8],
                              const Real_t x[8],
                              const Real_t y[8],
                              const Real_t z[8])
{
   VoluDer(x[1], x[2], x[3], x[4], x[5], x[7],
           y[1], y[2], y[3], y[4], y[5], y[7],
           z[1], z[2], z[3], z[4], z[5], z[7],
           &dvdx[0], &dvdy[0], &dvdz[0]);
   VoluDer(x[0], x[1], x[2], x[7], x[4], x[6],
           y[0], y[1], y[2], y[7], y[4], y[6],
           z[0], z[1], z[2], z[7], z[4], z[6],
           &dvdx[3], &dvdy[3], &dvdz[3]);
   VoluDer(x[3], x[0], x[1], x[6], x[7], x[5],
           y[3], y[0], y[1], y[6], y[7], y[5],
           z[3], z[0], z[1], z[6], z[7], z[5],
           &dvdx[2], &dvdy[2], &dvdz[2]);
   VoluDer(x[2], x[3], x[0], x[5], x[6], x[4],
           y[2], y[3], y[0], y[5], y[6], y[4],
           z[2], z[3], z[0], z[5], z[6], z[4],
           &dvdx[1], &dvdy[1], &dvdz[1]);
   VoluDer(x[7], x[6], x[5], x[0], x[3], x[1],
           y[7], y[6], y[5], y[0], y[3], y[1],
           z[7], z[6], z[5], z[0], z[3], z[1],
           &dvdx[4], &dvdy[4], &dvdz[4]);
   VoluDer(x[4], x[7], x[6], x[1], x[0], x[2],
           y[4], y[7], y[6], y[1], y[0], y[2],
           z[4], z[7], z[6], z[1], z[0], z[2],
           &dvdx[5], &dvdy[5], &dvdz[5]);
   VoluDer(x[5], x[4], x[7], x[2], x[1], x[3],
           y[5], y[4], y[7], y[2], y[1], y[3],
           z[5], z[4], z[7], z[2], z[1], z[3],
           &dvdx[6], &dvdy[6], &dvdz[6]);
   VoluDer(x[6], x[5], x[4], x[3], x[2], x[0],
           y[6], y[5], y[4], y[3], y[2], y[0],
           z[6], z[5], z[4], z[3], z[2], z[0],
           &dvdx[7], &dvdy[7], &dvdz[7]);
}

/******************************************/

static inline
void CalcElemFBHourglassForce(Real_t *xd, Real_t *yd, Real_t *zd,  Real_t hourgam[][4],
                              Real_t coefficient,
                              Real_t *hgfx, Real_t *hgfy, Real_t *hgfz )
{
   Real_t hxx[4];
   for(Index_t i = 0; i < 4; i++) {
      hxx[i] = hourgam[0][i] * xd[0] + hourgam[1][i] * xd[1] +
               hourgam[2][i] * xd[2] + hourgam[3][i] * xd[3] +
               hourgam[4][i] * xd[4] + hourgam[5][i] * xd[5] +
               hourgam[6][i] * xd[6] + hourgam[7][i] * xd[7];
   }
   for(Index_t i = 0; i < 8; i++) {
      hgfx[i] = coefficient *
                (hourgam[i][0] * hxx[0] + hourgam[i][1] * hxx[1] +
                 hourgam[i][2] * hxx[2] + hourgam[i][3] * hxx[3]);
   }
   for(Index_t i = 0; i < 4; i++) {
      hxx[i] = hourgam[0][i] * yd[0] + hourgam[1][i] * yd[1] +
               hourgam[2][i] * yd[2] + hourgam[3][i] * yd[3] +
               hourgam[4][i] * yd[4] + hourgam[5][i] * yd[5] +
               hourgam[6][i] * yd[6] + hourgam[7][i] * yd[7];
   }
   for(Index_t i = 0; i < 8; i++) {
      hgfy[i] = coefficient *
                (hourgam[i][0] * hxx[0] + hourgam[i][1] * hxx[1] +
                 hourgam[i][2] * hxx[2] + hourgam[i][3] * hxx[3]);
   }
   for(Index_t i = 0; i < 4; i++) {
      hxx[i] = hourgam[0][i] * zd[0] + hourgam[1][i] * zd[1] +
               hourgam[2][i] * zd[2] + hourgam[3][i] * zd[3] +
               hourgam[4][i] * zd[4] + hourgam[5][i] * zd[5] +
               hourgam[6][i] * zd[6] + hourgam[7][i] * zd[7];
   }
   for(Index_t i = 0; i < 8; i++) {
      hgfz[i] = coefficient *
                (hourgam[i][0] * hxx[0] + hourgam[i][1] * hxx[1] +
                 hourgam[i][2] * hxx[2] + hourgam[i][3] * hxx[3]);
   }
}

/******************************************/

static inline
void CalcFBHourglassForceForElems( Domain &domain,
                                   Real_t *determ,
                                   Real_t *x8n, Real_t *y8n, Real_t *z8n,
                                   Real_t *dvdx, Real_t *dvdy, Real_t *dvdz,
                                   Real_t hourg, Index_t numElem,
                                   Index_t numNode)
{

#if _OPENMP
   Index_t numthreads = omp_get_max_threads();
#else
   Index_t numthreads = 1;
#endif
   /*************************************************
    *
    *     FUNCTION: Calculates the Flanagan-Belytschko anti-hourglass
    *               force.
    *
    *************************************************/
  
   Index_t numElem8 = numElem * 8 ;

   Real_t *fx_elem; 
   Real_t *fy_elem; 
   Real_t *fz_elem; 

   if(numthreads > 1) {
      fx_elem = Allocate<Real_t>(numElem8) ;
      fy_elem = Allocate<Real_t>(numElem8) ;
      fz_elem = Allocate<Real_t>(numElem8) ;
   }

   Real_t  gamma[4][8];

   gamma[0][0] = Real_t( 1.);
   gamma[0][1] = Real_t( 1.);
   gamma[0][2] = Real_t(-1.);
   gamma[0][3] = Real_t(-1.);
   gamma[0][4] = Real_t(-1.);
   gamma[0][5] = Real_t(-1.);
   gamma[0][6] = Real_t( 1.);
   gamma[0][7] = Real_t( 1.);
   gamma[1][0] = Real_t( 1.);
   gamma[1][1] = Real_t(-1.);
   gamma[1][2] = Real_t(-1.);
   gamma[1][3] = Real_t( 1.);
   gamma[1][4] = Real_t(-1.);
   gamma[1][5] = Real_t( 1.);
   gamma[1][6] = Real_t( 1.);
   gamma[1][7] = Real_t(-1.);
   gamma[2][0] = Real_t( 1.);
   gamma[2][1] = Real_t(-1.);
   gamma[2][2] = Real_t( 1.);
   gamma[2][3] = Real_t(-1.);
   gamma[2][4] = Real_t( 1.);
   gamma[2][5] = Real_t(-1.);
   gamma[2][6] = Real_t( 1.);
   gamma[2][7] = Real_t(-1.);
   gamma[3][0] = Real_t(-1.);
   gamma[3][1] = Real_t( 1.);
   gamma[3][2] = Real_t(-1.);
   gamma[3][3] = Real_t( 1.);
   gamma[3][4] = Real_t( 1.);
   gamma[3][5] = Real_t(-1.);
   gamma[3][6] = Real_t( 1.);
   gamma[3][7] = Real_t(-1.);

/*************************************************/
/*    compute the hourglass modes */


   for(Index_t i2=0;i2<numElem;++i2){
      Real_t *fx_local, *fy_local, *fz_local ;
      Real_t hgfx[8], hgfy[8], hgfz[8] ;

      Real_t coefficient;

      Real_t hourgam[8][4];
      Real_t xd1[8], yd1[8], zd1[8] ;

      const Index_t *elemToNode = domain.nodelist(i2);
      Index_t i3=8*i2;
      Real_t volinv=Real_t(1.0)/determ[i2];
      Real_t ss1, mass1, volume13 ;
      for(Index_t i1=0;i1<4;++i1){

         Real_t hourmodx =
            x8n[i3] * gamma[i1][0] + x8n[i3+1] * gamma[i1][1] +
            x8n[i3+2] * gamma[i1][2] + x8n[i3+3] * gamma[i1][3] +
            x8n[i3+4] * gamma[i1][4] + x8n[i3+5] * gamma[i1][5] +
            x8n[i3+6] * gamma[i1][6] + x8n[i3+7] * gamma[i1][7];

         Real_t hourmody =
            y8n[i3] * gamma[i1][0] + y8n[i3+1] * gamma[i1][1] +
            y8n[i3+2] * gamma[i1][2] + y8n[i3+3] * gamma[i1][3] +
            y8n[i3+4] * gamma[i1][4] + y8n[i3+5] * gamma[i1][5] +
            y8n[i3+6] * gamma[i1][6] + y8n[i3+7] * gamma[i1][7];

         Real_t hourmodz =
            z8n[i3] * gamma[i1][0] + z8n[i3+1] * gamma[i1][1] +
            z8n[i3+2] * gamma[i1][2] + z8n[i3+3] * gamma[i1][3] +
            z8n[i3+4] * gamma[i1][4] + z8n[i3+5] * gamma[i1][5] +
            z8n[i3+6] * gamma[i1][6] + z8n[i3+7] * gamma[i1][7];

         hourgam[0][i1] = gamma[i1][0] -  volinv*(dvdx[i3  ] * hourmodx +
                                                  dvdy[i3  ] * hourmody +
                                                  dvdz[i3  ] * hourmodz );

         hourgam[1][i1] = gamma[i1][1] -  volinv*(dvdx[i3+1] * hourmodx +
                                                  dvdy[i3+1] * hourmody +
                                                  dvdz[i3+1] * hourmodz );

         hourgam[2][i1] = gamma[i1][2] -  volinv*(dvdx[i3+2] * hourmodx +
                                                  dvdy[i3+2] * hourmody +
                                                  dvdz[i3+2] * hourmodz );

         hourgam[3][i1] = gamma[i1][3] -  volinv*(dvdx[i3+3] * hourmodx +
                                                  dvdy[i3+3] * hourmody +
                                                  dvdz[i3+3] * hourmodz );

         hourgam[4][i1] = gamma[i1][4] -  volinv*(dvdx[i3+4] * hourmodx +
                                                  dvdy[i3+4] * hourmody +
                                                  dvdz[i3+4] * hourmodz );

         hourgam[5][i1] = gamma[i1][5] -  volinv*(dvdx[i3+5] * hourmodx +
                                                  dvdy[i3+5] * hourmody +
                                                  dvdz[i3+5] * hourmodz );

         hourgam[6][i1] = gamma[i1][6] -  volinv*(dvdx[i3+6] * hourmodx +
                                                  dvdy[i3+6] * hourmody +
                                                  dvdz[i3+6] * hourmodz );

         hourgam[7][i1] = gamma[i1][7] -  volinv*(dvdx[i3+7] * hourmodx +
                                                  dvdy[i3+7] * hourmody +
                                                  dvdz[i3+7] * hourmodz );

      }

      /* compute forces */
      /* store forces into h arrays (force arrays) */

      ss1=domain.ss(i2);
      mass1=domain.elemMass(i2);
      volume13=CBRT(determ[i2]);

      Index_t n0si2 = elemToNode[0];
      Index_t n1si2 = elemToNode[1];
      Index_t n2si2 = elemToNode[2];
      Index_t n3si2 = elemToNode[3];
      Index_t n4si2 = elemToNode[4];
      Index_t n5si2 = elemToNode[5];
      Index_t n6si2 = elemToNode[6];
      Index_t n7si2 = elemToNode[7];

      xd1[0] = domain.xd(n0si2);
      xd1[1] = domain.xd(n1si2);
      xd1[2] = domain.xd(n2si2);
      xd1[3] = domain.xd(n3si2);
      xd1[4] = domain.xd(n4si2);
      xd1[5] = domain.xd(n5si2);
      xd1[6] = domain.xd(n6si2);
      xd1[7] = domain.xd(n7si2);

      yd1[0] = domain.yd(n0si2);
      yd1[1] = domain.yd(n1si2);
      yd1[2] = domain.yd(n2si2);
      yd1[3] = domain.yd(n3si2);
      yd1[4] = domain.yd(n4si2);
      yd1[5] = domain.yd(n5si2);
      yd1[6] = domain.yd(n6si2);
      yd1[7] = domain.yd(n7si2);

      zd1[0] = domain.zd(n0si2);
      zd1[1] = domain.zd(n1si2);
      zd1[2] = domain.zd(n2si2);
      zd1[3] = domain.zd(n3si2);
      zd1[4] = domain.zd(n4si2);
      zd1[5] = domain.zd(n5si2);
      zd1[6] = domain.zd(n6si2);
      zd1[7] = domain.zd(n7si2);

      coefficient = - hourg * Real_t(0.01) * ss1 * mass1 / volume13;

      CalcElemFBHourglassForce(xd1,yd1,zd1,
                      hourgam,
                      coefficient, hgfx, hgfy, hgfz);

      // With the threaded version, we write into local arrays per elem
      // so we don't have to worry about race conditions
      if (numthreads > 1) {
         fx_local = &fx_elem[i3] ;
         fx_local[0] = hgfx[0];
         fx_local[1] = hgfx[1];
         fx_local[2] = hgfx[2];
         fx_local[3] = hgfx[3];
         fx_local[4] = hgfx[4];
         fx_local[5] = hgfx[5];
         fx_local[6] = hgfx[6];
         fx_local[7] = hgfx[7];

         fy_local = &fy_elem[i3] ;
         fy_local[0] = hgfy[0];
         fy_local[1] = hgfy[1];
         fy_local[2] = hgfy[2];
         fy_local[3] = hgfy[3];
         fy_local[4] = hgfy[4];
         fy_local[5] = hgfy[5];
         fy_local[6] = hgfy[6];
         fy_local[7] = hgfy[7];

         fz_local = &fz_elem[i3] ;
         fz_local[0] = hgfz[0];
         fz_local[1] = hgfz[1];
         fz_local[2] = hgfz[2];
         fz_local[3] = hgfz[3];
         fz_local[4] = hgfz[4];
         fz_local[5] = hgfz[5];
         fz_local[6] = hgfz[6];
         fz_local[7] = hgfz[7];
      }
      else {
         domain.fx(n0si2) += hgfx[0];
         domain.fy(n0si2) += hgfy[0];
         domain.fz(n0si2) += hgfz[0];

         domain.fx(n1si2) += hgfx[1];
         domain.fy(n1si2) += hgfy[1];
         domain.fz(n1si2) += hgfz[1];

         domain.fx(n2si2) += hgfx[2];
         domain.fy(n2si2) += hgfy[2];
         domain.fz(n2si2) += hgfz[2];

         domain.fx(n3si2) += hgfx[3];
         domain.fy(n3si2) += hgfy[3];
         domain.fz(n3si2) += hgfz[3];

         domain.fx(n4si2) += hgfx[4];
         domain.fy(n4si2) += hgfy[4];
         domain.fz(n4si2) += hgfz[4];

         domain.fx(n5si2) += hgfx[5];
         domain.fy(n5si2) += hgfy[5];
         domain.fz(n5si2) += hgfz[5];

         domain.fx(n6si2) += hgfx[6];
         domain.fy(n6si2) += hgfy[6];
         domain.fz(n6si2) += hgfz[6];

         domain.fx(n7si2) += hgfx[7];
         domain.fy(n7si2) += hgfy[7];
         domain.fz(n7si2) += hgfz[7];
      }
   }

   if (numthreads > 1) {
     // Collect the data from the local arrays into the final force arrays
      for( Index_t gnode=0 ; gnode<numNode ; ++gnode )
      {
         Index_t count = domain.nodeElemCount(gnode) ;
         Index_t *cornerList = domain.nodeElemCornerList(gnode) ;
         Real_t fx_tmp = Real_t(0.0) ;
         Real_t fy_tmp = Real_t(0.0) ;
         Real_t fz_tmp = Real_t(0.0) ;
         for (Index_t i=0 ; i < count ; ++i) {
            Index_t ielem = cornerList[i] ;
            fx_tmp += fx_elem[ielem] ;
            fy_tmp += fy_elem[ielem] ;
            fz_tmp += fz_elem[ielem] ;
         }
         domain.fx(gnode) += fx_tmp ;
         domain.fy(gnode) += fy_tmp ;
         domain.fz(gnode) += fz_tmp ;
      }
      Release(&fz_elem) ;
      Release(&fy_elem) ;
      Release(&fx_elem) ;
   }
}

/******************************************/

static inline
void CalcHourglassControlForElems(Domain& domain,
                                  Real_t determ[], Real_t hgcoef)
{
   Index_t numElem = domain.numElem() ;
   Index_t numElem8 = numElem * 8 ;
   Real_t *dvdx = Allocate<Real_t>(numElem8) ;
   Real_t *dvdy = Allocate<Real_t>(numElem8) ;
   Real_t *dvdz = Allocate<Real_t>(numElem8) ;
   Real_t *x8n  = Allocate<Real_t>(numElem8) ;
   Real_t *y8n  = Allocate<Real_t>(numElem8) ;
   Real_t *z8n  = Allocate<Real_t>(numElem8) ;

   /* start loop over elements */
   for (Index_t i=0 ; i<numElem ; ++i){
      Real_t  x1[8],  y1[8],  z1[8] ;
      Real_t pfx[8], pfy[8], pfz[8] ;

      Index_t* elemToNode = domain.nodelist(i);
      CollectDomainNodesToElemNodes(domain, elemToNode, x1, y1, z1);

      CalcElemVolumeDerivative(pfx, pfy, pfz, x1, y1, z1);

      /* load into temporary storage for FB Hour Glass control */
      for(Index_t ii=0;ii<8;++ii){
         Index_t jj=8*i+ii;

         dvdx[jj] = pfx[ii];
         dvdy[jj] = pfy[ii];
         dvdz[jj] = pfz[ii];
         x8n[jj]  = x1[ii];
         y8n[jj]  = y1[ii];
         z8n[jj]  = z1[ii];
      }

      determ[i] = domain.volo(i) * domain.v(i);

      /* Do a check for negative volumes */
      if ( domain.v(i) <= Real_t(0.0) ) {
#if USE_DISTRIBUTED         
         MPI_Abort(MPI_COMM_WORLD, VolumeError) ;
#else
         exit(VolumeError);
#endif
      }
   }

   if ( hgcoef > Real_t(0.) ) {
      CalcFBHourglassForceForElems( domain,
                                    determ, x8n, y8n, z8n, dvdx, dvdy, dvdz,
                                    hgcoef, numElem, domain.numNode()) ;
   }

   Release(&z8n) ;
   Release(&y8n) ;
   Release(&x8n) ;
   Release(&dvdz) ;
   Release(&dvdy) ;
   Release(&dvdx) ;

   return ;
}

/******************************************/

static inline
void CalcVolumeForceForElems(Domain& domain)
{
   Index_t numElem = domain.numElem() ;
   if (numElem != 0) {
      Real_t  hgcoef = domain.hgcoef() ;
      Real_t *sigxx  = Allocate<Real_t>(numElem) ;
      Real_t *sigyy  = Allocate<Real_t>(numElem) ;
      Real_t *sigzz  = Allocate<Real_t>(numElem) ;
      Real_t *determ = Allocate<Real_t>(numElem) ;

      /* Sum contributions to total stress tensor */
      InitStressTermsForElems(domain, sigxx, sigyy, sigzz, numElem);

      // call elemlib stress integration loop to produce nodal forces from
      // material stresses.
      IntegrateStressForElems( domain,
                               sigxx, sigyy, sigzz, determ, numElem,
                               domain.numNode()) ;

      // check for negative element volume
      for ( Index_t k=0 ; k<numElem ; ++k ) {
         if (determ[k] <= Real_t(0.0)) {
#if USE_DISTRIBUTED            
            MPI_Abort(MPI_COMM_WORLD, VolumeError) ;
#else
            exit(VolumeError);
#endif
         }
      }

      CalcHourglassControlForElems(domain, determ, hgcoef) ;

      Release(&determ) ;
      Release(&sigzz) ;
      Release(&sigyy) ;
      Release(&sigxx) ;
   }
}

/******************************************/

static inline void CalcForceForNodes(Domain& domain)
{
  Index_t numNode = domain.numNode() ;

#if USE_DISTRIBUTED  
  CommRecv(domain, MSG_COMM_SBN, 3,
           domain.sizeX() + 1, domain.sizeY() + 1, domain.sizeZ() + 1,
           true, false) ;
#endif  

  for (Index_t i=0; i<numNode; ++i) {
     domain.fx(i) = Real_t(0.0) ;
     domain.fy(i) = Real_t(0.0) ;
     domain.fz(i) = Real_t(0.0) ;
  }

  /* Calcforce calls partial, force, hourq */
  CalcVolumeForceForElems(domain) ;

#if USE_DISTRIBUTED  
  Domain_member fieldData[3] ;
  fieldData[0] = &Domain::fx ;
  fieldData[1] = &Domain::fy ;
  fieldData[2] = &Domain::fz ;
  
  CommSend(domain, MSG_COMM_SBN, 3, fieldData,
           domain.sizeX() + 1, domain.sizeY() + 1, domain.sizeZ() +  1,
           true, false) ;
  CommSBN(domain, 3, fieldData) ;
#endif  
}

/******************************************/

static inline
void CalcAccelerationForNodes(Domain &domain, Index_t numNode)
{
   
   for (Index_t i = 0; i < numNode; ++i) {
      domain.xdd(i) = domain.fx(i) / domain.nodalMass(i);
      domain.ydd(i) = domain.fy(i) / domain.nodalMass(i);
      domain.zdd(i) = domain.fz(i) / domain.nodalMass(i);
   }
}

/******************************************/

static inline
void ApplyAccelerationBoundaryConditionsForNodes(Domain& domain)
{
   Index_t size = domain.sizeX();
   Index_t numNodeBC = (size+1)*(size+1) ;

   {
      if (!domain.symmXempty() != 0) {
         for(Index_t i=0 ; i<numNodeBC ; ++i)
            domain.xdd(domain.symmX(i)) = Real_t(0.0) ;
      }

      if (!domain.symmYempty() != 0) {
         for(Index_t i=0 ; i<numNodeBC ; ++i)
            domain.ydd(domain.symmY(i)) = Real_t(0.0) ;
      }

      if (!domain.symmZempty() != 0) {
         for(Index_t i=0 ; i<numNodeBC ; ++i)
            domain.zdd(domain.symmZ(i)) = Real_t(0.0) ;
      }
   }
}

/******************************************/

static inline
void CalcVelocityForNodes(Domain &domain, const Real_t dt, const Real_t u_cut,
                          Index_t numNode)
{

   for ( Index_t i = 0 ; i < numNode ; ++i )
   {
     Real_t xdtmp, ydtmp, zdtmp ;

     xdtmp = domain.xd(i) + domain.xdd(i) * dt ;
     if( FABS(xdtmp) < u_cut ) xdtmp = Real_t(0.0);
     domain.xd(i) = xdtmp ;

     ydtmp = domain.yd(i) + domain.ydd(i) * dt ;
     if( FABS(ydtmp) < u_cut ) ydtmp = Real_t(0.0);
     domain.yd(i) = ydtmp ;

     zdtmp = domain.zd(i) + domain.zdd(i) * dt ;
     if( FABS(zdtmp) < u_cut ) zdtmp = Real_t(0.0);
     domain.zd(i) = zdtmp ;
   }
}

/******************************************/

static inline
void CalcPositionForNodes(Domain &domain, const Real_t dt, Index_t numNode)
{
   for ( Index_t i = 0 ; i < numNode ; ++i )
   {
     domain.x(i) += domain.xd(i) * dt ;
     domain.y(i) += domain.yd(i) * dt ;
     domain.z(i) += domain.zd(i) * dt ;
   }
}

/******************************************/

static inline
void LagrangeNodal(Domain& domain)
{
#ifdef SEDOV_SYNC_POS_VEL_EARLY
   Domain_member fieldData[6] ;
#endif

   const Real_t delt = domain.deltatime() ;
   Real_t u_cut = domain.u_cut() ;

  /* time of boundary condition evaluation is beginning of step for force and
   * acceleration boundary conditions. */
  CalcForceForNodes(domain);

#if USE_DISTRIBUTED  
#ifdef SEDOV_SYNC_POS_VEL_EARLY
   CommRecv(domain, MSG_SYNC_POS_VEL, 6,
            domain.sizeX() + 1, domain.sizeY() + 1, domain.sizeZ() + 1,
            false, false) ;
#endif
#endif
   
   CalcAccelerationForNodes(domain, domain.numNode());
   
   ApplyAccelerationBoundaryConditionsForNodes(domain);

   CalcVelocityForNodes( domain, delt, u_cut, domain.numNode()) ;

   CalcPositionForNodes( domain, delt, domain.numNode() );
#if USE_DISTRIBUTED
#ifdef SEDOV_SYNC_POS_VEL_EARLY
  fieldData[0] = &Domain::x ;
  fieldData[1] = &Domain::y ;
  fieldData[2] = &Domain::z ;
  fieldData[3] = &Domain::xd ;
  fieldData[4] = &Domain::yd ;
  fieldData[5] = &Domain::zd ;

   CommSend(domain, MSG_SYNC_POS_VEL, 6, fieldData,
            domain.sizeX() + 1, domain.sizeY() + 1, domain.sizeZ() + 1,
            false, false) ;
   CommSyncPosVel(domain) ;
#endif
#endif
   
  return;
}

/******************************************/

static inline
Real_t CalcElemVolume( const Real_t x0, const Real_t x1,
               const Real_t x2, const Real_t x3,
               const Real_t x4, const Real_t x5,
               const Real_t x6, const Real_t x7,
               const Real_t y0, const Real_t y1,
               const Real_t y2, const Real_t y3,
               const Real_t y4, const Real_t y5,
               const Real_t y6, const Real_t y7,
               const Real_t z0, const Real_t z1,
               const Real_t z2, const Real_t z3,
               const Real_t z4, const Real_t z5,
               const Real_t z6, const Real_t z7 )
{
  Real_t twelveth = Real_t(1.0)/Real_t(12.0);

  Real_t dx61 = x6 - x1;
  Real_t dy61 = y6 - y1;
  Real_t dz61 = z6 - z1;

  Real_t dx70 = x7 - x0;
  Real_t dy70 = y7 - y0;
  Real_t dz70 = z7 - z0;

  Real_t dx63 = x6 - x3;
  Real_t dy63 = y6 - y3;
  Real_t dz63 = z6 - z3;

  Real_t dx20 = x2 - x0;
  Real_t dy20 = y2 - y0;
  Real_t dz20 = z2 - z0;

  Real_t dx50 = x5 - x0;
  Real_t dy50 = y5 - y0;
  Real_t dz50 = z5 - z0;

  Real_t dx64 = x6 - x4;
  Real_t dy64 = y6 - y4;
  Real_t dz64 = z6 - z4;

  Real_t dx31 = x3 - x1;
  Real_t dy31 = y3 - y1;
  Real_t dz31 = z3 - z1;

  Real_t dx72 = x7 - x2;
  Real_t dy72 = y7 - y2;
  Real_t dz72 = z7 - z2;

  Real_t dx43 = x4 - x3;
  Real_t dy43 = y4 - y3;
  Real_t dz43 = z4 - z3;

  Real_t dx57 = x5 - x7;
  Real_t dy57 = y5 - y7;
  Real_t dz57 = z5 - z7;

  Real_t dx14 = x1 - x4;
  Real_t dy14 = y1 - y4;
  Real_t dz14 = z1 - z4;

  Real_t dx25 = x2 - x5;
  Real_t dy25 = y2 - y5;
  Real_t dz25 = z2 - z5;

#define TRIPLE_PRODUCT(x1, y1, z1, x2, y2, z2, x3, y3, z3) \
   ((x1)*((y2)*(z3) - (z2)*(y3)) + (x2)*((z1)*(y3) - (y1)*(z3)) + (x3)*((y1)*(z2) - (z1)*(y2)))

  Real_t volume =
    TRIPLE_PRODUCT(dx31 + dx72, dx63, dx20,
       dy31 + dy72, dy63, dy20,
       dz31 + dz72, dz63, dz20) +
    TRIPLE_PRODUCT(dx43 + dx57, dx64, dx70,
       dy43 + dy57, dy64, dy70,
       dz43 + dz57, dz64, dz70) +
    TRIPLE_PRODUCT(dx14 + dx25, dx61, dx50,
       dy14 + dy25, dy61, dy50,
       dz14 + dz25, dz61, dz50);

#undef TRIPLE_PRODUCT

  volume *= twelveth;

  return volume ;
}

/******************************************/

//inline
Real_t CalcElemVolume( const Real_t x[8], const Real_t y[8], const Real_t z[8] )
{
return CalcElemVolume( x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7],
                       y[0], y[1], y[2], y[3], y[4], y[5], y[6], y[7],
                       z[0], z[1], z[2], z[3], z[4], z[5], z[6], z[7]);
}

/******************************************/

static inline
Real_t AreaFace( const Real_t x0, const Real_t x1,
                 const Real_t x2, const Real_t x3,
                 const Real_t y0, const Real_t y1,
                 const Real_t y2, const Real_t y3,
                 const Real_t z0, const Real_t z1,
                 const Real_t z2, const Real_t z3)
{
   Real_t fx = (x2 - x0) - (x3 - x1);
   Real_t fy = (y2 - y0) - (y3 - y1);
   Real_t fz = (z2 - z0) - (z3 - z1);
   Real_t gx = (x2 - x0) + (x3 - x1);
   Real_t gy = (y2 - y0) + (y3 - y1);
   Real_t gz = (z2 - z0) + (z3 - z1);
   Real_t area =
      (fx * fx + fy * fy + fz * fz) *
      (gx * gx + gy * gy + gz * gz) -
      (fx * gx + fy * gy + fz * gz) *
      (fx * gx + fy * gy + fz * gz);
   return area ;
}

/******************************************/

static inline
Real_t CalcElemCharacteristicLength( const Real_t x[8],
                                     const Real_t y[8],
                                     const Real_t z[8],
                                     const Real_t volume)
{
   Real_t a, charLength = Real_t(0.0);

   a = AreaFace(x[0],x[1],x[2],x[3],
                y[0],y[1],y[2],y[3],
                z[0],z[1],z[2],z[3]) ;
   charLength = std::max(a,charLength) ;

   a = AreaFace(x[4],x[5],x[6],x[7],
                y[4],y[5],y[6],y[7],
                z[4],z[5],z[6],z[7]) ;
   charLength = std::max(a,charLength) ;

   a = AreaFace(x[0],x[1],x[5],x[4],
                y[0],y[1],y[5],y[4],
                z[0],z[1],z[5],z[4]) ;
   charLength = std::max(a,charLength) ;

   a = AreaFace(x[1],x[2],x[6],x[5],
                y[1],y[2],y[6],y[5],
                z[1],z[2],z[6],z[5]) ;
   charLength = std::max(a,charLength) ;

   a = AreaFace(x[2],x[3],x[7],x[6],
                y[2],y[3],y[7],y[6],
                z[2],z[3],z[7],z[6]) ;
   charLength = std::max(a,charLength) ;

   a = AreaFace(x[3],x[0],x[4],x[7],
                y[3],y[0],y[4],y[7],
                z[3],z[0],z[4],z[7]) ;
   charLength = std::max(a,charLength) ;

   charLength = Real_t(4.0) * volume / SQRT(charLength);

   return charLength;
}

/******************************************/

static inline
void CalcElemVelocityGradient( const Real_t* const xvel,
                                const Real_t* const yvel,
                                const Real_t* const zvel,
                                const Real_t b[][8],
                                const Real_t detJ,
                                Real_t* const d )
{
  const Real_t inv_detJ = Real_t(1.0) / detJ ;
  Real_t dyddx, dxddy, dzddx, dxddz, dzddy, dyddz;
  const Real_t* const pfx = b[0];
  const Real_t* const pfy = b[1];
  const Real_t* const pfz = b[2];

  d[0] = inv_detJ * ( pfx[0] * (xvel[0]-xvel[6])
                     + pfx[1] * (xvel[1]-xvel[7])
                     + pfx[2] * (xvel[2]-xvel[4])
                     + pfx[3] * (xvel[3]-xvel[5]) );

  d[1] = inv_detJ * ( pfy[0] * (yvel[0]-yvel[6])
                     + pfy[1] * (yvel[1]-yvel[7])
                     + pfy[2] * (yvel[2]-yvel[4])
                     + pfy[3] * (yvel[3]-yvel[5]) );

  d[2] = inv_detJ * ( pfz[0] * (zvel[0]-zvel[6])
                     + pfz[1] * (zvel[1]-zvel[7])
                     + pfz[2] * (zvel[2]-zvel[4])
                     + pfz[3] * (zvel[3]-zvel[5]) );

  dyddx  = inv_detJ * ( pfx[0] * (yvel[0]-yvel[6])
                      + pfx[1] * (yvel[1]-yvel[7])
                      + pfx[2] * (yvel[2]-yvel[4])
                      + pfx[3] * (yvel[3]-yvel[5]) );

  dxddy  = inv_detJ * ( pfy[0] * (xvel[0]-xvel[6])
                      + pfy[1] * (xvel[1]-xvel[7])
                      + pfy[2] * (xvel[2]-xvel[4])
                      + pfy[3] * (xvel[3]-xvel[5]) );

  dzddx  = inv_detJ * ( pfx[0] * (zvel[0]-zvel[6])
                      + pfx[1] * (zvel[1]-zvel[7])
                      + pfx[2] * (zvel[2]-zvel[4])
                      + pfx[3] * (zvel[3]-zvel[5]) );

  dxddz  = inv_detJ * ( pfz[0] * (xvel[0]-xvel[6])
                      + pfz[1] * (xvel[1]-xvel[7])
                      + pfz[2] * (xvel[2]-xvel[4])
                      + pfz[3] * (xvel[3]-xvel[5]) );

  dzddy  = inv_detJ * ( pfy[0] * (zvel[0]-zvel[6])
                      + pfy[1] * (zvel[1]-zvel[7])
                      + pfy[2] * (zvel[2]-zvel[4])
                      + pfy[3] * (zvel[3]-zvel[5]) );

  dyddz  = inv_detJ * ( pfz[0] * (yvel[0]-yvel[6])
                      + pfz[1] * (yvel[1]-yvel[7])
                      + pfz[2] * (yvel[2]-yvel[4])
                      + pfz[3] * (yvel[3]-yvel[5]) );
  d[5]  = Real_t( .5) * ( dxddy + dyddx );
  d[4]  = Real_t( .5) * ( dxddz + dzddx );
  d[3]  = Real_t( .5) * ( dzddy + dyddz );
}

/******************************************/

//static inline
void CalcKinematicsForElems( Domain &domain,
                             Real_t deltaTime, Index_t numElem )
{

  // loop over all elements
  for( Index_t k=0 ; k<numElem ; ++k )
  {
    Real_t B[3][8] ; /** shape function derivatives */
    Real_t D[6] ;
    Real_t x_local[8] ;
    Real_t y_local[8] ;
    Real_t z_local[8] ;
    Real_t xd_local[8] ;
    Real_t yd_local[8] ;
    Real_t zd_local[8] ;
    Real_t detJ = Real_t(0.0) ;

    Real_t volume ;
    Real_t relativeVolume ;
    const Index_t* const elemToNode = domain.nodelist(k) ;

    // get nodal coordinates from global arrays and copy into local arrays.
    CollectDomainNodesToElemNodes(domain, elemToNode, x_local, y_local, z_local);

    // volume calculations
    volume = CalcElemVolume(x_local, y_local, z_local );
    relativeVolume = volume / domain.volo(k) ;
    domain.vnew(k) = relativeVolume ;
    domain.delv(k) = relativeVolume - domain.v(k) ;

    // set characteristic length
    domain.arealg(k) = CalcElemCharacteristicLength(x_local, y_local, z_local,
                                             volume);

    // get nodal velocities from global array and copy into local arrays.
    for( Index_t lnode=0 ; lnode<8 ; ++lnode )
    {
      Index_t gnode = elemToNode[lnode];
      xd_local[lnode] = domain.xd(gnode);
      yd_local[lnode] = domain.yd(gnode);
      zd_local[lnode] = domain.zd(gnode);
    }

    Real_t dt2 = Real_t(0.5) * deltaTime;
    for ( Index_t j=0 ; j<8 ; ++j )
    {
       x_local[j] -= dt2 * xd_local[j];
       y_local[j] -= dt2 * yd_local[j];
       z_local[j] -= dt2 * zd_local[j];
    }

    CalcElemShapeFunctionDerivatives( x_local, y_local, z_local,
                                      B, &detJ );

    CalcElemVelocityGradient( xd_local, yd_local, zd_local,
                               B, detJ, D );

    // put velocity gradient quantities into their global arrays.
    domain.dxx(k) = D[0];
    domain.dyy(k) = D[1];
    domain.dzz(k) = D[2];
  }
}

/******************************************/

static inline
void CalcLagrangeElements(Domain& domain)
{
   Index_t numElem = domain.numElem() ;
   if (numElem > 0) {
      const Real_t deltatime = domain.deltatime() ;

      domain.AllocateStrains(numElem);

      CalcKinematicsForElems(domain, deltatime, numElem) ;

      // element loop to do some stuff not included in the elemlib function.
      for ( Index_t k=0 ; k<numElem ; ++k )
      {
         // calc strain rate and apply as constraint (only done in FB element)
         Real_t vdov = domain.dxx(k) + domain.dyy(k) + domain.dzz(k) ;
         Real_t vdovthird = vdov/Real_t(3.0) ;

         // make the rate of deformation tensor deviatoric
         domain.vdov(k) = vdov ;
         domain.dxx(k) -= vdovthird ;
         domain.dyy(k) -= vdovthird ;
         domain.dzz(k) -= vdovthird ;

        // See if any volumes are negative, and take appropriate action.
         if (domain.vnew(k) <= Real_t(0.0))
        {
#if USE_DISTRIBUTED           
           MPI_Abort(MPI_COMM_WORLD, VolumeError) ;
#else
           exit(VolumeError);
#endif
        }
      }
      domain.DeallocateStrains();
   }
}

/******************************************/

static inline
void CalcMonotonicQGradientsForElems(Domain& domain)
{
   Index_t numElem = domain.numElem();

   for (Index_t i = 0 ; i < numElem ; ++i ) {
      const Real_t ptiny = Real_t(1.e-36) ;
      Real_t ax,ay,az ;
      Real_t dxv,dyv,dzv ;

      const Index_t *elemToNode = domain.nodelist(i);
      Index_t n0 = elemToNode[0] ;
      Index_t n1 = elemToNode[1] ;
      Index_t n2 = elemToNode[2] ;
      Index_t n3 = elemToNode[3] ;
      Index_t n4 = elemToNode[4] ;
      Index_t n5 = elemToNode[5] ;
      Index_t n6 = elemToNode[6] ;
      Index_t n7 = elemToNode[7] ;

      Real_t x0 = domain.x(n0) ;
      Real_t x1 = domain.x(n1) ;
      Real_t x2 = domain.x(n2) ;
      Real_t x3 = domain.x(n3) ;
      Real_t x4 = domain.x(n4) ;
      Real_t x5 = domain.x(n5) ;
      Real_t x6 = domain.x(n6) ;
      Real_t x7 = domain.x(n7) ;

      Real_t y0 = domain.y(n0) ;
      Real_t y1 = domain.y(n1) ;
      Real_t y2 = domain.y(n2) ;
      Real_t y3 = domain.y(n3) ;
      Real_t y4 = domain.y(n4) ;
      Real_t y5 = domain.y(n5) ;
      Real_t y6 = domain.y(n6) ;
      Real_t y7 = domain.y(n7) ;

      Real_t z0 = domain.z(n0) ;
      Real_t z1 = domain.z(n1) ;
      Real_t z2 = domain.z(n2) ;
      Real_t z3 = domain.z(n3) ;
      Real_t z4 = domain.z(n4) ;
      Real_t z5 = domain.z(n5) ;
      Real_t z6 = domain.z(n6) ;
      Real_t z7 = domain.z(n7) ;

      Real_t xv0 = domain.xd(n0) ;
      Real_t xv1 = domain.xd(n1) ;
      Real_t xv2 = domain.xd(n2) ;
      Real_t xv3 = domain.xd(n3) ;
      Real_t xv4 = domain.xd(n4) ;
      Real_t xv5 = domain.xd(n5) ;
      Real_t xv6 = domain.xd(n6) ;
      Real_t xv7 = domain.xd(n7) ;

      Real_t yv0 = domain.yd(n0) ;
      Real_t yv1 = domain.yd(n1) ;
      Real_t yv2 = domain.yd(n2) ;
      Real_t yv3 = domain.yd(n3) ;
      Real_t yv4 = domain.yd(n4) ;
      Real_t yv5 = domain.yd(n5) ;
      Real_t yv6 = domain.yd(n6) ;
      Real_t yv7 = domain.yd(n7) ;

      Real_t zv0 = domain.zd(n0) ;
      Real_t zv1 = domain.zd(n1) ;
      Real_t zv2 = domain.zd(n2) ;
      Real_t zv3 = domain.zd(n3) ;
      Real_t zv4 = domain.zd(n4) ;
      Real_t zv5 = domain.zd(n5) ;
      Real_t zv6 = domain.zd(n6) ;
      Real_t zv7 = domain.zd(n7) ;

      Real_t vol = domain.volo(i)*domain.vnew(i) ;
      Real_t norm = Real_t(1.0) / ( vol + ptiny ) ;

      Real_t dxj = Real_t(-0.25)*((x0+x1+x5+x4) - (x3+x2+x6+x7)) ;
      Real_t dyj = Real_t(-0.25)*((y0+y1+y5+y4) - (y3+y2+y6+y7)) ;
      Real_t dzj = Real_t(-0.25)*((z0+z1+z5+z4) - (z3+z2+z6+z7)) ;

      Real_t dxi = Real_t( 0.25)*((x1+x2+x6+x5) - (x0+x3+x7+x4)) ;
      Real_t dyi = Real_t( 0.25)*((y1+y2+y6+y5) - (y0+y3+y7+y4)) ;
      Real_t dzi = Real_t( 0.25)*((z1+z2+z6+z5) - (z0+z3+z7+z4)) ;

      Real_t dxk = Real_t( 0.25)*((x4+x5+x6+x7) - (x0+x1+x2+x3)) ;
      Real_t dyk = Real_t( 0.25)*((y4+y5+y6+y7) - (y0+y1+y2+y3)) ;
      Real_t dzk = Real_t( 0.25)*((z4+z5+z6+z7) - (z0+z1+z2+z3)) ;

      /* find delvk and delxk ( i cross j ) */

      ax = dyi*dzj - dzi*dyj ;
      ay = dzi*dxj - dxi*dzj ;
      az = dxi*dyj - dyi*dxj ;

      domain.delx_zeta(i) = vol / SQRT(ax*ax + ay*ay + az*az + ptiny) ;

      ax *= norm ;
      ay *= norm ;
      az *= norm ;

      dxv = Real_t(0.25)*((xv4+xv5+xv6+xv7) - (xv0+xv1+xv2+xv3)) ;
      dyv = Real_t(0.25)*((yv4+yv5+yv6+yv7) - (yv0+yv1+yv2+yv3)) ;
      dzv = Real_t(0.25)*((zv4+zv5+zv6+zv7) - (zv0+zv1+zv2+zv3)) ;

      domain.delv_zeta(i) = ax*dxv + ay*dyv + az*dzv ;

      /* find delxi and delvi ( j cross k ) */

      ax = dyj*dzk - dzj*dyk ;
      ay = dzj*dxk - dxj*dzk ;
      az = dxj*dyk - dyj*dxk ;

      domain.delx_xi(i) = vol / SQRT(ax*ax + ay*ay + az*az + ptiny) ;

      ax *= norm ;
      ay *= norm ;
      az *= norm ;

      dxv = Real_t(0.25)*((xv1+xv2+xv6+xv5) - (xv0+xv3+xv7+xv4)) ;
      dyv = Real_t(0.25)*((yv1+yv2+yv6+yv5) - (yv0+yv3+yv7+yv4)) ;
      dzv = Real_t(0.25)*((zv1+zv2+zv6+zv5) - (zv0+zv3+zv7+zv4)) ;

      domain.delv_xi(i) = ax*dxv + ay*dyv + az*dzv ;

      /* find delxj and delvj ( k cross i ) */

      ax = dyk*dzi - dzk*dyi ;
      ay = dzk*dxi - dxk*dzi ;
      az = dxk*dyi - dyk*dxi ;

      domain.delx_eta(i) = vol / SQRT(ax*ax + ay*ay + az*az + ptiny) ;

      ax *= norm ;
      ay *= norm ;
      az *= norm ;

      dxv = Real_t(-0.25)*((xv0+xv1+xv5+xv4) - (xv3+xv2+xv6+xv7)) ;
      dyv = Real_t(-0.25)*((yv0+yv1+yv5+yv4) - (yv3+yv2+yv6+yv7)) ;
      dzv = Real_t(-0.25)*((zv0+zv1+zv5+zv4) - (zv3+zv2+zv6+zv7)) ;

      domain.delv_eta(i) = ax*dxv + ay*dyv + az*dzv ;
   }
}

/******************************************/

static inline
void CalcMonotonicQRegionForElems(Domain &domain, Int_t r,
                                  Real_t ptiny)
{
   Real_t monoq_limiter_mult = domain.monoq_limiter_mult();
   Real_t monoq_max_slope = domain.monoq_max_slope();
   Real_t qlc_monoq = domain.qlc_monoq();
   Real_t qqc_monoq = domain.qqc_monoq();

   for ( Index_t i = 0 ; i < domain.regElemSize(r); ++i ) {
      Index_t ielem = domain.regElemlist(r,i);
      Real_t qlin, qquad ;
      Real_t phixi, phieta, phizeta ;
      Int_t bcMask = domain.elemBC(ielem) ;
      Real_t delvm = 0.0, delvp =0.0;

      /*  phixi     */
      Real_t norm = Real_t(1.) / (domain.delv_xi(ielem)+ ptiny ) ;

      switch (bcMask & XI_M) {
         case XI_M_COMM: /* needs comm data */
         case 0:         delvm = domain.delv_xi(domain.lxim(ielem)); break ;
         case XI_M_SYMM: delvm = domain.delv_xi(ielem) ;       break ;
         case XI_M_FREE: delvm = Real_t(0.0) ;      break ;
         default:          fprintf(stderr, "Error in switch at %s line %d\n",
                                   __FILE__, __LINE__);
            delvm = 0; /* ERROR - but quiets the compiler */
            break;
      }
      switch (bcMask & XI_P) {
         case XI_P_COMM: /* needs comm data */
         case 0:         delvp = domain.delv_xi(domain.lxip(ielem)) ; break ;
         case XI_P_SYMM: delvp = domain.delv_xi(ielem) ;       break ;
         case XI_P_FREE: delvp = Real_t(0.0) ;      break ;
         default:          fprintf(stderr, "Error in switch at %s line %d\n",
                                   __FILE__, __LINE__);
            delvp = 0; /* ERROR - but quiets the compiler */
            break;
      }

      delvm = delvm * norm ;
      delvp = delvp * norm ;

      phixi = Real_t(.5) * ( delvm + delvp ) ;

      delvm *= monoq_limiter_mult ;
      delvp *= monoq_limiter_mult ;

      if ( delvm < phixi ) phixi = delvm ;
      if ( delvp < phixi ) phixi = delvp ;
      if ( phixi < Real_t(0.)) phixi = Real_t(0.) ;
      if ( phixi > monoq_max_slope) phixi = monoq_max_slope;


      /*  phieta     */
      norm = Real_t(1.) / ( domain.delv_eta(ielem) + ptiny ) ;

      switch (bcMask & ETA_M) {
         case ETA_M_COMM: /* needs comm data */
         case 0:          delvm = domain.delv_eta(domain.letam(ielem)) ; break ;
         case ETA_M_SYMM: delvm = domain.delv_eta(ielem) ;        break ;
         case ETA_M_FREE: delvm = Real_t(0.0) ;        break ;
         default:          fprintf(stderr, "Error in switch at %s line %d\n",
                                   __FILE__, __LINE__);
            delvm = 0; /* ERROR - but quiets the compiler */
            break;
      }
      switch (bcMask & ETA_P) {
         case ETA_P_COMM: /* needs comm data */
         case 0:          delvp = domain.delv_eta(domain.letap(ielem)) ; break ;
         case ETA_P_SYMM: delvp = domain.delv_eta(ielem) ;        break ;
         case ETA_P_FREE: delvp = Real_t(0.0) ;        break ;
         default:          fprintf(stderr, "Error in switch at %s line %d\n",
                                   __FILE__, __LINE__);
            delvp = 0; /* ERROR - but quiets the compiler */
            break;
      }

      delvm = delvm * norm ;
      delvp = delvp * norm ;

      phieta = Real_t(.5) * ( delvm + delvp ) ;

      delvm *= monoq_limiter_mult ;
      delvp *= monoq_limiter_mult ;

      if ( delvm  < phieta ) phieta = delvm ;
      if ( delvp  < phieta ) phieta = delvp ;
      if ( phieta < Real_t(0.)) phieta = Real_t(0.) ;
      if ( phieta > monoq_max_slope)  phieta = monoq_max_slope;

      /*  phizeta     */
      norm = Real_t(1.) / ( domain.delv_zeta(ielem) + ptiny ) ;

      switch (bcMask & ZETA_M) {
         case ZETA_M_COMM: /* needs comm data */
         case 0:           delvm = domain.delv_zeta(domain.lzetam(ielem)) ; break ;
         case ZETA_M_SYMM: delvm = domain.delv_zeta(ielem) ;         break ;
         case ZETA_M_FREE: delvm = Real_t(0.0) ;          break ;
         default:          fprintf(stderr, "Error in switch at %s line %d\n",
                                   __FILE__, __LINE__);
            delvm = 0; /* ERROR - but quiets the compiler */
            break;
      }
      switch (bcMask & ZETA_P) {
         case ZETA_P_COMM: /* needs comm data */
         case 0:           delvp = domain.delv_zeta(domain.lzetap(ielem)) ; break ;
         case ZETA_P_SYMM: delvp = domain.delv_zeta(ielem) ;         break ;
         case ZETA_P_FREE: delvp = Real_t(0.0) ;          break ;
         default:          fprintf(stderr, "Error in switch at %s line %d\n",
                                   __FILE__, __LINE__);
            delvp = 0; /* ERROR - but quiets the compiler */
            break;
      }

      delvm = delvm * norm ;
      delvp = delvp * norm ;

      phizeta = Real_t(.5) * ( delvm + delvp ) ;

      delvm *= monoq_limiter_mult ;
      delvp *= monoq_limiter_mult ;

      if ( delvm   < phizeta ) phizeta = delvm ;
      if ( delvp   < phizeta ) phizeta = delvp ;
      if ( phizeta < Real_t(0.)) phizeta = Real_t(0.);
      if ( phizeta > monoq_max_slope  ) phizeta = monoq_max_slope;

      /* Remove length scale */

      if ( domain.vdov(ielem) > Real_t(0.) )  {
         qlin  = Real_t(0.) ;
         qquad = Real_t(0.) ;
      }
      else {
         Real_t delvxxi   = domain.delv_xi(ielem)   * domain.delx_xi(ielem)   ;
         Real_t delvxeta  = domain.delv_eta(ielem)  * domain.delx_eta(ielem)  ;
         Real_t delvxzeta = domain.delv_zeta(ielem) * domain.delx_zeta(ielem) ;

         if ( delvxxi   > Real_t(0.) ) delvxxi   = Real_t(0.) ;
         if ( delvxeta  > Real_t(0.) ) delvxeta  = Real_t(0.) ;
         if ( delvxzeta > Real_t(0.) ) delvxzeta = Real_t(0.) ;

         Real_t rho = domain.elemMass(ielem) / (domain.volo(ielem) * domain.vnew(ielem)) ;

         qlin = -qlc_monoq * rho *
            (  delvxxi   * (Real_t(1.) - phixi) +
               delvxeta  * (Real_t(1.) - phieta) +
               delvxzeta * (Real_t(1.) - phizeta)  ) ;

         qquad = qqc_monoq * rho *
            (  delvxxi*delvxxi     * (Real_t(1.) - phixi*phixi) +
               delvxeta*delvxeta   * (Real_t(1.) - phieta*phieta) +
               delvxzeta*delvxzeta * (Real_t(1.) - phizeta*phizeta)  ) ;
      }

      domain.qq(ielem) = qquad ;
      domain.ql(ielem) = qlin  ;
   }
}

/******************************************/

static inline
void CalcMonotonicQForElems(Domain& domain)
{  
   //
   // initialize parameters
   // 
   const Real_t ptiny = Real_t(1.e-36) ;

   //
   // calculate the monotonic q for all regions
   //
   for (Index_t r=0 ; r<domain.numReg() ; ++r) {
      if (domain.regElemSize(r) > 0) {
         CalcMonotonicQRegionForElems(domain, r, ptiny) ;
      }
   }
}

/******************************************/

static inline
void CalcQForElems(Domain& domain)
{
   //
   // MONOTONIC Q option
   //

   Index_t numElem = domain.numElem() ;

   if (numElem != 0) {
      Int_t allElem = numElem +  /* local elem */
            2*domain.sizeX()*domain.sizeY() + /* plane ghosts */
            2*domain.sizeX()*domain.sizeZ() + /* row ghosts */
            2*domain.sizeY()*domain.sizeZ() ; /* col ghosts */

      domain.AllocateGradients(numElem, allElem);

#if USE_DISTRIBUTED      
      CommRecv(domain, MSG_MONOQ, 3,
               domain.sizeX(), domain.sizeY(), domain.sizeZ(),
               true, true) ;
#endif      

      /* Calculate velocity gradients */
      CalcMonotonicQGradientsForElems(domain);

#if USE_DISTRIBUTED      
      Domain_member fieldData[3] ;
      
      /* Transfer veloctiy gradients in the first order elements */
      /* problem->commElements->Transfer(CommElements::monoQ) ; */

      fieldData[0] = &Domain::delv_xi ;
      fieldData[1] = &Domain::delv_eta ;
      fieldData[2] = &Domain::delv_zeta ;

      CommSend(domain, MSG_MONOQ, 3, fieldData,
               domain.sizeX(), domain.sizeY(), domain.sizeZ(),
               true, true) ;

      CommMonoQ(domain) ;
#endif      

      CalcMonotonicQForElems(domain);

      // Free up memory
      domain.DeallocateGradients();

      /* Don't allow excessive artificial viscosity */
      Index_t idx = -1; 
      for (Index_t i=0; i<numElem; ++i) {
         if ( domain.q(i) > domain.qstop() ) {
            idx = i ;
            break ;
         }
      }

      if(idx >= 0) {
#if USE_DISTRIBUTED         
         MPI_Abort(MPI_COMM_WORLD, QStopError) ;
#else
         exit(QStopError);
#endif
      }
   }
}

/******************************************/

static inline
void CalcPressureForElems(Real_t* p_new, Real_t* bvc,
                          Real_t* pbvc, Real_t* e_old,
                          Real_t* compression, Real_t *vnewc,
                          Real_t pmin,
                          Real_t p_cut, Real_t eosvmax,
                          Index_t length, Index_t *regElemList)
{
   for (Index_t i = 0; i < length ; ++i) {
      Real_t c1s = Real_t(2.0)/Real_t(3.0) ;
      bvc[i] = c1s * (compression[i] + Real_t(1.));
      pbvc[i] = c1s;
   }

   for (Index_t i = 0 ; i < length ; ++i){
      Index_t ielem = regElemList[i];
      
      p_new[i] = bvc[i] * e_old[i] ;

      if    (FABS(p_new[i]) <  p_cut   )
         p_new[i] = Real_t(0.0) ;

      if    ( vnewc[ielem] >= eosvmax ) /* impossible condition here? */
         p_new[i] = Real_t(0.0) ;

      if    (p_new[i]       <  pmin)
         p_new[i]   = pmin ;
   }
}

/******************************************/

static inline
void CalcEnergyForElems(Real_t* p_new, Real_t* e_new, Real_t* q_new,
                        Real_t* bvc, Real_t* pbvc,
                        Real_t* p_old, Real_t* e_old, Real_t* q_old,
                        Real_t* compression, Real_t* compHalfStep,
                        Real_t* vnewc, Real_t* work, Real_t* delvc, Real_t pmin,
                        Real_t p_cut, Real_t  e_cut, Real_t q_cut, Real_t emin,
                        Real_t* qq_old, Real_t* ql_old,
                        Real_t rho0,
                        Real_t eosvmax,
                        Index_t length, Index_t *regElemList)
{
   Real_t *pHalfStep = Allocate<Real_t>(length) ;

   for (Index_t i = 0 ; i < length ; ++i) {
      e_new[i] = e_old[i] - Real_t(0.5) * delvc[i] * (p_old[i] + q_old[i])
         + Real_t(0.5) * work[i];

      if (e_new[i]  < emin ) {
         e_new[i] = emin ;
      }
   }

   CalcPressureForElems(pHalfStep, bvc, pbvc, e_new, compHalfStep, vnewc,
                        pmin, p_cut, eosvmax, length, regElemList);

   for (Index_t i = 0 ; i < length ; ++i) {
      Real_t vhalf = Real_t(1.) / (Real_t(1.) + compHalfStep[i]) ;

      if ( delvc[i] > Real_t(0.) ) {
         q_new[i] /* = qq_old[i] = ql_old[i] */ = Real_t(0.) ;
      }
      else {
         Real_t ssc = ( pbvc[i] * e_new[i]
                 + vhalf * vhalf * bvc[i] * pHalfStep[i] ) / rho0 ;

         if ( ssc <= Real_t(.1111111e-36) ) {
            ssc = Real_t(.3333333e-18) ;
         } else {
            ssc = SQRT(ssc) ;
         }

         q_new[i] = (ssc*ql_old[i] + qq_old[i]) ;
      }

      e_new[i] = e_new[i] + Real_t(0.5) * delvc[i]
         * (  Real_t(3.0)*(p_old[i]     + q_old[i])
              - Real_t(4.0)*(pHalfStep[i] + q_new[i])) ;
   }

   for (Index_t i = 0 ; i < length ; ++i) {

      e_new[i] += Real_t(0.5) * work[i];

      if (FABS(e_new[i]) < e_cut) {
         e_new[i] = Real_t(0.)  ;
      }
      if (     e_new[i]  < emin ) {
         e_new[i] = emin ;
      }
   }

   CalcPressureForElems(p_new, bvc, pbvc, e_new, compression, vnewc,
                        pmin, p_cut, eosvmax, length, regElemList);

   for (Index_t i = 0 ; i < length ; ++i){
      const Real_t sixth = Real_t(1.0) / Real_t(6.0) ;
      Index_t ielem = regElemList[i];
      Real_t q_tilde ;

      if (delvc[i] > Real_t(0.)) {
         q_tilde = Real_t(0.) ;
      }
      else {
         Real_t ssc = ( pbvc[i] * e_new[i]
                 + vnewc[ielem] * vnewc[ielem] * bvc[i] * p_new[i] ) / rho0 ;

         if ( ssc <= Real_t(.1111111e-36) ) {
            ssc = Real_t(.3333333e-18) ;
         } else {
            ssc = SQRT(ssc) ;
         }

         q_tilde = (ssc*ql_old[i] + qq_old[i]) ;
      }

      e_new[i] = e_new[i] - (  Real_t(7.0)*(p_old[i]     + q_old[i])
                               - Real_t(8.0)*(pHalfStep[i] + q_new[i])
                               + (p_new[i] + q_tilde)) * delvc[i]*sixth ;

      if (FABS(e_new[i]) < e_cut) {
         e_new[i] = Real_t(0.)  ;
      }
      if (     e_new[i]  < emin ) {
         e_new[i] = emin ;
      }
   }

   CalcPressureForElems(p_new, bvc, pbvc, e_new, compression, vnewc,
                        pmin, p_cut, eosvmax, length, regElemList);

   for (Index_t i = 0 ; i < length ; ++i){
      Index_t ielem = regElemList[i];

      if ( delvc[i] <= Real_t(0.) ) {
         Real_t ssc = ( pbvc[i] * e_new[i]
                 + vnewc[ielem] * vnewc[ielem] * bvc[i] * p_new[i] ) / rho0 ;

         if ( ssc <= Real_t(.1111111e-36) ) {
            ssc = Real_t(.3333333e-18) ;
         } else {
            ssc = SQRT(ssc) ;
         }

         q_new[i] = (ssc*ql_old[i] + qq_old[i]) ;

         if (FABS(q_new[i]) < q_cut) q_new[i] = Real_t(0.) ;
      }
   }

   Release(&pHalfStep) ;

   return ;
}

/******************************************/

static inline
void CalcSoundSpeedForElems(Domain &domain,
                            Real_t *vnewc, Real_t rho0, Real_t *enewc,
                            Real_t *pnewc, Real_t *pbvc,
                            Real_t *bvc, Real_t ss4o3,
                            Index_t len, Index_t *regElemList)
{
   for (Index_t i = 0; i < len ; ++i) {
      Index_t ielem = regElemList[i];
      Real_t ssTmp = (pbvc[i] * enewc[i] + vnewc[ielem] * vnewc[ielem] *
                 bvc[i] * pnewc[i]) / rho0;
      if (ssTmp <= Real_t(.1111111e-36)) {
         ssTmp = Real_t(.3333333e-18);
      }
      else {
         ssTmp = SQRT(ssTmp);
      }
      domain.ss(ielem) = ssTmp ;
   }
}

/******************************************/

static inline
void EvalEOSForElems(Domain& domain, Real_t *vnewc,
                     Int_t numElemReg, Index_t *regElemList, Int_t rep)
{
   Real_t  e_cut = domain.e_cut() ;
   Real_t  p_cut = domain.p_cut() ;
   Real_t  ss4o3 = domain.ss4o3() ;
   Real_t  q_cut = domain.q_cut() ;

   Real_t eosvmax = domain.eosvmax() ;
   Real_t eosvmin = domain.eosvmin() ;
   Real_t pmin    = domain.pmin() ;
   Real_t emin    = domain.emin() ;
   Real_t rho0    = domain.refdens() ;

   // These temporaries will be of different size for 
   // each call (due to different sized region element
   // lists)
   Real_t *e_old = Allocate<Real_t>(numElemReg) ;
   Real_t *delvc = Allocate<Real_t>(numElemReg) ;
   Real_t *p_old = Allocate<Real_t>(numElemReg) ;
   Real_t *q_old = Allocate<Real_t>(numElemReg) ;
   Real_t *compression = Allocate<Real_t>(numElemReg) ;
   Real_t *compHalfStep = Allocate<Real_t>(numElemReg) ;
   Real_t *qq_old = Allocate<Real_t>(numElemReg) ;
   Real_t *ql_old = Allocate<Real_t>(numElemReg) ;
   Real_t *work = Allocate<Real_t>(numElemReg) ;
   Real_t *p_new = Allocate<Real_t>(numElemReg) ;
   Real_t *e_new = Allocate<Real_t>(numElemReg) ;
   Real_t *q_new = Allocate<Real_t>(numElemReg) ;
   Real_t *bvc = Allocate<Real_t>(numElemReg) ;
   Real_t *pbvc = Allocate<Real_t>(numElemReg) ;
 
   //loop to add load imbalance based on region number 
   for(Int_t j = 0; j < rep; j++) {
      /* compress data, minimal set */
      {
         for (Index_t i=0; i<numElemReg; ++i) {
            Index_t ielem = regElemList[i];
            e_old[i] = domain.e(ielem) ;
            delvc[i] = domain.delv(ielem) ;
            p_old[i] = domain.p(ielem) ;
            q_old[i] = domain.q(ielem) ;
            qq_old[i] = domain.qq(ielem) ;
            ql_old[i] = domain.ql(ielem) ;
         }

         for (Index_t i = 0; i < numElemReg ; ++i) {
            Index_t ielem = regElemList[i];
            Real_t vchalf ;
            compression[i] = Real_t(1.) / vnewc[ielem] - Real_t(1.);
            vchalf = vnewc[ielem] - delvc[i] * Real_t(.5);
            compHalfStep[i] = Real_t(1.) / vchalf - Real_t(1.);
         }

      /* Check for v > eosvmax or v < eosvmin */
         if ( eosvmin != Real_t(0.) ) {
            for(Index_t i=0 ; i<numElemReg ; ++i) {
               Index_t ielem = regElemList[i];
               if (vnewc[ielem] <= eosvmin) { /* impossible due to calling func? */
                  compHalfStep[i] = compression[i] ;
               }
            }
         }
         if ( eosvmax != Real_t(0.) ) {
            for(Index_t i=0 ; i<numElemReg ; ++i) {
               Index_t ielem = regElemList[i];
               if (vnewc[ielem] >= eosvmax) { /* impossible due to calling func? */
                  p_old[i]        = Real_t(0.) ;
                  compression[i]  = Real_t(0.) ;
                  compHalfStep[i] = Real_t(0.) ;
               }
            }
         }

         for (Index_t i = 0 ; i < numElemReg ; ++i) {
            work[i] = Real_t(0.) ; 
         }
      }
      CalcEnergyForElems(p_new, e_new, q_new, bvc, pbvc,
                         p_old, e_old,  q_old, compression, compHalfStep,
                         vnewc, work,  delvc, pmin,
                         p_cut, e_cut, q_cut, emin,
                         qq_old, ql_old, rho0, eosvmax,
                         numElemReg, regElemList);
   }

   for (Index_t i=0; i<numElemReg; ++i) {
      Index_t ielem = regElemList[i];
      domain.p(ielem) = p_new[i] ;
      domain.e(ielem) = e_new[i] ;
      domain.q(ielem) = q_new[i] ;
   }

   CalcSoundSpeedForElems(domain,
                          vnewc, rho0, e_new, p_new,
                          pbvc, bvc, ss4o3,
                          numElemReg, regElemList) ;

   Release(&pbvc) ;
   Release(&bvc) ;
   Release(&q_new) ;
   Release(&e_new) ;
   Release(&p_new) ;
   Release(&work) ;
   Release(&ql_old) ;
   Release(&qq_old) ;
   Release(&compHalfStep) ;
   Release(&compression) ;
   Release(&q_old) ;
   Release(&p_old) ;
   Release(&delvc) ;
   Release(&e_old) ;
}

/******************************************/

static inline
void ApplyMaterialPropertiesForElems(Domain& domain)
{
   Index_t numElem = domain.numElem() ;

  if (numElem != 0) {
    /* Expose all of the variables needed for material evaluation */
    Real_t eosvmin = domain.eosvmin() ;
    Real_t eosvmax = domain.eosvmax() ;
    Real_t *vnewc = Allocate<Real_t>(numElem) ;

    {
       for(Index_t i=0 ; i<numElem ; ++i) {
          vnewc[i] = domain.vnew(i) ;
       }

       // Bound the updated relative volumes with eosvmin/max
       if (eosvmin != Real_t(0.)) {
          for(Index_t i=0 ; i<numElem ; ++i) {
             if (vnewc[i] < eosvmin)
                vnewc[i] = eosvmin ;
          }
       }

       if (eosvmax != Real_t(0.)) {
          for(Index_t i=0 ; i<numElem ; ++i) {
             if (vnewc[i] > eosvmax)
                vnewc[i] = eosvmax ;
          }
       }

       // This check may not make perfect sense in LULESH, but
       // it's representative of something in the full code -
       // just leave it in, please
       for (Index_t i=0; i<numElem; ++i) {
          Real_t vc = domain.v(i) ;
          if (eosvmin != Real_t(0.)) {
             if (vc < eosvmin)
                vc = eosvmin ;
          }
          if (eosvmax != Real_t(0.)) {
             if (vc > eosvmax)
                vc = eosvmax ;
          }
          if (vc <= 0.) {
#if USE_DISTRIBUTED
             MPI_Abort(MPI_COMM_WORLD, VolumeError) ;
#else
             exit(VolumeError);
#endif
          }
       }
    }

    for (Int_t r=0 ; r<domain.numReg() ; r++) {
       Index_t numElemReg = domain.regElemSize(r);
       Index_t *regElemList = domain.regElemlist(r);
       Int_t rep;
       //Determine load imbalance for this region
       //round down the number with lowest cost
       if(r < domain.numReg()/2)
	 rep = 1;
       //you don't get an expensive region unless you at least have 5 regions
       else if(r < (domain.numReg() - (domain.numReg()+15)/20))
         rep = 1 + domain.cost();
       //very expensive regions
       else
	 rep = 10 * (1+ domain.cost());
       EvalEOSForElems(domain, vnewc, numElemReg, regElemList, rep);
    }

    Release(&vnewc) ;
  }
}

/******************************************/

static inline
void UpdateVolumesForElems(Domain &domain,
                           Real_t v_cut, Index_t length)
{
   if (length != 0) {
      for(Index_t i=0 ; i<length ; ++i) {
         Real_t tmpV = domain.vnew(i) ;

         if ( FABS(tmpV - Real_t(1.0)) < v_cut )
            tmpV = Real_t(1.0) ;

         domain.v(i) = tmpV ;
      }
   }

   return ;
}

/******************************************/

static inline
void LagrangeElements(Domain& domain, Index_t numElem)
{
  CalcLagrangeElements(domain) ;

  /* Calculate Q.  (Monotonic q option requires communication) */
  CalcQForElems(domain) ;

  ApplyMaterialPropertiesForElems(domain) ;

  UpdateVolumesForElems(domain, 
                        domain.v_cut(), numElem) ;
}

/******************************************/

static inline
void CalcCourantConstraintForElems(Domain &domain, Index_t length,
                                   Index_t *regElemlist,
                                   Real_t qqc, Real_t& dtcourant)
{
#if _OPENMP
   const Index_t threads = omp_get_max_threads();
   Index_t courant_elem_per_thread[threads];
   Real_t dtcourant_per_thread[threads];
#else
   Index_t threads = 1;
   Index_t courant_elem_per_thread[1];
   Real_t  dtcourant_per_thread[1];
#endif

   {
      Real_t   qqc2 = Real_t(64.0) * qqc * qqc ;
      Real_t   dtcourant_tmp = dtcourant;
      Index_t  courant_elem  = -1 ;

#if _OPENMP
      Index_t thread_num = omp_get_thread_num();
#else
      Index_t thread_num = 0;
#endif      

      for (Index_t i = 0 ; i < length ; ++i) {
         Index_t indx = regElemlist[i] ;
         Real_t dtf = domain.ss(indx) * domain.ss(indx) ;

         if ( domain.vdov(indx) < Real_t(0.) ) {
            dtf = dtf
                + qqc2 * domain.arealg(indx) * domain.arealg(indx)
                * domain.vdov(indx) * domain.vdov(indx) ;
         }

         dtf = SQRT(dtf) ;
         dtf = domain.arealg(indx) / dtf ;

         if (domain.vdov(indx) != Real_t(0.)) {
            if ( dtf < dtcourant_tmp ) {
               dtcourant_tmp = dtf ;
               courant_elem  = indx ;
            }
         }
      }

      dtcourant_per_thread[thread_num]    = dtcourant_tmp ;
      courant_elem_per_thread[thread_num] = courant_elem ;
   }

   for (Index_t i = 1; i < threads; ++i) {
      if (dtcourant_per_thread[i] < dtcourant_per_thread[0] ) {
         dtcourant_per_thread[0]    = dtcourant_per_thread[i];
         courant_elem_per_thread[0] = courant_elem_per_thread[i];
      }
   }

   if (courant_elem_per_thread[0] != -1) {
      dtcourant = dtcourant_per_thread[0] ;
   }

   return ;

}

/******************************************/

static inline
void CalcHydroConstraintForElems(Domain &domain, Index_t length,
                                 Index_t *regElemlist, Real_t dvovmax, Real_t& dthydro)
{
#if _OPENMP
   const Index_t threads = omp_get_max_threads();
   Index_t hydro_elem_per_thread[threads];
   Real_t dthydro_per_thread[threads];
#else
   Index_t threads = 1;
   Index_t hydro_elem_per_thread[1];
   Real_t  dthydro_per_thread[1];
#endif

   {
      Real_t dthydro_tmp = dthydro ;
      Index_t hydro_elem = -1 ;

#if _OPENMP
      Index_t thread_num = omp_get_thread_num();
#else      
      Index_t thread_num = 0;
#endif      

      for (Index_t i = 0 ; i < length ; ++i) {
         Index_t indx = regElemlist[i] ;

         if (domain.vdov(indx) != Real_t(0.)) {
            Real_t dtdvov = dvovmax / (FABS(domain.vdov(indx))+Real_t(1.e-20)) ;

            if ( dthydro_tmp > dtdvov ) {
                  dthydro_tmp = dtdvov ;
                  hydro_elem = indx ;
            }
         }
      }

      dthydro_per_thread[thread_num]    = dthydro_tmp ;
      hydro_elem_per_thread[thread_num] = hydro_elem ;
   }

   for (Index_t i = 1; i < threads; ++i) {
      if(dthydro_per_thread[i] < dthydro_per_thread[0]) {
         dthydro_per_thread[0]    = dthydro_per_thread[i];
         hydro_elem_per_thread[0] =  hydro_elem_per_thread[i];
      }
   }

   if (hydro_elem_per_thread[0] != -1) {
      dthydro =  dthydro_per_thread[0] ;
   }

   return ;
}

/******************************************/

static inline
void CalcTimeConstraintsForElems(Domain& domain) {

   // Initialize conditions to a very large value
   domain.dtcourant() = 1.0e+20;
   domain.dthydro() = 1.0e+20;

   for (Index_t r=0 ; r < domain.numReg() ; ++r) {
      /* evaluate time constraint */
      CalcCourantConstraintForElems(domain, domain.regElemSize(r),
                                    domain.regElemlist(r),
                                    domain.qqc(),
                                    domain.dtcourant()) ;

      /* check hydro constraint */
      CalcHydroConstraintForElems(domain, domain.regElemSize(r),
                                  domain.regElemlist(r),
                                  domain.dvovmax(),
                                  domain.dthydro()) ;
   }
}

/******************************************/

static inline
void LagrangeLeapFrog(Domain& domain)
{
#ifdef SEDOV_SYNC_POS_VEL_LATE
   Domain_member fieldData[6] ;
#endif

   /* calculate nodal forces, accelerations, velocities, positions, with
    * applied boundary conditions and slide surface considerations */
   LagrangeNodal(domain);


#ifdef SEDOV_SYNC_POS_VEL_LATE
#endif

   /* calculate element quantities (i.e. velocity gradient & q), and update
    * material states */
   LagrangeElements(domain, domain.numElem());

#if USE_DISTRIBUTED   
#ifdef SEDOV_SYNC_POS_VEL_LATE
   CommRecv(domain, MSG_SYNC_POS_VEL, 6,
            domain.sizeX() + 1, domain.sizeY() + 1, domain.sizeZ() + 1,
            false, false) ;

   fieldData[0] = &Domain::x ;
   fieldData[1] = &Domain::y ;
   fieldData[2] = &Domain::z ;
   fieldData[3] = &Domain::xd ;
   fieldData[4] = &Domain::yd ;
   fieldData[5] = &Domain::zd ;
   
   CommSend(domain, MSG_SYNC_POS_VEL, 6, fieldData,
            domain.sizeX() + 1, domain.sizeY() + 1, domain.sizeZ() + 1,
            false, false) ;
#endif
#endif   

   CalcTimeConstraintsForElems(domain);

#if USE_DISTRIBUTED   
#ifdef SEDOV_SYNC_POS_VEL_LATE
   CommSyncPosVel(domain) ;
#endif
#endif   
}



// ===================== example main =====================

namespace {

// Plane-0 symmetry of the energy field, the check upstream prints as MaxRelDiff: the Sedov
// problem is symmetric in the mesh's first plane, so e(j,k) and e(k,j) must agree to rounding
// (~1e-14 on a correct run). Same loop as VerifyAndWriteFinalOutput, returned instead of printed.
double plane0_max_rel_diff(Domain &domain, Int_t nx) {
    double max_rel = 0.0;
    for (Index_t j = 0; j < nx; ++j) {
        for (Index_t k = j + 1; k < nx; ++k) {
            const double abs_diff = std::fabs(double(domain.e(j * nx + k)) - double(domain.e(k * nx + j)));
            const double rel = abs_diff / double(domain.e(k * nx + j));
            if (rel > max_rel)
                max_rel = rel;
        }
    }
    return max_rel;
}

bool is_perfect_cube(int n, int &side) {
    side = static_cast<int>(std::cbrt(static_cast<double>(n)) + 0.5);
    return side * side * side == n;
}

} // namespace

int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    fmi_examples::Flags flags;
    flags.add_int32("size", 30, "Edge length of every rank's cube mesh (upstream -s)");
    flags.add_int32("iters", 9999999, "Cycle limit; the default runs the problem to completion (upstream -i)");
    flags.add_int32("regions", 11, "Number of distinct material regions (upstream -r)");
    flags.add_int32("balance", 1, "Load balance between the regions of a domain (upstream -b)");
    flags.add_int32("cost", 1, "Extra cost of the more expensive regions (upstream -c)");
    flags.add_int32("progress", 0, "1 prints rank 0's cycle, time and dt every cycle (upstream -p)");
    flags.add_double("expected-energy", 0.0,
                     "Final origin energy rank 0 must reproduce to 1e-6 relative; 0 skips the check "
                     "(8 ranks at --size 30 run to completion: 7.130703e+05)");

    fmi_examples::Options opts;
    opts.ranks = 8; // the smallest multi-rank perfect cube; the launcher's usual default of 4 is not one

    int exit_code = 0;
    if (!fmi_examples::parse_cli(argc, argv, "lulesh", flags, opts, exit_code)) {
        return exit_code;
    }

    const int nx = opts.flags.get_int("size");
    const int its = opts.flags.get_int("iters");
    const int num_reg = opts.flags.get_int("regions");
    const int balance = opts.flags.get_int("balance");
    const int cost = opts.flags.get_int("cost");
    const int progress = opts.flags.get_int("progress");
    const double expected_energy = opts.flags.get_double("expected-energy");

    // LULESH decomposes the problem onto a cubic grid of ranks, so the count must be a perfect
    // cube. Checked before the launcher branch so an explicit --rank process rejects it too.
    int side = 0;
    if (!is_perfect_cube(opts.ranks, side)) {
        std::cerr << "lulesh requires a perfect-cube rank count (1, 8, 27, 64); got " << opts.ranks << std::endl;
        return 2;
    }
    if (nx < 1 || its < 1 || num_reg < 1) {
        std::cerr << "lulesh requires --size, --iters and --regions to be at least 1" << std::endl;
        return 2;
    }
    if (balance < 0 || cost < 0) {
        std::cerr << "lulesh requires --balance and --cost to be non-negative" << std::endl;
        return 2;
    }
    if (progress != 0 && progress != 1) {
        std::cerr << "lulesh requires --progress to be 0 or 1" << std::endl;
        return 2;
    }

    if (!opts.single_rank()) {
        return fmi_examples::spawn_local(argc, argv, opts);
    }

    const int rank = opts.rank;
    const int size = opts.ranks;
    bool ok = true;

    try {
        FMI::Communicator comm(rank, size, fmi_examples::config_path(), fmi_examples::comm_name(),
                               fmi_examples::faas_memory());
        lulesh_fmi::bind(comm, rank, size);

        std::cout << "rank " << rank << ": established communicator, running LULESH on a " << nx << "^3 mesh per rank ("
                  << side << "^3 ranks), cycle limit " << its << std::endl;

        // ---- upstream main(), minus MPI_Init/Finalize and the argv parser ----
        if (rank == 0) {
            std::cout << "Running problem size " << nx << "^3 per domain until completion\n";
            std::cout << "Num processors: " << size << "\n";
            std::cout << "Total number of elements: " << ((Int8_t)size * nx * nx * nx) << " \n\n";
        }

        // Set up the mesh and decompose. Assumes regular cubes for now
        Int_t col, row, plane, mesh_side;
        InitMeshDecomp(size, rank, &col, &row, &plane, &mesh_side);

        // Build the main data structure and initialize it
        Domain *locDom = new Domain(size, col, row, plane, nx, mesh_side, num_reg, balance, cost);

        Domain_member fieldData = &Domain::nodalMass;

        // Initial domain boundary communication
        CommRecv(*locDom, MSG_COMM_SBN, 1, locDom->sizeX() + 1, locDom->sizeY() + 1, locDom->sizeZ() + 1, true, false);
        CommSend(*locDom, MSG_COMM_SBN, 1, &fieldData, locDom->sizeX() + 1, locDom->sizeY() + 1, locDom->sizeZ() + 1,
                 true, false);
        CommSBN(*locDom, 1, &fieldData);

        // End initialization
        MPI_Barrier(MPI_COMM_WORLD);

        // BEGIN timestep to solution
        double start = MPI_Wtime();
        while ((locDom->time() < locDom->stoptime()) && (locDom->cycle() < its)) {
            TimeIncrement(*locDom);
            LagrangeLeapFrog(*locDom);

            if (progress && rank == 0) {
                std::cout << "cycle = " << locDom->cycle() << ", " << std::scientific << "time = " << double(locDom->time())
                          << ", " << "dt=" << double(locDom->deltatime()) << "\n";
                std::cout.unsetf(std::ios_base::floatfield);
            }
        }

        // Use reduced max elapsed time
        double elapsed_time = MPI_Wtime() - start;
        double elapsed_timeG = 0.0;
        MPI_Reduce(&elapsed_time, &elapsed_timeG, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

        if (rank == 0) {
            VerifyAndWriteFinalOutput(elapsed_timeG, *locDom, nx, size);
        }
        // ---- end of upstream main() ----

        const double cycles = static_cast<double>(locDom->cycle());
        const double final_time = static_cast<double>(locDom->time());
        const double origin_energy = static_cast<double>(locDom->e(0));

        // Every rank: the run advanced and left a finite energy at the origin element.
        if (locDom->cycle() < 1 || !std::isfinite(origin_energy)) {
            std::cout << "rank " << rank << " finished after " << locDom->cycle() << " cycles with origin energy "
                      << origin_energy << std::endl;
            ok = false;
        }
        if (rank == 0) {
            const double max_rel = plane0_max_rel_diff(*locDom, nx);
            if (!(max_rel < 1e-8)) {
                std::cout << "rank 0: plane-0 energy symmetry broken, MaxRelDiff = " << max_rel << std::endl;
                ok = false;
            }
            // The origin energy is bit-deterministic for a given (binary, size, rank count), so
            // with --expected-energy the run is held to a known value.
            if (expected_energy != 0.0) {
                const double rel = std::fabs(origin_energy - expected_energy) / std::fabs(expected_energy);
                if (!(rel <= 1e-6)) {
                    std::cout << "rank 0: final origin energy " << std::scientific << std::setprecision(6)
                              << origin_energy << " does not match expected " << expected_energy << std::endl;
                    std::cout.unsetf(std::ios_base::floatfield);
                    ok = false;
                }
            }
        }

        delete locDom;

        // The cycle count and the simulated time come out of the per-cycle dt allreduce, so every
        // rank must report the same pair. Gather them on rank 0 and compare against rank 0's own.
        std::vector<double> summary = {cycles, final_time};
        FMI::Comm::Data<std::vector<double>> senddata(summary);
        FMI::Comm::Data<std::vector<double>> recvdata(static_cast<std::size_t>(2 * size));
        comm.gather(senddata, recvdata, 0);
        if (rank == 0) {
            std::vector<double> gathered = recvdata.get();
            for (int p = 1; p < size; p++) {
                ok &= compare<double>(rank, "cycle count of rank " + std::to_string(p), gathered[0], gathered[2 * p]);
                ok &= compare<double>(rank, "final time of rank " + std::to_string(p), gathered[1], gathered[2 * p + 1]);
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
