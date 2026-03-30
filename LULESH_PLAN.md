# Port LULESH to FMI via a Native FMI Communication Layer

## Summary
Yes, this is feasible, but only as a targeted port of the MPI communication layer, not as a drop-in rebuild. The practical path is to keep LULESH's decomposition and compute kernels intact, replace the MPI code in `lulesh-comm.cc` with an FMI-backed adapter, and bring it up first on FMI's `Direct` backend.

The key reason this is viable is that LULESH's parallelism is mostly:
- nearest-neighbor halo exchange for boundary data
- one global timestep reduction each cycle

That maps onto FMI's existing blocking `send`/`recv`/`allreduce` surface in `/home/luca/fmi-original/fmi/include/Communicator.h`. The main mismatch is that LULESH's MPI code uses rank/world semantics and nonblocking message orchestration, while FMI currently exposes only blocking, untagged operations and no MPI topology/runtime layer.

## Implementation Changes
- Add a `USE_FMI` build path in LULESH alongside existing serial/OpenMP/MPI modes.
- Replace the MPI-specific code in `lulesh-comm.cc` with an FMI adapter class, for example `LuleshFmiComm`, responsible for:
  - communicator construction from `rank`, `nranks`, FMI config path, communicator name, and memory hint
  - deterministic point-to-point halo exchange
  - global reductions for timestep constraints
  - startup/shutdown barriers only where already required by LULESH logic
- Keep LULESH's current domain decomposition and neighbor tables unchanged. Do not redesign the mesh partitioning; only swap the transport.
- Marshal all halo buffers as contiguous byte ranges using `FMI::Comm::Data<void*>` from `/home/luca/fmi-original/fmi/include/comm/Data.h`. This avoids rewriting LULESH data structures into FMI container types.
- Convert each current MPI exchange phase into an explicit ordered FMI phase:
  - pack face/edge/corner buffers
  - perform blocking `send`/`recv` in a fixed peer order
  - unpack into ghost zones
- Remove dependence on MPI tags by making each communication phase globally ordered and unique. FMI's client-server backends key messages only by communicator name, peers, and per-peer operation counters in `/home/luca/fmi-original/fmi/src/comm/ClientServer.cpp`, so correctness depends on identical call order across ranks.
- Replace the global timestep reduction with FMI `allreduce`:
  - one `min` reduction for the courant constraint
  - one `min` reduction for the hydro constraint, or a single vector reduction if you want to preserve one call site
- Keep visualization and serial/OpenMP code paths unchanged unless they directly depend on MPI symbols.
- Add a small launcher contract outside LULESH to provide:
  - `FMI_RANK`
  - `FMI_SIZE`
  - `FMI_CONFIG`
  - `FMI_COMM_NAME`
  LULESH should not try to create peers itself; FMI assumes peers already exist.

## Public Interfaces / Build Surface
- New LULESH compile flag: `USE_FMI`
- New LULESH runtime arguments or env-based init for FMI bootstrap:
  - FMI config file path
  - communicator name
  - optional memory size hint for FMI policy selection
- No FMI public API changes are strictly required for v1 if the port uses:
  - blocking `send` / `recv`
  - `allreduce`
  - raw contiguous buffers via `Data<void*>`

If performance tuning becomes necessary later, the first FMI extension to consider is tagged or phased point-to-point sends, not new collectives.

## Test Plan
- Build LULESH in serial mode and `USE_FMI` single-rank mode; verify identical output and cycle progression.
- Run `USE_FMI` with 2, 4, and 8 ranks on the `Direct` backend; verify:
  - completion without deadlock
  - same iteration count for fixed inputs
  - acceptable numerical agreement with MPI LULESH output
- Add focused communication tests for:
  - face-only neighbors
  - edge/corner neighbors
  - ranks with missing neighbors at domain boundaries
  - non-power-of-two rank counts if LULESH permits them
- Stress deterministic ordering by running repeated same-size jobs with the same communicator name pattern and confirming stable results.
- Defer S3/Redis validation until after `Direct` is correct; they are likely to be functionally correct but poor fits for LULESH's fine-grained halo traffic.

## Assumptions
- Recommended target is a native FMI port, not an MPI shim.
- First supported backend should be `Direct`. This is an inference from FMI's current implementation: the S3/Redis-style client-server paths poll for objects/keys and are optimized for storage-backed collectives, not tight iterative halos.
- The LULESH codebase continues to isolate transport concerns mainly in `lulesh-comm.cc`, as described in the upstream README.
- It is acceptable for the FMI version to preserve correctness first and give up MPI-style nonblocking overlap in v1.
