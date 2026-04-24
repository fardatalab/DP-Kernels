# DP-Kernels

dpKernels are new computing pritmitives that harvest DPU compute resources for data-path efficiency in cloud data processing.

## Building and running the code

#### Machine setup

As described in the paper, dpKernels runs on the DPU, so it requires an appropriate machine and environment for compilation. Detailed setup may vary; for example, a good starting point for a BlueField-2 or -3 DPU is NVIDIA's official DOCA installation documentation (https://docs.nvidia.com/doca/sdk/doca-installation-guide-for-linux/index.html).

Our setup involves two dedicated machines, one as the client and one as the server, connected by 100 Gbps network links. Each machine is a Dell PowerEdge R7625 server equipped with an AMD EPYC CPU (12 cores @ 2.90 GHz), 128 GB DDR5 memory, and 2 TB NVMe SSD storage. The server machine is additionally equipped with a DPU (NVIDIA BlueField-2, NVIDIA BlueField-3, and AMD Pensando Elba).

#### Basic build flow

##### Main repository

The main dpKernels and dpManager code is built from the repository root with Meson. Some dependencies can be installed with `dep.sh`. Then run:

```sh
meson setup build
meson compile -C build
```

The resulting `dp_manager` and test/client binaries are under `build/`. On the DPU, `dp_manager` should be started before applications/clients; for example, `run.sh` starts the dpManager process. Relevant kernels for that platform should be automatically compiled and loaded by dpManager on startup.

##### Evaluation and applications

The `Apps/` directory contains integrations and applications used for evaluation. They run on the host and/or DPU, depending on the application.

###### Analytical Database Service

`DuckDB/` includes a fork of upstream DuckDB that integrates dpKernels into the parquet extension through a custom data-read path backed by the `DDS` storage engine.

First build the root DP-Kernels tree so the DDS DPU backend can
link against the embedded runtime library, then configure DuckDB-DPK with the
DDS POSIX frontend enabled. For example:

```sh
# From the DP-Kernels repository root.
meson setup build
meson compile -C build

# Build modified DuckDB with the sibling DDS storage engine enabled.
cd Apps/DuckDB/DuckDB-DPK
cmake -S . -B build/release \
  -DCMAKE_BUILD_TYPE=Release \
  -DDUCKDB_USE_DDS_POSIX=ON \
  -DDUCKDB_DDS_ROOT="$PWD/../DDS"
cmake --build build/release -j
```

The dpKernels-integrated DDS storage engine is split into the host frontend and the DPU backend. The main code is under `Apps/DuckDB/DDS/StorageEngine/`. The DPU backend is built from
`Apps/DuckDB/DDS/StorageEngine/DDSBackEndDPUService` after dpKernels is built:

```bash
cd Apps/DuckDB/DDS/StorageEngine/DDSBackEndDPUService
meson setup build
meson compile -C build
```

To run the TPC-H benchmark flow, the scripts under `Apps/DuckDB/DuckDB-DPK/` are the main entry points:

```bash
cd Apps/DuckDB/DuckDB-DPK

# Generate encrypted/compressed TPC-H parquet files using DuckDB's dbgen.
# This writes directories such as tpch_parquet_new_SF<scale-factor>.
./gendb.sh

# Create the persistent DuckDB metadata database and views over the parquet files.
# Check PARQUET_DIR inside the script; it should match the generated parquet path.
./setup_parquet.sh

# After the DDS backend/frontend side is running, load parquet files into DDS.
# DDS_PARQUET_DIR should point at the parquet directory generated above.
DDS_PARQUET_DIR=./tpch_parquet_new_SF50 ./build/release/tools/dds_parquet_loader

# Run the 22 TPC-H query files and emit per-query JSON profiles.
# Check QUERY_DIR inside the script; it is machine-specific in the evaluation setup.
./run_tpch.sh
```

`gendb.sh` is the TPC-H parquet data generation script, `setup_parquet.sh` creates the `tpch_metadata.db` view database used by the benchmark, and `run_tpch.sh` runs the TPC-H query files
against that database with profiling enabled.



###### In-memory Key-value Cache

`Faster/` contains the FASTER evaluation app. The local build has two layers:
`build-faster.sh` builds the upstream FASTER library
under `Faster/FASTER/cc/build/Release`, and `meson.build`
then builds the app executables `faster_server`, `faster_client`, and
`faster_dpu_app`. When running, start the DPU app first. Then start `build/faster_server` to populate/read an in-memory FASTER store, and run `build/faster_client` for the RDMA client.

For example:

```bash
cd Apps/Faster
./build-faster.sh
meson setup build
meson compile -C build
```


###### MessageExchange

`MessageExchange/` contains the shared-memory ring-buffer benchmark and the lock-based
baseline used for comparison.

`MessageExchange/FarRing/` is the lock-free ring with spaced backoff.

Build with CMake from `MessageExchange/SpinLockBuffer`, then run the generated `spinlock-buffer`
executable:

```bash
cd Apps/MessageExchange/SpinLockBuffer
cmake -S . -B build
cmake --build build
./build/spinlock-buffer
```



###### Optimizations and Scheduling

These optimizations are not standalone applications; the relevant knobs are within the codebase, such as the corresponding `#define`s in `Common/common.hpp`, for example `#define SCHEDULING`. Update and recompile as needed for various setups.



## Structure of the repo

```text
.
|-- Apps/                    # Evaluation applications and integration.
|   |-- DuckDB/              # DuckDB-DDS-DPK analytical database evaluation.
|   |-- Faster/              # FASTER key-value cache evaluation.
|   `-- MessageExchange/     # Shared-memory ring-buffer benchmarks and baseline.
|-- Common/                  # Shared types, utilities, and feature `#define`s.
|-- DPManager/               # dpManager runtime, memory management, and scheduling etc.
|   `-- device/              # Per-platform device backends.
|-- Kernels/                 # dpKernel implementations by target platform.
|
`-- test/                    # tests and microbenchmarks.
    `-- page_server/         # Page-server that was included in the pre-revision manuscript.
```

### Core implementation
The core two-level abstraction proposed in the paper, dpKernels and dpManager, is implemented in:
 - **dpKernels**: `Kernels/`. This contains the kernel implementations for all DPU platforms involved in the paper, each in its own directory.
 - **dpManager**: `DPManager/`. This contains the dpManager implementation, including optimizations discussed in the paper, e.g., scheduling and coalescing. DPManager also depends on shared code from `Common/`.

### Integration and Evaluation

All relevant integration and evaluation applications are under the `Apps/` dir.

- `Faster/`: FASTER key-value store evaluation application.
- `DuckDB/`: DuckDB with dpKernels integration for parquet file data path used for the
  DuckDB-DDS-DPK evaluation.
  - DuckDB handles parquet reads through the DDS POSIX frontend (`Apps/DuckDB/DDS/StorageEngine/DDSPosixInterface/DDSPosix.cpp`) instead of regular POSIX read calls, and the DDS storage engine's DPU backend (`Apps/DuckDB/DDS/StorageEngine/DDSBackEndDPUService`) invokes relevant dpKernels through dpManager.
- `MessageExchange/`: shared-memory ring-buffer with spaced backoff: benchmarks and their
  spin-lock baseline, used to evaluate portable efficiency and message exchange.
