# DPU dependencies

- DOCA 1.5.1
- DPDK 20.11.7.1.2 (default version in DOCA 1.5.1)
- SPDK v22.05 (included as a submodule)


# Host dependencies

- Linux - Ubuntu 22.04
- C/C++ compiler with C++11 support
- Rdma-core development libraries (`librdmacm`, `libibverbs`) 

# BF-2 Memory Usage

- In developing DDS, log files are accumulated under `/dev/shm/`. They should be cleared periodically.

# Snapshot scope

This copy is kept for the DuckDB-DPK evaluation path. It retains the DDS storage
engine pieces used by DuckDB-DPK:

- `StorageEngine/DDSPosixInterface` provides the POSIX-like entry point linked
  into DuckDB-DPK.
- `StorageEngine/DDSFrontEnd`, `StorageEngine/DDSBackEndHostService`, and
  `StorageEngine/DDSBackEndDPUService` provide the host/frontend and DPU backend
  sides used by the storage path.
- `Common`, `Scripts`, and `ThirdParty/spdk` are retained because the storage
  engine code depends on them.

The standalone DDS network/offload application stack, including the top-level
`Main`, `NetworkEngine`, and `OffloadEngine` directories, is omitted from this
review snapshot because it was not part of the DuckDB-DPK integration being
archived here. Development-planning metadata such as `openspec` and `.github`
was also removed.

# DDS Configuration

- Host: ARP entry for the DPU must be statically configured
	- Windows: (1) `Get-NetAdapter` to find the index of the BF-2 CX-6 NIC, and (2) `New-NetNeighbor -InterfaceIndex [BF-2 CX-6 NIC index] -IPAddress '[DPU IP]' -LinkLayerAddress '000000000100' -State Permanent`
	- Linux: run `Scripts/HostAddArpLinux.sh [DPU IP] 00:00:00:00:01:00`

- DPU: the storage backend under `StorageEngine/DDSBackEndDPUService` still
  assumes the original BF-2/SPDK/DPDK environment. The exact device and storage
  configuration was machine-specific in the evaluation setup rather than a
  self-contained fresh-machine script.
