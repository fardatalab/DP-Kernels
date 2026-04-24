#include "duckdb/common/dds_posix_debug.hpp"

#include <atomic>
#include <cstdio>

// DDS POSIX debug print macro.
// To enable these prints when DUCKDB_USE_DDS_POSIX is enabled, define DUCKDB_DDS_DEBUG_PRINT_ENABLED=1 at compile time.
// Example: -DDUCKDB_DDS_DEBUG_PRINT_ENABLED=1
#if defined(DUCKDB_USE_DDS_POSIX) && defined(DUCKDB_DDS_DEBUG_PRINT_ENABLED) && (DUCKDB_DDS_DEBUG_PRINT_ENABLED)
#define DUCKDB_DDS_DEBUG_PRINT(...) printf(__VA_ARGS__)
#else
#define DUCKDB_DDS_DEBUG_PRINT(...) ((void)0)
#endif

namespace duckdb {

namespace {

static constexpr uint64_t DDS_POSIX_PREAD_ALIGNMENT = 512;

std::atomic<uint64_t> dds_pread_aligned_calls {0};
std::atomic<uint64_t> dds_pread_unaligned_calls {0};
std::atomic<uint64_t> dds_pread_aligned_bytes {0};
std::atomic<uint64_t> dds_pread_unaligned_bytes {0};

} // namespace

/**
 * Records whether a DDS pread is aligned to 512 bytes for both offset and size.
 */
void DDSPosixDebugRecordPread(uint64_t size, uint64_t offset) {
#ifdef DUCKDB_USE_DDS_POSIX
	const bool aligned = (size % DDS_POSIX_PREAD_ALIGNMENT == 0) && (offset % DDS_POSIX_PREAD_ALIGNMENT == 0);
	if (aligned) {
		dds_pread_aligned_calls.fetch_add(1, std::memory_order_relaxed);
		dds_pread_aligned_bytes.fetch_add(size, std::memory_order_relaxed);
	} else {
		dds_pread_unaligned_calls.fetch_add(1, std::memory_order_relaxed);
		dds_pread_unaligned_bytes.fetch_add(size, std::memory_order_relaxed);
	}
#else
	(void)size;
	(void)offset;
#endif
}

/**
 * Prints and resets DDS pread alignment statistics for the completed query.
 */
void DDSPosixDebugPrintAndReset() {
#ifdef DUCKDB_USE_DDS_POSIX
	const auto aligned_calls = dds_pread_aligned_calls.exchange(0, std::memory_order_relaxed);
	const auto unaligned_calls = dds_pread_unaligned_calls.exchange(0, std::memory_order_relaxed);
	const auto aligned_bytes = dds_pread_aligned_bytes.exchange(0, std::memory_order_relaxed);
	const auto unaligned_bytes = dds_pread_unaligned_bytes.exchange(0, std::memory_order_relaxed);

	DUCKDB_DDS_DEBUG_PRINT(
	    "[DDS-IO] pread alignment calls aligned=%llu unaligned=%llu bytes aligned=%llu unaligned=%llu\n",
	    static_cast<unsigned long long>(aligned_calls), static_cast<unsigned long long>(unaligned_calls),
	    static_cast<unsigned long long>(aligned_bytes), static_cast<unsigned long long>(unaligned_bytes));
#endif
}

} // namespace duckdb
