#pragma once

#include "duckdb/common/string.hpp"

#include <cstdint>

namespace duckdb {

/**
 * Records alignment statistics for DDS pread calls.
 */
void DDSPosixDebugRecordPread(uint64_t size, uint64_t offset);

/**
 * Prints and resets DDS pread alignment statistics at query end.
 */
void DDSPosixDebugPrintAndReset();

} // namespace duckdb
