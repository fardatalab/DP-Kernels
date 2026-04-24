#include "column_reader.hpp"

#include "reader/boolean_column_reader.hpp"
#include "brotli/decode.h"
#include "reader/callback_column_reader.hpp"
#include "reader/decimal_column_reader.hpp"
#include "duckdb.hpp"
#include "reader/expression_column_reader.hpp"
#include "reader/interval_column_reader.hpp"
#include "reader/list_column_reader.hpp"
#include "lz4.hpp"
#include "miniz_wrapper.hpp"
#include "reader/null_column_reader.hpp"
#include "parquet_reader.hpp"
#include "parquet_timestamp.hpp"
#include "parquet_float16.hpp"
#include "parquet_crypto.hpp"

#include "reader/row_number_column_reader.hpp"
#include "snappy.h"
#include "reader/string_column_reader.hpp"
#include "reader/struct_column_reader.hpp"
#include "reader/templated_column_reader.hpp"
#include "reader/uuid_column_reader.hpp"

#include "zstd.h"

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/bit.hpp"

#include <cstdio>
#include <chrono>
#include <cerrno>
#include <cstring>

#ifdef DUCKDB_USE_DDS_POSIX
#include "DDSPosix.h"
#endif

namespace duckdb {

using duckdb_parquet::CompressionCodec;
using duckdb_parquet::ConvertedType;
using duckdb_parquet::Encoding;
using duckdb_parquet::PageType;
using duckdb_parquet::Type;

/**
 * DPK-oriented parquet read path (strict offload mode).
 *
 * Encrypted + DDS mode:
 * - Stage0 reads one encrypted module:
 *   [4-byte length][12-byte nonce][ciphertext][16-byte tag]
 * - Stage1 AES-GCM decrypt consumes [ciphertext||tag] with nonce immediately before it.
 * - Stage2 (optional) raw-deflate decompress is enabled only for GZIP-compressed pages.
 *
 * Output contract:
 * - Uncompressed pages: stageCount=1 and `dst` receives plaintext page bytes.
 * - GZIP compressed pages: stageCount=2 and `dst` receives fully decompressed page bytes.
 *
 * Strict mode:
 * - Offload is mandatory; if pread2 offload cannot be used, this throws.
 */
[[maybe_unused]] static void dpk_read_decomp_decrypt(ParquetReader &reader,
                                                     duckdb_apache::thrift::protocol::TProtocol &protocol,
                                                     data_ptr_t dst, idx_t dst_size, idx_t src_size,
                                                     CompressionCodec::type codec) {
	D_ASSERT(dst);
	if (src_size == 0 || dst_size == 0) {
		throw InvalidInputException("DPK read/decrypt requires non-zero src/dst sizes");
	}
	if (src_size > NumericLimits<uint32_t>::Maximum()) {
		throw InvalidInputException("DPK source size %llu exceeds uint32_t range",
		                            static_cast<unsigned long long>(src_size));
	}
	const bool enable_stage2_raw_deflate = (codec == CompressionCodec::GZIP && dst_size != src_size);
	if (!enable_stage2_raw_deflate && dst_size != src_size) {
		throw InvalidInputException(
		    "DPK offload currently supports stage2 decompression only for GZIP "
		    "[codec=%d, dst=%llu, src=%llu]",
		    static_cast<int>(codec), static_cast<unsigned long long>(dst_size),
		    static_cast<unsigned long long>(src_size));
	}

	if (reader.parquet_options.encryption_config) {
		auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol.getTransport());
		const auto module_size_u64 =
		    NumericCast<uint64_t>(ParquetCrypto::LENGTH_BYTES + ParquetCrypto::NONCE_BYTES + src_size +
		                          ParquetCrypto::TAG_BYTES);
		if (module_size_u64 > NumericLimits<size_t>::Maximum()) {
			throw InvalidInputException("Encrypted parquet module size %llu exceeds size_t range",
			                            static_cast<unsigned long long>(module_size_u64));
		}
		const auto module_size = NumericCast<size_t>(module_size_u64);
		const auto module_start = trans.GetLocation();
		if (module_start + module_size_u64 > trans.GetSize()) {
			throw InvalidInputException(
			    "Encrypted parquet module exceeds file bounds [location=%llu, module_size=%llu, file_size=%llu]",
			    static_cast<unsigned long long>(module_start), static_cast<unsigned long long>(module_size_u64),
			    static_cast<unsigned long long>(trans.GetSize()));
		}

		// Strict mode: pread2 offload is mandatory for this path.
#ifdef DUCKDB_USE_DDS_POSIX
			if (!reader.EnsureDDSRead2AesKeyConfigured()) {
				throw InvalidInputException(
				    "Strict DDS offload mode requires read2 AES key configuration, but key setup failed "
				    "[file=\"%s\"]",
				    trans.GetPath().c_str());
			}
		const auto fd64 = trans.GetSystemFileDescriptor();
		if (fd64 < 0 || fd64 > NumericCast<int64_t>(NumericLimits<int>::Maximum())) {
			throw InvalidInputException("Strict DDS offload mode requires a valid system fd for pread2 "
			                            "[fd=%lld, file=\"%s\"]",
			                            static_cast<long long>(fd64), trans.GetPath().c_str());
		}

			// Original decrypt-only stage configuration retained for reference.
			// size_t stage_sizes[2] = {NumericCast<size_t>(src_size), 0};
			// size_t stage_input_offsets[2] = {
			//     NumericCast<size_t>(ParquetCrypto::LENGTH_BYTES + ParquetCrypto::NONCE_BYTES), 0};
			// size_t stage_input_lengths[2] = {NumericCast<size_t>(src_size + ParquetCrypto::TAG_BYTES), 0};
			// uint16_t stage_count = 1;
			//
			// Updated: optional stage2 raw-deflate decompress for GZIP pages.
			size_t stage_sizes[2] = {NumericCast<size_t>(src_size), 0};
			size_t stage_input_offsets[2] = {
			    NumericCast<size_t>(ParquetCrypto::LENGTH_BYTES + ParquetCrypto::NONCE_BYTES), 0};
			size_t stage_input_lengths[2] = {NumericCast<size_t>(src_size + ParquetCrypto::TAG_BYTES), 0};
			uint16_t stage_count = 1;
			if (enable_stage2_raw_deflate) {
				// GZIP layout in stage1 output:
				// [10B header][raw deflate body][8B footer]
				// Stage2 consumes only the raw-deflate window.
				if (src_size <= 18) {
					throw InvalidInputException(
					    "Invalid GZIP payload size for stage2 raw-deflate offload [compressed=%llu, file=\"%s\"]",
					    static_cast<unsigned long long>(src_size), trans.GetPath().c_str());
				}
				const auto deflate_offset = NumericCast<size_t>(10);
				const auto deflate_length = NumericCast<size_t>(src_size - 18);
				stage_sizes[1] = NumericCast<size_t>(dst_size);
				stage_input_offsets[1] = deflate_offset;
				stage_input_lengths[1] = deflate_length;
				stage_count = 2;
			}

			// Original offload wall-time instrumentation (mapped to PARQUET_DECRYPTION_TIME) retained for reference.
			// const auto offload_start = std::chrono::steady_clock::now();
			// const auto read_bytes = DDSPosix::pread2(NumericCast<int>(fd64), dst, module_size,
			//                                          NumericCast<off_t>(module_start), stage_sizes, stage_input_offsets,
			//                                          stage_input_lengths, stage_count);
			// const auto offload_end = std::chrono::steady_clock::now();
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
			// Record host-observed elapsed time for aggregate pread2 thread-time, regardless of
			// whether detailed stage timings are reported by DDS.
			const auto pread2_start = std::chrono::steady_clock::now();
			DDSPosix::DDSOffloadStageTimings offload_timings = {};
			const auto read_bytes = DDSPosix::pread2(
			    NumericCast<int>(fd64), dst, module_size, NumericCast<off_t>(module_start), stage_sizes,
			    stage_input_offsets, stage_input_lengths, stage_count, &offload_timings);
#else
			// Record host-observed elapsed time for aggregate pread2 thread-time.
			const auto pread2_start = std::chrono::steady_clock::now();
			const auto read_bytes = DDSPosix::pread2(NumericCast<int>(fd64), dst, module_size,
			                                         NumericCast<off_t>(module_start), stage_sizes, stage_input_offsets,
			                                         stage_input_lengths, stage_count);
#endif
			const auto pread2_end = std::chrono::steady_clock::now();
			reader.AddDDSPosixPread2Metrics(
			    trans.GetPath(), NumericCast<uint64_t>(module_size_u64),
			    NumericCast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(pread2_end - pread2_start)
			                              .count()));
			if (read_bytes != NumericCast<ssize_t>(dst_size)) {
				const auto err = errno;
				throw InvalidInputException(
				    "DDS pread2 failed for encrypted page [read_bytes=%lld, expected=%llu, errno=%d (%s), "
				    "fd=%d, file=\"%s\", offset=%llu, module_size=%llu, stage_count=%u, stage_sizes=[%llu,%llu], "
				    "stage_input_offsets=[%llu,%llu], stage_input_lengths=[%llu,%llu]]",
				    static_cast<long long>(read_bytes), static_cast<unsigned long long>(dst_size), static_cast<int>(err),
				    strerror(err), NumericCast<int>(fd64), trans.GetPath().c_str(),
				    static_cast<unsigned long long>(module_start), static_cast<unsigned long long>(module_size_u64),
				    static_cast<unsigned>(stage_count), static_cast<unsigned long long>(stage_sizes[0]),
				    static_cast<unsigned long long>(stage_sizes[1]),
				    static_cast<unsigned long long>(stage_input_offsets[0]),
				    static_cast<unsigned long long>(stage_input_offsets[1]),
				    static_cast<unsigned long long>(stage_input_lengths[0]),
				    static_cast<unsigned long long>(stage_input_lengths[1]));
			}
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
			reader.AddOffloadParquetReadMetrics(offload_timings.read_ns);
			reader.AddOffloadParquetDecryptMetrics(offload_timings.stage1_ns);
			if (stage_count > 1) {
				reader.AddOffloadParquetDecompressMetrics(offload_timings.stage2_ns);
			}
#else
			reader.AddParquetDecryptionMetrics(
			    NumericCast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(pread2_end - pread2_start)
			                              .count()));
#endif
			trans.Skip(NumericCast<idx_t>(module_size));
			return;

#else
		throw InvalidInputException(
		    "Strict DDS offload mode requires DUCKDB_USE_DDS_POSIX build support for pread2 "
		    "[file=\"%s\"]",
		    trans.GetPath().c_str());
#endif

		// Original local decrypt+decompress fallback retained for reference.
		/*
		ResizeableBuffer compressed_data;
		compressed_data.resize(reader.allocator, src_size);
		const auto raw_module_size = NumericCast<idx_t>(ParquetCrypto::LENGTH_BYTES + ParquetCrypto::NONCE_BYTES +
		                                                src_size + ParquetCrypto::TAG_BYTES);
		ResizeableBuffer encrypted_module;
		encrypted_module.resize(reader.allocator, raw_module_size);
		const auto bytes_read =
		    reader.ReadEncryptedModuleRaw(protocol, encrypted_module.ptr, NumericCast<uint32_t>(src_size));
		D_ASSERT(bytes_read == raw_module_size);
		*/
	} else {
		throw InvalidInputException(
		    "dpk_read_decomp_decrypt strict offload path called for unencrypted page; this is not supported");

		// Original unencrypted fallback retained for reference.
		/*
		ResizeableBuffer compressed_data;
		compressed_data.resize(reader.allocator, src_size);
		auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol.getTransport());
		const auto bytes_read = trans.ReadDirect(compressed_data.ptr, NumericCast<uint32_t>(src_size));
		D_ASSERT(bytes_read == src_size);
		const auto decompress_start = std::chrono::steady_clock::now();
		MiniZStream s;
		s.Decompress(const_char_ptr_cast(compressed_data.ptr), src_size, char_ptr_cast(dst), dst_size);
		const auto decompress_end = std::chrono::steady_clock::now();
		reader.AddParquetDecompressionMetrics(
		    NumericCast<uint64_t>(
		        std::chrono::duration_cast<std::chrono::nanoseconds>(decompress_end - decompress_start).count()));
		*/
	}
}

const uint64_t ParquetDecodeUtils::BITPACK_MASKS[] = {0,
                                                      1,
                                                      3,
                                                      7,
                                                      15,
                                                      31,
                                                      63,
                                                      127,
                                                      255,
                                                      511,
                                                      1023,
                                                      2047,
                                                      4095,
                                                      8191,
                                                      16383,
                                                      32767,
                                                      65535,
                                                      131071,
                                                      262143,
                                                      524287,
                                                      1048575,
                                                      2097151,
                                                      4194303,
                                                      8388607,
                                                      16777215,
                                                      33554431,
                                                      67108863,
                                                      134217727,
                                                      268435455,
                                                      536870911,
                                                      1073741823,
                                                      2147483647,
                                                      4294967295,
                                                      8589934591,
                                                      17179869183,
                                                      34359738367,
                                                      68719476735,
                                                      137438953471,
                                                      274877906943,
                                                      549755813887,
                                                      1099511627775,
                                                      2199023255551,
                                                      4398046511103,
                                                      8796093022207,
                                                      17592186044415,
                                                      35184372088831,
                                                      70368744177663,
                                                      140737488355327,
                                                      281474976710655,
                                                      562949953421311,
                                                      1125899906842623,
                                                      2251799813685247,
                                                      4503599627370495,
                                                      9007199254740991,
                                                      18014398509481983,
                                                      36028797018963967,
                                                      72057594037927935,
                                                      144115188075855871,
                                                      288230376151711743,
                                                      576460752303423487,
                                                      1152921504606846975,
                                                      2305843009213693951,
                                                      4611686018427387903,
                                                      9223372036854775807,
                                                      18446744073709551615ULL};

const uint64_t ParquetDecodeUtils::BITPACK_MASKS_SIZE = sizeof(ParquetDecodeUtils::BITPACK_MASKS) / sizeof(uint64_t);

const uint8_t ParquetDecodeUtils::BITPACK_DLEN = 8;

ColumnReader::ColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema_p)
    : column_schema(schema_p), reader(reader), page_rows_available(0), dictionary_decoder(*this),
      delta_binary_packed_decoder(*this), rle_decoder(*this), delta_length_byte_array_decoder(*this),
      delta_byte_array_decoder(*this), byte_stream_split_decoder(*this) {
}

ColumnReader::~ColumnReader() {
}

Allocator &ColumnReader::GetAllocator() {
	return reader.allocator;
}

ParquetReader &ColumnReader::Reader() {
	return reader;
}

void ColumnReader::RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) {
	if (chunk) {
		uint64_t size = chunk->meta_data.total_compressed_size;
		transport.RegisterPrefetch(FileOffset(), size, allow_merge);
	}
}

unique_ptr<BaseStatistics> ColumnReader::Stats(idx_t row_group_idx_p, const vector<ColumnChunk> &columns) {
	return Schema().Stats(*reader.GetFileMetadata(), reader.parquet_options, row_group_idx_p, columns);
}

uint64_t ColumnReader::TotalCompressedSize() {
	if (!chunk) {
		return 0;
	}

	return chunk->meta_data.total_compressed_size;
}

// Note: It's not trivial to determine where all Column data is stored. Chunk->file_offset
// apparently is not the first page of the data. Therefore we determine the address of the first page by taking the
// minimum of all page offsets.
idx_t ColumnReader::FileOffset() const {
	if (!chunk) {
		throw std::runtime_error("FileOffset called on ColumnReader with no chunk");
	}
	auto min_offset = NumericLimits<idx_t>::Maximum();
	if (chunk->meta_data.__isset.dictionary_page_offset) {
		min_offset = MinValue<idx_t>(min_offset, chunk->meta_data.dictionary_page_offset);
	}
	if (chunk->meta_data.__isset.index_page_offset) {
		min_offset = MinValue<idx_t>(min_offset, chunk->meta_data.index_page_offset);
	}
	min_offset = MinValue<idx_t>(min_offset, chunk->meta_data.data_page_offset);

	return min_offset;
}

idx_t ColumnReader::GroupRowsAvailable() {
	return group_rows_available;
}

void ColumnReader::PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values) {
	throw NotImplementedException("PlainSkip not implemented");
}

void ColumnReader::Plain(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values, // NOLINT
                         idx_t result_offset, Vector &result) {
	throw NotImplementedException("Plain not implemented");
}

void ColumnReader::Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
                         idx_t result_offset, Vector &result) {
	Plain(*plain_data, defines, num_values, result_offset, result);
}

void ColumnReader::PlainSelect(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
                               Vector &result, const SelectionVector &sel, idx_t count) {
	throw NotImplementedException("PlainSelect not implemented");
}

void ColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns, TProtocol &protocol_p) {
	D_ASSERT(ColumnIndex() < columns.size());
	chunk = &columns[ColumnIndex()];
	protocol = &protocol_p;
	D_ASSERT(chunk);
	D_ASSERT(chunk->__isset.meta_data);

	if (chunk->__isset.file_path) {
		throw InvalidInputException("Failed to read file \"%s\": Only inlined data files are supported (no references)",
		                            Reader().GetFileName());
	}

	// ugh. sometimes there is an extra offset for the dict. sometimes it's wrong.
	chunk_read_offset = chunk->meta_data.data_page_offset;
	if (chunk->meta_data.__isset.dictionary_page_offset && chunk->meta_data.dictionary_page_offset >= 4) {
		// this assumes the data pages follow the dict pages directly.
		chunk_read_offset = chunk->meta_data.dictionary_page_offset;
	}
	group_rows_available = chunk->meta_data.num_values;
}

bool ColumnReader::PageIsFilteredOut(PageHeader &page_hdr) {
	if (!dictionary_decoder.HasFilteredOutAllValues()) {
		return false;
	}
	if (page_hdr.type != PageType::DATA_PAGE && page_hdr.type != PageType::DATA_PAGE_V2) {
		// we can only filter out data pages
		return false;
	}
	bool is_v1 = page_hdr.type == PageType::DATA_PAGE;
	auto &v1_header = page_hdr.data_page_header;
	auto &v2_header = page_hdr.data_page_header_v2;
	auto page_encoding = is_v1 ? v1_header.encoding : v2_header.encoding;
	if (page_encoding != Encoding::PLAIN_DICTIONARY && page_encoding != Encoding::RLE_DICTIONARY) {
		// not a dictionary page
		return false;
	}
	// the page has been filtered out!
	// skip forward
	auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol->getTransport());
	// jason: warn when we skip a filtered page which only skips compressed size, should also include length/nonce/tag
	// sizes
	const auto skip_start = trans.GetLocation();
	const auto skip_bytes = UnsafeNumericCast<idx_t>(page_hdr.compressed_page_size);
	const bool encryption_enabled = Reader().parquet_options.encryption_config != nullptr;
	fprintf(stderr, "[Parquet][Warning] PageIsFilteredOut skip start=%llu bytes=%llu encrypted=%s (type=%d)\n",
	        static_cast<unsigned long long>(skip_start), static_cast<unsigned long long>(skip_bytes),
	        encryption_enabled ? "true" : "false", static_cast<int>(page_hdr.type));
	trans.Skip(skip_bytes);

	page_rows_available = is_v1 ? v1_header.num_values : v2_header.num_values;
	encoding = ColumnEncoding::DICTIONARY;
	page_is_filtered_out = true;
	return true;
}

void ColumnReader::PrepareRead(optional_ptr<const TableFilter> filter, optional_ptr<TableFilterState> filter_state) {
	encoding = ColumnEncoding::INVALID;
	defined_decoder.reset();
	page_is_filtered_out = false;
	block.reset();
	PageHeader page_hdr;
	auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol->getTransport());
	if (trans.HasPrefetch()) {
		// Already has some data prefetched, let's not mess with it
		reader.Read(page_hdr, *protocol);
	} else {
		// No prefetch yet, prefetch the full header in one go (so thrift won't read byte-by-byte from storage)
		// 256 bytes should cover almost all headers (unless it's a V2 header with really LONG string statistics)
		static constexpr idx_t ASSUMED_HEADER_SIZE = 256;
		const auto prefetch_size = MinValue(trans.GetSize() - trans.GetLocation(), ASSUMED_HEADER_SIZE);
		trans.Prefetch(trans.GetLocation(), prefetch_size);
		reader.Read(page_hdr, *protocol);
		trans.ClearPrefetch();
	}
	// some basic sanity check
	if (page_hdr.compressed_page_size < 0 || page_hdr.uncompressed_page_size < 0) {
		throw InvalidInputException("Failed to read file \"%s\": Page sizes can't be < 0", Reader().GetFileName());
	}

	if (PageIsFilteredOut(page_hdr)) {
		// this page has been filtered out so we don't need to read it
		return;
	}

	switch (page_hdr.type) {
	case PageType::DATA_PAGE_V2:
		PreparePageV2(page_hdr);
		PrepareDataPage(page_hdr);
		break;
	case PageType::DATA_PAGE:
		PreparePage(page_hdr);
		PrepareDataPage(page_hdr);
		break;
	case PageType::DICTIONARY_PAGE: {
		PreparePage(page_hdr);
		auto dictionary_size = page_hdr.dictionary_page_header.num_values;
		if (dictionary_size < 0) {
			throw InvalidInputException("Failed to read file \"%s\": Invalid dictionary page header (num_values < 0)",
			                            Reader().GetFileName());
		}
		dictionary_decoder.InitializeDictionary(dictionary_size, filter, filter_state, HasDefines());
		break;
	}
	default:
		break; // ignore INDEX page type and any other custom extensions
	}
	ResetPage();
}

void ColumnReader::ResetPage() {
}

// Prepares a DATA_PAGE_V2 buffer, with an optional DPK gzip decrypt/decompress fast-path.
void ColumnReader::PreparePageV2(PageHeader &page_hdr) {
	D_ASSERT(page_hdr.type == PageType::DATA_PAGE_V2);
	throw InvalidInputException(
	    "DATA_PAGE_V2 is not supported in strict DDS offload mode (encrypted V1 GZIP only currently)");
	// Original V2 path retained below for reference.

	AllocateBlock(page_hdr.uncompressed_page_size + 1);
	bool uncompressed = false;
	if (page_hdr.data_page_header_v2.__isset.is_compressed && !page_hdr.data_page_header_v2.is_compressed) {
		uncompressed = true;
	}
	if (chunk->meta_data.codec == CompressionCodec::UNCOMPRESSED) {
		if (page_hdr.compressed_page_size != page_hdr.uncompressed_page_size) {
			throw InvalidInputException("Failed to read file \"%s\": Page size mismatch", Reader().GetFileName());
		}
		uncompressed = true;
	}
	if (uncompressed) {
		reader.ReadData(*protocol, block->ptr, page_hdr.compressed_page_size);
		return;
	}

	// copy repeats & defines as-is because FOR SOME REASON they are uncompressed
	auto uncompressed_bytes = page_hdr.data_page_header_v2.repetition_levels_byte_length +
	                          page_hdr.data_page_header_v2.definition_levels_byte_length;
	if (uncompressed_bytes > page_hdr.uncompressed_page_size) {
		throw InvalidInputException(
		    "Failed to read file \"%s\": header inconsistency, uncompressed_page_size needs to be larger than "
		    "repetition_levels_byte_length + definition_levels_byte_length",
		    Reader().GetFileName());
	}
	reader.ReadData(*protocol, block->ptr, uncompressed_bytes);

	auto compressed_bytes = page_hdr.compressed_page_size - uncompressed_bytes;

	// jason: placeholder for DPK-integrated read + decrypt + gzip decompress path
	/*
	if (compressed_bytes > 0) {
	    ResizeableBuffer compressed_buffer;
	    compressed_buffer.resize(GetAllocator(), compressed_bytes);
	    reader.ReadData(*protocol, compressed_buffer.ptr, compressed_bytes);

	    DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, compressed_bytes,
	                       block->ptr + uncompressed_bytes, page_hdr.uncompressed_page_size - uncompressed_bytes);
	}
	*/
	if (compressed_bytes > 0) {
		// Original active V2 path: keep ReadData + DecompressInternal behavior.
		ResizeableBuffer compressed_buffer;
		compressed_buffer.resize(GetAllocator(), compressed_bytes);
		reader.ReadData(*protocol, compressed_buffer.ptr, compressed_bytes);

		DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, compressed_bytes,
		                   block->ptr + uncompressed_bytes, page_hdr.uncompressed_page_size - uncompressed_bytes);
	}
}

void ColumnReader::AllocateBlock(idx_t size) {
	if (!block) {
		block = make_shared_ptr<ResizeableBuffer>(GetAllocator(), size);
	} else {
		block->resize(GetAllocator(), size);
	}
}

// Prepares a DATA_PAGE buffer, with an optional DPK read+decrypt offload fast-path.
void ColumnReader::PreparePage(PageHeader &page_hdr) {
	AllocateBlock(page_hdr.uncompressed_page_size + 1);
	if (chunk->meta_data.codec == CompressionCodec::UNCOMPRESSED) {
		if (page_hdr.compressed_page_size != page_hdr.uncompressed_page_size) {
			throw std::runtime_error("Page size mismatch");
		}
		// Encrypted + strict DDS path: offload read+decrypt for uncompressed pages.
		if (reader.parquet_options.encryption_config) {
			dpk_read_decomp_decrypt(reader, *protocol, block->ptr, page_hdr.uncompressed_page_size,
			                        page_hdr.compressed_page_size, chunk->meta_data.codec);
		} else {
			reader.ReadData(*protocol, block->ptr, page_hdr.compressed_page_size);
		}
		return;
	}

	// jason: placeholder for DPK-integrated read + decrypt offload path

	// Original V1 path retained for reference.
	// ResizeableBuffer compressed_buffer;
	// compressed_buffer.resize(GetAllocator(), page_hdr.compressed_page_size + 1);
	// reader.ReadData(*protocol, compressed_buffer.ptr, page_hdr.compressed_page_size);
	//
	// DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, page_hdr.compressed_page_size, block->ptr,
	//                    page_hdr.uncompressed_page_size);

	// Original active V1 path retained for reference; encrypted+GZIP now uses dpk_read_decomp_decrypt().
	// ResizeableBuffer compressed_buffer;
	// compressed_buffer.resize(GetAllocator(), page_hdr.compressed_page_size + 1);
	// reader.ReadData(*protocol, compressed_buffer.ptr, page_hdr.compressed_page_size);
	//
	// DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, page_hdr.compressed_page_size, block->ptr,
	//                    page_hdr.uncompressed_page_size);

	if (reader.parquet_options.encryption_config) {
		// Original host-side decompression path retained for reference.
		// ResizeableBuffer compressed_buffer;
		// compressed_buffer.resize(GetAllocator(), page_hdr.compressed_page_size + 1);
		// dpk_read_decomp_decrypt(reader, *protocol, compressed_buffer.ptr, page_hdr.compressed_page_size,
		//                         page_hdr.compressed_page_size, chunk->meta_data.codec);
		// DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, page_hdr.compressed_page_size, block->ptr,
		//                    page_hdr.uncompressed_page_size);
		//
		// Updated:
		// - GZIP pages use stage2 raw-deflate offload and write final plaintext directly to `block->ptr`.
		// - Non-GZIP compressed pages keep host decompression after stage1 decrypt.
		if (chunk->meta_data.codec == CompressionCodec::GZIP) {
			dpk_read_decomp_decrypt(reader, *protocol, block->ptr, page_hdr.uncompressed_page_size,
			                        page_hdr.compressed_page_size, chunk->meta_data.codec);
		} else {
			ResizeableBuffer compressed_buffer;
			compressed_buffer.resize(GetAllocator(), page_hdr.compressed_page_size + 1);
			dpk_read_decomp_decrypt(reader, *protocol, compressed_buffer.ptr, page_hdr.compressed_page_size,
			                        page_hdr.compressed_page_size, chunk->meta_data.codec);
			DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, page_hdr.compressed_page_size, block->ptr,
			                   page_hdr.uncompressed_page_size);
		}
	} else {
		ResizeableBuffer compressed_buffer;
		compressed_buffer.resize(GetAllocator(), page_hdr.compressed_page_size + 1);
		reader.ReadData(*protocol, compressed_buffer.ptr, page_hdr.compressed_page_size);

		DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, page_hdr.compressed_page_size, block->ptr,
		                   page_hdr.uncompressed_page_size);
	}

	/* if (chunk->meta_data.codec == CompressionCodec::GZIP) {
	    // DPK placeholder: replaces the ReadData + GZIP DecompressInternal path above.
	    dpk_read_decomp_decrypt(reader, *protocol, block->ptr, page_hdr.uncompressed_page_size,
	                            page_hdr.compressed_page_size);
	} else {
	    ResizeableBuffer compressed_buffer;
	    compressed_buffer.resize(GetAllocator(), page_hdr.compressed_page_size + 1);
	    reader.ReadData(*protocol, compressed_buffer.ptr, page_hdr.compressed_page_size);

	    DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, page_hdr.compressed_page_size, block->ptr,
	                       page_hdr.uncompressed_page_size);
	} */
}

// Decompresses a parquet page payload and records codec timing for profiling.
void ColumnReader::DecompressInternal(CompressionCodec::type codec, const_data_ptr_t src, idx_t src_size,
                                      data_ptr_t dst, idx_t dst_size) {
	const auto start = std::chrono::steady_clock::now();
	switch (codec) {
	case CompressionCodec::UNCOMPRESSED:
		throw InternalException("Parquet data unexpectedly uncompressed");
	case CompressionCodec::GZIP: {
		MiniZStream s;
		s.Decompress(const_char_ptr_cast(src), src_size, char_ptr_cast(dst), dst_size);
		break;
	}
	case CompressionCodec::LZ4_RAW: {
		auto res =
		    duckdb_lz4::LZ4_decompress_safe(const_char_ptr_cast(src), char_ptr_cast(dst),
		                                    UnsafeNumericCast<int32_t>(src_size), UnsafeNumericCast<int32_t>(dst_size));
		if (res != NumericCast<int>(dst_size)) {
			throw InvalidInputException("Failed to read file \"%s\": LZ4 decompression failure",
			                            Reader().GetFileName());
		}
		break;
	}
	case CompressionCodec::SNAPPY: {
		{
			size_t uncompressed_size = 0;
			auto res = duckdb_snappy::GetUncompressedLength(const_char_ptr_cast(src), src_size, &uncompressed_size);
			if (!res) {
				throw InvalidInputException("Failed to read file \"%s\": Snappy decompression failure",
				                            Reader().GetFileName());
			}
			if (uncompressed_size != dst_size) {
				throw InvalidInputException(
				    "Failed to read file \"%s\": Snappy decompression failure: Uncompressed data size mismatch",
				    Reader().GetFileName());
			}
		}
		auto res = duckdb_snappy::RawUncompress(const_char_ptr_cast(src), src_size, char_ptr_cast(dst));
		if (!res) {
			throw InvalidInputException("Failed to read file \"%s\": Snappy decompression failure",
			                            Reader().GetFileName());
		}
		break;
	}
	case CompressionCodec::ZSTD: {
		auto res = duckdb_zstd::ZSTD_decompress(dst, dst_size, src, src_size);
		if (duckdb_zstd::ZSTD_isError(res) || res != dst_size) {
			throw InvalidInputException("Failed to read file \"%s\": ZSTD Decompression failure",
			                            Reader().GetFileName());
		}
		break;
	}
	case CompressionCodec::BROTLI: {
		auto state = duckdb_brotli::BrotliDecoderCreateInstance(nullptr, nullptr, nullptr);
		size_t total_out = 0;
		auto src_size_size_t = NumericCast<size_t>(src_size);
		auto dst_size_size_t = NumericCast<size_t>(dst_size);

		auto res = duckdb_brotli::BrotliDecoderDecompressStream(state, &src_size_size_t, &src, &dst_size_size_t, &dst,
		                                                        &total_out);
		if (res != duckdb_brotli::BROTLI_DECODER_RESULT_SUCCESS) {
			throw InvalidInputException("Failed to read file \"%s\": Brotli Decompression failure",
			                            Reader().GetFileName());
		}
		duckdb_brotli::BrotliDecoderDestroyInstance(state);
		break;
	}

	default: {
		duckdb::stringstream codec_name;
		codec_name << codec;
		throw InvalidInputException("Failed to read file \"%s\": Unsupported compression codec \"%s\". Supported "
		                            "options are uncompressed, brotli, gzip, lz4_raw, snappy or zstd",
		                            Reader().GetFileName(), codec_name.str());
	}
	}
	// Record timing only after a successful decompression step.
	const auto elapsed_ns =
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count();
	reader.AddParquetDecompressionMetrics(NumericCast<uint64_t>(elapsed_ns));
}

void ColumnReader::PrepareDataPage(PageHeader &page_hdr) {
	if (page_hdr.type == PageType::DATA_PAGE && !page_hdr.__isset.data_page_header) {
		throw InvalidInputException("Failed to read file \"%s\": Missing data page header from data page",
		                            Reader().GetFileName());
	}
	if (page_hdr.type == PageType::DATA_PAGE_V2 && !page_hdr.__isset.data_page_header_v2) {
		throw InvalidInputException("Failed to read file \"%s\": Missing data page header from data page v2",
		                            Reader().GetFileName());
	}

	bool is_v1 = page_hdr.type == PageType::DATA_PAGE;
	bool is_v2 = page_hdr.type == PageType::DATA_PAGE_V2;
	auto &v1_header = page_hdr.data_page_header;
	auto &v2_header = page_hdr.data_page_header_v2;

	page_rows_available = is_v1 ? v1_header.num_values : v2_header.num_values;
	auto page_encoding = is_v1 ? v1_header.encoding : v2_header.encoding;

	if (HasRepeats()) {
		uint32_t rep_length = is_v1 ? block->read<uint32_t>() : v2_header.repetition_levels_byte_length;
		block->available(rep_length);
		repeated_decoder = make_uniq<RleBpDecoder>(block->ptr, rep_length, RleBpDecoder::ComputeBitWidth(MaxRepeat()));
		block->inc(rep_length);
	} else if (is_v2 && v2_header.repetition_levels_byte_length > 0) {
		block->inc(v2_header.repetition_levels_byte_length);
	}

	if (HasDefines()) {
		uint32_t def_length = is_v1 ? block->read<uint32_t>() : v2_header.definition_levels_byte_length;
		block->available(def_length);
		defined_decoder = make_uniq<RleBpDecoder>(block->ptr, def_length, RleBpDecoder::ComputeBitWidth(MaxDefine()));
		block->inc(def_length);
	} else if (is_v2 && v2_header.definition_levels_byte_length > 0) {
		block->inc(v2_header.definition_levels_byte_length);
	}

	switch (page_encoding) {
	case Encoding::RLE_DICTIONARY:
	case Encoding::PLAIN_DICTIONARY: {
		encoding = ColumnEncoding::DICTIONARY;
		dictionary_decoder.InitializePage();
		break;
	}
	case Encoding::RLE: {
		encoding = ColumnEncoding::RLE;
		rle_decoder.InitializePage();
		break;
	}
	case Encoding::DELTA_BINARY_PACKED: {
		encoding = ColumnEncoding::DELTA_BINARY_PACKED;
		delta_binary_packed_decoder.InitializePage();
		break;
	}
	case Encoding::DELTA_LENGTH_BYTE_ARRAY: {
		encoding = ColumnEncoding::DELTA_LENGTH_BYTE_ARRAY;
		delta_length_byte_array_decoder.InitializePage();
		break;
	}
	case Encoding::DELTA_BYTE_ARRAY: {
		encoding = ColumnEncoding::DELTA_BYTE_ARRAY;
		delta_byte_array_decoder.InitializePage();
		break;
	}
	case Encoding::BYTE_STREAM_SPLIT: {
		encoding = ColumnEncoding::BYTE_STREAM_SPLIT;
		byte_stream_split_decoder.InitializePage();
		break;
	}
	case Encoding::PLAIN:
		// nothing to do here, will be read directly below
		encoding = ColumnEncoding::PLAIN;
		break;

	default:
		throw InvalidInputException("Failed to read file \"%s\": Unsupported page encoding", Reader().GetFileName());
	}
}

void ColumnReader::BeginRead(data_ptr_t define_out, data_ptr_t repeat_out) {
	// we need to reset the location because multiple column readers share the same protocol
	auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol->getTransport());
	trans.SetLocation(chunk_read_offset);

	// Perform any skips that were not applied yet.
	if (define_out && repeat_out) {
		ApplyPendingSkips(define_out, repeat_out);
	}
}

idx_t ColumnReader::ReadPageHeaders(idx_t max_read, optional_ptr<const TableFilter> filter,
                                    optional_ptr<TableFilterState> filter_state) {
	while (page_rows_available == 0) {
		PrepareRead(filter, filter_state);
	}
	return MinValue<idx_t>(MinValue<idx_t>(max_read, page_rows_available), STANDARD_VECTOR_SIZE);
}

bool ColumnReader::PrepareRead(idx_t read_now, data_ptr_t define_out, data_ptr_t repeat_out, idx_t result_offset) {
	D_ASSERT(block);

	D_ASSERT(read_now + result_offset <= STANDARD_VECTOR_SIZE);
	D_ASSERT(!page_is_filtered_out);

	if (HasRepeats()) {
		D_ASSERT(repeated_decoder);
		repeated_decoder->GetBatch<uint8_t>(repeat_out + result_offset, read_now);
	}

	if (HasDefines()) {
		D_ASSERT(defined_decoder);
		const auto max_define = NumericCast<uint8_t>(MaxDefine());
		if (!HasRepeats() && defined_decoder->HasRepeatedBatch<uint8_t>(read_now, max_define)) {
			// Fast path: no repeats and all valid
			defined_decoder->GetRepeatedBatch<uint8_t>(read_now, max_define);
			return true;
		}
		defined_decoder->GetBatch<uint8_t>(define_out + result_offset, read_now);
		return false;
	}

	return true; // No defines, so everything is valid
}

void ColumnReader::ReadData(idx_t read_now, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result,
                            idx_t result_offset) {
	// flatten the result vector if required
	if (result_offset != 0 && result.GetVectorType() != VectorType::FLAT_VECTOR) {
		result.Flatten(result_offset);
		result.Resize(result_offset, STANDARD_VECTOR_SIZE);
	}
	if (page_is_filtered_out) {
		// page is filtered out - emit NULL for any rows
		auto &validity = FlatVector::Validity(result);
		for (idx_t i = 0; i < read_now; i++) {
			validity.SetInvalid(result_offset + i);
		}
		page_rows_available -= read_now;
		return;
	}
	// read the defines/repeats
	const auto all_valid = PrepareRead(read_now, define_out, repeat_out, result_offset);
	// read the data according to the encoder
	const auto define_ptr = all_valid ? nullptr : static_cast<uint8_t *>(define_out);
	switch (encoding) {
	case ColumnEncoding::DICTIONARY:
		dictionary_decoder.Read(define_ptr, read_now, result, result_offset);
		break;
	case ColumnEncoding::DELTA_BINARY_PACKED:
		delta_binary_packed_decoder.Read(define_ptr, read_now, result, result_offset);
		break;
	case ColumnEncoding::RLE:
		rle_decoder.Read(define_ptr, read_now, result, result_offset);
		break;
	case ColumnEncoding::DELTA_LENGTH_BYTE_ARRAY:
		delta_length_byte_array_decoder.Read(block, define_ptr, read_now, result, result_offset);
		break;
	case ColumnEncoding::DELTA_BYTE_ARRAY:
		delta_byte_array_decoder.Read(define_ptr, read_now, result, result_offset);
		break;
	case ColumnEncoding::BYTE_STREAM_SPLIT:
		byte_stream_split_decoder.Read(define_ptr, read_now, result, result_offset);
		break;
	default:
		Plain(block, define_ptr, read_now, result_offset, result);
		break;
	}
	page_rows_available -= read_now;
}

void ColumnReader::FinishRead(idx_t read_count) {
	auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol->getTransport());
	chunk_read_offset = trans.GetLocation();

	group_rows_available -= read_count;
}

idx_t ColumnReader::ReadInternal(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) {
	idx_t result_offset = 0;
	auto to_read = num_values;
	D_ASSERT(to_read <= STANDARD_VECTOR_SIZE);

	while (to_read > 0) {
		auto read_now = ReadPageHeaders(to_read);

		ReadData(read_now, define_out, repeat_out, result, result_offset);

		result_offset += read_now;
		to_read -= read_now;
	}
	FinishRead(num_values);

	return num_values;
}

idx_t ColumnReader::Read(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) {
	BeginRead(define_out, repeat_out);
	return ReadInternal(num_values, define_out, repeat_out, result);
}

void ColumnReader::Select(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result_out,
                          const SelectionVector &sel, idx_t approved_tuple_count) {
	if (SupportsDirectSelect() && approved_tuple_count < num_values) {
		DirectSelect(num_values, define_out, repeat_out, result_out, sel, approved_tuple_count);
		return;
	}
	Read(num_values, define_out, repeat_out, result_out);
}

void ColumnReader::DirectSelect(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result,
                                const SelectionVector &sel, idx_t approved_tuple_count) {
	auto to_read = num_values;

	// prepare the first read if we haven't yet
	BeginRead(define_out, repeat_out);
	auto read_now = ReadPageHeaders(num_values);

	// we can only push the filter into the decoder if we are reading the ENTIRE vector in one go
	if (read_now == to_read && encoding == ColumnEncoding::PLAIN) {
		const auto all_valid = PrepareRead(read_now, define_out, repeat_out, 0);
		const auto define_ptr = all_valid ? nullptr : static_cast<uint8_t *>(define_out);
		PlainSelect(block, define_ptr, read_now, result, sel, approved_tuple_count);

		page_rows_available -= read_now;
		FinishRead(num_values);
		return;
	}
	// fallback to regular read + filter
	ReadInternal(num_values, define_out, repeat_out, result);
}

void ColumnReader::Filter(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result,
                          const TableFilter &filter, TableFilterState &filter_state, SelectionVector &sel,
                          idx_t &approved_tuple_count, bool is_first_filter) {
	if (SupportsDirectFilter() && is_first_filter) {
		DirectFilter(num_values, define_out, repeat_out, result, filter, filter_state, sel, approved_tuple_count);
		return;
	}
	Select(num_values, define_out, repeat_out, result, sel, approved_tuple_count);
	ApplyFilter(result, filter, filter_state, num_values, sel, approved_tuple_count);
}

void ColumnReader::DirectFilter(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result,
                                const TableFilter &filter, TableFilterState &filter_state, SelectionVector &sel,
                                idx_t &approved_tuple_count) {
	auto to_read = num_values;

	// prepare the first read if we haven't yet
	BeginRead(define_out, repeat_out);
	auto read_now = ReadPageHeaders(num_values, &filter, &filter_state);

	// we can only push the filter into the decoder if we are reading the ENTIRE vector in one go
	if (encoding == ColumnEncoding::DICTIONARY && read_now == to_read && dictionary_decoder.HasFilter()) {
		if (page_is_filtered_out) {
			// the page has been filtered out entirely - skip
			approved_tuple_count = 0;
		} else {
			// Push filter into dictionary directly
			// read the defines/repeats
			const auto all_valid = PrepareRead(read_now, define_out, repeat_out, 0);
			const auto define_ptr = all_valid ? nullptr : static_cast<uint8_t *>(define_out);
			dictionary_decoder.Filter(define_ptr, read_now, result, sel, approved_tuple_count);
		}
		page_rows_available -= read_now;
		FinishRead(num_values);
		return;
	}
	// fallback to regular read + filter
	ReadInternal(num_values, define_out, repeat_out, result);
	ApplyFilter(result, filter, filter_state, num_values, sel, approved_tuple_count);
}

void ColumnReader::ApplyFilter(Vector &v, const TableFilter &filter, TableFilterState &filter_state, idx_t scan_count,
                               SelectionVector &sel, idx_t &approved_tuple_count) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(scan_count, vdata);
	ColumnSegment::FilterSelection(sel, v, vdata, filter, filter_state, scan_count, approved_tuple_count);
}

void ColumnReader::Skip(idx_t num_values) {
	pending_skips += num_values;
}

void ColumnReader::ApplyPendingSkips(data_ptr_t define_out, data_ptr_t repeat_out) {
	if (pending_skips == 0) {
		return;
	}
	idx_t num_values = pending_skips;
	pending_skips = 0;

	auto to_skip = num_values;
	// start reading but do not apply skips (we are skipping now)
	BeginRead(nullptr, nullptr);

	while (to_skip > 0) {
		auto skip_now = ReadPageHeaders(to_skip);
		if (page_is_filtered_out) {
			// the page has been filtered out entirely - skip
			page_rows_available -= skip_now;
			to_skip -= skip_now;
			continue;
		}
		const auto all_valid = PrepareRead(skip_now, define_out, repeat_out, 0);

		const auto define_ptr = all_valid ? nullptr : static_cast<uint8_t *>(define_out);
		switch (encoding) {
		case ColumnEncoding::DICTIONARY:
			dictionary_decoder.Skip(define_ptr, skip_now);
			break;
		case ColumnEncoding::DELTA_BINARY_PACKED:
			delta_binary_packed_decoder.Skip(define_ptr, skip_now);
			break;
		case ColumnEncoding::RLE:
			rle_decoder.Skip(define_ptr, skip_now);
			break;
		case ColumnEncoding::DELTA_LENGTH_BYTE_ARRAY:
			delta_length_byte_array_decoder.Skip(define_ptr, skip_now);
			break;
		case ColumnEncoding::DELTA_BYTE_ARRAY:
			delta_byte_array_decoder.Skip(define_ptr, skip_now);
			break;
		case ColumnEncoding::BYTE_STREAM_SPLIT:
			byte_stream_split_decoder.Skip(define_ptr, skip_now);
			break;
		default:
			PlainSkip(*block, define_ptr, skip_now);
			break;
		}
		page_rows_available -= skip_now;
		to_skip -= skip_now;
	}
	FinishRead(num_values);
}

//===--------------------------------------------------------------------===//
// Create Column Reader
//===--------------------------------------------------------------------===//
template <class T>
static unique_ptr<ColumnReader> CreateDecimalReader(ParquetReader &reader, const ParquetColumnSchema &schema) {
	switch (schema.type.InternalType()) {
	case PhysicalType::INT16:
		return make_uniq<TemplatedColumnReader<int16_t, TemplatedParquetValueConversion<T>>>(reader, schema);
	case PhysicalType::INT32:
		return make_uniq<TemplatedColumnReader<int32_t, TemplatedParquetValueConversion<T>>>(reader, schema);
	case PhysicalType::INT64:
		return make_uniq<TemplatedColumnReader<int64_t, TemplatedParquetValueConversion<T>>>(reader, schema);
	case PhysicalType::INT128:
		return make_uniq<TemplatedColumnReader<hugeint_t, TemplatedParquetValueConversion<T>>>(reader, schema);
	default:
		throw NotImplementedException("Unimplemented internal type for CreateDecimalReader");
	}
}

unique_ptr<ColumnReader> ColumnReader::CreateReader(ParquetReader &reader, const ParquetColumnSchema &schema) {
	switch (schema.type.id()) {
	case LogicalTypeId::BOOLEAN:
		return make_uniq<BooleanColumnReader>(reader, schema);
	case LogicalTypeId::UTINYINT:
		return make_uniq<TemplatedColumnReader<uint8_t, TemplatedParquetValueConversion<uint32_t>>>(reader, schema);
	case LogicalTypeId::USMALLINT:
		return make_uniq<TemplatedColumnReader<uint16_t, TemplatedParquetValueConversion<uint32_t>>>(reader, schema);
	case LogicalTypeId::UINTEGER:
		return make_uniq<TemplatedColumnReader<uint32_t, TemplatedParquetValueConversion<uint32_t>>>(reader, schema);
	case LogicalTypeId::UBIGINT:
		return make_uniq<TemplatedColumnReader<uint64_t, TemplatedParquetValueConversion<uint64_t>>>(reader, schema);
	case LogicalTypeId::TINYINT:
		return make_uniq<TemplatedColumnReader<int8_t, TemplatedParquetValueConversion<int32_t>>>(reader, schema);
	case LogicalTypeId::SMALLINT:
		return make_uniq<TemplatedColumnReader<int16_t, TemplatedParquetValueConversion<int32_t>>>(reader, schema);
	case LogicalTypeId::INTEGER:
		return make_uniq<TemplatedColumnReader<int32_t, TemplatedParquetValueConversion<int32_t>>>(reader, schema);
	case LogicalTypeId::BIGINT:
		return make_uniq<TemplatedColumnReader<int64_t, TemplatedParquetValueConversion<int64_t>>>(reader, schema);
	case LogicalTypeId::FLOAT:
		if (schema.type_info == ParquetExtraTypeInfo::FLOAT16) {
			return make_uniq<CallbackColumnReader<uint16_t, float, Float16ToFloat32>>(reader, schema);
		}
		return make_uniq<TemplatedColumnReader<float, TemplatedParquetValueConversion<float>>>(reader, schema);
	case LogicalTypeId::DOUBLE:
		if (schema.type_info == ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY) {
			return ParquetDecimalUtils::CreateReader(reader, schema);
		}
		return make_uniq<TemplatedColumnReader<double, TemplatedParquetValueConversion<double>>>(reader, schema);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		switch (schema.type_info) {
		case ParquetExtraTypeInfo::IMPALA_TIMESTAMP:
			return make_uniq<CallbackColumnReader<Int96, timestamp_t, ImpalaTimestampToTimestamp>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_MS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampMsToTimestamp>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_MICROS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampMicrosToTimestamp>>(reader,
			                                                                                                schema);
		case ParquetExtraTypeInfo::UNIT_NS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampNsToTimestamp>>(reader, schema);
		default:
			throw InternalException("TIMESTAMP requires type info");
		}
	case LogicalTypeId::TIMESTAMP_NS:
		switch (schema.type_info) {
		case ParquetExtraTypeInfo::IMPALA_TIMESTAMP:
			return make_uniq<CallbackColumnReader<Int96, timestamp_ns_t, ImpalaTimestampToTimestampNS>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_MS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_ns_t, ParquetTimestampMsToTimestampNs>>(reader,
			                                                                                                 schema);
		case ParquetExtraTypeInfo::UNIT_MICROS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_ns_t, ParquetTimestampUsToTimestampNs>>(reader,
			                                                                                                 schema);
		case ParquetExtraTypeInfo::UNIT_NS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_ns_t, ParquetTimestampNsToTimestampNs>>(reader,
			                                                                                                 schema);
		default:
			throw InternalException("TIMESTAMP_NS requires type info");
		}
	case LogicalTypeId::DATE:
		return make_uniq<CallbackColumnReader<int32_t, date_t, ParquetIntToDate>>(reader, schema);
	case LogicalTypeId::TIME:
		switch (schema.type_info) {
		case ParquetExtraTypeInfo::UNIT_MS:
			return make_uniq<CallbackColumnReader<int32_t, dtime_t, ParquetMsIntToTime>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_MICROS:
			return make_uniq<CallbackColumnReader<int64_t, dtime_t, ParquetIntToTime>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_NS:
			return make_uniq<CallbackColumnReader<int64_t, dtime_t, ParquetNsIntToTime>>(reader, schema);
		default:
			throw InternalException("TIME requires type info");
		}
	case LogicalTypeId::TIME_NS:
		switch (schema.type_info) {
		case ParquetExtraTypeInfo::UNIT_MS:
			return make_uniq<CallbackColumnReader<int32_t, dtime_ns_t, ParquetMsIntToTimeNs>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_MICROS:
			return make_uniq<CallbackColumnReader<int64_t, dtime_ns_t, ParquetUsIntToTimeNs>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_NS:
			return make_uniq<CallbackColumnReader<int64_t, dtime_ns_t, ParquetIntToTimeNs>>(reader, schema);
		default:
			throw InternalException("TIME requires type info");
		}
	case LogicalTypeId::TIME_TZ:
		switch (schema.type_info) {
		case ParquetExtraTypeInfo::UNIT_MS:
			return make_uniq<CallbackColumnReader<int32_t, dtime_tz_t, ParquetIntToTimeMsTZ>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_MICROS:
			return make_uniq<CallbackColumnReader<int64_t, dtime_tz_t, ParquetIntToTimeTZ>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_NS:
			return make_uniq<CallbackColumnReader<int64_t, dtime_tz_t, ParquetIntToTimeNsTZ>>(reader, schema);
		default:
			throw InternalException("TIME_TZ requires type info");
		}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return make_uniq<StringColumnReader>(reader, schema);
	case LogicalTypeId::DECIMAL:
		// we have to figure out what kind of int we need
		switch (schema.type_info) {
		case ParquetExtraTypeInfo::DECIMAL_INT32:
			return CreateDecimalReader<int32_t>(reader, schema);
		case ParquetExtraTypeInfo::DECIMAL_INT64:
			return CreateDecimalReader<int64_t>(reader, schema);
		case ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY:
			return ParquetDecimalUtils::CreateReader(reader, schema);
		default:
			throw NotImplementedException("Unrecognized Parquet type for Decimal");
		}
		break;
	case LogicalTypeId::UUID:
		return make_uniq<UUIDColumnReader>(reader, schema);
	case LogicalTypeId::INTERVAL:
		return make_uniq<IntervalColumnReader>(reader, schema);
	case LogicalTypeId::SQLNULL:
		return make_uniq<NullColumnReader>(reader, schema);
	default:
		break;
	}
	throw NotImplementedException(schema.type.ToString());
}

} // namespace duckdb
