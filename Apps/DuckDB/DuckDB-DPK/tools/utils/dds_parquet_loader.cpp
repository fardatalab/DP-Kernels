/**
 * @file dds_parquet_loader.cpp
 * @brief Utility to load local Parquet files into the DDS POSIX storage used by DuckDB.
 *
 * Purpose:
 *  - Recursively discover ``.parquet`` files under the directory specified by the
 *    environment variable ``DDS_PARQUET_DIR``.
 *  - Copy each discovered file into DDS using the original file's basename as a flat
 *    DDS filename (no directory structure preserved).
 *  - Ensure writes conform to the DDS alignment requirement (512-byte aligned offsets
 *    and write sizes). If the source's final write is not 512-byte aligned, the loader
 *    pads the final block and then truncates the DDS object back to the exact source size
 *    so Parquet footer offsets remain valid.
 *  - Abort on basename collisions to avoid accidental overwrites (DDS uses a flat namespace).
 *
 * Usage:
 *  - Build DuckDB with DDS POSIX support (e.g., ``-DDUCKDB_USE_DDS_POSIX=1``).
 *  - Set ``DDS_PARQUET_DIR`` to the directory to scan and run this tool. Progress and
 *    errors are printed to stdout/stderr; the process returns non-zero on failure.
 *
 * Warnings & limitations:
 *  - DDS has a flat namespace: files that share the same basename will conflict. Resolve
 *    collisions before running this loader.
 *  - Ensure the running user has appropriate rights to the DDS backend and the source files.
 */

#include "DDSPosix.h"

#include <cerrno>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

/**
 * Ensures DDS POSIX is shutdown so background poller threads are joined cleanly.
 */
struct DDSPosixShutdownGuard {
	~DDSPosixShutdownGuard() {
		// DDS uses a background poller thread; shutdown avoids std::terminate on exit.
		DDSPosix::shutdown_posix();
	}
};

/**
 * Extracts the DDS flat filename used by DuckDB (last path component only).
 * This intentionally keeps basename-only behavior to match NormalizeDDSPath.
 */
static std::string ExtractDDSName(const std::string &path) {
	if (path.empty()) {
		return std::string();
	}
	// Original behavior stripped a file:/ prefix for URL-like paths.
	// if (normalized.rfind("file:/", 0) == 0) {
	// 	normalized.erase(0, 6);
	// 	while (!normalized.empty() && normalized[0] == '/') {
	// 		normalized.erase(0, 1);
	// 	}
	// }
	// Updated: keep basename-only mapping (no URL prefix handling) to match NormalizeDDSPath.
	std::string normalized = path;
	const auto slash_pos = normalized.find_last_of("/\\");
	if (slash_pos == std::string::npos) {
		return normalized;
	}
	return normalized.substr(slash_pos + 1);
}

/**
 * Returns true if the path ends with ".parquet" (case-insensitive).
 */
static bool IsParquetFile(const std::string &path) {
	constexpr const char *suffix = ".parquet";
	if (path.size() < strlen(suffix)) {
		return false;
	}
	auto tail = path.substr(path.size() - strlen(suffix));
	for (auto &ch : tail) {
		ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
	}
	return tail == suffix;
}

/**
 * Recursively collects parquet files under the given directory.
 */
static void CollectParquetFiles(const std::string &dir, std::vector<std::string> &out_files) {
	DIR *handle = opendir(dir.c_str());
	if (!handle) {
		std::fprintf(stderr, "[DDS-LOADER] Failed to open directory \"%s\": %s\n", dir.c_str(), std::strerror(errno));
		return;
	}
	while (true) {
		errno = 0;
		auto *entry = readdir(handle);
		if (!entry) {
			break;
		}
		if (std::strcmp(entry->d_name, ".") == 0 || std::strcmp(entry->d_name, "..") == 0) {
			continue;
		}
		std::string full_path = dir;
		if (!full_path.empty() && full_path.back() != '/') {
			full_path.push_back('/');
		}
		full_path.append(entry->d_name);

		struct stat st;
		if (stat(full_path.c_str(), &st) != 0) {
			std::fprintf(stderr, "[DDS-LOADER] Failed to stat \"%s\": %s\n", full_path.c_str(), std::strerror(errno));
			continue;
		}
		if (S_ISDIR(st.st_mode)) {
			CollectParquetFiles(full_path, out_files);
			continue;
		}
		if (S_ISREG(st.st_mode) && IsParquetFile(full_path)) {
			out_files.push_back(full_path);
		}
	}
	closedir(handle);
}

/**
 * Reads a local parquet file and writes it to DDS using the flat DDS filename.
 */
static bool LoadParquetIntoDDS(const std::string &source_path, const std::string &dds_name) {
	constexpr size_t kBufferSize = 1 * 1024 * 1024;
	constexpr size_t kDDSAlignment = 512;
	std::vector<uint8_t> buffer(kBufferSize);
	std::vector<uint8_t> staging;
	staging.reserve(kBufferSize + kDDSAlignment);
	std::vector<uint8_t> pending;
	pending.reserve(kDDSAlignment);

	int src_fd = open(source_path.c_str(), O_RDONLY | O_CLOEXEC);
	if (src_fd < 0) {
		std::fprintf(stderr, "[DDS-LOADER] Failed to open source \"%s\": %s\n", source_path.c_str(),
		             std::strerror(errno));
		return false;
	}
	struct stat src_stat;
	if (fstat(src_fd, &src_stat) != 0) {
		std::fprintf(stderr, "[DDS-LOADER] Failed to stat source \"%s\": %s\n", source_path.c_str(),
		             std::strerror(errno));
		close(src_fd);
		return false;
	}
	const auto source_size = static_cast<uint64_t>(src_stat.st_size);

	int dds_fd = DDSPosix::open(dds_name.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0666);
	if (dds_fd < 0) {
		std::fprintf(stderr, "[DDS-LOADER] Failed to open DDS \"%s\": %s\n", dds_name.c_str(), std::strerror(errno));
		close(src_fd);
		return false;
	}

	uint64_t offset = 0;
	bool padded_tail = false;
	while (true) {
		const auto bytes_read = read(src_fd, buffer.data(), buffer.size());
		if (bytes_read < 0) {
			std::fprintf(stderr, "[DDS-LOADER] Failed to read \"%s\": %s\n", source_path.c_str(),
			             std::strerror(errno));
			DDSPosix::close(dds_fd);
			close(src_fd);
			return false;
		}
		if (bytes_read == 0) {
			break;
		}
		// Original direct pwrite path (kept for reference) did not enforce DDS alignment.
		// const auto bytes_written =
		//     DDSPosix::pwrite(dds_fd, buffer.data(), static_cast<size_t>(bytes_read), static_cast<off_t>(offset));
		// if (bytes_written < 0 || bytes_written != bytes_read) { ... }
		// offset += static_cast<uint64_t>(bytes_written);

		// DDS quirk: only 512-byte aligned writes are supported (both size and offset).
		staging.clear();
		if (!pending.empty()) {
			staging.insert(staging.end(), pending.begin(), pending.end());
		}
		staging.insert(staging.end(), buffer.begin(), buffer.begin() + bytes_read);
		const auto aligned_bytes = (staging.size() / kDDSAlignment) * kDDSAlignment;
		if (aligned_bytes > 0) {
			const auto bytes_written =
			    DDSPosix::pwrite(dds_fd, staging.data(), aligned_bytes, static_cast<off_t>(offset));
			if (bytes_written < 0 || static_cast<size_t>(bytes_written) != aligned_bytes) {
				std::fprintf(stderr,
				             "[DDS-LOADER] Failed to write DDS \"%s\" at offset %llu: %s\n",
				             dds_name.c_str(), static_cast<unsigned long long>(offset), std::strerror(errno));
				DDSPosix::close(dds_fd);
				close(src_fd);
				return false;
			}
			offset += static_cast<uint64_t>(bytes_written);
		}
		pending.assign(staging.begin() + aligned_bytes, staging.end());
	}
	if (!pending.empty()) {
		// Pad the tail to 512 bytes, then truncate back to the original size.
		std::vector<uint8_t> padded(kDDSAlignment, 0);
		std::memcpy(padded.data(), pending.data(), pending.size());
		const auto bytes_written =
		    DDSPosix::pwrite(dds_fd, padded.data(), padded.size(), static_cast<off_t>(offset));
		if (bytes_written < 0 || static_cast<size_t>(bytes_written) != padded.size()) {
			std::fprintf(stderr,
			             "[DDS-LOADER] Failed to write padded DDS \"%s\" at offset %llu: %s\n",
			             dds_name.c_str(), static_cast<unsigned long long>(offset), std::strerror(errno));
			DDSPosix::close(dds_fd);
			close(src_fd);
			return false;
		}
		offset += static_cast<uint64_t>(bytes_written);
		padded_tail = true;
	}
	if (padded_tail) {
		// Restore the exact source size so Parquet footer offsets stay correct.
		if (DDSPosix::ftruncate(dds_fd, static_cast<off_t>(source_size)) != 0) {
			std::fprintf(stderr, "[DDS-LOADER] Failed to truncate DDS \"%s\" to size %llu: %s\n",
			             dds_name.c_str(), static_cast<unsigned long long>(source_size), std::strerror(errno));
			DDSPosix::close(dds_fd);
			close(src_fd);
			return false;
		}
	}

	DDSPosix::close(dds_fd);
	close(src_fd);
	return true;
}

int main() {
	// DDS debug: ensure shutdown happens even on early returns.
	DDSPosixShutdownGuard dds_shutdown_guard;

	const char *env_dir = std::getenv("DDS_PARQUET_DIR");
	if (!env_dir || std::strlen(env_dir) == 0) {
		std::fprintf(stderr, "[DDS-LOADER] Set DDS_PARQUET_DIR to a directory containing parquet files.\n");
		return 1;
	}

	std::vector<std::string> parquet_files;
	CollectParquetFiles(env_dir, parquet_files);
	if (parquet_files.empty()) {
		std::fprintf(stderr, "[DDS-LOADER] No parquet files found under \"%s\".\n", env_dir);
		return 1;
	}

	std::unordered_set<std::string> dds_names;
	for (const auto &path : parquet_files) {
		auto dds_name = ExtractDDSName(path);
		if (dds_name.empty()) {
			std::fprintf(stderr, "[DDS-LOADER] Skipping empty DDS name for \"%s\".\n", path.c_str());
			continue;
		}
		// DDS uses a flat namespace; bail out on name collisions to avoid accidental overwrites.
		if (!dds_names.insert(dds_name).second) {
			std::fprintf(stderr,
			             "[DDS-LOADER] Duplicate DDS name \"%s\" from \"%s\". "
			             "DDS uses a flat namespace; resolve collisions before loading.\n",
			             dds_name.c_str(), path.c_str());
			return 1;
		}
		std::printf("[DDS-LOADER] Loading \"%s\" into DDS as \"%s\"...\n", path.c_str(), dds_name.c_str());
		if (!LoadParquetIntoDDS(path, dds_name)) {
			return 1;
		}
	}

	std::printf("[DDS-LOADER] Loaded %zu parquet file(s).\n", parquet_files.size());
	return 0;
}
