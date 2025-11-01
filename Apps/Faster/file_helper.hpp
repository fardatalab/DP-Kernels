// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <string>

#ifdef _WIN32
#define NOMINMAX
#define _WINSOCKAPI_
#include <Windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif


/// Basic wrapper around synchronous file read.
class BenchmarkFile
{
  public:
  BenchmarkFile(const std::string &filename)
    {
#ifdef _WIN32
        file_handle_ = ::CreateFileA(filename.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                                     FILE_FLAG_NO_BUFFERING, nullptr);
#else
        fd_ = ::open(filename.c_str(), O_RDONLY | O_DIRECT, S_IRUSR);
#endif
    }

    ~BenchmarkFile()
    {
#ifdef _WIN32
        ::CloseHandle(file_handle_);
#else
        ::close(fd_);
#endif
    }

    size_t Read(void *buf, size_t count, uint64_t offset)
    {
#ifdef _WIN32
        DWORD bytes_read{0};
        ::ReadFile(file_handle_, buf, static_cast<DWORD>(count), &bytes_read, nullptr);
        return bytes_read;
#else
        return ::pread(fd_, buf, count, offset);
#endif
    }

  private:
#ifdef _WIN32
    HANDLE file_handle_;
#else
    int fd_;
#endif
};

// #ifdef _WIN32
// #include "file_windows.h"
// #else
// #include "file_linux.h"
// #endif

#include <filesystem>

// namespace FASTER
// {
// namespace environment
// {

// inline std::string NormalizePath(const std::string &path)
// {
//     // Append a separator to the end of 'path' if not already present
//     static std::string sep{'/'};
//     std::string new_path{path};
//     if (path.size() < sep.size() || path.compare(path.size() - sep.size(), path.size(), sep) != 0)
//     {
//         new_path += sep;
//     }
//     return new_path;
// }
// inline std::string NormalizeAndCreatePath(const std::string &path)
// {
//     auto new_path = NormalizePath(path);
//     std::filesystem::create_directories(new_path);
//     return new_path;
// }

// } // namespace environment
// } // namespace FASTER