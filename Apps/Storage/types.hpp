#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "dpm_interface.hpp"

#define N_SERVER_THREADS 1
#define N_CLIENT_THREADS 1

#define __DEFAULT_NUM_REQUESTS_TO_SEND 128 // 100'000 per thread

#define __UNCOMPRESSED_SIZE 4096
#define __COMPRESSED_SIZE 1647

#define __VALUE_SIZE __COMPRESSED_SIZE //__UNCOMPRESSED_SIZE

#define __LOCAL_CAPACITY (1 * GB) // 1_GiB
#define __DATABASE_SIZE __LOCAL_CAPACITY

namespace pd3
{
namespace network_hashmap
{

enum class RequestType : uint8_t
{
    kGet = 0,
    kPut = 1,
};

static constexpr uint64_t RECORD_SIZE = __UNCOMPRESSED_SIZE;
using ValueType = char[RECORD_SIZE];

struct Request
{
    RequestType type;
    uint64_t key;
    bool local;
} __attribute__((packed));

struct PendingDecompressionOp
{
    uint64_t key;                // To identify the original request
    dpkernel_task *pending_task; // Context from your async decompression library
};

struct Response
{
    uint64_t key;
    ValueType value;
} __attribute__((packed));

struct MemoryBuffer
{
    uint64_t address;
    uint64_t rkey;
} __attribute__((packed));

struct RemoteMemoryDetails
{
    uint64_t num_entries;
    MemoryBuffer buffers[16];
} __attribute__((packed));

} // namespace network_hashmap
} // namespace pd3