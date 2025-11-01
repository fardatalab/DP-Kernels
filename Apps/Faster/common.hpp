#pragma once

#include "faster.h"
#include "null_disk.h"
#include <cstdint>
#include <cstdio>
#include <thread.h>

#define KB (1024)
#define MB (1024 * KB)
#define GB (1024 * MB)

#define UNCOMPRESSED_SIZE (4 * KB)
#define COMPRESSED_SIZE (2 * KB)

#define N_THREADS 1
#define PORT_BASE 10000

#define INPUT_FILE "/home/jasonhu/4K.deflate"
#define KEYS_FILE "/data/datasets/ycsb/load_uniform_250M_raw.dat"
// typedef FASTER::environment::QueueIoHandler handler_t;
// constexpr static uint64_t kFileSegmentSize = (1 << 30ULL); // 1GiB file segments

using namespace FASTER::core;
// NULL DISK is for in memory only FASTER store
typedef FASTER::device::NullDisk Disk;

// Define key type
struct Key
{
    uint64_t key;

    Key(uint64_t key_) : key{key_}
    {
    }

    // Required for the hash table
    inline static constexpr uint32_t size()
    {
        return static_cast<uint32_t>(sizeof(Key));
    }
    // Hash Function for the key
    inline KeyHash GetHash() const
    {
        return KeyHash{FasterHashHelper<uint64_t>::compute(key)};
    }
    // Comparison operators.
    inline bool operator==(const Key &other) const
    {
        return key == other.key;
    }
    inline bool operator!=(const Key &other) const
    {
        return key != other.key;
    }
};

// Define value type, is uncompressed
struct UncompressedValue
{
    uint8_t value_[UNCOMPRESSED_SIZE];

    UncompressedValue(const uint8_t *value)
    {
        printf("UncompressedValue() called, memcpying\n");
        std::memcpy(value_, value, UNCOMPRESSED_SIZE);
    }
    // Required for the records log
    inline static constexpr uint32_t size()
    {
        return static_cast<uint32_t>(sizeof(UncompressedValue));
    }
};

// Define value type, compressed (ACTUALLY PUT INTO FASTER STORE)
struct CompressedValue
{
    uint8_t value_[COMPRESSED_SIZE];

    CompressedValue(const uint8_t *value)
    {
        printf("CompressedValue() called, memcpying\n");
        std::memcpy(value_, value, COMPRESSED_SIZE);
    }
    // Required for the records log
    inline static constexpr uint32_t size()
    {
        return static_cast<uint32_t>(sizeof(CompressedValue));
    }
};