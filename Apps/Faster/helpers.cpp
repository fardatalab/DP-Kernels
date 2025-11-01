
#include "helpers.hpp"
#include "file_helper.hpp"
// #include "FASTER/cc/benchmark-dir/file.h"

#define LOAD_N_KEYS 1000

aligned_unique_ptr_t<uint64_t> init_keys_; // array of keys loaded from file
uint64_t kInitCount = LOAD_N_KEYS;         // number of keys to load

size_t load_keys(const std::string &load_filename, bool vanilla)
{
    // keep this part in server.exe or server mode
    constexpr size_t kFileChunkSize = 131072;

    auto chunk_guard = alloc_aligned<uint64_t>(512, kFileChunkSize);
    uint64_t *chunk = chunk_guard.get();

    BenchmarkFile init_file{load_filename};

    printf("loading keys from %s into memory...\n", load_filename.c_str());

    init_keys_ = alloc_aligned<uint64_t>(64, kInitCount * sizeof(uint64_t));
    uint64_t count = 0;

    uint64_t offset = 0;
    while (true)
    {
        uint64_t size = init_file.Read(chunk, kFileChunkSize, offset);
        for (uint64_t idx = 0; idx < size / 8; ++idx)
        {
            init_keys_.get()[count] = chunk[idx];
            ++count;
            if (count == kInitCount)
            {
                break;
            }
        }
        if (count == kInitCount)
        {
            break;
        }
        if (size == kFileChunkSize)
        {
            offset += kFileChunkSize;
        }
        else
        {
            break;
        }
    }
    if (kInitCount != count)
    {
        printf("Init file load fail!\n");
        exit(1);
    }

    printf("loaded %lu keys.\n", count);

    return count;
}