
#include "helpers.hpp"
#include "common.hpp"
#include "file_helper.hpp"

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

bool read_file(const char *filename, uint8_t **buffer, size_t *buffer_size)
{
    FILE *file = fopen(filename, "rb");
    if (!file)
    {
        perror("Failed to open input file");
        return false;
    }

    // get the file size
    fseek(file, 0, SEEK_END);
    size_t file_size = ftell(file);
    fseek(file, 0, SEEK_SET); // reset the file pointer to the beginning
    // allocate buffer
    *buffer = (uint8_t *)malloc(file_size);
    if (!*buffer)
    {
        perror("Failed to allocate memory for input buffer");
    }

    // read the file into the buffer
    size_t bytes_read = fread(*buffer, 1, file_size, file);
    if (bytes_read < file_size)
    {
        if (ferror(file))
        {
            perror("Failed to read input file");
            free(*buffer);
            fclose(file);
            return false;
        }
    }

    *buffer_size = bytes_read;

    fclose(file);
    return true;
}