#include "server.hpp"
#include "common.hpp"
#include "status.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>

FasterKv<Key, CompressedValue, Disk> faster_store{
    1 * (1 << 20),       // Hash table size (1M entries)
    1ULL * (1ULL << 30), // In-memory Log size (1 GiB)
    "",                  // Log path must be empty for in-memory store
    1.0,                 // Log Mutable fraction
};

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

int main(int argc, char *argv[])
{
    // read input file
    uint8_t *file_buffer = nullptr;
    uint8_t *buffer = (uint8_t *)malloc(COMPRESSED_SIZE);
    memset(buffer, 0, COMPRESSED_SIZE);
    size_t buffer_size = 0;
    if (!read_file(INPUT_FILE, &file_buffer, &buffer_size))
    {
        printf("Failed to read input file: %s\n", INPUT_FILE);
        return 1;
    }
    printf("Read %zu bytes from input file: %s\n", buffer_size, INPUT_FILE);
    // copy the input file data to the buffer
    memcpy(buffer, file_buffer, buffer_size);

    faster_store.StartSession();

    // allocate a buffer for upsert context data
    CompressedValue *compressed_value = (CompressedValue *)malloc(sizeof(CompressedValue));
    memset(compressed_value, 0, sizeof(CompressedValue));
    // in place new the value, this will copy the input data into CompressedValue
    new (compressed_value) CompressedValue(buffer);
    // ctx has a ptr to uc_data
    UpsertContext context{42, compressed_value};

    Status status = faster_store.Upsert(
        context,
        [](IAsyncContext *ctxt, Status result) {
            if (result != Status::Ok)
            {
                printf("Upsert failed with status: %s\n", StatusStr(result));
            }
            else
            {
                printf("Upsert succeeded.\n");
            }
        },
        1);
    if (status != Status::Ok)
    {
        printf("Failed to upsert data: %s\n", StatusStr(status));
        free(buffer);
        free(compressed_value);
        return 1;
    }
    else
    {
        printf("Upsert operation successful\n");
    }
    faster_store.CompletePending(true);

    // do a read
    // uint8_t *read_buffer = (uint8_t *)malloc(CompressedValue::size());
    CompressedValue *compressed_read_value = (CompressedValue *)malloc(sizeof(CompressedValue));
    memset(compressed_read_value, 0, sizeof(CompressedValue));
    printf("compressed_read_value == %p\n", compressed_read_value);
    // new (compressed_read_value) CompressedValue(read_buffer);
    ReadContext read_context{42, compressed_read_value};
    status = faster_store.Read(
        read_context,
        [](IAsyncContext *ctxt, Status result) {
            if (result != Status::Ok)
            {
                printf("Read failed with status: %s\n", StatusStr(result));
            }
            else
            {
                CallbackContext<ReadContext> context{ctxt};
                printf("Read succeeded, key: %lu\n", context->key().key);
                CompressedValue *read_value = context->GetValue();
                printf("read_value == %p\n", read_value);
                printf("Read value: ");
                read_value->value_[COMPRESSED_SIZE - 1] = '\0';
                printf("%s\n", read_value->value_);
            }
        },
        1);
    if (status != Status::Ok)
    {
        printf("Failed to read data: %s\n", StatusStr(status));
        free(buffer);
        free(compressed_value);
        // free(read_buffer);
        free(compressed_read_value);
        return 1;
    }
    else
    {
        printf("Read operation successful\n");
        CompressedValue *read_value = read_context.GetValue();
        printf("read_value == %p\n", read_value);
        printf("Read value: ");
        read_value->value_[COMPRESSED_SIZE - 1] = '\0';
        printf("%s\n", read_value->value_);
        int is_equal = std::memcmp(compressed_value->value_, read_value->value_, COMPRESSED_SIZE - 1) == 0;
        printf("Is read value equal to upserted value? %s\n", is_equal ? "Yes" : "No");
        is_equal = std::memcmp(buffer, read_value->value_, buffer_size) == 0;
        printf("Is read value equal to input file value? %s\n", is_equal ? "Yes" : "No");
    }

    faster_store.StopSession();

    return 0;
}