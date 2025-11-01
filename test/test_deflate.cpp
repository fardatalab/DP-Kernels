#include "libdeflate.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#define INPUT_FILE "/home/jasonhu/4K.deflate"
#define OUTPUT_FILE "4K.out"
#define OUTPUT_SIZE 4096 // 4K

// Get file size using stat
static size_t get_file_size(const char *filename)
{
    struct stat st;
    if (stat(filename, &st) != 0)
    {
        return 0;
    }
    return st.st_size;
}

int main()
{
    // Get input file size
    size_t input_size = get_file_size(INPUT_FILE);
    if (input_size == 0)
    {
        fprintf(stderr, "Failed to get input file size\n");
        return 1;
    }

    // Allocate input buffer
    void *input_buf = malloc(input_size);
    if (!input_buf)
    {
        fprintf(stderr, "Failed to allocate input buffer\n");
        return 1;
    }

    // Read input file
    FILE *in_fp = fopen(INPUT_FILE, "rb");
    if (!in_fp)
    {
        fprintf(stderr, "Failed to open input file\n");
        free(input_buf);
        return 1;
    }
    if (fread(input_buf, 1, input_size, in_fp) != input_size)
    {
        fprintf(stderr, "Failed to read input file\n");
        fclose(in_fp);
        free(input_buf);
        return 1;
    }
    fclose(in_fp);

    // Allocate output buffer
    void *output_buf = malloc(OUTPUT_SIZE);
    if (!output_buf)
    {
        fprintf(stderr, "Failed to allocate output buffer\n");
        free(input_buf);
        return 1;
    }

    // Create decompressor
    struct libdeflate_decompressor *decompressor = libdeflate_alloc_decompressor();
    if (!decompressor)
    {
        fprintf(stderr, "Failed to allocate decompressor\n");
        free(output_buf);
        free(input_buf);
        return 1;
    }

    // Perform decompression
    enum libdeflate_result result =
        libdeflate_deflate_decompress(decompressor, input_buf, input_size, output_buf, OUTPUT_SIZE, NULL);

    if (result != LIBDEFLATE_SUCCESS)
    {
        fprintf(stderr, "Decompression failed with error code: %d\n", result);
        libdeflate_free_decompressor(decompressor);
        free(output_buf);
        free(input_buf);
        return 1;
    }

    // Write output file
    FILE *out_fp = fopen(OUTPUT_FILE, "wb");
    if (!out_fp)
    {
        fprintf(stderr, "Failed to open output file\n");
        libdeflate_free_decompressor(decompressor);
        free(output_buf);
        free(input_buf);
        return 1;
    }
    if (fwrite(output_buf, 1, OUTPUT_SIZE, out_fp) != OUTPUT_SIZE)
    {
        fprintf(stderr, "Failed to write output file\n");
        fclose(out_fp);
        libdeflate_free_decompressor(decompressor);
        free(output_buf);
        free(input_buf);
        return 1;
    }
    fclose(out_fp);

    // Clean up
    libdeflate_free_decompressor(decompressor);
    free(output_buf);
    free(input_buf);

    printf("Decompression completed successfully\n");
    return 0;
}
