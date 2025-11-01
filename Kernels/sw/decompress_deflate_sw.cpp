#include "decompress_deflate_sw.hpp"
#include "common.hpp"
#include "memory.hpp"
#include <cstddef>
#include <cstdio>
#include <libdeflate.h>

static struct libdeflate_decompressor **decompressors;

static int n_cpus = 0;

bool sw_decompress_deflate_kernel_init(struct dpm_kernel_catalogue *result, int n_threads)
{
    n_cpus = n_threads;
    decompressors = (struct libdeflate_decompressor **)malloc(n_threads * sizeof(*decompressors));

    for (int i = 0; i < n_cpus; i++)
    {
        decompressors[i] = libdeflate_alloc_decompressor();
        if (!decompressors[i])
        {
            fprintf(stderr, "Failed to allocate decompressor\n");
            return false;
        }
    }

    return true;
}

bool sw_decompress_deflate_kernel_cleanup()
{
    for (int i = 0; i < n_cpus; i++)
    {
        if (decompressors[i])
        {
            libdeflate_free_decompressor(decompressors[i]);
            decompressors[i] = NULL;
        }
    }
    free(decompressors);
    decompressors = NULL;

    return true;
}

dpkernel_error sw_decompress_deflate_kernel_execute(dpkernel_task *task, int thread_id)
{
    dpkernel_task_base *dpk_task = &task->base;

    struct libdeflate_decompressor *decompressor = decompressors[thread_id];

    char *in_buf = dpm_get_input_ptr_from_shmptr(dpk_task->in);
    char *out_buf = dpm_get_output_ptr_from_shmptr(dpk_task->out);
    size_t actual_out_size = 0;

    enum libdeflate_result result = libdeflate_deflate_decompress(decompressor, in_buf, dpk_task->in_size, out_buf,
                                                                  dpk_task->out_size, &actual_out_size);

    if (result != LIBDEFLATE_SUCCESS)
    {
        if (result == LIBDEFLATE_INSUFFICIENT_SPACE)
        {
            printf("Insufficient space for decompression, out size = %u\n", dpk_task->out_size);
        }
        return DPK_ERROR_FAILED;
    }
    dpk_task->actual_out_size = actual_out_size;

    // synchronous completion, no polling needed
    dpk_task->completion.store(DPK_SUCCESS, std::memory_order_release);
    return DPK_SUCCESS;
}
