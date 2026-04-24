#include "decompress_deflate_sw.hpp"
#include "common.hpp"
#include "memory.hpp"
#include "memory_common.hpp"
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <libdeflate.h>
#include <sys/types.h>

static struct libdeflate_decompressor **decompressors;

static int n_cpus = 0;

static uint32_t __get_max_single_task_size()
{
    // NO actual ceiling
    return 1 * GB;
}

static const float tuning_param = 1.05 * 2;
static long __get_estimated_processing_time(uint32_t input_size, int thread_id)
{
#ifndef ENABLE_SCHEDULING
    return (float)input_size / (0.00001 * MB) * NANOSECONDS_PER_SECOND;
#endif
    switch (input_size)
    {
    case 51: { // 64B output
        return tuning_param * (float)input_size / (52.868751 * MB) * NANOSECONDS_PER_SECOND;
    }
    case 531: { // 1KB output
        return tuning_param * (float)input_size / (235.2130523 * MB) * NANOSECONDS_PER_SECOND;
    }
    case 1647: { // 4KB output
        return tuning_param * (float)input_size / (365.0443637 * MB) * NANOSECONDS_PER_SECOND;
    }
    default: {
        printf("sw_decompress_deflate no histogram for input size: %d, assuming full speed\n", input_size);
        return 2 * (float)input_size / (365.0443637 * MB) * NANOSECONDS_PER_SECOND;
    }
    }
    // return ((long)input_size / (0.001 * MB)) * NANOSECONDS_PER_SECOND;
}

static bool __is_coalescing_enabled()
{
    return false;
}

bool sw_decompress_deflate_get_catalogue(dpm_kernel_catalogue *catalogue)
{
    // set info in catalogue
    // catalogue->is_coalescing_enabled = __is_coalescing_enabled;
    catalogue->is_coalescing_enabled = __is_coalescing_enabled();
    // catalogue->get_max_single_task_size = __get_max_single_task_size;
    catalogue->max_single_task_size = __get_max_single_task_size();
    catalogue->get_estimated_processing_time_ns = __get_estimated_processing_time;

    return true;
}

bool sw_decompress_deflate_kernel_init(struct dpm_kernel_catalogue *catalogue, int n_threads)
{
    // set info in catalogue
    // catalogue->is_coalescing_enabled = __is_coalescing_enabled;
    catalogue->is_coalescing_enabled = __is_coalescing_enabled();
    // catalogue->get_max_single_task_size = __get_max_single_task_size;
    catalogue->max_single_task_size = __get_max_single_task_size();
    catalogue->get_estimated_processing_time_ns = __get_estimated_processing_time;

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
        else
        {
            printf("libdeflate decompression failed with error: %d\n", result);
        }
        return DPK_ERROR_FAILED;
    }
    dpk_task->actual_out_size = actual_out_size;

    // synchronous completion, no polling needed
    dpk_task->completion.store(DPK_SUCCESS, std::memory_order_release);
    return DPK_SUCCESS;
}
