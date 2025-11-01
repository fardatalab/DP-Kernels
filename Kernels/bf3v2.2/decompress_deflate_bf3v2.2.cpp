#include "decompress_deflate_bf3v2.2.hpp"
#include "bf3v2.2_device.hpp"
#include "common.hpp"
#include "doca_buf.h"
#include "doca_buf_inventory.h"
#include "doca_compress.h"
#include "doca_dev.h"
#include "doca_error.h"
#include "memory.hpp"
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <pthread.h>

static struct dpm_doca_decompress_deflate_bf3v2_2 bf3v2_2_decompress_deflate_state;

/// how many CPUs/threads on which the kernel execution etc. may be executed. Set upon kernel init.
static int n_cpus = 0;

/// array of counters for each hw kernel queue
static uint32_t *kernel_queue_remaining_capacity;

/// spin lock to protect doca_buf_inventory_buf_get_by_addr
// pthread_spinlock_t buf_inventory_lock;

bool bf3v2_2_decompress_deflate_handle_mem_req(struct dpm_mem_req *req)
{
    doca_error_t ret;
    dpkernel_task_bf3v2_2 *task;
    // uint16_t refcnt;

    switch (req->type)
    {
    case DPM_MEM_REQ_ALLOC_INPUT: {
        task = (dpkernel_task_bf3v2_2 *)dpm_get_task_ptr_from_shmptr(req->task);
        print_debug("bf3 decompress deflate handle mem req alloc input\n");
        char *input_buffer = dpm_get_input_ptr_from_shmptr(task->in);

        pthread_spin_lock(&dpm_doca_state.buf_inventory_lock);
        ret = doca_buf_inventory_buf_by_addr(bf3v2_2_decompress_deflate_state.global_doca_state->state.buf_inv,
                                             bf3v2_2_decompress_deflate_state.global_doca_state->state.src_mmap,
                                             input_buffer, task->in_size, &task->src_doca_buf);
        pthread_spin_unlock(&dpm_doca_state.buf_inventory_lock);
        if (ret != DOCA_SUCCESS) [[unlikely]]
        {
            printf("bf3 decompress deflate failed to get input buf from inventory: %s\n", doca_get_error_name(ret));
            return false;
        }
        ret = doca_buf_set_data(task->src_doca_buf, input_buffer, task->in_size);
        if (ret != DOCA_SUCCESS) [[unlikely]]
        {
            printf("bf3 decompress deflate failed to set input buf data: %s\n", doca_get_error_name(ret));
            return false;
        }
        return true;

        // break;
    }
    case DPM_MEM_REQ_ALLOC_OUTPUT: {
        task = (dpkernel_task_bf3v2_2 *)dpm_get_task_ptr_from_shmptr(req->task);
        print_debug("bf3 decompress deflate handle mem req alloc output\n");
        pthread_spin_lock(&dpm_doca_state.buf_inventory_lock);
        ret = doca_buf_inventory_buf_by_addr(bf3v2_2_decompress_deflate_state.global_doca_state->state.buf_inv,
                                             bf3v2_2_decompress_deflate_state.global_doca_state->state.dst_mmap,
                                             dpm_get_output_ptr_from_shmptr(task->out), task->out_size,
                                             &task->dst_doca_buf);
        pthread_spin_unlock(&dpm_doca_state.buf_inventory_lock);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 decompress deflate failed to get output buf from inventory: %s\n", doca_get_error_name(ret));
            return false;
        }
        return true;
        // break;
    }
    case DPM_MEM_REQ_ALLOC_COALESCED: {
        task = &((dpkernel_task *)(req->task))->bf3v2_2;
        print_debug("bf3 decompress deflate handle mem req alloc coalesced\n");
        // allocate a coalesced buffer for both input and output
        // the in/out buffer is now an actual buffer pointer, not a shm pointer
        // in/out size is the coalesced size
        pthread_spin_lock(&dpm_doca_state.buf_inventory_lock);
        ret = doca_buf_inventory_buf_by_addr(bf3v2_2_decompress_deflate_state.global_doca_state->state.buf_inv,
                                             bf3v2_2_decompress_deflate_state.global_doca_state->state.src_mmap,
                                             dpm_get_input_ptr_from_shmptr(task->in), task->in_size, &task->src_doca_buf);
        if (ret != DOCA_SUCCESS) [[unlikely]]
        {
            printf("bf3 decompress deflate failed to get input buf for coalesced task: %s\n", doca_get_error_name(ret));
            pthread_spin_unlock(&dpm_doca_state.buf_inventory_lock);
            return false;
        }
        ret = doca_buf_inventory_buf_by_addr(bf3v2_2_decompress_deflate_state.global_doca_state->state.buf_inv,
                                             bf3v2_2_decompress_deflate_state.global_doca_state->state.dst_mmap,
                                             dpm_get_output_ptr_from_shmptr(task->out), task->out_size,
                                             &task->dst_doca_buf);
        if (ret != DOCA_SUCCESS) [[unlikely]]
        {
            printf("bf3 decompress deflate failed to get output buf for coalesced task: %s\n",
                   doca_get_error_name(ret));
            pthread_spin_unlock(&dpm_doca_state.buf_inventory_lock);
            return false;
        }
        pthread_spin_unlock(&dpm_doca_state.buf_inventory_lock);
        return true;
        // break;
    }
    case DPM_MEM_REQ_FREE_INPUT: {
        task = (dpkernel_task_bf3v2_2 *)dpm_get_task_ptr_from_shmptr(req->task);
        print_debug("bf3 decompress deflate handle mem req free input\n");
        // free the input buffer
        ret = doca_buf_refcount_rm(task->src_doca_buf, NULL);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 decompress deflate failed to free input buf: %s\n", doca_get_error_name(ret));
            return false;
        }
        print_debug("input doca buf refcnt = %d\n", refcnt);
        print_debug("freed input doca buf\n");
        task->src_doca_buf = NULL;
        return true;
        // break;
    }
    case DPM_MEM_REQ_FREE_OUTPUT: {
        task = (dpkernel_task_bf3v2_2 *)dpm_get_task_ptr_from_shmptr(req->task);
        print_debug("bf3 decompress deflate handle mem req free output\n");
        // free the output buffer
        ret = doca_buf_refcount_rm(task->dst_doca_buf, NULL);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 decompress deflate failed to free output buf: %s\n", doca_get_error_name(ret));
            return false;
        }
        print_debug("output doca buf refcnt = %d\n", refcnt);
        task->dst_doca_buf = NULL;
        print_debug("freed output doca buf\n");
        return true;
        // break;
    }
    case DPM_MEM_REQ_FREE_COALESCED: {
        // free the input and output buffers
        ret = doca_buf_refcount_rm(task->src_doca_buf, NULL);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 decompress deflate failed to free input buf: %s\n", doca_get_error_name(ret));
            return false;
        }
        print_debug("input doca buf refcnt = %d\n", refcnt);
        ret = doca_buf_refcount_rm(task->dst_doca_buf, NULL);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 decompress deflate failed to free output buf: %s\n", doca_get_error_name(ret));
            return false;
        }
        print_debug("output doca buf refcnt = %d\n", refcnt);
        task->src_doca_buf = NULL;
        task->dst_doca_buf = NULL;
        print_debug("freed input and output doca buf\n");
        return true;
    }
    case DPM_MEM_REQ_ALLOC_TASK: {
        // nothing to do
        printf("bf3 decompress deflate handle mem req alloc task, nothing to do\n");
        return true;
    }
    case DPM_MEM_REQ_FREE_TASK: {
        // nothing to do
        printf("bf3 decompress deflate handle mem req free task, nothing to do\n");
        return true;
    }
    default: {
        printf("bf3 decompress deflate kernel unknown mem req->type: %d\n", req->type);
        return false;
        // break;
    }
    }
}

uint32_t bf3v2_2_decompress_deflate_hw_kernel_remaining_capacity(int thread_id, uint32_t *max_capacity)
{
    if (max_capacity != NULL)
    {
        *max_capacity = BF3v2_2_DECOMPRESS_DEFLATE_WORKQ_DEPTH;
    }
    return kernel_queue_remaining_capacity[thread_id];
}

static uint32_t __get_max_single_task_size()
{
    uint64_t max_size;
    if (doca_compress_get_max_buf_size(
            doca_dev_as_devinfo(bf3v2_2_decompress_deflate_state.global_doca_state->state.dev),
            DOCA_DECOMPRESS_DEFLATE_JOB, &max_size) == DOCA_SUCCESS)
    {
        printf("bf3v2_2_decompress_deflate max single task size: %lu\n", max_size);
        return max_size;
    }
    else
    {
        printf("bf3v2_2_decompress_deflate failed to get max single task size failed\n");
        return 0;
    }
}

static long __get_estimated_processing_time(uint32_t input_size, int thread_id)
{
    // TODO
    // 1K throughput for 4 threads
    // return (input_size / (3922.913447 * MB)) * NANOSECONDS_PER_SECOND; // / N_DPM_THREADS
    return (input_size / (1979.043004 * MB)) * NANOSECONDS_PER_SECOND;
}

static bool __is_coalescing_enabled()
{
    return false;
}

bool bf3v2_2_decompress_deflate_get_catalogue(dpm_kernel_catalogue *catalogue)
{
    // set info in catalogue
    catalogue->is_coalescing_enabled = __is_coalescing_enabled;
    catalogue->get_max_single_task_size = __get_max_single_task_size;
    catalogue->get_estimated_processing_time_ns = __get_estimated_processing_time;

    return true;
}

bool bf3v2_2_decompress_deflate_kernel_init(struct dpm_kernel_catalogue *catalogue, int n_threads)
{
    // set info in catalogue
    catalogue->is_coalescing_enabled = __is_coalescing_enabled;
    catalogue->get_max_single_task_size = __get_max_single_task_size;
    catalogue->get_estimated_processing_time_ns = __get_estimated_processing_time;

    // set the cpu mask for the kernel
    n_cpus = n_threads;

    // CPU_ZERO(&result->cpu_mask);
    // for (int i = 0; i < 32; ++i)
    // {
    //     if ((BF3v2_2_DECOMPRESS_DEFLATE_CPU_MASK >> i) & 1)
    //     {
    //         CPU_SET(i, &result->cpu_mask);
    //         n_cpus++;
    //     }
    // }
    printf("bf3v2_2_decompress_deflate_kernel_init cpu mask: %x, n_cpus = %d\n", BF3v2_2_DECOMPRESS_DEFLATE_CPU_MASK,
           n_cpus);

    // malloc the multithreaded ctxs etc
    kernel_queue_remaining_capacity = (uint32_t *)calloc(n_cpus, sizeof(uint32_t));
    bf3v2_2_decompress_deflate_state.compresses =
        (struct doca_compress **)calloc(n_cpus, sizeof(struct doca_compress *));
    bf3v2_2_decompress_deflate_state.workqs = (struct doca_workq **)calloc(n_cpus, sizeof(struct doca_workq *));
    bf3v2_2_decompress_deflate_state.ctxs = (struct doca_ctx **)calloc(n_cpus, sizeof(struct doca_ctx *));
    if (bf3v2_2_decompress_deflate_state.compresses == NULL || bf3v2_2_decompress_deflate_state.ctxs == NULL ||
        bf3v2_2_decompress_deflate_state.workqs == NULL || kernel_queue_remaining_capacity == NULL)
    {
        printf("Unable to allocate memory for bf3v2_2 decompress deflate state\n");
        return false;
    }

    for (int i = 0; i < n_cpus; i++)
    {
        kernel_queue_remaining_capacity[i] = BF3v2_2_DECOMPRESS_DEFLATE_WORKQ_DEPTH;
    }

    bf3v2_2_decompress_deflate_state.global_doca_state = &dpm_doca_state;
    doca_error_t ret;

    // for each cpu, create a doca compress engine and ctx and workq
    for (int i = 0; i < n_cpus; i++)
    {
        ret = doca_compress_create(&bf3v2_2_decompress_deflate_state.compresses[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create compress engine %d: %s", i, doca_get_error_string(ret));
            return false;
        }
        bf3v2_2_decompress_deflate_state.ctxs[i] = doca_compress_as_ctx(bf3v2_2_decompress_deflate_state.compresses[i]);

        auto dev = bf3v2_2_decompress_deflate_state.global_doca_state->state.dev;
        ret = doca_ctx_dev_add(bf3v2_2_decompress_deflate_state.ctxs[i], dev);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to register device with lib context: %s\n", doca_get_error_string(ret));
            bf3v2_2_decompress_deflate_state.ctxs[i] = NULL;
            return ret;
        }

        ret = doca_ctx_start(bf3v2_2_decompress_deflate_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Failed to start context: %s\n", doca_get_error_string(ret));
            return false;
        }

        ret = doca_workq_create(BF3v2_2_DECOMPRESS_DEFLATE_WORKQ_DEPTH, &bf3v2_2_decompress_deflate_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create work queue for decompress: %s", doca_get_error_string(ret));
            return false;
        }
        ret = doca_ctx_workq_add(bf3v2_2_decompress_deflate_state.ctxs[i], bf3v2_2_decompress_deflate_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to add work queue to context: %s\n", doca_get_error_string(ret));
            return false;
        }
    }

    //// single threaded old init below
    /* ret = doca_compress_create(&bf3v2_2_decompress_deflate_state.compress);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to create compress engine: %s", doca_get_error_string(ret));
        return false;
    }
    bf3v2_2_decompress_deflate_state.ctx = doca_compress_as_ctx(bf3v2_2_decompress_deflate_state.compress);

    auto dev = bf3v2_2_decompress_deflate_state.global_doca_state->state.dev;
    ret = doca_ctx_dev_add(bf3v2_2_decompress_deflate_state.ctx, dev);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to register device with lib context: %s\n", doca_get_error_string(ret));
        bf3v2_2_decompress_deflate_state.ctx = NULL;
        return ret;
    }

    ret = doca_ctx_start(bf3v2_2_decompress_deflate_state.ctx);
    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to start context: %s\n", doca_get_error_string(ret));
        return false;
    }

    // init the doca work queue
    ret = doca_workq_create(DPM_DOCA_WORKQ_DEPTH, &bf3v2_2_decompress_deflate_state.workq);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to create work queue for decompress: %s", doca_get_error_string(ret));
        return false;
    }
    ret = doca_ctx_workq_add(bf3v2_2_decompress_deflate_state.ctx, bf3v2_2_decompress_deflate_state.workq);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to add work queue to context: %s\n", doca_get_error_string(ret));
        return false;
    } */

    return true;
}

dpkernel_error bf3v2_2_decompress_deflate_kernel_execute(dpkernel_task *task, int thread_id)
{
    // printf("in shm_ptr: %lu, out shm_ptr: %lu\n", task->in, task->out);

    dpkernel_task_bf3v2_2 *dpk_task = &task->bf3v2_2;

    doca_error_t ret;

    // construct the task
    struct doca_compress_deflate_job decompress_job;
    decompress_job = {
        .base =
            (struct doca_job){
                .type = DOCA_DECOMPRESS_DEFLATE_JOB,
                .flags = DOCA_JOB_FLAGS_NONE,
                .ctx = bf3v2_2_decompress_deflate_state.ctxs[thread_id],
                .user_data = {.ptr = (void *)task}, // use this to get the actual out size after completion
            },
        .dst_buff = dpk_task->dst_doca_buf,
        .src_buff = dpk_task->src_doca_buf,
    };
    // printf("dst_buff: %p, src_buff: %p\n", decompress_job.dst_buff, decompress_job.src_buff);
    // printf("task ptr: %p\n", task);

    // try to enqueue the decompress task
    ret = doca_workq_submit(bf3v2_2_decompress_deflate_state.workqs[thread_id], &decompress_job.base);
    if (ret == DOCA_ERROR_NO_MEMORY)
    {
        print_debug("decompress doca queue full, try again later\n");
        return DPK_ERROR_AGAIN;
    }

    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to submit compress job: %s", doca_get_error_string(ret));
        doca_buf_refcount_rm(dpk_task->dst_doca_buf, NULL);
        doca_buf_refcount_rm(dpk_task->src_doca_buf, NULL);
        return DPK_ERROR_FAILED;
    }

    // decrement the capacity
    kernel_queue_remaining_capacity[thread_id]--;
    return DPK_SUCCESS;
}

dpkernel_error bf3v2_2_decompress_deflate_kernel_poll(dpkernel_task **task, int thread_id)
{
    doca_error_t ret;
    struct doca_event event = {0};
    ret = doca_workq_progress_retrieve(bf3v2_2_decompress_deflate_state.workqs[thread_id], &event,
                                       DOCA_WORKQ_RETRIEVE_FLAGS_NONE);

    if (ret == DOCA_ERROR_AGAIN)
    {
        // normal case, try again later
        return DPK_ERROR_AGAIN;
    }
    else if (ret == DOCA_SUCCESS)
    {
        // got a completion
        *task = (dpkernel_task *)event.user_data.ptr; // we store the task in user_data during execute
        dpkernel_task_bf3v2_2 *dpk_task = &(*task)->bf3v2_2;

        size_t data_len;
        size_t buf_len;
        doca_buf_get_data_len(dpk_task->dst_doca_buf, &data_len);
        doca_buf_get_len(dpk_task->dst_doca_buf, &buf_len);
        dpk_task->actual_out_size = static_cast<uint32_t>(data_len);

        print_debug("data_len: %lu, buf_len: %lu\n", data_len, buf_len);
        print_debug("decompress_executor_shm_poller got completion with actual_out_size: %u\n",
                    dpk_task->actual_out_size);
        // increment the capacity
        kernel_queue_remaining_capacity[thread_id]++;
        return DPK_SUCCESS;
    }
    else
    {
        printf("Failed to retrieve workq progress: %s", doca_get_error_string(ret));
        // task->completion.store(DPK_ERROR_FAILED);
        return DPK_ERROR_FAILED;
    }
}

bool bf3v2_2_decompress_deflate_kernel_cleanup()
{
    doca_error_t ret;
    // int n_cpus = 0;
    // for (int i = 0; i < 64; ++i)
    // {
    //     if ((BF3v2_2_DECOMPRESS_DEFLATE_CPU_MASK >> i) & 1)
    //     {
    //         n_cpus++;
    //     }
    // }

    // cleanup the workq and ctxs
    for (int i = 0; i < n_cpus; i++)
    {
        ret = doca_ctx_workq_rm(bf3v2_2_decompress_deflate_state.ctxs[i], bf3v2_2_decompress_deflate_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to remove work queue from context %d: %s", i, doca_get_error_string(ret));
            return false;
        }
        ret = doca_workq_destroy(bf3v2_2_decompress_deflate_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to destroy work queue %d: %s", i, doca_get_error_string(ret));
            return false;
        }
        bf3v2_2_decompress_deflate_state.workqs[i] = NULL;

        ret = doca_ctx_stop(bf3v2_2_decompress_deflate_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to stop context %d: %s", i, doca_get_error_string(ret));
            return false;
        }
        ret = doca_compress_destroy(bf3v2_2_decompress_deflate_state.compresses[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to destroy compress engine: %s", doca_get_error_string(ret));
            return false;
        }
    }

    /* ret = doca_compress_destroy(bf3v2_2_decompress_deflate_state.compress);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to destroy compress engine: %s", doca_get_error_string(ret));
        return false;
    }
    bf3v2_2_decompress_deflate_state.compress = NULL;

    ret = doca_ctx_stop(bf3v2_2_decompress_deflate_state.ctx);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to stop context: %s", doca_get_error_string(ret));
        return false;
    } */

    // printf("Waiting for all tasks to finish...\n");
    printf("bf3v2_2 decompress kernel cleanup done\n");
    return true;
}

/* uint64_t bf3v2_2_decompress_deflate_kernel_get_estimated_completion_time()
{
    // TODO:
    return 1000;
} */
