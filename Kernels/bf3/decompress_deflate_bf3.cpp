#include "decompress_deflate_bf3.hpp"
#include "common.hpp"
#include "doca_buf.h"
#include "doca_buf_inventory.h"
#include "doca_compress.h"
#include "doca_ctx.h"
#include "doca_error.h"
#include "memory.hpp"
#include "bounded_queue.hpp"
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <sys/types.h>

static struct dpm_doca_decompress_deflate_bf3 bf3_decompress_deflate_state;

static uint32_t *kernel_queue_remaining_capacity;
static int n_cpus = 0;
// Device capability: max input/output buffer size accepted by DOCA deflate-decompress task.
static uint64_t bf3_decompress_deflate_max_buf_size = 16 * 1024;

// spin lock to protect doca_buf_inventory_buf_get_by_addr
pthread_spinlock_t buf_inventory_lock;

// Completion queues per DPM thread (consumed by bf3_decompress_deflate_kernel_poll).
// Capacity is intentionally larger than HW depth to keep callback enqueue non-blocking.
static constexpr size_t BF3_COMPLETION_QUEUE_CAPACITY = DPM_HW_KERNEL_QUEUE_SIZE + BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH;
static std::array<BoundedQueue<dpkernel_task *, BF3_COMPLETION_QUEUE_CAPACITY>, N_DPM_THREADS> bf3_completed_tasks;

static long _bf3_decompress_estimated_processing_time(uint32_t input_size, int thread_id)
{
    // TODO: replace with measured model for bf3 path; use a conservative positive estimate for now.
    return (long)input_size;
}

/// Queue a completed task for its owning DPM thread, preserving callback/poll decoupling.
static inline void _bf3_enqueue_completed_task_for_polling(dpkernel_task *task)
{
    const int thread_id = (int)task->bf3.callback_owner_thread_id;

    if (thread_id < 0 || thread_id >= N_DPM_THREADS)
    {
        printf("bf3 decompress callback missing owner thread for task=%p\n", task);
        return;
    }

    // Defensive check: this should never be full under normal BF3 HW queue depth.
    if (!bf3_completed_tasks[thread_id].push(task))
    {
        printf("bf3 completion queue overflow on thread_id=%d for task=%p\n", thread_id, task);
    }
}

// new DOCA uses a callback mechanism to notify the completion of a task
void decompress_deflate_completed_callback(struct doca_compress_task_decompress_deflate *compress_task,
                                           union doca_data task_user_data, union doca_data ctx_user_data)
{
    (void)ctx_user_data;
    // dpk task is passed as user data
    dpkernel_task *task = (dpkernel_task *)task_user_data.ptr;
    dpkernel_task_base *base_task = &task->base;
#ifndef MEMCPY_KERNEL
    (void)base_task;
#endif
#ifdef MEMCPY_KERNEL
    // perform memcpy
    char *out_ptr = dpm_get_output_ptr_from_shmptr(base_task->out);
    char *in_ptr = dpm_get_input_ptr_from_shmptr(base_task->in);
    char *out_cpy_ptr = dpm_get_output_ptr_from_shmptr(base_task->out_cpy);
    char *in_cpy_ptr = dpm_get_input_ptr_from_shmptr(base_task->in_cpy);
    memcpy(in_cpy_ptr, in_ptr, base_task->in_size);
    memcpy(out_cpy_ptr, out_ptr, base_task->out_size);
#endif
    // Keep actual output size consistent with AES callback path for app-side accounting.
    if (task->bf3.dst_doca_buf != nullptr)
    {
        size_t actual_len = 0;
        if (doca_buf_get_data_len(task->bf3.dst_doca_buf, &actual_len) == DOCA_SUCCESS)
        {
            base_task->actual_out_size = (uint32_t)actual_len;
        }
    }
    // Keep callback->poll status local to the kernel; DPM sets base.completion in dpm_poll_completion().
    // base_task->completion.store(DPK_SUCCESS, std::memory_order_release);
    task->bf3.callback_status = DPK_SUCCESS;
    _bf3_enqueue_completed_task_for_polling(task);
    // NOTE: this is now NOT on the data path, but in the mem mgmt path
    // doca_buf_dec_refcount(task->src_doca_buf, NULL);
    // doca_buf_dec_refcount(task->dst_doca_buf, NULL);

    // Preallocated decompress task is reused across submissions; do not free on completion.
    (void)compress_task;
}

void decompress_deflate_error_callback(struct doca_compress_task_decompress_deflate *compress_task,
                                       union doca_data task_user_data, union doca_data ctx_user_data)
{
    (void)ctx_user_data;
    dpkernel_task *task = (dpkernel_task *)task_user_data.ptr;
    dpkernel_task_base *base_task = &task->base;
    // Keep callback->poll status local to the kernel; DPM sets base.completion in dpm_poll_completion().
    // base_task->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
    task->bf3.callback_status = DPK_ERROR_FAILED;
    _bf3_enqueue_completed_task_for_polling(task);
    printf("Compress task failed: %s\n",
           doca_error_get_descr(doca_task_get_status(doca_compress_task_decompress_deflate_as_task(compress_task))));
    printf("in size = %ld, out size = %ld\n", base_task->in_size, base_task->out_size);
}

bool bf3_decompress_deflate_handle_mem_req(struct dpm_mem_req *req)
{
    doca_error_t ret;
    dpkernel_task_bf3 *task = &((dpkernel_task *)dpm_get_task_ptr_from_shmptr(req->task))->bf3;
    uint16_t refcnt = 0;

    switch (req->type)
    {
    case DPM_MEM_REQ_ALLOC_INPUT:
    {
        print_debug("bf3 decompress deflate handle mem req alloc input\n");
        char *input_buffer = dpm_get_input_ptr_from_shmptr(task->in);

        pthread_spin_lock(&buf_inventory_lock);
        ret = doca_buf_inventory_buf_get_by_addr(
            bf3_decompress_deflate_state.global_doca_state->state.buf_inv,
            bf3_decompress_deflate_state.global_doca_state->state.src_mmap,
            input_buffer,
            task->in_size,
            &task->src_doca_buf);
        pthread_spin_unlock(&buf_inventory_lock);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 decompress deflate failed to get input buf from inventory: %s\n", doca_error_get_descr(ret));
            return false;
        }
        ret = doca_buf_set_data(task->src_doca_buf, input_buffer, task->in_size);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 decompress deflate failed to set input buf data: %s\n", doca_error_get_descr(ret));
            return false;
        }
        return true;

        // break;
    }
    case DPM_MEM_REQ_ALLOC_OUTPUT:
    {
        print_debug("bf3 decompress deflate handle mem req alloc output\n");
        pthread_spin_lock(&buf_inventory_lock);
        ret = doca_buf_inventory_buf_get_by_addr(
            bf3_decompress_deflate_state.global_doca_state->state.buf_inv,
            bf3_decompress_deflate_state.global_doca_state->state.dst_mmap,
            dpm_get_output_ptr_from_shmptr(task->out),
            task->out_size,
            &task->dst_doca_buf);
        pthread_spin_unlock(&buf_inventory_lock);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 decompress deflate failed to get output buf from inventory: %s\n", doca_error_get_descr(ret));
            return false;
        }
        return true;
        // break;
    }
    case DPM_MEM_REQ_FREE_INPUT:
    {
        print_debug("bf3 decompress deflate handle mem req free input\n");
        if (task->src_doca_buf == nullptr)
        {
            return true;
        }
        // free the input buffer
        ret = doca_buf_dec_refcount(task->src_doca_buf, NULL);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 decompress deflate failed to free input buf: %s\n", doca_error_get_descr(ret));
            return false;
        }
        print_debug("input doca buf refcnt = %d\n", refcnt);
        print_debug("freed input doca buf\n");
        task->src_doca_buf = NULL;
        return true;
        // break;
    }
    case DPM_MEM_REQ_FREE_OUTPUT:
    {
        print_debug("bf3 decompress deflate handle mem req free output\n");
        if (task->dst_doca_buf == nullptr)
        {
            return true;
        }
        // free the output buffer
        ret = doca_buf_dec_refcount(task->dst_doca_buf, NULL);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 decompress deflate failed to free output buf: %s\n", doca_error_get_descr(ret));
            return false;
        }
        print_debug("output doca buf refcnt = %d\n", refcnt);
        task->dst_doca_buf = NULL;
        print_debug("freed output doca buf\n");
        return true;
        // break;
    }
    case DPM_MEM_REQ_ALLOC_TASK:
    {
        if (task->prealloc_decompress_task != nullptr)
        {
            return true;
        }
        if (task->src_doca_buf == nullptr || task->dst_doca_buf == nullptr)
        {
            printf("bf3 decompress deflate alloc_task requires src/dst doca_buf to be bound first\n");
            return false;
        }
        int thread_id = (int)task->callback_owner_thread_id;
        if (thread_id < 0 || thread_id >= n_cpus || thread_id >= N_DPM_THREADS)
        {
            printf("bf3 decompress deflate alloc_task invalid callback_owner_thread_id=%d\n", thread_id);
            return false;
        }

        union doca_data task_user_data = {0};
        task_user_data.ptr = (void *)dpm_get_task_ptr_from_shmptr(req->task);
        struct doca_compress_task_decompress_deflate *decompress_deflate_task = nullptr;
        ret = doca_compress_task_decompress_deflate_alloc_init(bf3_decompress_deflate_state.compresses[thread_id],
                                                               task->src_doca_buf, task->dst_doca_buf, task_user_data,
                                                               &decompress_deflate_task);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 decompress deflate alloc_task failed to alloc init task: %s\n", doca_error_get_descr(ret));
            return false;
        }
        task->prealloc_decompress_task = decompress_deflate_task;
        task->prealloc_thread_id = (uint8_t)thread_id;
        return true;
    }
    case DPM_MEM_REQ_FREE_TASK:
    {
        if (task->prealloc_decompress_task == nullptr)
        {
            return true;
        }
        doca_task_free(doca_compress_task_decompress_deflate_as_task(task->prealloc_decompress_task));
        task->prealloc_decompress_task = nullptr;
        task->prealloc_thread_id = UINT8_MAX;
        return true;
    }
    default:
    {
        printf("bf3 decompress deflate kernel unknown mem req->type: %d\n", req->type);
        return false;
        // break;
    }
    }
}

uint32_t bf3_decompress_deflate_kernel_can_execute_kernels(int thread_id, uint32_t *max_capacity)
{
    if (max_capacity != NULL)
    {
        *max_capacity = BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH;
    }
    return kernel_queue_remaining_capacity[thread_id];
}

// spin lock for testing `doca_buf_inventory_buf_get_by_addr`
// static pthread_spinlock_t spin_lock;

bool bf3_decompress_deflate_get_catalogue(struct dpm_kernel_catalogue *catalogue)
{
    catalogue->is_coalescing_enabled = false;
    catalogue->max_single_task_size =
        (bf3_decompress_deflate_max_buf_size > UINT32_MAX) ? UINT32_MAX : (uint32_t)bf3_decompress_deflate_max_buf_size;
    catalogue->get_estimated_processing_time_ns = _bf3_decompress_estimated_processing_time;
    catalogue->kernel_capacity = BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH;
    return true;
}

bool bf3_decompress_deflate_kernel_init(struct dpm_kernel_catalogue *result, int n_threads)
{
    // TEST: init the spin lock
    // pthread_spin_init(&spin_lock, PTHREAD_PROCESS_PRIVATE);

    pthread_spin_init(&buf_inventory_lock, PTHREAD_PROCESS_PRIVATE);

    // Keep BF3 kernel catalogue aligned with the current dpm_kernel interface.
    bf3_decompress_deflate_get_catalogue(result);

    // NOTE: old logic used CPU mask in catalogue; current catalogue no longer stores cpu_mask.
    n_cpus = (n_threads > 0) ? n_threads : 1;
    printf("bf3_decompress_deflate_kernel_init n_cpus = %d\n", n_cpus);

    // malloc the multithreaded ctxs etc
    kernel_queue_remaining_capacity = (uint32_t *)calloc(n_cpus, sizeof(uint32_t));
    bf3_decompress_deflate_state.compresses = (struct doca_compress **)calloc(n_cpus, sizeof(struct doca_compress *));
    bf3_decompress_deflate_state.ctxs = (struct doca_ctx **)calloc(n_cpus, sizeof(struct doca_ctx *));
    bf3_decompress_deflate_state.pes = (struct doca_pe **)calloc(n_cpus, sizeof(struct doca_pe *));
    if (bf3_decompress_deflate_state.compresses == NULL || bf3_decompress_deflate_state.ctxs == NULL ||
        bf3_decompress_deflate_state.pes == NULL || kernel_queue_remaining_capacity == NULL)
    {
        printf("Unable to allocate memory for bf3 decompress deflate state\n");
        return false;
    }

    for (int i = 0; i < n_cpus; i++)
    {
        kernel_queue_remaining_capacity[i] = BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH;
    }

    bf3_decompress_deflate_state.global_doca_state = &dpm_doca_state;
    doca_error_t ret;

    // Original local variable retained for reference.
    // uint64_t max_buf_size = 0;
    ret = doca_compress_cap_task_decompress_deflate_get_max_buf_size(
        doca_dev_as_devinfo(bf3_decompress_deflate_state.global_doca_state->state.dev),
        &bf3_decompress_deflate_max_buf_size);
    if (ret != DOCA_SUCCESS)
    {
        printf("[BF3-DECOMP-CAP] status=FAIL err=%s\n", doca_error_get_descr(ret));
    }
    else
    {
        printf("[BF3-DECOMP-CAP] status=PASS max_buf_size=%lu workq_depth=%u n_threads=%d\n",
               (unsigned long)bf3_decompress_deflate_max_buf_size, (unsigned)BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH,
               n_cpus);
    }
    // Keep scheduler-visible max task size aligned with the runtime capability we just queried.
    bf3_decompress_deflate_get_catalogue(result);

    // for each cpu, create a compress engine and a context
    for (int i = 0; i < n_cpus; i++)
    {
        // create a PE for each CPU
        ret = doca_pe_create(&bf3_decompress_deflate_state.pes[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create PE %d: %s", i, doca_error_get_descr(ret));
            return false;
        }

        // Original code path for reference:
        // doca_compress_create(bf3_decompress_deflate_state.global_doca_state->state.dev,
        //                      &bf3_decompress_deflate_state.compresses[i]);
        ret = doca_compress_create(bf3_decompress_deflate_state.global_doca_state->state.dev,
                                   &bf3_decompress_deflate_state.compresses[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create compress engine %d: %s", i, doca_error_get_descr(ret));
            return false;
        }
        uint32_t max_num_tasks = 0;
        ret = doca_compress_cap_get_max_num_tasks(bf3_decompress_deflate_state.compresses[i], &max_num_tasks);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to query decompress max tasks for thread %d: %s\n", i, doca_error_get_descr(ret));
            return false;
        }
        if (BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH > max_num_tasks)
        {
            printf("Configured deflate-decompress workq depth %u exceeds device max %u for thread %d\n",
                   (unsigned)BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH, (unsigned)max_num_tasks, i);
            return false;
        }
        bf3_decompress_deflate_state.ctxs[i] = doca_compress_as_ctx(bf3_decompress_deflate_state.compresses[i]);
        ret = doca_pe_connect_ctx(bf3_decompress_deflate_state.pes[i], bf3_decompress_deflate_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to connect PE to context %d: %s", i, doca_error_get_descr(ret));
            return false;
        }
        ret = doca_compress_task_decompress_deflate_set_conf(bf3_decompress_deflate_state.compresses[i],
                                                             decompress_deflate_completed_callback,
                                                             decompress_deflate_error_callback, BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to set decompress deflate task conf with num_tasks = BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH = %d, %s",
                   BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH, doca_error_get_descr(ret));
            return false;
        }
        else
        {
            printf("decompress deflate task conf set for compress %d\n", i);
        }
        ret = doca_ctx_start(bf3_decompress_deflate_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Failed to start context %d: %s\n", i, doca_error_get_descr(ret));
        }
    }

    ////
    //// old single threaded init below
    ////
    /* ret = doca_compress_create(bf3_decompress_deflate_state.global_doca_state->state.dev,
                               &bf3_decompress_deflate_state.compress);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to create compress engine: %s", doca_error_get_descr(ret));
        return false;
    }
    bf3_decompress_deflate_state.ctx = doca_compress_as_ctx(bf3_decompress_deflate_state.compress);

    ret =
        doca_pe_connect_ctx(bf3_decompress_deflate_state.global_doca_state->state.pe, bf3_decompress_deflate_state.ctx);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to connect PE to context: %s", doca_error_get_descr(ret));
        return false;
    }

    ret = doca_compress_task_decompress_deflate_set_conf(bf3_decompress_deflate_state.compress,
                                                         decompress_deflate_completed_callback,
                                                         decompress_deflate_error_callback, DPM_DOCA_WORKQ_DEPTH);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to set decompress deflate task conf with num_tasks = DPM_DOCA_WORKQ_DEPTH = %d, %s",
               DPM_DOCA_WORKQ_DEPTH, doca_error_get_descr(ret));
        return false;
    }
    else
    {
        printf("decompress deflate task conf set\n");
    }

    ret = doca_ctx_start(bf3_decompress_deflate_state.ctx);
    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to start context: %s\n", doca_error_get_descr(ret));
    } */

    // TODO: figure out if this is needed at all
    // doca_ctx_set_state_changed_cb();

    // union doca_data ctx_user_data = {0};
    // This method sets a user data to a context. The user data is used as a parameter in
    // doca_ctx_state_changed_callback_t doca_ctx_set_user_data();

    return true;
}

bool bf3_decompress_deflate_kernel_cleanup()
{
    doca_error_t ret;

    for (int i = 0; i < n_cpus; i++)
    {
        // Modified cleanup path: request context stop with PE progress to drain in-flight callbacks.
        ret = request_stop_ctx(bf3_decompress_deflate_state.pes[i], bf3_decompress_deflate_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to stop context %d: %s", i, doca_error_get_descr(ret));
            return false;
        }
        ret = doca_compress_destroy(bf3_decompress_deflate_state.compresses[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to destroy compress engine %d: %s", i, doca_error_get_descr(ret));
            return false;
        }
        // Modified cleanup path: each per-thread PE created in init must be explicitly destroyed.
        ret = doca_pe_destroy(bf3_decompress_deflate_state.pes[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to destroy PE %d: %s", i, doca_error_get_descr(ret));
            return false;
        }
        bf3_decompress_deflate_state.pes[i] = NULL;
    }
    free(bf3_decompress_deflate_state.compresses);
    free(bf3_decompress_deflate_state.ctxs);
    free(bf3_decompress_deflate_state.pes);
    bf3_decompress_deflate_state.compresses = NULL;
    bf3_decompress_deflate_state.ctxs = NULL;
    bf3_decompress_deflate_state.pes = NULL;
    n_cpus = 0;

    for (int i = 0; i < N_DPM_THREADS; i++)
    {
        dpkernel_task *dropped_task = NULL;
        while (bf3_completed_tasks[i].pop(dropped_task))
        {
            (void)dropped_task;
        }
    }

    ////
    //// single threaded cleanup below
    ////
    /* ret = doca_compress_destroy(bf3_decompress_deflate_state.compress);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to destroy compress engine: %s", doca_error_get_descr(ret));
        return false;
    }
    bf3_decompress_deflate_state.compress = NULL;

    ret = doca_ctx_stop(bf3_decompress_deflate_state.ctx);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to stop context: %s", doca_error_get_descr(ret));
        return false;
    } */

    // TODO: wait for all tasks to finish
    // printf("Waiting for all tasks to finish...\n");
    printf("bf3 decompress kernel cleanup done\n");
    return true;
}

uint64_t task_num = 0;
dpkernel_error bf3_decompress_deflate_kernel_execute(dpkernel_task *task, int thread_id)
{
    dpkernel_task_bf3 *dpk_task = &task->bf3;
    print_debug("in shm_ptr: %lu, out shm_ptr: %lu\n", dpk_task->in, dpk_task->out);
    // get local ptr from shm ptr of in and out buffers
    char *in = dpm_get_input_ptr_from_shmptr(dpk_task->in);
    char *out = dpm_get_output_ptr_from_shmptr(dpk_task->out);

    doca_error_t ret;
    // struct compress_result task_result = {0};
    struct doca_task *decompress_task;

    // struct doca_buf *src_doca_buf = (struct doca_buf *)malloc(sizeof(src_doca_buf));
    // struct doca_buf *dst_doca_buf = (struct doca_buf *)malloc(sizeof(dst_doca_buf));

    // changed: now this would be done in the handle_mem_req already
    // struct doca_buf *src_doca_buf = nullptr;
    // struct doca_buf *dst_doca_buf = nullptr;

    // TEST: lock the spin lock
    // pthread_spin_lock(&spin_lock);

    if (dpk_task->src_doca_buf == nullptr || dpk_task->dst_doca_buf == nullptr)
    {
        printf("bf3_decompress_deflate_kernel_execute missing src/dst doca_buf for task=%p\n", task);
        return DPK_ERROR_FAILED;
    }
    if ((uint64_t)dpk_task->in_size > bf3_decompress_deflate_max_buf_size ||
        (uint64_t)dpk_task->out_size > bf3_decompress_deflate_max_buf_size)
    {
        printf("[BF3-DECOMP-SIZE-CHECK] status=FAIL in_size=%u out_size=%u max_buf_size=%lu task=%p\n",
               (unsigned)dpk_task->in_size, (unsigned)dpk_task->out_size,
               (unsigned long)bf3_decompress_deflate_max_buf_size, task);
        return DPK_ERROR_FAILED;
    }

    if (dpk_task->use_lazy_doca_buf_binding)
    {
        if (in == nullptr || out == nullptr)
        {
            printf("bf3_decompress_deflate_kernel_execute failed to map in/out shm ptrs in=%lu out=%lu for task=%p\n",
                   (unsigned long)dpk_task->in, (unsigned long)dpk_task->out, task);
            return DPK_ERROR_FAILED;
        }

        // Fast-path retarget of preallocated buffers to current per-submit slice.
        // Keep descriptor reuse and avoid inventory alloc/free churn on the hot path.
        ret = doca_buf_inventory_buf_reuse_by_data(dpk_task->src_doca_buf, in, dpk_task->in_size);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3_decompress_deflate_kernel_execute failed to reuse src buffer: %s\n", doca_error_get_descr(ret));
            return DPK_ERROR_FAILED;
        }

        // For decompress destination: reset output window to [out, out_size] with empty data.
        ret = doca_buf_inventory_buf_reuse_by_addr(dpk_task->dst_doca_buf, out, dpk_task->out_size);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3_decompress_deflate_kernel_execute failed to reuse dst buffer: %s\n", doca_error_get_descr(ret));
            return DPK_ERROR_FAILED;
        }
    }

    // changed: now this would be done in the handle_mem_req already
    /* ret = doca_buf_inventory_buf_get_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.src_mmap, in,
                                             dpk_task->in_size, &src_doca_buf); */
    // pthread_spin_unlock(&spin_lock);
    /* if (ret != DOCA_SUCCESS)
    {
        printf("Unable to acquire DOCA buffer for src buffer: %s\n", doca_error_get_name(ret));
        return DPK_ERROR_FAILED;
    } */

    // pthread_spin_lock(&spin_lock);
    /* ret = doca_buf_inventory_buf_get_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.dst_mmap, out,
                                             dpk_task->out_size, &dst_doca_buf); */
    // pthread_spin_unlock(&spin_lock);
    /* if (ret != DOCA_SUCCESS)
    {
        printf("Unable to acquire DOCA buffer for dst buffer: %s\n", doca_error_get_name(ret));
        return DPK_ERROR_FAILED;
    } */

    // changed: now this would be done in the handle_mem_req already
    /* ret = doca_buf_set_data(src_doca_buf, in, dpk_task->in_size);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to set DOCA src buffer data: %s\n", doca_error_get_name(ret));
        doca_buf_dec_refcount(src_doca_buf, NULL);
        doca_buf_dec_refcount(dst_doca_buf, NULL);
        return DPK_ERROR_FAILED;
    }

    // finally, set this to the task
    dpk_task->src_doca_buf = src_doca_buf;
    dpk_task->dst_doca_buf = dst_doca_buf; */

    if (thread_id < 0 || thread_id >= N_DPM_THREADS)
    {
        printf("bf3_decompress_deflate_kernel_execute invalid thread_id=%d for task=%p\n", thread_id, task);
        return DPK_ERROR_FAILED;
    }
    // Reset async callback status before every submission when reusing task objects.
    dpk_task->callback_status = DPK_ONGOING;
    dpk_task->callback_owner_thread_id = (uint8_t)thread_id;
    if (dpk_task->prealloc_decompress_task == nullptr)
    {
        printf("bf3_decompress_deflate_kernel_execute missing preallocated task for task=%p\n", task);
        return DPK_ERROR_FAILED;
    }
    if ((int)dpk_task->prealloc_thread_id != thread_id)
    {
        printf("bf3_decompress_deflate_kernel_execute thread mismatch prealloc_thread=%u submit_thread=%d task=%p\n",
               (unsigned)dpk_task->prealloc_thread_id, thread_id, task);
        return DPK_ERROR_FAILED;
    }

    doca_compress_task_decompress_deflate_set_src(dpk_task->prealloc_decompress_task, dpk_task->src_doca_buf);
    doca_compress_task_decompress_deflate_set_dst(dpk_task->prealloc_decompress_task, dpk_task->dst_doca_buf);
    decompress_task = doca_compress_task_decompress_deflate_as_task(dpk_task->prealloc_decompress_task);

    // dpk_task->user_data = (void *)task_num++;
    // ret = doca_task_try_submit(decompress_task);

    ret = doca_task_submit(decompress_task);

    if (ret == DOCA_SUCCESS)
    {
        print_debug("submit to decompress success\n");
        kernel_queue_remaining_capacity[thread_id] -= 1;
        return DPK_SUCCESS;
    }
    else if (ret == DOCA_ERROR_NO_MEMORY)
    {
        printf("submit to decompress failed, no memory (queue full?): %s\n", doca_error_get_name(ret));
        return DPK_ERROR_AGAIN;
    }
    else
    {
        printf("Failed to submit compress job: %s", doca_error_get_name(ret));
        return DPK_ERROR_FAILED;
    }
    // printf("dst_buff: %p, src_buff: %p\n", decompress_job.dst_buff, decompress_job.src_buff);
    // printf("task ptr: %p\n", task);
}

dpkernel_error bf3_decompress_deflate_kernel_poll(dpkernel_task **task, int thread_id)
{
    // struct doca_event event = {0};

    // if there are some completions, try to poll a bit more
    while (doca_pe_progress(bf3_decompress_deflate_state.pes[thread_id]) == 1) // cnt < N_CONSECUTIVE_POLLS &&
    {
        print_debug("got a completion, callback should be called already?\n");
    }

    if (bf3_completed_tasks[thread_id].pop(*task))
    {
        kernel_queue_remaining_capacity[thread_id] += 1;
        uint8_t callback_status = (*task)->bf3.callback_status;
        if (callback_status == DPK_ERROR_FAILED)
        {
            return DPK_ERROR_FAILED;
        }
        if (callback_status == DPK_SUCCESS)
        {
            return DPK_SUCCESS;
        }
        printf("bf3 decompress poll got unexpected callback_status=%u for task=%p\n", callback_status, *task);
        return DPK_ERROR_FAILED;
    }

    return DPK_ERROR_AGAIN;

    ///////////////////
    /* ret = doca_workq_progress_retrieve(dpm_doca_state.state.workq, &event, DOCA_WORKQ_RETRIEVE_FLAGS_NONE);

    if (ret == DOCA_ERROR_AGAIN)
    {
        // normal case, try again later
        // printf("decompress_executor_shm_poller try again later\n");
        return DPK_ERROR_AGAIN;
    }
    else if (ret == DOCA_SUCCESS)
    {
        // got a completion
        task = (dpkernel_task_base *)event.user_data.ptr; // we store the task in user_data during execute
        printf("task ptr: %p\n", task);
        printf("task->src_doca_buf: %p, task->dst_doca_buf: %p\n", task->src_doca_buf, task->dst_doca_buf);
        size_t data_len;
        size_t buf_len;
        doca_buf_get_data_len(task->dst_doca_buf, &data_len);
        doca_buf_get_len(task->dst_doca_buf, &buf_len);
        task->actual_out_size = data_len;
        printf("data_len: %lu, buf_len: %lu\n", data_len, buf_len);
        printf("decompress_executor_shm_poller got completion with actual_out_size: %lu\n", task->actual_out_size);
        if (doca_buf_dec_refcount(task->src_doca_buf, NULL) != DOCA_SUCCESS ||
            doca_buf_dec_refcount(task->dst_doca_buf, NULL) != DOCA_SUCCESS)
        {
            print_debug("Failed to decrease DOCA buffer reference count for buffers after completion\n");
        }
        // TODO: use proper memory management? note: rm ref cnt seems to free it?
        // free(task->src_doca_buf);
        // free(task->dst_doca_buf);

        // task->completion.store(DPK_SUCCESS);
        // printf("set flag to DPK_SUCCESS\n");
        return DPK_SUCCESS;
    }
    else
    {
        printf("Failed to retrieve workq progress: %s", doca_error_get_name(ret));
        // task->completion.store(DPK_ERROR_FAILED);
        return DPK_ERROR_FAILED;
    } */
}

uint64_t bf3_decompress_deflate_kernel_get_estimated_completion_time()
{
    // TODO:
    return 1000;
}
