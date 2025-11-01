#include "decompress_deflate_bf3.hpp"
#include "common.hpp"
#include "doca_buf.h"
#include "doca_compress.h"
#include "doca_ctx.h"
#include "doca_error.h"
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <sys/types.h>

struct dpm_doca_decompress_deflate_bf3 bf3_decompress_deflate_state;

// spin lock to protect doca_buf_inventory_buf_get_by_addr
pthread_spinlock_t buf_inventory_lock;

// new DOCA uses a callback mechanism to notify the completion of a task
void decompress_deflate_completed_callback(struct doca_compress_task_decompress_deflate *compress_task,
                                           union doca_data task_user_data, union doca_data ctx_user_data)
{
    // dpk task is passed as user data
    struct dpkernel_task_base *task = (struct dpkernel_task_base *)task_user_data.ptr;
#ifdef MEMCPY_KERNEL
    // perform memcpy
    char *out_ptr = dpm_get_output_ptr_from_shmptr(task->out);
    char *in_ptr = dpm_get_input_ptr_from_shmptr(task->in);
    char *out_cpy_ptr = dpm_get_output_ptr_from_shmptr(task->out_cpy);
    char *in_cpy_ptr = dpm_get_input_ptr_from_shmptr(task->in_cpy);
    memcpy(in_cpy_ptr, in_ptr, task->in_size);
    memcpy(out_cpy_ptr, out_ptr, task->out_size);
#endif
    task->completion.store(DPK_SUCCESS, std::memory_order_release);
    // NOTE: this is now NOT on the data path, but in the mem mgmt path
    // doca_buf_dec_refcount(task->src_doca_buf, NULL);
    // doca_buf_dec_refcount(task->dst_doca_buf, NULL);

    doca_task_free(doca_compress_task_decompress_deflate_as_task(compress_task));
}

void decompress_deflate_error_callback(struct doca_compress_task_decompress_deflate *compress_task,
                                       union doca_data task_user_data, union doca_data ctx_user_data)
{
    struct dpkernel_task_base *task = (struct dpkernel_task_base *)task_user_data.ptr;
    task->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
    doca_task_free(doca_compress_task_decompress_deflate_as_task(compress_task));
    printf("Compress task failed: %s\n",
           doca_error_get_descr(doca_task_get_status(doca_compress_task_decompress_deflate_as_task(compress_task))));
    printf("in size = %ld, out size = %ld\n", task->in_size, task->out_size);
}

bool bf3_decompress_deflate_handle_mem_req(struct dpm_mem_req *req)
{
    doca_error_t ret;
    dpkernel_task_base *task = dpm_get_task_ptr_from_shmptr(req->task);
    // uint16_t refcnt;

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
        // nothing to do
        printf("bf3 decompress deflate handle mem req alloc task, nothing to do\n");
        return true;
    }
    case DPM_MEM_REQ_FREE_TASK:
    {
        // nothing to do
        printf("bf3 decompress deflate handle mem req free task, nothing to do\n");
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

// spin lock for testing `doca_buf_inventory_buf_get_by_addr`
// static pthread_spinlock_t spin_lock;

bool bf3_decompress_deflate_kernel_init(struct dpm_kernel_registration_result *result)
{
    // TEST: init the spin lock
    // pthread_spin_init(&spin_lock, PTHREAD_PROCESS_PRIVATE);

    pthread_spin_init(&buf_inventory_lock, PTHREAD_PROCESS_PRIVATE);

    // set the cpu mask for the kernel
    int n_cpus = 0;

    CPU_ZERO(&result->cpu_mask);
    for (int i = 0; i < 32; ++i)
    {
        if ((BF3_DECOMPRESS_DEFLATE_CPU_MASK >> i) & 1)
        {
            CPU_SET(i, &result->cpu_mask);
            n_cpus++;
        }
    }
    printf("bf3_decompress_deflate_kernel_init cpu mask: %x, n_cpus = %d\n", BF3_DECOMPRESS_DEFLATE_CPU_MASK, n_cpus);

    // malloc the multithreaded ctxs etc
    bf3_decompress_deflate_state.compresses = (struct doca_compress **)malloc(sizeof(struct doca_compress *) * n_cpus);
    bf3_decompress_deflate_state.ctxs = (struct doca_ctx **)malloc(sizeof(struct doca_ctx *) * n_cpus);
    bf3_decompress_deflate_state.pes = (struct doca_pe **)malloc(sizeof(struct doca_pe *) * n_cpus);
    if (bf3_decompress_deflate_state.compresses == NULL || bf3_decompress_deflate_state.ctxs == NULL ||
        bf3_decompress_deflate_state.pes == NULL)
    {
        printf("Unable to allocate memory for bf3 decompress deflate state\n");
        return false;
    }

    bf3_decompress_deflate_state.global_doca_state = &dpm_doca_state;
    doca_error_t ret;

    uint64_t max_buf_size = 0;
    ret = doca_compress_cap_task_decompress_deflate_get_max_buf_size(
        doca_dev_as_devinfo(bf3_decompress_deflate_state.global_doca_state->state.dev), &max_buf_size);
    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to query decompress max buf size: %s", doca_error_get_descr(ret));
    }
    else
    {
        printf("BF3 decompress max buf size: %lu\n", max_buf_size);
    }

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

        doca_compress_create(bf3_decompress_deflate_state.global_doca_state->state.dev,
                             &bf3_decompress_deflate_state.compresses[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create compress engine %d: %s", i, doca_error_get_descr(ret));
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
                                                             decompress_deflate_error_callback, DPM_DOCA_WORKQ_DEPTH);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to set decompress deflate task conf with num_tasks = DPM_DOCA_WORKQ_DEPTH = %d, %s",
                   DPM_DOCA_WORKQ_DEPTH, doca_error_get_descr(ret));
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
    int n_cpus = 0;
    for (int i = 0; i < 64; ++i)
    {
        if ((BF3_DECOMPRESS_DEFLATE_CPU_MASK >> i) & 1)
        {
            n_cpus++;
        }
    }

    for (int i = 0; i < n_cpus; i++)
    {
        ret = doca_ctx_stop(bf3_decompress_deflate_state.ctxs[i]);
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
    }
    free(bf3_decompress_deflate_state.compresses);
    free(bf3_decompress_deflate_state.ctxs);
    free(bf3_decompress_deflate_state.pes);
    bf3_decompress_deflate_state.compresses = NULL;
    bf3_decompress_deflate_state.ctxs = NULL;
    bf3_decompress_deflate_state.pes = NULL;

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
dpkernel_error bf3_decompress_deflate_kernel_execute(dpkernel_task_base *dpk_task, int thread_id)
{
    print_debug("in shm_ptr: %lu, out shm_ptr: %lu\n", task->in, task->out);
    // get local ptr from shm ptr of in and out buffers
    char *in = dpm_get_input_ptr_from_shmptr(dpk_task->in);
    char *out = dpm_get_output_ptr_from_shmptr(dpk_task->out);

    doca_error_t ret;
    union doca_data task_user_data = {0};
    struct doca_compress_task_decompress_deflate *decompress_deflate_task;
    // struct compress_result task_result = {0};
    struct doca_task *decompress_task;

    // struct doca_buf *src_doca_buf = (struct doca_buf *)malloc(sizeof(src_doca_buf));
    // struct doca_buf *dst_doca_buf = (struct doca_buf *)malloc(sizeof(dst_doca_buf));

    // changed: now this would be done in the handle_mem_req already
    // struct doca_buf *src_doca_buf = nullptr;
    // struct doca_buf *dst_doca_buf = nullptr;

    // TEST: lock the spin lock
    // pthread_spin_lock(&spin_lock);

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

    task_user_data.ptr = (void *)dpk_task;
    ret = doca_compress_task_decompress_deflate_alloc_init(bf3_decompress_deflate_state.compresses[thread_id],
                                                           dpk_task->src_doca_buf, dpk_task->dst_doca_buf, task_user_data,
                                                           &decompress_deflate_task);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to alloc init decompress deflate task: %s\n", doca_error_get_descr(ret));
        doca_buf_dec_refcount(dpk_task->src_doca_buf, NULL);
        doca_buf_dec_refcount(dpk_task->dst_doca_buf, NULL);
        return DPK_ERROR_FAILED;
    }
    decompress_task = doca_compress_task_decompress_deflate_as_task(decompress_deflate_task);

    dpk_task->user_data = (void *)std::chrono::high_resolution_clock::now().time_since_epoch().count();

    // dpk_task->user_data = (void *)task_num++;
    // ret = doca_task_try_submit(decompress_task);

    ret = doca_task_submit(decompress_task);

    if (ret == DOCA_SUCCESS)
    {
        print_debug("submit to decompress success\n");
        return DPK_SUCCESS;
    }
    else if (ret == DOCA_ERROR_NO_MEMORY)
    {
        printf("submit to decompress failed, no memory (queue full?): %s\n", doca_error_get_name(ret));
        doca_buf_dec_refcount(dpk_task->src_doca_buf, NULL);
        doca_buf_dec_refcount(dpk_task->dst_doca_buf, NULL);
        doca_task_free(decompress_task);
        return DPK_ERROR_FAILED;
    }
    else
    {
        printf("Failed to submit compress job: %s", doca_error_get_name(ret));
        doca_buf_dec_refcount(dpk_task->src_doca_buf, NULL);
        doca_buf_dec_refcount(dpk_task->dst_doca_buf, NULL);
        doca_task_free(decompress_task);
        return DPK_ERROR_FAILED;
    }
    // printf("dst_buff: %p, src_buff: %p\n", decompress_job.dst_buff, decompress_job.src_buff);
    // printf("task ptr: %p\n", task);
}

dpkernel_error bf3_decompress_deflate_kernel_poll(dpkernel_task_base *task, int thread_id)
{
    dpkernel_error ret = DPK_ERROR_AGAIN;
    int cnt = 0;
    // struct doca_event event = {0};

    // if there are some completions, try to poll a bit more
    while (doca_pe_progress(bf3_decompress_deflate_state.pes[thread_id]) == 1) // cnt < N_CONSECUTIVE_POLLS &&
    {
        print_debug("got a completion, callback should be called already?\n");
        cnt++;
        ret = DPK_SUCCESS;
    }

    return ret;

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

/* uint64_t bf3_decompress_deflate_kernel_get_estimated_completion_time()
{
    return 1000;
} */