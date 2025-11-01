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

// new DOCA uses a callback mechanism to notify the completion of a task
void decompress_deflate_completed_callback(struct doca_compress_task_decompress_deflate *compress_task,
                                           union doca_data task_user_data, union doca_data ctx_user_data)
{
    // dpk task is passed as user data
    struct dpkernel_task_base *task = (struct dpkernel_task_base *)task_user_data.ptr;
    task->completion.store(DPK_SUCCESS, std::memory_order_release);
    doca_buf_dec_refcount(task->src_doca_buf, NULL);
    doca_buf_dec_refcount(task->dst_doca_buf, NULL);

    doca_task_free(doca_compress_task_decompress_deflate_as_task(compress_task));
}

void decompress_deflate_error_callback(struct doca_compress_task_decompress_deflate *compress_task,
                                       union doca_data task_user_data, union doca_data ctx_user_data)
{
    struct dpkernel_task_base *task = (struct dpkernel_task_base *)task_user_data.ptr;
    task->completion.store(DPK_ERROR_FAILED);
    doca_task_free(doca_compress_task_decompress_deflate_as_task(compress_task));
    printf("Compress task failed: %s",
           doca_error_get_descr(doca_task_get_status(doca_compress_task_decompress_deflate_as_task(compress_task))));
}

bool bf3_decompress_deflate_kernel_init()
{
    bf3_decompress_deflate_state.global_doca_state = &dpm_doca_state;
    doca_error_t ret;

    printf("n_ctxs: %d\n", bf3_decompress_deflate_state.n_ctxs);
    bf3_decompress_deflate_state.ctxs = (struct doca_ctx **)malloc(sizeof(struct doca_ctx *) * bf3_decompress_deflate_state.n_ctxs);
    bf3_decompress_deflate_state.compresses =
        (struct doca_compress **)malloc(sizeof(struct doca_compress *) * bf3_decompress_deflate_state.n_ctxs);
    bf3_decompress_deflate_state.pes = (struct doca_pe **)malloc(sizeof(struct doca_pe *) * bf3_decompress_deflate_state.n_ctxs);
    if (bf3_decompress_deflate_state.ctxs == NULL || bf3_decompress_deflate_state.compresses == NULL ||
        bf3_decompress_deflate_state.pes == NULL)
    {
        printf("Unable to allocate memory for ctxs or compresses\n");
        return false;
    }
    for (int i = 0; i < bf3_decompress_deflate_state.n_ctxs; i++)
    {
        bf3_decompress_deflate_state.ctxs[i] = NULL;
        bf3_decompress_deflate_state.compresses[i] = NULL;
        bf3_decompress_deflate_state.pes[i] = NULL;
    }

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

    for (int i = 0; i < bf3_decompress_deflate_state.n_ctxs; i++)
    {

        ret = doca_compress_create(bf3_decompress_deflate_state.global_doca_state->state.dev,
                                   &bf3_decompress_deflate_state.compresses[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create compress engine: %s", doca_error_get_descr(ret));
            return false;
        }
        bf3_decompress_deflate_state.ctxs[i] = doca_compress_as_ctx(bf3_decompress_deflate_state.compresses[i]);

        ret =
            doca_pe_connect_ctx(bf3_decompress_deflate_state.pes[i], bf3_decompress_deflate_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to connect PE to context: %s", doca_error_get_descr(ret));
            return false;
        }

        ret = doca_compress_task_decompress_deflate_set_conf(bf3_decompress_deflate_state.compresses[i],
                                                             decompress_deflate_completed_callback,
                                                             decompress_deflate_error_callback, DPM_SUBMISSION_QUEUE_SIZE);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to set decompress deflate task conf with num_tasks = DPM_SUBMISSION_QUEUE_SIZE = %d, %s",
                   DPM_SUBMISSION_QUEUE_SIZE, doca_error_get_descr(ret));
            return false;
        }
        else
        {
            printf("decompress deflate task conf set\n");
        }

        ret = doca_ctx_start(bf3_decompress_deflate_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Failed to start context: %s\n", doca_error_get_descr(ret));
        }
    }

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

    printf("n_ctxs: %d\n", bf3_decompress_deflate_state.n_ctxs);

    for (int i = 0; i < bf3_decompress_deflate_state.n_ctxs; i++)
    {
        ret = doca_ctx_stop(bf3_decompress_deflate_state.ctxs[i]);
        ret = doca_compress_destroy(bf3_decompress_deflate_state.compresses[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to destroy compress engine: %s", doca_error_get_descr(ret));
            return false;
        }
        bf3_decompress_deflate_state.compresses[i] = NULL;

        /* ret = doca_ctx_stop(bf3_decompress_deflate_state.ctx);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to stop context: %s", doca_error_get_descr(ret));
            return false;
        } */
    }

    // TODO: wait for all tasks to finish
    // printf("Waiting for all tasks to finish...\n");
    printf("bf3 decompress kernel cleanup done\n");
    return true;
}

uint64_t task_num = 0;
dpkernel_error bf3_decompress_deflate_kernel_execute(dpkernel_task_base *dpk_task)
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
    struct doca_buf *src_doca_buf = nullptr;
    struct doca_buf *dst_doca_buf = nullptr;

    ret = doca_buf_inventory_buf_get_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.src_mmap, in,
                                             dpk_task->in_size, &src_doca_buf);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to acquire DOCA buffer for src buffer: %s\n", doca_error_get_name(ret));
        return DPK_ERROR_FAILED;
    }
    ret = doca_buf_inventory_buf_get_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.dst_mmap, out,
                                             dpk_task->out_size, &dst_doca_buf);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to acquire DOCA buffer for dst buffer: %s\n", doca_error_get_name(ret));
        return DPK_ERROR_FAILED;
    }

    ret = doca_buf_set_data(src_doca_buf, in, dpk_task->in_size);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to set DOCA src buffer data: %s\n", doca_error_get_name(ret));
        doca_buf_dec_refcount(src_doca_buf, NULL);
        doca_buf_dec_refcount(dst_doca_buf, NULL);
        return DPK_ERROR_FAILED;
    }

    // finally, set this to the task
    dpk_task->src_doca_buf = src_doca_buf;
    dpk_task->dst_doca_buf = dst_doca_buf;

    task_user_data.ptr = (void *)dpk_task;
    ret = doca_compress_task_decompress_deflate_alloc_init(bf3_decompress_deflate_state.compress, src_doca_buf,
                                                           dst_doca_buf, task_user_data, &decompress_deflate_task);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to alloc init decompress deflate task: %s", doca_error_get_descr(ret));
        doca_buf_dec_refcount(dst_doca_buf, NULL);
        doca_buf_dec_refcount(src_doca_buf, NULL);
        return DPK_ERROR_FAILED;
    }
    decompress_task = doca_compress_task_decompress_deflate_as_task(decompress_deflate_task);

    dpk_task->user_data = (void *)std::chrono::high_resolution_clock::now().time_since_epoch().count();
    // dpk_task->user_data = (void *)task_num++;
    ret = doca_task_try_submit(decompress_task);

    if (ret == DOCA_SUCCESS)
    {
        print_debug("submit to decompress success\n");
        return DPK_SUCCESS;
    }
    else if (ret == DOCA_ERROR_NO_MEMORY)
    {
        printf("submit to decompress failed, no memory (queue full?): %s\n", doca_error_get_name(ret));
        doca_buf_dec_refcount(dst_doca_buf, NULL);
        doca_buf_dec_refcount(src_doca_buf, NULL);
        doca_task_free(decompress_task);
        return DPK_ERROR_FAILED;
    }
    else
    {
        printf("Failed to submit compress job: %s", doca_error_get_name(ret));
        doca_buf_dec_refcount(dst_doca_buf, NULL);
        doca_buf_dec_refcount(src_doca_buf, NULL);
        doca_task_free(decompress_task);
        return DPK_ERROR_FAILED;
    }
    // printf("dst_buff: %p, src_buff: %p\n", decompress_job.dst_buff, decompress_job.src_buff);
    // printf("task ptr: %p\n", task);
}

dpkernel_error bf3_decompress_deflate_kernel_poll(dpkernel_task_base *task)
{
    // doca_error_t ret;
    // struct doca_event event = {0};

    if (doca_pe_progress(bf3_decompress_deflate_state.global_doca_state->state.pe) == 1)
    {
        print_debug("got a completion, callback should be called already?\n");
        return DPK_SUCCESS;
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