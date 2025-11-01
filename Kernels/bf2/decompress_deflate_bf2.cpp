#include "decompress_deflate_bf2.hpp"

struct dpm_doca_decompress_deflate_bf2 bf2_decompress_deflate_state;

bool bf2_decompress_deflate_kernel_init()
{
    bf2_decompress_deflate_state.global_doca_state = &dpm_doca_state;
    doca_error_t ret;

    ret = doca_compress_create(&bf2_decompress_deflate_state.compress);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to create compress engine: %s", doca_get_error_string(ret));
        return false;
    }
    bf2_decompress_deflate_state.ctx = doca_compress_as_ctx(bf2_decompress_deflate_state.compress);

    auto dev = bf2_decompress_deflate_state.global_doca_state->state.dev;
    ret = doca_ctx_dev_add(bf2_decompress_deflate_state.ctx, dev);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to register device with lib context: %s\n", doca_get_error_string(ret));
        bf2_decompress_deflate_state.ctx = NULL;
        return ret;
    }

    ret = doca_ctx_start(bf2_decompress_deflate_state.ctx);
    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to start context: %s\n", doca_get_error_string(ret));
        return false;
    }

    // init the doca work queue
    ret = doca_workq_create(DPM_DOCA_WORKQ_DEPTH, &bf2_decompress_deflate_state.workq);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to create work queue for decompress: %s", doca_get_error_string(ret));
        return false;
    }
    ret = doca_ctx_workq_add(bf2_decompress_deflate_state.ctx, bf2_decompress_deflate_state.workq);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to add work queue to context: %s\n", doca_get_error_string(ret));
        return false;
    }

    return true;
}

dpkernel_error bf2_decompress_deflate_kernel_execute(dpkernel_task_base *task)
{
    // printf("in shm_ptr: %lu, out shm_ptr: %lu\n", task->in, task->out);

    // get local ptr from shm ptr of in and out buffers
    char *in = dpm_get_input_ptr_from_shmptr(task->in);
    char *out = dpm_get_output_ptr_from_shmptr(task->out);

    doca_error_t ret;
    // get doca bufs out of raw bufs
    struct doca_buf *src_doca_buf;
    struct doca_buf *dst_doca_buf; // = (struct doca_buf *)malloc(sizeof(dst_doca_buf));

    ret = doca_buf_inventory_buf_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.src_mmap, in, task->in_size,
                                         &src_doca_buf);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to acquire DOCA buffer for src buffer: %s\n", doca_get_error_string(ret));
        return DPK_ERROR_FAILED;
    }
    ret = doca_buf_inventory_buf_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.dst_mmap, out,
                                         task->out_size, &dst_doca_buf);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to acquire DOCA buffer for dst buffer: %s\n", doca_get_error_string(ret));
        return DPK_ERROR_FAILED;
    }

    ret = doca_buf_set_data(src_doca_buf, in, task->in_size);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to set DOCA src buffer data: %s\n", doca_get_error_string(ret));
        doca_buf_refcount_rm(src_doca_buf, NULL);
        doca_buf_refcount_rm(dst_doca_buf, NULL);
        return DPK_ERROR_FAILED;
    }

    // finally, set this to the task
    task->src_doca_buf = src_doca_buf;
    task->dst_doca_buf = dst_doca_buf;

    // construct the task
    struct doca_compress_job decompress_job;
    decompress_job = {
        .base =
            (struct doca_job){
                .type = DOCA_DECOMPRESS_DEFLATE_JOB,
                .flags = DOCA_JOB_FLAGS_NONE,
                .ctx = bf2_decompress_deflate_state.ctx,
                .user_data = {.ptr = (void *)task}, // use this to get the actual out size after completion
            },
        .dst_buff = dst_doca_buf,
        .src_buff = src_doca_buf,
    };
    // printf("dst_buff: %p, src_buff: %p\n", decompress_job.dst_buff, decompress_job.src_buff);
    // printf("task ptr: %p\n", task);

    // try to enqueue the decompress task
    ret = doca_workq_submit(bf2_decompress_deflate_state.workq, &decompress_job.base);
    if (ret == DOCA_ERROR_NO_MEMORY)
    {
        printf("decompress doca queue full, try again later\n");
        return DPK_ERROR_AGAIN;
    }

    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to submit compress job: %s", doca_get_error_string(ret));
        doca_buf_refcount_rm(dst_doca_buf, NULL);
        doca_buf_refcount_rm(src_doca_buf, NULL);
        return DPK_ERROR_FAILED;
    }

    return DPK_SUCCESS;
}

dpkernel_error bf2_decompress_deflate_kernel_poll(dpkernel_task_base *task)
{
    // *task = NULL; // don't touch the original task from the app

    doca_error_t ret;
    struct doca_event event = {0};
    ret = doca_workq_progress_retrieve(bf2_decompress_deflate_state.workq, &event, DOCA_WORKQ_RETRIEVE_FLAGS_NONE);

    if (ret == DOCA_ERROR_AGAIN)
    {
        // normal case, try again later
        return DPK_ERROR_AGAIN;
    }
    else if (ret == DOCA_SUCCESS)
    {
        // got a completion
        task = (dpkernel_task_base *)event.user_data.ptr; // we store the task in user_data during execute
        // printf("task ptr: %p\n", task);
        // printf("task->src_doca_buf: %p, task->dst_doca_buf: %p\n", task->src_doca_buf, task->dst_doca_buf);
        size_t data_len;
        size_t buf_len;
        doca_buf_get_data_len(task->dst_doca_buf, &data_len);
        doca_buf_get_len(task->dst_doca_buf, &buf_len);
        task->actual_out_size = data_len;
        // printf("data_len: %lu, buf_len: %lu\n", data_len, buf_len);
        // printf("decompress_executor_shm_poller got completion with actual_out_size: %lu\n", task->actual_out_size);
        if (doca_buf_refcount_rm(task->src_doca_buf, NULL) != DOCA_SUCCESS ||
            doca_buf_refcount_rm(task->dst_doca_buf, NULL) != DOCA_SUCCESS)
        {
            printf("Failed to decrease DOCA buffer reference count for buffers after completion\n");
        }

        // dpm will set the task->completion flag to DPK_SUCCESS when we return so
        task->completion.store(DPK_SUCCESS);
        // TODO: why do I need to set completion here? should be just returning and set outside!!!
        // printf("set flag to DPK_SUCCESS\n");
        return DPK_SUCCESS;
    }
    else
    {
        printf("Failed to retrieve workq progress: %s", doca_get_error_string(ret));
        // task->completion.store(DPK_ERROR_FAILED);
        return DPK_ERROR_FAILED;
    }
}

bool bf2_decompress_deflate_kernel_cleanup()
{
    doca_error_t ret;

    ret = doca_compress_destroy(bf2_decompress_deflate_state.compress);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to destroy compress engine: %s", doca_get_error_string(ret));
        return false;
    }
    bf2_decompress_deflate_state.compress = NULL;

    ret = doca_ctx_stop(bf2_decompress_deflate_state.ctx);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to stop context: %s", doca_get_error_string(ret));
        return false;
    }

    // TODO: wait for all tasks to finish
    // printf("Waiting for all tasks to finish...\n");
    printf("bf2 decompress kernel cleanup done\n");
    return true;
}

uint64_t bf2_decompress_deflate_kernel_get_estimated_completion_time()
{
    // TODO:
    return 1000;
}