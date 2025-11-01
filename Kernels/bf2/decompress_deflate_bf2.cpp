#include "decompress_deflate_bf2.hpp"
#include <sched.h>

struct dpm_doca_decompress_deflate_bf2 bf2_decompress_deflate_state;

// spin lock to protect doca_buf_inventory_buf_get_by_addr
pthread_spinlock_t buf_inventory_lock;

bool bf2_decompress_deflate_kernel_init(struct dpm_kernel_registration_result *result)
{
    doca_error_t ret;

    pthread_spin_init(&buf_inventory_lock, PTHREAD_PROCESS_PRIVATE);

    int n_cpus = 0;
    CPU_ZERO(&result->cpu_mask);
    for (int i = 0; i < 32; ++i)
    {
        if ((BF2_DECOMPRESS_DEFLATE_CPU_MASK >> i) & 1)
        {
            CPU_SET(i, &result->cpu_mask);
            n_cpus++;
        }
    }
    printf("bf2_decompress_deflate_kernel_init cpu mask: %x, n_cpus = %d\n", BF2_DECOMPRESS_DEFLATE_CPU_MASK, n_cpus);

    // malloc the multithreaded ctxs etc
    bf2_decompress_deflate_state.compresses = (struct doca_compress **)malloc(sizeof(struct doca_compress *) * n_cpus);
    bf2_decompress_deflate_state.ctxs = (struct doca_ctx **)malloc(sizeof(struct doca_ctx *) * n_cpus);
    bf2_decompress_deflate_state.workqs = (struct doca_workq **)malloc(sizeof(struct doca_workq *) * n_cpus);
    if (bf2_decompress_deflate_state.compresses == NULL || bf2_decompress_deflate_state.ctxs == NULL ||
        bf2_decompress_deflate_state.workqs == NULL)
    {
        printf("Unable to allocate memory for bf2 decompress deflate state\n");
        return false;
    }

    bf2_decompress_deflate_state.global_doca_state = &dpm_doca_state;

    // for each cpu, create a compress engine and a context
    for (int i = 0; i < n_cpus; i++)
    {
        ret = doca_compress_create(&bf2_decompress_deflate_state.compresses[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create compress engine %d: %s\n", i, doca_get_error_string(ret));
            return false;
        }

        bf2_decompress_deflate_state.ctxs[i] = doca_compress_as_ctx(bf2_decompress_deflate_state.compresses[i]);
        auto dev = bf2_decompress_deflate_state.global_doca_state->state.dev;
        ret = doca_ctx_dev_add(bf2_decompress_deflate_state.ctxs[i], dev);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to register device with context %d: %s\n", i, doca_get_error_string(ret));
            bf2_decompress_deflate_state.ctxs[i] = NULL;
            return ret;
        }

        ret = doca_ctx_start(bf2_decompress_deflate_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Failed to start context %d: %s\n", i, doca_get_error_string(ret));
            return false;
        }
        // init the doca work queue
        ret = doca_workq_create(DPM_DOCA_WORKQ_DEPTH, &bf2_decompress_deflate_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create work queue for decompress %d: %s", i, doca_get_error_string(ret));
            return false;
        }
        ret = doca_ctx_workq_add(bf2_decompress_deflate_state.ctxs[i], bf2_decompress_deflate_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to add work queue to context %d: %s\n", i, doca_get_error_string(ret));
            return false;
        }
    }
    return true;

    //// single threaded original version below

    /* ret = doca_compress_create(&bf2_decompress_deflate_state.compress);
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

    return true; */
}

bool bf2_decompress_deflate_handle_mem_req(struct dpm_mem_req *req)
{
    // TODO:
    doca_error_t ret;
    dpkernel_task_base *task = dpm_get_task_ptr_from_shmptr(req->task);

    switch (req->type)
    {
    case DPM_MEM_REQ_ALLOC_INPUT:
        // printf("bf2 decompress deflate handle mem req alloc input\n");
        ret = doca_buf_inventory_buf_by_addr(bf2_decompress_deflate_state.global_doca_state->state.buf_inv,
                                             bf2_decompress_deflate_state.global_doca_state->state.src_mmap,
                                             dpm_get_input_ptr_from_shmptr(task->in), task->in_size,
                                             &task->src_doca_buf);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf2 decompress deflate failed to get input buf from inventory: %s\n", doca_get_error_string(ret));
            return false;
        }
        ret = doca_buf_set_data(task->src_doca_buf, dpm_get_input_ptr_from_shmptr(task->in), task->in_size);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf2 decompress deflate failed to set input buf data: %s\n", doca_get_error_string(ret));
            return false;
        }
        break;
    case DPM_MEM_REQ_ALLOC_OUTPUT:
        // printf("bf2 decompress deflate handle mem req alloc output\n");
        ret = doca_buf_inventory_buf_by_addr(bf2_decompress_deflate_state.global_doca_state->state.buf_inv,
                                             bf2_decompress_deflate_state.global_doca_state->state.dst_mmap,
                                             dpm_get_output_ptr_from_shmptr(task->out), task->out_size,
                                             &task->dst_doca_buf);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf2 decompress deflate failed to get output buf from inventory: %s\n", doca_get_error_string(ret));
            return false;
        }
        break;
    case DPM_MEM_REQ_FREE_INPUT:
        // printf("bf2 decompress deflate handle mem req free input\n");
        // free the input buffer
        ret = doca_buf_refcount_rm(task->src_doca_buf, NULL);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf2 decompress deflate failed to free input buf: %s\n", doca_get_error_string(ret));
            return false;
        }
        task->src_doca_buf = NULL;
        break;
    case DPM_MEM_REQ_FREE_OUTPUT:
        // printf("bf2 decompress deflate handle mem req free output\n");
        // free the output buffer
        ret = doca_buf_refcount_rm(task->dst_doca_buf, NULL);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf2 decompress deflate failed to free output buf: %s\n", doca_get_error_string(ret));
            return false;
        }
        task->dst_doca_buf = NULL;
        break;
    case DPM_MEM_REQ_ALLOC_TASK:
        // nothing to do
        // printf("bf2 decompress deflate handle mem req alloc task, nothing to do\n");
        return true;
    case DPM_MEM_REQ_FREE_TASK:
        // nothing to do
        // printf("bf2 decompress deflate handle mem req free task, nothing to do\n");
        return true;
    default:
        printf("bf2 decompress deflate handle mem req unknown type: %d\n", req->type);
        return false;
    }
}

dpkernel_error bf2_decompress_deflate_kernel_execute(dpkernel_task_base *task, int thread_id)
{
    // printf("in shm_ptr: %lu, out shm_ptr: %lu\n", task->in, task->out);

    // get local ptr from shm ptr of in and out buffers
    char *in = dpm_get_input_ptr_from_shmptr(task->in);
    char *out = dpm_get_output_ptr_from_shmptr(task->out);

    doca_error_t ret;

    // NO LONGER DONE HERE: will be part of mem mgmt. get doca bufs out of raw bufs
    /* struct doca_buf *src_doca_buf;
    struct doca_buf *dst_doca_buf; // = (struct doca_buf *)malloc(sizeof(dst_doca_buf)); */

    // NO LONGER DONE HERE: will be part of mem mgmt
    /* // finally, set this to the task
    task->src_doca_buf = src_doca_buf;
    task->dst_doca_buf = dst_doca_buf; */

    // construct the task
    struct doca_compress_job decompress_job;
    decompress_job = {
        .base =
            (struct doca_job){
                .type = DOCA_DECOMPRESS_DEFLATE_JOB,
                .flags = DOCA_JOB_FLAGS_NONE,
                .ctx = bf2_decompress_deflate_state.ctxs[thread_id],
                .user_data = {.ptr = (void *)task}, // use this to get the actual out size after completion
            },
        .dst_buff = task->dst_doca_buf,
        .src_buff = task->src_doca_buf,
    };
    // printf("dst_buff: %p, src_buff: %p\n", decompress_job.dst_buff, decompress_job.src_buff);
    // printf("task ptr: %p\n", task);

    // try to enqueue the decompress task
    ret = doca_workq_submit(bf2_decompress_deflate_state.workqs[thread_id], &decompress_job.base);
    if (ret == DOCA_ERROR_NO_MEMORY)
    {
        printf("decompress doca queue full, try again later\n");
        return DPK_ERROR_AGAIN;
    }

    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to submit compress job: %s", doca_get_error_string(ret));
        doca_buf_refcount_rm(task->dst_doca_buf, NULL);
        doca_buf_refcount_rm(task->src_doca_buf, NULL);
        return DPK_ERROR_FAILED;
    }

    return DPK_SUCCESS;
}

dpkernel_error bf2_decompress_deflate_kernel_poll(dpkernel_task_base *task, int thread_id)
{
    doca_error_t ret;
    struct doca_event event = {0};
    ret = doca_workq_progress_retrieve(bf2_decompress_deflate_state.workqs[thread_id], &event, DOCA_WORKQ_RETRIEVE_FLAGS_NONE);

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
        task->completion.store(DPK_SUCCESS, std::memory_order_release);
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
    int n_cpus = 0;
    for (int i = 0; i < 64; ++i)
    {
        if ((BF2_DECOMPRESS_DEFLATE_CPU_MASK >> i) & 1)
        {
            n_cpus++;
        }
    }
    for (int i = 0; i < n_cpus; i++)
    {
        ret = doca_ctx_workq_rm(bf2_decompress_deflate_state.ctxs[i], bf2_decompress_deflate_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to remove work queue from context %d: %s\n", i, doca_get_error_string(ret));
            return false;
        }
        ret = doca_workq_destroy(bf2_decompress_deflate_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to destroy work queue %d: %s\n", i, doca_get_error_string(ret));
            return false;
        }

        ret = doca_compress_destroy(bf2_decompress_deflate_state.compresses[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to destroy compress engine %d: %s", i, doca_get_error_string(ret));
            return false;
        }
        ret = doca_ctx_stop(bf2_decompress_deflate_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to stop context %d: %s", i, doca_get_error_string(ret));
            return false;
        }
    }
    free(bf2_decompress_deflate_state.compresses);
    free(bf2_decompress_deflate_state.ctxs);
    free(bf2_decompress_deflate_state.workqs);
    // printf("Waiting for all tasks to finish...\n");
    printf("bf2 decompress kernel cleanup done\n");
    return true;
}

/* uint64_t bf2_decompress_deflate_kernel_get_estimated_completion_time()
{
    // TODO:
    return 1000;
} */