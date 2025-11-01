#include "executor.hpp"
#include "dpm_interface.hpp"

struct null_executor null_exec = {};

/* bool null_executor_execute(void *in, size_t in_size, void *out, size_t *out_size, void *user_data)
{
    for (auto &comp : null_exec.completions)
    {
        if (comp.error.load() == DPK_ONGOING)
        {
            // mock up
            comp.user_data = user_data;
            comp.out = out;
            comp.out_size = in_size;

            // success now, dp manager will poll and find this
            comp.error.store(DPK_SUCCESS);
            return true;
        }
    }
    return false;
} */

// TODO: out_size is a pointer?
/* bool null_executor_msgq_execute(void *in, size_t in_size, void *out, size_t *out_size, void *user_data)
{
#ifdef TEST_USE_MEMCPY
    return null_exec.task_queue.bounded_push(
        dpkernel_task_base{.in = in, .in_size = in_size, .out_size = *out_size, .user_data = user_data});
#else
    return null_exec.task_queue.bounded_push(
        dpkernel_task_base{.in = in, .in_size = in_size, .out = out, .out_size = *out_size, .user_data = user_data});
#endif
} */

bool null_executor_shm_execute(dpkernel_task_base *task)
{
#ifdef TEST_USE_MEMCPY
    return null_exec.task_queue.bounded_push(
        dpkernel_task_base{.in = in, .in_size = in_size, .out_size = *out_size, .user_data = user_data});
#else
    /* char *in = dpm_get_input_ptr_from_shmptr(task->in);
    char *out = dpm_get_output_ptr_from_shmptr(task->out); */
    // memset(out, 42, task->out_size);
    print_debug("null executor written to out\n");
    // emulate completion
    // task->actual_out_size = 0xdeadbeef;
    task->completion.store(DPK_SUCCESS);

    // don't enqueue, just complete immediately
    return true;
    //// return null_exec.enqueue_task(task);
#endif
}

bool null_executor_shm_poller(dpkernel_task_base **task)
{
    // printf("null_executor_shm_poller\n");
    // just return true
    return true;

    // try to pop a task from the queue
    if (null_exec.dequeue_task(task))
    {
        print_debug("null_executor_shm_poller got task\n");
        print_debug("actual_out_size: %lu\n", (*task)->actual_out_size);
        (*task)->completion.store(DPK_SUCCESS);

        return true;
    }
    else
    {
        return false;
    }
}

/* dpkernel_completion *null_executor_poller()
{
    auto &shm_comp = submission_completion_shm->completion;

    // check all and find a completion of null executor
    for (auto &completion : null_exec.completions)
    {
        if (completion.error.load() == DPK_SUCCESS)
        {
            return &completion;
        }
    }
    return nullptr;
} */

/* bool null_executor_msgq_poller(dpkernel_completion *completion)
{
    dpkernel_task_base task;
    if (null_exec.task_queue.pop(task))
    {
        completion->user_data = task.user_data;
#ifdef TEST_USE_MEMCPY
        memcpy(completion->out, task.out, task.out_size);
#else
        completion->out = task.out;
#endif
        completion->out_size = task.out_size;
        completion->error.store(DPK_SUCCESS);
        return true;
    }
    else
    {
        return false;
    }
} */

/* bool null_executor_msgq_is_full()
{
    return false;
} */

// DOCA_LOG_REGISTER("executor");
bool decompress_executor_shm_execute(dpkernel_task_base *task)
{
    printf("in shm_ptr: %lu, out shm_ptr: %lu\n", task->in, task->out);
    // get local ptr from shm ptr of in and out buffers
    char *in = dpm_get_input_ptr_from_shmptr(task->in);
    char *out = dpm_get_output_ptr_from_shmptr(task->out);

    doca_error_t ret;
    // get doca bufs out of raw bufs
    struct doca_buf *src_doca_buf = (struct doca_buf *)malloc(sizeof(src_doca_buf));
    struct doca_buf *dst_doca_buf = (struct doca_buf *)malloc(sizeof(dst_doca_buf));

    ret = doca_buf_inventory_buf_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.src_mmap, in, task->in_size,
                                         &src_doca_buf);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to acquire DOCA buffer for src buffer: %s\n", doca_get_error_string(ret));
        return false;
    }
    ret = doca_buf_inventory_buf_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.dst_mmap, out,
                                         task->out_size, &dst_doca_buf);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to acquire DOCA buffer for dst buffer: %s\n", doca_get_error_string(ret));
        return false;
    }

    ret = doca_buf_set_data(src_doca_buf, in, task->in_size);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to set DOCA src buffer data: %s\n", doca_get_error_string(ret));
        doca_buf_refcount_rm(src_doca_buf, NULL);
        doca_buf_refcount_rm(dst_doca_buf, NULL);
        return false;
    }

    // finally, set this to the task
    task->src_doca_buf = src_doca_buf;
    task->dst_doca_buf = dst_doca_buf;

    // construct the task
    struct doca_compress_deflate_job decompress_job;
    decompress_job = {
        .base =
            (struct doca_job){
                .type = DOCA_DECOMPRESS_DEFLATE_JOB,
                .flags = DOCA_JOB_FLAGS_NONE,
                .ctx = dpm_doca_state.state.ctx,
                .user_data = {.ptr = (void *)task}, // use this to get the actual out size after completion
            },
        .dst_buff = dst_doca_buf,
        .src_buff = src_doca_buf,
    };
    printf("dst_buff: %p, src_buff: %p\n", decompress_job.dst_buff, decompress_job.src_buff);
    printf("task ptr: %p\n", task);
    // try to enqueue the decompress task
    ret = doca_workq_submit(dpm_doca_state.state.workq, &decompress_job.base);
    if (ret == DOCA_ERROR_NO_MEMORY)
    {
        printf("decompress doca queue full, try again later\n");
        return false;
    }

    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to submit compress job: %s", doca_get_error_string(ret));
        doca_buf_refcount_rm(dst_doca_buf, NULL);
        doca_buf_refcount_rm(src_doca_buf, NULL);
        return false;
    }

    return true;
}

bool decompress_executor_shm_poller(dpkernel_task_base **task)
{
    *task = NULL;

    doca_error_t ret;
    struct doca_event event = {0};
    ret = doca_workq_progress_retrieve(dpm_doca_state.state.workq, &event, DOCA_WORKQ_RETRIEVE_FLAGS_NONE);

    if (ret == DOCA_ERROR_AGAIN)
    {
        // normal case, try again later
        // printf("decompress_executor_shm_poller try again later\n");
        return false;
    }
    else if (ret == DOCA_SUCCESS)
    {
        // got a completion
        *task = (dpkernel_task_base *)event.user_data.ptr; // we store the task in user_data during execute
        printf("task ptr: %p\n", *task);
        printf("task->src_doca_buf: %p, task->dst_doca_buf: %p\n", (*task)->src_doca_buf, (*task)->dst_doca_buf);
        size_t data_len;
        size_t buf_len;
        doca_buf_get_data_len((*task)->dst_doca_buf, &data_len);
        doca_buf_get_len((*task)->dst_doca_buf, &buf_len);
        (*task)->actual_out_size = data_len;
        printf("data_len: %lu, buf_len: %lu\n", data_len, buf_len);
        printf("decompress_executor_shm_poller got completion with actual_out_size: %lu\n", (*task)->actual_out_size);
        if (doca_buf_refcount_rm((*task)->src_doca_buf, NULL) != DOCA_SUCCESS ||
            doca_buf_refcount_rm((*task)->dst_doca_buf, NULL) != DOCA_SUCCESS)
        {
            print_debug("Failed to decrease DOCA buffer reference count for buffers after completion\n");
        }
        // TODO: use proper memory management? note: rm ref cnt seems to free it?
        // free((*task)->src_doca_buf);
        // free((*task)->dst_doca_buf);
        (*task)->completion.store(DPK_SUCCESS);
        printf("set flag to DPK_SUCCESS\n");
        return true;
    }
    else
    {
        print_debug("Failed to retrieve workq progress: %s", doca_get_error_string(ret));
        return false;
    }
}

bool regex_executor_shm_execute(dpkernel_task_base *task)
{
    // get local ptr from shm ptr of in and out buffers
    char *in = dpm_get_input_ptr_from_shmptr(task->in);
    char *out = dpm_get_output_ptr_from_shmptr(task->out);

    doca_error_t ret;
    // get doca bufs out of raw bufs
    struct doca_buf *src_doca_buf = (struct doca_buf *)malloc(sizeof(src_doca_buf));
    struct doca_buf *dst_doca_buf = (struct doca_buf *)malloc(sizeof(dst_doca_buf));

    ret = doca_buf_inventory_buf_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.src_mmap, in, task->in_size,
                                         &src_doca_buf);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to acquire DOCA buffer for src buffer: %s\n", doca_get_error_string(ret));
        return false;
    }
    ret = doca_buf_inventory_buf_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.dst_mmap, out,
                                         task->out_size, &dst_doca_buf);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to acquire DOCA buffer for dst buffer: %s\n", doca_get_error_string(ret));
        return false;
    }

    ret = doca_buf_set_data(src_doca_buf, in, task->in_size);
    if (ret != DOCA_SUCCESS)
    {
        printf("Unable to set DOCA src buffer data: %s\n", doca_get_error_string(ret));
        doca_buf_refcount_rm(src_doca_buf, NULL);
        doca_buf_refcount_rm(dst_doca_buf, NULL);
        return false;
    }

    // finally, set this to the task
    task->src_doca_buf = src_doca_buf;
    task->dst_doca_buf = dst_doca_buf;

    // regex results array
    struct doca_regex_search_result *results =
        (struct doca_regex_search_result *)calloc(NB_REGEX_RESULTS, sizeof(struct doca_regex_search_result));

    // construct the task
    struct doca_regex_job_search job_request;
    job_request = {
        .base =
            (struct doca_job){
                .type = DOCA_REGEX_JOB_SEARCH,
                .flags = DOCA_JOB_FLAGS_NONE,
                .ctx = dpm_doca_state.state.ctx,
                .user_data = {.ptr = (void *)task}, // use this to get the actual out size after completion
            },
        .rule_group_ids = {0},
        .buffer = src_doca_buf,
        .result = results,
        .allow_batching = false,
    };
    ret = doca_workq_submit(dpm_doca_state.state.workq, &job_request.base);
    if (ret == DOCA_ERROR_NO_MEMORY)
    {
        printf("regex doca queue full, try again later\n");
        doca_buf_refcount_rm(src_doca_buf, NULL);
        return false;
    }
    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to submit regex job: %s\n", doca_get_error_string(ret));
        doca_buf_refcount_rm(src_doca_buf, NULL);
        doca_buf_refcount_rm(dst_doca_buf, NULL);
        return false;
    }
}

bool regex_executor_shm_poller(dpkernel_task_base **task)
{
    *task = NULL;

    doca_error_t ret;
    struct doca_event event = {0};
    ret = doca_workq_progress_retrieve(dpm_doca_state.state.workq, &event, DOCA_WORKQ_RETRIEVE_FLAGS_NONE);

    if (ret == DOCA_ERROR_AGAIN)
    {
        // normal case, try again later
        // printf("decompress_executor_shm_poller try again later\n");
        return false;
    }
    else if (ret == DOCA_SUCCESS)
    {
        // got a completion
        *task = (dpkernel_task_base *)event.user_data.ptr; // we store the task in user_data during execute
        printf("task ptr: %p\n", *task);
        printf("task->src_doca_buf: %p, task->dst_doca_buf: %p\n", (*task)->src_doca_buf, (*task)->dst_doca_buf);
        size_t data_len;
        size_t buf_len;
        struct doca_regex_search_result *result = (struct doca_regex_search_result *)event.result.ptr;
        result->detected_matches;
        struct doca_regex_match *ptr = result->matches;
        ptr->next;
    }
}
