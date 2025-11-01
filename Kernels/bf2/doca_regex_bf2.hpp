#pragma once
#include "kernel_interface.hpp"
#include <sys/types.h>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_compress.h>
#include <doca_ctx.h>
#include <doca_error.h>
#include <doca_log.h>

#include <doca_regex.h>

#include "bf2_device.hpp"

struct dpm_doca_regex_bf2
{
    struct dpm_doca_state *global_doca_state;
    struct doca_workq *workq;
    struct doca_ctx *ctx;
    struct doca_regex *regex;
};

struct dpm_doca_regex_bf2 bf2_regex_state;

dpkernel_error regex_executor_shm_execute(dpkernel_task_base *task);
dpkernel_error regex_executor_shm_execute(dpkernel_task_base *task)
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
                .ctx = bf2_regex_state.ctx,
                .user_data = {.ptr = (void *)task}, // use this to get the actual out size after completion
            },
        .rule_group_ids = {0},
        .buffer = src_doca_buf,
        .result = results,
        .allow_batching = false,
    };
    ret = doca_workq_submit(bf2_regex_state.workq, &job_request.base);
    if (ret == DOCA_ERROR_NO_MEMORY)
    {
        printf("regex doca queue full, try again later\n");
        doca_buf_refcount_rm(src_doca_buf, NULL);
        return DPK_ERROR_AGAIN;
    }
    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to submit regex job: %s\n", doca_get_error_string(ret));
        doca_buf_refcount_rm(src_doca_buf, NULL);
        doca_buf_refcount_rm(dst_doca_buf, NULL);
        return DPK_ERROR_FAILED;
    }
    return DPK_SUCCESS;
}

dpkernel_error regex_executor_shm_poller(dpkernel_task_base *task);
dpkernel_error regex_executor_shm_poller(dpkernel_task_base *task)
{
    // *task = NULL;

    doca_error_t ret;
    struct doca_event event = {0};
    ret = doca_workq_progress_retrieve(bf2_regex_state.workq, &event, DOCA_WORKQ_RETRIEVE_FLAGS_NONE);

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
        struct doca_regex_search_result *result = (struct doca_regex_search_result *)event.result.ptr;
        result->detected_matches;
        struct doca_regex_match *ptr = result->matches;
        ptr->next;

        //////
        return DPK_SUCCESS;
    }
    else
    {
        printf("regex: Failed to retrieve workq progress: %s", doca_get_error_string(ret));
        return DPK_ERROR_FAILED;
    }
}