#pragma once
#include "executor.hpp"
// #include <sys/types.h>

dpkernel_error bf3_decompress_deflate_kernel_execute(dpkernel_task_base *task);
dpkernel_error bf3_decompress_deflate_kernel_execute(dpkernel_task_base *task)
{
    // printf("in shm_ptr: %lu, out shm_ptr: %lu\n", task->in, task->out);
    // // get local ptr from shm ptr of in and out buffers
    // char *in = dpm_get_input_ptr_from_shmptr(task->in);
    // char *out = dpm_get_output_ptr_from_shmptr(task->out);

    // doca_error_t ret;
    // // get doca bufs out of raw bufs
    // struct doca_buf *src_doca_buf = (struct doca_buf *)malloc(sizeof(src_doca_buf));
    // struct doca_buf *dst_doca_buf = (struct doca_buf *)malloc(sizeof(dst_doca_buf));

    // ret = doca_buf_inventory_buf_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.src_mmap, in,
    // task->in_size,
    //                                      &src_doca_buf);
    // if (ret != DOCA_SUCCESS)
    // {
    //     printf("Unable to acquire DOCA buffer for src buffer: %s\n", doca_get_error_string(ret));
    //     return DPK_ERROR_FAILED;
    // }
    // ret = doca_buf_inventory_buf_by_addr(dpm_doca_state.state.buf_inv, dpm_doca_state.state.dst_mmap, out,
    //                                      task->out_size, &dst_doca_buf);
    // if (ret != DOCA_SUCCESS)
    // {
    //     printf("Unable to acquire DOCA buffer for dst buffer: %s\n", doca_get_error_string(ret));
    //     return DPK_ERROR_FAILED;
    // }

    // ret = doca_buf_set_data(src_doca_buf, in, task->in_size);
    // if (ret != DOCA_SUCCESS)
    // {
    //     printf("Unable to set DOCA src buffer data: %s\n", doca_get_error_string(ret));
    //     doca_buf_refcount_rm(src_doca_buf, NULL);
    //     doca_buf_refcount_rm(dst_doca_buf, NULL);
    //     return DPK_ERROR_FAILED;
    // }

    // // finally, set this to the task
    // task->src_doca_buf = src_doca_buf;
    // task->dst_doca_buf = dst_doca_buf;

    // // construct the task
    // struct doca_compress_deflate_job decompress_job;
    // decompress_job = {
    //     .base =
    //         (struct doca_job){
    //             .type = DOCA_DECOMPRESS_DEFLATE_JOB,
    //             .flags = DOCA_JOB_FLAGS_NONE,
    //             .ctx = dpm_doca_state.state.ctx,
    //             .user_data = {.ptr = (void *)task}, // use this to get the actual out size after completion
    //         },
    //     .dst_buff = dst_doca_buf,
    //     .src_buff = src_doca_buf,
    // };
    // printf("dst_buff: %p, src_buff: %p\n", decompress_job.dst_buff, decompress_job.src_buff);
    // printf("task ptr: %p\n", task);
    // // try to enqueue the decompress task
    // ret = doca_workq_submit(dpm_doca_state.state.workq, &decompress_job.base);
    // if (ret == DOCA_ERROR_NO_MEMORY)
    // {
    //     printf("decompress doca queue full, try again later\n");
    //     return DPK_ERROR_AGAIN;
    // }

    // if (ret != DOCA_SUCCESS)
    // {
    //     printf("Failed to submit compress job: %s", doca_get_error_string(ret));
    //     doca_buf_refcount_rm(dst_doca_buf, NULL);
    //     doca_buf_refcount_rm(src_doca_buf, NULL);
    //     return DPK_ERROR_FAILED;
    // }

    // return DPK_SUCCESS;
}

dpkernel_error bf3_decompress_deflate_kernel_poll(dpkernel_task_base *task);
dpkernel_error bf3_decompress_deflate_kernel_poll(dpkernel_task_base *task)
{
    // // *task = NULL; // don't touch the original task from the app

    // doca_error_t ret;
    // struct doca_event event = {0};
    // ret = doca_workq_progress_retrieve(dpm_doca_state.state.workq, &event, DOCA_WORKQ_RETRIEVE_FLAGS_NONE);

    // if (ret == DOCA_ERROR_AGAIN)
    // {
    //     // normal case, try again later
    //     // printf("decompress_executor_shm_poller try again later\n");
    //     return DPK_ERROR_AGAIN;
    // }
    // else if (ret == DOCA_SUCCESS)
    // {
    //     // got a completion
    //     task = (dpkernel_task_base *)event.user_data.ptr; // we store the task in user_data during execute
    //     printf("task ptr: %p\n", task);
    //     printf("task->src_doca_buf: %p, task->dst_doca_buf: %p\n", task->src_doca_buf, task->dst_doca_buf);
    //     size_t data_len;
    //     size_t buf_len;
    //     doca_buf_get_data_len(task->dst_doca_buf, &data_len);
    //     doca_buf_get_len(task->dst_doca_buf, &buf_len);
    //     task->actual_out_size = data_len;
    //     printf("data_len: %lu, buf_len: %lu\n", data_len, buf_len);
    //     printf("decompress_executor_shm_poller got completion with actual_out_size: %lu\n", task->actual_out_size);
    //     if (doca_buf_refcount_rm(task->src_doca_buf, NULL) != DOCA_SUCCESS ||
    //         doca_buf_refcount_rm(task->dst_doca_buf, NULL) != DOCA_SUCCESS)
    //     {
    //         print_debug("Failed to decrease DOCA buffer reference count for buffers after completion\n");
    //     }
    //     // TODO: use proper memory management? note: rm ref cnt seems to free it?
    //     // free(task->src_doca_buf);
    //     // free(task->dst_doca_buf);

    //     // task->completion.store(DPK_SUCCESS);
    //     // printf("set flag to DPK_SUCCESS\n");
    //     return DPK_SUCCESS;
    // }
    // else
    // {
    //     printf("Failed to retrieve workq progress: %s", doca_get_error_string(ret));
    //     // task->completion.store(DPK_ERROR_FAILED);
    //     return DPK_ERROR_FAILED;
    // }
}

uint64_t bf3_decompress_deflate_kernel_get_estimated_completion_time();
uint64_t bf3_decompress_deflate_kernel_get_estimated_completion_time()
{
    // TODO:
    return 1000;
}