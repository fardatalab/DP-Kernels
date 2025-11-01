#pragma once
#include "executor.hpp"

dpkernel_error null_kernel_execute(dpkernel_task_base *task);
dpkernel_error null_kernel_execute(dpkernel_task_base *task)
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
    return DPK_SUCCESS;
    //// return null_exec.enqueue_task(task);
#endif
}