#include "null_kernel.hpp"
#include "common.hpp"

dpkernel_error null_kernel_execute(dpkernel_task *task, int thread_id)
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

    dpkernel_task_base *dpk_task = &task->base;
    char *in = dpm_get_input_ptr_from_shmptr(dpk_task->in);
    char *out = dpm_get_output_ptr_from_shmptr(dpk_task->out);
#ifdef MEMCPY_KERNEL
    char *in_cpy = dpm_get_input_ptr_from_shmptr(dpk_task->in_cpy);
    char *out_cpy = dpm_get_output_ptr_from_shmptr(dpk_task->out_cpy);
    memcpy(in_cpy, in, dpk_task->in_size);
    memcpy(out_cpy, out, dpk_task->out_size);
#endif
    dpk_task->completion.store(DPK_SUCCESS);

    // don't enqueue, just complete immediately
    return DPK_SUCCESS;
    //// return null_exec.enqueue_task(task);
#endif
}

/// can always execute kernels
uint32_t null_kernel_can_execute_kernels(int thread_id)
{
    return 1;
}