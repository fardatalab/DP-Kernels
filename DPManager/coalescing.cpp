#include "coalescing.hpp"
#include "common.hpp"
#include "dp_manager_msgq.hpp"
#include "kernel_interface.hpp"
#include "memory.hpp"
#include <chrono>
#include <cstdint>

long coalesce_start_timestamps[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

dpkernel_task *coalesce_tasks[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

shm_ptr coalesce_input_buffer_start_addresses[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] =
    {};

uint32_t coalesce_input_buffer_sizes[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

shm_ptr coalesce_output_buffer_start_addresses[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] =
    {};

uint32_t coalesce_output_buffer_sizes[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

bool _can_coalesce_task(dpkernel_task_base &task)
{
    // check if the task is coalescable
    if (!dpm_kernel_catalogues[task.device][task.task].coalescing_enabled)
    {
        return false;
    }

    return true;
}

dpkernel_error _coalesce_task_and_submit(dpkernel_task_base *task, int thread_id)
{
    // check how many bytes are in the coalesced buffer
    uint32_t next_in_size = coalesce_input_buffer_sizes[thread_id][task->device][task->task] + task->in_size;
    uint32_t next_out_size = coalesce_output_buffer_sizes[thread_id][task->device][task->task] + task->out_size;

    shm_ptr coalesce_input_buffer_start = coalesce_input_buffer_start_addresses[thread_id][task->device][task->task];
    uint32_t coalesce_input_buffer_size = coalesce_input_buffer_sizes[thread_id][task->device][task->task];

    shm_ptr coalesce_output_buffer_start = coalesce_output_buffer_start_addresses[thread_id][task->device][task->task];
    uint32_t coalesce_output_buffer_size = coalesce_output_buffer_sizes[thread_id][task->device][task->task];

    // get current time
    long time_since_last_submit = std::chrono::high_resolution_clock::now().time_since_epoch().count() -
                                  coalesce_start_timestamps[thread_id][task->device][task->task];
    // check if we are waiting for too long, or, we have accumulated too much
    // flush and submit the up to now coalesced task
    if (next_in_size > dpm_kernel_catalogues[task->device][task->task].max_single_task_size ||
        time_since_last_submit > COALESCE_TIMEOUT)
    {

        // reset the coalescing buffer with current values
        coalesce_input_buffer_start_addresses[thread_id][task->device][task->task] = task->in;
        coalesce_input_buffer_sizes[thread_id][task->device][task->task] = task->in_size;
        coalesce_output_buffer_start_addresses[thread_id][task->device][task->task] = task->out;
        coalesce_output_buffer_sizes[thread_id][task->device][task->task] = task->out_size;
        // reset the start timestamp
        coalesce_start_timestamps[thread_id][task->device][task->task] =
            std::chrono::high_resolution_clock::now().time_since_epoch().count();

        // else we can coalesce
        // handle memory first
        dpkernel_error ret;
        dpkernel_task coalesced_task;
        coalesced_task.base.in = coalesce_input_buffer_start;
        coalesced_task.base.in_size = coalesce_input_buffer_size;
        coalesced_task.base.out = coalesce_output_buffer_start;
        coalesced_task.base.out_size = coalesce_output_buffer_size;
        coalesced_task.base.device = task->device;
        coalesced_task.base.task = task->task;

        // perform kernel specific memory request handling, if any
        dpm_mem_req req = {
            .type = DPM_MEM_REQ_ALLOC_COALESCED,
            .task = (shm_ptr)(&coalesced_task.base), // actually a real ptr, not shm_ptr
            .completion = std::atomic<dpkernel_error>(DPK_ONGOING),
        };
        auto kernel = get_kernel((enum dpm_device)task->device, (enum dpm_task_name)task->task);
        if (kernel->handle_mem_req != NULL)
        {
            if (kernel->handle_mem_req(&req))
            {
                print_debug("device %d kernel %d handled coalesced mem req\n", task->device, task->task);
                // req.completion.store(DPK_SUCCESS, std::memory_order_release);
                // ret = DPK_SUCCESS;
            }
            else
            {
                printf("device %d kernel %d failed to handle coalesced mem req\n", coalesced_task.base.device,
                       coalesced_task.base.task);
                // req.completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                return DPK_ERROR_FAILED;
            }
        }
        else // no kernel specific mem req handler
        {
            // req.completion.store(DPK_SUCCESS, std::memory_order_release);
            // ret = DPK_SUCCESS;
        }

        // now memory is handled, we can submit the task
        _dpm_try_execute_or_queue_task(thread_id, &coalesced_task);
    }
    // we should wait a bit and coalesce more
    else
    {
        coalesce_input_buffer_sizes[thread_id][task->device][task->task] = next_in_size;
        coalesce_output_buffer_sizes[thread_id][task->device][task->task] = next_out_size;
        return DPK_ERROR_AGAIN;
    }
}