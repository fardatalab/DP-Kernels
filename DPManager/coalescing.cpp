#include "coalescing.hpp"
#include "dpm_interface.hpp"
#include <chrono>
#include <cstdint>
#include <cstdio>

ObjectPool<dpkernel_task, COALESCE_TASK_POOL_SIZE> *coalesce_task_pools[N_DPM_THREADS] = {nullptr};

long coalesce_start_timestamps[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {{{0}}};

dpkernel_task *linked_list_tasks_tail[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {
    {{nullptr}}};
dpkernel_task *head_task_of_chain[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {
    {{nullptr}}};

shm_ptr coalesce_input_buffer_start_addresses[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {
    {{0}}};

uint32_t coalesce_input_buffer_sizes[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {{{0}}};

uint32_t coalesce_input_nums[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {{{0}}};

shm_ptr coalesce_output_buffer_start_addresses[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] =
    {{{0}}};

uint32_t coalesce_output_buffer_sizes[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {{{0}}};

static inline void _reset_coalesce_start_timestamp(int thread_id, enum dpm_device device, enum dpm_task_name task_name)
{
    coalesce_start_timestamps[thread_id][device][task_name] = 0;
}
static inline void _set_coalesce_start_timestamp(int thread_id, enum dpm_device device, enum dpm_task_name task_name)
{
    coalesce_start_timestamps[thread_id][device][task_name] =
        std::chrono::high_resolution_clock::now().time_since_epoch().count();
}

bool _setup_coalescing_task_pools(int num_threads)
{
    for (int i = 0; i < num_threads; i++)
    {
        coalesce_task_pools[i] = new ObjectPool<dpkernel_task, COALESCE_TASK_POOL_SIZE>();
        if (coalesce_task_pools[i] == nullptr)
        {
            printf("Failed to allocate coalescing task pool\n");
            return false;
        }
    }
    return true;
}

bool _can_coalesce_task(int thread_id, dpkernel_task_base *task)
{
    bool input_buf_is_contiguous, output_buf_is_contiguous;
    if (coalesce_input_buffer_sizes[thread_id][task->device][task->task] == 0 ||
        coalesce_output_buffer_sizes[thread_id][task->device][task->task] == 0)
    {
        // coalescing has been reset
        input_buf_is_contiguous = true;
        output_buf_is_contiguous = true;
    }
    else
    {
        input_buf_is_contiguous = coalesce_input_buffer_start_addresses[thread_id][task->device][task->task] +
                                      coalesce_input_buffer_sizes[thread_id][task->device][task->task] ==
                                  task->in;
        output_buf_is_contiguous = coalesce_output_buffer_start_addresses[thread_id][task->device][task->task] +
                                       coalesce_output_buffer_sizes[thread_id][task->device][task->task] ==
                                   task->out;

        ////debug
        /* if (coalesce_input_buffer_start_addresses[thread_id][task->device][task->task] +
                coalesce_input_buffer_sizes[thread_id][task->device][task->task] !=
            task->in)
        {
            printf("coalescing in size %u, start addr shm_ptr = %lu, task.in = %lu\n",
                   coalesce_input_buffer_sizes[thread_id][task->device][task->task],
                   coalesce_input_buffer_start_addresses[thread_id][task->device][task->task], task->in);
            printf("coalescing task: input buffer not contiguous, should not happen\n");
        } */
        ////
    }
    if (!dpm_kernel_catalogues[task->device][task->task].is_coalescing_enabled)
    {
        printf("coalescing not enabled for device %d task %d\n", task->device, task->task);
    }
    return (dpm_kernel_catalogues[task->device][task->task].is_coalescing_enabled && input_buf_is_contiguous &&
            output_buf_is_contiguous);
}

bool _coalesce_should_flush_full(int thread_id, enum dpm_device device, enum dpm_task_name task_name, int next_in_size)
{
    // check how many bytes are in the coalesced buffer
    uint32_t next_size = coalesce_input_buffer_sizes[thread_id][device][task_name] + next_in_size;
    //// uint32_t next_out_size = coalesce_output_buffer_sizes[thread_id][device][task_name] + task->out_size;

    bool is_too_many = coalesce_input_nums[thread_id][device][task_name] >= MAX_COALESCE_INPUT_NUM;
    return is_too_many || next_size > dpm_kernel_catalogues[device][task_name].max_single_task_size;
}

dpkernel_task *_create_coalesced_task(int thread_id, enum dpm_device device, enum dpm_task_name task_name)
{
    // NOTE: if we get here, we could still be in a case when head_task_of_chain is null: empty chain with timeout or
    // huge incoming task
    if (head_task_of_chain[thread_id][device][task_name] == nullptr)
    {
        printf("SHOULD NOT HAPPEND: head_task_of_chain is null, not creating coalesced task...\n");
        return nullptr;
    }

    // dpkernel_task *coalesced_task = (dpkernel_task *)allocate_output_buf(sizeof(dpkernel_task));
    // dpkernel_task *coalesced_task = coalesce_task_pools[thread_id]->acquire();
    // NOTE: use the head of task as the coalesced task
    dpkernel_task *coalesced_task = head_task_of_chain[thread_id][device][task_name];

    ////////////////////
    // reset the coalesced task
    // linked_list_tasks_tail[thread_id][device][task_name] = nullptr;
    // head_task_of_chain[thread_id][device][task_name] = nullptr;

    // coalesced_task->base.is_coalesced_head = true;

    // return coalesced_task;
    //////////////////////////

    if (coalesced_task == nullptr)
    {
        printf("failed to allocate coalesced task\n");
        return nullptr;
    }
    coalesced_task->base.is_coalesced_head = true;

    // coalesced_task->base.in = coalesce_output_buffer_start_addresses[thread_id][device][task_name];
    // coalesced_task->base.in = head_task_of_chain[thread_id][device][task_name]->base.in;
    coalesced_task->base.in_size = coalesce_input_buffer_sizes[thread_id][device][task_name];
    print_debug("coalesced_task->base.in_size == coalesce_input_buffer_sizes[][][] = %u\n",
                coalesced_task->base.in_size);
    // coalesced_task->base.out = coalesce_output_buffer_start_addresses[thread_id][device][task_name];
    // coalesced_task->base.out = head_task_of_chain[thread_id][device][task_name]->base.out;
    coalesced_task->base.out_size = coalesce_output_buffer_sizes[thread_id][device][task_name];
    // coalesced_task->base.out_size = head_task_of_chain[thread_id][device][task_name]->base.out_size;
    // coalesced_task->base.device = device;
    // coalesced_task->base.task = task_name;
    // DON'T use the kernel's mem req handler, just update the doca buf of the head
    doca_buf_set_data(
        coalesced_task->bf3v2_2_regex.src_doca_buf,
        dpm_get_input_ptr_from_shmptr(coalesce_input_buffer_start_addresses[thread_id][device][task_name]),
        coalesce_input_buffer_sizes[thread_id][device][task_name]);

    print_debug("modifying head of coalesced task %p, in size %u, out size %u\n", coalesced_task,
                coalesced_task->base.in_size, coalesced_task->base.out_size);

    // NO NEED TO point to the head anymore, itself is the head
    /* // points to the next task in the coalesced task (the head of linked list)
    coalesced_task->base.user_data = head_task_of_chain[thread_id][device][task_name]; */

    // null terminate the end of the linked list
    /* if (linked_list_tasks_tail[thread_id][device][task_name] != nullptr)
    {
        linked_list_tasks_tail[thread_id][device][task_name]->base.user_data = nullptr;
    }
    else
    {
        printf("SHOULD NOT HAPPEND: linked_list_tasks is null, not creating coalesced task...\n");
    } */

    // reset the coalesced task
    linked_list_tasks_tail[thread_id][device][task_name] = nullptr;
    head_task_of_chain[thread_id][device][task_name] = nullptr;

    // // perform kernel specific memory request handling, if any
    // dpm_mem_req req = {
    //     .type = DPM_MEM_REQ_ALLOC_COALESCED,
    //     .task = (shm_ptr)(&coalesced_task->base), // actually a real ptr, not shm_ptr
    // };
    // req.completion.store(DPK_ONGOING, std::memory_order_release);

    // auto kernel = get_kernel((enum dpm_device)device, (enum dpm_task_name)task_name);
    // if (kernel->handle_mem_req != NULL)
    // {
    //     if (kernel->handle_mem_req(&req))
    //     {
    //         print_debug("device %d kernel %d handled coalesced mem req\n", device, task_name);
    //         // req.completion.store(DPK_SUCCESS, std::memory_order_release);
    //         // ret = DPK_SUCCESS;
    //     }
    //     else
    //     {
    //         printf("device %d kernel %d failed to handle coalesced mem req\n", coalesced_task->base.device,
    //                coalesced_task->base.task);
    //         // req.completion.store(DPK_ERROR_FAILED, std::memory_order_release);
    //         // return DPK_ERROR_FAILED;
    //         return nullptr;
    //     }
    // }
    // else // no kernel specific mem req handler
    // {
    //     // req.completion.store(DPK_SUCCESS, std::memory_order_release);
    //     // ret = DPK_SUCCESS;
    // }

    // printf("coalesced task created, in size %u, out size %u\n", coalesced_task->base.in_size,
    //        coalesced_task->base.out_size);

    // XXX: why would this help performance? by more than 5x?
    // THIS SHOULD NOT BE NEEDED, and now it doesn't help anyway
    /* for (int i = 0; i < 10000; i++)
    {
        __asm volatile("yield");
    } */

    return coalesced_task;
}

dpkernel_task *_coalesce_tasks(dpkernel_task_base *task, int thread_id, enum dpm_device device,
                               enum dpm_task_name task_name, bool force_flush)
{
    ////long current_time = std::chrono::high_resolution_clock::now().time_since_epoch().count();

    if (force_flush) // this means the task ptr is NULL, we detected a timeout and do force flush
    {
        dpkernel_task *flushed_task = nullptr;
        if (head_task_of_chain[thread_id][device][task_name] != nullptr)
        {
            flushed_task = _create_coalesced_task(thread_id, device, task_name);

            // head_task_of_chain and linked_list_tasks are reset inside _handle_mem_for_coalesced_task
            // head_task_of_chain[thread_id][device][task_name] = nullptr;

            // reset the coalescing buffer with current values
            coalesce_input_buffer_start_addresses[thread_id][device][task_name] = (shm_ptr)NULL;
            coalesce_input_buffer_sizes[thread_id][device][task_name] = 0;
            coalesce_output_buffer_start_addresses[thread_id][device][task_name] = (shm_ptr)NULL;
            coalesce_output_buffer_sizes[thread_id][device][task_name] = 0;
            coalesce_input_nums[thread_id][device][task_name] = 0;
            // reset the start timestamp
            // coalesce_start_timestamps[thread_id][device][task_name] = current_time;
            _reset_coalesce_start_timestamp(thread_id, device, task_name);

            // linked list and head are already reset in _create_coalesced_task to nullptr
        }
        else
        {
            printf("coalescing task should flush: head_task_of_chain is null, not creating coalesced task\n");
        }

        return flushed_task;
    }
    // flush the previously accumulated coalesced task (excluding this one)
    else if (_coalesce_should_flush_full(thread_id, device, task_name, task->in_size))
    {
        print_debug("coalescing task should flush: accumulated in size %u, out size %u\n",
                    coalesce_input_buffer_sizes[thread_id][device][task_name],
                    coalesce_output_buffer_sizes[thread_id][device][task_name]);

        dpkernel_task *task_to_submit = nullptr;

        task_to_submit = _create_coalesced_task(thread_id, device, task_name);
        if (task_to_submit == nullptr)
        {
            // means head is null, so didn't create any task really, should just submit the current task

            // reset the coalescing buffer with current values
            coalesce_input_buffer_start_addresses[thread_id][device][task_name] = (shm_ptr)NULL;
            coalesce_input_buffer_sizes[thread_id][device][task_name] = 0;
            coalesce_output_buffer_start_addresses[thread_id][device][task_name] = (shm_ptr)NULL;
            coalesce_output_buffer_sizes[thread_id][device][task_name] = 0;
            coalesce_input_nums[thread_id][device][task_name] = 0;
            // reset the start timestamp
            // coalesce_start_timestamps[thread_id][device][task_name] = current_time;
            _reset_coalesce_start_timestamp(thread_id, device, task_name);
            printf("coalesce should flush full: head_task_of_chain is null, not creating coalesced task, this task in "
                   "size = %u\n",
                   task->in_size);

            return (dpkernel_task *)task;
        }
        // After _create_coalesced_task, head_task_of_chain and linked_list_tasks are nullptr.
        // Buffer sizes and addresses are effectively reset for the new chain starting below.
        // Explicitly clear sizes for the new chain that will be formed by the current task.

        // The current 'task' starts a new chain.
        // This happens regardless of whether an old chain was flushed or if the chain was initially empty.

        // Set buffer start addresses from the first task of the new chain.
        coalesce_input_buffer_start_addresses[thread_id][device][task_name] = task->in;
        coalesce_output_buffer_start_addresses[thread_id][device][task_name] = task->out;
        // Set/update buffer sizes for the new chain (initially just this task's size).
        coalesce_input_buffer_sizes[thread_id][device][task_name] = task->in_size;
        coalesce_output_buffer_sizes[thread_id][device][task_name] = task->out_size;
        coalesce_input_nums[thread_id][device][task_name] = 1;

        // Add the current task as the head and tail of the new chain.
        head_task_of_chain[thread_id][device][task_name] = (dpkernel_task *)task;
        linked_list_tasks_tail[thread_id][device][task_name] = (dpkernel_task *)task;
        ((dpkernel_task *)task)->base.user_data = nullptr;

        // Reset the start timestamp for this new chain.
        // coalesce_start_timestamps[thread_id][device][task_name] = current_time;
        // this is the first task in the new chain, so set the start timestamp
        _set_coalesce_start_timestamp(thread_id, device, task_name);

        return task_to_submit; // Return the flushed old chain (if any); current task is now queued
    }
    // we should wait a bit and coalesce more, should return nullptr
    else
    {
        print_debug("coalescing task %d, device %d, in size %u, out size %u\n", task_name, device,
                    coalesce_input_buffer_sizes[thread_id][device][task_name],
                    coalesce_output_buffer_sizes[thread_id][device][task_name]);
        coalesce_input_buffer_sizes[thread_id][device][task_name] += task->in_size;
        coalesce_output_buffer_sizes[thread_id][device][task_name] += task->out_size;
        coalesce_input_nums[thread_id][device][task_name]++;

        // make the linked list, point prev one to this task
        if (head_task_of_chain[thread_id][device][task_name] != nullptr)
        {
            linked_list_tasks_tail[thread_id][device][task_name]->base.user_data = task;
            linked_list_tasks_tail[thread_id][device][task_name] = (dpkernel_task *)task;
            ((dpkernel_task *)task)->base.user_data = nullptr;
        }
        else
        {
            // first task in the linked list
            head_task_of_chain[thread_id][device][task_name] = (dpkernel_task *)task;
            linked_list_tasks_tail[thread_id][device][task_name] = (dpkernel_task *)task;
            ((dpkernel_task *)task)->base.user_data = nullptr;
            // if this is the very first task for this chain, set initial buffer addresses
            coalesce_input_buffer_start_addresses[thread_id][device][task_name] = task->in;
            coalesce_output_buffer_start_addresses[thread_id][device][task_name] = task->out;

            // first task in the new chain, so set the start timestamp
            _set_coalesce_start_timestamp(thread_id, device, task_name);
        }
        // make sure the new tail of the list points to nullptr
        ((dpkernel_task *)task)->base.user_data = nullptr;

        // linked_list_tasks[thread_id][device][task_name]->base.user_data = task;
        // linked_list_tasks[thread_id][device][task_name] = (dpkernel_task *)task;

        //// dpkernel_task **next_task_ptr =
        ////     (dpkernel_task **)&linked_list_tasks[thread_id][device][task_name]->base.user_data;
        //// *next_task_ptr = (dpkernel_task *)task;
        return nullptr;
    }
}

dpkernel_task *flush_task_if_timeout(int thread_id, enum dpm_device device)
{
    // check if we are waiting for too long, if so, rerturn a coalesced task
    for (int i = 0; i < dpm_task_name::TASK_NAME_LAST; i++)
    {
        auto kernel = get_kernel(device, (dpm_task_name)i);
        if (kernel == nullptr || kernel->get_catalogue == nullptr)
        {
            continue;
        }
        if (dpm_kernel_catalogues[device][i].is_coalescing_enabled &&
            coalesce_start_timestamps[thread_id][device][i] != 0)
        {
            long current_time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
            long time_since_last_submit = current_time - coalesce_start_timestamps[thread_id][device][i];
            if (head_task_of_chain[thread_id][device][i] != nullptr && time_since_last_submit > COALESCE_TIMEOUT) //
            {
                // printf("coalescing task timed out, flushing coalesced task %d\n", i);
                return _coalesce_tasks(nullptr, thread_id, device, (dpm_task_name)i, true);
            }
        }
    }
    return nullptr;
}