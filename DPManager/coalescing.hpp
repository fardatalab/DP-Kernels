#pragma once
#include "common.hpp"
#include "kernel_interface.hpp"
#include "object_pool.hpp"
// #include <doca_buf.h>

// NOTE: only requests with contiguous input mem addr are coalesced

#define MAX_COALESCE_INPUT_NUM 64

#define COALESCE_TASK_POOL_SIZE 1048576

#define COALESCE_TIMEOUT 50000 // 50 us

// for each device+kernel combo we record the start time of the first moment we start coalescing/waiting for more
// requests, should be reset to 0 after we flush, and be set when the first task (head) starts being coalesced
extern long coalesce_start_timestamps[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

/// for each device+kernel combo we record the head of the coalescing tasks
/// each subsequent task will be merged into the head (increment the size of the in/out buffers)
extern dpkernel_task *linked_list_tasks_tail[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

// where the merged/coalesced requests start (i.e. the start of the first request)
extern shm_ptr coalesce_input_buffer_start_addresses[N_DPM_THREADS][dpm_device::DEVICE_LAST]
                                                    [dpm_task_name::TASK_NAME_LAST];

// how many bytes are in the coalesced buffer (from the buffer start)
extern uint32_t coalesce_input_buffer_sizes[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

// where the merged/coalesced output buffer start (i.e. the start of the first request)
extern shm_ptr coalesce_output_buffer_start_addresses[N_DPM_THREADS][dpm_device::DEVICE_LAST]
                                                     [dpm_task_name::TASK_NAME_LAST];

// how many bytes are in the coalesced out buffer (from the buffer start)
extern uint32_t coalesce_output_buffer_sizes[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

bool _can_coalesce_task(int thread_id, dpkernel_task_base *task);

dpkernel_task *_coalesce_tasks(dpkernel_task_base *task, int thread_id, enum dpm_device device,
                               enum dpm_task_name task_name, bool force_flush);

bool _coalesce_should_flush_full(int thread_id, enum dpm_device device, enum dpm_task_name task_name);

dpkernel_task *_create_coalesced_task(int thread_id, enum dpm_device device, enum dpm_task_name task_name);

/// this function should always be called by DPM thread (repeatedly) to check and flush
/// @return the coalesced task, or NULL if timeout not yet reached
dpkernel_task *flush_task_if_timeout(int thread_id, enum dpm_device device);

extern ObjectPool<dpkernel_task, COALESCE_TASK_POOL_SIZE> *coalesce_task_pools[N_DPM_THREADS];

bool _setup_coalescing_task_pools(int num_threads);