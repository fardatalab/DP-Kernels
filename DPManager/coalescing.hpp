#include "common.hpp"
#include "kernel_interface.hpp"

// NOTE: only requests with contiguous input mem addr are coalesced

#define COALESCE_TIMEOUT 10000 // 10 us

// for each device+kernel combo we record the start time of the first moment we start coalescing/waiting for more
// requests
extern long coalesce_start_timestamps[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

/// for each device+kernel combo we record the head of the coalescing tasks
/// each subsequent task will be merged into the head (increment the size of the in/out buffers)
extern dpkernel_task *coalesce_tasks[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

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

bool _can_coalesce_task(dpkernel_task_base &task);

dpkernel_error _coalesce_task_and_submit(dpkernel_task_base *task, int thread_id);