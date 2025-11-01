#pragma once
#include "common.hpp"

#define MOVING_AVERAGE_ALPHA 0.3

// for each device+kernel combo we record how many bytes are inflight
extern size_t inflight_bytes[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

// std::atomic<long> last_submission_timestamps[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

// for each device+kernel combo we record the last time we processed a request
extern long last_completion_timestamps[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

// for each kernel, keep track of the moving average draining rate
extern float draining_rates[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

void _inc_inflight_bytes_on_submit(const struct dpkernel_task_base &dpk_task, int thread_id);

void _update_kernel_draining_rate(const struct dpkernel_task_base &dpk_task, int thread_id);

void _dec_inflight_bytes_on_completion(const struct dpkernel_task_base &dpk_task, int thread_id);