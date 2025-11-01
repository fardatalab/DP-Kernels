#include "scheduling.hpp"
#include "common.hpp"
#include "kernel_interface.hpp"
#include <chrono>

// for each device+kernel combo we record how many bytes are inflight
size_t inflight_bytes[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

// std::atomic<long> last_submission_timestamps[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

// for each device+kernel combo we record the last time we processed a request
long last_completion_timestamps[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

// for each kernel, keep track of the moving average draining rate
float draining_rates[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

void _inc_inflight_bytes_on_submit(const struct dpkernel_task_base &dpk_task, int thread_id)
{
    // increment the inflight bytes
    inflight_bytes[thread_id][dpk_task.device][dpk_task.task] += dpk_task.in_size;
}

void _dec_inflight_bytes_on_completion(const struct dpkernel_task_base &dpk_task, int thread_id)
{
    // decrement the inflight bytes
    inflight_bytes[thread_id][dpk_task.device][dpk_task.task] -= dpk_task.in_size;
}

void _update_kernel_draining_rate(const struct dpkernel_task_base &dpk_task, int thread_id)
{
    // get current timestamp
    auto completion_time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    // get the last completion timestamp
    auto last_completion_time = last_completion_timestamps[thread_id][dpk_task.device][dpk_task.task];

    // calculate the time taken to process the request
    auto time_taken = completion_time - last_completion_time;
    auto kernel_inflight_bytes = inflight_bytes[thread_id][dpk_task.device][dpk_task.task];
    // update the last processing timestamp
    last_completion_timestamps[thread_id][dpk_task.device][dpk_task.task] = completion_time;
    // decrement the inflight bytes
    inflight_bytes[thread_id][dpk_task.device][dpk_task.task] -= dpk_task.in_size;

    // calculate the draining rate
    // if first time then use line rate from catalogue
    float draining_rate;
    if (last_completion_time == 0)
    {
        draining_rate = dpm_kernel_catalogues[dpk_task.device][dpk_task.task].line_rate;
        // first time processing this task, use the line rate
        draining_rates[thread_id][dpk_task.device][dpk_task.task] = draining_rate;
    }
    else
    {
        // calculate the draining rate
        draining_rate = (float)kernel_inflight_bytes / (float)time_taken;
    }

    // update the draining rate with moving average
    draining_rates[thread_id][dpk_task.device][dpk_task.task] =
        MOVING_AVERAGE_ALPHA * draining_rate +
        (1 - MOVING_AVERAGE_ALPHA) * draining_rates[thread_id][dpk_task.device][dpk_task.task];
}