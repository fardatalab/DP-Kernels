#include "scheduling.hpp"
#include "common.hpp"
#include "kernel_interface.hpp"
#include <chrono>
#include <cstdio>

// NOTE: we use accumulated estimated time instead of the inflight bytes
/* // for each device+kernel combo we record how many bytes are inflight
per_thread_inflight_bytes inflight_bytes[N_DPM_THREADS] = {}; */

// std::atomic<long> last_submission_timestamps[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

// for each device+kernel combo we record the last time we processed a request
// thread_local BoundedQueue<long, DPM_HW_KERNEL_QUEUE_SIZE>
//     last_completion_timestamps[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

/* BoundedQueue<long, DPM_HW_KERNEL_QUEUE_SIZE>
    last_submission_or_enqueue_timestamps[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};
 */

// long estimated_hw_kernels_queuing_time[N_DPM_THREADS][dpm_task_name::TASK_NAME_LAST];
aligned_hw_kernel_queuing_time estimated_hw_kernels_queuing_time[N_DPM_THREADS] = {};

aligned_sw_kernel_queuing_time estimated_sw_kernels_queuing_time[N_SW_KERNELS_THREADS] = {};

aligned_hw_kernel_sched_start_time hw_kernels_sched_start_time[N_DPM_THREADS] = {};

// long last_estimated_hw_kernels_timestamps[N_DPM_THREADS][dpm_task_name::TASK_NAME_LAST];

// long last_estimated_sw_kernels_timestamps[N_SW_KERNELS_THREADS][dpm_task_name::TASK_NAME_LAST];

/* // for each kernel, keep track of the moving average draining rate
per_thread_draining_rates draining_rates[N_DPM_THREADS] = {}; */

/* float _get_draining_rate(enum dpm_device device, enum dpm_task_name task, int thread_id)
{
    // get the draining rate for this device and task
    float draining_rate = draining_rates[thread_id][device][task];

    // if the draining rate is not yet calculated, use the line rate from the catalogue
    if (draining_rate == 0)
    {
        // draining_rate = dpm_kernel_catalogues[device][task].line_rate;
        print_debug("first time processing this task, using line rate %f\n", draining_rate);
    }

    return draining_rate;
} */

/* void _inc_inflight_bytes_on_submit_or_enqueue(const struct dpkernel_task_base &dpk_task, int thread_id)
{
    // increment the inflight bytes
    inflight_bytes[thread_id][dpk_task.device][dpk_task.task].fetch_add(dpk_task.in_size);
    // take the current timestamp
    auto submission_time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    // push back the timestamp
    last_submission_or_enqueue_timestamps[thread_id][dpk_task.device][dpk_task.task].push(submission_time);
} */

/* void _dec_inflight_bytes_on_completion(const struct dpkernel_task_base &dpk_task, int thread_id)
{
    // decrement the inflight bytes
    inflight_bytes[thread_id][dpk_task.device][dpk_task.task].fetch_sub(dpk_task.in_size);
} */

/* void _update_kernel_draining_rate(const struct dpkernel_task_base &dpk_task, int thread_id)
{
    // get current timestamp
    auto completion_time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    // // get the last completion timestamp
    // auto last_completion_time = last_completion_timestamps[dpk_task.device][dpk_task.task];

    // get the earliest submission or enqueue timestamp, assume that this corresponds to this completion
    long last_submission_time = 0;
    if (last_submission_or_enqueue_timestamps[thread_id][dpk_task.device][dpk_task.task].pop(last_submission_time))
    {
        // printf("got last submission time %lu\n", last_submission_time);
    }
    else
    {
        // if we can't get the last submission time, just keep it 0
        printf("no last submission time found, using 0\n");
    }

    // calculate the time taken to process the request
    auto time_taken = completion_time - last_submission_time;
    // auto kernel_inflight_bytes = inflight_bytes[thread_id][dpk_task.device][dpk_task.task];
    //// update the last processing timestamp
    ////last_completion_timestamps[dpk_task.device][dpk_task.task] = completion_time;

    // calculate the draining rate
    float cur_draining_rate = _get_draining_rate(dpk_task.device, dpk_task.task, thread_id);
    float draining_rate;

    draining_rate = (float)dpk_task.in_size / (float)time_taken;

    // update the draining rate with moving average
    auto new_rate = MOVING_AVERAGE_ALPHA * cur_draining_rate + (1 - MOVING_AVERAGE_ALPHA) * draining_rate;
    draining_rates[thread_id][dpk_task.device][dpk_task.task] = new_rate;
    print_debug("new draining rate for device %d task %d: %f, time taken: %lu, inflight bytes: %lu\n", dpk_task.device,
                dpk_task.task, draining_rate, time_taken, kernel_inflight_bytes);
} */