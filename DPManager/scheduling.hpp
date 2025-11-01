#pragma once
#include "bounded_queue.hpp"
#include "common.hpp"
#include "dp_manager_msgq.hpp"
#include <atomic>
#include <cstddef>
#include <sys/types.h>

#define MOVING_AVERAGE_ALPHA 0.3

struct alignas(64) per_thread_inflight_bytes
{
    std::atomic<size_t> bytes[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

    per_thread_inflight_bytes() : bytes{}
    {
    }

  public:
    std::atomic<size_t> *operator[](dpm_device dev)
    {
        uint dev_idx = (uint)dev;
        return bytes[dev_idx];
    }

    const std::atomic<size_t> *operator[](dpm_device dev) const
    {
        uint dev_idx = (uint)dev;
        return &bytes[dev_idx][0];
    }
};

struct alignas(64) per_thread_draining_rates
{
    size_t rates[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

    per_thread_draining_rates() : rates{}
    {
    }

  public:
    size_t *operator[](dpm_device dev)
    {
        uint dev_idx = (uint)dev;
        return rates[dev_idx];
    }

    const size_t *operator[](dpm_device dev) const
    {
        uint dev_idx = (uint)dev;
        return rates[dev_idx];
    }
};

struct alignas(64) aligned_sw_kernel_queuing_time
{
    std::atomic<long> time[dpm_task_name::TASK_NAME_LAST];

    aligned_sw_kernel_queuing_time() : time{}
    {
    }

  public:
    std::atomic<long> *operator[](dpm_task_name task)
    {
        uint task_idx = (uint)task;
        return &time[task_idx];
    }

    const std::atomic<long> *operator[](dpm_task_name task) const
    {
        uint task_idx = (uint)task;
        return &time[task_idx];
    }
};

struct alignas(64) aligned_hw_kernel_queuing_time
{
    long time[dpm_task_name::TASK_NAME_LAST];

    aligned_hw_kernel_queuing_time() : time{}
    {
    }

  public:
    long *operator[](dpm_task_name task)
    {
        uint task_idx = (uint)task;
        return &time[task_idx];
    }

    const long *operator[](dpm_task_name task) const
    {
        uint task_idx = (uint)task;
        return &time[task_idx];
    }
};

struct alignas(64) aligned_hw_kernel_sched_start_time : public aligned_hw_kernel_queuing_time
{
};

// NOTE: we use accumulated estimated time instead of the inflight bytes
/* // for each device+kernel combo we record how many bytes are inflight
extern per_thread_inflight_bytes inflight_bytes[N_DPM_THREADS]; */

/// for each DPM thread, for each kernel we accumulate the estimated time, and will subtract from it once task
/// completed
// extern long estimated_hw_kernels_queuing_time[N_DPM_THREADS][dpm_task_name::TASK_NAME_LAST];
extern aligned_hw_kernel_queuing_time estimated_hw_kernels_queuing_time[N_DPM_THREADS];

extern aligned_hw_kernel_sched_start_time hw_kernels_sched_start_time[N_DPM_THREADS];

/// this timestamp help keep track of how long has elapsed since the accumulated time was last incremented,
/// and can be used to get a better estimate of the time remaining when doing scheduling
extern long last_estimated_hw_kernels_timestamps[N_DPM_THREADS][dpm_task_name::TASK_NAME_LAST];

/// for each sw kernel thread, for each kernel we accumulate the estimated time, and will subtract from it once task
/// completed
extern aligned_sw_kernel_queuing_time estimated_sw_kernels_queuing_time[N_SW_KERNELS_THREADS];

/// this timestamp help keep track of how long has elapsed since the accumulated time was last incremented,
/// and can be used to get a better estimate of the time remaining when doing scheduling
extern long last_estimated_sw_kernels_timestamps[N_SW_KERNELS_THREADS][dpm_task_name::TASK_NAME_LAST];

// std::atomic<long> last_submission_timestamps[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

// for each device+kernel combo we record the last timestamps we processed (submitted or enqueued successfully) a
// request
// extern thread_local BoundedQueue<long, DPM_HW_KERNEL_QUEUE_SIZE>
//     last_completion_timestamps[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

/* // for each kernel, keep track of the moving average draining rate
extern per_thread_draining_rates draining_rates[N_DPM_THREADS]; */

/* /// get the draining rate for a specific device and task,
/// will use the catalogue line rate if the draining rate is not yet calculated
float _get_draining_rate(enum dpm_device, enum dpm_task_name, int thread_id); */

#ifdef SCHEDULING
#define INCREMENT_INFLIGHT_BYTES_IF_SCHEDULING(dpk_task_ptr, tid) \
    _inc_inflight_bytes_on_submit_or_enqueue(*(dpk_task_ptr), (tid));
#else
#define INCREMENT_INFLIGHT_BYTES_IF_SCHEDULING(dpk_task_ptr, tid) // No-op
#endif

/* /// whenever a task is successfully submitted or enqueued, we increment the inflight bytes.
void _inc_inflight_bytes_on_submit_or_enqueue(const struct dpkernel_task_base &dpk_task, int thread_id); */

inline void _inc_hw_queueing_delay_on_submit_or_enqueue(int thread_id, struct dpkernel_task_base *dpk_task)
{
    // calc this task's processing time
    // long this_task_time = dpm_kernel_catalogues[dpk_task->device][dpk_task->task].get_estimated_processing_time_ns(
    //     dpk_task->in_size, thread_id);

    // increment the queueing delay
    // *estimated_hw_kernels_queuing_time[thread_id][dpk_task->task] += this_task_time;
    *estimated_hw_kernels_queuing_time[thread_id][dpk_task->task] += dpk_task->estimated_completion_time;
    // get a timestamp for this very moment we incremented the queueing delay
    // last_estimated_hw_kernels_timestamps[thread_id][dpk_task->task] =
    //     std::chrono::high_resolution_clock::now().time_since_epoch().count();

    // dpk_task->estimated_completion_time = this_task_time;

    print_debug("hw estimated completion time for device %d task %d: %f\n", dpk_task->device, dpk_task->task,
                dpk_task->estimated_completion_time);
};

inline void _inc_sw_queueing_delay_on_submit_or_enqueue(int thread_id, struct dpkernel_task_base *dpk_task)
{
    // calc this task's processing time
    // long this_task_time = dpm_kernel_catalogues[dpk_task->device][dpk_task->task].get_estimated_processing_time_ns(
    //     dpk_task->in_size, thread_id);

    // increment the queueing delay
    // NOTE: the estimated time for sw is in ns (since we need atomic long, float won't work)
    // estimated_sw_kernels_queuing_time[thread_id][dpk_task->task]->fetch_add(this_task_time,
    // std::memory_order_relaxed);
    estimated_sw_kernels_queuing_time[thread_id][dpk_task->task]->fetch_add(dpk_task->estimated_completion_time,
                                                                            std::memory_order_relaxed);

    // get a timestamp for this very moment we incremented the queueing delay
    // last_estimated_sw_kernels_timestamps[thread_id][dpk_task->task] =
    //     std::chrono::high_resolution_clock::now().time_since_epoch().count();

    // dpk_task->estimated_completion_time = this_task_time;

    print_debug("sw estimated completion time for device %d task %d: %f\n", dpk_task->device, dpk_task->task,
                dpk_task->estimated_completion_time);
};

#ifdef SCHEDULING
#define DECREMENT_INFLIGHT_BYTES_IF_SCHEDULING(dpk_task_ptr, tid) \
    _update_kernel_draining_rate(*(dpk_task_ptr), (tid)); \
    _dec_inflight_bytes_on_completion(*(dpk_task_ptr), (tid));
#else
#define DECREMENT_INFLIGHT_BYTES_IF_SCHEDULING(dpk_task_ptr, tid) // No-op
#endif

/* /// based on the time taken to complete this specific request, we update the kernel draining rate (moving average).
void _update_kernel_draining_rate(const struct dpkernel_task_base &dpk_task, int thread_id); */

/* /// whenever a task is completed, we decrement the inflight bytes.
void _dec_inflight_bytes_on_completion(const struct dpkernel_task_base &dpk_task, int thread_id); */

inline void _dec_hw_queueing_delay_on_completion(int thread_id, struct dpkernel_task_base *dpk_task)
{
    // last_estimated_hw_kernels_timestamps[thread_id][dpk_task->task] =
    //     std::chrono::high_resolution_clock::now().time_since_epoch().count();
    *estimated_hw_kernels_queuing_time[thread_id][dpk_task->task] -= dpk_task->estimated_completion_time;
};

inline void _dec_sw_queueing_delay_on_completion(int thread_id, struct dpkernel_task_base *dpk_task)
{
    // last_estimated_sw_kernels_timestamps[thread_id][dpk_task->task] =
    //     std::chrono::high_resolution_clock::now().time_since_epoch().count();
    estimated_sw_kernels_queuing_time[thread_id][dpk_task->task]->fetch_sub(dpk_task->estimated_completion_time,
                                                                            std::memory_order_relaxed);
};