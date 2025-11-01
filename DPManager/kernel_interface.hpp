/// executors interface with the DPManager to execute tasks on the DPU,
/// they shpuld be asnyc!
/// they take in a task struct, using the relevant information (to construct device specific args) and execute it
/// on completion, the executor will fill in the completion struct, setting the completion status and other relevant
/// information. DP Manager will poll tasks for completion,
/// and return the completion struct to the application (dpm_submission_completion)

#pragma once

#include "common.hpp"
#include <cstddef>
#include <cstdint>
#include <queue>
#include <vector>

// TODO: this should not be declared in memory.hpp
#include "memory.hpp" // for: extern struct dpm_doca_state dpm_doca_state;

using std::queue;

/// kernel implementors should fill in this struct after successfully registering the kernel.
/// e.g., some kernel specific info
struct dpm_kernel_catalogue
{
    /// the line rate processing speed of the kernel, in bytes per second
    /// this will be used to estimate the completion time of the kernel
    // float line_rate;

    /// get the maximum size of a single task that the kernel can handle, this will be useful for coalescing (up to this
    /// size)
    // uint32_t (*get_max_single_task_size)();
    uint32_t max_single_task_size;

    /// dpm will call this for each invocation, passing in the input size
    /// @return the estimated processing time of the kernel, in nanoseconds
    long (*get_estimated_processing_time_ns)(uint32_t input_size, int thread_id);

    /// can this kernel be coalesced? DPM will make the decision based on this
    // bool (*is_coalescing_enabled)();
    bool is_coalescing_enabled;

    uint32_t kernel_capacity; // the capacity of the kernel queue, used for scheduling
};

/// the kernel interface.
/// Each task/kernel should implement this interface, and be registered with the DPManager.
/// Execution and polling should not block, and should return DPK_ERROR_AGAIN if needed
struct dpm_kernel
{
    /// Initialization of the executor, will be called on startup by DPManager
    /// @param [out] result the result of the registration, will be empty when passed in, filled by kernel
    /// @param n_threads the number of threads that the kernel maybe invoked on (could be hw threads for parallelism or
    /// sw thread pool size). Kernel initialization probably want to match this
    /// @note this is nullable.
    bool (*initialize)(struct dpm_kernel_catalogue *result, int n_threads);

    /// catalogue information should be filled in once successfully initialized
    /// @param [out] catalogue the result of the registration, will be empty when passed in, filled by kernel
    bool (*get_catalogue)(struct dpm_kernel_catalogue *catalogue);

    /// Cleanup of the executor, will be called on shutdown by DPManager
    /// @note this is nullable.
    bool (*cleanup)();

    /// If the kernel need to do anything for the allocated buffer from shared mem, dpm will call this function
    bool (*handle_mem_req)(struct dpm_mem_req *req);

    /// how many kernels this thread of hw kernel can execute right now
    uint32_t (*hw_kernel_remaining_capacity)(int thread_id, uint32_t *max_capacity);

    /// Once a request is received by DPManager, the executor will be called to execute the task
    /// @note this cannot be null.
    dpkernel_error (*execute)(dpkernel_task *task, int thread_id);

    /// DPM will periodically poll the executor for completions, if the executor's tasks requires polling completions.
    /// @note This can be a NULL function if the executor does not require polling.
    dpkernel_error (*poll)(dpkernel_task **task, int thread_id);

    /// Get the estimated completion time of the executor (under the current situation) in nanoseconds
    /// @note This should not be nullable.
    // uint64_t (*get_estimated_completion_time)();
};

// extern std::vector<Executor *> executors;

extern dpm_kernel dpm_executors[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

extern struct dpm_kernel_catalogue dpm_kernel_catalogues[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

// #ifdef BASELINE_SCHEDULING
struct alignas(64) aligned_hw_kernel_capacity
{
    long capacity[dpm_task_name::TASK_NAME_LAST];

    aligned_hw_kernel_capacity() : capacity{}
    {
    }

  public:
    long *operator[](dpm_task_name task)
    {
        uint task_idx = (uint)task;
        return &capacity[task_idx];
    }

    const long *operator[](dpm_task_name task) const
    {
        uint task_idx = (uint)task;
        return &capacity[task_idx];
    }
};

extern aligned_hw_kernel_capacity hw_kernels_capacity[N_DPM_THREADS];
// #endif

void dpm_init_hw_kernels();

void dpm_cleanup_kernels();

/// Get the executor for the given device and kernel name,
/// will be NULL if not initialized/registered beforehand.
struct dpm_kernel *get_kernel(dpm_device device, dpm_task_name name);

// replace old null_executor with new class instance
// extern NullExecutor null_exec;
/* struct null_executor
{
    /// the task queue, acts as both the submission and completion queue for null executor
    // boost::lockfree::queue<dpkernel_task_base, boost::lockfree::capacity<NULL_EXECUTOR_CAPACITY>> task_queue{};
    /// the completion queue, dp manager will poll from here
    // std::array<dpkernel_completion, 32> completions;

    std::atomic<uint> in_flight{0};

    null_executor()
    {
        // Initialize the completions array
        for (auto &completion : completions)
        {
            completion.error.store(DPK_ONGOING);
        }
    }
}; */

extern struct null_executor null_exec;

// Update function declarations to use the new class methods
// Remove old function declarations that are now part of the classes

// Keep the remaining specialized functions if needed

/// the null executor for msgq
// bool null_executor_msgq_execute(void *in, size_t in_size, void *out, size_t *out_size, void *user_data);
dpkernel_error null_executor_shm_execute(dpkernel_task_base *task);

// the application should poll this for completion
dpkernel_error null_executor_shm_poller(dpkernel_task_base **task);

/// poller, will be called by the DPManager repeatedly to poll for completions
/// will fill in the dpm_submission_completion's completion for dp manager to return to the application
// dpkernel_completion *null_executor_poller();

/// poll for completion of the executor's tasks
/// @return true if there is a completion, and the `completion` will be filled in; false otherwise
bool null_executor_msgq_poller(dpkernel_completion *completion);

/// check if the null executor is at full capacity. If so, should not call _execute on it.
// bool null_executor_msgq_is_full();

/// the bf3 decompress executor for msgq
dpkernel_error decompress_executor_shm_execute(dpkernel_task_base *task);

/// poller, will be called by the DPManager repeatedly to poll for completions
/// will fill in the dpm_submission_completion's completion for dp manager to return to the application
/// @param task the task pointer, will be null if no completion, otherwise will be a previous task pointer that has
/// completed
dpkernel_error decompress_executor_shm_poller(dpkernel_task_base *task);

dpkernel_error regex_executor_shm_execute(dpkernel_task_base *task);

dpkernel_error regex_executor_shm_poller(dpkernel_task_base *task);
