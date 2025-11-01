/// executors interface with the DPManager to execute tasks on the DPU,
/// they shpuld be asnyc!
/// they take in a task struct, using the relevant information (to construct device specific args) and execute it
/// on completion, the executor will fill in the completion struct, setting the completion status and other relevant
/// information. DP Manager will poll tasks for completion,
/// and return the completion struct to the application (dpm_submission_completion)

#pragma once

#include "common.hpp"
#include <queue>
#include <vector>

// #include <doca_buf.h>
// #include <doca_buf_inventory.h>
// #include <doca_compress.h>
// #include <doca_ctx.h>
// #include <doca_error.h>
// #include <doca_log.h>

// // DOCA 1.5+ removed regex support
// #ifdef DOCA_VER_1_5
// #include <doca_regex.h>
// #endif

// TODO: this should not be declared in memory.hpp
#include "memory.hpp" // for: extern struct dpm_doca_state dpm_doca_state;

using std::queue;

// #include <boost/lockfree/queue.hpp>

#define NULL_EXECUTOR_CAPACITY 1024

#define NB_REGEX_RESULTS 32

/// the executor interface.
/// Each task executor should implement this interface, and be registered with the DPManager.
/// Execution should not block.
struct dpm_task_executor
{
    /// Initialization of the executor, will be called on startup by DPManager
    /// @note this is nullable.
    bool (*initialize)();

    /// Cleanup of the executor, will be called on shutdown by DPManager
    /// @note this is nullable.
    bool (*cleanup)();

    /// Once a request is received by DPManager, the executor will be called to execute the task
    /// @note this cannot be null.
    dpkernel_error (*execute)(dpkernel_task_base *task);

    /// DPM will periodically poll the executor for completions, if the executor's tasks requires polling completions.
    /// @note This can be a NULL function if the executor does not require polling.
    dpkernel_error (*poll)(dpkernel_task_base *task);

    /// Get the estimated completion time of the executor (under the current situation) in nanoseconds
    /// @note This should not be nullable.
    uint64_t (*get_estimated_completion_time)();
};

// extern std::vector<Executor *> executors;
extern dpm_task_executor dpm_executors[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];
// extern dpm_task_executor dpm_executors[dpm_task_name::TASK_NAME_LAST];

/// This sets up each executor, before they can be used
// void dpm_register_executors(dpm_task_executor global_executors[DEVICE_LAST][TASK_NAME_LAST]);
// void dpm_register_executors();

void dpm_init_kernels();

void dpm_cleanup_kernels();

/// Get the executor for the given device and kernel name,
/// will be NULL if not initialized/registered beforehand.
struct dpm_task_executor *get_executor(dpm_device device, dpm_task_name name);

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
