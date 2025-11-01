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

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_compress.h>
#include <doca_ctx.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_regex.h>

// TODO: this should not be declared in memory.hpp
#include "memory.hpp"
// extern struct dpm_doca_state dpm_doca_state;

using std::queue;

#include <boost/lockfree/queue.hpp>

#define NULL_EXECUTOR_CAPACITY 1024

#define NB_REGEX_RESULTS 32

/// the null executor, doesn't actually execute anything
/// just directly adds completion to its completion queue
struct null_executor
{
    /// the task queue, acts as both the submission and completion queue for null executor
    // boost::lockfree::queue<dpkernel_task_base *, boost::lockfree::capacity<NULL_EXECUTOR_CAPACITY>> task_queue;
    std::queue<dpkernel_task_base *> task_queue;
    size_t max_queue_size = NULL_EXECUTOR_CAPACITY;

    bool enqueue_task(dpkernel_task_base *task)
    {
        if (task_queue.size() >= max_queue_size)
        {
            return false; // Queue is full
        }
        task_queue.push(task);
        return true;
    }

    bool dequeue_task(dpkernel_task_base **task)
    {
        if (task_queue.empty())
        {
            return false; // Queue is empty
        }
        *task = task_queue.front();
        task_queue.pop();
        return true;
    }
    /// the completion queue, dp manager will poll from here
    // std::array<dpkernel_completion, 32> completions;

    std::atomic<uint> in_flight{0};

    null_executor() : task_queue()
    {
        // Initialize the completions array
        // for (auto &completion : completions)
        // {
        //     completion.error.store(DPK_ONGOING);
        // }

        // Initialize the task queue (already done in initializer list)
    }
};

extern struct null_executor null_exec;

// bool null_executor_setup();

/// these two act as interfaces for the DPManager

/// the null executor, does nothing
bool null_executor_execute(void *in, size_t in_size, void *out, size_t *out_size, void *user_data);

/// the null executor for msgq
// bool null_executor_msgq_execute(void *in, size_t in_size, void *out, size_t *out_size, void *user_data);
bool null_executor_shm_execute(dpkernel_task_base *task);

// the application should poll this for completion
bool null_executor_shm_poller(dpkernel_task_base **task);

/// poller, will be called by the DPManager repeatedly to poll for completions
/// will fill in the dpm_submission_completion's completion for dp manager to return to the application
// dpkernel_completion *null_executor_poller();

/// poll for completion of the executor's tasks
/// @return true if there is a completion, and the `completion` will be filled in; false otherwise
bool null_executor_msgq_poller(dpkernel_completion *completion);

/// check if the null executor is at full capacity. If so, should not call _execute on it.
// bool null_executor_msgq_is_full();

/// the bf3 decompress executor for msgq
bool decompress_executor_shm_execute(dpkernel_task_base *task);

/// poller, will be called by the DPManager repeatedly to poll for completions
/// will fill in the dpm_submission_completion's completion for dp manager to return to the application
/// @param task the task pointer, will be null if no completion, otherwise will be a previous task pointer that has
/// completed
bool decompress_executor_shm_poller(dpkernel_task_base **task);

bool regex_executor_shm_execute(dpkernel_task_base *task);

bool regex_executor_shm_poller(dpkernel_task_base **task);
