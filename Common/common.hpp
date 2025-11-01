#pragma once
#pragma clang diagnostic ignored "-Wdeprecated-declarations"

#include <atomic>
#include <cstddef>
#include <doca_buf.h>

#define SUBMISSION_QUEUE_NAME "dpm_submission_queue"
// #define SUBMISSION_QUEUE_BACKING_SHM_NAME "dpm_submission_queue_shm"
#define COMPLETION_QUEUE_NAME "dpm_completion_queue"

#define DPM_SUBMISSION_QUEUE_SIZE 128 // TODO: dynamic? parameter?

#define RING_BUFFER_SHM_NAME "/request_ring_buffer"

// #define RING_BUFFER_SHM_NAME2 "/request_ring_buffer2"
#define RING_BUFFER_NUMBER 15

// #define DPM_DEBUG
#undef DPM_DEBUG
#ifdef DPM_DEBUG
#include <cstdio>
#include <iostream>
#define print_debug(fmt, ...) printf("DEBUG %s: " fmt "\n", __func__, ##__VA_ARGS__)
#else
#define print_debug(fmt, ...)
#endif

/// we pass pointers in shared memory as an offset from the start of the shared
/// memory region
typedef size_t shm_ptr;

/// differentiate between memory management requests and task submission
/// requests dpm will process mem requests immediately, but pass on task
/// submission requests to the executors
enum dpm_req_type
{
    DPM_REQ_TYPE_MEM,
    DPM_REQ_TYPE_TASK
};

/// the struct for all requests messages (using the q mpmc queue) that the
/// application can submit to the DPManager, including both memory management
/// and task submission requests, `ptr` will be casted to the appropriate
/// request type
struct dpm_req_msg
{
    enum dpm_req_type type;
    shm_ptr ptr;
};

// kernel/task name, they should be independent of the executor platform
enum dpm_task_name
{
    TASK_COMPRESS_DEFLATE,
    TASK_DECOMPRESS_DEFLATE,
    TASK_DECOMPRESS_LZ4,
    TASK_REGEX_COUNT,  // only the aggregate count
    TASK_REGEX_SEARCH, // all occurrences found
    TASK_NULL,
    TASK_NAME_LAST // keep last don't use, this is actually used as the size of
                   // the enum
};

/* enum dpm_kernel_name
{
    COMPRESS_DEFLATE_DPKERNEL,
    DECOMPRESS_DEFLATE_DPKERNEL,
    DECOMPRESS_LZ4_DPKERNEL,
    NULL_KERNEL,
    KERNEL_NAME_LAST // keep last don't use, this is actually used as the size
of the enum
}; */

// supported devices
// e.g. application can select some devices, initialize them,
// and the dp kernel manager will schedule accordingly
enum dpm_device
{
    DEVICE_BLUEFIELD_2,
    DEVICE_BLUEFIELD_3,
    DEVICE_SOFTWARE,
    DEVICE_NULL,
    DEVICE_NONE, // no device specified, will be chosen by the dp manager
    DEVICE_LAST  // keep last don't use, this is actually used as the size of the
                 // enum
};

// NOTE: the application no longer specify a performance hint
// it should just choose where (which dpkernel_device) to execute the compute
// kernel on
/* enum perf_hint
{
    PERF_HINT_LATENCY,
    PERF_HINT_THROUGHPUT
}; */

// error codes here
enum dpkernel_error
{
    DPK_ONGOING,     // task is ongoing, submitted but not completed
    DPK_SUCCESS,     // task completed successfully
    DPK_ERROR_AGAIN, // try again later, e.g. queue is full etc.
    DPK_ERROR_FAILED // task failed
};

/// this is the base struct for all tasks that the application can submit to the
/// DPManager. Essentially, this is the interface. DP Manager will construct the
/// corresponding task struct for execution

#define MEMCPY_NULL_KERNEL
// #undef MEMCPY_NULL_KERNEL

#ifdef TEST_USE_MEMCPY
struct dpkernel_task_base
{
    enum dpkernel_device device;
    enum dpkernel_name name;
    /* enum perf_hint hint; */

    // /// signifies completion of the task, start execution with ONGOING
    // enum dpkernel_error completion;

    /// input buffer, raw pointer owned by application
    void *in;
    /// input data size
    size_t in_size;

    /// output buffer, raw pointer owned by application
    char out[TEST_PAYLOAD_SIZE];
    /// output data size, filled in after execution of kernel
    size_t out_size;

    /// some context that the user may or may not need, pass in upon task
    /// submission, get the same thing back this uniquely identifies the task
    void *user_data;
};
#else
struct dpkernel_task_base
{
    enum dpm_device device;
    enum dpm_task_name task;
    /* enum perf_hint hint; */

    /// signifies completion of the task, start execution with ONGOING
    std::atomic<enum dpkernel_error> completion;

    /// input buffer, allocate only using the DPM API, which gives back a shm_ptr
    shm_ptr in;

    /// input data size
    size_t in_size;

    /// output buffer, allocate only using the DPM API, which gives back a shm_ptr
    shm_ptr out;

    /// output buf size
    size_t out_size;

    /// filled in after execution of kernel
    // size_t actual_out_size;

#ifdef MEMCPY_NULL_KERNEL
    shm_ptr in_cpy;
    shm_ptr out_cpy;
#endif

    doca_buf *src_doca_buf;
    doca_buf *dst_doca_buf;

    /// some context that the user may or may not need, pass in upon task
    /// submission, get the same thing back this uniquely identifies the task
    void *user_data;
};
#endif

/// the completion struct that the application can get back from the DPManager
/// after submitting and executing a task e.g. the null executor holds a queue
/// of tasks, and also a queue of completions where dpm polls from
#ifdef TEST_USE_MEMCPY
struct dpkernel_completion
{
    /// the user data of the task that was completed
    void *user_data;

    /// output buffer, raw pointer owned by application
    char out[TEST_PAYLOAD_SIZE];
    /// output data size, filled in after execution of kernel
    size_t out_size;

    /// the error code of the task
    std::atomic<dpkernel_error> completion;
    // dpkernel_error error;

    /// once error is not NONE, dp manager shared mem is ready to be consumed
    /// if one app consumes (polls) this, it should set this to true
    /// and reset when it is done consuming
    // std::atomic<bool> being_consumed;

    /* dpkernel_completion(const dpkernel_completion &other)
        : user_data(other.user_data), error(other.error) {}

    dpkernel_completion &operator=(const dpkernel_completion &other)
    {
        if (this != &other)
        {
            user_data = other.user_data;
            error = other.error;
        }
        return *this;
    }

    ~dpkernel_completion() = default; */
};
#else
struct dpkernel_completion
{
    /// the user data of the task that was completed
    void *user_data;

    /// output buffer, raw pointer owned by application
    void *out;
    /// output data size, filled in after execution of kernel
    size_t out_size;

    /// the error code of the task
    std::atomic<dpkernel_error> error;
    // dpkernel_error error;

    /// once error is not NONE, dp manager shared mem is ready to be consumed
    /// if one app consumes (polls) this, it should set this to true
    /// and reset when it is done consuming
    // std::atomic<bool> being_consumed;

    /* dpkernel_completion(const dpkernel_completion &other)
        : user_data(other.user_data), error(other.error) {}

    dpkernel_completion &operator=(const dpkernel_completion &other)
    {
        if (this != &other)
        {
            user_data = other.user_data;
            error = other.error;
        }
        return *this;
    }

    ~dpkernel_completion() = default; */
};
#endif

/// this lives in shared memory and is accessed concurrently by the manager
/// (consumer) and applications (producers) templated on the task type (e.g.
/// task base, or specialized for DOCA, etc)
/* template <typename TaskType> struct dpm_submission
{
    // lock when submitting to, and when polling from (by Manager) the task
    std::mutex mutex;
    /// ready to be executed
    std::atomic<bool> is_ready;
    TaskType task;
};

struct dpm_completion
{
    // lock when submitting to (by Manager), and when polling from the task
    std::mutex mutex;
    dpkernel_completion completion;
};

/// the pair lives in shared memory
struct dpm_submission_completion
{
    dpm_submission<dpkernel_task_base> submission;
    dpm_completion completion;
}; */