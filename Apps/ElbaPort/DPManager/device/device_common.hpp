#pragma once

/// we pass pointers in shared memory as an offset from the start of the shared
/// memory region
#include <cstddef>
typedef size_t shm_ptr;

// supported devices
// e.g. application can select some devices, initialize them,
// and the dp kernel manager will schedule accordingly
#include <atomic>
#include <cstdint>
enum dpm_device : uint8_t
{
    DEVICE_BLUEFIELD_2,
    DEVICE_BLUEFIELD_3, // DOCA v2.5
    // DEVICE_BLUEFIELD_3_V2_2, // DOCA v2.2
    DEVICE_ELBA, // Pensando Elba
    DEVICE_SOFTWARE,
    DEVICE_NULL,
    DEVICE_NONE, // no device specified, will be chosen by the dp manager
    DEVICE_LAST  // keep last don't use, this is actually used as the size of the
                 // enum
};

// error codes here
enum dpkernel_error : uint8_t
{
    DPK_ONGOING,     // task is ongoing, submitted but not completed
    DPK_SUCCESS,     // task completed successfully
    DPK_ERROR_AGAIN, // try again later, e.g. queue is full etc.
    DPK_ERROR_FAILED // task failed
};

// kernel/task name, they should be independent of the executor platform
enum dpm_task_name : uint8_t
{
    // TASK_COMPRESS_DEFLATE,
    TASK_DECOMPRESS_DEFLATE,
    // TASK_REGEX_COUNT,  // only the aggregate count
    TASK_REGEX_SEARCH, // all occurrences found
    TASK_NULL,
    TASK_NAME_LAST // keep last don't use, this is actually used as the size of
                   // the enum
};

/// this is the base struct for all tasks that the application can submit to the
/// DPManager. Essentially, this is the interface. DP Manager will construct the
/// corresponding task struct for execution
struct dpkernel_task_base
{
    /// signifies completion of the task, start execution with ONGOING
    std::atomic<enum dpkernel_error> completion{dpkernel_error::DPK_ONGOING}; // no need to alignas(64)..?
    enum dpm_device device;
    enum dpm_task_name task;
    /* enum perf_hint hint; */

    /// input data size
    uint32_t in_size;

    /// output buf size
    uint32_t out_size;

    /// filled in after execution of kernel
    uint32_t actual_out_size;

    /// input buffer, allocate only using the DPM API, which gives back a shm_ptr
    shm_ptr in;

    /// output buffer, allocate only using the DPM API, which gives back a shm_ptr
    shm_ptr out;

#ifdef MEMCPY_KERNEL
    shm_ptr in_cpy;
    shm_ptr out_cpy;
#endif

    // #ifdef SCHEDULING
    /// the estimated time for the task to be completed, computed during scheduling, will need to subtract it once
    /// completed
    long estimated_completion_time;
    // #endif

    // #if defined(COALESCING)
    /// the head of the coalesced task is NOT from or allocated by the user, needs special identification
    bool is_coalesced_head;
    // #endif

    /// some context that the user may or may not need, pass in upon task
    /// submission, get the same thing back this uniquely identifies the task
    void *user_data;

#ifdef CHECK_LATENCY
    /// the timestamp when the task was submitted to the DPManager from app side, used for latency measurement
    long submission_timestamp;
    /// timestamp when the task is consumed/executed
    long execution_timestamp;
    /// the timestamp when the task was completed, as checked by the application
    long completion_timestamp;
#endif
};