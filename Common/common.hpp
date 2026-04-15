#pragma once
#include <cstdint>
#pragma clang diagnostic ignored "-Wdeprecated-declarations"

#include <atomic>
#include <cstddef>
#include <doca_buf.h>

/// Build-time feature gate for DOCA Regex support.
/// Meson sets this explicitly; default to disabled for safety in mixed environments.
#ifndef DPK_HAS_DOCA_REGEX
#define DPK_HAS_DOCA_REGEX 0
#endif

#if DPK_HAS_DOCA_REGEX
#include <doca_regex.h>
#endif

// Forward declarations for per-task preallocated DOCA task handles.
// We avoid pulling heavy task headers into this common header.
struct doca_compress_task_decompress_deflate;
struct doca_aes_gcm_task_decrypt;

// NOTE: this is for the baseline comparison, should always use ring buffer for better performance
#define USE_RING_BUFFER
// #undef USE_RING_BUFFER

/// baseline is this policy: at the moment of scheduling, we just check if the hw kernel is at full capacity or not
/// if it is, we just use the sw kernel; otherwise we use the hw kernel
#define BASELINE_SCHEDULING
// #undef BASELINE_SCHEDULING

#define MEMCPY_KERNEL
#undef MEMCPY_KERNEL

#define SCHEDULING
#undef SCHEDULING
#define ENABLE_SCHEDULING
#undef ENABLE_SCHEDULING

#define CHECK_LATENCY
#undef CHECK_LATENCY

#define COALESCING
// #undef COALESCING

/// If DOCA Regex is unavailable in this build, disable coalescing globally to avoid touching regex-specific task state.
#if !DPK_HAS_DOCA_REGEX
#undef COALESCING
#endif

#define SUBMISSION_QUEUE_NAME "dpm_submission_queue"
// #define SUBMISSION_QUEUE_BACKING_SHM_NAME "dpm_submission_queue_shm"
#define COMPLETION_QUEUE_NAME "dpm_completion_queue"

#define DPM_SUBMISSION_QUEUE_SIZE 128 // TODO: dynamic? parameter?

#define RING_BUFFER_SHM_NAME "/request_ring_buffer"

// #define RING_BUFFER_SHM_NAME2 "/request_ring_buffer2"
#define RING_BUFFER_NUMBER 4

/// no. of threads for parallelism in DPM (threads for hw kernels)
#define N_DPM_THREADS RING_BUFFER_NUMBER // 1 // RING_BUFFER_NUMBER

/// no. of threads for sw kernels, all sw kernels share the same thread pool
#define N_SW_KERNELS_THREADS 2

/// Base CPU core index used by DPK thread pinning.
/// DPM thread i pins to (DPK_THREAD_PIN_BASE_CORE + i); SW kernel threads start after DPM threads.
/// Default reserves cores 0..3 for DDS app/workers/agent.
#ifndef DPK_THREAD_PIN_BASE_CORE
#define DPK_THREAD_PIN_BASE_CORE 4
#endif
/// Backward-compatible alias.
#ifndef DPK_DPM_PIN_BASE_CORE
#define DPK_DPM_PIN_BASE_CORE DPK_THREAD_PIN_BASE_CORE
#endif

static_assert(N_DPM_THREADS >= N_SW_KERNELS_THREADS,
              "N_DPM_THREADS must be >= N_SW_KERNELS_THREADS, bc. currently things like `size_t "
              "inflight_bytes[N_DPM_THREADS][dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};` is shared "
              "between DPM threads and sw kernels' threads");
static_assert(N_DPM_THREADS <= 255, "N_DPM_THREADS must fit into bf3 callback_owner_thread_id (uint8_t).");

// the size for each kernel's request queue
#define DPM_KERNELS_QUEUE_SIZE (65536)                  //(1048576) seems to be large enough
#define DPM_HW_KERNEL_QUEUE_SIZE DPM_KERNELS_QUEUE_SIZE // DPM_SUBMISSION_QUEUE_SIZE

// #define DPM_DEBUG
#undef DPM_DEBUG
#ifdef DPM_DEBUG
#include <cstdio>
#include <iostream>
#define print_debug(fmt, ...) printf("DEBUG %s: " fmt "\n", __func__, ##__VA_ARGS__)
#else
#define print_debug(fmt, ...)
#endif

/// Build-time toggle for using one shared-memory pool for both input and output buffers.
/// 1: single combined IO region; 0: legacy split input/output regions.
#ifndef DPK_SINGLE_IO_REGION
#define DPK_SINGLE_IO_REGION 1
#endif

#define NANOSECONDS_PER_SECOND 1000000000

/// we pass pointers in shared memory as an offset from the start of the shared
/// memory region
typedef size_t shm_ptr;

/// differentiate between memory management requests and task submission
/// requests dpm will process mem requests immediately, but pass on task
/// submission requests to the executors
enum dpm_req_type : uint8_t
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
enum dpm_task_name : uint8_t
{
    // TASK_COMPRESS_DEFLATE,
    TASK_DECOMPRESS_DEFLATE,
    TASK_AES_GCM, // decrypt-only prototype kernel on BF3 (DOCA AES-GCM)
    // TASK_REGEX_COUNT,  // only the aggregate count
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
enum dpm_device : uint8_t
{
    DEVICE_BLUEFIELD_2,
    DEVICE_BLUEFIELD_3, // DOCA v2.5
    // DEVICE_BLUEFIELD_3_V2_2, // DOCA v2.2
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
enum dpkernel_error : uint8_t
{
    DPK_ONGOING,     // task is ongoing, submitted but not completed
    DPK_SUCCESS,     // task completed successfully
    DPK_ERROR_AGAIN, // try again later, e.g. queue is full etc.
    DPK_ERROR_FAILED // task failed
};

/// Buffer binding state for DPM memory management.
/// This explicitly models ownership and allocation state, instead of inferring from shm_ptr value.
enum dpm_io_binding_state : uint8_t
{
    DPM_IO_BINDING_UNBOUND = 0, // no buffer bound
    DPM_IO_BINDING_DPM_OWNED,   // host buffer allocated/owned by DPM
    DPM_IO_BINDING_EXTERNAL,    // caller-owned external buffer (e.g., SPDK DMA ring)
};

/// this is the base struct for all tasks that the application can submit to the
/// DPManager. Essentially, this is the interface. DP Manager will construct the
/// corresponding task struct for execution

struct dpkernel_task_base
{
    /// signifies completion of the task, start execution with ONGOING
    std::atomic<enum dpkernel_error> completion{dpkernel_error::DPK_ONGOING}; // no need to alignas(64)..?
    enum dpm_device device;
    /// Device that owns any kernel-specific memory resources created via DPM mem requests
    /// (e.g., DOCA doca_buf references). This must remain stable across scheduling changes,
    /// so that freeing memory requests can be dispatched to the correct kernel even if
    /// `device` is later changed to DEVICE_SOFTWARE by the scheduler.
    enum dpm_device mem_device;
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

    /// Per-buffer binding/ownership states for host-side memory decisions.
    /// Unlike shm_ptr value checks, this remains valid when offset 0 is a legitimate external address.
    uint8_t in_binding_state = DPM_IO_BINDING_UNBOUND;
    uint8_t out_binding_state = DPM_IO_BINDING_UNBOUND;

    /// Enable lazy DOCA data binding on execute path.
    /// When true, BF3 execute handlers refresh src/dst DOCA buffer data pointers from
    /// current task `in/out` and `in_size/out_size` via doca_buf_set_data().
    /// This is intended for preallocated task/doca_buf reuse patterns (e.g., ring slots).
    bool use_lazy_doca_buf_binding = false;

    /// True when `in/out` point to caller-owned memory (e.g., external SPDK DMA ring region).
    /// DPM must skip host-side mi_free for these pointers on FREE_* requests.
    bool io_is_externally_owned = false;

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

/// the common bf3 (DOCA v2.5) extension from the base task struct
struct dpkernel_task_bf3 : dpkernel_task_base
{
    doca_buf *src_doca_buf;
    doca_buf *dst_doca_buf;
    /// Preallocated DOCA decompress task object, allocated/freed by DPM mem-req path.
    /// Reused on the hot path to avoid alloc/free per submission.
    doca_compress_task_decompress_deflate *prealloc_decompress_task;
    /// DPM thread id that allocated prealloc_decompress_task.
    uint8_t prealloc_thread_id;
    /// Internal callback->poll status handoff for BF3 callback-driven kernels.
    /// Callback updates this field; DPM thread consumes it in kernel_poll and
    /// then writes final `base.completion` in dpm_poll_completion.
    uint8_t callback_status;
    /// Internal-only routing hint for callback-driven BF3 completion path.
    /// This must never reuse base.user_data because that belongs to the app.
    uint8_t callback_owner_thread_id;
};

/// BF3 AES-GCM task extension.
/// NOTE: this prototype stores key material in req_ctx shared memory (key_shm),
/// instead of DOCA input/output payload buffers, because the key is control-plane metadata.
struct dpkernel_task_bf3_aes_gcm : dpkernel_task_base
{
    doca_buf *src_doca_buf;
    doca_buf *dst_doca_buf;
    /// Preallocated DOCA AES decrypt task object, allocated/freed by DPM mem-req path.
    /// Reused on the hot path to avoid alloc/free per submission.
    doca_aes_gcm_task_decrypt *prealloc_decrypt_task;
    /// DPM thread id that allocated prealloc_decrypt_task.
    uint8_t prealloc_thread_id;
    uint8_t callback_owner_thread_id;
    /// Internal callback->poll status handoff for AES-GCM.
    /// Callback updates this field; DPM thread consumes it in kernel_poll and
    /// then writes final `base.completion` in dpm_poll_completion.
    uint8_t callback_status;

    /// Offset into req_ctx shared memory where raw key bytes are stored.
    shm_ptr key_shm;

    /// Raw AES key byte length in req_ctx:
    /// 16 bytes (AES-128) or 32 bytes (AES-256).
    uint8_t key_size_bytes;
};

/// common bf2 (v1.5) task struct is the same as bf3 v2.5
struct dpkernel_task_bf2 : dpkernel_task_bf3
{
};

/// common bf3v2.2 (DOCA v2.2) task struct is the same as bf3 v2.5
struct dpkernel_task_bf3v2_2 : dpkernel_task_bf3
{
};

/// bf3v2.2 regex task struct
struct dpkernel_task_bf3v2_2_regex : dpkernel_task_base
{
    doca_buf *src_doca_buf;
#if DPK_HAS_DOCA_REGEX
    struct doca_regex_search_result results;
#else
    /// Placeholder when DOCA Regex is unavailable in this build.
    void *results;
#endif

    // uint32_t n_results;
};

/// union of all task types, this is mostly useful for hiding details from the app/interface side
union dpkernel_task {
    struct dpkernel_task_base base;
    struct dpkernel_task_bf3 bf3;
    struct dpkernel_task_bf3_aes_gcm bf3_aes_gcm;
    struct dpkernel_task_bf2 bf2;
    struct dpkernel_task_bf3v2_2 bf3v2_2;
    struct dpkernel_task_bf3v2_2_regex bf3v2_2_regex;

    dpkernel_task() : base()
    {
    }
};

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
