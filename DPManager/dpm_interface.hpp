#pragma once

#include "common.hpp"
// #include "memory.hpp"
#include "memory_common.hpp"
#include "mimalloc.h"
// #include "mpmc_queue.hpp"
// #include <boost/interprocess/ipc/message_queue.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <fcntl.h>
#include <mutex>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

/// this is the first thing an app should do to initialize the DPManager
bool dpm_frontend_initialize();

bool _dpm_setup_msgq();

bool dpm_teardown_msgq();

/// submit a task to the DPManager.
/// Internally, fill in (copy from shared mem in dpm_submission_completion) the task struct for the corresponding
/// executor
/* bool dpm_submit_task(enum dpkernel_device device, enum dpkernel_name name, void *in, size_t in_size, void *out,
                     size_t out_size, void *user_data); */
// bool dpm_submit_task(dpkernel_task_base *submission);

/// submit a task to the DPManager using message queue
// bool dpm_submit_task_msgq(dpkernel_task_base *submission);
void dpm_submit_task_msgq_blocking(dpkernel_task *submission, dpm_device device);
void dpm_submit_task_msgq_blocking(int thread_id, dpkernel_task *submission, dpm_device device);

/// submit multiple tasks at once as a same chunk of message
void dpm_submit_task_msgq_batched(int thread_id, dpkernel_task **tasks, int n_tasks, dpm_device device);

/// this is for multithreaded DPM, the front end should try to distribute the invocations to different threads
void dpm_submit_task_msgq_multithread(dpkernel_task *submission, dpm_device device);

/// deprecated
// bool dpm_submit_task_msgq(dpkernel_task_base *submission, dpm_device device);

/// consume the completion in shared mem of dpm_submission_completion
// bool consume_completion();

/// User facing (concurrent access), should pass in his own completion struct, which will be filled in by the DPManager
/// return true if there is a completion and the pointed struct will be filled in, false otherwise
/// user should repeatedly call this until it returns true if it wants a completion
// bool dpm_try_get_completion(dpkernel_completion *completion);

// bool dpm_try_get_completion_msgq(dpkernel_completion *completion);

// no need for dpm polling, application will do this instead
// bool dpm_get_completion_shm(dpkernel_task_base *task);

unsigned long dpm_get_completion_queue_current_size();

/// the shared memory region for app itself
extern struct dpm_io_mem_region app_own_mem_region;
// app uses this, records the DPM's memory regions
// struct dpm_io_mem_region app_dpm_mem_region;

// the shared memory region for the memory request context for DPM
// struct dpm_shared_mem dpm_req_ctx_shm;
extern std::mutex heap_alloc_mutex;

// app/interface uses this, records its own mem req region
// unused now
// struct dpm_io_mem_region app_dpm_mem_req_ctx_region;

// use this queue to exchange memory region info beween app and dpm
// boost::interprocess::message_queue *app_shm_region_exchange_queue;

// bool _setup_shm_region_exchange_queue_app();

bool _setup_shm_region_mem_req_ctx();

/// app needs to know where the DPM has placed the input and output buffers;
/// it also needs to know where the DPM has placed the input and output buffers
void _app_send_then_recv_mem_region_info();

/// MUST call this during initialization, the DPM will wait and use this to setup the shared memory;
/// the app shouldn't wait though, assuming DPM is already running and sent the msg
void _exchange_mem_info();

// the app will allocate and mmap the shared memory, then NOT send this info to the DPM
extern struct dpm_shared_mem app_req_ctx_shm;

inline shm_ptr app_get_shm_ptr_for_mem_req_ctx(struct dpm_mem_req *buf)
{
    return shm_ptr((char *)buf - (char *)app_req_ctx_shm.shm_ptr);
}
inline shm_ptr app_get_shm_ptr_for_task(dpkernel_task *task)
{
    return shm_ptr((char *)task - (char *)app_req_ctx_shm.shm_ptr);
}
inline char *app_get_input_ptr_from_shmptr(shm_ptr offset)
{
    return (char *)app_own_mem_region.input_region.shm_ptr + offset;
}

inline char *app_get_output_ptr_from_shmptr(shm_ptr offset)
{
    return (char *)app_own_mem_region.output_region.shm_ptr + offset;
}

inline shm_ptr app_get_shm_ptr_for_input_buf(char *buf)
{
    return shm_ptr((char *)buf - (char *)app_own_mem_region.input_region.shm_ptr);
}

inline shm_ptr app_get_shm_ptr_for_output_buf(char *buf)
{
    return shm_ptr((char *)buf - (char *)app_own_mem_region.output_region.shm_ptr);
}

//// functions for interacting with DPM shared memory

/// @param size the size of the buffer to allocate
/// @param buf the (pointed to) shm ptr will be filled in if alloc succeeded
// bool _dpm_alloc_shared_mem(size_t size, shm_ptr *buf, dpm_mem_req_type type);
bool _dpm_alloc_shared_mem(dpkernel_task *task, enum dpm_mem_req_type type);

struct dpm_mem_req *_dpm_submit_shared_mem_req_async(dpkernel_task *task, enum dpm_mem_req_type type);

// bool dpm_alloc_input_buf(size_t size, shm_ptr *buf);
bool dpm_alloc_input_buf(uint32_t size, dpkernel_task *task);

struct dpm_mem_req *dpm_alloc_input_buf_async(uint32_t size, dpkernel_task *task);

// bool dpm_alloc_output_buf(size_t size, shm_ptr *buf);
bool dpm_alloc_output_buf(uint32_t size, dpkernel_task *task);

bool dpm_alloc_input_output_buf(uint32_t in_size, uint32_t out_size, dpkernel_task *task);

struct dpm_mem_req *dpm_alloc_output_buf_async(uint32_t size, dpkernel_task *task);

bool dpm_wait_for_mem_req_completion(struct dpm_mem_req *req);

/// @brief allocate a task struct for the application, should be `dpkernel_task` which is a union of all task types
/// @note FOR PEACE OF MIND, there probably is NO reason to use a different task type -- should always use the default
/// of union
/// @param [out] task allocate a task struct, a pointer to the task struct will be filled in
/// @param device the device to allocate the task for, NONE means allocate a task for all possible devices
/// @return true if the allocation succeeded, false otherwise
bool app_alloc_task_request(dpkernel_task **task);

void app_free_task_request(dpkernel_task *task);

inline dpkernel_error app_check_task_completion(dpkernel_task *task)
{
    return task->base.completion.load(std::memory_order_acquire);
}

inline dpkernel_error app_check_task_completion(dpkernel_task_base *task)
{
    return task->completion.load(std::memory_order_acquire);
}

struct dpm_mem_req *_dpm_free_shared_mem(dpkernel_task *task, bool is_input);

// struct dpm_mem_req *_dpm_free_shared_mem_async(dpkernel_task *task, bool is_input);

struct dpm_mem_req *dpm_free_input_buf_async(dpkernel_task *task);
struct dpm_mem_req *dpm_free_output_buf_async(dpkernel_task *task);

bool dpm_free_input_buf(dpkernel_task *task);
bool dpm_free_output_buf(dpkernel_task *task);
