#pragma once
#include "common.hpp"
#include <mimalloc.h>
#include <pthread.h>
#include <unordered_map>
#include <vector>

// well maybe just use message queue...
// #define DPM_SHM_NAME "dpm_shared_mem"
#define DPM_SHM_INPUT_REGION_NAME "dpm_shared_mem_input"
#define DPM_SHM_OUTPUT_REGION_NAME "dpm_shared_mem_output"
#define DPM_SHM_IO_REGION_NAME "dpm_shared_mem_io"
#define DPM_REQ_CTX_SHM_NAME "dpm_req_ctx_shared_mem"
// #define DPM_MEM_REGION_EXCHANGE_NAME "dpm_mem_region_exchange"

#define GB (1024 * 1024 * 1024)
#define MB (1024 * 1024)
#define KB (1024)

// this size is used for both input and output memory regions
#define DPM_INPUT_SHM_SIZE (((size_t)4 * GB))  // 2GB
#define DPM_OUTPUT_SHM_SIZE (((size_t)4 * GB)) // 2GB
// Single-IO mode default size.
// NOTE: keep this conservative by default; users may override at build time for larger rings.
#ifndef DPM_COMBINED_IO_SHM_SIZE
#define DPM_COMBINED_IO_SHM_SIZE (DPM_INPUT_SHM_SIZE)
#endif
// #define DPM_SHM_IO_BUF_SIZE 2 * 1024 * 1024 * 1024 // 2GB
#define DPM_REQ_CTX_SHM_SIZE 1 * 1024 * 1024 * 1024 // 1GB

// the parameter when initializing DOCA mem, max doca_bufs allowed in the inventory
#define DPM_DOCA_MAX_BUF (1048576 * 4) // 8192

// struct mi_allocator
// {
//     mi_arena_id_t arena_id;
//     mi_heap_t *heap;
// };

// a thread local mimalloc heap, since mi heaps are only legal to use in the thread that created them
struct thread_local_mi_heap
{
    mi_heap_t *heap = nullptr;

    thread_local_mi_heap() = default;

    ~thread_local_mi_heap()
    {
        if (heap != nullptr)
        {
            // mi_heap_destroy(heap);
            // mi_heap_delete(heap);
            // heap = nullptr;
        }
    }
};

//// API for messages regarding mem management
enum dpm_mem_req_type
{
    DPM_MEM_REQ_ALLOC_INPUT,
    DPM_MEM_REQ_ALLOC_OUTPUT,
    /// Allocate both input and output buffers in one mem request roundtrip.
    DPM_MEM_REQ_ALLOC_IO,
    // DPM_MEM_REQ_ALLOC_INPUT_OUTPUT,
    DPM_MEM_REQ_ALLOC_COALESCED,
    DPM_MEM_REQ_FREE_INPUT,
    DPM_MEM_REQ_FREE_OUTPUT,
    /// Free both input and output buffers in one mem request roundtrip.
    DPM_MEM_REQ_FREE_IO,
    // DPM_MEM_REQ_FREE_INPUT_OUTPUT,
    DPM_MEM_REQ_FREE_COALESCED,
    DPM_MEM_REQ_ALLOC_TASK,
    DPM_MEM_REQ_FREE_TASK,
};

// the memory request context that will be allocated in shared memory and sent (its ptr) thru the message queue
struct dpm_mem_req
{
    enum dpm_mem_req_type type;
    // size_t size; // these should be available in the task struct
    // shm_ptr buf;                            // buffer ptr will be filled in if alloc, and used for free
    shm_ptr task; // of type `dpkernel_task_base`. backend should have device specific func handler to fill in relevant
                  // fields (src/dst doca_buf)
    std::atomic<dpkernel_error> completion; // set to true when the request is done, app should poll this for completion
};

struct dpm_shared_mem
{
    void *shm_ptr;
    size_t shm_size;

    /// the linux shared memory file descriptor
    int shm_fd;

    /// the mimalloc arena from which all heaps will allocate
    mi_arena_id_t arena_id;
    /// each thread must have its own heap, which may allocate from the same arena
    // std::unordered_map<pthread_t, mi_heap_t *> heaps = {};
};

struct dpm_io_mem_region
{
    struct dpm_shared_mem input_region;
    struct dpm_shared_mem output_region;
};
