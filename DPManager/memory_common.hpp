#pragma once
#include <mimalloc.h>
#include <pthread.h>
#include <unordered_map>
#include <vector>

// well maybe just use message queue...
// #define DPM_SHM_NAME "dpm_shared_mem"
#define DPM_SHM_INPUT_REGION_NAME "dpm_shared_mem_input"
#define DPM_SHM_OUTPUT_REGION_NAME "dpm_shared_mem_output"
#define DPM_REQ_CTX_SHM_NAME "dpm_req_ctx_shared_mem"
// #define DPM_MEM_REGION_EXCHANGE_NAME "dpm_mem_region_exchange"

#define GB (1024 * 1024 * 1024)
#define MB (1024 * 1024)
#define KB (1024)

// this size is used for both input and output memory regions
#define DPM_INPUT_SHM_SIZE (((size_t)4 * GB))  // 2GB
#define DPM_OUTPUT_SHM_SIZE (((size_t)4 * GB)) // 2GB
// #define DPM_SHM_IO_BUF_SIZE 2 * 1024 * 1024 * 1024 // 2GB
#define DPM_REQ_CTX_SHM_SIZE 1 * 1024 * 1024 * 1024 // 1GB

// the parameter when initializing DOCA mem, max doca_bufs allowed in the inventory
#define DPM_DOCA_MAX_BUF 1048576 // 8192

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
