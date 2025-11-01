#pragma once
// #include <boost/interprocess/ipc/message_queue.hpp>
// #include <boost/lockfree/queue.hpp>
#include <fcntl.h>
#include <mimalloc.h>
#include <mpmc_queue.hpp>
#include <string.h>
#include <sys/mman.h>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_compress.h>
#include <doca_ctx.h>
#include <doca_error.h>
#include <doca_log.h>

#include "doca_common.h"

#include "common.hpp"

// well maybe just use message queue...
// #define DPM_SHM_NAME "dpm_shared_mem"
#define DPM_SHM_INPUT_REGION_NAME "dpm_shared_mem_input"
#define DPM_SHM_OUTPUT_REGION_NAME "dpm_shared_mem_output"
#define DPM_REQ_CTX_SHM_NAME "dpm_req_ctx_shared_mem"
// #define DPM_MEM_REGION_EXCHANGE_NAME "dpm_mem_region_exchange"

// this size is used for both input and output memory regions
#define DPM_INPUT_SHM_SIZE 1024 * 1024 * 1024  // 1GB
#define DPM_OUTPUT_SHM_SIZE 1024 * 1024 * 1024 // 1GB
// #define DPM_SHM_IO_BUF_SIZE 2 * 1024 * 1024 * 1024 // 2GB
#define DPM_REQ_CTX_SHM_SIZE 1 * 1024 * 1024 * 1024 // 1GB

#define DPM_DOCA_WORKQ_DEPTH 128               // probably move this somewhere else

// the parameter when initializing DOCA mem, max doca_bufs allowed in the inventory
#define DPM_DOCA_MAX_BUF 256 // 8192

// TODO: make this configurable
#define DPM_DOCA_PCI_ADDR "e1:00.0"

struct dpm_shared_mem
{
    void *shm_ptr;
    size_t shm_size;

    /// the linux shared memory file descriptor
    int shm_fd;

    // TODO: this part shouldn't be sent thru message queue
    struct mi_allocator
    {
        mi_arena_id_t arena_id;
        mi_heap_t *heap;
    } allocator;
};

struct dpm_io_mem_region
{
    struct dpm_shared_mem input_region;
    struct dpm_shared_mem output_region;
};

#define DPM_MEM_REGION_EXCHANGE_QUEUE_NAME "dpm_mem_region_exchange_queue"
#define DPM_MEM_REGION_EXCHANGE_QUEUE_SIZE 16
// use this queue to exchange memory region info beween app and dpm
// XXX: no need to exchange info now
// boost::interprocess::message_queue *dpm_shm_region_exchange_queue;

bool _setup_shm_region_exchange_queue_dpm();

/// boost message queue for submission
// completion is done through
// boost::interprocess::message_queue *dpm_submission_queue;

/// the shared memory region for DPM itself
extern struct dpm_io_mem_region dpm_own_mem_region;
// dpm uses this, records the app's memory regions
// struct dpm_io_mem_region dpm_app_mem_region;

// the shared memory region for the memory request context for DPM
extern struct dpm_shared_mem dpm_req_ctx_shm;
// the shared memory region for the memory request context for the *app*
// struct dpm_shared_mem app_req_ctx_shm;

/// will initialize the shared memory, and `dpm_shared_mem` struct
extern bool _setup_shared_memory(size_t size, struct dpm_shared_mem *dpm_shared_mem, const char *shm_name);

/// call this to setup the 3 regions (input, output, req_ctx) of shared memory
extern bool _setup_shared_memory_regions(struct dpm_io_mem_region *io_region, struct dpm_shared_mem *req_ctx_shm);

/// setup mimalloc options, call this AFTER shared memory is setup
extern bool _setup_mi_options();

/// setup the heap and arena etc. for mimalloc, afterwards, mimalloc functions can be used
bool setup_mimalloc(struct dpm_io_mem_region *io_region, struct dpm_shared_mem *req_ctx_shm);

/// teardown mimalloc, unmap shared memory, close file descriptor etc.
bool teardown_mimalloc(struct dpm_shared_mem *dpm_shared_mem, const char *shm_name);

struct dpm_doca_state
{
    char *src_mem_range;
    char *dst_mem_range;
    struct doca_compress *compress;
    struct program_core_objects state;
};

extern struct dpm_doca_state dpm_doca_state;

/// setup shared memory for device, call once per device (not kernel) during initialization
/// this is the entry point for setting up the device and memory
bool setup_bluefield(const char *pci_addr, struct dpm_doca_state *dpm_doca_state);

bool cleanup_bluefield();

// TODO: unmap the shm region properly, close the fd etc.
void doca_mem_region_free_cb(void *addr, size_t len, void *opaque);

/// setup DOCA (IO buffers) for shared memory
bool setup_doca_shared_mem(const char *pci_addr, struct dpm_doca_state *dpm_doca_state);

/// DPM needs to tell the app about the mem req ctx shared memory region
void _dpm_send_mem_region_info();

/// DPM needs to know the app's buf memory regions
/// in order to allocate then return shm_ptr to the app.
/// Also needs to know the mem req ctx shared memory region
/// in order to find the local ptr to the app passed mem req ctx struct
void _dpm_recv_mem_region_info();

// void _dpm_exchange_mem_info();

char *dpm_get_input_ptr_from_shmptr(shm_ptr offset);
char *dpm_get_output_ptr_from_shmptr(shm_ptr offset);
struct dpm_mem_req *dpm_get_mem_req_ptr_from_shmptr(shm_ptr offset);

dpkernel_task_base *dpm_get_task_ptr_from_shmptr(shm_ptr offset);

shm_ptr dpm_get_shm_ptr_for_input_buf(char *buf);
shm_ptr dpm_get_shm_ptr_for_output_buf(char *buf);
inline shm_ptr dpm_get_shm_ptr_for_mem_req_ctx(char *buf);

char *allocate_input_buf(size_t size);
char *allocate_output_buf(size_t size);

void free_buf(char *buf);

//// API for messages regarding mem management
enum dpm_mem_req_type
{
    DPM_MEM_REQ_ALLOC_INPUT,
    DPM_MEM_REQ_ALLOC_OUTPUT,
    DPM_MEM_REQ_FREE_INPUT,
    DPM_MEM_REQ_FREE_OUTPUT,
    DPM_MEM_REQ_ALLOC_TASK,
    DPM_MEM_REQ_FREE_TASK,
};

// the memory request context that will be allocated in shared memory and sent (its ptr) thru the message queue
struct dpm_mem_req
{
    enum dpm_mem_req_type type;
    size_t size;
    shm_ptr buf;           // buffer ptr will be filled in if alloc, and used for free
    std::atomic<dpkernel_error> completion; // set to true when the request is done, app should poll this for completion
};

// boost message queue for memory requests
#define DPM_MEM_REQ_QUEUE_NAME "dpm_mem_req_queue"
#define DPM_MEM_REQ_QUEUE_SIZE 32
extern rigtorp::MPMCQueue<shm_ptr, DPM_MEM_REQ_QUEUE_SIZE> *dpm_mem_req_queue;

// setup the message queue for memory requests
bool _setup_mem_req_queue();

/// poll the message queue for memory requests, and perform the allocation/free.
/// Eventually set the done flag in the request struct, which the app would poll on
/// return true if there is a request and it will be processed, false otherwise
extern bool dpm_poll_mem_req();

/// entry point to initialize the shared memory and mimalloc and everything
bool dpm_mem_init();