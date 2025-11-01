#include "memory.hpp"
#include "common.hpp"
#include "device_specific.hpp"
#include <cstdio>
#ifdef DOCA_VER_1_5
#include "doca_common_v_1_5.h"
#elif DOCA_VER_2_5
#include "doca_common_v_2_5.h"
#endif
#include "dp_manager_msgq.hpp"
#include "mpmc_queue.hpp"

// struct dpm_doca_state dpm_doca_state;

struct dpm_shared_mem dpm_req_ctx_shm;

struct dpm_io_mem_region dpm_own_mem_region;

// boost::interprocess::message_queue *dpm_mem_req_queue;
// rigtorp::MPMCQueue<shm_ptr, DPM_MEM_REQ_QUEUE_SIZE> *dpm_mem_req_queue;

bool _setup_shared_memory(size_t shm_size, struct dpm_shared_mem *dpm_shared_mem, const char *shm_name)
{
    dpm_shared_mem->shm_size = shm_size;

    dpm_shared_mem->shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (dpm_shared_mem->shm_fd == -1)
    {
        perror("shm_open");
        return false;
    }

    if (ftruncate(dpm_shared_mem->shm_fd, shm_size) == -1)
    {
        perror("ftruncate");
        close(dpm_shared_mem->shm_fd);
        return false;
    }

    dpm_shared_mem->shm_ptr =
        (void *)mmap(nullptr, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, dpm_shared_mem->shm_fd, 0);

    if (dpm_shared_mem->shm_ptr == MAP_FAILED)
    {
        perror("mmap shm");
        close(dpm_shared_mem->shm_fd);
        return false;
    }
    return true;
}

bool _setup_shared_memory_regions(struct dpm_io_mem_region *io_region, struct dpm_shared_mem *req_ctx_shm)
{
    if (!_setup_shared_memory(DPM_INPUT_SHM_SIZE, &io_region->input_region, DPM_SHM_INPUT_REGION_NAME))
    {
        printf("setup_shared_memory input region failed\n");
        return false;
    }
    if (!_setup_shared_memory(DPM_OUTPUT_SHM_SIZE, &io_region->output_region, DPM_SHM_OUTPUT_REGION_NAME))
    {
        printf("setup_shared_memory output region failed\n");
        return false;
    }
    if (!_setup_shared_memory(DPM_REQ_CTX_SHM_SIZE, req_ctx_shm, DPM_REQ_CTX_SHM_NAME))
    {
        printf("setup_shared_memory req_ctx_shm failed\n");
        return false;
    }
    return true;
}

bool _setup_mi_options()
{
    mi_option_enable(mi_option_disallow_os_alloc);
    printf("mi_option_reserve_os_memory = %lu\n", mi_option_get_size(mi_option_reserve_os_memory));
    // mi_option_set(mi_option_disallow_os_alloc, 1);
    // mi_option_enable(mi_option_limit_os_alloc);
    // mi_option_set(mi_option_limit_os_alloc, 1);
    return true;
}

bool setup_mimalloc(struct dpm_io_mem_region *io_region, struct dpm_shared_mem *req_ctx_shm)
{
    // setup shared mem first
    if (!_setup_shared_memory_regions(io_region, req_ctx_shm))
    {
        printf("_setup_shared_memory_regions failed\n");
        return false;
    }

    // setup mimalloc options
    if (!_setup_mi_options())
    {
        printf("setup_mi_options failed\n");
        return false;
    }

    if (!mi_manage_os_memory_ex(io_region->input_region.shm_ptr, io_region->input_region.shm_size, true, false, false,
                                -1, true, &io_region->input_region.allocator.arena_id))
    {
        printf("mi_manage_os_memory_ex failed for input region\n");
        return false;
    }
    if (!mi_manage_os_memory_ex(io_region->output_region.shm_ptr, io_region->output_region.shm_size, true, false, false,
                                -1, true, &io_region->output_region.allocator.arena_id))
    {
        printf("mi_manage_os_memory_ex failed for output region\n");
        return false;
    }
    // mimalloc seems to always zero out the memory
    // memset(dpm_shared_mem->shm_ptr, 42, dpm_shared_mem->shm_size);

    io_region->input_region.allocator.heap = mi_heap_new_in_arena(io_region->input_region.allocator.arena_id);
    if (io_region->input_region.allocator.heap == NULL)
    {
        printf("mi_heap_new_in_arena failed for input region\n");
        return false;
    }
    io_region->output_region.allocator.heap = mi_heap_new_in_arena(io_region->output_region.allocator.arena_id);
    if (io_region->output_region.allocator.heap == NULL)
    {
        printf("mi_heap_new_in_arena failed for output region\n");
        return false;
    }

    // app will do this
    /* if (!mi_manage_os_memory_ex(req_ctx_shm->shm_ptr, req_ctx_shm->shm_size, true, false, false, -1, true,
                                &req_ctx_shm->allocator.arena_id))
    {
        printf("mi_manage_os_memory_ex failed\n");
        return false;
    }

    req_ctx_shm->allocator.heap = mi_heap_new_in_arena(req_ctx_shm->allocator.arena_id);
    if (req_ctx_shm->allocator.heap == NULL)
    {
        printf("mi_heap_new_in_arena failed\n");
        return false;
    } */
    return true;
}

bool teardown_mimalloc(struct dpm_shared_mem *dpm_shared_mem, const char *shm_name)
{
    // XXX: somehow this makes mi_heap_delete seg fault...
    // mi_heap_destroy(dpm_shared_mem.allocator.heap);
    mi_heap_delete(dpm_shared_mem->allocator.heap);

    if (munmap(dpm_shared_mem->shm_ptr, dpm_shared_mem->shm_size) == -1)
    {
        perror("munmap DPM_SHM_NAME");
        return false;
    }
    close(dpm_shared_mem->shm_fd);
    if (shm_unlink(shm_name) == -1)
    {
        perror("shm_unlink DPM_SHM_NAME");
        return false;
    }

    return true;
}

bool setup_memory_allocator(struct dpm_io_mem_region *io_region, struct dpm_shared_mem *req_ctx_shm)
{
    bool ret = setup_mimalloc(io_region, &dpm_req_ctx_shm);
    if (!ret)
    {
        printf("setup_mimalloc failed\n");
        return false;
    }
    printf("setup_mimalloc succeeded\n");
    return true;
}

// bool setup_bluefield(const char *pci_addr, struct dpm_doca_state *dpm_doca_state)
// {
//     if (open_doca_device_with_pci(pci_addr, NULL, &dpm_doca_state->state.dev) != DOCA_SUCCESS)
//     {
//         printf("open_doca_device_with_pci failed\n");
//         // XXX HACK: return false; // shouldn't fail, not returning for testing purposes
//     }
//     printf("open_doca_device_with_pci succeeded\n");

//     bool ret = setup_mimalloc(&dpm_own_mem_region, &dpm_req_ctx_shm);
//     if (!ret)
//     {
//         printf("setup_mimalloc failed\n");
//         return false;
//     }
//     printf("setup_mimalloc succeeded\n");

// #ifdef DOCA_VER_1_5
//     ret = create_core_objects(&dpm_doca_state->state, DPM_DOCA_WORKQ_DEPTH);
//     if (ret != DOCA_SUCCESS)
//     {
//         printf("init_core_objects_workq failed\n");
//         return false;
//     }
//     printf("init_core_objects_workq succeeded\n");
// #elif DOCA_VER_2_5
//     ret = create_core_objects(&dpm_doca_state->state, DPM_DOCA_MAX_BUF);
//     if (ret != DOCA_SUCCESS)
//     {
//         printf("create_core_objects failed\n");
//         return false;
//     }
// #endif
//     printf("create_core_objects succeeded\n");

//     ret = setup_doca_shared_mem(pci_addr, dpm_doca_state, &dpm_own_mem_region);
//     if (!ret)
//     {
//         printf("setup_doca_shared_mem failed\n");
//         return false;
//     }
//     printf("setup_doca_shared_mem succeeded\n");

//     return true;
// }

// bool cleanup_bluefield()
// {
//     if (destroy_core_objects(&dpm_doca_state.state) != DOCA_SUCCESS)
//     {
//         printf("destroy_core_objects failed\n");
//         return false;
//     }
//     /* if (doca_compress_destroy(dpm_doca_state.compress) != DOCA_SUCCESS)
//     {
//         printf("doca_compress_destroy failed\n");
//         return false;
//     } */
//     return true;
// }

void doca_mem_region_free_cb(void *addr, size_t len, void *opaque)
{
    // seems to cause double free
    /* (void)len;
    (void)opaque;

    free(addr); */
}

// struct dpm_doca_state dpm_doca_state;

// bool setup_doca_shared_mem(const char *pci_addr, struct dpm_doca_state *dpm_doca_state,
//                            struct dpm_io_mem_region *mem_region)
// {
//     doca_error_t result;
//     // struct program_core_objects state = dpm_doca_state->state;
//     // state = {};
//     // TODO: add logic (the func which is NULL now) to sanely check for functionality required

//     // doing the init outside this function, in `setup_bluefield`
//     /* if (init_core_objects_mem(&dpm_doca_state->state, DPM_DOCA_MAX_BUF) != DOCA_SUCCESS)
//     {
//         printf("init_core_objects_mem failed\n");
//         return false;
//     }
//     print_debug("init_core_objects_mem succeeded\n"); */

//     if (doca_mmap_set_memrange(dpm_doca_state->state.src_mmap, mem_region->input_region.shm_ptr,
//                                mem_region->input_region.shm_size) != DOCA_SUCCESS)
//     {
//         printf("doca_mmap_set_memrange failed for src_mmap\n");
//         return false;
//     }
//     printf("doca_mmap_set_memrange succeeded for src_mmap\n");
//     if (doca_mmap_set_memrange(dpm_doca_state->state.dst_mmap, mem_region->output_region.shm_ptr,
//                                mem_region->output_region.shm_size) != DOCA_SUCCESS)
//     {
//         printf("doca_mmap_set_memrange failed for dst_mmap\n");
//         return false;
//     }
//     print_debug("doca_mmap_set_memrange succeeded for dst_mmap\n");

//     result = doca_mmap_set_free_cb(dpm_doca_state->state.src_mmap, doca_mem_region_free_cb, NULL);
//     if (result != DOCA_SUCCESS)
//     {
//         printf("doca_mmap_set_free_cb failed for src_mmap\n");
//         return false;
//     }
//     result = doca_mmap_set_free_cb(dpm_doca_state->state.dst_mmap, doca_mem_region_free_cb, NULL);
//     if (result != DOCA_SUCCESS)
//     {
//         printf("doca_mmap_set_free_cb failed for dst_mmap\n");
//         return false;
//     }

//     result = doca_mmap_start(dpm_doca_state->state.src_mmap);
//     if (result != DOCA_SUCCESS)
//     {
//         printf("doca_mmap_start failed for src_mmap\n");
//         return false;
//     }
//     result = doca_mmap_start(dpm_doca_state->state.dst_mmap);
//     if (result != DOCA_SUCCESS)
//     {
//         printf("doca_mmap_start failed for dst_mmap\n");
//         return false;
//     }
//     print_debug("doca_mmap_start succeeded for src_mmap and dst_mmap\n");

//     return true;
// }

// struct dpm_mem_region_exchange exchange;

// bool _exchange_mem_region_info_old(struct dpm_io_mem_region *dpm_mem_region, struct dpm_mem_region_exchange
// *exchange)
// {
//     const char *shm_name = DPM_MEM_REGION_EXCHANGE_NAME;
//     int shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
//     if (shm_fd == -1)
//     {
//         perror("shm_open");
//         return false;
//     }

//     size_t shm_size = sizeof(struct dpm_mem_region_exchange);
//     if (ftruncate(shm_fd, shm_size) == -1)
//     {
//         perror("ftruncate");
//         close(shm_fd);
//         return false;
//     }

//     void *shm_ptr = mmap(nullptr, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
//     if (shm_ptr == MAP_FAILED)
//     {
//         perror("mmap");
//         close(shm_fd);
//         return false;
//     }

//     // DON'T NEED TO put its own info in the shared memory
//     /* struct dpm_mem_region_exchange *exchange = (struct dpm_mem_region_exchange *)shm_ptr;
//     exchange->dpm_start.store(1);
//     exchange->dpm_done.store(0);
//     memcpy(&exchange->dpm_mem_region, dpm_mem_region, sizeof(struct dpm_io_mem_region));
//     exchange->dpm_done.store(1); */

//     // wait for the app to put its info
//     // TODO: do we really want to wait here?
//     while (exchange->app_done.load() == 0)
//         ;
//     // read the app's info
//     memcpy(&app_mem_region, &exchange->app_mem_region, sizeof(struct dpm_io_mem_region));
//     printf("app_mem_region.input_region.shm_ptr: %p\n", app_mem_region.input_region.shm_ptr);
//     printf("app_mem_region.output_region.shm_size: %lu\n", app_mem_region.output_region.shm_size);

//     return true;
// }

/* void _dpm_send_mem_region_info()
{
    // send the dpm's info using the message queue
    printf("dpm_own_mem_region.input_region.shm_ptr: %p\n", dpm_own_mem_region.input_region.shm_ptr);
    dpm_shm_region_exchange_queue->send(&dpm_own_mem_region, sizeof(struct dpm_io_mem_region), 0);
    printf("sent dpm's memory region info\n");

    printf("req_ctx_shm.shm_ptr: %p\n", dpm_req_ctx_shm.shm_ptr);
    dpm_shm_region_exchange_queue->send(&dpm_req_ctx_shm, sizeof(dpm_req_ctx_shm), 0);
    printf("sent req_ctx_shm region info\n");
} */

/* void _dpm_recv_mem_region_info()
{
    // receive the app's info using the message queue
    unsigned long recvd_size;
    unsigned int priority;
    printf("receiving app's memory region info\n");
    dpm_shm_region_exchange_queue->receive(&dpm_app_mem_region, sizeof(struct dpm_io_mem_region), recvd_size, priority);
    printf("dpm_app_mem_region.input_region.shm_ptr: %p\n", dpm_app_mem_region.input_region.shm_ptr);

    printf("receiving req_ctx_shm region info\n");
    dpm_shm_region_exchange_queue->receive(&dpm_req_ctx_shm, sizeof(dpm_req_ctx_shm), recvd_size, priority);
    printf("req_ctx_shm.shm_ptr: %p\n", dpm_req_ctx_shm.shm_ptr);
} */

char *dpm_get_input_ptr_from_shmptr(shm_ptr offset)
{
    return (char *)dpm_own_mem_region.input_region.shm_ptr + offset;
}

char *dpm_get_output_ptr_from_shmptr(shm_ptr offset)
{
    return (char *)dpm_own_mem_region.output_region.shm_ptr + offset;
}

dpm_mem_req *dpm_get_mem_req_ptr_from_shmptr(shm_ptr offset)
{
    return (dpm_mem_req *)((char *)dpm_req_ctx_shm.shm_ptr + offset);
}

dpkernel_task_base *dpm_get_task_ptr_from_shmptr(shm_ptr offset)
{
    return (dpkernel_task_base *)((char *)dpm_req_ctx_shm.shm_ptr + offset);
}

shm_ptr dpm_get_shm_ptr_for_input_buf(char *buf)
{
    return shm_ptr((char *)buf - (char *)dpm_own_mem_region.input_region.shm_ptr);
}

shm_ptr dpm_get_shm_ptr_for_output_buf(char *buf)
{
    return shm_ptr((char *)buf - (char *)dpm_own_mem_region.output_region.shm_ptr);
}

char *allocate_input_buf(size_t size)
{
    return (char *)mi_heap_malloc(dpm_own_mem_region.input_region.allocator.heap, size);
}

char *allocate_output_buf(size_t size)
{
    return (char *)mi_heap_malloc(dpm_own_mem_region.output_region.allocator.heap, size);
}

void free_buf(char *buf)
{
    mi_free(buf);
}

// bool _setup_mem_req_queue()
// {
//     /* boost::interprocess::message_queue::remove(DPM_MEM_REQ_QUEUE_NAME);
//     dpm_mem_req_queue = new boost::interprocess::message_queue(boost::interprocess::create_only,
//     DPM_MEM_REQ_QUEUE_NAME,
//                                                                DPM_MEM_REQ_QUEUE_SIZE, sizeof(shm_ptr)); */
//     // use mpmc queue
//     dpm_mem_req_queue = rigtorp::create_mpmc_queue<shm_ptr, DPM_MEM_REQ_QUEUE_SIZE>(DPM_MEM_REQ_QUEUE_NAME);
//     if (dpm_mem_req_queue == nullptr)
//     {
//         printf("create_mpmc_queue for memory requests failed\n");
//     }
//     else
//     {
//         printf("create_mpmc_queue for memory requests succeeded\n");
//     }
//     return dpm_mem_req_queue != nullptr;
// }

/* bool dpm_poll_mem_req()
{
    struct dpm_mem_req *req;
    size_t recvd_size;
    unsigned int priority;
    shm_ptr req_ptr = 0;
    if (dpm_mem_req_queue->try_pop(req_ptr))
    {
        print_debug("received req_ptr: %lu\n", req_ptr);
        // convert shm ptr to local ptr
        req = dpm_get_mem_req_ptr_from_shmptr(req_ptr);
        print_debug("req->type: %d\n", req->type);
        if (req->type == DPM_MEM_REQ_ALLOC_INPUT)
        {
            char *buf = allocate_input_buf(req->size);
            req->buf = dpm_get_shm_ptr_for_input_buf(buf);
            print_debug("allocated input buf: %lu\n", req->buf);
            req->completion.store(true, std::memory_order_release);
        }
        else if (req->type == DPM_MEM_REQ_ALLOC_OUTPUT)
        {
            char *buf = allocate_output_buf(req->size);
            req->buf = dpm_get_shm_ptr_for_output_buf(buf);
            print_debug("allocated output buf: %lu\n", req->buf);
            req->completion.store(true, std::memory_order_release);
        }
        else if (req->type == DPM_MEM_REQ_FREE_INPUT)
        {
            mi_free(dpm_get_input_ptr_from_shmptr(req->buf));
            req->completion.store(true);
        }
        else if (req->type == DPM_MEM_REQ_FREE_OUTPUT)
        {
            mi_free(dpm_get_output_ptr_from_shmptr(req->buf));
            req->completion.store(true);
        }
        else
        {
            printf("unknown req->type: %d\n", req->type);
        }
        return true;
    }
    return false;
}
 */
