#include "dpm_interface.hpp"
#include "common.hpp"
#include "mpmc_queue.hpp"
#include "ring_buffer.hpp"
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>
using namespace DDS_FrontEnd;
#include <iostream>
#include <mutex> // Add mutex include

// boost message queue for submission
// boost::interprocess::message_queue *submission_queue;
/// MPMC queue for submission and completion
/* using rigtorp::MPMCQueue;
MPMCQueue<struct dpm_req_msg, DPM_SUBMISSION_QUEUE_SIZE> *submission_queue_mpmc; */

struct RequestRingBufferProgressive *submission_queue_ring_buffer;
// struct RequestRingBufferProgressive *submission_queue_ring_buffer2;
std::vector<struct RequestRingBufferProgressive *> submission_queue_ring_buffers;

// boost::interprocess::message_queue *completion_queue;

// boost::interprocess::message_queue *mem_req_queue;
// rigtorp::MPMCQueue<shm_ptr, DPM_MEM_REQ_QUEUE_SIZE> *mem_req_queue;

struct dpm_shared_mem app_req_ctx_shm;
std::mutex heap_alloc_mutex; // Add mutex definition

struct dpm_io_mem_region app_own_mem_region;

/* bool dpm_setup_deprecated()
{
    int fd = shm_open(SHM_NAME_SUBMISSION_COMPLETION, O_RDWR, 0666);
    if (fd < 0)
    {
        perror("shm_open dpm_setup");
        return false;
    }

    shm_user = (dpm_submission_completion *)mmap(NULL, sizeof(dpm_submission_completion), PROT_READ | PROT_WRITE,
                                                 MAP_SHARED, fd, 0);
    if (shm_user == MAP_FAILED)
    {
        perror("mmap dpm_setup");
        close(fd);
        return false;
    }

    return true;
} */

/* bool dpm_submit_task(dpkernel_task_base *submission)
{
    auto &sub = shm_user->submission;

    sub.mutex.lock();

    // TODO: this is still copying
    sub.task = *submission;

    sub.mutex.unlock();
    return true;
} */

/* bool dpm_submit_task_msgq_old(dpkernel_task_base *submission)
{
    return submission_queue->try_send(submission, sizeof(dpkernel_task_base), 0);
} */

/* bool dpm_submit_task_msgq(dpkernel_task_base *submission)
{
    return dpm_submit_task_msgq(submission, dpm_device::DEVICE_NONE);
} */

bool dpm_submit_task_msgq(dpkernel_task_base *submission, dpm_device device = dpm_device::DEVICE_NONE)
{
    submission->device = device;
    // convert to shm_ptr
    shm_ptr task_ptr = get_shm_ptr_for_task(submission);
    submission->completion.store(DPK_ONGOING);
    struct dpm_req_msg msg{DPM_REQ_TYPE_TASK, task_ptr};
    ////return submission_queue->try_send(&task_ptr, sizeof(task_ptr), 0);
    // return submission_queue_mpmc->try_push(msg);
    return InsertToRequestBufferProgressive(submission_queue_ring_buffer, (char *)&msg, sizeof(msg));
}

#define BACKOFF
#undef BACKOFF
#ifdef BACKOFF
#include <emmintrin.h>

int backoff_delay = 32;
int thread_backoff_scale = 8; // attempt to make each thread wait a different amount of time
const int max_backoff = 4096;
#endif
uint32_t ring_buffer_num = 0;
void dpm_submit_task_msgq_blocking(dpkernel_task_base *submission, dpm_device device = dpm_device::DEVICE_NONE)
{
    submission->device = device;
    // convert to shm_ptr
    shm_ptr task_ptr = get_shm_ptr_for_task(submission);
    submission->completion.store(DPK_ONGOING);
    struct dpm_req_msg msg{DPM_REQ_TYPE_TASK, task_ptr};
    // return submission_queue->send(&task_ptr, sizeof(task_ptr), 0);
    // return submission_queue_mpmc->push(msg);

#ifdef BACKOFF
    while (!InsertToRequestBufferProgressive(submission_queue_ring_buffer, (char *)&msg, sizeof(msg)))
    {
        // exponential backoff
        /* for (int i = 0; i < thread_id * thread_backoff_scale + backoff_delay; i++)
        {
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
#elif defined(__arm__) || defined(__aarch64__)
            __asm volatile("yield");
#else
            std::this_thread::yield();
#endif
        }
        backoff_delay *= 2;
        if (backoff_delay > max_backoff)
            backoff_delay = max_backoff; */
        std::this_thread::sleep_for(std::chrono::nanoseconds(1));
        // std::this_thread::yield();
    }
#else
    auto ring_buf = submission_queue_ring_buffers[ring_buffer_num % RING_BUFFER_NUMBER];
    ring_buffer_num++;
    while (!InsertToRequestBufferProgressive(ring_buf, &msg))
        ;
    // while (!InsertToRequestBufferProgressive(ring_buf, (char *)&msg, sizeof(msg)))
    //     ;
    // while (!InsertToRequestBufferProgressive(submission_queue_ring_buffer, (char *)&msg, sizeof(msg)))
    //     ;
#endif
}

void dpm_submit_task_msgq_blocking(int thread_id, dpkernel_task_base *submission,
                                   dpm_device device = dpm_device::DEVICE_NONE)
{
    submission->device = device;
    // convert to shm_ptr
    shm_ptr task_ptr = get_shm_ptr_for_task(submission);
    submission->completion.store(DPK_ONGOING);
    struct dpm_req_msg msg{DPM_REQ_TYPE_TASK, task_ptr};
    // return submission_queue->send(&task_ptr, sizeof(task_ptr), 0);
    // return submission_queue_mpmc->push(msg);

#ifdef BACKOFF
    while (!InsertToRequestBufferProgressive(submission_queue_ring_buffer, (char *)&msg, sizeof(msg)))
    {
        // exponential backoff
        /* for (int i = 0; i < thread_id * thread_backoff_scale + backoff_delay; i++)
        {
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
#elif defined(__arm__) || defined(__aarch64__)
            __asm volatile("yield");
#else
            std::this_thread::yield();
#endif
        }
        backoff_delay *= 2;
        if (backoff_delay > max_backoff)
            backoff_delay = max_backoff; */
        std::this_thread::sleep_for(std::chrono::nanoseconds(1));
        // std::this_thread::yield();
    }
#else
    auto ring_buf = submission_queue_ring_buffers[thread_id % RING_BUFFER_NUMBER];
    while (!InsertToRequestBufferProgressive(thread_id, ring_buf, (char *)&msg, sizeof(msg)))
        ;
#endif
}

/* bool consume_completion()
{
    auto &shm_comp = submission_completion_shm->completion;

    shm_comp.mutex.lock();
    if (shm_comp.completion.error.load() == DPK_NONE)
        // nothing to consume, shouldn't have called this
        return false;
    shm_comp.completion.error.store(DPK_NONE);
    shm_comp.mutex.unlock();
    return true;
} */

bool dpm_initialize()
{
    // setup shared memory regions
    _setup_shared_memory_regions(&app_own_mem_region, &app_req_ctx_shm);
    // if (!_setup_shm_region_mem_req_ctx())
    // {
    //     printf("setup_shm_region_mem_req_ctx failed\n");
    //     return false;
    // }

    // setup mimalloc options
    if (!_setup_mi_options())
    {
        printf("setup_mi_options failed\n");
        return false;
    }

    if (!mi_manage_os_memory_ex(app_req_ctx_shm.shm_ptr, app_req_ctx_shm.shm_size, true, false, false, -1, true,
                                &app_req_ctx_shm.allocator.arena_id))
    {
        printf("mi_manage_os_memory_ex failed for input region\n");
        return false;
    }
    app_req_ctx_shm.allocator.heap = mi_heap_new_in_arena(app_req_ctx_shm.allocator.arena_id);
    if (app_req_ctx_shm.allocator.heap == NULL)
    {
        printf("mi_heap_new_in_arena failed for mem req ctx region\n");
        return false;
    }
    /* if (!_setup_shm_region_exchange_queue_app())
    {
        printf("setup_shm_region_exchange_queue_app failed\n");
        return false;
    } */

    // assuming DPM is already up and waiting for app to exchange shared mem region info
    // dpm_mem_region_info_exchange_deprecated(&dpm_mem_region, &exchange);
    // _exchange_mem_info();

    if (!_dpm_setup_msgq())
    {
        printf("dpm_setup_msgq failed\n");
        return false;
    }
    return true;
}

bool _dpm_setup_msgq()
{
    // prepare the boost queues
    // boost::interprocess::message_queue::remove(SUBMISSION_QUEUE_NAME);
    // boost::interprocess::message_queue::remove(COMPLETION_QUEUE_NAME);
    ////mmap the shared memory for submission_queue_mpmc
    // Create shared memory object

    ////////////////
    // int shm_fd = shm_open(SUBMISSION_QUEUE_NAME, O_CREAT | O_RDWR, 0666);
    // if (shm_fd == -1)
    // {
    //     perror("shm_open failed");
    //     return 1;
    // }

    // // Define the size of shared memory object
    // size_t shm_size = sizeof(rigtorp::MPMCQueue<shm_ptr>);

    // // Map shared memory object
    // void *ptr = mmap(0, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    // if (ptr == MAP_FAILED)
    // {
    //     perror("mmap failed");
    //     return 1;
    // }
    // std::cout << "Queue sizeof: " << sizeof(rigtorp::MPMCQueue<shm_ptr>) << std::endl;
    // std::cout << "shm_size: " << shm_size << std::endl;

    // // cast the ptr to the queue
    // rigtorp::MPMCQueue<shm_ptr> *queue = (rigtorp::MPMCQueue<shm_ptr> *)ptr;
    // // change/remap the slots_ ptr
    // queue->setup_from_shared_mem(DPM_SUBMISSION_QUEUE_SIZE, SUBMISSION_QUEUE_BACKING_SHM_NAME);
    // submission_queue_mpmc = queue;
    // BOOST_TRY
    // {
    //     /* submission_queue =
    //         new boost::interprocess::message_queue(boost::interprocess::open_only, SUBMISSION_QUEUE_NAME);
    //     completion_queue =
    //         new boost::interprocess::message_queue(boost::interprocess::open_only, COMPLETION_QUEUE_NAME); */
    //     mem_req_queue = new boost::interprocess::message_queue(boost::interprocess::open_only,
    //     DPM_MEM_REQ_QUEUE_NAME);
    // }
    // BOOST_CATCH(boost::interprocess::interprocess_exception & ex)
    // {
    //     std::cerr << "boost msg queue ex: " << ex.what() << std::endl;
    //     return false;
    // }
    // BOOST_CATCH_END;
    ////////////////

    // setup task submission queue
    /* submission_queue_mpmc =
        rigtorp::connect_mpmc_queue<struct dpm_req_msg, DPM_SUBMISSION_QUEUE_SIZE>(SUBMISSION_QUEUE_NAME);
    if (submission_queue_mpmc == nullptr)
    {
        printf("connect_mpmc_queue for submission failed\n");
        return false;
    }
    else
    {
        printf("connect_mpmc_queue for submission succeeded\n");
    } */

    // setup the ring buffer for submission
    /* submission_queue_ring_buffer = SetupRequestBufferProgressive(RING_BUFFER_SHM_NAME);
    if (submission_queue_ring_buffer == nullptr)
    {
        printf("setup submission queue ring buffer failed\n");
        return false;
    }

    submission_queue_ring_buffer2 = SetupRequestBufferProgressive(RING_BUFFER_SHM_NAME2);
    if (submission_queue_ring_buffer2 == nullptr)
    {
        printf("setup submission queue ring buffer2 failed\n");
        return false;
    } */
    for (int i = 0; i < RING_BUFFER_NUMBER; i++)
    {
        std::string ring_buffer_name = RING_BUFFER_SHM_NAME + std::to_string(i);
        struct RequestRingBufferProgressive *ring_buffer = SetupRequestBufferProgressive(ring_buffer_name.c_str());
        if (ring_buffer == nullptr)
        {
            printf("setup submission queue ring buffer failed\n");
            return false;
        }
        submission_queue_ring_buffers.push_back(ring_buffer);
    }

    // setup the mem req queue
    // mem_req_queue = rigtorp::connect_mpmc_queue<shm_ptr, DPM_MEM_REQ_QUEUE_SIZE>(DPM_MEM_REQ_QUEUE_NAME);
    // if (mem_req_queue == nullptr)
    // {
    //     printf("connect_mpmc_queue for memory requests failed\n");
    //     return false;
    // }
    // else
    // {
    //     printf("connect_mpmc_queue for memory requests succeeded\n");
    // }

    /* printf("submission queue get_max_msg_size: %lu\n", submission_queue->get_max_msg_size());
    printf("get_max_msg_size: %lu\n", submission_queue->get_max_msg_size()); */

    return true;
}

bool dpm_teardown_msgq()
{
    bool ret;
    // ret = boost::interprocess::message_queue::remove(DPM_MEM_REQ_QUEUE_NAME);
    // ret = boost::interprocess::message_queue::remove(SUBMISSION_QUEUE_NAME);
    // ret &= boost::interprocess::message_queue::remove(COMPLETION_QUEUE_NAME);
    return ret;
}

/* bool dpm_try_get_completion(dpkernel_completion *completion)
{
    bool ret = dpm_poll_completion();
    if (ret)
    {
        // copy into the pointed struct
        auto &shm_comp = shm_user->completion;
        shm_comp.mutex.lock();

        completion->user_data = shm_comp.completion.user_data;
        completion->out = shm_comp.completion.out;
        completion->out_size = shm_comp.completion.out_size;
        completion->error.store(shm_comp.completion.error.load());

        // consume the completion, DPK_NONE means not in use
        shm_comp.completion.error.store(DPK_NONE);

        shm_comp.mutex.unlock();
        return true;
    }
    return false;
}
 */

/// @deprecated
/* bool dpm_try_get_completion_msgq(dpkernel_completion *completion)
{
    ulong recvd_size = sizeof(dpkernel_completion);
    uint priority = 0;
    bool ret = completion_queue->try_receive(completion, sizeof(dpkernel_completion), recvd_size, priority);
    if (ret && (recvd_size != sizeof(dpkernel_completion) || priority != 0))
    {
        // TODO: unexpected priority, investigate what's going on
        printf("dpm_try_get_completion_msgq: recvd_size: %lu, priority: %u\n", recvd_size, priority);
        // return false;
    }
    return ret;
} */

/* bool _setup_shm_region_exchange_queue_app()
{
    app_shm_region_exchange_queue =
        new boost::interprocess::message_queue(boost::interprocess::open_only, DPM_MEM_REGION_EXCHANGE_QUEUE_NAME);
    printf("app_shm_region_exchange_queue max_msg_size: %lu\n", app_shm_region_exchange_queue->get_max_msg_size());
    return true;
} */

// struct dpm_shared_mem app_req_ctx_shm = {};

/* bool _setup_shm_region_mem_req_ctx()
{
    bool ret = _setup_shared_memory(DPM_REQ_CTX_SHM_SIZE, &app_req_ctx_shm, DPM_REQ_CTX_SHM_NAME);
    if (!ret)
    {
        printf("_setup_shared_memory app_req_ctx_shm failed\n");
        return false;
    }

    // setup mimalloc options
    if (!_setup_mi_options())
    {
        printf("setup_mi_options failed\n");
        return false;
    }

    if (!mi_manage_os_memory_ex(app_req_ctx_shm.shm_ptr, app_req_ctx_shm.shm_size, true, false, false, -1, true,
                                &app_req_ctx_shm.allocator.arena_id))
    {
        printf("mi_manage_os_memory_ex failed for input region\n");
        return false;
    }
    app_req_ctx_shm.allocator.heap = mi_heap_new_in_arena(app_req_ctx_shm.allocator.arena_id);
    if (app_req_ctx_shm.allocator.heap == NULL)
    {
        printf("mi_heap_new_in_arena failed for mem req ctx region\n");
        return false;
    }

    return true;
} */

/* void _app_send_then_recv_mem_region_info()
{
    ulong recvd_size;
    uint priority;

    // send its own buf mem region info
    printf("app_own_mem_region.input_region.shm_ptr: %p\n", app_own_mem_region.input_region.shm_ptr);
    app_shm_region_exchange_queue->send(&app_own_mem_region, sizeof(app_own_mem_region), 0);
    printf("sent app's buf memory region info\n");

    // send the req_ctx_shm info
    printf("app_req_ctx_shm.shm_ptr: %p\n", app_req_ctx_shm.shm_ptr);
    app_shm_region_exchange_queue->send(&app_req_ctx_shm, sizeof(app_req_ctx_shm), 0);
    printf("sent app_req_ctx_shm region info\n");

    printf("receiving dpm's memory region info\n");
    app_shm_region_exchange_queue->receive(&dpm_app_mem_region, sizeof(dpm_app_mem_region), recvd_size, priority);
    printf("dpm_app_mem_region.input_region.shm_ptr: %p\n", dpm_app_mem_region.input_region.shm_ptr);

    // receives the DPM's buf mem region info
    printf("receiving dpm's memory region info\n");
    app_shm_region_exchange_queue->receive(&dpm_req_ctx_shm, sizeof(dpm_req_ctx_shm), recvd_size, priority);
    printf("dpm_req_ctx_shm.shm_ptr: %p\n", dpm_req_ctx_shm.shm_ptr);
} */

/* void _exchange_mem_info()
{
    _app_send_then_recv_mem_region_info();
} */

bool _dpm_alloc_shared_mem(size_t size, shm_ptr *buf, enum dpm_mem_req_type type)
{
    struct dpm_mem_req *req;
    {
        std::lock_guard<std::mutex> lock(heap_alloc_mutex);
        req = (struct dpm_mem_req *)mi_heap_malloc(app_req_ctx_shm.allocator.heap, sizeof(struct dpm_mem_req));
    }

    if (req == NULL)
    {
        printf("mi_heap_malloc req failed\n");
        return false;
    }

    req->size = size;
    req->buf = NULL;
    req->type = type;
    req->completion.store(DPK_ONGOING);

    // convert the buf ptr to an offset from the start of the shared memory region
    shm_ptr req_ptr = get_shm_ptr_for_mem_req_ctx(req);
    // print_debug("alloc req_ptr: %lu\n", req_ptr);
    // mem_req_queue->send(&req_ptr, sizeof(req_ptr), 0);
    // mem_req_queue->push(req_ptr);
    struct dpm_req_msg msg{DPM_REQ_TYPE_MEM, req_ptr};
    // submission_queue_mpmc->try_push(msg);
    while (!InsertToRequestBufferProgressive(submission_queue_ring_buffer, (char *)&msg, sizeof(msg)))
        ;
    print_debug("sent alloc req\n");

    print_debug("waiting for mem req to be done\n");
    // wait for the request to be done
    while (req->completion.load(std::memory_order_acquire) == DPK_ONGOING)
        ;
    print_debug("mem req done\n");
    // once request is done, the buf ptr will be filled in
    if (type == DPM_MEM_REQ_ALLOC_INPUT)
        *buf = req->buf;
    else if (type == DPM_MEM_REQ_ALLOC_OUTPUT)
        *buf = req->buf;
    else
    {
        printf("unknown req->type: %d\n", req->type);
        // return false;
        throw std::runtime_error("unknown req->type: " + std::to_string(req->type));
    }

    print_debug("got buf shm_ptr: %lu\n", *buf);
    // free the request struct
    mi_free(req);
    return true;
}

bool dpm_alloc_input_buf(size_t size, shm_ptr *buf)
{
    return _dpm_alloc_shared_mem(size, buf, DPM_MEM_REQ_ALLOC_INPUT);
}

bool dpm_alloc_output_buf(size_t size, shm_ptr *buf)
{
    return _dpm_alloc_shared_mem(size, buf, DPM_MEM_REQ_ALLOC_OUTPUT);
}

bool app_alloc_task_request(dpkernel_task_base **task)
{
    {
        std::lock_guard<std::mutex> lock(heap_alloc_mutex);
        *task = (dpkernel_task_base *)mi_heap_malloc(app_req_ctx_shm.allocator.heap, sizeof(dpkernel_task_base));
    }
    if (task == NULL)
    {
        printf("mi_heap_malloc task failed\n");
        return false;
    }
    return true;
}

void app_free_task_request(dpkernel_task_base *task)
{
    mi_free(task);
}

bool _dpm_free_shared_mem(char *buf, bool is_input)
{
    // send the request thru the message queue
    struct dpm_mem_req *req;
    {
        std::lock_guard<std::mutex> lock(heap_alloc_mutex);
        req = (struct dpm_mem_req *)mi_heap_malloc(app_req_ctx_shm.allocator.heap, sizeof(struct dpm_mem_req));
    }
    if (req == NULL)
    {
        printf("mi_heap_malloc req allocation failed\n");
        return false;
    }
    req->completion.store(DPK_ONGOING);

    if (is_input)
    {
        req->type = DPM_MEM_REQ_FREE_INPUT;
        req->buf = get_shm_ptr_for_input_buf(buf);
        }
        else
        {
            req->type = DPM_MEM_REQ_FREE_OUTPUT;
            req->buf = get_shm_ptr_for_output_buf(buf);
        }
        // req->done.store(false, std::memory_order_release);

        // convert the buf ptr to an offset from the start of the shared memory region
        shm_ptr req_ptr = get_shm_ptr_for_mem_req_ctx(req);
        print_debug("free req_ptr: %lu\n", req_ptr);
        // mem_req_queue->send(&req_ptr, sizeof(req_ptr), 0);
        // mem_req_queue->push(req_ptr);
        struct dpm_req_msg msg{DPM_REQ_TYPE_MEM, req_ptr};
        // submission_queue_mpmc->try_push(msg);
        while (!InsertToRequestBufferProgressive(submission_queue_ring_buffer, (char *)&msg, sizeof(msg)))
            ;
        print_debug("sent free req\n");

        // wait for the request to be done
        print_debug("waiting for mem req to be done\n");
        while (req->completion.load(std::memory_order_acquire) == DPK_ONGOING)
            ;
        print_debug("mem free req done\n");
        // free the request
        mi_free(req);
        return true;
}

bool dpm_free_input_buf(char *buf)
{
    return _dpm_free_shared_mem(buf, true);
}

bool dpm_free_output_buf(char *buf)
{
    return _dpm_free_shared_mem(buf, false);
}

// bool dpm_mem_region_info_exchange_deprecated(struct dpm_io_mem_region *app_mem_region,
//                                              dpm_mem_region_exchange *exchange)
// {
//     int fd = shm_open(DPM_MEM_REGION_EXCHANGE_NAME, O_RDWR, 0666);
//     if (fd < 0)
//     {
//         perror("shm_open dpm_mem_region_info_exchange");
//         return false;
//     }

//     exchange = (dpm_mem_region_exchange *)mmap(NULL, sizeof(dpm_mem_region_exchange), PROT_READ | PROT_WRITE,
//                                                MAP_SHARED, fd, 0);
//     if (exchange == MAP_FAILED)
//     {
//         perror("mmap dpm_mem_region_info_exchange");
//         close(fd);
//         return false;
//     }

//     // don't need to know about the DPM's shared mem region
//     // wait for DPM to put its info
//     while (exchange->dpm_done.load() == 0)
//         ;

//     // read the DPM's info
//     /* exchange->app_start.store(1);
//     memcpy(&dpm_mem_region, &exchange->dpm_mem_region, sizeof(struct dpm_io_mem_region));
//     exchange->app_done.store(1);
//     printf("dpm_mem_region.input_region.shm_ptr: %p\n", dpm_mem_region.input_region.shm_ptr);
//     printf("dpm_mem_region.input_region.shm_size: %lu\n", dpm_mem_region.input_region.shm_size); */

//     // put its own info in the shared memory
//     exchange->app_start.store(1);
//     memcpy(&exchange->app_mem_region, app_mem_region, sizeof(struct dpm_io_mem_region));
//     exchange->app_done.store(1);
//     printf("app_mem_region.input_region.shm_ptr: %p\n", app_mem_region->input_region.shm_ptr);
//     printf("app_mem_region.output_region.shm_size: %lu\n", app_mem_region->output_region.shm_size);

//     close(fd);
//     return true;
// }
