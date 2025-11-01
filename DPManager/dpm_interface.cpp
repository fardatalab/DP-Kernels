#include "dpm_interface.hpp"
#include "common.hpp"
#include "mpmc_queue.hpp"
#include "ring_buffer.hpp"
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>
using namespace DDS_FrontEnd;
#include "memory.hpp"
#include <iostream>
#include <mutex>

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
// std::mutex heap_alloc_mutex;
// pthread_spinlock_t heap_alloc_spinlock;

struct dpm_io_mem_region app_own_mem_region;

// thread_local bool this_thread_has_heap = false;
thread_local thread_local_mi_heap this_thread_heap;

// NOTE: submission_queue_ring_buffer is uninitialized!!!
/* bool dpm_submit_task_msgq(dpkernel_task_base *submission, dpm_device device = dpm_device::DEVICE_NONE)
{
    submission->device = device;
    // convert to shm_ptr
    shm_ptr task_ptr = get_shm_ptr_for_task(submission);
    submission->completion.store(DPK_ONGOING);
    struct dpm_req_msg msg{DPM_REQ_TYPE_TASK, task_ptr};
    ////return submission_queue->try_send(&task_ptr, sizeof(task_ptr), 0);
    // return submission_queue_mpmc->try_push(msg);
    return InsertToRequestBufferProgressive(submission_queue_ring_buffer, (char *)&msg, sizeof(msg));
} */

#define BACKOFF
#undef BACKOFF
#ifdef BACKOFF
#include <emmintrin.h>

int backoff_delay = 32;
int thread_backoff_scale = 8; // attempt to make each thread wait a different amount of time
const int max_backoff = 4096;
#endif
static std::atomic<uint32_t> ring_buffer_num = 0;
void dpm_submit_task_msgq_blocking(dpkernel_task *task, dpm_device device = dpm_device::DEVICE_NONE)
{
    // everything extends from dpkernel_task_base
    dpkernel_task_base *submission = &task->base;

    submission->device = device;
    // convert to shm_ptr
    shm_ptr task_ptr = get_shm_ptr_for_task(task);
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
    ring_buffer_num.fetch_add(1, std::memory_order_relaxed);
    while (!InsertToRequestBufferProgressive(ring_buf, (char *)&msg, sizeof(msg)))
        ;
    // while (!InsertToRequestBufferProgressive(ring_buf, (char *)&msg, sizeof(msg)))
    //     ;
    // while (!InsertToRequestBufferProgressive(submission_queue_ring_buffer, (char *)&msg, sizeof(msg)))
    //     ;
#endif
}

void dpm_submit_task_msgq_blocking(int thread_id, dpkernel_task *task, dpm_device device = dpm_device::DEVICE_NONE)
{
    // everything extends from dpkernel_task_base
    dpkernel_task_base *submission = &task->base;
    submission->device = device;
    // convert to shm_ptr
    shm_ptr task_ptr = get_shm_ptr_for_task(task);
    submission->completion.store(DPK_ONGOING);
    struct dpm_req_msg msg{DPM_REQ_TYPE_TASK, task_ptr};
    // return submission_queue->send(&task_ptr, sizeof(task_ptr), 0);
    // return submission_queue_mpmc->push(msg);

    auto ring_buf = submission_queue_ring_buffers[thread_id % RING_BUFFER_NUMBER];
    // while (!InsertToRequestBufferProgressive(thread_id, ring_buf, (char *)&msg, sizeof(msg)))
    while (!InsertToRequestBufferProgressive(ring_buf, (char *)&msg, sizeof(msg)))
        ;
}

std::atomic<uint> _dpm_thread_id = 0;
void dpm_submit_task_msgq_multithread(dpkernel_task *task, dpm_device device)
{
    // everything extends from dpkernel_task_base
    dpkernel_task_base *submission = &task->base;
    submission->device = device;
    // convert to shm_ptr
    shm_ptr task_ptr = get_shm_ptr_for_task(task);
    submission->completion.store(DPK_ONGOING);
    struct dpm_req_msg msg{DPM_REQ_TYPE_TASK, task_ptr};

    auto ring_buf = submission_queue_ring_buffers[_dpm_thread_id.load(std::memory_order_acquire) % RING_BUFFER_NUMBER];
    _dpm_thread_id.fetch_add(1, std::memory_order_release);
    while (!InsertToRequestBufferProgressive(ring_buf, (char *)&msg, sizeof(msg)))
        ;
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

bool dpm_frontend_initialize()
{
    // pthread_spin_init(&heap_alloc_spinlock, PTHREAD_PROCESS_PRIVATE);
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
                                &app_req_ctx_shm.arena_id))
    {
        printf("mi_manage_os_memory_ex failed for input region\n");
        return false;
    }

    // app_req_ctx_shm.allocator.heap = mi_heap_new_in_arena(app_req_ctx_shm.allocator.arena_id);
    // if (app_req_ctx_shm.allocator.heap == NULL)
    // {
    //     printf("mi_heap_new_in_arena failed for mem req ctx region\n");
    //     return false;
    // }

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

struct dpm_mem_req *_dpm_submit_shared_mem_req_async(dpkernel_task *task, enum dpm_mem_req_type type)
{
    struct dpm_mem_req *req;
    if (this_thread_heap.heap == nullptr)
    {
        // this thread has not initialized the heap, so we need to do it
        auto heap = mi_heap_new_in_arena(app_req_ctx_shm.arena_id);
        if (heap == NULL)
        {
            printf("mi_heap_new_in_arena failed for input regionm thread id = %lu\n", pthread_self());
            return nullptr;
        }
        this_thread_heap.heap = heap;
    }
    {
        // std::lock_guard<std::mutex> lock(heap_alloc_mutex);
        // pthread_spin_lock(&heap_alloc_spinlock);
        // req = (struct dpm_mem_req *)mi_heap_malloc(app_req_ctx_shm.allocator.heap, sizeof(struct dpm_mem_req));
        req = (struct dpm_mem_req *)mi_heap_malloc(this_thread_heap.heap, sizeof(struct dpm_mem_req));
        // pthread_spin_unlock(&heap_alloc_spinlock);
    }

    if (req == NULL)
    {
        printf("mi_heap_malloc req failed\n");
        return NULL;
    }

    req->type = type;
    req->task = get_shm_ptr_for_task(task);
    req->completion.store(DPK_ONGOING);

    // convert the buf ptr to an offset from the start of the shared memory region
    shm_ptr req_ptr = get_shm_ptr_for_mem_req_ctx(req);
    // print_debug("alloc req_ptr: %lu\n", req_ptr);
    // mem_req_queue->send(&req_ptr, sizeof(req_ptr), 0);
    // mem_req_queue->push(req_ptr);
    struct dpm_req_msg msg{DPM_REQ_TYPE_MEM, req_ptr};
    // submission_queue_mpmc->try_push(msg);

    // default use the first ring buffer
    // auto insert_start = std::chrono::high_resolution_clock::now();
    while (!InsertToRequestBufferProgressive(submission_queue_ring_buffers[0], (char *)&msg, sizeof(msg)))
    {
        /* int x = 0;
#if defined(__x86_64__) || defined(_M_X64)
        while (x < 50)
#elif defined(__arm__) || defined(__aarch64__)
        while (x < 50000)
#else
        while (x < 100000)
#endif
        {
            // std::this_thread::yield();
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
#elif defined(__arm__) || defined(__aarch64__)
            __asm volatile("yield");
#else
            std::this_thread::yield();
#endif
            x++;
        }
        x++; */
    }
    print_debug("sent alloc req\n");
    // auto insert_end = std::chrono::high_resolution_clock::now();
    // auto insert_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_end - insert_start);
    // printf("insert duration: %lu ns\n", insert_duration.count());

    print_debug("waiting for mem req to be done\n");
    // auto wait_start = std::chrono::high_resolution_clock::now();
    // DON'T wait for the request to be done
    return req;
}

bool _dpm_alloc_shared_mem(dpkernel_task *task, enum dpm_mem_req_type type)
{
    // struct dpm_mem_req *req;
    // {
    //     // std::lock_guard<std::mutex> lock(heap_alloc_mutex);
    //     pthread_spin_lock(&heap_alloc_spinlock);
    //     req = (struct dpm_mem_req *)mi_heap_malloc(app_req_ctx_shm.allocator.heap, sizeof(struct dpm_mem_req));
    //     pthread_spin_unlock(&heap_alloc_spinlock);
    // }

    // if (req == NULL)
    // {
    //     printf("mi_heap_malloc req failed\n");
    //     return false;
    // }

    // // no longer needed, the task struct will have the size
    // /* req->size = size;
    // req->buf = NULL; */

    // req->type = type;
    // req->task = get_shm_ptr_for_task(task);
    // req->completion.store(DPK_ONGOING);

    // // convert the buf ptr to an offset from the start of the shared memory region
    // shm_ptr req_ptr = get_shm_ptr_for_mem_req_ctx(req);
    // // print_debug("alloc req_ptr: %lu\n", req_ptr);
    // // mem_req_queue->send(&req_ptr, sizeof(req_ptr), 0);
    // // mem_req_queue->push(req_ptr);
    // struct dpm_req_msg msg{DPM_REQ_TYPE_MEM, req_ptr};
    // // submission_queue_mpmc->try_push(msg);

    // // default use the first ring buffer
    // auto insert_start = std::chrono::high_resolution_clock::now();
    // while (!InsertToRequestBufferProgressive(submission_queue_ring_buffers[0], (char *)&msg, sizeof(msg)))
    // {
    //     for (int i = 0; i < 10000; i++)
    //     {
    //         // std::this_thread::sleep_for(std::chrono::nanoseconds(1));
    //         std::this_thread::yield();
    //     }
    // }
    // print_debug("sent alloc req\n");
    // auto insert_end = std::chrono::high_resolution_clock::now();
    // auto insert_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_end - insert_start);
    // // printf("insert duration: %lu ns\n", insert_duration.count());

    struct dpm_mem_req *req = _dpm_submit_shared_mem_req_async(task, type);
    if (req == NULL)
    {
        printf("mi_heap_malloc req failed\n");
        return false;
    }

    print_debug("waiting for mem req to be done\n");
    // auto wait_start = std::chrono::high_resolution_clock::now();
    // wait for the request to be done
    while (req->completion.load(std::memory_order_acquire) == DPK_ONGOING)
    {
        // maybe yield will help?
        for (int i = 0; i < 10; i++)
        {
            std::this_thread::yield();
        }
    }
    // auto wait_end = std::chrono::high_resolution_clock::now();
    // auto wait_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(wait_end - wait_start);
    // printf("wait duration: %lu ns\n", wait_duration.count());
    print_debug("mem req done\n");
    // once request is done, the buf ptr will be filled in by DPM in `task` struct already
    // this is no longer needed
    /* if (type == DPM_MEM_REQ_ALLOC_INPUT)
        task->in = req->buf;
    else if (type == DPM_MEM_REQ_ALLOC_OUTPUT)
        task->out = req->buf;
    else
    {
        printf("unknown req->type: %d\n", req->type);
        // return false;
        throw std::runtime_error("unknown req->type: " + std::to_string(req->type));
    } */

    print_debug("got buf shm_ptr: %lu\n", *buf);
    // free the request struct
    mi_free(req);
    return true;
}

bool dpm_alloc_input_buf(uint32_t size, dpkernel_task *task)
{
    task->base.in_size = size;
    return _dpm_alloc_shared_mem(task, DPM_MEM_REQ_ALLOC_INPUT);
}

struct dpm_mem_req *dpm_alloc_input_buf_async(uint32_t size, dpkernel_task *task)
{
    task->base.in_size = size;
    return _dpm_submit_shared_mem_req_async(task, DPM_MEM_REQ_ALLOC_INPUT);
}

struct dpm_mem_req *dpm_alloc_output_buf_async(uint32_t size, dpkernel_task *task)
{
    task->base.out_size = size;
    return _dpm_submit_shared_mem_req_async(task, DPM_MEM_REQ_ALLOC_OUTPUT);
}

bool dpm_wait_for_mem_req_completion(struct dpm_mem_req *req)
{
    if (req == NULL)
    {
        printf("mi_heap_malloc req failed, waiting for NULL req completion\n");
        return false;
    }
    // wait for the request to be done
    print_debug("waiting for mem req to be done\n");
    while (req->completion.load(std::memory_order_acquire) == DPK_ONGOING)
    {
        /* int x = 0;
#if defined(__x86_64__) || defined(_M_X64)
        while (x < 50)
#elif defined(__arm__) || defined(__aarch64__)
        while (x < 50000)
#else
        while (x < 100000)
#endif
        {
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
#elif defined(__arm__) || defined(__aarch64__)
            __asm volatile("yield");
#else
            std::this_thread::yield();
#endif
            x++; */
    }
    if (req->completion.load(std::memory_order_acquire) == DPK_ERROR_FAILED)
    {
        printf("dpm frontend got mem req failed\n");
        mi_free(req);
        return false;
    }

    // free the request
    mi_free(req);

    return true;

    print_debug("mem req done\n");
}

bool dpm_alloc_output_buf(uint32_t size, dpkernel_task *task)
{
    task->base.out_size = size;
    return _dpm_alloc_shared_mem(task, DPM_MEM_REQ_ALLOC_OUTPUT);
}

bool app_alloc_task_request(dpkernel_task **task)
{
    if (this_thread_heap.heap == nullptr)
    {
        // this thread has not initialized the heap, so we need to do it
        auto heap = mi_heap_new_in_arena(app_req_ctx_shm.arena_id);
        if (heap == NULL)
        {
            printf("mi_heap_new_in_arena failed for input regionm thread id = %lu\n", pthread_self());
            return false;
        }
        this_thread_heap.heap = heap;
    }
    {
        // std::lock_guard<std::mutex> lock(heap_alloc_mutex);
        // pthread_spin_lock(&heap_alloc_spinlock);
        // *task = (dpkernel_task *)mi_heap_malloc(app_req_ctx_shm.allocator.heap, sizeof(dpkernel_task));
        *task = (dpkernel_task *)mi_heap_malloc(this_thread_heap.heap, sizeof(dpkernel_task));
        // pthread_spin_unlock(&heap_alloc_spinlock);
    }
    if (*task == NULL)
    {
        printf("mi_heap_malloc task failed\n");
        return false;
    }
    return true;
}

void app_free_task_request(dpkernel_task *task)
{
    mi_free(task);
}

// bool _dpm_free_shared_mem(dpkernel_task *task, bool is_input)
// {
//     // send the request thru the message queue
//     struct dpm_mem_req *req;
//     {
//         // std::lock_guard<std::mutex> lock(heap_alloc_mutex);
//         pthread_spin_lock(&heap_alloc_spinlock);
//         req = (struct dpm_mem_req *)mi_heap_malloc(app_req_ctx_shm.allocator.heap, sizeof(struct dpm_mem_req));
//         pthread_spin_unlock(&heap_alloc_spinlock);
//     }
//     if (req == NULL)
//     {
//         printf("mi_heap_malloc req allocation failed\n");
//         return false;
//     }
//     req->completion.store(DPK_ONGOING);

//     req->task = get_shm_ptr_for_task(task);
//     if (is_input)
//     {
//         req->type = DPM_MEM_REQ_FREE_INPUT;
//         // req->buf = get_shm_ptr_for_input_buf(buf);
//     }
//     else
//     {
//         req->type = DPM_MEM_REQ_FREE_OUTPUT;
//         // req->buf = get_shm_ptr_for_output_buf(buf);
//     }
//     // req->done.store(false, std::memory_order_release);

//     // convert the buf ptr to an offset from the start of the shared memory region
//     shm_ptr req_ptr = get_shm_ptr_for_mem_req_ctx(req);
//     print_debug("free req_ptr: %lu\n", req_ptr);
//     // mem_req_queue->send(&req_ptr, sizeof(req_ptr), 0);
//     // mem_req_queue->push(req_ptr);
//     struct dpm_req_msg msg{DPM_REQ_TYPE_MEM, req_ptr};
//     // submission_queue_mpmc->try_push(msg);
//     while (!InsertToRequestBufferProgressive(submission_queue_ring_buffers[0], (char *)&msg, sizeof(msg)))
//     {
//         int x = 0;
// #if defined(__x86_64__) || defined(_M_X64)
//         while (x < 50)
// #elif defined(__arm__) || defined(__aarch64__)
//         while (x < 50000)
// #else
//         while (x < 100000)
// #endif
//         {
// #if defined(__x86_64__) || defined(_M_X64)
//             _mm_pause();
// #elif defined(__arm__) || defined(__aarch64__)
//             __asm volatile("yield");
// #else
//             std::this_thread::yield();
// #endif
//             x++;
//         }
//     }
//     print_debug("sent free req\n");

//     // wait for the request to be done
//     print_debug("waiting for mem req to be done\n");
//     while (req->completion.load(std::memory_order_acquire) == DPK_ONGOING)
//     {
//         // maybe yield will help?
//         std::this_thread::yield();
//     }
//     print_debug("mem free req done\n");
//     // free the request
//     mi_free(req);
//     return true;
// }

struct dpm_mem_req *dpm_free_input_buf_async(dpkernel_task *task)
{
    return _dpm_submit_shared_mem_req_async(task, DPM_MEM_REQ_FREE_INPUT);
}

struct dpm_mem_req *dpm_free_output_buf_async(dpkernel_task *task)
{
    return _dpm_submit_shared_mem_req_async(task, DPM_MEM_REQ_FREE_OUTPUT);
}

bool dpm_free_input_buf(dpkernel_task *task)
{
    auto req = _dpm_submit_shared_mem_req_async(task, DPM_MEM_REQ_FREE_INPUT);
    if (req == NULL)
    {
        printf("mi_heap_malloc free input req allocation failed\n");
        return false;
    }
    print_debug("waiting for mem req to be done\n");
    return dpm_wait_for_mem_req_completion(req);
}
bool dpm_free_output_buf(dpkernel_task *task)
{
    auto req = _dpm_submit_shared_mem_req_async(task, DPM_MEM_REQ_FREE_OUTPUT);
    if (req == NULL)
    {
        printf("mi_heap_malloc free output req allocation failed\n");
        return false;
    }
    print_debug("waiting for mem req to be done\n");
    return dpm_wait_for_mem_req_completion(req);
}
