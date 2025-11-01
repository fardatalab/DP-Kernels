#include "dp_manager_msgq.hpp"
#include "bounded_queue.hpp"
#include "common.hpp"
#include "device_specific.hpp"
#include "executor.hpp"
#include "memory.hpp"
#include "mpmc_queue.hpp"
#include "ring_buffer.hpp"
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <string>
#include <vector>

std::atomic<bool> polling_loop{true};
// extern std::thread submission_thread;
// extern std::thread completion_thread;
// use a single thread
// std::thread manager_thread;

/// boost message queue for submission
// boost::interprocess::message_queue *dpm_submission_queue;
////boost::interprocess::message_queue *dpm_completion_queue;

/// MPMC queue for submission and completion
/* using rigtorp::MPMCQueue;
MPMCQueue<struct dpm_req_msg, DPM_SUBMISSION_QUEUE_SIZE> *dpm_submission_queue_mpmc; */

struct RequestRingBufferProgressive *dpm_submission_queue_ring_buffer;
struct RequestRingBufferProgressive *dpm_submission_queue_ring_buffer2;
char *req_ring_buffer_holder;
std::vector<struct RequestRingBufferProgressive *> dpm_submission_queue_ring_buffers;
// char *req_ring_buffer_holder2;

// will be nullptr if successfully consumed an item from submission queue, otherwise will be `_mq_polled_submission`
// dpkernel_task_base *mq_polled_submission;
// shm_ptr *mq_polled_submission;
// dpkernel_task_base _mq_polled_submission;
// shm_ptr _mq_polled_submission;

uint active_kernels[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

// for each type of task we hold a queue of requests
BoundedQueue<shm_ptr, DPM_TASKS_QUEUE_SIZE> *tasks_queues[dpm_task_name::TASK_NAME_LAST] = {};

bool dpm_detect_platform()
{
    // load all dynamic libraries
    std::vector<std::string> so_files;
    for (const auto &entry : std::filesystem::directory_iterator(PLATFORM_SPECIFIC_LIB_PATH))
    {
        // check if the file is a shared object and starts with libdpm
        {
            if (entry.is_regular_file() && entry.path().extension() == ".so" &&
                entry.path().filename().string().find("libdpm") == 0)
            {
                so_files.push_back(entry.path().string());
            }
        }
    }

    for (const auto &so_file : so_files)
    {
        void *handle = dlopen(so_file.c_str(), RTLD_NOW);
        if (!handle)
        {
            printf("Error: %s\n", dlerror());
            return false;
        }
        else
        {
            printf("loaded device specific lib: %s\n", so_file.c_str());
        }
        detect_platform_func detector = (detect_platform_func)dlsym(handle, DETECT_PLATFORM_FUNC_NAME);
        if (!detector)
        {
            std::cerr << "Cannot find detector in " << so_file << '\n';
            dlclose(handle);
            continue;
        }

        // detected the current platform
        if (detector())
        {
            printf("platform detector found in %s\n", so_file.c_str());
            // load the device and memory initializers for global use
            if (!dpm_load_device_and_mem_initializers(handle, &dpm_device_initializers, &dpm_mem_initializers))
            {
                std::cerr << "Cannot load device and memory initializers from " << so_file << '\n';
                dlclose(handle);
                return false;
            }

            // then register the kernels, need to initialize the executors later after device and memory initializations
            memset(dpm_executors, 0, sizeof(dpm_executors));
            if (!dpm_load_kernels(handle, dpm_executors))
            {
                std::cerr << "Cannot load kernels from " << so_file << '\n';
                dlclose(handle);
                return false;
            }
            return true;
        }
    }
    return true;
}

bool _dpm_init_kernel_queues()
{
    for (int i = 0; i < dpm_task_name::TASK_NAME_LAST; i++)
    {
        tasks_queues[i] = new BoundedQueue<shm_ptr, DPM_TASKS_QUEUE_SIZE>();
        if (tasks_queues[i] == nullptr)
        {
            printf("Failed to allocate kernel queue %d\n", i);
            return false;
        }
    }
    return true;
}

// let the application poll on dpkernel_task_base in shared memory for completion
/* dpkernel_completion *mq_polled_completion;
dpkernel_completion _mq_polled_completion; */

int dp_kernel_manager_msgq_start(void *args)
{
    // detect DPU platform first, will load the initializers, and the kernels
    dpm_detect_platform();

    // no longer register here, done during dpm_detect_platform()
    // dpm_register_executors();

    _dpm_init_kernel_queues();
    ////_create_msg_queue_boost();

    // _create_msg_queue_mpmc();

    _create_msg_queue_ring_buffer();

    // mq_polled_submission = NULL;
    // _mq_polled_submission = {};

    // mq_polled_completion = nullptr;
    // _mq_polled_completion;

    /* submission_thread = std::thread([]() {
        while (polling_loop)
        {
            dpm_poll_submission_msgq();
        }
    });

    completion_thread = std::thread([]() {
        while (polling_loop)
        {
            dpm_poll_completion_msgq();
        }
    }); */

    printf("initializing mem allocator\n");
    setup_memory_allocator(&dpm_own_mem_region, &dpm_req_ctx_shm);
    printf("initializing mem allocator done\n");

    printf("dpm_mem_init\n");
    dpm_device_and_mem_init(&dpm_own_mem_region);
    printf("dpm_mem_init done\n");

    printf("initializing kernels\n");
    dpm_init_kernels();
    printf("initializing kernels done\n");

    int i = 0;
    while (polling_loop)
    {
        // no longer polls mem requests separately
        // dpm_poll_mem_req();

        while (i < 1000)
        {
            i++;
            dpm_poll_requests();
        }
        i = 0;

        // after polling we need to execute the tasks (drain the queues)
        // remember that each kernel has its own queue

        // TODO: better strategy for draining the queues, maybe don't take too long here?
        dpm_drain_kernel_queues();

        /// DPM polls for completion and sets the completion flag, then the application can poll for it
        dpm_poll_completion();
    }

    return 0;
}

// void _create_msg_queue_boost()
// {
//     boost::interprocess::message_queue::remove(SUBMISSION_QUEUE_NAME);
//     boost::interprocess::message_queue::remove(COMPLETION_QUEUE_NAME);
//     dpm_submission_queue = new boost::interprocess::message_queue(boost::interprocess::create_only,
//                                                                   SUBMISSION_QUEUE_NAME, // name
//                                                                   DPM_SUBMISSION_QUEUE_SIZE,
//                                                                   // sizeof(dpkernel_task_base) // max message size
//                                                                   sizeof(shm_ptr) // only pass shm_ptr now
//     );
//     /* dpm_completion_queue = new boost::interprocess::message_queue(boost::interprocess::create_only,
//                                                                   COMPLETION_QUEUE_NAME, // name
//                                                                   DPM_SUBMISSION_QUEUE_SIZE,
//                                                                   sizeof(dpkernel_completion) // max message size
//     ); */

//     printf("submission queue get_max_msg_size: %lu\n", dpm_submission_queue->get_max_msg_size());
//     printf("get_max_msg_size: %lu\n", dpm_submission_queue->get_max_msg_size());
// }

// bool _create_msg_queue_mpmc()
// {
//     /* // std::string name = std::string(SUBMISSION_QUEUE_BACKING_SHM_NAME);
//     // std::string q_name = std::string(SUBMISSION_QUEUE_NAME);
//     // Create shared memory object
//     int shm_fd = shm_open(q_name.c_str(), O_CREAT | O_RDWR, 0666);
//     if (shm_fd == -1)
//     {
//         perror("create mpmc queue shm_open failed");
//         return false;
//     }

//     // Define the size of shared memory object
//     size_t shm_size = sizeof(rigtorp::MPMCQueue<shm_ptr, DPM_SUBMISSION_QUEUE_SIZE>);
//     if (ftruncate(shm_fd, shm_size) == -1)
//     {
//         perror("create mpmc queue ftruncate failed");
//         return false;
//     }

//     // Map shared memory object
//     void *ptr = mmap(0, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
//     if (ptr == MAP_FAILED)
//     {
//         perror("create mpmc queue mmap failed");
//         return false;
//     }

//     // Construct MPMCQueue in-place in shared memory
//     new (ptr) rigtorp::MPMCQueue<shm_ptr, DPM_SUBMISSION_QUEUE_SIZE>();
//     dpm_submission_queue_mpmc = (rigtorp::MPMCQueue<shm_ptr> *)ptr;
//     // printf("dpm_submission_queue_mpmc created\n", dpm_submission_queue_mpmc->slots_);
//      */
//     dpm_submission_queue_mpmc =
//         rigtorp::create_mpmc_queue<struct dpm_req_msg, DPM_SUBMISSION_QUEUE_SIZE>(SUBMISSION_QUEUE_NAME);
//     if (dpm_submission_queue_mpmc == nullptr)
//     {
//         printf("create_mpmc_queue for submission failed\n");
//         return false;
//     }
//     else
//     {
//         printf("create_mpmc_queue for submission succeeded\n");
//     }
//     return true;
// }

bool _create_msg_queue_ring_buffer()
{
    /* dpm_submission_queue_ring_buffer = AllocateRequestBufferProgressive(RING_BUFFER_SHM_NAME);
    if (dpm_submission_queue_ring_buffer == nullptr)
    {
        printf("Failed to allocate ring buffer\n");
        return false;
    }
    req_ring_buffer_holder = new char[RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT];

    dpm_submission_queue_ring_buffer2 = AllocateRequestBufferProgressive(RING_BUFFER_SHM_NAME2);
    if (dpm_submission_queue_ring_buffer2 == nullptr)
    {
        printf("Failed to allocate ring buffer\n");
        return false;
    }
    req_ring_buffer_holder2 = new char[RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT]; */
    for (int i = 0; i < RING_BUFFER_NUMBER; i++)
    {
        std::string ring_buffer_name = RING_BUFFER_SHM_NAME + std::to_string(i);
        struct RequestRingBufferProgressive *ringBuffer = AllocateRequestBufferProgressive(ring_buffer_name.c_str());
        if (ringBuffer == nullptr)
        {
            printf("Failed to allocate ring buffer\n");
            return false;
        }
        dpm_submission_queue_ring_buffers.push_back(ringBuffer);
    }

    req_ring_buffer_holder = new char[RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT];
    return true;
}

enum dpm_device _choose_optimal_device_for_task(enum dpm_task_name name)
{
    switch (name)
    {

    case TASK_COMPRESS_DEFLATE: {
        return dpm_device::DEVICE_BLUEFIELD_3;
    }
    case TASK_DECOMPRESS_DEFLATE: {
        return dpm_device::DEVICE_BLUEFIELD_3;
    }
    case TASK_DECOMPRESS_LZ4:
        return dpm_device::DEVICE_BLUEFIELD_3;
    case TASK_NULL:
        return dpm_device::DEVICE_NULL;
    default:
        printf("unknown kernel %d\n", name);
        return dpm_device::DEVICE_NULL;
        break;
    }
}

dpkernel_error _dpm_execute_task(dpkernel_task_base *task)
{
    dpkernel_error ret;
    print_debug("mq_polled_submission shm_ptr = %lu\n", *mq_polled_submission);
    // dpkernel_task_base *task = dpm_get_task_ptr_from_shmptr(*mq_polled_submission);
    print_debug("got task from shm_ptr: %p\n", task);

    struct dpm_task_executor *executor = get_executor(task->device, (enum dpm_task_name)task->task);
    if (executor == NULL) [[unlikely]]
    {
        printf("ERROR: no executor found for device = %d, task = %d\n", task->device, task->task);
        return DPK_ERROR_FAILED;
    }
    ////struct dpm_task_executor *executor = &dpm_executors[DEVICE_NULL][TASK_NULL];

    ret = executor->execute(task);

    if (ret == DPK_SUCCESS) [[likely]]
    {
        active_kernels[task->device][task->task]++;
    }
    else if (ret == DPK_ERROR_AGAIN) // the backing hw accelerator is full, etc.
    {
        print_debug("kernel %d EAGAIN\n", task->name);
        // task->completion.store(DPK_ERROR_AGAIN);
    }
    else
    {
        print_debug("kernel %d  failed\n", task->name);
        // task->completion.store(DPK_ERROR_FAILED);
    }
    return ret;
}

dpkernel_error _handle_mem_req(struct dpm_mem_req *req)
{
    print_debug("req->type: %d\n", req->type);
    if (req->type == DPM_MEM_REQ_ALLOC_INPUT)
    {
        char *buf = allocate_input_buf(req->size);
        if (buf == nullptr)
        {
            printf("failed to allocate input buf, size = %lu\n", req->size);
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        req->buf = dpm_get_shm_ptr_for_input_buf(buf);
        print_debug("allocated input buf: %lu\n", req->buf);
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
    }
    else if (req->type == DPM_MEM_REQ_ALLOC_OUTPUT)
    {
        char *buf = allocate_output_buf(req->size);
        if (buf == nullptr)
        {
            printf("failed to allocate output buf, size = %lu\n", req->size);
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        req->buf = dpm_get_shm_ptr_for_output_buf(buf);
        print_debug("allocated output buf: %lu\n", req->buf);
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
    }
    else if (req->type == DPM_MEM_REQ_FREE_INPUT)
    {
        mi_free(dpm_get_input_ptr_from_shmptr(req->buf));
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
    }
    else if (req->type == DPM_MEM_REQ_FREE_OUTPUT)
    {
        mi_free(dpm_get_output_ptr_from_shmptr(req->buf));
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
    }
    else
    {
        printf("unknown mem req->type: %d\n", req->type);
        return DPK_ERROR_FAILED;
    }
    return DPK_SUCCESS;
}

dpkernel_error dpm_poll_requests()
{
    dpkernel_error ret;
    struct dpm_req_msg *msg;

    // if (dpm_submission_queue_mpmc->try_pop(msg))
    FileIOSizeT req_size = 0;
    FileIOSizeT offset = 0;
    struct RequestRingBufferProgressive *req_ring_buffer;
    for (struct RequestRingBufferProgressive *ring_buffer : dpm_submission_queue_ring_buffers)
    {
        req_ring_buffer = ring_buffer;

        if (FetchFromRequestBufferProgressive(req_ring_buffer, req_ring_buffer_holder, &req_size))
        {
            offset = 0;
            // process all requests in the ring buffer
            while (offset < req_size)
            {
                FileIOSizeT totalBytes =
                    *(FileIOSizeT *)(req_ring_buffer_holder + offset); // total bytes contain the leading int
                // printf("totalBytes: %u\n", totalBytes);
                msg = (struct dpm_req_msg *)(req_ring_buffer_holder + offset + sizeof(FileIOSizeT));
                offset += totalBytes;
                switch (msg->type)
                {
                // can always directly handle memory requests
                case dpm_req_type::DPM_REQ_TYPE_MEM:
                    [[unlikely]]
                    {
                        // Handle memory request
                        struct dpm_mem_req *req = dpm_get_mem_req_ptr_from_shmptr(msg->ptr);
                        ret = _handle_mem_req(req);
                        if (ret != DPK_SUCCESS)
                        {
                            printf("failed to handle mem req\n");
                        }
                        // mq_polled_submission = NULL;

                        // return ret;
                        break;
                    }
                case dpm_req_type::DPM_REQ_TYPE_TASK:
                    [[likely]] // task request
                    {
                        dpkernel_task_base *task = dpm_get_task_ptr_from_shmptr(msg->ptr);
                        print_debug("got task from shm_ptr: %p\n", task);
                        if (task->device == DEVICE_NONE) [[unlikely]]
                        {
                            // choose the optimal device for the task
                            print_debug("task->device is DEVICE_NONE\n");
                            task->device = _choose_optimal_device_for_task((enum dpm_task_name)task->task);
                        }

                        // TODO: separate queues for each device (kernel: CPU/ASIC)
                        if (tasks_queues[task->task]->empty()) [[likely]]
                        {
                            // just execute the task if there is nothing queued
                            print_debug("kernel queue empty\n");
                            ret = _dpm_execute_task(task);
                            if (ret == DPK_ERROR_AGAIN) [[unlikely]]
                            {
                                // well we have to queue it because the backing kernel/hw is full
                                tasks_queues[task->task]->push(msg->ptr);
                            }
                        }
                        else if (tasks_queues[task->task]->full()) [[unlikely]]
                        {
                            // DPM (and the backing kernel/hw) at full capacity, let the application know
                            print_debug("dpm kernel queue full, device = %d, task = %d\n", task->device, task->task);
                            // try again later
                            task->completion.store(DPK_ERROR_AGAIN);
                            ret = DPK_ERROR_AGAIN;
                        }
                        else
                        {
                            // push the task to the queue, will be executed/drained later
                            print_debug("kernel queue push\n");
                            tasks_queues[task->task]->push(msg->ptr);
                            ret = DPK_SUCCESS;
                        }

                        // return ret;
                        break;
                    }
                default: {
                    print_debug("unknown request msg type: %d\n", msg.type);
                    // return DPK_ERROR_FAILED;
                    break;
                    // mq_polled_submission = NULL;
                }
                }
            }
            // processed all requests in this ring buffer
            // return ret;
        }
    }
    // else didn't receive anything
    return DPK_ERROR_AGAIN;
}

dpkernel_error dpm_drain_kernel_queues()
{
    dpkernel_error ret;
    for (int i = 0; i < dpm_task_name::TASK_NAME_LAST; i++)
    {
        // Process all tasks in the queue
        while (!tasks_queues[i]->empty())
        {
            shm_ptr task_ptr;
            tasks_queues[i]->pop(task_ptr);
            dpkernel_task_base *task = dpm_get_task_ptr_from_shmptr(task_ptr);
            print_debug("got task from shm_ptr: %p\n", task);
            ret = _dpm_execute_task(task);
            if (ret == DPK_ERROR_AGAIN)
            {
                // if task cannot be executed now, put it back in queue
                tasks_queues[i]->push_front(task_ptr);
                break;
            }
        }
    }
    return DPK_SUCCESS;
}

// the polling loop should complete the task, and then set the flag in dpkernel_task_base, then the application
// can poll for completion
void dpm_poll_completion()
{
    dpkernel_task_base task;
    dpkernel_error ret;
    struct dpm_task_executor *executor;
    // for each ACTIVE kernel, poll for completion
    for (int i = 0; i < DEVICE_LAST; i++)
    {
        for (int j = 0; j < TASK_NAME_LAST; j++)
        {
            executor = get_executor((dpm_device)i, (dpm_task_name)j);

            while (active_kernels[i][j] > 0)
            {
                // check executor exists and it provides completion polling
                if (executor == NULL || executor->poll == NULL)
                {
                    break;
                }
                // otherwise, can poll for completion
                ret = executor->poll(&task);

                if (ret != DPK_ERROR_AGAIN)
                {
                    print_debug("poll got completion, ret = %d\n", ret);
                    print_debug("actual_out_size: %lu\n", task->actual_out_size);
                    active_kernels[i][j]--;
                    task.completion.store(ret);
                }
                else
                { // we have drained all the completions for now
                    break;
                }
            }
        }
    }
}

int dp_kernel_manager_msgq_stop()
{
    polling_loop.store(false);
    nanosleep((const struct timespec[]){{0, 100000000L}}, NULL); // 100ms

    // perform cleanup of kernels first
    dpm_cleanup_kernels();

    return cleanup_dpm_msgq();
}

bool cleanup_dpm_msgq()
{
    bool ret = false;
    // ret = boost::interprocess::message_queue::remove(SUBMISSION_QUEUE_NAME);
    // ret &= boost::interprocess::message_queue::remove(COMPLETION_QUEUE_NAME);

    /// close shared memory
    /* if (munmap(dpm_submission_queue_mpmc, sizeof(*dpm_submission_queue_mpmc)) == -1)
    {
        perror("munmap dpm_submission_queue_mpmc");
        // return -1;
    } */
    // close(dpm_submission_queue_mpmc->allocator_.shm_fd);
    /* if (shm_unlink(SUBMISSION_QUEUE_BACKING_SHM_NAME) == -1)
    {
        // perror("shm_unlink");
        return -1;
    } */

    // cleanup memory
    printf("cleanup bluefield\n");
    // ret &= cleanup_bluefield();
    ret &= dpm_device_initializers.cleanup_device(dpm_device_initializers.setup_device_ctx);

    /// cleanup shared memory
    printf("cleanup shared memory\n");
    printf("teardown_mimalloc dpm_req_ctx_shm\n");
    teardown_mimalloc(&dpm_req_ctx_shm, DPM_REQ_CTX_SHM_NAME);
    printf("teardown_mimalloc dpm_own_mem_region.input_region\n");
    teardown_mimalloc(&dpm_own_mem_region.input_region, DPM_SHM_INPUT_REGION_NAME);
    printf("teardown_mimalloc dpm_own_mem_region.output_region\n");
    teardown_mimalloc(&dpm_own_mem_region.output_region, DPM_SHM_OUTPUT_REGION_NAME);
    printf("cleaned up shared memory\n");
    return ret;
}
