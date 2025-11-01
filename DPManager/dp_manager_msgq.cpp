#include "dp_manager_msgq.hpp"
#include "bounded_queue.hpp"
#include "common.hpp"
#include "device_specific.hpp"
#include "kernel_interface.hpp"
#include "memory.hpp"
// #include "mpmc_queue.hpp"
#include "kernel_queue.hpp"
#include "ring_buffer.hpp"
#include "scheduling.hpp"
#include "sw_device.hpp"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <vector>

// the underlying device, will be filled in by device sproc on detection
enum dpm_device dpm_underlying_device = dpm_device::DEVICE_NULL;

std::atomic<bool> polling_loop{true};
std::thread dpm_threads[N_DPM_THREADS];

std::vector<char *> req_ring_buffer_holders;
std::vector<struct RequestRingBufferProgressive *> dpm_submission_queue_ring_buffers;

uint active_kernels[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

// NOTE: no longer need actual queues, just use the moving average of bytes processed per unit time
// TODO: switch to DDS ring buffer
// TODO: need to allow DDS ring buffer to use a given size...
// for each type of sw kernel we hold a software queue of requests
// MPMCQueue<shm_ptr> *sw_kernel_queues[dpm_task_name::TASK_NAME_LAST] = {};

// for each dpm thread, for each type of hw kernel, we hold a software queue
BoundedQueue<dpkernel_task *, DPM_HW_KERNEL_QUEUE_SIZE>
    *hw_kernel_queues[N_DPM_THREADS][dpm_task_name::TASK_NAME_LAST] = {};

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
        if (detector(&dpm_underlying_device))
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
        else
        {
            printf("platform detector not found in %s\n", so_file.c_str());
            dlclose(handle);
        }
    }
    return false;
}

bool _dpm_init_kernel_queues()
{
    for (int t = 0; t < N_DPM_THREADS; t++)
    {
        for (int i = 0; i < dpm_task_name::TASK_NAME_LAST; i++)
        {
            // sw_kernel_queues[i] = new MPMCQueue<shm_ptr>(DPM_SW_KERNEL_QUEUE_SIZE);
            // if (sw_kernel_queues[i] == nullptr)
            // {
            //     printf("Failed to allocate kernel queue %d\n", i);
            //     return false;
            // }
            hw_kernel_queues[t][i] = new BoundedQueue<dpkernel_task *, DPM_HW_KERNEL_QUEUE_SIZE>();
            if (hw_kernel_queues[i] == nullptr)
            {
                printf("Failed to allocate kernel queue %d\n", i);
                return false;
            }
        }
    }
    memset(draining_rates, 0, sizeof(draining_rates));
    memset(inflight_bytes, 0, sizeof(inflight_bytes));
    memset(active_kernels, 0, sizeof(active_kernels));
    return true;
}

// let the application poll on dpkernel_task_base in shared memory for completion
/* dpkernel_completion *mq_polled_completion;
dpkernel_completion _mq_polled_completion; */

void _dpm_thread_handler(void *args)
{
    int thread_id = (int)(intptr_t)args;

    // pin thread to core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thread_id, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
        std::cerr << "Error calling pthread_setaffinity_np for thread " << (int)(intptr_t)args << " on core "
                  << (thread_id) << ": " << strerror(rc) << std::endl;
        return;
    }

    int i = 0;
    uint loop_cnt = 0;
    dpkernel_error ret;
    while (polling_loop.load(std::memory_order_relaxed))
    {
        // no longer polls mem requests separately
        // dpm_poll_mem_req();

        loop_cnt = 0;
        while (loop_cnt < 1000000)
        {
            while (i < 1000)
            {
                i++;
                ret = dpm_poll_requests(thread_id);
                if (ret == DPK_SUCCESS)
                {
                    // got a request, poll more in case there are more
                    continue;
                }
                else
                {
                    // assuming DPK_ERROR_AGAIN, no more requests, get out of the loop faster
                    i += 256;
                }
            }
            i = 0;

            // drain hardware queues if we have a hardware device
            if (dpm_underlying_device != DEVICE_NULL) [[likely]]
            {
                dpm_drain_kernel_queues(thread_id);
            }

            /// DPM polls for completion and sets the completion flag, then the application can poll for it
            dpm_poll_completion(thread_id);
            loop_cnt++;
        }
    }
}

int dp_kernel_manager_msgq_start(void *args)
{
    // detect DPU platform first, will load the initializers, and the kernels
    dpm_detect_platform();

    _dpm_init_kernel_queues();
    ////_create_msg_queue_boost();

    // _create_msg_queue_mpmc();

    _create_msg_queue_ring_buffer();

    printf("initializing mem allocator\n");
    setup_memory_allocator(&dpm_own_mem_region, &dpm_req_ctx_shm);
    printf("initializing mem allocator done\n");

    printf("dpm_mem_init\n");
    dpm_device_and_mem_init(&dpm_own_mem_region);
    printf("dpm_mem_init done\n");

    printf("initializing sw kernels\n");
    dpm_init_sw_kernels();
    printf("initializing sw kernels done\n");

    printf("initializing hw kernels\n");
    dpm_init_hw_kernels();
    printf("initializing hw kernels done\n");

    // start the dpm threads (== RING_BUFFER_NUMBER threads)
    for (int i = 1; i < N_DPM_THREADS; i++)
    {
        dpm_threads[i] = std::thread(_dpm_thread_handler, (void *)(intptr_t)i);
        if (!dpm_threads[i].joinable())
        {
            printf("Failed to create thread %d\n", i);
            return -1;
        }
        printf("DPM thread %d created\n", i);
    }

    // run the first thread in the main thread
    _dpm_thread_handler((void *)(intptr_t)0);

    // wait for the threads to finish (starting from 1, 0 is self and unused)
    for (int i = 1; i < N_DPM_THREADS; i++)
    {
        if (dpm_threads[i].joinable())
        {
            dpm_threads[i].join();
            printf("DPM thread %d joined\n", i);
        }
    }

    printf("cleaning up sw kernels\n");
    dpm_cleanup_sw_kernels();
    printf("clean up sw kernels done\n");

    return 0;
}

bool _create_msg_queue_ring_buffer()
{
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

        char *buf_holder = new char[RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT];
        if (buf_holder == nullptr)
        {
            printf("Failed to allocate ring buffer holder %d\n", i);
            return false;
        }
        req_ring_buffer_holders.push_back(buf_holder);
    }

    return true;
}

enum dpm_device _schedule_task_for_optimal_device(dpkernel_task *dpk_task, int thread_id)
{
    enum dpm_task_name task_name = (enum dpm_task_name)dpk_task->base.task;
    // if there is no hw kernel, or if the task is not supported on the hw device, use the sw one
    if (dpm_underlying_device == dpm_device::DEVICE_NULL ||
        dpm_executors[dpm_underlying_device][task_name].execute == nullptr)
    {
        return dpm_device::DEVICE_SOFTWARE;
    }

    // otherwise, perform scheduling based on queueing delay and kernel execution time
    float sw_kernel_speed = draining_rates[thread_id][dpm_device::DEVICE_SOFTWARE][task_name];
    float hw_kernel_speed = draining_rates[thread_id][dpm_underlying_device][task_name];

    // given the draining speed, and the inflight bytes, we can choose the device that gives us shortest time to
    // completion
    auto sw_inflight_bytes = inflight_bytes[thread_id][dpm_device::DEVICE_SOFTWARE][task_name];
    auto hw_inflight_bytes = inflight_bytes[thread_id][dpm_underlying_device][task_name];
    float sw_queuing_time = (sw_inflight_bytes + dpk_task->base.in_size) / sw_kernel_speed;
    float hw_queuing_time = (hw_inflight_bytes + dpk_task->base.in_size) / hw_kernel_speed;

    if (sw_queuing_time < hw_queuing_time)
    {
        printf("sw kernel is faster, using sw kernel, sw rate = %f, hw rate = %f, sw inflight bytes = %lu, hw inflight "
               "bytes = %lu\n",
               sw_kernel_speed, hw_kernel_speed, sw_inflight_bytes, hw_inflight_bytes);
        return dpm_device::DEVICE_SOFTWARE;
    }
    else
    {
        return dpm_underlying_device;
    }

    ////old stuff
    /* switch (name)
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
    } */
}

dpkernel_error _dpm_execute_task(dpkernel_task *task, int thread_id)
{
    dpkernel_task_base *dpk_task = &task->base;
    dpkernel_error ret;
    print_debug("mq_polled_submission shm_ptr = %lu\n", *mq_polled_submission);
    // dpkernel_task_base *task = dpm_get_task_ptr_from_shmptr(*mq_polled_submission);
    print_debug("got task from shm_ptr: %p\n", task);

    struct dpm_kernel *executor = get_kernel(dpk_task->device, (enum dpm_task_name)dpk_task->task);
    if (executor == NULL) [[unlikely]]
    {
        printf("ERROR: no executor found for device = %d, task = %d\n", dpk_task->device, dpk_task->task);
        return DPK_ERROR_FAILED;
    }
    ////struct dpm_task_executor *executor = &dpm_executors[DEVICE_NULL][TASK_NULL];

    ret = executor->execute(task, thread_id);

    if (ret == DPK_SUCCESS) [[likely]]
    {
        active_kernels[dpk_task->device][dpk_task->task]++;
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
    print_debug("_handle_mem_req got mem req: %p\n", req);
    print_debug("req->type: %d\n", req->type);
    dpkernel_task *task = dpm_get_task_ptr_from_shmptr(req->task);
    auto &dpk_task = task->base;

    switch (req->type)
    {
    case DPM_MEM_REQ_ALLOC_INPUT:
    {
        char *buf = allocate_input_buf(dpk_task.in_size);
        if (buf == nullptr)
        {
            printf("failed to allocate input buf, size = %u\n", dpk_task.in_size);
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        dpk_task.in = dpm_get_shm_ptr_for_input_buf(buf);
#ifdef MEMCPY_KERNEL
        // also allocate the cpy buffers
        char *in_cpy_buf = allocate_input_buf(dpk_task.in_size);
        if (in_cpy_buf == nullptr)
        {
            printf("failed to allocate input cpy buf, size = %u\n", dpk_task.in_size);
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        dpk_task.in_cpy = dpm_get_shm_ptr_for_input_buf(in_cpy_buf);
#endif
        print_debug("allocated input buf: %lu\n", req->buf);
        break;
    }
    case DPM_MEM_REQ_ALLOC_OUTPUT:
    {
        char *buf = allocate_output_buf(dpk_task.out_size);
        if (buf == nullptr)
        {
            printf("failed to allocate output buf, size = %u\n", dpk_task.out_size);
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        dpk_task.out = dpm_get_shm_ptr_for_output_buf(buf);
#ifdef MEMCPY_KERNEL
        // also allocate the cpy buffers
        char *out_cpy_buf = allocate_output_buf(dpk_task.out_size);
        if (out_cpy_buf == nullptr)
        {
            printf("failed to allocate output cpy buf, size = %u\n", dpk_task.out_size);
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        dpk_task.out_cpy = dpm_get_shm_ptr_for_output_buf(out_cpy_buf);
#endif
        print_debug("allocated output buf: %lu\n", req->buf);
        break;
    }
    case DPM_MEM_REQ_FREE_INPUT:
    {
        mi_free(dpm_get_input_ptr_from_shmptr(dpk_task.in));
#ifdef MEMCPY_KERNEL
        mi_free(dpm_get_input_ptr_from_shmptr(dpk_task.in_cpy));
#endif
        break;
    }
    case DPM_MEM_REQ_FREE_OUTPUT:
    {
        mi_free(dpm_get_output_ptr_from_shmptr(dpk_task.out));
#ifdef MEMCPY_KERNEL
        mi_free(dpm_get_output_ptr_from_shmptr(dpk_task.out_cpy));
#endif
        break;
    }
    default:
    {
        printf("unknown mem req->type: %d\n", req->type);
        req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
        return DPK_ERROR_FAILED;
    }
    }

    // perform kernel specific memory request handling, if any
    auto kernel = get_kernel((enum dpm_device)dpk_task.device, (enum dpm_task_name)dpk_task.task);
    if (kernel->handle_mem_req != NULL)
    {
        if (kernel->handle_mem_req(req))
        {
            print_debug("device %d kernel %d handled mem req\n", task->device, task->task);
            req->completion.store(DPK_SUCCESS, std::memory_order_release);
            return DPK_SUCCESS;
        }
        else
        {
            printf("device %d kernel %d failed to handle mem req\n", dpk_task.device, dpk_task.task);
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
    }
    else // no kernel specific mem req handler
    {
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
        return DPK_SUCCESS;
    }
}

dpkernel_error _dpm_try_execute_or_queue_task(int thread_id, dpkernel_task *task)
{
    dpkernel_error ret;
    dpkernel_task_base *dpk_task = &task->base;
    auto hw_kernel_queue = hw_kernel_queues[thread_id][dpk_task->task];
    dpm_kernel *kernel = get_kernel(dpk_task->device, dpk_task->task);
    uint32_t remaining_capacity = kernel->hw_kernel_remaining_capacity(thread_id);

    if (remaining_capacity > 0) // can directly execute
    {
        ret = _dpm_execute_task(task, thread_id);
        if (ret == DPK_ERROR_AGAIN) [[unlikely]]
        {
            // well we have to queue it because the backing kernel is full
            printf("IMPOSSIBLE: kernel %d EAGAIN to execute on thread %d\n", dpk_task->task, thread_id);
            ////kernel_queue->push(msg->ptr);
            if (hw_kernel_queue->push(task))
            {
                // nothing to do here
                // increment the inflight bytes
                printf("pushed task to hw kernel queue, device = %d, task = %d\n", dpk_task->device, dpk_task->task);
                _inc_inflight_bytes_on_submit(*dpk_task, thread_id);
                ret = DPK_SUCCESS;
            }
            else
            {
                // can't enqueue, queue is full, just fail it
                printf("thread %d kernel to be executed on hw, but queue full, device = %d, task = %d\n", thread_id,
                       dpk_task->device, dpk_task->task);
                dpk_task->completion.store(DPK_ERROR_AGAIN, std::memory_order_release);
            }
        }
        else if (ret == DPK_ERROR_FAILED) [[unlikely]]
        {
            printf("kernel %d failed to execute on thread %d\n", dpk_task->task, thread_id);
            dpk_task->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
        }
        else // success
        {
            // either enqueued, or submitted to hardware
            _inc_inflight_bytes_on_submit(*dpk_task, thread_id);
            ////DON'T increment the inflight bytes, the hardware can handle it
        }
    }
    else // must queue
    {
        if (hw_kernel_queue->push(task))
        {
            // increment the inflight bytes
            printf("pushed task to hw kernel queue, device = %d, task = %d\n", dpk_task->device, dpk_task->task);
            _inc_inflight_bytes_on_submit(*dpk_task, thread_id);
            ret = DPK_SUCCESS;
            // active_kernels[task->device][task->task]++;
        }
        else // hw kernel queue is full, just fail
        {
            printf("hw kernel to be scheduled on, but queue full, device = %d, task = %d\n", dpk_task->device,
                   dpk_task->task);
            ret = DPK_ERROR_FAILED;
            dpk_task->completion.store(DPK_ERROR_AGAIN, std::memory_order_release);
        }
    }
    return ret;
}
dpkernel_error dpm_poll_requests(int thread_id)
{
    dpkernel_error ret;
    struct dpm_req_msg *msg;

    FileIOSizeT req_size = 0;
    FileIOSizeT offset = 0;
    struct RequestRingBufferProgressive *req_ring_buffer = dpm_submission_queue_ring_buffers[thread_id];
    char *req_ring_buffer_holder = req_ring_buffer_holders[thread_id];

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
                    // dpkernel_task *task = dpm_get_task_ptr_from_shmptr(req->task);
                    print_debug("mem req got device %d task %d\n", task->device, task->task);
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
                    // bool device_is_user_specified = true;
                    print_debug("got task request\n");
                    dpkernel_task *task = dpm_get_task_ptr_from_shmptr(msg->ptr);
                    dpkernel_task_base *dpk_task = &task->base;
                    print_debug("got task from shm_ptr: %p\n", task);
                    if (dpk_task->device == DEVICE_NONE) [[unlikely]]
                    {
                        // device_is_user_specified = false;
                        // choose the optimal device for the task
                        print_debug("task->device is DEVICE_NONE\n");
                        dpk_task->device = _schedule_task_for_optimal_device(task, thread_id);
                    }

                    // now we have a device (either we chose/scheduled it or the user specified it)
                    bool is_sw_kernel = dpk_task->device == DEVICE_SOFTWARE;
                    // auto hw_kernel_queue = hw_kernel_queues[dpk_task->task];

                    // if is HW kernel, check remaining capacity and try to execute directly if possible
                    if (!is_sw_kernel)
                    {
                        // check if we can coalesce the task
                        /* if (_can_coalesce_task(*dpk_task))
                        {
                            if (_coalesce_task_and_submit(*dpk_task, thread_id))
                            {
                                // TODO: coalescing HOW??
                            }
                        } */

                        // try to execute it, or it will be queued up, or it will fail
                        _dpm_try_execute_or_queue_task(thread_id, task);
                    }
                    else // sw kernel, we always push to the ring buffer because the SW threads (pool) will just grab
                         // work from there
                    {
                        if (InsertToRequestBufferProgressive(sw_kernel_ring_buffers[thread_id % N_SW_KERNELS_THREADS],
                                                             (char *)&msg, sizeof(*msg)))
                        {
                            // successfully enqueued
                            printf("pushed task to sw kernel ring buffer, device = %d, task = %d\n", dpk_task->device,
                                   dpk_task->task);
                            _inc_inflight_bytes_on_submit(*dpk_task, thread_id);
                        }
                        else
                        {
                            // can't enqueue, ring buffer is full, just fail it
                            printf("sw kernel to be scheduled on, but ring buffer is full, device = %d, task = %d\n",
                                   dpk_task->device, dpk_task->task);
                            dpk_task->completion.store(DPK_ERROR_AGAIN, std::memory_order_release);
                        }
                    }
                    break;
                }
            default:
            {
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

    // else didn't receive anything
    return DPK_ERROR_AGAIN;
}

dpkernel_error dpm_drain_kernel_queues(int thread_id)
{
    dpkernel_error ret;
    for (int i = 0; i < dpm_task_name::TASK_NAME_LAST; i++)
    {
        struct dpm_kernel *executor = get_kernel(dpm_underlying_device, (dpm_task_name)i);
        // check if the executor exists and it provides execution
        if (executor->hw_kernel_remaining_capacity == NULL)
        {
            continue;
        }

        uint32_t remaining_capacity = executor->hw_kernel_remaining_capacity(thread_id);

        // Process tasks in the hw queue
        uint32_t j = 0;

        dpkernel_task *task;
        dpkernel_task_base *dpk_task;
        // shm_ptr task_ptr;
        while (j < remaining_capacity)
        {
            // get a task and let the kernel execute it
            // if (hw_kernel_queues[i][thread_id].pop(task_ptr))
            if (hw_kernel_queues[thread_id][i]->pop(task))
            {
                // got a task, execute it
                printf("thread %d got task %d from hw queue\n", thread_id, dpk_task->task);
                // task = dpm_get_task_ptr_from_shmptr(task_ptr);
                dpk_task = &task->base;

                // can directly execute
                ret = _dpm_execute_task(task, thread_id);
                if (ret == DPK_ERROR_AGAIN) [[unlikely]]
                {
                    printf("IMPOSSIBLE: kernel %d EAGAIN to execute on thread %d\n", dpk_task->task, thread_id);
                }
                else if (ret == DPK_ERROR_FAILED) [[unlikely]]
                {
                    printf("kernel %d failed to execute on thread %d\n", dpk_task->task, thread_id);
                    dpk_task->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                }
                // else success
                j++;
                _inc_inflight_bytes_on_submit(*dpk_task, thread_id);
            }
            else // got no task from the queue, we are done
            {
                break;
            }
        }
    }
    return DPK_SUCCESS;
}

// the polling loop should complete the task, and then set the flag in dpkernel_task_base, then the application
// can poll for completion
void dpm_poll_completion(int thread_id)
{
    dpkernel_task task;
    dpkernel_error ret;
    struct dpm_kernel *kernel;
    // for each ACTIVE kernel, poll for completion
    for (int i = 0; i < DEVICE_LAST; i++)
    {
        for (int j = 0; j < TASK_NAME_LAST; j++)
        {
            kernel = get_kernel((dpm_device)i, (dpm_task_name)j);

            while (active_kernels[i][j] > 0)
            {
                // check executor exists and it provides completion polling
                if (kernel == NULL || kernel->poll == NULL)
                {
                    break;
                }
                // otherwise, can poll for completion
                ret = kernel->poll(&task, thread_id);

                if (ret != DPK_ERROR_AGAIN)
                {
                    print_debug("poll got completion, ret = %d\n", ret);
                    print_debug("actual_out_size: %lu\n", task->actual_out_size);
                    active_kernels[i][j]--;

                    auto &dpk_task = task.base;

                    _update_kernel_draining_rate(dpk_task, thread_id);
                    _dec_inflight_bytes_on_completion(dpk_task, thread_id);

                    // set the completion on behalf of kernel
                    dpk_task.completion.store(ret);
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
