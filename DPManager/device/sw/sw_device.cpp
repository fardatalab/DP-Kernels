#include "sw_device.hpp"
#include "common.hpp"
#include "kernel_interface.hpp"
#include "ring_buffer.hpp"
#include "scheduling.hpp"
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#endif
#include <sys/types.h>
#include <thread>

dpm_kernel sw_kernels[dpm_task_name::TASK_NAME_LAST];

std::thread *sw_kernel_threads;
static struct sw_kernel_thread_args *sw_kernel_thread_args;

/// per sw kernel thread ring buffer, they will each poll on their own ring buffer
struct RequestRingBufferProgressive **sw_kernel_ring_buffers;

struct dpm_kernel_catalogue sw_kernel_catalogues[TASK_NAME_LAST];

bool dpm_init_sw_kernels()
{
    sw_kernel_threads = (std::thread *)calloc(N_SW_KERNELS_THREADS, sizeof(std::thread));
    sw_kernel_thread_args =
        (struct sw_kernel_thread_args *)calloc(N_SW_KERNELS_THREADS, sizeof(struct sw_kernel_thread_args));
    // allocate the ring buffers
    sw_kernel_ring_buffers = (struct RequestRingBufferProgressive **)calloc(
        N_SW_KERNELS_THREADS, sizeof(struct RequestRingBufferProgressive *));

    // init the ring buffers
    for (int i = 0; i < N_SW_KERNELS_THREADS; i++)
    {
        sw_kernel_ring_buffers[i] = LocalAllocateRequestBufferProgressive(sizeof(struct RequestRingBufferProgressive));
    }
    printf("sw kernel ring buffers allocated\n");

    // init the kernels
    // decompress deflate kernel
    sw_kernels[TASK_DECOMPRESS_DEFLATE] = (dpm_kernel){
        .initialize = sw_decompress_deflate_kernel_init,
        .get_catalogue = sw_decompress_deflate_get_catalogue,
        .cleanup = sw_decompress_deflate_kernel_cleanup,
        .handle_mem_req = NULL,
        .execute = sw_decompress_deflate_kernel_execute,
    };
    // regex re2 kernel
    sw_kernels[TASK_REGEX_SEARCH] = (dpm_kernel){
        .initialize = sw_regex_deflate_kernel_init,
        .get_catalogue = sw_regex_deflate_get_catalogue,
        .cleanup = sw_regex_deflate_kernel_cleanup,
        .handle_mem_req = NULL,
        .execute = sw_regex_deflate_kernel_execute,
    };
    // TODO: other kernels init here

    // go through all the sw kernels and init them
    for (int i = 0; i < TASK_NAME_LAST; i++)
    {
        if (sw_kernels[i].initialize != NULL)
        {
            struct dpm_kernel_catalogue *result = &dpm_kernel_catalogues[DEVICE_SOFTWARE][i];
            if (sw_kernels[i].initialize(result, N_SW_KERNELS_THREADS))
            {
                if (sw_kernels[i].get_catalogue != NULL && sw_kernels[i].get_catalogue(result))
                {
                    printf("sw kernel %d got catalogue\n", i);
                }
                else
                {
                    printf("sw kernel %d has no get_catalogue\n", i);
                }

                printf("sw kernel %d initialized\n", i);
            }
            else
            {
                printf("sw kernel %d failed to initialize\n", i);
            }
        }
    }

    // init the threads
    for (int i = 0; i < N_SW_KERNELS_THREADS; i++)
    {
        sw_kernel_thread_args[i].thread_id = i;
        sw_kernel_thread_args[i].run.store(true, std::memory_order_release);
        sw_kernel_threads[i] = std::thread(sw_kernel_thread_handler, &sw_kernel_thread_args[i]);
        if (!sw_kernel_threads[i].joinable())
        {
            printf("Failed to create thread %d\n", i);
            return false;
        }
        printf("SW kernel thread %d created\n", i);
    }

    return true;
}

bool dpm_cleanup_sw_kernels()
{
    // cleanup the threads
    for (int i = 0; i < N_SW_KERNELS_THREADS; i++)
    {
        sw_kernel_thread_args[i].run.store(false, std::memory_order_release);
        if (sw_kernel_threads[i].joinable())
        {
            sw_kernel_threads[i].join();
        }
    }

    // cleanup the ring buffers
    for (int i = 0; i < N_SW_KERNELS_THREADS; i++)
    {
        LocalDeallocateRequestBufferProgressive(sw_kernel_ring_buffers[i]);
    }

    // cleanup the kernels
    sw_decompress_deflate_kernel_cleanup();

    free(sw_kernel_thread_args);
    free(sw_kernel_ring_buffers);

    return true;
}

void sw_kernel_thread_handler(struct sw_kernel_thread_args *args)
{
    // pin thread to core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(args->thread_id + N_DPM_THREADS, &cpuset); // pin after DPM threads
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
        std::cerr << "Error calling pthread_setaffinity_np for sw kernel thread " << args->thread_id << ": "
                  << strerror(rc) << std::endl;
        return;
    }
    else
    {
        printf("SW kernel thread %lu pinned to core %d\n", pthread_self(), args->thread_id + N_DPM_THREADS);
    }

    int thread_id = args->thread_id;
    uint loop_cnt = 0;

    struct RequestRingBufferProgressive *ring_buffer = sw_kernel_ring_buffers[thread_id];
    char *buf_holder = NULL;
    if (posix_memalign((void **)&buf_holder, 64, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT))
    {
        printf("Failed to allocate ring buffer holder for sw thread %d\n", thread_id);
        return;
    }
    if (buf_holder == NULL)
    {
        printf("Failed to allocate ring buffer holder for sw thread %d\n", thread_id);
        return;
    }

    printf("sw kernel thread %d started\n", thread_id);

    FileIOSizeT req_size = 0;
    FileIOSizeT offset = 0;
    dpm_req_msg *msg = NULL;
    dpkernel_task *dpk_task = NULL;
    dpkernel_error ret;

    // main loop
    while (args->run.load(std::memory_order_acquire))
    {
        // printf("sw kernel thread %d polling requests\n", thread_id);
        loop_cnt = 0;
        while (loop_cnt < 10000)
        {
            if (FetchFromRequestBufferProgressive(ring_buffer, buf_holder, &req_size))
            {
                print_debug("sw kernel thread %d got requests: %lu bytes\n", thread_id, req_size);
                offset = 0;
                // process all requests in the ring buffer
                while (offset < req_size)
                {
                    FileIOSizeT totalBytes =
                        *(FileIOSizeT *)(buf_holder + offset); // total bytes contain the leading int
                    msg = (struct dpm_req_msg *)(buf_holder + offset + sizeof(FileIOSizeT));
                    ////dpk_task = (dpkernel_task *)(buf_holder + offset + sizeof(FileIOSizeT));
                    offset += totalBytes;
                    dpk_task = dpm_get_task_ptr_from_shmptr(msg->ptr);
                    print_debug("dpkernel task %p, device %d, task %d\n", dpk_task, dpk_task->base.device,
                                dpk_task->base.task);

                    // execute the dpk_task
                    switch (dpk_task->base.task)
                    {
                    case TASK_DECOMPRESS_DEFLATE:
                        // this is blocking
                        ret = sw_decompress_deflate_kernel_execute(dpk_task, thread_id);
                        if (ret != DPK_SUCCESS) [[unlikely]]
                        {
                            printf("Failed to execute decompress deflate task\n");
                        }
                        else
                        {
                            // printf("decompress deflate sw task executed successfully\n");
                        }
                        break;
                    case TASK_COMPRESS_DEFLATE:
                        printf("Compress deflate not implemented yet\n");
                        break;
                    case TASK_REGEX_SEARCH:
                        ret = sw_regex_deflate_kernel_execute(dpk_task, thread_id);
                        if (ret != DPK_SUCCESS) [[unlikely]]
                        {
                            printf("Failed to execute regex search task\n");
                        }
                        else
                        {
                            // printf("regex search sw task executed successfully\n");
                        }
                        break;
                    case TASK_NULL:
                        printf("Null task, nothing to do\n");
                        break;
                    case TASK_NAME_LAST:
                        printf("should not use task last!!!\n");
                        break;
                    default:
                        printf("Unknown task %d\n", dpk_task->base.task);
                        break;
                    }

                    print_debug("update kernel draining rate for sw kernel\n");
#ifdef SCHEDULING
                    _dec_sw_queueing_delay_on_completion(thread_id, &dpk_task->base);
                    /* _update_kernel_draining_rate(dpk_task->base, args->thread_id);
                    _dec_inflight_bytes_on_completion(dpk_task->base, args->thread_id); */
#endif
                }
            }
            else
            {
                // no requests in the ring buffer, yield a bit
                int x = 0;
#if defined(__x86_64__) || defined(_M_X64)
                while (x < 500)
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
            }
            loop_cnt++;
        }
    }
    free(buf_holder);
    buf_holder = NULL;
    printf("sw kernel thread %d exited\n", thread_id);
}