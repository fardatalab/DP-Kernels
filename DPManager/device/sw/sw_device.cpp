#include "sw_device.hpp"
#include "common.hpp"
#include "ring_buffer.hpp"
#include "scheduling.hpp"
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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
        .cleanup = sw_decompress_deflate_kernel_cleanup,
        .handle_mem_req = NULL,
        .execute = sw_decompress_deflate_kernel_execute,
    };
    // TODO: other kernels init here

    // go through all the sw kernels and init them
    for (int i = 0; i < TASK_NAME_LAST; i++)
    {
        if (sw_kernels[i].initialize != NULL)
        {
            struct dpm_kernel_catalogue *result = &sw_kernel_catalogues[i];
            if (sw_kernels[i].initialize(result, N_SW_KERNELS_THREADS))
            {
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
    dpkernel_task *dpk_task = NULL;
    dpkernel_error ret;

    // main loop
    while (args->run.load(std::memory_order_acquire))
    {
        while (loop_cnt < 1000000)
        {
            if (FetchFromRequestBufferProgressive(ring_buffer, buf_holder, &req_size))
            {
                printf("sw kernel thread %d got request\n", thread_id);
                offset = 0;
                // process all requests in the ring buffer
                while (offset < req_size)
                {
                    FileIOSizeT totalBytes =
                        *(FileIOSizeT *)(buf_holder + offset); // total bytes contain the leading int
                    dpk_task = (dpkernel_task *)(buf_holder + offset + sizeof(FileIOSizeT));
                    offset += totalBytes;

                    // execute the dpk_task
                    switch (dpk_task->base.task)
                    {
                    case TASK_DECOMPRESS_DEFLATE:
                        ret = sw_decompress_deflate_kernel_execute(dpk_task, thread_id);
                        if (ret != DPK_SUCCESS) [[unlikely]]
                        {
                            printf("Failed to execute decompress deflate task\n");
                        }
                        break;
                    case TASK_COMPRESS_DEFLATE:
                        printf("Compress deflate not implemented yet\n");
                        break;
                    case TASK_REGEX_SEARCH:
                        printf("Regex search not implemented yet\n");
                        break;
                    case TASK_NULL:
                        printf("Null task, nothing to do\n");
                        break;
                    case TASK_NAME_LAST:
                        printf("should not use task last!!!\n");
                        break;
                    }

                    _update_kernel_draining_rate(dpk_task->base, args->thread_id);
                    _dec_inflight_bytes_on_completion(dpk_task->base, args->thread_id);
                }
            }
            loop_cnt++;
        }
    }
    free(buf_holder);
    buf_holder = NULL;
    printf("sw kernel thread %d exiting\n", thread_id);
}