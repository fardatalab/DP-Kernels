#include "common.hpp"
#include "dpm_interface.hpp"
#include "memory.hpp"
#include "memory_common.hpp"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#endif
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#define LOOPS 20000
// #define DEPTH 128
// #define OUTPUT_SIZE 4096
// #define INPUT_FILE "4K.deflate"

#define EXPLICIT_DEVICE
// #undef EXPLICIT_DEVICE

#define BLOCKING_SUBMIT
// #undef BLOCKING_SUBMIT

#define BACKOFF
#undef BACKOFF // NOTE: backoff is in dpm_interface.cpp

#ifdef BACKOFF
int backoff = 1;
constexpr int max_backoff = 1024;
#endif

// #define N_BUFS 16384 // per thread

#define PIN_THREAD
// #undef PIN_THREAD

int NUM_THREADS = 1; // default to 1 thread

// Remove these global vectors that cause thread safety issues
// std::vector<shm_ptr> in_bufs;
// std::vector<shm_ptr> out_bufs;
// std::vector<shm_ptr> in_cpy_bufs;
// std::vector<shm_ptr> out_cpy_bufs;

// std::string INPUT_FILE = "/home/ubuntu/deflate/4K.deflate";
// std::string OUTPUT_FILE = "/home/ubuntu/txt/4K.txt";
std::string INPUT_FILE = "/data/jason/deflate/1K.deflate";
std::string OUTPUT_FILE = "/data/jason/txt/1K.txt";
int OUTPUT_SIZE = 1 * KB;
const int MEM_SIZE = (size_t)1 * GB;
int N_BUFS; // per thread

int INPUT_SIZE;
int DEPTH;
ulong loops_per_thread;

char *input_buf = NULL;
char *output_buf = NULL;

// Global statistics
std::atomic<ulong> total_submitted(0);
std::atomic<ulong> total_completed(0);
std::atomic<double> max_thread_time(0.0); // Add this line

void worker_thread(int thread_id, int num_threads, char *input_buf, size_t input_file_size)
{
#ifdef PIN_THREAD
    // Pin thread to core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    int core_id = 8 + thread_id * 1;
    CPU_SET(core_id, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
        std::cerr << "Error calling pthread_setaffinity_np for thread " << thread_id << " on core " << (thread_id)
                  << ": " << strerror(rc) << std::endl;
        return;
    }
    printf("Thread %d, %lu pinned to core %d\n", thread_id, pthread_self(), core_id);
#endif
    dpkernel_task_base *task;
    bool ret;
    ulong submitted_cnt = 0;
    ulong completed_cnt = 0;

    // Allocate input and output buffers fewer than all tasks
    shm_ptr in_buf, out_buf, in_cpy_buf, out_cpy_buf;

    // Create thread-local buffer vectors instead of using the global ones
    std::vector<shm_ptr> in_bufs;
    std::vector<shm_ptr> out_bufs;
    std::vector<shm_ptr> in_cpy_bufs;
    std::vector<shm_ptr> out_cpy_bufs;

    std::vector<dpm_mem_req *> in_bufs_req;
    std::vector<dpm_mem_req *> out_bufs_req;

    std::chrono::duration<double> alloc_buf_elapsed;
    std::chrono::time_point<std::chrono::high_resolution_clock> alloc_buf_time_start, alloc_buf_time_end, alloc_task_time_start, alloc_task_time_end;

    std::vector<dpkernel_task *> task_list;
    alloc_task_time_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < N_BUFS; ++i)
    {
        dpkernel_task *alloc_task = nullptr;
        if (!app_alloc_task_request(&alloc_task))
        {
            printf("Thread %d: Failed to allocate task %d\n", thread_id, i);
            return;
        }
        dpkernel_task_base *temp_task = &alloc_task->base;
        temp_task->in_size = INPUT_SIZE;
        temp_task->out_size = OUTPUT_SIZE;
        temp_task->task = dpm_task_name::TASK_NULL;
        temp_task->device = dpm_device::DEVICE_NULL;

        auto in_req = dpm_alloc_input_buf_async(input_file_size, alloc_task);
        if (in_req == NULL)
        {
            printf("Thread %d: Failed to allocate input buf %d\n", thread_id, i);
            exit(-1);
        }
        in_bufs_req.push_back(in_req);
        auto out_req = dpm_alloc_output_buf_async(OUTPUT_SIZE, alloc_task);
        if (out_req == NULL)
        {
            printf("Thread %d: Failed to allocate output buf %d\n", thread_id, i);
            exit(-1);
        }
        out_bufs_req.push_back(out_req);

        task_list.push_back(alloc_task);
    }

    printf("Thread %d: Allocated %d tasks\n", thread_id, N_BUFS);
    // wait for all mem reqs to be done
    for (int i = 0; i < N_BUFS; ++i)
    {
        auto temp_task = &task_list[i]->base;
        auto in_req = in_bufs_req[i];
        auto out_req = out_bufs_req[i];
        dpm_wait_for_mem_req_completion(in_req);
        dpm_wait_for_mem_req_completion(out_req);

        in_buf = temp_task->in;
        memcpy(app_get_input_ptr_from_shmptr(temp_task->in), input_buf, input_file_size);
        out_buf = temp_task->out;
        memcpy(app_get_output_ptr_from_shmptr(temp_task->out), output_buf, OUTPUT_SIZE);
#ifdef MEMCPY_KERNEL
        in_cpy_buf = temp_task->in_cpy;

        out_cpy_buf = temp_task->out_cpy;
#endif
        in_bufs.push_back(in_buf);
        out_bufs.push_back(out_buf);
#ifdef MEMCPY_KERNEL
        in_cpy_bufs.push_back(in_cpy_buf);
        out_cpy_bufs.push_back(out_cpy_buf);
#endif
    }
    printf("Thread %d: Allocated %d input and output buffers\n", thread_id, N_BUFS);

    for (int i = N_BUFS; i < DEPTH; ++i)
    {
        dpkernel_task *alloc_task = nullptr;
        if (!app_alloc_task_request(&alloc_task))
        {
            printf("Thread %d: Failed to allocate task %d\n", thread_id, i);
            return;
        }
        dpkernel_task_base *temp_task = &alloc_task->base;
        temp_task->in_size = INPUT_SIZE;
        temp_task->out_size = OUTPUT_SIZE;
        temp_task->task = dpm_task_name::TASK_NULL;
        temp_task->device = dpm_device::DEVICE_NULL;

        temp_task->in = in_bufs[i % N_BUFS];
        temp_task->out = out_bufs[i % N_BUFS];
#ifdef MEMCPY_KERNEL
        temp_task->in_cpy = in_cpy_bufs[i % N_BUFS];
        temp_task->out_cpy = out_cpy_bufs[i % N_BUFS];
#endif
        task_list.push_back(alloc_task);
        //         dpkernel_task *alloc_task = nullptr;
        //         if (!app_alloc_task_request(&alloc_task))
        //         {
        //             printf("Thread %d: Failed to allocate task %d\n", thread_id, i);
        //             return;
        //         }
        //         dpkernel_task_base *temp_task = &alloc_task->base;
        //         temp_task->in_size = INPUT_SIZE;
        //         temp_task->out_size = OUTPUT_SIZE;
        //         temp_task->task = dpm_task_name::TASK_NULL;
        //         temp_task->device = dpm_device::DEVICE_NULL;

        //         if (i < N_BUFS)
        //         {
        //             if (i == 0) {
        //                 alloc_buf_time_start = std::chrono::high_resolution_clock::now();
        //             }
        //             if (i == N_BUFS - 1) {
        //                 alloc_buf_time_end = std::chrono::high_resolution_clock::now();
        //                 alloc_buf_elapsed = alloc_buf_time_end - alloc_buf_time_start;
        //                 // printf("Thread %d: Alloc buf time: %f seconds\n", thread_id, alloc_buf_elapsed.count());
        //             }

        //             if (!dpm_alloc_input_buf(input_file_size, alloc_task))
        //             {
        //                 printf("Thread %d: Failed to allocate input buf %d\n", thread_id, i);
        //             }
        //             memcpy(app_get_input_ptr_from_shmptr(temp_task->in), input_buf, input_file_size);
        //             in_buf = temp_task->in;

        //             if (!dpm_alloc_output_buf(OUTPUT_SIZE, alloc_task))
        //             {
        //                 printf("Thread %d: Failed to allocate output buf %d\n", thread_id, i);
        //             }
        //             memcpy(app_get_output_ptr_from_shmptr(temp_task->out), output_buf, OUTPUT_SIZE);
        //             out_buf = temp_task->out;

        // #ifdef MEMCPY_KERNEL
        //             // // alloc the cpy bufs
        //             // if (!dpm_alloc_input_buf(input_file_size, alloc_task))
        //             // {
        //             //     printf("Thread %d: Failed to allocate input cpy buf %d\n", thread_id, i);
        //             //     break;
        //             // }
        //             in_cpy_buf = temp_task->in_cpy;

        //             // if (!dpm_alloc_output_buf(OUTPUT_SIZE, alloc_task))
        //             // {
        //             //     printf("Thread %d: Failed to allocate output cpy buf %d\n", thread_id, i);
        //             //     break;
        //             // }
        //             out_cpy_buf = temp_task->out_cpy;
        // #endif
        //             in_bufs.push_back(in_buf);
        //             out_bufs.push_back(out_buf);
        // #ifdef MEMCPY_KERNEL
        //             in_cpy_bufs.push_back(in_cpy_buf);
        //             out_cpy_bufs.push_back(out_cpy_buf);
        // #endif
        //         }
        //         else
        //         {
        //             // reuse the buffers
        //             temp_task->in = in_bufs[i % N_BUFS];
        //             temp_task->out = out_bufs[i % N_BUFS];
        // #ifdef MEMCPY_KERNEL
        //             temp_task->in_cpy = in_cpy_bufs[i % N_BUFS];
        //             temp_task->out_cpy = out_cpy_bufs[i % N_BUFS];
        // #endif
        //         }

        //         /* ret = dpm_alloc_input_buf(temp_task->in_size, &temp_task->in);
        //         if (!ret)
        //         {
        //             printf("Thread %d: Failed to allocate input buf\n", thread_id);
        //             break;
        //         }

        //         ret = dpm_alloc_output_buf(OUTPUT_SIZE, &temp_task->out);
        //         if (!ret)
        //         {
        //             printf("Thread %d: Failed to allocate output buf\n", thread_id);
        //             break;
        //         } */

        //         task_list.push_back(alloc_task);
    }

    alloc_task_time_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> alloc_task_elapsed = alloc_task_time_end - alloc_task_time_start;
    // printf("Thread %d: Alloc task time: %f seconds\n", thread_id, alloc_task_elapsed.count());
    // printf("percentage of alloc task time: %f\n", (alloc_buf_elapsed.count() / alloc_task_elapsed.count()) * 100);

    static std::mutex barrier_mutex;
    static std::condition_variable barrier_cv;
    static int barrier_count = 0;

    {
        std::unique_lock<std::mutex> lock(barrier_mutex);
        printf("thread %d waiting\n", thread_id);
        barrier_count++;
        if (barrier_count == num_threads)
        {
            barrier_cv.notify_all();
        }
        else
        {
            barrier_cv.wait(lock, [&] { return barrier_count == num_threads; });
        }
    }
    // Start timing here
    auto thread_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < DEPTH; ++i)
    {
        auto dpk_task = task_list[i];
        do
        {
#ifdef EXPLICIT_DEVICE
#ifdef BLOCKING_SUBMIT
            // dpm_submit_task_msgq_blocking(dpk_task, dpm_device::DEVICE_NULL);
            dpm_submit_task_msgq_blocking(thread_id, dpk_task,
                                          dpm_device::DEVICE_NULL); // each thread has its own queue

            // dpm_submit_task_msgq_blocking(task, dpm_device::DEVICE_NULL);
#else
            ret = dpm_submit_task_msgq(task, dpm_device::DEVICE_NULL);
#endif
#else
            ret = dpm_submit_task_msgq(task);
#endif

#ifdef BLOCKING_SUBMIT
#else
            if (!ret)
#endif
            {
                // printf("Thread %d: Failed to submit task, retrying...\n", thread_id);
                // NOTE: all these don't do much to throughput, at most ~5-10%
                // std::this_thread::yield();

                // _mm_pause();

                // Implement exponential backoff
                /* backoff = 1;
                while (backoff < max_backoff)
                {
                    for (int j = 0; j < backoff; j++)
                    {
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
#elif defined(__arm__) || defined(__aarch64__)
            __asm volatile("yield");
#else
            std::this_thread::yield();
#endif
                    }
                    backoff *= 2;
                } */
            }
#ifdef BLOCKING_SUBMIT
#else
            else
#endif
            {
                submitted_cnt++;
                // total_submitted++;
            }
#ifdef BLOCKING_SUBMIT
        } while (0);
#else
        } while (!ret);
#endif
    }

    auto submit_end_time = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> submit_elapsed = submit_end_time - thread_start;

    // printf("thread %d submitted %lu tasks\n", thread_id, submitted_cnt);
    //// check completion of the last task, to reduce contention possibly
    auto dpk_task = task_list[DEPTH - 1];
    task = &dpk_task->base;

    // struct timespec sleep_ts = {.tv_sec = 0, .tv_nsec = 100000000};
    while (task->completion.load(std::memory_order_release) != DPK_SUCCESS)
    {
        std::this_thread::yield();

        // nanosleep(&sleep_ts, NULL);
    }
    completed_cnt += DEPTH;
    ////

    //////
    // total_completed.fetch_add(submitted_cnt);
    //////

    //     while (completed_cnt < loops_per_thread)
    //     {
    //         for (int i = 0; i < DEPTH; ++i)
    //         {
    //             task = task_list[i];
    //             // if (task->completion.load() == DPK_SUCCESS)
    //             while (task->completion.load() != DPK_SUCCESS)
    //             {
    //                 // std::this_thread::yield();
    //             }

    //             {
    //                 completed_cnt++;
    //                 // total_completed++;

    //                 /* if (completed_cnt >= loops_per_thread)
    //                 {
    //                     break;
    //                 } */

    //                 if (submitted_cnt < loops_per_thread)
    //                 {
    //                     printf("submitted_cnt < loops_per_thread\n");
    //                     do
    //                     {
    // #ifdef EXPLICIT_DEVICE
    //                         ret = dpm_submit_task_msgq(task, dpm_device::DEVICE_NULL);
    // #else
    //                         ret = dpm_submit_task_msgq(task);
    // #endif
    //                         if (!ret)
    //                         {
    //                             // printf("Thread %d: Failed to submit task, retrying...\n", thread_id);
    //                             // std::this_thread::yield();
    //                         }
    //                         else
    //                         {
    //                             submitted_cnt++;
    //                             total_submitted++;
    //                         }
    //                     } while (!ret);
    //                 }
    //             }
    //         }
    //     }

    // End timing here
    auto thread_end = std::chrono::high_resolution_clock::now();
    // std::chrono::duration<double> submit_elapsed = submit_end_time - thread_start;
    std::chrono::duration<double> thread_elapsed = thread_end - thread_start;

    total_completed.fetch_add(completed_cnt);
    ////total_completed.fetch_add(submitted_cnt);

    printf("thread %d submit elapsed time: %f\n", thread_id, submit_elapsed.count());


    // Update max_thread_time if this thread took longer
    double current_max = max_thread_time.load();
    printf("thread %d submit elapsed time: %f\n", thread_id, submit_elapsed.count());
    printf("thread %d elapsed time: %f\n", thread_id, thread_elapsed.count());
    while (thread_elapsed.count() > current_max &&
           !max_thread_time.compare_exchange_weak(current_max, thread_elapsed.count()))
        ;

    std::vector<dpm_mem_req *> free_in_bufs_req;
    std::vector<dpm_mem_req *> free_out_bufs_req;
    // Clean up tasks
    for (auto task : task_list)
    {
        // get local ptr from shm ptr of in and out buffers
        // char *in = app_get_input_ptr_from_shmptr(task->in);
        // char *out = app_get_output_ptr_from_shmptr(task->out);
        // dpm_free_input_buf(in);
        // dpm_free_output_buf(out);
        // dpm_free_input_buf(task);
        // dpm_free_output_buf(task);
        // app_free_task_request(task);
    }
    for (int i = 0; i < N_BUFS; i++)
    {
        auto temp_task = task_list[i];
        free_in_bufs_req.push_back(dpm_free_input_buf_async(temp_task));
        free_out_bufs_req.push_back(dpm_free_output_buf_async(temp_task));
    }
    printf("thread %d freeing input and output buffers\n", thread_id);
    for (int i = 0; i < N_BUFS; i++)
    {
        dpm_wait_for_mem_req_completion(free_in_bufs_req[i]);
        dpm_wait_for_mem_req_completion(free_out_bufs_req[i]);
    }
    printf("thread %d freed input and output buffers\n", thread_id);

    printf("thread %d finished\n", thread_id);
}

int main(int argc, char *argv[])
{
    if (argc > 1)
    {
        NUM_THREADS = std::atoi(argv[1]);
    }
    N_BUFS = MEM_SIZE / OUTPUT_SIZE / NUM_THREADS; // per thread

    // Each thread handles LOOPS/num_threads iterations
    loops_per_thread = LOOPS / NUM_THREADS;

    if (argc > 2)
    {
        DEPTH = std::atoi(argv[2]);
    }
    else
    {
        DEPTH = loops_per_thread;
    }
    printf("DEPTH: %d\n", DEPTH);

    if (argc > 3)
    {
        INPUT_FILE = argv[3];
    }
    if (argc > 4)
    {
        OUTPUT_FILE = argv[4];
    }
    if (argc > 5)
    {
        OUTPUT_SIZE = std::atoi(argv[5]);
    }

    std::ifstream file(INPUT_FILE, std::ios::binary | std::ios::ate);
    if (!file)
    {
        std::cerr << "Error: Cannot open file " << INPUT_FILE << std::endl;
        exit(EXIT_FAILURE);
    }
    std::streamsize file_size = file.tellg();
    if (file_size < 0)
    {
        std::cerr << "Error: Failed to determine file size." << std::endl;
        exit(EXIT_FAILURE);
    }
    INPUT_SIZE = static_cast<int>(file_size);
    file.seekg(0, std::ios::beg);
    input_buf = new char[INPUT_SIZE];
    if (!file.read(input_buf, file_size))
    {
        std::cerr << "Error: Failed to read file data." << std::endl;
        delete[] input_buf;
        exit(EXIT_FAILURE);
    }
    file.close();

    std::ifstream outfile(OUTPUT_FILE, std::ios::binary);
    if (!outfile)
    {
        std::cerr << "Error: Cannot open file " << OUTPUT_FILE << std::endl;
        exit(EXIT_FAILURE);
    }
    output_buf = new char[OUTPUT_SIZE];
    outfile.read(output_buf, OUTPUT_SIZE);
    if (!outfile)
    {
        std::cerr << "Error: Only " << outfile.gcount() << " bytes could be read from " << OUTPUT_FILE << std::endl;
    }
    outfile.close();

    // Initialize DPM
    dpm_frontend_initialize();
    printf("dpm init completed\n");

    std::vector<std::thread> threads;

    // Remove or comment out the start timing here
    // auto start = std::chrono::high_resolution_clock::now();

    // Create threads
    for (int i = 0; i < NUM_THREADS; i++)
    {
        threads.emplace_back(worker_thread, i, NUM_THREADS, input_buf, INPUT_SIZE);
    }

    // Wait for all threads to complete
    for (auto &thread : threads)
    {
        thread.join();
    }

    // Use max_thread_time instead of overall time
    double elapsed_time = max_thread_time.load();

    // printf("Total submitted tasks: %lu\n", total_submitted.load());
    // printf("Total completed tasks: %lu\n", total_completed.load());
    std::cout << "Max thread time: " << elapsed_time << " s\n";

    double throughput_million_ops = (double)total_completed.load() / elapsed_time / 1000000;
    printf("Throughput: %.6f million ops/s\n", throughput_million_ops);

    return 0;
}
