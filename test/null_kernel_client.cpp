#include "common.hpp"
#include "dpm_interface.hpp"
#include <atomic>
#include <condition_variable>
#include <cstdio>
#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#endif
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#define LOOPS 10000000
// #define DEPTH 128
#define OUTPUT_SIZE 4096
#define INPUT_FILE "4K.deflate"

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

#define PIN_THREAD
// #undef PIN_THREAD

int INPUT_SIZE = 4096;
int DEPTH;
ulong loops_per_thread;

char *input_buf = NULL;

// Global statistics
std::atomic<ulong> total_submitted(0);
std::atomic<ulong> total_completed(0);
std::atomic<double> max_thread_time(0.0); // Add this line

void worker_thread(int thread_id, int num_threads)
{
#ifdef PIN_THREAD
    // Pin thread to core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(1 + thread_id * 1, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
        std::cerr << "Error calling pthread_setaffinity_np for thread " << thread_id << " on core "
                  << (0 + thread_id * 1) << ": " << strerror(rc) << std::endl;
        return;
    }
#endif
    dpkernel_task_base *task;
    bool ret;
    ulong submitted_cnt = 0;
    ulong completed_cnt = 0;

    std::vector<dpkernel_task_base *> task_list;
    for (int i = 0; i < DEPTH; ++i)
    {
        dpkernel_task_base *temp_task = nullptr;
        if (!app_alloc_task_request(&temp_task))
        {
            printf("Thread %d: Failed to allocate task %d\n", thread_id, i);
            return;
        }
        temp_task->in_size = INPUT_SIZE;
        temp_task->out_size = OUTPUT_SIZE;
        temp_task->task = dpm_task_name::TASK_NULL;

        /* ret = dpm_alloc_input_buf(temp_task->in_size, &temp_task->in);
        if (!ret)
        {
            printf("Thread %d: Failed to allocate input buf\n", thread_id);
            break;
        }

        ret = dpm_alloc_output_buf(OUTPUT_SIZE, &temp_task->out);
        if (!ret)
        {
            printf("Thread %d: Failed to allocate output buf\n", thread_id);
            break;
        } */

        task_list.push_back(temp_task);
    }

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
        task = task_list[i];
        do
        {
#ifdef EXPLICIT_DEVICE
#ifdef BLOCKING_SUBMIT
            // dpm_submit_task_msgq_blocking(thread_id, task, dpm_device::DEVICE_NULL);
            dpm_submit_task_msgq_blocking(task, dpm_device::DEVICE_NULL);
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

    //// check completion of the last task, to reduce contention possibly
    task = task_list[DEPTH - 1];
    while (task->completion.load(std::memory_order_acquire) != DPK_SUCCESS)
    {
        // std::this_thread::yield();
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
    std::chrono::duration<double> thread_elapsed = thread_end - thread_start;

    total_completed.fetch_add(completed_cnt);
    ////total_completed.fetch_add(submitted_cnt);

    // Update max_thread_time if this thread took longer
    double current_max = max_thread_time.load();
    printf("thread %d elapsed time: %f\n", thread_id, thread_elapsed.count());
    while (thread_elapsed.count() > current_max &&
           !max_thread_time.compare_exchange_weak(current_max, thread_elapsed.count()))
        ;

    // Clean up tasks
    /* for (auto task : task_list)
    {
        // get local ptr from shm ptr of in and out buffers
        char *in = app_get_input_ptr_from_shmptr(task->in);
        char *out = app_get_output_ptr_from_shmptr(task->out);
        dpm_free_input_buf(in);
        dpm_free_output_buf(out);
    } */
    printf("thread %d finished\n", thread_id);
}

int main(int argc, char *argv[])
{
    int num_threads = 1; // default to 1 thread
    if (argc > 1)
    {
        num_threads = std::atoi(argv[1]);
    }

    // Each thread handles LOOPS/num_threads iterations
    loops_per_thread = LOOPS / num_threads;

    if (argc > 2)
    {
        DEPTH = std::atoi(argv[2]);
    }
    else
    {
        DEPTH = loops_per_thread;
    }
    printf("DEPTH: %d\n", DEPTH);

    // Initialize DPM
    dpm_initialize();
    printf("dpm init completed\n");

    std::vector<std::thread> threads;

    // Remove or comment out the start timing here
    // auto start = std::chrono::high_resolution_clock::now();

    // Create threads
    for (int i = 0; i < num_threads; i++)
    {
        threads.emplace_back(worker_thread, i, num_threads);
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