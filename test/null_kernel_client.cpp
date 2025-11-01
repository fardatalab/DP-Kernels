#include "common.hpp"
#include "dpm_interface.hpp"
#include <atomic>
#include <condition_variable>
#include <cstdio>
#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#endif
#include <iostream>
#include <fstream>
#include <mutex>
#include <thread>
#include <vector>

std::vector<shm_ptr> in_bufs;
std::vector<shm_ptr> out_bufs;
std::vector<shm_ptr> in_cpy_bufs;
std::vector<shm_ptr> out_cpy_bufs;

#define N_BUFS 256 // per thread
#define LOOPS 1000000
// #define DEPTH 128
#include <string>

// Global variables for command line arguments with defaults
std::string INPUT_FILE = "/home/ubuntu/deflate/4K.deflate";
std::string OUTPUT_FILE = "/home/ubuntu/txt/4K.txt";
int OUTPUT_SIZE = 4096;

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
    CPU_SET(0 + thread_id * 1, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
        std::cerr << "Error calling pthread_setaffinity_np for thread " << thread_id << " on core " << (3 + thread_id)
                  << ": " << strerror(rc) << std::endl;
        return;
    }
#endif
    dpkernel_task_base *task;
    bool ret;
    ulong submitted_cnt = 0;
    ulong completed_cnt = 0;

    // Allocate input and output buffers fewer than all tasks
    shm_ptr in_buf, out_buf, in_cpy_buf, out_cpy_buf;
    for (int i = 0; i < N_BUFS; i++)
    {
        dpm_alloc_input_buf(input_file_size, &in_buf);
        if (!in_buf)
        {
            printf("Thread %d: Failed to allocate input buf\n", thread_id);
            break;
        }
        // prepare input buffer
        memcpy(app_get_input_ptr_from_shmptr(in_buf), input_buf, input_file_size);
        // prepare output buffer
        dpm_alloc_output_buf(OUTPUT_SIZE, &out_buf);
        if (!out_buf)
        {
            printf("Thread %d: Failed to allocate output buf\n", thread_id);
            break;
        }
        memcpy(app_get_output_ptr_from_shmptr(out_buf), output_buf, OUTPUT_SIZE);

#ifdef MEMCPY_NULL_KERNEL
        // alloc the cpy bufs
        dpm_alloc_input_buf(input_file_size, &in_cpy_buf);
        if (!in_cpy_buf)
        {
            printf("Thread %d: Failed to allocate input cpy buf\n", thread_id);
            break;
        }
        dpm_alloc_output_buf(OUTPUT_SIZE, &out_cpy_buf);
        if (!out_cpy_buf)
        {
            printf("Thread %d: Failed to allocate output cpy buf\n", thread_id);
            break;
        }
#endif
        in_bufs.push_back(in_buf);
        out_bufs.push_back(out_buf);
#ifdef MEMCPY_NULL_KERNEL
        in_cpy_bufs.push_back(in_cpy_buf);
        out_cpy_bufs.push_back(out_cpy_buf);
#endif
    }

    std::vector<dpkernel_task_base *> task_list;
    for (int i = 0; i < DEPTH; ++i)
    {
        // printf("Thread %d: Allocating task %d\n", thread_id, i);
        dpkernel_task_base *temp_task = nullptr;
        if (!app_alloc_task_request(&temp_task))
        {
            printf("Thread %d: Failed to allocate task %d\n", thread_id, i);
            return;
        }
        temp_task->in_size = input_file_size;
        temp_task->in = in_bufs[i % N_BUFS];
        // prepare output buffer
        temp_task->out = out_bufs[i % N_BUFS];

#ifdef MEMCPY_NULL_KERNEL
        // alloc the cpy bufs
        temp_task->in_cpy = in_cpy_bufs[i % N_BUFS];
        temp_task->out_cpy = out_cpy_bufs[i % N_BUFS];
#endif

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
      if (barrier_count == num_threads) {
        barrier_cv.notify_all();
      } else {
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
            dpm_submit_task_msgq_blocking(thread_id, task, dpm_device::DEVICE_NULL); // each thread has its own queue
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

    //// check completion of the last task, to reduce contention possibly
    task = task_list[DEPTH - 1];
    while (task->completion.load(std::memory_order_acquire) != DPK_SUCCESS)
    {
        std::this_thread::yield();
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
        if (DEPTH == 0)
        {
            DEPTH = loops_per_thread;
        }
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
    for (int i = 0; i < num_threads; i++)
    {
        threads.emplace_back(worker_thread, i, num_threads, input_buf, INPUT_SIZE);
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