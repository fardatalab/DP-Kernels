#include "common.hpp"
#include "dpm_interface.hpp"
#include "memory.hpp"
#include "memory_common.hpp"
#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
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

#define SMALL_REQ_SIZE 64
#define MED_REQ_SIZE 1 * KB
#define LARGE_REQ_SIZE 4 * KB
#define SMALL_REQ_FILE "/home/ubuntu/deflate/64B.deflate"
#define MED_REQ_FILE "/home/ubuntu/deflate/1K.deflate"
#define LARGE_REQ_FILE "/home/ubuntu/deflate/4K.deflate"

#define SMALL_REQ_WEIGHT 0.33
#define MED_REQ_WEIGHT 0.33
#define LARGE_REQ_WEIGHT 0.34

#define LOOPS 100000
// #define DEPTH 128
// #define OUTPUT_SIZE 4096
// #define INPUT_FILE "4K.deflate"

#define EXPLICIT_DEVICE
// #undef EXPLICIT_DEVICE

#define BLOCKING_SUBMIT
// #undef BLOCKING_SUBMIT

#define BACKOFF
#undef BACKOFF // NOTE: backoff is in dpm_interface.cpp

#define REQ_BATCH_SIZE 256

#ifdef BACKOFF
int backoff = 1;
constexpr int max_backoff = 1024;
#endif

// #define N_BUFS 16384 // per thread

#define PIN_THREAD
// #undef PIN_THREAD

int NUM_THREADS = 1; // default to 1 thread

std::string INPUT_FILE = "/home/ubuntu/deflate/64B.deflate";
std::string OUTPUT_FILE = "/home/ubuntu/txt/64B.txt";
int OUTPUT_SIZE = 64;
int N_BUFS; // per thread

int INPUT_SIZE;
int DEPTH;
ulong loops_per_thread;

char *input_buf = NULL;
char *output_buf = NULL;

// Global statistics
std::atomic<ulong> total_submitted(0);
std::atomic<ulong> total_completed(0);
std::atomic<double> max_thread_time(0.0);

std::atomic<int> total_small_reqs(0);
std::atomic<int> total_med_reqs(0);
std::atomic<int> total_large_reqs(0);
size_t small_req_size, med_req_size, large_req_size;

std::vector<long> all_queuing_latencies;
std::vector<long> all_completion_latencies;
std::mutex latencies_mutex;

void generate_random_requests(int thread_id, std::vector<dpkernel_task *> &task_list, int num_reqs, int *n_small_reqs,
                              int *n_med_reqs, int *n_large_reqs)
{
    task_list.clear();
    task_list.reserve(num_reqs);
    // open the small med and large input files
    std::ifstream small_file(SMALL_REQ_FILE, std::ios::binary);
    if (!small_file)
    {
        std::cerr << "Error: Cannot open small req input file " << SMALL_REQ_FILE << std::endl;
        exit(EXIT_FAILURE);
    }
    std::ifstream med_file(MED_REQ_FILE, std::ios::binary);
    if (!med_file)
    {
        std::cerr << "Error: Cannot open med req input file " << MED_REQ_FILE << std::endl;
        exit(EXIT_FAILURE);
    }
    std::ifstream large_file(LARGE_REQ_FILE, std::ios::binary);
    if (!large_file)
    {
        std::cerr << "Error: Cannot open large req input file " << LARGE_REQ_FILE << std::endl;
        exit(EXIT_FAILURE);
    }
    small_file.seekg(0, std::ios::end);
    size_t small_file_size = small_file.tellg();
    small_file.seekg(0, std::ios::beg);
    med_file.seekg(0, std::ios::end);
    size_t med_file_size = med_file.tellg();
    med_file.seekg(0, std::ios::beg);
    large_file.seekg(0, std::ios::end);
    size_t large_file_size = large_file.tellg();
    large_file.seekg(0, std::ios::beg);

    small_req_size = small_file_size;
    med_req_size = med_file_size;
    large_req_size = large_file_size;

    // read the files into buffers
    std::vector<char> small_buf(small_file_size);
    std::vector<char> med_buf(med_file_size);
    std::vector<char> large_buf(large_file_size);
    small_file.read(small_buf.data(), small_file_size);
    if (!small_file)
    {
        std::cerr << "Error: Failed to read small req input file " << SMALL_REQ_FILE << std::endl;
        exit(EXIT_FAILURE);
    }
    med_file.read(med_buf.data(), med_file_size);
    if (!med_file)
    {
        std::cerr << "Error: Failed to read med req input file " << MED_REQ_FILE << std::endl;
        exit(EXIT_FAILURE);
    }
    large_file.read(large_buf.data(), large_file_size);
    if (!large_file)
    {
        std::cerr << "Error: Failed to read large req input file " << LARGE_REQ_FILE << std::endl;
        exit(EXIT_FAILURE);
    }
    small_file.close();
    med_file.close();
    large_file.close();

    // generate random requests
    std::vector<dpm_mem_req *> in_bufs_req;
    std::vector<dpm_mem_req *> out_bufs_req;
    int n_small = 0, n_med = 0, n_large = 0;
    for (int i = 0; i < num_reqs; ++i)
    {
        dpkernel_task *alloc_task = nullptr;
        if (!app_alloc_task_request(&alloc_task))
        {
            printf("Thread %d: Failed to allocate task %d\n", thread_id, i);
            return;
        }
        dpkernel_task_base *temp_task = &alloc_task->base;

        // int req_type = rand() % 3; // 0: small, 1: med, 2: large
        int req_type;
        double rand_val = static_cast<double>(rand()) / RAND_MAX;
        if (rand_val < SMALL_REQ_WEIGHT)
            req_type = 0; // small
        else if (rand_val < SMALL_REQ_WEIGHT + MED_REQ_WEIGHT)
            req_type = 1; // med
        else
            req_type = 2; // large
        if (req_type == 0)
        {
            n_small++;
            temp_task->in_size = small_file_size;
            temp_task->out_size = SMALL_REQ_SIZE;
        }
        else if (req_type == 1)
        {
            n_med++;
            temp_task->in_size = med_file_size;
            temp_task->out_size = MED_REQ_SIZE;
        }
        else
        {
            n_large++;
            temp_task->in_size = large_file_size;
            temp_task->out_size = LARGE_REQ_SIZE;
        }

        temp_task->actual_out_size = 0;
        temp_task->task = dpm_task_name::TASK_DECOMPRESS_DEFLATE;
        temp_task->device = dpm_device::DEVICE_NONE;

        auto in_req = dpm_alloc_input_buf_async(temp_task->in_size, alloc_task);
        if (in_req == NULL)
        {
            printf("Thread %d: Failed to allocate input buf %d\n", thread_id, i);
            exit(-1);
        }
        in_bufs_req.push_back(in_req);
        auto out_req = dpm_alloc_output_buf_async(temp_task->out_size, alloc_task);
        if (out_req == NULL)
        {
            printf("Thread %d: Failed to allocate output buf %d\n", thread_id, i);
            exit(-1);
        }
        out_bufs_req.push_back(out_req);

        task_list.push_back(alloc_task);
    }
    printf("Thread %d: Allocated %d tasks\n", thread_id, num_reqs);
    // wait for all mem reqs to be done
    for (int i = 0; i < num_reqs; ++i)
    {
        auto temp_task = &task_list[i]->base;
        auto in_req = in_bufs_req[i];
        auto out_req = out_bufs_req[i];
        if (!dpm_wait_for_mem_req_completion(in_req))
        {
            printf("Thread %d: Failed for input buf %d\n", thread_id, i);
            exit(-1);
        }
        if (!dpm_wait_for_mem_req_completion(out_req))
        {
            printf("Thread %d: Failed for output buf %d\n", thread_id, i);
            exit(-1);
        }

        shm_ptr in_buf = temp_task->in;

        // memcpy the req data into the input buffer
        if (temp_task->in_size == small_file_size)
        {
            memcpy(app_get_input_ptr_from_shmptr(in_buf), small_buf.data(), small_file_size);
        }
        else if (temp_task->in_size == med_file_size)
        {
            memcpy(app_get_input_ptr_from_shmptr(in_buf), med_buf.data(), med_file_size);
        }
        else
        {
            memcpy(app_get_input_ptr_from_shmptr(in_buf), large_buf.data(), large_file_size);
        }
    }
    printf("Thread %d: Allocated %d input and output buffers\n", thread_id, num_reqs);
    *n_small_reqs = n_small;
    *n_med_reqs = n_med;
    *n_large_reqs = n_large;
    printf("Thread %d: Generated %d small, %d med, %d large requests\n", thread_id, n_small, n_med, n_large);
}

void worker_thread(int thread_id, int num_threads, char *input_buf, size_t input_file_size)
{
#ifdef PIN_THREAD
    // Pin thread to core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    int core_id = 9 + thread_id * 1;
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

    int n_small_reqs = 0, n_med_reqs = 0, n_large_reqs = 0;
    std::vector<dpkernel_task *> task_list;
    generate_random_requests(thread_id, task_list, N_BUFS, &n_small_reqs, &n_med_reqs, &n_large_reqs);
    // keep a copy, used when freeing tasks
    std::vector<dpkernel_task *> task_list_copy(task_list);

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

    for (int i = 0; i < N_BUFS; i += REQ_BATCH_SIZE)
    // for (int i = 0; i < N_BUFS; i++)
    {
        auto dpk_task = task_list[i];
        do
        {
#ifdef EXPLICIT_DEVICE
#ifdef BLOCKING_SUBMIT
            int n_tasks = N_BUFS - i < REQ_BATCH_SIZE ? N_BUFS - i : REQ_BATCH_SIZE;
            // #ifdef CHECK_LATENCY
            //             long submit_time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
            //             for (int j = 0; j < n_tasks; ++j)
            //             {
            //                 task_list[i + j]->base.submission_timestamp = submit_time;
            //             }
            // #endif
            dpm_submit_task_msgq_batched(thread_id, &task_list[i], n_tasks, DEVICE_NONE);
            // dpm_submit_task_msgq_blocking(thread_id, dpk_task,
            //   dpm_device::DEVICE_BLUEFIELD_3); // each thread has its own queue

            // dpm_submit_task_msgq_blocking(dpk_task, dpm_device::DEVICE_NONE);
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

    std::vector<dpkernel_task *> retry_tasks, completed_tasks;
    retry_tasks.reserve(DEPTH);
    completed_tasks.reserve(DEPTH);
    uint completed_task_cnt = 0, retry_task_cnt = 0; //, ongoing_task_cnt = DEPTH;

    /* // Check for tasks that are still ongoing and retry them
    for (int i = 0; i < DEPTH; ++i)
    {
        auto dpk_task = task_list[i];
        if (dpk_task == nullptr)
            continue; // skip if the task was already removed (retrying or completed)

        task = &dpk_task->base;
        if (task->completion.load(std::memory_order_acquire) == DPK_ERROR_AGAIN)
        {
            // printf("Thread %d: Task %d EAGAIN, retrying...\n", thread_id, i);
            retry_tasks.push_back(dpk_task);
            // remove it from the task list
            task_list[i] = nullptr;
            retry_task_cnt++;
        }
        else if (task->completion.load(std::memory_order_acquire) == DPK_SUCCESS)
        {
            // printf("Thread %d: Task %d completed successfully\n", thread_id, i);
#ifdef CHECK_LATENCY
            task->completion_timestamp = std::chrono::high_resolution_clock::now().time_since_epoch().count();
#endif
            completed_tasks.push_back(dpk_task);
            // remove it from the task list
            task_list[i] = nullptr;
            completed_task_cnt++;
        }
        else if (task->completion.load(std::memory_order_acquire) == DPK_ONGOING)
        {
            // normal case, nothing to do
            // printf("Thread %d: Task %d is ongoing\n", thread_id, i);
            continue;
        }
        else
        {
            // should NOT happen!!!
            printf("Thread %d: Task %d failed\n", thread_id, i);
            // remove it from the task list
            task_list[i] = nullptr;
            completed_task_cnt++;
        }
    } */

    // while some tasks are still not completed, we need to keep checking
    while (completed_task_cnt < DEPTH || retry_task_cnt > 0)
    {
        // Retry and check completion of tasks that were put into retry_tasks
        for (int i = 0; i < retry_tasks.size(); ++i)
        {
            auto dpk_task = retry_tasks[i];
            if (dpk_task == nullptr)
                continue;

            task = &dpk_task->base;
            dpkernel_error comp = app_check_task_completion(task);
            if (comp == DPK_SUCCESS)
            {
                // printf("Thread %d: Task %p completed successfully on retry\n", thread_id, dpk_task);
                // #ifdef CHECK_LATENCY
                //                 task->completion_timestamp =
                //                 std::chrono::high_resolution_clock::now().time_since_epoch().count();
                // #endif
                completed_tasks.push_back(dpk_task);
                completed_task_cnt++;
                retry_task_cnt--;
                // remove it from the task list
                retry_tasks[i] = nullptr;
            }
            else if (comp == DPK_ERROR_AGAIN)
            {
#ifdef CHECK_LATENCY
                // Resubmit the task
                dpk_task->base.submission_timestamp =
                    std::chrono::high_resolution_clock::now().time_since_epoch().count();
#endif
                dpm_submit_task_msgq_blocking(dpk_task, dpm_device::DEVICE_NONE);
            }
            else if (comp == DPK_ONGOING)
            {
                // printf("Thread %d: Task %p is still ongoing on retry\n", thread_id, dpk_task);
                for (int x = 0; x < 5000; x++)
                {
#if defined(__x86_64__) || defined(_M_X64)
                    _mm_pause();
#elif defined(__arm__) || defined(__aarch64__)
                    __asm volatile("yield");
#else
                    std::this_thread::yield();
#endif
                }
                continue; // nothing to do, just wait
            }
            else
            {
                // should NOT happen!!!
                printf("Thread %d: Task %p failed on retry\n", thread_id, dpk_task);
                completed_tasks.push_back(dpk_task);
                completed_task_cnt++;
                retry_task_cnt--;
                // remove it from the task list
                retry_tasks[i] = nullptr;
            }
        }

        // Check for completed tasks in all submitted tasks, if EAGAIN found then move them into retry_tasks
        for (int i = 0; i < DEPTH; ++i)
        {
            auto dpk_task = task_list[i];
            if (dpk_task == nullptr)
                continue; // skip if the task was already removed (retrying or completed)

            task = &dpk_task->base;
            auto comp = task->completion.load(std::memory_order_acquire);
            if (comp == DPK_SUCCESS)
            {
                // printf("Thread %d: Task %d completed successfully\n", thread_id, i);
                // #ifdef CHECK_LATENCY
                //                 task->completion_timestamp =
                //                 std::chrono::high_resolution_clock::now().time_since_epoch().count();
                // #endif
                completed_tasks.push_back(dpk_task);
                // remove it from the task list
                task_list[i] = nullptr;
                completed_task_cnt++;
            }
            else if (comp == DPK_ERROR_AGAIN)
            {
                // printf("Thread %d: Task %d EAGAIN, retrying...\n", thread_id, i);
                retry_tasks.push_back(dpk_task);
                // remove it from the task list
                task_list[i] = nullptr;
                retry_task_cnt++;
            }
            else if (comp == DPK_ONGOING)
            {
                // printf("Thread %d: Task %d is ongoing\n", thread_id, i);
                for (int x = 0; x < 5000; x++)
                {
#if defined(__x86_64__) || defined(_M_X64)
                    _mm_pause();
#elif defined(__arm__) || defined(__aarch64__)
                    __asm volatile("yield");
#else
                    std::this_thread::yield();
#endif
                }
                continue; // nothing to do, just wait
            }
            else
            {
                // should NOT happen!!!
                printf("Thread %d: Task %d failed\n", thread_id, i);
                // remove it from the task list
                task_list[i] = nullptr;
                completed_task_cnt++;
            }
        }
    }
    completed_cnt = DEPTH;

    //// check completion of the last task, to reduce contention possibly
    //     auto dpk_task = task_list[DEPTH - 1];
    //     task = &dpk_task->base;

    //     while (task->completion.load(std::memory_order_release) == DPK_ONGOING)
    //     {
    //         for (int x = 0; x < 5000; x++)
    //         {
    // #if defined(__x86_64__) || defined(_M_X64)
    //             _mm_pause();
    // #elif defined(__arm__) || defined(__aarch64__)
    //             __asm volatile("yield");
    // #else
    //                 std::this_thread::yield();
    // #endif
    //         }
    //     }
    //     // wait for all other tasks to complete
    //     for (int i = 0; i < DEPTH - 1; ++i)
    //     {
    //         auto dpk_task = task_list[i];
    //         task = &dpk_task->base;
    //         while (task->completion.load(std::memory_order_release) != DPK_SUCCESS)
    //         {
    //             for (int x = 0; x < 5000; x++)
    //             {
    // #if defined(__x86_64__) || defined(_M_X64)
    //                 _mm_pause();
    // #elif defined(__arm__) || defined(__aarch64__)
    //                 __asm volatile("yield");
    // #else
    //                     std::this_thread::yield();
    // #endif
    //             }
    //         }
    //     }

    //     completed_cnt += DEPTH;
    ////

    // End timing here
    auto thread_end = std::chrono::high_resolution_clock::now();
    // std::chrono::duration<double> submit_elapsed = submit_end_time - thread_start;
    std::chrono::duration<double> thread_elapsed = thread_end - thread_start;

    total_completed.fetch_add(completed_cnt);
    ////total_completed.fetch_add(submitted_cnt);

    total_small_reqs.fetch_add(n_small_reqs);
    total_med_reqs.fetch_add(n_med_reqs);
    total_large_reqs.fetch_add(n_large_reqs);

    // printf("thread %d submit elapsed time: %f\n", thread_id, submit_elapsed.count());

    // Update max_thread_time if this thread took longer
    double current_max = max_thread_time.load();
    // printf("thread %d submit elapsed time: %f\n", thread_id, submit_elapsed.count());
    printf("thread %d elapsed time: %f\n", thread_id, thread_elapsed.count());
    while (thread_elapsed.count() > current_max &&
           !max_thread_time.compare_exchange_weak(current_max, thread_elapsed.count()))
        ;
    // compare output buffer with the original output buffer
    int cmp_result = memcmp(output_buf, app_get_output_ptr_from_shmptr(task->out), OUTPUT_SIZE);
    if (cmp_result != 0)
    {
        printf("Thread %d: Output buffer comparison failed\n", thread_id);
    }

    // Clean up tasks
    std::vector<dpm_mem_req *> free_in_bufs_req;
    std::vector<dpm_mem_req *> free_out_bufs_req;
    printf("thread %d freeing input and output buffers\n", thread_id);
    for (int i = 0; i < DEPTH; i++)
    {
        auto temp_task = task_list_copy[i];

        free_in_bufs_req.push_back(dpm_free_input_buf_async(temp_task));
        free_out_bufs_req.push_back(dpm_free_output_buf_async(temp_task));
    }
    for (int i = 0; i < DEPTH; i++)
    {
        dpm_wait_for_mem_req_completion(free_in_bufs_req[i]);
        dpm_wait_for_mem_req_completion(free_out_bufs_req[i]);
    }
    printf("thread %d freed input and output buffers\n", thread_id);

#ifdef CHECK_LATENCY
    std::vector<long> local_queuing_latencies_ns;
    local_queuing_latencies_ns.reserve(completed_tasks.size());
    for (auto dpk_task : completed_tasks)
    {
        task = &dpk_task->base;

        long latency_ns = task->execution_timestamp - task->submission_timestamp;
        local_queuing_latencies_ns.push_back(latency_ns);
        // printf("Thread %d: Task %p completed successfully, latency: %ld ns\n", thread_id, dpk_task,
        //         latency_ns);
        // Free the task
        // app_free_task_request(dpk_task);
    }

    if (!local_queuing_latencies_ns.empty())
    {
        std::lock_guard<std::mutex> lock(latencies_mutex);
        all_queuing_latencies.insert(all_queuing_latencies.end(), local_queuing_latencies_ns.begin(),
                                     local_queuing_latencies_ns.end());
    }

    std::vector<long> local_completion_latencies_ns;
    local_completion_latencies_ns.reserve(completed_tasks.size());
    for (auto dpk_task : completed_tasks)
    {
        task = &dpk_task->base;

        long latency_ns = task->completion_timestamp - task->execution_timestamp;
        local_completion_latencies_ns.push_back(latency_ns);
        // printf("Thread %d: Task %p completed successfully, latency: %ld ns\n", thread_id, dpk_task,
        //         latency_ns);
        // Free the task
        app_free_task_request(dpk_task);
    }
    if (!local_completion_latencies_ns.empty())
    {
        std::lock_guard<std::mutex> lock(latencies_mutex);
        all_completion_latencies.insert(all_completion_latencies.end(), local_completion_latencies_ns.begin(),
                                        local_completion_latencies_ns.end());
    }
#endif
    printf("thread %d completed %lu tasks\n", thread_id, completed_cnt);
    printf("thread %d finished\n", thread_id);

    /* std::vector<dpm_mem_req *> free_in_bufs_req;
    std::vector<dpm_mem_req *> free_out_bufs_req;
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
    printf("thread %d finished\n", thread_id); */
}

int main(int argc, char *argv[])
{
    if (argc > 1)
    {
        NUM_THREADS = std::atoi(argv[1]);
    }

    N_BUFS = (size_t)1 * GB / OUTPUT_SIZE / NUM_THREADS;
    // if (N_BUFS > LOOPS)
    {
        N_BUFS = LOOPS / NUM_THREADS;
    }

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
    // printf("throughput (MB/s): %.6f\n", (double)total_completed.load() * INPUT_SIZE / elapsed_time / 1024 / 1024);
    size_t total_input_size = (size_t)total_small_reqs.load() * small_req_size +
                              (size_t)total_med_reqs.load() * med_req_size +
                              (size_t)total_large_reqs.load() * large_req_size;
    size_t total_output_size = (size_t)total_small_reqs.load() * SMALL_REQ_SIZE +
                              (size_t)total_med_reqs.load() * MED_REQ_SIZE +
                              (size_t)total_large_reqs.load() * LARGE_REQ_SIZE;
    double throughput_mbps = (double)total_output_size / elapsed_time / 1024 / 1024; // MB/s
    printf("Total small requests: %d\n", total_small_reqs.load());
    printf("Total medium requests: %d\n", total_med_reqs.load());
    printf("Total large requests: %d\n", total_large_reqs.load());
    printf("input sizes are: small: %zu, medium: %zu, large: %zu\n", small_req_size, med_req_size, large_req_size);

    printf("throughput (MB/s): %.6f\n", throughput_mbps);

#ifdef CHECK_LATENCY
    if (!all_queuing_latencies.empty() && !all_completion_latencies.empty())
    {
        std::sort(all_queuing_latencies.begin(), all_queuing_latencies.end());

        long long sum_latencies_ns = 0;
        for (long lat_ns : all_queuing_latencies)
        {
            sum_latencies_ns += lat_ns;
        }
        double mean_queuing_latency_ns = static_cast<double>(sum_latencies_ns) / all_queuing_latencies.size();

        // Percentile calculation using (N-1)*p method for index
        auto get_percentile_ns = [&](double p) {
            if (all_queuing_latencies.empty())
                return 0L;
            // Ensure p is within [0,1]
            double clamped_p = std::max(0.0, std::min(1.0, p));
            size_t index = static_cast<size_t>(std::floor(clamped_p * (all_queuing_latencies.size() - 1)));
            // Clamp index to be safe, though floor should handle it for p <= 1.0
            index = std::min(index, all_queuing_latencies.size() - 1);
            return all_queuing_latencies[index];
        };

        long queuing_p50_latency_ns = get_percentile_ns(0.50);
        long queuing_p99_latency_ns = get_percentile_ns(0.99);
        long queuing_p999_latency_ns = get_percentile_ns(0.999);

        std::sort(all_completion_latencies.begin(), all_completion_latencies.end());
        long long sum_completion_latencies_ns = 0;
        for (long lat_ns : all_completion_latencies)
        {
            sum_completion_latencies_ns += lat_ns;
        }
        double mean_completion_latency_ns =
            static_cast<double>(sum_completion_latencies_ns) / all_completion_latencies.size();
        // Percentile calculation using (N-1)*p method for index
        auto get_completion_percentile_ns = [&](double p) {
            if (all_completion_latencies.empty())
                return 0L;
            // Ensure p is within [0,1]
            double clamped_p = std::max(0.0, std::min(1.0, p));
            size_t index = static_cast<size_t>(std::floor(clamped_p * (all_completion_latencies.size() - 1)));
            // Clamp index to be safe, though floor should handle it for p <= 1.0
            index = std::min(index, all_completion_latencies.size() - 1);
            return all_completion_latencies[index];
        };
        long completion_p50_latency_ns = get_completion_percentile_ns(0.50);
        long completion_p99_latency_ns = get_completion_percentile_ns(0.99);
        long completion_p999_latency_ns = get_completion_percentile_ns(0.999);

        printf("\n---Latency Statistics---\n");
        // printf("Total latency samples: %zu\n", all_latencies.size());

        // printf("\nIn Nanoseconds (ns):\n");
        // printf("  Mean: %.3f ns\n", mean_latency_ns);
        // printf("  p50 (Median): %ld ns\n", p50_latency_ns);
        // printf("  p99: %ld ns\n", p99_latency_ns);
        // printf("  p99.9: %ld ns\n", p999_latency_ns);

        printf("\n queuing latency (us):\n");
        printf("  Mean: %.3f us\n", mean_queuing_latency_ns / 1000.0);
        printf("  p50 (Median): %.3f us\n", static_cast<double>(queuing_p50_latency_ns) / 1000.0);
        printf("  p99: %.3f us\n", static_cast<double>(queuing_p99_latency_ns) / 1000.0);
        printf("  p99.9: %.3f us\n", static_cast<double>(queuing_p999_latency_ns) / 1000.0);

        printf("\n completion latency (us):\n");
        printf("  Mean: %.3f us\n", mean_completion_latency_ns / 1000.0);
        printf("  p50 (Median): %.3f us\n", static_cast<double>(completion_p50_latency_ns) / 1000.0);
        printf("  p99: %.3f us\n", static_cast<double>(completion_p99_latency_ns) / 1000.0);
        printf("  p99.9: %.3f us\n", static_cast<double>(completion_p999_latency_ns) / 1000.0);

        // printf("\nIn Milliseconds (ms):\n");
        // printf("  Mean: %.3f ms\n", mean_latency_ns / 1000000.0);
        // printf("  p50 (Median): %.3f ms\n", static_cast<double>(p50_latency_ns) / 1000000.0);
        // printf("  p99: %.3f ms\n", static_cast<double>(p99_latency_ns) / 1000000.0);
        // printf("  p99.9: %.3f ms\n", static_cast<double>(p999_latency_ns) / 1000000.0);
    }
    else
    {
        printf("No latency data collected...\n");
    }
#else
    // printf("\nLatency statistics not available: CHECK_LATENCY macro was not defined.\n");
#endif

    return 0;
}
