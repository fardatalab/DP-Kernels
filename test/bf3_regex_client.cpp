#include "common.hpp"
#include "dpm_interface.hpp"
#include "memory.hpp"
#include "memory_common.hpp"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#endif
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "duckdb_regex/regduck_api.hpp"

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

#ifdef BACKOFF
int backoff = 1;
constexpr int max_backoff = 1024;
#endif

// #define N_BUFS 16384 // per thread

#define PIN_THREAD
// #undef PIN_THREAD

int NUM_THREADS = 1; // default to 1 thread

std::string INPUT_FILE = "/home/ubuntu/regex_files/lineitem.tbl";

int INPUT_SIZE = 16 * KB; // max size per batch

int OUTPUT_SIZE = 16 * KB;
int N_BUFS; // per thread

int DEPTH;
ulong loops_per_thread;

char *input_buf = NULL;
char *output_buf = NULL;

// Global statistics
std::atomic<ulong> total_submitted(0);
std::atomic<ulong> total_completed(0);
std::atomic<double> max_thread_time(0.0); // Add this line

void worker_thread(int thread_id, int num_threads, char **input_string_bufs, int n_bufs, int *buf_sizes)
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
    std::chrono::time_point<std::chrono::high_resolution_clock> alloc_buf_time_start, alloc_buf_time_end,
        alloc_task_time_start, alloc_task_time_end;

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
        // temp_task->in_size = INPUT_SIZE;
        temp_task->in_size = buf_sizes[i];
        temp_task->out_size = OUTPUT_SIZE;
        temp_task->actual_out_size = 0;
        temp_task->task = dpm_task_name::TASK_REGEX_SEARCH;
        temp_task->device = dpm_device::DEVICE_BLUEFIELD_3;

        auto in_req = dpm_alloc_input_buf_async(buf_sizes[i], alloc_task);
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

        in_buf = temp_task->in;
        memcpy(app_get_input_ptr_from_shmptr(temp_task->in), input_string_bufs[i], buf_sizes[i]);
        // no need to copy the output buffer here
        // out_buf = temp_task->out;
        // memcpy(app_get_output_ptr_from_shmptr(temp_task->out), output_buf, OUTPUT_SIZE);
#ifdef MEMCPY_KERNEL
        in_cpy_buf = temp_task->in_cpy;

        out_cpy_buf = temp_task->out_cpy;
#endif
        in_bufs.push_back(in_buf);
        // out_bufs.push_back(out_buf);
#ifdef MEMCPY_KERNEL
        in_cpy_bufs.push_back(in_cpy_buf);
        out_cpy_bufs.push_back(out_cpy_buf);
#endif
    }
    printf("Thread %d: Allocated %d input and output buffers\n", thread_id, N_BUFS);

    /* for (int i = N_BUFS; i < DEPTH; ++i)
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
        temp_task->task = dpm_task_name::TASK_DECOMPRESS_DEFLATE;
        temp_task->device = dpm_device::DEVICE_BLUEFIELD_3;

        temp_task->in = in_bufs[i % N_BUFS];
        temp_task->out = out_bufs[i % N_BUFS];
#ifdef MEMCPY_KERNEL
        temp_task->in_cpy = in_cpy_bufs[i % N_BUFS];
        temp_task->out_cpy = out_cpy_bufs[i % N_BUFS];
#endif
        task_list.push_back(alloc_task);
    } */

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
            dpm_submit_task_msgq_blocking(thread_id, dpk_task,
                                          dpm_device::DEVICE_BLUEFIELD_3); // each thread has its own queue
            // dpm_submit_task_msgq_blocking(dpk_task, dpm_device::DEVICE_BLUEFIELD_3);
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
    }
    // wait for all other tasks to complete
    for (int i = 0; i < DEPTH - 1; ++i)
    {
        auto dpk_task = task_list[i];
        task = &dpk_task->base;
        while (task->completion.load(std::memory_order_release) != DPK_SUCCESS)
        {
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
        }
    }

    completed_cnt += DEPTH;

    // End timing here
    auto thread_end = std::chrono::high_resolution_clock::now();
    // std::chrono::duration<double> submit_elapsed = submit_end_time - thread_start;
    std::chrono::duration<double> thread_elapsed = thread_end - thread_start;

    total_completed.fetch_add(completed_cnt);
    ////total_completed.fetch_add(submitted_cnt);

    // printf("thread %d submit elapsed time: %f\n", thread_id, submit_elapsed.count());

    // Update max_thread_time if this thread took longer
    double current_max = max_thread_time.load();
    // printf("thread %d submit elapsed time: %f\n", thread_id, submit_elapsed.count());
    printf("thread %d elapsed time: %f\n", thread_id, thread_elapsed.count());
    while (thread_elapsed.count() > current_max &&
           !max_thread_time.compare_exchange_weak(current_max, thread_elapsed.count()))
        ;
    // print number of matches found in total, aggregate over all ouput buffers
    int total_matches = 0;
    for (int i = 0; i < DEPTH; ++i)
    {
        auto dpk_task = task_list[i];
        task = &dpk_task->base;
        uint32_t matches = *((uint32_t *)app_get_output_ptr_from_shmptr(task->out));
        total_matches += matches;
    }
    printf("thread %d found %d matches\n", thread_id, total_matches);

    // Clean up tasks
    std::vector<dpm_mem_req *> free_in_bufs_req;
    std::vector<dpm_mem_req *> free_out_bufs_req;

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
    // INPUT_SIZE = static_cast<int>(file_size);
    file.seekg(0, std::ios::beg);
    input_buf = new char[static_cast<int>(file_size)];
    if (!file.read(input_buf, file_size))
    {
        std::cerr << "Error: Failed to read file data." << std::endl;
        delete[] input_buf;
        exit(EXIT_FAILURE);
    }
    file.close();

    // Allocate input string buffers, each should be max of INPUT_SIZE
    // N_BUFS = file_size / INPUT_SIZE;
    // char **input_string_bufs = new char *[N_BUFS];
    // int *buf_sizes = new int[N_BUFS];

    N_BUFS = 0;
    std::vector<char *> parsed_strings = {};
    std::vector<int> string_buf_sizes = {};

    char *string_buf = new char[INPUT_SIZE];
    size_t string_size = load_strings_in_batch(string_buf, INPUT_SIZE - 1);
    while (string_size > 0)
    {
        string_buf[string_size] = '\0';
        // printf("string content: %s\n", string_buf);

        parsed_strings.push_back(string_buf);
        string_buf_sizes.push_back(INPUT_SIZE);
        N_BUFS++;

        string_buf = new char[INPUT_SIZE];

        string_size = load_strings_in_batch(string_buf, INPUT_SIZE - 1);
    }

    int average_string_buf_size = 0;
    for (int i = 0; i < N_BUFS; i++)
    {
        average_string_buf_size += string_buf_sizes[i];
    }
    average_string_buf_size /= N_BUFS;
    printf("duckdb parser produced N_BUFS: %d with average chunk size = %d\n", N_BUFS, average_string_buf_size);
    // for (int i = 0; i < N_BUFS; i++)
    // {
    //     input_string_bufs[i] = new char[INPUT_SIZE];
    //     buf_sizes[i] = INPUT_SIZE;
    //     memcpy(input_string_bufs[i], input_buf + i * INPUT_SIZE, INPUT_SIZE);
    // }

    if (argc > 1)
    {
        NUM_THREADS = std::atoi(argv[1]);
    }

    // N_BUFS = (size_t)1 * GB / OUTPUT_SIZE / NUM_THREADS;
    // // if (N_BUFS > LOOPS)
    // {
    //     N_BUFS = LOOPS / NUM_THREADS;
    // }

    // Each thread handles LOOPS/num_threads iterations
    loops_per_thread = LOOPS / NUM_THREADS;

    if (argc > 2)
    {
        DEPTH = std::atoi(argv[2]);
    }
    else
    {
        ////DEPTH = loops_per_thread;
        DEPTH = N_BUFS;
    }
    printf("DEPTH: %d\n", DEPTH);

    if (argc > 3)
    {
        INPUT_FILE = argv[3];
    }
    // if (argc > 4)
    // {
    //     OUTPUT_FILE = argv[4];
    // }
    // if (argc > 5)
    // {
    //     OUTPUT_SIZE = std::atoi(argv[5]);
    // }

    // std::ifstream outfile(OUTPUT_FILE, std::ios::binary);
    // if (!outfile)
    // {
    //     std::cerr << "Error: Cannot open file " << OUTPUT_FILE << std::endl;
    //     exit(EXIT_FAILURE);
    // }
    // output_buf = new char[OUTPUT_SIZE];
    // outfile.read(output_buf, OUTPUT_SIZE);
    // if (!outfile)
    // {
    //     std::cerr << "Error: Only " << outfile.gcount() << " bytes could be read from " << OUTPUT_FILE << std::endl;
    // }
    // outfile.close();

    // Initialize DPM
    dpm_frontend_initialize();
    printf("dpm init completed\n");

    std::vector<std::thread> threads;

    // Create threads
    for (int i = 0; i < NUM_THREADS; i++)
    {
        // threads.emplace_back(worker_thread, i, NUM_THREADS, input_string_bufs, N_BUFS, buf_sizes);
        threads.emplace_back(worker_thread, i, NUM_THREADS, parsed_strings.data(), N_BUFS, string_buf_sizes.data());
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
    printf("throughput (MB/s): %.6f\n", N_BUFS * INPUT_SIZE * NUM_THREADS / elapsed_time / 1024 / 1024);

    return 0;
}
