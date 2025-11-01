#include "dpm_interface.hpp"
#include <atomic>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#define LOOPS 1000000
#define DEPTH 1
#define OUTPUT_SIZE 4096
#define INPUT_FILE "4K.deflate"

int INPUT_SIZE = 4096;
char *input_buf = NULL;

/// submit everything then complete everything

std::atomic<ulong> total_submitted(0);
std::atomic<ulong> total_completed(0);
std::atomic<double> max_thread_time(0.0);

void worker_thread(int thread_id, int num_threads)
{
    dpkernel_task_base *task;
    bool ret;
    ulong submitted_cnt = 0;
    ulong completed_cnt = 0;

    // Each thread handles LOOPS/num_threads iterations
    const ulong loops_per_thread = LOOPS / num_threads;

    shm_ptr in;
    shm_ptr out;
    ret = dpm_alloc_input_buf(INPUT_SIZE, &in);
    if (!ret)
    {
        printf("Thread %d: Failed to allocate input buf\n", thread_id);
        return;
    }

    ret = dpm_alloc_output_buf(OUTPUT_SIZE, &out);
    if (!ret)
    {
        printf("Thread %d: Failed to allocate output buf\n", thread_id);
        return;
    }

    dpkernel_task_base *task_list[loops_per_thread];
    for (int i = 0; i < loops_per_thread; ++i)
    {
        dpkernel_task_base *temp_task = nullptr;
        if (!app_alloc_task_request(&temp_task))
        {
            printf("Thread %d: Failed to allocate task %d\n", thread_id, i);
            return;
        }
        temp_task->in_size = INPUT_SIZE;
        temp_task->out_size = OUTPUT_SIZE;
        temp_task->name = dpkernel_name::NULL_KERNEL;

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
        temp_task->in = in;
        temp_task->out = out;
        task_list[i] = temp_task;
    }

    // Start timing here
    auto thread_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < loops_per_thread; i++)
    {
        while (dpm_submit_task_msgq(task_list[i]) == false)
        {
            // std::this_thread::yield();
        }

        submitted_cnt++;
        total_submitted++;
    }
    printf("thread %d submitted\n", thread_id);

    for (int i = 0; i < loops_per_thread; i++)
    {
        while (task_list[i]->completion.load() == DPK_ONGOING)
        {
            // std::this_thread::yield();
        }
        completed_cnt++;
        total_completed++;
    }
    printf("thread %d completed\n", thread_id);

    // for (int i = 0; i < DEPTH; ++i)
    // {
    //     task = task_list[i];
    //     ret = dpm_submit_task_msgq(task);
    //     if (!ret)
    //     {
    //         printf("Thread %d: Failed to submit task\n", thread_id);
    //         break;
    //     }
    //     else
    //     {
    //         submitted_cnt++;
    //         total_submitted++;
    //     }
    // }

    // while (completed_cnt < loops_per_thread)
    // {
    //     for (int i = 0; i < DEPTH; ++i)
    //     {
    //         task = task_list[i];
    //         if (task->completion.load() == DPK_SUCCESS)
    //         {
    //             completed_cnt++;
    //             total_completed++;

    //             /* if (completed_cnt >= loops_per_thread)
    //             {
    //                 break;
    //             } */

    //             if (submitted_cnt < loops_per_thread)
    //             {
    //                 ret = dpm_submit_task_msgq(task);
    //                 if (!ret)
    //                 {
    //                     printf("Thread %d: Failed to submit task\n", thread_id);
    //                     break;
    //                 }
    //                 else
    //                 {
    //                     submitted_cnt++;
    //                     total_submitted++;
    //                 }
    //             }
    //         }
    //     }
    // }

    // End timing here
    auto thread_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> thread_elapsed = thread_end - thread_start;

    // Update max_thread_time if this thread took longer
    double current_max = max_thread_time.load();
    while (thread_elapsed.count() > current_max &&
           !max_thread_time.compare_exchange_weak(current_max, thread_elapsed.count()))
        ;

    // Clean up tasks
    for (int i = 0; i < loops_per_thread; i++)
    {
        // get local ptr from shm ptr of in and out buffers
        char *in = app_get_input_ptr_from_shmptr(task_list[i]->in);
        char *out = app_get_output_ptr_from_shmptr(task_list[i]->out);
        dpm_free_input_buf(in);
        dpm_free_output_buf(out);
    }
    printf("thread %d finished\n", thread_id);
}

int main(int argc, char *argv[])
{
    int num_threads = 1; // default to 1 thread
    if (argc > 1)
    {
        num_threads = std::atoi(argv[1]);
    }

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

    printf("Total submitted tasks: %lu\n", total_submitted.load());
    printf("Total completed tasks: %lu\n", total_completed.load());
    std::cout << "Max thread time: " << elapsed_time << " s\n";

    double throughput_million_ops = (double)total_completed.load() / elapsed_time / 1000000;
    printf("Throughput: %.6f million ops/s\n", throughput_million_ops);

    return 0;
}