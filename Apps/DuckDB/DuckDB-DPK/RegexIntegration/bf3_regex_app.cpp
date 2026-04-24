#include "common.hpp"
#include "dpm_interface.hpp"
#include "memory.hpp"
#include "memory_common.hpp"
#include "Parser/src/interface.h"
#include <chrono>
#include <fstream>
#include <numeric>
#include <thread>
#include <vector>

#define PROCESS_TASKS_BATCHED
// #define PROCESS_TASKS_BLOCKING

#define MAX_DPK_BUF_CNT (5000)
#define SUBMISSION_BATCH_SIZE (64)
#define STRINGS_BUF_SIZE (16 * 1024)
#define LENGTHS_BUF_SIZE (1024)
#define DPK_INPUT_BUF_SIZE (16 * 1024)
#define DPK_OUTPUT_BUF_SIZE (8)
#define DUCKDB_DB_FILE "/home/ubuntu/dbfiles/sf50.db"

static size_t data_size = 0;
//static size_t n_bufs = 0;
//static size_t n_threads = 1;
//static size_t pipeline_depth = 1;
//static size_t n_batches = 1;

static char** contig_strings_ptr;
// std::vector<char *> contig_strings = {};
std::vector<std::vector<dpm_mem_req*>> input_buf_alloc_reqs;
std::vector<std::vector<dpm_mem_req*>> output_buf_alloc_reqs;
std::vector<std::vector<dpm_mem_req*>> input_buf_dealloc_reqs;
std::vector<std::vector<dpm_mem_req*>> output_buf_dealloc_reqs;
std::vector<std::vector<dpkernel_task*>> per_thread_task_lists;
std::vector<shm_ptr> in_bufs;

std::chrono::duration<double> execution_elapsed(0.0);

void duckdb_load_strings(char* path);
void dpm_allocate_bufs(size_t tid, size_t n_bufs);
void copy_input_to_dpm_inbufs(size_t tid, size_t start_buf, size_t end_buf);
void dpm_deallocate_bufs(size_t tid, size_t bid, size_t n_bufs);

void worker_func(size_t tid, size_t bid, size_t start_buf, size_t end_buf, size_t* result) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    int core_id = 8 + tid;
    CPU_SET(core_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset)) {
        printf("error: can't pin thread %lu to core %d\n", tid, core_id);
        exit(1);
    }
    size_t n_bufs = end_buf - start_buf;
    dpm_allocate_bufs(tid, n_bufs);
    copy_input_to_dpm_inbufs(tid, start_buf, end_buf);

	auto start = std::chrono::high_resolution_clock::now();
#ifdef PROCESS_TASKS_BATCHED
    // Submit tasks in batches
    for (size_t i = 0; i < n_bufs; i += SUBMISSION_BATCH_SIZE) {
        size_t n_tasks = (n_bufs-i < SUBMISSION_BATCH_SIZE) ? n_bufs-i : SUBMISSION_BATCH_SIZE;
        dpkernel_task** task_batch = &per_thread_task_lists[tid][i];
        dpm_submit_task_msgq_batched(tid, task_batch, n_tasks, dpm_device::DEVICE_NONE);
    }
    // Wait for task completion
    dpkernel_task_base* last_task_in_batch = &per_thread_task_lists[tid][n_bufs-1]->base;
    while (last_task_in_batch->completion.load(std::memory_order_release) != DPK_SUCCESS) {
        std::this_thread::yield();
    }
    /// @note Also wait for other tasks in the same batch to finish
    // (otherwise the allocated space would be assigned to other buffers, causing segfault)
    for (size_t i = 0; i < n_bufs-1; i++) {
        dpkernel_task_base* task = &per_thread_task_lists[tid][i]->base;
        while (task->completion.load(std::memory_order_release) != DPK_SUCCESS) {
            std::this_thread::yield();
        }
    }
#else
    // Blocking submit
    for (size_t i = 0; i < n_bufs; i++) {
        dpm_submit_task_msgq_blocking(tid, per_thread_task_lists[tid][i], dpm_device::DEVICE_NONE);
    }
    // Poll task completion
    for (size_t i = 0; i < n_bufs; i++) {
        dpkernel_task* task = per_thread_task_lists[tid][i];
        while (task->base.completion.load(std::memory_order_release) != DPK_SUCCESS) {
            std::this_thread::yield();
        }
    }
#endif
    size_t num_matches = 0;
    // Collect total number of matches for this batch of buffers
	for (size_t i = 0; i < n_bufs; i++) {
		auto task = &per_thread_task_lists[tid][i]->base;
		shm_ptr out_shmptr = task->out;
		char* out_ptr = app_get_output_ptr_from_shmptr(out_shmptr);
		uint32_t partial_match_count = *reinterpret_cast<uint32_t*>(out_ptr);
		num_matches += partial_match_count;
	}
	printf("%lu matches found in batch %lu\n", num_matches, bid);
	*result = num_matches;
	auto end = std::chrono::high_resolution_clock::now();
	execution_elapsed += (end - start);
    // Free up space for next batch's buffer allocation
    dpm_deallocate_bufs(tid, bid, n_bufs);
    input_buf_alloc_reqs[tid].clear();
    output_buf_alloc_reqs[tid].clear();
    input_buf_dealloc_reqs[tid].clear();
    output_buf_dealloc_reqs[tid].clear();
    per_thread_task_lists[tid].clear();
}

extern "C" bool bf3_regex_app_driver(char** contig_strings, size_t n_bufs, size_t num_threads, size_t pipeline_depth) {
    contig_strings_ptr = contig_strings;
    
	// Load strings into memory
    //n_threads = num_threads;
    //depth = (pipeline_depth > 0) ? pipeline_depth : n_bufs;
    // n_bufs = num_bufs;
    printf("strings loaded into memory, %lu contiguous strings produced\n", n_bufs);

    input_buf_alloc_reqs.resize(num_threads);
    output_buf_alloc_reqs.resize(num_threads);
    input_buf_dealloc_reqs.resize(num_threads);
    output_buf_dealloc_reqs.resize(num_threads);
    per_thread_task_lists.resize(num_threads);
    // printf("n_threads: %lu\n", n_threads);
    
    // (De)Allocate in/output buffers in batches
    size_t n_batches = n_bufs / MAX_DPK_BUF_CNT;
    if (n_batches * MAX_DPK_BUF_CNT < n_bufs) {
        n_batches++;
    }
    size_t last_batch_size = n_bufs - (n_batches-1) * MAX_DPK_BUF_CNT;
    printf("no. batches: %lu\n", n_batches);
    
    // Intialize dpm frontend
    dpm_frontend_initialize();
    printf("dpm frontend initialized\n");

    std::vector<size_t> per_batch_match_counts(n_batches);
    for (size_t b = 0; b < n_batches; b++) {
        // printf("batch %lu\n", b);
        std::vector<std::thread> threads;
        size_t batch_size = (b == n_batches-1) ? last_batch_size : MAX_DPK_BUF_CNT;
        size_t start_buf = b * MAX_DPK_BUF_CNT;
        size_t end_buf = start_buf + batch_size;
        size_t workload = batch_size / num_threads;
        for (size_t i = 0; i < num_threads; i++) {
            size_t start = start_buf + i * workload;
            size_t end = (i == num_threads-1) ? end_buf : start_buf + (i+1) * workload;
            threads.emplace_back(worker_func, i, b, start, end, &per_batch_match_counts[b]);
        }
        for (auto& t : threads) {
            t.join();
        }
    }
	size_t total_match_count = static_cast<size_t>(std::accumulate(per_batch_match_counts.begin(), per_batch_match_counts.end(), 0));
    double latency_sec = execution_elapsed.count();
	printf("regex kernel found %lu matches\n", total_match_count);
	printf("regex kernel execution latency: %.6f sec\n", latency_sec);
    printf("regex kernel throughput: %.6f GB/sec\n", data_size * 1.0 / (1 * GB) / latency_sec);
    
    return 0;
}

//void duckdb_load_strings(char* path) {
//    db_init(path);
//    while (1) {
//        char* strings_buf = new char[STRINGS_BUF_SIZE];
//        uint8_t* lengths_buf = new uint8_t[LENGTHS_BUF_SIZE];
//        size_t size = load_strings_in_batch(path, STRINGS_BUF_SIZE, strings_buf, lengths_buf);
//        if (size == 0) {
//            break;
//        }
//        contig_strings.push_back(strings_buf);
//        n_bufs++;
//    	for (size_t i = 0; i < size; i++) {
//			data_size += lengths_buf[i];
//		}
//	}
//	printf("data size: %.6f GB\n", data_size * 1.0 / (1 * GB));
//}

void dpm_allocate_bufs(size_t tid, size_t n_bufs) {
    for (size_t i = 0; i < n_bufs; i++) {
        // Allocate a task and fill in its metadata
        dpkernel_task* alloc_task = nullptr;
        if (!app_alloc_task_request(&alloc_task)) {
            printf("[thread %lu] failed to allocate task %lu\n", tid, i);
            exit(1);
        }
        dpkernel_task_base* temp_task = &alloc_task->base;
        temp_task->task = dpm_task_name::TASK_REGEX_SEARCH;
        temp_task->device = dpm_device::DEVICE_NONE;
        temp_task->in_size = DPK_INPUT_BUF_SIZE;
        temp_task->out_size = DPK_OUTPUT_BUF_SIZE;
        // Allocate dpm-managed in/output buffers
        auto in_req = dpm_alloc_input_buf_async(temp_task->in_size, alloc_task);
        if (in_req == nullptr) {
            printf("[thread %lu] failed to allocate input buffer %lu\n", tid, i);
            exit(1);
        }
        input_buf_alloc_reqs[tid].push_back(in_req);
        auto out_req = dpm_alloc_output_buf_async(temp_task->out_size, alloc_task);
        if (out_req == nullptr) {
            printf("[thread %lu] failed to allocate output buffer %lu\n", tid, i);
            exit(1);
        }
        output_buf_alloc_reqs[tid].push_back(out_req);
        per_thread_task_lists[tid].push_back(alloc_task);
    }
}

void copy_input_to_dpm_inbufs(size_t tid, size_t start_buf, size_t end_buf) {
    for (size_t i = start_buf; i < end_buf; i++) {
        // Wait for allocation tasks to finish before populating input
        size_t req_idx = i - start_buf;
        auto temp_task = &per_thread_task_lists[tid][req_idx]->base;
        auto in_req = input_buf_alloc_reqs[tid][req_idx];
        auto out_req = output_buf_alloc_reqs[tid][req_idx];
        if (!dpm_wait_for_mem_req_completion(in_req)) {
            printf("[thread %lu] input buffer %lu can't be allocated\n", tid, req_idx);
            exit(1);
        }
        if (!dpm_wait_for_mem_req_completion(out_req)) {
            printf("[thread %lu] output buffer %lu can't be allocated\n", tid, req_idx);
            exit(1);
        }
        shm_ptr inptr = temp_task->in;
        char* strings = contig_strings_ptr[i];
        memcpy(app_get_input_ptr_from_shmptr(inptr), strings, strlen(strings));
        delete[] strings;
    }
}

void dpm_deallocate_bufs(size_t tid, size_t bid, size_t n_bufs) {
	for (size_t i = 0; i < n_bufs; i++) {
        auto temp_task = per_thread_task_lists[tid][i];
        /// @note: DO NOT DO THIS to avoid running out of memory (this is telling dpm NOT to free the buffers!)
        // temp_task->base.in = 0;
        // temp_task->base.out = 0;
        auto in_dealloc_req = dpm_free_input_buf_async(temp_task);
        if (in_dealloc_req == nullptr) {
            printf("[thread %lu] failed to submit request deallocate input buffer %lu\n", tid, i);
            exit(1);
        }
        input_buf_dealloc_reqs[tid].push_back(in_dealloc_req);
        auto out_dealloc_req = dpm_free_output_buf_async(temp_task);
        if (out_dealloc_req == nullptr) {
            printf("[thread %lu] failed to submit request to deallocate output buffer %lu\n", tid, i);
            exit(1);
        }
        output_buf_dealloc_reqs[tid].push_back(out_dealloc_req);
    }
    for (size_t i = 0; i < n_bufs; i++) {
        if (!dpm_wait_for_mem_req_completion(input_buf_dealloc_reqs[tid][i])) {
            printf("[thread %lu] failed to deallocate input buffer %lu\n", tid, i);
            exit(1);
        }
        if (!dpm_wait_for_mem_req_completion(output_buf_dealloc_reqs[tid][i])) {
            printf("[thread %lu] failed to deallocate output buffer %lu\n", tid, i);
            exit(1);
        }
	}
}
