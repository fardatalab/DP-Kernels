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
#define DUCKDB_DB_FILE "/home/ubuntu/dbfiles/sf1.db"

static size_t data_size = 0;
static size_t n_bufs = 0;
static size_t n_threads = 1;
static size_t pipeline_depth = 1;
static size_t n_batches = 1;

std::vector<char *> contig_strings = {};
std::chrono::duration<double> execution_elapsed(0.0);

void duckdb_load_strings(char* path);

void worker_func_coal(size_t bid, shm_ptr in_region, shm_ptr out_region, size_t nbufs) {
    std::vector<dpm_mem_req *> in_bufs_req;
    std::vector<dpm_mem_req *> out_bufs_req;
    std::vector<dpkernel_task *> task_list;

    for (size_t i = 0; i < nbufs; i++) {
        dpkernel_task* alloc_task = nullptr;
        app_alloc_task_request(&alloc_task);
        dpkernel_task_base *temp_task = &alloc_task->base;
        temp_task->in_size = DPK_INPUT_BUF_SIZE;
        temp_task->out_size = DPK_OUTPUT_BUF_SIZE;
        temp_task->task = dpm_task_name::TASK_REGEX_SEARCH;
        temp_task->device = dpm_device::DEVICE_BLUEFIELD_3;
        temp_task->in = in_region + i * DPK_INPUT_BUF_SIZE;
        temp_task->out = out_region + i * DPK_OUTPUT_BUF_SIZE;

        auto in_req = dpm_alloc_input_buf_async(DPK_INPUT_BUF_SIZE, alloc_task);
        auto out_req = dpm_alloc_output_buf_async(DPK_OUTPUT_BUF_SIZE, alloc_task);
        in_bufs_req.push_back(in_req);
        out_bufs_req.push_back(out_req);
        task_list.push_back(alloc_task);
    }
    for (size_t i = 0; i < nbufs; i++) {
        auto temp_task = &task_list[i]->base;
        auto in_req = in_bufs_req[i];
        auto out_req = out_bufs_req[i];
        dpm_wait_for_mem_req_completion(in_req);
        dpm_wait_for_mem_req_completion(out_req);
    }
    for (size_t i = 0; i < nbufs; i += SUBMISSION_BATCH_SIZE) {
        size_t ntasks = (nbufs - i < SUBMISSION_BATCH_SIZE) ? nbufs - i : SUBMISSION_BATCH_SIZE;
        dpm_submit_task_msgq_batched(0, &task_list[i], ntasks, DEVICE_BLUEFIELD_3);
    }
    auto dpk_task = task_list[nbufs - 1];
    auto task = &dpk_task->base;
    while (task->completion.load(std::memory_order_release) != DPK_SUCCESS) {
        std::this_thread::yield();
    }
    for (size_t i = 0; i < nbufs - 1; i++) {
        dpk_task = task_list[i];
        task = &dpk_task->base;
        while (task->completion.load(std::memory_order_release) != DPK_SUCCESS) {
            std::this_thread::yield();
        }   
    }
    size_t total_matches = 0;
    for (size_t i = 0; i < nbufs; i++) {
        dpk_task = task_list[i];
        task = &dpk_task->base;
        uint32_t matches = *((uint32_t *)app_get_output_ptr_from_shmptr(task->out));
        total_matches += matches;
    }
    printf("%lu matches found in batch %lu\n", total_matches, bid);

    std::vector<dpm_mem_req *> free_in_bufs_req;
    std::vector<dpm_mem_req *> free_out_bufs_req;
    for (int i = 0; i < nbufs; i++) {
        auto temp_task = task_list[i];
        temp_task->base.out = 0;
        temp_task->base.in = 0;
        free_in_bufs_req.push_back(dpm_free_input_buf_async(temp_task));
        free_out_bufs_req.push_back(dpm_free_output_buf_async(temp_task));
    }
    for (int i = 0; i < nbufs; i++) {
        dpm_wait_for_mem_req_completion(free_in_bufs_req[i]);
        dpm_wait_for_mem_req_completion(free_out_bufs_req[i]);
    }
}

int main(int argc, char** argv) {
    //// Get file size
    //std::ifstream db_file(DUCKDB_DB_FILE, std::ios::binary | std::ios::ate);
    //if (!db_file) {
    //    printf("error: failed to open %s, abort\n", DUCKDB_DB_FILE);
    //    exit(1);
    //}
    //size_t file_size = static_cast<size_t>(db_file.tellg());
    //printf("data size: %.6f GB, %lu\n", file_size * 1.0 / (1 * GB), file_size);
    //db_file.close();

    // Load strings into memory
    duckdb_load_strings(DUCKDB_DB_FILE);
    pipeline_depth = n_bufs;
    printf("strings loaded into memory, %lu buffers produced\n", n_bufs);

   	// Intialize dpm frontend
    dpm_frontend_initialize();
    printf("dpm frontend initialized\n");
    
    // (De)Allocate in/output buffers in batches
    n_batches = n_bufs / MAX_DPK_BUF_CNT;
    if (n_batches * MAX_DPK_BUF_CNT < n_bufs) {
        n_batches++;
    }
    size_t last_batch_size = n_bufs - (n_batches-1) * MAX_DPK_BUF_CNT;
    printf("no. batches: %lu\n", n_batches);
   
	std::vector<size_t> per_batch_match_counts(n_batches) ;
    for (size_t b = 0; b < n_batches; b++) {
        size_t batch_size = (b == n_batches-1) ? last_batch_size : MAX_DPK_BUF_CNT;
        dpkernel_task* alloc_task = nullptr;
        app_alloc_task_request(&alloc_task);
		printf("batch %lu: in alloc requested %lu bytes\n", batch_size * DPK_INPUT_BUF_SIZE);
        auto in_alloc_req = dpm_alloc_input_buf_async(batch_size * DPK_INPUT_BUF_SIZE, alloc_task);
        auto out_alloc_req = dpm_alloc_output_buf_async(batch_size * DPK_OUTPUT_BUF_SIZE, alloc_task);
        
		dpm_wait_for_mem_req_completion(in_alloc_req);
		dpm_wait_for_mem_req_completion(out_alloc_req);
        // copy input strings over to the shared memory
        shm_ptr in_buf = alloc_task->base.in;
        shm_ptr out_buf = alloc_task->base.out;
        size_t start = b * MAX_DPK_BUF_CNT;
        size_t end = start + batch_size;
        char* in_region = app_get_input_ptr_from_shmptr(in_buf);
        for (size_t i = start; i < end; i++) {
            memcpy(in_region + (i-start) * DPK_INPUT_BUF_SIZE, contig_strings[i], strlen(contig_strings[i]));
        }

        worker_func_coal(b, in_buf, out_buf, batch_size);

        auto in_dealloc_req = dpm_free_input_buf_async(alloc_task);
        auto out_dealloc_req = dpm_free_output_buf_async(alloc_task);
        dpm_wait_for_mem_req_completion(in_dealloc_req);
        dpm_wait_for_mem_req_completion(out_dealloc_req);
    }
    double latency_sec = execution_elapsed.count();
	printf("regex kernel found %d matches\n",
			std::accumulate(per_batch_match_counts.begin(), per_batch_match_counts.end(), 0));
    printf("regex kernel execution latency: %.6f sec\n", latency_sec);
    printf("regex kernel throughput: %.6f GB/sec\n", data_size * 1.0 / (1 * GB) / latency_sec);
    
    return 0;
}

void duckdb_load_strings(char* path) {
    db_init(path);
    while (1) {
        char* strings_buf = new char[STRINGS_BUF_SIZE];
        uint8_t* lengths_buf = new uint8_t[LENGTHS_BUF_SIZE];
        size_t size = load_strings_in_batch(path, STRINGS_BUF_SIZE, strings_buf, lengths_buf);
        if (size == 0) {
            break;
        }
        contig_strings.push_back(strings_buf);
        n_bufs++;
    	for (size_t i = 0; i < size; i++) {
			data_size += lengths_buf[i];
		}
	}
	printf("data size: %.6f GB\n", data_size * 1.0 / (1 * GB));
}

