#include "common.hpp"
#include "dpm_interface.hpp"
#include "memory.hpp"
#include "memory_common.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace
{
constexpr const char *kCiphertextFilePath =
    "/home/ubuntu/jason/doca3.2/samples/doca_aes_gcm/aes_gcm_decrypt/enc_files/4K.enc";
constexpr uint32_t kAesKeySizeBytes = 32;
constexpr uint32_t kAesIvSizeBytes = 12;
constexpr uint32_t kAesTagSizeBytes = 12;
constexpr uint32_t kAesAadSizeBytes = 0;
constexpr int kDefaultTotalLoops = 100000;
constexpr int kDefaultNumThreads = 1;
constexpr uint32_t kLazyBindRingSlots = 64;

// Prototype constants requested by workload definition.
constexpr std::array<uint8_t, kAesKeySizeBytes> kAesZeroKey = {};
constexpr std::array<uint8_t, kAesIvSizeBytes> kAesZeroIv = {};

std::atomic<unsigned long> g_total_completed(0);
std::atomic<double> g_max_thread_time_sec(0.0);

struct worker_result
{
    bool success = false;
    unsigned long completed = 0;
    uint64_t total_actual_out_bytes = 0;
    uint32_t last_actual_out_size = 0;
};

bool read_file_to_buffer(const std::string &file_path, std::vector<uint8_t> *buffer)
{
    if (buffer == nullptr)
    {
        return false;
    }

    std::ifstream file(file_path, std::ios::binary | std::ios::ate);
    if (!file)
    {
        std::printf("Failed to open ciphertext file: %s\n", file_path.c_str());
        return false;
    }

    std::streamsize file_size = file.tellg();
    if (file_size <= 0)
    {
        std::printf("Invalid ciphertext file size: %ld\n", (long)file_size);
        return false;
    }

    file.seekg(0, std::ios::beg);
    buffer->resize((size_t)file_size);
    if (!file.read((char *)buffer->data(), file_size))
    {
        std::printf("Failed to read ciphertext file bytes\n");
        return false;
    }
    return true;
}

bool wait_for_task_completion(dpkernel_task *task)
{
    const auto start = std::chrono::steady_clock::now();
    while (true)
    {
        dpkernel_error status = app_check_task_completion(task);
        if (status == DPK_SUCCESS)
        {
            return true;
        }
        if (status == DPK_ERROR_FAILED || status == DPK_ERROR_AGAIN)
        {
            std::printf("Task completion status = %d\n", status);
            return false;
        }

        const auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > std::chrono::seconds(30))
        {
            std::printf("Timeout waiting for task completion\n");
            return false;
        }
        std::this_thread::yield();
    }
}

void worker_thread(int thread_id, int loops_for_thread, const std::vector<uint8_t> *ciphertext, worker_result *result)
{
    if (result == nullptr || ciphertext == nullptr)
    {
        return;
    }

    result->success = false;

    if (loops_for_thread <= 0)
    {
        result->success = true;
        return;
    }

    dpkernel_task *task = nullptr;
    if (!app_alloc_task_request(&task))
    {
        std::printf("Thread %d: app_alloc_task_request failed\n", thread_id);
        return;
    }

    // Allocate key in req_ctx shared memory through frontend API; avoid exposing allocator internals to client code.
    uint8_t *key_bytes = (uint8_t *)app_alloc_req_ctx_buffer(kAesKeySizeBytes);
    if (key_bytes == nullptr)
    {
        std::printf("Thread %d: app_alloc_req_ctx_buffer failed for key bytes\n", thread_id);
        app_free_task_request(task);
        return;
    }
    std::memcpy(key_bytes, kAesZeroKey.data(), kAesZeroKey.size());

    task->base.task = TASK_AES_GCM;
    task->base.device = DEVICE_NONE;
    task->base.in_size = (uint32_t)ciphertext->size();
    task->base.out_size = (uint32_t)ciphertext->size();
    task->base.actual_out_size = 0;
    task->bf3_aes_gcm.key_shm = app_get_shm_ptr_for_req_ctx_ptr(key_bytes);
    task->bf3_aes_gcm.key_size_bytes = kAesKeySizeBytes;

    // Preallocate one ring region per task, then reuse one preallocated src/dst doca_buf pair forever.
    const uint64_t ring_in_bytes_u64 = (uint64_t)task->base.in_size * (uint64_t)kLazyBindRingSlots;
    const uint64_t ring_out_bytes_u64 = (uint64_t)task->base.out_size * (uint64_t)kLazyBindRingSlots;
    if (ring_in_bytes_u64 > UINT32_MAX || ring_out_bytes_u64 > UINT32_MAX)
    {
        std::printf("Thread %d: ring bytes exceed uint32_t limits in=%lu out=%lu\n", thread_id,
                    (unsigned long)ring_in_bytes_u64, (unsigned long)ring_out_bytes_u64);
        app_free_req_ctx_buffer(key_bytes);
        app_free_task_request(task);
        return;
    }
    const uint32_t ring_in_bytes = (uint32_t)ring_in_bytes_u64;
    const uint32_t ring_out_bytes = (uint32_t)ring_out_bytes_u64;

#if DPK_SINGLE_IO_REGION
    // Single-IO mode: one mem request roundtrip for both in/out regions.
    auto io_req = dpm_alloc_mem_buf_async(ring_in_bytes, ring_out_bytes, task);
    if (io_req == nullptr)
    {
        std::printf("Thread %d: Failed to submit combined IO mem allocation request\n", thread_id);
        app_free_req_ctx_buffer(key_bytes);
        app_free_task_request(task);
        return;
    }
    if (!dpm_wait_for_mem_req_completion(io_req))
    {
        std::printf("Thread %d: Combined IO mem allocation request failed\n", thread_id);
        app_free_req_ctx_buffer(key_bytes);
        app_free_task_request(task);
        return;
    }
#else
    // Legacy split mode fallback: allocate in/out rings separately.
    auto in_req = dpm_alloc_input_buf_async(ring_in_bytes, task);
    auto out_req = dpm_alloc_output_buf_async(ring_out_bytes, task);
    if (in_req == nullptr || out_req == nullptr)
    {
        std::printf("Thread %d: Failed to submit mem allocation requests for input/output rings\n", thread_id);
        app_free_req_ctx_buffer(key_bytes);
        app_free_task_request(task);
        return;
    }
    if (!dpm_wait_for_mem_req_completion(in_req) || !dpm_wait_for_mem_req_completion(out_req))
    {
        std::printf("Thread %d: Input/output ring mem allocation request failed\n", thread_id);
        app_free_req_ctx_buffer(key_bytes);
        app_free_task_request(task);
        return;
    }
#endif

    const shm_ptr ring_base_in = task->base.in;
    const shm_ptr ring_base_out = task->base.out;

    // Restore per-submit logical sizes after ring allocation (allocation used total ring bytes).
    task->base.in_size = (uint32_t)ciphertext->size();
    task->base.out_size = (uint32_t)ciphertext->size();

    // Groundwork for reusable preallocated doca_bufs: execute path refreshes data pointers each submit.
    dpm_set_task_lazy_bind_mode(task, true);

    // Prefill ring input slots once; each submission picks one slot by offset.
    for (uint32_t slot = 0; slot < kLazyBindRingSlots; ++slot)
    {
        const shm_ptr slot_in = ring_base_in + (shm_ptr)slot * (shm_ptr)task->base.in_size;
        const shm_ptr slot_out = ring_base_out + (shm_ptr)slot * (shm_ptr)task->base.out_size;
        std::memcpy(app_get_input_ptr_from_shmptr(slot_in), ciphertext->data(), ciphertext->size());
        std::memset(app_get_output_ptr_from_shmptr(slot_out), 0, task->base.out_size);
    }

    auto thread_start = std::chrono::high_resolution_clock::now();
    bool success = true;
    for (int i = 0; i < loops_for_thread; ++i)
    {
        const uint32_t slot = (uint32_t)i % kLazyBindRingSlots;
        task->base.in = ring_base_in + (shm_ptr)slot * (shm_ptr)task->base.in_size;
        task->base.out = ring_base_out + (shm_ptr)slot * (shm_ptr)task->base.out_size;
        task->base.actual_out_size = 0;

        dpm_submit_task_msgq_blocking(thread_id, task, DEVICE_NONE);
        if (!wait_for_task_completion(task))
        {
            std::printf("Thread %d: Task failed at iteration %d\n", thread_id, i);
            success = false;
            break;
        }
        result->completed++;
        result->total_actual_out_bytes += task->base.actual_out_size;
        result->last_actual_out_size = task->base.actual_out_size;
    }
    auto thread_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> thread_elapsed = thread_end - thread_start;
    double elapsed_sec = thread_elapsed.count();

    g_total_completed.fetch_add(result->completed, std::memory_order_relaxed);
    double current_max = g_max_thread_time_sec.load(std::memory_order_relaxed);
    while (elapsed_sec > current_max &&
           !g_max_thread_time_sec.compare_exchange_weak(current_max, elapsed_sec, std::memory_order_relaxed))
        ;

    // Important: restore base pointers before free so deallocation targets the ring base.
    task->base.in = ring_base_in;
    task->base.out = ring_base_out;
#if DPK_SINGLE_IO_REGION
    auto free_io_req = dpm_free_mem_buf_async(task);
    if (free_io_req != nullptr)
    {
        dpm_wait_for_mem_req_completion(free_io_req);
    }
#else
    auto free_in_req = dpm_free_input_buf_async(task);
    auto free_out_req = dpm_free_output_buf_async(task);
    if (free_in_req != nullptr)
    {
        dpm_wait_for_mem_req_completion(free_in_req);
    }
    if (free_out_req != nullptr)
    {
        dpm_wait_for_mem_req_completion(free_out_req);
    }
#endif

    app_free_req_ctx_buffer(key_bytes);
    app_free_task_request(task);

    result->success = success && (result->completed == (unsigned long)loops_for_thread);
}
} // namespace

int main(int argc, char **argv)
{
    int num_threads = kDefaultNumThreads;
    int total_loops = kDefaultTotalLoops;
    std::string ciphertext_file_path = kCiphertextFilePath;

    if (argc > 1)
    {
        num_threads = std::atoi(argv[1]);
    }
    if (argc > 2)
    {
        total_loops = std::atoi(argv[2]);
    }
    if (argc > 3)
    {
        ciphertext_file_path = argv[3];
    }

    if (num_threads <= 0)
    {
        std::printf("Invalid num_threads value: %d\n", num_threads);
        return EXIT_FAILURE;
    }
    if (total_loops <= 0)
    {
        std::printf("Invalid total_loops value: %d\n", total_loops);
        return EXIT_FAILURE;
    }

    std::vector<uint8_t> ciphertext;
    if (!read_file_to_buffer(ciphertext_file_path, &ciphertext))
    {
        return EXIT_FAILURE;
    }

    if (!dpm_frontend_initialize())
    {
        std::printf("dpm_frontend_initialize failed\n");
        return EXIT_FAILURE;
    }

    std::printf("AES-GCM test constants: key=%uB iv=%uB tag=%uB aad=%uB loops=%d threads=%d path=%s (iv[0]=%u)\n",
                kAesKeySizeBytes, kAesIvSizeBytes, kAesTagSizeBytes, kAesAadSizeBytes, total_loops, num_threads,
                ciphertext_file_path.c_str(), (unsigned)kAesZeroIv[0]);

    g_total_completed.store(0, std::memory_order_relaxed);
    g_max_thread_time_sec.store(0.0, std::memory_order_relaxed);

    std::vector<std::thread> threads;
    std::vector<worker_result> results((size_t)num_threads);
    threads.reserve((size_t)num_threads);

    int base_loops = total_loops / num_threads;
    int extra_loops = total_loops % num_threads;
    for (int i = 0; i < num_threads; ++i)
    {
        int loops_for_thread = base_loops + ((i < extra_loops) ? 1 : 0);
        threads.emplace_back(worker_thread, i, loops_for_thread, &ciphertext, &results[(size_t)i]);
    }

    for (auto &thread : threads)
    {
        thread.join();
    }

    unsigned long success_count = 0;
    uint64_t total_actual_out_bytes = 0;
    uint32_t last_actual_out_size = 0;
    bool all_success = true;
    for (const auto &result : results)
    {
        success_count += result.completed;
        total_actual_out_bytes += result.total_actual_out_bytes;
        last_actual_out_size = result.last_actual_out_size;
        all_success = all_success && result.success;
    }

    double elapsed_time_sec = g_max_thread_time_sec.load(std::memory_order_relaxed);
    if (elapsed_time_sec <= 0.0)
    {
        elapsed_time_sec = 1e-9;
    }

    std::printf("Submitted loops=%d, successful=%lu, elapsed=%.6f sec, last_actual_out_size=%u\n", total_loops,
                success_count, elapsed_time_sec, last_actual_out_size);
    std::printf("Max thread time: %.6f s\n", elapsed_time_sec);
    double throughput_million_ops =
        (double)g_total_completed.load(std::memory_order_relaxed) / elapsed_time_sec / 1000000.0;
    std::printf("Throughput: %.6f million ops/s\n", throughput_million_ops);
    double throughput_mbps = (double)total_actual_out_bytes / elapsed_time_sec / 1024.0 / 1024.0;
    std::printf("throughput (MB/s): %.6f\n", throughput_mbps);

    return (all_success && success_count == (unsigned long)total_loops) ? EXIT_SUCCESS : EXIT_FAILURE;
}
