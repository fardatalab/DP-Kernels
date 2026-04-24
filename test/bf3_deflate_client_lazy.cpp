#include "common.hpp"
#include "dpm_interface.hpp"
#include "memory.hpp"
#include "memory_common.hpp"

#include <atomic>
#include <chrono>
#include <climits>
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
constexpr const char *kDefaultInputFilePath = "/home/ubuntu/deflate/4K.deflate";
constexpr int kDefaultTotalLoops = 100000;
constexpr int kDefaultNumThreads = 1;
constexpr uint32_t kDefaultOutputSizeBytes = 4 * KB;
constexpr uint32_t kLazyBindRingSlots = 64;
// Keep one extra slot of backing as guard space for lazy doca_buf_set_data rebinding near ring tail.
constexpr uint32_t kLazyBindRingGuardSlots = 1;

std::atomic<unsigned long> g_total_completed(0);
std::atomic<double> g_max_thread_time_sec(0.0);

struct worker_result
{
    bool success = false;
    unsigned long completed = 0;
    uint64_t total_actual_out_bytes = 0;
    uint32_t last_actual_out_size = 0;
};

/* Read an entire compressed input file into a byte buffer. */
bool read_file_to_buffer(const std::string &file_path, std::vector<uint8_t> *buffer)
{
    if (buffer == nullptr)
    {
        return false;
    }

    std::ifstream file(file_path, std::ios::binary | std::ios::ate);
    if (!file)
    {
        std::printf("Failed to open deflate input file: %s\n", file_path.c_str());
        return false;
    }

    std::streamsize file_size = file.tellg();
    if (file_size <= 0)
    {
        std::printf("Invalid deflate input file size: %ld\n", (long)file_size);
        return false;
    }

    file.seekg(0, std::ios::beg);
    buffer->resize((size_t)file_size);
    if (!file.read((char *)buffer->data(), file_size))
    {
        std::printf("Failed to read deflate input bytes\n");
        return false;
    }
    return true;
}

/* Poll task completion until success/failure or timeout. */
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

/* Per-thread worker: preallocate one ring-backed task, then reuse with lazy DOCA binding forever. */
void worker_thread(int thread_id, int loops_for_thread, const std::vector<uint8_t> *compressed_input,
                   uint32_t output_size_bytes, worker_result *result)
{
    if (result == nullptr || compressed_input == nullptr)
    {
        return;
    }

    result->success = false;

    if (loops_for_thread <= 0)
    {
        result->success = true;
        return;
    }

    if (output_size_bytes == 0)
    {
        std::printf("Thread %d: invalid output size 0\n", thread_id);
        return;
    }

    dpkernel_task *task = nullptr;
    if (!app_alloc_task_request(&task))
    {
        std::printf("Thread %d: app_alloc_task_request failed\n", thread_id);
        return;
    }

    task->base.task = TASK_DECOMPRESS_DEFLATE;
    task->base.device = DEVICE_NONE;
    task->base.in_size = (uint32_t)compressed_input->size();
    task->base.out_size = output_size_bytes;
    task->base.actual_out_size = 0;

    // Preallocate one ring region per task, then reuse one preallocated src/dst doca_buf pair forever.
    const uint64_t ring_alloc_slots = (uint64_t)kLazyBindRingSlots + (uint64_t)kLazyBindRingGuardSlots;
    const uint64_t ring_in_bytes_u64 = (uint64_t)task->base.in_size * ring_alloc_slots;
    const uint64_t ring_out_bytes_u64 = (uint64_t)task->base.out_size * ring_alloc_slots;
    if (ring_in_bytes_u64 > UINT32_MAX || ring_out_bytes_u64 > UINT32_MAX)
    {
        std::printf("Thread %d: ring bytes exceed uint32_t limits in=%lu out=%lu\n", thread_id,
                    (unsigned long)ring_in_bytes_u64, (unsigned long)ring_out_bytes_u64);
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
        std::printf("Thread %d: failed to submit combined IO mem allocation request\n", thread_id);
        app_free_task_request(task);
        return;
    }
    if (!dpm_wait_for_mem_req_completion(io_req))
    {
        std::printf("Thread %d: combined IO mem allocation request failed\n", thread_id);
        app_free_task_request(task);
        return;
    }
#else
    // Legacy split mode fallback: allocate in/out rings separately.
    auto in_req = dpm_alloc_input_buf_async(ring_in_bytes, task);
    auto out_req = dpm_alloc_output_buf_async(ring_out_bytes, task);
    if (in_req == nullptr || out_req == nullptr)
    {
        std::printf("Thread %d: failed to submit mem allocation requests for input/output rings\n", thread_id);
        app_free_task_request(task);
        return;
    }
    if (!dpm_wait_for_mem_req_completion(in_req) || !dpm_wait_for_mem_req_completion(out_req))
    {
        std::printf("Thread %d: input/output ring mem allocation request failed\n", thread_id);
        app_free_task_request(task);
        return;
    }
#endif

    const shm_ptr ring_base_in = task->base.in;
    const shm_ptr ring_base_out = task->base.out;

#if DPK_SINGLE_IO_REGION
    // Defensive diagnostics: in single-IO mode, input/output share one backing region.
    // Verify the two subranges are disjoint so output writes cannot corrupt future input slots.
    const uint64_t in_begin = (uint64_t)ring_base_in;
    const uint64_t in_end = in_begin + ring_in_bytes_u64;
    const uint64_t out_begin = (uint64_t)ring_base_out;
    const uint64_t out_end = out_begin + ring_out_bytes_u64;
    if (!(in_end <= out_begin || out_end <= in_begin))
    {
        std::printf("Thread %d: single-IO in/out overlap detected: in=[%lu,%lu) out=[%lu,%lu)\n", thread_id,
                    (unsigned long)in_begin, (unsigned long)in_end, (unsigned long)out_begin, (unsigned long)out_end);
        task->base.in = ring_base_in;
        task->base.out = ring_base_out;
        auto free_io_req = dpm_free_mem_buf_async(task);
        if (free_io_req != nullptr)
        {
            dpm_wait_for_mem_req_completion(free_io_req);
        }
        app_free_task_request(task);
        return;
    }
#endif

    // Restore per-submit logical sizes after ring allocation (allocation used total ring bytes).
    task->base.in_size = (uint32_t)compressed_input->size();
    task->base.out_size = output_size_bytes;

    // Groundwork for reusable preallocated doca_bufs: execute path refreshes data pointers each submit.
    dpm_set_task_lazy_bind_mode(task, true);

    // Prefill ring input slots once; each submission picks one slot by offset.
    for (uint32_t slot = 0; slot < kLazyBindRingSlots; ++slot)
    {
        const shm_ptr slot_in = ring_base_in + (shm_ptr)slot * (shm_ptr)task->base.in_size;
        const shm_ptr slot_out = ring_base_out + (shm_ptr)slot * (shm_ptr)task->base.out_size;
        std::memcpy(app_get_input_ptr_from_shmptr(slot_in), compressed_input->data(), compressed_input->size());
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
            std::printf("Thread %d: task failed at iteration %d slot=%u in=%lu out=%lu in_size=%u out_size=%u\n",
                        thread_id, i, slot, (unsigned long)task->base.in, (unsigned long)task->base.out,
                        task->base.in_size, task->base.out_size);
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

    // Restore base pointers before free so deallocation targets the ring base.
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

    app_free_task_request(task);

    result->success = success && (result->completed == (unsigned long)loops_for_thread);
}
} // namespace

int main(int argc, char **argv)
{
    int num_threads = kDefaultNumThreads;
    int total_loops = kDefaultTotalLoops;
    std::string input_file_path = kDefaultInputFilePath;
    uint32_t output_size_bytes = kDefaultOutputSizeBytes;

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
        input_file_path = argv[3];
    }
    if (argc > 4)
    {
        long parsed_out_size = std::strtol(argv[4], nullptr, 10);
        if (parsed_out_size > 0 && parsed_out_size <= INT32_MAX)
        {
            output_size_bytes = (uint32_t)parsed_out_size;
        }
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
    if (output_size_bytes == 0)
    {
        std::printf("Invalid output_size_bytes value: %u\n", output_size_bytes);
        return EXIT_FAILURE;
    }

    std::vector<uint8_t> compressed_input;
    if (!read_file_to_buffer(input_file_path, &compressed_input))
    {
        return EXIT_FAILURE;
    }

    if (!dpm_frontend_initialize())
    {
        std::printf("dpm_frontend_initialize failed\n");
        return EXIT_FAILURE;
    }

    std::printf("Deflate client: loops=%d threads=%d input=%s in_size=%u out_size=%u slots=%u guard=%u\n", total_loops,
                num_threads, input_file_path.c_str(), (uint32_t)compressed_input.size(), output_size_bytes,
                kLazyBindRingSlots, kLazyBindRingGuardSlots);

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
        threads.emplace_back(worker_thread, i, loops_for_thread, &compressed_input, output_size_bytes,
                             &results[(size_t)i]);
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
