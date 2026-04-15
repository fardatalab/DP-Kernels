#include "dp_manager_msgq.hpp"
#include "bounded_queue.hpp"
#include "common.hpp"
#include "device_specific.hpp"
#include "kernel_interface.hpp"
#include "kernel_queue.hpp"
#include "memory.hpp"

#include "mpmc_queue.hpp"
#include "ring_buffer.hpp"

#include "scheduling.hpp"
#include "sw_device.hpp"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>

#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#endif

#include <iostream>
#include <set>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

static bool main_thread_running = true;
static bool hw_eagain =
    false; // whenever we get EAGAIN from the kernel, we set this flag and don't bother with submitting to hw afterwards
static std::atomic<bool> dpm_runtime_ready{false};
static std::atomic<bool> dpm_thread_pinning_enabled{true};

// the underlying device, will be filled in by device sproc on detection
enum dpm_device dpm_underlying_device = dpm_device::DEVICE_NULL;

std::atomic<bool> polling_loop{true};
std::thread dpm_threads[N_DPM_THREADS];

#ifdef USE_RING_BUFFER
std::vector<char *> req_ring_buffer_holders;
std::vector<RequestRingMsgQ *> dpm_submission_queue_ring_buffers;
#else // use mpmc queue
rigtorp::MPMCQueue<struct dpm_req_msg, DPM_SUBMISSION_QUEUE_SIZE> **dpm_submission_queues_mpmc;
#endif

// uint active_kernels[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST] = {};

// TODO: need to allow DDS ring buffer to use a given size...
// for each type of sw kernel we hold a software queue of requests
// MPMCQueue<shm_ptr> *sw_kernel_queues[dpm_task_name::TASK_NAME_LAST] = {};

// for each dpm thread, for each type of hw kernel, we hold a software queue
BoundedQueue<dpkernel_task *, DPM_HW_KERNEL_QUEUE_SIZE> *hw_kernel_queues[N_DPM_THREADS][dpm_task_name::TASK_NAME_LAST];

aligned_hw_kernel_capacity hw_kernels_capacity[N_DPM_THREADS];

bool dpm_detect_platform()
{
    // Load all dynamic libraries from robust runtime locations.
    // This avoids relying on a specific CWD when embedded in another process (e.g., DDS backend).
    std::vector<std::filesystem::path> search_dirs = {};
    std::set<std::string> seen_dirs;
    auto add_search_dir = [&](const std::filesystem::path &dir) {
        if (dir.empty())
        {
            return;
        }
        std::error_code norm_ec;
        std::filesystem::path normalized = std::filesystem::weakly_canonical(dir, norm_ec);
        if (norm_ec)
        {
            normalized = dir.lexically_normal();
        }
        std::string key = normalized.string();
        if (key.empty())
        {
            return;
        }
        if (seen_dirs.insert(key).second)
        {
            search_dirs.push_back(normalized);
        }
    };
    auto add_env_path_list = [&](const char *env_name) {
        const char *raw = std::getenv(env_name);
        if (raw == nullptr || raw[0] == '\0')
        {
            return;
        }
        std::string paths(raw);
        size_t start = 0;
        while (start <= paths.size())
        {
            size_t delim = paths.find(':', start);
            std::string token = (delim == std::string::npos) ? paths.substr(start) : paths.substr(start, delim - start);
            if (!token.empty())
            {
                add_search_dir(std::filesystem::path(token));
            }
            if (delim == std::string::npos)
            {
                break;
            }
            start = delim + 1;
        }
    };

    // User-provided paths take precedence.
    add_env_path_list("DPK_PLATFORM_LIB_DIRS");
    add_env_path_list("DPK_PLATFORM_LIB_DIR");

    // Build-time/platform defaults.
    add_search_dir(std::filesystem::path(PLATFORM_SPECIFIC_LIB_PATH));
    add_search_dir(std::filesystem::path(PLATFORM_SPECIFIC_LIB_PATH) / "build");

    // Location of the currently loaded module/binary containing DPM runtime symbols.
    // Use a TU-local static object to avoid function-name overload ambiguity here.
    Dl_info self_info = {};
    if (dladdr((void *)&dpm_runtime_ready, &self_info) != 0 && self_info.dli_fname != nullptr)
    {
        std::filesystem::path self_path(self_info.dli_fname);
        add_search_dir(self_path.parent_path());
        add_search_dir(self_path.parent_path() / "build");
    }

    // Current process executable path.
    std::error_code ec;
    std::filesystem::path exe_path = std::filesystem::read_symlink("/proc/self/exe", ec);
    if (!ec && !exe_path.empty())
    {
        add_search_dir(exe_path.parent_path());
        add_search_dir(exe_path.parent_path() / "build");
    }

    // Runtime loader search path and cwd fallbacks.
    add_env_path_list("LD_LIBRARY_PATH");
    std::error_code cwd_ec;
    std::filesystem::path cwd = std::filesystem::current_path(cwd_ec);
    if (!cwd_ec)
    {
        add_search_dir(cwd);
        add_search_dir(cwd / "build");
    }

    std::set<std::string> so_file_set;
    for (const auto &dir : search_dirs)
    {
        std::error_code dir_ec;
        if (!std::filesystem::exists(dir, dir_ec) || dir_ec || !std::filesystem::is_directory(dir, dir_ec))
        {
            continue;
        }
        for (const auto &entry : std::filesystem::directory_iterator(dir, dir_ec))
        {
            if (dir_ec)
            {
                break;
            }
            if (entry.is_regular_file() && entry.path().extension() == ".so" &&
                entry.path().filename().string().find("libdpm") == 0)
            {
                so_file_set.insert(entry.path().string());
            }
        }
    }
    std::vector<std::string> so_files(so_file_set.begin(), so_file_set.end());
    if (so_files.empty())
    {
        std::cerr << "No device specific library found (expected libdpm*.so in runtime search paths)\n";
        std::cerr << "Scanned directories:\n";
        for (const auto &dir : search_dirs)
        {
            std::cerr << "  - " << dir.string() << '\n';
        }
        return false;
    }

    for (const auto &so_file : so_files)
    {
        void *handle = dlopen(so_file.c_str(), RTLD_NOW);
        if (!handle)
        {
            printf("Error: %s\n", dlerror());
            continue;
        }
        else
        {
            printf("loaded device specific lib: %s\n", so_file.c_str());
        }
        detect_platform_func detector = (detect_platform_func)dlsym(handle, DETECT_PLATFORM_FUNC_NAME);
        if (!detector)
        {
            std::cerr << "Cannot find detector in " << so_file << '\n';
            dlclose(handle);
            continue;
        }

        // detected the current platform
        if (detector(&dpm_underlying_device))
        {
            printf("platform detector found in %s\n", so_file.c_str());
            // load the device and memory initializers for global use
            if (!dpm_load_device_and_mem_initializers(handle, &dpm_device_initializers, &dpm_mem_initializers))
            {
                std::cerr << "Cannot load device and memory initializers from " << so_file << '\n';
                dlclose(handle);
                return false;
            }

            // then register the kernels, need to initialize the executors later after device and memory initializations
            memset(dpm_executors, 0, sizeof(dpm_executors));
            if (!dpm_load_kernels(handle, dpm_executors))
            {
                std::cerr << "Cannot load kernels from " << so_file << '\n';
                dlclose(handle);
                return false;
            }
            return true;
        }
        else
        {
            printf("platform detector did not match in %s\n", so_file.c_str());
            dlclose(handle);
        }
    }
    return false;
}

bool _dpm_init_kernel_queues()
{
    for (int t = 0; t < N_DPM_THREADS; t++)
    {
        for (int i = 0; i < dpm_task_name::TASK_NAME_LAST; i++)
        {
            // sw_kernel_queues[i] = new MPMCQueue<shm_ptr>(DPM_SW_KERNEL_QUEUE_SIZE);
            // if (sw_kernel_queues[i] == nullptr)
            // {
            //     printf("Failed to allocate kernel queue %d\n", i);
            //     return false;
            // }

            // using thread local hw queues now, no need to allocate them
            ////
            BoundedQueue<dpkernel_task *, DPM_HW_KERNEL_QUEUE_SIZE> *alloc_q;
            if (posix_memalign((void **)&alloc_q, 64, sizeof(BoundedQueue<dpkernel_task *, DPM_HW_KERNEL_QUEUE_SIZE>)))
            {
                printf("Failed to allocate kernel queue %d\n", i);
                return false;
            }
            new (alloc_q) BoundedQueue<dpkernel_task *, DPM_HW_KERNEL_QUEUE_SIZE>();
            hw_kernel_queues[t][i] = alloc_q;
            /* if (hw_kernel_queues[i] == nullptr)
            {
                printf("Failed to allocate kernel queue %d\n", i);
                return false;
            } */
        }
    }
    ////memset(draining_rates, 0, sizeof(draining_rates));
    ////memset(inflight_bytes, 0, sizeof(inflight_bytes));
    ////memset(active_kernels, 0, sizeof(active_kernels));
    return true;
}

/* void dpm_check_coalescing_timeout(int thread_id)
{
    // check if we are waiting for too long, if so, rerturn a coalesced task
    dpkernel_task* coalesced_task = flush_task_if_timeout(thread_id, dpm_underlying_device);
} */

void _dpm_thread_handler(void *args)
{
    int thread_id = (int)(intptr_t)args;

    // pin thread to core
    if (dpm_thread_pinning_enabled.load(std::memory_order_acquire))
    {
        int base_core = DPK_THREAD_PIN_BASE_CORE;
        int target_core = base_core + thread_id;
        long online_cores = sysconf(_SC_NPROCESSORS_ONLN);
        if (online_cores > 0 && target_core >= online_cores)
        {
            target_core %= (int)online_cores;
            printf("DPM thread %d requested core %d exceeds online cores=%ld; wrapping to core %d\n", thread_id,
                   base_core + thread_id, online_cores, target_core);
        }

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(target_core, &cpuset);
        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0)
        {
            std::cerr << "Error calling pthread_setaffinity_np for thread " << (int)(intptr_t)args << " on core "
                      << target_core << ": " << strerror(rc) << std::endl;
            std::cerr << "Continuing thread " << thread_id << " without explicit pinning" << std::endl;
        }
        else
        {
            printf("DPM thread %lu pinned to core %d\n", pthread_self(), target_core);
        }
    }
    else
    {
        printf("DPM thread %lu running without explicit pinning\n", pthread_self());
    }

    // init hw kernel capacity for this thread
    for (int i = 0; i < dpm_task_name::TASK_NAME_LAST; i++)
    {
        *hw_kernels_capacity[thread_id][(enum dpm_task_name)i] =
            DPM_HW_KERNEL_QUEUE_SIZE +
            dpm_kernel_catalogues[dpm_underlying_device][(enum dpm_task_name)i].kernel_capacity;
        print_debug("DPM thread %d, task %d, hw kernel capacity: %ld\n", thread_id, i,
                    *hw_kernels_capacity[thread_id][(enum dpm_task_name)i]);
    }

    int i = 0;
    uint loop_cnt = 0;
    dpkernel_error ret;
    while (polling_loop.load(std::memory_order_relaxed)) //.load(std::memory_order_relaxed)
    {
        // no longer polls mem requests separately
        // dpm_poll_mem_req();

        // loop_cnt = 0;
        // while (loop_cnt < 1000000)
        {
            // while (i < 1000)
            {
                // i++;

#if defined(COALESCING)
                // check if we are waiting for too long, if so, rerturn a coalesced task
                dpkernel_task *coalesced_task = flush_task_if_timeout(thread_id, dpm_underlying_device);
                if (coalesced_task != NULL) // need to execute the coalesced task
                {
                    ret = _dpm_try_execute_or_queue_task(thread_id, coalesced_task, hw_eagain);
                    if (ret == DPK_ERROR_AGAIN)
                    {
                        hw_eagain = true;
                    }
                }
                // else nothing to execute
#endif

                // while (i < 100)
                {
                    // i++;

                    ret = dpm_poll_requests(thread_id);
                    if (ret == DPK_ERROR_AGAIN)
                    {
                        // continue;

                        ////////
                        //                     int x = 0;
                        // #if defined(__x86_64__) || defined(_M_X64)
                        //                     while (x < 500)
                        // #elif defined(__arm__) || defined(__aarch64__)
                        //                     while (x < 50000)
                        // #else
                        //                     while (x < 100000)
                        // #endif
                        //                     {
                        //                         // std::this_thread::yield();
                        // #if defined(__x86_64__) || defined(_M_X64)
                        //                         _mm_pause();
                        // #elif defined(__arm__) || defined(__aarch64__)
                        //                         __asm volatile("yield");
                        // #else
                        //                         std::this_thread::yield();
                        // #endif
                        //                         x++;
                        //                     }
                        ////////
                    }
                    // if (ret == DPK_SUCCESS)
                    {
                        // got a request, poll more in case there are more
                        // continue;
                    }
                    // else
                    {
                        // assuming DPK_ERROR_AGAIN, no more requests, get out of the loop faster
                        // i += 256;
                    }
                }
                // i = 0;
            }
            // i = 0;

            // drain hardware queues if we have a hardware device
            if (dpm_underlying_device != DEVICE_NULL) [[likely]]
            {
                dpm_drain_kernel_queues(thread_id);
            }

            /// DPM polls for completion and sets the completion flag, then the application can poll for it
            dpm_poll_completion(thread_id);
            // loop_cnt++;
        }
    }
}

int dp_kernel_manager_msgq_start(void *args)
{
    dpm_runtime_ready.store(false, std::memory_order_release);
    polling_loop.store(true, std::memory_order_release);

    // detect DPU platform first, will load the initializers, and the kernels
    if (!dpm_detect_platform())
    {
        std::cerr << "Failed to detect platform or load libdpm initializers\n";
        return -1;
    }

    _dpm_init_kernel_queues();

    ////_create_msg_queue_boost();
#ifdef USE_RING_BUFFER
    _create_msg_queue_ring_buffer();
#else
    _create_msg_queue_mpmc();
#endif

    printf("initializing mem allocator\n");
    setup_memory_allocator(&dpm_own_mem_region, &dpm_req_ctx_shm);
#if defined(COALESCING)
    // _setup_coalescing_task_pools(N_DPM_THREADS);
#endif
    printf("initializing mem allocator done\n");

    printf("dpm_mem_init\n");
    if (!dpm_device_and_mem_init(&dpm_own_mem_region))
    {
        std::cerr << "Failed to initialize device and memory regions\n";
        return -1;
    }
    printf("dpm_mem_init done\n");

    printf("initializing sw kernels\n");
    dpm_init_sw_kernels();
    printf("initializing sw kernels done\n");

    printf("initializing hw kernels\n");
    dpm_init_hw_kernels();
    printf("initializing hw kernels done\n");

    // Embedded callers wait for this readiness signal before frontend setup/submission.
    dpm_runtime_ready.store(true, std::memory_order_release);

    // start the dpm threads (== RING_BUFFER_NUMBER threads)
    for (int i = 1; i < N_DPM_THREADS; i++)
    {
        dpm_threads[i] = std::thread(_dpm_thread_handler, (void *)(intptr_t)i);
        if (!dpm_threads[i].joinable())
        {
            printf("Failed to create thread %d\n", i);
            return -1;
        }
        printf("DPM thread %d created\n", i);
    }

    // run the first thread in the main thread
    _dpm_thread_handler((void *)(intptr_t)0);

    // // wait for main thread to finish
    // while (main_thread_running)
    // {
    //     std::this_thread::yield();
    // }

    // perform cleanup of all hw kernels
    dpm_cleanup_kernels();

    if (cleanup_dpm_msgq())
    {
        dpm_runtime_ready.store(false, std::memory_order_release);
        printf("dpm cleanup done\n");
        return 0;
    }
    else
    {
        dpm_runtime_ready.store(false, std::memory_order_release);
        printf("dpm cleanup failed\n");
        return -1;
    }
}

#ifdef USE_RING_BUFFER
bool _create_msg_queue_ring_buffer()
{
    for (int i = 0; i < RING_BUFFER_NUMBER; i++)
    {
        std::string ring_buffer_name = RING_BUFFER_SHM_NAME + std::to_string(i);
        RequestRingMsgQ *ringBuffer = AllocateRequestBufferProgressive(ring_buffer_name.c_str());
        if (ringBuffer == nullptr)
        {
            printf("Failed to allocate ring buffer\n");
            return false;
        }
        dpm_submission_queue_ring_buffers.push_back(ringBuffer);

        // char *buf_holder = new char[RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT];
        // if (buf_holder == nullptr)
        // {
        //     printf("Failed to allocate ring buffer holder %d\n", i);
        //     return false;
        // }
        char *buf_holder = nullptr;
        if (posix_memalign((void **)&buf_holder, 64, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT))
        {
            printf("Failed to allocate ring buffer holder %d\n", i);
            return false;
        }

        req_ring_buffer_holders.push_back(buf_holder);
    }

    return true;
}
#else
bool _create_msg_queue_mpmc()
{
    // setup task submission queues
    dpm_submission_queues_mpmc =
        new rigtorp::MPMCQueue<struct dpm_req_msg, DPM_SUBMISSION_QUEUE_SIZE> *[RING_BUFFER_NUMBER];
    for (int i = 0; i < RING_BUFFER_NUMBER; i++)
    {
        std::string queue_name = SUBMISSION_QUEUE_NAME + std::to_string(i);
        dpm_submission_queues_mpmc[i] =
            rigtorp::create_mpmc_queue<struct dpm_req_msg, DPM_SUBMISSION_QUEUE_SIZE>(queue_name.c_str());
        if (dpm_submission_queues_mpmc[i] == nullptr)
        {
            printf("Failed to allocate mpmc queue %d\n", i);
            return false;
        }
    }
    return true;
}
#endif

alignas(64) thread_local uint get_sched_start_time_cnt;
static inline bool _is_sw_executor_available(enum dpm_task_name task_name)
{
    return dpm_executors[DEVICE_SOFTWARE][task_name].execute != nullptr;
}

enum dpm_device _schedule_task_for_optimal_device(dpkernel_task *dpk_task, int thread_id)
{
    enum dpm_task_name task_name = (enum dpm_task_name)dpk_task->base.task;
    const bool sw_available = _is_sw_executor_available(task_name);

    // If there is no usable HW executor, only route to SW when SW implements this task.
    // This avoids misrouting HW-only kernels (e.g., prototype AES-GCM) into SW queues.
    if (dpm_underlying_device == dpm_device::DEVICE_NULL ||
        dpm_executors[dpm_underlying_device][task_name].execute == nullptr)
    {
        return sw_available ? dpm_device::DEVICE_SOFTWARE : dpm_device::DEVICE_NULL;
    }

#if defined(SCHEDULING) && !defined(BASELINE_SCHEDULING)
    /* float sw_kernel_speed = _get_draining_rate(dpk_task->base.device, dpk_task->base.task, thread_id);
    float hw_kernel_speed = _get_draining_rate(dpm_underlying_device, dpk_task->base.task, thread_id); */

    // return dpm_underlying_device;

    // otherwise, perform scheduling based on queueing delay and kernel execution time
    long hw_exec_time_ns = dpm_kernel_catalogues[dpm_underlying_device][task_name].get_estimated_processing_time_ns(
        dpk_task->base.in_size, thread_id);
    long sw_exec_time = dpm_kernel_catalogues[DEVICE_SOFTWARE][task_name].get_estimated_processing_time_ns(
        dpk_task->base.in_size, thread_id);

    //// test...
    // return hw_exec_time_ns < sw_exec_time ? dpm_underlying_device : dpm_device::DEVICE_SOFTWARE;
    ////

    /* // given the draining speed, and the inflight bytes, we can choose the device that gives us shortest time to
    // completion
    auto sw_inflight_bytes = inflight_bytes[thread_id][dpm_device::DEVICE_SOFTWARE][task_name].load();
    auto hw_inflight_bytes = inflight_bytes[thread_id][dpm_underlying_device][task_name].load();
    float sw_queuing_time = (sw_inflight_bytes + dpk_task->base.in_size) / sw_kernel_speed;
    float hw_queuing_time = (hw_inflight_bytes + dpk_task->base.in_size) / hw_kernel_speed; */

    // given the queuing delay and the execution time, we can choose the device that gives us shortest time

    // auto current_time = std::chrono::high_resolution_clock::now().time_since_epoch().count();

    long hw_total_time_ns = hw_exec_time_ns + *estimated_hw_kernels_queuing_time[thread_id][task_name];
    if (get_sched_start_time_cnt == 1)
    {
        auto current_time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
        hw_total_time_ns -= (current_time - *hw_kernels_sched_start_time[thread_id][TASK_DECOMPRESS_DEFLATE]);
        get_sched_start_time_cnt = 0;
    }
    get_sched_start_time_cnt++;
    //   - (current_time - *hw_kernels_sched_start_time[thread_id][TASK_DECOMPRESS_DEFLATE]);
    // - (current_time - last_estimated_hw_kernels_timestamps[thread_id][task_name]);

    long sw_queuing_time = estimated_sw_kernels_queuing_time[thread_id][task_name]->load(std::memory_order_acquire);
    long sw_total_time_ns = sw_exec_time + sw_queuing_time;
    // - (current_time - last_estimated_sw_kernels_timestamps[thread_id][task_name]);

    print_debug(
        "sw kernel is faster, using sw kernel, sw time = %ld, hw time = %ld, sw queuing delay = %ld, hw queuing "
        "delay = %ld, sw total time = %ld, hw total time = %ld\n",
        sw_exec_time, hw_exec_time_ns, sw_queuing_time, *estimated_hw_kernels_queuing_time[thread_id][task_name],
        sw_total_time_ns, hw_total_time_ns);

    ////if (sw_queuing_time < hw_queuing_time)
    if (hw_total_time_ns > sw_total_time_ns)
    {
        if (!sw_available)
        {
            dpk_task->base.estimated_completion_time = hw_exec_time_ns;
            return dpm_underlying_device;
        }
        print_debug(
            "sw kernel is faster, using sw kernel, sw time = %ld, hw time = %ld, sw queuing delay = %ld, hw queuing "
            "delay = %ld, "
            "sw total time = %ld, hw total time = %ld\n",
            sw_exec_time, hw_exec_time_ns, sw_queuing_time, *estimated_hw_kernels_queuing_time[thread_id][task_name],
            sw_total_time_ns, hw_total_time_ns);

        dpk_task->base.estimated_completion_time = sw_exec_time;
        return dpm_device::DEVICE_SOFTWARE;
    }
    else
    {
        // printf("hw kernel is faster, using hw kernel, sw time = %f, hw time = %f, sw queuing delay = %f, hw queuing "
        //        "delay = %f\n",
        //        sw_exec_time, hw_exec_time, estimated_hw_kernels_queuing_time[thread_id][task_name],
        //        sw_queuing_time_f);

        dpk_task->base.estimated_completion_time = hw_exec_time_ns;
        return dpm_underlying_device;
    }

#elif defined(SCHEDULING) && defined(BASELINE_SCHEDULING)
    /* if (dpm_executors[dpm_underlying_device][task_name].hw_kernel_remaining_capacity != NULL)
    {
        uint32_t max_capacity = 0;
        uint32_t remaining_capacity =
            dpm_executors[dpm_underlying_device][task_name].hw_kernel_remaining_capacity(thread_id, &max_capacity);
        if (remaining_capacity > 0)
        {
            return dpm_underlying_device;
        }
        else
        {
            return dpm_device::DEVICE_SOFTWARE;
        }
    } */
    uint32_t remaining_capacity = *hw_kernels_capacity[thread_id][task_name];
    if (remaining_capacity > 0) [[likely]]
    {
        return dpm_underlying_device;
    }
    else
    {
        return sw_available ? dpm_device::DEVICE_SOFTWARE : dpm_underlying_device;
    }
#else // NOTE: no scheduling: always say use the hw kernel
    return dpm_underlying_device;
#endif
}

dpkernel_error _dpm_execute_task(dpkernel_task *task, int thread_id)
{
    dpkernel_task_base *dpk_task = &task->base;
    dpkernel_error ret;
    print_debug("mq_polled_submission shm_ptr = %lu\n", *mq_polled_submission);
    // dpkernel_task_base *task = dpm_get_task_ptr_from_shmptr(*mq_polled_submission);
    print_debug("got task from shm_ptr: %p\n", task);

    struct dpm_kernel *executor = get_kernel(dpk_task->device, (enum dpm_task_name)dpk_task->task);
    if (executor == NULL || executor->execute == NULL) [[unlikely]]
    {
        printf("ERROR: no executable kernel found for device = %d, task = %d\n", dpk_task->device, dpk_task->task);
        return DPK_ERROR_FAILED;
    }
    ////struct dpm_task_executor *executor = &dpm_executors[DEVICE_NULL][TASK_NULL];

    ret = executor->execute(task, thread_id);

    if (ret == DPK_SUCCESS) [[likely]]
    {
        // active_kernels[dpk_task->device][dpk_task->task]++;
#ifdef CHECK_LATENCY
        dpk_task->execution_timestamp = std::chrono::high_resolution_clock::now().time_since_epoch().count();
#endif
        print_debug("_dpm_execute_task kernel submitted to device successfully\n");
    }
    else if (ret == DPK_ERROR_AGAIN) // the backing hw accelerator is full, etc.
    {
        print_debug("kernel %d EAGAIN\n", task->name);
        // task->completion.store(DPK_ERROR_AGAIN);
    }
    else
    {
        print_debug("kernel %d  failed\n", task->name);
        // task->completion.store(DPK_ERROR_FAILED);
    }
    return ret;
}

dpkernel_error _handle_mem_req(struct dpm_mem_req *req)
{
    print_debug("_handle_mem_req got mem req: %p\n", req);
    print_debug("req->type: %d\n", req->type);
    dpkernel_task *task = dpm_get_task_ptr_from_shmptr(req->task);
    auto &dpk_task = task->base;

    // NOTE(original): kernel device was derived from dpk_task.device (or underlying if NONE).
    // This breaks when scheduling later mutates dpk_task.device to DEVICE_SOFTWARE after a HW kernel already
    // created device-specific memory resources (e.g., DOCA doca_buf refs) during ALLOC_* mem requests.
    // In that case, FREE_* would be dispatched to SW (no handle_mem_req), leaking those HW resources.
    // dpm_device device = dpk_task.device == DEVICE_NONE ? dpm_underlying_device : dpk_task.device;

    auto is_free_mem_req = [&](enum dpm_mem_req_type type) -> bool {
        switch (type)
        {
        case DPM_MEM_REQ_FREE_INPUT:
        case DPM_MEM_REQ_FREE_OUTPUT:
        case DPM_MEM_REQ_FREE_IO:
        case DPM_MEM_REQ_FREE_COALESCED:
        case DPM_MEM_REQ_FREE_TASK:
            return true;
        default:
            return false;
        }
    };

    // Resolve kernel device for this mem request.
    // - For FREE_*: prefer the stable mem_device (device that created kernel-specific resources).
    // - Otherwise: use the current execution device if set, else underlying device.
    dpm_device device_for_req =
        (is_free_mem_req(req->type) && dpk_task.mem_device != DEVICE_NONE)
            ? dpk_task.mem_device
            : (dpk_task.device == DEVICE_NONE ? dpm_underlying_device : dpk_task.device);

    auto kernel = get_kernel(device_for_req, (enum dpm_task_name)dpk_task.task);

    auto handle_kernel_mem_req_for_type = [&](enum dpm_mem_req_type mem_req_type) -> dpkernel_error {
        if (kernel->handle_mem_req != NULL)
        {
            struct dpm_mem_req kernel_req = {};
            kernel_req.type = mem_req_type;
            kernel_req.task = req->task;
            if (kernel->handle_mem_req(&kernel_req)) [[likely]]
            {
                print_debug("device %d kernel %d handled mem req type=%d\n", task->device, task->task, mem_req_type);
                return DPK_SUCCESS;
            }
            printf("device %d kernel %d failed to handle mem req type=%d\n", dpk_task.device, dpk_task.task, mem_req_type);
            return DPK_ERROR_FAILED;
        }
        // no kernel specific mem req handler
        return DPK_SUCCESS;
    };
    auto handle_kernel_mem_req = [&]() -> dpkernel_error { return handle_kernel_mem_req_for_type(req->type); };
    auto refresh_external_ownership_flag = [&]() {
        dpk_task.io_is_externally_owned =
            (dpk_task.in_binding_state == DPM_IO_BINDING_EXTERNAL ||
             dpk_task.out_binding_state == DPM_IO_BINDING_EXTERNAL);
    };

    switch (req->type)
    {
    case DPM_MEM_REQ_ALLOC_INPUT: {
        // Bind mem_device on first alloc so later FREE_* can be dispatched to the allocating kernel even
        // if scheduling mutates dpk_task.device to DEVICE_SOFTWARE.
        if (dpk_task.mem_device == DEVICE_NONE)
        {
            dpk_task.mem_device = device_for_req;
        }
        if (dpk_task.in_binding_state == DPM_IO_BINDING_UNBOUND)
        {
            if (dpk_task.io_is_externally_owned)
            {
                // External caller pre-seeded task->in/task->in_size; only kernel-side setup is required.
                dpk_task.in_binding_state = DPM_IO_BINDING_EXTERNAL;
            }
            else
            {
                char *buf = allocate_input_buf(dpk_task.in_size);
                if (buf == nullptr)
                {
                    printf("failed to allocate input buf, size = %u\n", dpk_task.in_size);
                    req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                    return DPK_ERROR_FAILED;
                }
                dpk_task.in = dpm_get_shm_ptr_for_input_buf(buf);
                dpk_task.in_binding_state = DPM_IO_BINDING_DPM_OWNED;
            }
        }
        else if (dpk_task.in_binding_state == DPM_IO_BINDING_DPM_OWNED && dpk_task.in == (shm_ptr)0)
        {
            printf("inconsistent input binding state: DPM_OWNED but shm_ptr=0\n");
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        refresh_external_ownership_flag();

#ifdef MEMCPY_KERNEL
        // also allocate the cpy buffers
        char *in_cpy_buf = allocate_input_buf(dpk_task.in_size);
        if (in_cpy_buf == nullptr)
        {
            printf("failed to allocate input cpy buf, size = %u\n", dpk_task.in_size);
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        dpk_task.in_cpy = dpm_get_shm_ptr_for_input_buf(in_cpy_buf);
#endif
        print_debug("allocated input buf: %lu\n", req->buf);
        break;
    }
    case DPM_MEM_REQ_ALLOC_OUTPUT: {
        // Bind mem_device on first alloc so later FREE_* can be dispatched to the allocating kernel even
        // if scheduling mutates dpk_task.device to DEVICE_SOFTWARE.
        if (dpk_task.mem_device == DEVICE_NONE)
        {
            dpk_task.mem_device = device_for_req;
        }
        if (dpk_task.out_binding_state == DPM_IO_BINDING_UNBOUND)
        {
            if (dpk_task.io_is_externally_owned)
            {
                // External caller pre-seeded task->out/task->out_size; only kernel-side setup is required.
                dpk_task.out_binding_state = DPM_IO_BINDING_EXTERNAL;
            }
            else
            {
                char *buf = allocate_output_buf(dpk_task.out_size);
                if (buf == nullptr)
                {
                    printf("failed to allocate output buf, size = %u\n", dpk_task.out_size);
                    req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                    return DPK_ERROR_FAILED;
                }
                dpk_task.out = dpm_get_shm_ptr_for_output_buf(buf);
                dpk_task.out_binding_state = DPM_IO_BINDING_DPM_OWNED;
            }
        }
        else if (dpk_task.out_binding_state == DPM_IO_BINDING_DPM_OWNED && dpk_task.out == (shm_ptr)0)
        {
            printf("inconsistent output binding state: DPM_OWNED but shm_ptr=0\n");
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        refresh_external_ownership_flag();

#ifdef MEMCPY_KERNEL
        // also allocate the cpy buffers
        char *out_cpy_buf = allocate_output_buf(dpk_task.out_size);
        if (out_cpy_buf == nullptr)
        {
            printf("failed to allocate output cpy buf, size = %u\n", dpk_task.out_size);
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        dpk_task.out_cpy = dpm_get_shm_ptr_for_output_buf(out_cpy_buf);
#endif
        print_debug("allocated output buf: %lu\n", req->buf);
        break;
    }
    case DPM_MEM_REQ_ALLOC_IO: {
        // Bind mem_device on first alloc so later FREE_* can be dispatched to the allocating kernel even
        // if scheduling mutates dpk_task.device to DEVICE_SOFTWARE.
        if (dpk_task.mem_device == DEVICE_NONE)
        {
            dpk_task.mem_device = device_for_req;
        }

        if (dpk_task.in_binding_state == DPM_IO_BINDING_UNBOUND)
        {
            if (dpk_task.io_is_externally_owned)
            {
                dpk_task.in_binding_state = DPM_IO_BINDING_EXTERNAL;
            }
            else
            {
                char *in_buf = allocate_input_buf(dpk_task.in_size);
                if (in_buf == nullptr)
                {
                    printf("failed to allocate input buf for ALLOC_IO, size = %u\n", dpk_task.in_size);
                    req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                    return DPK_ERROR_FAILED;
                }
                dpk_task.in = dpm_get_shm_ptr_for_input_buf(in_buf);
                dpk_task.in_binding_state = DPM_IO_BINDING_DPM_OWNED;
            }
        }
        else if (dpk_task.in_binding_state == DPM_IO_BINDING_DPM_OWNED && dpk_task.in == (shm_ptr)0)
        {
            printf("inconsistent input binding state in ALLOC_IO: DPM_OWNED but shm_ptr=0\n");
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }

        if (dpk_task.out_binding_state == DPM_IO_BINDING_UNBOUND)
        {
            if (dpk_task.io_is_externally_owned)
            {
                dpk_task.out_binding_state = DPM_IO_BINDING_EXTERNAL;
            }
            else
            {
                char *out_buf = allocate_output_buf(dpk_task.out_size);
                if (out_buf == nullptr)
                {
                    printf("failed to allocate output buf for ALLOC_IO, size = %u\n", dpk_task.out_size);
                    req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                    return DPK_ERROR_FAILED;
                }
                dpk_task.out = dpm_get_shm_ptr_for_output_buf(out_buf);
                dpk_task.out_binding_state = DPM_IO_BINDING_DPM_OWNED;
            }
        }
        else if (dpk_task.out_binding_state == DPM_IO_BINDING_DPM_OWNED && dpk_task.out == (shm_ptr)0)
        {
            printf("inconsistent output binding state in ALLOC_IO: DPM_OWNED but shm_ptr=0\n");
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        refresh_external_ownership_flag();

        // Reuse existing kernel handlers to create kernel-side resources for both buffers.
        if (handle_kernel_mem_req_for_type(DPM_MEM_REQ_ALLOC_INPUT) != DPK_SUCCESS ||
            handle_kernel_mem_req_for_type(DPM_MEM_REQ_ALLOC_OUTPUT) != DPK_SUCCESS)
        {
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
        return DPK_SUCCESS;
    }
    case DPM_MEM_REQ_ALLOC_TASK: {
        // Bind mem_device for task-object lifecycle so FREE_TASK is routed back to same kernel backend.
        if (dpk_task.mem_device == DEVICE_NONE)
        {
            dpk_task.mem_device = device_for_req;
        }
        if (handle_kernel_mem_req() != DPK_SUCCESS)
        {
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
        return DPK_SUCCESS;
    }
    case DPM_MEM_REQ_FREE_TASK: {
        if (handle_kernel_mem_req() != DPK_SUCCESS)
        {
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        // Keep mem_device unchanged here; FREE_IO may still need stable routing to release src/dst kernel resources.
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
        return DPK_SUCCESS;
    }
    case DPM_MEM_REQ_FREE_INPUT: {
        // Modified order: release kernel-side resources first (e.g., DOCA buf refs) before freeing host memory.
        if (handle_kernel_mem_req() != DPK_SUCCESS)
        {
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        if (dpk_task.in_binding_state == DPM_IO_BINDING_DPM_OWNED)
        {
            if (dpk_task.in == (shm_ptr)0)
            {
                printf("inconsistent input free: DPM_OWNED but shm_ptr=0\n");
                req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                return DPK_ERROR_FAILED;
            }
            mi_free(dpm_get_input_ptr_from_shmptr(dpk_task.in));
        }
        dpk_task.in = (shm_ptr)0;
        dpk_task.in_binding_state = DPM_IO_BINDING_UNBOUND;
#ifdef MEMCPY_KERNEL
        if (dpk_task.in_cpy != (shm_ptr)0)
        {
            mi_free(dpm_get_input_ptr_from_shmptr(dpk_task.in_cpy));
            dpk_task.in_cpy = (shm_ptr)0;
        }
#endif
        refresh_external_ownership_flag();
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
        return DPK_SUCCESS;
    }
    case DPM_MEM_REQ_FREE_IO: {
        // Release kernel-side resources first, matching the non-IO free path ordering.
        if (handle_kernel_mem_req_for_type(DPM_MEM_REQ_FREE_OUTPUT) != DPK_SUCCESS ||
            handle_kernel_mem_req_for_type(DPM_MEM_REQ_FREE_INPUT) != DPK_SUCCESS)
        {
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }

        // Free host buffers only for DPM-owned bindings; avoid double-free on aliased in/out pointers.
        char *freed_output_ptr = nullptr;
        if (dpk_task.out_binding_state == DPM_IO_BINDING_DPM_OWNED)
        {
            if (dpk_task.out == (shm_ptr)0)
            {
                printf("inconsistent output free_io: DPM_OWNED but shm_ptr=0\n");
                req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                return DPK_ERROR_FAILED;
            }
            freed_output_ptr = dpm_get_output_ptr_from_shmptr(dpk_task.out);
            mi_free(freed_output_ptr);
        }
        if (dpk_task.in_binding_state == DPM_IO_BINDING_DPM_OWNED)
        {
            if (dpk_task.in == (shm_ptr)0)
            {
                printf("inconsistent input free_io: DPM_OWNED but shm_ptr=0\n");
                req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                return DPK_ERROR_FAILED;
            }
            char *input_ptr = dpm_get_input_ptr_from_shmptr(dpk_task.in);
            if (input_ptr != freed_output_ptr)
            {
                mi_free(input_ptr);
            }
        }
        dpk_task.out = (shm_ptr)0;
        dpk_task.in = (shm_ptr)0;
        dpk_task.out_binding_state = DPM_IO_BINDING_UNBOUND;
        dpk_task.in_binding_state = DPM_IO_BINDING_UNBOUND;
        dpk_task.io_is_externally_owned = false;
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
        return DPK_SUCCESS;
    }
    case DPM_MEM_REQ_FREE_OUTPUT: {
        // Modified order: release kernel-side resources first (e.g., DOCA buf refs) before freeing host memory.
        if (handle_kernel_mem_req() != DPK_SUCCESS)
        {
            req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
            return DPK_ERROR_FAILED;
        }
        if (dpk_task.out_binding_state == DPM_IO_BINDING_DPM_OWNED)
        {
            if (dpk_task.out == (shm_ptr)0)
            {
                printf("inconsistent output free: DPM_OWNED but shm_ptr=0\n");
                req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                return DPK_ERROR_FAILED;
            }
            mi_free(dpm_get_output_ptr_from_shmptr(dpk_task.out));
        }
        dpk_task.out = (shm_ptr)0;
        dpk_task.out_binding_state = DPM_IO_BINDING_UNBOUND;
#ifdef MEMCPY_KERNEL
        if (dpk_task.out_cpy != (shm_ptr)0)
        {
            mi_free(dpm_get_output_ptr_from_shmptr(dpk_task.out_cpy));
            dpk_task.out_cpy = (shm_ptr)0;
        }
#endif
        refresh_external_ownership_flag();
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
        return DPK_SUCCESS;
    }
    default:
    {
        printf("unknown mem req->type: %d\n", req->type);
        req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
        return DPK_ERROR_FAILED;
    }
    }

    // For non-free paths, run kernel-specific mem handling after host allocation.
    if (handle_kernel_mem_req() == DPK_SUCCESS)
    {
        req->completion.store(DPK_SUCCESS, std::memory_order_release);
        return DPK_SUCCESS;
    }
    req->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
    return DPK_ERROR_FAILED;
}

dpkernel_error _dpm_try_execute_or_queue_task(int thread_id, dpkernel_task *task, bool last_hw_eagain)
{
    dpkernel_error ret;
    dpkernel_task_base *dpk_task = &task->base;
    auto kernel_queue = hw_kernel_queues[thread_id][dpk_task->task];
    // dpm_kernel *kernel = get_kernel(dpk_task->device, dpk_task->task);
    // uint32_t max_capacity = 0;
    // uint32_t remaining_capacity = kernel->hw_kernel_remaining_capacity(thread_id, &max_capacity);
    print_debug("remaining capacity = %u, max capacity = %u\n", remaining_capacity, max_capacity);

#ifdef CHECK_LATENCY
    dpk_task->submission_timestamp = std::chrono::high_resolution_clock::now().time_since_epoch().count();
#endif

    // NOTE: this capacity check seems to be beneficial (a little)
    // if (remaining_capacity > 0) //[[likely]] // can directly execute
    if (!last_hw_eagain) // can probably still directly submit to hardware
    // if (true)
    {
        ret = _dpm_execute_task(task, thread_id);
        if (ret == DPK_ERROR_AGAIN) //[[unlikely]]
        {
            // well we have to queue it because the backing kernel is full
            print_debug("SHOULD NOT HAPPEN (DOCA BUG?): kernel %d EAGAIN to execute on thread %d\n", dpk_task->task,
                        thread_id);
            ////kernel_queue->push(msg->ptr);
            if (kernel_queue->push(task))
            {
                // nothing to do here
                // increment the inflight bytes
                print_debug("pushed task to hw kernel queue, device = %d, task = %d\n", dpk_task->device,
                            dpk_task->task);
                *hw_kernels_capacity[thread_id][dpk_task->task] =
                    *hw_kernels_capacity[thread_id][dpk_task->task] - 1; // decrement the hw kernel capacity
#ifdef SCHEDULING
                _inc_hw_queueing_delay_on_submit_or_enqueue(thread_id, dpk_task);
                /* _inc_inflight_bytes_on_submit_or_enqueue(*dpk_task, thread_id); */
#endif
                ret = DPK_ERROR_AGAIN;
            }
            else
            {
                // can't enqueue, queue is full, just fail it
                print_debug("thread %d kernel to be executed on hw, but queue full, device = %d, task = %d\n",
                            thread_id, dpk_task->device, dpk_task->task);
                dpk_task->completion.store(DPK_ERROR_AGAIN, std::memory_order_release);
            }
        }
        else if (ret == DPK_ERROR_FAILED) [[unlikely]]
        {
            printf("kernel %d failed to execute on thread %d\n", dpk_task->task, thread_id);
            dpk_task->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
        }
        else [[likely]] // success
        {
            // either enqueued, or submitted to hardware
            *hw_kernels_capacity[thread_id][dpk_task->task] =
                *hw_kernels_capacity[thread_id][dpk_task->task] - 1; // decrement the hw kernel capacity
#ifdef SCHEDULING
            _inc_hw_queueing_delay_on_submit_or_enqueue(thread_id, dpk_task);
            /* _inc_inflight_bytes_on_submit_or_enqueue(*dpk_task, thread_id); */
#endif
        }
    }
    else // must queue
    {
        if (kernel_queue->push(task))
        {
            // increment the inflight bytes
            print_debug("hw queue full, must push task to sw queue, device = %d, task = %d\n", dpk_task->device,
                        dpk_task->task);
            *hw_kernels_capacity[thread_id][dpk_task->task] =
                *hw_kernels_capacity[thread_id][dpk_task->task] - 1; // decrement the hw kernel capacity
#ifdef SCHEDULING
            _inc_hw_queueing_delay_on_submit_or_enqueue(thread_id, dpk_task);
            /* _inc_inflight_bytes_on_submit_or_enqueue(*dpk_task, thread_id); */
#endif
            ret = DPK_ERROR_AGAIN;
            // active_kernels[dpk_task->device][dpk_task->task]++;
        }
        else // hw kernel queue is full, just fail
        {
            print_debug("hw kernel software queue full, will EAGAIN, device = %d, task = %d\n", dpk_task->device,
                        dpk_task->task);
            ret = DPK_ERROR_AGAIN;
            dpk_task->completion.store(DPK_ERROR_AGAIN, std::memory_order_release);
        }
    }
    return ret;
}

#ifdef USE_RING_BUFFER
dpkernel_error dpm_poll_requests(int thread_id)
{
    dpkernel_error ret;
    struct dpm_req_msg *msg;

    FileIOSizeT req_size = 0;
    FileIOSizeT offset = 0;
    FileIOSizeT batch_offset = 0;
    RequestRingMsgQ *req_ring_buffer = dpm_submission_queue_ring_buffers[thread_id];
    char *req_ring_buffer_holder = req_ring_buffer_holders[thread_id];

    // this is used to compensate for the time we spent in handling all the requests here
    // which is non-negligible, since the estimated time is only decremented when completions are polled
#ifdef SCHEDULING
    *hw_kernels_sched_start_time[thread_id][dpm_task_name::TASK_DECOMPRESS_DEFLATE] =
        std::chrono::high_resolution_clock::now().time_since_epoch().count();
#endif

    if (FetchFromRequestBufferProgressive(req_ring_buffer, req_ring_buffer_holder, &req_size))
    {
        hw_eagain = false; // whenever we get EAGAIN from the kernel, we set this flag and don't bother with
                           // submitting to hw afterwards
        offset = 0;
        // process all requests in the ring buffer
        while (offset < req_size)
        {
            FileIOSizeT totalBytes =
                *(FileIOSizeT *)(req_ring_buffer_holder +
                                 offset); // total bytes of single submitted req, containing the leading int
            batch_offset = 0 + sizeof(FileIOSizeT);
            print_debug("totalBytes: %lu\n", totalBytes);

            while (batch_offset < totalBytes)
            {

                // FileIOSizeT totalBytes =
                //     *(FileIOSizeT *)(req_ring_buffer_holder + offset); // total bytes contain the leading int
                //                                                        // printf("totalBytes: %lu\n", totalBytes);
                // offset += sizeof(FileIOSizeT);
                msg = (struct dpm_req_msg *)(req_ring_buffer_holder + offset + batch_offset); //+ sizeof(FileIOSizeT)

                switch (msg->type)
                {
                // can always directly handle memory requests
                case dpm_req_type::DPM_REQ_TYPE_MEM:
                    // [[unlikely]]
                    {
                        // Handle memory request
                        struct dpm_mem_req *req = dpm_get_mem_req_ptr_from_shmptr(msg->ptr);
                        // dpkernel_task *task = dpm_get_task_ptr_from_shmptr(req->task);
                        print_debug("mem req got device %d task %d\n", task->device, task->task);
                        // auto mem_req_start = std::chrono::high_resolution_clock::now();
                        ret = _handle_mem_req(req);
                        // auto mem_req_end = std::chrono::high_resolution_clock::now();
                        // auto mem_req_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(mem_req_end -
                        // mem_req_start); printf("mem req duration: %lu ns\n", mem_req_duration.count());
                        if (ret != DPK_SUCCESS)
                        {
                            printf("failed to handle mem req\n");
                        }
                        print_debug("handled mem req, type = %d\n", req->type);

                        // return ret;
                        break;
                    }
                case dpm_req_type::DPM_REQ_TYPE_TASK:
                    // [[likely]] // task request
                    {
                        print_debug("got task request\n");
                        dpkernel_task *task = dpm_get_task_ptr_from_shmptr(msg->ptr);
                        dpkernel_task_base *dpk_task = &task->base;
                        print_debug("got task from shm_ptr: %p\n", task);
                        // #ifdef SCHEDULING
                        if (dpk_task->device == DEVICE_NONE) //[[unlikely]]
                        {
                            // device_is_user_specified = false;
                            // choose the optimal device for the task
                            print_debug("task->device is DEVICE_NONE\n");
                            dpk_task->device = _schedule_task_for_optimal_device(task, thread_id);
                        }
                        // #endif

                        // now we have a device (either we chose/scheduled it or the user specified it)
                        // If the chosen/forced HW executor is unavailable in this build (e.g., DOCA regex disabled),
                        // defensively reroute to software to avoid null execute pointers.
                        if (dpk_task->device != DEVICE_SOFTWARE &&
                            dpm_executors[dpk_task->device][dpk_task->task].execute == nullptr)
                        {
                            if (_is_sw_executor_available((enum dpm_task_name)dpk_task->task))
                            {
                                print_debug("kernel unavailable on device %d for task %d, falling back to software\n",
                                            dpk_task->device, dpk_task->task);
                                dpk_task->device = DEVICE_SOFTWARE;
                            }
                            else
                            {
                                printf("kernel unavailable on device %d for task %d and no software fallback exists\n",
                                       dpk_task->device, dpk_task->task);
                                dpk_task->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                                break;
                            }
                        }

                        bool is_sw_kernel = dpk_task->device == DEVICE_SOFTWARE;
                        // auto hw_kernel_queue = hw_kernel_queues[dpk_task->task];

                        // if is HW kernel, check remaining capacity and try to execute directly if possible
                        if (!is_sw_kernel) //[[likely]]
                        {
#if defined(COALESCING)
                        // check if we can coalesce the task
                        if (_can_coalesce_task(thread_id, dpk_task))
                        {
                            dpkernel_task *coalesced_task =
                                _coalesce_tasks(dpk_task, thread_id, dpk_task->device, dpk_task->task, false);
                            if (coalesced_task != NULL) // need to execute the coalesced task
                            {
                                ret = _dpm_try_execute_or_queue_task(thread_id, coalesced_task, hw_eagain);
                                if (ret == DPK_ERROR_AGAIN)
                                {
                                    hw_eagain = true;
                                }
                            }
                            // else nothing to execute
                        }
                        else
                        {
                            // cannot coalesce: we should force flush all the tasks there are
                            printf("cannot coalesce task %p, device = %d, task = %d, will force flush\n", dpk_task,
                                   dpk_task->device, dpk_task->task);
                            dpkernel_task *flush_task =
                                _coalesce_tasks(dpk_task, thread_id, dpk_task->device, dpk_task->task, true);
                            if (flush_task != NULL) // need to execute the coalesced task
                            {
                                ret = _dpm_try_execute_or_queue_task(thread_id, flush_task, hw_eagain);
                                if (ret == DPK_ERROR_AGAIN)
                                {
                                    hw_eagain = true;
                                }
                            }
                        }
#else
                        // try to execute it, or it will be queued up, or it will fail
                        ret = _dpm_try_execute_or_queue_task(thread_id, task, hw_eagain);
                        if (ret == DPK_ERROR_AGAIN)
                        {
                            hw_eagain = true;
                        }
#endif
                        }
                        else //[[unlikely]] // sw kernel, we always push to the ring buffer because the SW threads
                             //(pool)
                        // will just grab work from there
                        {
                            if (InsertToRequestBufferProgressive<SW_KERNELS_QUEUE_SIZE,
                                                                 SW_KERNELS_QUEUE_MAX_TAIL_ADVANCEMENT>(
                                    sw_kernel_ring_buffers[thread_id % N_SW_KERNELS_THREADS], (char *)msg,
                                    sizeof(*msg)))
                            {
                                // successfully enqueued
                                print_debug("pushed task %p to sw kernel ring buffer, device = %d, task = %d\n",
                                            dpk_task, dpk_task->device, dpk_task->task);
#ifdef CHECK_LATENCY
                                dpk_task->submission_timestamp =
                                    std::chrono::high_resolution_clock::now().time_since_epoch().count();
#endif

#ifdef SCHEDULING
                            _inc_sw_queueing_delay_on_submit_or_enqueue(thread_id % N_SW_KERNELS_THREADS, dpk_task);
#endif
                            }
                            else
                            {
                                // queue is full, eagain the task
                                print_debug("sw kernel to be scheduled on, but sw kernel queue is full, device = %d, "
                                            "task = %d\n",
                                            dpk_task->device, dpk_task->task);
                                dpk_task->completion.store(DPK_ERROR_AGAIN, std::memory_order_release);
                            }

                            //                             if (InsertToRequestBufferProgressive(
                            //                                     sw_kernel_ring_buffers[thread_id %
                            //                                     N_SW_KERNELS_THREADS], (char *)msg, sizeof(*msg)))
                            //                             {
                            //                                 // successfully enqueued
                            //                                 print_debug("pushed task %p to sw kernel ring buffer,
                            //                                 device = %d, task = %d\n",
                            //                                             dpk_task, dpk_task->device, dpk_task->task);
                            // #ifdef SCHEDULING
                            //                                 _inc_sw_queueing_delay_on_submit_or_enqueue(thread_id %
                            //                                 N_SW_KERNELS_THREADS, dpk_task);
                            //                                 /* _inc_inflight_bytes_on_submit_or_enqueue(*dpk_task,
                            //                                 thread_id); */
                            // #endif
                            //                             }
                            //                             else
                            //                             {
                            //                                 // can't enqueue, ring buffer is full, just fail it
                            //                                 printf(
                            //                                     "sw kernel to be scheduled on, but ring buffer is
                            //                                     full, device = %d, task = %d\n", dpk_task->device,
                            //                                     dpk_task->task);
                            //                                 dpk_task->completion.store(DPK_ERROR_AGAIN,
                            //                                 std::memory_order_release);
                            //                             }
                        }
                        break;
                    }
            default:
            {
                print_debug("unknown request msg type: %d\n", msg.type);
                // return DPK_ERROR_FAILED;
                break;
                // mq_polled_submission = NULL;
            }
            }
            batch_offset += sizeof(struct dpm_req_msg);
            }
            offset += totalBytes;
        }
        // processed all requests in this ring buffer
        // return ret;
        return DPK_SUCCESS;
    }

    // else didn't receive anything
    return DPK_ERROR_AGAIN;
}
#else // use mpmc queue
dpkernel_error dpm_poll_requests(int thread_id)
{
    dpkernel_error ret;
    struct dpm_req_msg msg;

    if (dpm_submission_queues_mpmc[thread_id]->try_pop(msg))
    {
        switch (msg.type)
        {
        // can always directly handle memory requests
        case dpm_req_type::DPM_REQ_TYPE_MEM:
            // [[unlikely]]
            {
                // Handle memory request
                struct dpm_mem_req *req = dpm_get_mem_req_ptr_from_shmptr(msg.ptr);
                // dpkernel_task *task = dpm_get_task_ptr_from_shmptr(req->task);
                print_debug("mem req got device %d task %d\n", task->device, task->task);
                // auto mem_req_start = std::chrono::high_resolution_clock::now();
                ret = _handle_mem_req(req);
                // auto mem_req_end = std::chrono::high_resolution_clock::now();
                // auto mem_req_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(mem_req_end -
                // mem_req_start); printf("mem req duration: %lu ns\n", mem_req_duration.count());
                if (ret != DPK_SUCCESS)
                {
                    printf("failed to handle mem req\n");
                }
                // mq_polled_submission = NULL;

                // return ret;
                break;
            }
        case dpm_req_type::DPM_REQ_TYPE_TASK:
            // [[likely]] // task request
            {
                print_debug("got task request\n");
                dpkernel_task *task = dpm_get_task_ptr_from_shmptr(msg.ptr);
                dpkernel_task_base *dpk_task = &task->base;
                print_debug("got task from shm_ptr: %p\n", task);
                if (dpk_task->device == DEVICE_NONE) [[unlikely]]
                {
                    // device_is_user_specified = false;
                    // choose the optimal device for the task
                    print_debug("task->device is DEVICE_NONE\n");
                    dpk_task->device = _schedule_task_for_optimal_device(task, thread_id);
                }

                if (dpk_task->device != DEVICE_SOFTWARE &&
                    dpm_executors[dpk_task->device][dpk_task->task].execute == nullptr)
                {
                    if (_is_sw_executor_available((enum dpm_task_name)dpk_task->task))
                    {
                        dpk_task->device = DEVICE_SOFTWARE;
                    }
                    else
                    {
                        printf("kernel unavailable on device %d for task %d and no software fallback exists\n",
                               dpk_task->device, dpk_task->task);
                        dpk_task->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                        break;
                    }
                }

                // now we have a device (either we chose/scheduled it or the user specified it)
                bool is_sw_kernel = dpk_task->device == DEVICE_SOFTWARE;
                // auto hw_kernel_queue = hw_kernel_queues[dpk_task->task];

                // if is HW kernel, check remaining capacity and try to execute directly if possible
                if (!is_sw_kernel) //[[likely]]
                {
                    // check if we can coalesce the task
                    /* if (_can_coalesce_task(*dpk_task))
                    {
                        if (_coalesce_task_and_submit(*dpk_task, thread_id))
                        {
                            // TODO: coalescing HOW??
                        }
                    } */

                    // try to execute it, or it will be queued up, or it will fail
                    ret = _dpm_try_execute_or_queue_task(thread_id, task, false);
                }
                else //[[unlikely]] // sw kernel, we always push to the ring buffer because the SW threads (pool)
                     // will
                     // just grab work from there
                {
                    if (InsertToRequestBufferProgressive(sw_kernel_ring_buffers[thread_id % N_SW_KERNELS_THREADS],
                                                         (char *)&msg, sizeof(msg)))
                    {
                        // successfully enqueued
                        printf("pushed task %p to sw kernel ring buffer, device = %d, task = %d\n", dpk_task,
                               dpk_task->device, dpk_task->task);
#ifdef SCHEDULING
                        _inc_sw_queueing_delay_on_submit_or_enqueue(thread_id % N_SW_KERNELS_THREADS, dpk_task);
                        /* _inc_inflight_bytes_on_submit_or_enqueue(*dpk_task, thread_id); */
#endif
                    }
                    else
                    {
                        // can't enqueue, ring buffer is full, just fail it
                        printf("sw kernel to be scheduled on, but ring buffer is full, device = %d, task = %d\n",
                               dpk_task->device, dpk_task->task);
                        dpk_task->completion.store(DPK_ERROR_AGAIN, std::memory_order_release);
                    }
                }
                break;
            }
        default: {
            print_debug("unknown request msg type: %d\n", msg.type);
            // return DPK_ERROR_FAILED;
            break;
            // mq_polled_submission = NULL;
        }
        }
        // processed this msg
        return DPK_SUCCESS;
    }

    // else didn't receive anything
    return DPK_ERROR_AGAIN;
}
#endif

dpkernel_error dpm_drain_kernel_queues(int thread_id)
{
    dpkernel_error ret;
    uint32_t remaining_capacity, max_capacity;
    for (uint8_t i = 0; i < dpm_task_name::TASK_NAME_LAST; i++)
    {
        dpm_task_name task_name = (dpm_task_name)i;
        // struct dpm_kernel *executor = get_kernel(dpm_underlying_device, task_name);
        // check if the executor exists and it provides execution
        // if (executor->hw_kernel_remaining_capacity == NULL)
        // {
        //     continue;
        // }

        // remaining_capacity = executor->hw_kernel_remaining_capacity(thread_id, &max_capacity);

        // Process tasks in the hw queue
        uint32_t j = 0;

        dpkernel_task *task;
        dpkernel_task_base *dpk_task;

        /* if (task_name == TASK_DECOMPRESS_DEFLATE && max_capacity - remaining_capacity != 0)
        {
            printf("draining kernel %d on thread %d, remaining capacity = %u, max capacity = %u\n", i, thread_id,
                   remaining_capacity, max_capacity);
        } */

        // NOTE: checking remaining capacity actually slow things down a bit...
        // while (j < remaining_capacity)
        while (true)
        {
            // get a task and let the kernel execute it
            // if (hw_kernel_queues[i][thread_id].pop(task_ptr))
            if (hw_kernel_queues[thread_id][task_name]->pop(task))
            {
#ifdef COALESCING
                if (!task->base.is_coalesced_head)
                {
                    printf("IMPOSSIBLE: draining queue but got non head of coalesced task %p, device = %d, task = %d\n",
                           task, task->base.device, task->base.task);
                }
                // got a task, execute it
                // task = dpm_get_task_ptr_from_shmptr(task_ptr);
                dpk_task = &task->base;
                print_debug("thread %d got task %d from hw queue\n", thread_id, dpk_task->task);
#endif

#if defined(COALESCING_DONTUSE)
                dpkernel_task *task_to_execute = nullptr;
                // check if we can coalesce the task
                if (_can_coalesce_task(thread_id, dpk_task))
                {
                    dpkernel_task *coalesced_task =
                        _coalesce_tasks(dpk_task, thread_id, dpk_task->device, dpk_task->task, false);
                    // if (coalesced_task != NULL) // need to execute the coalesced task
                    {
                        task_to_execute = coalesced_task;
                    }
                    // else nothing to execute, still coalescing
                }
                else
                {
                    // cannot coalesce
                    printf("cannot coalesce task %p, device = %d, task = %d\n", dpk_task, dpk_task->device,
                           dpk_task->task);
                    task_to_execute = task;
                }

                if (task_to_execute == NULL)
                {
                    // this is because we are still coalescing, just go on
                    continue;
                }
                ret = _dpm_execute_task(task_to_execute, thread_id);
                if (ret == DPK_ERROR_AGAIN) //[[unlikely]]
                {
                    // push it to the front of queue (again)
                    // if (hw_kernel_queues[thread_id][task_name]->push_front(task))
                    if (hw_kernel_queues[thread_id][task_name]->undo_pop())
                    {
                        print_debug("when draining got EAGAIN, pushed task to hw kernel queue, device = %d, "
                                    "task = %d\n",
                                    dpk_task->device, dpk_task->task);
                    }
                    break;
                }
                else if (ret == DPK_ERROR_FAILED) [[unlikely]]
                {
                    printf("kernel %d failed to execute on thread %d\n", dpk_task->task, thread_id);
                    dpk_task->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                }
                // else success
                else
                {
                    print_debug("kernel %d drained on thread %d\n", dpk_task->task, thread_id);
                }
                j++;
#ifdef SCHEDULING
                ////NOTE: there's no need to inc again when draining, since increment happens during enqueuing
                // _inc_hw_queueing_delay_on_submit_or_enqueue(thread_id, dpk_task);
                /* _inc_inflight_bytes_on_submit_or_enqueue(*dpk_task, thread_id); */
#endif
#else

                // can directly execute
                ret = _dpm_execute_task(task, thread_id);
                if (ret == DPK_ERROR_AGAIN) //[[unlikely]]
                {
                    print_debug("SHOULD NOT HAPPEN: draining kernel %d EAGAIN to execute on thread %d\n",
                                dpk_task->task, thread_id);
                    print_debug("remaining capacity = %u, max capacity = %u\n", remaining_capacity, max_capacity);
                    // push it to the front of queue (again)
                    // if (hw_kernel_queues[thread_id][task_name]->push_front(task))
                    if (hw_kernel_queues[thread_id][task_name]->undo_pop())
                    {
                        print_debug(
                            "when draining got EAGAIN, pushed task to hw kernel queue, device = %d, task = %d\n",
                            dpk_task->device, dpk_task->task);
                        break;
                    }
                    else // should never happen!!
                    {
                        // can't enqueue, queue is full, just fail it
                        printf("when draining: undo pop task failed, thread %d kernel kernel hw queue full, device = "
                               "%d, task = "
                               "%d\n",
                               thread_id, dpk_task->device, dpk_task->task);
                        dpk_task->completion.store(DPK_ERROR_AGAIN, std::memory_order_release);
                    }
                    break;
                }
                else if (ret == DPK_ERROR_FAILED) [[unlikely]]
                {
                    printf("kernel %d failed to execute on thread %d\n", dpk_task->task, thread_id);
                    dpk_task->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
                }
                // else success
                else
                {
                    print_debug("kernel %d drained on thread %d\n", dpk_task->task, thread_id);
                }
                j++;
#ifdef SCHEDULING
                ////NOTE: there's no need to inc again when draining, since increment happens during enqueuing
                // _inc_hw_queueing_delay_on_submit_or_enqueue(thread_id, dpk_task);
                /* _inc_inflight_bytes_on_submit_or_enqueue(*dpk_task, thread_id); */
#endif

#endif // end of coalescing
            }
            else // got no task from the queue, we are done
            {
                break;
            }
        }
    }
    return DPK_SUCCESS;
}

// the polling loop should complete the task, and then set the flag in dpkernel_task_base, then the application
// can poll for completion
void dpm_poll_completion(int thread_id)
{
    dpkernel_task *task;
    dpkernel_error ret;
    struct dpm_kernel *kernel;
    // for each ACTIVE kernel, poll for completion
    // for (int i = 0; i < DEVICE_LAST; i++)
    // {
    for (int j = 0; j < TASK_NAME_LAST; j++)
    {
        kernel = get_kernel(dpm_underlying_device, (dpm_task_name)j);

// while (inflight_bytes[thread_id][(dpm_device)i][(dpm_task_name)j] > 0)

// while there is some task outstanding (to poll for)
#ifdef SCHEDULING
        // NOTE: this would lead to floating point inprecision
        // while (estimated_hw_kernels_queuing_time[thread_id][(dpm_task_name)j] > 0)
        while (true)
#else
        while (true)
#endif
        {
            // check executor exists and it provides completion polling
            if (kernel == NULL || kernel->poll == NULL)
            {
                break;
            }
            // otherwise, can poll for completion
            ret = kernel->poll(&task, thread_id);

            if (ret == DPK_SUCCESS)
            {
                // active_kernels[i][j]--;

                auto &dpk_task = task->base;
                print_debug("poll got completion, actual_out_size: %u\n", dpk_task.actual_out_size);
                *hw_kernels_capacity[thread_id][dpk_task.task] =
                    *hw_kernels_capacity[thread_id][dpk_task.task] + 1; // increment the hw kernel capacity
#ifdef CHECK_LATENCY
                task->base.completion_timestamp = std::chrono::high_resolution_clock::now().time_since_epoch().count();
#endif

#ifdef SCHEDULING
                _dec_hw_queueing_delay_on_completion(thread_id, &dpk_task);
                /* _update_kernel_draining_rate(dpk_task, thread_id);
                _dec_inflight_bytes_on_completion(dpk_task, thread_id); */
#endif

                // set the completion on behalf of kernel
                print_debug("dpm polled completion, setting completion %u\n", ret);
                dpk_task.completion.store(ret);
            }
            else if (ret == DPK_ERROR_FAILED)
            {

                printf("poll got error failed\n");
                // no need to touch the task, we got nothing into it anyway
                auto &dpk_task = task->base;

                *hw_kernels_capacity[thread_id][dpk_task.task] =
                    *hw_kernels_capacity[thread_id][dpk_task.task] + 1; // increment the hw kernel capacity

                dpk_task.completion.store(ret);
            }
            else // poll got nothing, we have drained all the completions for now
            {
                break;
            }
        }
    }
    // }
}

int dp_kernel_manager_msgq_stop()
{
    polling_loop.store(false);
    dpm_runtime_ready.store(false, std::memory_order_release);

    printf("cleaning up sw kernels\n");
    dpm_cleanup_sw_kernels();
    printf("clean up sw kernels done\n");

    // wait for the threads to finish (starting from 1, 0 is self and unused)
    for (int i = 1; i < N_DPM_THREADS; i++)
    {
        if (dpm_threads[i].joinable())
        {
            dpm_threads[i].join();
            printf("DPM thread %d joined\n", i);
        }
    }
    return 0;
}

void dp_kernel_manager_set_thread_pinning(bool enable)
{
    dpm_thread_pinning_enabled.store(enable, std::memory_order_release);
}

bool dp_kernel_manager_is_ready()
{
    return dpm_runtime_ready.load(std::memory_order_acquire);
}

bool cleanup_dpm_msgq()
{
    bool ret = true;
    // ret = boost::interprocess::message_queue::remove(SUBMISSION_QUEUE_NAME);
    // ret &= boost::interprocess::message_queue::remove(COMPLETION_QUEUE_NAME);

    /// close shared memory
    /* if (munmap(dpm_submission_queue_mpmc, sizeof(*dpm_submission_queue_mpmc)) == -1)
    {
        perror("munmap dpm_submission_queue_mpmc");
        // return -1;
    } */
    // close(dpm_submission_queue_mpmc->allocator_.shm_fd);
    /* if (shm_unlink(SUBMISSION_QUEUE_BACKING_SHM_NAME) == -1)
    {
        // perror("shm_unlink");
        return -1;
    } */

    // cleanup memory
    printf("cleanup bluefield\n");
    // ret &= cleanup_bluefield();
    ret &= dpm_device_initializers.cleanup_device(dpm_device_initializers.setup_device_ctx);

    /// cleanup shared memory
    printf("cleanup shared memory\n");
    printf("teardown_mimalloc dpm_req_ctx_shm\n");
    teardown_mimalloc(&dpm_req_ctx_shm, DPM_REQ_CTX_SHM_NAME);
    if (dpm_io_regions_share_backing(&dpm_own_mem_region))
    {
        printf("teardown_mimalloc dpm_own_mem_region.combined_io_region\n");
        teardown_mimalloc(&dpm_own_mem_region.input_region, DPM_SHM_IO_REGION_NAME);
    }
    else
    {
        printf("teardown_mimalloc dpm_own_mem_region.input_region\n");
        teardown_mimalloc(&dpm_own_mem_region.input_region, DPM_SHM_INPUT_REGION_NAME);
        printf("teardown_mimalloc dpm_own_mem_region.output_region\n");
        teardown_mimalloc(&dpm_own_mem_region.output_region, DPM_SHM_OUTPUT_REGION_NAME);
    }
    printf("cleaned up shared memory\n");
    return ret;
}
