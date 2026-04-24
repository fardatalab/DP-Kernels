#include "dpk_embed_c_api.h"

#include "dp_manager_msgq.hpp"
#include "dpm_interface.hpp"
#include "memory.hpp"

#include <chrono>
#include <cstring>
#include <mutex>
#include <thread>

struct dpk_embed_task_handle
{
    uint8_t stage_type = 0;
    bool io_bound = false;
    uint64_t bound_io_base_addr = 0;
    uint32_t bound_io_bytes = 0;
    dpkernel_task *tasks_by_thread[N_DPM_THREADS] = {};
    bool prepared_by_thread[N_DPM_THREADS] = {};
};

namespace
{
std::mutex g_runtime_lock;
std::thread g_manager_thread;
bool g_runtime_started = false;

std::mutex g_key_lock;
uint64_t g_global_key_shm = 0;
uint8_t g_global_key_size = 0;
void *g_global_key_ptr = nullptr;

inline bool is_runtime_ready_unsafe()
{
    return g_runtime_started && dp_kernel_manager_is_ready();
}

inline bool convert_abs_addr_to_shm(uint64_t abs_addr, uint32_t len, shm_ptr *out_shm)
{
    if (out_shm == nullptr)
    {
        return false;
    }
    uintptr_t io_base = (uintptr_t)app_own_mem_region.input_region.shm_ptr;
    uintptr_t io_size = (uintptr_t)app_own_mem_region.input_region.shm_size;
    uintptr_t start = (uintptr_t)abs_addr;
    uintptr_t end = start + (uintptr_t)len;
    if (start < io_base || end < start || end > io_base + io_size)
    {
        return false;
    }
    *out_shm = (shm_ptr)(start - io_base);
    return true;
}

inline bool wait_task_completion(dpkernel_task *task, uint32_t timeout_ms)
{
    auto start = std::chrono::steady_clock::now();
    while (true)
    {
        dpkernel_error status = app_check_task_completion(task);
        if (status == DPK_SUCCESS)
        {
            return true;
        }
        if (status == DPK_ERROR_FAILED || status == DPK_ERROR_AGAIN)
        {
            return false;
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
        if (elapsed.count() > (long)timeout_ms)
        {
            return false;
        }
        std::this_thread::yield();
    }
}

inline bool is_valid_submit_thread_id(uint32_t submit_thread_id)
{
    return submit_thread_id < (uint32_t)N_DPM_THREADS;
}

inline bool init_task_metadata_for_stage(dpkernel_task *task, uint8_t stage_type, uint32_t submit_thread_id)
{
    if (task == nullptr || !is_valid_submit_thread_id(submit_thread_id))
    {
        return false;
    }

    if (stage_type == DPK_EMBED_STAGE_AES_GCM)
    {
        task->base.task = TASK_AES_GCM;
        task->bf3_aes_gcm.callback_owner_thread_id = (uint8_t)submit_thread_id;
    }
    else if (stage_type == DPK_EMBED_STAGE_DECOMPRESS_DEFLATE)
    {
        task->base.task = TASK_DECOMPRESS_DEFLATE;
        task->bf3.callback_owner_thread_id = (uint8_t)submit_thread_id;
    }
    else
    {
        return false;
    }
    task->base.device = DEVICE_NONE;
    dpm_set_task_lazy_bind_mode(task, true);
    return true;
}

inline void apply_bound_io_region_to_task(dpkernel_task *task, shm_ptr io_base_shm, uint32_t io_bytes)
{
    task->base.in = io_base_shm;
    task->base.out = io_base_shm;
    task->base.in_size = io_bytes;
    task->base.out_size = io_bytes;
    task->base.in_binding_state = DPM_IO_BINDING_EXTERNAL;
    task->base.out_binding_state = DPM_IO_BINDING_EXTERNAL;
    task->base.io_is_externally_owned = true;
}

inline dpkernel_task *get_or_create_thread_task(dpk_embed_task_handle *handle, uint32_t submit_thread_id)
{
    if (handle == nullptr || !is_valid_submit_thread_id(submit_thread_id))
    {
        return nullptr;
    }
    dpkernel_task *task = handle->tasks_by_thread[submit_thread_id];
    if (task != nullptr)
    {
        return task;
    }
    if (!app_alloc_task_request(&task) || task == nullptr)
    {
        return nullptr;
    }
    if (!init_task_metadata_for_stage(task, handle->stage_type, submit_thread_id))
    {
        app_free_task_request(task);
        return nullptr;
    }
    handle->tasks_by_thread[submit_thread_id] = task;
    return task;
}

int release_thread_resources_internal(dpk_embed_task_handle *handle, uint32_t submit_thread_id)
{
    if (handle == nullptr || !is_valid_submit_thread_id(submit_thread_id))
    {
        return -1;
    }
    dpkernel_task *task = handle->tasks_by_thread[submit_thread_id];
    if (task == nullptr)
    {
        handle->prepared_by_thread[submit_thread_id] = false;
        return 0;
    }

    if (handle->prepared_by_thread[submit_thread_id])
    {
        auto free_task_req = dpm_free_task_async_on_thread(task, submit_thread_id);
        if (free_task_req != nullptr && !dpm_wait_for_mem_req_completion(free_task_req))
        {
            return -1;
        }

        auto free_io_req = dpm_free_mem_buf_async_on_thread(task, submit_thread_id);
        if (free_io_req != nullptr && !dpm_wait_for_mem_req_completion(free_io_req))
        {
            return -1;
        }
    }

    handle->prepared_by_thread[submit_thread_id] = false;
    app_free_task_request(task);
    handle->tasks_by_thread[submit_thread_id] = nullptr;
    return 0;
}
} // namespace

int dpk_embed_runtime_start(const dpk_embed_runtime_config *config)
{
    if (config == nullptr || config->io_base == nullptr || config->io_bytes == 0)
    {
        return -1;
    }

    std::lock_guard<std::mutex> lock(g_runtime_lock);
    if (is_runtime_ready_unsafe())
    {
        return 0;
    }

    if (!dpm_configure_external_io_region(config->io_base, config->io_bytes))
    {
        return -1;
    }

    dp_kernel_manager_set_thread_pinning(config->enable_thread_pinning != 0);

    if (!g_manager_thread.joinable())
    {
        g_manager_thread = std::thread([]() { dp_kernel_manager_msgq_start(nullptr); });
    }

    uint32_t timeout_ms = (config->startup_timeout_ms == 0) ? 30000 : config->startup_timeout_ms;
    auto start = std::chrono::steady_clock::now();
    while (!dp_kernel_manager_is_ready())
    {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
        if ((uint32_t)elapsed.count() > timeout_ms)
        {
            if (g_manager_thread.joinable())
            {
                dp_kernel_manager_msgq_stop();
                g_manager_thread.join();
            }
            return -1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (!dpm_frontend_initialize_inproc())
    {
        if (g_manager_thread.joinable())
        {
            dp_kernel_manager_msgq_stop();
            g_manager_thread.join();
        }
        return -1;
    }

    g_runtime_started = true;
    return 0;
}

int dpk_embed_runtime_stop(void)
{
    std::lock_guard<std::mutex> lock(g_runtime_lock);

    {
        std::lock_guard<std::mutex> key_lock(g_key_lock);
        if (g_global_key_ptr != nullptr && g_runtime_started)
        {
            app_free_req_ctx_buffer(g_global_key_ptr);
        }
        g_global_key_ptr = nullptr;
        g_global_key_shm = 0;
        g_global_key_size = 0;
    }

    if (g_manager_thread.joinable())
    {
        dp_kernel_manager_msgq_stop();
        g_manager_thread.join();
    }
    g_runtime_started = false;
    return 0;
}

int dpk_embed_runtime_is_ready(void)
{
    std::lock_guard<std::mutex> lock(g_runtime_lock);
    return is_runtime_ready_unsafe() ? 1 : 0;
}

int dpk_embed_get_max_submit_threads(void)
{
    return (int)N_DPM_THREADS;
}

int dpk_embed_task_create(uint8_t stage_type, dpk_embed_task_handle **out_handle)
{
    if (out_handle == nullptr)
    {
        return -1;
    }
    if (stage_type != DPK_EMBED_STAGE_AES_GCM && stage_type != DPK_EMBED_STAGE_DECOMPRESS_DEFLATE)
    {
        return -1;
    }

    auto *handle = new dpk_embed_task_handle();
    handle->stage_type = stage_type;
    *out_handle = handle;
    return 0;
}

int dpk_embed_task_destroy(dpk_embed_task_handle *handle)
{
    if (handle == nullptr)
    {
        return 0;
    }

    for (uint32_t thread_id = 0; thread_id < (uint32_t)N_DPM_THREADS; thread_id++)
    {
        (void)release_thread_resources_internal(handle, thread_id);
    }
    delete handle;
    return 0;
}

int dpk_embed_task_bind_io_region(dpk_embed_task_handle *handle, uint64_t io_base_addr, uint32_t io_bytes)
{
    if (handle == nullptr || io_base_addr == 0 || io_bytes == 0)
    {
        return -1;
    }

    shm_ptr io_base_shm = 0;
    if (!convert_abs_addr_to_shm(io_base_addr, io_bytes, &io_base_shm))
    {
        return -1;
    }

    if (handle->io_bound && handle->bound_io_base_addr == (uint64_t)io_base_shm && handle->bound_io_bytes == io_bytes)
    {
        return 0;
    }

    // Rebind path: release all previously prepared per-thread resources first.
    if (handle->io_bound)
    {
        for (uint32_t thread_id = 0; thread_id < (uint32_t)N_DPM_THREADS; thread_id++)
        {
            if (release_thread_resources_internal(handle, thread_id) != 0)
            {
                return -1;
            }
        }
    }

    handle->io_bound = true;
    handle->bound_io_base_addr = (uint64_t)io_base_shm;
    handle->bound_io_bytes = io_bytes;
    return 0;
}

int dpk_embed_task_prepare_for_thread(dpk_embed_task_handle *handle, uint32_t submit_thread_id)
{
    if (handle == nullptr || !handle->io_bound || !is_valid_submit_thread_id(submit_thread_id))
    {
        return -1;
    }

    dpkernel_task *task = get_or_create_thread_task(handle, submit_thread_id);
    if (task == nullptr)
    {
        return -1;
    }
    if (handle->prepared_by_thread[submit_thread_id])
    {
        return 0;
    }

    apply_bound_io_region_to_task(task, (shm_ptr)handle->bound_io_base_addr, handle->bound_io_bytes);
    auto io_req = dpm_alloc_mem_buf_async_on_thread(handle->bound_io_bytes, handle->bound_io_bytes, task, submit_thread_id);
    if (io_req == nullptr || !dpm_wait_for_mem_req_completion(io_req))
    {
        return -1;
    }

    // Hint kernel-side ALLOC_TASK handler which DPM thread owns this preallocated task object.
    if (handle->stage_type == DPK_EMBED_STAGE_AES_GCM)
    {
        task->bf3_aes_gcm.callback_owner_thread_id = (uint8_t)submit_thread_id;
    }
    else
    {
        task->bf3.callback_owner_thread_id = (uint8_t)submit_thread_id;
    }

    auto task_req = dpm_alloc_task_async_on_thread(task, submit_thread_id);
    if (task_req == nullptr || !dpm_wait_for_mem_req_completion(task_req))
    {
        auto rollback_req = dpm_free_mem_buf_async_on_thread(task, submit_thread_id);
        if (rollback_req != nullptr)
        {
            (void)dpm_wait_for_mem_req_completion(rollback_req);
        }
        return -1;
    }

    // Important: app_alloc_task_request initializes task->base.completion to DPK_ONGOING.
    // That state means "not completed yet", which is correct for a newly allocated task object
    // but wrong for an idle preprepared task that has never been submitted.
    //
    // Our reuse guard in dpk_embed_task_submit_and_wait_internal checks for DPK_ONGOING to detect
    // true in-flight reuse. Without resetting this here, the first real submit on each preprepared
    // task is falsely rejected as "[DPK-EMBED-TASK-REUSE-BUSY]".
    task->base.completion.store(DPK_SUCCESS, std::memory_order_release);

    handle->prepared_by_thread[submit_thread_id] = true;
    return 0;
}

int dpk_embed_task_release_for_thread(dpk_embed_task_handle *handle, uint32_t submit_thread_id)
{
    return release_thread_resources_internal(handle, submit_thread_id);
}

int dpk_embed_task_set_aes_key_shm(dpk_embed_task_handle *handle, uint64_t key_shm, uint8_t key_size_bytes)
{
    if (handle == nullptr || handle->stage_type != DPK_EMBED_STAGE_AES_GCM)
    {
        return -1;
    }
    if (key_size_bytes != 16 && key_size_bytes != 32)
    {
        return -1;
    }

    for (uint32_t thread_id = 0; thread_id < (uint32_t)N_DPM_THREADS; thread_id++)
    {
        dpkernel_task *task = handle->tasks_by_thread[thread_id];
        if (task == nullptr)
        {
            continue;
        }
        task->bf3_aes_gcm.key_shm = (shm_ptr)key_shm;
        task->bf3_aes_gcm.key_size_bytes = key_size_bytes;
    }
    return 0;
}

//
// Common submit helper shared by sync and async APIs.
// It validates task/thread state, binds in/out pointers for this submit, and enqueues to DPM.
//
static inline int dpk_embed_task_submit_internal(dpk_embed_task_handle *handle, uint32_t submit_thread_id,
                                                 uint64_t input_addr, uint32_t input_size, uint64_t output_addr,
                                                 uint32_t output_size, dpm_device submit_device,
                                                 dpkernel_task **submitted_task)
{
    if (handle == nullptr || !handle->io_bound || !is_valid_submit_thread_id(submit_thread_id) || input_size == 0 ||
        output_size == 0)
    {
        return -1;
    }
    if (!handle->prepared_by_thread[submit_thread_id])
    {
        return -1;
    }
    dpkernel_task *task = handle->tasks_by_thread[submit_thread_id];
    if (task == nullptr)
    {
        return -1;
    }

    // Defensive overlap guard:
    // A preallocated task object must not be resubmitted before its prior completion
    // has transitioned away from DPK_ONGOING. If this happens, HW submit may return
    // DOCA_ERROR_BAD_STATE because the same task object is still in-flight.
    dpkernel_error prior_completion = app_check_task_completion(task);
    if (prior_completion == DPK_ONGOING)
    {
        printf("[DPK-EMBED-TASK-REUSE-BUSY] stage=%u submit_thread=%u task=%p "
               "in=%u out=%u device=%d completion=%d\n",
               (unsigned)handle->stage_type, (unsigned)submit_thread_id, (void *)task, (unsigned)input_size,
               (unsigned)output_size, (int)submit_device, (int)prior_completion);
        return -1;
    }

    shm_ptr in_shm = 0;
    shm_ptr out_shm = 0;
    if (!convert_abs_addr_to_shm(input_addr, input_size, &in_shm) ||
        !convert_abs_addr_to_shm(output_addr, output_size, &out_shm))
    {
        return -1;
    }

    task->base.in = in_shm;
    task->base.out = out_shm;
    task->base.in_size = input_size;
    task->base.out_size = output_size;
    task->base.actual_out_size = 0;

    if (handle->stage_type == DPK_EMBED_STAGE_AES_GCM)
    {
        task->bf3_aes_gcm.callback_owner_thread_id = (uint8_t)submit_thread_id;
    }
    else
    {
        task->bf3.callback_owner_thread_id = (uint8_t)submit_thread_id;
    }

    dpm_submit_task_msgq_blocking((int)submit_thread_id, task, submit_device);
    if (submitted_task != nullptr)
    {
        *submitted_task = task;
    }
    return 0;
}

int dpk_embed_task_submit_async(dpk_embed_task_handle *handle, uint32_t submit_thread_id, uint64_t input_addr,
                                uint32_t input_size, uint64_t output_addr, uint32_t output_size)
{
    return dpk_embed_task_submit_internal(handle, submit_thread_id, input_addr, input_size, output_addr, output_size,
                                          DEVICE_NONE, nullptr);
}

int dpk_embed_task_check_completion(dpk_embed_task_handle *handle, uint32_t submit_thread_id, uint32_t *actual_out_size,
                                    uint8_t *completion_state)
{
    if (completion_state == nullptr || handle == nullptr || !is_valid_submit_thread_id(submit_thread_id))
    {
        return -1;
    }
    if (actual_out_size != nullptr)
    {
        *actual_out_size = 0;
    }
    if (!handle->prepared_by_thread[submit_thread_id])
    {
        return -1;
    }

    dpkernel_task *task = handle->tasks_by_thread[submit_thread_id];
    if (task == nullptr)
    {
        return -1;
    }

    dpkernel_error status = app_check_task_completion(task);
    if (status == DPK_ONGOING)
    {
        *completion_state = DPK_EMBED_TASK_COMPLETION_PENDING;
        return 0;
    }
    if (status == DPK_SUCCESS)
    {
        *completion_state = DPK_EMBED_TASK_COMPLETION_SUCCESS;
        if (actual_out_size != nullptr)
        {
            *actual_out_size = task->base.actual_out_size;
        }
        return 0;
    }

    *completion_state = DPK_EMBED_TASK_COMPLETION_ERROR;
    return 0;
}

static inline int dpk_embed_task_submit_and_wait_internal(dpk_embed_task_handle *handle, uint32_t submit_thread_id,
                                                          uint64_t input_addr, uint32_t input_size,
                                                          uint64_t output_addr, uint32_t output_size,
                                                          dpm_device submit_device, uint32_t timeout_ms,
                                                          uint32_t *actual_out_size)
{
    dpkernel_task *task = nullptr;
    if (dpk_embed_task_submit_internal(handle, submit_thread_id, input_addr, input_size, output_addr, output_size,
                                       submit_device, &task) != 0 ||
        task == nullptr)
    {
        return -1;
    }

    uint32_t wait_timeout_ms = (timeout_ms == 0) ? 30000 : timeout_ms;
    auto wait_start = std::chrono::steady_clock::now();
    if (!wait_task_completion(task, wait_timeout_ms))
    {
        dpkernel_error final_completion = app_check_task_completion(task);
        auto elapsed_ms =
            (uint32_t)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wait_start)
                .count();
        const char *reason = "unknown";
        if (final_completion == DPK_ERROR_FAILED)
        {
            reason = "kernel-failed";
        }
        else if (final_completion == DPK_ERROR_AGAIN)
        {
            reason = "again";
        }
        else if (final_completion == DPK_ONGOING)
        {
            reason = (elapsed_ms >= wait_timeout_ms) ? "timeout" : "still-ongoing";
        }
        // Note: not all non-success waits are terminal failures for the caller.
        // Example: DDS stage-2 may apply software fallback after a DEVICE_NONE/HW failure.
        printf("[DPK-EMBED-TASK-WAIT-NOT-SUCCESS] stage=%u submit_thread=%u task=%p device=%d "
               "in=%u out=%u timeout_limit_ms=%u elapsed_ms=%u completion=%d reason=%s\n",
               (unsigned)handle->stage_type, (unsigned)submit_thread_id, (void *)task, (int)submit_device,
               (unsigned)input_size, (unsigned)output_size, (unsigned)wait_timeout_ms, (unsigned)elapsed_ms,
               (int)final_completion, reason);
        return -1;
    }

    if (actual_out_size != nullptr)
    {
        *actual_out_size = task->base.actual_out_size;
    }
    return 0;
}

int dpk_embed_task_submit_and_wait(dpk_embed_task_handle *handle, uint32_t submit_thread_id, uint64_t input_addr,
                                   uint32_t input_size, uint64_t output_addr, uint32_t output_size,
                                   uint32_t timeout_ms, uint32_t *actual_out_size)
{
    return dpk_embed_task_submit_and_wait_internal(handle, submit_thread_id, input_addr, input_size, output_addr,
                                                   output_size, DEVICE_NONE, timeout_ms, actual_out_size);
}

int dpk_embed_set_global_aes_key(const void *key_bytes, uint8_t key_size_bytes)
{
    if (key_bytes == nullptr)
    {
        return -1;
    }
    if (key_size_bytes != 16 && key_size_bytes != 32)
    {
        return -1;
    }

    auto *key_copy = (uint8_t *)app_alloc_req_ctx_buffer(key_size_bytes);
    if (key_copy == nullptr)
    {
        return -1;
    }
    std::memcpy(key_copy, key_bytes, key_size_bytes);
    uint64_t key_shm = (uint64_t)app_get_shm_ptr_for_req_ctx_ptr(key_copy);

    std::lock_guard<std::mutex> lock(g_key_lock);
    if (g_global_key_ptr != nullptr)
    {
        app_free_req_ctx_buffer(g_global_key_ptr);
    }
    g_global_key_ptr = (void *)key_copy;
    g_global_key_shm = key_shm;
    g_global_key_size = key_size_bytes;
    return 0;
}

int dpk_embed_get_global_aes_key(uint64_t *key_shm, uint8_t *key_size_bytes)
{
    if (key_shm == nullptr || key_size_bytes == nullptr)
    {
        return -1;
    }

    std::lock_guard<std::mutex> lock(g_key_lock);
    if (g_global_key_shm == 0 || g_global_key_size == 0)
    {
        return -1;
    }
    *key_shm = g_global_key_shm;
    *key_size_bytes = g_global_key_size;
    return 0;
}

int dpk_embed_copy_global_aes_key(uint8_t *key_bytes, uint8_t key_capacity_bytes, uint8_t *key_size_bytes)
{
    if (key_bytes == nullptr || key_size_bytes == nullptr)
    {
        return -1;
    }

    std::lock_guard<std::mutex> lock(g_key_lock);
    if (g_global_key_ptr == nullptr || g_global_key_size == 0 || g_global_key_shm == 0)
    {
        return -1;
    }
    if (key_capacity_bytes < g_global_key_size)
    {
        return -1;
    }
    std::memcpy(key_bytes, g_global_key_ptr, g_global_key_size);
    *key_size_bytes = g_global_key_size;
    return 0;
}
