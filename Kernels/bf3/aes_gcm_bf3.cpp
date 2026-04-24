#include "aes_gcm_bf3.hpp"
#include "bounded_queue.hpp"
#include "common.hpp"
#include "doca_buf.h"
#include "doca_buf_inventory.h"
#include "doca_dev.h"
#include "doca_error.h"
#include "memory.hpp"
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>

static struct dpm_doca_aes_gcm_bf3 bf3_aes_gcm_state;

static int n_cpus = 0;
static uint32_t *kernel_queue_remaining_capacity;
// Updated prototype requirement: use 16-byte (128-bit) authentication tag.
static uint32_t bf3_aes_gcm_tag_size_bytes = 16;
static uint64_t bf3_aes_gcm_max_buf_size = 16 * 1024;

// req_ctx-resident key bytes are used for key creation/caching, but buffers still come from DOCA input/output mmaps.
static pthread_spinlock_t bf3_aes_gcm_buf_inventory_lock;

// Completion queues per DPM thread (consumed by bf3_aes_gcm_kernel_poll).
static constexpr size_t BF3_AES_GCM_COMPLETION_QUEUE_CAPACITY = DPM_HW_KERNEL_QUEUE_SIZE + BF3_AES_GCM_WORKQ_DEPTH;
static std::array<BoundedQueue<dpkernel_task *, BF3_AES_GCM_COMPLETION_QUEUE_CAPACITY>, N_DPM_THREADS>
    bf3_aes_gcm_completed_tasks;

// Decrypt task alloc-init still needs an IV pointer, but execute() overrides IV per submission.
static constexpr uint8_t BF3_AES_GCM_FIXED_IV[12] = {0};
static constexpr uint32_t BF3_AES_GCM_FIXED_IV_LEN = 12;
static constexpr uint32_t BF3_AES_GCM_FIXED_AAD_SIZE = 0;
// DDS read2 decrypt contract: stage1 input is [ciphertext||tag], and nonce/IV is the
// 12 bytes immediately preceding this input window in stage0 data.
static constexpr uint32_t BF3_AES_GCM_NONCE_SIZE_BYTES = 12;

struct bf3_thread_key_cache_entry
{
    bool has_key = false;
    struct doca_aes_gcm_key *doca_key = nullptr;
    shm_ptr key_shm = (shm_ptr)0;
};

static bf3_thread_key_cache_entry *bf3_key_caches;

static long _bf3_aes_gcm_estimated_processing_time(uint32_t input_size, int thread_id)
{
    (void)thread_id;
    return (long)input_size;
}

static inline void _bf3_aes_gcm_enqueue_completed_task_for_polling(dpkernel_task *task)
{
    const int owner_thread_id = (int)task->bf3_aes_gcm.callback_owner_thread_id;

    if (owner_thread_id < 0 || owner_thread_id >= N_DPM_THREADS)
    {
        printf("bf3 aes_gcm callback missing owner thread for task=%p\n", task);
        return;
    }

    if (!bf3_aes_gcm_completed_tasks[owner_thread_id].push(task))
    {
        printf("bf3 aes_gcm completion queue overflow on thread_id=%d for task=%p\n", owner_thread_id, task);
    }
}

static bool _bf3_aes_gcm_is_key_size_valid(uint8_t key_size_bytes)
{
    // Raw key bytes for DOCA AES-GCM key creation.
    return key_size_bytes == 16 || key_size_bytes == 32;
}

//
// Rearm a preallocated AES-GCM decrypt task after failure.
//
// Rationale:
// We intentionally reuse task objects for performance, but repeated DOCA failures can leave
// a specific task handle in a BAD_STATE-for-submit condition. Rebuilding only this task object
// on the failure path keeps the hot path unchanged while restoring forward progress.
//
static bool _bf3_aes_gcm_rearm_prealloc_task(dpkernel_task *task, int thread_id, const char *reason)
{
    if (task == nullptr || thread_id < 0 || thread_id >= n_cpus || thread_id >= N_DPM_THREADS)
    {
        printf("[BF3-AES-REARM] status=FAIL reason=invalid-args task=%p thread_id=%d trigger=%s\n", task, thread_id,
               (reason != nullptr) ? reason : "n/a");
        return false;
    }

    dpkernel_task_bf3_aes_gcm *aes_task = &task->bf3_aes_gcm;
    if (aes_task->src_doca_buf == nullptr || aes_task->dst_doca_buf == nullptr)
    {
        printf("[BF3-AES-REARM] status=FAIL reason=missing-bufs task=%p thread_id=%d trigger=%s\n", task, thread_id,
               (reason != nullptr) ? reason : "n/a");
        return false;
    }

    // Keep original pointer for diagnostics.
    struct doca_aes_gcm_task_decrypt *old_task = aes_task->prealloc_decrypt_task;
    if (aes_task->prealloc_decrypt_task != nullptr)
    {
        doca_task_free(doca_aes_gcm_task_decrypt_as_task(aes_task->prealloc_decrypt_task));
        aes_task->prealloc_decrypt_task = nullptr;
        aes_task->prealloc_thread_id = UINT8_MAX;
    }

    // Reuse cached per-thread key if available; otherwise create a placeholder key.
    bf3_thread_key_cache_entry *thread_key_cache = &bf3_key_caches[thread_id];
    if (!thread_key_cache->has_key || thread_key_cache->doca_key == nullptr)
    {
        static const uint8_t kPlaceholderKey[16] = {0};
        struct doca_aes_gcm_key *bootstrap_key = nullptr;
        doca_error_t bootstrap_ret =
            doca_aes_gcm_key_create(bf3_aes_gcm_state.aes_gcms[thread_id], kPlaceholderKey, DOCA_AES_GCM_KEY_128,
                                    &bootstrap_key);
        if (bootstrap_ret != DOCA_SUCCESS)
        {
            printf("[BF3-AES-REARM] status=FAIL reason=placeholder-key-create thread=%d trigger=%s err=%s\n", thread_id,
                   (reason != nullptr) ? reason : "n/a", doca_error_get_descr(bootstrap_ret));
            return false;
        }
        thread_key_cache->has_key = true;
        thread_key_cache->doca_key = bootstrap_key;
        thread_key_cache->key_shm = (shm_ptr)SIZE_MAX;
    }

    union doca_data task_user_data = {0};
    task_user_data.ptr = task;
    struct doca_aes_gcm_task_decrypt *new_task = nullptr;
    doca_error_t ret =
        doca_aes_gcm_task_decrypt_alloc_init(bf3_aes_gcm_state.aes_gcms[thread_id], aes_task->src_doca_buf,
                                             aes_task->dst_doca_buf, thread_key_cache->doca_key,
                                             BF3_AES_GCM_FIXED_IV, BF3_AES_GCM_FIXED_IV_LEN,
                                             bf3_aes_gcm_tag_size_bytes, BF3_AES_GCM_FIXED_AAD_SIZE, task_user_data,
                                             &new_task);
    if (ret != DOCA_SUCCESS)
    {
        printf("[BF3-AES-REARM] status=FAIL reason=alloc-init thread=%d old_task=%p trigger=%s err=%s\n", thread_id, old_task,
               (reason != nullptr) ? reason : "n/a", doca_error_get_descr(ret));
        return false;
    }

    aes_task->prealloc_decrypt_task = new_task;
    aes_task->prealloc_thread_id = (uint8_t)thread_id;
    aes_task->callback_status = DPK_SUCCESS;

    printf("[BF3-AES-REARM] status=PASS thread=%d old_task=%p new_task=%p trigger=%s\n", thread_id, old_task,
           (void *)new_task, (reason != nullptr) ? reason : "n/a");
    return true;
}

static doca_error_t _bf3_aes_gcm_destroy_thread_key_cache(int thread_id)
{
    if (bf3_key_caches == nullptr || !bf3_key_caches[thread_id].has_key || bf3_key_caches[thread_id].doca_key == nullptr)
    {
        return DOCA_SUCCESS;
    }

    doca_error_t ret = doca_aes_gcm_key_destroy(bf3_key_caches[thread_id].doca_key);
    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to destroy cached AES-GCM key on thread %d: %s\n", thread_id, doca_error_get_descr(ret));
        return ret;
    }

    bf3_key_caches[thread_id].has_key = false;
    bf3_key_caches[thread_id].doca_key = nullptr;
    bf3_key_caches[thread_id].key_shm = (shm_ptr)0;
    return DOCA_SUCCESS;
}

void bf3_aes_gcm_decrypt_completed_callback(struct doca_aes_gcm_task_decrypt *decrypt_task,
                                            union doca_data task_user_data, union doca_data ctx_user_data)
{
    (void)ctx_user_data;
    dpkernel_task *task = (dpkernel_task *)task_user_data.ptr;
    dpkernel_task_base *base_task = &task->base;
    dpkernel_task_bf3_aes_gcm *aes_task = &task->bf3_aes_gcm;

    // Original completion sizing path kept for reference:
    // Preserve actual output size from destination DOCA buffer for app-side consumers.
    // if (aes_task->dst_doca_buf != nullptr)
    // {
    //     size_t actual_len = 0;
    //     if (doca_buf_get_data_len(aes_task->dst_doca_buf, &actual_len) == DOCA_SUCCESS)
    //     {
    //         base_task->actual_out_size = (uint32_t)actual_len;
    //     }
    // }

    // For this decrypt-only prototype, output size is deterministic: ciphertext bytes minus fixed GCM tag bytes.
    // Using this avoids ambiguous DOCA buffer data-len behavior when buffers are lazily rebound and reused.
    if (base_task->in_size >= bf3_aes_gcm_tag_size_bytes)
    {
        base_task->actual_out_size = base_task->in_size - bf3_aes_gcm_tag_size_bytes;
    }
    else
    {
        printf("bf3 aes_gcm callback got in_size(%u) < tag_size(%u), forcing output size to 0 for task=%p\n",
               base_task->in_size, bf3_aes_gcm_tag_size_bytes, task);
        base_task->actual_out_size = 0;
    }

    // Keep a diagnostic read of DOCA data len to surface any divergence from expected AES-GCM semantics.
    // NOTE: this should be good now, just need to reset the buffer on reuse
    if (aes_task->dst_doca_buf != nullptr)
    {
        size_t doca_actual_len = 0;
        if (doca_buf_get_data_len(aes_task->dst_doca_buf, &doca_actual_len) == DOCA_SUCCESS &&
            doca_actual_len != (size_t)base_task->actual_out_size)
        {
            printf("bf3 aes_gcm callback size mismatch: doca_len=%lu expected=%u task=%p\n",
                   (unsigned long)doca_actual_len, base_task->actual_out_size, task);
        }
    }

    // Keep callback->poll status local to the kernel; DPM sets base.completion in dpm_poll_completion().
    // base_task->completion.store(DPK_SUCCESS, std::memory_order_release);
    aes_task->callback_status = DPK_SUCCESS;
    _bf3_aes_gcm_enqueue_completed_task_for_polling(task);
    // Preallocated decrypt task is reused across submissions; do not free on completion.
    (void)decrypt_task;
}

void bf3_aes_gcm_decrypt_error_callback(struct doca_aes_gcm_task_decrypt *decrypt_task, union doca_data task_user_data,
                                        union doca_data ctx_user_data)
{
    (void)ctx_user_data;
    dpkernel_task *task = (dpkernel_task *)task_user_data.ptr;
    // dpkernel_task_base *base_task = &task->base;
    struct doca_task *doca_task = doca_aes_gcm_task_decrypt_as_task(decrypt_task);

    // Keep callback->poll status local to the kernel; DPM sets base.completion in dpm_poll_completion().
    // base_task->completion.store(DPK_ERROR_FAILED, std::memory_order_release);
    task->bf3_aes_gcm.callback_status = DPK_ERROR_FAILED;
    _bf3_aes_gcm_enqueue_completed_task_for_polling(task);

    printf("AES-GCM decrypt task failed: %s\n", doca_error_get_descr(doca_task_get_status(doca_task)));
    // Preallocated decrypt task is reused across submissions; do not free on error callback.
}

bool bf3_aes_gcm_handle_mem_req(struct dpm_mem_req *req)
{
    doca_error_t ret;
    dpkernel_task_bf3_aes_gcm *task = &((dpkernel_task *)dpm_get_task_ptr_from_shmptr(req->task))->bf3_aes_gcm;

    switch (req->type)
    {
    case DPM_MEM_REQ_ALLOC_INPUT:
    {
        char *input_buffer = dpm_get_input_ptr_from_shmptr(task->in);
        pthread_spin_lock(&bf3_aes_gcm_buf_inventory_lock);
        ret = doca_buf_inventory_buf_get_by_addr(bf3_aes_gcm_state.global_doca_state->state.buf_inv,
                                                 bf3_aes_gcm_state.global_doca_state->state.src_mmap, input_buffer,
                                                 task->in_size, &task->src_doca_buf);
        pthread_spin_unlock(&bf3_aes_gcm_buf_inventory_lock);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 aes_gcm failed to get input buf from inventory: %s\n", doca_error_get_descr(ret));
            return false;
        }
        ret = doca_buf_set_data(task->src_doca_buf, input_buffer, task->in_size);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 aes_gcm failed to set input buf data: %s\n", doca_error_get_descr(ret));
            return false;
        }
        return true;
    }
    case DPM_MEM_REQ_ALLOC_OUTPUT:
    {
        pthread_spin_lock(&bf3_aes_gcm_buf_inventory_lock);
        ret = doca_buf_inventory_buf_get_by_addr(bf3_aes_gcm_state.global_doca_state->state.buf_inv,
                                                 bf3_aes_gcm_state.global_doca_state->state.dst_mmap,
                                                 dpm_get_output_ptr_from_shmptr(task->out), task->out_size,
                                                 &task->dst_doca_buf);
        pthread_spin_unlock(&bf3_aes_gcm_buf_inventory_lock);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 aes_gcm failed to get output buf from inventory: %s\n", doca_error_get_descr(ret));
            return false;
        }
        return true;
    }
    case DPM_MEM_REQ_FREE_INPUT:
    {
        if (task->src_doca_buf == nullptr)
        {
            return true;
        }
        ret = doca_buf_dec_refcount(task->src_doca_buf, nullptr);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 aes_gcm failed to free input buf: %s\n", doca_error_get_descr(ret));
            return false;
        }
        task->src_doca_buf = nullptr;
        return true;
    }
    case DPM_MEM_REQ_FREE_OUTPUT:
    {
        if (task->dst_doca_buf == nullptr)
        {
            return true;
        }
        ret = doca_buf_dec_refcount(task->dst_doca_buf, nullptr);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 aes_gcm failed to free output buf: %s\n", doca_error_get_descr(ret));
            return false;
        }
        task->dst_doca_buf = nullptr;
        return true;
    }
    case DPM_MEM_REQ_ALLOC_TASK:
    {
        if (task->prealloc_decrypt_task != nullptr)
        {
            return true;
        }
        if (task->src_doca_buf == nullptr || task->dst_doca_buf == nullptr)
        {
            printf("bf3 aes_gcm alloc_task requires src/dst doca_buf to be bound first\n");
            return false;
        }

        int thread_id = (int)task->callback_owner_thread_id;
        if (thread_id < 0 || thread_id >= n_cpus || thread_id >= N_DPM_THREADS)
        {
            printf("bf3 aes_gcm alloc_task invalid callback_owner_thread_id=%d\n", thread_id);
            return false;
        }

        // ALLOC_TASK may run before real key metadata is configured.
        // Seed a thread-local placeholder key and replace it on first execute when key_shm is set.
        bf3_thread_key_cache_entry *thread_key_cache = &bf3_key_caches[thread_id];
        if (!thread_key_cache->has_key || thread_key_cache->doca_key == nullptr)
        {
            static const uint8_t kPlaceholderKey[16] = {0};
            struct doca_aes_gcm_key *bootstrap_key = nullptr;
            ret = doca_aes_gcm_key_create(bf3_aes_gcm_state.aes_gcms[thread_id], kPlaceholderKey, DOCA_AES_GCM_KEY_128,
                                          &bootstrap_key);
            if (ret != DOCA_SUCCESS)
            {
                printf("bf3 aes_gcm alloc_task failed to create placeholder key: %s\n", doca_error_get_descr(ret));
                return false;
            }
            thread_key_cache->has_key = true;
            thread_key_cache->doca_key = bootstrap_key;
            thread_key_cache->key_shm = (shm_ptr)SIZE_MAX;
        }

        union doca_data task_user_data = {0};
        task_user_data.ptr = (void *)dpm_get_task_ptr_from_shmptr(req->task);
        struct doca_aes_gcm_task_decrypt *decrypt_task = nullptr;
        ret = doca_aes_gcm_task_decrypt_alloc_init(bf3_aes_gcm_state.aes_gcms[thread_id], task->src_doca_buf,
                                                   task->dst_doca_buf, thread_key_cache->doca_key, BF3_AES_GCM_FIXED_IV,
                                                   BF3_AES_GCM_FIXED_IV_LEN, bf3_aes_gcm_tag_size_bytes,
                                                   BF3_AES_GCM_FIXED_AAD_SIZE, task_user_data, &decrypt_task);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 aes_gcm alloc_task failed to alloc init decrypt task: %s\n", doca_error_get_descr(ret));
            return false;
        }
        task->prealloc_decrypt_task = decrypt_task;
        task->prealloc_thread_id = (uint8_t)thread_id;
        return true;
    }
    case DPM_MEM_REQ_FREE_TASK:
    {
        if (task->prealloc_decrypt_task == nullptr)
        {
            return true;
        }
        doca_task_free(doca_aes_gcm_task_decrypt_as_task(task->prealloc_decrypt_task));
        task->prealloc_decrypt_task = nullptr;
        task->prealloc_thread_id = UINT8_MAX;
        return true;
    }
    default:
    {
        printf("bf3 aes_gcm kernel unknown mem req->type: %d\n", req->type);
        return false;
    }
    }
}

uint32_t bf3_aes_gcm_kernel_can_execute_kernels(int thread_id, uint32_t *max_capacity)
{
    if (max_capacity != nullptr)
    {
        *max_capacity = BF3_AES_GCM_WORKQ_DEPTH;
    }

    if (thread_id < 0 || thread_id >= n_cpus)
    {
        return 0;
    }
    return kernel_queue_remaining_capacity[thread_id];
}

bool bf3_aes_gcm_get_catalogue(struct dpm_kernel_catalogue *catalogue)
{
    catalogue->is_coalescing_enabled = false;
    catalogue->max_single_task_size = (bf3_aes_gcm_max_buf_size > UINT32_MAX) ? UINT32_MAX : (uint32_t)bf3_aes_gcm_max_buf_size;
    catalogue->get_estimated_processing_time_ns = _bf3_aes_gcm_estimated_processing_time;
    catalogue->kernel_capacity = BF3_AES_GCM_WORKQ_DEPTH;
    return true;
}

bool bf3_aes_gcm_kernel_init(struct dpm_kernel_catalogue *result, int n_threads)
{
    bf3_aes_gcm_state.global_doca_state = &dpm_doca_state;
    n_cpus = (n_threads > 0) ? n_threads : 1;
    pthread_spin_init(&bf3_aes_gcm_buf_inventory_lock, PTHREAD_PROCESS_PRIVATE);

    kernel_queue_remaining_capacity = (uint32_t *)calloc(n_cpus, sizeof(uint32_t));
    bf3_aes_gcm_state.aes_gcms = (struct doca_aes_gcm **)calloc(n_cpus, sizeof(struct doca_aes_gcm *));
    bf3_aes_gcm_state.ctxs = (struct doca_ctx **)calloc(n_cpus, sizeof(struct doca_ctx *));
    bf3_aes_gcm_state.pes = (struct doca_pe **)calloc(n_cpus, sizeof(struct doca_pe *));
    bf3_key_caches = new bf3_thread_key_cache_entry[n_cpus];
    if (kernel_queue_remaining_capacity == nullptr || bf3_aes_gcm_state.aes_gcms == nullptr ||
        bf3_aes_gcm_state.ctxs == nullptr || bf3_aes_gcm_state.pes == nullptr || bf3_key_caches == nullptr)
    {
        printf("Unable to allocate memory for bf3 aes_gcm state\n");
        return false;
    }

    struct doca_devinfo *devinfo = doca_dev_as_devinfo(bf3_aes_gcm_state.global_doca_state->state.dev);
    doca_error_t ret = doca_aes_gcm_cap_task_decrypt_is_supported(devinfo);
    if (ret != DOCA_SUCCESS)
    {
        printf("Device does not support AES-GCM decrypt tasks: %s\n", doca_error_get_descr(ret));
        return false;
    }

    uint32_t max_iv_len = 0;
    ret = doca_aes_gcm_cap_task_decrypt_get_max_iv_len(devinfo, &max_iv_len);
    if (ret != DOCA_SUCCESS || max_iv_len < BF3_AES_GCM_FIXED_IV_LEN)
    {
        printf("Device AES-GCM IV capability mismatch (max=%u): %s\n", max_iv_len, doca_error_get_descr(ret));
        return false;
    }

    // Prototype requirement: fixed 16-byte (128-bit) authentication tag.
    // Original capability check preserved for reference:
    // if (doca_aes_gcm_cap_task_decrypt_is_tag_96_supported(devinfo) != DOCA_SUCCESS)
    if (doca_aes_gcm_cap_task_decrypt_is_tag_128_supported(devinfo) != DOCA_SUCCESS)
    {
        printf("Device does not support 16-byte AES-GCM tags for decrypt\n");
        return false;
    }

    ret = doca_aes_gcm_cap_task_decrypt_get_max_buf_size(devinfo, &bf3_aes_gcm_max_buf_size);
    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to query AES-GCM decrypt max buffer size: %s\n", doca_error_get_descr(ret));
        return false;
    }
    printf("[BF3-AES-CAP] max_buf_size=%lu tag_size=%u workq_depth=%u n_threads=%d\n",
           (unsigned long)bf3_aes_gcm_max_buf_size, (unsigned)bf3_aes_gcm_tag_size_bytes,
           (unsigned)BF3_AES_GCM_WORKQ_DEPTH, n_cpus);

    for (int i = 0; i < n_cpus; i++)
    {
        kernel_queue_remaining_capacity[i] = BF3_AES_GCM_WORKQ_DEPTH;

        ret = doca_pe_create(&bf3_aes_gcm_state.pes[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create PE %d: %s\n", i, doca_error_get_descr(ret));
            return false;
        }

        ret = doca_aes_gcm_create(bf3_aes_gcm_state.global_doca_state->state.dev, &bf3_aes_gcm_state.aes_gcms[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create AES-GCM engine %d: %s\n", i, doca_error_get_descr(ret));
            return false;
        }
        uint32_t max_num_tasks = 0;
        ret = doca_aes_gcm_cap_get_max_num_tasks(bf3_aes_gcm_state.aes_gcms[i], &max_num_tasks);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to query AES-GCM max tasks for thread %d: %s\n", i, doca_error_get_descr(ret));
            return false;
        }
        if (BF3_AES_GCM_WORKQ_DEPTH > max_num_tasks)
        {
            printf("Configured AES-GCM workq depth %u exceeds device max %u for thread %d\n",
                   (unsigned)BF3_AES_GCM_WORKQ_DEPTH, (unsigned)max_num_tasks, i);
            return false;
        }

        bf3_aes_gcm_state.ctxs[i] = doca_aes_gcm_as_ctx(bf3_aes_gcm_state.aes_gcms[i]);
        if (bf3_aes_gcm_state.ctxs[i] == nullptr)
        {
            printf("Unable to cast AES-GCM engine %d to context\n", i);
            return false;
        }

        ret = doca_pe_connect_ctx(bf3_aes_gcm_state.pes[i], bf3_aes_gcm_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to connect PE to AES-GCM context %d: %s\n", i, doca_error_get_descr(ret));
            return false;
        }

        ret = doca_aes_gcm_task_decrypt_set_conf(bf3_aes_gcm_state.aes_gcms[i], bf3_aes_gcm_decrypt_completed_callback,
                                                 bf3_aes_gcm_decrypt_error_callback, BF3_AES_GCM_WORKQ_DEPTH);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to set AES-GCM decrypt task conf for thread %d: %s\n", i, doca_error_get_descr(ret));
            return false;
        }

        ret = doca_ctx_start(bf3_aes_gcm_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Failed to start AES-GCM context %d: %s\n", i, doca_error_get_descr(ret));
            return false;
        }
    }

    bf3_aes_gcm_get_catalogue(result);
    return true;
}

bool bf3_aes_gcm_kernel_cleanup()
{
    doca_error_t ret;

    if (bf3_key_caches != nullptr)
    {
        for (int i = 0; i < n_cpus; i++)
        {
            ret = _bf3_aes_gcm_destroy_thread_key_cache(i);
            if (ret != DOCA_SUCCESS)
            {
                return false;
            }
        }
        delete[] bf3_key_caches;
        bf3_key_caches = nullptr;
    }

    for (int i = 0; i < n_cpus; i++)
    {
        if (bf3_aes_gcm_state.ctxs != nullptr && bf3_aes_gcm_state.ctxs[i] != nullptr &&
            bf3_aes_gcm_state.pes != nullptr && bf3_aes_gcm_state.pes[i] != nullptr)
        {
            ret = request_stop_ctx(bf3_aes_gcm_state.pes[i], bf3_aes_gcm_state.ctxs[i]);
            if (ret != DOCA_SUCCESS)
            {
                printf("Unable to stop AES-GCM context %d: %s\n", i, doca_error_get_descr(ret));
                return false;
            }
        }

        if (bf3_aes_gcm_state.aes_gcms != nullptr && bf3_aes_gcm_state.aes_gcms[i] != nullptr)
        {
            ret = doca_aes_gcm_destroy(bf3_aes_gcm_state.aes_gcms[i]);
            if (ret != DOCA_SUCCESS)
            {
                printf("Unable to destroy AES-GCM engine %d: %s\n", i, doca_error_get_descr(ret));
                return false;
            }
            bf3_aes_gcm_state.aes_gcms[i] = nullptr;
        }

        if (bf3_aes_gcm_state.pes != nullptr && bf3_aes_gcm_state.pes[i] != nullptr)
        {
            ret = doca_pe_destroy(bf3_aes_gcm_state.pes[i]);
            if (ret != DOCA_SUCCESS)
            {
                printf("Unable to destroy PE %d: %s\n", i, doca_error_get_descr(ret));
                return false;
            }
            bf3_aes_gcm_state.pes[i] = nullptr;
        }
    }

    free(kernel_queue_remaining_capacity);
    free(bf3_aes_gcm_state.aes_gcms);
    free(bf3_aes_gcm_state.ctxs);
    free(bf3_aes_gcm_state.pes);
    kernel_queue_remaining_capacity = nullptr;
    bf3_aes_gcm_state.aes_gcms = nullptr;
    bf3_aes_gcm_state.ctxs = nullptr;
    bf3_aes_gcm_state.pes = nullptr;

    for (int i = 0; i < N_DPM_THREADS; i++)
    {
        dpkernel_task *dropped_task = nullptr;
        while (bf3_aes_gcm_completed_tasks[i].pop(dropped_task))
        {
            (void)dropped_task;
        }
    }

    pthread_spin_destroy(&bf3_aes_gcm_buf_inventory_lock);
    n_cpus = 0;
    return true;
}

dpkernel_error bf3_aes_gcm_kernel_execute(dpkernel_task *task, int thread_id)
{
    if (thread_id < 0 || thread_id >= n_cpus || thread_id >= N_DPM_THREADS)
    {
        printf("bf3_aes_gcm_kernel_execute invalid thread_id=%d for task=%p\n", thread_id, task);
        return DPK_ERROR_FAILED;
    }

    dpkernel_task_bf3_aes_gcm *dpk_task = &task->bf3_aes_gcm;
    if (dpk_task->src_doca_buf == nullptr || dpk_task->dst_doca_buf == nullptr)
    {
        printf("bf3 aes_gcm execute missing src/dst doca_buf for task=%p\n", task);
        return DPK_ERROR_FAILED;
    }

    char *input_buffer = dpm_get_input_ptr_from_shmptr(dpk_task->in);
    char *output_buffer = dpm_get_output_ptr_from_shmptr(dpk_task->out);
    if (input_buffer == nullptr || output_buffer == nullptr)
    {
        printf("bf3 aes_gcm failed to map in/out shm ptrs in=%lu out=%lu for task=%p\n", (unsigned long)dpk_task->in,
               (unsigned long)dpk_task->out, task);
        return DPK_ERROR_FAILED;
    }

    if (dpk_task->in_size < bf3_aes_gcm_tag_size_bytes)
    {
        printf("bf3 aes_gcm execute input too small in_size=%u tag_size=%u task=%p\n", dpk_task->in_size,
               bf3_aes_gcm_tag_size_bytes, task);
        return DPK_ERROR_FAILED;
    }
    if ((uint64_t)dpk_task->in_size > bf3_aes_gcm_max_buf_size || (uint64_t)dpk_task->out_size > bf3_aes_gcm_max_buf_size)
    {
        printf("[BF3-AES-SIZE-CHECK] status=FAIL in_size=%u out_size=%u max_buf_size=%lu task=%p\n",
               (unsigned)dpk_task->in_size, (unsigned)dpk_task->out_size, (unsigned long)bf3_aes_gcm_max_buf_size, task);
        return DPK_ERROR_FAILED;
    }
    if (dpk_task->in < (shm_ptr)BF3_AES_GCM_NONCE_SIZE_BYTES)
    {
        printf("bf3 aes_gcm execute cannot derive IV: input shm offset=%lu < nonce bytes=%u task=%p\n",
               (unsigned long)dpk_task->in, BF3_AES_GCM_NONCE_SIZE_BYTES, task);
        return DPK_ERROR_FAILED;
    }
    // Layout contract for DDS pread2 decrypt stage:
    // task input starts at [ciphertext||tag], and IV is the 12 bytes immediately before it.
    const uint8_t *iv_ptr = (const uint8_t *)(input_buffer - BF3_AES_GCM_NONCE_SIZE_BYTES);

    if (dpk_task->use_lazy_doca_buf_binding)
    {
        // Fast-path retarget of preallocated buffers to current per-submit slice.
        // Keep descriptor reuse and avoid inventory alloc/free churn on the hot path.
        doca_error_t src_reuse_ret =
            doca_buf_inventory_buf_reuse_by_data(dpk_task->src_doca_buf, input_buffer, dpk_task->in_size);
        if (src_reuse_ret != DOCA_SUCCESS)
        {
            printf("bf3 aes_gcm failed to reuse src buffer: %s\n", doca_error_get_descr(src_reuse_ret));
            return DPK_ERROR_FAILED;
        }

        // For decrypt destination: reset output window to [output_buffer, out_size] with empty data.
        doca_error_t dst_reuse_ret =
            doca_buf_inventory_buf_reuse_by_addr(dpk_task->dst_doca_buf, output_buffer, dpk_task->out_size);
        if (dst_reuse_ret != DOCA_SUCCESS)
        {
            printf("bf3 aes_gcm failed to reuse dst buffer: %s\n", doca_error_get_descr(dst_reuse_ret));
            return DPK_ERROR_FAILED;
        }
    }
    if (!_bf3_aes_gcm_is_key_size_valid(dpk_task->key_size_bytes))
    {
        printf("bf3 aes_gcm execute invalid key size=%u for task=%p\n", dpk_task->key_size_bytes, task);
        return DPK_ERROR_FAILED;
    }
    if (dpk_task->key_shm > dpm_req_ctx_shm.shm_size ||
        dpm_req_ctx_shm.shm_size - dpk_task->key_shm < dpk_task->key_size_bytes)
    {
        printf("bf3 aes_gcm execute key_shm out-of-range key_shm=%lu key_size=%u req_ctx_size=%zu\n", dpk_task->key_shm,
               dpk_task->key_size_bytes, dpm_req_ctx_shm.shm_size);
        return DPK_ERROR_FAILED;
    }

    auto *key_bytes = (const uint8_t *)dpm_get_req_ctx_ptr_from_shmptr(dpk_task->key_shm);

    struct doca_aes_gcm_key *cached_key = nullptr;
    bf3_thread_key_cache_entry *thread_key_cache = &bf3_key_caches[thread_id];
    if (thread_key_cache->has_key && thread_key_cache->key_shm == dpk_task->key_shm)
    {
        // Hot-path fast reuse: cache identity is the req_ctx shm offset (caller guarantees no key mutation in-place).
        cached_key = thread_key_cache->doca_key;
    }
    else
    {
        const enum doca_aes_gcm_key_type key_type =
            (dpk_task->key_size_bytes == 16) ? DOCA_AES_GCM_KEY_128 : DOCA_AES_GCM_KEY_256;
        struct doca_aes_gcm_key *new_key = nullptr;
        doca_error_t key_create_ret =
            doca_aes_gcm_key_create(bf3_aes_gcm_state.aes_gcms[thread_id], key_bytes, key_type, &new_key);
        if (key_create_ret != DOCA_SUCCESS)
        {
            printf("bf3 aes_gcm failed to create key on thread %d: %s\n", thread_id, doca_error_get_descr(key_create_ret));
            return DPK_ERROR_FAILED;
        }

        if (thread_key_cache->has_key && thread_key_cache->doca_key != nullptr)
        {
            doca_error_t key_destroy_ret = doca_aes_gcm_key_destroy(thread_key_cache->doca_key);
            if (key_destroy_ret != DOCA_SUCCESS)
            {
                printf("bf3 aes_gcm failed to rotate cached key on thread %d: %s\n", thread_id,
                       doca_error_get_descr(key_destroy_ret));
                doca_aes_gcm_key_destroy(new_key);
                return DPK_ERROR_FAILED;
            }
        }

        thread_key_cache->has_key = true;
        thread_key_cache->doca_key = new_key;
        thread_key_cache->key_shm = dpk_task->key_shm;
        cached_key = thread_key_cache->doca_key;
    }

    // Reset async callback status before every submission when reusing task objects.
    dpk_task->callback_status = DPK_ONGOING;
    dpk_task->callback_owner_thread_id = (uint8_t)thread_id;

    if (dpk_task->prealloc_decrypt_task == nullptr)
    {
        printf("bf3 aes_gcm execute missing preallocated decrypt task for task=%p\n", task);
        return DPK_ERROR_FAILED;
    }
    if ((int)dpk_task->prealloc_thread_id != thread_id)
    {
        printf("bf3 aes_gcm execute thread mismatch prealloc_thread=%u submit_thread=%d task=%p\n",
               (unsigned)dpk_task->prealloc_thread_id, thread_id, task);
        return DPK_ERROR_FAILED;
    }

    doca_aes_gcm_task_decrypt_set_src(dpk_task->prealloc_decrypt_task, dpk_task->src_doca_buf);
    doca_aes_gcm_task_decrypt_set_dst(dpk_task->prealloc_decrypt_task, dpk_task->dst_doca_buf);
    doca_aes_gcm_task_decrypt_set_key(dpk_task->prealloc_decrypt_task, cached_key);
    // Original fixed-IV path preserved for reference:
    // doca_aes_gcm_task_decrypt_set_iv(dpk_task->prealloc_decrypt_task, BF3_AES_GCM_FIXED_IV, BF3_AES_GCM_FIXED_IV_LEN);
    doca_aes_gcm_task_decrypt_set_iv(dpk_task->prealloc_decrypt_task, iv_ptr, BF3_AES_GCM_NONCE_SIZE_BYTES);
    doca_aes_gcm_task_decrypt_set_tag_size(dpk_task->prealloc_decrypt_task, bf3_aes_gcm_tag_size_bytes);
    doca_aes_gcm_task_decrypt_set_aad_size(dpk_task->prealloc_decrypt_task, BF3_AES_GCM_FIXED_AAD_SIZE);

    doca_error_t ret;
    struct doca_task *submit_task = doca_aes_gcm_task_decrypt_as_task(dpk_task->prealloc_decrypt_task);
    ret = doca_task_submit(submit_task);
    if (ret == DOCA_SUCCESS)
    {
        kernel_queue_remaining_capacity[thread_id] -= 1;
        return DPK_SUCCESS;
    }
    if (ret == DOCA_ERROR_NO_MEMORY)
    {
        return DPK_ERROR_AGAIN;
    }

    printf("bf3 aes_gcm failed to submit decrypt task: %s\n", doca_error_get_descr(ret));
    // Failure-path recovery: if task object got into bad state, rebuild it so future submits can proceed.
    (void)_bf3_aes_gcm_rearm_prealloc_task(task, thread_id, "submit-failed");
    return DPK_ERROR_FAILED;
}

dpkernel_error bf3_aes_gcm_kernel_poll(dpkernel_task **task, int thread_id)
{
    if (thread_id < 0 || thread_id >= n_cpus || thread_id >= N_DPM_THREADS)
    {
        return DPK_ERROR_FAILED;
    }

    while (doca_pe_progress(bf3_aes_gcm_state.pes[thread_id]) == 1)
    {
        // Keep draining completions until there are no more events for this PE.
    }

    if (bf3_aes_gcm_completed_tasks[thread_id].pop(*task))
    {
        kernel_queue_remaining_capacity[thread_id] += 1;
        uint8_t callback_status = (*task)->bf3_aes_gcm.callback_status;
        if (callback_status == DPK_ERROR_FAILED)
        {
            // Error callback already fired for this task. Rearm task object for next submit.
            (void)_bf3_aes_gcm_rearm_prealloc_task(*task, thread_id, "callback-failed");
            return DPK_ERROR_FAILED;
        }
        if (callback_status == DPK_SUCCESS)
        {
            return DPK_SUCCESS;
        }
        printf("bf3 aes_gcm poll got unexpected callback_status=%u for task=%p\n", callback_status, *task);
        return DPK_ERROR_FAILED;
    }
    return DPK_ERROR_AGAIN;
}
