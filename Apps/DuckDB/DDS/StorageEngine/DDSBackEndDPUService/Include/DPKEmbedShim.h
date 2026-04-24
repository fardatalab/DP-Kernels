#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dpk_embed_task_handle dpk_embed_task_handle;

enum dpk_embed_stage_type
{
    DPK_EMBED_STAGE_AES_GCM = 1,
    DPK_EMBED_STAGE_DECOMPRESS_DEFLATE = 2,
};

// Async completion states for dpk_embed_task_check_completion().
enum dpk_embed_task_completion_state
{
    DPK_EMBED_TASK_COMPLETION_PENDING = 0,
    DPK_EMBED_TASK_COMPLETION_SUCCESS = 1,
    DPK_EMBED_TASK_COMPLETION_ERROR = 2,
};

typedef struct dpk_embed_runtime_config
{
    void *io_base;
    size_t io_bytes;
    int enable_thread_pinning;
    uint32_t startup_timeout_ms;
} dpk_embed_runtime_config;

int dpk_embed_runtime_start(const dpk_embed_runtime_config *config);
int dpk_embed_runtime_stop(void);
int dpk_embed_runtime_is_ready(void);
int dpk_embed_get_max_submit_threads(void);

int dpk_embed_task_create(uint8_t stage_type, dpk_embed_task_handle **out_handle);
int dpk_embed_task_destroy(dpk_embed_task_handle *handle);
int dpk_embed_task_bind_io_region(dpk_embed_task_handle *handle, uint64_t io_base_addr, uint32_t io_bytes);
int dpk_embed_task_prepare_for_thread(dpk_embed_task_handle *handle, uint32_t submit_thread_id);
int dpk_embed_task_release_for_thread(dpk_embed_task_handle *handle, uint32_t submit_thread_id);
int dpk_embed_task_set_aes_key_shm(dpk_embed_task_handle *handle, uint64_t key_shm, uint8_t key_size_bytes);
int dpk_embed_task_submit_async(dpk_embed_task_handle *handle, uint32_t submit_thread_id, uint64_t input_addr,
                                uint32_t input_size, uint64_t output_addr, uint32_t output_size);
int dpk_embed_task_check_completion(dpk_embed_task_handle *handle, uint32_t submit_thread_id, uint32_t *actual_out_size,
                                    uint8_t *completion_state);
int dpk_embed_task_submit_and_wait(dpk_embed_task_handle *handle, uint32_t submit_thread_id, uint64_t input_addr,
                                   uint32_t input_size, uint64_t output_addr, uint32_t output_size,
                                   uint32_t timeout_ms, uint32_t *actual_out_size);

int dpk_embed_set_global_aes_key(const void *key_bytes, uint8_t key_size_bytes);
int dpk_embed_get_global_aes_key(uint64_t *key_shm, uint8_t *key_size_bytes);
int dpk_embed_copy_global_aes_key(uint8_t *key_bytes, uint8_t key_capacity_bytes, uint8_t *key_size_bytes);

#ifdef __cplusplus
}
#endif
