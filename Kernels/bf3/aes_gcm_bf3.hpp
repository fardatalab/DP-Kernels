#pragma once

#include "common.hpp"
#include "doca_aes_gcm.h"
#include "doca_buf.h"
#include "doca_ctx.h"
#include "doca_error.h"
#include "doca_pe.h"
#include "kernel_interface.hpp"
#include <cstdint>

#include "bf3_device.hpp"

// Original prototype depth kept for reference.
// #define BF3_AES_GCM_WORKQ_DEPTH 128
// Raised to cover DDS startup preprepare across all provisioned slots.
#define BF3_AES_GCM_WORKQ_DEPTH 1024

/**
 * BF3 AES-GCM kernel state.
 *
 * One DOCA AES-GCM engine/context/PE tuple is created per DPM thread.
 * This mirrors the callback-driven BF3 decompress kernel architecture.
 */
struct dpm_doca_aes_gcm_bf3
{
    struct dpm_doca_state *global_doca_state;
    struct doca_aes_gcm **aes_gcms;
    struct doca_ctx **ctxs;
    struct doca_pe **pes;
};

bool bf3_aes_gcm_kernel_init(struct dpm_kernel_catalogue *result, int n_threads);

bool bf3_aes_gcm_get_catalogue(struct dpm_kernel_catalogue *catalogue);

bool bf3_aes_gcm_kernel_cleanup();

bool bf3_aes_gcm_handle_mem_req(struct dpm_mem_req *req);

uint32_t bf3_aes_gcm_kernel_can_execute_kernels(int thread_id, uint32_t *max_capacity);

dpkernel_error bf3_aes_gcm_kernel_execute(dpkernel_task *task, int thread_id);

dpkernel_error bf3_aes_gcm_kernel_poll(dpkernel_task **task, int thread_id);
