#pragma once
#include "common.hpp"
#include "doca_buf.h"
#include "doca_ctx.h"
#include "doca_error.h"
#include "doca_pe.h"
#include "kernel_interface.hpp"
#include <cstdint>
#include <cstdio>
#include <sys/types.h>

#include "bf3_device.hpp"

// 4 CPUs
#define BF3_DECOMPRESS_DEFLATE_CPU_MASK 0x01

// Original prototype depth kept for reference.
// #define BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH 128
// Raised to cover DDS startup preprepare across all provisioned slots.
#define BF3_DECOMPRESS_DEFLATE_WORKQ_DEPTH 1024
#define N_CONSECUTIVE_POLLS 256

/* struct dpm_doca_decompress_deflate_bf3
{
    struct dpm_doca_state *global_doca_state;
    struct doca_ctx *ctx;
    struct doca_compress *compress;
}; */

struct dpm_doca_decompress_deflate_bf3
{
    struct dpm_doca_state *global_doca_state;
    struct doca_compress **compresses;
    struct doca_ctx **ctxs;
    struct doca_pe **pes;
};

/// @param result the result of the registration, should fill in the catalogue
bool bf3_decompress_deflate_kernel_init(struct dpm_kernel_catalogue *result, int n_threads);

bool bf3_decompress_deflate_get_catalogue(struct dpm_kernel_catalogue *catalogue);

bool bf3_decompress_deflate_kernel_cleanup();

bool bf3_decompress_deflate_handle_mem_req(struct dpm_mem_req *req);

uint32_t bf3_decompress_deflate_kernel_can_execute_kernels(int thread_id, uint32_t *max_capacity);

dpkernel_error bf3_decompress_deflate_kernel_execute(dpkernel_task *task, int thread_id);

dpkernel_error bf3_decompress_deflate_kernel_poll(dpkernel_task **task, int thread_id);

uint64_t bf3_decompress_deflate_kernel_get_estimated_completion_time();
