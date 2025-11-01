#pragma once
#include "kernel_interface.hpp"
#include <sys/types.h>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_compress.h>
#include <doca_ctx.h>
#include <doca_error.h>
#include <doca_log.h>

#include "bf2_device.hpp"

// 4 CPUs
#define BF2_DECOMPRESS_DEFLATE_CPU_MASK 0x0F

struct dpm_doca_decompress_deflate_bf2
{
    struct dpm_doca_state *global_doca_state;
    // struct doca_workq *workq;
    struct doca_workq **workqs;
    // struct doca_ctx *ctx;
    struct doca_ctx **ctxs;
    // struct doca_compress *compress;
    struct doca_compress **compresses;
};

bool bf2_decompress_deflate_kernel_init(struct dpm_kernel_registration_result *result);

bool bf2_decompress_deflate_handle_mem_req(struct dpm_mem_req *req);

dpkernel_error bf2_decompress_deflate_kernel_execute(dpkernel_task_base *task, int thread_id);

dpkernel_error bf2_decompress_deflate_kernel_poll(dpkernel_task_base *task, int thread_id);

bool bf2_decompress_deflate_kernel_cleanup();

// uint64_t bf2_decompress_deflate_kernel_get_estimated_completion_time();
