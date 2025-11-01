#pragma once
#include "kernel_interface.hpp"
#include <sys/types.h>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_compress.h>
#include <doca_ctx.h>
#include <doca_error.h>
#include <doca_log.h>

#include "bf3v2.2_device.hpp"

// 1 CPUs
#define BF3v2_2_DECOMPRESS_DEFLATE_CPU_MASK 0x01

#define BF3v2_2_DECOMPRESS_DEFLATE_WORKQ_DEPTH 128
// #define N_CONSECUTIVE_POLLS 256

struct dpm_doca_decompress_deflate_bf3v2_2
{
    struct dpm_doca_state *global_doca_state;
    struct doca_workq **workqs;
    struct doca_ctx **ctxs;
    struct doca_compress **compresses;
};

bool bf3v2_2_decompress_deflate_kernel_init(struct dpm_kernel_catalogue *result, int n_threads);

bool bf3v2_2_decompress_deflate_handle_mem_req(struct dpm_mem_req *req);

uint32_t bf3v2_2_decompress_deflate_hw_kernel_remaining_capacity(int thread_id);

dpkernel_error bf3v2_2_decompress_deflate_kernel_execute(dpkernel_task *task, int thread_id);

dpkernel_error bf3v2_2_decompress_deflate_kernel_poll(dpkernel_task *task, int thread_id);

bool bf3v2_2_decompress_deflate_kernel_cleanup();

// uint64_t bf3v2_2_decompress_deflate_kernel_get_estimated_completion_time();
