#pragma once
#include "kernel_interface.hpp"
#include <cstdint>
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

#define BF3v2_2_DECOMPRESS_DEFLATE_WORKQ_DEPTH 128 // 256 seems to be the largest value
// #define N_CONSECUTIVE_POLLS 256

struct dpm_doca_decompress_deflate_bf3v2_2
{
    struct dpm_doca_state *global_doca_state;
    struct doca_workq **workqs;
    struct doca_ctx **ctxs;
    struct doca_compress **compresses;
};

static uint32_t __get_max_single_task_size();
static long __get_estimated_processing_time(uint32_t input_size, int thread_id);
static bool __is_coalescing_enabled();

bool bf3v2_2_decompress_deflate_get_catalogue(dpm_kernel_catalogue *catalogue);

bool bf3v2_2_decompress_deflate_kernel_init(struct dpm_kernel_catalogue *result, int n_threads);

bool bf3v2_2_decompress_deflate_handle_mem_req(struct dpm_mem_req *req);

uint32_t bf3v2_2_decompress_deflate_hw_kernel_remaining_capacity(int thread_id, uint32_t *max_capacity);

dpkernel_error bf3v2_2_decompress_deflate_kernel_execute(dpkernel_task *task, int thread_id);

dpkernel_error bf3v2_2_decompress_deflate_kernel_poll(dpkernel_task **task, int thread_id);

bool bf3v2_2_decompress_deflate_kernel_cleanup();

// uint64_t bf3v2_2_decompress_deflate_kernel_get_estimated_completion_time();
