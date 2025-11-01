#pragma once
#include "executor.hpp"
#include <sys/types.h>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_compress.h>
#include <doca_ctx.h>
#include <doca_error.h>
#include <doca_log.h>

#include "bf3v2.2_device.hpp"

struct dpm_doca_decompress_deflate_bf3v2_2
{
    struct dpm_doca_state *global_doca_state;
    struct doca_workq *workq;
    struct doca_ctx *ctx;
    struct doca_compress *compress;
};

bool bf3v2_2_decompress_deflate_kernel_init();

dpkernel_error bf3v2_2_decompress_deflate_kernel_execute(dpkernel_task_base *task);

dpkernel_error bf3v2_2_decompress_deflate_kernel_poll(dpkernel_task_base *task);

bool bf3v2_2_decompress_deflate_kernel_cleanup();

uint64_t bf3v2_2_decompress_deflate_kernel_get_estimated_completion_time();
