#pragma once
#include "common.hpp"
#include "doca_buf.h"
#include "doca_ctx.h"
#include "doca_error.h"
#include "doca_pe.h"
#include "executor.hpp"
#include <cstdint>
#include <cstdio>
#include <sys/types.h>

#include "bf3_device.hpp"

struct dpm_doca_decompress_deflate_bf3
{
    struct dpm_doca_state *global_doca_state;
    struct doca_ctx *ctx;
    struct doca_compress *compress;
};

bool bf3_decompress_deflate_kernel_init();

bool bf3_decompress_deflate_kernel_cleanup();

dpkernel_error bf3_decompress_deflate_kernel_execute(dpkernel_task_base *task);

dpkernel_error bf3_decompress_deflate_kernel_poll(dpkernel_task_base *task);

uint64_t bf3_decompress_deflate_kernel_get_estimated_completion_time();