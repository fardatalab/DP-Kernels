#pragma once
#include "kernel_interface.hpp"
#include <sys/types.h>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_compress.h>
#include <doca_ctx.h>
#include <doca_error.h>
#include <doca_log.h>

#include <doca_regex.h>

#include "bf3v2.2_device.hpp"

#define BF3v2_2_REGEX_RULES_FILE "/home/ubuntu/bf3regex/regex_rule.rof2.binary"

// 1 CPU
#define BF3v2_2_REGEX_CPU_MASK 0x01
#define BF3v2_2_REGEX_WORKQ_DEPTH 512

/// number of matches that can be possibly matched by the regex engine
#define BF3V2_2_REGEX_MATCHES_POOL_SIZE 1

struct dpm_doca_regex_bf3v2_2
{
    struct dpm_doca_state *global_doca_state;
    struct doca_workq **workqs;
    struct doca_ctx **ctxs;
    struct doca_regex **regexes;
};

bool bf3v2_2_regex_kernel_init(struct dpm_kernel_catalogue *result, int n_threads);

bool bf3v2_2_regex_kernel_cleanup();

bool bf3v2_2_regex_handle_mem_req(struct dpm_mem_req *req);

uint32_t bf3v2_2_regex_hw_kernel_remaining_capacity(int thread_id);

dpkernel_error bf3v2_2_regex_kernel_execute(dpkernel_task *task, int thread_id);

/// @note currently it only returns the number of matches, which is stored as a uint32_t in the task->out
dpkernel_error bf3v2_2_regex_kernel_poll(dpkernel_task *task, int thread_id);
