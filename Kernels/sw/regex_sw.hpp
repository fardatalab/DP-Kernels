#pragma once
#include "kernel_interface.hpp"
#include <cstddef>
#include <re2/re2.h>
#include <sys/types.h>

// #include "bf3v2.2_device.hpp"

#define REGEX_PATTERN "(.*?regular.*?)"

struct regex_match_details
{
    size_t length;
    size_t offset;
};

bool sw_regex_deflate_get_catalogue(dpm_kernel_catalogue *catalogue);

bool sw_regex_deflate_kernel_init(struct dpm_kernel_catalogue *result, int n_threads);

// no need to handle mem reqs
// bool sw_regex_deflate_handle_mem_req(struct dpm_mem_req *req);

// no need to report back remaining capacity for any sw kernels
// uint32_t sw_regex_deflate_hw_kernel_remaining_capacity(int thread_id);

dpkernel_error sw_regex_deflate_kernel_execute(dpkernel_task *task, int thread_id);

// don't need to poll completion as this is synchronous -- just complete the task after successful execution
// dpkernel_error sw_regex_deflate_kernel_poll(dpkernel_task *task, int thread_id);

bool sw_regex_deflate_kernel_cleanup();