#pragma once
#include "kernel_interface.hpp"
#include "libdeflate.h"
#include <sys/types.h>

// #include "bf3v2.2_device.hpp"

/// get the maximum size of a single task that the kernel can handle, this will be useful for coalescing (up to this
/// size)
// static uint32_t __get_max_single_task_size();

// /// dpm will call this for each invocation, passing in the input size
// static long __get_estimated_processing_time(uint32_t input_size, int thread_id);

// /// can this kernel be coalesced? DPM will make the decision based on this
// static bool __is_coalescing_enabled();

bool sw_decompress_deflate_get_catalogue(dpm_kernel_catalogue *catalogue);

bool sw_decompress_deflate_kernel_init(struct dpm_kernel_catalogue *result, int n_threads);

// no need to handle mem reqs
// bool sw_decompress_deflate_handle_mem_req(struct dpm_mem_req *req);

// no need to report back remaining capacity for any sw kernels
// uint32_t sw_decompress_deflate_hw_kernel_remaining_capacity(int thread_id);

dpkernel_error sw_decompress_deflate_kernel_execute(dpkernel_task *task, int thread_id);

// don't need to poll completion as this is synchronous -- just complete the task after successful execution
// dpkernel_error sw_decompress_deflate_kernel_poll(dpkernel_task *task, int thread_id);

bool sw_decompress_deflate_kernel_cleanup();