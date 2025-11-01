#pragma once
#include "common.hpp"
#include "device_specific.hpp"
#include "kernel_interface.hpp"
#include "ring_buffer.hpp"
#include <atomic>
#include <thread>

using namespace DDS_FrontEnd;

// sw kernels
#include "decompress_deflate_sw.hpp"

struct sw_kernel_thread_args
{
    int thread_id;
    std::atomic<bool> run;
};

/// holds the kernel catalogues for each and all possible tasks/kernels
extern struct dpm_kernel_catalogue sw_kernel_catalogues[TASK_NAME_LAST];

/// the threads for sw kernels, will be N_SW_KERNELS_THREADS long
extern std::thread *sw_kernel_threads;

extern dpm_kernel sw_kernels[dpm_task_name::TASK_NAME_LAST];

/// the ring buffers for sw kernels, same no. as sw threads
extern struct RequestRingBufferProgressive **sw_kernel_ring_buffers;

/// init the sw kernels, create the threads and the ring buffers, and init the kernels
bool dpm_init_sw_kernels();

/// cleanup the sw kernels
bool dpm_cleanup_sw_kernels();

/// the thread handler for the sw kernel threads.
/// @details each thread polls on its own ring buffer, and executes them.
/// @param args should just cast to the thread id
void sw_kernel_thread_handler(struct sw_kernel_thread_args *args);