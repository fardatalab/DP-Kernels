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
#include "regex_sw.hpp"

#define SW_KERNELS_QUEUE_SIZE                                                                                          \
    (std::max((size_t)64, (size_t)DPM_KERNELS_QUEUE_SIZE) * (sizeof(FileIOSizeT) + sizeof(struct dpm_req_msg)) +       \
     sizeof(FileIOSizeT))
#define SW_KERNELS_QUEUE_MAX_TAIL_ADVANCEMENT (SW_KERNELS_QUEUE_SIZE)

using SWKernelQueue = struct RequestRingBufferProgressive<SW_KERNELS_QUEUE_SIZE, SW_KERNELS_QUEUE_MAX_TAIL_ADVANCEMENT>;

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
extern SWKernelQueue **sw_kernel_ring_buffers;

/// init the sw kernels, create the threads and the ring buffers, and init the kernels
bool dpm_init_sw_kernels();

/// cleanup the sw kernels
bool dpm_cleanup_sw_kernels();

/// the thread handler for the sw kernel threads.
/// @details each thread polls on its own ring buffer, and executes them.
/// @param args should just cast to the thread id
void sw_kernel_thread_handler(struct sw_kernel_thread_args *args);