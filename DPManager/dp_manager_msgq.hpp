#pragma once
#include "common.hpp"
#include "kernel_interface.hpp"
#include "memory.hpp"
#include "mpmc_queue.hpp"
#include <atomic>
// #include <boost/interprocess/ipc/message_queue.hpp>
#include <cstddef>
#include <cstdlib>
// #include <fcntl.h>
#include <mutex>
#include <string.h>
// #include <sys/mman.h>
#include "ring_buffer.hpp"
using namespace DDS_FrontEnd;
#include "device_specific.hpp"
#include <filesystem>
#include <thread>
#include <unistd.h>

#include "coalescing.hpp"
#include "scheduling.hpp"

#include "sw_device.hpp"

// the size for each kernel's request queue
#define DPM_SW_KERNEL_QUEUE_SIZE 128  // DPM_SUBMISSION_QUEUE_SIZE
#define DPM_HW_KERNEL_QUEUE_SIZE 1024 // DPM_SUBMISSION_QUEUE_SIZE

// void _create_msg_queue_boost();

// bool _create_msg_queue_mpmc();

bool _create_msg_queue_ring_buffer();

/// create the thread pool for sw kernels, and load and init all the sw kernels
bool dpm_init_sw_kernels();

/// automatically detect the platform and load the corresponding dynamic library symbols (functions) for device and mem
/// init.
/// @details it will load all the shared libraries in the platform specific path, and run their function for detecting
/// the platform. The assumption is at most one library will return true for the platform detection.
/// Once the platform is detected, it will load the device and memory initializers from the library, into a global
/// struct. Further calls to the device and memory initializers will use this struct.
/// @note the library should be named libdpm_<platform>.so, and should be in the platform specific path
/// @note the library should implement the function dpm_detect_platform() that returns true if the platform is
/// detected, and false otherwise.
bool dpm_detect_platform();

/// This function will be the main loop for each dpm thread spawned
/// @param args the thread id, starting from 0, 0th thread is the main thread
void _dpm_thread_handler(void *args);

int dp_kernel_manager_msgq_start(void *args);

dpkernel_error _handle_mem_req(struct dpm_mem_req *req);

/// if the task does not have a device specified, perform scheduling
/// given the observed moving average for HW & SW kernels for the task type
/// find the shortest time to completion (== queueing delay + kernel execution time for itself)
/// @param task the task to be executed
enum dpm_device _schedule_task_for_optimal_device(dpkernel_task *task, int thread_id);

/// internall calls _dpm_execute_task() and will try to queue it up (in software queue)
/// will fail if can't execute and can't enqueue
/// @param thread_id the thread id to execute the task on
/// @param msg the message to be executed
/// @param task the task to be executed, it is actually the one from msg->ptr
dpkernel_error _dpm_try_execute_or_queue_task(int thread_id, dpkernel_task *task);

/// Execute the task in the shared mem of dpm_submission_completion.
/// Calling the corresponding executor based on the device and task name.
/// @note A CPU (thread id) should be decided beforehand, and passed in as an argument.
/// @note the device should have already been set at this point, either by the app or by finding an optimal one
/// ourselves before this
/// @remark the task completion should not be set within this function (or directly by the executor kernel), will be
/// done by the caller depending on the return value
dpkernel_error _dpm_execute_task(dpkernel_task *task, int thread_id);

/// (only the dpm submission polling thread will) poll for requests, including both memory and task submissions
/// in the shared mem of dpm_submission_completion.
/// return true if there is a submission and the task will be executed, false otherwise
dpkernel_error dpm_poll_requests(int thread_id);
// dpkernel_error dpm_poll_requests();

/// drain the kernel queues, execute the tasks
dpkernel_error dpm_drain_kernel_queues(int thread_id);

/// poll for completion of tasks in the shared mem of dpm_submission_completion.
// TODO: problem here: rely on the application to consume and clear the completion, what if it doesn't?
void dpm_poll_completion(int thread_id);

int dp_kernel_manager_msgq_stop();

bool cleanup_dpm_msgq();