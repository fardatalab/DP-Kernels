#pragma once
#include "common.hpp"
#include "executor.hpp"
#include "memory.hpp"
#include "mpmc_queue.hpp"
#include <atomic>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <cstddef>
#include <cstdlib>
// #include <fcntl.h>
#include <mutex>
#include <string.h>
// #include <sys/mman.h>
#include <thread>
#include <unistd.h>

#include "ring_buffer.hpp"
using namespace DDS_FrontEnd;

void _create_msg_queue_boost();

bool _create_msg_queue_mpmc();

bool _create_msg_queue_ring_buffer();

int dp_kernel_manager_msgq_start(void *args);

/// execute the task in the shared mem of dpm_submission_completion
/// calling the executor and passing the corresponding arguments
bool _dpm_execute_task_msgq();

/// (only the dpm submission polling thread will) poll for submission of tasks
/// in the shared mem of dpm_submission_completion.
/// return true if there is a submission and the task will be executed, false otherwise
bool dpm_poll_submission_msgq();

/// poll for completion of tasks in the shared mem of dpm_submission_completion.
// TODO: problem here: rely on the application to consume and clear the completion, what if it doesn't?
bool dpm_poll_completion_msgq();

int dp_kernel_manager_msgq_stop();

bool cleanup_dpm_msgq();