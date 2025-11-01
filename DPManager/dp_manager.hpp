#pragma once
#include "common.hpp"
#include "executor.hpp"
#include <atomic>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <cstddef>
#include <cstdlib>
#include <fcntl.h>
#include <mutex>
#include <string.h>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>

// #include <stdio.h>
// #include <stdlib.h>

extern int shmFd;
extern dpm_submission_completion *submission_completion_shm;

extern std::atomic<bool> polling_loop;
extern std::atomic<bool> stop_prog;
extern std::thread submission_thread;
extern std::thread completion_thread;

#define MAX_MANAGER_QUEUE_SIZE 32 // TODO: dynamic? parameter?

int dp_kernel_manager_start(void *args);

int setup_submission_completion_shm();

/// execute the task in the shared mem of dpm_submission_completion
/// calling the executor and passing the corresponding arguments
bool _dpm_execute_task();

/// (only the dpm submission polling thread will) poll for submission of tasks
/// in the shared mem of dpm_submission_completion.
/// return true if there is a submission and the task will be executed, false otherwise
bool dpm_poll_submission();

/// poll for completion of tasks in the shared mem of dpm_submission_completion.
// TODO: problem here: rely on the application to consume and clear the completion, what if it doesn't?
bool dpm_poll_completion();

int cleanup_submission_completion_shm();

int dp_kernel_manager_stop();
