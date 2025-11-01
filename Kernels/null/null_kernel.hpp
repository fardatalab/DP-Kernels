#pragma once
#include "kernel_interface.hpp"
#include <cstdint>

dpkernel_error null_kernel_execute(dpkernel_task *task, int thread_id);
uint32_t null_kernel_can_execute_kernels(int thread_id, uint32_t *max_capacity);