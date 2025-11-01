/*
 * Interface for device and memory initialization for different platforms, that will be compiled separately, and loaded
 * at runtime by DP Manager.
 */

#pragma once
#include "common.hpp"
#include "kernel_interface.hpp"
#include "memory.hpp"
#include <dlfcn.h>
#include <iostream>

/// this path will be searched for the device and mem initializers and kernels
#define PLATFORM_SPECIFIC_LIB_PATH "."

////#define SETUP_DEVICE_CTX_NAME "dpm_setup_device_ctx"

// TODO: DOCA seems to require two different memory regions for input and output
// this may not be true for all devices, but DPM currently assumes this is the case, and manages two separate regions
// with mimalloc
// typedef bool (*setup_input_region_func)(size_t size, struct dpm_shared_mem *dpm_shared_mem, const char *shm_name);
// typedef bool (*setup_output_region_func)(struct dpm_io_mem_region *io_region, struct dpm_shared_mem *req_ctx_shm);

/// platform implementation specific function names, all implementations must have these functions

#define SETUP_MEMORY_REGIONS_FUNC_NAME "dpm_setup_memory_regions"
typedef bool (*setup_memory_regions_func)(void *ctx, dpm_io_mem_region *io_region);
extern "C" bool dpm_setup_memory_regions(void *ctx, dpm_io_mem_region *io_region);

#define DETECT_PLATFORM_FUNC_NAME "dpm_detect_platform"
typedef bool (*detect_platform_func)();
extern "C" bool dpm_detect_platform();

#define SETUP_DEVICE_CTX_NAME "dpm_setup_device_ctx"
extern "C" void *dpm_setup_device_ctx; // this is going into the device_initializers struct (the same field)

#define GENERATE_DEVICE_CTX_FUNC_NAME "dpm_generate_device_ctx"
typedef bool (*generate_device_ctx_func)();
extern "C" bool dpm_generate_device_ctx();

#define SETUP_DEVICE_FUNC_NAME "dpm_setup_device"
typedef bool (*setup_device_func)(void *ctx);
extern "C" bool dpm_setup_device(void *ctx);

#define CLEANUP_DEVICE_FUNC_NAME "dpm_cleanup_device"
typedef bool (*cleanup_device_func)(void *ctx);
extern "C" bool dpm_cleanup_device(void *ctx);

#define REGISTER_EXECUTOR_FUNC_NAME "dpm_register_executors"
typedef bool (*register_executors_func)(dpm_kernel global_executors[DEVICE_LAST][TASK_NAME_LAST]);
extern "C" bool dpm_register_executors(dpm_kernel global_executors[DEVICE_LAST][TASK_NAME_LAST]);

struct mem_initializers
{
    // setup_input_region_func setup_input_region;
    // setup_output_region_func setup_output_region;
    setup_memory_regions_func setup_memory_regions;
};

struct device_initializers
{
    // detect_platform_func detect_platform; // this is called outside, to determine the platform, before filling in the
    // struct
    void *setup_device_ctx;
    setup_device_func setup_device;
    cleanup_device_func cleanup_device;
};

/// the global device initializers to be used by DPM, filled in by loading the platform specific library
extern struct device_initializers dpm_device_initializers;
/// the global memory initializers to be used by DPM, filled in by loading the platform specific library
extern struct mem_initializers dpm_mem_initializers;

/// load from the shared library, the function pointers for the device and memory initializers
bool dpm_load_device_and_mem_initializers(void *handle, struct device_initializers *device_init,
                                          struct mem_initializers *mem_init);

/// load the kernels from the shared library, caller will register the executors
/// @param handle the handle to the shared library
/// @param global_executors the global executors to be filled in (will be: dpm_task_executor
/// dpm_executors[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];)
/// @return true if the kernels were loaded successfully, and the parameters will be filled in, false otherwise
bool dpm_load_kernels(void *handle, dpm_kernel global_executors[DEVICE_LAST][TASK_NAME_LAST]);

/// entry point to initialize the shared memory and mimalloc and everything
bool dpm_device_and_mem_init(struct dpm_io_mem_region *dpm_own_mem_region);