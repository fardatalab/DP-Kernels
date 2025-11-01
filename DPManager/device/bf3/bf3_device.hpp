#pragma once
// includes for the platform specific interfaces
#include "device_specific.hpp"
#include "doca_common_v_2_5.h"
#include "executor.hpp"

// kernels for bf3
#include "decompress_deflate_bf3.hpp"
#include "null_kernel.hpp"

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_compress.h>
#include <doca_ctx.h>
#include <doca_error.h>
#include <doca_log.h>

// TODO: make this configurable?
#define DPM_DOCA_PCI_ADDR "03:00.0"

struct dpm_doca_state
{
    // unused...
    /* char *src_mem_range;
    char *dst_mem_range; */

    // struct doca_compress *compress;
    struct program_core_objects state;
};

extern struct dpm_doca_state dpm_doca_state;

// XXX: NOTE: this must be passed in by DPM!!!!
// extern struct dpm_io_mem_region dpm_own_mem_region;

// extern void *setup_device_ctx; // declared in device_specific.hpp

struct bf3_device_init_ctx
{
    char *pci_addr;
    struct dpm_doca_state *dpm_doca_state;
    // struct dpm_io_mem_region *dpm_own_mem_region; // this is defined in memory.cpp, NOT using it
};

// bool dpm_generate_device_ctx();
// bool dpm_register_executors(dpm_task_executor global_executors[DEVICE_LAST][TASK_NAME_LAST]);
// bool dpm_setup_device(void *ctx);
// bool dpm_cleanup_device(void *ctx);
