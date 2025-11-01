#pragma once
#include "common.hpp"
// includes for the platform specific interfaces
#include "device_specific.hpp"
#include "doca_common_v_2_2.h"
#include "kernel_interface.hpp"

// kernels
#include "decompress_deflate_bf3v2.2.hpp"
#include "null_kernel.hpp"

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_compress.h>
#include <doca_ctx.h>
#include <doca_error.h>
#include <doca_log.h>

// TODO: make this configurable?
#define DPM_DOCA_PCI_ADDR "e1:00.0"

struct dpm_doca_state
{
    // unused...
    /* char *src_mem_range;
    char *dst_mem_range; */

    // struct doca_compress *compress;
    struct program_core_objects state;
};

extern struct dpm_doca_state dpm_doca_state;

struct bf3v2_2_device_init_ctx
{
    char *pci_addr;
    struct dpm_doca_state *dpm_doca_state;
    // struct dpm_io_mem_region *dpm_own_mem_region; // this is defined in memory.cpp, NOT using it
};