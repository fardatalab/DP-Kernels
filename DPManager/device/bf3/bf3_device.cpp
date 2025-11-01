#include "bf3_device.hpp"
#include <cstddef>
#include <cstdio>
#include <cstdlib>

struct dpm_doca_state dpm_doca_state;

// XXX: NOTE: this must be passed in by DPM!!!!
// struct dpm_io_mem_region dpm_own_mem_region;

// void *setup_device_ctx = NULL;

void *dpm_setup_device_ctx = NULL;

bool dpm_detect_platform()
{
    const char *version_file_path = "/opt/mellanox/doca/applications/VERSION";
    FILE *file = fopen(version_file_path, "r");
    if (!file)
    {
        printf("Failed to open DOCA version file\n");
        return false;
    }

    char version[64];
    if (fgets(version, sizeof(version), file) == NULL)
    {
        printf("Failed to read DOCA version\n");
        fclose(file);
        return false;
    }

    fclose(file);

    // Check if version starts with "2.5"
    if (version[0] == '2' && version[1] == '.' && version[2] == '5')
    {
        printf("Detected BlueField-3 platform (DOCA %s)\n", version);
        return true;
    }

    return false;
}

bool dpm_generate_device_ctx()
{
    struct bf3_device_init_ctx *setup_bf3_ctx =
        (struct bf3_device_init_ctx *)malloc(sizeof(struct bf3_device_init_ctx));
    setup_bf3_ctx->pci_addr = const_cast<char *>(DPM_DOCA_PCI_ADDR);
    setup_bf3_ctx->dpm_doca_state = &dpm_doca_state;
    // setup_bf3_ctx->dpm_own_mem_region = &dpm_own_mem_region; // this is defined in memory.cpp, NOT using it
    dpm_setup_device_ctx = (void *)setup_bf3_ctx;
    return true;
}

bool dpm_register_executors(dpm_kernel global_executors[DEVICE_LAST][TASK_NAME_LAST])
{
    // memset(global_executors, 0, sizeof(global_executors));

    // init the null executor
    global_executors[DEVICE_NULL][TASK_NULL] = (struct dpm_kernel){
        .initialize = NULL, .execute = null_kernel_execute, .poll = NULL, .get_estimated_completion_time = NULL};
    printf("null executor registered for device %d, task %d\n", DEVICE_NULL, TASK_NULL);

    // init the bf3 decompress executor
    global_executors[DEVICE_BLUEFIELD_3][TASK_DECOMPRESS_DEFLATE] = (struct dpm_kernel){
        .initialize = bf3_decompress_deflate_kernel_init,
        .cleanup = bf3_decompress_deflate_kernel_cleanup,
        .handle_mem_req = bf3_decompress_deflate_handle_mem_req,
        .execute = bf3_decompress_deflate_kernel_execute,
        .poll = bf3_decompress_deflate_kernel_poll,
        .get_estimated_completion_time = bf3_decompress_deflate_kernel_get_estimated_completion_time};
    printf("bf3 decompress executor registered for device %d, task %d\n", DEVICE_BLUEFIELD_3, TASK_DECOMPRESS_DEFLATE);
    return true;
}

bool setup_doca_shared_mem(struct dpm_doca_state *dpm_doca_state, struct dpm_io_mem_region *mem_region)
{
    doca_error_t result;
    // struct program_core_objects state = dpm_doca_state->state;
    // state = {};
    // TODO: add logic (the func which is NULL now) to sanely check for functionality required

    // doing the init outside this function, in `setup_bluefield`
    /* if (init_core_objects_mem(&dpm_doca_state->state, DPM_DOCA_MAX_BUF) != DOCA_SUCCESS)
    {
        printf("init_core_objects_mem failed\n");
        return false;
    }
    print_debug("init_core_objects_mem succeeded\n"); */

    if (doca_mmap_set_memrange(dpm_doca_state->state.src_mmap, mem_region->input_region.shm_ptr,
                               mem_region->input_region.shm_size) != DOCA_SUCCESS)
    {
        printf("doca_mmap_set_memrange failed for src_mmap\n");
        return false;
    }
    printf("doca_mmap_set_memrange succeeded for src_mmap\n");
    if (doca_mmap_set_memrange(dpm_doca_state->state.dst_mmap, mem_region->output_region.shm_ptr,
                               mem_region->output_region.shm_size) != DOCA_SUCCESS)
    {
        printf("doca_mmap_set_memrange failed for dst_mmap\n");
        return false;
    }
    print_debug("doca_mmap_set_memrange succeeded for dst_mmap\n");

    /* result = doca_mmap_set_free_cb(dpm_doca_state->state.src_mmap, NULL, NULL);
    if (result != DOCA_SUCCESS)
    {
        printf("doca_mmap_set_free_cb failed for src_mmap\n");
        return false;
    } */
    /* result = doca_mmap_set_free_cb(dpm_doca_state->state.dst_mmap, NULL, NULL);
    if (result != DOCA_SUCCESS)
    {
        printf("doca_mmap_set_free_cb failed for dst_mmap\n");
        return false;
    } */

    result = doca_mmap_start(dpm_doca_state->state.src_mmap);
    if (result != DOCA_SUCCESS)
    {
        printf("doca_mmap_start failed for src_mmap\n");
        return false;
    }
    result = doca_mmap_start(dpm_doca_state->state.dst_mmap);
    if (result != DOCA_SUCCESS)
    {
        printf("doca_mmap_start failed for dst_mmap\n");
        return false;
    }
    print_debug("doca_mmap_start succeeded for src_mmap and dst_mmap\n");

    return true;
}

bool dpm_setup_memory_regions(void *ctx, struct dpm_io_mem_region *io_region)
{
    struct bf3_device_init_ctx *bf3_ctx = (struct bf3_device_init_ctx *)ctx;
    struct dpm_doca_state *dpm_doca_state = bf3_ctx->dpm_doca_state;

    if (!setup_doca_shared_mem(dpm_doca_state, io_region))
    {
        printf("setup_doca_shared_mem failed\n");
        return false;
    }
    printf("setup_doca_shared_mem succeeded\n");

    return true;
}

bool dpm_setup_device(void *ctx)
{
    bool ret;
    struct bf3_device_init_ctx *bf3_ctx = (struct bf3_device_init_ctx *)ctx;
    const char *pci_addr = bf3_ctx->pci_addr;
    struct dpm_doca_state *dpm_doca_state = bf3_ctx->dpm_doca_state;
    // struct dpm_io_mem_region *dpm_own_mem_region = bf3_ctx->dpm_own_mem_region;

    if (open_doca_device_with_pci(pci_addr, NULL, &dpm_doca_state->state.dev) != DOCA_SUCCESS)
    {
        printf("open_doca_device_with_pci failed\n");
        // XXX HACK: return false; // shouldn't fail, not returning for testing purposes
    }
    printf("open_doca_device_with_pci succeeded\n");

    // TODO: This MUST be done by DPM!!
    /* bool ret = setup_mimalloc(dpm_own_mem_region, &dpm_req_ctx_shm);
    if (!ret)
    {
        printf("setup_mimalloc failed\n");
        return false;
    }
    printf("setup_mimalloc succeeded\n"); */

    // #ifdef DOCA_VER_1_5
    //     ret = create_core_objects(&dpm_doca_state->state, DPM_DOCA_WORKQ_DEPTH);
    //     if (ret != DOCA_SUCCESS)
    //     {
    //         printf("init_core_objects_workq failed\n");
    //         return false;
    //     }
    //     printf("init_core_objects_workq succeeded\n");
    // #elif DOCA_VER_2_5
    ret = create_core_objects(&dpm_doca_state->state, DPM_DOCA_MAX_BUF);
    if (ret != DOCA_SUCCESS)
    {
        printf("create_core_objects failed\n");
        return false;
    }
    // #endif
    printf("create_core_objects succeeded\n");

    // ret = setup_doca_shared_mem(pci_addr, dpm_doca_state, dpm_own_mem_region);
    // if (!ret)
    // {
    //     printf("setup_doca_shared_mem failed\n");
    //     return false;
    // }
    // printf("setup_doca_shared_mem succeeded\n");

    return true;
}

bool dpm_cleanup_device(void *ctx)
{
    if (destroy_core_objects(&dpm_doca_state.state) != DOCA_SUCCESS)
    {
        printf("destroy_core_objects failed\n");
        return false;
    }
    /* if (doca_compress_destroy(dpm_doca_state.compress) != DOCA_SUCCESS)
    {
        printf("doca_compress_destroy failed\n");
        return false;
    } */
    return true;
}