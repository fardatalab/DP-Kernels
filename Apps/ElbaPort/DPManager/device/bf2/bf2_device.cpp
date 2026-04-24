#include "bf2_device.hpp"
#include <cstddef>
#include <cstdio>
#include <cstdlib>

struct dpm_doca_state dpm_doca_state;

void *dpm_setup_device_ctx = NULL;

bool dpm_detect_platform(enum dpm_device *device)
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

    // Check if version starts with "1" (DOCA v1.5)
    if (version[0] == '1')
    {
        printf("Detected BlueField-2 platform (DOCA %s)\n", version);
        *device = DEVICE_BLUEFIELD_2;
        return true;
    }

    return false;
}

bool dpm_generate_device_ctx()
{
    struct bf3v2_2_device_init_ctx *setup_bf2_ctx =
        (struct bf3v2_2_device_init_ctx *)malloc(sizeof(struct bf3v2_2_device_init_ctx));
    setup_bf2_ctx->pci_addr = const_cast<char *>(DPM_DOCA_PCI_ADDR);
    setup_bf2_ctx->dpm_doca_state = &dpm_doca_state;
    dpm_setup_device_ctx = (void *)setup_bf2_ctx;
    return true;
}

bool dpm_register_executors(dpm_task_executor global_executors[DEVICE_LAST][TASK_NAME_LAST])
{
    // memset(global_executors, 0, sizeof(global_executors));

    // init the null executor
    global_executors[dpm_device::DEVICE_NULL][dpm_task_name::TASK_NULL] = (struct dpm_task_executor){
        .initialize = NULL, .execute = null_kernel_execute, .poll = NULL, .get_estimated_completion_time = NULL};
    printf("null executor registered for device %d, task %d\n", DEVICE_NULL, TASK_NULL);

    // init the bf2 executor
    global_executors[DEVICE_BLUEFIELD_2][TASK_DECOMPRESS_DEFLATE] = (struct dpm_task_executor){
        .initialize = bf2_decompress_deflate_kernel_init,
        .cleanup = bf2_decompress_deflate_kernel_cleanup,
        .execute = bf2_decompress_deflate_kernel_execute,
        .poll = bf2_decompress_deflate_kernel_poll,
        .get_estimated_completion_time = bf2_decompress_deflate_kernel_get_estimated_completion_time};
    printf("bf2 decompress executor registered for device %d, task %d\n", DEVICE_BLUEFIELD_2, TASK_DECOMPRESS_DEFLATE);

    return true;
}

void free_mmap_callback(void *addr, size_t len, void *opaque)
{
    // do nothing
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

    if ((result = doca_mmap_populate(dpm_doca_state->state.src_mmap, mem_region->input_region.shm_ptr,
                                     mem_region->input_region.shm_size, 4096, free_mmap_callback, NULL)) != DOCA_SUCCESS)
    {
        printf("doca_mmap_populate failed for src_mmap: %s\n", doca_get_error_string(result));
        return false;
    }
    printf("doca_mmap_populate succeeded for src_mmap\n");
    if ((result = doca_mmap_populate(dpm_doca_state->state.dst_mmap, mem_region->output_region.shm_ptr,
                                     mem_region->output_region.shm_size, 4096, free_mmap_callback, NULL)) != DOCA_SUCCESS)
    {
        printf("doca_mmap_populate failed for dst_mmap %s\n", doca_get_error_string(result));
        return false;
    }
    printf("doca_mmap_populate succeeded for dst_mmap\n");

    print_debug("doca_mmap_start succeeded for src_mmap and dst_mmap\n");

    /* result = doca_mmap_dev_add(dpm_doca_state->state.src_mmap, dpm_doca_state->state.dev);
    if (result != DOCA_SUCCESS)
    {
        printf("Unable to add device to source mmap: %s\n", doca_get_error_string(result));
        doca_mmap_destroy(dpm_doca_state->state.src_mmap);
        dpm_doca_state->state.src_mmap = NULL;
        return result;
    }
    result = doca_mmap_dev_add(dpm_doca_state->state.dst_mmap, dpm_doca_state->state.dev);
    if (result != DOCA_SUCCESS)
    {
        printf("Unable to add device to destination mmap: %s\n", doca_get_error_string(result));
        doca_mmap_destroy(dpm_doca_state->state.dst_mmap);
        dpm_doca_state->state.dst_mmap = NULL;
        return result;
    }
    printf("doca_mmap_dev_add succeeded for src_mmap and dst_mmap\n"); */

    return true;
}

bool dpm_setup_memory_regions(void *ctx, struct dpm_io_mem_region *io_region)
{
    struct bf3v2_2_device_init_ctx *bf2_ctx = (struct bf3v2_2_device_init_ctx *)ctx;
    struct dpm_doca_state *dpm_doca_state = bf2_ctx->dpm_doca_state;

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
    struct bf3v2_2_device_init_ctx *bf2_ctx = (struct bf3v2_2_device_init_ctx *)ctx;
    const char *pci_addr = bf2_ctx->pci_addr;
    struct dpm_doca_state *dpm_doca_state = bf2_ctx->dpm_doca_state;
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
