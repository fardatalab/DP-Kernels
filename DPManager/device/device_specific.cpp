#include "device_specific.hpp"
#include "common.hpp"
#include <cstdio>

// delcare the extern'd globals
struct device_initializers dpm_device_initializers;
struct mem_initializers dpm_mem_initializers;

bool dpm_load_device_and_mem_initializers(void *handle, struct device_initializers *device_init,
                                          struct mem_initializers *mem_init)
{
    generate_device_ctx_func generate_device_ctx =
        (generate_device_ctx_func)dlsym(handle, GENERATE_DEVICE_CTX_FUNC_NAME);
    if (!generate_device_ctx)
    {
        std::cerr << "Cannot find generate_device_ctx in " << handle << '\n';
        return false;
    }
    // generate the device context, by whatever means the platform specific library wants
    if (!generate_device_ctx())
    {
        printf("generate device context failed\n");
        return false;
    }
    // load the symbol of the ctx
    void **ctx = (void **)dlsym(handle, SETUP_DEVICE_CTX_NAME);
    if (!ctx)
    {
        std::cerr << "Cannot find setup_device_ctx in " << handle << '\n';
        return false;
    }
    device_init->setup_device_ctx = *ctx;

    setup_device_func setup_device = (setup_device_func)dlsym(handle, SETUP_DEVICE_FUNC_NAME);
    if (!setup_device)
    {
        std::cerr << "Cannot find setup_device in " << handle << '\n';
        return false;
    }
    device_init->setup_device = setup_device;

    cleanup_device_func cleanup_device = (cleanup_device_func)dlsym(handle, CLEANUP_DEVICE_FUNC_NAME);
    if (!cleanup_device)
    {
        std::cerr << "Cannot find cleanup_device in " << handle << '\n';
        return false;
    }
    device_init->cleanup_device = cleanup_device;

    setup_memory_regions_func setup_memory_regions =
        (setup_memory_regions_func)dlsym(handle, SETUP_MEMORY_REGIONS_FUNC_NAME);
    if (!setup_memory_regions)
    {
        std::cerr << "Cannot find setup_memory_regions in " << handle << '\n';
        return false;
    }
    mem_init->setup_memory_regions = setup_memory_regions;
    return true;
}

bool dpm_load_kernels(void *handle, dpm_kernel global_executors[DEVICE_LAST][TASK_NAME_LAST])
{
    register_executors_func register_executors = (register_executors_func)dlsym(handle, REGISTER_EXECUTOR_FUNC_NAME);
    if (!register_executors)
    {
        std::cerr << "Cannot find register_executors in " << handle << '\n';
        return false;
    }
    if (!register_executors(global_executors))
    {
        std::cerr << "Cannot register executors in " << handle << '\n';
        return false;
    }
    return true;
}

bool dpm_device_and_mem_init(struct dpm_io_mem_region *dpm_own_mem_region)
{
    ////_setup_mem_req_queue();
    // return setup_bluefield(DPM_DOCA_PCI_ADDR, &dpm_doca_state);

    if (dpm_device_initializers.setup_device == nullptr)
    {
        std::cerr << "Device initializer is null: setup_device was not loaded\n";
        return false;
    }
    if (dpm_mem_initializers.setup_memory_regions == nullptr)
    {
        std::cerr << "Memory initializer is null: setup_memory_regions was not loaded\n";
        return false;
    }

    if (!dpm_device_initializers.setup_device(dpm_device_initializers.setup_device_ctx))
    {
        std::cerr << "Cannot setup device\n";
        return false;
    }

    if (!dpm_mem_initializers.setup_memory_regions(dpm_device_initializers.setup_device_ctx, dpm_own_mem_region))
    {
        std::cerr << "Cannot setup shared memory regions\n";
        return false;
    }
    return true;
}
