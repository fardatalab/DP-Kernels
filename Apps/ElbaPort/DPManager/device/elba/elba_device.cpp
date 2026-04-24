#include "elba_device.hpp"
#include "common.hpp"

void *dpm_setup_device_ctx = NULL;

bool dpm_detect_platform(enum dpm_device *device)
{
    *device = DEVICE_ELBA;
    printf("underlying device detected as Pensando Elba\n");
    return true;
}

bool dpm_generate_device_ctx()
{
    return true;
}

bool dpm_register_executors(dpm_kernel global_executors[DEVICE_LAST][TASK_NAME_LAST])
{
    // init the null executor
    global_executors[dpm_device::DEVICE_NULL][dpm_task_name::TASK_NULL] =
        (struct dpm_kernel){.initialize = NULL,
                            .get_catalogue = null_kernel_get_catalogue,
                            .hw_kernel_remaining_capacity = null_kernel_can_execute_kernels,
                            .execute = null_kernel_execute,
                            .poll = NULL};
    printf("null executor registered for device %d, task %d\n", DEVICE_NULL, TASK_NULL);

    return true;
}

bool dpm_setup_memory_regions(void *ctx, struct dpm_io_mem_region *io_region)
{
    return true; // Elba does not require any special memory setup
}

bool dpm_setup_device(void *ctx)
{
    // Elba does not require any special device setup
    return true;
}

bool dpm_cleanup_device(void *ctx)
{
    // Elba does not require any special cleanup
    return true;
}