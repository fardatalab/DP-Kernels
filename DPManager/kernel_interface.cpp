#include "kernel_interface.hpp"
#include "common.hpp"

// #ifdef DOCA_VER_1_5
// #include "decompress_deflate_bf2.hpp"
// #include "doca_regex_bf2.hpp"
// #elif DOCA_VER_2_5
// #include "decompress_deflate_bf3.hpp"
// #endif

// #include "null_kernel.hpp"
#include <cstddef>
#include <cstring>

dpm_kernel dpm_executors[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

struct dpm_kernel_catalogue dpm_kernel_catalogues[dpm_device::DEVICE_LAST][dpm_task_name::TASK_NAME_LAST];

void dpm_init_hw_kernels()
{
    for (int i = 0; i < dpm_device::DEVICE_LAST; i++)
    {
        if (i == DEVICE_SOFTWARE)
        {
            // TODO: skip software device for now, they require separate init (n_threads would be different)
            continue;
        }
        for (int j = 0; j < dpm_task_name::TASK_NAME_LAST; j++)
        {
            if (dpm_executors[i][j].initialize != NULL)
            {
                struct dpm_kernel_catalogue *result = &dpm_kernel_catalogues[i][j];
                // if (dpm_executors[i][j].initialize(result, N_DPM_THREADS))
                if (dpm_executors[i][j].initialize(result, N_HW_KERNEL_QUEUES))
                {
                    // // copy the result to the global catalogue
                    // dpm_kernel_catalogues[i][j] = *result;
                    printf("kernel %d %d initialized\n", i, j);
                    if (dpm_executors[i][j].get_catalogue != NULL && dpm_executors[i][j].get_catalogue(result))
                    {
                        printf("kernel %d %d got catalogue\n", i, j);
                    }
                    else
                    {
                        printf("kernel %d %d has no get_catalogue\n", i, j);
                    }
                }
                else
                {
                    printf("kernel %d %d failed to initialize\n", i, j);
                }
            }
        }
    }
}

void dpm_cleanup_kernels()
{
    for (int i = 0; i < dpm_device::DEVICE_LAST; i++)
    {
        for (int j = 0; j < dpm_task_name::TASK_NAME_LAST; j++)
        {
            if (dpm_executors[i][j].cleanup != NULL)
            {
                dpm_executors[i][j].cleanup();
                printf("executor %d %d cleaned up\n", i, j);
            }
        }
    }
}

struct dpm_kernel *get_kernel(dpm_device device, dpm_task_name name)
{
    return &dpm_executors[device][name];
}
