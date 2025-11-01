#include "memory_test.hpp"

struct dpm_shared_mem dpm_shared_mem;

bool _setup_shared_memory(size_t shm_size)
{
    dpm_shared_mem.shm_size = shm_size;

    dpm_shared_mem.shm_fd = shm_open(DPM_SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (dpm_shared_mem.shm_fd == -1)
    {
        perror("shm_open");
        return false;
    }

    if (ftruncate(dpm_shared_mem.shm_fd, shm_size) == -1)
    {
        perror("ftruncate");
        close(dpm_shared_mem.shm_fd);
        return false;
    }

    dpm_shared_mem.shm_ptr =
        (void *)mmap(nullptr, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, dpm_shared_mem.shm_fd, 0);

    printf("dpm_shared_mem.shm_ptr: %p\n", dpm_shared_mem.shm_ptr);

    if (dpm_shared_mem.shm_ptr == MAP_FAILED)
    {
        perror("mmap shm");
        close(dpm_shared_mem.shm_fd);
        return false;
    }
    memset(dpm_shared_mem.shm_ptr, 42, shm_size);
    return true;
}

bool _setup_mi_options()
{
    mi_option_enable(mi_option_disallow_os_alloc);
    printf("mi_option_reserve_os_memory = %lu\n", mi_option_get_size(mi_option_reserve_os_memory));
    // mi_option_set(mi_option_disallow_os_alloc, 1);
    // mi_option_enable(mi_option_limit_os_alloc);
    // mi_option_set(mi_option_limit_os_alloc, 1);
    return true;
}

bool setup_mimalloc()
{
    // setup shared mem first
    if (!_setup_shared_memory(DPM_SHM_SIZE))
    {
        printf("setup_shared_memory failed\n");
        return false;
    }

    // setup mimalloc options
    if (!_setup_mi_options())
    {
        printf("setup_mi_options failed\n");
        return false;
    }

    if (!mi_manage_os_memory_ex(dpm_shared_mem.shm_ptr, dpm_shared_mem.shm_size, true, false, false, -1, true,
                                &dpm_shared_mem.allocator.arena_id))
    {
        printf("mi_manage_os_memory_ex failed\n");
        return false;
    }
    // mimalloc seems to always zero out the memory
    // memset(dpm_shared_mem.shm_ptr, 42, dpm_shared_mem.shm_size);

    dpm_shared_mem.allocator.heap = mi_heap_new_in_arena(dpm_shared_mem.allocator.arena_id);
    if (dpm_shared_mem.allocator.heap == NULL)
    {
        printf("mi_heap_new_in_arena failed\n");
        return false;
    }
    return true;
}

bool teardown_mimalloc()
{
    // XXX: somehow this makes mi_heap_delete seg fault...
    // mi_heap_destroy(dpm_shared_mem.allocator.heap);
    mi_heap_delete(dpm_shared_mem.allocator.heap);

    if (munmap(dpm_shared_mem.shm_ptr, dpm_shared_mem.shm_size) == -1)
    {
        perror("munmap DPM_SHM_NAME");
        return false;
    }
    close(dpm_shared_mem.shm_fd);
    if (shm_unlink(DPM_SHM_NAME) == -1)
    {
        perror("shm_unlink DPM_SHM_NAME");
        return false;
    }
    return true;
}

int main()
{
    setup_mimalloc();

    void *ptr;

    for (int i = 0; i < 99999; i++)
    {
        printf("i = %d\n", i);
        ptr = mi_heap_malloc(dpm_shared_mem.allocator.heap, 1024 / 32 * 1024 * 1024);
        if (ptr == NULL)
        {
            printf("mi_heap_malloc failed\n");
            return -1;
        }
        printf("ptr: %p\n", ptr);
        printf("*ptr as int: %d\n", *(int *)ptr);
        mi_free(ptr);
    }

    // for (int i = 0; i < 1024; i++)
    // // int i = 0;
    // // while (1)
    // {
    //     ptr = mi_heap_malloc(dpm_shared_mem.allocator.heap, 1024 * 1024);
    //     if (ptr == NULL)
    //     {
    //         printf("mi_heap_malloc failed, i = %d\n", i);
    //         return -1;
    //     }
    //     printf("i = %d\n", i);
    //     printf("*ptr as int: %d\n", *(int *)ptr);
    //     printf("ptr: %p\n", ptr);
    //     // mi_free(ptr);
    // }

    teardown_mimalloc();
    return 0;
}
