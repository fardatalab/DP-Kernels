#include <boost/lockfree/queue.hpp>
#include <fcntl.h>
#include <mimalloc.h>
#include <string.h>
#include <sys/mman.h>

#define DPM_SHM_NAME "dpm_shared_mem"
#define DPM_SHM_SIZE 1024 * 1024 * 1024 // 1GB

struct dpm_shared_mem
{
    void *shm_ptr;
    size_t shm_size;

    /// the linux shared memory file descriptor
    int shm_fd;

    struct mi_allocator
    {
        mi_arena_id_t arena_id;
        mi_heap_t *heap;
    } allocator;
};

extern struct dpm_shared_mem dpm_shared_mem;

/// will initialize the shared memory, and `dpm_shared_mem` struct
bool _setup_shared_memory(size_t size);

/// setup mimalloc options, call this AFTER shared memory is setup
bool _setup_mi_options();

/// setup the heap and arena etc. for mimalloc, afterwards, mimalloc functions can be used
bool setup_mimalloc();

/// teardown mimalloc, unmap shared memory, close file descriptor etc.
bool teardown_mimalloc();