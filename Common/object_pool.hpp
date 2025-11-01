#pragma once
#include <array>
#include <cstdio>
#include <vector>

// #ifdef __GNUC__
// #pragma GCC diagnostic push
// #pragma GCC diagnostic ignored "-Wclass-memaccess"
// #endif
/// object pool for the extra (non user allocated) coalescing tasks
/// this is a pool of dpkernel_task objects, which are used for coalescing
template <typename ObjType, size_t N> class ObjectPool
{
  private:
    std::array<ObjType, N> pool_storage_ = {};

    // acts as a stack (LIFO) for acquiring/releasing objects
    std::vector<ObjType *> free_list_;

  public:
    /**
     * @brief Constructs the ObjectPool.
     *
     * Initializes the pool storage with N objects and populates the free list.
     * Each object in the pool is value-initialized. For POD types, this means
     * they are zero-initialized.
     */
    ObjectPool()
    {
        // Ensure N is not excessively large to avoid huge stack allocation if this pool
        // itself is on the stack, though pool_storage_ is heap-allocated.
        // This is more of a general caution for large N.

        if (N == 0)
        {
            // Pool is empty, nothing to initialize.
            return;
        }

        // pool_storage_.resize(N);
        free_list_.reserve(N);

        // Initialize each object and add its pointer to the free list.
        for (size_t i = 0; i < N; ++i)
        {
            // pool_storage_[i] = ObjType();
            memset(&pool_storage_[i], 0, sizeof(ObjType)); // Zero-initialize the object

            // The current LIFO behavior of acquire/release means the order of initial population
            // determines the order of initial acquisition if multiple objects are acquired sequentially
            free_list_.push_back(&pool_storage_[N - 1 - i]);
        }
    }

    ~ObjectPool() = default;

    ObjectPool(const ObjectPool &) = delete;
    ObjectPool &operator=(const ObjectPool &) = delete;
    ObjectPool(ObjectPool &&) = default;
    ObjectPool &operator=(ObjectPool &&) = default;

    ObjType *acquire()
    {
        if (free_list_.empty())
        {
            printf("ObjectPool: No available objects in the pool.\n");
            return nullptr; // Pool is exhausted
        }

        ObjType *obj = free_list_.back();
        free_list_.pop_back();
        return obj;
    }

    void release(ObjType *obj)
    {
        // Reset the object
        // *obj = ObjType();
        memset(obj, 0, sizeof(ObjType)); // Zero-initialize the object

        free_list_.push_back(obj);
    }

    size_t available_count() const
    {
        return free_list_.size();
    }

    size_t total_size() const
    {
        return N;
    }
};
