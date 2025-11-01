#pragma once
#include "faster_bench_common.hpp"
#include "libdeflate.h"
#include "rdma_comm.hpp"
#include <cstdint>
#include <cstdio>

// #include "null_disk.h"
#include "faster.h"

using namespace FASTER::core;
// NULL DISK is for in memory only FASTER store
typedef FASTER::device::NullDisk Disk;

/// Class passed to store_t::Read().
class ReadContext : public IAsyncContext
{
  public:
    typedef Key key_t;
    typedef CompressedValue value_t;

    ReadContext(uint64_t key, value_t *value) : key_{key}, value_{value}
    {
    }

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext &other) : key_{other.key_}, value_{other.value_}
    {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const
    {
        return key_;
    }

    inline void Get(const value_t &value)
    {
        // printf("ReadContext::Get() called for key %lu\n", key_.key);

        // // do a ptr assignment
        // value_ = const_cast<value_t *>(&value);

        // do a memcpy of the value
        std::memcpy(value_, &value, sizeof(value_t));
    }
    inline void GetAtomic(const value_t &value)
    {
        // same as Get()
        Get(value);
    }

    inline value_t *GetValue()
    {
        // printf("ReadContext::GetValue() returning value for key %lu\n", key_.key);
        return value_;
    }

  protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy)
    {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

  private:
    Key key_;
    value_t *value_;
};

/// Class passed to store_t::Upsert().
class UpsertContext : public IAsyncContext
{
  public:
    typedef Key key_t;
    typedef CompressedValue value_t;

    UpsertContext(uint64_t key, value_t *input) : key_{key}, input_{input}
    {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext &other) : key_{other.key_}, input_{other.input_}
    {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const
    {
        return key_;
    }
    inline static constexpr uint32_t value_size()
    {
        return sizeof(value_t);
    }

    /// Non-atomic and atomic Put() methods.
    inline void Put(value_t &value)
    {
        // value.value_ = input_;
        // memcpy the input bytes to the value
        // printf("UpsertContext::Put() called for key %lu\n", key_.key);
        std::memcpy(&(value.value_), input_, sizeof(*input_));
    }
    inline bool PutAtomic(value_t &value)
    {
        // value.atomic_value_.store(input_);
        // return true;

        // same as Put()
        Put(value);
        return true;
    }

  protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy)
    {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

  private:
    key_t key_;
    value_t *input_;
};

using store_t = FasterKv<Key, CompressedValue, Disk>;

void server_run_benchmark(store_t *store, vector<RDMAConnectionResource> &clients, size_t num_threads, int BatchSize,
                          int QueueDepth, uint64_t ReadNum, bool vanilla);

void server_thread_run_benchmark(store_t *store, RDMAConnectionResource &conn, size_t thread_idx, int BatchSize,
                                 int QueueDepth, uint64_t ReadNum);
//  , int RecvSize, int SendSize);

/// decompressor per thread
vector<libdeflate_decompressor *> decompressors;
/// compressor per thread
vector<libdeflate_compressor *> compressors;

inline bool init_libdeflate(int n_threads)
{
    decompressors.resize(n_threads);
    compressors.resize(n_threads);
    // init compressors and decompressors
    for (int i = 0; i < n_threads; ++i)
    {
        compressors[i] = libdeflate_alloc_compressor(6); // default is 6
        decompressors[i] = libdeflate_alloc_decompressor();
    }
    return true;
}
//// LIBDEFLATE de/compression

inline bool init_libdeflate(int n_threads);

inline size_t deflate_compress_data(int thread_idx, const void *in, size_t in_nbytes, void *out,
                                    size_t out_nbytes_avail)
{
    size_t compressed_size = libdeflate_deflate_compress(compressors[thread_idx], in, in_nbytes, out, out_nbytes_avail);
    return compressed_size; // return 0 if compression was unsuccessful
}

inline size_t deflate_decompress_data(int thread_idx, const void *in, size_t in_nbytes, void *out,
                                      size_t out_nbytes_avail, size_t *actual_out_nbytes_ret)
{
    enum libdeflate_result result = libdeflate_deflate_decompress(decompressors[thread_idx], in, in_nbytes, out,
                                                                  out_nbytes_avail, actual_out_nbytes_ret);
    if (result != LIBDEFLATE_SUCCESS)
    {
        if (result == LIBDEFLATE_INSUFFICIENT_SPACE)
        {
            printf("Insufficient space for decompression, out size = %zu\n", out_nbytes_avail);
        }
        else
        {
            printf("libdeflate decompression failed with error: %d\n", result);
        }
        return 0;
    }
    return *actual_out_nbytes_ret; // return the actual output size
}