#pragma once
#include "common.hpp"
#include <cstdint>
#include <cstdio>

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
        // do a ptr assignment
        value_ = const_cast<value_t *>(&value);
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