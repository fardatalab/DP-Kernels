#pragma once

#include "types.hpp"
#include "spinlock.hpp"

static constexpr size_t BUFFER_SIZE = 128 * 1024 * 1024; // 128MB, must be a power of 2
static constexpr size_t BUFFER_MIN_TAIL_ADVANCEMENT = 8192; // 8KB, must be a power of 2
static constexpr size_t BUFFER_MAX_TAIL_ADVANCEMENT = 128 * 1024 * 1024; // 128MB, must be a power of 2 

// Defensive compile-time checks for ring buffer invariants.
static_assert((BUFFER_SIZE & (BUFFER_SIZE - 1)) == 0, "BUFFER_SIZE must be a power of 2");
static_assert((BUFFER_MIN_TAIL_ADVANCEMENT & (BUFFER_MIN_TAIL_ADVANCEMENT - 1)) == 0,
              "BUFFER_MIN_TAIL_ADVANCEMENT must be a power of 2");
static_assert((BUFFER_MAX_TAIL_ADVANCEMENT & (BUFFER_MAX_TAIL_ADVANCEMENT - 1)) == 0,
              "BUFFER_MAX_TAIL_ADVANCEMENT must be a power of 2");
static_assert(BUFFER_MAX_TAIL_ADVANCEMENT <= BUFFER_SIZE,
              "BUFFER_MAX_TAIL_ADVANCEMENT must not exceed BUFFER_SIZE");

class SpinlockBuffer {

  using CacheAlignedU64 = CacheAlignedT<uint64_t>;
  using LockType = SpinLock;
public:

  SpinlockBuffer() = default;
  ~SpinlockBuffer() = default;

  // Initialize all indices and clear the buffer. Must be called before use.
  void Initialize();

  // Produce a batch of items:
  // - Reserve space under a spinlock.
  // - Copy payloads outside the lock.
  // - Commit (publish) the batch under a second spinlock.
  bool ProduceBatch(char** data_list, size_t* size_list, size_t num_items, int* num_inserted);

  // Consume the next committed batch into copy_to; size receives the total batch size.
  bool ConsumeBatch(char* copy_to, uint64_t* size);

  // Consume up to max_batches already-committed batches into copy_to in one call.
  // - copy_capacity bounds the output write size defensively.
  // - total_size receives total bytes copied.
  // - batches_consumed receives number of batches consumed.
  // This keeps lock acquisitions constant per drain call.
  bool ConsumeBatchesDrain(char* copy_to, uint64_t copy_capacity, int max_batches,
                           uint64_t* total_size, int* batches_consumed);

  char* buffer() noexcept {
    return buffer_;
  }

  // Returns the committed (published) producer index (tail).
  const uint64_t producer_index() const noexcept {  
    return producer_index_.value;
  }

  const uint64_t consumer_index() const noexcept {
    return consumer_index_.value;
  }

  const uint64_t get_buffer_idx(uint64_t idx) const noexcept {
    return idx & (BUFFER_SIZE - 1);
  }
  
private:

  // NOTE: spin_lock_ is now used as the reservation/consumer index lock.
  // The original single-lock design is preserved in comments in buffer.cpp.
  LockType spin_lock_;
  LockType commit_lock_;
  // producer_index_ now represents the committed (published) tail index.
  CacheAlignedU64 producer_index_;
  // reserve_index_ represents the reserved tail index for in-flight producers.
  CacheAlignedU64 reserve_index_;
  CacheAlignedU64 consumer_index_;

  char buffer_[BUFFER_SIZE];

};
