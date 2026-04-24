#include "buffer.hpp"

#include <cstring>
#include <cassert>
#include <iostream>
#include <mutex>
#include <thread>

void SpinlockBuffer::Initialize() {
  // Original initialization logic (commented out to preserve intent/history):
  // producer_index_.value = 0;
  // consumer_index_.value = 0;
  // std::memset(buffer_, 0, BUFFER_SIZE);

  // Modified initialization: clear all indices, including the new reserve index.
  // This aligns with the reserve/commit scheme for producers.
  producer_index_.value = 0; // committed tail
  reserve_index_.value = 0;  // reserved tail
  consumer_index_.value = 0; // head
  std::memset(buffer_, 0, BUFFER_SIZE);
}

// Original ProduceBatch implementation (commented out to preserve intent/history).
// bool SpinlockBuffer::ProduceBatch(char** data_list, size_t* size_list, size_t num_items, int* num_inserted) { ... }

/**
 * Reserve/commit producer path:
 * 1) Reserve a contiguous region under spin_lock_ (reservation lock).
 * 2) Copy payloads outside the lock.
 * 3) Commit (publish) the batch under commit_lock_ in reservation order.
 */
bool SpinlockBuffer::ProduceBatch(char** data_list, size_t* size_list, size_t num_items, int* num_inserted) {
  // Defensive argument checks.
  if (num_inserted == nullptr) {
#ifndef NDEBUG
    std::cerr << "ProduceBatch error: num_inserted is null\n";
#endif
    return false;
  }
  *num_inserted = 0;
  if (data_list == nullptr || size_list == nullptr || num_items == 0) {
#ifndef NDEBUG
    std::cerr << "ProduceBatch error: invalid input pointers or num_items == 0\n";
#endif
    return false;
  }

  int resp_index = 0;
  uint64_t batch_bytes = 0;
  uint64_t reserve_start = 0;
  uint64_t reserve_end = 0;
  uint64_t reserve_pos = 0;

  {
    // Reservation lock: protect reserve_index_ and reads of consumer_index_.
    std::lock_guard<LockType> locker(spin_lock_);

    auto consumer_idx = consumer_index_.value;
    auto reserve_idx = reserve_index_.value;
    auto reserve_pos_local = get_buffer_idx(reserve_idx);

    // Available distance in the ring based on reserved tail and consumer head.
    uint64_t distance = BUFFER_SIZE - reserve_idx + consumer_idx;

    if (distance < BUFFER_MIN_TAIL_ADVANCEMENT + sizeof(uint64_t) + sizeof(int)) [[unlikely]] {
      // Not enough headroom to write even a minimal batch.
      return false;
    }

    // If header can't fit at the end, wrap to the start.
    auto distance_to_end = BUFFER_SIZE - reserve_pos_local;
    if (distance_to_end < sizeof(uint64_t) + sizeof(int)) {
      // Wrap around to the beginning; adjust distance accordingly.
      reserve_idx += distance_to_end;
      reserve_pos_local = 0;
      distance -= distance_to_end;
    }

    // Start reserving bytes for the batch (header + payloads).
    uint64_t old_tail = reserve_idx;
    batch_bytes += sizeof(uint64_t) + sizeof(int);
    reserve_idx += batch_bytes;

    for (; resp_index != static_cast<int>(num_items); resp_index++) {
      uint64_t data_bytes = size_list[resp_index];
      uint64_t next_bytes = batch_bytes + data_bytes;

      // Round to 8-byte alignment to keep batch boundaries aligned.
      if (next_bytes % sizeof(uint64_t) != 0) {
        next_bytes += (sizeof(uint64_t) - (next_bytes % sizeof(uint64_t)));
      }

      if (next_bytes > distance || next_bytes > BUFFER_MAX_TAIL_ADVANCEMENT) {
        // No more space for additional items.
        break;
      }

      batch_bytes += data_bytes;
      reserve_idx += data_bytes;
    }

    // Round total response bytes up to a multiple of 8 bytes.
    if (batch_bytes % sizeof(uint64_t) != 0) {
      batch_bytes += (sizeof(uint64_t) - (batch_bytes % sizeof(uint64_t)));
    }

    if (resp_index == 0) {
      // Avoid reserving or publishing empty batches.
      return false;
    }

    // Commit the reservation locally and publish the reserved tail.
    reserve_start = old_tail;
    reserve_end = old_tail + batch_bytes;
    reserve_pos = get_buffer_idx(reserve_start);
    reserve_index_.value = reserve_end;

    // Defensive sanity checks while still under the reservation lock.
    assert(reserve_start % sizeof(uint64_t) == 0);
    assert(reserve_end >= reserve_start);
  }

  // Copy payloads outside the reservation lock to reduce contention.
  uint64_t write_idx = reserve_start + sizeof(uint64_t) + sizeof(int);
  for (int i = 0; i < resp_index; ++i) {
    uint64_t data_bytes = size_list[i];
    auto curr_write_pos = get_buffer_idx(write_idx);
    if (curr_write_pos + data_bytes > BUFFER_SIZE) {
      uint64_t size_1 = BUFFER_SIZE - curr_write_pos;
      uint64_t size_2 = data_bytes - size_1;
      std::memcpy(buffer_ + curr_write_pos, data_list[i], size_1);
      std::memcpy(buffer_, data_list[i] + size_1, size_2);
    } else {
      std::memcpy(buffer_ + curr_write_pos, data_list[i], data_bytes);
    }
    write_idx += data_bytes;
  }

  // Commit in reservation order; this publishes the header and advances the committed tail.
  while (true) {
    {
      std::lock_guard<LockType> locker(commit_lock_);
      if (producer_index_.value == reserve_start) {
        // Write the header at commit time to avoid consumers seeing partial data.
        *(uint64_t*)(buffer_ + reserve_pos) = batch_bytes;
        *(int*)(buffer_ + reserve_pos + sizeof(uint64_t)) = resp_index;
        producer_index_.value = reserve_end;
        break;
      }
    }
    // Another producer is ahead in the commit order; yield to reduce contention.
    std::this_thread::yield();
  }

  *num_inserted = resp_index;
  return true;
}

// Original ConsumeBatch implementation (commented out to preserve intent/history).
// bool SpinlockBuffer::ConsumeBatch(char* copy_to, uint64_t* size) { ... }

/**
 * Consume path:
 * - Snapshot committed tail under commit_lock_.
 * - Read and copy batch data outside the lock.
 * - Advance consumer index under the reservation lock.
 */
bool SpinlockBuffer::ConsumeBatch(char* copy_to, uint64_t* size) {
  // Defensive argument checks.
  if (copy_to == nullptr || size == nullptr) {
#ifndef NDEBUG
    std::cerr << "ConsumeBatch error: copy_to or size is null\n";
#endif
    return false;
  }
  *size = 0;

  uint64_t committed_idx = 0;
  {
    // Acquire commit_lock_ to synchronize with producer publishes.
    std::lock_guard<LockType> locker(commit_lock_);
    committed_idx = producer_index_.value;
  }

  uint64_t consumer_idx = 0;
  {
    // Protect consumer_index_ so producers see consistent head values.
    std::lock_guard<LockType> locker(spin_lock_);
    consumer_idx = consumer_index_.value;
  }

  if (committed_idx == consumer_idx) {
    return false;
  }

  auto consumer_pos = get_buffer_idx(consumer_idx);
  auto distance_to_end = BUFFER_SIZE - consumer_pos;
  if (distance_to_end < sizeof(uint64_t) + sizeof(int)) {
    // Wrap around to the start if header can't fit at the end.
    consumer_idx += distance_to_end;
    consumer_pos = 0;
  }

  auto response_size = *(uint64_t*)(buffer_ + consumer_pos);
  if (response_size == 0) {
#ifndef NDEBUG
    std::cerr << "ConsumeBatch error: response_size == 0 (unpublished or corrupt header)\n";
#endif
    return false;
  }

  // Copy the response to the output buffer.
  auto remaining_size = BUFFER_SIZE - consumer_pos;
  if (remaining_size < response_size) {
    std::memcpy(copy_to, buffer_ + consumer_pos, remaining_size);
    std::memcpy(copy_to + remaining_size, buffer_, response_size - remaining_size);
  } else {
    std::memcpy(copy_to, buffer_ + consumer_pos, response_size);
  }
  *size = response_size;

  // Mark this batch as consumed.
  consumer_idx += response_size;
  {
    std::lock_guard<LockType> locker(spin_lock_);
    consumer_index_.value = consumer_idx;
  }
  return true;
}


/**
 * Drain path:
 * - Snapshot committed tail once under commit_lock_.
 * - Snapshot consumer head once under spin_lock_.
 * - Copy up to max_batches committed batches outside locks.
 * - Publish the advanced consumer index once under spin_lock_.
 */
bool SpinlockBuffer::ConsumeBatchesDrain(char* copy_to, uint64_t copy_capacity, int max_batches,
                                         uint64_t* total_size, int* batches_consumed) {
  // Defensive argument checks.
  if (copy_to == nullptr || total_size == nullptr || batches_consumed == nullptr) {
#ifndef NDEBUG
    std::cerr << "ConsumeBatchesDrain error: null output argument(s)\n";
#endif
    return false;
  }
  *total_size = 0;
  *batches_consumed = 0;
  if (max_batches <= 0 || copy_capacity == 0) {
#ifndef NDEBUG
    std::cerr << "ConsumeBatchesDrain error: invalid max_batches or copy_capacity\n";
#endif
    return false;
  }

  uint64_t committed_idx = 0;
  {
    // Snapshot publish tail once.
    std::lock_guard<LockType> locker(commit_lock_);
    committed_idx = producer_index_.value;
  }

  uint64_t consumer_idx = 0;
  {
    // Snapshot consume head once.
    std::lock_guard<LockType> locker(spin_lock_);
    consumer_idx = consumer_index_.value;
  }

  if (committed_idx == consumer_idx) {
    return false;
  }

  uint64_t read_idx = consumer_idx;
  uint64_t write_offset = 0;
  int drained = 0;

  while (drained < max_batches && read_idx < committed_idx) {
    auto read_pos = get_buffer_idx(read_idx);
    auto distance_to_end = BUFFER_SIZE - read_pos;

    // Match producer wrap rule for header placement.
    if (distance_to_end < sizeof(uint64_t) + sizeof(int)) {
      read_idx += distance_to_end;
      if (read_idx >= committed_idx) {
        break;
      }
      read_pos = 0;
      distance_to_end = BUFFER_SIZE;
    }

    auto batch_size = *(uint64_t*)(buffer_ + read_pos);
    if (batch_size < sizeof(uint64_t) + sizeof(int) || batch_size > BUFFER_MAX_TAIL_ADVANCEMENT) {
#ifndef NDEBUG
      std::cerr << "ConsumeBatchesDrain error: invalid batch_size=" << batch_size << "\n";
#endif
      return false;
    }

    // Don't overrun caller output buffer; stop draining for this call.
    if (write_offset + batch_size > copy_capacity) {
      break;
    }

    // Don't consume beyond the committed tail snapshot.
    if (read_idx + batch_size > committed_idx) {
      break;
    }

    // Copy this batch contiguously into caller buffer.
    if (read_pos + batch_size > BUFFER_SIZE) {
      auto size_1 = BUFFER_SIZE - read_pos;
      auto size_2 = batch_size - size_1;
      std::memcpy(copy_to + write_offset, buffer_ + read_pos, size_1);
      std::memcpy(copy_to + write_offset + size_1, buffer_, size_2);
    } else {
      std::memcpy(copy_to + write_offset, buffer_ + read_pos, batch_size);
    }

    write_offset += batch_size;
    read_idx += batch_size;
    drained += 1;
  }

  if (drained == 0) {
    return false;
  }

  // Publish head advance once for this drained burst.
  {
    std::lock_guard<LockType> locker(spin_lock_);
    consumer_index_.value = read_idx;
  }

  *total_size = write_offset;
  *batches_consumed = drained;
  return true;
}
