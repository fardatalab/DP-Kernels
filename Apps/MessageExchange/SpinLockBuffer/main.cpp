#include "buffer.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <thread>
#include <vector>
#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

// Restoring original correctness-check toggle; this change is unrelated to dual-copy functionality.
// #define TEST_CORRECTNESS true
#undef TEST_CORRECTNESS

static constexpr uint64_t NUM_ITEMS = 400'000;
static constexpr uint64_t RESPONSE_VALUE = 42;
static constexpr uint64_t NUM_PRODUCERS = 6; // 1 is 2
// Drain multiple committed single-item batches with constant lock operations per drain call.
static constexpr int CONSUMER_DRAIN_BURST = 1;

static std::atomic_uint64_t TotalReceivedResponses = 0;

struct Response {
  int size;
  int* data;
};

struct SmallResponse : Response {
  SmallResponse() {
    size = 8;
    data = new int[3];
  }
  ~SmallResponse() {
    delete[] data;
  }
};

struct MediumResponse : Response {
  MediumResponse() {
      size = 1024; // 1KB
      data = new int[257];
  }
  ~MediumResponse() {
    delete[] data;
  }
};

struct LargeResponse : Response {
  LargeResponse() {
    size = 1024 * 1024; // 1MB
    data = new int[262145];
  }
  ~LargeResponse() {
    delete[] data;
  }
};


struct alignas(CACHE_LINE_SIZE) ProducerState {
  Response** responses;
  char** message_data;
  uint64_t* message_sizes;
  uint64_t num_items;
};

/**
 * Pin a std::thread to a specific CPU core (Linux only).
 * This uses pthread_setaffinity_np and is a no-op on non-Linux platforms.
 */
static void PinThreadToCore(std::thread &thread, int core_id)
{
#if defined(__linux__)
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    int rc = pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
        std::cerr << "Failed to pin thread to core " << core_id << " (errno=" << rc << "): " << std::strerror(rc)
                  << "\n";
    }
#else
    (void)thread;
    (void)core_id;
#endif
}

void producer_thread_func(SpinlockBuffer* buffer, ProducerState& state) {
  // TODO: do we need to pin to a CPU core?
  auto num_items = state.num_items;
  int num_items_completed = 0;
  // Force non-batched producer behavior: one logical item per ProduceBatch call.
  // This models lock acquire/release per item instead of per multi-item batch.
  static constexpr size_t PRODUCER_BATCH_SIZE = 1;
  while (num_items_completed < num_items) {
    int num_inserted = 0;
    auto items_remaining = static_cast<size_t>(num_items - num_items_completed);
    auto items_to_submit = (items_remaining < PRODUCER_BATCH_SIZE) ? items_remaining : PRODUCER_BATCH_SIZE;
    // Original batched submission (kept for reference):
    // buffer->ProduceBatch(&state.message_data[num_items_completed], &state.message_sizes[num_items_completed], num_items - num_items_completed, &num_inserted);
    buffer->ProduceBatch(&state.message_data[num_items_completed], &state.message_sizes[num_items_completed], items_to_submit, &num_inserted);

    // New behavior: if reservation fails (no progress), yield to avoid tight spinning.
    // This gives the consumer a chance to advance consumer_index_ and free ring space.
    if (num_inserted == 0)
    {
        std::this_thread::yield();
        continue;
    }

    num_items_completed += num_inserted;
  }
  // done producing 
}

/**
 * Consume committed batches and perform a second consumer-side copy.
 * - `copy_to_primary` receives bytes copied out of the ring via `ConsumeBatch`.
 * - `copy_to_secondary` receives an additional copy of the same consumed bytes.
 */
void consumer_thread_func(SpinlockBuffer* buffer, char* copy_to_primary, char* copy_to_secondary) {
  // TODO: do we need to pin to a CPU core?
  if (copy_to_primary == nullptr || copy_to_secondary == nullptr) {
    std::cerr << "Consumer buffers must not be null\n";
    return;
  }
  auto start_time = std::chrono::high_resolution_clock::now();
  int total_items = 0;
  // Original non-draining loop (kept for reference):
  // while (total_items < NUM_ITEMS) {
  //   uint64_t size = 0;
  //   buffer->ConsumeBatch(copy_to_primary, &size);
  //   if (size == 0) {
  //     continue;
  //   }
  //   std::memcpy(copy_to_secondary, copy_to_primary, size);
  //   auto current_batch_items = *reinterpret_cast<int*>(copy_to_primary + sizeof(uint64_t));
  //   total_items += current_batch_items;
  // }

  while (total_items < NUM_ITEMS) {
      uint64_t drained_bytes = 0;
      int drained_batches = 0;

      // New burst-drain API: one call may consume multiple committed batches.
      buffer->ConsumeBatchesDrain(copy_to_primary, BUFFER_MAX_TAIL_ADVANCEMENT, CONSUMER_DRAIN_BURST, &drained_bytes,
                                  &drained_batches);

      if (drained_bytes == 0 || drained_batches == 0)
      {
          continue;
      }

    // Preserve the extra copy behavior, now applied once to the drained burst payload.
    std::memcpy(copy_to_secondary, copy_to_primary, drained_bytes);

    uint64_t offset = 0;
    for (int b = 0; b < drained_batches && total_items < NUM_ITEMS; ++b)
    {
        if (offset + sizeof(uint64_t) + sizeof(int) > drained_bytes)
        {
            std::cerr << "Invalid drained payload layout: truncated batch header\n";
            return;
        }

        auto batch_size = *reinterpret_cast<uint64_t *>(copy_to_primary + offset);
        auto current_batch_items = *reinterpret_cast<int *>(copy_to_primary + offset + sizeof(uint64_t));

        if (batch_size < sizeof(uint64_t) + sizeof(int))
        {
            std::cerr << "Invalid drained batch size: " << batch_size << "\n";
            return;
        }
        if (offset + batch_size > drained_bytes)
        {
            std::cerr << "Invalid drained payload layout: batch exceeds drained bytes\n";
            return;
        }
        if (current_batch_items != 1)
        {
            std::cerr << "Non-batched mode violation: expected 1 item per consumed batch, got " << current_batch_items
                      << "\n";
            return;
        }
        if (current_batch_items <= 0)
        {
            std::cerr << "Invalid batch item count: " << current_batch_items << "\n";
            return;
        }
#ifdef TEST_CORRECTNESS
      uint64_t batch_offset = offset + sizeof(uint64_t) + sizeof(int);
      for (int i = 0; i < current_batch_items; ++i)
      {
          auto response_size = *reinterpret_cast<int *>(copy_to_primary + batch_offset);
          auto size_1 = 8192;
          auto size_2 = 8;
          if (response_size != size_1 && response_size != size_2)
          {
              std::cout << "Incorrect response size at index " << total_items + i << std::endl;
              std::cout << "Response size: " << response_size << std::endl;
              std::cout << "Expected size 1: " << size_1 << std::endl;
              std::cout << "Expected size 2: " << size_2 << std::endl;
              return;
          }
          auto num_ints = response_size / sizeof(int);
          batch_offset += sizeof(int);
          for (int j = 0; j < num_ints; ++j)
          {
              int value = *reinterpret_cast<int *>(copy_to_primary + batch_offset);
              batch_offset += sizeof(int);
              if (value != RESPONSE_VALUE)
              {
                  std::cout << "Incorrect response value at index " << total_items + i << " and item " << j
                            << std::endl;
                  return;
              }
          }
      }
#endif
      total_items += current_batch_items;
      offset += batch_size;
    }
  }
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
  std::cout << "Consumer thread took " << duration.count() << " microseconds" << std::endl;
  double tput = (double)NUM_ITEMS / duration.count();
  std::cout << "Throughput: " << tput << " MOps\n";
  // done consuming
}

int main() {
  auto buffer = std::make_unique<SpinlockBuffer>();
  buffer->Initialize();

  std::vector<std::thread> producer_threads;
  std::vector<ProducerState> producer_states;
  std::thread consumer_thread;

  // prepare messages
  std::cout << "Preparing messages..." << std::endl;
  auto num_items_per_producer = NUM_ITEMS / NUM_PRODUCERS;
  for (int X = 0; X < NUM_PRODUCERS; X++) {
    if (X == NUM_PRODUCERS - 1) {
      num_items_per_producer = NUM_ITEMS - X * num_items_per_producer;
    }
    std::cout << "Preparing " << num_items_per_producer << " items for producer " << X << std::endl;
    ProducerState state;
    state.responses = new Response*[num_items_per_producer];
    state.message_data = new char*[num_items_per_producer];
    state.message_sizes = new uint64_t[num_items_per_producer];
    for (uint64_t i = 0; i < num_items_per_producer; i++) {
        int choice = 1;
        switch (choice)
        {
        case 0:
          state.responses[i] = new SmallResponse();
          break;
        case 1:
          state.responses[i] = new MediumResponse();
          break;
        case 2:
          state.responses[i] = new LargeResponse();
          break;
        default:
          state.responses[i] = new SmallResponse();
          break;
      }
      int num_ints = state.responses[i]->size / sizeof(int);
      state.responses[i]->data[0] = state.responses[i]->size;
      for (int j = 1; j < num_ints + 1; j++) {
        state.responses[i]->data[j] = RESPONSE_VALUE;
      }
      state.message_data[i] = reinterpret_cast<char*>(state.responses[i]->data);
      state.message_sizes[i] = state.responses[i]->size + sizeof(int);
    }
    state.num_items = num_items_per_producer;
    producer_states.push_back(state);
  }
  std::cout << "All producer states prepared" << std::endl;

  // Start producer threads, then pin them to distinct cores starting at core 0.
  for (int X = 0; X < NUM_PRODUCERS; X++) {
    producer_threads.push_back(std::thread(producer_thread_func, buffer.get(), std::ref(producer_states[X])));
    PinThreadToCore(producer_threads.back(), X);
  }
  // Original single destination buffer for consumer copies.
  // char* copy_to = new char[BUFFER_MAX_TAIL_ADVANCEMENT];
  char* copy_to_primary = new char[BUFFER_MAX_TAIL_ADVANCEMENT];
  char* copy_to_secondary = new char[BUFFER_MAX_TAIL_ADVANCEMENT];
  consumer_thread = std::thread(consumer_thread_func, buffer.get(), copy_to_primary, copy_to_secondary);
  // Pin consumer to the next core after producers.
  PinThreadToCore(consumer_thread, static_cast<int>(NUM_PRODUCERS));
  for (auto& thread : producer_threads) {
    thread.join();
  }
  consumer_thread.join();

  std::cout << "All threads joined\n";

  for (auto& state : producer_states) {
    delete[] state.responses;
    delete[] state.message_data;
    delete[] state.message_sizes;
  }
  delete[] copy_to_primary;
  delete[] copy_to_secondary;
  return 0;
}
