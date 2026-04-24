#pragma once

#include <atomic>
#include <cstdint>
#include <thread>
#include <cassert>

// NOTE: default behavior preserves the existing custom spin lock implementation.
// Enable this macro to switch SpinLock to POSIX pthread spin lock.
// #define USE_POSIX_SPINLOCK
// #undef USE_POSIX_SPINLOCK

#ifdef USE_POSIX_SPINLOCK
#include <cerrno>
#include <pthread.h>

class SpinLock {
public:
  SpinLock() noexcept {
    const int rc = pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
    assert(rc == 0);
    initialized_ = (rc == 0);
  }

  ~SpinLock() noexcept {
    if (initialized_) {
      const int rc = pthread_spin_destroy(&lock_);
      assert(rc == 0);
      (void)rc;
    }
  }

  SpinLock(const SpinLock&) = delete;
  SpinLock& operator=(const SpinLock&) = delete;

  void lock() noexcept {
    assert(initialized_);
    const int rc = pthread_spin_lock(&lock_);
    assert(rc == 0);
    (void)rc;
  }

  bool try_lock() noexcept {
    assert(initialized_);
    const int rc = pthread_spin_trylock(&lock_);
    if (rc == 0) {
      return true;
    }
    if (rc == EBUSY) {
      return false;
    }
    assert(false);
    return false;
  }

  void unlock() noexcept {
    assert(initialized_);
    const int rc = pthread_spin_unlock(&lock_);
    assert(rc == 0);
    (void)rc;
  }

private:
  pthread_spinlock_t lock_{};
  bool initialized_{false};
};

#else

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#include <immintrin.h> // _mm_pause
#endif

class SpinLock {

public:
  SpinLock() noexcept : flag_(ATOMIC_FLAG_INIT) {}

  SpinLock(const SpinLock&) = delete;
  SpinLock& operator=(const SpinLock&) = delete;

  void lock() noexcept {
    if (!flag_.test_and_set(std::memory_order_acquire))
      return;

    // Contended: exponential back-off with cpu pause.
    std::uint32_t spins = 1;
    while (true) {
      for (std::uint32_t i = 0; i < spins; ++i) {
        cpu_relax();
        if (!flag_.test_and_set(std::memory_order_acquire))
          return;
      }
      if (spins < kMaxSpins) {
        spins <<= 1;
      } else {
        std::this_thread::yield();
      }
    }
  }

  // Try to acquire the lock once; return true on success.
  bool try_lock() noexcept {
    return !flag_.test_and_set(std::memory_order_acquire);
  }

  // Release the lock.
  void unlock() noexcept {
    flag_.clear(std::memory_order_release);
  }

private:
  static inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    _mm_pause(); // mapped to PAUSE (aka REP NOP) on x86
#elif defined(__aarch64__) || defined(__arm__)
    asm volatile("yield" ::: "memory");
#else
    // Fallback: no-op.
#endif
  }

  static constexpr std::uint32_t kMaxSpins = 64;
  std::atomic_flag flag_;
};

#endif
