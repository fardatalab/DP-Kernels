// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <atomic>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>

#include "file.h"

#include "core/auto_ptr.h"
#include "core/faster.h"
#include "device/null_disk.h"
#include <cstdlib>
#include <winsock2.h>
#include <iostream>
#include <thread>
#include <string>
#include <string.h>
#include <windows.h>
#include <tchar.h>
#include <random>

#include "Common/Include/Config.h"
#include "Common/Include/LatencyHelpers.h"

#pragma comment(lib,"ws2_32.lib")
#pragma warning(disable: 6385)
#pragma warning(disable: 6386)
#pragma warning(disable: 4996) 
#pragma warning(disable: 4267)

using std::srand;
using std::rand;
using std::cout;
using std::endl;
using std::thread;

const int RAND_SEED = 0;

using namespace std::chrono_literals;
using namespace FASTER::core;

/// Basic YCSB benchmark.

enum class Op : uint8_t {
  Insert = 0,
  Read = 1,
  Upsert = 2,
  Scan = 3,
  ReadModifyWrite = 4,
};

enum class Workload {
  A_50_50 = 0,
  RMW_100 = 1,
  A_100_0 = 2,
};

static constexpr uint64_t kInitCount = 250000000;
static constexpr uint64_t kTxnCount = 1000000000;
static constexpr uint64_t kChunkSize =1000;
static constexpr uint64_t kRefreshInterval = 64;
static constexpr uint64_t kCompletePendingInterval = 1600;

static_assert(kInitCount % kChunkSize == 0, "kInitCount % kChunkSize != 0");
static_assert(kTxnCount % kChunkSize == 0, "kTxnCount % kChunkSize != 0");
static_assert(kCompletePendingInterval % kRefreshInterval == 0,
              "kCompletePendingInterval % kRefreshInterval != 0");

static constexpr uint64_t kNanosPerSecond = 1000000000;

static constexpr uint64_t kMaxKey = 268435456;
static constexpr uint64_t kRunSeconds = 20;
static constexpr uint64_t kCheckpointSeconds = 0;

aligned_unique_ptr_t<uint64_t> init_keys_;
aligned_unique_ptr_t<uint64_t> txn_keys_;
std::atomic<uint64_t> idx_{ 0 };
std::atomic<bool> done_{ false };
std::atomic<uint64_t> total_duration_{ 0 };
std::atomic<uint64_t> total_reads_done_{ 0 };
std::atomic<uint64_t> total_writes_done_{ 0 };
// global variable for server part
atomic_uint64_t* msgProcessedCount;  // of size [no. connections]
double** readLatencies;  // of size [no. connections][readNum]
double** sendBackLatencies;  // of size [no. connections][readNum]

Statistics* allClientsReadLatencyStats;
Percentiles* allClientsReadLatencyPercentiles;
Statistics* allClientsSendBackStats;
Percentiles* allClientsSendBackPercentiles;
SOCKET listenSocket;
SOCKET* clientSocket;

class ReadContext;
class UpsertContext;
class RmwContext;

/// This benchmark stores 8-byte keys in key-value store.
class Key {
 public:
  Key(uint64_t key)
    : key_{ key } {
  }

  /// Methods and operators required by the (implicit) interface:
  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(Key));
  }
  inline KeyHash GetHash() const {
    return KeyHash{ Utility::GetHashCode(key_) };
  }

  /// Comparison operators.
  inline bool operator==(const Key& other) const {
    return key_ == other.key_;
  }
  inline bool operator!=(const Key& other) const {
    return key_ != other.key_;
  }

 private:
  uint64_t key_;
};

/// This benchmark stores an 8-byte value in the key-value store.
class Value {
 public:
  Value()
    : value_{ 0 } {
  }

  Value(const Value& other)
    : value_{ other.value_ } {
  }

  Value(uint64_t value)
    : value_{ value } {
  }

  inline static constexpr uint32_t size() {
    return static_cast<uint32_t>(sizeof(Value));
  }

  friend class ReadContext;
  friend class UpsertContext;
  friend class RmwContext;

 private:
  union {
    uint64_t value_;
    std::atomic<uint64_t> atomic_value_;
  };
};

/// Class passed to store_t::Read().
class ReadContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  ReadContext(uint64_t key)
    : key_{ key } {
  }

  /// Copy (and deep-copy) constructor.
  ReadContext(const ReadContext& other)
    : key_{ other.key_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }

  // For this benchmark, we don't copy out, so these are no-ops.
  inline void Get(const value_t& value) { }
  inline void GetAtomic(const value_t& value) { }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
};

/// Class passed to store_t::Upsert().
class UpsertContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  UpsertContext(uint64_t key, uint64_t input)
    : key_{ key }
    , input_{ input } {
  }

  /// Copy (and deep-copy) constructor.
  UpsertContext(const UpsertContext& other)
    : key_{ other.key_ }
    , input_{ other.input_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  inline const Key& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }

  /// Non-atomic and atomic Put() methods.
  inline void Put(value_t& value) {
    value.value_ = input_;
  }
  inline bool PutAtomic(value_t& value) {
    value.atomic_value_.store(input_);
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
  uint64_t input_;
};

/// Class passed to store_t::RMW().
class RmwContext : public IAsyncContext {
 public:
  typedef Key key_t;
  typedef Value value_t;

  RmwContext(uint64_t key, uint64_t incr)
    : key_{ key }
    , incr_{ incr } {
  }

  /// Copy (and deep-copy) constructor.
  RmwContext(const RmwContext& other)
    : key_{ other.key_ }
    , incr_{ other.incr_ } {
  }

  /// The implicit and explicit interfaces require a key() accessor.
  const Key& key() const {
    return key_;
  }
  inline static constexpr uint32_t value_size() {
    return sizeof(value_t);
  }
  inline static constexpr uint32_t value_size(const value_t& old_value) {
    return sizeof(value_t);
  }

  /// Initial, non-atomic, and atomic RMW methods.
  inline void RmwInitial(value_t& value) {
    value.value_ = incr_;
  }
  inline void RmwCopy(const value_t& old_value, value_t& value) {
    value.value_ = old_value.value_ + incr_;
  }
  inline bool RmwAtomic(value_t& value) {
    value.atomic_value_.fetch_add(incr_);
    return true;
  }

 protected:
  /// The explicit interface requires a DeepCopy_Internal() implementation.
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

 private:
  Key key_;
  uint64_t incr_;
};

/// Key-value store, specialized to our key and value types.
#ifdef _WIN32
typedef FASTER::environment::ThreadPoolIoHandler handler_t;
#else
typedef FASTER::environment::QueueIoHandler handler_t;
#endif
//typedef FASTER::device::FileSystemDisk<handler_t, 1073741824ull> disk_t;
typedef FASTER::device::DDSDisk disk_t;
using store_t = FasterKv<Key, Value, disk_t>;

inline Op ycsb_a_50_50(std::mt19937& rng) {
  if(rng() % 100 < 50) {
    return Op::Read;
  } else {
    return Op::Upsert;
  }
}

inline Op ycsb_rmw_100(std::mt19937& rng) {
  return Op::ReadModifyWrite;
}

inline Op ycsb_a_100_0(std::mt19937& rng) {
	return Op::Read;
}

/// Affinitize to hardware threads on the same core first, before
/// moving on to the next core.
void SetThreadAffinity(size_t core) {

  // For now, assume 36 cores. (Set this correctly for your test system.)
  constexpr size_t kCoreCount = 36;
#ifdef _WIN32
  HANDLE thread_handle = ::GetCurrentThread();
  GROUP_AFFINITY group;
  group.Group = WORD(core / kCoreCount);
  group.Mask = KAFFINITY(0x1llu << (core - kCoreCount * group.Group));
  ::SetThreadGroupAffinity(thread_handle, &group, nullptr);
#else
  // On our 28-core test system, we see CPU 0, Core 0 assigned to 0, 28;
  //                                    CPU 1, Core 0 assigned to 1, 29; etc.
  cpu_set_t mask;
  CPU_ZERO(&mask);
#ifdef NUMA
  switch(core % 4) {
  case 0:
    // 0 |-> 0
    // 4 |-> 2
    // 8 |-> 4
    core = core / 2;
    break;
  case 1:
    // 1 |-> 28
    // 5 |-> 30
    // 9 |-> 32
    core = kCoreCount + (core - 1) / 2;
    break;
  case 2:
    // 2  |-> 1
    // 6  |-> 3
    // 10 |-> 5
    core = core / 2;
    break;
  case 3:
    // 3  |-> 29
    // 7  |-> 31
    // 11 |-> 33
    core = kCoreCount + (core - 1) / 2;
    break;
  }
#else
  switch(core % 2) {
  case 0:
    // 0 |-> 0
    // 2 |-> 2
    // 4 |-> 4
    core = core;
    break;
  case 1:
    // 1 |-> 28
    // 3 |-> 30
    // 5 |-> 32
    core = (core - 1) + kCoreCount;
    break;
  }
#endif
  CPU_SET(core, &mask);

  ::sched_setaffinity(0, sizeof(mask), &mask);
#endif
}

void load_files(const std::string& load_filename, bool vanilla) {
  //keep this part in server.exe or server mode
  constexpr size_t kFileChunkSize = 131072;

  auto chunk_guard = alloc_aligned<uint64_t>(512, kFileChunkSize);
  uint64_t* chunk = chunk_guard.get();

  FASTER::benchmark::File init_file{ load_filename };

  printf("loading keys from %s into memory...\n", load_filename.c_str());

  init_keys_ = alloc_aligned<uint64_t>(64, kInitCount * sizeof(uint64_t));
  uint64_t count = 0;

  uint64_t offset = 0;
  while(true) {
    uint64_t size = init_file.Read(chunk, kFileChunkSize, offset);
    for(uint64_t idx = 0; idx < size / 8; ++idx) {
      init_keys_.get()[count] = chunk[idx];
      ++count;
			if (count == kInitCount) {
				break;
			}
    }
    if (count == kInitCount) {
      break;
    }
    if(size == kFileChunkSize) {
      offset += kFileChunkSize;
    } else {
      break;
    }
  }
  if(kInitCount != count) {
    printf("Init file load fail!\n");
    exit(1);
  }

  printf("loaded %" PRIu64 " keys.\n", count);



  //we could move this part to client.exe or in client mode
  if (vanilla) {
      printf("generating random txns...\n");

      txn_keys_ = alloc_aligned<uint64_t>(64, kTxnCount * sizeof(uint64_t));

      count = 0;
      offset = 0;

      std::random_device rd{};
      std::mt19937 rng{ rd() };

      while (count != kTxnCount) {
          txn_keys_.get()[count] = init_keys_.get()[rng() % kInitCount];
          ++count;
      }
      printf("generated %" PRIu64 " txns.\n", count);
  }
}

void load_run_file(const std::string& run_filename) {
    constexpr size_t kFileChunkSize = 131072;

    auto chunk_guard = alloc_aligned<uint64_t>(512, kFileChunkSize);
    uint64_t* chunk = chunk_guard.get();
    FASTER::benchmark::File txn_file{ run_filename };

    printf("loading txns from %s into memory...\n", run_filename.c_str());

    txn_keys_ = alloc_aligned<uint64_t>(64, kTxnCount * sizeof(uint64_t));

    uint64_t count = 0;
    uint64_t offset = 0;

    while (true) {
        uint64_t size = txn_file.Read(chunk, kFileChunkSize, offset);
        for (uint64_t idx = 0; idx < size / 8; ++idx) {
            txn_keys_.get()[count] = chunk[idx];
            ++count;
        }
        if (size == kFileChunkSize) {
            offset += kFileChunkSize;
        }
        else {
            break;
        }
    }
    if (kTxnCount != count) {
        printf("Txn file load fail!\n");
        exit(1);
    }
    printf("loaded %" PRIu64 " txns.\n", count);
}

//put the code for setting the connection in the server part here
void thread_setup_store(store_t* store, size_t thread_idx) {
  auto callback = [](IAsyncContext* ctxt, Status result) {
    assert(result == Status::Ok);
  };

  SetThreadAffinity(thread_idx);

  Guid guid = store->StartSession();

  uint64_t value = 42;
  for(uint64_t chunk_idx = idx_.fetch_add(kChunkSize); chunk_idx < kInitCount;
      chunk_idx = idx_.fetch_add(kChunkSize)) {
    for(uint64_t idx = chunk_idx; idx < chunk_idx + kChunkSize; ++idx) {
      if(idx % kRefreshInterval == 0) {
        store->Refresh();
        if(idx % kCompletePendingInterval == 0) {
          store->CompletePending(false);
        }
      }

      UpsertContext context{ init_keys_.get()[idx], value };
      store->Upsert(context, callback, 1);
    }
  }

  store->CompletePending(true);
  store->StopSession();
}

void setup_store(store_t* store, size_t num_threads) {
  idx_ = 0;
  std::deque<std::thread> threads;
  for(size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
    threads.emplace_back(&thread_setup_store, store, thread_idx);
  }
  for(auto& thread : threads) {
    thread.join();
  }

  init_keys_.reset();

  printf("Finished populating store: contains ?? elements.\n");
}


static std::atomic<int64_t> async_reads_done{ 0 };
static std::atomic<int64_t> async_writes_done{ 0 };

template <Op(*FN)(std::mt19937&)>
void thread_run_benchmark(store_t* store, size_t thread_idx, int BatchSize, int QueueDepth, uint64_t ReadNum) {
  SetThreadAffinity(thread_idx);

  std::random_device rd{};
  std::mt19937 rng{ rd() };

  auto start_time = std::chrono::high_resolution_clock::now();

  uint64_t upsert_value = 0;
  int64_t reads_done = 0;
  int64_t writes_done = 0;
  int iResult;
  // Receive the first message
  int msgSize = 0;
  iResult = 0;
  iResult = recv(clientSocket[thread_idx], (char*)(&msgSize), sizeof(int), 0);
  if (iResult == SOCKET_ERROR || iResult != sizeof(int)) {
      cout << "Error receiving the first message: " << WSAGetLastError() << endl;
      closesocket(clientSocket[thread_idx]);
      WSACleanup();
      return;
  }
  int mbatchSize = msgSize / sizeof(MessageHeader);
  if (mbatchSize != BatchSize) {
      cerr << "ERROR: batch size MISMATCH on server and client" << endl;
  }
  int TotalRecvSize = sizeof(MessageHeader) * BatchSize;
  int TotalSendSize = (sizeof(MessageHeader) + sizeof(uint64_t)) * BatchSize;
  char* recvBuffer = new char[TotalRecvSize];
  char* sendBuffer = new char[TotalSendSize];
  memset(recvBuffer, 0, TotalRecvSize);
  memset(sendBuffer, 0, TotalSendSize);

  int oneReceive = 0;
  int remainingBytesToReceive = TotalRecvSize;

  int oneSend = 0;
  int remainingBytesToSend = TotalSendSize;
  int actualBytesToSend;
  int tcpNum = 0;
  Guid guid = store->StartSession();
  cout << "finished setup steps" << endl;
  auto start = std::chrono::high_resolution_clock::now();
  auto current_time = start;
  bool over = false;
  std::chrono::nanoseconds dur;
  int loopnum = 0;
  while(!over) {
    //cout << "loop " << loopnum << endl;
    uint64_t chunk_idx = idx_.fetch_add(kChunkSize);
    while(chunk_idx >= kTxnCount) {
      if(chunk_idx == kTxnCount) {
        idx_ = 0;
      }
      chunk_idx = idx_.fetch_add(kChunkSize);
    }
    for(uint64_t idx = chunk_idx; idx < chunk_idx + kChunkSize; idx+=BatchSize) {
       // cout << "idx: " << idx << "chunk_idx + kChunkSize: "<< chunk_idx + kChunkSize << endl;
      if(idx % kRefreshInterval == 0) {
        store->Refresh();
        if(idx % kCompletePendingInterval == 0) {
          store->CompletePending(false);
        }
      }
      //cout << "Thread " << thread_idx << "start to receive " << tcpNum + 1 << "message" << endl;
      while (oneReceive < TotalRecvSize) {
          //cout << "receiving message..." << endl;
          iResult = recv(clientSocket[thread_idx], recvBuffer + oneReceive, remainingBytesToReceive, 0);
          if (iResult == SOCKET_ERROR) {
              if ((int64_t)msgProcessedCount[thread_idx] != ReadNum) {
                  cout << "Error receiving message: " << WSAGetLastError() << endl;
                  cout << "msgProcessedCount: " << (int64_t)msgProcessedCount[thread_idx] << endl;
              }
              else {
                  cout << "recv from client but client is finished and closed (conn rest)" << endl;
              }
              over = true;
              break;
          }
          else if (iResult == 0) {
              cout << "client closed socket" << endl;
              over = true;
              break;
          }
          oneReceive += iResult;
          remainingBytesToReceive = TotalRecvSize - oneReceive;
      }
      if (over) {
          break;
      }
      //cout << "Thread " << thread_idx << "received " << tcpNum + 1 << "message" << endl;
      oneReceive = 0;
      remainingBytesToReceive = TotalRecvSize;
      MessageHeader* requestMsg;
      for (int i = 0; i < BatchSize; i++) {
          requestMsg = (MessageHeader*)(recvBuffer + i * sizeof(MessageHeader));
          switch (requestMsg->Operation) {
          case 0: //0
          case 2: { //2
              auto callback = [](IAsyncContext* ctxt, Status result) {
                  CallbackContext<UpsertContext> context{ ctxt };
              };

              UpsertContext context{ requestMsg->Key, upsert_value };
              Status result = store->Upsert(context, callback, 1);
              ++writes_done;
              break;
          }
          case 3: //3
              printf("Scan currently not supported!\n");
              exit(1);
              break;
          case 1: { //1
              auto callback = [](IAsyncContext* ctxt, Status result) {
                  CallbackContext<ReadContext> context{ ctxt };
              };
              ReadContext context{ requestMsg->Key };
              Status result = store->Read(context, callback, 1);
              ++reads_done;
              break;
          }
          case 4: //4
              auto callback = [](IAsyncContext* ctxt, Status result) {
                  CallbackContext<RmwContext> context{ ctxt };
              };

              RmwContext context{ requestMsg->Key, 5 };
              Status result = store->Rmw(context, callback, 1);
              if (result == Status::Ok) {
                  ++writes_done;
              }
              break;
          }
          memcpy(sendBuffer + (i * (sizeof(MessageHeader) + sizeof(uint64_t))), recvBuffer + i * sizeof(MessageHeader), sizeof(MessageHeader));
      }
      tcpNum += 1;
      //cout << "Thread " << thread_idx << "start to send " << tcpNum << "message" << endl;
      while (oneSend < TotalSendSize) {
        actualBytesToSend = remainingBytesToSend > SEND_PACKET_SIZE ? SEND_PACKET_SIZE : remainingBytesToSend;
          iResult = send(clientSocket[thread_idx], sendBuffer + oneSend, actualBytesToSend, 0);
          if (iResult == SOCKET_ERROR) {
              cerr << "Error sending response: " << WSAGetLastError() << endl;
              closesocket(clientSocket[thread_idx]);
              WSACleanup();
              over = true;
              break;
          }
          oneSend += iResult;
          remainingBytesToSend = TotalSendSize - oneSend;
      }
      //cout << "Thread " << thread_idx << "sent " << tcpNum << "message" << endl;
      oneSend = 0;
      remainingBytesToSend = TotalSendSize;
      if (over) {
          break;
      }
      /*
      if (tcpNum == ReadNum / BatchSize) {
          cout << "over = true" << endl;
          over = true;
          break;
      }*/
      current_time = std::chrono::high_resolution_clock::now();
      /*if (!(current_time - start < std::chrono::seconds(kRunSeconds))) {
          over = true;
          break;
      }*/
    }
    loopnum += 1;
  }
  dur = current_time - start;
  cout << "dur: " << (double)dur.count() / kNanosPerSecond << endl;
  store->CompletePending(true);
  printf("complete pending finished\n");
  store->StopSession();

  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::nanoseconds duration = end_time - start_time;
  total_duration_ += duration.count();
  total_reads_done_ += reads_done;
  total_writes_done_ += writes_done;
  printf("Finished thread %" PRIu64 " : %" PRIu64 " reads, %" PRIu64 " writes, in %.2f seconds.\n",
         thread_idx, reads_done, writes_done, (double)duration.count() / kNanosPerSecond);
  // Close the socket for this client
  int ret = closesocket(clientSocket[thread_idx]);
  if (ret) {
      cout << "closing socket err: " << WSAGetLastError() << endl;;
  }
  else {
      cout << "Thread " << thread_idx << " closing client socket completed" << endl;
  }
}
template <Op(*FN)(std::mt19937&)>
void thread_run_benchmark_vanilla(store_t* store, size_t thread_idx, int BatchSize, int QueueDepth, uint64_t ReadNum) {
    SetThreadAffinity(thread_idx);

    std::random_device rd{};
    std::mt19937 rng{ rd() };

    auto start_time = std::chrono::high_resolution_clock::now();

    uint64_t upsert_value = 0;
    int64_t reads_done = 0;
    int64_t writes_done = 0;
    Guid guid = store->StartSession();
    while (!done_) {
        uint64_t chunk_idx = idx_.fetch_add(kChunkSize);
        while (chunk_idx >= kTxnCount) {
            if (chunk_idx == kTxnCount) {
                idx_ = 0;
            }
            chunk_idx = idx_.fetch_add(kChunkSize);
        }
        for (uint64_t idx = chunk_idx; idx < chunk_idx + kChunkSize; idx += BatchSize) {
            if (idx % kRefreshInterval == 0) {
                store->Refresh();
                if (idx % kCompletePendingInterval == 0) {
                    store->CompletePending(false);
                }
            }
                switch (FN(rng)) {
                case Op::Insert : //0
                    case Op::Upsert: { //2
                    auto callback = [](IAsyncContext* ctxt, Status result) {
                        CallbackContext<UpsertContext> context{ ctxt };
                    };

                    UpsertContext context{ txn_keys_.get()[idx], upsert_value };
                    Status result = store->Upsert(context, callback, 1);
                    ++writes_done;
                    break;
                }
                    case Op::Scan: //3
                    printf("Scan currently not supported!\n");
                    exit(1);
                    break;
                    case Op::Read:{
                        auto callback = [](IAsyncContext* ctxt, Status result) {
                            CallbackContext<ReadContext> context{ ctxt };
                        };
                        ReadContext context{ txn_keys_.get()[idx] };
                        Status result = store->Read(context, callback, 1);
                        ++reads_done;
                        break;
                }
                    case Op::ReadModifyWrite: //4
                    auto callback = [](IAsyncContext* ctxt, Status result) {
                        CallbackContext<RmwContext> context{ ctxt };
                    };

                    RmwContext context{ txn_keys_.get()[idx], 5 };
                    Status result = store->Rmw(context, callback, 1);
                    if (result == Status::Ok) {
                        ++writes_done;
                    }
                    break;
                }
        }
    }
    store->CompletePending(true);
    store->StopSession();

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::nanoseconds duration = end_time - start_time;
    total_duration_ += duration.count();
    total_reads_done_ += reads_done;
    total_writes_done_ += writes_done;
    printf("Finished thread %" PRIu64 " : %" PRIu64 " reads, %" PRIu64 " writes, in %.2f seconds.\n",
        thread_idx, reads_done, writes_done, (double)duration.count() / kNanosPerSecond);
    // Close the socket for this client
    int ret = closesocket(clientSocket[thread_idx]);
    if (ret) {
        cout << "closing socket err: " << WSAGetLastError() << endl;;
    }
    else {
        // cout << "Thread " << clientIndex << " closing client socket completed" << endl;
    }
}

template <Op(*FN)(std::mt19937&)>
void run_benchmark(store_t* store, size_t num_threads, int BatchSize, int QueueDepth, uint64_t ReadNum, bool vanilla) {
  idx_ = 0;
  total_duration_ = 0;
  total_reads_done_ = 0;
  total_writes_done_ = 0;
  done_ = false;
  std::deque<std::thread> threads;
  if (vanilla) {
      for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
          threads.emplace_back(&thread_run_benchmark_vanilla<FN>, store, thread_idx, BatchSize, QueueDepth, ReadNum);
      }
  }
  else {
      for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
          threads.emplace_back(&thread_run_benchmark<FN>, store, thread_idx, BatchSize, QueueDepth, ReadNum);
      }
  }
 

  static std::atomic<uint64_t> num_checkpoints;
  num_checkpoints = 0;

  /*if(kCheckpointSeconds == 0) {
    std::this_thread::sleep_for(std::chrono::seconds(kRunSeconds));
  }
  else {
    auto callback = [](Status result, uint64_t persistent_serial_num) {
      if(result != Status::Ok) {
        printf("Thread %" PRIu32 " reports checkpoint failed.\n",
               Thread::id());
      } else {
        ++num_checkpoints;
      }
    };

    auto start_time = std::chrono::high_resolution_clock::now();
    auto last_checkpoint_time = start_time;
    auto current_time = start_time;

    uint64_t checkpoint_num = 0;

    while(current_time - start_time < std::chrono::seconds(kRunSeconds)) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      current_time = std::chrono::high_resolution_clock::now();
      if(current_time - last_checkpoint_time >= std::chrono::seconds(kCheckpointSeconds)) {
        Guid token;
        bool success = store->Checkpoint(nullptr, callback, token);
        if(success) {
          printf("Starting checkpoint %" PRIu64 ".\n", checkpoint_num);
          ++checkpoint_num;
        } else {
          printf("Failed to start checkpoint.\n");
        }
        last_checkpoint_time = current_time;
      }
    }
  }*/

  done_ = true;

  for(auto& thread : threads) {
    thread.join();
  }

  printf("Finished benchmark: %" PRIu64 " thread checkpoints completed;  %.2f ops/second/thread\n",
         num_checkpoints.load(),
         ((double)total_reads_done_ + (double)total_writes_done_) / ((double)total_duration_ /
             kNanosPerSecond));
  // Close the listen socket and cleanup
  int ret = closesocket(listenSocket);
  if (ret) {
      cout << "closing listen socket err: " << WSAGetLastError() << endl;;
  }
  WSACleanup();

}

void run(Workload workload, size_t num_threads, int BatchSize, int QueueDepth, uint64_t ReadNum, bool vanilla) {
  // FASTER store has a hash table with approx. kInitCount / 2 entries and a log of size 16 GB
  size_t init_size = next_power_of_two(kInitCount / 2);
  store_t store{ init_size, 536870912, "E:\\log", 0.5 };

  printf("Populating the store...\n");

  setup_store(&store, num_threads);

  store.DumpDistribution();

  printf("Running benchmark on %" PRIu64 " threads...\n", num_threads);
  if (!vanilla) {
      int batchNum = (int)ReadNum / BatchSize;
      // Initialize Winsock
      WSADATA wsaData;
      int iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
      if (iResult != 0) {
          cout << "WSAStartup failed with error: " << iResult << endl;
          return;
      }

      // Create a socket for the server
      listenSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
      if (listenSocket == INVALID_SOCKET) {
          cout << "Error creating socket: " << WSAGetLastError() << endl;
          WSACleanup();
          return;
      }

      // Set up the address of the server
      sockaddr_in serverAddr;
      serverAddr.sin_family = AF_INET;
      serverAddr.sin_port = htons(SERVER_PORT);
      serverAddr.sin_addr.s_addr = INADDR_ANY;
      // Bind the socket to the address
      iResult = ::bind(listenSocket, (SOCKADDR*)&serverAddr, sizeof(serverAddr));
      if (iResult == SOCKET_ERROR) {
          cout << "Error binding socket: " << WSAGetLastError() << endl;
          closesocket(listenSocket);
          WSACleanup();
          return;
      }
      int clientIndex = 0;
      clientSocket = new SOCKET[num_threads];
      while (clientIndex != num_threads) {
          cout << "Waiting for clients..." << endl;

          // Listen for incoming connections
          iResult = listen(listenSocket, SOMAXCONN);
          if (iResult == SOCKET_ERROR) {
              cout << "Error listening for connections: " << WSAGetLastError() << endl;
              closesocket(listenSocket);
              WSACleanup();
              return;
          }

          // Accept incoming connections and handle them
          sockaddr_in clientAddr;
          int clientAddrSize = sizeof(clientAddr);

          // Accept connection
          clientSocket[clientIndex] = accept(listenSocket, (SOCKADDR*)&clientAddr, &clientAddrSize);

          if (clientSocket[clientIndex] == INVALID_SOCKET) {
              cout << "Error accepting connection: " << WSAGetLastError() << endl;
              closesocket(listenSocket);
              WSACleanup();
              return;
          }
          cout << "accepted client " << clientIndex << endl;
          clientIndex++;
      }
      cout << "all clients connected, waiting for them to finish" << endl;
  }
  switch(workload) {
  case Workload::A_50_50:
    run_benchmark<ycsb_a_50_50>(&store, num_threads, BatchSize, QueueDepth, ReadNum, vanilla);
    break;
  case Workload::RMW_100:
    run_benchmark<ycsb_rmw_100>(&store, num_threads, BatchSize, QueueDepth, ReadNum, vanilla);
    break;
  case Workload::A_100_0:
    run_benchmark<ycsb_a_100_0>(&store, num_threads, BatchSize, QueueDepth, ReadNum, vanilla);
    break;
  default:
    printf("Unknown workload!\n");
    exit(1);
  }
}
void ThreadFunc(
    int ThreadNum,
    int BatchSize,
    const uint64_t ReadNum,
    const int QueueDepth,
    SOCKET* ClientSocket,
    char* SendBuffer,
    char* RecvBuffer,
    uint64_t* Keys,
    double* latencies,
    std::chrono::steady_clock::time_point* StartTime,
    std::chrono::steady_clock::time_point* EndTime,
    uint64_t* BytesCompleted
) {
    int iResult;
    const int HeaderSize = sizeof(MessageHeader);
    const int firstMsg = HeaderSize * BatchSize;
    const int TOTAL_MESSAGE_SIZE = BatchSize * (HeaderSize + sizeof(uint64_t));
    iResult = send(*ClientSocket, (const char*)&firstMsg, sizeof(int), 0);
    if (iResult == SOCKET_ERROR || iResult != sizeof(int)) {
        cout << "Error sending the first message: " << WSAGetLastError() << endl;
        closesocket(*ClientSocket);
        WSACleanup();
        return;
    }

    uint64_t bytesCompleted = 0;
    uint64_t bytesSent = 0;

    int oneSend = 0;
    int remainingBytesToSend = firstMsg;
    int actualBytesToSend;
    uint64_t msgIndex = 0;

    MessageHeader* hdr;
    *StartTime = high_resolution_clock::now();
    auto start = high_resolution_clock::now();
    auto current_time = start;
    bool over = false;
    for (uint64_t q = 0; q != QueueDepth; q++) {
        for (int i = 0; i != BatchSize; i++) {
            hdr = (MessageHeader*)(SendBuffer + i * HeaderSize);
            hdr->TimeSend = high_resolution_clock::now().time_since_epoch().count();
            hdr->BatchId = (uint16_t)q;
            hdr->Key = Keys[msgIndex * BatchSize + i];
            hdr->Operation = (uint8_t)1;

        }
        msgIndex++;
        while (oneSend < firstMsg) {
            actualBytesToSend = remainingBytesToSend > SEND_PACKET_SIZE ? SEND_PACKET_SIZE : remainingBytesToSend;
            iResult = send(*ClientSocket, SendBuffer + oneSend, actualBytesToSend, 0);
            if (iResult == SOCKET_ERROR) {
                cout << "Error sending message: " << WSAGetLastError() << endl;
                closesocket(*ClientSocket);
                WSACleanup();
                return;
            }

            oneSend += iResult;
            remainingBytesToSend = firstMsg - oneSend;
            //cout << "Thread " << ThreadNum << "sent " << msgIndex << "message" << endl;
        }

        bytesSent += oneSend;

        oneSend = 0;
        remainingBytesToSend = firstMsg;
    }

    int oneReceive = 0;
    int remainingBytesToReceive = TOTAL_MESSAGE_SIZE;
    uint64_t RespIndex = 0;

    while (true and !over) {//current_time - start < std::chrono::seconds(kRunSeconds)
        //
        // receive one message
        //
        //
        while (oneReceive < TOTAL_MESSAGE_SIZE) {
            iResult = recv(*ClientSocket, RecvBuffer + oneReceive, remainingBytesToReceive, 0);
            if (iResult == SOCKET_ERROR) {
                cout << "Error receiving message: " << WSAGetLastError() << endl;
                closesocket(*ClientSocket);
                WSACleanup();
                over = true;
                break;
            }

            oneReceive += iResult;
            remainingBytesToReceive = TOTAL_MESSAGE_SIZE - oneReceive;
        }
        //
        // measure latency
        //
        //
        long long diff;
        uint16_t BatchID;
        for (int i = 0; i != BatchSize; i++) {
            //diff returns the time in nanosecond, here we divide it by 1000 to get time in microsecond
            hdr = (MessageHeader*)(RecvBuffer + i * (HeaderSize + sizeof(uint64_t)));
            diff = high_resolution_clock::now().time_since_epoch().count() - hdr->TimeSend;
            BatchID = hdr->BatchId;
            if (BatchID > QueueDepth) {
                cerr << "ERROR: got wrong batchId from server: " << BatchID << endl;
            }
            latencies[RespIndex * BatchSize + i] = (double)(diff / 1000);
        }
        bytesCompleted += oneReceive;
        RespIndex++;
        oneReceive = 0;
        remainingBytesToReceive = TOTAL_MESSAGE_SIZE;
        //cout << "Thread " << ThreadNum << "received " << RespIndex << "message" << endl;

        if (RespIndex == ReadNum / BatchSize) {
            break;
        }

        //
        // send another message
        //
        //
        if (msgIndex < ReadNum / BatchSize) {
            for (int i = 0; i != BatchSize; i++) {
                hdr = (MessageHeader*)(SendBuffer + i * HeaderSize);
                hdr->TimeSend = high_resolution_clock::now().time_since_epoch().count();
                hdr->BatchId = BatchID;
                hdr->Key = Keys[msgIndex * BatchSize + i];
                hdr->Operation = (uint8_t)1;

            }
            msgIndex++;
            while (oneSend < firstMsg) {
                actualBytesToSend = remainingBytesToSend > SEND_PACKET_SIZE ? SEND_PACKET_SIZE : remainingBytesToSend;
                iResult = send(*ClientSocket, SendBuffer + oneSend, actualBytesToSend, 0);
                if (iResult == SOCKET_ERROR) {
                    std::cout << "Error sending message: " << WSAGetLastError() << endl;
                    closesocket(*ClientSocket);
                    WSACleanup();
                    over = true;
                    break;
                }

                oneSend += iResult;
                remainingBytesToSend = firstMsg - oneSend;
            }

            bytesSent += oneSend;

            oneSend = 0;
            remainingBytesToSend = firstMsg;
            //cout << "Thread " << ThreadNum << "sent " << msgIndex << "message" << endl;
        }
        current_time = high_resolution_clock::now();
    }

    *EndTime = high_resolution_clock::now();
    *BytesCompleted = bytesCompleted;
}

int RunClientForThroughput(
    int BatchSize,
    int QueueDepth,
    uint64_t ReadNum,
    int PortBase,
    int NumConnections
    ) {
    //
    // Initialize Winsock
    //
    //
    WSADATA wsaData;
    int iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (iResult != 0) {
        cout << "WSAStartup failed with error: " << iResult << endl;
        return 1;
    }

    //
    // Create sockets for the client
    //
    //
    SOCKET* clientSockets = new SOCKET[NumConnections];
    sockaddr_in* clientAddrs = new sockaddr_in[NumConnections];

    for (int i = 0; i != NumConnections; i++) {
        clientSockets[i] = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (clientSockets[i] == INVALID_SOCKET) {
            cout << "Error creating socket: " << WSAGetLastError() << endl;
            WSACleanup();
            return 1;
        }

        clientAddrs[i].sin_family = AF_INET;
        clientAddrs[i].sin_port = htons(PortBase + i);
        clientAddrs[i].sin_addr.s_addr = inet_addr(CLIENT_IP);

        //
        // set TCP_NODELAY for sockets
        //
        //
        int noDelay = 1;
        iResult = setsockopt(clientSockets[i], IPPROTO_TCP, TCP_NODELAY, (char*) &noDelay, sizeof(noDelay));
        if (iResult) {
            printf("set socket %d TCP_NODELAY failed: %d\n", i, WSAGetLastError());
        }
    }

    // Set up the address of the server
    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(SERVER_PORT);
    serverAddr.sin_addr.s_addr = inet_addr(SERVER_IP);

    cout << "Connecting to the server..." << endl;

    //
    // Connect to the server
    //
    //
    for (int i = NumConnections - 1; i >= 0; i--) {
        cout << "trying connection " << i << endl;
        iResult = connect(clientSockets[i], (SOCKADDR*)&serverAddr, sizeof(serverAddr));
        if (iResult == SOCKET_ERROR) {
            cout << "Unable to connect to server: " << WSAGetLastError() << endl;
            closesocket(clientSockets[i]);
            WSACleanup();
            return 1;
        }
        cout << "Connection #" << i << " has been connected" << endl;
    }

    cout << "Preparing message buffers..." << endl;

    // Send message to the server and receive response
    const int HeaderSize = sizeof(MessageHeader);


    char** sendBuffers = new char* [NumConnections];
    char** recvBuffers = new char* [NumConnections];
    int TOTAL_MESSAGE_SIZE = (HeaderSize + sizeof(uint64_t)) * BatchSize;
    for (int i = 0; i != NumConnections; i++) {
        sendBuffers[i] = new char[HeaderSize * BatchSize];
        memset(sendBuffers[i], 0, HeaderSize * BatchSize);
        recvBuffers[i] = new char[TOTAL_MESSAGE_SIZE];
        memset(recvBuffers[i], 0, TOTAL_MESSAGE_SIZE);
    }

    //
    //Prepare key for each request
    //
    //
    std::random_device rd;
    std::mt19937_64 eng(rd());
    std::uniform_int_distribution<uint64_t> distr;

    uint64_t** Key = new uint64_t * [NumConnections];
    double** latencies = new double* [NumConnections];
    for (int i = 0; i != NumConnections; i++) {
        Key[i] = new uint64_t[ReadNum];
        latencies[i] = new double[ReadNum];
        for (int l = 0; l != ReadNum; l++) {
            Key[i][l] = txn_keys_.get()[i* ReadNum + l];
            latencies[i][l] = 0;
        }
    }

    cout << "Starting measurement..." << endl;
    std::chrono::steady_clock::time_point* startTimes = new std::chrono::steady_clock::time_point[NumConnections];
    std::chrono::steady_clock::time_point* endTimes = new std::chrono::steady_clock::time_point[NumConnections];
    uint64_t* bytesCompleted = new uint64_t[NumConnections];

    thread** ioThreads = new thread * [NumConnections];
    for (int i = 0; i != NumConnections; i++) {
        thread* worker = new thread([i, BatchSize, ReadNum, QueueDepth, clientSockets, sendBuffers, recvBuffers, Key, latencies, startTimes, endTimes, bytesCompleted]
            {
                ThreadFunc(i, BatchSize, ReadNum, QueueDepth, &clientSockets[i], sendBuffers[i], recvBuffers[i], Key[i], latencies[i], &startTimes[i], &endTimes[i], &bytesCompleted[i]);
            });
        ioThreads[i] = worker;
    }

    for (int i = 0; i != NumConnections; i++) {
        ioThreads[i]->join();
    }

    cout << "Processing results..." << endl;

    //
    // Calculate throughput
    //
    //
    std::chrono::steady_clock::time_point aggStartTime = startTimes[0], aggEndTime = endTimes[0];
    uint64_t aggBytesCompleted = 0;
    double P50Sum = 0;
    double P90Sum = 0;
    double P99Sum = 0;
    Statistics LatencyStats;
    Percentiles PercentileStats;
    auto* durations = new long long[NumConnections];
    for (int i = 0; i != NumConnections; i++) {
        if (aggStartTime > startTimes[i]) {
            aggStartTime = startTimes[i];
        }
        if (aggEndTime < endTimes[i]) {
            aggEndTime = endTimes[i];
        }
        durations[i] = duration_cast<microseconds>(endTimes[i] - startTimes[i]).count();

        aggBytesCompleted += bytesCompleted[i];
    }
    auto duration = duration_cast<microseconds>(aggEndTime - aggStartTime).count(); // duration in microseconds
    double throughput = (double)aggBytesCompleted / (double)duration * 1000000.0f / (1024.0 * 1024.0 * 1024.0); // in giga bytes per second
    for (int i = 0; i != NumConnections; i++) {
        GetStatistics(latencies[i], ReadNum, &LatencyStats, &PercentileStats);
        printf(
            "Thread %d: Result for %zu requests (%.2lf seconds): %.2lf RPS, Min: %.2lf, Max: %.2lf, 50th: %.2lf, 90th: %.2lf, 99th: %.2lf, 99.9th: %.2lf, 99.99th: %.2lf, StdErr: %.2lf\n",
            i,
            bytesCompleted[i]/(sizeof(MessageHeader)+sizeof(uint64_t)),
            (durations[i] / 1000000.0),
            (bytesCompleted[i] / (sizeof(MessageHeader) + sizeof(uint64_t)) / (durations[i] / 1000000.0)),
            LatencyStats.Min,
            LatencyStats.Max,
            PercentileStats.P50,
            PercentileStats.P90,
            PercentileStats.P99,
            PercentileStats.P99p9,
            PercentileStats.P99p99,
            LatencyStats.StandardError);
        P50Sum += PercentileStats.P50;
        P90Sum += PercentileStats.P90;
        P99Sum += PercentileStats.P99;
    }


    cout << "Throughput: " << throughput << " GB/s" << endl;
    cout << "IOPS: " << aggBytesCompleted / (sizeof(MessageHeader) + sizeof(uint64_t)) / ((double)duration / 1000000) << "RPS" << endl;
    cout << "Avg P50: " << P50Sum / NumConnections << endl;
    cout << "Avg P90: " << P90Sum / NumConnections << endl;
    cout << "Avg P99: " << P99Sum / NumConnections << endl;

    //
    // Release buffers
    //
    //
    for (int i = 0; i != NumConnections; i++) {
        delete[] sendBuffers[i];
        delete[] recvBuffers[i];
        delete[] Key[i];
        delete[] latencies[i];
    }
    delete[] sendBuffers;
    delete[] recvBuffers;
    delete[] Key;
    delete[] latencies;
    delete[] durations;

    //
    // Close the socket and clean up
    //
    //
    for (int i = 0; i != NumConnections; i++) {
        closesocket(clientSockets[i]);
    }
    delete[] clientSockets;
    WSACleanup();

    return 0;
}

int main(int argc, char* argv[]) {
  constexpr size_t kNumArgs = 10;

  if (argc == 4) {
      //printf("Usage: benchmark.exe <workload> <# thread> <load_filename>\n");
      Workload workload = static_cast<Workload>(std::atol(argv[1]));
      size_t num_threads = ::atol(argv[2]);
      int thread_num = int(num_threads);
      std::string load_filename{ argv[3] };
      load_files(load_filename, true);
      run(workload, num_threads, 1, 1, 1, true);
  }

  if(argc != kNumArgs) {
    printf("Usage: benchmark.exe <workload> <load_filename> <run_filename> <Mode> <# threads> <Batch size> <Queue Depth> <Read Num> <Port>\n");
    exit(0);
  }
  Workload workload = static_cast<Workload>(std::atol(argv[1]));
  std::string load_filename{ argv[2] };
  std::string run_filename{ argv[3] };
  int mode = atol(argv[4]); //1 for server and 0 for client
  size_t num_threads = ::atol(argv[5]);
  int thread_num = int(num_threads);
  int BatchSize = atol(argv[6]);
  int QueueDepth = atol(argv[7]);
  uint64_t ReadNum = atol(argv[8]);
  int PortBase = atol(argv[9]);
  if (mode) {
      load_files(load_filename, false);
      run(workload, num_threads, BatchSize, QueueDepth, ReadNum, false);
  }
  else {
      /*load_files(load_filename, mode);*/
      load_run_file(run_filename);
      do
      {
          cout << '\n' << "Press a Enter to continue..." << endl;
      } while (cin.get() != '\n');
      RunClientForThroughput(BatchSize, QueueDepth, ReadNum, PortBase, thread_num);
  }

  return 0;
}