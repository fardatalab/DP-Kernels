#include "server.hpp"
#include "common.hpp"
#include "helpers.hpp"
#include "status.h"
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>

FasterKv<Key, CompressedValue, Disk> faster_store{
    1 * (1 << 20),       // Hash table size (1M entries)
    1ULL * (1ULL << 30), // In-memory Log size (1 GiB)
    "",                  // Log path must be empty for in-memory store
    1.0,                 // Log Mutable fraction
};

bool read_file(const char *filename, uint8_t **buffer, size_t *buffer_size)
{
    FILE *file = fopen(filename, "rb");
    if (!file)
    {
        perror("Failed to open input file");
        return false;
    }

    // get the file size
    fseek(file, 0, SEEK_END);
    size_t file_size = ftell(file);
    fseek(file, 0, SEEK_SET); // reset the file pointer to the beginning
    // allocate buffer
    *buffer = (uint8_t *)malloc(file_size);
    if (!*buffer)
    {
        perror("Failed to allocate memory for input buffer");
    }

    // read the file into the buffer
    size_t bytes_read = fread(*buffer, 1, file_size, file);
    if (bytes_read < file_size)
    {
        if (ferror(file))
        {
            perror("Failed to read input file");
            free(*buffer);
            fclose(file);
            return false;
        }
    }

    *buffer_size = bytes_read;

    fclose(file);
    return true;
}

void ServerThreadFunc(RDMAConnectionResource &conn, int ThreadNum, int BatchSize, const uint64_t ReadNum,
                      const int QueueDepth)
{
}

int main(int argc, char *argv[])
{
    // read the file
    uint8_t *file_buffer = nullptr;
    size_t buffer_size = 0;
    if (!read_file(INPUT_FILE, &file_buffer, &buffer_size))
    {
        printf("Failed to read input file: %s\n", INPUT_FILE);
        return 1;
    }
    printf("Read %zu bytes from input file: %s\n", buffer_size, INPUT_FILE);

    // start RDMA connections

    // start the FASTER store session
    faster_store.StartSession();
}

int test_main(int argc, char *argv[])
{
    // read input file
    uint8_t *file_buffer = nullptr;
    uint8_t *buffer = (uint8_t *)malloc(COMPRESSED_SIZE);
    memset(buffer, 0, COMPRESSED_SIZE);
    size_t buffer_size = 0;
    if (!read_file(INPUT_FILE, &file_buffer, &buffer_size))
    {
        printf("Failed to read input file: %s\n", INPUT_FILE);
        return 1;
    }
    printf("Read %zu bytes from input file: %s\n", buffer_size, INPUT_FILE);
    // copy the input file data to the buffer
    memcpy(buffer, file_buffer, buffer_size);

    faster_store.StartSession();

    // allocate a buffer for upsert context data
    CompressedValue *compressed_value = (CompressedValue *)malloc(sizeof(CompressedValue));
    memset(compressed_value, 0, sizeof(CompressedValue));
    // in place new the value, this will copy the input data into CompressedValue
    new (compressed_value) CompressedValue(buffer);
    // ctx has a ptr to uc_data
    UpsertContext context{42, compressed_value};

    Status status = faster_store.Upsert(
        context,
        [](IAsyncContext *ctxt, Status result) {
            if (result != Status::Ok)
            {
                printf("Upsert failed with status: %s\n", StatusStr(result));
            }
            else
            {
                printf("Upsert succeeded.\n");
            }
        },
        1);
    if (status != Status::Ok)
    {
        printf("Failed to upsert data: %s\n", StatusStr(status));
        free(buffer);
        free(compressed_value);
        return 1;
    }
    else
    {
        printf("Upsert operation successful\n");
    }
    faster_store.CompletePending(true);

    // do a read
    // uint8_t *read_buffer = (uint8_t *)malloc(CompressedValue::size());
    CompressedValue *compressed_read_value = (CompressedValue *)malloc(sizeof(CompressedValue));
    memset(compressed_read_value, 0, sizeof(CompressedValue));
    printf("compressed_read_value == %p\n", compressed_read_value);
    // new (compressed_read_value) CompressedValue(read_buffer);
    ReadContext read_context{42, compressed_read_value};
    status = faster_store.Read(
        read_context,
        [](IAsyncContext *ctxt, Status result) {
            if (result != Status::Ok)
            {
                printf("Read failed with status: %s\n", StatusStr(result));
            }
            else
            {
                CallbackContext<ReadContext> context{ctxt};
                printf("Read succeeded, key: %lu\n", context->key().key);
                CompressedValue *read_value = context->GetValue();
                printf("read_value == %p\n", read_value);
                printf("Read value: ");
                read_value->value_[COMPRESSED_SIZE - 1] = '\0';
                printf("%s\n", read_value->value_);
            }
        },
        1);
    if (status != Status::Ok)
    {
        printf("Failed to read data: %s\n", StatusStr(status));
        free(buffer);
        free(compressed_value);
        // free(read_buffer);
        free(compressed_read_value);
        return 1;
    }
    else
    {
        printf("Read operation successful\n");
        CompressedValue *read_value = read_context.GetValue();
        printf("read_value == %p\n", read_value);
        printf("Read value: ");
        read_value->value_[COMPRESSED_SIZE - 1] = '\0';
        printf("%s\n", read_value->value_);
        int is_equal = std::memcmp(compressed_value->value_, read_value->value_, COMPRESSED_SIZE - 1) == 0;
        printf("Is read value equal to upserted value? %s\n", is_equal ? "Yes" : "No");
        is_equal = std::memcmp(buffer, read_value->value_, buffer_size) == 0;
        printf("Is read value equal to input file value? %s\n", is_equal ? "Yes" : "No");
    }

    faster_store.StopSession();

    return 0;
}


void run_server(Workload workload, size_t num_threads, int BatchSize, int QueueDepth, uint64_t ReadNum, bool vanilla) {
    // FASTER store has a hash table with approx. kInitCount / 2 entries and a log of size 16 GB
    size_t init_size = next_power_of_two(kInitCount / 2);
    store_t store{ init_size, 536870912, "log", 0.5 };
    setup_store(&store, num_threads);
    printf("Populating the store...\n");
  
    store.DumpDistribution();
  
    std::vecor<RDMAClientResource> clients;
    printf("Running benchmark on %" PRIu64 " threads...\n", num_threads);
  
    rdma_event_channel* ec;
    rdma_cm_id* listen_id;
    if (!vanilla) {
        int batchNum = (int)ReadNum / BatchSize;
  
        // Initialize ec
        ec = rdma_create_event_channel();
        if (!ec) {
            cout << "Error creating RDMA event channel: " << strerror(errno) << endl;
            return;
        }
        if (rdma_create_id(ec, &listen_id, nullptr, RDMA_PS_TCP)) {
            cout << "Error creating RDMA CM ID: " << strerror(errno) << endl;
            rdma_destroy_event_channel(ec);
            return;
        }
  
        // Set up the address of the server
        sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(SERVER_PORT);
        addr.sin_addr.s_addr = INADDR_ANY;
        
        if (rdma_bind_addr(listen_id, (sockaddr*)&addr)) {
          cout << "Error binding RDMA CM ID: " << strerror(errno) << endl;
          rdma_destroy_id(listen_id);
          rdma_destroy_event_channel(ec);
          return;
        }
        if (rdma_listen(listen_id, SOMAXCONN)) {
            cout << "Error listening on RDMA CM ID: " << strerror(errno) << endl;
            rdma_destroy_id(listen_id);
            rdma_destroy_event_channel(ec);
            return;
        }
        std::cout << "Listening for " << num_threads << " clients on port " << SERVER_PORT << "..." << std::endl;
        
        // Bind the socket to the address
        int clientIndex = 0;
        client.resize(num_threads);
        while (clientIndex != num_threads) {
            cout << "Waiting for clients..." << endl;
  
            rdma_cm_event* event = nullptr;
            if (rdma_get_cm_event(ec, &event)) {
                cout << "Error getting RDMA CM event: " << strerror(errno) << endl;
                rdma_destroy_id(listen_id);
                rdma_destroy_event_channel(ec);
                return;
            }
            if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
                cout << "Unexpected RDMA CM event: " << rdma_event_str(event->event) << endl;
                rdma_ack_cm_event(event);
                continue;
            }
            rdma_cm_id* conn_id = event->id;
            rdma_ack_cm_event(event);
  
            RDMAClientResource& res = clients[clientIndex];
            memset(&res, 0, sizeof(RDMAClientResource));
            res.cm_id = conn_id;
  
            if (!setup_rdma_qp_and_buffers(conn_id, res, QueueDepth)) {
              cout << "Failed to set up RDMA QP and buffers for client " << clientIndex << endl;
              rdma_destroy_id(conn_id);
              continue;
            }
  
            rdma_conn_param conn_param = {};
            conn_param.initiator_depth = QueueDepth;
            conn_param.responder_resources = QueueDepth;
            conn_param.rnr_retry_count = 7; // Retry count for RNR errors
  
            cout << "accepted client " << clientIndex << endl;
            clientIndex++;
        }
        cout << "all clients connected, waiting for them to finish" << endl;
    }
    switch(workload) {
    case Workload::A_50_50:
      run_benchmark<ycsb_a_50_50>(&store, clients, num_threads, BatchSize, QueueDepth, ReadNum, vanilla);
      break;
    case Workload::RMW_100:
      run_benchmark<ycsb_rmw_100>(&store, clients, num_threads, BatchSize, QueueDepth, ReadNum, vanilla);
      break;
    case Workload::A_100_0:
      run_benchmark<ycsb_a_100_0>(&store, clients, num_threads, BatchSize, QueueDepth, ReadNum, vanilla);
      break;
    default:
      printf("Unknown workload!\n");
      exit(1);
    }
  
    if (rdma_destroy_id(listen_id)) {
      std::cerr << "Error destroying RDMA listen_id: " << strerror(errno) << std::endl;
    }
    rdma_destroy_event_channel(ec);
}
  
bool setup_rdma_qp_and_buffers(rdma_cm_id* id, RDMAClientResource& conn, int QueueDepth) {
    ibv_cq* cq = ibv_create_cq(id->verbs, QueueDepth*2, nullptr, nullptr, 0);
    if (!cq) {
      cout << "Error creating CQ: " << strerror(errno) << endl;
      return false;
    }
  
    ibv_qp_init_attr qp_attr = {};
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.cap.max_send_wr = QueueDepth;
    qp_attr.cap.max_recv_wr = QueueDepth;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
  
    if (rdma_create_qp(id, nullptr, &qp_attr)) {
      cout << "Error creating QP: " << strerror(errno) << endl;
      ibv_destroy_cq(cq);
      return false;
    }
    resource.qp = id->qp;
    resource.cq = cq;
  
    // allocate & register buffers
    int TotalRecvSize = sizeof(MessageHeader) * BatchSize;
    int TotalSendSize = (sizeof(MessageHeader) + sizeof(uint64_t)) * BatchSize;
    resource.send_buf = (char*)aligned_alloc(4096, TotalSendSize);
    resource.recv_buf = (char*)aligned_alloc(4096, TotalRecvSize);
    if (!resource.send_buf || !resource.recv_buf) {
      cout << "Error allocating buffers: " << strerror(errno) << endl;
      rdma_destroy_qp(id);
      ibv_destroy_cq(cq);
      return false;
    }
    memset(conn.recv_buf, 0, TotalRecvSize);
    memset(conn.send_buf, 0, TotalSendSize);
  
    resource.send_mr = ibv_reg_mr(id->qp->pd, resource.send_buf, TotalSendSize, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    resource.recv_mr = ibv_reg_mr(id->qp->pd, resource.recv_buf, TotalRecvSize, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!resource.send_mr || !resource.recv_mr) {
      cout << "Error registering MR: " << strerror(errno) << endl;
      free(resource.send_buf);
      free(resource.recv_buf);
      rdma_destroy_qp(id);
      ibv_destroy_cq(cq);
      return false;
    }
  
    return true;
  }
  
  template <Op(*FN)(std::mt19937&)>
  void run_benchmark(store_t* store, vector<RDMAClientResource>& clients, size_t num_threads, int BatchSize, int QueueDepth, uint64_t ReadNum, bool vanilla) {
    idx_ = 0;
    total_duration_ = 0;
    total_reads_done_ = 0;
    total_writes_done_ = 0;
    done_ = false;
    std::deque<std::thread> threads;
    if (vanilla) {
        for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
            threads.emplace_back(&thread_run_benchmark_vanilla<FN>, store, ref(clients[thread_idx]), thread_idx, BatchSize, QueueDepth, ReadNum);
        }
    }
    else {
        for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx) {
            threads.emplace_back(&thread_run_benchmark<FN>, store, ref(clients[thread_idx]), thread_idx, BatchSize, QueueDepth, ReadNum);
        }
    }
   
    static std::atomic<uint64_t> num_checkpoints;
    num_checkpoints = 0;
    done_ = true;
    for(auto& thread : threads) {
      thread.join();
    }
  
    printf("Finished benchmark: %" PRIu64 " thread checkpoints completed;  %.2f ops/second/thread\n",
           num_checkpoints.load(),
           ((double)total_reads_done_ + (double)total_writes_done_) / ((double)total_duration_ /
               kNanosPerSecond));
    
    cout << "Cleaning up RDMA resources..." << endl;
    return;
}

template <Op(*FN)(std::mt19937&)>
void thread_run_benchmark(store_t* store, RDMAClientResource& conn, size_t thread_idx, int BatchSize, int QueueDepth, uint64_t ReadNum) {
  SetThreadAffinity(thread_idx);

  std::random_device rd{};
  std::mt19937 rng{ rd() };

  auto start_time = std::chrono::high_resolution_clock::now();

  uint64_t upsert_value = 0;
  int64_t reads_done = 0;
  int64_t writes_done = 0;

  // post recv wr to receive first msg
  ibv_sge sge = {
    .addr = (uintptr_t)conn.recv_buf, 
    .length = sizeof(int),
    .lkey = conn.recv_mr->lkey
  };
  ibv_recv_wr wr = {
    .wr_id = thread_idx, // Use thread_idx as wr_id
    .sg_list = &sge, 
    .num_sge = 1
  }
  ibv_recv_wr* bad_wr;
  if(ibv_post_recv(conn.qp, &wr, &bad_wr)) {
    cout << "Error posting recv: " << strerror(errno) << endl;
    return;
  }

  ibv_wc wc; 
  while(ibv_poll_cq(conn.cq, 1, &wc) < 1 || wc.status != IBV_WC_SUCCESS);
  int msgSize = *(int*)(conn.recv_buf);
  int mbatchSize = msgSize / sizeof(MessageHeader);
  if (mbatchSize != BatchSize) {
      cerr << "ERROR: batch size MISMATCH on server and client" << endl;
  }
  cout << "Thread " << thread_idx << " received first message of size " << msgSize << endl;

  int TotalRecvSize = sizeof(MessageHeader) * BatchSize;
  int TotalSendSize = (sizeof(MessageHeader) + sizeof(uint64_t)) * BatchSize;

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
      
      ibv_sge sge = {
        .addr = (uintptr_t)conn.recv_buf, 
        .length = TotalRecvSize,
        .lkey = conn.recv_mr->lkey
      };
      ibv_recv_wr wr = {
        .wr_id = thread_idx + idx,
        .sg_list = &sge, 
        .num_sge = 1
      }
      if(ibv_post_recv(conn.qp, &wr, &bad_wr)) {
        cout << "Error posting recv: " << strerror(errno) << endl;
        return;
      }
      while(ibv_poll_cq(conn.cq, 1, &wc) < 1 || wc.status != IBV_WC_SUCCESS);
      
      if (over) {
          break;
      }
      //cout << "Thread " << thread_idx << "received " << tcpNum + 1 << "message" << endl;
      MessageHeader* requestMsg;
      for (int i = 0; i < BatchSize; i++) {
          requestMsg = (MessageHeader*)(conn.recv_buf + i * sizeof(MessageHeader));
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
          memcpy(conn.send_buf + (i * (sizeof(MessageHeader) + sizeof(uint64_t))), conn.recv_buf + i * sizeof(MessageHeader), sizeof(MessageHeader));
      }
      tcpNum += 1;
      //cout << "Thread " << thread_idx << "start to send " << tcpNum << "message" << endl;
      
      ibv_sge send_sge = {
        .addr = (uintptr_t) conn.send_buf,
        .length = TotalSendSize,
        .lkey = conn.send_mr->lkey
      };
      ibv_send_wr send_wr = {
        .wr_id = thread_idx,
        .sg_list = &send_sge,
        .num_sge = 1,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED
      };
      ibv_send_wr* bad_send_wr;
      if (ibv_post_send(conn.qp, &send_wr, &bad_send_wr)) {
          cout << "Error posting send: " << strerror(errno) << endl;
          over = true;
          break;
      }
      while(ibv_poll_cq(conn.cq, 1, &wc) < 1 || wc.status != IBV_WC_SUCCESS);

      //cout << "Thread " << thread_idx << "sent " << tcpNum << "message" << endl;
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

  cout << "Thread " << thread_idx << " closing client socket completed" << endl;

}