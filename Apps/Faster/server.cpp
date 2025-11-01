#include "server.hpp"
// #include "benchmark.h"
#include "common.hpp"
#include "helpers.hpp"
#include "rdma_comm.hpp"
// #include "status.h"
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>

store_t faster_store{
    FASTER_HASH_TABLE_SIZE,       // Hash table size (1M entries)
    FASTER_LOG_SIZE, // In-memory Log size (1 GiB)
    FASTER_LOG_PATH,                  // Log path must be empty for in-memory store
    FASTER_LOG_MUTABLE_FRACTION,                 // Log Mutable fraction
};

uint8_t *file_buffer = nullptr;
size_t buffer_size = 0;

void server_thread_run_benchmark(store_t *store, RDMAConnectionResource &conn, size_t thread_idx, int BatchSize,
                                 int QueueDepth, uint64_t ReadNum)
{
    // pin pthread to a CPU core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(thread_idx % std::thread::hardware_concurrency(), &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    // allocate buffers to hold uncompressed values before compression and upsert,
    // as well as buffers to hold compressed values from faster read before decompression and sending back
    u_int8_t *uncompressed_data_bufs = (u_int8_t *)::aligned_alloc(4096, BatchSize * sizeof(UncompressedValue));
#ifdef BASELINE
    u_int8_t *compressed_data_bufs = (u_int8_t *)::aligned_alloc(4096, BatchSize * sizeof(UncompressedValue));

#else
    u_int8_t *compressed_data_bufs = (u_int8_t *)::aligned_alloc(4096, BatchSize * sizeof(CompressedValue));
#endif
    RDMAConnectionResource &server_conn = server_threads_connections[thread_idx];

    auto start_time = std::chrono::high_resolution_clock::now();

    // uint64_t upsert_value = 0;
#ifdef BASELINE
    UncompressedValue *this_upsert_value = NULL;
#else
    CompressedValue *this_upsert_value = NULL;
#endif
    int64_t reads_done = 0;
    int64_t writes_done = 0;

    const int n_batches = ReadNum / BatchSize;
    int loopnum = 0; // up to n_batches

    Guid guid = store->StartSession(); // session is per thread

    const int MAX_RECV_MSG_SIZE = (sizeof(MessageHeader) + sizeof(UncompressedValue)); // upsert OP request
    const int MAX_SEND_MSG_SIZE = (sizeof(MessageHeader) + sizeof(UncompressedValue)); // read OP response
    const int MAX_RECV_BATCH_SIZE = BatchSize * MAX_RECV_MSG_SIZE;
    const int MAX_SEND_BATCH_SIZE = BatchSize * MAX_SEND_MSG_SIZE;

    // post recv buffers
    // rdma_post_all_recv_buffers_in_queue(conn, QueueDepth, BatchSize * MAX_RECV_MSG_SIZE);

    cout << "finished setup steps" << endl;

    auto start = std::chrono::high_resolution_clock::now();
    auto current_time = start;
    bool over = false;
    std::chrono::nanoseconds dur;

    while (!over)
    {
        // wait for a received message batch
        uint64_t wr_id = rdma_wait_recv_completion(conn);

        // cout << "Thread " << thread_idx << "received " << tcpNum + 1 << "message" << endl;
        MessageHeader *requestMsg;
        uint16_t batch_id;
        u_int8_t *recv_buf = get_recv_buffer_address(conn.recv_buf, wr_id, MAX_RECV_BATCH_SIZE);
        int this_resp_size = 0;
        int nb_msg_offset = 0; // offset in the batch
        // printf("Thread %zu received batch with wr_id: %lu\n", thread_idx, wr_id);

        // process each message in the batch
        for (int i = 0; i < BatchSize; i++)
        {
            // we don't yet know if this is a read or upsert (which has an extra payload after the MessageHeader), so we
            // need to check the operation, and infer the offset from the OP (see inc at the end of each case)

            ////requestMsg = (MessageHeader *)(recv_buf + i * MAX_RECV_MSG_SIZE);
            requestMsg = (MessageHeader *)(recv_buf + nb_msg_offset);
            batch_id = requestMsg->BatchId;
            /* printf("Thread %zu processing batch %d, key: %lu, operation: %d\n", thread_idx, batch_id,
               requestMsg->Key, requestMsg->Operation); */
            switch (requestMsg->Operation)
            {
            case OP::READ: {
                this_resp_size = MAX_SEND_MSG_SIZE;

                auto callback = [](IAsyncContext *ctxt, Status result) { CallbackContext<ReadContext> context{ctxt}; };

                u_int8_t *send_buf_batch_base = get_send_buffer_address(conn.send_buf, wr_id, MAX_SEND_BATCH_SIZE);
                u_int8_t *send_buf = send_buf_batch_base + i * MAX_SEND_MSG_SIZE;

                // read from faster, the value will be compressed, so decompress it first then put the decompressed data
                // into send buf
#ifdef BASELINE
                u_int8_t *compressed_buf = compressed_data_bufs + i * sizeof(UncompressedValue);
#else
                u_int8_t *compressed_buf = compressed_data_bufs + i * sizeof(CompressedValue);
#endif

                // put the read value in this preallocated buffer
#ifdef BASELINE
                ReadContext context{requestMsg->Key, (UncompressedValue *)compressed_buf};
#else
                ReadContext context{requestMsg->Key, (CompressedValue *)compressed_buf};
#endif

                Status result = store->Read(context, callback, 1);
                if (result != Status::Ok)
                {
                    printf("Read key %lu failed with status: %s\n", requestMsg->Key, StatusStr(result));
                    // over = true;
                    // break;
                }
                else
                {
                    // printf("Thread %zu Read key: %lu\n", thread_idx, requestMsg->Key);
                }

                store->CompletePending(true);

                void *read_value = context.GetValue();

                /* if (memcmp(file_buffer, read_value, sizeof(CompressedValue)) != 0)
                {
                    printf("Read value DOES NOT match expected value for key: %lu\n", requestMsg->Key);
                }
                else
                {
                    printf("Read value matches expected value for key: %lu\n", requestMsg->Key);
                } */

#ifdef BASELINE
                // copy the header to the send buffer
                memcpy(send_buf, requestMsg, sizeof(MessageHeader));
                // copy the uncompressed value to the send buffer after the header
                memcpy(send_buf + sizeof(MessageHeader), read_value, sizeof(UncompressedValue));
#else
                // decompress the value and put it into the send buffer
                size_t actual_out_size = 0;
                // directly put the decompressed data into the send buffer after the header
                if (deflate_decompress_data(thread_idx, read_value, sizeof(CompressedValue),
                                            send_buf + sizeof(MessageHeader), sizeof(UncompressedValue),
                                            &actual_out_size) == 0)
                {
                    printf("could not decompress data for key: %lu\n", requestMsg->Key);
                }
                // copy the header to the send buffer
                memcpy(send_buf, requestMsg, sizeof(MessageHeader));
#endif

                ++reads_done;

                nb_msg_offset += sizeof(MessageHeader); // read has no payload

                break;
            }
            case OP::UPSERT: {
                this_resp_size = sizeof(MessageHeader); // upsert has no payload to send back

                auto callback = [](IAsyncContext *ctxt, Status result) {
                    CallbackContext<UpsertContext> context{ctxt};
                };

                // using MAX_SEND_MSG_SIZE here, since each batch occupies the max possible size of a batch, even though we don't need all of it
                u_int8_t *recv_buf_batch_base = get_recv_buffer_address(conn.recv_buf, wr_id, MAX_RECV_BATCH_SIZE);
                // the actual recv buf is a MessageHeader + CompressedValue (payload)
                u_int8_t *recv_buf = recv_buf_batch_base + i * MAX_RECV_MSG_SIZE;

#ifdef BASELINE
                u_int8_t *uncompressed_buf = uncompressed_data_bufs + i * sizeof(UncompressedValue);
                this_upsert_value = (UncompressedValue *)uncompressed_buf;
                // copy the data from the recv buffer to the uncompressed buffer
                memcpy(uncompressed_buf, recv_buf + sizeof(MessageHeader), sizeof(UncompressedValue));
#else
                u_int8_t *uncompressed_buf = uncompressed_data_bufs + i * sizeof(UncompressedValue);

                if (deflate_compress_data(thread_idx, recv_buf + sizeof(MessageHeader), sizeof(CompressedValue),
                                          uncompressed_buf, sizeof(UncompressedValue)) == 0)
                {
                    printf("could not compress data for key: %lu\n", requestMsg->Key);
                }
                this_upsert_value = (CompressedValue *)uncompressed_buf;
#endif

                UpsertContext context{requestMsg->Key, this_upsert_value};
                Status result = store->Upsert(context, callback, 1);

                store->CompletePending(true);

                if (result != Status::Ok)
                {
                    printf("Upsert failed with status: %s\n", StatusStr(result));
                }
                else
                    printf("Thread %zu Upserted key: %lu\n", thread_idx, requestMsg->Key);

                // copy the header to the send buffer
                u_int8_t *send_buf_batch_base = get_send_buffer_address(conn.send_buf, wr_id, MAX_SEND_BATCH_SIZE);
                u_int8_t *send_buf = send_buf_batch_base + i * sizeof(MessageHeader); // this is an upsert, so sending only the header
                memcpy(send_buf, requestMsg, sizeof(MessageHeader));
                ++writes_done;

                nb_msg_offset += sizeof(MessageHeader) + sizeof(CompressedValue); // upsert has a payload

                break;
            }
            }
            // no memcpy needed here, we directly let faster put the read data into the send buffer
            // memcpy(conn.send_buf + (i * (sizeof(MessageHeader) + sizeof(uint64_t))),
            //        conn.recv_buf + i * sizeof(MessageHeader), sizeof(MessageHeader));
        }

        // either READ or UPSERT batch is served, send back the batch of responses
        store->CompletePending(true);

        // rearm the slot for receive
        rdma_post_one_recv_buffer(server_conn, MAX_RECV_BATCH_SIZE, wr_id);

        // now send the batch of responses, with corresponding size
        // XXX: wr_id is the receive slot index, the send slot should be determined by the batch_id
        ////rdma_send_batch(server_conn, wr_id, MAX_SEND_BATCH_SIZE, this_resp_size * BatchSize);
        rdma_send_batch(server_conn, batch_id, MAX_SEND_BATCH_SIZE, this_resp_size * BatchSize);

        loopnum += 1;
        if (loopnum == n_batches)
        {
            over = true;
            // printf("Thread %zu completed all batches.\n", thread_idx);
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::nanoseconds duration = end_time - start_time;

    printf("Finished thread %" PRIu64 " : %" PRIu64 " reads, %" PRIu64 " writes, in %.6f seconds.\n", thread_idx,
           reads_done, writes_done, (double)duration.count() / 1000000000);

    store->StopSession();
}

int main(int argc, char *argv[])
{
    // init libdeflate for compression and decompression
    if (!init_libdeflate(N_THREADS))
    {
        printf("Failed to initialize libdeflate.\n");
        return -1;
    }

    // read input file
    file_buffer = nullptr;
    buffer_size = 0;
    if (!read_file(INPUT_FILE, &file_buffer, &buffer_size))
    {
        printf("Failed to read input file: %s\n", INPUT_FILE);
        return 1;
    }
    printf("Read %zu bytes from input file: %s\n", buffer_size, INPUT_FILE);

    // read keys from file
    size_t n_keys = load_keys(KEYS_FILE);

    // populate the store with the input file data
    faster_store.StartSession();

    // allocate a buffer for upsert context data
#ifdef BASELINE
    UncompressedValue *compressed_value = (UncompressedValue *)malloc(sizeof(UncompressedValue));
    memset(compressed_value, 0, sizeof(UncompressedValue));
    // in place new the value, this will copy the input data into CompressedValue
    new (compressed_value) UncompressedValue(file_buffer);
#else
    CompressedValue *compressed_value = (CompressedValue *)malloc(sizeof(CompressedValue));
    memset(compressed_value, 0, sizeof(CompressedValue));
    // in place new the value, this will copy the input data into CompressedValue
    new (compressed_value) CompressedValue(file_buffer);
#endif

    for (int i = 0; i < n_keys; ++i)
    {
        // for each key, we will upsert the same value
        UpsertContext context{init_keys_.get()[i], compressed_value};
        Status status = faster_store.Upsert(
            context,
            [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
                if (result != Status::Ok)
                {
                    printf("async Upsert failed with status: %s\n", StatusStr(result));
                }
                /* else
                {
                    printf("Upsert succeeded for key: %lu\n", context->key().key);
                } */
            },
            1);
        if (status != Status::Ok && status != Status::Pending)
        {
            printf("Failed to upsert data for %dth key: %s\n", i, StatusStr(status));
            free(file_buffer);
            free(compressed_value);
            return 1;
        }
    }

    printf("Upserted %zu keys into the store.\n", n_keys);

    faster_store.CompletePending(true);
    faster_store.StopSession();

    // start RDMA connections
    setup_rdma_connections_server(N_THREADS, PORT_BASE, BATCH_SIZE, QUEUE_DEPTH);

    // XXX: try post all recv before threads even start?
    for (int i = 0; i < N_THREADS; ++i)
    {
        printf("Posting all recv buffers for thread %d\n", i);
        rdma_post_all_recv_buffers_in_queue(server_threads_connections[i], QUEUE_DEPTH,
                                            BATCH_SIZE * (sizeof(MessageHeader) + sizeof(UncompressedValue)));
    }

    std::deque<std::thread> threads;
    for (size_t thread_idx = 0; thread_idx < N_THREADS; ++thread_idx)
    {
        threads.emplace_back(&server_thread_run_benchmark, &faster_store, ref(server_threads_connections[thread_idx]),
                             thread_idx, BATCH_SIZE, QUEUE_DEPTH, LOAD_N_KEYS / N_THREADS);
    }

    for(auto& thread : threads) {
      thread.join();
    }
    // cout << "Cleaning up RDMA resources..." << endl;
    cout << "Cleaning up RDMA resources..." << endl;

    // TODO: clean up for each connection?

    // if (rdma_destroy_id(listen_id))
    // {
    //     std::cerr << "Error destroying RDMA listen_id: " << strerror(errno) << std::endl;
    // }
    // rdma_destroy_event_channel(ec);

    if (rdma_destroy_id(listen_id))
    {
        std::cerr << "Error destroying RDMA listen_id: " << strerror(errno) << std::endl;
    }
    rdma_destroy_event_channel(server_ec);
}

/* int test_main(int argc, char *argv[])
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

        int is_equal = std::memcmp(compressed_value->value_, read_value->value_, COMPRESSED_SIZE - 1) == 0;
        printf("Is read value equal to upserted value? %s\n", is_equal ? "Yes" : "No");
        is_equal = std::memcmp(buffer, read_value->value_, buffer_size) == 0;
        printf("Is read value equal to input file value? %s\n", is_equal ? "Yes" : "No");

        read_value->value_[COMPRESSED_SIZE - 1] = '\0';
        printf("%s\n", read_value->value_);
    }

    faster_store.StopSession();

    return 0;
} */
