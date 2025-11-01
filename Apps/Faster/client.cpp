#include "common.hpp"
#include "helpers.hpp"
#include "latency_helpers.hpp"
#include "rdma_comm.hpp"
#include <cstddef>
#include <sys/types.h>

void ClientThreadFunc(RDMAConnectionResource &conn, int ThreadNum, int BatchSize, const uint64_t ReadNum,
                      const int QueueDepth, uint64_t *Keys, double *latencies,
                      std::chrono::high_resolution_clock::time_point *StartTime,
                      std::chrono::high_resolution_clock::time_point *EndTime, uint64_t *BytesCompleted)
{
    // read the input data file
    uint8_t *file_buffer = nullptr;
    size_t buffer_size = 0;
    if (!read_file(INPUT_FILE, &file_buffer, &buffer_size))
    {
        printf("Failed to read input file: %s\n", INPUT_FILE);
        return;
    }
    printf("Read %zu bytes from input file: %s\n", buffer_size, INPUT_FILE);

    const int HeaderSize = sizeof(MessageHeader);
    const int TOTAL_MESSAGE_SIZE = BatchSize * (HeaderSize);
    const int MAX_RECV_MSG_SIZE = (HeaderSize + sizeof(UncompressedValue)); // read OP response
    const int MAX_SEND_MSG_SIZE = (HeaderSize + sizeof(UncompressedValue));   // upsert OP request
    const int MAX_RECV_BATCH_SIZE = BatchSize * MAX_RECV_MSG_SIZE;
    const int MAX_SEND_BATCH_SIZE = BatchSize * MAX_SEND_MSG_SIZE;
    const int n_batches = ReadNum / BatchSize;
    printf("nbatches = %d\n", n_batches);
    

    // Send queue depth number of batches
    uint64_t bytesCompleted = 0;
    uint64_t bytesSent = 0;
    uint64_t msgIndex = 0;
    uint64_t RespIndex = 1; // nbatch = 25, respindex starts from 1

    MessageHeader *hdr;
    *StartTime = high_resolution_clock::now();
    auto start = high_resolution_clock::now();
    ////auto current_time = start;
    bool over = false;

    // do upsert first

    // post all receive buffers first
    rdma_post_all_recv_buffers_in_queue(client_threads_connections[ThreadNum], QueueDepth,
                                        BatchSize * MAX_RECV_MSG_SIZE);

    // TODO: do upsert requests first
    /* // fill up the queue with batches of send (upsert)
    for (int q = 0; q != QueueDepth; q++)
    {
        u_int8_t *batch_buf_start = get_send_buffer_address(conn.send_buf, q, MAX_SEND_BATCH_SIZE);
        for (int i = 0; i != BatchSize; i++)
        {
            hdr = (MessageHeader *)(batch_buf_start + i * MAX_SEND_MSG_SIZE);
            hdr->TimeSend = high_resolution_clock::now().time_since_epoch().count();
            hdr->BatchId = (uint16_t)q;
            hdr->Key = Keys[msgIndex * BatchSize + i];
            hdr->Operation = OP::UPSERT;
        }
        msgIndex++;

        rdma_send(ThreadNum, q, TOTAL_MESSAGE_SIZE);
        bytesSent += TOTAL_MESSAGE_SIZE;
    }



    // reset msgIndex and RespIndex etc. for read operation
    msgIndex = 0;
    RespIndex = 0;
    bytesSent = 0;
    bytesCompleted = 0; */

    // fill up the queue with batches of send (read)
    for (int q = 0; q != QueueDepth; q++)
    {
        u_int8_t *batch_buf_start = (uint8_t*) (conn.send_buf + q * MAX_SEND_BATCH_SIZE);
        printf("[DEBUG] Posting RECV: wr_id=%lu addr=%p\n", q, batch_buf_start);

        for (int i = 0; i != BatchSize; i++)
        {
            // hdr = (MessageHeader *)(conn.send_buf + q * TOTAL_MESSAGE_SIZE + i * HeaderSize);
            hdr = (MessageHeader *)(batch_buf_start + i * MAX_SEND_MSG_SIZE);
            hdr->TimeSend = high_resolution_clock::now().time_since_epoch().count();
            hdr->BatchId = (uint16_t)q;
            hdr->Key = Keys[msgIndex * BatchSize + i];
            hdr->Operation = OP::READ;
        }
        msgIndex++;

        rdma_send(conn, q, MAX_SEND_BATCH_SIZE);
        bytesSent += MAX_SEND_BATCH_SIZE;
    }

    // Handle Receive
    while (!over)
    {
        // ibv_wc wc;
        // while (ibv_poll_cq(conn.cq, 1, &wc) < 1 || wc.status != IBV_WC_SUCCESS);

        // uint64_t recv_wr = wc.wr_id;
        uint64_t recv_wr = rdma_wait_recv_completion(conn);
        long long diff;
        uint16_t BatchID;

        printf("wr_id: %ld, RespIndex: %d\n", recv_wr, RespIndex);
        if (RespIndex == n_batches)
        {
            // we are done with all OPs
            break;
        }
        
        u_int8_t *recv_buf_address =
            get_recv_buffer_address(conn.recv_buf, get_index_from_wr_id(recv_wr), MAX_RECV_BATCH_SIZE);

        // measure latency
        for (int i = 0; i != BatchSize; i++)
        {
            // diff returns the time in nanosecond, here we divide it by 1000 to get time in microsecond
            hdr = (MessageHeader *)(recv_buf_address + i * MAX_SEND_MSG_SIZE);
            diff = high_resolution_clock::now().time_since_epoch().count() - hdr->TimeSend;
            BatchID = hdr->BatchId;
            if (BatchID > QueueDepth) // debug assertion
            {
                cerr << "ERROR: got wrong batchId from server: " << BatchID << endl;
            }
            latencies[RespIndex * BatchSize + i] = ((double)diff / 1000);
    

        }
            // rearm the slot for receive, need to use the max possible size of a batch
            rdma_post_one_recv_buffer(conn, MAX_RECV_BATCH_SIZE, recv_wr);

            // reuse the slot to send another
            u_int8_t *batch_buf_start = get_send_buffer_address(conn.send_buf, recv_wr, MAX_SEND_BATCH_SIZE);

            if (BatchID != recv_wr) // debug assert, batch id should always be the same as the slot index
            {
                cerr << "ERROR: got wrong batchId from server: " << BatchID << ", expected (recv_wr_id): " << recv_wr
                     << endl;
            }

            if (msgIndex < n_batches) [[likely]] // send one more batch if we haven't sent all yet
            {
                for (int i = 0; i != BatchSize; i++)
                {
                    hdr = (MessageHeader *)(batch_buf_start + i * MAX_SEND_MSG_SIZE);
                    hdr->TimeSend = high_resolution_clock::now().time_since_epoch().count();
                    hdr->BatchId = BatchID;
                    hdr->Key = Keys[msgIndex * BatchSize + i];
                    hdr->Operation = OP::READ;
                }

                rdma_send(conn, recv_wr, MAX_SEND_BATCH_SIZE);

                msgIndex++;
                bytesSent += MAX_SEND_BATCH_SIZE;
        } // end for each in batch
        bytesCompleted += TOTAL_MESSAGE_SIZE;
        RespIndex++;
    }
    // end of processing loop
    *EndTime = high_resolution_clock::now();
    *BytesCompleted = bytesCompleted;
}

int main(int argc, char *argv[])
{
    // load keys from file
    size_t count = load_keys(KEYS_FILE);
    size_t per_thread_read_num = count / N_THREADS;
    if (count != LOAD_N_KEYS)
    {
        printf("Warning: number of keys loaded (%zu) does not match expected (%d). "
               "Using %zu keys per thread.\n",
               count, LOAD_N_KEYS, per_thread_read_num);
    }

    int max_send_msg_size = sizeof(MessageHeader) + sizeof(UncompressedValue); // upsert
    int max_recv_msg_size = sizeof(MessageHeader) + sizeof(UncompressedValue); // read
    setup_rdma_connections_client(N_THREADS, PORT_BASE, QUEUE_DEPTH, BATCH_SIZE, max_send_msg_size, max_recv_msg_size);

    cout << "Starting measurement..." << endl;
    std::chrono::high_resolution_clock::time_point *startTimes =
        new std::chrono::high_resolution_clock::time_point[N_THREADS];
    std::chrono::high_resolution_clock::time_point *endTimes =
        new std::chrono::high_resolution_clock::time_point[N_THREADS];
    uint64_t *bytesCompleted = new uint64_t[N_THREADS];

    double **latencies = new double *[N_THREADS];
    for (int i = 0; i != N_THREADS; i++)
    {
        latencies[i] = new double[per_thread_read_num];
        for (uint64_t l = 0; l != per_thread_read_num; l++)
        {
            latencies[i][l] = 0;
        }
    }

    // 2: Dispatch worker threads
    std::vector<thread> threads;
    for (int i = 0; i < N_THREADS; ++i)
    {
        threads.emplace_back(ClientThreadFunc, std::ref(client_threads_connections[i]), i, BATCH_SIZE,
                             per_thread_read_num, QUEUE_DEPTH, &init_keys_.get()[i * per_thread_read_num],
                             static_cast<double *>(&latencies[i][0]), &startTimes[i], &endTimes[i], &bytesCompleted[i]);
    }

    for (auto& t : threads) t.join();
    cout << "Processing results..." << endl;

    // Calculate throughput
    std::chrono::high_resolution_clock::time_point aggStartTime = startTimes[0], aggEndTime = endTimes[0];
    uint64_t aggBytesCompleted = 0;
    double P50Sum = 0;
    double P90Sum = 0;
    double P99Sum = 0;
    Statistics LatencyStats;
    Percentiles PercentileStats;
    PercentileStats.P50 = 0;
    PercentileStats.P90 = 0;
    PercentileStats.P99 = 0;
    auto *durations = new long long[N_THREADS];

    for (int i = 0; i != N_THREADS; i++)
    {
        if (aggStartTime > startTimes[i])
        {
            aggStartTime = startTimes[i];
        }
        if (aggEndTime < endTimes[i])
        {
            aggEndTime = endTimes[i];
        }
        durations[i] = duration_cast<microseconds>(endTimes[i] - startTimes[i]).count();

        aggBytesCompleted += bytesCompleted[i];
    }
    auto duration = duration_cast<microseconds>(aggEndTime - aggStartTime).count(); // duration in microseconds
    double throughput = (double)aggBytesCompleted / (double)duration * 1000000.0f /
                        (1024.0 * 1024.0 * 1024.0); // in giga bytes per second

    for (int i = 0; i != N_THREADS; i++)
    {
        GetStatistics(latencies[i], per_thread_read_num, &LatencyStats, &PercentileStats);
        printf("Thread %d: Result for %zu requests (%.2lf seconds): %.2lf RPS, Min: %.2lf, Max: %.2lf, 50th: %.2lf, "
               "90th: %.2lf, 99th: %.2lf, 99.9th: %.2lf, 99.99th: %.2lf, StdErr: %.2lf\n",
               i, per_thread_read_num, (durations[i] / 1000000.0), (per_thread_read_num / (durations[i] / 1000000.0)),
               LatencyStats.Min, LatencyStats.Max, PercentileStats.P50, PercentileStats.P90, PercentileStats.P99,
               PercentileStats.P99p9, PercentileStats.P99p99, LatencyStats.StandardError);
        P50Sum += PercentileStats.P50;
        P90Sum += PercentileStats.P90;
        P99Sum += PercentileStats.P99;
    }

    cout << "Throughput: " << throughput << " GB/s" << endl;
    cout << "IOPS: " << per_thread_read_num * N_THREADS / ((double)duration / 1000000) << "RPS" << endl;
    cout << "Avg P50: " << P50Sum / N_THREADS << endl;
    cout << "Avg P90: " << P90Sum / N_THREADS << endl;
    cout << "Avg P99: " << P99Sum / N_THREADS << endl;

    return 0;
}