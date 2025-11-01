#include "common.hpp"
#include "helpers.hpp"
#include "rdma_comm.hpp"
#include <cstddef>

void ClientThreadFunc(RDMAConnectionResource &conn, int ThreadNum, int BatchSize, const uint64_t ReadNum,
                      const int QueueDepth, uint64_t *Keys, double *latencies,
                      std::chrono::high_resolution_clock::time_point *StartTime,
                      std::chrono::high_resolution_clock::time_point *EndTime, uint64_t *BytesCompleted)
{
    int flag = 1;

    const int HeaderSize = sizeof(MessageHeader);
    const int firstMsg = HeaderSize * BatchSize;
    const int TOTAL_MESSAGE_SIZE = BatchSize * (HeaderSize);
    memcpy(conn.send_buf, &firstMsg, sizeof(firstMsg));

    if (rdma_send(ThreadNum, sizeof(firstMsg))) {
        cerr << "Error sending first message from thread " << ThreadNum << endl;
        return;
    }

    ibv_wc wc;
    while (ibv_poll_cq(conn.cq, 1, &wc) < 1 || wc.status != IBV_WC_SUCCESS);
    if (wc.status != IBV_WC_SUCCESS)
    {
        printf("rdma_send: ibv_poll_cq failed with status %s\n", ibv_wc_status_str(wc.status));
        return -1;
    }

    // Send Quedepth number of Batches
    uint64_t bytesCompleted = 0;
    uint64_t bytesSent = 0;
    uint64_t msgIndex = 0;
    uint64_t RespIndex = 0;

    MessageHeader *hdr;
    *StartTime = high_resolution_clock::now();
    auto start = high_resolution_clock::now();
    ////auto current_time = start;
    bool over = false;

    // post all receive buffers first
    rdma_post_all_recv_buffers_in_queue(ThreadNum, QueueDepth, TOTAL_MESSAGE_SIZE);

    // fill up the queue with batches
    for (int q = 0; q != QueueDepth; q++)
    {
        for (int i = 0; i != BatchSize; i++)
        {
            hdr = (MessageHeader *)(conn.send_buf + q * TOTAL_MESSAGE_SIZE + i * HeaderSize);
            hdr->TimeSend = high_resolution_clock::now().time_since_epoch().count();
            hdr->BatchId = (uint16_t)q;
            hdr->Key = Keys[msgIndex * BatchSize + i];
            hdr->Operation = OP::READ;
        }
        msgIndex++;

        rdma_send(ThreadNum, TOTAL_MESSAGE_SIZ, q);
        bytesSent += TOTAL_MESSAGE_SIZE;
    }

    // Handle Receive
    while (!over)
    {
        ibv_wc wc;
        while(ibv_poll_cq(conn.cq, 1, &wc) < 1 || wc.status != IBV_WC_SUCCESS);

        // 1 send completed
        if (wc.opcode == IBV_WR_SEND)
        {
            int send_wr = wc.wr_id;
            if (msgIndex < ReadNum / BatchSize)
            {
                for (int i = 0; i != BatchSize; i++)
                {
                    // send in the slot just freed
                    hdr = (MessageHeader *)(conn.send_buf + send_wr * TOTAL_MESSAGE_SIZE + i * HeaderSize);
                    hdr->TimeSend = high_resolution_clock::now().time_since_epoch().count();
                    hdr->BatchId = BatchID;
                    hdr->Key = Keys[msgIndex * BatchSize + i];
                    hdr->Operation = OP::READ;
                }
                rdma_send(ThreadNum, TOTAL_MESSAGE_SIZE, send_wr);
    
                msgIndex++;
                bytesSent += TOTAL_MESSAGE_SIZE;
            } else continue;

        } else if (wc.opcode == IBV_WR_RECV)
        {
            int recv_wr = wc.wr_id;
            // wait for receive completion  & process 1 batch
            long long diff;
            uint16_t BatchID;

            // measure latency
            for (int i = 0; i != BatchSize; i++)
            {
                // diff returns the time in nanosecond, here we divide it by 1000 to get time in microsecond
                hdr = (MessageHeader *)(get_recv_buffer_address(conn.recv_buf, get_index_from_wr_id(recv_wr),
                                                                TOTAL_MESSAGE_SIZE) + i * HeaderSize);
                diff = high_resolution_clock::now().time_since_epoch().count() - hdr->TimeSend;
                BatchID = hdr->BatchId;
                if (BatchID > QueueDepth)
                {
                    cerr << "ERROR: got wrong batchId from server: " << BatchID << endl;
                }
                latencies[RespIndex * BatchSize + i] = ((double)diff / 1000);
            }
            bytesCompleted += TOTAL_MESSAGE_SIZE;
            RespIndex++;

            if (RespIndex == ReadNum / BatchSize)
            {
                break;
            }
            
            // post another receive
            rdma_post_recv_buf(thread_idx, TOTAL_MESSAGE_SIZE, recv_wr);

        } else
        {
            cerr << "Unexpected opcode: " << wc.opcode << endl;
            continue;
        }

        // send another message, reuse the same slot
        // else
        // {
        //     over = true;
        // }
        ////current_time = high_resolution_clock::now();
    }
    *EndTime = high_resolution_clock::now();
    *BytesCompleted = bytesCompleted;
}

int main(int argc, char *argv[])
{
    // load keys from file
    size_t count = load_keys(KEYS_FILE);
    size_t per_thread_read_num = count / N_THREADS;

    setup_rdma_connections(N_THREADS, PORT_BASE, QUEUE_DEPTH, BATCH_SIZE, per_thread_read_num, sizeof(MessageHeader),
                           sizeof(CompressedValue));

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
        threads.emplace_back(ClientThreadFunc, std::ref(connections[i]), i, BATCH_SIZE, per_thread_read_num,
                             QUEUE_DEPTH, &init_keys_.get()[i * per_thread_read_num],
                             static_cast<double *>(&latencies[i][0]), &startTimes[i], &endTimes[i], &bytesCompleted[i]);
    }

    for (auto& t : threads) t.join();
    cout << "Processing results..." << endl;

    return 0;
}