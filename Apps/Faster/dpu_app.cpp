#include "dpu_app.hpp"
#include "dpm_interface.hpp"
#include "faster_bench_common.hpp"
#include "rdma_comm.hpp"
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

vector<char *> contig_input_buf_per_thread;  // for DPU threads, each thread has its own input buffer
vector<char *> contig_output_buf_per_thread; // for DPU threads, each thread has its own output buffer

vector<vector<dpkernel_task *>> task_list_per_thread; // each thread has its own list of tasks preallocated
vector<vector<int>> task_done_counter_per_thread; // each thread has its own list of tasks done counters, if we reach
                                                  // BatchSize, we can send back
vector<vector<bool>>
    inflight_batch_flags_per_thread; // each thread has its own list of flags to indicate if a batch is inflight

thread_local uint64_t batch_id_to_server_conn_recv_slot[QUEUE_DEPTH]; // each thread has its own mapping of batch_id to
                                                                      // server connection receive slot
thread_local uint64_t batch_id_to_client_conn_recv_slot[QUEUE_DEPTH];

/// setup RDMA connections for each DPU APP thread -- as client (recv compressed values from server)
bool setup_rdma_connections_dpu_client(int NumConnections, int PortBase, int QueueDepth, int BatchSize,
                                       int MaxSendMsgSize, int MaxRecvMsgSize, vector<RDMAConnectionResource> &conns)
{
    // NumConnections qp
    vector<RDMAConnectionResource> &client_threads_connections = conns;
    client_ec = rdma_create_event_channel();
    rdma_cm_event *event;

    // 1: Connect all qp
    for (int i = 0; i < NumConnections; ++i)
    {
        auto &conn = client_threads_connections[i];
        rdma_create_id(client_ec, &conn.cm_id, nullptr, RDMA_PS_TCP);

        sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(PortBase);
        inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);

        rdma_resolve_addr(conn.cm_id, nullptr, (sockaddr *)&addr, TIMEOUT_IN_MS);
        rdma_get_cm_event(client_ec, &event);
        if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
        {
            cout << "Error resolving address for connection " << i << endl;
            return false;
        }
        rdma_ack_cm_event(event);

        rdma_resolve_route(conn.cm_id, TIMEOUT_IN_MS);
        rdma_get_cm_event(client_ec, &event);
        if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
        {
            cout << "Error resolving route for connection " << i << endl;
            return false;
        }
        rdma_ack_cm_event(event);

        conn.pd = ibv_alloc_pd(conn.cm_id->verbs);
        conn.cq = ibv_create_cq(conn.cm_id->verbs, QueueDepth * 2, nullptr, nullptr, 0);
        if (!conn.pd || !conn.cq)
        {
            cout << "Error allocating PD or creating CQ for connection " << i << endl;
            return false;
        }

        ibv_qp_init_attr qp_attr = {};
        qp_attr.send_cq = conn.cq;
        qp_attr.recv_cq = conn.cq;
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.cap.max_send_wr = QueueDepth;
        qp_attr.cap.max_recv_wr = QueueDepth;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
        rdma_create_qp(conn.cm_id, conn.pd, &qp_attr);

        // as a client, the send buf is just relaying messages, no need for DPM managed memory
        conn.send_buf = (char *)std::aligned_alloc(4096, MaxSendMsgSize * BatchSize * QueueDepth);
        // as a client, the recv buf is receiving compressed values from the server (to be decompressed) so we need DPM
        // managed memory
        conn.recv_buf = (char *)contig_input_buf_per_thread[i]; // DPM managed memory for inputs
        // memset(conn.send_buf, 0, MaxSendMsgSize * BatchSize * QueueDepth);
        // memset(conn.recv_buf, 0, MaxRecvMsgSize * BatchSize * QueueDepth);

        conn.send_mr =
            ibv_reg_mr(conn.pd, conn.send_buf, MaxSendMsgSize * BatchSize * QueueDepth, IBV_ACCESS_LOCAL_WRITE);
        conn.recv_mr = ibv_reg_mr(conn.pd, conn.recv_buf, MaxRecvMsgSize * BatchSize * QueueDepth,
                                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

        RDMAConnectionContext local_ctx = {.remote_addr = reinterpret_cast<uint64_t>(conn.recv_buf),
                                           .rkey = conn.recv_mr->rkey};

        rdma_conn_param conn_param = {};
        conn_param.initiator_depth = 1;
        conn_param.responder_resources = 1;
        conn_param.retry_count = 7;
        conn_param.private_data = &local_ctx;
        conn_param.private_data_len = sizeof(local_ctx);

        rdma_connect(conn.cm_id, &conn_param);
        rdma_get_cm_event(client_ec, &event);
        if (event->event != RDMA_CM_EVENT_ESTABLISHED)
        {
            cout << "Error establishing connection for connection " << i << endl;
            return false;
        }
        rdma_ack_cm_event(event);
    }

    return true;
}

/// setup RDMA connections for each thread -- server side
bool setup_rdma_connections_dpu_server(int NumConnections, int PortBase, int BatchSize, int QueueDepth,
                                       vector<RDMAConnectionResource> &conns)
{

    vector<RDMAConnectionResource> &server_threads_connections = conns;
    server_ec = rdma_create_event_channel();
    rdma_create_id(server_ec, &listen_id, nullptr, RDMA_PS_TCP);

    // Set up the address of the server
    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PortBase);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (rdma_bind_addr(listen_id, (sockaddr *)&addr))
    {
        cout << "Error binding RDMA CM ID: " << strerror(errno) << endl;
        rdma_destroy_id(listen_id);
        rdma_destroy_event_channel(server_ec);
        return false;
    }
    if (rdma_listen(listen_id, SOMAXCONN))
    {
        cout << "Error listening on RDMA CM ID: " << strerror(errno) << endl;
        rdma_destroy_id(listen_id);
        rdma_destroy_event_channel(server_ec);
        return false;
    }
    std::cout << "Listening for " << NumConnections << " clients on port " << PortBase << "..." << std::endl;

    // Bind the socket to the address
    int clientIndex = 0;
    rdma_cm_event *event;

    cout << "Waiting for clients..." << endl;

    while (clientIndex != NumConnections)
    {
        event = nullptr;
        if (rdma_get_cm_event(server_ec, &event))
        {
            cout << "Error getting RDMA CM event: " << strerror(errno) << endl;
            rdma_destroy_id(listen_id);
            rdma_destroy_event_channel(server_ec);
            return false;
        }
        if (event->event == RDMA_CM_EVENT_ESTABLISHED)
        {
            rdma_ack_cm_event(event);
            continue;
        }
        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST)
        {
            cout << "Unexpected RDMA CM event: " << rdma_event_str(event->event) << endl;
            rdma_ack_cm_event(event);
            continue;
        }
        rdma_cm_id *conn_id = event->id;
        rdma_ack_cm_event(event);

        RDMAConnectionResource &res = server_threads_connections[clientIndex];
        memset(&res, 0, sizeof(RDMAConnectionResource));
        res.cm_id = conn_id;

        int max_send_msg_size = sizeof(MessageHeader) + sizeof(UncompressedValue); // read OP response
        int max_recv_msg_size = sizeof(MessageHeader) + sizeof(UncompressedValue); // upsert OP request

        if (!setup_rdma_dpu_server_qp_and_buffers(conn_id, res, QueueDepth, BatchSize, max_send_msg_size,
                                                  max_recv_msg_size, clientIndex)) // upsert is message header + value
        {
            cout << "Failed to set up RDMA QP and buffers for client " << clientIndex << endl;
            rdma_destroy_id(conn_id);
            continue;
        }

        rdma_conn_param conn_param = {};
        conn_param.initiator_depth = 1;
        conn_param.responder_resources = 1;
        conn_param.rnr_retry_count = 7; // Retry count for RNR errors

        // Accept the connection after setting up QP and buffers
        if (rdma_accept(res.cm_id, &conn_param))
        {
            cout << "Error accepting RDMA connection: " << strerror(errno) << endl;
            rdma_destroy_qp(conn_id);
            ibv_destroy_cq(res.cq);
            ibv_dereg_mr(res.send_mr);
            ibv_dereg_mr(res.recv_mr);
            free(res.send_buf);
            free(res.recv_buf);
            rdma_destroy_id(conn_id);
            continue;
        }

        cout << "accepted client " << clientIndex << endl;
        clientIndex++;
    }
    cout << "all clients connected, waiting for them to finish" << endl;

    return true;
}

void dpu_thread_func(RDMAConnectionResource &server_conn, RDMAConnectionResource &client_conn, int ThreadNum,
                     int BatchSize, const uint64_t ReadNum, const int QueueDepth)
{
    // pin pthread to a CPU core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(ThreadNum % std::thread::hardware_concurrency() + 8, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    const int HeaderSize = sizeof(MessageHeader);
    const int TOTAL_MESSAGE_SIZE = BatchSize * (HeaderSize);
    const int MAX_RECV_MSG_SIZE = (HeaderSize + sizeof(UncompressedValue));
    const int MAX_READ_RECV_MSG_SIZE = (HeaderSize + sizeof(CompressedValue));   // read OP response
    const int MAX_READ_SEND_MSG_SIZE = (HeaderSize + sizeof(UncompressedValue)); // read OP response
    const int MAX_SEND_MSG_SIZE = (HeaderSize + sizeof(UncompressedValue));
    const int MAX_RECV_BATCH_SIZE = BatchSize * MAX_RECV_MSG_SIZE;
    const int MAX_SEND_BATCH_SIZE = BatchSize * MAX_SEND_MSG_SIZE;
    const int n_batches = ReadNum / BatchSize;

    int n_batches_done = 0;

    // int64_t reads_done = 0;
    // int64_t writes_done = 0;

    u_int8_t *recv_buf;
    int this_resp_size;
    int nb_msg_offset = 0; // offset in the batch
    MessageHeader *requestMsg;
    MessageHeader *respMsg;
    uint16_t batch_id;
    dpkernel_task **task_batch_head; // to submit to DPM

    u_int8_t *send_buf_batch_base;
    int batch_to_send = -1;

    bool over = false;

    // 1. recv message header batch from real client, as server threads
    // 2. forward the request to the actual server
    // 3. recv response (compressed data) from the actual server, as client threads
    // 4. decompress using DPM
    // 5. send the response back to the real client, as server threads
    while (!over)
    {
        //
        // 1,2. try to recv from client as a server
        //
        //
    RECV_AND_PROCESS_FROM_CLIENT:
        uint64_t wr_id = try_rdma_wait_recv_completion(server_conn);
        if (wr_id == -1)
        {
            // no completion, goto recv from server as client, i.e. try recv from real client
            goto RECV_FROM_REAL_SERVER;
        }
        else
        {
            // printf("Thread %d server conn received batch with wr_id: %lu\n", ThreadNum, wr_id);
        }

        // receives 1 batch
        recv_buf = get_recv_buffer_address(server_conn.recv_buf, get_index_from_wr_id(wr_id), MAX_RECV_BATCH_SIZE);

        this_resp_size = 0;
        nb_msg_offset = 0; // offset in the batch

        // process (copy from server conn recv buf to client conn send buf for relaying) each message in the batch
        // just for READ OP, copy everything in the batch
        requestMsg = (MessageHeader *)(recv_buf);
        batch_id = requestMsg->BatchId;
        batch_id_to_server_conn_recv_slot[batch_id] = wr_id;
        this_resp_size = sizeof(MessageHeader); // only sending/relaying the headers
        send_buf_batch_base = get_send_buffer_address(client_conn.send_buf, batch_id, MAX_SEND_BATCH_SIZE);
        memcpy(send_buf_batch_base, recv_buf, TOTAL_MESSAGE_SIZE);

        // for (int i = 0; i < BatchSize; i++)
        // {
        //     // we don't yet know if this is a read or upsert (which has an extra payload after the MessageHeader), so
        //     we
        //     // need to check the operation, and infer the offset from the OP (see inc at the end of each case)

        //     ////requestMsg = (MessageHeader *)(recv_buf + i * MAX_RECV_MSG_SIZE);
        //     requestMsg = (MessageHeader *)(recv_buf + nb_msg_offset);
        //     batch_id = requestMsg->BatchId;
        //     batch_id_to_server_conn_recv_slot[batch_id] = wr_id;

        //     switch (requestMsg->Operation)
        //     {
        //     case OP::READ: {
        //         this_resp_size = sizeof(MessageHeader); // only sending/relaying the headers

        //         // get the send buf addr from the CLIENT connection, since we are relaying the request to the real
        //         // server
        //         u_int8_t *send_buf_batch_base =
        //             get_send_buffer_address(client_conn.send_buf, batch_id, MAX_SEND_BATCH_SIZE);
        //         u_int8_t *send_buf = send_buf_batch_base + i * sizeof(MessageHeader);

        //         // just forward the full batch of read requests to the real server using the client_conn
        //         // TODO: can just memcpy once instead of per message
        //         memcpy(send_buf, requestMsg, sizeof(MessageHeader));
        //         // memcpy(send_buf_batch_base, recv_buf, MAX_RECV_BATCH_SIZE); // copy the whole batch to the send
        //         buffer
        //         // goto RELAY_TO_REAL_SERVER; // relay to the real server

        //         nb_msg_offset += sizeof(MessageHeader); // read has no payload

        //         break;
        //     }
        //     case OP::UPSERT: {
        //         this_resp_size = MAX_SEND_MSG_SIZE; // upsert will have a payload to relay back

        //         // using MAX_SEND_MSG_SIZE here, since each batch occupies the max possible size of a batch, even
        //         though
        //         // we don't need all of it
        //         u_int8_t *recv_buf_batch_base =
        //             get_recv_buffer_address(server_conn.recv_buf, wr_id, MAX_RECV_BATCH_SIZE);
        //         // the actual recv buf is a MessageHeader + CompressedValue (payload)
        //         u_int8_t *recv_buf = recv_buf_batch_base + i * MAX_RECV_MSG_SIZE;

        //         // copy the header to the send buffer
        //         u_int8_t *send_buf_batch_base =
        //             get_send_buffer_address(server_conn.send_buf, batch_id, MAX_SEND_BATCH_SIZE);
        //         u_int8_t *send_buf =
        //             send_buf_batch_base + i * MAX_SEND_MSG_SIZE; // this is an upsert so sending the header + payload
        //         // memcpy the header + payload to the send buffer
        //         memcpy(send_buf, recv_buf, MAX_SEND_MSG_SIZE);

        //         nb_msg_offset += MAX_SEND_MSG_SIZE; // upsert has uncompressed payload

        //         break;
        //     }
        //     }
        // } // end for each in batch

        // rearm the server conn recv slot
        //// XXX: DO NOT REARM HERE! ONLY DO IT AFTER WE SEND THE BATCH BACK TO THE REAL CLIENT
        // rdma_post_one_recv_buffer(server_conn, MAX_RECV_BATCH_SIZE, wr_id);

    RELAY_TO_REAL_SERVER:
        // now relay to real server as CLIENT, with corresponding size (already copied the data during above)
        // XXX: wr_id is the receive slot index, the send slot should be determined by the batch_id
        rdma_send_batch(client_conn, batch_id, MAX_SEND_BATCH_SIZE, this_resp_size * BatchSize);
        // printf("Thread %d relayed batch %d to real server with wr_id: %lu\n", ThreadNum, batch_id, wr_id);

        // goto RECV_AND_PROCESS_FROM_CLIENT; // go back to receive more requests from the real client, will jump to
        // recv from server if no more

    RECV_FROM_REAL_SERVER:
        //
        // 3. try to recv from the actual server as a client
        //
        //
        wr_id = try_rdma_wait_recv_completion(client_conn);
        if (wr_id == -1)
        {
            // no completion, continue to next iteration
            goto CHECK_TASK_COMPLETIONS;
        }
        else
        {
            // printf("Thread %d client conn received batch with wr_id: %lu\n", ThreadNum, wr_id);
        }

        // receives 1 batch of compressed values from real server
        recv_buf = get_recv_buffer_address(client_conn.recv_buf, get_index_from_wr_id(wr_id), MAX_RECV_BATCH_SIZE);
        ////
        batch_id = ((MessageHeader *)(recv_buf))->BatchId; // get the batch id from the first message header
        batch_id_to_client_conn_recv_slot[batch_id] = wr_id;
        ////
        /* for (int i = 0; i != BatchSize; i++)
        {
            respMsg = (MessageHeader *)(recv_buf + i * MAX_READ_RECV_MSG_SIZE);
            batch_id = respMsg->BatchId;
            batch_id_to_client_conn_recv_slot[batch_id] = wr_id;
            if (batch_id > QueueDepth) // debug assertion
            {
                cerr << "ERROR: got wrong batchId from server: " << batch_id << endl;
            }
        } */
        // get the first DPM task from this batch from preallocated task list for this thread
        task_batch_head = &task_list_per_thread[ThreadNum].data()[batch_id * BATCH_SIZE];
        // decompress the compressed value using DPM

        dpm_submit_task_msgq_batched(ThreadNum, task_batch_head, BATCH_SIZE, DEVICE_BLUEFIELD_3);
        inflight_batch_flags_per_thread[ThreadNum][batch_id] = true; // mark this batch as inflight
        // printf("Thread %d submitted decompression tasks for batch %d\n", ThreadNum, batch_id);

    CHECK_TASK_COMPLETIONS:
        batch_to_send = -1; // reset
        // nonblocking wait for the tasks to complete
        for (int i = 0; i < QUEUE_DEPTH; i++)
        {
            if (inflight_batch_flags_per_thread[ThreadNum][i]) // if batch is in flight
            {
                bool all_tasks_done = true; // assume all tasks in this batch are done
                dpkernel_task *task = task_list_per_thread[ThreadNum][i * BATCH_SIZE + BATCH_SIZE - 1];
                if (task->base.completion.load(std::memory_order_acquire) != DPK_ONGOING)
                {
                    // check the other tasks in the batch
                    for (int j = 0; j < BATCH_SIZE - 1; j++)
                    {
                        // find the tasks in the list for this batch
                        dpkernel_task *task_j = task_list_per_thread[ThreadNum][i * BATCH_SIZE + j];
                        if (task_j->base.completion.load(std::memory_order_acquire) == DPK_ONGOING)
                        {
                            // some still not yet finished
                            all_tasks_done = false;
                            break;
                        }
                    }
                    if (!all_tasks_done)
                    {
                        // not all tasks in this batch are done, continue to next iteration
                        continue;
                    }
                    else
                    {
                        // batch is done, we can send it back to the real client
                        task_done_counter_per_thread[ThreadNum][i] = BATCH_SIZE; // all tasks in this batch are done
                        inflight_batch_flags_per_thread[ThreadNum][i] = false; // mark this batch as no longer inflight
                        // printf("Thread %d: batch %d is done, sending back to client\n", ThreadNum, i);
                        batch_to_send = i; // set the batch to send back to the real client

                        // goto SEND_DECOMPRESSED_TO_CLIENT; // go send the decompressed values back to the real client
                        break;
                    }
                }
                /* int cnt = 0;
                for (int j = 0; j < BATCH_SIZE; j++)
                {
                    // find the tasks in the list for this batch
                    dpkernel_task *task = task_list_per_thread[ThreadNum][i * BATCH_SIZE + j];
                    if (task->base.completion.load(std::memory_order_acquire) != DPK_ONGOING)
                    {
                        cnt++;
                    }
                }
                task_done_counter_per_thread[ThreadNum][i] = cnt; // update the done counter for this batch */
            }
        }

    SEND_DECOMPRESSED_TO_CLIENT:
        // check if any batch is done, and send the decompressed values back to the real client
        if (batch_to_send == -1)
        {
            // no batch to send, continue to next iteration
            continue;
        }
        task_done_counter_per_thread[ThreadNum][batch_to_send] = 0;        // reset the done counter for this batch
        inflight_batch_flags_per_thread[ThreadNum][batch_to_send] = false; // mark this batch as no longer inflight
        u_int8_t *send_buf_batch_base =
            get_send_buffer_address(server_conn.send_buf, batch_to_send, MAX_SEND_BATCH_SIZE);
        // for every msg in the batch, copy the header from the server conn's recv buffer
        for (int j = 0; j < BATCH_SIZE; j++)
        {
            // copy the header from the server conn's recv buffer
            // each batch is MAX_RECV_BATCH_SIZE, so we can just copy the header from the recv buffer
            // at the corresponding index

            u_int8_t *send_buf = send_buf_batch_base + j * MAX_SEND_MSG_SIZE;

            MessageHeader *send_header = (MessageHeader *)(server_conn.recv_buf + batch_to_send * MAX_RECV_BATCH_SIZE +
                                                           j * sizeof(MessageHeader));
            // copy the header to the send buffer
            memcpy(send_buf, send_header, sizeof(MessageHeader));
            // sanity check that the batch id is the same
            if (send_header->BatchId != batch_to_send)
            {
                printf("Thread %d: ERROR: batch id mismatch, expected %d, got %d copied from server conn recv "
                       "buf\n",
                       ThreadNum, batch_to_send, send_header->BatchId);
            }
        }
        // rearm the server conn recv slot for the next batch
        rdma_post_one_recv_buffer(server_conn, MAX_RECV_BATCH_SIZE, batch_id_to_server_conn_recv_slot[batch_to_send]);
        // printf("Thread %d rearmed server conn recv slot %lu for batch %d\n", ThreadNum,
        //        batch_id_to_server_conn_recv_slot[i], i);
        // rearm the client conn recv slot for the next batch
        rdma_post_one_recv_buffer(client_conn, MAX_RECV_BATCH_SIZE, batch_id_to_client_conn_recv_slot[batch_to_send]);
        // printf("Thread %d rearmed client conn recv slot %lu for batch %d\n", ThreadNum,
        //        batch_id_to_client_conn_recv_slot[i], i);

        // send the batch back to the real client
        rdma_send_batch(server_conn, batch_to_send, MAX_SEND_BATCH_SIZE, MAX_SEND_BATCH_SIZE);
        batch_to_send = -1; // reset
        // printf("Thread %d sent decompressed batch %d back to client\n", ThreadNum, i);
        n_batches_done++;

        /* for (int i = 0; i < QUEUE_DEPTH; i++)
        {
            if (task_done_counter_per_thread[ThreadNum][i] == BATCH_SIZE) // every task in a batch completed
            {
                task_done_counter_per_thread[ThreadNum][i] = 0;        // reset the done counter for this batch
                inflight_batch_flags_per_thread[ThreadNum][i] = false; // mark this batch as no longer inflight
                // printf("Thread %d: batch %d is done, sending back to client\n", ThreadNum, i);
                // this batch is done, send it back to the real client
                u_int8_t *send_buf_batch_base = get_send_buffer_address(server_conn.send_buf, i, MAX_SEND_BATCH_SIZE);
                u_int8_t *send_buf = send_buf_batch_base + i * MAX_SEND_MSG_SIZE;

                // for every msg in the batch, copy the header from the server conn's recv buffer
                for (int j = 0; j < BATCH_SIZE; j++)
                {
                    // copy the header from the server conn's recv buffer
                    // each batch is MAX_RECV_BATCH_SIZE, so we can just copy the header from the recv buffer
                    // at the corresponding index
                    // memcpy(send_buf + j * MAX_SEND_MSG_SIZE, server_conn.recv_buf + i * MAX_RECV_BATCH_SIZE + j *
                    // MAX_RECV_MSG_SIZE, sizeof(MessageHeader));
                    int offset = j * MAX_READ_RECV_MSG_SIZE;
                    MessageHeader *send_header =
                        (MessageHeader *)(server_conn.recv_buf + i * MAX_RECV_BATCH_SIZE + j * sizeof(MessageHeader));
                    // copy the header to the send buffer
                    memcpy(send_buf + j * MAX_SEND_MSG_SIZE, send_header, sizeof(MessageHeader));
                    // sanity check that the batch id is the same
                    if (send_header->BatchId != i)
                    {
                        printf("Thread %d: ERROR: batch id mismatch, expected %d, got %d copied from server conn recv "
                               "buf\n",
                               ThreadNum, i, send_header->BatchId);
                    }
                }

                // rearm the server conn recv slot for the next batch
                rdma_post_one_recv_buffer(server_conn, MAX_RECV_BATCH_SIZE, batch_id_to_server_conn_recv_slot[i]);
                // printf("Thread %d rearmed server conn recv slot %lu for batch %d\n", ThreadNum,
                //        batch_id_to_server_conn_recv_slot[i], i);
                // rearm the client conn recv slot for the next batch
                rdma_post_one_recv_buffer(client_conn, MAX_RECV_BATCH_SIZE, batch_id_to_client_conn_recv_slot[i]);
                // printf("Thread %d rearmed client conn recv slot %lu for batch %d\n", ThreadNum,
                //        batch_id_to_client_conn_recv_slot[i], i);

                // send the batch back to the real client
                rdma_send_batch(server_conn, i, MAX_SEND_BATCH_SIZE, MAX_SEND_BATCH_SIZE);
                // printf("Thread %d sent decompressed batch %d back to client\n", ThreadNum, i);
                n_batches_done++;
            }
        } // end of SEND_DECOMPRESSED_TO_CLIENT */

        // if (reads_done >= ReadNum)
        if (n_batches_done >= n_batches)
        {
            over = true;
            // wait until send is done?
            ibv_wc wc;
            printf("Thread %d: waiting for last send completion\n", ThreadNum);
            while (true)
            {
                int c = ibv_poll_cq(server_conn.cq, 1, &wc);
                if (c > 0 && wc.status == IBV_WC_SUCCESS && wc.opcode == IBV_WC_SEND)
                {
                    // printf("Thread %d: send completed for batch %d\n", ThreadNum, wc.wr_id);
                    break; // exit the loop when we get a successful completion
                }
                else if (c < 0)
                {
                    printf("Thread %d: Error polling CQ: %s\n", ThreadNum, ibv_wc_status_str(wc.status));
                    break;
                }
            }
        }

    } // end while loop (main loop)

    // cleanup DPM memory
    std::vector<dpm_mem_req *> free_in_bufs_req;
    std::vector<dpm_mem_req *> free_out_bufs_req;

    for (int i = 0; i < QUEUE_DEPTH * BATCH_SIZE; ++i)
    {
        dpkernel_task *task = task_list_per_thread[ThreadNum][i];
        if (task != nullptr)
        {
            task->base.in = 0;
            task->base.out = 0;
            free_in_bufs_req.push_back(dpm_free_input_buf_async(task));
            free_out_bufs_req.push_back(dpm_free_output_buf_async(task));
        }
    }
    for (int i = 0; i < free_in_bufs_req.size(); i++)
    {
        dpm_wait_for_mem_req_completion(free_in_bufs_req[i]);
        dpm_wait_for_mem_req_completion(free_out_bufs_req[i]);
    }
    // printf("thread %d freed input and output buffers\n", ThreadNum);
    // printf("thread %d finished\n", ThreadNum);

    // free the contig chunks
    size_t total_recv_size = MAX_RECV_MSG_SIZE * BATCH_SIZE * QUEUE_DEPTH;
    size_t total_send_size = MAX_SEND_MSG_SIZE * BATCH_SIZE * QUEUE_DEPTH;
    dpkernel_task *large_chunk_task = nullptr;
    app_alloc_task_request(&large_chunk_task);
    large_chunk_task->base.in = app_get_shm_ptr_for_input_buf(contig_input_buf_per_thread[ThreadNum]);
    large_chunk_task->base.out = app_get_shm_ptr_for_output_buf(contig_output_buf_per_thread[ThreadNum]);
    large_chunk_task->base.in_size = total_recv_size;
    large_chunk_task->base.out_size = total_send_size;

    auto req = dpm_free_input_buf_async(large_chunk_task);
    dpm_wait_for_mem_req_completion(req);
    req = dpm_free_output_buf_async(large_chunk_task);
    dpm_wait_for_mem_req_completion(req);
    printf("Freed DPM buffers\n");
}

bool init_dpm_buffers()
{
    const int MAX_SEND_MSG_SIZE = sizeof(MessageHeader) + sizeof(UncompressedValue); // upsert
    const int MAX_RECV_MSG_SIZE = sizeof(MessageHeader) + sizeof(UncompressedValue); // read
    const int MAX_SEND_BATCH_SIZE = BATCH_SIZE * MAX_SEND_MSG_SIZE;
    const int MAX_RECV_BATCH_SIZE = BATCH_SIZE * MAX_RECV_MSG_SIZE;
    const int MAX_READ_RECV_MSG_SIZE = sizeof(MessageHeader) + sizeof(CompressedValue); // read response from server
    const int MAX_READ_SEND_MSG_SIZE = sizeof(MessageHeader) + sizeof(UncompressedValue);
    // const int MAX_UPSERT_RECV_MSG_SIZE = sizeof(MessageHeader) + sizeof(UncompressedValue);
    // const int MAX_UPSERT_SEND_MSG_SIZE = sizeof(MessageHeader) + sizeof(UncompressedValue);

    // initialize DPM
    dpm_frontend_initialize();

    // allocate large chunks of input and output buffers for each thread (DPM managed memory)
    contig_input_buf_per_thread.resize(N_THREADS);
    contig_output_buf_per_thread.resize(N_THREADS);
    task_list_per_thread.resize(N_THREADS);
    task_done_counter_per_thread.resize(N_THREADS);
    inflight_batch_flags_per_thread.resize(N_THREADS);

    for (int i = 0; i < N_THREADS; ++i)
    {
        int thread_id = i;
        // allocate contiguous chunks of dpm memory for recv buffers (input) and send buffers (output)
        size_t total_recv_size = MAX_RECV_MSG_SIZE * BATCH_SIZE * QUEUE_DEPTH;
        size_t total_send_size = MAX_SEND_MSG_SIZE * BATCH_SIZE * QUEUE_DEPTH;

        dpkernel_task *large_chunk_task = nullptr;
        if (!app_alloc_task_request(&large_chunk_task))
        {
            printf("Failed to allocate task for large chunk task\n");
            return -1;
        }

        struct dpm_mem_req *alloc_req = dpm_alloc_input_buf_async(total_recv_size, large_chunk_task);
        if (!dpm_wait_for_mem_req_completion(alloc_req))
        {
            printf("Failed to allocate input/recv buffer for thread %d\n", i);
            return -1;
        }
        alloc_req = dpm_alloc_output_buf_async(total_send_size, large_chunk_task);
        if (!dpm_wait_for_mem_req_completion(alloc_req))
        {
            printf("Failed to allocate output/send buffer for thread %d\n", i);
            return -1;
        }
        printf("Thread %d: Allocated contig chunk for input/recv and output/send buffers\n", i);

        contig_input_buf_per_thread[i] = app_get_input_ptr_from_shmptr(large_chunk_task->base.in);
        contig_output_buf_per_thread[i] = app_get_output_ptr_from_shmptr(large_chunk_task->base.out);

        // also allocate individual DPM buffers for each possible inflight task
        const int n_tasks_per_thread = QUEUE_DEPTH * BATCH_SIZE;
        task_list_per_thread[i].resize(n_tasks_per_thread);
        std::vector<dpm_mem_req *> in_bufs_req;
        std::vector<dpm_mem_req *> out_bufs_req;

        task_done_counter_per_thread[i].resize(QUEUE_DEPTH);
        inflight_batch_flags_per_thread[i].resize(QUEUE_DEPTH);

        for (int q = 0; q < QUEUE_DEPTH; ++q)
        {
            for (int b = 0; b < BATCH_SIZE; ++b)
            {
                dpkernel_task *alloc_task = nullptr;
                if (!app_alloc_task_request(&alloc_task))
                {
                    printf("Thread %d: Failed to allocate task %d\n", i, q * BATCH_SIZE + b);
                    return -1;
                }
                dpkernel_task_base *temp_task = &alloc_task->base;
                // temp_task->in_size = INPUT_SIZE;
                temp_task->in_size = sizeof(CompressedValue);
                temp_task->out_size = sizeof(UncompressedValue);
                temp_task->actual_out_size = 0;
                temp_task->task = dpm_task_name::TASK_DECOMPRESS_DEFLATE;
                temp_task->device = dpm_device::DEVICE_BLUEFIELD_3;
                ////
                char *this_input_buf_base = contig_input_buf_per_thread[thread_id] + q * MAX_RECV_BATCH_SIZE;

                // within a batch, everything is packed together of size MAX_READ_RECV_MSG_SIZE
                // also need to skip the message header at the beginning
                temp_task->in = app_get_shm_ptr_for_input_buf(this_input_buf_base + b * MAX_READ_RECV_MSG_SIZE +
                                                              sizeof(MessageHeader));

                char *this_output_buf_base = contig_output_buf_per_thread[thread_id] + q * MAX_SEND_BATCH_SIZE;
                // within a batch, everything is packed together of size MAX_SEND_MSG_SIZE
                // also need to skip the message header at the beginning
                temp_task->out = app_get_shm_ptr_for_output_buf(this_output_buf_base + b * MAX_SEND_MSG_SIZE +
                                                                sizeof(MessageHeader));
                ////

                auto in_req = dpm_alloc_input_buf_async(COMPRESSED_SIZE, alloc_task);
                if (in_req == NULL)
                {
                    printf("Thread %d: Failed to allocate input buf %d\n", thread_id, i);
                    exit(-1);
                }
                in_bufs_req.push_back(in_req);
                auto out_req = dpm_alloc_output_buf_async(UNCOMPRESSED_SIZE, alloc_task);
                if (out_req == NULL)
                {
                    printf("Thread %d: Failed to allocate output buf %d\n", thread_id, i);
                    exit(-1);
                }
                out_bufs_req.push_back(out_req);

                // task_list_per_thread[thread_id].push_back(alloc_task);//// DON'T PUSH BACK, IT'S ALREADY PREALLOCATED
                task_list_per_thread[thread_id][q * BATCH_SIZE + b] = alloc_task; // store the task in the list
            }
        }

        // wait for all mem reqs to be done
        for (auto req : in_bufs_req)
        {
            if (!dpm_wait_for_mem_req_completion(req))
            {
                printf("Thread %d: Failed to allocate input buf %d\n", thread_id, i);
                exit(-1);
            }
        }
        for (auto req : out_bufs_req)
        {
            if (!dpm_wait_for_mem_req_completion(req))
            {
                printf("Thread %d: Failed to allocate output buf %d\n", thread_id, i);
                exit(-1);
            }
        }
        printf("Thread %d: Allocated %d tasks and corresponding in/out buffers\n", thread_id, n_tasks_per_thread);
    }
    printf("Allocated DPM managed mem for input/recv and output/send buffer for all threads\n");
    return true;
}

int main(int argc, char *argv[])
{
    const int MAX_SEND_MSG_SIZE = sizeof(MessageHeader) + sizeof(UncompressedValue); // upsert
    const int MAX_RECV_MSG_SIZE = sizeof(MessageHeader) + sizeof(UncompressedValue); // read
    const int MAX_SEND_BATCH_SIZE = BATCH_SIZE * MAX_SEND_MSG_SIZE;
    const int MAX_RECV_BATCH_SIZE = BATCH_SIZE * MAX_RECV_MSG_SIZE;
    const int MAX_READ_RECV_MSG_SIZE = sizeof(MessageHeader) + sizeof(CompressedValue); // read response from server
    const int MAX_READ_SEND_MSG_SIZE = sizeof(MessageHeader) + sizeof(UncompressedValue);

    uint64_t per_thread_read_num = LOAD_N_KEYS / N_THREADS;

    init_dpm_buffers();

    server_threads_connections_for_dpu.resize(N_THREADS);
    client_threads_connections_for_dpu.resize(N_THREADS);
    // connect to the actual server
    if (!setup_rdma_connections_dpu_client(N_THREADS, PORT_BASE, QUEUE_DEPTH, BATCH_SIZE, MAX_SEND_MSG_SIZE,
                                           MAX_RECV_MSG_SIZE, client_threads_connections_for_dpu))
    {
        printf("Failed to setup client connections to server.\n");
        return 1;
    }
    printf("connected to FASTER server using %d threads\n", N_THREADS);

    // setup and wait for actual clients to connect to us
    server_threads_connections_for_dpu.resize(N_THREADS);
    if (!setup_rdma_connections_dpu_server(N_THREADS, DPU_PORT_BASE, BATCH_SIZE, QUEUE_DEPTH,
                                           server_threads_connections_for_dpu))
    {
        printf("Failed to setup server connections to client.\n");
        return 1;
    }

    // try posting all recv for dpu server threads
    for (int i = 0; i < N_THREADS; ++i)
    {
        printf("Posting all recv buffers for thread %d\n", i);
        rdma_post_all_recv_buffers_in_queue(server_threads_connections_for_dpu[i], QUEUE_DEPTH,
                                            BATCH_SIZE * MAX_RECV_MSG_SIZE);
        // post all receive buffers as client too
        rdma_post_all_recv_buffers_in_queue(client_threads_connections_for_dpu[i], QUEUE_DEPTH,
                                            BATCH_SIZE * MAX_RECV_MSG_SIZE);
    }
    printf("All buffers posted. Starting worker threads\n");

    // 2: Dispatch worker threads
    std::vector<thread> threads;
    for (int i = 0; i < N_THREADS; ++i)
    {
        threads.emplace_back([&, i]() {
            dpu_thread_func(server_threads_connections_for_dpu[i], client_threads_connections_for_dpu[i], i, BATCH_SIZE,
                            per_thread_read_num, QUEUE_DEPTH);
        });
    }
    for (auto &t : threads)
        t.join();

    return 0;
}

bool setup_rdma_dpu_server_qp_and_buffers(rdma_cm_id *id, RDMAConnectionResource &conn, int QueueDepth, int BatchSize,
                                          int MaxSendMsgSize, int MaxRecvMsgSize, int thread_idx)
{
    conn.pd = ibv_alloc_pd(conn.cm_id->verbs);
    conn.cq = ibv_create_cq(conn.cm_id->verbs, QueueDepth * 2, nullptr, nullptr, 0);
    if (!conn.pd || !conn.cq)
    {
        cout << "Error allocating PD or creating CQ " << endl;
        return 1;
    }

    ibv_qp_init_attr qp_attr = {};
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.send_cq = conn.cq;
    qp_attr.recv_cq = conn.cq;
    qp_attr.cap.max_send_wr = QueueDepth;
    qp_attr.cap.max_recv_wr = QueueDepth;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    if (rdma_create_qp(conn.cm_id, conn.pd, &qp_attr))
    {
        cout << "Error creating QP: " << strerror(errno) << endl;
        ibv_destroy_cq(conn.cq);
        return false;
    }

    // set the connection's protection domain and completion queue, otherwise SEGFAULT
    // conn.pd = id->qp->pd;
    // conn.cq = cq;

    // allocate & register buffers
    int TotalRecvSize = MaxRecvMsgSize * BatchSize * QueueDepth; // recv batches of MessageHeader (requests)
    int TotalSendSize = MaxSendMsgSize * BatchSize * QueueDepth; // send batches of MessageHeader + ResponseType
    // as a server, the send buffer sends back decompressed values, so we need DPM managed memory
    conn.send_buf = (char *)contig_output_buf_per_thread[thread_idx];
    // as a server, the recv buffer receives client requests (headers), so no need for DPM managed memory
    conn.recv_buf = (char *)::aligned_alloc(4096, TotalRecvSize);
    if (!conn.send_buf || !conn.recv_buf)
    {
        cout << "Error allocating buffers: " << strerror(errno) << endl;
        rdma_destroy_qp(id);
        ibv_destroy_cq(conn.cq);
        return false;
    }
    memset(conn.recv_buf, 0, TotalRecvSize);
    memset(conn.send_buf, 0, TotalSendSize);

    conn.send_mr = ibv_reg_mr(conn.pd, conn.send_buf, TotalSendSize,
                              IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    conn.recv_mr = ibv_reg_mr(conn.pd, conn.recv_buf, TotalRecvSize,
                              IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!conn.send_mr || !conn.recv_mr)
    {
        cout << "Error registering MR: " << strerror(errno) << endl;
        free(conn.send_buf);
        free(conn.recv_buf);
        rdma_destroy_qp(id);
        ibv_destroy_cq(conn.cq);
        return false;
    }

    return true;
}