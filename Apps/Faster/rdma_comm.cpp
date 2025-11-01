#include "rdma_comm.hpp"
// #include "latency_helpers.hpp"
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sys/types.h>
#include <vector>

vector<RDMAConnectionResource> client_threads_connections;
vector<RDMAConnectionResource> server_threads_connections;
vector<RDMAConnectionResource> client_threads_connections_for_dpu; // for DPU threads acting as clients
vector<RDMAConnectionResource> server_threads_connections_for_dpu; // for DPU threads acting as servers

rdma_event_channel *client_ec;
rdma_event_channel *server_ec;
rdma_cm_id *listen_id;

thread_local uint64_t recv_wr_id = 0; // thread-local variable to track the next wr id to use for receiving messages
thread_local uint64_t send_wr_id = 0; // thread-local variable to track the next wr id to use for sending messages

/// setup RDMA connections for each thread -- server side
bool setup_rdma_connections_server(int NumConnections, int PortBase, int BatchSize, int QueueDepth,
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

        if (!setup_rdma_server_qp_and_buffers(conn_id, res, QueueDepth, BatchSize, max_send_msg_size,
                                                max_recv_msg_size)) // upsert is message header + value
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

/// setup RDMA connections for each thread -- client side
bool setup_rdma_connections_client(int NumConnections, int PortBase, int QueueDepth, int BatchSize, int MaxSendMsgSize,
                                   int MaxRecvMsgSize, vector<RDMAConnectionResource> &conns)
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
        // inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
        inet_pton(AF_INET, DPU_IP, &addr.sin_addr);

        rdma_resolve_addr(conn.cm_id, nullptr, (sockaddr *)&addr, TIMEOUT_IN_MS);
        rdma_get_cm_event(client_ec, &event);
        if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
        {
            cout << "Error resolving address for connection " << i << endl;
            return 1;
        }
        rdma_ack_cm_event(event);

        rdma_resolve_route(conn.cm_id, TIMEOUT_IN_MS);
        rdma_get_cm_event(client_ec, &event);
        if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
        {
            cout << "Error resolving route for connection " << i << endl;
            return 1;
        }
        rdma_ack_cm_event(event);

        conn.pd = ibv_alloc_pd(conn.cm_id->verbs);
        conn.cq = ibv_create_cq(conn.cm_id->verbs, QueueDepth * 2, nullptr, nullptr, 0);
        if (!conn.pd || !conn.cq)
        {
            cout << "Error allocating PD or creating CQ for connection " << i << endl;
            return 1;
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

        conn.send_buf = (char *)std::aligned_alloc(4096, MaxSendMsgSize * BatchSize * QueueDepth);
        conn.recv_buf = (char *)std::aligned_alloc(4096, MaxRecvMsgSize * BatchSize * QueueDepth);
        memset(conn.send_buf, 0, MaxSendMsgSize * BatchSize * QueueDepth);
        memset(conn.recv_buf, 0, MaxRecvMsgSize * BatchSize * QueueDepth);

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
            return 1;
        }
        rdma_ack_cm_event(event);
    }

    return true;
}

bool cleanup_rdma_client()
{
    // clean up RDMA
    for (auto &conn : client_threads_connections)
    {
        rdma_disconnect(conn.cm_id);
        rdma_destroy_qp(conn.cm_id);
        rdma_destroy_id(conn.cm_id);
        ibv_dereg_mr(conn.send_mr);
        ibv_dereg_mr(conn.recv_mr);
        free(conn.send_buf);
        free(conn.recv_buf);
        ibv_destroy_cq(conn.cq);
        ibv_dealloc_pd(conn.pd);
    }
    rdma_destroy_event_channel(client_ec);

    return true;
}

/// this assumes that the send buffer is already populated
/// DOES NOT include poll waiting until the send is complete, just post the send
/// @param data_size this size should be for the full batch, not per message
int rdma_send_batch(RDMAConnectionResource &conn, int slot_index, u_int32_t max_batch_size, u_int32_t data_size)
{
    ibv_send_wr *bad_wr = nullptr;
    // ibv_wc wc;

    uint64_t send_buf_address =
        (uint64_t)get_send_buffer_address((u_int8_t *)conn.send_buf, slot_index, max_batch_size);

    ibv_sge sge = {.addr = send_buf_address, .length = data_size, .lkey = conn.send_mr->lkey};

    ibv_send_wr wr = {.wr_id = generate_send_wr_id(slot_index),
                      .sg_list = &sge,
                      .num_sge = 1,
                      .opcode = IBV_WR_SEND,
                      .send_flags = IBV_SEND_SIGNALED};

    ibv_post_send(conn.cm_id->qp, &wr, &bad_wr);
    if (bad_wr)
    {
        printf("rdma_send ibv_post_send failed with bad_wr: wr_id = %lu, addr = %p, length = %u\n", bad_wr->wr_id,
               (void *)bad_wr->sg_list->addr, bad_wr->sg_list->length);
        return -1;
    }

    return 0;
}

/// for each slot in the queue, post a receive work request
/// wr_id IS the slot index in the queue, so can be used to identify the slot
int rdma_post_all_recv_buffers_in_queue(RDMAConnectionResource &conn, int queue_depth, u_int32_t data_size)
{
    for (uint64_t i = 0; i < queue_depth; ++i)
    {
        ibv_sge sge = {
            .addr = (uintptr_t)(conn.recv_buf + i * data_size),
            .length = data_size,
            .lkey = conn.recv_mr->lkey
        };

        ibv_recv_wr wr = {.wr_id = i, .sg_list = &sge, .num_sge = 1};

        ibv_recv_wr *bad_wr;
        if (ibv_post_recv(conn.cm_id->qp, &wr, &bad_wr))
        {
            printf("rdma_post_all_recv_buffers_in_queue: ibv_post_recv failed with bad_wr %p\n", bad_wr);
            return -1;
        }
        /* printf("rdma_post_all_recv_buffers_in_queue: posted recv buffer for slot %lu, addr = %p\n", i,
               (conn.recv_buf + i * data_size)); */
    }
    return 0;
}

/// again, wr_id IS the slot index in the queue
int rdma_post_one_recv_buffer(RDMAConnectionResource &conn, u_int32_t data_size, uint64_t wr_id)
{
    ibv_sge sge = {
        .addr = (uintptr_t)conn.recv_buf + wr_id * data_size, 
        .length = data_size, 
        .lkey = conn.recv_mr->lkey
    };
    ibv_recv_wr wr = {
        .wr_id = wr_id, 
        .sg_list = &sge,
        .num_sge = 1
    };
    ibv_recv_wr *bad_wr;
    if (ibv_post_recv(conn.cm_id->qp, &wr, &bad_wr))
    {
        printf("rdma_post_one_recv_buffer: ibv_post_recv failed with bad_wr %p\n", bad_wr);
        return -1;
    }
    // printf("rdma_post_one_recv_buffer: posted recv buffer for slot %lu, size = %u\n", wr_id, data_size);
    return 0;
}

/// given a slot in the queue, post a receive for that slot.
/// DOES NOT include poll waiting until the receive is complete, just post the receive
/// @param data_size this size should be for the full batch, not per message
/// @return the buffer address where received data is to be found, DON'T access it until the receive is complete
u_int8_t *rdma_recv(RDMAConnectionResource &conn, int slot_index, u_int32_t data_size)
{
    ibv_recv_wr *bad_wr = nullptr;
    ibv_wc wc;

    uint64_t recv_buf_address = (uint64_t)get_recv_buffer_address((u_int8_t *)conn.recv_buf, slot_index, data_size);
    ibv_sge sge = {.addr = recv_buf_address, .length = data_size, .lkey = conn.recv_mr->lkey};

    ibv_recv_wr wr = {.wr_id = generate_recv_wr_id(slot_index), .sg_list = &sge, .num_sge = 1};

    ibv_post_recv(conn.cm_id->qp, &wr, &bad_wr);
    if (bad_wr)
    {
        printf("rdma_recv: ibv_post_recv failed with bad_wr %p\n", bad_wr);
        return NULL;
    }

    return (u_int8_t *)recv_buf_address;
}

bool setup_rdma_server_qp_and_buffers(rdma_cm_id *id, RDMAConnectionResource &conn, int QueueDepth, int BatchSize,
                                      int MaxSendMsgSize, int MaxRecvMsgSize)
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
    conn.send_buf = (char *)::aligned_alloc(4096, TotalSendSize);
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
