#include "rdma_comm.hpp"
#include "latency_helpers.hpp"
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sys/types.h>

rdma_event_channel *ec;
vector<RDMAConnectionResource> connections;

thread_local uint64_t recv_wr_id = 0; // thread-local variable to track the next wr id to use for receiving messages
thread_local uint64_t send_wr_id = 0; // thread-local variable to track the next wr id to use for sending messages

/// setup RDMA connections for each thread
bool setup_rdma_connections_client(int NumConnections, int PortBase, int QueueDepth, int BatchSize, uint64_t ReadNum,
                            int SendMsgSize, int RecvMsgSize)
{
    // NumConnections qp
    connections = vector<RDMAConnectionResource>(NumConnections);
    ec = rdma_create_event_channel();
    rdma_cm_event *event;

    // 1: Connect all qp
    for (int i = 0; i < NumConnections; ++i)
    {
        auto &conn = connections[i];
        rdma_create_id(ec, &conn.cm_id, nullptr, RDMA_PS_TCP);

        sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(PortBase);
        inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);

        rdma_resolve_addr(conn.cm_id, nullptr, (sockaddr *)&addr, TIMEOUT_IN_MS);
        rdma_get_cm_event(ec, &event);
        if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
        {
            cout << "Error resolving address for connection " << i << endl;
            return 1;
        }
        rdma_ack_cm_event(event);

        rdma_resolve_route(conn.cm_id, TIMEOUT_IN_MS);
        rdma_get_cm_event(ec, &event);
        if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
        {
            cout << "Error resolving route for connection " << i << endl;
            return 1;
        }
        rdma_ack_cm_event(event);

        conn.pd = ibv_alloc_pd(conn.cm_id->verbs);
        conn.cq = ibv_create_cq(conn.cm_id->verbs, QueueDepth, nullptr, nullptr, 0);
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

        // const int HeaderSize = sizeof(MessageHeader);
        // int SendMessageSize = HeaderSize * BatchSize;
        // int RecvMessageSize = (HeaderSize + sizeof(uint64_t)) * BatchSize;

        conn.send_buf = (char *)std::aligned_alloc(4096, SendMsgSize * BatchSize * QueueDepth);
        conn.recv_buf = (char *)std::aligned_alloc(4096, RecvMsgSize * BatchSize * QueueDepth);
        // memset(conn.send_buf, 0, HeaderSize * BatchSize);
        memset(conn.send_buf, 0, SendMsgSize * BatchSize * QueueDepth);
        // memset(conn.recv_buf, 0, RecvMessageSize);
        memset(conn.recv_buf, 0, RecvMsgSize * BatchSize * QueueDepth);

        // conn.send_mr = ibv_reg_mr(conn.pd, conn.send_buf, SendMessageSize, IBV_ACCESS_LOCAL_WRITE);
        conn.send_mr = ibv_reg_mr(conn.pd, conn.send_buf, SendMsgSize * BatchSize * QueueDepth, IBV_ACCESS_LOCAL_WRITE);
        conn.recv_mr = ibv_reg_mr(conn.pd, conn.recv_buf, RecvMsgSize * BatchSize * QueueDepth,
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
        rdma_get_cm_event(ec, &event);
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
    for (auto &conn : connections)
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
    rdma_destroy_event_channel(ec);

    return true;
}

/// this assumes that the send buffer is already populated
/// Also includes poll waiting until the send is complete
int rdma_send(int thread_idx, u_int32_t data_size, int wr_id)
{
    ibv_send_wr *bad_wr = nullptr;
    ibv_wc wc;

    RDMAConnectionResource &conn = connections[thread_idx];
    ibv_sge sge = {
        .addr = (uintptr_t)conn.send_buf,
        .length = data_size, 
        .lkey = conn.send_mr->lkey
    };

    ibv_send_wr wr = {.wr_id = wr_id, // never needed? // SEND_WRID(ThreadNum, msgIndex),
                      .sg_list = &sge,
                      .num_sge = 1,
                      .opcode = IBV_WR_SEND,
                      .send_flags = IBV_SEND_SIGNALED};

    ibv_post_send(conn.cm_id->qp, &wr, &bad_wr);
    if (bad_wr)
    {
        printf("rdma_send: ibv_post_send failed with bad_wr %p\n", bad_wr);
        return -1;
    }

    return 0;
}

int rdma_post_all_recv_buffers_in_queue(int thread_idx, int queue_depth, u_int32_t data_size)
{
    RDMAConnectionResource &conn = connections[thread_idx];
    for (int i = 0; i < queue_depth; ++i)
    {
        ibv_sge sge = {
            .addr = (uintptr_t)conn.recv_buf + i * data_size,
            .length = data_size,
            .lkey = conn.recv_mr->lkey
        };

        ibv_recv_wr wr = {
            .wr_id = i,
            .sg_list = &sge,
            .opcode = IBV_WR_RECV,
            .num_sge = 1
        };

        ibv_recv_wr *bad_wr;
        if (ibv_post_recv(conn.cm_id->qp, &wr, &bad_wr))
        {
            printf("rdma_post_all_recv_buffers_in_queue: ibv_post_recv failed with bad_wr %p\n", bad_wr);
            return -1;
        }
    }
    return 0;
}

int rdma_post_one_recv_buffer(int thread_idx, int data_size, int wr_id) {
    RDMAConnectionResource &conn = connections[thread_idx];
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
    return 0;
}

/// given a slot in the queue, post a receive for that slot
/// @return the buffer address where received data is to be found
u_int8_t *rdma_recv(int thread_idx, int slot_index, u_int32_t data_size)
{
    ibv_recv_wr *bad_wr = nullptr;
    ibv_wc wc;

    RDMAConnectionResource &conn = connections[thread_idx];
    uint64_t recv_buf_address = (uint64_t)get_recv_buffer_address((u_int8_t *)conn.recv_buf, slot_index, data_size);
    ibv_sge sge = {.addr = recv_buf_address, .length = data_size, .lkey = conn.recv_mr->lkey};

    ibv_recv_wr wr = {.wr_id = generate_recv_wr_id(slot_index), .sg_list = &sge, .num_sge = 1};

    ibv_post_recv(conn.cm_id->qp, &wr, &bad_wr);
    if (bad_wr)
    {
        printf("rdma_recv: ibv_post_recv failed with bad_wr %p\n", bad_wr);
        return NULL;
    }
    while (ibv_poll_cq(conn.cq, 1, &wc) < 1 || wc.status != IBV_WC_SUCCESS)
        ;
    if (wc.status != IBV_WC_SUCCESS)
    {
        printf("rdma_recv: ibv_poll_cq failed with status %s\n", ibv_wc_status_str(wc.status));
        return NULL;
    }
    return (u_int8_t *)recv_buf_address;
}
