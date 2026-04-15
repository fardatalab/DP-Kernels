#include "rdma_comm.hpp"
#include "latency_helpers.hpp"
#include <cstdint>
#include <cstring>

rdma_event_channel *ec;
vector<RDMAClientResource> connections;

/// setup RDMA connections for each thread
bool setup_rdma_connections(int NumConnections, int PortBase, int QueueDepth, int BatchSize, uint64_t ReadNum,
                            int SendMsgSize, int RecvMsgSize)
{
    // NumConnections qp
    connections = vector<RDMAClientResource>(NumConnections);
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

        conn.send_buf = (char *)std::aligned_alloc(4096, SendMsgSize * BatchSize);
        conn.recv_buf = (char *)std::aligned_alloc(4096, RecvMsgSize * BatchSize);
        // memset(conn.send_buf, 0, HeaderSize * BatchSize);
        memset(conn.send_buf, 0, SendMsgSize * BatchSize);
        // memset(conn.recv_buf, 0, RecvMessageSize);
        memset(conn.recv_buf, 0, RecvMsgSize * BatchSize);

        // conn.send_mr = ibv_reg_mr(conn.pd, conn.send_buf, SendMessageSize, IBV_ACCESS_LOCAL_WRITE);
        conn.send_mr = ibv_reg_mr(conn.pd, conn.send_buf, SendMsgSize * BatchSize, IBV_ACCESS_LOCAL_WRITE);
        conn.recv_mr = ibv_reg_mr(conn.pd, conn.recv_buf, RecvMsgSize * BatchSize,
                                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

        RDMAConnectionContext local_ctx = {.remote_addr = reinterpret_cast<uint64_t>(conn.recv_buf),
                                           .rkey = conn.recv_mr->rkey};

        rdma_conn_param conn_param = {};
        conn_param.initiator_depth = QueueDepth;
        conn_param.responder_resources = QueueDepth;
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

bool cleanup_rdma()
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