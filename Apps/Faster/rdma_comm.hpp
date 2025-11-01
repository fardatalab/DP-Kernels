#pragma once
#include "faster_bench_common.hpp"
#include <arpa/inet.h>
#include <cstdint>
#include <cstdio>
#include <cwchar>
#include <infiniband/verbs.h>
#include <iostream>
#include <netinet/in.h>
#include <rdma/rdma_cma.h>
#include <sys/types.h>
#include <vector>

using namespace std;
using namespace std::chrono;

#define SERVER_IP "10.10.1.101"
// #define SERVER_PORT 10000

#define DPU_IP "10.10.1.11"
#define DPU_PORT_BASE 10001

#define TIMEOUT_IN_MS 500
#define RECV_WRID_BASE 1000000

#define QUEUE_DEPTH 64 // 64 for 4K
#define BATCH_SIZE 32  // 32 for 4K

struct RDMAConnectionResource
{
    rdma_cm_id *cm_id;
    ibv_pd *pd;
    ibv_cq *cq;
    ibv_mr *send_mr;
    ibv_mr *recv_mr;
    char *send_buf;
    char *recv_buf;
};

struct RDMAConnectionContext
{
    uint64_t remote_addr;
    uint32_t rkey;
};

struct MessageHeader
{
    uint8_t Operation;
    uint16_t BatchId;
    long long TimeSend;
    uint64_t Key;
    // uint64_t Value;
};

// extern rdma_event_channel *ec;

extern vector<RDMAConnectionResource> client_threads_connections;
extern vector<RDMAConnectionResource> server_threads_connections;
extern vector<RDMAConnectionResource> client_threads_connections_for_dpu; // for DPU threads acting as clients
extern vector<RDMAConnectionResource> server_threads_connections_for_dpu; // for DPU threads

extern vector<char *> contig_input_buf_per_thread;  // for DPU threads, each thread has its own input buffer
extern vector<char *> contig_output_buf_per_thread; // for DPU threads, each thread has its own output buffer

extern rdma_cm_id *listen_id;
extern rdma_event_channel *client_ec;
extern rdma_event_channel *server_ec;

bool setup_rdma_connections_client(int NumConnections, int PortBase, int QueueDepth, int BatchSize, int MaxSendMsgSize,
                                   int MaxRecvMsgSize, vector<RDMAConnectionResource> &conns);

bool setup_rdma_connections_server(int NumConnections, int PortBase, int BatchSize, int QueueDepth,
                                   vector<RDMAConnectionResource> &conns);
bool cleanup_rdma();

int rdma_send_batch(RDMAConnectionResource &conn, int slot_index, u_int32_t max_batch_size, u_int32_t data_size);
int rdma_post_all_recv_buffers_in_queue(RDMAConnectionResource &conn, int queue_depth, u_int32_t data_size);
int rdma_post_one_recv_buffer(RDMAConnectionResource &conn, u_int32_t data_size, uint64_t wr_id);

u_int8_t *rdma_recv(RDMAConnectionResource &conn, int slot_index, u_int32_t data_size);

/// wait for a receive cqe, IGNORES any other completions (like sends), since we don't care or have anything  special to
/// do
/// @return wr_id of the completed receive
inline uint64_t rdma_wait_recv_completion(RDMAConnectionResource &conn)
{
    ibv_wc wc;

    while (true)
    {
        while (ibv_poll_cq(conn.cq, 1, &wc) < 1 || wc.status != IBV_WC_SUCCESS)
            ;

        // sanity check
        if (wc.status != IBV_WC_SUCCESS)
        {
            std::cerr << "rdma_wait_recv_completion: ibv_poll_cq failed with status " << ibv_wc_status_str(wc.status)
                      << std::endl;
            return -1;
        }

        if (wc.opcode == IBV_WC_RECV)
        {
            // print the received size
            // printf("rdma_wait_recv_completion: received completion with wr_id %lu, size %u\n", wc.wr_id,
            // wc.byte_len);
            break; // found a receive completion
        }
        // else we don't care (about sends)
    }

    return wc.wr_id;
}

/// still ignores any other completions (like sends), since we don't care or have anything special to do
/// But, we if there's no receive completion, we return -1 instead of blocking indefinitely
inline uint64_t try_rdma_wait_recv_completion(RDMAConnectionResource &conn)
{
    ibv_wc wc;

    while (true)
    {
        int c = ibv_poll_cq(conn.cq, 1, &wc);

        if (c == 0)
        {
            // printf("try_rdma_wait_recv_completion: no completion available, return -1\n");
            return -1; // no completion available, return -1
        }

        // sanity check
        if (wc.status != IBV_WC_SUCCESS)
        {
            std::cerr << "rdma_wait_recv_completion: ibv_poll_cq failed with status " << ibv_wc_status_str(wc.status)
                      << std::endl;
            return -1;
        }

        if (wc.opcode == IBV_WC_RECV)
        {
            // print the received size
            // printf("rdma_wait_recv_completion: received completion with wr_id %lu, size %u\n", wc.wr_id,
            // wc.byte_len);
            return wc.wr_id; // found a receive completion
        }
        // else we don't care (about sends)
    }

    // return wc.wr_id;
}

/// given a slot index (position in the queue up to QueueDepth), get a unique receive work request ID, just the slot
/// index itself will do
static inline uint64_t generate_recv_wr_id(int slot_index)
{
    return static_cast<uint64_t>(slot_index);
}
static inline uint64_t generate_send_wr_id(int slot_index)
{
    return static_cast<uint64_t>(slot_index);
}

static inline int get_index_from_wr_id(uint64_t wr_id)
{
    return static_cast<int>(wr_id);
}

static inline u_int8_t *get_recv_buffer_address(u_int8_t *recv_buf_base, int slot_index, int message_size)
{
    return recv_buf_base + (static_cast<uintptr_t>(slot_index) * message_size);
}
static inline u_int8_t *get_recv_buffer_address(char *recv_buf_base, int slot_index, int message_size)
{
    return reinterpret_cast<u_int8_t *>(recv_buf_base + (static_cast<uintptr_t>(slot_index) * message_size));
}

static inline u_int8_t *get_send_buffer_address(u_int8_t *send_buf_base, int slot_index, int message_size)
{
    return send_buf_base + (static_cast<uintptr_t>(slot_index) * message_size);
}
static inline u_int8_t *get_send_buffer_address(char *send_buf_base, int slot_index, int message_size)
{
    return reinterpret_cast<u_int8_t *>(send_buf_base + (static_cast<uintptr_t>(slot_index) * message_size));
}

inline u_int8_t *get_send_buffer(int thread_idx, int idx)
{
    // if (thread_idx < 0 || thread_idx >= connections.size())
    // {
    //     return nullptr;
    // }
    auto &conn = client_threads_connections[thread_idx];
    return reinterpret_cast<u_int8_t *>(conn.send_buf);
}

/* inline int rdma_post_recv_buf(RDMAConnectionResource conn, int slot_index, uint32_t message_size)
{
    ibv_sge recv_sge = {
        .addr = (uintptr_t)get_recv_buffer_address(conn.recv_buf, slot_index, message_size),
        .length = message_size,
        .lkey = conn.recv_mr->lkey,
    };

    ibv_recv_wr recv_wr = {
        .wr_id = generate_recv_wr_id(slot_index),
        .sg_list = &recv_sge,
        .num_sge = 1,
    };

    ibv_recv_wr *bad_recv_wr;
    if (ibv_post_recv(conn.cm_id->qp, &recv_wr, &bad_recv_wr))
    {
        // std::cerr << "Failed to post recv WR for slot " << slot_index << std::endl;
        return -1;
    }
    return 0;
} */

/// for each slot in the queue, post a receive work request
/* inline int rdma_post_all_recv_buffers_in_queue(int thread_idx, int QueueDepth, uint32_t total_message_size)
{
    const RDMAConnectionResource &conn = connections[thread_idx];
    for (int i = 0; i < QueueDepth; i++)
    {
        if (rdma_post_recv_buf(conn, i, total_message_size))
        {
            std::cerr << "Failed to post recv WR for slot " << i << std::endl;
            return -1;
        }
    }
    return 0;
} */

bool setup_rdma_server_qp_and_buffers(rdma_cm_id *id, RDMAConnectionResource &conn, int QueueDepth, int BatchSize,
                                      int MaxSendMsgSize, int MaxRecvMsgSize);