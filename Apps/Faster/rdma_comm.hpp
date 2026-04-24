#include "common.hpp"
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <iostream>
#include <netinet/in.h>
#include <rdma/rdma_cma.h>
#include <vector>

using namespace std;
using namespace std::chrono;

#define SERVER_IP "10.10.1.101"

#define TIMEOUT_IN_MS 500
#define RECV_WRID_BASE 1000000
#define SEND_WRID(thread, idx) ((thread) * 1000 + (idx))
#define RECV_WRID(thread, slot) (RECV_WRID_BASE + (thread) * 1000 + (slot))
#define RECV_SLOT_FROM_WRID(wrid, QueueDepth) (((wrid) - RECV_WRID_BASE) % (QueueDepth))

#define QUEUE_DEPTH 16
#define BATCH_SIZE 4

struct RDMAClientResource
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
    long long TimeSend;
    uint16_t BatchId;
    uint16_t FileId;
    uint64_t Offset;
    int Length;
};

extern rdma_event_channel *ec;
extern vector<RDMAClientResource> connections;

bool setup_rdma_connections(int NumConnections, int PortBase, int QueueDepth, int BatchSize, uint64_t ReadNum,
                            int SendMsgSize, int RecvMsgSize);
bool cleanup_rdma();