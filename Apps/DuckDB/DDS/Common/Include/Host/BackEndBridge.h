#pragma once

/* libibverbs specific */
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#include "MsgTypes.h"
#include "Protocol.h"
// #include "RDMC.h"

#define BACKEND_TYPE_IN_MEMORY 1
#define BACKEND_TYPE_DMA 2
#define BACKEND_TYPE BACKEND_TYPE_DMA
#define RESOLVE_TIMEOUT_MS 2000

#define BACKEND_BRIDGE_VERBOSE

//
// Connector that fowards requests to and receives responses from the back end
//
//
class BackEndBridge {
public:
    //
    // Back end configuration
    //
    //
    char BackEndAddr[16];
    unsigned short BackEndPort;
    struct sockaddr_in BackEndSock;

    //
    // RDMA configuration
    //
    //
    struct rdma_event_channel *ec;      // Event channel
    struct rdma_cm_id *cm_id;           // Communication identifier
    struct ibv_pd *pd;                  // Protection domain
    struct ibv_cq *cq;                  // Completion queue
    struct ibv_qp *qp;                  // Queue pair
    struct ibv_mr *mr;                  // Memory region
    
    // Queue configuration
    size_t QueueDepth;
    size_t MaxSge;
    size_t InlineThreshold;

    // SGE and message buffer
    struct ibv_sge *CtrlSgl;            // Scatter-gather element
    struct ibv_sge send_sge;            // Send SGE
    struct ibv_sge recv_sge;            // Receive SGE
    char CtrlMsgBuf[CTRL_MSG_SIZE];     // Control message buffer

    int ClientId;

public:
    BackEndBridge();

    //
    // Connect to the backend
    //
    //
    bool
    Connect();

    bool
    ConnectTest();

    //
    // Disconnect from the backend
    //
    //
    void
    Disconnect();
};
