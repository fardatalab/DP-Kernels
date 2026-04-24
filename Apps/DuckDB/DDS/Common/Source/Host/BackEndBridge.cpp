#include <string.h>

/* libibverbs specific*/
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <arpa/inet.h>
#include <errno.h>
#include <infiniband/verbs.h>

#include "BackEndBridge.h"

BackEndBridge::BackEndBridge() {
    //
    // Record buffer capacity and back end address and port
    //
    //
    strcpy(BackEndAddr, DDS_BACKEND_ADDR);
    BackEndPort = DDS_BACKEND_PORT;
    memset(&BackEndSock, 0, sizeof(BackEndSock));

    //
    // Initialize NDSPI variables
    //
    //
    ec = NULL;
    cm_id = NULL;
    pd = NULL;
    cq = NULL;
    qp = NULL;
    mr = NULL;
    send_sge = NULL;
    recv_sge = NULL;
    memset(&BackEndSock, 0, sizeof(BackEndSock));

    CtrlSgl = NULL;
    memset(CtrlMsgBuf, 0, CTRL_MSG_SIZE);

    ClientId = -1;
}

/* Clean up logic */
void BackEndBridge::cleanup() {
    if (mr) {
        ibv_dereg_mr(mr);
    }
    if (qp) {
        ibv_destroy_qp(qp);
    }
    if (cq) {
        ibv_destroy_cq(cq);
    }
    if (pd) {
        ibv_dealloc_pd(pd);
    }
    if (cm_id) {
        rdma_destroy_id(cm_id);
    }
    if (ec) {
        rdma_destroy_event_channel(ec);
    }
    if (CtrlSgl) {
        delete[] CtrlSgl;
    }
}

//
// Connect to the backend
//
//
bool
BackEndBridge::Connect() {
    /* Change RDMA NDSPI into libibverbs with RDMA CM */

    // 1. Deleted Windows Socket WSAStartup
    // 2. Changed hr = NdStartup (initializes Network Direct Subsystem)
    // 2. Create event channel 
    ec = rdma_create_event_channel();
    if (!ec) {
        printf("Failed to create event channel: %s\n", strerror(errno));
        return false;
    }
#ifdef BACKEND_BRIDGE_VERBOSE
    printf("Event channel created successfully\n");
#endif

    // 2.5 Create RDMA CM ID 
    int ret = rdma_create_id(ec, &cm_id, NULL, RDMA_PS_TCP);
    if (ret) {
        printf("Failed to create CM ID: %s\n", strerror(errno));
        cleanup();
        return false;
    }
#ifdef BACKEND_BRIDGE_VERBOSE
    printf("CM ID created successfully\n");
#endif

    // Setup socket address
    memset(&BackEndSock, 0, sizeof(BackEndSock));
    BackEndSock.sin_family = AF_INET;
    BackEndSock.sin_port = htons(BackEndPort);
    
    if (inet_pton(AF_INET, BackEndAddr, &BackEndSock.sin_addr) != 1) {
        printf("Failed to convert IP address: %s\n", strerror(errno));
        cleanup();
        return false;
    }

    // Resolve address
    ret = rdma_resolve_addr(cm_id, NULL, (struct sockaddr*)&BackEndSock, RESOLVE_TIMEOUT_MS);
    if (ret) {
        printf("Failed to resolve address: %s\n", strerror(errno));
        cleanup();
        return false;
    }

    // Wait for address resolution
    struct rdma_cm_event *event;
    ret = rdma_get_cm_event(ec, &event);
    if (ret || event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        printf("Failed to get address resolved event: %s\n", strerror(errno));
        cleanup();
        return false;
    }
    rdma_ack_cm_event(event);

    // Resolve route
    ret = rdma_resolve_route(cm_id, RESOLVE_TIMEOUT_MS);
    if (ret) {
        printf("Failed to resolve route: %s\n", strerror(errno));
        cleanup();
        return false;
    }

    // Wait for route resolution
    ret = rdma_get_cm_event(ec, &event);
    if (ret || event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
        printf("Failed to get route resolved event: %s\n", strerror(errno));
        cleanup();
        return false;
    }
    rdma_ack_cm_event(event);

    //
    // Connect to the back end
    //
    //
    // Query device attributes
    struct ibv_device_attr dev_attr;
    ret = ibv_query_device(cm_id->verbs, &dev_attr);
    if (ret) {
        printf("Failed to query device attributes: %s\n", strerror(errno));
        cleanup();
        return false;
    }

    // Set queue parameters based on device capabilities
    QueueDepth = dev_attr.max_cq_entries;
    QueueDepth = std::min(QueueDepth, (uint64_t)dev_attr.max_qp_wr);
    QueueDepth = std::min(QueueDepth, (uint64_t)dev_attr.max_qp_rd_atom);
    MaxSge = std::min((uint64_t)dev_attr.max_sge, (uint64_t)dev_attr.max_qp_rd_atom);
    InlineThreshold = dev_attr.max_inline_data;

    printf("BackEndBridge: adapter information\n");
    printf("- Queue depth = %lu\n", QueueDepth);
    printf("- Max SGE = %lu\n", MaxSge);
    printf("- Inline threshold = %lu\n", InlineThreshold);

    // Create Completion Queue
    cq = ibv_create_cq(cm_id->verbs, QueueDepth, NULL, NULL, 0);
    if (!cq) {
        printf("Failed to create CQ: %s\n", strerror(errno));
        cleanup();
        return false;
    }
#ifdef BACKEND_BRIDGE_VERBOSE
    printf("CQ created successfully\n");
#endif

    
    
    // Create Queue Pair
    struct ibv_qp_init_attr qp_attr = {};
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.sq_sig_all = 1;
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.cap.max_send_wr = QueueDepth;
    qp_attr.cap.max_recv_wr = QueueDepth;
    qp_attr.cap.max_send_sge = MaxSge;
    qp_attr.cap.max_recv_sge = MaxSge;
    qp_attr.cap.max_inline_data = InlineThreshold;

    ret = rdma_create_qp(cm_id, NULL, &qp_attr);
    if (ret) {
        printf("Failed to create QP: %s\n", strerror(errno));
        cleanup();
        return false;
    }
#ifdef BACKEND_BRIDGE_VERBOSE
    printf("QP created successfully\n");
#endif

    // Register memory region
    mr = ibv_reg_mr(cm_id->pd, CtrlMsgBuf, CTRL_MSG_SIZE,
                    IBV_ACCESS_LOCAL_WRITE |
                    IBV_ACCESS_REMOTE_READ |
                    IBV_ACCESS_REMOTE_WRITE);
    if (!mr) {
        printf("Failed to register MR: %s\n", strerror(errno));
        cleanup();
        return false;
    }
#ifdef BACKEND_BRIDGE_VERBOSE
    printf("MR registered successfully\n");
#endif

    // Setup SGE
    CtrlSgl = new ibv_sge[1];
    CtrlSgl[0].addr = (uint64_t)CtrlMsgBuf;
    CtrlSgl[0].length = CTRL_MSG_SIZE;
    CtrlSgl[0].lkey = mr->lkey;

    // Connect to remote
    struct rdma_conn_param conn_param = {};
    uint8_t privData = CTRL_CONN_PRIV_DATA;
    conn_param.private_data = &privData;
    conn_param.private_data_len = sizeof(privData);
    conn_param.responder_resources = QueueDepth;
    conn_param.initiator_depth = QueueDepth;
    conn_param.retry_count = 7;
    conn_param.rnr_retry_count = 7;

    ret = rdma_connect(cm_id, &conn_param);
    if (ret) {
        printf("Failed to connect: %s\n", strerror(errno));
        cleanup();
        return false;
    }

    // Wait for connection establishment
    ret = rdma_get_cm_event(ec, &event);
    if (ret || event->event != RDMA_CM_EVENT_ESTABLISHED) {
        printf("Failed to establish connection: %s\n", strerror(errno));
        cleanup();
        return false;
    }
    rdma_ack_cm_event(event);

#ifdef BACKEND_BRIDGE_VERBOSE
    printf("Connection established successfully\n");
#endif


    //
    // Request client id and wait for response
    //
    //
    ((MsgHeader*)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQUEST_ID;
    ((CtrlMsgF2BRequestId*)(CtrlMsgBuf + sizeof(MsgHeader)))->Dummy = 42;

    struct ibv_send_wr send_wr = {};
    struct ibv_send_wr *bad_send_wr;
    
    send_wr.wr_id = MSG_CTXT;
    send_wr.sg_list = CtrlSgl;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    CtrlSgl[0].length = sizeof(MsgHeader) + sizeof(CtrlMsgF2BRequestId);

    ret = ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr);
    if (ret) {
        printf("Failed to post send: %s\n", strerror(errno));
        cleanup();
        return false;
    }

    // Wait for send completion
    struct ibv_wc wc;
    while (ibv_poll_cq(cq, 1, &wc) == 0);
    if (wc.status != IBV_WC_SUCCESS || wc.wr_id != MSG_CTXT) {
        printf("Send completion failed\n");
        cleanup();
        return false;
    }

    // Post receive for response
    struct ibv_recv_wr recv_wr = {};
    struct ibv_recv_wr *bad_recv_wr;
    
    CtrlSgl[0].length = CTRL_MSG_SIZE;
    recv_wr.wr_id = MSG_CTXT;
    recv_wr.sg_list = CtrlSgl;
    recv_wr.num_sge = 1;

    ret = ibv_post_recv(cm_id->qp, &recv_wr, &bad_recv_wr);
    if (ret) {
        printf("Failed to post receive: %s\n", strerror(errno));
        cleanup();
        return false;
    }

    // Wait for receive completion
    while (ibv_poll_cq(cq, 1, &wc) == 0);
    if (wc.status != IBV_WC_SUCCESS || wc.wr_id != MSG_CTXT) {
        printf("Receive completion failed\n");
        cleanup();
        return false;
    }

    // Process response
    if (((MsgHeader*)CtrlMsgBuf)->MsgId == CTRL_MSG_B2F_RESPOND_ID) {
        ClientId = ((CtrlMsgB2FRespondId*)(CtrlMsgBuf + sizeof(MsgHeader)))->ClientId;
        printf("BackEndBridge: connected to the back end with assigned Id (%d)\n", ClientId);
    } else {
        printf("BackEndBridge: wrong message from the back end\n");
        cleanup();
        return false;
    }

    return true;
}






/*
 * ConnectTest() - Test RDMA connectivity
 * 
 * Windows NDSPI to Linux libibverbs/RDMA CM Port Guide
 * -------------------------------------------------
 *
 * 1. Connection Management:
 *    Windows: Uses NDSPI's NdStartup() and connector-based approach
 *    Linux: Uses rdma_cm for connection management, which provides an event-driven model
 *
 * 2. Memory Registration:
 *    Windows: Uses NdRegisterMemory with ND_MR_FLAG flags
 *    Linux: Uses ibv_reg_mr with IBV_ACCESS flags
 *
 * 3. Queue Pair Creation:
 *    Windows: Explicit CQ and QP creation through NDSPI
 *    Linux: Uses rdma_create_qp which integrates with CM
 *
 * 4. Event Handling:
 *    Windows: Uses Windows Events and Overlapped I/O
 *    Linux: Uses rdma_cm events and completion queues
 */

bool
BackEndBridge::ConnectTest() {
    /*
    * Step 1: RDMA Device Setup
    * ------------------------
    * Windows: Required WSAStartup() and NdStartup()
    * Linux: Uses rdma_cm which handles device discovery
    */
    ec = rdma_create_event_channel();
    if (!ec) {
        printf("Failed to create event channel: %s\n", strerror(errno));
        return false;
    }
#ifdef BACKEND_BRIDGE_VERBOSE
    printf("Event channel created successfully\n");
#endif

    // Create RDMA CM ID (replaces Windows ND connector concept)
    int ret = rdma_create_id(ec, &cm_id, NULL, RDMA_PS_TCP);
    if (ret) {
        printf("Failed to create CM ID: %s\n", strerror(errno));
        cleanup();
        return false;
    }
#ifdef BACKEND_BRIDGE_VERBOSE
    printf("CM ID created successfully\n");
#endif

    /*
     * Step 2: Address Resolution
     * -------------------------
     * Windows: Used WSAStringToAddress and NdResolveAddress
     * Linux: Uses standard socket addressing and rdma_resolve_addr
     */
    memset(&BackEndSock, 0, sizeof(BackEndSock));
    BackEndSock.sin_family = AF_INET;
    BackEndSock.sin_port = htons(BackEndPort);
    
    // Convert IP address string to network format
    if (inet_pton(AF_INET, BackEndAddr, &BackEndSock.sin_addr) != 1) {
        printf("Failed to convert IP address: %s\n", strerror(errno));
        cleanup();
        return false;
    }

    // Resolve address (combines Windows address and route resolution steps)
    ret = rdma_resolve_addr(cm_id, NULL, (struct sockaddr*)&BackEndSock, RESOLVE_TIMEOUT_MS);
    if (ret) {
        printf("Failed to resolve address: %s\n", strerror(errno));
        cleanup();
        return false;
    }

    /*
     * Step 3: Event Handling
     * ---------------------
     * Windows: Used Overlapped I/O with Windows Events
     * Linux: Uses rdma_cm events
     */
    struct rdma_cm_event *event;
    ret = rdma_get_cm_event(ec, &event);
    if (ret || event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        printf("Failed to get address resolved event: %s\n", strerror(errno));
        cleanup();
        return false;
    }
    rdma_ack_cm_event(event);

    /*
     * Step 4: Device Capability Query
     * ------------------------------
     * Windows: Used NdQueryAddressInfo
     * Linux: Uses ibv_query_device
     */
    struct ibv_device_attr dev_attr;
    ret = ibv_query_device(cm_id->verbs, &dev_attr);
    if (ret) {
        printf("Failed to query device attributes: %s\n", strerror(errno));
        cleanup();
        return false;
    }

    // Set queue parameters based on device capabilities
    // Similar logic to Windows, but using different attribute names
    QueueDepth = dev_attr.max_cq_entries;
    QueueDepth = std::min(QueueDepth, (uint64_t)dev_attr.max_qp_wr);
    QueueDepth = std::min(QueueDepth, (uint64_t)dev_attr.max_qp_rd_atom);
    MaxSge = std::min((uint64_t)dev_attr.max_sge, (uint64_t)dev_attr.max_qp_rd_atom);
    InlineThreshold = dev_attr.max_inline_data;

    printf("BackEndBridge: adapter information\n");
    printf("- Queue depth = %lu\n", QueueDepth);
    printf("- Max SGE = %lu\n", MaxSge);
    printf("- Inline threshold = %lu\n", InlineThreshold);

    /*
     * Step 5: Queue Pair Setup
     * -----------------------
     * Windows: Separate creation of CQ and QP using NDSPI
     * Linux: Creates CQ separately but integrates QP with CM
     */
    cq = ibv_create_cq(cm_id->verbs, QueueDepth, NULL, NULL, 0);
    if (!cq) {
        printf("Failed to create CQ: %s\n", strerror(errno));
        cleanup();
        return false;
    }
#ifdef BACKEND_BRIDGE_VERBOSE
    printf("CQ created successfully\n");
#endif

    // Create protection domain (required for Linux RDMA)
    pd = ibv_alloc_pd(cm_id->verbs);
    if (!pd) {
        printf("Failed to allocate PD: %s\n", strerror(errno));
        cleanup();
        return false;
    }

    // Queue Pair attributes (similar to Windows but with Linux structs)
    struct ibv_qp_init_attr qp_attr = {};
    qp_attr.qp_type = IBV_QPT_RC;    // Reliable Connection (equivalent to Windows ND)
    qp_attr.sq_sig_all = 1;          // Signal all completions
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.cap.max_send_wr = QueueDepth;
    qp_attr.cap.max_recv_wr = QueueDepth;
    qp_attr.cap.max_send_sge = MaxSge;
    qp_attr.cap.max_recv_sge = MaxSge;
    qp_attr.cap.max_inline_data = InlineThreshold;

    /*
     * Step 6: Memory Registration
     * -------------------------
     * Windows: Used NdRegisterMemory with ND_MR_FLAG flags
     * Linux: Uses ibv_reg_mr with IBV_ACCESS flags
     */
    size_t dataBufSize = 1024 * 1024 * 1024; // 1GB test buffer
    char* dataBuf = new char[dataBufSize];
    memset(dataBuf, 23, dataBufSize);

    // Register data buffer
    // Windows: ND_MR_FLAG_ALLOW_LOCAL_WRITE | ND_MR_FLAG_ALLOW_REMOTE_READ | ND_MR_FLAG_ALLOW_REMOTE_WRITE
    // Linux: IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
    mr = ibv_reg_mr(pd, dataBuf, dataBufSize,
                    IBV_ACCESS_LOCAL_WRITE |
                    IBV_ACCESS_REMOTE_READ |
                    IBV_ACCESS_REMOTE_WRITE);
    if (!mr) {
        printf("Failed to register data MR: %s\n", strerror(errno));
        delete[] dataBuf;
        cleanup();
        return false;
    }

    // Register control message buffer
    ctrl_mr = ibv_reg_mr(pd, CtrlMsgBuf, CTRL_MSG_SIZE,
                        IBV_ACCESS_LOCAL_WRITE |
                        IBV_ACCESS_REMOTE_READ |
                        IBV_ACCESS_REMOTE_WRITE);
    if (!ctrl_mr) {
        printf("Failed to register control MR: %s\n", strerror(errno));
        delete[] dataBuf;
        cleanup();
        return false;
    }

    /*
     * Step 7: Scatter-Gather Setup
     * ---------------------------
     * Windows: Used ND2_SGE structure
     * Linux: Uses ibv_sge structure
     */
    CtrlSgl = new ibv_sge[1];
    CtrlSgl[0].addr = (uint64_t)CtrlMsgBuf;
    CtrlSgl[0].length = CTRL_MSG_SIZE;
    CtrlSgl[0].lkey = ctrl_mr->lkey;  // Local key instead of Windows token

    /*
     * Step 8: Connection Establishment
     * ------------------------------
     * Windows: Used NdConnect with connector
     * Linux: Uses rdma_connect with CM ID
     */
    struct rdma_conn_param conn_param = {};
    uint8_t privData = CTRL_CONN_PRIV_DATA;
    conn_param.private_data = &privData;
    conn_param.private_data_len = sizeof(privData);
    conn_param.responder_resources = QueueDepth;
    conn_param.initiator_depth = QueueDepth;
    conn_param.retry_count = 7;        // Similar to Windows retry logic
    conn_param.rnr_retry_count = 7;

    ret = rdma_connect(cm_id, &conn_param);
    if (ret) {
        printf("Failed to connect: %s\n", strerror(errno));
        delete[] dataBuf;
        cleanup();
        return false;
    }

    // Wait for connection establishment
    ret = rdma_get_cm_event(ec, &event);
    if (ret || event->event != RDMA_CM_EVENT_ESTABLISHED) {
        printf("Failed to establish connection: %s\n", strerror(errno));
        delete[] dataBuf;
        cleanup();
        return false;
    }
    rdma_ack_cm_event(event);

    /*
     * Step 9: Send Buffer Information
     * -----------------------------
     * Structure remains similar, but uses Linux-specific memory tokens
     */
    *((uint64_t*)CtrlMsgBuf) = (uint64_t)dataBuf;
    *((uint32_t*)((char*)CtrlMsgBuf + sizeof(uint64_t))) = htonl(mr->rkey);  // Remote key instead of Windows token
    *((uint32_t*)((char*)CtrlMsgBuf + sizeof(uint64_t) + sizeof(uint32_t))) = dataBufSize;

    printf("Buffer info:\n");
    printf("- Addr = %p\n", (void*)*((uint64_t*)CtrlMsgBuf));
    printf("- Token = %x\n", ntohl(*((uint32_t*)((char*)CtrlMsgBuf + sizeof(uint64_t)))));
    printf("- Size = %u\n", *((uint32_t*)((char*)CtrlMsgBuf + sizeof(uint64_t) + sizeof(uint32_t))));

    /*
     * Step 10: Send Operation
     * ----------------------
     * Windows: Used NdSend
     * Linux: Uses ibv_post_send
     */
    struct ibv_send_wr send_wr = {};
    struct ibv_send_wr *bad_wr;
    CtrlSgl[0].length = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t);
    
    send_wr.wr_id = MSG_CTXT;
    send_wr.sg_list = CtrlSgl;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_SEND;
    send_wr.send_flags = IBV_SEND_SIGNALED;

    ret = ibv_post_send(cm_id->qp, &send_wr, &bad_wr);
    if (ret) {
        printf("Failed to post send: %s\n", strerror(errno));
        delete[] dataBuf;
        cleanup();
        return false;
    }

    /*
     * Step 11: Completion Handling
     * ---------------------------
     * Windows: Used overlapped I/O completion
     * Linux: Uses completion queue polling
     */
    struct ibv_wc wc;
    while (ibv_poll_cq(cq, 1, &wc) == 0);  // Poll until completion
    if (wc.status != IBV_WC_SUCCESS || wc.wr_id != MSG_CTXT) {
        printf("Send completion failed\n");
        delete[] dataBuf;
        cleanup();
        return false;
    }

    // 120000ms -> 120s
    sleep(120);

/*

    CtrlSgl->BufferLength = CTRL_MSG_SIZE;
    RDMC_PostReceive(CtrlQPair, CtrlSgl, 1, MSG_CTXT);
#ifdef BACKEND_BRIDGE_VERBOSE
    printf("RDMC_PostReceive succeeded\n");
#endif

    RDMC_WaitForCompletionAndCheckContext(CtrlCompQ, &Ov, MSG_CTXT, false);
#ifdef BACKEND_BRIDGE_VERBOSE
    printf("RDMC_WaitForCompletionAndCheckContext succeeded\n");
#endif

*/


    /* Cleanup */ 
    delete[] dataBuf;
    return true;
}

//
// Disconnect from the backend
//
//
void
BackEndBridge::Disconnect() {
    //
    // Send the exit message to the back end
    //
    //
    if (ClientId >= 0) {
        // Send termination message
        ((MsgHeader*)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_TERMINATE;
        ((CtrlMsgF2BTerminate*)(CtrlMsgBuf + sizeof(MsgHeader)))->ClientId = ClientId;

        struct ibv_send_wr send_wr = {};
        struct ibv_send_wr *bad_wr;
        
        send_wr.wr_id = MSG_CTXT;
        send_wr.sg_list = CtrlSgl;
        send_wr.num_sge = 1;
        send_wr.opcode = IBV_WR_SEND;
        send_wr.send_flags = IBV_SEND_SIGNALED;
        CtrlSgl[0].length = sizeof(MsgHeader) + sizeof(CtrlMsgF2BTerminate);

        if (ibv_post_send(cm_id->qp, &send_wr, &bad_wr) == 0) {
            struct ibv_wc wc;
            while (ibv_poll_cq(cq, 1, &wc) == 0);
            printf("BackEndBridge: disconnected from the back end\n");
        }
    }

    // Disconnect and cleanup
    if (cm_id) {
        rdma_disconnect(cm_id);
    }

    cleanup();
}
