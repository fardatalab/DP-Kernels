#include <atomic>
#include <cstdint>
#include <cstdio>
#include <string.h>

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <poll.h>
#include <rdma/rdma_cma.h>

#if DDS_NOTIFICATION_METHOD == DDS_NOTIFICATION_METHOD_TIMER
#include <thread>
#endif

#include "DDSBackEndBridge.h"
#define BACKEND_BRIDGE_VERBOSE 1

namespace DDS_FrontEnd
{

#if BACKEND_TYPE == BACKEND_TYPE_DPU

    DDSBackEndBridge::DDSBackEndBridge()
    {
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
        // Adapter = NULL;
        // AdapterFileHandle = NULL;
        // memset(&AdapterInfo, 0, sizeof(ND2_ADAPTER_INFO));
        // memset(&Ov, 0, sizeof(Ov));
        QueueDepth = 0;
        MaxSge = 0;
        InlineThreshold = 0;
        memset(&LocalSock, 0, sizeof(LocalSock));
        LocalSock.sin_family = AF_INET;
        LocalSock.sin_addr.s_addr = inet_addr(LOCALSOCK); // Your ibp225s0f0 IP
        LocalSock.sin_port = 0;                           // Let system choose port for client

        // CtrlConnector = NULL;
        // CtrlCompQ = NULL;
        // CtrlQPair = NULL;
        // CtrlMemRegion = NULL;
        // CtrlSgl = NULL;
        memset(CtrlMsgBuf, 0, CTRL_MSG_SIZE);

        ClientId = -1;
    }

    ErrorCodeT DDSBackEndBridge::resolveBackendAddress()
    {
        // init socket structure
        memset(&BackEndSock, 0, sizeof(BackEndSock));
        BackEndSock.sin_family = AF_INET;
        BackEndSock.sin_port = htons(BackEndPort);

        // Windows: WSAStringToAddress with wchar_t
        // Linux: inet_pton for IPv4 address conversion
        if (inet_pton(AF_INET, BackEndAddr, &BackEndSock.sin_addr) != 1)
        {
            printf("DDSBackEndBridge: inet_pton failed: %s\n", strerror(errno));
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }

        // Address validation (equivalent to Windows checks)
        if (BackEndSock.sin_addr.s_addr == INADDR_ANY || BackEndSock.sin_addr.s_addr == INADDR_NONE)
        {
            printf("DDSBackEndBridge: invalid backend address\n");
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }

        // Windows: NdResolveAddress
        return DDS_ERROR_CODE_SUCCESS;
    }

    // Update cleanup function to handle new resources
    void DDSBackEndBridge::cleanup()
    {
        if (mr)
        {
            ibv_dereg_mr(mr);
        }
        if (cq)
        {
            ibv_destroy_cq(cq);
        }
        if (cm_id)
        {
            if (cm_id->qp)
            {
                rdma_destroy_qp(cm_id);
            }
            rdma_destroy_id(cm_id);
        }
        if (ec)
        {
            rdma_destroy_event_channel(ec);
        }
    }

    // Connect to the backend (RDMA Client)
    ErrorCodeT DDSBackEndBridge::Connect()
    {
        /* Change RDMA NDSPI into libibverbs with RDMA CM */

        // 1. Deleted Windows Socket WSAStartup
        // 2. Changed hr = NdStartup (initializes Network Direct Subsystem)
        // 2. Create event channel

        printf("In DDSBackEndBridge Connect()\n");
        ec = rdma_create_event_channel();
        if (!ec)
        {
            printf("Failed to create event channel: %s\n", strerror(errno));
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
#ifdef BACKEND_BRIDGE_VERBOSE
        printf("Event channel created successfully\n");
#endif

        // 2.5 Create RDMA CM ID
        int ret = rdma_create_id(ec, &cm_id, NULL, RDMA_PS_TCP);
        if (ret)
        {
            printf("Failed to create CM ID: %s\n", strerror(errno));
            rdma_destroy_event_channel(ec);
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
#ifdef BACKEND_BRIDGE_VERBOSE
        printf("CM ID created successfully\n");
#endif

        // 3. Address Resolution
        ErrorCodeT result = resolveBackendAddress();
        if (result < 0)
        {
            cleanup();
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }

        // 3.1 RDMA address & route resolution
        ret = rdma_resolve_addr(cm_id, NULL, (struct sockaddr *)&BackEndSock, RESOLVE_TIMEOUT_MS);
        if (ret)
        {
            printf("Failed to resolve addr: %s\n", strerror(errno));
            cleanup();
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }

        // 3.2 Wait for addr resolution event
        struct rdma_cm_event *event;
        ret = rdma_get_cm_event(ec, &event);
        if (ret || event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
        {
            printf("Failed to resolve addr (event): %s\n", strerror(errno));
            cleanup();
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
        rdma_ack_cm_event(event);

        // 3.3 Reslve route
        ret = rdma_resolve_route(cm_id, RESOLVE_TIMEOUT_MS);
        if (ret)
        {
            printf("Failed to resolve route: %s\n", strerror(errno));
            cleanup();
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }

        // 3.4 Wait for route resolution event
        ret = rdma_get_cm_event(ec, &event);
        if (ret || event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
        {
            printf("Failed to resolve route (event): %s\n", strerror(errno));
            cleanup();
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
        rdma_ack_cm_event(event);

        //
        //
        // Connect to back-end
        //

        // Windows: Adapter->Query for capabilities
        // With RDMA CM, you can query device attributes after connection:
        struct ibv_device_attr dev_attr;
        ret = ibv_query_device(cm_id->verbs, &dev_attr);
        if (ret)
        {
            printf("Failed to query device attributes: %s\n", strerror(errno));
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }

        // Matches Win Code
        QueueDepth = dev_attr.max_cqe; // MaxCompletionQueueDepth
        // jason: don't use the small max_qp_rd_atom as queue depth
        QueueDepth = DDS_MAX_COMPLETION_BUFFERING;
        /* QueueDepth = std::min<uint32_t>(QueueDepth, dev_attr.max_qp_wr);    // Specify type explicitly
        QueueDepth = std::min<uint32_t>(QueueDepth, dev_attr.max_qp_rd_atom);      // For max outstanding operations */
        MaxSge = std::min<uint32_t>(dev_attr.max_sge, dev_attr.max_qp_rd_atom);
        InlineThreshold = INLINE_THREASHOLD; // InlineRequestThreshold

        printf("DDSBackEndBridge: adapter information\n");
        printf("- Queue depth = %lu\n", QueueDepth);
        printf("- Max SGE = %lu\n", MaxSge);
        printf("- Inline threshold = %lu\n", InlineThreshold);

        // 3.5 Deleted OpenAdaptor (equivalent to libibverbs ctx, but cm handles ctx)
        // 3.6 Create QP #TODO

        // Create CQ (equivalent to RDMC_CreateCQ)
        struct ibv_cq *cq =
            ibv_create_cq(cm_id->verbs, DDS_MAX_COMPLETION_BUFFERING * 2 /* QueueDepth */, NULL, NULL, 0);
        if (!cq)
        {
            printf("Failed to create CQ: %s\n", strerror(errno));
            cleanup();
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
#ifdef BACKEND_BRIDGE_VERBOSE
        printf("CQ creation succeeded\n");
#endif
        this->cq = cq;

        // Setup QP attributes (equivalent to RDMC_CreateQueuePair)
        struct ibv_qp_init_attr qp_attr;
        memset(&qp_attr, 0, sizeof(qp_attr));
        qp_attr.qp_type = IBV_QPT_RC;
        qp_attr.sq_sig_all = 1;                                                  // Every operation generates completion
        qp_attr.send_cq = cq;                                                    // Use same CQ for send
        qp_attr.recv_cq = cq;                                                    // and receive
        qp_attr.cap.max_send_wr = DDS_MAX_COMPLETION_BUFFERING /* QueueDepth */; // Using your QueueDepth
        qp_attr.cap.max_recv_wr = DDS_MAX_COMPLETION_BUFFERING /* QueueDepth */;
        qp_attr.cap.max_send_sge = MaxSge; // Using your MaxSge
        qp_attr.cap.max_recv_sge = MaxSge;
        qp_attr.cap.max_inline_data = InlineThreshold;

        // Create QP using RDMA CM
        ret = rdma_create_qp(cm_id, NULL, &qp_attr);
        if (ret)
        {
            printf("Failed to create QP: %s\n", strerror(errno));
            cleanup();
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
#ifdef BACKEND_BRIDGE_VERBOSE
        printf("QP creation succeeded\n");
#endif

        // Register memory region (equivalent to RDMC_CreateMR and RDMC_RegisterDataBuffer)
        struct ibv_mr *mr = ibv_reg_mr(cm_id->pd, CtrlMsgBuf, CTRL_MSG_SIZE,
                                       IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
        if (!mr)
        {
            printf("Failed to register MR: %s\n", strerror(errno));
            cleanup();
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
#ifdef BACKEND_BRIDGE_VERBOSE
        printf("MR registration succeeded\n");
#endif
        this->mr = mr;

        // 4. Connect
        struct rdma_conn_param conn_param = {};
        // jason: QueueDepth used to capped at max_qp_rd_atom, now explicitly use the dev cap
        conn_param.responder_resources = dev_attr.max_qp_rd_atom;
        conn_param.initiator_depth = dev_attr.max_qp_rd_atom;
        /* conn_param.responder_resources = QueueDepth;
        conn_param.initiator_depth = QueueDepth; */
        conn_param.retry_count = 7; // Similar to Windows default
        conn_param.rnr_retry_count = 7;
        uint8_t privData = CTRL_CONN_PRIV_DATA;
        conn_param.private_data = &privData;
        conn_param.private_data_len = sizeof(privData);

        // Connect (combines RDMC_Connect and RDMC_CompleteConnect)
        ret = rdma_connect(cm_id, &conn_param);
        if (ret)
        {
            printf("Failed to connect: %s\n", strerror(errno));
            cleanup();
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }

        // 5. Wait for Connection Event
        ret = rdma_get_cm_event(ec, &event);
        if (ret || event->event != RDMA_CM_EVENT_ESTABLISHED)
        {
            printf("Failed to establish connection: %s\n", strerror(errno));
            cleanup();
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
        rdma_ack_cm_event(event);

#ifdef BACKEND_BRIDGE_VERBOSE
        printf("Connection established successfully\n");
#endif

        // return DDS_ERROR_CODE_SUCCESS;

        // Ov.hEvent = CreateEvent(nullptr, FALSE, FALSE, nullptr);
        // allocate event for overlapped operations (async IO)
        // hevent is a synchronization primitive (like a flag) set by async op start/end

        // hr = Adapter->CreateOverlappedFile(&AdapterFileHandle);
        // CreateOverlappedFile
        // Instead of overlapped I/O, Linux RDMA uses event-based asynchronous operations

        //
        // Request client id and wait for response
        //
        //

        // Setup scatter/gather entry (equivalent to CtrlSgl setup)
        // Setup send request for client ID
        struct ibv_send_wr send_wr = {};
        struct ibv_sge send_sge = {};

        // Prepare message buffer (same as Windows)
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQUEST_ID;
        ((CtrlMsgF2BRequestId *)(CtrlMsgBuf + sizeof(MsgHeader)))->Dummy = 42;

        // Setup SGE for send
        send_sge.addr = (uint64_t)CtrlMsgBuf;
        send_sge.length = sizeof(MsgHeader) + sizeof(CtrlMsgF2BRequestId);
        send_sge.lkey = mr->lkey;

        // Setup send work request
        send_wr.wr_id = MSG_CTXT; // Using same context as Windows
        send_wr.sg_list = &send_sge;
        send_wr.num_sge = 1;
        send_wr.opcode = IBV_WR_SEND;
        send_wr.send_flags = IBV_SEND_SIGNALED; // Generate completion

        // Post send
        struct ibv_send_wr *bad_send_wr;
        ret = ibv_post_send(cm_id->qp, &send_wr, &bad_send_wr);
        if (ret)
        {
            printf("Failed to post send: %s\n", strerror(errno));
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
#ifdef BACKEND_BRIDGE_VERBOSE
        printf("Send posted successfully\n");
#endif

        // Wait for send completion
        struct ibv_wc wc;
        while (ibv_poll_cq(cq, 1, &wc) == 0)
            ; // Wait for completion
        if (wc.status != IBV_WC_SUCCESS || wc.wr_id != MSG_CTXT)
        {
            printf("Send completion failed or wrong context (got: %lu, expected: %lu)\n", wc.wr_id, MSG_CTXT);
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
#ifdef BACKEND_BRIDGE_VERBOSE
        printf("Send completed successfully\n");
#endif

        // Post receive for response
        struct ibv_recv_wr recv_wr = {};
        struct ibv_sge recv_sge = {};

        recv_sge.addr = (uint64_t)CtrlMsgBuf;
        recv_sge.length = CTRL_MSG_SIZE;
        recv_sge.lkey = mr->lkey;

        recv_wr.wr_id = MSG_CTXT;
        recv_wr.sg_list = &recv_sge;
        recv_wr.num_sge = 1;

        struct ibv_recv_wr *bad_recv_wr;
        ret = ibv_post_recv(cm_id->qp, &recv_wr, &bad_recv_wr);
        if (ret)
        {
            printf("Failed to post receive: %s\n", strerror(errno));
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
#ifdef BACKEND_BRIDGE_VERBOSE
        printf("Receive posted successfully\n");
#endif

        // Wait for receive completion
        while (ibv_poll_cq(cq, 1, &wc) == 0)
            ; // Wait for completion
        if (wc.status != IBV_WC_SUCCESS || wc.wr_id != MSG_CTXT)
        {
            printf("Receive completion failed or wrong context (got: %lu, expected: %lu)\n", wc.wr_id, MSG_CTXT);
            return DDS_ERROR_CODE_FAILED_CONNECTION;
        }
#ifdef BACKEND_BRIDGE_VERBOSE
        printf("Receive completed successfully\n");
#endif

        // Check response message (same as Windows)
        if (((MsgHeader *)CtrlMsgBuf)->MsgId == CTRL_MSG_B2F_RESPOND_ID)
        {
            ClientId = ((CtrlMsgB2FRespondId *)(CtrlMsgBuf + sizeof(MsgHeader)))->ClientId;
            printf("DDSBackEndBridge: connected to the back end with assigned id (%d)\n", ClientId);
        }
        else
        {
            printf("DDSBackEndBridge: wrong message from the back end\n");
            return DDS_ERROR_CODE_UNEXPECTED_MSG;
        }

        return DDS_ERROR_CODE_SUCCESS;
    }

    //
    // Disconnect from the backend
    // NOTE: Best-effort disconnect event handling to avoid blocking shutdown.
    //
    // jason: Use a bounded wait for CM events and tolerate already-closed peers.
    //
    ErrorCodeT DDSBackEndBridge::Disconnect()
    {
        //
        // Send the exit message to the back end
        //
        //
        if (ClientId >= 0)
        {
            ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_TERMINATE;
            ((CtrlMsgF2BTerminate *)(CtrlMsgBuf + sizeof(MsgHeader)))->ClientId = ClientId;

            /*
             CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BTerminate);
             RDMC_Send(CtrlQPair, CtrlSgl, 1, 0, MSG_CTXT);
             RDMC_WaitForCompletionAndCheckContext(CtrlCompQ, &Ov, MSG_CTXT, false);
             */

            // Set up SGE for send
            struct ibv_sge send_sge = {};
            send_sge.addr = (uint64_t)CtrlMsgBuf;
            send_sge.length = sizeof(MsgHeader) + sizeof(CtrlMsgF2BTerminate);
            send_sge.lkey = mr->lkey;

            // Setup send work request
            struct ibv_send_wr send_wr = {};
            send_wr.wr_id = MSG_CTXT;
            send_wr.sg_list = &send_sge;
            send_wr.num_sge = 1;
            send_wr.opcode = IBV_WR_SEND;
            send_wr.send_flags = IBV_SEND_SIGNALED;

            // Post send
            struct ibv_send_wr *bad_wr;
            if (ibv_post_send(cm_id->qp, &send_wr, &bad_wr))
            {
                printf("Failed to post termination send\n");
                return DDS_ERROR_CODE_IO_FAILURE;
            }

            // Wait for completion
            struct ibv_wc wc;
            while (ibv_poll_cq(cq, 1, &wc) == 0)
                ;
            if (wc.status != IBV_WC_SUCCESS)
            {
                printf("Termination send failed\n");
                return DDS_ERROR_CODE_IO_FAILURE;
            }

            printf("DDSBackEndBridge: disconnected from the back end\n");
        }

        //
        // Disconnect and release all resources
        //
        //

        // Disconnect RDMA connection
        if (cm_id && cm_id->qp)
        {
#ifdef BACKEND_BRIDGE_VERBOSE
            // jason: Trace disconnect flow to confirm whether a CM event arrives.
            printf("DDSBackEndBridge: issuing rdma_disconnect for control connection\n");
#endif
            /*
             rdma_disconnect(cm_id);

             // Wait for disconnect event
             struct rdma_cm_event *event;
             if (rdma_get_cm_event(ec, &event) == 0)
             {
                 if (event->event == RDMA_CM_EVENT_DISCONNECTED)
                 {
                     rdma_ack_cm_event(event);
                 }
             }
             */

            // jason: Best-effort disconnect; if peer already closed, do not block.
            int discRet = rdma_disconnect(cm_id);
            if (discRet)
            {
                // jason: rdma_disconnect can fail if the peer already closed or QP is in error.
                fprintf(stderr, "DDSBackEndBridge: rdma_disconnect failed (%d: %s); skipping CM wait\n",
                        discRet, strerror(errno));
            }
            else
            {
                // jason: Wait with a timeout for CM events to avoid hanging on shutdown.
                const int kDisconnectWaitMs = 2000;
                struct pollfd pfd = {};
                pfd.fd = ec->fd;
                pfd.events = POLLIN;
                int pollRet = poll(&pfd, 1, kDisconnectWaitMs);
                if (pollRet > 0 && (pfd.revents & POLLIN))
                {
                    struct rdma_cm_event *event = NULL;
                    if (rdma_get_cm_event(ec, &event) == 0 && event)
                    {
                        // jason: Either DISCONNECTED or TIMEWAIT_EXIT is acceptable here.
                        if (event->event == RDMA_CM_EVENT_DISCONNECTED ||
                            event->event == RDMA_CM_EVENT_TIMEWAIT_EXIT)
                        {
#ifdef BACKEND_BRIDGE_VERBOSE
                            fprintf(stdout, "DDSBackEndBridge: CM event %s observed during disconnect\n",
                                    rdma_event_str(event->event));
#endif
                        }
                        rdma_ack_cm_event(event);
                    }
                }
                else if (pollRet == 0)
                {
                    // jason: Timeout is expected if the peer already tore down or no CM event was emitted.
#ifdef BACKEND_BRIDGE_VERBOSE
                    fprintf(stdout, "DDSBackEndBridge: CM disconnect wait timed out\n");
#endif
                }
                else if (pollRet < 0)
                {
                    fprintf(stderr, "DDSBackEndBridge: poll error during disconnect wait: %s\n",
                            strerror(errno));
                }
            }
        }

        // Release memory buffer
        // RDMA cleaning
        cleanup();

        // HRESULT hr = NdCleanup();
        // if (FAILED(hr))
        // {
        //     printf("NdCleanup failed with %08x\n", hr);
        // }

        // _fcloseall();
        // WSACleanup();

        return DDS_ERROR_CODE_SUCCESS;
    }

    //
    // Compute control-plane request size from request MsgId.
    // Returns 0 for unknown or unsupported request IDs.
    //
    static inline size_t GetCtrlRequestSize(RequestIdT RequestMsgId)
    {
        switch (RequestMsgId)
        {
        case CTRL_MSG_F2B_REQUEST_ID:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BRequestId);
        case CTRL_MSG_F2B_TERMINATE:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BTerminate);
        case CTRL_MSG_F2B_REQ_CREATE_DIR:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqCreateDirectory);
        case CTRL_MSG_F2B_REQ_REMOVE_DIR:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqRemoveDirectory);
        case CTRL_MSG_F2B_REQ_CREATE_FILE:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqCreateFile);
        case CTRL_MSG_F2B_REQ_DELETE_FILE:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqDeleteFile);
        case CTRL_MSG_F2B_REQ_CHANGE_FILE_SIZE:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqChangeFileSize);
        case CTRL_MSG_F2B_REQ_GET_FILE_SIZE:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqGetFileSize);
        case CTRL_MSG_F2B_REQ_GET_FILE_INFO:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqGetFileInfo);
        case CTRL_MSG_F2B_REQ_GET_FILE_ATTR:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqGetFileAttr);
        case CTRL_MSG_F2B_REQ_GET_FREE_SPACE:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqGetFreeSpace);
        case CTRL_MSG_F2B_REQ_MOVE_FILE:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqMoveFile);
        case CTRL_MSG_F2B_REQ_FIND_FIRST_FILE:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqFindFirstFile);
        case CTRL_MSG_F2B_REQ_GET_IO_ALIGN:
            // jason: include GET_IO_ALIGN request size so alignment fetch is sent correctly.
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqGetIoAlign);
        case CTRL_MSG_F2B_REQ_SET_READ2_AES_KEY:
            return sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqSetRead2AesKey);
        default:
            return 0;
        }
    }

    //
    // Send a control message and wait (w/ blocking) for response.
    // Uses the request MsgId in CtrlMsgBuf to size the send payload.
    //
    static inline ErrorCodeT SendCtrlMsgAndWait(DDSBackEndBridge *BackEnd, RequestIdT ExpectedMsgId)
    {
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

        struct ibv_send_wr send_wr = {}, *bad_send_wr;
        struct ibv_recv_wr recv_wr = {}, *bad_recv_wr;
        struct ibv_wc wc;
        struct ibv_sge send_sge, recv_sge;

        // Setup SGEs; request size is derived from the request MsgId.
        send_sge.addr = (uint64_t)BackEnd->CtrlMsgBuf;
        RequestIdT requestMsgId = ((MsgHeader *)BackEnd->CtrlMsgBuf)->MsgId;
        size_t requestSize = GetCtrlRequestSize(requestMsgId);
        if (requestSize == 0 || requestSize > CTRL_MSG_SIZE)
        {
            // Guard against unknown or oversized control requests
            printf("ERROR: invalid control request MsgId %u\n", requestMsgId);
            return DDS_ERROR_CODE_UNEXPECTED_MSG;
        }
        send_sge.length = (uint32_t)requestSize;
        send_sge.lkey = BackEnd->mr->lkey;

        recv_sge.addr = (uint64_t)BackEnd->CtrlMsgBuf;
        recv_sge.length = CTRL_MSG_SIZE;
        recv_sge.lkey = BackEnd->mr->lkey;

        // Setup send work request
        send_wr.wr_id = MSG_CTXT;
        send_wr.sg_list = &send_sge;
        send_wr.num_sge = 1;
        send_wr.opcode = IBV_WR_SEND;
        send_wr.send_flags = IBV_SEND_SIGNALED;

        /// receive

        /// send
        // Post send
        if (ibv_post_send(BackEnd->cm_id->qp, &send_wr, &bad_send_wr))
        {
            return DDS_ERROR_CODE_IO_FAILURE;
        }

        // Wait for send completion
        while (ibv_poll_cq(BackEnd->cq, 1, &wc) == 0)
            ;
        if (wc.status != IBV_WC_SUCCESS || wc.wr_id != MSG_CTXT)
        {
            return DDS_ERROR_CODE_IO_FAILURE;
        }

        // BackEnd->CtrlSgl->BufferLength = CTRL_MSG_SIZE;
        // RDMC_PostReceive(BackEnd->CtrlQPair, BackEnd->CtrlSgl, 1, MSG_CTXT);
        // RDMC_WaitForCompletionAndCheckContext(BackEnd->CtrlCompQ, &BackEnd->Ov, MSG_CTXT, true);

        // Setup receive
        recv_sge.length = CTRL_MSG_SIZE;
        recv_wr.wr_id = MSG_CTXT;
        recv_wr.sg_list = &recv_sge;
        recv_wr.num_sge = 1;

        // Post receive
        if (ibv_post_recv(BackEnd->cm_id->qp, &recv_wr, &bad_recv_wr))
        {
            return DDS_ERROR_CODE_IO_FAILURE;
        }

        // Wait for receive completion
        while (ibv_poll_cq(BackEnd->cq, 1, &wc) == 0)
            ;
        if (wc.status != IBV_WC_SUCCESS || wc.wr_id != MSG_CTXT)
        {
            return DDS_ERROR_CODE_IO_FAILURE;
        }

        // Check message ID
        if (((MsgHeader *)BackEnd->CtrlMsgBuf)->MsgId != ExpectedMsgId)
        {
            result = DDS_ERROR_CODE_UNEXPECTED_MSG;
        }
        return result;
    }

    //
    // Create a diretory.
    // Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::CreateDirectory(const char *PathName, DirIdT DirId, DirIdT ParentId)
    {
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        //
        // Send a create directory request to the back end
        //
        //
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_CREATE_DIR;

        CtrlMsgF2BReqCreateDirectory *req = (CtrlMsgF2BReqCreateDirectory *)(CtrlMsgBuf + sizeof(MsgHeader));

        req->DirId = DirId;
        req->ParentDirId = ParentId;
        strcpy(req->PathName, PathName);

        // CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqCreateDirectory);
        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_CREATE_DIR);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }
        // printf("Before SendCtrlMsgAndWait\n");

        CtrlMsgB2FAckCreateDirectory *resp = (CtrlMsgB2FAckCreateDirectory *)(CtrlMsgBuf + sizeof(MsgHeader));
        return resp->Result;
    }

    //
    // Delete a directory.
    // Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::RemoveDirectory(DirIdT DirId)
    {
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        //
        // Send a remove directory request to the back end
        //
        //
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_REMOVE_DIR;

        CtrlMsgF2BReqRemoveDirectory *req = (CtrlMsgF2BReqRemoveDirectory *)(CtrlMsgBuf + sizeof(MsgHeader));
        req->DirId = DirId;

        // CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqRemoveDirectory);

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_REMOVE_DIR);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }

        CtrlMsgB2FAckRemoveDirectory *resp = (CtrlMsgB2FAckRemoveDirectory *)(CtrlMsgBuf + sizeof(MsgHeader));
        return resp->Result;
    }

    //
    // Find a file by name.
    // Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::FindFirstFile(const char *FileName, FileIdT *FileId)
    {

        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_FIND_FIRST_FILE;

        CtrlMsgF2BReqFindFirstFile *req = (CtrlMsgF2BReqFindFirstFile *)(CtrlMsgBuf + sizeof(MsgHeader));
        strncpy(req->FileName, FileName, sizeof(req->FileName) - 1);
        req->FileName[sizeof(req->FileName) - 1] = '\0';
        printf("DDSBackEndBridge::FindFirstFile: req->FileName: %s\n", req->FileName);

        // CtrlConn->SendWr.sg_list->length =
        // sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqFindFirstFile);

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_FIND_FIRST_FILE);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            printf("In DDSBackEndBridge.cpp. SendCtrlMsgAndWait returned: %s\n", result);
            return result;
        }
        // CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqFindFirstFile);

        CtrlMsgB2FAckFindFirstFile *resp = (CtrlMsgB2FAckFindFirstFile *)(CtrlMsgBuf + sizeof(MsgHeader));
        *FileId = resp->FileId;
        // printf("resp->FileId %d\n", resp->FileId);
        return resp->Result;
    }

    //
    // Create a file.
    // Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::CreateFile(const char *FileName, FileAttributesT FileAttributes, FileIdT FileId,
                                            DirIdT DirId)
    {
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        //
        // Send a create tile request to the back end
        //
        //
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_CREATE_FILE;

        CtrlMsgF2BReqCreateFile *req = (CtrlMsgF2BReqCreateFile *)(CtrlMsgBuf + sizeof(MsgHeader));
        req->FileId = FileId;
        req->FileAttributes = FileAttributes;
        req->DirId = DirId;
        strcpy(req->FileName, FileName);

        // CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqCreateFile);

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_CREATE_FILE);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }

        CtrlMsgB2FAckCreateFile *resp = (CtrlMsgB2FAckCreateFile *)(CtrlMsgBuf + sizeof(MsgHeader));
        return resp->Result;
    }

    //
    // Delete a file.
    // Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::DeleteFile(FileIdT FileId, DirIdT DirId)
    {
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        //
        // Send a delete file request to the back end
        //
        //
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_DELETE_FILE;

        CtrlMsgF2BReqDeleteFile *req = (CtrlMsgF2BReqDeleteFile *)(CtrlMsgBuf + sizeof(MsgHeader));
        req->DirId = DirId;
        req->FileId = FileId;

        // CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqDeleteFile);

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_DELETE_FILE);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }

        CtrlMsgB2FAckDeleteFile *resp = (CtrlMsgB2FAckDeleteFile *)(CtrlMsgBuf + sizeof(MsgHeader));
        return resp->Result;
    }

    //
    // Change the size of a file.
    // Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::ChangeFileSize(FileIdT FileId, FileSizeT NewSize)
    {
        printf("FrontEnd: ChangeFileSize: %ld\n", NewSize);
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        //
        // Send a change file size request to the back end
        //
        //
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_CHANGE_FILE_SIZE;

        CtrlMsgF2BReqChangeFileSize *req = (CtrlMsgF2BReqChangeFileSize *)(CtrlMsgBuf + sizeof(MsgHeader));
        req->FileId = FileId;
        req->NewSize = NewSize;

        // CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqChangeFileSize);

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_CHANGE_FILE_SIZE);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }

        CtrlMsgB2FAckChangeFileSize *resp = (CtrlMsgB2FAckChangeFileSize *)(CtrlMsgBuf + sizeof(MsgHeader));
        return resp->Result;
    }

    //
    // Get file size.
    // Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::GetFileSize(FileIdT FileId, FileSizeT *FileSize)
    {
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        //
        // Send a get file size request to the back end
        //
        //
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_GET_FILE_SIZE;

        CtrlMsgF2BReqGetFileSize *req = (CtrlMsgF2BReqGetFileSize *)(CtrlMsgBuf + sizeof(MsgHeader));
        req->FileId = FileId;

        // CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqGetFileSize);

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_GET_FILE_SIZE);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }

        CtrlMsgB2FAckGetFileSize *resp = (CtrlMsgB2FAckGetFileSize *)(CtrlMsgBuf + sizeof(MsgHeader));
        *FileSize = resp->FileSize;
        return resp->Result;
    }

    //
    // Get backend I/O alignment parameters.
    // Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::GetIoAlignment(FileIOSizeT *BlockSize, FileIOSizeT *BufAlign)
    {
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        //
        // Send a get I/O alignment request to the back end
        //
        //
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_GET_IO_ALIGN;
        CtrlMsgF2BReqGetIoAlign *req = (CtrlMsgF2BReqGetIoAlign *)(CtrlMsgBuf + sizeof(MsgHeader));
        req->Dummy = 0;

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_GET_IO_ALIGN);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }

        CtrlMsgB2FAckGetIoAlign *resp = (CtrlMsgB2FAckGetIoAlign *)(CtrlMsgBuf + sizeof(MsgHeader));
        if (BlockSize)
        {
            *BlockSize = resp->BlockSize;
        }
        if (BufAlign)
        {
            *BufAlign = resp->BufAlign;
        }
        return resp->Result;
    }

    ErrorCodeT DDSBackEndBridge::SetRead2AesKey(const uint8_t *KeyBytes, uint8_t KeySizeBytes)
    {
        if (KeyBytes == nullptr || (KeySizeBytes != 16 && KeySizeBytes != 32))
        {
            return DDS_ERROR_CODE_INVALID_PARAM;
        }

        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_SET_READ2_AES_KEY;
        CtrlMsgF2BReqSetRead2AesKey *req = (CtrlMsgF2BReqSetRead2AesKey *)(CtrlMsgBuf + sizeof(MsgHeader));
        memset(req, 0, sizeof(*req));
        req->KeySizeBytes = KeySizeBytes;
        memcpy(req->KeyBytes, KeyBytes, KeySizeBytes);

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_SET_READ2_AES_KEY);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }

        CtrlMsgB2FAckSetRead2AesKey *resp = (CtrlMsgB2FAckSetRead2AesKey *)(CtrlMsgBuf + sizeof(MsgHeader));
        return resp->Result;
    }

    //
    // Async read from a file
    //
    //
    ErrorCodeT DDSBackEndBridge::ReadFile(FileIdT FileId, FileSizeT Offset, BufferT DestBuffer, FileIOSizeT BytesToRead,
                                          FileIOSizeT *BytesRead, ContextT Context, PollT *Poll)
    {
        RequestIdT requestId = ((FileIOT *)Context)->RequestId;

        bool bufferResult = InsertReadRequest(Poll->Channels[0].RequestRing, requestId, FileId, Offset, BytesToRead,
                                              BytesToRead, nullptr, nullptr, nullptr, 0, false);

        if (!bufferResult)
        {
            return DDS_ERROR_CODE_REQUEST_RING_FAILURE;
        }

        return DDS_ERROR_CODE_IO_PENDING;
    }

    //
    // Async read from a file with explicit stage sizes (read2)
    //
    ErrorCodeT DDSBackEndBridge::ReadFile2(FileIdT FileId, FileSizeT Offset, BufferT DestBuffer,
                                          FileIOSizeT LogicalBytesToRead, const FileIOSizeT* StageSizes,
                                          const FileIOSizeT* StageInputOffsets,
                                          const FileIOSizeT* StageInputLengths, uint16_t StageCount,
                                          FileIOSizeT *BytesRead, FileIOSizeT *LogicalBytesRead, ContextT Context,
                                          PollT *Poll)
    {
        RequestIdT requestId = ((FileIOT *)Context)->RequestId;

        if (!StageSizes || !StageInputOffsets || !StageInputLengths || StageCount < 1 || StageCount > 2) {
            return DDS_ERROR_CODE_INVALID_PARAM;
        }
        // Validate stage windows against the previous source bytes:
        // stage0 -> logical read bytes, stageN -> stage(N-1) output bytes.
        FileIOSizeT sourceBytes = LogicalBytesToRead;
        for (uint16_t stageIndex = 0; stageIndex < StageCount; stageIndex++) {
            if (StageInputOffsets[stageIndex] > sourceBytes || StageInputLengths[stageIndex] > sourceBytes ||
                StageInputLengths[stageIndex] > sourceBytes - StageInputOffsets[stageIndex]) {
                return DDS_ERROR_CODE_INVALID_PARAM;
            }
            sourceBytes = StageSizes[stageIndex];
        }
        FileIOSizeT outputBytes = StageSizes[StageCount - 1];
        // Task 2.1: ReadFile2 requests go to channel 1 (pread2).
        bool bufferResult = InsertReadRequest(Poll->Channels[DDS_IO_CHANNEL_PREAD2].RequestRing, requestId, FileId,
                                              Offset, LogicalBytesToRead, outputBytes, StageSizes, StageInputOffsets,
                                              StageInputLengths, StageCount, true);

        if (!bufferResult)
        {
            return DDS_ERROR_CODE_REQUEST_RING_FAILURE;
        }

        // jason: BytesRead/LogicalBytesRead are populated on completion.
        (void)BytesRead;
        (void)LogicalBytesRead;
        (void)DestBuffer;

        return DDS_ERROR_CODE_IO_PENDING;
    }

    //
    // Async read from a file with scattering
    //
    //
    ErrorCodeT DDSBackEndBridge::ReadFileScatter(FileIdT FileId, FileSizeT Offset, BufferT *DestBufferArray,
                                                 FileIOSizeT BytesToRead, FileIOSizeT *BytesRead, ContextT Context,
                                                 PollT *Poll)
    {
        RequestIdT requestId = ((FileIOT *)Context)->RequestId;

        bool bufferResult = InsertReadRequest(Poll->Channels[0].RequestRing, requestId, FileId, Offset, BytesToRead,
                                              BytesToRead, nullptr, nullptr, nullptr, 0, false);

        if (!bufferResult)
        {
            return DDS_ERROR_CODE_REQUEST_RING_FAILURE;
        }

        return DDS_ERROR_CODE_IO_PENDING;
    }

    //
    // Async write to a file
    //
    //
    ErrorCodeT DDSBackEndBridge::WriteFile(FileIdT FileId, FileSizeT Offset, BufferT SourceBuffer,
                                           FileIOSizeT BytesToWrite, FileIOSizeT *BytesWritten, ContextT Context,
                                           PollT *Poll)
    {
        RequestIdT requestId = ((FileIOT *)Context)->RequestId;

        bool bufferResult = InsertWriteFileRequest(Poll->Channels[0].RequestRing, requestId, FileId, Offset,
                                                   BytesToWrite, SourceBuffer);

        if (!bufferResult)
        {
            return DDS_ERROR_CODE_REQUEST_RING_FAILURE;
        }

        return DDS_ERROR_CODE_IO_PENDING;
    }

    //
    // Async write to a file with gathering
    //
    //
    ErrorCodeT DDSBackEndBridge::WriteFileGather(FileIdT FileId, FileSizeT Offset, BufferT *SourceBufferArray,
                                                 FileIOSizeT BytesToWrite, FileIOSizeT *BytesWritten, ContextT Context,
                                                 PollT *Poll)
    {
        RequestIdT requestId = ((FileIOT *)Context)->RequestId;

        bool bufferResult = InsertWriteFileGatherRequest(Poll->Channels[0].RequestRing, requestId, FileId, Offset,
                                                         BytesToWrite, SourceBufferArray);

        if (!bufferResult)
        {
            return DDS_ERROR_CODE_REQUEST_RING_FAILURE;
        }

        return DDS_ERROR_CODE_IO_PENDING;
    }

    //
    // Get file properties by file id.
    // - Map backend-only DPU file info into front-end FilePropertiesT.
    // - Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::GetFileInformationById(FileIdT FileId, FilePropertiesT *FileProperties)
    {
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        //
        // Send a get file information request to the back end
        //
        //
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_GET_FILE_INFO;

        CtrlMsgF2BReqGetFileInfo *req = (CtrlMsgF2BReqGetFileInfo *)(CtrlMsgBuf + sizeof(MsgHeader));
        req->FileId = FileId;

        // CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqGetFileInfo);

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_GET_FILE_INFO);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }

        CtrlMsgB2FAckGetFileInfo *resp = (CtrlMsgB2FAckGetFileInfo *)(CtrlMsgBuf + sizeof(MsgHeader));
        // memcpy(FileProperties, &resp->FileInfo, sizeof(FilePropertiesT));
        // Map backend-only file info to front-end FilePropertiesT.
        FileProperties->FileAttributes = resp->FileInfo.FileAttributes;
        FileProperties->CreationTime = resp->FileInfo.CreationTime;
        FileProperties->LastAccessTime = resp->FileInfo.LastAccessTime;
        FileProperties->LastWriteTime = resp->FileInfo.LastWriteTime;
        FileProperties->FileSize = resp->FileInfo.FileSize;
        // Access/share/position are front-end state; default to 0 on rehydration.
        FileProperties->Access = 0;
        FileProperties->ShareMode = 0;
        FileProperties->Position = 0;
        strncpy(FileProperties->FileName, resp->FileInfo.FileName, sizeof(FileProperties->FileName) - 1);
        FileProperties->FileName[sizeof(FileProperties->FileName) - 1] = '\0';
        return resp->Result;
    }

    //
    // Get file attributes by file name.
    // Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::GetFileAttributes(FileIdT FileId, FileAttributesT *FileAttributes)
    {
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        //
        // Send a get file attributes request to the back end
        //
        //
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_GET_FILE_ATTR;

        CtrlMsgF2BReqGetFileAttr *req = (CtrlMsgF2BReqGetFileAttr *)(CtrlMsgBuf + sizeof(MsgHeader));
        req->FileId = FileId;

        // CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqGetFileAttr);

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_GET_FILE_ATTR);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }

        CtrlMsgB2FAckGetFileAttr *resp = (CtrlMsgB2FAckGetFileAttr *)(CtrlMsgBuf + sizeof(MsgHeader));
        *FileAttributes = resp->FileAttr;
        return resp->Result;
    }

    //
    // Get the size of free space on the storage.
    // Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::GetStorageFreeSpace(FileSizeT *StorageFreeSpace)
    {
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        //
        // Send a get free space request to the back end
        //
        //
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_GET_FREE_SPACE;

        CtrlMsgF2BReqGetFreeSpace *req = (CtrlMsgF2BReqGetFreeSpace *)(CtrlMsgBuf + sizeof(MsgHeader));
        req->Dummy = 42;

        // CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqGetFreeSpace);

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_GET_FREE_SPACE);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }

        CtrlMsgB2FAckGetFreeSpace *resp = (CtrlMsgB2FAckGetFreeSpace *)(CtrlMsgBuf + sizeof(MsgHeader));
        *StorageFreeSpace = resp->FreeSpace;
        return resp->Result;
    }

    //
    // Move an existing file or a directory,
    // including its children.
    // Serialized to protect shared control-plane buffers.
    //
    ErrorCodeT DDSBackEndBridge::MoveFile(FileIdT FileId, const char *NewFileName)
    {
        ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        // Serialize control-plane ops; CtrlMsgBuf/CQ are shared.
        std::lock_guard<std::mutex> lock(ctrl_mutex);

        //
        // Send a move file request to the back end
        //
        //
        ((MsgHeader *)CtrlMsgBuf)->MsgId = CTRL_MSG_F2B_REQ_MOVE_FILE;

        CtrlMsgF2BReqMoveFile *req = (CtrlMsgF2BReqMoveFile *)(CtrlMsgBuf + sizeof(MsgHeader));
        req->FileId = FileId;
        strcpy(req->NewFileName, NewFileName);

        // CtrlSgl->BufferLength = sizeof(MsgHeader) + sizeof(CtrlMsgF2BReqMoveFile);

        result = SendCtrlMsgAndWait(this, CTRL_MSG_B2F_ACK_MOVE_FILE);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            return result;
        }

        CtrlMsgB2FAckMoveFile *resp = (CtrlMsgB2FAckMoveFile *)(CtrlMsgBuf + sizeof(MsgHeader));
        return resp->Result;
    }

    //
    // Pad a contiguous buffer with 0xFF starting at offset.
    //
    static inline void PadContiguousBuffer(BufferT Buffer, FileIOSizeT Offset, FileIOSizeT Bytes)
    {
        if (!Buffer || Bytes == 0)
        {
            return;
        }
        memset((char *)Buffer + Offset, 0xFF, Bytes);
    }

    //
    // Pad a scatter buffer array with 0xFF starting at logical offset.
    //
    static inline void PadScatterBuffer(BufferT *Buffers, FileIOSizeT Offset, FileIOSizeT Bytes)
    {
        if (!Buffers || Bytes == 0)
        {
            return;
        }
        FileIOSizeT remaining = Bytes;
        FileIOSizeT cursor = Offset;
        while (remaining)
        {
            FileIOSizeT segIndex = cursor / DDS_PAGE_SIZE;
            FileIOSizeT segOffset = cursor % DDS_PAGE_SIZE;
            FileIOSizeT segSpace = DDS_PAGE_SIZE - segOffset;
            FileIOSizeT toFill = remaining < segSpace ? remaining : segSpace;
            memset((char *)Buffers[segIndex] + segOffset, 0xFF, toFill);
            remaining -= toFill;
            cursor += toFill;
        }
    }

    //
    // Slice a splittable buffer at an offset/length, preserving wrap-around semantics.
    // Used to select the final stage payload inside a larger read2 response payload.
    //
    static inline SplittableBufferT SliceSplittableBuffer(const SplittableBufferT *Buffer, FileIOSizeT Offset,
                                                          FileIOSizeT Bytes)
    {
        SplittableBufferT slice = {};
        if (!Buffer || Bytes == 0 || Offset >= Buffer->TotalSize)
        {
            return slice;
        }
        FileIOSizeT maxBytes = Buffer->TotalSize - Offset;
        if (Bytes > maxBytes)
        {
            Bytes = maxBytes;
        }
        slice.TotalSize = Bytes;
        if (Offset < Buffer->FirstSize)
        {
            slice.FirstAddr = Buffer->FirstAddr + Offset;
            slice.FirstSize = Buffer->FirstSize - Offset;
            if (slice.FirstSize > Bytes)
            {
                slice.FirstSize = Bytes;
                slice.SecondAddr = nullptr;
                return slice;
            }
            slice.SecondAddr = Buffer->SecondAddr;
            return slice;
        }
        if (!Buffer->SecondAddr)
        {
            return slice;
        }
        FileIOSizeT secondOffset = Offset - Buffer->FirstSize;
        slice.FirstAddr = Buffer->SecondAddr + secondOffset;
        slice.FirstSize = Bytes;
        slice.SecondAddr = nullptr;
        return slice;
    }

    //
    // Handle the completion of a file I/O operation
    // Updated: skip aligned prefix using IO->CopyStart before copying payload.
    //
    //
    static inline void CompleteIO(FileIOT *IO, BuffMsgB2FAckHeader *Resp, SplittableBufferT *DataBuff)
    {
        if (IO->IsRead)
        {
            if (IO->AppBuffer)
            {
                //
                // Due to alignment, DataBuff.TotalSize might be larger than the actual data
                // Hence, use Resp->BytesServiced as the bytes to copy
                //
                //
                // FileIOSizeT bytesToCopy = Resp->BytesServiced;
                // int delta = (int)bytesToCopy - (int)DataBuff->FirstSize;
                // if (delta > 0) {
                //     memcpy(IO->AppBuffer, DataBuff->FirstAddr, DataBuff->FirstSize);
                //     memcpy(IO->AppBuffer + DataBuff->FirstSize, DataBuff->SecondAddr, delta);
                // }
                // else {
                //     memcpy(IO->AppBuffer, DataBuff->FirstAddr, bytesToCopy);
                // }
                // jason: Skip the aligned prefix (copyStart) before copying logical bytes.
                FileIOSizeT outputBytes = Resp->BytesServiced;
                FileIOSizeT logicalBytes = Resp->LogicalBytes;
                if (logicalBytes == 0 && outputBytes > 0 && !IO->IsRead2)
                {
                    // jason: maintain legacy behavior when logical bytes are not populated.
                    logicalBytes = outputBytes;
                }
                SplittableBufferT payload = *DataBuff;
                FileIOSizeT bytesToCopy = 0;
                if (IO->IsRead2 && IO->StageCount > 0)
                {
                    // jason: read2 responses carry stage0 + intermediate stages; only the final stage is delivered.
                    FileIOSizeT finalOffset = IO->AlignedBytes;
                    for (uint16_t stageIndex = 0; stageIndex + 1 < IO->StageCount; stageIndex++)
                    {
                        finalOffset += IO->StageSizes[stageIndex];
                    }
                    payload = SliceSplittableBuffer(DataBuff, finalOffset, outputBytes);
                    bytesToCopy = outputBytes;
                    if (bytesToCopy > payload.TotalSize)
                    {
                        bytesToCopy = payload.TotalSize;
                    }
                    if (bytesToCopy > IO->OutputBytes)
                    {
                        bytesToCopy = IO->OutputBytes;
                    }
                }
                else
                {
                    // jason: legacy reads deliver logical bytes starting at the aligned copy-start.
                    bytesToCopy = logicalBytes;
                    if (bytesToCopy > outputBytes)
                    {
                        bytesToCopy = outputBytes;
                    }
                    if (bytesToCopy > IO->BytesDesired)
                    {
                        bytesToCopy = IO->BytesDesired;
                    }
                    FileIOSizeT copyStart = IO->CopyStart;
                    if (copyStart >= payload.TotalSize)
                    {
                        bytesToCopy = 0;
                    }
                    else
                    {
                        payload.TotalSize -= copyStart;
                        if (copyStart < payload.FirstSize)
                        {
                            payload.FirstAddr += copyStart;
                            payload.FirstSize -= copyStart;
                        }
                        else
                        {
                            FileIOSizeT secondOffset = copyStart - payload.FirstSize;
                            payload.FirstAddr = payload.SecondAddr ? payload.SecondAddr + secondOffset : payload.FirstAddr;
                            payload.FirstSize = payload.TotalSize;
                            payload.SecondAddr = NULL;
                        }
                    }
                }
                if (bytesToCopy > 0)
                {
                    int delta = (int)bytesToCopy - (int)payload.FirstSize;
                    if (delta > 0)
                    {
                        memcpy(IO->AppBuffer, payload.FirstAddr, payload.FirstSize);
                        memcpy(IO->AppBuffer + payload.FirstSize, payload.SecondAddr, delta);
                    }
                    else
                    {
                        memcpy(IO->AppBuffer, payload.FirstAddr, bytesToCopy);
                    }
                }
                // Original host-side padding for read2 preserved for reference.
                // if (IO->IsRead2 && outputBytes > bytesToCopy)
                // {
                //     PadContiguousBuffer(IO->AppBuffer, bytesToCopy, outputBytes - bytesToCopy);
                // }
            }
            else
            {
                //
                // A scattered read
                // Due to alignment, DataBuff.TotalSize might be larger than the actual data
                // Hence, use Resp->BytesServiced as the bytes to copy
                //
                //
                // FileIOSizeT bytesToCopy = Resp->BytesServiced;
                // FileIOSizeT firstSize = DataBuff->FirstSize;
                // if (firstSize > bytesToCopy) {
                //     firstSize = bytesToCopy;
                // }
                // FileIOSizeT secondSize = bytesToCopy - firstSize;
                //
                // int numWholePagesInFirst = firstSize / DDS_PAGE_SIZE;
                // int segIndex = 0;
                //
                // //
                // // Handle whole pages
                // //
                // //
                // for (; segIndex != numWholePagesInFirst; segIndex++) {
                //     memcpy(IO->AppBufferArray[segIndex], DataBuff->FirstAddr + segIndex * (size_t)DDS_PAGE_SIZE,
                //     DDS_PAGE_SIZE);
                // }
                //
                // //
                // // Handle the residual
                // //
                // //
                // FileIOSizeT residual = firstSize % DDS_PAGE_SIZE;
                // if (residual) {
                //     memcpy(IO->AppBufferArray[segIndex], DataBuff->FirstAddr + segIndex * (size_t)DDS_PAGE_SIZE,
                //     residual);
                // }
                //
                // if (secondSize) {
                //     FileIOSizeT secondLeftResidual = DDS_PAGE_SIZE - residual;
                //
                //     if (secondSize > secondLeftResidual) {
                //         //
                //         // Handle the left residual
                //         //
                //         //
                //         memcpy(IO->AppBufferArray[segIndex] + residual, DataBuff->SecondAddr, secondLeftResidual);
                //         segIndex++;
                //
                //         //
                //         // Handle whole pages
                //         //
                //         //
                //         secondSize -= secondLeftResidual;
                //         int numWholePagesInSecond = secondSize / DDS_PAGE_SIZE;
                //         for (int j = 0; j != numWholePagesInSecond; segIndex++, j++) {
                //             memcpy(IO->AppBufferArray[segIndex], DataBuff->SecondAddr + secondLeftResidual + j *
                //             (size_t)DDS_PAGE_SIZE, DDS_PAGE_SIZE);
                //         }
                //
                //         //
                //         // Handle the right residual
                //         //
                //         //
                //         FileIOSizeT secondRightResidual = secondSize % DDS_PAGE_SIZE;
                //         if (secondRightResidual) {
                //             memcpy(IO->AppBufferArray[segIndex], DataBuff->SecondAddr + secondRightResidual +
                //             numWholePagesInSecond * (size_t)DDS_PAGE_SIZE, secondRightResidual);
                //         }
                //     }
                //     else {
                //         memcpy(IO->AppBufferArray[segIndex] + residual, DataBuff->SecondAddr, secondSize);
                //     }
                // }
                // jason: Skip the aligned prefix (copyStart) before copying logical bytes.
                FileIOSizeT outputBytes = Resp->BytesServiced;
                FileIOSizeT logicalBytes = Resp->LogicalBytes;
                if (logicalBytes == 0 && outputBytes > 0 && !IO->IsRead2)
                {
                    // jason: maintain legacy behavior when logical bytes are not populated.
                    logicalBytes = outputBytes;
                }
                SplittableBufferT payload = *DataBuff;
                FileIOSizeT bytesToCopy = 0;
                if (IO->IsRead2 && IO->StageCount > 0)
                {
                    FileIOSizeT finalOffset = IO->AlignedBytes;
                    for (uint16_t stageIndex = 0; stageIndex + 1 < IO->StageCount; stageIndex++)
                    {
                        finalOffset += IO->StageSizes[stageIndex];
                    }
                    payload = SliceSplittableBuffer(DataBuff, finalOffset, outputBytes);
                    bytesToCopy = outputBytes;
                    if (bytesToCopy > payload.TotalSize)
                    {
                        bytesToCopy = payload.TotalSize;
                    }
                }
                else
                {
                    bytesToCopy = logicalBytes;
                    if (bytesToCopy > outputBytes)
                    {
                        bytesToCopy = outputBytes;
                    }
                    FileIOSizeT copyStart = IO->CopyStart;
                    if (copyStart >= payload.TotalSize)
                    {
                        bytesToCopy = 0;
                    }
                    else
                    {
                        payload.TotalSize -= copyStart;
                        if (copyStart < payload.FirstSize)
                        {
                            payload.FirstAddr += copyStart;
                            payload.FirstSize -= copyStart;
                        }
                        else
                        {
                            FileIOSizeT secondOffset = copyStart - payload.FirstSize;
                            payload.FirstAddr = payload.SecondAddr ? payload.SecondAddr + secondOffset : payload.FirstAddr;
                            payload.FirstSize = payload.TotalSize;
                            payload.SecondAddr = NULL;
                        }
                    }
                }
                if (bytesToCopy > 0)
                {
                    FileIOSizeT firstSize = payload.FirstSize;
                    if (firstSize > bytesToCopy)
                    {
                        firstSize = bytesToCopy;
                    }
                    FileIOSizeT secondSize = bytesToCopy - firstSize;

                    int numWholePagesInFirst = firstSize / DDS_PAGE_SIZE;
                    int segIndex = 0;

                    //
                    // Handle whole pages
                    //
                    //
                    for (; segIndex != numWholePagesInFirst; segIndex++)
                    {
                        memcpy(IO->AppBufferArray[segIndex], payload.FirstAddr + segIndex * (size_t)DDS_PAGE_SIZE,
                               DDS_PAGE_SIZE);
                    }

                    //
                    // Handle the residual
                    //
                    //
                    FileIOSizeT residual = firstSize % DDS_PAGE_SIZE;
                    if (residual)
                    {
                        memcpy(IO->AppBufferArray[segIndex], payload.FirstAddr + segIndex * (size_t)DDS_PAGE_SIZE,
                               residual);
                    }

                    if (secondSize)
                    {
                        FileIOSizeT secondLeftResidual = DDS_PAGE_SIZE - residual;

                        if (secondSize > secondLeftResidual)
                        {
                            //
                            // Handle the left residual
                            //
                            //
                            memcpy(IO->AppBufferArray[segIndex] + residual, payload.SecondAddr, secondLeftResidual);
                            segIndex++;

                            //
                            // Handle whole pages
                            //
                            //
                            secondSize -= secondLeftResidual;
                            int numWholePagesInSecond = secondSize / DDS_PAGE_SIZE;
                            for (int j = 0; j != numWholePagesInSecond; segIndex++, j++)
                            {
                                memcpy(IO->AppBufferArray[segIndex],
                                       payload.SecondAddr + secondLeftResidual + j * (size_t)DDS_PAGE_SIZE,
                                       DDS_PAGE_SIZE);
                            }

                            //
                            // Handle the right residual
                            //
                            //
                            FileIOSizeT secondRightResidual = secondSize % DDS_PAGE_SIZE;
                            if (secondRightResidual)
                            {
                                memcpy(IO->AppBufferArray[segIndex],
                                       payload.SecondAddr + secondRightResidual +
                                           numWholePagesInSecond * (size_t)DDS_PAGE_SIZE,
                                       secondRightResidual);
                            }
                        }
                        else
                        {
                            memcpy(IO->AppBufferArray[segIndex] + residual, payload.SecondAddr, secondSize);
                        }
                    }
                }
                // Original host-side padding for read2 preserved for reference.
                // if (IO->IsRead2 && outputBytes > bytesToCopy)
                // {
                //     PadScatterBuffer(IO->AppBufferArray, bytesToCopy, outputBytes - bytesToCopy);
                // }
            }
        }
    }

#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
    //
    // Retrieve a response from the cached batch.
    // Updated: acquire Result to order BytesServiced visibility.
    // Updated: honor payload padding/alignment within batched responses.
    // Updated: acquire published request metadata before copying payload bytes.
    //
    // Task 2.2: ChannelOut identifies which channel produced the response.
    // Currently always 0 (primary) until backend dual-channel polling is enabled (Task 3.x).
    // Task 3.x: chIdx identifies which channel's batch/ring to process.
    // ChannelOut is set to chIdx so PollWait can look up the correct outstanding context.
    ErrorCodeT DDSBackEndBridge::GetResponseFromCachedBatch(PollT *Poll, FileIOSizeT *BytesServiced, RequestIdT *ReqId,
                                                            int *ChannelOut, int chIdx)
    {
        PollChannelBatchCacheT &batch = Poll->Channels[chIdx].BatchCache;

        // Set ChannelOut to the actual channel being processed.
        if (ChannelOut)
            *ChannelOut = chIdx;

        // jason: implicit wrap rule within a batch; skip tail slack that cannot hold size+header.
        // jason: no-split rings may inject a zero-sized wrap marker before the next response record.
        const FileIOSizeT headerBytes = (FileIOSizeT)(sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));
        while (true)
        {
            bool inFirstSegment = (batch.ProcessedBytes < batch.BatchRef.FirstSize);
            FileIOSizeT segmentRemaining =
                inFirstSegment ? (FileIOSizeT)(batch.BatchRef.FirstSize - batch.ProcessedBytes)
                               : (FileIOSizeT)(batch.BatchRef.TotalSize - batch.ProcessedBytes);

            if (segmentRemaining == 0)
            {
                IncrementProgress(Poll->Channels[chIdx].ResponseRing, batch.BatchRef.TotalSize +
                                                                          sizeof(FileIOSizeT) +
                                                                          sizeof(BuffMsgB2FAckHeader));
                batch.NextResponse = NULL;
                batch.ProcessedBytes = 0;
                return DDS_ERROR_CODE_NO_COMPLETION;
            }

            if (inFirstSegment && segmentRemaining < headerBytes)
            {
                // jason: not enough bytes for a response header on this segment; consume slack.
                batch.ProcessedBytes += segmentRemaining;
                if (batch.BatchRef.SecondAddr)
                {
                    batch.NextResponse = batch.BatchRef.SecondAddr;
                    continue;
                }
                IncrementProgress(Poll->Channels[chIdx].ResponseRing, batch.BatchRef.TotalSize +
                                                                          sizeof(FileIOSizeT) +
                                                                          sizeof(BuffMsgB2FAckHeader));
                batch.NextResponse = NULL;
                batch.ProcessedBytes = 0;
                return DDS_ERROR_CODE_NO_COMPLETION;
            }

            FileIOSizeT respSizeProbe = *((FileIOSizeT *)batch.NextResponse);
            if (respSizeProbe == 0)
            {
                // jason: explicit wrap marker; consume this segment's slack and continue.
                batch.ProcessedBytes += segmentRemaining;
                if (inFirstSegment && batch.BatchRef.SecondAddr)
                {
                    batch.NextResponse = batch.BatchRef.SecondAddr;
                    continue;
                }
                IncrementProgress(Poll->Channels[chIdx].ResponseRing, batch.BatchRef.TotalSize +
                                                                          sizeof(FileIOSizeT) +
                                                                          sizeof(BuffMsgB2FAckHeader));
                batch.NextResponse = NULL;
                batch.ProcessedBytes = 0;
                return DDS_ERROR_CODE_NO_COMPLETION;
            }
            break;
        }

        std::atomic_thread_fence(std::memory_order_acquire);
        FileIOSizeT respSize = *((FileIOSizeT *)batch.NextResponse);
        FileIOSizeT remainingInBatch = (batch.BatchRef.TotalSize > batch.ProcessedBytes)
                                           ? (batch.BatchRef.TotalSize - batch.ProcessedBytes)
                                           : 0;
        if (respSize < headerBytes || respSize > remainingInBatch)
        {
            // Updated: with CQE-bounded publication and ordered wrapped-batch
            // transfer, this should not happen. Keep a silent guard here so we
            // do not step past the cached batch on malformed metadata.
            return DDS_ERROR_CODE_NO_COMPLETION;
        }
        BuffMsgB2FAckHeader *resp = (BuffMsgB2FAckHeader *)(batch.NextResponse + sizeof(FileIOSizeT));
        // jason: acquire Result to ensure BytesServiced is visible.
        ErrorCodeT respResult = DDS_ATOMIC_ERRORCODE_LOAD(&resp->Result, DDS_ATOMIC_ORDER_ACQUIRE);

        if (resp->RequestId >= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL)
        {
            return DDS_ERROR_CODE_NO_COMPLETION;
        }
        FileIOT *io = Poll->Channels[chIdx].OutstandingRequests[resp->RequestId];
        if (!io)
        {
            return DDS_ERROR_CODE_NO_COMPLETION;
        }
        *ReqId = resp->RequestId;
        *BytesServiced = resp->BytesServiced;
        // jason: capture logical bytes for read2 callers.
        io->LogicalBytesServiced = resp->LogicalBytes ? resp->LogicalBytes : resp->BytesServiced;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
        io->OffloadReadTimeNs = resp->OffloadReadTimeNs;
        io->OffloadStage1TimeNs = resp->OffloadStage1TimeNs;
        io->OffloadStage2TimeNs = resp->OffloadStage2TimeNs;
#endif

        SplittableBufferT dataBuff;
        // dataBuff.TotalSize = respSize - sizeof(FileIOSizeT) - sizeof(BuffMsgB2FAckHeader);
        // int delta = (int)ChBatch[chIdx].ProcessedBytes + (int)sizeof(FileIOSizeT) + (int)sizeof(BuffMsgB2FAckHeader)
        // - (int)ChBatch[chIdx].BatchRef.FirstSize; if (delta >= 0) {
        //     dataBuff.FirstAddr = ChBatch[chIdx].BatchRef.SecondAddr + delta;
        //     dataBuff.FirstSize = dataBuff.TotalSize;
        //     dataBuff.SecondAddr = NULL;
        // }
        // else {
        //     dataBuff.FirstAddr = ChBatch[chIdx].BatchRef.FirstAddr + ((int)(ChBatch[chIdx].BatchRef.FirstSize) +
        //     delta);
        //
        //     if (((int)dataBuff.TotalSize + delta) > 0) {
        //         dataBuff.FirstSize = 0 - delta;
        //         dataBuff.SecondAddr = ChBatch[chIdx].BatchRef.SecondAddr;
        //     }
        //     else {
        //         dataBuff.FirstSize = dataBuff.TotalSize;
        //         dataBuff.SecondAddr = NULL;
        //     }
        // }
        // jason: Align payload start to backend buffer alignment (padding after header).
        FileIOSizeT payloadAlign = GetResponsePayloadAlignment();
        if (respSize > headerBytes)
        {
            int respOffset = (int)(batch.NextResponse - Poll->Channels[chIdx].ResponseRing->Buffer);
            int dataOffset = (respOffset + (int)headerBytes) % DDS_RESPONSE_RING_BYTES;
            uintptr_t baseAddr = (uintptr_t)Poll->Channels[chIdx].ResponseRing->Buffer;
            uintptr_t payloadAddr = baseAddr + (uintptr_t)dataOffset;
            uintptr_t alignedAddr = ((payloadAddr + payloadAlign - 1) / payloadAlign) * payloadAlign;
            FileIOSizeT pad = 0;
            if (alignedAddr < baseAddr + DDS_RESPONSE_RING_BYTES)
            {
                pad = (FileIOSizeT)(alignedAddr - payloadAddr);
                dataOffset = (dataOffset + (int)pad) % DDS_RESPONSE_RING_BYTES;
            }
            else
            {
                // jason: wrap to base; assume ring base is aligned.
                pad = (FileIOSizeT)(DDS_RESPONSE_RING_BYTES - dataOffset);
                dataOffset = 0;
            }

            dataBuff.TotalSize = respSize - headerBytes - pad;
            dataBuff.FirstAddr = Poll->Channels[chIdx].ResponseRing->Buffer + dataOffset;
            if (dataOffset + (int)dataBuff.TotalSize > DDS_RESPONSE_RING_BYTES)
            {
                dataBuff.FirstSize = DDS_RESPONSE_RING_BYTES - dataOffset;
                dataBuff.SecondAddr = Poll->Channels[chIdx].ResponseRing->Buffer;
            }
            else
            {
                dataBuff.FirstSize = dataBuff.TotalSize;
                dataBuff.SecondAddr = NULL;
            }
        }
        else
        {
            dataBuff.TotalSize = 0;
            dataBuff.FirstAddr = NULL;
            dataBuff.FirstSize = 0;
            dataBuff.SecondAddr = NULL;
        }
        // CompleteIO(io, resp, &dataBuff);
        // jason: ensure CopyStart/AppBuffer are visible before copying from the response ring.
        (void)io->IsAvailable.load(std::memory_order_acquire);
        CompleteIO(io, resp, &dataBuff);

        batch.ProcessedBytes += respSize;
        if (batch.BatchRef.TotalSize - batch.ProcessedBytes <
            (sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader)))
        {
            //
            // Advance the response ring progress and reset the batch cache
            // Additional |BuffMsgB2FAckHeader| bytes in meta data because of alignment
            //
            //
            IncrementProgress(Poll->Channels[chIdx].ResponseRing,
                              batch.BatchRef.TotalSize + sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));

            batch.NextResponse = NULL;
            batch.ProcessedBytes = 0;
        }
        else
        {
            //
            // Move to the next response
            //
            //
            if (batch.ProcessedBytes >= batch.BatchRef.FirstSize)
            {
                batch.NextResponse = batch.BatchRef.SecondAddr + batch.ProcessedBytes - batch.BatchRef.FirstSize;
            }
            else
            {
                batch.NextResponse = batch.BatchRef.FirstAddr + batch.ProcessedBytes;
            }
        }

        // return resp->Result;
        return respResult;
    }
#endif

    //
    // Retrieve a response from the response ring.
    // Thread-safe for DDS_NOTIFICATION_METHOD_TIMER
    // Not thread-safe for DDS_NOTIFICATION_METHOD_INTERRUPT
    // Updated: acquire Result to order BytesServiced visibility.
    // Updated: gate new response consumption on CQE notifications when using interrupt mode.
    // Updated: acquire published request metadata before copying payload bytes.
    //
    // Task 3.x: polls both channels in round-robin order, returning the first
    // available completion. ChannelOut is set to the channel that produced the response.
    ErrorCodeT DDSBackEndBridge::GetResponse(PollT *Poll, size_t WaitTime, FileIOSizeT *BytesServiced,
                                             RequestIdT *ReqId, int *ChannelOut)
    {
        if (ChannelOut)
            *ChannelOut = DDS_IO_CHANNEL_PRIMARY;

#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
        //
        // Phase 1: check cached batches across all channels (round-robin start).
        //
        for (int ci = 0; ci < DDS_NUM_IO_CHANNELS; ci++)
        {
            int ch = (Poll->LastPolledChannel + ci) % DDS_NUM_IO_CHANNELS;
            if (Poll->Channels[ch].BatchCache.NextResponse)
            {
                Poll->LastPolledChannel = (ch + 1) % DDS_NUM_IO_CHANNELS;
                return GetResponseFromCachedBatch(Poll, BytesServiced, ReqId, ChannelOut, ch);
            }
        }

#if DDS_NOTIFICATION_METHOD == DDS_NOTIFICATION_METHOD_INTERRUPT
        //
        // Phase 2 (interrupt): drain CQE notifications from all channels.
        //
        for (int ch = 0; ch < DDS_NUM_IO_CHANNELS; ch++)
        {
            if (Poll->Channels[ch].PendingNotifications.load(std::memory_order_relaxed) == 0)
            {
                int completions = 0;
                uint32_t lastImmData = 0;
                int immCompletions = 0;
                if (WaitTime == INFINITE)
                {
                    completions = Poll->Channels[ch].MsgBuffer->WaitForACompletion(true, &lastImmData, &immCompletions);
                }
                else if (WaitTime == 0)
                {
                    completions =
                        Poll->Channels[ch].MsgBuffer->WaitForACompletion(false, &lastImmData, &immCompletions);
                }
                if (immCompletions > 0)
                {
                    Poll->Channels[ch].PendingNotifications.fetch_add(static_cast<size_t>(immCompletions),
                                                                      std::memory_order_relaxed);
                    Poll->Channels[ch].PublishedTail.store((int)lastImmData, std::memory_order_relaxed);
                }
            }
        }
#endif

        //
        // Phase 3: try fetching a new batch from each channel (round-robin).
        //
        for (int ci = 0; ci < DDS_NUM_IO_CHANNELS; ci++)
        {
            int ch = (Poll->LastPolledChannel + ci) % DDS_NUM_IO_CHANNELS;
            PollChannelBatchCacheT &batch = Poll->Channels[ch].BatchCache;
            int publishedTail = Poll->Channels[ch].PublishedTail.load(std::memory_order_relaxed);
            if (FetchResponseBatch(Poll->Channels[ch].ResponseRing, &batch.BatchRef, publishedTail))
            {
                batch.NextResponse = batch.BatchRef.FirstAddr;
                batch.ProcessedBytes = 0;

#if DDS_NOTIFICATION_METHOD == DDS_NOTIFICATION_METHOD_INTERRUPT
                // Consume one CQE credit per batch from this channel.
                if (Poll->Channels[ch].PendingNotifications.load(std::memory_order_relaxed) > 0)
                {
                    Poll->Channels[ch].PendingNotifications.fetch_sub(1, std::memory_order_relaxed);
                }
#endif

                Poll->LastPolledChannel = (ch + 1) % DDS_NUM_IO_CHANNELS;
                return GetResponseFromCachedBatch(Poll, BytesServiced, ReqId, ChannelOut, ch);
            }
        }
#else
        // Non-batch path: try each channel for a single response.
        BuffMsgB2FAckHeader *response;
        SplittableBufferT dataBuff;

#if DDS_NOTIFICATION_METHOD == DDS_NOTIFICATION_METHOD_INTERRUPT
        // Drain CQE notifications from all channels.
        for (int ch = 0; ch < DDS_NUM_IO_CHANNELS; ch++)
        {
            if (Poll->Channels[ch].PendingNotifications.load(std::memory_order_relaxed) == 0)
            {
                int completions = 0;
                uint32_t lastImmData = 0;
                int immCompletions = 0;
                if (WaitTime == INFINITE)
                {
                    completions = Poll->Channels[ch].MsgBuffer->WaitForACompletion(true, &lastImmData, &immCompletions);
                }
                else if (WaitTime == 0)
                {
                    completions =
                        Poll->Channels[ch].MsgBuffer->WaitForACompletion(false, &lastImmData, &immCompletions);
                }
                if (immCompletions > 0)
                {
                    Poll->Channels[ch].PendingNotifications.fetch_add(static_cast<size_t>(immCompletions),
                                                                      std::memory_order_relaxed);
                    Poll->Channels[ch].PublishedTail.store((int)lastImmData, std::memory_order_relaxed);
                }
            }
        }
#endif

        // Try fetching a single response from each channel (round-robin).
        for (int ci = 0; ci < DDS_NUM_IO_CHANNELS; ci++)
        {
            int ch = (Poll->LastPolledChannel + ci) % DDS_NUM_IO_CHANNELS;
            int publishedTail = Poll->Channels[ch].PublishedTail.load(std::memory_order_relaxed);
            if (FetchResponse(Poll->Channels[ch].ResponseRing, &response, &dataBuff, publishedTail))
            {
                // jason: validate the slot index before touching OutstandingRequests[].
                if (response->RequestId >= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL)
                {
                    return DDS_ERROR_CODE_NO_COMPLETION;
                }
                FileIOT *io = Poll->Channels[ch].OutstandingRequests[response->RequestId];
                if (!io)
                {
                    return DDS_ERROR_CODE_NO_COMPLETION;
                }
                ErrorCodeT respResult = DDS_ATOMIC_ERRORCODE_LOAD(&response->Result, DDS_ATOMIC_ORDER_ACQUIRE);
                *ReqId = response->RequestId;
                *BytesServiced = response->BytesServiced;
                io->LogicalBytesServiced = response->LogicalBytes ? response->LogicalBytes : response->BytesServiced;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
                io->OffloadReadTimeNs = response->OffloadReadTimeNs;
                io->OffloadStage1TimeNs = response->OffloadStage1TimeNs;
                io->OffloadStage2TimeNs = response->OffloadStage2TimeNs;
#endif

                (void)io->IsAvailable.load(std::memory_order_acquire);
                CompleteIO(io, response, &dataBuff);
                IncrementProgress(Poll->Channels[ch].ResponseRing,
                                  dataBuff.TotalSize + sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));

#if DDS_NOTIFICATION_METHOD == DDS_NOTIFICATION_METHOD_INTERRUPT
                if (Poll->Channels[ch].PendingNotifications.load(std::memory_order_relaxed) > 0)
                {
                    Poll->Channels[ch].PendingNotifications.fetch_sub(1, std::memory_order_relaxed);
                }
#endif

                if (ChannelOut)
                    *ChannelOut = ch;
                Poll->LastPolledChannel = (ch + 1) % DDS_NUM_IO_CHANNELS;
                return respResult;
            }
        }
#endif

        //
        // No response found on any channel.
        // Check wait time.
        //
#if DDS_NOTIFICATION_METHOD == DDS_NOTIFICATION_METHOD_INTERRUPT
        if (WaitTime > 0)
        {
            return DDS_ERROR_CODE_NOT_IMPLEMENTED;
        }
#elif DDS_NOTIFICATION_METHOD == DDS_NOTIFICATION_METHOD_TIMER
        size_t sleptMs = 0;

        while (sleptMs < WaitTime)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

            // Retry all channels after sleeping.
            for (int ci = 0; ci < DDS_NUM_IO_CHANNELS; ci++)
            {
                int ch = (Poll->LastPolledChannel + ci) % DDS_NUM_IO_CHANNELS;
#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
                PollChannelBatchCacheT &batch = Poll->Channels[ch].BatchCache;
                int publishedTail = Poll->Channels[ch].PublishedTail.load(std::memory_order_relaxed);
                if (FetchResponseBatch(Poll->Channels[ch].ResponseRing, &batch.BatchRef, publishedTail))
                {
                    batch.NextResponse = batch.BatchRef.FirstAddr;
                    batch.ProcessedBytes = 0;
                    Poll->LastPolledChannel = (ch + 1) % DDS_NUM_IO_CHANNELS;
                    return GetResponseFromCachedBatch(Poll, BytesServiced, ReqId, ChannelOut, ch);
                }
#else
                int publishedTail = Poll->Channels[ch].PublishedTail.load(std::memory_order_relaxed);
                if (FetchResponse(Poll->Channels[ch].ResponseRing, &response, &dataBuff, publishedTail))
                {
                    // jason: validate the slot index before touching OutstandingRequests[].
                    if (response->RequestId >= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL)
                    {
                        return DDS_ERROR_CODE_NO_COMPLETION;
                    }
                    FileIOT *io = Poll->Channels[ch].OutstandingRequests[response->RequestId];
                    if (!io)
                    {
                        return DDS_ERROR_CODE_NO_COMPLETION;
                    }
                    ErrorCodeT respResult = DDS_ATOMIC_ERRORCODE_LOAD(&response->Result, DDS_ATOMIC_ORDER_ACQUIRE);
                    *ReqId = response->RequestId;
                    *BytesServiced = response->BytesServiced;
                    io->LogicalBytesServiced =
                        response->LogicalBytes ? response->LogicalBytes : response->BytesServiced;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
                    io->OffloadReadTimeNs = response->OffloadReadTimeNs;
                    io->OffloadStage1TimeNs = response->OffloadStage1TimeNs;
                    io->OffloadStage2TimeNs = response->OffloadStage2TimeNs;
#endif
                    CompleteIO(io, response, &dataBuff);
                    IncrementProgress(Poll->Channels[ch].ResponseRing,
                                      dataBuff.TotalSize + sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));
                    if (ChannelOut)
                        *ChannelOut = ch;
                    Poll->LastPolledChannel = (ch + 1) % DDS_NUM_IO_CHANNELS;
                    return respResult;
                }
#endif
            }
            sleptMs++;
        }
#else
#error "Unknown notification method"
#endif

        return DDS_ERROR_CODE_NO_COMPLETION;
    }

#endif

}
