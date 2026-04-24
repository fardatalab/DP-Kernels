#include "DMABuffer.h"
#include "RingBufferProgressive.h"

/* libibverbs porting */
#include<cstring>
#include<stdexcept>
#include<errno.h>
#include<unistd.h>
#define RESOLVE_TIMEOUT_MS 2000

DMABuffer::DMABuffer(const char *_BackEndAddr, const unsigned short _BackEndPort, const size_t _Capacity,
                     const int _ClientId, const int _ChannelIndex)
{
    //
	// Record buffer capacity
	//
	//
	Capacity = _Capacity;
	ClientId = _ClientId;
	BufferId = -1;
    ChannelIndex = _ChannelIndex;

    //
	// Initialize NDSPI variables
	//
	//
	// Adapter = NULL;
	// AdapterFileHandle = NULL;
	// memset(&Ov, 0, sizeof(Ov));
	// CompQ = NULL;
	// QPair = NULL;
	// MemRegion = NULL;
	// MemWindow = NULL;
	ec = nullptr;
    cm_id = nullptr;
    pd = nullptr;
    cq = nullptr;
    qp = nullptr;
    mr = nullptr;
    msg_mr = nullptr;
	MsgSgl = NULL;
	BufferAddress = NULL;

	memset(MsgBuf, 0, BUFF_MSG_SIZE);
}

//
// Allocate the buffer with the specified capacity
// and register it to the NIC;
// Updated: pre-post receive WQEs for write-with-immediate notifications.
// Not thread-safe
//
bool
DMABuffer::Allocate(
	struct sockaddr_in* LocalSock,
	struct sockaddr_in* BackEndSock,
	const size_t QueueDepth,
	const size_t MaxSge,
	const size_t InlineThreshold
) {
	//
	// Set up RDMA with NDSPI
	//
	//
	// Ov.hEvent = CreateEvent(nullptr, FALSE, FALSE, nullptr);
	// if (Ov.hEvent == nullptr) {
	// 	printf("DMABuffer: failed to allocate event for overlapped operations\n");
	// 	return false;
	// }
	//
	// RDMC_OpenAdapter(&Adapter, LocalSock, &AdapterFileHandle, &Ov);
	// RDMC_CreateConnector(Adapter, AdapterFileHandle, &Connector);
	// RDMC_CreateCQ(Adapter, AdapterFileHandle, (DWORD)QueueDepth, &CompQ);
	// RDMC_CreateQueuePair(Adapter, CompQ, (DWORD)QueueDepth, (DWORD)MaxSge, (DWORD)InlineThreshold, &QPair);



	//
 	//
	// Set up RDMA with libibverbs
    //
	ec = rdma_create_event_channel();
    if (!ec) {
        fprintf(stderr, "Failed to create event channel, errno: %d\n", errno);
        return false;
    }

    // Create RDMA CM ID
    if (rdma_create_id(ec, &cm_id, nullptr, RDMA_PS_TCP)) {
        fprintf(stderr, "Failed to create CM ID, errno: %d\n", errno);
        cleanup();
        return false;
    }

    // // Bind to local address if specified
    // if (LocalSock && rdma_bind_addr(cm_id, (struct sockaddr*)LocalSock)) {
    //     fprintf(stderr, "Failed to bind to local address, errno: %d\n", errno);
    //     cleanup();
    //     return false;
    // }

    // 3.1 RDMA address & route resolution 
    int ret = rdma_resolve_addr(cm_id, NULL, 
                            (struct sockaddr*)BackEndSock, 
                            RESOLVE_TIMEOUT_MS);
    if (ret) {
        printf("Failed to resolve addr: %s\n", strerror(errno));
        cleanup();
        return DDS_ERROR_CODE_FAILED_CONNECTION;
    }

    // 3.2 Wait for addr resolution event 
    struct rdma_cm_event *event;
    ret = rdma_get_cm_event(ec, &event);
    if (ret || event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        printf("Failed to resolve addr (event): %s\n", strerror(errno));
        cleanup();
        return DDS_ERROR_CODE_FAILED_CONNECTION;
    }
    rdma_ack_cm_event(event);

    // 3.3 Reslve route 
    ret = rdma_resolve_route(cm_id, RESOLVE_TIMEOUT_MS);
    if (ret) {
        printf("Failed to resolve route: %s\n", strerror(errno));
        cleanup();
        return DDS_ERROR_CODE_FAILED_CONNECTION;
    }

    // 3.4 Wait for route resolution event 
    ret = rdma_get_cm_event(ec, &event);
    if (ret || event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
        printf("Failed to resolve route (event): %s\n", strerror(errno));
        cleanup();
        return DDS_ERROR_CODE_FAILED_CONNECTION;        
    }    
    rdma_ack_cm_event(event);

    // Create verbs objects
    pd = ibv_alloc_pd(cm_id->verbs);
    if (!pd) {
        fprintf(stderr, "Failed to allocate PD, errno: %d\n", errno);
        cleanup();
        return false;
    }

    // Create completion queue
    cq = ibv_create_cq(cm_id->verbs, QueueDepth, nullptr, nullptr, 0);
    if (!cq) {
        fprintf(stderr, "Failed to create CQ, errno: %d\n", errno);
        cleanup();
        return false;
    }

    // Create QP
    struct ibv_qp_init_attr qp_attr = {};
    qp_attr.send_cq = cq;
    qp_attr.recv_cq = cq;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.cap.max_send_wr = QueueDepth;
    qp_attr.cap.max_recv_wr = QueueDepth;
    qp_attr.cap.max_send_sge = MaxSge;
    qp_attr.cap.max_recv_sge = MaxSge;
    qp_attr.cap.max_inline_data = InlineThreshold;

    if (rdma_create_qp(cm_id, pd, &qp_attr)) {
        fprintf(stderr, "Failed to create QP, errno: %d\n", errno);
        cleanup();
        return false;
    }
    qp = cm_id->qp;


	//
	// Create and register a memory buffer and an additional buffer for messages
	//
	//
	// BufferAddress = reinterpret_cast<char*>(HeapAlloc(GetProcessHeap(), 0, Capacity));
	// if (BufferAddress == nullptr) {
	// 	printf("DMABuffer: failed to allocate a buffer of %llu bytes\n", Capacity);
	// 	return false;
	// }
	BufferAddress = static_cast<char*>(aligned_alloc(4096, Capacity));
    if (!BufferAddress) {
        fprintf(stderr, "Failed to allocate buffer of %zu bytes\n", Capacity);
        cleanup();
        return false;
	}
	memset(BufferAddress, 0, Capacity);
	// jason: reset request/response ring pointers before advertising the buffer
	// to the backend so it observes the phased alignment on first meta read.
	{
		DDS_FrontEnd::RequestRingBufferProgressive *reqRing =
			DDS_FrontEnd::AllocateRequestBufferProgressive(BufferAddress);
		DDS_FrontEnd::ResponseRingBufferProgressive *respRing =
			DDS_FrontEnd::AllocateResponseBufferProgressive(reqRing->Buffer + DDS_REQUEST_RING_BYTES);
		DDS_FrontEnd::ResetRequestRingBufferProgressive(reqRing);
		DDS_FrontEnd::ResetResponseRingBufferProgressive(respRing);
	}
	


	// RDMC_CreateMR(Adapter, AdapterFileHandle, &MemRegion);
	// unsigned long flags = ND_MR_FLAG_ALLOW_LOCAL_WRITE | ND_MR_FLAG_ALLOW_REMOTE_READ | ND_MR_FLAG_ALLOW_REMOTE_WRITE;
	// RDMC_RegisterDataBuffer(MemRegion, BufferAddress, (DWORD)Capacity, flags, &Ov);

	// RDMC_CreateMR(Adapter, AdapterFileHandle, &MsgMemRegion);
	// RDMC_RegisterDataBuffer(MsgMemRegion, MsgBuf, BUFF_MSG_SIZE, flags, &Ov);

	 // Register memory regions
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    
    mr = ibv_reg_mr(pd, BufferAddress, Capacity, access);
    if (!mr) {
        fprintf(stderr, "Failed to register main MR, errno: %d\n", errno);
        cleanup();
        return false;
    }

    msg_mr = ibv_reg_mr(pd, MsgBuf, BUFF_MSG_SIZE, access);
    if (!msg_mr) {
        fprintf(stderr, "Failed to register message MR, errno: %d\n", errno);
        cleanup();
        return false;
    }


	//
	// Connect to the back end
	//
	//
	// uint8_t privData = BUFF_CONN_PRIV_DATA;
	// RDMC_Connect(Connector, QPair, &Ov, *LocalSock, *BackEndSock, 0, (DWORD)QueueDepth, &privData, sizeof(privData));
	// RDMC_CompleteConnect(Connector, &Ov);
	






	//
	// Create memory window
	// Send buffer address and token to the back end
	// Wait for response
	//
	//
	// RDMC_CreateMW(Adapter, &MemWindow);
	// RDMC_Bind(QPair, MemRegion, MemWindow, BufferAddress, (DWORD)Capacity, ND_OP_FLAG_ALLOW_READ | ND_OP_FLAG_ALLOW_WRITE, CompQ, &Ov);

	// MsgSgl = new ND2_SGE[1];
	// MsgSgl[0].Buffer = MsgBuf;
	// MsgSgl[0].BufferLength = BUFF_MSG_SIZE;
	// MsgSgl[0].MemoryRegionToken = MsgMemRegion->GetLocalToken();

	// ((MsgHeader*)MsgBuf)->MsgId = BUFF_MSG_F2B_REQUEST_ID;
	// BuffMsgF2BRequestId* msg = (BuffMsgF2BRequestId*)(MsgBuf + sizeof(MsgHeader));
	// msg->ClientId = ClientId;
	// msg->BufferAddress = (uint64_t)BufferAddress;
	// msg->Capacity = (uint32_t)Capacity;
	

	// Set up SGE for messages
    MsgSgl = new ibv_sge();
    MsgSgl->addr = (uint64_t)MsgBuf;
    MsgSgl->length = BUFF_MSG_SIZE;
    MsgSgl->lkey = msg_mr->lkey;

	// Connect to server
    struct rdma_conn_param conn_param = {};
    uint8_t privData = BUFF_CONN_PRIV_DATA;
    conn_param.private_data = &privData;
    conn_param.private_data_len = sizeof(privData);
    conn_param.responder_resources = 1;
    conn_param.initiator_depth = 1;
    conn_param.flow_control = 1;
    conn_param.retry_count = 7;
    conn_param.rnr_retry_count = 7;

    if (rdma_connect(cm_id, &conn_param)) {
        fprintf(stderr, "Failed to connect, errno: %d\n", errno);
        cleanup();
        return false;
    }
    // 5. Wait for Connection Event
    ret = rdma_get_cm_event(ec, &event);
    if (ret || event->event != RDMA_CM_EVENT_ESTABLISHED) {
        printf("Failed to establish connection: %s\n", strerror(errno));
        cleanup();
        return DDS_ERROR_CODE_FAILED_CONNECTION;
    }
    rdma_ack_cm_event(event);

#ifdef BACKEND_BRIDGE_VERBOSE
    printf("Connection established successfully\n");
#endif     


    
	//
	// NOTE: translating the token from host encoding to network encoding is necessary for the Linux back end
	//
	//
	// msg->AccessToken = htonl(MemWindow->GetRemoteToken());
	// MsgSgl->BufferLength = sizeof(MsgHeader) + sizeof(BuffMsgF2BRequestId);

	// RDMC_Send(QPair, MsgSgl, 1, 0, MSG_CTXT);
	// RDMC_WaitForCompletionAndCheckContext(CompQ, &Ov, MSG_CTXT, false);

	// MsgSgl->BufferLength = BUFF_MSG_SIZE;
	// RDMC_PostReceive(QPair, MsgSgl, 1, MSG_CTXT);
	// RDMC_WaitForCompletionAndCheckContext(CompQ, &Ov, MSG_CTXT, false);

	// Send buffer registration message
    struct ibv_send_wr wr = {}, *bad_wr = nullptr;
    wr.wr_id = 0;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = MsgSgl;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    // Prepare registration message
    ((MsgHeader*)MsgBuf)->MsgId = BUFF_MSG_F2B_REQUEST_ID;
    BuffMsgF2BRequestId* msg = (BuffMsgF2BRequestId*)(MsgBuf + sizeof(MsgHeader));
    msg->ClientId = ClientId;
    msg->BufferAddress = (uint64_t)BufferAddress;
    msg->Capacity = (uint32_t)Capacity;
    msg->AccessToken = mr->rkey;  // Use MR rkey as access token
    // Tag this buffer registration with its logical IO channel so the backend
    // can set the correct ChannelIndex on the BuffConnConfig.
    msg->ChannelIndex = ChannelIndex;
    MsgSgl->length = sizeof(MsgHeader) + sizeof(BuffMsgF2BRequestId);


    // Post send
    if (ibv_post_send(qp, &wr, &bad_wr)) {
        fprintf(stderr, "Failed to post send, errno: %d\n", errno);
        cleanup();
        return false;
    }

    // Wait for completion
    struct ibv_wc wc;
    while (ibv_poll_cq(cq, 1, &wc) == 0);
    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "Send failed with status: %d\n", wc.status);
        cleanup();
        return false;
    }

    // Post receive for response
    struct ibv_recv_wr recv_wr = {}, *bad_recv_wr = nullptr;
    recv_wr.wr_id = 1;
    recv_wr.sg_list = MsgSgl;
    recv_wr.num_sge = 1;
    MsgSgl->length = BUFF_MSG_SIZE;

    if (ibv_post_recv(qp, &recv_wr, &bad_recv_wr)) {
        fprintf(stderr, "Failed to post receive, errno: %d\n", errno);
        cleanup();
        return false;
    }

    // Wait for response
    while (ibv_poll_cq(cq, 1, &wc) == 0);
    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "Receive failed with status: %d\n", wc.status);
        cleanup();
        return false;
    }



	// Process response
	if (((MsgHeader*)MsgBuf)->MsgId == BUFF_MSG_B2F_RESPOND_ID) {
		BufferId = ((BuffMsgB2FRespondId*)(MsgBuf + sizeof(MsgHeader)))->BufferId;
		printf("DMABuffer: connected to the back end with assigned buffer id (%d)\n", BufferId);
	}
	else {
		printf("DMABuffer: wrong message from the back end\n");
		cleanup();
		return false;
	}

/* #if DDS_NOTIFICATION_METHOD == DDS_NOTIFICATION_METHOD_INTERRUPT
		//
		// Post receives to allow backend to write responses
		//
		//
		// for (int i = 0; i != DDS_MAX_COMPLETION_BUFFERING; i++) {
		// 	RDMC_PostReceive(QPair, MsgSgl, 1, MSG_CTXT);
		// }
    for (int i = 0; i < DDS_MAX_COMPLETION_BUFFERING; i++) {
        if (ibv_post_recv(qp, &recv_wr, &bad_recv_wr)) {
            fprintf(stderr, "Failed to post receive %d, errno: %d\n", i, errno);
            cleanup();
            return false;
        }
    }
#endif */
	//
	// jason: always post receives for write-with-immediate CQE notifications.
	//
    for (int i = 0; i < DDS_MAX_COMPLETION_BUFFERING; i++) {
        if (ibv_post_recv(qp, &recv_wr, &bad_recv_wr)) {
            fprintf(stderr, "Failed to post receive %d, errno: %d\n", i, errno);
            cleanup();
            return false;
        }
    }

	//
	// This buffer is RDMA-accessible from DPU now
	//
	//
	return true;
}

//
// Wait for a completion event
// Not thread-safe
//
//
// Updated: return the number of CQEs consumed for response notifications.
int
DMABuffer::WaitForACompletion(
	bool Blocking,
	uint32_t *LastImmediateData,
	int *ImmediateCompletions
) {
	// RDMC_WaitForCompletionAndCheckContext(CompQ, &Ov, MSG_CTXT, Blocking);
	// RDMC_PostReceive(QPair, MsgSgl, 1, MSG_CTXT);
	/* struct ibv_wc wc;
    int num_completions;

    do {
        num_completions = ibv_poll_cq(cq, 1, &wc);
    } while (Blocking && num_completions == 0);

    if (num_completions > 0 && wc.status == IBV_WC_SUCCESS) {
        // Post a new receive for future messages
        struct ibv_recv_wr recv_wr = {}, *bad_recv_wr = nullptr;
        recv_wr.wr_id = 1;
        recv_wr.sg_list = MsgSgl;
        recv_wr.num_sge = 1;
        MsgSgl->length = BUFF_MSG_SIZE;

        ibv_post_recv(qp, &recv_wr, &bad_recv_wr);
    } */

    int completions = 0;
    uint32_t lastImmediateData = 0;
    int immediateCompletions = 0;
    bool needBlock = Blocking;
    struct ibv_wc wc;

    while (true) {
        int num_completions = ibv_poll_cq(cq, 1, &wc);
        if (num_completions < 0) {
            fprintf(stderr, "WaitForACompletion: ibv_poll_cq failed\n");
            break;
        }
        if (num_completions == 0) {
            if (needBlock) {
                continue;
            }
            break;
        }

        // Consume the completion and replenish the receive queue if needed.
        completions += 1;
        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "WaitForACompletion: CQE status %d\n", wc.status);
        }

        if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
            lastImmediateData = ntohl(wc.imm_data);
            immediateCompletions += 1;
        }

        if (wc.opcode == IBV_WC_RECV || wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
            struct ibv_recv_wr recv_wr = {}, *bad_recv_wr = nullptr;
            recv_wr.wr_id = 1;
            recv_wr.sg_list = MsgSgl;
            recv_wr.num_sge = 1;
            MsgSgl->length = BUFF_MSG_SIZE;

            ibv_post_recv(qp, &recv_wr, &bad_recv_wr);
        }

        // After one CQE, stop blocking and drain what's immediately available.
        needBlock = false;
    }

    if (LastImmediateData) {
        *LastImmediateData = lastImmediateData;
    }
    if (ImmediateCompletions) {
        *ImmediateCompletions = immediateCompletions;
    }

    return completions;
}


//
// Release the allocated buffer;
// Not thread-safe
//
//
void
DMABuffer::Release() {
	//
	// Send the exit message to the back end
	//
	//
	// if (BufferId >= 0) {
	// 	((MsgHeader*)MsgBuf)->MsgId = BUFF_MSG_F2B_RELEASE;
	// 	BuffMsgF2BRelease* msg = (BuffMsgF2BRelease*)(MsgBuf + sizeof(MsgHeader));
	// 	msg->ClientId = ClientId;
	// 	msg->BufferId = BufferId;
	// 	MsgSgl->BufferLength = sizeof(MsgHeader) + sizeof(BuffMsgF2BRelease);
	// 	RDMC_Send(QPair, MsgSgl, 1, 0, MSG_CTXT);
	// 	RDMC_WaitForCompletionAndCheckContext(CompQ, &Ov, MSG_CTXT, false);
	// 	printf("BackEndBridge: released the back end buffer\n");
	// }
	if (BufferId >= 0) {
        // Send release message
        ((MsgHeader*)MsgBuf)->MsgId = BUFF_MSG_F2B_RELEASE;
        BuffMsgF2BRelease* msg = (BuffMsgF2BRelease*)(MsgBuf + sizeof(MsgHeader));
        msg->ClientId = ClientId;
        msg->BufferId = BufferId;
        MsgSgl->length = sizeof(MsgHeader) + sizeof(BuffMsgF2BRelease);

        struct ibv_send_wr wr = {}, *bad_wr = nullptr;
        wr.wr_id = 2;
        wr.opcode = IBV_WR_SEND;
        wr.sg_list = MsgSgl;
        wr.num_sge = 1;
        wr.send_flags = IBV_SEND_SIGNALED;

        // Post send
        if (ibv_post_send(qp, &wr, &bad_wr) == 0) {
            // Wait for send completion
            struct ibv_wc wc;
            while (ibv_poll_cq(cq, 1, &wc) == 0);
            
            if (wc.status == IBV_WC_SUCCESS) {
                printf("DMABuffer: successfully sent release message to backend\n");
            } else {
                fprintf(stderr, "DMABuffer: failed to send release message, status: %d\n", wc.status);
            }
        } else {
            fprintf(stderr, "DMABuffer: failed to post release message send\n");
        }

        // Disconnect RDMA connection
        if (cm_id) {
            rdma_disconnect(cm_id);
        }
    }


	// clean up
	cleanup();

	//
	// Disconnect and release all resources
	//
	//
	// if (Connector) {
	// 	Connector->Disconnect(&Ov);

	// 	//
	// 	// TODO: the line below crashes when using with sockets
	// 	// 
	// 	// 
	// 	// Connector->Release();
	// }

	// if (MemRegion) {
	// 	MemRegion->Deregister(&Ov);
	// 	MemRegion->Release();
	// }

	// if (MemWindow) {
	// 	MemWindow->Release();
	// }

	// if (CompQ) {
	// 	CompQ->Release();
	// }

	// if (QPair) {
	// 	QPair->Release();
	// }
	
	// if (AdapterFileHandle) {
	// 	CloseHandle(AdapterFileHandle);
	// }

	// if (Adapter) {
	// 	Adapter->Release();
	// }

	// if (Ov.hEvent) {
	// 	CloseHandle(Ov.hEvent);
	// }

	// //
	// // Release memory buffer
	// //
	// //
	// if (MsgSgl) {
	// 	delete[] MsgSgl;
	// }

	// if (BufferAddress) {
	// 	HeapFree(GetProcessHeap(), 0, BufferAddress);
	// }
}

// Cleanup helper function
void DMABuffer::cleanup() {
    if (qp && cm_id) {
        rdma_destroy_qp(cm_id);
    }
    
    if (msg_mr) {
        ibv_dereg_mr(msg_mr);
    }
    
    if (mr) {
        ibv_dereg_mr(mr);
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
    
    if (MsgSgl) {
        delete MsgSgl;
    }
    
    if (BufferAddress) {
        free(BufferAddress);
    }

    // Reset all pointers
    ec = nullptr;
    cm_id = nullptr;
    pd = nullptr;
    cq = nullptr;
    qp = nullptr;
    mr = nullptr;
    msg_mr = nullptr;
    MsgSgl = nullptr;
    BufferAddress = nullptr;
}
