#pragma once

// #include "RDMC.h"
#include "MsgTypes.h"

/* libibverbs specific */
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <stdint.h>


/*
 * @brief DMABuffer class manages RDMA memory buffers and communication
 * 
 * This class handles:
 * - RDMA buffer allocation and registration
 * - Connection management with rdma_cm
 * - Queue pair operations
 * - Memory region management
 */
class DMABuffer {
private:
    size_t Capacity;        // size of rdma buffer
    int ClientId;           
    int BufferId;
    int ChannelIndex; // Logical IO channel (DDS_IO_CHANNEL_PRIMARY or DDS_IO_CHANNEL_PREAD2)

    // IND2Adapter* Adapter;
    // HANDLE AdapterFileHandle;
    // IND2Connector* Connector;
    // OVERLAPPED Ov;
    // IND2CompletionQueue* CompQ;
    // IND2QueuePair* QPair;
    // IND2MemoryRegion* MemRegion;
    // IND2MemoryWindow* MemWindow;
    
    // RDMA Connection management 
    struct rdma_event_channel *ec;
    struct rdma_cm_id *cm_id;

    // verbs resources 
    struct ibv_pd *pd;    
    struct ibv_cq *cq;     
    struct ibv_qp *qp;       
    struct ibv_mr *mr;      
    struct ibv_mr *msg_mr;    

    //
    // Variables for messages
    //
    //

    // ND2_SGE* MsgSgl;
    // IND2MemoryRegion* MsgMemRegion;
    struct ibv_sge *MsgSgl;             // Scatter-Gather elements
    char MsgBuf[BUFF_MSG_SIZE];
    void cleanup(); // new 

public:
    char* BufferAddress;

public:
    DMABuffer(const char *_BackEndAddr, const unsigned short _BackEndPort, const size_t _Capacity, const int _ClientId,
              const int _ChannelIndex = 0);

    //
    // Allocate the buffer with the specified capacity
    // and register it to the NIC;
    // Not thread-safe
    //
    //
    bool
    Allocate(
        struct sockaddr_in* LocalSock,
        struct sockaddr_in* BackEndSock,
        const size_t QueueDepth,
        const size_t MaxSge,
        const size_t InlineThreshold
    );

    //
    // Wait for a completion event
    // Not thread-safe
    //
    //
    // Updated: return the number of CQEs consumed for response notifications.
    int
    WaitForACompletion(
        bool Blocking,
        uint32_t* LastImmediateData = nullptr,
        int* ImmediateCompletions = nullptr
    );

    //
    // Release the allocated buffer;
    // Not thread-safe
    //
    //
    void
    Release();
};
