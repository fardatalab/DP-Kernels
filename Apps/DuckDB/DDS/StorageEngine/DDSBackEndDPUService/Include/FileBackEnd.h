#pragma once

#include <infiniband/ib.h>
#include <inttypes.h>
#include <rdma/rdma_cma.h>
#include <stdatomic.h>
#include <stdlib.h>

#include "BackEndTypes.h"
#include "FileService.h"
#include "ControlPlaneHandler.h"
#include "DataPlaneHandlers.h"
#include "DDSTypes.h"
#include "DPUBackEnd.h"
#include "DPUBackEndDir.h"
#include "DPUBackEndFile.h"
#include "DPUBackEndStorage.h"
#include "bdev.h"
#include "Zmalloc.h"

#include "MsgTypes.h"
#include "Protocol.h"
#include "RingBufferPolling.h"

#define LISTEN_BACKLOG 64
#define RESOLVE_TIMEOUT_MS 2000

#define CTRL_COMPQ_DEPTH 16
#define CTRL_SENDQ_DEPTH 16
#define CTRL_RECVQ_DEPTH 16
#define CTRL_SEND_WR_ID 0
#define CTRL_RECV_WR_ID 1

#define BUFF_COMPQ_DEPTH 16
#define BUFF_SENDQ_DEPTH 16
#define BUFF_RECVQ_DEPTH 16
#define BUFF_SEND_WR_ID 0
#define BUFF_RECV_WR_ID 1
#define BUFF_READ_REQUEST_META_WR_ID 2
#define BUFF_WRITE_REQUEST_META_WR_ID 3
#define BUFF_READ_REQUEST_DATA_WR_ID 4
#define BUFF_READ_REQUEST_DATA_SPLIT_WR_ID 5
#define BUFF_WRITE_REQUEST_DATA_WR_ID 6
#define BUFF_WRITE_REQUEST_DATA_SPLIT_WR_ID 7
#define BUFF_READ_RESPONSE_META_WR_ID 8
#define BUFF_WRITE_RESPONSE_META_WR_ID 9
#define BUFF_READ_RESPONSE_DATA_WR_ID 10
#define BUFF_READ_RESPONSE_DATA_SPLIT_WR_ID 11
#define BUFF_WRITE_RESPONSE_DATA_WR_ID 12
#define BUFF_WRITE_RESPONSE_DATA_SPLIT_WR_ID 13

#define BUFF_READ_DATA_SPLIT_STATE_NOT_SPLIT 1
#define BUFF_READ_DATA_SPLIT_STATE_SPLIT 0

#define CONN_STATE_AVAILABLE 0
#define CONN_STATE_OCCUPIED 1
#define CONN_STATE_CONNECTED 2
#define CONN_STATE_DISCONNECTING 3

#define DATA_PLANE_WEIGHT 10

// Task 5.1: Periodic telemetry dump interval — number of dataPlaneCounter resets
// between channel-scoped telemetry prints. This is instrumentation-only and is
// disabled by default to avoid steady-state logging on the DMA agent loop.
//
// Original default retained for reference:
// #define DDS_CHANNEL_TELEMETRY_INTERVAL 100000
//
// Enable explicitly for debugging (for example, set to 100000).
#define DDS_CHANNEL_TELEMETRY_INTERVAL 0

#define DDS_STORAGE_FILE_BACKEND_VERBOSE 1

//
// The global config for (R)DMA
//
//
typedef struct {
    struct rdma_event_channel *CmChannel;
    struct rdma_cm_id *CmId;
} DMAConfig;

#ifdef CREATE_DEFAULT_DPU_FILE
//
// File creation state
//
//
enum FileCreationState {
    FILE_NULL = 0,
    FILE_CREATION_SUBMITTED,
    FILE_CREATED,
    FILE_CHANGED
};
#endif

//
// The configuration for a control connection
//
//
typedef struct {
    uint32_t CtrlId;
    uint8_t State;

    //
    // Setup for a DMA channel
    //
    //
    struct rdma_cm_id *RemoteCmId;
    struct ibv_comp_channel *Channel;
    struct ibv_cq *CompQ;
    struct ibv_pd *PDomain;
    struct ibv_qp *QPair;

    //
    // Setup for control messages
    //
    //
    struct ibv_recv_wr RecvWr;
    struct ibv_sge RecvSgl;
    struct ibv_mr *RecvMr;
    char RecvBuff[CTRL_MSG_SIZE];

    struct ibv_send_wr SendWr;
    struct ibv_sge SendSgl;
    struct ibv_mr *SendMr;
    char SendBuff[CTRL_MSG_SIZE];

    //
    // Pending control-plane request
    //
    //
    ControlPlaneRequestContext PendingControlPlaneRequest;

#ifdef CREATE_DEFAULT_DPU_FILE
    //
    // Signals, reqs, and responses for creating a default file on DPU with specified size
    //
    //
    enum FileCreationState DefaultDpuFileCreationState;
    CtrlMsgF2BReqCreateFile DefaultCreateFileRequest;
    CtrlMsgB2FAckCreateFile DefaultCreateFileResponse;
    CtrlMsgF2BReqChangeFileSize DefaultChangeFileRequest;
    CtrlMsgB2FAckChangeFileSize DefaultChangeFileResponse;
#endif
} CtrlConnConfig;

#include "DDSChannelDefs.h"

// ---------------------------------------------------------------------------
// Per-channel posted-response transfer tracking.
// One descriptor represents one contiguous response-ring byte range that has
// been posted to the RNIC for RDMA write to the host, but whose local source
// bytes are not yet reclaimable until all response-data WR CQEs arrive.
//
// Design assumption: each logical channel uses one RC QP with `sq_sig_all = 1`,
// so send-queue CQEs are observed in posting order for that channel. The FIFO
// below therefore tracks response data WR completions by channel-local post
// order, not by unique WR IDs.
// ---------------------------------------------------------------------------
typedef struct
{
    uint8_t Valid;
    uint8_t PendingDataWrs;
    int Start;
    int End;
    FileIOSizeT Bytes;
    uint64_t Sequence;
} ResponseTransferDescriptor;

// ---------------------------------------------------------------------------
// Per-channel backend transport state.
// Each IO channel owns its own QP/CQ, request/response DMA WRs, ring buffers,
// deferred-head state, and outstanding request context array.
// ---------------------------------------------------------------------------
typedef struct {
    int ChannelIndex; // DDS_IO_CHANNEL_PRIMARY or DDS_IO_CHANNEL_PREAD2

    //
    // Per-channel QP/CQ resources
    //
    struct ibv_comp_channel *Channel;
    struct ibv_cq *CompQ;
    struct ibv_qp *QPair;

    //
    // Setup for data exchange for requests (per-channel)
    //
    struct ibv_send_wr RequestDMAReadDataWr;
    struct ibv_sge RequestDMAReadDataSgl;
    struct ibv_mr *RequestDMAReadDataMr;
    char* RequestDMAReadDataBuff;
    struct ibv_send_wr RequestDMAReadDataSplitWr;
    struct ibv_sge RequestDMAReadDataSplitSgl;
    RingSizeT RequestDMAReadDataSize;
    RingSizeT RequestDMAReadDataSplitState;
    struct ibv_send_wr RequestDMAReadMetaWr;
    struct ibv_sge RequestDMAReadMetaSgl;
    struct ibv_mr *RequestDMAReadMetaMr;
    char RequestDMAReadMetaBuff[RING_BUFFER_REQUEST_META_DATA_SIZE];
    struct ibv_send_wr RequestDMAWriteMetaWr;
    struct ibv_sge RequestDMAWriteMetaSgl;
    struct ibv_mr *RequestDMAWriteMetaMr;
    char* RequestDMAWriteMetaBuff;

    //
    // Setup for data exchange for responses (per-channel)
    //
    struct ibv_send_wr ResponseDMAWriteDataWr;
    struct ibv_sge ResponseDMAWriteDataSgl;
    struct ibv_mr *ResponseDMAWriteDataMr;
    char* ResponseDMAWriteDataBuff;
    struct ibv_send_wr ResponseDMAWriteDataSplitWr;
    struct ibv_sge ResponseDMAWriteDataSplitSgl;
    RingSizeT ResponseDMAWriteDataSize;
    struct ibv_send_wr ResponseDMAReadMetaWr;
    struct ibv_sge ResponseDMAReadMetaSgl;
    struct ibv_mr *ResponseDMAReadMetaMr;
    char ResponseDMAReadMetaBuff[RING_BUFFER_RESPONSE_META_DATA_SIZE];
    struct ibv_send_wr ResponseDMAWriteMetaWr;
    struct ibv_sge ResponseDMAWriteMetaSgl;
    struct ibv_mr *ResponseDMAWriteMetaMr;
    char* ResponseDMAWriteMetaBuff;

    //
    // Ring buffers (per-channel)
    //
    struct RequestRingBufferBackEnd RequestRing;
    struct ResponseRingBufferBackEnd ResponseRing;
    // Local response-source reclaim boundary. Unlike ResponseRing.TailC, this
    // advances only after response data WR CQEs confirm the RNIC has finished
    // reading the local source bytes.
    int ResponseReclaimTail;
    ResponseTransferDescriptor ResponseTransfers[DDS_MAX_OUTSTANDING_IO_PER_CHANNEL];
    uint16_t ResponseTransferHead;
    uint16_t ResponseTransferTail;
    uint16_t ResponseTransferCount;
    uint64_t ResponseTransferSequence;

    //
    // Deferred request head update — channel-local (safe variant: update after consumption).
    //
    int RequestPendingHead;
    int RequestPendingHeadValid;

    //
    // Pending data-plane requests (per-channel, indexed by RequestId).
    //
    DataPlaneRequestContext PendingDataPlaneRequests[DDS_MAX_OUTSTANDING_IO_PER_CHANNEL];

    //
    // Task 2.4: Per-channel debug counters for backend visibility.
    //
    uint64_t RequestsDispatched;    // Total requests consumed from this channel's ring.
    uint64_t CompletionsProcessed;  // Total IO completions on this channel.
    uint64_t DeferredHeadPublishes; // Times deferred head was published on this channel.
} BuffConnChannelState;

//
// The configuration for a buffer connection.
// Design invariant: one BuffConnConfig represents exactly one logical IO channel
// connection (primary or pread2), and owns exactly one channel state object.
// Shared connection identity and PD remain at the top level.
//
// Original single-channel BuffConnConfig preserved for reference:
// typedef struct {
//     uint32_t BuffId; uint32_t CtrlId; uint8_t State;
//     struct rdma_cm_id *RemoteCmId; struct ibv_comp_channel *Channel;
//     struct ibv_cq *CompQ; struct ibv_pd *PDomain; struct ibv_qp *QPair;
//     ... (request/response DMA WRs) ...
//     struct RequestRingBufferBackEnd RequestRing;
//     struct ResponseRingBufferBackEnd ResponseRing;
//     int RequestPendingHead; int RequestPendingHeadValid;
//     DataPlaneRequestContext PendingDataPlaneRequests[DDS_MAX_OUTSTANDING_IO];
//     RequestIdT NextRequestContext;
// } BuffConnConfig;
//
typedef struct
{
    uint32_t BuffId;
    uint32_t CtrlId;
    uint8_t State;

    //
    // Shared connection identity for this single channel connection.
    //
    struct rdma_cm_id *RemoteCmId;
    struct ibv_pd *PDomain;

    //
    // Setup for control messages for this single channel connection.
    //
    struct ibv_recv_wr RecvWr;
    struct ibv_sge RecvSgl;
    struct ibv_mr *RecvMr;
    char RecvBuff[CTRL_MSG_SIZE];

    struct ibv_send_wr SendWr;
    struct ibv_sge SendSgl;
    struct ibv_mr *SendMr;
    char SendBuff[CTRL_MSG_SIZE];

    //
    // Per-connection transport state (QP/CQ, rings, contexts, DMA WRs).
    // This state belongs to exactly one logical channel; ChannelIndex records
    // whether it is primary or pread2.
    //
    BuffConnChannelState ChannelState;
} BuffConnConfig;

//
// Back end configuration
//
//
typedef struct {
    uint32_t ServerIp;
    uint16_t ServerPort;
    uint32_t MaxClients;
    uint32_t MaxBuffs;
    DMAConfig DMAConf;
    CtrlConnConfig* CtrlConns;
    BuffConnConfig* BuffConns;

    FileService* FS;
} BackEndConfig;

//
// The entry point for the back end,
// but SPDK requires all the parameters be in just one struct, like the following
//
//
int RunFileBackEnd(
    const char* ServerIpStr,
    const int ServerPort,
    const uint32_t MaxClients,
    const uint32_t MaxBuffs,
    int Argc,
    char **Argv
);

//
// the main loop where we submit requests and check for completions, happens after all initializations
//
//
void RunAgentLoop(
    void *Ctx
);

//
// Stop the back end
//
//
int StopFileBackEnd();
