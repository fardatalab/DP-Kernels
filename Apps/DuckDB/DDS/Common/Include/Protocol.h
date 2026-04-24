#pragma once

#define DDS_ERROR_CODE_SUCCESS 0
#define DDS_ERROR_CODE_TOO_MANY_DIRS 1
#define DDS_ERROR_CODE_TOO_MANY_FILES 2
#define DDS_ERROR_CODE_DIR_EXISTS 3
#define DDS_ERROR_CODE_DIR_NOT_FOUND 4
#define DDS_ERROR_CODE_DIR_NOT_EMPTY 5
#define DDS_ERROR_CODE_FILE_EXISTS 6
#define DDS_ERROR_CODE_FILE_NOT_FOUND 7
#define DDS_ERROR_CODE_READ_OVERFLOW 8
#define DDS_ERROR_CODE_WRITE_OVERFLOW 9
#define DDS_ERROR_CODE_BUFFER_OVERFLOW 10
#define DDS_ERROR_CODE_NOT_IMPLEMENTED 11
#define DDS_ERROR_CODE_REQUIRE_POLLING 12
#define DDS_ERROR_CODE_TOO_MANY_POLLS 13
#define DDS_ERROR_CODE_INVALID_POLL_DELETION 14
#define DDS_ERROR_CODE_IO_PENDING 15
#define DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE 16
#define DDS_ERROR_CODE_INVALID_FILE_POSITION 17
#define DDS_ERROR_CODE_UNEXPECTED_MSG 18
#define DDS_ERROR_CODE_FAILED_CONNECTION 19
#define DDS_ERROR_CODE_FAILED_BUFFER_ALLOCATION 20
#define DDS_ERROR_CODE_OOM 21
#define DDS_ERROR_CODE_TOO_MANY_REQUESTS 22
#define DDS_ERROR_CODE_REQUEST_RING_FAILURE 23
#define DDS_ERROR_CODE_NO_COMPLETION 24
#define DDS_ERROR_CODE_IO_FAILURE 25 // the opposite of DDS_ERROR_CODE_SUCCESS, if somehow SPDK IO fails
#define DDS_ERROR_CODE_SEGMENT_RETRIEVAL_FAILURE 26
#define DDS_ERROR_CODE_RESERVED_SEGMENT_ERROR 27
#define DDS_ERROR_CODE_OUT_OF_MEMORY 28
// jason: invalid parameter passed by caller (e.g., read2 stage sizes/lengths).
#define DDS_ERROR_CODE_INVALID_PARAM 29

// DDS offload-stage timing instrumentation toggle.
// Keep disabled by default so existing signatures/data layouts stay unchanged.
// #ifndef DDS_OFFLOAD_STAGE_TIMING_ENABLED
// #define DDS_OFFLOAD_STAGE_TIMING_ENABLED 0
// #endif

// enable timing instrumentation for offload stages
#define DDS_OFFLOAD_STAGE_TIMING_ENABLED 0

#define DDS_CACHE_LINE_SIZE 64
#define DDS_CACHE_LINE_SIZE_BY_INT 16
#define DDS_CACHE_LINE_SIZE_BY_LONGLONG 8
#define DDS_CACHE_LINE_SIZE_BY_POINTER 8

#define DDS_MAX_FILE_PATH 64
#define DDS_MAX_DEVICE_NAME_LEN 32
#define DDS_MAX_DIRS 5
#define DDS_MAX_FILES 10
#define DDS_MAX_FILES_PER_DIR 1000
#define DDS_MAX_POLLS 5
#define DDS_DIR_INVALID 0xFFFFUL
#define DDS_DIR_ROOT 0
#define DDS_FILE_INVALID 0xFFFFUL
#define DDS_PAGE_SIZE 4096
#define DDS_POLL_DEFAULT 0
#define DDS_POLL_MAX_LATENCY_MICROSECONDS 100
#define DDS_REQUEST_RING_SIZE 20480
#define DDS_REQUEST_RING_SLOT_SIZE DDS_PAGE_SIZE
#define DDS_REQUEST_RING_BYTES 83886080
#define DDS_RESPONSE_RING_SIZE 30720
#define DDS_RESPONSE_RING_SLOT_SIZE DDS_PAGE_SIZE
#define DDS_RESPONSE_RING_BYTES 125829120
// jason: phase the request ring so (request start + sizeof(FileIOSizeT)) is 8-byte aligned.
#define DDS_REQUEST_RING_HEADER_PHASE 4
#define DDS_REQUEST_INVALID 0xFFFFUL

#define RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT 125829120
#define BACKEND_REQUEST_MAX_DMA_SIZE RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT
#define BACKEND_REQUEST_BUFFER_SIZE DDS_REQUEST_RING_BYTES
#define RING_BUFFER_RESPONSE_MINIMUM_TAIL_ADVANCEMENT 8192
#define RING_BUFFER_RESPONSE_MAXIMUM_TAIL_ADVANCEMENT 1048576
#define BACKEND_RESPONSE_MAX_DMA_SIZE RING_BUFFER_RESPONSE_MAXIMUM_TAIL_ADVANCEMENT
#define BACKEND_RESPONSE_BUFFER_SIZE DDS_RESPONSE_RING_BYTES
#define RING_BUFFER_RESPONSE_BATCH_ENABLED

#define RING_BUFFER_REQUEST_META_DATA_SIZE 128
#define RING_BUFFER_RESPONSE_META_DATA_SIZE 128

// ---------------------------------------------------------------------------
// Dual IO channel definitions.
// channel-0 (primary): pread, write, and all non-pread2 operations.
// channel-1 (secondary): pread2 exclusively.
// The mapping is deterministic and used by both frontend submission and backend
// dispatch paths.  DDS_NUM_IO_CHANNELS controls how many channel-scoped
// structures (rings, QP/CQ, contexts) are allocated.
//
// KEY INVARIANTS (Task 5.3):
//
//   1. Channel Mapping: DDS_OpToChannel() in DDSChannelDefs.h provides the
//      deterministic op-to-channel mapping.  The frontend selects the channel
//      at submission time (DDSFrontEnd.cpp: each DPU submission site picks
//      poll->Channels[ch]).  The backend does NOT re-dispatch; requests arrive
//      on the correct channel's ring because the frontend wrote them there.
//
//   2. Transport Model: Each IO channel is a separate RDMA connection
//      (rdma_connect/rdma_accept pair).  On the frontend, each DMABuffer owns
//      one connection.  On the backend, each connection is one BuffConnConfig
//      with one BuffConnChannelState instance.  The
//      BuffConnConfig.ChannelState.ChannelIndex field records the logical
//      channel identity (set from the BuffMsgF2BRequestId registration message).
//
//   3. WR-ID Partitioning: WR-IDs do NOT encode channel identity.  Each
//      BuffConn has its own QP/CQ, so completions from a CQ belong to that
//      BuffConn's channel implicitly.  WR-ID constants
//      (BUFF_READ_REQUEST_META_WR_ID, etc.) identify operation class only.
//
//   4. Slot/Context Lifecycle:
//      a. Backend: PendingDataPlaneRequests[reqId].IsPending guards against
//         context reuse while an IO is in-flight.  MarkRequestContextCompleted
//         clears IsPending after the response is no longer IO_PENDING.
//      b. Frontend: The deferred request head (PublishDeferredRequestHead) is
//         published only after CheckAndProcessIOCompletions confirms all
//         responses in a batch are complete.  This prevents the host from
//         resubmitting to ring space still occupied by in-flight requests.
//
//   5. Ring Isolation: Each channel has independent request/response ring
//      buffers.  Ring progress pointers (Head, TailA/B/C) are never shared
//      or compared across channels.
//
//   6. Capacity: Total outstanding = DDS_MAX_OUTSTANDING_IO_PER_CHANNEL *
//      DDS_NUM_IO_CHANNELS.  Startup validation in both frontend
//      (DDSFrontEnd.cpp Initialize) and backend (RunFileBackEnd) ensures
//      pool sizes meet this requirement.
// ---------------------------------------------------------------------------
#define DDS_NUM_IO_CHANNELS 2
#define DDS_IO_CHANNEL_PRIMARY 0 // pread / write / other
#define DDS_IO_CHANNEL_PREAD2 1  // pread2 only

// Original single-channel outstanding limit retained for reference:
// #define DDS_MAX_OUTSTANDING_IO 256
//
// Legacy constant kept for compatibility with existing call sites.
// In dual-channel mode, this value is the per-channel limit.
#define DDS_MAX_OUTSTANDING_IO 256
// Per-channel outstanding IO limit.
#define DDS_MAX_OUTSTANDING_IO_PER_CHANNEL DDS_MAX_OUTSTANDING_IO
// Total outstanding IO capacity across all channels.
#define DDS_MAX_OUTSTANDING_IO_TOTAL (DDS_MAX_OUTSTANDING_IO_PER_CHANNEL * DDS_NUM_IO_CHANNELS)
#define DDS_MAX_COMPLETION_BUFFERING 16
#define DDS_DPU_IO_SLOT_NUMBER_BASE DDS_MAX_OUTSTANDING_IO
// Original value retained for reference:
// #define DDS_DPU_IO_PARALLELISM 2
//
// jason: match DDS storage-engine worker parallelism with DPM hardware threads.
// This controls storage worker count via WORKER_THREAD_COUNT.
#define DDS_DPU_IO_PARALLELISM 4
#define DDS_CONTROL_PLANE_IO_SLOT_NUMBER 512
#define DDS_BACKEND_SECTOR_SIZE 512

#define DDS_NOTIFICATION_METHOD_INTERRUPT 0
#define DDS_NOTIFICATION_METHOD_TIMER 1
#define DDS_NOTIFICATION_METHOD DDS_NOTIFICATION_METHOD_INTERRUPT

// Original fixed size retained for reference:
// #define OFFLOAD_RESPONSE_RING_BYTES 251658240
//
// jason: keep legacy ring size when offload timing is disabled. When enabled, response records
// carry three extra uint64 timing fields, so use a size that remains divisible by both
// (sizeof(OffloadWorkResponse) + sizeof(FileIOSizeT)) and 4KB pages.
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
#define OFFLOAD_RESPONSE_RING_BYTES 251682816
#else
#define OFFLOAD_RESPONSE_RING_BYTES 251658240
#endif

#define CACHE_TABLE_OCC_GRANULARITY_ITEM 0
#define CACHE_TABLE_OCC_GRANULARITY_BUCKET 1
#define CACHE_TABLE_OCC_GRANULARITY CACHE_TABLE_OCC_GRANULARITY_BUCKET

// DDS storage engine thread-core layout (DPU-side):
// app thread + four worker threads + DMA agent thread are pinned to distinct cores.
#define CORE_ALLOCATION_STORAGE_ENGINE_APP_CORE 0
#define CORE_ALLOCATION_STORAGE_ENGINE_WORKER0_CORE 1
#define CORE_ALLOCATION_STORAGE_ENGINE_WORKER1_CORE 2
// Additional workers (index >= 2) are mapped by GetStorageWorkerCoreId():
// core = CORE_ALLOCATION_STORAGE_ENGINE_WORKER1_CORE + (workerIndex - 1)
// so worker2->core3 and worker3->core4 for DDS_DPU_IO_PARALLELISM=4.
// Original 2-worker baseline value retained for reference:
// #define CORE_ALLOCATION_STORAGE_ENGINE_AGENT_CORE 3
#define CORE_ALLOCATION_STORAGE_ENGINE_AGENT_CORE 5

#define OFFLOAD_ENGINE_ZERO_COPY_NONE 0
#define OFFLOAD_ENGINE_ZERO_COPY_DYNAMIC 1
#define OFFLOAD_ENGINE_ZERO_COPY_STATIC 2
#define OFFLOAD_ENGINE_ZERO_COPY OFFLOAD_ENGINE_ZERO_COPY_NONE
#define OFFLOAD_ENGINE_RESPONSE_INCLUDES_REQUEST
#define OFFLOAD_ENGINE_BATCH_SIZE 1
#undef OFFLOAD_ENGINE_ALLOW_NONALIGNED_IO

#define OPT_FILE_SERVICE_ZERO_COPY
#define OPT_FILE_SERVICE_BATCHING

// NOTE: Default DPU file creation disabled to avoid early control-plane requests
// before the file service worker threads are initialized.
// #define CREATE_DEFAULT_DPU_FILE
#ifdef CREATE_DEFAULT_DPU_FILE
#define DEFAULT_DPU_FILE_ID 0
#define DEFAULT_DPU_FILE_SIZE 1073741824ULL
#define DEFAULT_DPU_FILE_CREATION_CTRL_CONN 0
#endif

#define PRELOAD_CACHE_TABLE_ITEMS
#ifdef PRELOAD_CACHE_TABLE_ITEMS
#define CACHE_TABLE_FILE_PATH "/home/ubuntu/FasterCacheTable"
#endif

//
// Check a few parameters at the compile time
//
//
#define AssertStaticProtocol(e, num)                                                                                   \
    enum                                                                                                               \
    {                                                                                                                  \
        AssertStaticProtocol__##num = 1 / (e)                                                                          \
    }

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
#else
#pragma warning(push)
#pragma warning(disable : 4804)
#endif

AssertStaticProtocol(RING_BUFFER_REQUEST_META_DATA_SIZE == DDS_CACHE_LINE_SIZE * 2, 0);
AssertStaticProtocol(RING_BUFFER_RESPONSE_META_DATA_SIZE == DDS_CACHE_LINE_SIZE * 2, 1);
AssertStaticProtocol(DDS_REQUEST_RING_BYTES == DDS_REQUEST_RING_SIZE * DDS_REQUEST_RING_SLOT_SIZE, 2);
AssertStaticProtocol(DDS_REQUEST_RING_BYTES % DDS_CACHE_LINE_SIZE == 0, 3);
AssertStaticProtocol(DDS_RESPONSE_RING_BYTES == DDS_RESPONSE_RING_SIZE * DDS_RESPONSE_RING_SLOT_SIZE, 4);
AssertStaticProtocol(DDS_RESPONSE_RING_BYTES % DDS_CACHE_LINE_SIZE == 0, 5);

#if DDS_NOTIFICATION_METHOD == DDS_NOTIFICATION_METHOD_INTERRUPT
#ifndef RING_BUFFER_RESPONSE_BATCH_ENABLED
#error "Responses must be batched on the DPU"
#endif
#endif

#ifdef __GNUC__
#pragma GCC diagnostic pop
#else
#pragma warning(pop)
#endif

#define DDS_BACKEND_ADDR "10.10.2.201"
#define DDS_BACKEND_PORT 478
#define LOCALSOCK "192.168.100.2"
