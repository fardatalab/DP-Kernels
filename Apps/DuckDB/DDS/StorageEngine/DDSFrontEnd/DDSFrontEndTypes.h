#pragma once

#include <atomic>
#include <cstring>

#include "DDSFrontEndConfig.h"
#include "DDSTypes.h"

#if BACKEND_TYPE == BACKEND_TYPE_DPU
#include "DMABuffer.h"
#include "RingBufferProgressive.h"
#endif

namespace DDS_FrontEnd {

template <typename T>
using Atomic = std::atomic<T>;

//
// File read/write operation
//
//
/* Original FileIOT preserved for reference.
typedef struct FileIOT {
    bool IsRead = false;
#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
    Atomic<bool> IsComplete = true;
    Atomic<bool> IsReadyForPoll = false;
#elif BACKEND_TYPE == BACKEND_TYPE_DPU
    RequestIdT RequestId = 0;
    Atomic<bool> IsAvailable {true};
#endif
    ContextT FileReference = (ContextT)nullptr;
    FileIdT FileId = (FileIdT)DDS_FILE_INVALID;
    FileSizeT Offset = (FileSizeT)0;
    BufferT AppBuffer = (BufferT)nullptr;
    BufferT* AppBufferArray = (BufferT*)nullptr;
    FileIOSizeT BytesDesired = (FileIOSizeT)0;
    // jason: Alignment metadata for backend-aligned reads.
    FileSizeT AlignedOffset = (FileSizeT)0;
    FileIOSizeT AlignedBytes = (FileIOSizeT)0;
    FileIOSizeT CopyStart = (FileIOSizeT)0;
    ReadWriteCallback AppCallback = (ReadWriteCallback)nullptr;
    ContextT Context = (ContextT)nullptr;
} FileIOT;
*/

// jason: Extended FileIOT tracks read2 output/logical sizes and callback variants.
typedef struct FileIOT {
    bool IsRead = false;
    bool IsRead2 = false;
#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
    Atomic<bool> IsComplete = true;
    Atomic<bool> IsReadyForPoll = false;
#elif BACKEND_TYPE == BACKEND_TYPE_DPU
    RequestIdT RequestId = 0;
    Atomic<bool> IsAvailable {true};
#endif
    ContextT FileReference = (ContextT)nullptr;
    FileIdT FileId = (FileIdT)DDS_FILE_INVALID;
    FileSizeT Offset = (FileSizeT)0;
    BufferT AppBuffer = (BufferT)nullptr;
    BufferT* AppBufferArray = (BufferT*)nullptr;
    FileIOSizeT BytesDesired = (FileIOSizeT)0;
    // OutputBytes == bytes to place into the app buffer (read2).
    FileIOSizeT OutputBytes = (FileIOSizeT)0;
    // StageCount == number of read2 post-processing stages (decrypt/decompress).
    uint16_t StageCount = 0;
    // StageSizes == per-stage output sizes (stage1 decrypt, stage2 decompress).
    FileIOSizeT StageSizes[2] = {(FileIOSizeT)0, (FileIOSizeT)0};
    // LogicalBytesServiced == logical bytes read (EOF-clamped).
    FileIOSizeT LogicalBytesServiced = (FileIOSizeT)0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    // Offload stage timing copied from response header for read2.
    uint64_t OffloadReadTimeNs = 0;
    uint64_t OffloadStage1TimeNs = 0;
    uint64_t OffloadStage2TimeNs = 0;
#endif
    // jason: Alignment metadata for backend-aligned reads.
    FileSizeT AlignedOffset = (FileSizeT)0;
    FileIOSizeT AlignedBytes = (FileIOSizeT)0;
    FileIOSizeT CopyStart = (FileIOSizeT)0;
    // Local-memory bookkeeping fields used by Read/Write paths.
    bool IsSegmented = false;
    FileIOSizeT BytesServiced = (FileIOSizeT)0;
    ReadWriteCallback AppCallback = (ReadWriteCallback)nullptr;
    ReadWriteCallback2 AppCallback2 = (ReadWriteCallback2)nullptr;
    ContextT Context = (ContextT)nullptr;
} FileIOT;

//
// Poll structure
#include "DDSChannelDefs.h"

//
// Per-channel state within a poll structure (DPU backend only).
// Each channel owns its own DMA buffer connection, request/response rings,
// outstanding-request context array, and CQE notification counter.
// The channel index follows DDS_IO_CHANNEL_PRIMARY / DDS_IO_CHANNEL_PREAD2.
//
// Task 5.3 Invariants:
//   - A request is always submitted to exactly one channel, chosen by
//     DDS_OpToChannel(opCode) at the submission site in DDSFrontEnd.cpp.
//   - The OutstandingRequests[] array is per-channel (not global), indexed
//     by the slot id returned from the channel's request ring. Completions
//     are routed back to the correct channel via DDSBackEndBridge::GetResponse
//     setting *ChannelOut.
//   - EnqueueCount, CompletionCount, CopyBackCount are per-channel atomics
//     for telemetry. CopyBackCount is separate from CompletionCount because
//     not all completions involve payload copy-back (e.g., writes).
//
#if BACKEND_TYPE == BACKEND_TYPE_DPU
    /**
     * Cached response-batch state owned by one poll/channel consumer.
     *
     * This state used to live on DDSBackEndBridge, which made ownership
     * ambiguous once multiple polls/channels were introduced. It is now owned
     * by PollChannelT because the cache is tied to one response ring.
     */
    typedef struct PollChannelBatchCacheT
    {
        SplittableBufferT BatchRef;
        FileIOSizeT ProcessedBytes;
        BufferT NextResponse;

        PollChannelBatchCacheT()
            : ProcessedBytes(0), NextResponse(nullptr)
        {
            memset(&BatchRef, 0, sizeof(BatchRef));
        }
    } PollChannelBatchCacheT;

    typedef struct PollChannelT
    {
        DMABuffer *MsgBuffer;
        struct RequestRingBufferProgressive *RequestRing;
        struct ResponseRingBufferProgressive *ResponseRing;
        // Outstanding request contexts for this channel (indexed by RequestId).
        FileIOT *OutstandingRequests[DDS_MAX_OUTSTANDING_IO_PER_CHANNEL];
        Atomic<size_t> NextRequestSlot;
        // CQE notifications available for response consumption on this channel.
        Atomic<size_t> PendingNotifications;
        // Last response-ring tail value published by a WRITE_WITH_IMM CQE on
        // this channel. The consumer parses only up to this boundary.
        Atomic<int> PublishedTail;
        // Channel index for debug/log purposes.
        int ChannelIndex;

        // Task 2.4: Per-channel debug counters for visibility into enqueue/completion/backlog.
        Atomic<uint64_t> EnqueueCount;    // Total requests submitted to this channel.
        Atomic<uint64_t> CompletionCount; // Total completions consumed from this channel.
        Atomic<uint64_t> CopyBackCount;   // Total copy-back operations (payload copied to app buffer).
#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
        // Cached response-batch parser state for this channel only.
        PollChannelBatchCacheT BatchCache;
#endif

        PollChannelT()
            : MsgBuffer(nullptr), RequestRing(nullptr), ResponseRing(nullptr), NextRequestSlot(0),
              PendingNotifications(0), PublishedTail(0), ChannelIndex(-1), EnqueueCount(0), CompletionCount(0),
              CopyBackCount(0)
        {
            for (int i = 0; i < DDS_MAX_OUTSTANDING_IO_PER_CHANNEL; i++)
            {
                OutstandingRequests[i] = nullptr;
            }
        }
    } PollChannelT;
#endif

    //
    // Poll structure — now wraps per-channel state arrays.
    //
    // Original single-channel PollT preserved for reference:
    // typedef struct PollT {
    //     FileIOT* OutstandingRequests[DDS_MAX_OUTSTANDING_IO];
    //     Atomic<size_t> NextRequestSlot;
    //     DMABuffer* MsgBuffer;
    //     struct RequestRingBufferProgressive* RequestRing;
    //     struct ResponseRingBufferProgressive* ResponseRing;
    //     Atomic<size_t> PendingNotifications;
    // } PollT;
    //
    typedef struct PollT
    {
        // Legacy flat outstanding-request array kept for backward compatibility
        // with control-path lookups.  Hot-path submission/completion use
        // Channels[ch].OutstandingRequests instead.
        FileIOT *OutstandingRequests[DDS_MAX_OUTSTANDING_IO];
        // Legacy slot counter — used only by non-channel-aware callers.
        Atomic<size_t> NextRequestSlot;
#if BACKEND_TYPE == BACKEND_TYPE_DPU
        // Per-channel state (rings, DMA buffers, contexts, CQE counters).
        PollChannelT Channels[DDS_NUM_IO_CHANNELS];
        // Round-robin consumer cursor for multi-channel completion polling.
        int LastPolledChannel = 0;
        // Legacy aliases preserved for incremental migration.
        // Original single-channel fields:
        // DMABuffer* MsgBuffer;
        // struct RequestRingBufferProgressive* RequestRing;
        // struct ResponseRingBufferProgressive* ResponseRing;
        // Atomic<size_t> PendingNotifications;
#endif

    PollT();

#if BACKEND_TYPE == BACKEND_TYPE_DPU
    ErrorCodeT SetUpDMABuffer(void* BackEndDPU);
    void DestroyDMABuffer();
    void InitializeRings();
#endif
    } PollT;
}
