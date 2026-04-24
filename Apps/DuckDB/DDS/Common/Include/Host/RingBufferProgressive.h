#pragma once

#include <atomic>
#include <cstdint>

#include "DDSFrontEndInterface.h"
#include "MsgTypes.h"
namespace DDS_FrontEnd {

template <class C>
using Atomic = std::atomic<C>;

//
// **************************************************************
// Request buffer
// **************************************************************
//

//
// A ring buffer for exchanging requests from the host to the DPU;
// This object should be allocated from the DMA area;
// All members are cache line aligned to avoid false sharing between threads
//
//
struct RequestRingBufferProgressive{
    Atomic<int> Progress[DDS_CACHE_LINE_SIZE_BY_INT];
    Atomic<int> Tail[DDS_CACHE_LINE_SIZE_BY_INT];
    int Head[DDS_CACHE_LINE_SIZE_BY_INT];
    char Buffer[DDS_REQUEST_RING_BYTES];
};

//
// Allocate a request buffer object
//
//
RequestRingBufferProgressive*
AllocateRequestBufferProgressive(
    BufferT BufferAddress
);

//
// Deallocate a request buffer object
//
//
void
DeallocateRequestBufferProgressive(
    RequestRingBufferProgressive* RingBuffer
);

//
// Insert a request into the request buffer
//
//
bool
InsertToRequestBufferProgressive(
    RequestRingBufferProgressive* RingBuffer,
    const BufferT CopyFrom,
    FileIOSizeT RequestSize
);

//
// Insert a WriteFile request into the request buffer
//
//
bool
InsertWriteFileRequest(
    RequestRingBufferProgressive* RingBuffer,
    RequestIdT RequestId,
    FileIdT FileId,
    FileSizeT Offset,
    FileIOSizeT Bytes,
    BufferT SourceBuffer
);

//
// Insert a WriteFileGather request into the request buffer
//
//
bool
InsertWriteFileGatherRequest(
    RequestRingBufferProgressive* RingBuffer,
    RequestIdT RequestId,
    FileIdT FileId,
    FileSizeT Offset,
    FileIOSizeT Bytes,
    BufferT* SourceBufferArray
);

//
// Insert a ReadFile/ReadFileScatter/ReadFile2 request into the request buffer.
// - LogicalBytes is the logical read size.
// - OutputBytes is the final stage/output size.
// - StageSizes/StageInputOffsets/StageInputLengths/StageCount are optional and only used for read2.
// - IsRead2 selects the opcode (READ vs READ2).
//
/* Original signature preserved for reference.
bool
InsertReadRequest(
    RequestRingBufferProgressive* RingBuffer,
    RequestIdT RequestId,
    FileIdT FileId,
    FileSizeT Offset,
    FileIOSizeT LogicalBytes,
    FileIOSizeT OutputBytes,
    bool IsRead2
);
*/
bool
InsertReadRequest(
    RequestRingBufferProgressive* RingBuffer,
    RequestIdT RequestId,
    FileIdT FileId,
    FileSizeT Offset,
    FileIOSizeT LogicalBytes,
    FileIOSizeT OutputBytes,
    const FileIOSizeT* StageSizes,
    const FileIOSizeT* StageInputOffsets,
    const FileIOSizeT* StageInputLengths,
    uint16_t StageCount,
    bool IsRead2
);

/* InsertRead2Request deprecated; consolidated into InsertReadRequest.
//
// Insert a Read2 request into the request buffer (logical bytes + output bytes).
//
bool
InsertRead2Request(
    RequestRingBufferProgressive* RingBuffer,
    RequestIdT RequestId,
    FileIdT FileId,
    FileSizeT Offset,
    FileIOSizeT LogicalBytes,
    FileIOSizeT OutputBytes
);
*/

//
// Fetch requests from the request buffer
//
//
bool
FetchFromRequestBufferProgressive(
    RequestRingBufferProgressive* RingBuffer,
    BufferT CopyTo,
    FileIOSizeT* RequestSize
);

//
// Parse a request from copied data
// Note: RequestSize is greater than the actual request size
//
//
void
ParseNextRequestProgressive(
    BufferT CopyTo,
    FileIOSizeT TotalSize,
    BufferT* RequestPointer,
    FileIOSizeT* RequestSize,
    BufferT* StartOfNext,
    FileIOSizeT* RemainingSize
);

//
// Wait for completion
//
//
bool
CheckForRequestCompletionProgressive(
    RequestRingBufferProgressive* RingBuffer
);

//
// **************************************************************
// Response buffer
// **************************************************************
//

//
// A ring buffer for exchanging responses from the DPU to the host;
// This object should be allocated from the DMA area;
// All members are cache line aligned to avoid false sharing between threads
//
//
struct ResponseRingBufferProgressive{
    Atomic<int> Progress[DDS_CACHE_LINE_SIZE_BY_INT];
    Atomic<int> Head[DDS_CACHE_LINE_SIZE_BY_INT];
    int Tail[DDS_CACHE_LINE_SIZE_BY_INT];
    char Buffer[DDS_RESPONSE_RING_BYTES];
};

//
// Diagnostic snapshot for host-side request publications
//
typedef struct {
    uint64_t HostWriteCount;
    uint64_t HostWriteWrapCount;
    uint32_t LastReqId;
    uint32_t LastFileId;
    uint64_t LastOffset;
    uint32_t LastBytes;
    int LastTail;
    int LastHead;
    int LastProgress;
    uint32_t LastRequestBytes;
    uint32_t LastHeaderBytes;
    uint32_t LastWrapped;
} RingDiagSnapshot;

//
// Allocate a response buffer object
//
//
ResponseRingBufferProgressive*
AllocateResponseBufferProgressive(
    BufferT BufferAddress
);

//
// Reset request ring state for a (re)connect so header phase alignment is enforced.
//
void
ResetRequestRingBufferProgressive(
    RequestRingBufferProgressive* RingBuffer
);

//
// Reset response ring state for a (re)connect so producer/consumer pointers match backend expectations.
//
void
ResetResponseRingBufferProgressive(
    ResponseRingBufferProgressive* RingBuffer
);

//
// Deallocate a response buffer object
//
//
void
DeallocateResponseBufferProgressive(
    ResponseRingBufferProgressive* RingBuffer
);

//
// Configure response payload alignment for FetchResponse parsing.
//
//
void
SetResponsePayloadAlignment(
    FileIOSizeT Alignment
);

//
// Get response payload alignment for consumers.
//
//
FileIOSizeT
GetResponsePayloadAlignment();

//
// Fetch responses from the response buffer
//
//
bool
FetchFromResponseBufferProgressive(
    ResponseRingBufferProgressive* RingBuffer,
    BufferT CopyTo,
    FileIOSizeT* ResponseSize
);

//
// Fetch a response from the response buffer
// Updated: account for payload padding to honor backend buffer alignment.
//
//
bool
FetchResponse(
    ResponseRingBufferProgressive* RingBuffer,
    BuffMsgB2FAckHeader** Response,
    SplittableBufferT* DataBuffer,
    int PublishedTail
);

#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
//
// Fetch a batch of responses from the response buffer
//
//
bool
FetchResponseBatch(
    ResponseRingBufferProgressive* RingBuffer,
    SplittableBufferT* Responses,
    int PublishedTail
);
#endif

//
// Increment the progress
//
//
void
IncrementProgress(
    ResponseRingBufferProgressive* RingBuffer,
    FileIOSizeT ResponseSize
);

//
// Insert a response into the response buffer
//
//
bool
InsertToResponseBufferProgressive(
    ResponseRingBufferProgressive* RingBuffer,
    const BufferT* CopyFromList,
    FileIOSizeT* ResponseSizeList,
    int NumResponses,
    int* NumResponsesInserted
);

//
// Parse a response from copied data
// Note: ResponseSize is greater than the actual Response size
//
//
void
ParseNextResponseProgressive(
    BufferT CopyTo,
    FileIOSizeT TotalSize,
    BufferT* ResponsePointer,
    FileIOSizeT* ResponseSize,
    BufferT* StartOfNext,
    FileIOSizeT* RemainingSize
);

/**
 * @brief Fetch a snapshot of host-side request publication diagnostics.
 *
 * @param OutSnapshot Populated on success; zeroed when diagnostics are disabled.
 * @return True when diagnostics are available and OutSnapshot is filled.
 */
bool
GetHostWriteDiagSnapshot(
    RingDiagSnapshot* OutSnapshot
);

//
// Wait for completion
//
//
bool
CheckForResponseCompletionProgressive(
    ResponseRingBufferProgressive* RingBuffer
);

}
