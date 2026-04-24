#pragma once

#include <stdint.h>
#include "DDSTypes.h"
#include "MsgTypes.h"

//
// Context for a pending data plane request from the host
//
//
/* Original data-plane request context preserved for reference.
typedef struct {
    int IsRead;
    BuffMsgF2BReqHeader* Request;
    BuffMsgB2FAckHeader* Response;
    SplittableBufferT DataBuffer;
} DataPlaneRequestContext;
*/

// jason: Extended context includes opcode, logical/output sizes, and read2 stage metadata.
typedef struct {
    int IsRead;
    // IsPending == 1 when this RequestId context has an in-flight backend request.
    // Unlike Response->Result probing, this is not affected by response-ring memory reuse/wrap.
    uint8_t IsPending;
    uint16_t OpCode;
    // ChannelIndex records the logical IO channel that owns this request context.
    // It is required for channel-aware SPDK slot mapping (globalSlot = ch * per_ch + reqId).
    int16_t ChannelIndex;
    BuffMsgF2BReqHeader* Request;
    BuffMsgB2FAckHeader* Response;
    SplittableBufferT DataBuffer;
    // LogicalBytes == logical bytes requested (EOF-clamped later).
    FileIOSizeT LogicalBytes;
    // OutputBytes == bytes to place into the app buffer.
    FileIOSizeT OutputBytes;
    // StageCount == number of post-read stages carried in the request payload.
    uint16_t StageCount;
    // StageSizes == per-stage output sizes (stage1 decrypt, stage2 decompress).
    FileIOSizeT StageSizes[2];
    // StageInputOffsets == per-stage input window offsets from the previous stage output.
    FileIOSizeT StageInputOffsets[2];
    // StageInputLengths == per-stage input window lengths from the previous stage output.
    FileIOSizeT StageInputLengths[2];
    // StageBuffers == ring-resident buffers for stage outputs (stage1, stage2).
    SplittableBufferT StageBuffers[2];
    // CopyStart == logical offset within the aligned stage0 read buffer.
    FileIOSizeT CopyStart;
    // jason: Response ring backing range for DPK stage task IO binding.
    char *ResponseRingBase;
    FileIOSizeT ResponseRingBytes;
    // BatchNextRequestId == next request id in a parsed backend batch chain.
    // DDS_REQUEST_INVALID marks end-of-chain.
    RequestIdT BatchNextRequestId;
} DataPlaneRequestContext;

//
// Context for a pending control plane request
//
//
typedef struct {
    RequestIdT RequestId;
    BufferT Request;
    BufferT Response;
    void* SPDKContext;  // thread specific SPDK ctx
} ControlPlaneRequestContext;

//
// Check a few parameters at the compile time
//
//
#define AssertStaticBackEndTypes(E, Num) \
    enum { AssertStaticBackEndTypes__##Num = 1/(E) }

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result" 
#else
#pragma warning(push)
#pragma warning (disable: 4804)
#endif
//
// Ring space is allocated at the |size of a response| + |header size|
// The alignment is enforced once a response is inserted into the ring
//
//
AssertStaticBackEndTypes(OFFLOAD_RESPONSE_RING_BYTES % (sizeof(OffloadWorkResponse) + sizeof(FileIOSizeT)) == 0, 0);
#ifdef __GNUC__
#pragma GCC diagnostic pop
#else
#pragma warning(pop)
#endif
