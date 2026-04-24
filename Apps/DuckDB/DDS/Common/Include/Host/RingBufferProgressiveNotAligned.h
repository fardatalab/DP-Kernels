#pragma once

#include <atomic>

#include "DDSFrontEndInterface.h"

namespace DDS_FrontEnd {

template <class C>
using Atomic = std::atomic<C>;

//
// A ring buffer for message exchanges between front and back ends;
// This object should be allocated from the DMA area;
// Members are not cache line aligned
//
//
struct RequestRingBufferProgressiveNotAligned{
    Atomic<int> Progress;
    Atomic<int> Tail;
    int Head;
    char Buffer[DDS_REQUEST_RING_BYTES];
};

//
// Allocate a request buffer object
//
//
RequestRingBufferProgressiveNotAligned*
AllocateRequestBufferProgressiveNotAligned(
    BufferT BufferAddress
);

//
// Deallocate a request buffer object
//
//
void
DeallocateRequestBufferProgressiveNotAligned(
    RequestRingBufferProgressiveNotAligned* RingBuffer
);

//
// Insert a request into the request buffer
//
//
bool
InsertToRequestBufferProgressiveNotAligned(
    RequestRingBufferProgressiveNotAligned* RingBuffer,
    const BufferT CopyFrom,
    FileIOSizeT RequestSize
);

//
// Fetch requests from the request buffer
//
//
bool
FetchFromRequestBufferProgressiveNotAligned(
    RequestRingBufferProgressiveNotAligned* RingBuffer,
    BufferT CopyTo,
    FileIOSizeT* RequestSize
);

//
// Parse a request from copied data
// Note: RequestSize is greater than the actual request size and is cache line aligned
//
//
void
ParseNextRequestProgressiveNotAligned(
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
CheckForCompletionProgressiveNotAligned(
    RequestRingBufferProgressiveNotAligned* RingBuffer
);

}