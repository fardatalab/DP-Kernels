#pragma once

#include <mutex>

#include "DDSFrontEndInterface.h"

namespace DDS_FrontEnd {

using Mutex = std::mutex;

//
// A Lock-based ring buffer
// No cache line alignment
//
//
struct RequestRingBufferLockBased{
    int Tail;
    int Head;
    Mutex* RingLock;
    char Buffer[DDS_REQUEST_RING_BYTES];
};

//
// Allocate a request buffer object
//
//
RequestRingBufferLockBased*
AllocateRequestBufferLockBased(
    BufferT BufferAddress
);

//
// Deallocate a request buffer object
//
//
void
DeallocateRequestBufferLockBased(
    RequestRingBufferLockBased* RingBuffer
);

//
// Insert a request into the request buffer
//
//
bool
InsertToRequestBufferLockBased(
    RequestRingBufferLockBased* RingBuffer,
    const BufferT CopyFrom,
    FileIOSizeT RequestSize
);

//
// Fetch requests from the request buffer
//
//
bool
FetchFromRequestBufferLockBased(
    RequestRingBufferLockBased* RingBuffer,
    BufferT CopyTo,
    FileIOSizeT* RequestSize
);

//
// Parse a request from copied data
// Note: RequestSize is greater than the actual request size and is request header aligned
//
//
void
ParseNextRequestLockBased(
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
CheckForCompletionLockBased(
    RequestRingBufferLockBased* RingBuffer
);

}