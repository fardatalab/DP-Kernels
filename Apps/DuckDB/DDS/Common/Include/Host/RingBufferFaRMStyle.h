#pragma once

#include <atomic>
#include <iostream>

#include "DDSFrontEndInterface.h"

#define RING_BUFFER_REQUEST_HEADER_SIZE sizeof(FileIOSizeT)

namespace DDS_FrontEnd {

template <class C>
using Atomic = std::atomic<C>;

//
// A FaRM-like ring buffer
// No cache line alignment
//
//
struct RequestRingBufferFaRMStyle{
    Atomic<int> Tail;
    int Head;
    char Buffer[DDS_REQUEST_RING_BYTES];
};

//
// Allocate a request buffer object
//
//
RequestRingBufferFaRMStyle*
AllocateRequestBufferFaRMStyle(
    BufferT BufferAddress
);

//
// Deallocate a request buffer object
//
//
void
DeallocateRequestBufferFaRMStyle(
    RequestRingBufferFaRMStyle* RingBuffer
);

//
// Insert a request into the request buffer
//
//
bool
InsertToRequestBufferFaRMStyle(
    RequestRingBufferFaRMStyle* RingBuffer,
    const BufferT CopyFrom,
    FileIOSizeT RequestSize
);

//
// Fetch requests from the request buffer
//
//
bool
FetchFromRequestBufferFaRMStyle(
    RequestRingBufferFaRMStyle* RingBuffer,
    BufferT CopyTo,
    FileIOSizeT* RequestSize
);

//
// Parse a request from copied data
// Note: RequestSize is greater than the actual request size and is request header aligned
//
//
void
ParseNextRequestFaRMStyle(
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
CheckForCompletionFaRMStyle(
    RequestRingBufferFaRMStyle* RingBuffer
);

}