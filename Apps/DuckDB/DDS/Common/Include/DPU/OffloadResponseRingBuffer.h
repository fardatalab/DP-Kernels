#pragma once

#include "BackEndTypes.h"
#include "Protocol.h" 

//
// A ring buffer for storing read responses, owned by a network thread that performs offloading
// Tail[0] - TailA
// Tail[1] - TailB
// Tail[2] - TailC
//
//
typedef struct {
    int Tail[DDS_CACHE_LINE_SIZE_BY_INT];
    char Buffer[OFFLOAD_RESPONSE_RING_BYTES];
} OffloadResponseRingBuffer;

//
// Allocate a response buffer object
//
//
OffloadResponseRingBuffer*
AllocateOffloadResponseBuffer(
    BufferT BufferAddress
);

//
// Deallocate a response queue buffer object
//
//
void
DellocateOffloadResponseBuffer(
    OffloadResponseRingBuffer* RingBuffer
);