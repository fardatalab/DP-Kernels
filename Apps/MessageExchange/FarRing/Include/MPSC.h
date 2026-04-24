#pragma once

#include <atomic>

#define RING_SIZE 16777216
#define FORWARD_DEGREE 1048576
#define CACHE_LINE 64
#define INT_ALIGNED (CACHE_LINE / sizeof(int))

template <class C>
using Atomic = std::atomic<C>;
typedef char *BufferT;
typedef unsigned int MessageSizeT;
typedef unsigned int RingSizeT;

//
// A ring buffer for message exchanges between multiple producers and a consumer;
// All members are cache line aligned to avoid false sharing between threads
//
//
struct FarRingMPSC
{
       Atomic<int> Progress[INT_ALIGNED];
       Atomic<int> Tail[INT_ALIGNED];
       int Head[INT_ALIGNED];
       char Buffer[RING_SIZE];
};

//
// Allocate a ring buffer object
//
//
FarRingMPSC *
AllocateMPSC(
    BufferT Buffer);

//
// Deallocate a ring buffer object
//
//
void DeallocateMPSC(
    FarRingMPSC *Ring);

//
// Insert a message into the buffer
//
//
bool InsertMPSC(
    FarRingMPSC *Ring,
    const BufferT CopyFrom,
    MessageSizeT MessageSize);

//
// Fetch messages from the message buffer
//
//
bool FetchMPSC(
    FarRingMPSC *Ring,
    BufferT CopyTo,
    MessageSizeT *MessageSize);

//
// Parse a message from copied data
// Note: MessageSize is greater than the actual message size and is cache line aligned
//
//
void ParseNextMessageMPSC(
    BufferT CopyTo,
    MessageSizeT TotalSize,
    BufferT *MessagePointer,
    MessageSizeT *MessageSize,
    BufferT *StartOfNext,
    MessageSizeT *RemainingSize);
