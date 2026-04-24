#pragma once

#include <atomic>
 
#define RING_SIZE           16777216
#define FORWARD_DEGREE      1048576
#define CACHE_LINE          64
#define INT_ALIGNED         (CACHE_LINE/sizeof(int))
 
template <class C>
using Atomic = std::atomic<C>;
typedef char*        BufferT;
typedef unsigned int MessageSizeT;
typedef unsigned int RingSizeT;
 
//
// A ring buffer for message exchanges between a single producer and multiple consumers;
// All members are cache line aligned to avoid false sharing between threads
//
//
struct FarRingSPMC {
       Atomic<int> Progress[INT_ALIGNED];
       Atomic<int> Head[INT_ALIGNED];
       int Tail[INT_ALIGNED];
       char Buffer[RING_SIZE];
};

//
// Allocate a ring buffer object
//
//
FarRingSPMC*
AllocateSPMC(
       BufferT Buffer
);

//
// Deallocate a ring buffer object
//
//
void
DeallocateSPMC(
       FarRingSPMC* Ring
);

//
// Insert a message into the buffer
//
//
bool
InsertSPMC(
       FarRingSPMC* Ring,
       const BufferT CopyFrom,
       MessageSizeT MessageSize
);
 
//
// Fetch messages from the message buffer
//
//
bool
FetchSPMC(
       FarRingSPMC* Ring,
       BufferT CopyTo,
       MessageSizeT* MessageSize
);

//
// Wait for completion
//
//
bool
CheckForCompletionSPMC(
    FarRingSPMC* Ring
);