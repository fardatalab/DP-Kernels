#pragma once

#include <atomic>
#include <errno.h>
#include <fcntl.h> // for O_* constants
#include <stddef.h>
#include <string.h>
#include <sys/mman.h> // for shm_open
#include <sys/types.h>
#include <unistd.h>   // for ftruncate

#include "common.hpp"

// #include "DDSFrontEndInterface.h"
#define CACHE_LINE_SIZE 64
#define CACHE_LINE_SIZE_BY_INT (CACHE_LINE_SIZE / sizeof(int))
#define DDS_CACHE_LINE_SIZE_BY_LONGLONG 8
#define DDS_CACHE_LINE_SIZE_BY_POINTER 8

#define DDS_REQUEST_RING_BYTES 83886080                       // 83886080//163840
#define RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT 83886080 // 83886080 // 1048576

typedef char *BufferT;
typedef void *ContextT;
// typedef uint32_t FileIOSizeT;
typedef u_int64_t FileIOSizeT; // force 8 byte alignment
typedef uint32_t RingSizeT;

namespace DDS_FrontEnd
{

template <class C> using Atomic = std::atomic<C>;

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
template <size_t BufferSize = DDS_REQUEST_RING_BYTES,
          size_t MaxTailAdvancement = RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
struct RequestRingBufferProgressive
{
    Atomic<int> Progress[CACHE_LINE_SIZE_BY_INT];
    Atomic<int> Tail[CACHE_LINE_SIZE_BY_INT];
    int Head[CACHE_LINE_SIZE_BY_INT];
    char Buffer[BufferSize];
};

//
// Allocate a request buffer object
// Note: This is a local allocation, not shared memory
//
//
template <size_t BufferSize = DDS_REQUEST_RING_BYTES,
          size_t MaxTailAdvancement = RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *LocalAllocateRequestBufferProgressive();

//
// Corresponding to the local allocation above
// Deallocate the request buffer object. Note: This is a local deallocation, not shared memory
//
//
template <size_t BufferSize = DDS_REQUEST_RING_BYTES,
          size_t MaxTailAdvancement = RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
bool LocalDeallocateRequestBufferProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *ringBuffer);

//
// Allocate a request buffer object
//
//
template <size_t BufferSize = DDS_REQUEST_RING_BYTES,
          size_t MaxTailAdvancement = RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *AllocateRequestBufferProgressive(const char *shm_name);

//
// Setup the shared memory for the request buffer, and return the pointer to the buffer
// Note: must be called by the client interface after DPM sets it up
//
//
template <size_t BufferSize = DDS_REQUEST_RING_BYTES,
          size_t MaxTailAdvancement = RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *SetupRequestBufferProgressive(const char *shm_name);

//
// Deallocate a request buffer object
//
//
template <size_t BufferSize = DDS_REQUEST_RING_BYTES,
          size_t MaxTailAdvancement = RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
void DeallocateRequestBufferProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer);

//
// Insert a request into the request buffer
//
//
template <size_t BufferSize = DDS_REQUEST_RING_BYTES,
          size_t MaxTailAdvancement = RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
bool InsertToRequestBufferProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer,
                                      const BufferT CopyFrom, FileIOSizeT RequestSize);

template <size_t BufferSize = DDS_REQUEST_RING_BYTES,
          size_t MaxTailAdvancement = RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
bool InsertToRequestBufferProgressive(int thread_id,
                                      RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer,
                                      const BufferT CopyFrom, FileIOSizeT RequestSize);

template <size_t BufferSize = DDS_REQUEST_RING_BYTES,
          size_t MaxTailAdvancement = RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
bool InsertToRequestBufferProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer,
                                      const struct dpm_req_msg *CopyFrom);

//
// Fetch requests from the request buffer
//
//
template <size_t BufferSize = DDS_REQUEST_RING_BYTES,
          size_t MaxTailAdvancement = RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
bool FetchFromRequestBufferProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer,
                                       BufferT CopyTo, FileIOSizeT *RequestSize);

//
// Parse a request from copied data
// Note: RequestSize is greater than the actual request size
//
//
void ParseNextRequestProgressive(BufferT CopyTo, FileIOSizeT TotalSize, BufferT *RequestPointer,
                                 FileIOSizeT *RequestSize, BufferT *StartOfNext, FileIOSizeT *RemainingSize);

//
// Wait for completion
//
//
template <size_t BufferSize = DDS_REQUEST_RING_BYTES,
          size_t MaxTailAdvancement = RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
bool CheckForRequestCompletionProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer);

/*
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
struct ResponseRingBufferProgressive
{
    Atomic<int> Progress[CACHE_LINE_SIZE_BY_INT];
    Atomic<int> Head[CACHE_LINE_SIZE_BY_INT];
    int Tail[CACHE_LINE_SIZE_BY_INT];
    char Buffer[DDS_RESPONSE_RING_BYTES];
};

//
// Allocate a response buffer object
//
//
ResponseRingBufferProgressive *AllocateResponseBufferProgressive(BufferT BufferAddress);

//
// Deallocate a response buffer object
//
//
void DeallocateResponseBufferProgressive(ResponseRingBufferProgressive *RingBuffer);

//
// Fetch responses from the response buffer
//
//
bool FetchFromResponseBufferProgressive(ResponseRingBufferProgressive *RingBuffer, BufferT CopyTo,
                                        FileIOSizeT *ResponseSize);

//
// Fetch a response from the response buffer
//
//
bool FetchResponse(ResponseRingBufferProgressive *RingBuffer, BuffMsgB2FAckHeader **Response,
                   SplittableBufferT *DataBuffer);

#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
//
// Fetch a batch of responses from the response buffer
//
//
bool FetchResponseBatch(ResponseRingBufferProgressive *RingBuffer, SplittableBufferT *Responses);
#endif

//
// Increment the progress
//
//
void IncrementProgress(ResponseRingBufferProgressive *RingBuffer, FileIOSizeT ResponseSize);

//
// Insert a response into the response buffer
//
//
bool InsertToResponseBufferProgressive(ResponseRingBufferProgressive *RingBuffer, const BufferT *CopyFromList,
                                       FileIOSizeT *ResponseSizeList, int NumResponses, int *NumResponsesInserted);

//
// Parse a response from copied data
// Note: ResponseSize is greater than the actual Response size
//
//
void ParseNextResponseProgressive(BufferT CopyTo, FileIOSizeT TotalSize, BufferT *ResponsePointer,
                                  FileIOSizeT *ResponseSize, BufferT *StartOfNext, FileIOSizeT *RemainingSize);

//
// Wait for completion
//
//
bool CheckForResponseCompletionProgressive(ResponseRingBufferProgressive *RingBuffer);

 */
} // namespace DDS_FrontEnd

// this is used for the IPC message queue
using RequestRingMsgQ = struct DDS_FrontEnd::RequestRingBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>;
