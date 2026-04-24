#include <stdio.h>
#include <string.h>

#include "RingBufferPolling.h"

//
// Initialize a buffer
//
//
void InitializeRequestRingBufferBackEnd(struct RequestRingBufferBackEnd *RingBuffer, uint64_t RemoteAddr,
                                        uint32_t AccessToken, uint32_t Capacity)
{
    memset(RingBuffer, 0, sizeof(struct RequestRingBufferBackEnd));
    RingBuffer->RemoteAddr = RemoteAddr;
    RingBuffer->AccessToken = AccessToken;
    RingBuffer->Capacity = Capacity;
    // jason: initialize to the request header phase so headers stay naturally aligned.
    RingBuffer->Head = DDS_REQUEST_RING_HEADER_PHASE;

    //
    // Align the buffer by cache line size
    //
    //
    uint64_t ringBufferAddress = (uint64_t)RingBuffer->RemoteAddr;
    while (ringBufferAddress % DDS_CACHE_LINE_SIZE != 0)
    {
        ringBufferAddress++;
    }

    //
    // Read the tail and the progress in the same request to minimize latency/overhead
    //
    //
    RingBuffer->ReadMetaAddr = ringBufferAddress;
    RingBuffer->ReadMetaSize = DDS_CACHE_LINE_SIZE * 2;

    //
    // Only write the head int
    //
    //
    RingBuffer->WriteMetaAddr = ringBufferAddress + RingBuffer->ReadMetaSize;
    RingBuffer->WriteMetaSize = sizeof(int);

    //
    // The data is after the meta data
    //
    //
    RingBuffer->DataBaseAddr = ringBufferAddress + DDS_CACHE_LINE_SIZE * 3;

    //
    // Check if the buffer is large enough
    // The error can be better handled by notifying the host
    //
    //
    if (RingBuffer->DataBaseAddr + DDS_REQUEST_RING_BYTES > RemoteAddr + Capacity)
    {
        fprintf(stderr, "%s [error]: Not enough space for request ring\n", __func__);
    }
}

//
// Initialize a buffer that is not cache aligned
//
//
void InitializeRequestRingBufferNotAlignedBackEnd(struct RequestRingBufferBackEnd *RingBuffer, uint64_t RemoteAddr,
                                                  uint32_t AccessToken, uint32_t Capacity)
{
    memset(RingBuffer, 0, sizeof(struct RequestRingBufferBackEnd));
    RingBuffer->RemoteAddr = RemoteAddr;
    RingBuffer->AccessToken = AccessToken;
    RingBuffer->Capacity = Capacity;
    // jason: initialize to the request header phase so headers stay naturally aligned.
    RingBuffer->Head = DDS_REQUEST_RING_HEADER_PHASE;

    uint64_t ringBufferAddress = (uint64_t)RingBuffer->RemoteAddr;

    //
    // Read the tail and the progress in the same request to minimize latency/overhead
    //
    //
    RingBuffer->ReadMetaAddr = ringBufferAddress;
    RingBuffer->ReadMetaSize = sizeof(int) * 2;

    //
    // Only write the head int
    //
    //
    RingBuffer->WriteMetaAddr = ringBufferAddress + RingBuffer->ReadMetaSize;
    RingBuffer->WriteMetaSize = sizeof(int);

    //
    // The data is after the meta data
    //
    //
    RingBuffer->DataBaseAddr = ringBufferAddress + sizeof(int) * 3;

    //
    // Check if the buffer is large enough
    // The error can be better handled by notifying the host
    //
    //
    if (RingBuffer->DataBaseAddr + DDS_REQUEST_RING_BYTES > RemoteAddr + Capacity)
    {
        fprintf(stderr, "%s [error]: Not enough space for request ring\n", __func__);
    }
}

//
// Initialize a buffer for FaRM-style ring buffer
//
//
void InitializeRequestRingBufferFaRMStyleBackEnd(struct RequestRingBufferBackEnd *RingBuffer, uint64_t RemoteAddr,
                                                 uint32_t AccessToken, uint32_t Capacity)
{
    memset(RingBuffer, 0, sizeof(struct RequestRingBufferBackEnd));
    RingBuffer->RemoteAddr = RemoteAddr;
    RingBuffer->AccessToken = AccessToken;
    RingBuffer->Capacity = Capacity;
    // jason: initialize to the request header phase so headers stay naturally aligned.
    RingBuffer->Head = DDS_REQUEST_RING_HEADER_PHASE;

    uint64_t ringBufferAddress = (uint64_t)RingBuffer->RemoteAddr;

    //
    // The data is after the meta data
    //
    //
    RingBuffer->DataBaseAddr = ringBufferAddress + sizeof(int) * 2;

    //
    // Meta data is the op size at the beginning of each request
    //
    //
    RingBuffer->ReadMetaAddr = RingBuffer->DataBaseAddr;
    RingBuffer->ReadMetaSize = sizeof(int);

    //
    // Only write the head int
    //
    //
    RingBuffer->WriteMetaAddr = ringBufferAddress + sizeof(int);
    RingBuffer->WriteMetaSize = sizeof(int);

    //
    // Check if the buffer is large enough
    // The error can be better handled by notifying the host
    //
    //
    if (RingBuffer->DataBaseAddr + DDS_REQUEST_RING_BYTES > RemoteAddr + Capacity)
    {
        fprintf(stderr, "%s [error]: Not enough space for request ring\n", __func__);
    }
}

//
// Initialize a buffer for lock-based ring buffer
//
//
void InitializeRequestRingBufferLockBasedBackEnd(struct RequestRingBufferBackEnd *RingBuffer, uint64_t RemoteAddr,
                                                 uint32_t AccessToken, uint32_t Capacity)
{
    memset(RingBuffer, 0, sizeof(struct RequestRingBufferBackEnd));
    RingBuffer->RemoteAddr = RemoteAddr;
    RingBuffer->AccessToken = AccessToken;
    RingBuffer->Capacity = Capacity;
    // jason: initialize to the request header phase so headers stay naturally aligned.
    RingBuffer->Head = DDS_REQUEST_RING_HEADER_PHASE;

    uint64_t ringBufferAddress = (uint64_t)RingBuffer->RemoteAddr;

    //
    // Read the tail
    //
    //
    RingBuffer->ReadMetaAddr = ringBufferAddress;
    RingBuffer->ReadMetaSize = sizeof(int);

    //
    // Only write the head int
    //
    //
    RingBuffer->WriteMetaAddr = ringBufferAddress + RingBuffer->ReadMetaSize;
    RingBuffer->WriteMetaSize = sizeof(int);

    //
    // The data is after the meta data
    //
    //
    RingBuffer->DataBaseAddr = ringBufferAddress + sizeof(int) * 2 + sizeof(void *);

    //
    // Check if the buffer is large enough
    // The error can be better handled by notifying the host
    //
    //
    if (RingBuffer->DataBaseAddr + DDS_REQUEST_RING_BYTES > RemoteAddr + Capacity)
    {
        fprintf(stderr, "%s [error]: Not enough space for request ring\n", __func__);
    }
}

//
// Initialize a buffer
//
//
void InitializeResponseRingBufferBackEnd(struct ResponseRingBufferBackEnd *RingBuffer, uint64_t RemoteAddr,
                                         uint32_t AccessToken, uint32_t Capacity)
{
    memset(RingBuffer, 0, sizeof(struct ResponseRingBufferBackEnd));
    RingBuffer->RemoteAddr = RemoteAddr;
    RingBuffer->AccessToken = AccessToken;
    RingBuffer->Capacity = Capacity;
    RingBuffer->TailC = 0;
    RingBuffer->TailB = 0;
    RingBuffer->TailA = 0;

    // Align the buffer by cache line size
    //
    //
    uint64_t ringBufferAddress = (uint64_t)RingBuffer->RemoteAddr;
    while (ringBufferAddress % DDS_CACHE_LINE_SIZE != 0)
    {
        ringBufferAddress++;
    }

    //
    // Read the progress and the head in the same request to minimize latency/overhead
    //
    //
    RingBuffer->ReadMetaAddr = ringBufferAddress;
    RingBuffer->ReadMetaSize = DDS_CACHE_LINE_SIZE * 2;

    //
    // Only write the tail int
    //
    //
    RingBuffer->WriteMetaAddr = ringBufferAddress + RingBuffer->ReadMetaSize;
    RingBuffer->WriteMetaSize = sizeof(int);

    //
    // The data is after the meta data
    //
    //
    RingBuffer->DataBaseAddr = ringBufferAddress + DDS_CACHE_LINE_SIZE * 3;

    //
    // Check if the buffer is large enough
    // The error can be better handled by notifying the host
    //
    //
    if (RingBuffer->DataBaseAddr + DDS_RESPONSE_RING_BYTES > RemoteAddr + Capacity)
    {
        fprintf(stderr, "%s [error]: Not enough space for request ring\n", __func__);
    }
}

//
// Initialize a buffer for both request and response rings
//
//
void InitializeRingBufferBackEnd(struct RequestRingBufferBackEnd *RequestRingBuffer,
                                 struct ResponseRingBufferBackEnd *ResponseRingBuffer, uint64_t RemoteAddr,
                                 uint32_t AccessToken, uint32_t Capacity)
{
    InitializeRequestRingBufferBackEnd(RequestRingBuffer, RemoteAddr, AccessToken, Capacity);

    InitializeResponseRingBufferBackEnd(
        ResponseRingBuffer, RequestRingBuffer->DataBaseAddr + DDS_REQUEST_RING_BYTES, AccessToken,
        Capacity - (RequestRingBuffer->DataBaseAddr - RemoteAddr) - DDS_REQUEST_RING_BYTES);
}
