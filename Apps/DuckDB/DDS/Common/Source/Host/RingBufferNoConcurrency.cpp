#pragma once

#include "RingBufferNoConcurrency.h"

namespace DDS_FrontEnd {

//
// Allocate a request buffer object
//
//
RequestRingBuffer*
AllocateRequestBuffer(
    BufferT BufferAddress
) {
    RequestRingBuffer* ringBuffer = (RequestRingBuffer*)BufferAddress;

    //
    // Align the buffer by cache line size
    //
    //
    size_t ringBufferAddress = (size_t)ringBuffer;
    while (ringBufferAddress % DDS_CACHE_LINE_SIZE != 0) {
        ringBufferAddress++;
    }
    ringBuffer = (RequestRingBuffer*)ringBufferAddress;

    memset(ringBuffer, 0, sizeof(RequestRingBuffer));

    return ringBuffer;
}

//
// Deallocate a request buffer object
//
//
void
DeallocateRequestBuffer(
    RequestRingBuffer* RingBuffer
) {
    memset(RingBuffer, 0, sizeof(RequestRingBuffer));
}

//
// Insert a request into the request buffer
//
//
bool
InsertToRequestBuffer(
    RequestRingBuffer* RingBuffer,
    const BufferT CopyFrom,
    FileIOSizeT RequestSize
) {
    //
    // Align the request to cache line size to avoid false sharing;
    // Append request size to the beginning of the request;
    // Append a flag byte to the end of the request
    //
    //
    FileIOSizeT requestBytes = sizeof(FileIOSizeT) + RequestSize + 1;
    while (requestBytes % DDS_CACHE_LINE_SIZE != 0) {
        requestBytes++;
    }

    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];
    RingSizeT space = 0;

    if (tail < head) {
        space = head - tail;
    }
    else {
        space = DDS_REQUEST_RING_BYTES - tail - head;
    }

    if (requestBytes > space) {
        return false;
    }

    while (RingBuffer->Tail[0].compare_exchange_weak(tail, (tail + requestBytes) % DDS_REQUEST_RING_BYTES) == false) {
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];

        if (tail <= head) {
            space = tail + DDS_REQUEST_RING_BYTES - head;
        }
        else {
            space = tail - head;
        }

        if (requestBytes > space) {
            return false;
        }
    }

    if (tail + requestBytes <= DDS_REQUEST_RING_BYTES) {
        char* requestAddress = &RingBuffer->Buffer[tail];

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT*)requestAddress) = requestBytes;

        //
        // Write the request
        //
        //
        memcpy(requestAddress + sizeof(FileIOSizeT), CopyFrom, RequestSize);

        //
        // Set the completion flag
        //
        //
        requestAddress[requestBytes - 1] = 1;
    }
    else {
        //
        // We need to wrap the buffer around
        //
        //
        RingSizeT remainingBytes = DDS_REQUEST_RING_BYTES - tail;
        char* requestAddress1 = &RingBuffer->Buffer[tail];
        char* requestAddress2 = &RingBuffer->Buffer[0];

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT*)requestAddress1) = requestBytes;

        //
        // Write the request to two memory locations
        //
        //
        memcpy(requestAddress1 + sizeof(FileIOSizeT), CopyFrom, remainingBytes);
        memcpy(requestAddress2, (const char*)CopyFrom + remainingBytes, requestBytes - remainingBytes);

        //
        // Set the completion flag
        //
        //
        RingBuffer->Buffer[(tail + requestBytes) % DDS_REQUEST_RING_BYTES - 1] = 1;
    }

    return true;
}

//
// Fetch requests from the request buffer
//
//
bool
FetchFromRequestBuffer(
    RequestRingBuffer* RingBuffer,
    BufferT CopyTo,
    FileIOSizeT* RequestSize
) {
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];

    if (tail == head) {
        return false;
    }

    RingSizeT availBytes = 0;
    char* sourceBuffer1 = nullptr;
    char* sourceBuffer2 = nullptr;

    if (tail > head) {
        availBytes = tail - head;
        *RequestSize = availBytes;
        sourceBuffer1 = &RingBuffer->Buffer[head];
    }
    else {
        availBytes = DDS_REQUEST_RING_BYTES - head;
        *RequestSize = availBytes + tail;
        sourceBuffer1 = &RingBuffer->Buffer[head];
        sourceBuffer2 = &RingBuffer->Buffer[0];
    }

    //
    // Check if the request is completely written
    //
    //
    if (!tail) {
        if (!RingBuffer->Buffer[DDS_REQUEST_RING_BYTES - 1]) {
            return false;
        }
    }
    else {
        if (!RingBuffer->Buffer[tail - 1]) {
            return false;
        }
    }

    memcpy(CopyTo, sourceBuffer1, availBytes);
    memset(sourceBuffer1, 0, availBytes);

    if (sourceBuffer2) {
        memcpy((char*)CopyTo + availBytes, sourceBuffer2, tail);
        memset(sourceBuffer2, 0, tail);
    }

    RingBuffer->Head[0] = tail;

    return true;
}

//
// Parse a request from copied data
// Note: RequestSize is greater than the actual request size and is cache line aligned
//
//
void
ParseNextRequest(
    BufferT CopyTo,
    FileIOSizeT TotalSize,
    BufferT* RequestPointer,
    FileIOSizeT* RequestSize,
    BufferT* StartOfNext,
    FileIOSizeT* RemainingSize
) {
    char* bufferAddress = (char*)CopyTo;
    FileIOSizeT totalBytes = *(FileIOSizeT*)bufferAddress;
    *RequestPointer = (BufferT)(bufferAddress + sizeof(FileIOSizeT));
    *RequestSize = totalBytes - sizeof(FileIOSizeT);
    *RemainingSize = TotalSize - totalBytes;

    if (*RemainingSize > 0) {
        *StartOfNext = (BufferT)(bufferAddress + totalBytes);
    }
    else {
        *StartOfNext = nullptr;
    }
}

}