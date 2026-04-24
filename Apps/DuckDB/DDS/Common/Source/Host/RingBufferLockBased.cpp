#pragma once

#include <iostream>

#include "RingBufferLockBased.h"

namespace DDS_FrontEnd {

//
// Allocate a request buffer object
//
//
RequestRingBufferLockBased*
AllocateRequestBufferLockBased(
    BufferT BufferAddress
) {
    RequestRingBufferLockBased* ringBuffer = (RequestRingBufferLockBased*)BufferAddress;

    memset(ringBuffer, 0, sizeof(RequestRingBufferLockBased));
    ringBuffer->RingLock = new Mutex();

    return ringBuffer;
}

//
// Deallocate a request buffer object
//
//
void
DeallocateRequestBufferLockBased(
    RequestRingBufferLockBased* RingBuffer
) {
    delete RingBuffer->RingLock;
    memset(RingBuffer, 0, sizeof(RequestRingBufferLockBased));
}

//
// Insert a request into the request buffer
//
//
bool
InsertToRequestBufferLockBased(
    RequestRingBufferLockBased* RingBuffer,
    const BufferT CopyFrom,
    FileIOSizeT RequestSize
) {
    RingBuffer->RingLock->lock();

    int tail = RingBuffer->Tail;
    int head = RingBuffer->Head;
    RingSizeT distance = 0;

    if (tail < head) {
        distance = tail + DDS_REQUEST_RING_BYTES - head;
    }
    else {
        distance = tail - head;
    }

    FileIOSizeT requestBytes = sizeof(FileIOSizeT) + RequestSize;

    if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT) {
        RingBuffer->RingLock->unlock();
        return false;
    }

    //
    // Check space
    //
    //
    if (requestBytes > DDS_REQUEST_RING_BYTES - distance) {
        RingBuffer->RingLock->unlock();
        return false;
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
    }
    else {
        //
        // We need to wrap the buffer around
        //
        //
        RingSizeT remainingBytes = DDS_REQUEST_RING_BYTES - tail - sizeof(FileIOSizeT);
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
        if (RequestSize <= remainingBytes) {
            memcpy(requestAddress1 + sizeof(FileIOSizeT), CopyFrom, RequestSize);
        }
        else {
            memcpy(requestAddress1 + sizeof(FileIOSizeT), CopyFrom, remainingBytes);
            memcpy(requestAddress2, (const char*)CopyFrom + remainingBytes, RequestSize - remainingBytes);
        }
    }

    RingBuffer->Tail = (tail + requestBytes) % DDS_REQUEST_RING_BYTES;
    RingBuffer->RingLock->unlock();

    return true;
}

//
// Fetch requests from the request buffer
//
//
bool
FetchFromRequestBufferLockBased(
    RequestRingBufferLockBased* RingBuffer,
    BufferT CopyTo,
    FileIOSizeT* RequestSize
) {
    int head = RingBuffer->Head;
    int tail = RingBuffer->Tail;
    
    //
    // Check if there are requests
    //
    //
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

    memcpy(CopyTo, sourceBuffer1, availBytes);
    memset(sourceBuffer1, 0, availBytes);

    if (sourceBuffer2) {
        memcpy((char*)CopyTo + availBytes, sourceBuffer2, tail);
        memset(sourceBuffer2, 0, tail);
    }

    RingBuffer->Head = tail;

    return true;
}

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

//
// Wait for completion
//
//
bool
CheckForCompletionLockBased(
    RequestRingBufferLockBased* RingBuffer
) {
    return RingBuffer->Head == RingBuffer->Tail;
}

}