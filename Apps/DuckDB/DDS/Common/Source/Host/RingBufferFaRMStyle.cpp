#pragma once

#include <iostream>

#include "RingBufferFaRMStyle.h"

namespace DDS_FrontEnd {

//
// Allocate a request buffer object
//
//
RequestRingBufferFaRMStyle*
AllocateRequestBufferFaRMStyle(
    BufferT BufferAddress
) {
    RequestRingBufferFaRMStyle* ringBuffer = (RequestRingBufferFaRMStyle*)BufferAddress;

    memset(ringBuffer, 0, sizeof(RequestRingBufferFaRMStyle));

    return ringBuffer;
}

//
// Deallocate a request buffer object
//
//
void
DeallocateRequestBufferFaRMStyle(
    RequestRingBufferFaRMStyle* RingBuffer
) {
    memset(RingBuffer, 0, sizeof(RequestRingBufferFaRMStyle));
}

//
// Insert a request into the request buffer
//
//
bool
InsertToRequestBufferFaRMStyle(
    RequestRingBufferFaRMStyle* RingBuffer,
    const BufferT CopyFrom,
    FileIOSizeT RequestSize
) {
    int tail = RingBuffer->Tail;
    int head = RingBuffer->Head;
    RingSizeT distance = 0;

    if (tail < head) {
        distance = tail + DDS_REQUEST_RING_BYTES - head;
    }
    else {
        distance = tail - head;
    }

    FileIOSizeT requestBytes = sizeof(FileIOSizeT) + RequestSize + 1;
    while (requestBytes % RING_BUFFER_REQUEST_HEADER_SIZE != 0) {
        requestBytes++;
    }

    //
    // Check space
    //
    //
    if (requestBytes > DDS_REQUEST_RING_BYTES - distance) {
        return false;
    }

    while (RingBuffer->Tail.compare_exchange_weak(tail, (tail + requestBytes) % DDS_REQUEST_RING_BYTES) == false) {
        tail = RingBuffer->Tail;
        head = RingBuffer->Head;

        if (tail < head) {
            distance = tail + DDS_REQUEST_RING_BYTES - head;
        }
        else {
            distance = tail - head;
        }

        //
        // Check space
        //
        //
        if (requestBytes > DDS_REQUEST_RING_BYTES - distance) {
            return false;
        }
    }

    //
    // Now, all good
    //
    //
    int newTail = tail + requestBytes;
    if (newTail <= DDS_REQUEST_RING_BYTES) {
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

        //
        // Set the completion flag
        //
        //
        RingBuffer->Buffer[newTail % DDS_REQUEST_RING_BYTES - 1] = 1;
    }

    return true;
}

//
// Fetch requests from the request buffer
//
//
bool
FetchFromRequestBufferFaRMStyle(
    RequestRingBufferFaRMStyle* RingBuffer,
    BufferT CopyTo,
    FileIOSizeT* RequestSize
) {
    int head = RingBuffer->Head;
    char* requestBuffer = &RingBuffer->Buffer[head];

    //
    // Check if there are requests
    //
    //
    FileIOSizeT requestSize = *((FileIOSizeT*)requestBuffer);
    if (requestSize == 0) {
        return false;
    }

    int tail = (head + requestSize) % DDS_REQUEST_RING_BYTES;

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
ParseNextRequestFaRMStyle(
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
CheckForCompletionFaRMStyle(
    RequestRingBufferFaRMStyle* RingBuffer
) {
    return RingBuffer->Tail == RingBuffer->Head;
}

}