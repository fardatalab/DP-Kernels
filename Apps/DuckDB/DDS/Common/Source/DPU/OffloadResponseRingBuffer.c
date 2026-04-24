#include <string.h>

#include "OffloadResponseRingBuffer.h"

//
// Allocate a response buffer object
//
//
OffloadResponseRingBuffer*
AllocateOffloadResponseBuffer(
    BufferT BufferAddress
) {
    OffloadResponseRingBuffer* ringBuffer = (OffloadResponseRingBuffer*)BufferAddress;

    //
    // Align the buffer by cache line size
    //
    //
    size_t ringBufferAddress = (size_t)ringBuffer;
    while (ringBufferAddress % DDS_CACHE_LINE_SIZE != 0) {
        ringBufferAddress++;
    }
    ringBuffer = (OffloadResponseRingBuffer*)ringBufferAddress;

    return ringBuffer;
}

//
// Deallocate a response queue buffer object
//
//
void
DellocateOffloadResponseBuffer(
    OffloadResponseRingBuffer* RingBuffer
) {
    memset(RingBuffer, 0, sizeof(OffloadResponseRingBuffer));
}