#include <stdlib.h>
#include <string.h>
#include <thread>

#include "SPMC.h"

//
// Allocate a ring buffer object
//
//
FarRingSPMC *
AllocateSPMC(
    BufferT Buffer)
{
    FarRingSPMC *ringBuffer = (FarRingSPMC *)Buffer;

    //
    // Align the buffer by cache line size
    //
    //
    size_t ringBufferAddress = (size_t)ringBuffer;
    while (ringBufferAddress % CACHE_LINE != 0)
    {
        ringBufferAddress++;
    }
    ringBuffer = (FarRingSPMC *)ringBufferAddress;

    memset((char *)ringBuffer, 0, sizeof(FarRingSPMC));

    return ringBuffer;
}

//
// Deallocate a ring buffer object
//
//
void DeallocateSPMC(
    FarRingSPMC *Ring)
{
    memset((char *)Ring, 0, sizeof(FarRingSPMC));
}

//
// Insert a message into the buffer
//
//
bool InsertSPMC(
    FarRingSPMC *Ring,
    const BufferT CopyFrom,
    MessageSizeT MessageSize)
{
    //
    // In order to make this ring buffer safe, we must maintain the invariant below:
    // Each consumer moves the head before it increments the progress.
    // Every consumser maintains this invariant:
    // They (1) advance the head,
    //      (2) read the response, and
    //      (3) increment the progress.
    // However, the order of reading progress and head at the producer matters.
    // If the producer reads the head first, then it's possible that
    // before it reads the progress, a concurrent consumer performs all three steps above
    // and thus the progress is updated.
    //
    //

    int progress = Ring->Progress[0];
    int head = Ring->Head[0];
    int tail = Ring->Tail[0];

    //
    // Check if responses are safe to be inserted
    //
    //
    if (head != progress) {
        return false;
    }

    RingSizeT distance = 0;
    
    if (tail >= head) {
        distance = head + RING_SIZE - tail;
    }
    else {
        distance = head - tail;
    }

    //
    // Not enough space for batched responses
    //
    //
    if (distance < FORWARD_DEGREE) {
        return false;
    }

    //
    // Now, it's safe to insert responses
    //
    //

    MessageSizeT messageBytes = MessageSize + sizeof(MessageSizeT);
        
    //
    // Align to the size of FileIOSizeT
    //
    //
    if (messageBytes % sizeof(MessageSizeT) != 0) {
        messageBytes += (sizeof(MessageSizeT) - (messageBytes % sizeof(MessageSizeT)));
    }
    
    if (messageBytes > distance || messageBytes > FORWARD_DEGREE) {
        //
        // No more space or reaching maximum batch size
        //
        //
        return false;
    }

    if (tail + sizeof(MessageSizeT) + messageBytes <= RING_SIZE) {
        //
        // Write one response
        // On DPU, these responses should batched and there should be no extra memory copy
        //
        //
        memcpy(&Ring->Buffer[(tail + sizeof(MessageSizeT)) % RING_SIZE], CopyFrom, MessageSize);
    }
    else {
        MessageSizeT firstPartBytes = RING_SIZE - tail - sizeof(MessageSizeT);
        MessageSizeT secondPartBytes = MessageSize - firstPartBytes;

        //
        // Write one response to two locations
        // On DPU, these responses should batched and there should be no extra memory copy
        //
        //
        if (firstPartBytes > 0) {
            memcpy(&Ring->Buffer[tail + sizeof(MessageSizeT)], CopyFrom, firstPartBytes);
        }
        memcpy(&Ring->Buffer[0], (char*)CopyFrom + firstPartBytes, secondPartBytes);
    }
    
    *(MessageSizeT*)&Ring->Buffer[tail] = messageBytes;
    tail = (tail + messageBytes) % RING_SIZE;

    Ring->Tail[0] = tail;

    return true;
}

//
// Fetch messages from the ring buffer
//
//
bool FetchSPMC(
    FarRingSPMC *Ring,
    BufferT CopyTo,
    MessageSizeT *MessageSize)
{
    ///
    // Check if there is a response at the head
    //
    //
    int tail = Ring->Tail[0];
    int head = Ring->Head[0];

    if (tail == head) {
        return false;
    }

    MessageSizeT responseSize = *(MessageSizeT*)&Ring->Buffer[head];

    if (responseSize == 0) {
        return false;
    }

    //
    // Grab the current head
    //
    //
    while(Ring->Head[0].compare_exchange_weak(head, (head + responseSize) % RING_SIZE) == false) {
        //
        // The following statement does not improve MPSC but seems to work here
        //
        //
        std::this_thread::yield();

        tail = Ring->Tail[0];
        head = Ring->Head[0];
        responseSize = *(MessageSizeT*)&Ring->Buffer[head];

        if (tail == head) {
            return false;
        }

        if (responseSize == 0) {
            return false;
        }
    }

    //
    // Now, it's safe to copy the response
    //
    //
    int rTail = (head + responseSize) % RING_SIZE;
    RingSizeT availBytes = 0;
    char* sourceBuffer1 = nullptr;
    char* sourceBuffer2 = nullptr;

    if (rTail > head) {
        availBytes = responseSize;
        *MessageSize = availBytes;
        sourceBuffer1 = &Ring->Buffer[head];
    }
    else {
        availBytes = RING_SIZE - head;
        *MessageSize = availBytes + rTail;
        sourceBuffer1 = &Ring->Buffer[head];
        sourceBuffer2 = &Ring->Buffer[0];
    }

    memcpy(CopyTo, sourceBuffer1, availBytes);
    memset(sourceBuffer1, 0, availBytes);

    if (sourceBuffer2) {
        memcpy((char*)CopyTo + availBytes, sourceBuffer2, rTail);
        memset(sourceBuffer2, 0, rTail);
    }

    //
    // Increment the progress
    //
    //
    int progress = Ring->Progress[0];
    while (Ring->Progress[0].compare_exchange_weak(progress, (progress + responseSize) % RING_SIZE) == false) {
        progress = Ring->Progress[0];
    }

    return true;
}


//
// Wait for completion
//
//
bool
CheckForCompletionSPMC(
    FarRingSPMC* Ring
) {
    int progress = Ring->Progress[0];
    int head = Ring->Head[0];
    int tail = Ring->Tail[0];

    return progress == head && head == tail;
}
