#pragma once

// #include "MsgTypes.h"
#include "ring_buffer.hpp"
#include <stdio.h>

#undef DEBUG_RING_BUFFER
#ifdef DEBUG_RING_BUFFER
#define DebugPrint(Fmt, ...) fprintf(stderr, Fmt, __VA_ARGS__)
#else
static inline void DebugPrint(const char *Fmt, ...)
{
}
#endif

#define RING_BUFFER_SHM_NAME "/request_ring_buffer"

namespace DDS_FrontEnd
{

//
// **************************************************************
// Request buffer implementation
// **************************************************************
//

//
// Allocate a request buffer object
// Note: Assuming buffer has been zero'ed
//
//
RequestRingBufferProgressive *AllocateRequestBufferProgressive()
{
    const size_t BUFFER_SIZE = sizeof(RequestRingBufferProgressive);

    // Create or open shared memory object
    int shm_fd = shm_open(RING_BUFFER_SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1)
    {
        printf("shm_open failed for allocate ring buffer\n");
        return nullptr;
    }

    // Set the size of the shared memory object
    if (ftruncate(shm_fd, BUFFER_SIZE) == -1)
    {
        close(shm_fd);
        shm_unlink(RING_BUFFER_SHM_NAME);
        printf("ftruncate failed for allocate ring buffer\n");
        return nullptr;
    }

    // Map the shared memory object into this process's memory space
    RequestRingBufferProgressive *ringBuffer =
        (RequestRingBufferProgressive *)mmap(NULL, BUFFER_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);

    if (ringBuffer == MAP_FAILED)
    {
        close(shm_fd);
        shm_unlink(RING_BUFFER_SHM_NAME);
        return nullptr;
    }
    memset(ringBuffer, 0, sizeof(RequestRingBufferProgressive));

    // Close the file descriptor (the mapping remains)
    close(shm_fd);

    //
    // Align the buffer by cache line size
    //
    //
    /* size_t ringBufferAddress = (size_t)ringBuffer;
    while (ringBufferAddress % CACHE_LINE_SIZE != 0)
    {
        printf("ringBufferAddress: %lu\n", ringBufferAddress);
        ringBufferAddress++;
    }
    ringBuffer = (RequestRingBufferProgressive *)ringBufferAddress; */
    if ((size_t)ringBuffer % CACHE_LINE_SIZE != 0)
    {
        printf("ring buffer not aligned to cache line\n");
        return nullptr;
    }

    return ringBuffer;
}

//
// Setup the shared memory for the request buffer, and return the pointer to the buffer
// Note: must be called by the client interface after DPM sets it up
//
//
RequestRingBufferProgressive *SetupRequestBufferProgressive()
{
    const size_t BUFFER_SIZE = sizeof(RequestRingBufferProgressive);

    // Open shared memory object
    int shm_fd = shm_open(RING_BUFFER_SHM_NAME, O_RDWR, 0666);
    if (shm_fd == -1)
    {
        printf("shm_open failed for setup ring buffer\n");
        return nullptr;
    }

    // Map the shared memory object into this process's memory space
    RequestRingBufferProgressive *ringBuffer =
        (RequestRingBufferProgressive *)mmap(NULL, BUFFER_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);

    if (ringBuffer == MAP_FAILED)
    {
        close(shm_fd);
        printf("mmap failed for setup ring buffer\n");
        return nullptr;
    }

    // Close the file descriptor (the mapping remains)
    close(shm_fd);

    return ringBuffer;
}

//
// Deallocate a request buffer object
//
//
void DeallocateRequestBufferProgressive(RequestRingBufferProgressive *RingBuffer)
{
    memset(RingBuffer, 0, sizeof(RequestRingBufferProgressive));
}

//
// Insert a request into the request buffer
//
//
bool InsertToRequestBufferProgressive(RequestRingBufferProgressive *RingBuffer, const BufferT CopyFrom,
                                      FileIOSizeT RequestSize)
{
    if (RingBuffer->Head[0] || RingBuffer->Tail[0] || RingBuffer->Progress[0])
    {
        printf("insert, head = %d, tail = %d, progress = %d\n", RingBuffer->Head[0], RingBuffer->Tail[0].load(),
               RingBuffer->Progress[0].load());
    }
    //
    // Check if the tail exceeds the allowable advance
    //
    //
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];
    RingSizeT distance = 0;

    if (tail < head)
    {
        distance = tail + DDS_REQUEST_RING_BYTES - head;
    }
    else
    {
        distance = tail - head;
    }

    //
    // Append request size to the beginning of the request
    // Check alignment
    //
    //
    FileIOSizeT requestBytes = sizeof(FileIOSizeT) + RequestSize;

    if (requestBytes % sizeof(FileIOSizeT) != 0)
    {
        requestBytes += (sizeof(FileIOSizeT) - (requestBytes % sizeof(FileIOSizeT)));
    }

    if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
    {
        return false;
    }

    if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
    {
        return false;
    }

    while (RingBuffer->Tail[0].compare_exchange_weak(tail, (tail + requestBytes) % DDS_REQUEST_RING_BYTES) == false)
    {
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];

        //
        // Check if the tail exceeds the allowable advance
        //
        //
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];

        if (tail <= head)
        {
            distance = tail + DDS_REQUEST_RING_BYTES - head;
        }
        else
        {
            distance = tail - head;
        }

        if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
        {
            return false;
        }

        //
        // Check space
        //
        //
        if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
        {
            return false;
        }
    }

    //
    // Now, both tail and space are good
    //
    //
    if (tail + sizeof(FileIOSizeT) + RequestSize <= DDS_REQUEST_RING_BYTES)
    {
        char *requestAddress = &RingBuffer->Buffer[tail];
        printf("tail = %d, head = %d, progress = %d\n", tail, head, RingBuffer->Progress[0].load());
        printf("payload shm_ptr = %lu\n", *((size_t *)CopyFrom));

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT *)requestAddress) = requestBytes;

        //
        // Write the request
        //
        //
        memcpy(requestAddress + sizeof(FileIOSizeT), CopyFrom, RequestSize);

        //
        // Increment the progress
        //
        //
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                           DDS_REQUEST_RING_BYTES) == false)
        {
            progress = RingBuffer->Progress[0];
        }
    }
    else
    {
        //
        // We need to wrap the buffer around
        //
        //
        RingSizeT remainingBytes = DDS_REQUEST_RING_BYTES - tail - sizeof(FileIOSizeT);
        char *requestAddress1 = &RingBuffer->Buffer[tail];
        char *requestAddress2 = &RingBuffer->Buffer[0];

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT *)requestAddress1) = requestBytes;

        //
        // Write the request to two memory locations
        //
        //
        if (remainingBytes > 0)
        {
            memcpy(requestAddress1 + sizeof(FileIOSizeT), CopyFrom, remainingBytes);
        }
        memcpy(requestAddress2, (const char *)CopyFrom + remainingBytes, RequestSize - remainingBytes);

        //
        // Increment the progress
        //
        //
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                           DDS_REQUEST_RING_BYTES) == false)
        {
            progress = RingBuffer->Progress[0];
        }
    }

    return true;
}

//
// Insert a WriteFile request into the request buffer
//
//
/* bool InsertWriteFileRequest(RequestRingBufferProgressive *RingBuffer, RequestIdT RequestId, FileIdT FileId,
                            FileSizeT Offset, FileIOSizeT Bytes, BufferT SourceBuffer)
{
    FileIOSizeT requestSize = sizeof(BuffMsgF2BReqHeader) + Bytes;

    //
    // Check if the tail exceeds the allowable advance
    //
    //
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];
    RingSizeT distance = 0;

    if (tail < head)
    {
        distance = tail + DDS_REQUEST_RING_BYTES - head;
    }
    else
    {
        distance = tail - head;
    }

    //
    // Append request size to the beginning of the request
    // Check alignment
    //
    //
    FileIOSizeT requestBytes = sizeof(FileIOSizeT) + requestSize;
    FileIOSizeT alignment = sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader);

    if (requestBytes % alignment != 0)
    {
        requestBytes += (alignment - (requestBytes % alignment));
    }

    if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
    {
        return false;
    }

    if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
    {
        return false;
    }

    while (RingBuffer->Tail[0].compare_exchange_weak(tail, (tail + requestBytes) % DDS_REQUEST_RING_BYTES) == false)
    {
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];

        //
        // Check if the tail exceeds the allowable advance
        //
        //
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];

        if (tail <= head)
        {
            distance = tail + DDS_REQUEST_RING_BYTES - head;
        }
        else
        {
            distance = tail - head;
        }

        if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
        {
            return false;
        }

        if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
        {
            return false;
        }
    }

    //
    // Now, both tail and space are good
    //
    //
    if (tail + sizeof(FileIOSizeT) + requestSize <= DDS_REQUEST_RING_BYTES)
    {
        char *requestAddress = &RingBuffer->Buffer[tail];

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT *)requestAddress) = requestBytes;

        //
        // Write the request
        //
        //
        BuffMsgF2BReqHeader *header = (BuffMsgF2BReqHeader *)(requestAddress + sizeof(FileIOSizeT));
        header->RequestId = RequestId;
        header->FileId = FileId;
        header->Offset = Offset;
        header->Bytes = Bytes;
        memcpy(requestAddress + sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader), SourceBuffer, Bytes);

        //
        // Increment the progress
        //
        //
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                           DDS_REQUEST_RING_BYTES) == false)
        {
            progress = RingBuffer->Progress[0];
        }
    }
    else
    {
        //
        // We need to wrap the buffer around
        //
        //
        RingSizeT remainingBytes = DDS_REQUEST_RING_BYTES - tail - sizeof(FileIOSizeT);
        char *requestAddress1 = &RingBuffer->Buffer[tail];
        char *requestAddress2 = &RingBuffer->Buffer[0];

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT *)requestAddress1) = requestBytes;

        //
        // Write the request to two memory locations
        //
        //
        if (remainingBytes >= sizeof(BuffMsgF2BReqHeader))
        {
            BuffMsgF2BReqHeader *header = (BuffMsgF2BReqHeader *)(requestAddress1 + sizeof(FileIOSizeT));
            header->RequestId = RequestId;
            header->FileId = FileId;
            header->Offset = Offset;
            header->Bytes = Bytes;

            remainingBytes -= sizeof(BuffMsgF2BReqHeader);
            if (remainingBytes > 0)
            {
                memcpy(requestAddress1 + sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader), SourceBuffer,
                       remainingBytes);
            }
            memcpy(requestAddress2, (const char *)SourceBuffer + remainingBytes, Bytes - remainingBytes);
        }
        else
        {
            BuffMsgF2BReqHeader header;
            header.RequestId = RequestId;
            header.FileId = FileId;
            header.Offset = Offset;
            header.Bytes = Bytes;
            size_t headerOverflowSize = sizeof(BuffMsgF2BReqHeader) - remainingBytes;

            memcpy(requestAddress1 + sizeof(FileIOSizeT), &header, remainingBytes);
            memcpy(requestAddress2, (const char *)&header + remainingBytes, headerOverflowSize);
            memcpy(requestAddress2 + headerOverflowSize, SourceBuffer, Bytes);
        }

        //
        // Increment the progress
        //
        //
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                           DDS_REQUEST_RING_BYTES) == false)
        {
            progress = RingBuffer->Progress[0];
        }
    }

    return true;
}
 */

//
// Insert a WriteFileGather request into the request buffer
//
//
/* bool InsertWriteFileGatherRequest(RequestRingBufferProgressive *RingBuffer, RequestIdT RequestId, FileIdT FileId,
                                  FileSizeT Offset, FileIOSizeT Bytes, BufferT *SourceBufferArray)
{
    FileIOSizeT requestSize = sizeof(BuffMsgF2BReqHeader) + Bytes;

    //
    // Check if the tail exceeds the allowable advance
    //
    //
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];
    RingSizeT distance = 0;

    if (tail < head)
    {
        distance = tail + DDS_REQUEST_RING_BYTES - head;
    }
    else
    {
        distance = tail - head;
    }

    //
    // Append request size to the beginning of the request
    // Check alignment
    //
    //
    FileIOSizeT requestBytes = sizeof(FileIOSizeT) + requestSize;
    FileIOSizeT alignment = sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader);

    if (requestBytes % alignment != 0)
    {
        requestBytes += (alignment - (requestBytes % alignment));
    }

    if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
    {
        return false;
    }

    if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
    {
        return false;
    }

    while (RingBuffer->Tail[0].compare_exchange_weak(tail, (tail + requestBytes) % DDS_REQUEST_RING_BYTES) == false)
    {
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];

        //
        // Check if the tail exceeds the allowable advance
        //
        //
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];

        if (tail <= head)
        {
            distance = tail + DDS_REQUEST_RING_BYTES - head;
        }
        else
        {
            distance = tail - head;
        }

        if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
        {
            return false;
        }

        //
        // Check space
        //
        //
        if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
        {
            return false;
        }
    }

    //
    // Now, both tail and space are good
    //
    //
    if (tail + sizeof(FileIOSizeT) + requestSize <= DDS_REQUEST_RING_BYTES)
    {
        char *requestAddress = &RingBuffer->Buffer[tail];

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT *)requestAddress) = requestBytes;

        //
        // Write the request
        //
        //
        BuffMsgF2BReqHeader *header = (BuffMsgF2BReqHeader *)(requestAddress + sizeof(FileIOSizeT));
        header->RequestId = RequestId;
        header->FileId = FileId;
        header->Offset = Offset;
        header->Bytes = Bytes;

        int numSegments = (int)(Bytes / DDS_PAGE_SIZE);
        int p = 0;
        char *writeBuffer = requestAddress + sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader);

        for (; p != numSegments; p++)
        {
            memcpy(writeBuffer, SourceBufferArray[p], DDS_PAGE_SIZE);
            writeBuffer += DDS_PAGE_SIZE;
        }

        //
        // Handle the residual
        //
        //
        FileIOSizeT residual = Bytes - (FileIOSizeT)(DDS_PAGE_SIZE * numSegments);
        if (residual)
        {
            memcpy(writeBuffer, SourceBufferArray[p], residual);
        }

        //
        // Increment the progress
        //
        //
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                           DDS_REQUEST_RING_BYTES) == false)
        {
            progress = RingBuffer->Progress[0];
        }
    }
    else
    {
        //
        // We need to wrap the buffer around
        //
        //
        RingSizeT remainingBytes = DDS_REQUEST_RING_BYTES - tail - sizeof(FileIOSizeT);
        char *requestAddress1 = &RingBuffer->Buffer[tail];
        char *requestAddress2 = &RingBuffer->Buffer[0];

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT *)requestAddress1) = requestBytes;

        //
        // Write the request to two memory locations
        //
        //
        if (remainingBytes >= sizeof(BuffMsgF2BReqHeader))
        {
            BuffMsgF2BReqHeader *header = (BuffMsgF2BReqHeader *)(requestAddress1 + sizeof(FileIOSizeT));
            header->RequestId = RequestId;
            header->FileId = FileId;
            header->Offset = Offset;
            header->Bytes = Bytes;

            remainingBytes -= sizeof(BuffMsgF2BReqHeader);
            FileIOSizeT residualLeft = 0;
            int numSegmentsLeft = 0;
            if (remainingBytes > 0)
            {
                // memcpy(requestAddress1 + sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader), SourceBuffer,
                // remainingBytes);
                numSegmentsLeft = (int)(remainingBytes / DDS_PAGE_SIZE);
                int p = 0;
                char *writeBuffer = requestAddress1 + sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader);

                for (; p != numSegmentsLeft; p++)
                {
                    memcpy(writeBuffer, SourceBufferArray[p], DDS_PAGE_SIZE);
                    writeBuffer += DDS_PAGE_SIZE;
                }

                //
                // Handle the residual
                //
                //
                residualLeft = remainingBytes - (FileIOSizeT)(DDS_PAGE_SIZE * numSegmentsLeft);
                if (residualLeft)
                {
                    memcpy(writeBuffer, SourceBufferArray[p], residualLeft);
                }
            }

            if (numSegmentsLeft || residualLeft)
            {
                int numSegments = (int)(Bytes / DDS_PAGE_SIZE);
                int p = numSegmentsLeft;
                char *writeBuffer = requestAddress2;

                //
                // Handle the residual on the left side
                //
                //
                if (residualLeft)
                {
                    size_t remainingBytesLeft = DDS_PAGE_SIZE - residualLeft;
                    memcpy(writeBuffer, SourceBufferArray[p] + residualLeft, remainingBytesLeft);
                    writeBuffer += remainingBytesLeft;
                    p++;
                }

                //
                // Copy whole pages
                //
                //
                for (; p != numSegments; p++)
                {
                    memcpy(writeBuffer, SourceBufferArray[p], DDS_PAGE_SIZE);
                    writeBuffer += DDS_PAGE_SIZE;
                }

                //
                // Handle the residual on the right side
                //
                //
                FileIOSizeT residualRight = Bytes - (FileIOSizeT)(DDS_PAGE_SIZE * numSegments);
                if (residualRight)
                {
                    memcpy(writeBuffer, SourceBufferArray[p], residualRight);
                }
            }
            else
            {
                int numSegments = (int)(Bytes / DDS_PAGE_SIZE);
                int p = 0;
                char *writeBuffer = requestAddress2;

                for (; p != numSegments; p++)
                {
                    memcpy(writeBuffer, SourceBufferArray[p], DDS_PAGE_SIZE);
                    writeBuffer += DDS_PAGE_SIZE;
                }

                //
                // Handle the residual
                //
                //
                FileIOSizeT residual = Bytes - (FileIOSizeT)(DDS_PAGE_SIZE * numSegments);
                if (residual)
                {
                    memcpy(writeBuffer, SourceBufferArray[p], residual);
                }
            }
        }
        else
        {
            BuffMsgF2BReqHeader header;
            header.RequestId = RequestId;
            header.FileId = FileId;
            header.Offset = Offset;
            header.Bytes = Bytes;
            size_t headerOverflowSize = sizeof(BuffMsgF2BReqHeader) - remainingBytes;

            memcpy(requestAddress1 + sizeof(FileIOSizeT), &header, remainingBytes);
            memcpy(requestAddress2, (const char *)&header + remainingBytes, headerOverflowSize);

            int numSegments = Bytes / DDS_PAGE_SIZE;
            int p = 0;
            char *writeBuffer = requestAddress2 + headerOverflowSize;

            for (; p != numSegments; p++)
            {
                memcpy(writeBuffer, SourceBufferArray[p], DDS_PAGE_SIZE);
                writeBuffer += DDS_PAGE_SIZE;
            }

            //
            // Handle the residual
            //
            //
            FileIOSizeT residual = Bytes - (FileIOSizeT)(DDS_PAGE_SIZE * numSegments);
            if (residual)
            {
                memcpy(writeBuffer, SourceBufferArray[p], residual);
            }
        }

        //
        // Increment the progress
        //
        //
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                           DDS_REQUEST_RING_BYTES) == false)
        {
            progress = RingBuffer->Progress[0];
        }
    }

    return true;
}
 */

//
// Insert a ReadFile or ReadFileScatter request into the request buffer
//
//
/* bool InsertReadRequest(RequestRingBufferProgressive *RingBuffer, RequestIdT RequestId, FileIdT FileId, FileSizeT
Offset, FileIOSizeT Bytes)
{
    FileIOSizeT requestSize = sizeof(BuffMsgF2BReqHeader);

    //
    // Check if the tail exceeds the allowable advance
    //
    //
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];
    RingSizeT distance = 0;

    if (tail < head)
    {
        distance = tail + DDS_REQUEST_RING_BYTES - head;
    }
    else
    {
        distance = tail - head;
    }

    //
    // Append request size to the beginning of the request
    // No need to check alignment
    //
    //
    FileIOSizeT requestBytes = sizeof(FileIOSizeT) + requestSize;

    if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
    {
        return false;
    }

    if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
    {
        return false;
    }

    while (RingBuffer->Tail[0].compare_exchange_weak(tail, (tail + requestBytes) % DDS_REQUEST_RING_BYTES) == false)
    {
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];

        //
        // Check if the tail exceeds the allowable advance
        //
        //
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];

        if (tail <= head)
        {
            distance = tail + DDS_REQUEST_RING_BYTES - head;
        }
        else
        {
            distance = tail - head;
        }

        if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
        {
            return false;
        }

        //
        // Check space
        //
        //
        if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
        {
            return false;
        }
    }

    //
    // Now, both tail and space are good
    //
    //
    if (tail + sizeof(FileIOSizeT) + requestSize <= DDS_REQUEST_RING_BYTES)
    {
        char *requestAddress = &RingBuffer->Buffer[tail];

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT *)requestAddress) = requestBytes;

        //
        // Write the request
        //
        //
        BuffMsgF2BReqHeader *header = (BuffMsgF2BReqHeader *)(requestAddress + sizeof(FileIOSizeT));
        header->RequestId = RequestId;
        header->FileId = FileId;
        header->Offset = Offset;
        header->Bytes = Bytes;

        //
        // Increment the progress
        //
        //
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                           DDS_REQUEST_RING_BYTES) == false)
        {
            progress = RingBuffer->Progress[0];
        }
    }
    else
    {
        //
        // We need to wrap the buffer around
        //
        //
        RingSizeT remainingBytes = DDS_REQUEST_RING_BYTES - tail - sizeof(FileIOSizeT);
        char *requestAddress1 = &RingBuffer->Buffer[tail];
        char *requestAddress2 = &RingBuffer->Buffer[0];

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT *)requestAddress1) = requestBytes;

        //
        // Write the request to two memory locations
        //
        //
        BuffMsgF2BReqHeader header;
        header.RequestId = RequestId;
        header.FileId = FileId;
        header.Offset = Offset;
        header.Bytes = Bytes;
        size_t headerOverflowSize = sizeof(BuffMsgF2BReqHeader) - remainingBytes;

        memcpy(requestAddress1 + sizeof(FileIOSizeT), &header, remainingBytes);
        memcpy(requestAddress2, (const char *)&header + remainingBytes, headerOverflowSize);

        //
        // Increment the progress
        //
        //
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                           DDS_REQUEST_RING_BYTES) == false)
        {
            progress = RingBuffer->Progress[0];
        }
    }

    return true;
}
 */

//
// Fetch requests from the request buffer
//
//
bool FetchFromRequestBufferProgressive(RequestRingBufferProgressive *RingBuffer, BufferT CopyTo,
                                       FileIOSizeT *RequestSize)
{
    if (RingBuffer->Head[0] || RingBuffer->Tail[0] || RingBuffer->Progress[0])
    {
        printf("fetch, head = %d, tail = %d, progress = %d\n", RingBuffer->Head[0], RingBuffer->Tail[0].load(),
               RingBuffer->Progress[0].load());
    }

    //
    // In order to make this ring buffer safe, we must maintain the invariant below:
    // Each producer moves tail before it increments the progress.
    // Every producer maintains this invariant:
    // They (1) advance the tail,
    //      (2) insert the request, and
    //      (3) increment the progress.
    // However, the order of reading progress and tail at the consumer matters.
    // If the consumer reads the tail first, then it's possible that
    // before it reads the progress, a producer performs all three steps above
    // and thus the progress is updated.
    //
    //
    int progress = RingBuffer->Progress[0];
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];

    //
    // Check if there are requests
    //
    //
    if (tail == head)
    {
        return false;
    }

    //
    // Check if requests are safe to be copied
    //
    //
    if (tail != progress)
    {
        return false;
    }

    //
    // Now, it's safe to copy requests
    //
    //
    RingSizeT availBytes = 0;
    char *sourceBuffer1 = nullptr;
    char *sourceBuffer2 = nullptr;

    if (progress > head)
    {
        availBytes = progress - head;
        *RequestSize = availBytes;
        sourceBuffer1 = &RingBuffer->Buffer[head];
    }
    else
    {
        availBytes = DDS_REQUEST_RING_BYTES - head;
        *RequestSize = availBytes + progress;
        sourceBuffer1 = &RingBuffer->Buffer[head];
        sourceBuffer2 = &RingBuffer->Buffer[0];
    }

    memcpy(CopyTo, sourceBuffer1, availBytes);
    // memset(sourceBuffer1, 0, availBytes);

    if (sourceBuffer2)
    {
        printf("sourceBuffer2\n");
        memcpy((char *)CopyTo + availBytes, sourceBuffer2, progress);
        // memset(sourceBuffer2, 0, progress);
    }

    RingBuffer->Head[0] = progress;

    return true;
}

//
// Parse a request from copied data
// Note: RequestSize is greater than the actual request size
//
//
void ParseNextRequestProgressive(BufferT CopyTo, FileIOSizeT TotalSize, BufferT *RequestPointer,
                                 FileIOSizeT *RequestSize, BufferT *StartOfNext, FileIOSizeT *RemainingSize)
{
    char *bufferAddress = (char *)CopyTo;
    FileIOSizeT totalBytes = *(FileIOSizeT *)bufferAddress;

    *RequestPointer = (BufferT)(bufferAddress + sizeof(FileIOSizeT));
    *RequestSize = totalBytes - sizeof(FileIOSizeT);
    *RemainingSize = TotalSize - totalBytes;

    if (*RemainingSize > 0)
    {
        *StartOfNext = (BufferT)(bufferAddress + totalBytes);
    }
    else
    {
        *StartOfNext = nullptr;
    }
}

//
// Wait for completion
//
//
bool CheckForRequestCompletionProgressive(RequestRingBufferProgressive *RingBuffer)
{
    int head = RingBuffer->Head[0];
    int tail = RingBuffer->Tail[0];

    return head == tail;
}

/*
//
// **************************************************************
// Response buffer implementation
// **************************************************************
//

//
// Allocate a response buffer object
// Note: Assuming buffer has been zero'ed
//
//
ResponseRingBufferProgressive *AllocateResponseBufferProgressive(BufferT BufferAddress)
{
    ResponseRingBufferProgressive *ringBuffer = (ResponseRingBufferProgressive *)BufferAddress;

    //
    // Align the buffer by cache line size
    //
    //
    size_t ringBufferAddress = (size_t)ringBuffer;
    while (ringBufferAddress % DDS_CACHE_LINE_SIZE != 0)
    {
        ringBufferAddress++;
    }
    ringBuffer = (ResponseRingBufferProgressive *)ringBufferAddress;

    return ringBuffer;
}

//
// Deallocate a response buffer object
//
//
void DeallocateResponseBufferProgressive(ResponseRingBufferProgressive *RingBuffer)
{
    memset(RingBuffer, 0, sizeof(ResponseRingBufferProgressive));
}

//
// Fetch a response from the response buffer
//
//
bool FetchFromResponseBufferProgressive(ResponseRingBufferProgressive *RingBuffer, const BufferT CopyTo,
                                        FileIOSizeT *ResponseSize)
{
    //
    // Check if there is a response at the head
    //
    //
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];

    if (tail == head)
    {
        return false;
    }

    FileIOSizeT responseSize = *(FileIOSizeT *)&RingBuffer->Buffer[head];

    if (responseSize == 0)
    {
        return false;
    }

    //
    // Grab the current head
    //
    //
    while (RingBuffer->Head[0].compare_exchange_weak(head, (head + responseSize) % DDS_RESPONSE_RING_BYTES) == false)
    {
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];
        responseSize = *(FileIOSizeT *)&RingBuffer->Buffer[head];

        if (tail == head)
        {
            return false;
        }

        if (responseSize == 0)
        {
            return false;
        }
    }

    //
    // Now, it's safe to copy the response
    //
    //
    int rTail = (head + responseSize) % DDS_RESPONSE_RING_BYTES;
    RingSizeT availBytes = 0;
    char *sourceBuffer1 = nullptr;
    char *sourceBuffer2 = nullptr;

    if (rTail > head)
    {
        availBytes = responseSize;
        *ResponseSize = availBytes;
        sourceBuffer1 = &RingBuffer->Buffer[head];
    }
    else
    {
        availBytes = DDS_RESPONSE_RING_BYTES - head;
        *ResponseSize = availBytes + rTail;
        sourceBuffer1 = &RingBuffer->Buffer[head];
        sourceBuffer2 = &RingBuffer->Buffer[0];
    }

    memcpy(CopyTo, sourceBuffer1, availBytes);
    memset(sourceBuffer1, 0, availBytes);

    if (sourceBuffer2)
    {
        memcpy((char *)CopyTo + availBytes, sourceBuffer2, rTail);
        memset(sourceBuffer2, 0, rTail);
    }

    //
    // Increment the progress
    //
    //
    int progress = RingBuffer->Progress[0];
    while (RingBuffer->Progress[0].compare_exchange_weak(progress,
                                                         (progress + responseSize) % DDS_RESPONSE_RING_BYTES) == false)
    {
        progress = RingBuffer->Progress[0];
    }

    return true;
}

//
// Fetch a response from the response buffer
//
//
bool FetchResponse(ResponseRingBufferProgressive *RingBuffer, BuffMsgB2FAckHeader **Response,
                   SplittableBufferT *DataBuffer)
{
    //
    // Check if there is a response at the head
    //
    //
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];

    if (tail == head)
    {
        return false;
    }

    FileIOSizeT responseSize = *(FileIOSizeT *)&RingBuffer->Buffer[head];

    if (responseSize == 0)
    {
        DebugPrint("[Error] An empty response\n");
        return false;
    }

    DebugPrint("[Debug] Fetching a response: head = %d, response size = %d\n", head, responseSize);

    //
    // Grab the current head
    //
    //
    while (RingBuffer->Head[0].compare_exchange_weak(head, (head + responseSize) % DDS_RESPONSE_RING_BYTES) == false)
    {
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];
        responseSize = *(FileIOSizeT *)&RingBuffer->Buffer[head];

        if (tail == head)
        {
            return false;
        }

        if (responseSize == 0)
        {
            return false;
        }
    }

    //
    // Now, it's safe to return the response
    //
    //
    *Response = (BuffMsgB2FAckHeader *)(&RingBuffer->Buffer[head + sizeof(FileIOSizeT)]);

    FileIOSizeT offset = sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader);

    if (responseSize > offset)
    {
        int dataOffset = (head + offset) % DDS_RESPONSE_RING_BYTES;

        DataBuffer->TotalSize = responseSize - offset;
        DataBuffer->FirstAddr = &RingBuffer->Buffer[dataOffset];

        if (dataOffset + DataBuffer->TotalSize > DDS_RESPONSE_RING_BYTES)
        {
            DataBuffer->FirstSize = DDS_RESPONSE_RING_BYTES - dataOffset;
            DataBuffer->SecondAddr = &RingBuffer->Buffer[0];
        }
        else
        {
            DataBuffer->FirstSize = DataBuffer->TotalSize;
            DataBuffer->SecondAddr = NULL;
        }
    }
    else
    {
        DataBuffer->TotalSize = 0;
    }

    return true;
}

#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
//
// Fetch a batch of responses from the response buffer
//
//
bool FetchResponseBatch(ResponseRingBufferProgressive *RingBuffer, SplittableBufferT *Responses)
{
    //
    // Check if there is a response at the head
    //
    //
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];

    if (tail == head)
    {
        return false;
    }

    FileIOSizeT responseSize = *(FileIOSizeT *)&RingBuffer->Buffer[head];

    if (responseSize == 0)
    {
        DebugPrint("[Error] An empty response\n");
        return false;
    }

    DebugPrint("[Debug] Fetching a response batch: head = %d, response size = %d\n", head, responseSize);

    //
    // Grab the current head
    //
    //
    while (RingBuffer->Head[0].compare_exchange_weak(head, (head + responseSize) % DDS_RESPONSE_RING_BYTES) == false)
    {
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];
        responseSize = *(FileIOSizeT *)&RingBuffer->Buffer[head];

        if (tail == head)
        {
            return false;
        }

        if (responseSize == 0)
        {
            return false;
        }
    }

    //
    // Now, it's safe to return the response
    //
    //
    FileIOSizeT batchMetaSize = (FileIOSizeT)(sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));
    Responses->TotalSize = responseSize - batchMetaSize;
    int spillOver = head + (int)batchMetaSize - (int)DDS_RESPONSE_RING_BYTES;
    if (spillOver >= 0)
    {
        Responses->FirstAddr = &RingBuffer->Buffer[spillOver];
        Responses->FirstSize = Responses->TotalSize;
    }
    else
    {
        Responses->FirstAddr = &RingBuffer->Buffer[head + batchMetaSize];

        if (head + responseSize > DDS_RESPONSE_RING_BYTES)
        {
            Responses->FirstSize = 0 - spillOver;
            Responses->SecondAddr = &RingBuffer->Buffer[0];
        }
        else
        {
            Responses->FirstSize = Responses->TotalSize;
            Responses->SecondAddr = NULL;
        }
    }

    return true;
}
#endif

//
// Increment the progress
//
//
void IncrementProgress(ResponseRingBufferProgressive *RingBuffer, FileIOSizeT ResponseSize)
{
    int progress = RingBuffer->Progress[0];

    while (RingBuffer->Progress[0].compare_exchange_weak(progress,
                                                         (progress + ResponseSize) % DDS_RESPONSE_RING_BYTES) == false)
    {
        progress = RingBuffer->Progress[0];
    }

    DebugPrint("Response progress is incremented by %d. Total progress = %d\n", ResponseSize,
               (progress + ResponseSize) % DDS_RESPONSE_RING_BYTES);
}

//
// Insert responses into the response buffer
//
//
bool InsertToResponseBufferProgressive(ResponseRingBufferProgressive *RingBuffer, const BufferT *CopyFromList,
                                       FileIOSizeT *ResponseSizeList, int NumResponses, int *NumResponsesInserted)
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
    int progress = RingBuffer->Progress[0];
    int head = RingBuffer->Head[0];
    int tail = RingBuffer->Tail[0];

    //
    // Check if responses are safe to be inserted
    //
    //
    if (head != progress)
    {
        return false;
    }

    RingSizeT distance = 0;

    if (tail >= head)
    {
        distance = head + DDS_RESPONSE_RING_BYTES - tail;
    }
    else
    {
        distance = head - tail;
    }

    //
    // Not enough space for batched responses
    //
    //
    if (distance < RING_BUFFER_RESPONSE_MINIMUM_TAIL_ADVANCEMENT)
    {
        return false;
    }

    //
    // Now, it's safe to insert responses
    //
    //
    int respIndex = 0;
    FileIOSizeT responseBytes = 0;
    FileIOSizeT totalResponseBytes = 0;
    FileIOSizeT nextBytes = 0;
#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
    int oldTail = tail;
    totalResponseBytes = sizeof(FileIOSizeT) + sizeof(int);
    tail += totalResponseBytes;
    for (; respIndex != NumResponses; respIndex++)
    {
        responseBytes = ResponseSizeList[respIndex];

        nextBytes = totalResponseBytes + responseBytes;

        //
        // Align to the size of FileIOSizeT
        //
        //
        if (nextBytes % sizeof(FileIOSizeT) != 0)
        {
            nextBytes += (sizeof(FileIOSizeT) - (nextBytes % sizeof(FileIOSizeT)));
        }

        if (nextBytes > distance || nextBytes > RING_BUFFER_RESPONSE_MAXIMUM_TAIL_ADVANCEMENT)
        {
            //
            // No more space or reaching maximum batch size
            //
            //
            break;
        }

        if (tail + ResponseSizeList[respIndex] <= DDS_RESPONSE_RING_BYTES)
        {
            //
            // Write one response
            // On DPU, these responses should batched and there should be no extra memory copy
            //
            //
            memcpy(&RingBuffer->Buffer[tail], CopyFromList[respIndex], ResponseSizeList[respIndex]);
        }
        else
        {
            FileIOSizeT firstPartBytes = DDS_RESPONSE_RING_BYTES - tail;
            FileIOSizeT secondPartBytes = ResponseSizeList[respIndex] - firstPartBytes;

            //
            // Write one response to two locations
            // On DPU, these responses should batched and there should be no extra memory copy
            //
            //
            memcpy(&RingBuffer->Buffer[tail], CopyFromList[respIndex], firstPartBytes);
            memcpy(&RingBuffer->Buffer[0], (char *)CopyFromList[respIndex] + firstPartBytes, secondPartBytes);
        }

        totalResponseBytes += responseBytes;
        tail = (tail + responseBytes) % DDS_RESPONSE_RING_BYTES;
    }

    if (totalResponseBytes % sizeof(FileIOSizeT) != 0)
    {
        totalResponseBytes += (sizeof(FileIOSizeT) - (totalResponseBytes % sizeof(FileIOSizeT)));
    }

    tail = oldTail + totalResponseBytes;
    *(FileIOSizeT *)&RingBuffer->Buffer[oldTail] = totalResponseBytes;
    *(int *)&RingBuffer->Buffer[oldTail + sizeof(FileIOSizeT)] = respIndex;
#else
    for (; respIndex != NumResponses; respIndex++)
    {
        responseBytes = ResponseSizeList[respIndex] + sizeof(FileIOSizeT);

        //
        // Align to the size of FileIOSizeT
        //
        //
        if (responseBytes % sizeof(FileIOSizeT) != 0)
        {
            responseBytes += (sizeof(FileIOSizeT) - (responseBytes % sizeof(FileIOSizeT)));
        }

        nextBytes = totalResponseBytes + responseBytes;
        if (nextBytes > distance || nextBytes > RING_BUFFER_RESPONSE_MAXIMUM_TAIL_ADVANCEMENT)
        {
            //
            // No more space or reaching maximum batch size
            //
            //
            break;
        }

        if (tail + sizeof(FileIOSizeT) + ResponseSizeList[respIndex] <= DDS_RESPONSE_RING_BYTES)
        {
            //
            // Write one response
            // On DPU, these responses should batched and there should be no extra memory copy
            //
            //
            memcpy(&RingBuffer->Buffer[(tail + sizeof(FileIOSizeT)) % DDS_RESPONSE_RING_BYTES], CopyFromList[respIndex],
                   ResponseSizeList[respIndex]);
        }
        else
        {
            FileIOSizeT firstPartBytes = DDS_RESPONSE_RING_BYTES - tail - sizeof(FileIOSizeT);
            FileIOSizeT secondPartBytes = ResponseSizeList[respIndex] - firstPartBytes;

            //
            // Write one response to two locations
            // On DPU, these responses should batched and there should be no extra memory copy
            //
            //
            if (firstPartBytes > 0)
            {
                memcpy(&RingBuffer->Buffer[tail + sizeof(FileIOSizeT)], CopyFromList[respIndex], firstPartBytes);
            }
            memcpy(&RingBuffer->Buffer[0], (char *)CopyFromList[respIndex] + firstPartBytes, secondPartBytes);
        }

        *(FileIOSizeT *)&RingBuffer->Buffer[tail] = responseBytes;
        totalResponseBytes += responseBytes;
        tail = (tail + responseBytes) % DDS_RESPONSE_RING_BYTES;
    }
#endif

    *NumResponsesInserted = respIndex;
    RingBuffer->Tail[0] = tail;

    return true;
}

//
// Parse a response from copied data
// Note: ResponseSize is greater than the actual response size because of alignment
//
//
void ParseNextResponseProgressive(BufferT CopyTo, FileIOSizeT TotalSize, BufferT *ResponsePointer,
                                  FileIOSizeT *ResponseSize, BufferT *StartOfNext, FileIOSizeT *RemainingSize)
{
    char *bufferAddress = (char *)CopyTo;
    FileIOSizeT totalBytes = *(FileIOSizeT *)bufferAddress;

    *ResponsePointer = (BufferT)(bufferAddress + sizeof(FileIOSizeT));
    *ResponseSize = totalBytes - sizeof(FileIOSizeT);
    *RemainingSize = TotalSize - totalBytes;

    if (*RemainingSize > 0)
    {
        *StartOfNext = (BufferT)(bufferAddress + totalBytes);
    }
    else
    {
        *StartOfNext = nullptr;
    }
}

//
// Wait for completion
//
//
bool CheckForResponseCompletionProgressive(ResponseRingBufferProgressive *RingBuffer)
{
    int progress = RingBuffer->Progress[0];
    int head = RingBuffer->Head[0];
    int tail = RingBuffer->Tail[0];

    return progress == head && head == tail;
}
 */

} // namespace DDS_FrontEnd