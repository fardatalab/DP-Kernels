#include <stdlib.h>
#include <string.h>

#include "MPSC.h"

//
// Allocate a ring buffer object
//
//
FarRingMPSC *
AllocateMPSC(
    BufferT Buffer)
{
       FarRingMPSC *ringBuffer = (FarRingMPSC *)Buffer;

       //
       // Align the buffer by cache line size
       //
       //
       size_t ringBufferAddress = (size_t)ringBuffer;
       while (ringBufferAddress % CACHE_LINE != 0)
       {
              ringBufferAddress++;
       }
       ringBuffer = (FarRingMPSC *)ringBufferAddress;

       memset((char *)ringBuffer, 0, sizeof(FarRingMPSC));

       return ringBuffer;
}

//
// Deallocate a ring buffer object
//
//
void DeallocateMPSC(
    FarRingMPSC *Ring)
{
       memset((char *)Ring, 0, sizeof(FarRingMPSC));
}

//
// Insert a message into the buffer
//
//
bool InsertMPSC(
    FarRingMPSC *Ring,
    const BufferT CopyFrom,
    MessageSizeT MessageSize)
{
       //
       // Check if the progress exceeds the forward degree
       //
       //
       int progress = Ring->Progress[0];
       int head = Ring->Head[0];
       RingSizeT distance = 0;

       if (progress < head)
       {
              distance = progress + RING_SIZE - head;
       }
       else
       {
              distance = progress - head;
       }

       if (distance >= FORWARD_DEGREE)
       {
              return false;
       }

       //
       // Align the message to cache line size to avoid false sharing;
       // Append message size to the beginning of the message;
       //
       //
       MessageSizeT messageBytes = sizeof(MessageSizeT) + MessageSize;
       while (messageBytes % CACHE_LINE != 0)
       {
              messageBytes++;
       }

       //
       // Check space
       //
       //
       if (messageBytes > RING_SIZE - distance)
       {
              return false;
       }

       while (Ring->Progress[0].compare_exchange_weak(progress, (progress + messageBytes) % RING_SIZE) == false)
       {
              progress = Ring->Progress[0];
              head = Ring->Head[0];

              //
              // Check if the progress exceeds the forward degree
              //
              //
              progress = Ring->Progress[0];
              head = Ring->Head[0];

              if (progress <= head)
              {
                     distance = progress + RING_SIZE - head;
              }
              else
              {
                     distance = progress - head;
              }

              if (distance >= FORWARD_DEGREE)
              {
                     return false;
              }

              //
              // Check space
              //
              //
              if (messageBytes > RING_SIZE - distance)
              {
                     return false;
              }
       }

       //
       // Now, both forward degree and space are good
       //
       //
       if (progress + messageBytes <= RING_SIZE)
       {
              char *messageAddress = &Ring->Buffer[progress];

              //
              // Write the number of bytes in this message
              //
              //
              *((MessageSizeT *)messageAddress) = messageBytes;

              //
              // Write the message
              //
              //
              memcpy(messageAddress + sizeof(MessageSizeT), CopyFrom, MessageSize);

              //
              // Increment the tail
              //
              //
              int tail = Ring->Tail[0];
              while (Ring->Tail[0].compare_exchange_weak(tail, (tail + messageBytes) % RING_SIZE) == false)
              {
                     tail = Ring->Tail[0];
              }
       }
       else
       {
              //
              // We need to wrap the buffer around
              //
              //
              RingSizeT remainingBytes = RING_SIZE - progress - sizeof(MessageSizeT);
              char *messageAddress1 = &Ring->Buffer[progress];

              //
              // Write the number of bytes in this message
              //
              //
              *((MessageSizeT *)messageAddress1) = messageBytes;

              //
              // Write the message to two memory locations
              //
              //
              if (MessageSize <= remainingBytes)
              {
                     memcpy(messageAddress1 + sizeof(MessageSizeT), CopyFrom, MessageSize);
              }
              else
              {
                     char *messageAddress2 = &Ring->Buffer[0];
                     if (remainingBytes)
                     {
                            memcpy(messageAddress1 + sizeof(MessageSizeT), CopyFrom, remainingBytes);
                     }
                     memcpy(messageAddress2, (const char *)CopyFrom + remainingBytes, MessageSize - remainingBytes);
              }

              //
              // Increment the tail
              //
              //
              int tail = Ring->Tail[0];
              while (Ring->Tail[0].compare_exchange_weak(tail, (tail + messageBytes) % RING_SIZE) == false)
              {
                     tail = Ring->Tail[0];
              }
       }

       return true;
}

//
// Fetch messages from the message buffer
//
//
bool FetchMPSC(
    FarRingMPSC *Ring,
    BufferT CopyTo,
    MessageSizeT *MessageSize)
{
       //
       // In order to make this ring buffer to be safe, we must maintain the invariant below:
       // Each producer moves aggressive tail before it moves the tail.
       // Every producer maintains this invariant:
       // They (1) advance aggressive tail,
       //            (2) insert the message, and
       //            (3) advance the tail.
       // However, the order of reading safe and aggresive tails at the consumer matters.
       // If the consumer reads the aggressive tail first, then it's possible that
       // before it reads the tail, a producer performs all three steps above
       // and thus the tail is updated.
       //
       //
       int tail = Ring->Tail[0];
       int progress = Ring->Progress[0];
       int head = Ring->Head[0];

       //
       // Check if there are messages
       //
       //
       if (progress == head)
       {
              return false;
       }

       //
       // Check if messages are safe to be copied
       //
       //
       if (progress != tail)
       {
              return false;
       }

       //
       // Now, it's safe to copy messages
       //
       //
       RingSizeT availBytes = 0;
       char *sourceBuffer1 = nullptr;
       char *sourceBuffer2 = nullptr;

       if (tail > head)
       {
              availBytes = tail - head;
              *MessageSize = availBytes;
              sourceBuffer1 = &Ring->Buffer[head];
       }
       else
       {
              availBytes = RING_SIZE - head;
              *MessageSize = availBytes + tail;
              sourceBuffer1 = &Ring->Buffer[head];
              sourceBuffer2 = &Ring->Buffer[0];
       }

       memcpy(CopyTo, sourceBuffer1, availBytes);
       memset(sourceBuffer1, 0, availBytes);

       if (sourceBuffer2)
       {
              memcpy((char *)CopyTo + availBytes, sourceBuffer2, tail);
              memset(sourceBuffer2, 0, tail);
       }

       Ring->Head[0] = tail;

       return true;
}

//
// Parse a message from copied data
// Note: RequestSize is greater than the actual message size and is cache line aligned
//
//
void ParseNextMessageMPSC(
    BufferT CopyTo,
    MessageSizeT TotalSize,
    BufferT *MessagePointer,
    MessageSizeT *MessageSize,
    BufferT *StartOfNext,
    MessageSizeT *RemainingSize)
{
       char *bufferAddress = (char *)CopyTo;
       MessageSizeT totalBytes = *(MessageSizeT *)bufferAddress;

       *MessagePointer = (BufferT)(bufferAddress + sizeof(MessageSizeT));
       *MessageSize = totalBytes - sizeof(MessageSizeT);
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
