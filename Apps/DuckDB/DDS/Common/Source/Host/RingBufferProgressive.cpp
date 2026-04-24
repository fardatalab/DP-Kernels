#pragma once

#include "RingBufferProgressive.h"
#include "MsgTypes.h"
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstring>

#undef DEBUG_RING_BUFFER
#ifdef DEBUG_RING_BUFFER
#define DebugPrint(Fmt, ...) fprintf(stderr, Fmt, __VA_ARGS__)
#else
static inline void DebugPrint(const char *Fmt, ...) {}
#endif

#ifndef DDS_RING_DIAGNOSTICS
#define DDS_RING_DIAGNOSTICS 0
#endif

#ifndef DDS_RING_DIAG_LOG_EVERY
#define DDS_RING_DIAG_LOG_EVERY 0
#endif

namespace DDS_FrontEnd
{
    // jason: Forward declarations for static helpers used before their definitions.
    static bool ImplicitWrapRequestProducer(RequestRingBufferProgressive *RingBuffer, FileIOSizeT contiguousBytes);
    static bool ImplicitWrapResponseConsumer(ResponseRingBufferProgressive *RingBuffer, FileIOSizeT headerBytes);
    static bool ConsumeResponseWrapMarker(ResponseRingBufferProgressive *RingBuffer, int head);

    // jason: Response payload alignment (bytes) for FetchResponse parsing.
    // Atomic to avoid data races between initialization and response consumer threads.
    static std::atomic<FileIOSizeT> g_responsePayloadAlign{0};

    /**
     * Set the response payload alignment used by FetchResponse parsing.
     */
    void SetResponsePayloadAlignment(FileIOSizeT Alignment)
    {
        if (Alignment == 0)
        {
            Alignment = 1;
        }
        g_responsePayloadAlign.store(Alignment, std::memory_order_relaxed);
    }

    /**
     * Get the response payload alignment used by FetchResponse parsing.
     */
    FileIOSizeT GetResponsePayloadAlignment()
    {
        FileIOSizeT align = g_responsePayloadAlign.load(std::memory_order_relaxed);
        if (align == 0)
        {
            align = 1;
        }
        return align;
    }

#if DDS_RING_DIAGNOSTICS
    static std::atomic<unsigned long long> g_requestDiagCounter{0};

    /**
     * Diagnostic counters for host-side request publication.
     *
     * Tracks aggregate counts and the last observed write metadata without printing.
     */
    typedef struct RingDiagCounters
    {
        std::atomic<uint64_t> HostWriteCount{0};
        std::atomic<uint64_t> HostWriteWrapCount{0};
        std::atomic<uint32_t> LastReqId{0};
        std::atomic<uint32_t> LastFileId{0};
        std::atomic<uint64_t> LastOffset{0};
        std::atomic<uint32_t> LastBytes{0};
        std::atomic<int> LastTail{0};
        std::atomic<int> LastHead{0};
        std::atomic<int> LastProgress{0};
        std::atomic<uint32_t> LastRequestBytes{0};
        std::atomic<uint32_t> LastHeaderBytes{0};
        std::atomic<uint32_t> LastWrapped{0};
    } RingDiagCounters;

    static RingDiagCounters g_ringDiagCounters;

    /**
     * Record host-side write publication metadata without emitting logs.
     */
    static inline void RecordHostWriteDiag(RequestIdT RequestId, FileIdT FileId, FileSizeT Offset, FileIOSizeT Bytes,
                                           int Tail, int Head, int Progress, FileIOSizeT RequestBytes,
                                           FileIOSizeT HeaderBytes, bool Wrapped)
    {
        g_ringDiagCounters.HostWriteCount.fetch_add(1, std::memory_order_relaxed);
        if (Wrapped)
        {
            g_ringDiagCounters.HostWriteWrapCount.fetch_add(1, std::memory_order_relaxed);
        }

        g_ringDiagCounters.LastReqId.store((uint32_t)RequestId, std::memory_order_relaxed);
        g_ringDiagCounters.LastFileId.store((uint32_t)FileId, std::memory_order_relaxed);
        g_ringDiagCounters.LastOffset.store((uint64_t)Offset, std::memory_order_relaxed);
        g_ringDiagCounters.LastBytes.store((uint32_t)Bytes, std::memory_order_relaxed);
        g_ringDiagCounters.LastTail.store(Tail, std::memory_order_relaxed);
        g_ringDiagCounters.LastHead.store(Head, std::memory_order_relaxed);
        g_ringDiagCounters.LastProgress.store(Progress, std::memory_order_relaxed);
        g_ringDiagCounters.LastRequestBytes.store((uint32_t)RequestBytes, std::memory_order_relaxed);
        g_ringDiagCounters.LastHeaderBytes.store((uint32_t)HeaderBytes, std::memory_order_relaxed);
        g_ringDiagCounters.LastWrapped.store(Wrapped ? 1U : 0U, std::memory_order_relaxed);
    }
#endif

    /**
     * Fetch a snapshot of host-side request publication diagnostics.
     */
    bool GetHostWriteDiagSnapshot(RingDiagSnapshot *OutSnapshot)
    {
        if (!OutSnapshot)
        {
            return false;
        }

#if DDS_RING_DIAGNOSTICS
        OutSnapshot->HostWriteCount = g_ringDiagCounters.HostWriteCount.load(std::memory_order_relaxed);
        OutSnapshot->HostWriteWrapCount = g_ringDiagCounters.HostWriteWrapCount.load(std::memory_order_relaxed);
        OutSnapshot->LastReqId = g_ringDiagCounters.LastReqId.load(std::memory_order_relaxed);
        OutSnapshot->LastFileId = g_ringDiagCounters.LastFileId.load(std::memory_order_relaxed);
        OutSnapshot->LastOffset = g_ringDiagCounters.LastOffset.load(std::memory_order_relaxed);
        OutSnapshot->LastBytes = g_ringDiagCounters.LastBytes.load(std::memory_order_relaxed);
        OutSnapshot->LastTail = g_ringDiagCounters.LastTail.load(std::memory_order_relaxed);
        OutSnapshot->LastHead = g_ringDiagCounters.LastHead.load(std::memory_order_relaxed);
        OutSnapshot->LastProgress = g_ringDiagCounters.LastProgress.load(std::memory_order_relaxed);
        OutSnapshot->LastRequestBytes = g_ringDiagCounters.LastRequestBytes.load(std::memory_order_relaxed);
        OutSnapshot->LastHeaderBytes = g_ringDiagCounters.LastHeaderBytes.load(std::memory_order_relaxed);
        OutSnapshot->LastWrapped = g_ringDiagCounters.LastWrapped.load(std::memory_order_relaxed);
        return true;
#else
        memset(OutSnapshot, 0, sizeof(*OutSnapshot));
        return false;
#endif
    }

//
// **************************************************************
// Request buffer implementation
// **************************************************************
//

/**
 * Pad the request ring so request headers never split across the ring end.
 * Inserts a size-only padding record when fewer than headerBytes remain.
 *
 * NOTE: Explicit padding is disabled in favor of implicit wrap. This is kept
 * for reference if we revert to padding in the future.
 */
#if 0
static bool PadRequestRingForHeader(RequestRingBufferProgressive* RingBuffer, FileIOSizeT headerBytes)
{
    while (true) {
        int tail = RingBuffer->Tail[0];
        int head = RingBuffer->Head[0];
        RingSizeT distance = 0;

        if (tail < head) {
            distance = tail + DDS_REQUEST_RING_BYTES - head;
        }
        else {
            distance = tail - head;
        }

        RingSizeT tailSpace = DDS_REQUEST_RING_BYTES - tail;
        if (tailSpace >= headerBytes) {
            return true;
        }

        FileIOSizeT paddingBytes = (FileIOSizeT)tailSpace;
        if (distance + paddingBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT) {
            return false;
        }

        if (paddingBytes > DDS_REQUEST_RING_BYTES - distance) {
            return false;
        }

        int expectedTail = tail;
        if (RingBuffer->Tail[0].compare_exchange_weak(
                expectedTail,
                (expectedTail + paddingBytes) % DDS_REQUEST_RING_BYTES) == false) {
            continue;
        }

        // jason: write a size-only padding record when it fits; the consumer guard skips short entries.
        if (paddingBytes >= sizeof(FileIOSizeT)) {
            *((FileIOSizeT*)&RingBuffer->Buffer[expectedTail]) = paddingBytes;
        }

        // Advance progress so the consumer can observe the padding entry.
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(
                   progress,
                   (progress + paddingBytes) % DDS_REQUEST_RING_BYTES) == false) {
            progress = RingBuffer->Progress[0];
        }
    }
}
#endif

    /**
     * Implicit wrap rule for request producers:
     * If tail space can't hold the required contiguous record bytes, advance tail/progress by
     * tail slack and wrap to the phased header offset.
     *
     * For full-record no-split mode, this helper writes a zero-sized marker at the old tail
     * (when possible) so the DPU parser can deterministically skip wrap slack.
     */
    static bool ImplicitWrapRequestProducer(RequestRingBufferProgressive *RingBuffer, FileIOSizeT contiguousBytes)
    {
        while (true)
        {
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

            RingSizeT tailSpace = DDS_REQUEST_RING_BYTES - tail;
            if (tailSpace >= contiguousBytes)
            {
                return true;
            }

            // jason: publish an explicit wrap marker when the size field fits at the old tail.
            if (tailSpace >= (RingSizeT)sizeof(FileIOSizeT))
            {
                *((FileIOSizeT *)&RingBuffer->Buffer[tail]) = 0;
            }
            // jason: wrap to the header phase so (start + sizeof(FileIOSizeT)) stays naturally aligned.
            FileIOSizeT slackBytes = (FileIOSizeT)tailSpace + (FileIOSizeT)DDS_REQUEST_RING_HEADER_PHASE;
            if (distance + slackBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
            {
                return false;
            }

            if (slackBytes > DDS_REQUEST_RING_BYTES - distance)
            {
                return false;
            }

            int expectedTail = tail;
            if (RingBuffer->Tail[0].compare_exchange_weak(expectedTail, (expectedTail + slackBytes) %
                                                                            DDS_REQUEST_RING_BYTES) == false)
            {
                continue;
            }

            // jason: advance progress so the consumer can observe the implicit wrap.
            std::atomic_thread_fence(std::memory_order_release);
            int progress = RingBuffer->Progress[0];
            while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + slackBytes) %
                                                                               DDS_REQUEST_RING_BYTES) == false)
            {
                progress = RingBuffer->Progress[0];
            }
        }
    }

    //
    // Allocate a request buffer object
    // Note: Assuming buffer has been zero'ed
    //
    //
    RequestRingBufferProgressive *AllocateRequestBufferProgressive(BufferT BufferAddress)
    {
        RequestRingBufferProgressive *ringBuffer = (RequestRingBufferProgressive *)BufferAddress;

        //
        // Align the buffer by cache line size
        //
        //
        size_t ringBufferAddress = (size_t)ringBuffer;
        while (ringBufferAddress % DDS_CACHE_LINE_SIZE != 0)
        {
            ringBufferAddress++;
        }
        ringBuffer = (RequestRingBufferProgressive *)ringBufferAddress;

        // jason: initialize the ring to the header phase so the request header is naturally aligned.
        if (ringBuffer->Tail[0] == 0 && ringBuffer->Progress[0] == 0 && ringBuffer->Head[0] == 0)
        {
            ringBuffer->Tail[0] = DDS_REQUEST_RING_HEADER_PHASE;
            ringBuffer->Progress[0] = DDS_REQUEST_RING_HEADER_PHASE;
            ringBuffer->Head[0] = DDS_REQUEST_RING_HEADER_PHASE;
        }

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

        // jason: enforce no-split records by wrapping early when a full record does not fit contiguously.
        if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
        {
            return false;
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

        if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
        {
            return false;
        }

        if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
        {
            return false;
        }
        if (tail + (int)requestBytes > DDS_REQUEST_RING_BYTES)
        {
            if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
            {
                return false;
            }
            tail = RingBuffer->Tail[0];
            head = RingBuffer->Head[0];
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
            if (tail + (int)requestBytes > DDS_REQUEST_RING_BYTES)
            {
                // jason: another producer may have moved tail close to ring end; re-apply wrap rule.
                if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
                {
                    return false;
                }
                tail = RingBuffer->Tail[0];
                head = RingBuffer->Head[0];
            }
        }

        //
        // Now, both tail and space are good
        //
        //
        if (tail + sizeof(FileIOSizeT) + RequestSize <= DDS_REQUEST_RING_BYTES)
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
            memcpy(requestAddress + sizeof(FileIOSizeT), CopyFrom, RequestSize);

            //
            // Increment the progress
            //
            //
            /* int progress = RingBuffer->Progress[0];
            while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
            DDS_REQUEST_RING_BYTES) == false) { progress = RingBuffer->Progress[0];
            } */
            // jason: ensure request payload/header visible before publishing progress to RDMA consumer.
            std::atomic_thread_fence(std::memory_order_release);
            int progress = RingBuffer->Progress[0];
            while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                               DDS_REQUEST_RING_BYTES) == false)
            {
                progress = RingBuffer->Progress[0];
            }
        }
        else
        {
            // jason: no-split mode forbids wrapped request payloads.
            printf("InsertToRequestBufferProgressive: split request is not allowed in no-split mode\n");
            return false;
        }

        return true;
    }

    //
    // Insert a WriteFile request into the request buffer.
    // NOTE: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
    // NOTE: DPU parses request type via OpCode; size-based inference is not used on this branch.
    // Diagnostic: emits request header metadata to correlate with DPU-side parsing.
    // Diagnostic: updates in-memory counters for low-overhead inspection.
    //
    bool InsertWriteFileRequest(RequestRingBufferProgressive *RingBuffer, RequestIdT RequestId, FileIdT FileId,
                                FileSizeT Offset, FileIOSizeT Bytes, BufferT SourceBuffer)
    {
        FileIOSizeT requestSize = sizeof(BuffMsgF2BReqHeader) + Bytes;
        FileIOSizeT logicalRequestBytes = sizeof(FileIOSizeT) + requestSize;
        FileIOSizeT requestBytes = logicalRequestBytes;
        FileIOSizeT headerBytes = sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader);
        // jason: wrap to the phased offset and verify the header start is naturally aligned.
        const FileIOSizeT headerAlign = (FileIOSizeT)DDS_ALIGNOF(BuffMsgF2BReqHeader);
        FileIOSizeT alignment = (FileIOSizeT)DDS_ALIGNOF(BuffMsgF2BReqHeader);
        if (requestBytes % alignment != 0)
        {
            requestBytes += (alignment - (requestBytes % alignment));
        }
        // jason: enforce no-split records by wrapping early when a full request record does not fit.
        if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
        {
            return false;
        }
        {
            int preparedTail = RingBuffer->Tail[0];
            RingSizeT preparedTailSpace = DDS_REQUEST_RING_BYTES - preparedTail;
            if (preparedTailSpace < requestBytes ||
                ((preparedTail + (int)sizeof(FileIOSizeT)) % (int)headerAlign) != 0)
            {
                fprintf(
                    stderr,
                    "%s [error]: request record not contiguous/aligned (tail=%d tailSpace=%u requestBytes=%u align=%u)\n",
                    __func__, preparedTail, (unsigned)preparedTailSpace, (unsigned)requestBytes, (unsigned)headerAlign);
                return false;
            }
            assert(((preparedTail + (int)sizeof(FileIOSizeT)) % (int)headerAlign) == 0);
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

        if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
        {
            return false;
        }

        if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
        {
            return false;
        }
        if (tail + (int)requestBytes > DDS_REQUEST_RING_BYTES)
        {
            if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
            {
                return false;
            }
            tail = RingBuffer->Tail[0];
            head = RingBuffer->Head[0];
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
            if (tail + (int)requestBytes > DDS_REQUEST_RING_BYTES)
            {
                if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
                {
                    return false;
                }
                tail = RingBuffer->Tail[0];
                head = RingBuffer->Head[0];
            }
        }

        //
        // Now, both tail and space are good
        //
        //
        if (tail + sizeof(FileIOSizeT) + requestSize <= DDS_REQUEST_RING_BYTES)
        {
            char *requestAddress = &RingBuffer->Buffer[tail];
            // jason: request start is phased so the header (after the size field) is naturally aligned.
            assert(((tail + (int)sizeof(FileIOSizeT)) % (int)DDS_ALIGNOF(BuffMsgF2BReqHeader)) == 0);

            //
            // Write the number of bytes in this request
            //
            //
            // jason: publish the logical request size; padding is implicit via phased advancement.
            *((FileIOSizeT *)requestAddress) = logicalRequestBytes;

            //
            // Write the request
            //
            //
            BuffMsgF2BReqHeader *header = (BuffMsgF2BReqHeader *)(requestAddress + sizeof(FileIOSizeT));
            // Original header population preserved for reference.
            // header->RequestId = RequestId;
            // header->FileId = FileId;
            // header->Offset = Offset;
            // header->Bytes = Bytes;
            // jason: Include opcode/flags for explicit request type parsing on the DPU.
            header->OpCode = BUFF_MSG_F2B_REQ_OP_WRITE;
            header->Flags = 0;
            header->RequestId = RequestId;
            header->FileId = FileId;
            header->Offset = Offset;
            header->Bytes = Bytes;
            // jason: non-read2 uses Bytes for both logical and output sizes.
            header->BufferBytes = Bytes;
            // jason: RingPadding is a structural pad; always zero it for determinism.
            header->RingPadding = 0;
            memcpy(requestAddress + sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader), SourceBuffer, Bytes);

#if DDS_RING_DIAGNOSTICS
            // Diagnostic: log request publication state for correlation with DPU parsing.
            unsigned long long diagCount = g_requestDiagCounter.fetch_add(1, std::memory_order_relaxed);
            if (DDS_RING_DIAG_LOG_EVERY > 0 && (diagCount % DDS_RING_DIAG_LOG_EVERY) == 0)
            {
                int diagProgress = RingBuffer->Progress[0];
                fprintf(stderr,
                        "RingDiag HostWrite: reqId=%u fileId=%u offset=%llu bytes=%u tail=%d head=%d progress=%d "
                        "requestBytes=%u headerBytes=%u\n",
                        (unsigned)RequestId, (unsigned)FileId, (unsigned long long)Offset, (unsigned)Bytes, tail, head,
                        diagProgress, (unsigned)requestBytes, (unsigned)headerBytes);
            }
            // Diagnostic: record metadata without printing for low-overhead inspection.
            RecordHostWriteDiag(RequestId, FileId, Offset, Bytes, tail, head, RingBuffer->Progress[0], requestBytes,
                                headerBytes, false);
#endif

            //
            // Increment the progress
            //
            //
            /* int progress = RingBuffer->Progress[0];
            while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
            DDS_REQUEST_RING_BYTES) == false) { progress = RingBuffer->Progress[0];
            } */
            // jason: ensure request payload/header visible before publishing progress to RDMA consumer.
            std::atomic_thread_fence(std::memory_order_release);
            int progress = RingBuffer->Progress[0];
            while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                               DDS_REQUEST_RING_BYTES) == false)
            {
                progress = RingBuffer->Progress[0];
            }
        }
        else
        {
            // jason: no-split mode forbids wrapped request payloads.
            printf("InsertWriteFileRequest: split request is not allowed in no-split mode\n");
            return false;
        }

        return true;
    }

    //
    // Insert a WriteFileGather request into the request buffer.
    // Updated: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
    // NOTE: DPU parses request type via OpCode; size-based inference is not used on this branch.
    //
    bool InsertWriteFileGatherRequest(RequestRingBufferProgressive *RingBuffer, RequestIdT RequestId, FileIdT FileId,
                                      FileSizeT Offset, FileIOSizeT Bytes, BufferT *SourceBufferArray)
    {
        FileIOSizeT requestSize = sizeof(BuffMsgF2BReqHeader) + Bytes;
        FileIOSizeT logicalRequestBytes = sizeof(FileIOSizeT) + requestSize;
        FileIOSizeT requestBytes = logicalRequestBytes;

        // jason: wrap to the phased offset and verify the header start is naturally aligned.
        const FileIOSizeT headerAlign = (FileIOSizeT)DDS_ALIGNOF(BuffMsgF2BReqHeader);
        FileIOSizeT alignment = (FileIOSizeT)DDS_ALIGNOF(BuffMsgF2BReqHeader);
        if (requestBytes % alignment != 0)
        {
            requestBytes += (alignment - (requestBytes % alignment));
        }
        // jason: enforce no-split records by wrapping early when a full request record does not fit.
        if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
        {
            return false;
        }
        {
            int preparedTail = RingBuffer->Tail[0];
            RingSizeT preparedTailSpace = DDS_REQUEST_RING_BYTES - preparedTail;
            if (preparedTailSpace < requestBytes ||
                ((preparedTail + (int)sizeof(FileIOSizeT)) % (int)headerAlign) != 0)
            {
                fprintf(
                    stderr,
                    "%s [error]: request record not contiguous/aligned (tail=%d tailSpace=%u requestBytes=%u align=%u)\n",
                    __func__, preparedTail, (unsigned)preparedTailSpace, (unsigned)requestBytes, (unsigned)headerAlign);
                return false;
            }
            assert(((preparedTail + (int)sizeof(FileIOSizeT)) % (int)headerAlign) == 0);
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

        if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
        {
            return false;
        }

        if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
        {
            return false;
        }
        if (tail + (int)requestBytes > DDS_REQUEST_RING_BYTES)
        {
            if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
            {
                return false;
            }
            tail = RingBuffer->Tail[0];
            head = RingBuffer->Head[0];
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
            if (tail + (int)requestBytes > DDS_REQUEST_RING_BYTES)
            {
                if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
                {
                    return false;
                }
                tail = RingBuffer->Tail[0];
                head = RingBuffer->Head[0];
            }
        }

        //
        // Now, both tail and space are good
        //
        //
        if (tail + sizeof(FileIOSizeT) + requestSize <= DDS_REQUEST_RING_BYTES)
        {
            char *requestAddress = &RingBuffer->Buffer[tail];
            // jason: request start is phased so the header (after the size field) is naturally aligned.
            assert(((tail + (int)sizeof(FileIOSizeT)) % (int)DDS_ALIGNOF(BuffMsgF2BReqHeader)) == 0);

            //
            // Write the number of bytes in this request
            //
            //
            // jason: publish the logical request size; padding is implicit via phased advancement.
            *((FileIOSizeT *)requestAddress) = logicalRequestBytes;

            //
            // Write the request
            //
            //
            BuffMsgF2BReqHeader *header = (BuffMsgF2BReqHeader *)(requestAddress + sizeof(FileIOSizeT));
            // Original header population preserved for reference.
            // header->RequestId = RequestId;
            // header->FileId = FileId;
            // header->Offset = Offset;
            // header->Bytes = Bytes;
            // jason: Include opcode/flags for explicit request type parsing on the DPU.
            header->OpCode = BUFF_MSG_F2B_REQ_OP_WRITE_GATHER;
            header->Flags = 0;
            header->RequestId = RequestId;
            header->FileId = FileId;
            header->Offset = Offset;
            header->Bytes = Bytes;
            // jason: non-read2 uses Bytes for both logical and output sizes.
            header->BufferBytes = Bytes;
            // jason: RingPadding is a structural pad; always zero it for determinism.
            header->RingPadding = 0;

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
            /* int progress = RingBuffer->Progress[0];
            while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
            DDS_REQUEST_RING_BYTES) == false) { progress = RingBuffer->Progress[0];
            } */
            // jason: ensure request payload/header visible before publishing progress to RDMA consumer.
            std::atomic_thread_fence(std::memory_order_release);
            int progress = RingBuffer->Progress[0];
            while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                               DDS_REQUEST_RING_BYTES) == false)
            {
                progress = RingBuffer->Progress[0];
            }
        }
        else
        {
            // jason: no-split mode forbids wrapped request payloads.
            printf("InsertWriteFileGatherRequest: split request is not allowed in no-split mode\n");
            return false;
        }

        return true;
    }

    //
    // Copy a request payload into the ring buffer, handling wrap-around.
    // The payload starts at payloadOffset (ring-relative), after the header.
    //
    // jason: retained helper for legacy split-payload request paths.
#if 0
    static inline void CopyRequestPayloadWithWrap(RequestRingBufferProgressive *RingBuffer, int payloadOffset,
                                                  const void *payload, FileIOSizeT payloadBytes)
    {
        if (!payload || payloadBytes == 0)
        {
            return;
        }
        int bytesToEnd = DDS_REQUEST_RING_BYTES - payloadOffset;
        if ((int)payloadBytes <= bytesToEnd)
        {
            memcpy(&RingBuffer->Buffer[payloadOffset], payload, (size_t)payloadBytes);
            return;
        }
        // jason: payload wraps; copy tail then head.
        memcpy(&RingBuffer->Buffer[payloadOffset], payload, (size_t)bytesToEnd);
        memcpy(&RingBuffer->Buffer[0], (const char *)payload + bytesToEnd, (size_t)payloadBytes - (size_t)bytesToEnd);
    }
#endif

    //
    // Insert a ReadFile/ReadFileScatter/ReadFile2 request into the request buffer.
    // Updated: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
    // Updated: read2 encodes stage descriptors (size/input offset/input length) as payload.
    // NOTE: DPU parses request type via OpCode; read2 stage descriptors are carried as payload.
    //
    bool InsertReadRequest(RequestRingBufferProgressive *RingBuffer, RequestIdT RequestId, FileIdT FileId,
                           FileSizeT Offset, FileIOSizeT LogicalBytes, FileIOSizeT OutputBytes,
                           const FileIOSizeT *StageSizes, const FileIOSizeT *StageInputOffsets,
                           const FileIOSizeT *StageInputLengths, uint16_t StageCount, bool IsRead2)
    {
        FileIOSizeT stagePayload[6] = {(FileIOSizeT)0, (FileIOSizeT)0, (FileIOSizeT)0,
                                       (FileIOSizeT)0, (FileIOSizeT)0, (FileIOSizeT)0};
        const FileIOSizeT *stagePayloadPtr = nullptr;
        // Original request sizing preserved for reference.
        // FileIOSizeT requestSize = sizeof(BuffMsgF2BReqHeader);
        FileIOSizeT stagePayloadBytes =
            IsRead2 ? (FileIOSizeT)(3u * (uint32_t)StageCount) * (FileIOSizeT)sizeof(FileIOSizeT) : 0;
        FileIOSizeT requestSize = (FileIOSizeT)sizeof(BuffMsgF2BReqHeader) + stagePayloadBytes;
        FileIOSizeT logicalRequestBytes = sizeof(FileIOSizeT) + requestSize;
        FileIOSizeT requestBytes = logicalRequestBytes;
        // jason: cache the header alignment used by phased request ring checks below.
        FileIOSizeT headerAlign = (FileIOSizeT)DDS_ALIGNOF(BuffMsgF2BReqHeader);
        FileIOSizeT alignment = headerAlign;
        if (OutputBytes == 0 && LogicalBytes > 0)
        {
            // jason: guard legacy callers or default output size for non-read2.
            OutputBytes = LogicalBytes;
        }
        if (!IsRead2)
        {
            // jason: non-read2 must not carry a payload.
            assert(StageSizes == nullptr);
            assert(StageInputOffsets == nullptr);
            assert(StageInputLengths == nullptr);
            assert(StageCount == 0);
        }
        else
        {
            // jason: read2 supports one or two stages (decrypt, optional decompress).
            if (!StageSizes || !StageInputOffsets || !StageInputLengths || StageCount < 1 || StageCount > 2)
            {
                return false;
            }
            assert(StageSizes != nullptr);
            assert(StageInputOffsets != nullptr);
            assert(StageInputLengths != nullptr);
            assert(StageCount <= 2);
            FileIOSizeT sourceBytes = LogicalBytes;
            for (uint16_t stageIndex = 0; stageIndex < StageCount; stageIndex++)
            {
                if (StageInputOffsets[stageIndex] > sourceBytes || StageInputLengths[stageIndex] > sourceBytes ||
                    StageInputLengths[stageIndex] > sourceBytes - StageInputOffsets[stageIndex])
                {
                    return false;
                }
                sourceBytes = StageSizes[stageIndex];
            }
            FileIOSizeT finalStageBytes = StageSizes[StageCount - 1];
            // Original: OutputBytes supplied directly by caller.
            // jason: enforce OutputBytes == final stage size for consistency.
            OutputBytes = finalStageBytes;
            // jason: stage payload layout is [sizes(stageCount), offsets(stageCount), lengths(stageCount)].
            for (uint16_t stageIndex = 0; stageIndex < StageCount; stageIndex++)
            {
                stagePayload[stageIndex] = StageSizes[stageIndex];
                stagePayload[StageCount + stageIndex] = StageInputOffsets[stageIndex];
                stagePayload[(2 * StageCount) + stageIndex] = StageInputLengths[stageIndex];
            }
            stagePayloadPtr = stagePayload;
        }
        // jason: read2 is the only path allowed to vary output bytes from logical bytes.
        assert(IsRead2 || OutputBytes == LogicalBytes);
        if (requestBytes % alignment != 0)
        {
            requestBytes += (alignment - (requestBytes % alignment));
        }

        // jason: enforce no-split records by wrapping early when a full request record does not fit.
        if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
        {
            return false;
        }
        {
            int preparedTail = RingBuffer->Tail[0];
            RingSizeT preparedTailSpace = DDS_REQUEST_RING_BYTES - preparedTail;
            if (preparedTailSpace < requestBytes ||
                ((preparedTail + (int)sizeof(FileIOSizeT)) % (int)headerAlign) != 0)
            {
                fprintf(
                    stderr,
                    "%s [error]: request record not contiguous/aligned (tail=%d tailSpace=%u requestBytes=%u align=%u)\n",
                    __func__, preparedTail, (unsigned)preparedTailSpace, (unsigned)requestBytes, (unsigned)headerAlign);
                return false;
            }
            assert(((preparedTail + (int)sizeof(FileIOSizeT)) % (int)headerAlign) == 0);
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

        if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT)
        {
            return false;
        }

        if (requestBytes > DDS_REQUEST_RING_BYTES - distance)
        {
            return false;
        }
        if (tail + (int)requestBytes > DDS_REQUEST_RING_BYTES)
        {
            if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
            {
                return false;
            }
            tail = RingBuffer->Tail[0];
            head = RingBuffer->Head[0];
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
            if (tail + (int)requestBytes > DDS_REQUEST_RING_BYTES)
            {
                if (!ImplicitWrapRequestProducer(RingBuffer, requestBytes))
                {
                    return false;
                }
                tail = RingBuffer->Tail[0];
                head = RingBuffer->Head[0];
            }
        }

        //
        // Now, both tail and space are good
        //
        //
        if (tail + sizeof(FileIOSizeT) + requestSize <= DDS_REQUEST_RING_BYTES)
        {
            char *requestAddress = &RingBuffer->Buffer[tail];
            // jason: request start is phased so the header (after the size field) is naturally aligned.
            assert(((tail + (int)sizeof(FileIOSizeT)) % (int)DDS_ALIGNOF(BuffMsgF2BReqHeader)) == 0);

            //
            // Write the number of bytes in this request
            //
            //
            // jason: publish the logical request size; padding is implicit via phased advancement.
            *((FileIOSizeT *)requestAddress) = logicalRequestBytes;

            //
            // Write the request
            //
            //
            BuffMsgF2BReqHeader *header = (BuffMsgF2BReqHeader *)(requestAddress + sizeof(FileIOSizeT));
            // Original header population preserved for reference.
            // header->RequestId = RequestId;
            // header->FileId = FileId;
            // header->Offset = Offset;
            // header->Bytes = Bytes;
            // jason: Include opcode/flags for explicit request type parsing on the DPU.
            header->OpCode = IsRead2 ? BUFF_MSG_F2B_REQ_OP_READ2 : BUFF_MSG_F2B_REQ_OP_READ;
            header->Flags = 0;
            header->RequestId = RequestId;
            header->FileId = FileId;
            header->Offset = Offset;
            header->Bytes = LogicalBytes;
            header->BufferBytes = OutputBytes;
            // jason: RingPadding is a structural pad; always zero it for determinism.
            header->RingPadding = 0;
            if (IsRead2 && stagePayloadBytes)
            {
                // jason: append read2 stage payload immediately after the header.
                char *payloadStart = requestAddress + sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader);
                memcpy(payloadStart, stagePayloadPtr, (size_t)stagePayloadBytes);
            }

            //
            // Increment the progress
            //
            //
            /* int progress = RingBuffer->Progress[0];
            while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
            DDS_REQUEST_RING_BYTES) == false) { progress = RingBuffer->Progress[0];
            } */
            // jason: ensure request payload/header visible before publishing progress to RDMA consumer.
            std::atomic_thread_fence(std::memory_order_release);
            int progress = RingBuffer->Progress[0];
            while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) %
                                                                               DDS_REQUEST_RING_BYTES) == false)
            {
                progress = RingBuffer->Progress[0];
            }
        }
        else
        {
            // jason: no-split mode forbids wrapped request payloads.
            printf("InsertReadRequest: split request is not allowed in no-split mode\n");
            return false;
        }

        return true;
    }

// InsertRead2Request deprecated; consolidated into InsertReadRequest.
#if 0
//
// Insert a Read2 request into the request buffer.
// Updated: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
//
bool
InsertRead2Request(
    RequestRingBufferProgressive* RingBuffer,
    RequestIdT RequestId,
    FileIdT FileId,
    FileSizeT Offset,
    FileIOSizeT LogicalBytes,
    FileIOSizeT OutputBytes
) {
    FileIOSizeT requestSize = sizeof(BuffMsgF2BReqRead2);
    FileIOSizeT headerBytes = sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqRead2);

    // jason: implicit wrap rule: skip tail slack if size+header doesn't fit.
    if (!ImplicitWrapRequestProducer(RingBuffer, headerBytes)) {
        return false;
    }

    //
    // Check if the tail exceeds the allowable advance
    //
    //
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];
    RingSizeT distance = 0;

    if (tail < head) {
        distance = tail + DDS_REQUEST_RING_BYTES - head;
    }
    else {
        distance = tail - head;
    }

    //
    // Append request size to the beginning of the request
    // Align to the header's natural alignment so the DPU can safely read 8-byte fields.
    //
    //
        // Original request sizing preserved for reference.
        // FileIOSizeT requestBytes = sizeof(FileIOSizeT) + requestSize;
        FileIOSizeT logicalRequestBytes = sizeof(FileIOSizeT) + requestSize;
        FileIOSizeT requestBytes = logicalRequestBytes;
        // FileIOSizeT alignment = (FileIOSizeT)DDS_ALIGNOF(BuffMsgF2BReqHeader);
        FileIOSizeT alignment = headerAlign;
    if (requestBytes % alignment != 0) {
        requestBytes += (alignment - (requestBytes % alignment));
    }

    if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT) {
        return false;
    }

    if (requestBytes > DDS_REQUEST_RING_BYTES - distance) {
        return false;
    }

    while (RingBuffer->Tail[0].compare_exchange_weak(tail, (tail + requestBytes) % DDS_REQUEST_RING_BYTES) == false) {
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];

        //
        // Check if the tail exceeds the allowable advance
        //
        //
        tail = RingBuffer->Tail[0];
        head = RingBuffer->Head[0];

        if (tail <= head) {
            distance = tail + DDS_REQUEST_RING_BYTES - head;
        }
        else {
            distance = tail - head;
        }

        if (distance + requestBytes >= RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT) {
            return false;
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
    // Now, both tail and space are good
    //
    //
    if (tail + sizeof(FileIOSizeT) + requestSize <= DDS_REQUEST_RING_BYTES) {
        char* requestAddress = &RingBuffer->Buffer[tail];
        // jason: request start is phased so the header (after the size field) is naturally aligned.
        assert(((tail + (int)sizeof(FileIOSizeT)) % (int)DDS_ALIGNOF(BuffMsgF2BReqHeader)) == 0);

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT*)requestAddress) = requestBytes;

        //
        // Write the request
        //
        //
        BuffMsgF2BReqRead2* header = (BuffMsgF2BReqRead2*)(requestAddress + sizeof(FileIOSizeT));
        // Original header population preserved for reference.
        // header->Base.OpCode = BUFF_MSG_F2B_REQ_OP_READ2;
        // header->Base.Flags = 0;
        // header->Base.RequestId = RequestId;
        // header->Base.FileId = FileId;
        // header->Base.Offset = Offset;
        // header->Base.Bytes = LogicalBytes;
        // header->BufferBytes = OutputBytes;
        // jason: Read2 uses the unified header layout with BufferBytes.
        header->OpCode = BUFF_MSG_F2B_REQ_OP_READ2;
        header->Flags = 0;
        header->RequestId = RequestId;
        header->FileId = FileId;
        header->Offset = Offset;
        header->Bytes = LogicalBytes;
        header->BufferBytes = OutputBytes;
        // jason: RingPadding is a structural pad; always zero it for determinism.
        header->RingPadding = 0;

        //
        // Increment the progress
        //
        //
        /* int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) % DDS_REQUEST_RING_BYTES) == false) {
            progress = RingBuffer->Progress[0];
        } */
        // jason: ensure request payload/header visible before publishing progress to RDMA consumer.
        std::atomic_thread_fence(std::memory_order_release);
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) % DDS_REQUEST_RING_BYTES) == false) {
            progress = RingBuffer->Progress[0];
        }
    }
    else {
        //
        // We need to wrap the buffer around
        //
        //
        printf("InsertRead2Request: wrap around should be unreachable\n");
        RingSizeT remainingBytes = DDS_REQUEST_RING_BYTES - tail - sizeof(FileIOSizeT);
        char* requestAddress1 = &RingBuffer->Buffer[tail];
        // jason: request start is phased so the header (after the size field) is naturally aligned.
        assert(((tail + (int)sizeof(FileIOSizeT)) % (int)DDS_ALIGNOF(BuffMsgF2BReqHeader)) == 0);

        //
        // Write the number of bytes in this request
        //
        //
        *((FileIOSizeT*)requestAddress1) = requestBytes;

        //
        // Write the request; header-split should be prevented by implicit wrap.
        //
        //
        if (remainingBytes < sizeof(BuffMsgF2BReqRead2)) {
            printf("InsertRead2Request: split header should be unreachable\n");
            return false;
        }

        BuffMsgF2BReqRead2* header = (BuffMsgF2BReqRead2*)(requestAddress1 + sizeof(FileIOSizeT));
        // Original header population preserved for reference.
        // header->Base.OpCode = BUFF_MSG_F2B_REQ_OP_READ2;
        // header->Base.Flags = 0;
        // header->Base.RequestId = RequestId;
        // header->Base.FileId = FileId;
        // header->Base.Offset = Offset;
        // header->Base.Bytes = LogicalBytes;
        // header->BufferBytes = OutputBytes;
        // jason: Read2 uses the unified header layout with BufferBytes.
        header->OpCode = BUFF_MSG_F2B_REQ_OP_READ2;
        header->Flags = 0;
        header->RequestId = RequestId;
        header->FileId = FileId;
        header->Offset = Offset;
        header->Bytes = LogicalBytes;
        header->BufferBytes = OutputBytes;
        // jason: RingPadding is a structural pad; always zero it for determinism.
        header->RingPadding = 0;

        //
        // Increment the progress
        //
        //
        /* int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) % DDS_REQUEST_RING_BYTES) == false) {
            progress = RingBuffer->Progress[0];
        } */
        // jason: ensure request payload/header visible before publishing progress to RDMA consumer.
        std::atomic_thread_fence(std::memory_order_release);
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) % DDS_REQUEST_RING_BYTES) == false) {
            progress = RingBuffer->Progress[0];
        }
    }

    return true;
}
#endif

    //
    // Fetch requests from the request buffer
    //
    //
    bool FetchFromRequestBufferProgressive(RequestRingBufferProgressive *RingBuffer, BufferT CopyTo,
                                           FileIOSizeT *RequestSize)
    {
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
        memset(sourceBuffer1, 0, availBytes);

        if (sourceBuffer2)
        {
            memcpy((char *)CopyTo + availBytes, sourceBuffer2, progress);
            memset(sourceBuffer2, 0, progress);
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
    // Reset request ring state for a (re)connect so header phase alignment is enforced.
    //
    void ResetRequestRingBufferProgressive(RequestRingBufferProgressive *RingBuffer)
    {
        if (!RingBuffer)
        {
            return;
        }

        // jason: reset all request ring pointers to the phased offset for header alignment.
        for (size_t i = 0; i < DDS_CACHE_LINE_SIZE_BY_INT; i++)
        {
            RingBuffer->Progress[i].store(DDS_REQUEST_RING_HEADER_PHASE, std::memory_order_relaxed);
            RingBuffer->Tail[i].store(DDS_REQUEST_RING_HEADER_PHASE, std::memory_order_relaxed);
            RingBuffer->Head[i] = DDS_REQUEST_RING_HEADER_PHASE;
        }
    }

    //
    // Reset response ring state for a (re)connect so producer/consumer pointers match backend expectations.
    //
    void ResetResponseRingBufferProgressive(ResponseRingBufferProgressive *RingBuffer)
    {
        if (!RingBuffer)
        {
            return;
        }

        // jason: response rings do not use a header phase; reset to zero to match backend tails.
        for (size_t i = 0; i < DDS_CACHE_LINE_SIZE_BY_INT; i++)
        {
            RingBuffer->Progress[i].store(0, std::memory_order_relaxed);
            RingBuffer->Head[i].store(0, std::memory_order_relaxed);
            RingBuffer->Tail[i] = 0;
        }
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

        // jason: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
        FileIOSizeT headerBytes = (FileIOSizeT)(sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));
        if (ImplicitWrapResponseConsumer(RingBuffer, headerBytes))
        {
            return false;
        }

        // jason: defensive check that the response header (after the size field) is naturally aligned.
        assert(((head + (int)sizeof(FileIOSizeT)) % (int)DDS_ALIGNOF(BuffMsgB2FAckHeader)) == 0);
        FileIOSizeT responseSize = *(FileIOSizeT *)&RingBuffer->Buffer[head];

        if (responseSize == 0)
        {
            // jason: zero size is a no-split wrap marker.
            ConsumeResponseWrapMarker(RingBuffer, head);
            return false;
        }

        //
        // Grab the current head
        //
        //
        while (RingBuffer->Head[0].compare_exchange_weak(head, (head + responseSize) % DDS_RESPONSE_RING_BYTES) ==
               false)
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
                ConsumeResponseWrapMarker(RingBuffer, head);
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
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + responseSize) %
                                                                           DDS_RESPONSE_RING_BYTES) == false)
        {
            progress = RingBuffer->Progress[0];
        }

        return true;
    }

    /**
     * Implicit wrap rule for response consumers:
     * If head space can't hold size+header, advance head/progress by the tail slack and wrap.
     */
    static bool ImplicitWrapResponseConsumer(ResponseRingBufferProgressive *RingBuffer, FileIOSizeT headerBytes)
    {
        int head = RingBuffer->Head[0];
        if (head + (int)headerBytes <= DDS_RESPONSE_RING_BYTES)
        {
            return false;
        }

        FileIOSizeT slackBytes = (FileIOSizeT)(DDS_RESPONSE_RING_BYTES - head);
        int expectedHead = head;
        while (RingBuffer->Head[0].compare_exchange_weak(expectedHead, (expectedHead + slackBytes) %
                                                                           DDS_RESPONSE_RING_BYTES) == false)
        {
            expectedHead = RingBuffer->Head[0];
        }

        IncrementProgress(RingBuffer, slackBytes);
        return true;
    }

    /**
     * Consume an explicit zero-sized wrap marker on the response ring.
     *
     * Used by no-split producer mode where end-of-ring slack is skipped before writing
     * the next contiguous record at ring offset 0.
     */
    static bool ConsumeResponseWrapMarker(ResponseRingBufferProgressive *RingBuffer, int head)
    {
        if (head < 0 || head >= DDS_RESPONSE_RING_BYTES)
        {
            return false;
        }
        if (head == 0)
        {
            // jason: a marker at offset 0 is invalid; avoid a no-op "consume".
            return false;
        }

        FileIOSizeT slackBytes = (FileIOSizeT)(DDS_RESPONSE_RING_BYTES - head);
        if (slackBytes == 0)
        {
            return false;
        }

        int expectedHead = head;
        while (RingBuffer->Head[0].compare_exchange_weak(expectedHead, (expectedHead + slackBytes) %
                                                                           DDS_RESPONSE_RING_BYTES) == false)
        {
            expectedHead = RingBuffer->Head[0];
        }
        IncrementProgress(RingBuffer, slackBytes);
        return true;
    }

    static inline FileIOSizeT ResponseBytesAvailable(ResponseRingBufferProgressive *RingBuffer, int head, int tail)
    {
        if (tail >= head)
        {
            return (FileIOSizeT)(tail - head);
        }
        return (FileIOSizeT)(tail + DDS_RESPONSE_RING_BYTES - head);
    }

    //
    // Fetch a response from the response buffer
    // jason: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
    // Updated: payload parsing honors backend buffer alignment and padding.
    //
    bool FetchResponse(ResponseRingBufferProgressive *RingBuffer, BuffMsgB2FAckHeader **Response,
                       SplittableBufferT *DataBuffer, int PublishedTail)
    {
        // jason: initialize output buffer view defensively to avoid stale split pointers.
        if (DataBuffer)
        {
            DataBuffer->TotalSize = 0;
            DataBuffer->FirstAddr = NULL;
            DataBuffer->FirstSize = 0;
            DataBuffer->SecondAddr = NULL;
        }

        //
        // Check if there is a response at the head
        //
        //
        int tail = PublishedTail;
        int head = RingBuffer->Head[0];
        std::atomic_thread_fence(std::memory_order_acquire);

        if (tail == head)
        {
            return false;
        }

        FileIOSizeT headerBytes = (FileIOSizeT)(sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));

        // jason: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
        if (ImplicitWrapResponseConsumer(RingBuffer, headerBytes))
        {
            return false;
        }

        FileIOSizeT responseSize = *(FileIOSizeT *)&RingBuffer->Buffer[head];
        FileIOSizeT availableBytes = ResponseBytesAvailable(RingBuffer, head, tail);

        if (responseSize == 0)
        {
            ConsumeResponseWrapMarker(RingBuffer, head);
            return false;
        }

        // Updated: parsing is bounded by the CQE-published tail; if the current
        // record still looks incomplete or malformed, leave the ring state
        // unchanged and retry after more completions arrive.
        if (responseSize < headerBytes || responseSize > availableBytes)
        {
            return false;
        }
        DebugPrint("[Debug] Fetching a response: head = %d, response size = %d\n", head, responseSize);

        //
        // Grab the current head
        //
        //
        while (RingBuffer->Head[0].compare_exchange_weak(head, (head + responseSize) % DDS_RESPONSE_RING_BYTES) ==
               false)
        {
            tail = PublishedTail;
            head = RingBuffer->Head[0];
            std::atomic_thread_fence(std::memory_order_acquire);
            responseSize = *(FileIOSizeT *)&RingBuffer->Buffer[head];
            availableBytes = ResponseBytesAvailable(RingBuffer, head, tail);

            if (tail == head)
            {
                return false;
            }

            if (responseSize == 0)
            {
                ConsumeResponseWrapMarker(RingBuffer, head);
                return false;
            }
            if (responseSize < headerBytes || responseSize > availableBytes)
            {
                return false;
            }
        }

        //
        // Now, it's safe to return the response
        //
        //
        // jason: defensive check that the response header (after the size field) is naturally aligned.
        assert(((head + (int)sizeof(FileIOSizeT)) % (int)DDS_ALIGNOF(BuffMsgB2FAckHeader)) == 0);
        *Response = (BuffMsgB2FAckHeader *)(&RingBuffer->Buffer[head + sizeof(FileIOSizeT)]);

        // FileIOSizeT offset = sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader);
        // if (responseSize > offset) {
        //     int dataOffset = (head + offset) % DDS_RESPONSE_RING_BYTES;
        //
        //     DataBuffer->TotalSize = responseSize - offset;
        //     DataBuffer->FirstAddr = &RingBuffer->Buffer[dataOffset];
        //
        //     if (dataOffset + DataBuffer->TotalSize > DDS_RESPONSE_RING_BYTES) {
        //         DataBuffer->FirstSize = DDS_RESPONSE_RING_BYTES - dataOffset;
        //         DataBuffer->SecondAddr = &RingBuffer->Buffer[0];
        //     }
        //     else {
        //         DataBuffer->FirstSize = DataBuffer->TotalSize;
        //         DataBuffer->SecondAddr = NULL;
        //     }
        // }
        // else {
        //     DataBuffer->TotalSize = 0;
        // }
        // jason: Adjust payload start to meet backend buffer alignment (padding between header and payload).
        // FileIOSizeT headerBytes = (FileIOSizeT)(sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));
        // jason: reuse headerBytes computed earlier in this function.
        FileIOSizeT payloadAlign = g_responsePayloadAlign.load(std::memory_order_relaxed);
        if (payloadAlign == 0)
        {
            payloadAlign = 1;
        }
        if (responseSize > headerBytes)
        {
            int dataOffset = (head + (int)headerBytes) % DDS_RESPONSE_RING_BYTES;
            uintptr_t baseAddr = (uintptr_t)&RingBuffer->Buffer[0];
            uintptr_t payloadAddr = baseAddr + (uintptr_t)dataOffset;
            uintptr_t alignedAddr = ((payloadAddr + payloadAlign - 1) / payloadAlign) * payloadAlign;
            FileIOSizeT pad = 0;
            if (alignedAddr < baseAddr + DDS_RESPONSE_RING_BYTES)
            {
                pad = (FileIOSizeT)(alignedAddr - payloadAddr);
                dataOffset = (dataOffset + (int)pad) % DDS_RESPONSE_RING_BYTES;
            }
            else
            {
                // jason: wrap to base; assume ring base is aligned.
                pad = (FileIOSizeT)(DDS_RESPONSE_RING_BYTES - dataOffset);
                dataOffset = 0;
            }

            DataBuffer->TotalSize = responseSize - headerBytes - pad;
            DataBuffer->FirstAddr = &RingBuffer->Buffer[dataOffset];

            if (dataOffset + (int)DataBuffer->TotalSize > DDS_RESPONSE_RING_BYTES)
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
            DataBuffer->FirstAddr = NULL;
            DataBuffer->FirstSize = 0;
            DataBuffer->SecondAddr = NULL;
        }

        return true;
    }

#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
    //
    // Fetch a batch of responses from the response buffer.
    // Updated: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
    //
    //
    bool FetchResponseBatch(ResponseRingBufferProgressive *RingBuffer, SplittableBufferT *Responses, int PublishedTail)
    {
        // jason: initialize output buffer view defensively to avoid stale split pointers.
        if (Responses)
        {
            Responses->TotalSize = 0;
            Responses->FirstAddr = NULL;
            Responses->FirstSize = 0;
            Responses->SecondAddr = NULL;
        }

        //
        // Check if there is a response at the head
        //
        //
        int tail = PublishedTail;
        int head = RingBuffer->Head[0];
        std::atomic_thread_fence(std::memory_order_acquire);

        if (tail == head)
        {
            return false;
        }

        FileIOSizeT headerBytes = (FileIOSizeT)(sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));

        // jason: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
        if (ImplicitWrapResponseConsumer(RingBuffer, headerBytes))
        {
            return false;
        }

        FileIOSizeT responseSize = *(FileIOSizeT *)&RingBuffer->Buffer[head];
        FileIOSizeT availableBytes = ResponseBytesAvailable(RingBuffer, head, tail);

        if (responseSize == 0)
        {
            ConsumeResponseWrapMarker(RingBuffer, head);
            return false;
        }

        // Updated: parsing is bounded by the CQE-published tail; if the current
        // batch still looks incomplete or malformed, leave the ring state
        // unchanged and retry after more completions arrive.
        if (responseSize < headerBytes || responseSize > availableBytes)
        {
            return false;
        }
        DebugPrint("[Debug] Fetching a response batch: head = %d, response size = %d\n", head, responseSize);

        //
        // Grab the current head
        //
        //
        while (RingBuffer->Head[0].compare_exchange_weak(head, (head + responseSize) % DDS_RESPONSE_RING_BYTES) ==
               false)
        {
            tail = PublishedTail;
            head = RingBuffer->Head[0];
            std::atomic_thread_fence(std::memory_order_acquire);
            responseSize = *(FileIOSizeT *)&RingBuffer->Buffer[head];
            availableBytes = ResponseBytesAvailable(RingBuffer, head, tail);

            if (tail == head)
            {
                return false;
            }

            if (responseSize == 0)
            {
                ConsumeResponseWrapMarker(RingBuffer, head);
                return false;
            }
            if (responseSize < headerBytes || responseSize > availableBytes)
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
            Responses->SecondAddr = NULL;
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

        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + ResponseSize) %
                                                                           DDS_RESPONSE_RING_BYTES) == false)
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
                memcpy(&RingBuffer->Buffer[(tail + sizeof(FileIOSizeT)) % DDS_RESPONSE_RING_BYTES],
                       CopyFromList[respIndex], ResponseSizeList[respIndex]);
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

}
