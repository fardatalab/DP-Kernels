#include "ring_buffer.hpp"
#include "sw_device.hpp"

#include "common.hpp"
#include <cstddef>
#include <cstdlib>
#include <unistd.h>
#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#endif
#include <stdio.h>
#include <thread>

#define PROGRESS_CAS_BACKOFF
// #undef PROGRESS_CAS_BACKOFF

#define ATOMIC_BACKOFF_START
// #undef ATOMIC_BACKOFF_START

#if defined(__x86_64__) || defined(_M_X64)
int BackoffDelay = 1;
const int MaxBackoff = 4096; // 4096; //1024;
const int BACKOFF_DELAY_FACTOR = 8;
#else
int BackoffDelay = 1;
const int MaxBackoff = 2048;
const int BACKOFF_DELAY_FACTOR = 8;
#endif

#ifdef ATOMIC_BACKOFF_START
alignas(64) std::atomic<uint> BackoffStart{0};
#else
alignas(64) uint BackoffStart = 0;
#endif
const int BACKOFF_START_WINDOW = 1024; // 1024;
const int BACKOFF_INCREMENT = 64;

#undef DEBUG_RING_BUFFER
#ifdef DEBUG_RING_BUFFER
#define DebugPrint(Fmt, ...) fprintf(stderr, Fmt, __VA_ARGS__)
#else
static inline void DebugPrint(const char *Fmt, ...)
{
}
#endif

// shm_ptr payload;
struct dpm_req_msg msg;

namespace DDS_FrontEnd
{

//
// **************************************************************
// Request buffer implementation
// **************************************************************
//

template <size_t BufferSize, size_t MaxTailAdvancement>
RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *LocalAllocateRequestBufferProgressive()
{
    RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *ringBuffer = NULL;
    posix_memalign((void **)&ringBuffer, CACHE_LINE_SIZE,
                   sizeof(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement>));

    if (ringBuffer == NULL)
    {
        printf("Failed to allocate local ring buffer of size %lu\n", BufferSize);
        return nullptr;
    }
    memset(ringBuffer, 0, BufferSize);
    if ((size_t)ringBuffer % CACHE_LINE_SIZE != 0)
    {
        printf("ring buffer not aligned to cache line\n");
        return nullptr;
    }

    return ringBuffer;
}

template <size_t BufferSize, size_t MaxTailAdvancement>
bool LocalDeallocateRequestBufferProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *ringBuffer)
{
    if (ringBuffer == NULL)
    {
        printf("Failed to deallocate local ring buffer\n");
        return false;
    }
    free(ringBuffer);
    return true;
}

//
// Allocate a request buffer object
// Note: Assuming buffer has been zero'ed
//
//
template <size_t BufferSize, size_t MaxTailAdvancement>
RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *AllocateRequestBufferProgressive(const char *shm_name)
{
    const size_t BUFFER_SIZE = sizeof(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement>);

    // Create or open shared memory object
    int shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1)
    {
        printf("shm_open failed for allocate ring buffer\n");
        return nullptr;
    }

    // Set the size of the shared memory object
    if (ftruncate(shm_fd, BUFFER_SIZE) == -1)
    {
        close(shm_fd);
        shm_unlink(shm_name);
        printf("ftruncate failed for allocate ring buffer\n");
        return nullptr;
    }

    // Map the shared memory object into this process's memory space
    RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *ringBuffer =
        (RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *)mmap(NULL, BUFFER_SIZE, PROT_READ | PROT_WRITE,
                                                                             MAP_SHARED, shm_fd, 0);

    if (ringBuffer == MAP_FAILED)
    {
        close(shm_fd);
        shm_unlink(shm_name);
        return nullptr;
    }
    memset(ringBuffer, 0, sizeof(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement>));

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
template <size_t BufferSize, size_t MaxTailAdvancement>
RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *SetupRequestBufferProgressive(const char *shm_name)
{
    const size_t BUFFER_SIZE = sizeof(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement>);

    // Open shared memory object
    int shm_fd = shm_open(shm_name, O_RDWR, 0666);
    if (shm_fd == -1)
    {
        printf("shm_open failed for setup ring buffer\n");
        return nullptr;
    }

    // Map the shared memory object into this process's memory space
    RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *ringBuffer =
        (RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *)mmap(NULL, BUFFER_SIZE, PROT_READ | PROT_WRITE,
                                                                             MAP_SHARED, shm_fd, 0);

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
template <size_t BufferSize, size_t MaxTailAdvancement>
void DeallocateRequestBufferProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer)
{
    memset(RingBuffer, 0, sizeof(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement>));
}

#if defined(__x86_64__) || defined(_M_X64)
int backoff_delay = 1;
const int max_backoff = 1024;
// #elif defined(__arm__) || defined(__aarch64__)
#else
int backoff_delay = 16;
const int max_backoff = 4096;
#endif

const int thread_backoff_factor = 4; // for use with the thread_id version
//
// Insert a request into the request buffer
//
//
template <size_t BufferSize, size_t MaxTailAdvancement>
bool InsertToRequestBufferProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer,
                                      const BufferT CopyFrom, FileIOSizeT RequestSize)
{
    // int backoff_delay = 1;  // use the global backoff delay, seems to be better (more adaptive?)

    //
    // Check if the tail exceeds the allowable advance
    //
    //
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];
    RingSizeT distance = 0;

    if (tail < head)
    {
        distance = tail + BufferSize - head;
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

    if (requestBytes % sizeof(FileIOSizeT) != 0) [[unlikely]]
    {
        requestBytes += (sizeof(FileIOSizeT) - (requestBytes % sizeof(FileIOSizeT)));
    }

    // Using the MaxTailAdvancement parameter for the maximum tail advancement
    if (distance + requestBytes >= MaxTailAdvancement)
    {
        return false;
    }

    if (requestBytes > BufferSize - distance)
    {
        return false;
    }

    while (RingBuffer->Tail[0].compare_exchange_weak(tail, (tail + requestBytes) % BufferSize) == false)
    {
#ifdef PROGRESS_CAS_BACKOFF
#ifdef ATOMIC_BACKOFF_START
        auto backoff = BackoffStart.fetch_add(BACKOFF_INCREMENT, std::memory_order_acq_rel);
#else
        BackoffStart = BackoffStart + BACKOFF_INCREMENT;
#endif
#ifdef ATOMIC_BACKOFF_START
        for (int i = backoff % BACKOFF_START_WINDOW; i < BackoffDelay; i++)
#else
        for (int i = BackoffStart % BACKOFF_START_WINDOW; i < BackoffDelay; i++)
#endif
        {
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
#elif defined(__arm__) || defined(__aarch64__)
            __asm volatile("isb");
#else
            std::this_thread::yield();
#endif
        }
        BackoffDelay *= BACKOFF_DELAY_FACTOR;
        if (BackoffDelay > MaxBackoff)
            BackoffDelay = MaxBackoff;
#endif
        head = RingBuffer->Head[0];

        if (tail <= head)
        {
            distance = tail + BufferSize - head;
        }
        else
        {
            distance = tail - head;
        }

        if (distance + requestBytes >= MaxTailAdvancement)
        {
            return false;
        }

        //
        // Check space
        //
        //
        if (requestBytes > BufferSize - distance)
        {
            return false;
        }
    }

    //
    // Now, both tail and space are good
    //
    //
    if (tail + sizeof(FileIOSizeT) + RequestSize <= BufferSize) [[likely]]
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
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) % BufferSize) == false)
        {
#ifdef PROGRESS_CAS_BACKOFF
#ifdef ATOMIC_BACKOFF_START
            auto backoff = BackoffStart.fetch_add(BACKOFF_INCREMENT, std::memory_order_relaxed);
#else
            BackoffStart = BackoffStart + BACKOFF_INCREMENT;
#endif

#ifdef ATOMIC_BACKOFF_START
            for (int i = backoff % BACKOFF_START_WINDOW; i < BackoffDelay; i++)
#else
            for (int i = BackoffStart % BACKOFF_START_WINDOW; i < BackoffDelay; i++)
#endif
            {
#if defined(__x86_64__) || defined(_M_X64)
                _mm_pause();
#elif defined(__arm__) || defined(__aarch64__)
                __asm volatile("isb");
#else
                std::this_thread::yield();
#endif
            }
            BackoffDelay *= BACKOFF_DELAY_FACTOR;
            if (BackoffDelay > MaxBackoff)
                BackoffDelay = MaxBackoff;
#endif
            progress = RingBuffer->Progress[0];
        }
    }
    else [[unlikely]]
    {
        //
        // We need to wrap the buffer around
        //
        //
        RingSizeT remainingBytes = BufferSize - tail - sizeof(FileIOSizeT);
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
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) % BufferSize) == false)
        {
            std::this_thread::yield();
            progress = RingBuffer->Progress[0];
        }
    }

    return true;
}

/////////
// instead of arbitrary sized messages, support only struct dpm_req_msg
/////////
template <size_t BufferSize, size_t MaxTailAdvancement>
bool InsertToRequestBufferProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer,
                                      const struct dpm_req_msg *CopyFrom)
{
    // int backoff_delay = 1;  // use the global backoff delay, seems to be better (more adaptive?)

    //
    // Check if the tail exceeds the allowable advance
    //
    //
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];
    RingSizeT distance = 0;

    if (tail < head)
    {
        distance = tail + BufferSize - head;
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
    FileIOSizeT requestBytes = sizeof(FileIOSizeT) + sizeof(struct dpm_req_msg);

    if (requestBytes % sizeof(FileIOSizeT) != 0) [[unlikely]]
    {
        printf("requestBytes mod sizeof(FileIOSizeT) != 0, requestBytes: %lu\n", requestBytes);
        requestBytes += (sizeof(FileIOSizeT) - (requestBytes % sizeof(FileIOSizeT)));
    }

    if (distance + requestBytes >= MaxTailAdvancement)
    {
        return false;
    }

    if (requestBytes > BufferSize - distance)
    {
        return false;
    }

    // Q: will backoff help here? A: exponential backoff helps!! (need tuning probably)
    while (RingBuffer->Tail[0].compare_exchange_weak(tail, (tail + requestBytes) % BufferSize) == false)
    {
        // pause here
        // _mm_pause();

        // exponential backoff
        for (int i = 0; i < backoff_delay; i++)
        {
            // _mm_pause();
            // std::this_thread::sleep_for(std::chrono::nanoseconds(1));
            // std::this_thread::yield();
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
// #elif defined(__arm__) || defined(__aarch64__)
//             __asm volatile("isb");
#else
            std::this_thread::yield();
#endif
        }
        backoff_delay *= 8;
        if (backoff_delay > max_backoff)
            backoff_delay = max_backoff;

        // NOTE: duplicate??
        // NOTE: don't need to load tail again?
        // compiler probably optimized away these...?
        // tail = RingBuffer->Tail[0];
        // head = RingBuffer->Head[0];

        //
        // Check if the tail exceeds the allowable advance
        //
        //
        // tail = RingBuffer->Tail[0]; // tail should be already updated, no??
        head = RingBuffer->Head[0];

        if (tail <= head)
        {
            distance = tail + BufferSize - head;
        }
        else
        {
            distance = tail - head;
        }

        if (distance + requestBytes >= MaxTailAdvancement)
        {
            return false;
        }

        //
        // Check space
        //
        //
        if (requestBytes > BufferSize - distance)
        {
            return false;
        }
    }

    //
    // Now, both tail and space are good
    //
    //
    if (tail + sizeof(FileIOSizeT) + sizeof(struct dpm_req_msg) <= BufferSize)
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

        // use assignment instead of memcpy
        // memcpy(requestAddress + sizeof(FileIOSizeT), CopyFrom, RequestSize);
        ((struct dpm_req_msg *)(requestAddress + sizeof(FileIOSizeT)))->type = CopyFrom->type;
        ((struct dpm_req_msg *)(requestAddress + sizeof(FileIOSizeT)))->ptr = CopyFrom->ptr;

        //
        // Increment the progress
        //
        //
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) % BufferSize) == false)
        {
            std::this_thread::yield();
            progress = RingBuffer->Progress[0];
        }
    }
    else
    {
        //
        // We need to wrap the buffer around
        //
        //
        RingSizeT remainingBytes = BufferSize - tail - sizeof(FileIOSizeT);
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
        memcpy(requestAddress2, (const char *)CopyFrom + remainingBytes, sizeof(struct dpm_req_msg) - remainingBytes);

        //
        // Increment the progress
        //
        //
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) % BufferSize) == false)
        {
            std::this_thread::yield();
            progress = RingBuffer->Progress[0];
        }
    }

    return true;
}

template <size_t BufferSize, size_t MaxTailAdvancement>
bool InsertToRequestBufferProgressive(int thread_id,
                                      RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer,
                                      const BufferT CopyFrom, FileIOSizeT RequestSize)
{
    int backoff_delay = 8;
    //
    // Check if the tail exceeds the allowable advance
    //
    //
    int tail = RingBuffer->Tail[0];
    int head = RingBuffer->Head[0];
    RingSizeT distance = 0;

    if (tail < head)
    {
        distance = tail + BufferSize - head;
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

    if (distance + requestBytes >= MaxTailAdvancement)
    {
        return false;
    }

    if (requestBytes > BufferSize - distance)
    {
        return false;
    }

    // Q: will backoff help here? A: exponential backoff helps!! (need tuning probably)
    while (RingBuffer->Tail[0].compare_exchange_weak(tail, (tail + requestBytes) % BufferSize) == false)
    {
        // pause here
        // _mm_pause();

        // exponential backoff
        int thread_backoff = thread_id * thread_backoff_factor;
        /* if (thread_backoff > 256) {
            // TODO: maybe cap it to some value
        } */
        for (int i = 0; i < thread_backoff + backoff_delay; i++)
        {
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause();
// #elif defined(__arm__) || defined(__aarch64__)
//             __asm volatile("isb");
#else
            std::this_thread::yield();
#endif
        }
        backoff_delay *= 8;
        if (backoff_delay > max_backoff)
            backoff_delay = max_backoff;

        // NOTE: duplicate??
        // NOTE: don't need to load tail again?
        // compiler probably optimized away these...?
        // tail = RingBuffer->Tail[0];
        // head = RingBuffer->Head[0];

        //
        // Check if the tail exceeds the allowable advance
        //
        //
        // tail = RingBuffer->Tail[0]; // tail should be already updated, no??
        head = RingBuffer->Head[0];

        if (tail <= head)
        {
            distance = tail + BufferSize - head;
        }
        else
        {
            distance = tail - head;
        }

        if (distance + requestBytes >= MaxTailAdvancement)
        {
            return false;
        }

        //
        // Check space
        //
        //
        if (requestBytes > BufferSize - distance)
        {
            return false;
        }
    }

    //
    // Now, both tail and space are good
    //
    //
    if (tail + sizeof(FileIOSizeT) + RequestSize <= BufferSize)
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
        int progress = RingBuffer->Progress[0];
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) % BufferSize) == false)
        {
            std::this_thread::yield();
            progress = RingBuffer->Progress[0];
        }
    }
    else
    {
        //
        // We need to wrap the buffer around
        //
        //
        RingSizeT remainingBytes = BufferSize - tail - sizeof(FileIOSizeT);
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
        while (RingBuffer->Progress[0].compare_exchange_weak(progress, (progress + requestBytes) % BufferSize) == false)
        {
            std::this_thread::yield();
            progress = RingBuffer->Progress[0];
        }
    }

    return true;
}

//
// Fetch requests from the request buffer
//
//
template <size_t BufferSize, size_t MaxTailAdvancement>
bool FetchFromRequestBufferProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer,
                                       BufferT CopyTo, FileIOSizeT *RequestSize)
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
        availBytes = BufferSize - head;
        *RequestSize = availBytes + progress;
        sourceBuffer1 = &RingBuffer->Buffer[head];
        sourceBuffer2 = &RingBuffer->Buffer[0];
    }

    memcpy(CopyTo, sourceBuffer1, availBytes);
    // TODO: no need to memset here?
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
// Wait for completion
//
//
template <size_t BufferSize, size_t MaxTailAdvancement>
bool CheckForRequestCompletionProgressive(RequestRingBufferProgressive<BufferSize, MaxTailAdvancement> *RingBuffer)
{
    int head = RingBuffer->Head[0];
    int tail = RingBuffer->Tail[0];

    return head == tail;
}

// Add explicit template instantiations for the default sizes
// template RequestRingBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
//     *LocalAllocateRequestBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>();
// template bool LocalDeallocateRequestBufferProgressive<DDS_REQUEST_RING_BYTES,
//                                                       RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>(
//     RequestRingBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT> *ringBuffer);

template RequestRingBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
    *AllocateRequestBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>(
        const char *shm_name);
template RequestRingBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>
    *SetupRequestBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>(
        const char *shm_name);
template void DeallocateRequestBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>(
    RequestRingBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT> *RingBuffer);
template bool InsertToRequestBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>(
    RequestRingBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT> *RingBuffer,
    const BufferT CopyFrom, FileIOSizeT RequestSize);
template bool InsertToRequestBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>(
    int thread_id,
    RequestRingBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT> *RingBuffer,
    const BufferT CopyFrom, FileIOSizeT RequestSize);
template bool InsertToRequestBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>(
    RequestRingBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT> *RingBuffer,
    const struct dpm_req_msg *CopyFrom);
template bool FetchFromRequestBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>(
    RequestRingBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT> *RingBuffer,
    BufferT CopyTo, FileIOSizeT *RequestSize);
template bool CheckForRequestCompletionProgressive<DDS_REQUEST_RING_BYTES,
                                                   RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT>(
    RequestRingBufferProgressive<DDS_REQUEST_RING_BYTES, RING_BUFFER_REQUEST_MAXIMUM_TAIL_ADVANCEMENT> *RingBuffer);

// Add explicit template instantiations for the sw kernels queue version
template bool InsertToRequestBufferProgressive<SW_KERNELS_QUEUE_SIZE, SW_KERNELS_QUEUE_MAX_TAIL_ADVANCEMENT>(
    RequestRingBufferProgressive<SW_KERNELS_QUEUE_SIZE, SW_KERNELS_QUEUE_MAX_TAIL_ADVANCEMENT> *RingBuffer,
    const BufferT CopyFrom, FileIOSizeT RequestSize);

template RequestRingBufferProgressive<SW_KERNELS_QUEUE_SIZE, SW_KERNELS_QUEUE_MAX_TAIL_ADVANCEMENT>
    *LocalAllocateRequestBufferProgressive<SW_KERNELS_QUEUE_SIZE, SW_KERNELS_QUEUE_MAX_TAIL_ADVANCEMENT>();

template bool LocalDeallocateRequestBufferProgressive<SW_KERNELS_QUEUE_SIZE, SW_KERNELS_QUEUE_MAX_TAIL_ADVANCEMENT>(
    RequestRingBufferProgressive<SW_KERNELS_QUEUE_SIZE, SW_KERNELS_QUEUE_MAX_TAIL_ADVANCEMENT> *ringBuffer);

template bool FetchFromRequestBufferProgressive<SW_KERNELS_QUEUE_SIZE, SW_KERNELS_QUEUE_MAX_TAIL_ADVANCEMENT>(
    RequestRingBufferProgressive<SW_KERNELS_QUEUE_SIZE, SW_KERNELS_QUEUE_MAX_TAIL_ADVANCEMENT> *RingBuffer,
    BufferT CopyTo, FileIOSizeT *RequestSize);

} // namespace DDS_FrontEnd