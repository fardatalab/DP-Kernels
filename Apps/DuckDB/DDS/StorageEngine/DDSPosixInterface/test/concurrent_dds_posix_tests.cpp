#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "DDSPosix.h"

// Test tracking variables
static int totalTests = 0;
static int passedTests = 0;

// Simple test assertion macros
#define TEST_START(name)                                                                                               \
    do                                                                                                                 \
    {                                                                                                                  \
        std::cout << "Running test: " << name << "... \n";                                                             \
        totalTests++;                                                                                                  \
    } while (0)

#define TEST_ASSERT(condition)                                                                                         \
    do                                                                                                                 \
    {                                                                                                                  \
        if (!(condition))                                                                                              \
        {                                                                                                              \
            std::cout << "FAILED at line " << __LINE__ << ": " << #condition << std::endl;                             \
            return false;                                                                                              \
        }                                                                                                              \
    } while (0)

#define TEST_END()                                                                                                     \
    do                                                                                                                 \
    {                                                                                                                  \
        std::cout << "PASSED" << std::endl;                                                                            \
        passedTests++;                                                                                                 \
        return true;                                                                                                   \
    } while (0)

/**
 * Fill a buffer with a deterministic byte value.
 */
static void FillBuffer(std::vector<unsigned char> &buffer, unsigned char value)
{
    std::fill(buffer.begin(), buffer.end(), value);
}

/**
 * Write a deterministic pattern to a file using pwrite.
 *
 * Uses aligned chunk sizes so pread does not need extra padding.
 */
static bool WritePatternFile(int fd, size_t chunkSize, size_t numChunks)
{
    std::vector<unsigned char> buffer(chunkSize);
    for (size_t i = 0; i < numChunks; ++i)
    {
        FillBuffer(buffer, static_cast<unsigned char>(i));
        ssize_t written = DDSPosix::pwrite(fd, buffer.data(), chunkSize, static_cast<off_t>(i * chunkSize));
        if (written != static_cast<ssize_t>(chunkSize))
        {
            printf("Write failed at chunk %zu, written=%zd, errno=%d\n", i, written, errno);
            return false;
        }
    }
    return true;
}

/**
 * Verify that all bytes in the buffer match the expected value.
 */
static bool VerifyBuffer(const std::vector<unsigned char> &buffer, unsigned char expected)
{
    for (size_t i = 0; i < buffer.size(); ++i)
    {
        if (buffer[i] != expected)
        {
            return false;
        }
    }
    return true;
}

/**
 * Verify buffer content for reads that may span multiple chunks.
 *
 * The file is written such that each chunkSize block is filled with the byte value == chunkIndex.
 * For an arbitrary offset, expected value for byte i is floor((offset + i) / chunkSize).
 */
static bool VerifyBufferSpanningChunks(const std::vector<unsigned char> &buffer, size_t chunkSize, off_t offset)
{
    size_t firstMismatchIndex = buffer.size();
    size_t mismatchCount = 0;

    for (size_t i = 0; i < buffer.size(); ++i)
    {
        size_t chunkIndex = static_cast<size_t>((offset + static_cast<off_t>(i)) / static_cast<off_t>(chunkSize));
        unsigned char expected = static_cast<unsigned char>(chunkIndex);
        unsigned char actual = buffer[i];

        if (actual != expected)
        {
            if (firstMismatchIndex == buffer.size())
            {
                firstMismatchIndex = i;
            }
            ++mismatchCount;
        }
    }

    if (mismatchCount != 0)
    {
        size_t firstChunkIndex =
            static_cast<size_t>((offset + static_cast<off_t>(firstMismatchIndex)) / static_cast<off_t>(chunkSize));
        unsigned char expectedFirst = static_cast<unsigned char>(firstChunkIndex);
        unsigned char actualFirst = buffer[firstMismatchIndex];

        const long long fileByteIndex = static_cast<long long>(offset + static_cast<off_t>(firstMismatchIndex));

        printf("VerifyBufferSpanningChunks mismatch: firstMismatch(bufferIdx)=%zu fileByteIdx=%lld "
               "expected=0x%02x actual=0x%02x mismatchedBytes=%zu totalBytes=%zu (offset=%lld chunkSize=%zu)\n",
               firstMismatchIndex, fileByteIndex, static_cast<unsigned int>(expectedFirst),
               static_cast<unsigned int>(actualFirst), mismatchCount, buffer.size(), static_cast<long long>(offset),
               chunkSize);
        return false;
    }

    return true;
}

/**
 * Pick a random value in [minValue, maxValue] that is not aligned to alignment when possible.
 */
static size_t PickNonAlignedValue(std::mt19937 &rng, size_t minValue, size_t maxValue, size_t alignment)
{
    if (minValue > maxValue)
    {
        return minValue;
    }

    std::uniform_int_distribution<size_t> dist(minValue, maxValue);
    size_t value = dist(rng);

    if (alignment != 0 && (value % alignment) == 0)
    {
        if (value + 1 <= maxValue)
        {
            value += 1;
        }
        else if (value > minValue)
        {
            value -= 1;
        }
    }

    return value;
}

/**
 * Verify buffer prefix content for reads that may span multiple chunks.
 *
 * The file is written such that each chunkSize block is filled with the byte value == chunkIndex.
 * For an arbitrary offset, expected value for byte i is floor((offset + i) / chunkSize).
 */
static bool VerifyBufferPrefixSpanningChunks(const std::vector<unsigned char> &buffer, size_t prefixBytes,
                                              size_t chunkSize, off_t offset)
{
    size_t firstMismatchIndex = prefixBytes;
    size_t mismatchCount = 0;

    for (size_t i = 0; i < prefixBytes; ++i)
    {
        size_t chunkIndex = static_cast<size_t>((offset + static_cast<off_t>(i)) / static_cast<off_t>(chunkSize));
        unsigned char expected = static_cast<unsigned char>(chunkIndex);
        unsigned char actual = buffer[i];

        if (actual != expected)
        {
            if (firstMismatchIndex == prefixBytes)
            {
                firstMismatchIndex = i;
            }
            ++mismatchCount;
        }
    }

    if (mismatchCount != 0)
    {
        size_t firstChunkIndex =
            static_cast<size_t>((offset + static_cast<off_t>(firstMismatchIndex)) / static_cast<off_t>(chunkSize));
        unsigned char expectedFirst = static_cast<unsigned char>(firstChunkIndex);
        unsigned char actualFirst = buffer[firstMismatchIndex];

        const long long fileByteIndex = static_cast<long long>(offset + static_cast<off_t>(firstMismatchIndex));

        printf("VerifyBufferPrefixSpanningChunks mismatch: firstMismatch(bufferIdx)=%zu fileByteIdx=%lld "
               "expected=0x%02x actual=0x%02x mismatchedBytes=%zu totalBytes=%zu (offset=%lld chunkSize=%zu)\n",
               firstMismatchIndex, fileByteIndex, static_cast<unsigned int>(expectedFirst),
               static_cast<unsigned int>(actualFirst), mismatchCount, prefixBytes, static_cast<long long>(offset),
               chunkSize);
        return false;
    }

    return true;
}

/**
 * Verify that all bytes in a range match the expected value, with debug output on mismatch.
 */
static bool VerifyBufferRangeValueWithDebug(const std::vector<unsigned char> &buffer, size_t start, size_t length,
                                            unsigned char expected, const char *label)
{
    if (length == 0)
    {
        return true;
    }
    if (start >= buffer.size())
    {
        printf("VerifyBufferRangeValueWithDebug out of range: start=%zu length=%zu total=%zu label=%s\n", start, length,
               buffer.size(), label ? label : "range");
        return false;
    }
    if (start + length > buffer.size())
    {
        printf("VerifyBufferRangeValueWithDebug truncated range: start=%zu length=%zu total=%zu label=%s\n", start,
               length, buffer.size(), label ? label : "range");
        length = buffer.size() - start;
    }

    for (size_t i = 0; i < length; ++i)
    {
        unsigned char actual = buffer[start + i];
        if (actual != expected)
        {
            printf("VerifyBufferRangeValueWithDebug mismatch: index=%zu expected=0x%02x actual=0x%02x "
                   "start=%zu length=%zu label=%s\n",
                   start + i, static_cast<unsigned int>(expected), static_cast<unsigned int>(actual), start, length,
                   label ? label : "range");
            return false;
        }
    }
    return true;
}

/**
 * Verify that all bytes in the buffer match the expected value, with debug output on mismatch.
 */
static bool VerifyBufferValueWithDebug(const std::vector<unsigned char> &buffer, unsigned char expected)
{
    return VerifyBufferRangeValueWithDebug(buffer, 0, buffer.size(), expected, "buffer");
}

static bool ReadFileBytes(const char *path, std::vector<unsigned char> &out)
{
    out.clear();
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in.good())
    {
        printf("Failed to open fixture file: %s\n", path);
        return false;
    }

    std::streamsize size = in.tellg();
    if (size < 0)
    {
        printf("Failed to determine fixture size: %s\n", path);
        return false;
    }
    in.seekg(0, std::ios::beg);

    out.resize(static_cast<size_t>(size));
    if (size == 0)
    {
        return true;
    }
    if (!in.read(reinterpret_cast<char *>(out.data()), size))
    {
        printf("Failed to read fixture bytes: %s\n", path);
        return false;
    }
    return true;
}

/**
 * Align size up to the next multiple of alignment.
 *
 * Returns 0 on overflow.
 */
static size_t AlignUpSize(size_t value, size_t alignment)
{
    if (alignment == 0)
    {
        return value;
    }
    size_t rem = value % alignment;
    if (rem == 0)
    {
        return value;
    }
    size_t delta = alignment - rem;
    if (value > std::numeric_limits<size_t>::max() - delta)
    {
        return 0;
    }
    return value + delta;
}

/**
 * Verify exact plaintext equality and print first mismatch details.
 */
static bool VerifyExactBuffer(const std::vector<unsigned char> &actual, const std::vector<unsigned char> &expected,
                              size_t threadIndex, size_t iter, size_t recordIndex)
{
    if (actual.size() != expected.size())
    {
        printf("VerifyExactBuffer size mismatch: actual=%zu expected=%zu thread=%zu iter=%zu record=%zu\n",
               actual.size(), expected.size(), threadIndex, iter, recordIndex);
        return false;
    }
    for (size_t i = 0; i < actual.size(); ++i)
    {
        if (actual[i] != expected[i])
        {
            printf("VerifyExactBuffer mismatch: idx=%zu actual=0x%02x expected=0x%02x thread=%zu iter=%zu record=%zu\n",
                   i, static_cast<unsigned int>(actual[i]), static_cast<unsigned int>(expected[i]), threadIndex, iter,
                   recordIndex);
            return false;
        }
    }
    return true;
}

/**
 * Concurrent pread test across multiple threads on a shared fd.
 */
static bool testConcurrentPread()
{
    TEST_START("ConcurrentPRead");

    const char *filename = "test_concurrent_pread.bin";
    const size_t chunkSize = 512;
    const size_t numChunks = 1024;

    int fd = DDSPosix::open(filename, O_CREAT | O_TRUNC | O_RDWR, 0644);
    TEST_ASSERT(fd >= DDS_POSIX_FD_START);

    TEST_ASSERT(WritePatternFile(fd, chunkSize, numChunks));

    std::atomic<int> failures(0);
    const size_t threadCount = 8;
    const size_t iterations = 64;

    auto reader = [&](size_t threadIndex)
    {
        std::vector<unsigned char> buffer(chunkSize);
        std::mt19937 rng(static_cast<unsigned int>(threadIndex + 1));
        std::uniform_int_distribution<size_t> dist(0, numChunks - 1);
        for (size_t iter = 0; iter < iterations; ++iter)
        {
            size_t chunkIndex = dist(rng);
            ssize_t readBytes =
                DDSPosix::pread(fd, buffer.data(), chunkSize, static_cast<off_t>(chunkIndex * chunkSize));
            if (readBytes != static_cast<ssize_t>(chunkSize) ||
                !VerifyBuffer(buffer, static_cast<unsigned char>(chunkIndex)))
            {
                failures.fetch_add(1, std::memory_order_relaxed);
                printf("Failure in thread %zu at iteration %zu: readBytes=%zd, expected=%zu\n", threadIndex, iter,
                       readBytes, chunkSize);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i)
    {
        threads.emplace_back(reader, i);
    }
    for (auto &thread : threads)
    {
        thread.join();
    }

    TEST_ASSERT(failures.load(std::memory_order_relaxed) == 0);
    TEST_ASSERT(DDSPosix::close(fd) == 0);

    TEST_END();
}

/**
 * Concurrent pread test across multiple threads on a shared fd.
 *
 * Aligned offset, but read size spans multiple full chunks.
 */
static bool testConcurrentPreadMultiChunkAligned()
{
    TEST_START("ConcurrentPReadMultiChunkAligned");

    const char *filename = "test_concurrent_pread_multichunk_aligned.bin";
    const size_t chunkSize = 512;
    const size_t numChunks = 1024;
    const size_t chunksPerRead = 4;
    const size_t readSize = chunkSize * chunksPerRead;

    int fd = DDSPosix::open(filename, O_CREAT | O_TRUNC | O_RDWR, 0644);
    TEST_ASSERT(fd >= DDS_POSIX_FD_START);

    TEST_ASSERT(WritePatternFile(fd, chunkSize, numChunks));

    std::atomic<int> failures(0);
    const size_t threadCount = 8;
    const size_t iterations = 64;

    auto reader = [&](size_t threadIndex)
    {
        std::vector<unsigned char> buffer(readSize);
        std::mt19937 rng(static_cast<unsigned int>(threadIndex + 101));
        std::uniform_int_distribution<size_t> dist(0, numChunks - chunksPerRead);
        for (size_t iter = 0; iter < iterations; ++iter)
        {
            size_t startChunkIndex = dist(rng);
            off_t offset = static_cast<off_t>(startChunkIndex * chunkSize);
            ssize_t readBytes = DDSPosix::pread(fd, buffer.data(), readSize, offset);
            if (readBytes != static_cast<ssize_t>(readSize) || !VerifyBufferSpanningChunks(buffer, chunkSize, offset))
            {
                failures.fetch_add(1, std::memory_order_relaxed);
                printf("Failure in thread %zu at iteration %zu: readBytes=%zd, expected=%zu\n", threadIndex, iter,
                       readBytes, readSize);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i)
    {
        threads.emplace_back(reader, i);
    }
    for (auto &thread : threads)
    {
        thread.join();
    }

    TEST_ASSERT(failures.load(std::memory_order_relaxed) == 0);
    TEST_ASSERT(DDSPosix::close(fd) == 0);

    TEST_END();
}

/**
 * Concurrent pread2 test across multiple threads on a shared fd.
 *
 * Original intent (kept for reference): stage 1 size equals logical read size,
 * stage 2 size expands the output and is expected to be filled with 0xFF by the stub.
 *
 * Updated semantics: stage 1 may expand beyond the logical read size, and the stub copies
 * the read data, fills stage 1 tail with 0xEE, then stage 2 copies stage 1 and fills tail
 * with 0xFF. Both sizes are unaligned to 512 when possible.
 */
static bool testConcurrentPread2Unaligned()
{
    TEST_START("ConcurrentPRead2Unaligned");

    const char *filename = "test_concurrent_pread2_unaligned.bin";
    const size_t chunkSize = 4096;
    const size_t fileSizeTarget = static_cast<size_t>(2ULL * 1024ULL * 1024ULL * 1024ULL); // 2GB
    const size_t numChunks = fileSizeTarget / chunkSize;
    const size_t fileSize = chunkSize * numChunks;
    const size_t maxReadSize = (8 * chunkSize) + 137;
    const size_t maxStage1Expand = (2 * chunkSize) + 19;
    const size_t maxStage2Expand = (4 * chunkSize) + 23;

    int fd = DDSPosix::open(filename, O_CREAT | O_TRUNC | O_RDWR, 0644);
    TEST_ASSERT(fd >= DDS_POSIX_FD_START);

    TEST_ASSERT(WritePatternFile(fd, chunkSize, numChunks));

    std::atomic<int> failures(0);
    std::atomic<unsigned long long> totalOutputBytes(0);
    const size_t threadCount = 8;
    const size_t iterations = 2048;

    auto reader = [&](size_t threadIndex)
    {
        std::mt19937 rng(static_cast<unsigned int>(threadIndex + 401));

        for (size_t iter = 0; iter < iterations; ++iter)
        {
            size_t readSize = PickNonAlignedValue(rng, 1, maxReadSize, chunkSize);
            if (readSize >= fileSize - 1)
            {
                readSize = fileSize - 2;
            }
            // Ensure read size remains unaligned after clamping.
            if ((readSize % chunkSize) == 0 && readSize > 1)
            {
                readSize -= 1;
            }

            size_t maxOffset = fileSize - readSize;
            size_t offsetSize = PickNonAlignedValue(rng, 1, maxOffset, chunkSize);
            if (offsetSize + readSize > fileSize)
            {
                offsetSize = maxOffset;
            }
            // Ensure offset stays unaligned after clamping.
            if ((offsetSize % chunkSize) == 0 && offsetSize > 1)
            {
                offsetSize -= 1;
            }
            off_t offset = static_cast<off_t>(offsetSize);

            size_t stageSizes[2];
            size_t stageInputOffsets[2];
            size_t stageInputLengths[2];
            std::uniform_int_distribution<int> stage1GrowDist(0, 1);
            bool growStage1 = stage1GrowDist(rng) != 0;
            if (growStage1)
            {
                size_t stage1Extra = PickNonAlignedValue(rng, 1, maxStage1Expand, chunkSize);
                stageSizes[0] = readSize + stage1Extra;
            }
            else
            {
                stageSizes[0] = readSize;
            }
            size_t stage2Extra = PickNonAlignedValue(rng, 1, maxStage2Expand, chunkSize);
            stageSizes[1] = stageSizes[0] + stage2Extra;
            // jason: preserve legacy full-stage chaining for this test.
            stageInputOffsets[0] = 0;
            stageInputLengths[0] = readSize;
            stageInputOffsets[1] = 0;
            stageInputLengths[1] = stageSizes[0];

            std::vector<unsigned char> buffer(stageSizes[1]);
            ssize_t readBytes = DDSPosix::pread2(fd, buffer.data(), readSize, offset, stageSizes, stageInputOffsets,
                                                 stageInputLengths, static_cast<uint16_t>(2)
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
                                                 , nullptr
#endif
                                                 );

            if (readBytes > 0)
            {
                totalOutputBytes.fetch_add(static_cast<unsigned long long>(readBytes), std::memory_order_relaxed);
            }
            // jason: Original stub expectation (kept for reference) validated 0xFF over the entire buffer.
            // if (readBytes != static_cast<ssize_t>(stageSizes[1]) ||
            //     !VerifyBufferValueWithDebug(buffer, static_cast<unsigned char>(0xFF)))
            bool okRead = (readBytes == static_cast<ssize_t>(stageSizes[1]));
            bool okPrefix = VerifyBufferPrefixSpanningChunks(buffer, readSize, chunkSize, offset);
            bool okStage1Tail = VerifyBufferRangeValueWithDebug(buffer, readSize, stageSizes[0] - readSize,
                                                                static_cast<unsigned char>(0xEE), "stage1-tail");
            bool okStage2Tail = VerifyBufferRangeValueWithDebug(buffer, stageSizes[0], stageSizes[1] - stageSizes[0],
                                                                static_cast<unsigned char>(0xFF), "stage2-tail");
            if (!okRead || !okPrefix || !okStage1Tail || !okStage2Tail)
            {
                failures.fetch_add(1, std::memory_order_relaxed);
                printf("Failure in thread %zu at iteration %zu: readBytes=%zd expected=%zu "
                       "(readSize=%zu offset=%zu stage2=%zu)\n",
                       threadIndex, iter, readBytes, stageSizes[1], readSize, offsetSize, stageSizes[1]);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(threadCount);

    auto readStart = std::chrono::steady_clock::now();
    for (size_t i = 0; i < threadCount; ++i)
    {
        threads.emplace_back(reader, i);
    }
    for (auto &thread : threads)
    {
        thread.join();
    }
    auto readEnd = std::chrono::steady_clock::now();
    double readSeconds = std::chrono::duration_cast<std::chrono::duration<double>>(readEnd - readStart).count();
    double readMiB = static_cast<double>(totalOutputBytes.load(std::memory_order_relaxed)) / (1024.0 * 1024.0);
    double readMiBps = (readSeconds > 0.0) ? (readMiB / readSeconds) : 0.0;
    printf("pread2 aggregate: %.2f MiB in %.3f s (%.2f MiB/s)\n", readMiB, readSeconds, readMiBps);

    TEST_ASSERT(failures.load(std::memory_order_relaxed) == 0);
    TEST_ASSERT(DDSPosix::close(fd) == 0);

    TEST_END();
}

/**
 * Mixed-channel concurrent read test:
 * runs pread (primary channel) and pread2 (pread2 channel) concurrently
 * against one shared file descriptor and validates both correctness and
 * aggregate throughput numbers for each path.
 *
 * pread2 in this test uses the same read-decrypt-decompress fixture layout
 * and stage windows as testConcurrentPread2DpkOffloadAesDeflate().
 */
static bool testConcurrentMixedPreadAndPread2()
{
    TEST_START("ConcurrentMixedPreadAndPread2");

    const char *filename = "test_concurrent_mixed_pread_pread2.bin";
    const char *encryptedFixturePath = "/data/dpk/DDS/StorageEngine/DDSPosixInterface/test/4K-16Btag.deflate.encrypted";
    const char *expectedPlainPath = "/data/dpk/DDS/StorageEngine/DDSPosixInterface/test/4K.txt";
    const size_t preadChunkSize = 4096;
    const size_t preadRegionTargetBytes = static_cast<size_t>(128ULL * 1024ULL * 1024ULL); // 128 MiB
    const size_t preadNumChunks = preadRegionTargetBytes / preadChunkSize;
    const size_t preadRegionBytes = preadNumChunks * preadChunkSize;
    const size_t recordCount = 64;
    const size_t recordLenPrefixBytes = 4;
    const size_t aesNonceBytes = 12;
    const size_t aesTagBytes = 16;
    const size_t backendIoAlign = 512;
    std::vector<unsigned char> aesKey(32, 0);
    std::vector<unsigned char> encryptedPayload;
    std::vector<unsigned char> expectedPlain;

    TEST_ASSERT(ReadFileBytes(encryptedFixturePath, encryptedPayload));
    TEST_ASSERT(!encryptedPayload.empty());
    TEST_ASSERT(encryptedPayload.size() > aesTagBytes);
    TEST_ASSERT(ReadFileBytes(expectedPlainPath, expectedPlain));
    TEST_ASSERT(!expectedPlain.empty());

    const size_t stage1InputBytes = encryptedPayload.size();   // [ciphertext||tag]
    const size_t stage1Bytes = stage1InputBytes - aesTagBytes; // deflate bytes after AES decrypt
    const size_t plainBytes = expectedPlain.size();
    const size_t logicalBytes = recordLenPrefixBytes + aesNonceBytes + stage1InputBytes;
    TEST_ASSERT(logicalBytes <= (std::numeric_limits<size_t>::max() / recordCount));
    const size_t totalLogicalBytes = logicalBytes * recordCount;
    const size_t totalPaddedBytes = AlignUpSize(totalLogicalBytes, backendIoAlign);
    TEST_ASSERT(totalPaddedBytes != 0);
    TEST_ASSERT(totalPaddedBytes >= totalLogicalBytes);
    const size_t pread2BaseOffset = preadRegionBytes;
    TEST_ASSERT(pread2BaseOffset <= static_cast<size_t>(std::numeric_limits<off_t>::max()));
    TEST_ASSERT(totalPaddedBytes <= (static_cast<size_t>(std::numeric_limits<off_t>::max()) - pread2BaseOffset));

    const size_t preadThreads = 4;
    const size_t pread2Threads = 4;
    const size_t iterationsPerThread = 40960;

    int fd = DDSPosix::open(filename, O_CREAT | O_TRUNC | O_RDWR, 0644);
    TEST_ASSERT(fd >= DDS_POSIX_FD_START);
    TEST_ASSERT(WritePatternFile(fd, preadChunkSize, preadNumChunks));

    // Build pread2 records using the same encrypted fixture layout as the DPK offload test:
    // [4B len][12B nonce][ciphertext][16B tag].
    std::vector<unsigned char> packedRecord(logicalBytes, 0);
    const uint32_t encodedPayloadLenLe = static_cast<uint32_t>(aesNonceBytes + stage1InputBytes);
    packedRecord[0] = static_cast<unsigned char>(encodedPayloadLenLe & 0xffu);
    packedRecord[1] = static_cast<unsigned char>((encodedPayloadLenLe >> 8) & 0xffu);
    packedRecord[2] = static_cast<unsigned char>((encodedPayloadLenLe >> 16) & 0xffu);
    packedRecord[3] = static_cast<unsigned char>((encodedPayloadLenLe >> 24) & 0xffu);
    memset(packedRecord.data() + recordLenPrefixBytes, 0, aesNonceBytes);
    memcpy(packedRecord.data() + recordLenPrefixBytes + aesNonceBytes, encryptedPayload.data(), stage1InputBytes);

    std::vector<unsigned char> writePadded(totalPaddedBytes, 0);
    for (size_t rec = 0; rec < recordCount; ++rec)
    {
        size_t recStart = rec * logicalBytes;
        TEST_ASSERT(recStart + logicalBytes <= writePadded.size());
        memcpy(writePadded.data() + recStart, packedRecord.data(), logicalBytes);
    }
    ssize_t written =
        DDSPosix::pwrite(fd, writePadded.data(), writePadded.size(), static_cast<off_t>(pread2BaseOffset));
    TEST_ASSERT(written == static_cast<ssize_t>(writePadded.size()));
    TEST_ASSERT(DDSPosix::ftruncate(fd, static_cast<off_t>(pread2BaseOffset + totalLogicalBytes)) == 0);
    TEST_ASSERT(DDSPosix::set_read2_aes_key(aesKey.data(), aesKey.size()) == 0);

    std::atomic<int> failures(0);
    std::atomic<unsigned long long> totalPreadBytes(0);
    std::atomic<unsigned long long> totalPread2Bytes(0);
    std::atomic<int> readyReaders(0);
    std::atomic<bool> startFlag(false);

    auto preadReader = [&](size_t threadIndex)
    {
        readyReaders.fetch_add(1, std::memory_order_release);
        while (!startFlag.load(std::memory_order_acquire))
        {
            std::this_thread::yield();
        }

        std::mt19937 rng(static_cast<unsigned int>(threadIndex + 5001));
        for (size_t iter = 0; iter < iterationsPerThread; ++iter)
        {
            size_t readSize = PickNonAlignedValue(rng, 64, (17 * preadChunkSize) + 17, preadChunkSize);
            if (readSize >= preadRegionBytes - 1)
            {
                readSize = preadRegionBytes - 2;
            }

            size_t maxOffset = preadRegionBytes - readSize;
            size_t offsetSize = PickNonAlignedValue(rng, 1, maxOffset, preadChunkSize);
            if (offsetSize + readSize > preadRegionBytes)
            {
                offsetSize = maxOffset;
            }
            off_t offset = static_cast<off_t>(offsetSize);

            std::vector<unsigned char> buffer(readSize);
            ssize_t readBytes = DDSPosix::pread(fd, buffer.data(), readSize, offset);
            bool okRead = (readBytes == static_cast<ssize_t>(readSize));
            bool okData = okRead && VerifyBufferSpanningChunks(buffer, preadChunkSize, offset);
            if (!okData)
            {
                failures.fetch_add(1, std::memory_order_relaxed);
                printf("Mixed pread failure thread=%zu iter=%zu readBytes=%zd expected=%zu offset=%zu\n", threadIndex,
                       iter, readBytes, readSize, offsetSize);
                continue;
            }
            totalPreadBytes.fetch_add(static_cast<unsigned long long>(readBytes), std::memory_order_relaxed);
        }
    };

    auto pread2Reader = [&](size_t threadIndex)
    {
        readyReaders.fetch_add(1, std::memory_order_release);
        while (!startFlag.load(std::memory_order_acquire))
        {
            std::this_thread::yield();
        }

        std::mt19937 rng(static_cast<unsigned int>(threadIndex + 7001));
        std::uniform_int_distribution<size_t> recDist(0, recordCount - 1);
        size_t stageSizes[2] = {stage1Bytes, plainBytes};
        // AES stage consumes [ciphertext||tag], with nonce immediately before it.
        size_t stageInputOffsets[2] = {recordLenPrefixBytes + aesNonceBytes, 0};
        size_t stageInputLengths[2] = {stage1InputBytes, stage1Bytes};
        std::vector<unsigned char> out(plainBytes);

        for (size_t iter = 0; iter < iterationsPerThread; ++iter)
        {
            size_t rec = recDist(rng);
            off_t recOffset = static_cast<off_t>(pread2BaseOffset + (rec * logicalBytes));
            ssize_t readBytes = DDSPosix::pread2(fd, out.data(), logicalBytes, recOffset, stageSizes, stageInputOffsets,
                                                 stageInputLengths, static_cast<uint16_t>(2)
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
                                                                        ,
                                                 nullptr
#endif
            );

            bool okRead = (readBytes == static_cast<ssize_t>(plainBytes));
            bool okData = okRead && VerifyExactBuffer(out, expectedPlain, threadIndex, iter, rec);
            if (!okData)
            {
                failures.fetch_add(1, std::memory_order_relaxed);
                printf("Mixed pread2 failure thread=%zu iter=%zu rec=%zu readBytes=%zd expected=%zu\n", threadIndex,
                       iter, rec, readBytes, plainBytes);
                continue;
            }
            totalPread2Bytes.fetch_add(static_cast<unsigned long long>(readBytes), std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(preadThreads + pread2Threads);
    for (size_t i = 0; i < preadThreads; ++i)
    {
        threads.emplace_back(preadReader, i);
    }
    for (size_t i = 0; i < pread2Threads; ++i)
    {
        threads.emplace_back(pread2Reader, i);
    }

    const size_t totalThreads = preadThreads + pread2Threads;
    while (readyReaders.load(std::memory_order_acquire) < static_cast<int>(totalThreads))
    {
        std::this_thread::yield();
    }

    auto start = std::chrono::steady_clock::now();
    startFlag.store(true, std::memory_order_release);
    for (auto &thread : threads)
    {
        thread.join();
    }
    auto end = std::chrono::steady_clock::now();

    double seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
    double preadMiB = static_cast<double>(totalPreadBytes.load(std::memory_order_relaxed)) / (1024.0 * 1024.0);
    double pread2MiB = static_cast<double>(totalPread2Bytes.load(std::memory_order_relaxed)) / (1024.0 * 1024.0);
    double preadMiBps = (seconds > 0.0) ? (preadMiB / seconds) : 0.0;
    double pread2MiBps = (seconds > 0.0) ? (pread2MiB / seconds) : 0.0;
    double totalMiBps = (seconds > 0.0) ? ((preadMiB + pread2MiB) / seconds) : 0.0;
    printf("mixed pread/pread2 aggregate: pread=%.2f MiB (%.2f MiB/s), pread2=%.2f MiB (%.2f MiB/s), total=%.2f "
           "MiB/s over %.3f s\n",
           preadMiB, preadMiBps, pread2MiB, pread2MiBps, totalMiBps, seconds);

    TEST_ASSERT(failures.load(std::memory_order_relaxed) == 0);
    TEST_ASSERT(totalPreadBytes.load(std::memory_order_relaxed) > 0);
    TEST_ASSERT(totalPread2Bytes.load(std::memory_order_relaxed) > 0);
    TEST_ASSERT(DDSPosix::close(fd) == 0);

    TEST_END();
}

/**
 * Concurrent pread2 test for full DPK offload path:
 * stage0 read (encrypted+deflate) -> stage1 AES-GCM decrypt -> stage2 deflate decompress.
 *
 * Uses an external encrypted+compressed fixture for stage0 payload and validates final plaintext
 * against an external expected file.
 */
static bool testConcurrentPread2DpkOffloadAesDeflate()
{
    TEST_START("ConcurrentPRead2DpkOffloadAesDeflate");

    const char *filename = "test_concurrent_pread2_dpk_offload_aes_deflate.bin";
    const char *encryptedFixturePath = "/data/dpk/DDS/StorageEngine/DDSPosixInterface/test/4K-16Btag.deflate.encrypted";
    const char *expectedPlainPath = "/data/dpk/DDS/StorageEngine/DDSPosixInterface/test/4K.txt";
    const size_t recordCount = 64;
    // Updated read2 record layout:
    // [4B encoded_payload_len][12B nonce][ciphertext][16B tag]
    // where encoded_payload_len = nonce + ciphertext + tag (DuckDB encrypted module convention).
    const size_t recordLenPrefixBytes = 4;
    const size_t aesNonceBytes = 12;
    const size_t aesTagBytes = 16;
    // Matches the current DPK AES-GCM prototype path used by existing BF3 clients.
    std::vector<unsigned char> aesKey(32, 0);

    std::vector<unsigned char> encryptedPayload;
    TEST_ASSERT(ReadFileBytes(encryptedFixturePath, encryptedPayload));
    TEST_ASSERT(!encryptedPayload.empty());
    TEST_ASSERT(encryptedPayload.size() > aesTagBytes);

    std::vector<unsigned char> expectedPlain;
    TEST_ASSERT(ReadFileBytes(expectedPlainPath, expectedPlain));
    TEST_ASSERT(!expectedPlain.empty());

    int fd = DDSPosix::open(filename, O_CREAT | O_TRUNC | O_RDWR, 0644);
    TEST_ASSERT(fd >= DDS_POSIX_FD_START);

    const size_t stage1InputBytes = encryptedPayload.size();   // [ciphertext||tag]
    const size_t stage1Bytes = stage1InputBytes - aesTagBytes; // plaintext deflate bytes after AES decrypt
    const size_t plainBytes = expectedPlain.size();
    TEST_ASSERT(stage1Bytes <= static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
    const size_t logicalBytes = recordLenPrefixBytes + aesNonceBytes + stage1InputBytes;
    const size_t backendIoAlign = 512;
    TEST_ASSERT(logicalBytes <= (std::numeric_limits<size_t>::max() / recordCount));
    const size_t totalLogicalBytes = logicalBytes * recordCount;
    const size_t totalPaddedBytes = AlignUpSize(totalLogicalBytes, backendIoAlign);
    TEST_ASSERT(totalPaddedBytes != 0);
    TEST_ASSERT(totalPaddedBytes >= totalLogicalBytes);

    // Build one logical record with layout [4B len][12B nonce][ciphertext][16B tag].
    std::vector<unsigned char> packedRecord(logicalBytes, 0);
    const uint32_t encodedPayloadLenLe = static_cast<uint32_t>(aesNonceBytes + stage1InputBytes);
    packedRecord[0] = static_cast<unsigned char>(encodedPayloadLenLe & 0xffu);
    packedRecord[1] = static_cast<unsigned char>((encodedPayloadLenLe >> 8) & 0xffu);
    packedRecord[2] = static_cast<unsigned char>((encodedPayloadLenLe >> 16) & 0xffu);
    packedRecord[3] = static_cast<unsigned char>((encodedPayloadLenLe >> 24) & 0xffu);
    // Nonce/IV is all zero for this fixture and is consumed by AES stage from bytes before ciphertext input window.
    memset(packedRecord.data() + recordLenPrefixBytes, 0, aesNonceBytes);
    memcpy(packedRecord.data() + recordLenPrefixBytes + aesNonceBytes, encryptedPayload.data(), stage1InputBytes);

    std::vector<unsigned char> writePadded(totalPaddedBytes, 0);
    for (size_t rec = 0; rec < recordCount; ++rec)
    {
        size_t recStart = rec * logicalBytes;
        TEST_ASSERT(recStart + logicalBytes <= writePadded.size());
        memcpy(writePadded.data() + recStart, packedRecord.data(), logicalBytes);
    }

    // Original per-record write path (kept for reference): it can fail when offset/count are not backend-aligned.
    // for (size_t rec = 0; rec < recordCount; ++rec)
    // {
    //     off_t recOffset = static_cast<off_t>(rec * logicalBytes);
    //     ssize_t written = DDSPosix::pwrite(fd, encryptedPayload.data(), encryptedPayload.size(), recOffset);
    //     TEST_ASSERT(written == static_cast<ssize_t>(encryptedPayload.size()));
    // }

    // Use one backend-aligned write, then truncate back to logical bytes so pread2 offsets remain contiguous.
    ssize_t written = DDSPosix::pwrite(fd, writePadded.data(), writePadded.size(), 0);
    TEST_ASSERT(written == static_cast<ssize_t>(writePadded.size()));
    TEST_ASSERT(DDSPosix::ftruncate(fd, static_cast<off_t>(totalLogicalBytes)) == 0);

    // Side channel key setup for stage1 AES-GCM decrypt.
    TEST_ASSERT(DDSPosix::set_read2_aes_key(aesKey.data(), aesKey.size()) == 0);

    std::atomic<int> failures(0);
    const size_t threadCount = 8;
    const size_t iterations = 1000000 / 8;

    auto reader = [&](size_t threadIndex)
    {
        std::mt19937 rng(static_cast<unsigned int>(threadIndex + 901));
        std::uniform_int_distribution<size_t> recDist(0, recordCount - 1);

        size_t stageSizes[2] = {stage1Bytes, plainBytes};
        // For AES stage: input window points to [ciphertext||tag], with nonce immediately before it.
        size_t stageInputOffsets[2] = {recordLenPrefixBytes + aesNonceBytes, 0};
        size_t stageInputLengths[2] = {stage1InputBytes, stage1Bytes};

        std::vector<unsigned char> out(plainBytes);
        for (size_t iter = 0; iter < iterations; ++iter)
        {
            size_t rec = recDist(rng);
            off_t recOffset = static_cast<off_t>(rec * logicalBytes);

            ssize_t readBytes = DDSPosix::pread2(fd, out.data(), logicalBytes, recOffset, stageSizes, stageInputOffsets,
                                                 stageInputLengths, static_cast<uint16_t>(2)
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
                                                 , nullptr
#endif
                                                 );
            if (readBytes != static_cast<ssize_t>(plainBytes) ||
                !VerifyExactBuffer(out, expectedPlain, threadIndex, iter, rec))
            {
                failures.fetch_add(1, std::memory_order_relaxed);
                printf("DPK pread2 failure thread=%zu iter=%zu rec=%zu readBytes=%zd expected=%zu\n", threadIndex, iter,
                       rec, readBytes, plainBytes);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i)
    {
        threads.emplace_back(reader, i);
    }
    for (auto &thread : threads)
    {
        thread.join();
    }

    TEST_ASSERT(failures.load(std::memory_order_relaxed) == 0);
    TEST_ASSERT(DDSPosix::close(fd) == 0);

    TEST_END();
}

/**
 * Concurrent pread test across multiple threads on a shared fd.
 *
 * Unaligned offset and unaligned read size.
 */
static bool testConcurrentPreadUnaligned()
{
    TEST_START("ConcurrentPReadUnaligned");

    const char *filename = "test_concurrent_pread_unaligned.bin";
    const size_t chunkSize = 512;
    const size_t numChunks = 1024;
    const size_t fileSize = chunkSize * numChunks;

    int fd = DDSPosix::open(filename, O_CREAT | O_TRUNC | O_RDWR, 0644);
    TEST_ASSERT(fd >= DDS_POSIX_FD_START);

    TEST_ASSERT(WritePatternFile(fd, chunkSize, numChunks));

    std::atomic<int> failures(0);
    const size_t threadCount = 8;
    const size_t iterations = 64;

    auto reader = [&](size_t threadIndex)
    {
        std::mt19937 rng(static_cast<unsigned int>(threadIndex + 201));
        std::uniform_int_distribution<size_t> countDist(1, (3 * chunkSize) + 17);
        std::uniform_int_distribution<size_t> offsetDist(1, fileSize - 1);

        for (size_t iter = 0; iter < iterations; ++iter)
        {
            size_t readSize = countDist(rng);
            off_t offset = static_cast<off_t>(offsetDist(rng));

            // Force unaligned offset/count.
            if ((offset % static_cast<off_t>(chunkSize)) == 0)
            {
                offset += 1;
            }
            if ((readSize % chunkSize) == 0)
            {
                readSize += 1;
            }

            // Keep read fully within the file.
            if (static_cast<size_t>(offset) + readSize > fileSize)
            {
                if (static_cast<size_t>(offset) >= fileSize)
                {
                    offset = static_cast<off_t>(fileSize - 1);
                }
                readSize = fileSize - static_cast<size_t>(offset);
                if ((readSize % chunkSize) == 0 && readSize > 1)
                {
                    readSize -= 1;
                }
            }

            std::vector<unsigned char> buffer(readSize);
            ssize_t readBytes = DDSPosix::pread(fd, buffer.data(), readSize, offset);
            if (readBytes != static_cast<ssize_t>(readSize) || !VerifyBufferSpanningChunks(buffer, chunkSize, offset))
            {
                failures.fetch_add(1, std::memory_order_relaxed);
                printf("Failure in thread %zu at iteration %zu: readBytes=%zd, expected=%zu\n", threadIndex, iter,
                       readBytes, readSize);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i)
    {
        threads.emplace_back(reader, i);
    }
    for (auto &thread : threads)
    {
        thread.join();
    }

    int n_fails = failures.load();
    if (n_fails > 0)
    {
        printf("Total iterations: %zu; Total failures: %d\n", threadCount * iterations, n_fails);
    }
    TEST_ASSERT(n_fails == 0);
    TEST_ASSERT(DDSPosix::close(fd) == 0);

    TEST_END();
}

/**
 * Concurrent pread test where each thread opens the same file independently.
 *
 * - The file is created once and filled with a deterministic 2GB pattern via WritePatternFile.
 * - Each thread opens the file concurrently and performs unaligned reads (offset and count unaligned).
 */
static bool testConcurrentPreadUnalignedPerThreadOpen2GB()
{
    TEST_START("ConcurrentPReadUnalignedPerThreadOpen2GB");

    const char *filename = "test_concurrent_pread_unaligned_per_thread_open_2gb.bin";
    const size_t chunkSize = 4096;                                                         // 4096;
    const size_t fileSizeTarget = static_cast<size_t>(2ULL * 1024ULL * 1024ULL * 1024ULL); // 2GB
    const size_t numChunks = fileSizeTarget / chunkSize;
    const size_t fileSize = chunkSize * numChunks;

    // Create and initialize the file once.
    {
        int fd = DDSPosix::open(filename, O_CREAT | O_TRUNC | O_RDWR, 0644);
        TEST_ASSERT(fd >= DDS_POSIX_FD_START);

        auto writeStart = std::chrono::steady_clock::now();
        TEST_ASSERT(WritePatternFile(fd, chunkSize, numChunks));
        auto writeEnd = std::chrono::steady_clock::now();
        double writeSeconds = std::chrono::duration_cast<std::chrono::duration<double>>(writeEnd - writeStart).count();
        double writeMiB = static_cast<double>(fileSize) / (1024.0 * 1024.0);
        double writeMiBps = (writeSeconds > 0.0) ? (writeMiB / writeSeconds) : 0.0;
        printf("WritePatternFile: %.2f MiB in %.3f s (%.2f MiB/s)\n", writeMiB, writeSeconds, writeMiBps);

        TEST_ASSERT(DDSPosix::close(fd) == 0);
    }

    std::atomic<int> failures(0);
    std::atomic<unsigned long long> totalReadBytes(0);
    // Accumulate per-pread latency across all threads (nanoseconds) and number of calls.
    std::atomic<unsigned long long> totalReadNanos(0);
    std::atomic<unsigned long long> totalReadCalls(0);
    std::atomic<size_t> readyReaders(0);
    std::atomic<bool> startReads(false);
    std::mutex latencyMutex;
    std::vector<unsigned long long> readLatenciesNs;
    const size_t threadCount = 16;
    const size_t iterations = 8192;

    auto reader = [&](size_t threadIndex)
    {
        int fd = DDSPosix::open(filename, O_RDONLY, 0644);
        if (fd < DDS_POSIX_FD_START)
        {
            failures.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        readyReaders.fetch_add(1, std::memory_order_acq_rel);
        while (!startReads.load(std::memory_order_acquire))
        {
            std::this_thread::yield();
        }

        std::mt19937 rng(static_cast<unsigned int>(threadIndex + 301));
        std::uniform_int_distribution<size_t> countDist(1, (17 * chunkSize) + 17);
        std::uniform_int_distribution<size_t> offsetDist(1, fileSize - 1);
        std::vector<unsigned long long> localLatencies;
        localLatencies.reserve(iterations);

        for (size_t iter = 0; iter < iterations; ++iter)
        {
            size_t readSize = countDist(rng);
            off_t offset = static_cast<off_t>(offsetDist(rng));

            // Force unaligned offset/count.
            if ((offset % static_cast<off_t>(chunkSize)) == 0)
            {
                offset += 1;
            }
            if ((readSize % chunkSize) == 0)
            {
                readSize += 1;
            }

            // Keep read fully within the file.
            if (static_cast<size_t>(offset) + readSize > fileSize)
            {
                if (static_cast<size_t>(offset) >= fileSize)
                {
                    offset = static_cast<off_t>(fileSize - 1);
                }
                readSize = fileSize - static_cast<size_t>(offset);
                if ((readSize % chunkSize) == 0 && readSize > 1)
                {
                    readSize -= 1;
                }
            }

            std::vector<unsigned char> buffer(readSize);
            auto start = std::chrono::steady_clock::now();
            ssize_t readBytes = DDSPosix::pread(fd, buffer.data(), readSize, offset);
            auto end = std::chrono::steady_clock::now();
            auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            totalReadNanos.fetch_add(static_cast<unsigned long long>(dur), std::memory_order_relaxed);
            totalReadCalls.fetch_add(1, std::memory_order_relaxed);
            localLatencies.push_back(static_cast<unsigned long long>(dur));

            if (readBytes > 0)
            {
                totalReadBytes.fetch_add(static_cast<unsigned long long>(readBytes), std::memory_order_relaxed);
            }
            if (readBytes != static_cast<ssize_t>(readSize) || !VerifyBufferSpanningChunks(buffer, chunkSize, offset))
            {
                failures.fetch_add(1, std::memory_order_relaxed);
                printf("Failure in thread %zu at iteration %zu: readBytes=%zd, expected=%zu\n", threadIndex, iter,
                       readBytes, readSize);
            }
        }

        if (!localLatencies.empty())
        {
            std::lock_guard<std::mutex> lock(latencyMutex);
            readLatenciesNs.insert(readLatenciesNs.end(), localLatencies.begin(), localLatencies.end());
        }

        (void)DDSPosix::close(fd);
    };

    std::vector<std::thread> threads;
    threads.reserve(threadCount);

    auto readStart = std::chrono::steady_clock::now();
    for (size_t i = 0; i < threadCount; ++i)
    {
        threads.emplace_back(reader, i);
    }
    while (readyReaders.load(std::memory_order_acquire) < threadCount)
    {
        std::this_thread::yield();
    }
    startReads.store(true, std::memory_order_release);
    for (auto &thread : threads)
    {
        thread.join();
    }
    auto readEnd = std::chrono::steady_clock::now();
    double readSeconds = std::chrono::duration_cast<std::chrono::duration<double>>(readEnd - readStart).count();
    double readMiB = static_cast<double>(totalReadBytes.load(std::memory_order_relaxed)) / (1024.0 * 1024.0);
    double readMiBps = (readSeconds > 0.0) ? (readMiB / readSeconds) : 0.0;
    printf("pread aggregate: %.2f MiB in %.3f s (%.2f MiB/s)\n", readMiB, readSeconds, readMiBps);

    {
        unsigned long long calls = totalReadCalls.load(std::memory_order_relaxed);
        if (calls > 0)
        {
            double avgNs =
                static_cast<double>(totalReadNanos.load(std::memory_order_relaxed)) / static_cast<double>(calls);
            double avgUs = avgNs / 1000.0;
            printf("pread avg latency: %.2f ns (%.3f us) over %llu calls\n", avgNs, avgUs, calls);
        }
        else
        {
            printf("pread avg latency: no calls recorded\n");
        }
    }

    if (!readLatenciesNs.empty())
    {
        std::sort(readLatenciesNs.begin(), readLatenciesNs.end());
        const size_t count = readLatenciesNs.size();
        const size_t p50Index = (count - 1) * 50 / 100;
        const size_t p99Index = (count - 1) * 99 / 100;
        const unsigned long long p50Ns = readLatenciesNs[p50Index];
        const unsigned long long p99Ns = readLatenciesNs[p99Index];
        printf("pread p50 latency: %llu ns (%.3f us)\n", p50Ns, static_cast<double>(p50Ns) / 1000.0);
        printf("pread p99 latency: %llu ns (%.3f us)\n", p99Ns, static_cast<double>(p99Ns) / 1000.0);
    }
    else
    {
        printf("pread p50/p99 latency: no samples recorded\n");
    }

    TEST_ASSERT(failures.load(std::memory_order_relaxed) == 0);

    TEST_END();
}

/**
 * Concurrent pwrite test across multiple threads on a shared fd.
 */
static bool testConcurrentPwrite()
{
    TEST_START("ConcurrentPWrite");

    const char *filename = "test_concurrent_pwrite.bin";
    const size_t chunkSize = 512;
    const size_t numChunks = 1024;

    int fd = DDSPosix::open(filename, O_CREAT | O_TRUNC | O_RDWR, 0644);
    TEST_ASSERT(fd >= DDS_POSIX_FD_START);

    std::atomic<int> failures(0);

    auto writer = [&](size_t chunkIndex)
    {
        std::vector<unsigned char> buffer(chunkSize);
        unsigned char value = static_cast<unsigned char>(0xA0 + chunkIndex);
        FillBuffer(buffer, value);
        ssize_t written = DDSPosix::pwrite(fd, buffer.data(), chunkSize, static_cast<off_t>(chunkIndex * chunkSize));
        if (written != static_cast<ssize_t>(chunkSize))
        {
            failures.fetch_add(1, std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(numChunks);
    for (size_t i = 0; i < numChunks; ++i)
    {
        threads.emplace_back(writer, i);
    }
    for (auto &thread : threads)
    {
        thread.join();
    }

    TEST_ASSERT(failures.load(std::memory_order_relaxed) == 0);

    std::vector<unsigned char> readBuffer(chunkSize);
    for (size_t i = 0; i < numChunks; ++i)
    {
        ssize_t readBytes = DDSPosix::pread(fd, readBuffer.data(), chunkSize, static_cast<off_t>(i * chunkSize));
        TEST_ASSERT(readBytes == static_cast<ssize_t>(chunkSize));
        TEST_ASSERT(VerifyBuffer(readBuffer, static_cast<unsigned char>(0xA0 + i)));
    }

    TEST_ASSERT(DDSPosix::close(fd) == 0);

    TEST_END();
}

/**
 * Execute concurrent POSIX front-end tests and shut down DDSPosix.
 *
 * Ensures the background poller thread is stopped to avoid std::terminate
 * on process exit.
 */
int main()
{
    bool ok = true;
    // ok = ok && testConcurrentPread();
    // ok = ok && testConcurrentPreadMultiChunkAligned();
    // ok = ok && testConcurrentPreadUnaligned();
    // ok = ok && testConcurrentPread2Unaligned();
    // ok = ok && testConcurrentMixedPreadAndPread2();
    // ok = ok && testConcurrentPread2DpkOffloadAesDeflate();
    ok = ok && testConcurrentPreadUnalignedPerThreadOpen2GB();
    // ok = ok && testConcurrentPwrite();

    std::cout << "Tests passed: " << passedTests << "/" << totalTests << std::endl;

    // return ok ? 0 : 1;
    // jason: Stop the background poller thread; otherwise std::thread destructor aborts.
    // (see DDSPosix::shutdown_posix in StorageEngine/DDSPosixInterface/DDSPosix.cpp).
    (void)DDSPosix::shutdown_posix();
    int exitCode = ok ? 0 : 1;
    return exitCode;
}
