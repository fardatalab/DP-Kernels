#include "DataPlaneHandlers.h"
#include "CacheTable.h"
#include "FileService.h"
#include "DPKEmbedShim.h"
#include "DDSChannelDefs.h"
#include <assert.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <openssl/evp.h>
#include <zlib.h>
#include "spdk/thread.h"
//#include <cstdio>

#undef DEBUG_DATAPLANE_HANDLERS
#ifdef DEBUG_DATAPLANE_HANDLERS
#include <stdio.h>
#define DebugPrint(Fmt, ...) fprintf(stderr, Fmt, __VA_ARGS__)
#else
static inline void DebugPrint(const char* Fmt, ...) { }
#endif

// Original widened slot-count macro retained for reference:
// #ifndef DDS_BACKEND_SLOT_COUNT
// #define DDS_BACKEND_SLOT_COUNT (DDS_MAX_OUTSTANDING_IO * (DDS_DPU_IO_PARALLELISM + 1))
// #endif

//
// The global cache table
//
//
// CacheTableT* CacheTable;

// jason: round-robin selector for stage execution worker threads.
static atomic_uint g_stageWorkerRR = 0;

#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
// Monotonic nanoseconds helper for offload-stage timing.
static inline uint64_t OffloadNowNs(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
#endif

static inline bool IsRead2WithStages(struct PerSlotContext *SlotContext)
{
    return SlotContext && SlotContext->Ctx && SlotContext->Ctx->OpCode == BUFF_MSG_F2B_REQ_OP_READ2 &&
           SlotContext->StageCount > 0;
}

static inline uint32_t SelectStageWorkerIndex(void)
{
    if (!FS || FS->WorkerThreadCount <= 0)
    {
        return 0;
    }
    unsigned rr = atomic_fetch_add_explicit(&g_stageWorkerRR, 1u, memory_order_relaxed);
    return (uint32_t)(rr % (unsigned)FS->WorkerThreadCount);
}

// Fixed, preallocated per-worker inflight-stage capacity.
// One request can have at most one in-flight stage at a time (stage chain is serialized),
// so DDS_MAX_OUTSTANDING_IO_TOTAL per worker is a conservative upper bound.
#ifndef DDS_DPK_STAGE_INFLIGHT_CAPACITY
#define DDS_DPK_STAGE_INFLIGHT_CAPACITY DDS_MAX_OUTSTANDING_IO_TOTAL
#endif

// Poll on every SPDK loop tick to minimize completion latency.
#ifndef DDS_DPK_STAGE_POLLER_PERIOD_US
#define DDS_DPK_STAGE_POLLER_PERIOD_US 0
#endif

struct StageInflightEntry {
    int Next;
    struct Read2StageTaskCtx *Task;
    FileIOSizeT ExpectedOutputBytes;
    FileIOSizeT InputBytes;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    uint64_t SubmitStartNs;
#endif
};

struct WorkerStageInflightQueue {
    bool Initialized;
    uint32_t WorkerIndex;
    struct spdk_poller *Poller;
    int ActiveHead;
    int FreeHead;
    uint16_t ActiveCount;
    struct StageInflightEntry Entries[DDS_DPK_STAGE_INFLIGHT_CAPACITY];
};

static struct WorkerStageInflightQueue g_workerStageInflight[WORKER_THREAD_COUNT];
static DataPlaneStagePollerCtx g_workerStagePollerCtx[WORKER_THREAD_COUNT];

static int StageCompletionPoller(void *Ctx);
static void StageCompleteOnOwner(void *Ctx);

//
// Initialize the fixed-size per-worker inflight list and free list.
// Must run on the corresponding worker thread before stage submissions are accepted.
//
static inline void InitializeWorkerStageInflightQueue(struct WorkerStageInflightQueue *queue, uint32_t workerIndex)
{
    if (queue == NULL)
    {
        return;
    }
    memset(queue, 0, sizeof(*queue));
    queue->Initialized = true;
    queue->WorkerIndex = workerIndex;
    queue->Poller = NULL;
    queue->ActiveHead = -1;
    queue->FreeHead = 0;
    for (int i = 0; i < DDS_DPK_STAGE_INFLIGHT_CAPACITY; i++)
    {
        queue->Entries[i].Next = (i + 1 < DDS_DPK_STAGE_INFLIGHT_CAPACITY) ? (i + 1) : -1;
        queue->Entries[i].Task = NULL;
        queue->Entries[i].ExpectedOutputBytes = 0;
        queue->Entries[i].InputBytes = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
        queue->Entries[i].SubmitStartNs = 0;
#endif
    }
}

static inline struct WorkerStageInflightQueue *GetWorkerStageInflightQueue(uint32_t workerIndex)
{
    if (workerIndex >= WORKER_THREAD_COUNT)
    {
        return NULL;
    }
    return &g_workerStageInflight[workerIndex];
}

static inline bool ReserveWorkerStageInflightEntry(struct WorkerStageInflightQueue *queue, int *entryIndexOut)
{
    if (queue == NULL || !queue->Initialized || entryIndexOut == NULL)
    {
        return false;
    }
    if (queue->FreeHead < 0)
    {
        return false;
    }
    int idx = queue->FreeHead;
    struct StageInflightEntry *entry = &queue->Entries[idx];
    queue->FreeHead = entry->Next;
    entry->Next = -1;
    entry->Task = NULL;
    entry->ExpectedOutputBytes = 0;
    entry->InputBytes = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    entry->SubmitStartNs = 0;
#endif
    *entryIndexOut = idx;
    return true;
}

static inline void ReleaseReservedWorkerStageInflightEntry(struct WorkerStageInflightQueue *queue, int entryIndex)
{
    if (queue == NULL || entryIndex < 0 || entryIndex >= DDS_DPK_STAGE_INFLIGHT_CAPACITY)
    {
        return;
    }
    struct StageInflightEntry *entry = &queue->Entries[entryIndex];
    entry->Task = NULL;
    entry->ExpectedOutputBytes = 0;
    entry->InputBytes = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    entry->SubmitStartNs = 0;
#endif
    entry->Next = queue->FreeHead;
    queue->FreeHead = entryIndex;
}

static inline void ActivateWorkerStageInflightEntry(struct WorkerStageInflightQueue *queue, int entryIndex,
                                                    struct Read2StageTaskCtx *task, FileIOSizeT expectedOutputBytes,
                                                    FileIOSizeT inputBytes
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
                                                    ,
                                                    uint64_t submitStartNs
#endif
)
{
    if (queue == NULL || task == NULL || entryIndex < 0 || entryIndex >= DDS_DPK_STAGE_INFLIGHT_CAPACITY)
    {
        return;
    }
    struct StageInflightEntry *entry = &queue->Entries[entryIndex];
    entry->Task = task;
    entry->ExpectedOutputBytes = expectedOutputBytes;
    entry->InputBytes = inputBytes;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    entry->SubmitStartNs = submitStartNs;
#endif
    entry->Next = queue->ActiveHead;
    queue->ActiveHead = entryIndex;
    queue->ActiveCount += 1;
}

void RegisterDataPlaneStagePoller(void *Ctx)
{
    DataPlaneStagePollerCtx *pollerCtx = (DataPlaneStagePollerCtx *)Ctx;
    if (pollerCtx == NULL)
    {
        fprintf(stderr, "%s [error]: null poller ctx\n", __func__);
        return;
    }
    uint32_t workerIndex = pollerCtx->WorkerIndex;
    if (workerIndex >= WORKER_THREAD_COUNT)
    {
        fprintf(stderr, "%s [error]: invalid worker index=%u (max=%u)\n", __func__, (unsigned)workerIndex,
                (unsigned)WORKER_THREAD_COUNT);
        return;
    }

    struct WorkerStageInflightQueue *queue = GetWorkerStageInflightQueue(workerIndex);
    if (queue == NULL)
    {
        fprintf(stderr, "%s [error]: queue lookup failed for worker=%u\n", __func__, (unsigned)workerIndex);
        return;
    }
    if (!queue->Initialized)
    {
        InitializeWorkerStageInflightQueue(queue, workerIndex);
    }

    g_workerStagePollerCtx[workerIndex].WorkerIndex = workerIndex;
    if (queue->Poller != NULL)
    {
        fprintf(stderr, "%s [warn]: poller already registered worker=%u\n", __func__, (unsigned)workerIndex);
        return;
    }
    queue->Poller = spdk_poller_register(StageCompletionPoller, &g_workerStagePollerCtx[workerIndex],
                                         DDS_DPK_STAGE_POLLER_PERIOD_US);
    if (queue->Poller == NULL)
    {
        fprintf(stderr, "%s [error]: spdk_poller_register failed worker=%u\n", __func__, (unsigned)workerIndex);
        return;
    }
    fprintf(stderr, "%s: registered stage completion poller worker=%u period_us=%u\n", __func__, (unsigned)workerIndex,
            (unsigned)DDS_DPK_STAGE_POLLER_PERIOD_US);
}

void UnregisterDataPlaneStagePoller(void *Ctx)
{
    DataPlaneStagePollerCtx *pollerCtx = (DataPlaneStagePollerCtx *)Ctx;
    if (pollerCtx == NULL)
    {
        fprintf(stderr, "%s [error]: null poller ctx\n", __func__);
        return;
    }
    uint32_t workerIndex = pollerCtx->WorkerIndex;
    if (workerIndex >= WORKER_THREAD_COUNT)
    {
        fprintf(stderr, "%s [error]: invalid worker index=%u (max=%u)\n", __func__, (unsigned)workerIndex,
                (unsigned)WORKER_THREAD_COUNT);
        return;
    }

    struct WorkerStageInflightQueue *queue = GetWorkerStageInflightQueue(workerIndex);
    if (queue == NULL || !queue->Initialized)
    {
        return;
    }
    if (queue->ActiveCount != 0)
    {
        fprintf(stderr, "%s [warn]: unregister while inflight stages still active worker=%u active=%u\n", __func__,
                (unsigned)workerIndex, (unsigned)queue->ActiveCount);
    }
    if (queue->Poller != NULL)
    {
        spdk_poller_unregister(&queue->Poller);
        queue->Poller = NULL;
    }
}

static inline bool EnsureStageTaskBound(struct PerSlotContext *SlotContext, uint16_t stageIndex)
{
    if (SlotContext == NULL || SlotContext->Ctx == NULL || stageIndex >= 2)
    {
        return false;
    }
    if (dpk_embed_runtime_is_ready() == 0)
    {
        fprintf(stderr, "%s [error]: DPK runtime is not ready (reqId=%u)\n", __func__,
                (unsigned)SlotContext->Ctx->Response->RequestId);
        return false;
    }

    // Original lazy path (create + bind on first submit) removed intentionally.
    // We now require all stage tasks to be created/bound once during backend startup.
    // This keeps DPM control-plane work out of the data path.
    if (!SlotContext->DpkTaskStateInitialized)
    {
        fprintf(stderr, "%s [error]: DPK task state was not preinitialized stage=%u reqId=%u\n", __func__,
                (unsigned)stageIndex, (unsigned)SlotContext->Ctx->Response->RequestId);
        return false;
    }

    dpk_embed_task_handle *task = (dpk_embed_task_handle *)SlotContext->DpkStageTaskHandles[stageIndex];
    if (task == NULL)
    {
        fprintf(stderr, "%s [error]: DPK stage handle missing stage=%u reqId=%u\n", __func__, (unsigned)stageIndex,
                (unsigned)SlotContext->Ctx->Response->RequestId);
        return false;
    }

    return true;
}

//
// Validate that a pointer range is fully within the per-request response ring region.
// This catches bad slice math before we submit a DOCA task with out-of-region addresses.
//
static inline bool BufferRangeWithinResponseRing(struct PerSlotContext *SlotContext, const char *ptr, FileIOSizeT bytes,
                                                 FileIOSizeT *offsetOut)
{
    if (offsetOut)
    {
        *offsetOut = 0;
    }
    if (SlotContext == NULL || SlotContext->ResponseRingBase == NULL || SlotContext->ResponseRingBytes == 0 || ptr == NULL)
    {
        return false;
    }
    uintptr_t base = (uintptr_t)SlotContext->ResponseRingBase;
    uintptr_t end = base + (uintptr_t)SlotContext->ResponseRingBytes;
    uintptr_t start = (uintptr_t)ptr;
    if (start < base || start >= end)
    {
        return false;
    }
    if (bytes > 0)
    {
        uintptr_t rangeEnd = start + (uintptr_t)bytes;
        if (rangeEnd < start || rangeEnd > end)
        {
            return false;
        }
    }
    if (offsetOut)
    {
        *offsetOut = (FileIOSizeT)(start - base);
    }
    return true;
}

// AES module layout expected by DDS read2 stage-1 decrypt:
//   [4-byte little-endian length][12-byte IV/nonce][ciphertext][16-byte GCM tag]
// where stage-1 input points at [ciphertext||tag], and IV is immediately before it.
#define DDS_STAGE1_AES_LEN_FIELD_BYTES 4u
#define DDS_STAGE1_AES_IV_BYTES 12u
#define DDS_STAGE1_AES_TAG_BYTES 16u
#define DDS_STAGE1_AES_PREFIX_BYTES (DDS_STAGE1_AES_LEN_FIELD_BYTES + DDS_STAGE1_AES_IV_BYTES)

static inline uint32_t Crc32Fingerprint(const unsigned char *ptr, FileIOSizeT bytes)
{
    if (ptr == NULL || bytes == 0)
    {
        return 0;
    }
    uLong crc = crc32(0L, Z_NULL, 0);
    FileIOSizeT remaining = bytes;
    const unsigned char *cur = ptr;
    while (remaining > 0)
    {
        uInt chunk = (remaining > UINT_MAX) ? UINT_MAX : (uInt)remaining;
        crc = crc32(crc, (const Bytef *)cur, chunk);
        cur += chunk;
        remaining -= chunk;
    }
    return (uint32_t)crc;
}

static inline bool LoadLe32FromBytes(const unsigned char *src, uint32_t *valueOut)
{
    if (src == NULL || valueOut == NULL)
    {
        return false;
    }
    *valueOut = ((uint32_t)src[0]) | ((uint32_t)src[1] << 8u) | ((uint32_t)src[2] << 16u) | ((uint32_t)src[3] << 24u);
    return true;
}

//
// Validate stage-1 AES buffer layout and log fingerprints.
// Search tag:
//   [DDS-DPK-STAGE1-AES-LAYOUT]
//
static inline bool ValidateAndLogStage1AesLayout(struct PerSlotContext *SlotContext, const char *inputPtr,
                                                 FileIOSizeT inputBytes, const char *outputPtr, FileIOSizeT outputBytes,
                                                 bool forceLog, const char *reason)
{
    unsigned reqId = 0;
    FileIdT fileId = 0;
    FileSizeT fileOffset = 0;
    if (SlotContext && SlotContext->Ctx && SlotContext->Ctx->Response)
    {
        reqId = (unsigned)SlotContext->Ctx->Response->RequestId;
    }
    if (SlotContext && SlotContext->Ctx && SlotContext->Ctx->Request)
    {
        fileId = SlotContext->Ctx->Request->FileId;
        fileOffset = SlotContext->Ctx->Request->Offset;
    }

    bool ptrsOk = (SlotContext != NULL) && (inputPtr != NULL) && (outputPtr != NULL);
    bool sizeContractOk = false;
    if (inputBytes >= DDS_STAGE1_AES_TAG_BYTES)
    {
        sizeContractOk = ((inputBytes - DDS_STAGE1_AES_TAG_BYTES) == outputBytes);
    }

    const char *moduleStart = NULL;
    const unsigned char *ivPtr = NULL;
    const unsigned char *cipherPtr = NULL;
    const unsigned char *tagPtr = NULL;
    if (ptrsOk)
    {
        moduleStart = (const char *)((uintptr_t)inputPtr - (uintptr_t)DDS_STAGE1_AES_PREFIX_BYTES);
        ivPtr = (const unsigned char *)(moduleStart + DDS_STAGE1_AES_LEN_FIELD_BYTES);
        cipherPtr = (const unsigned char *)inputPtr;
        if (inputBytes >= DDS_STAGE1_AES_TAG_BYTES)
        {
            tagPtr = (const unsigned char *)(inputPtr + (inputBytes - DDS_STAGE1_AES_TAG_BYTES));
        }
    }

    bool lenFieldOk = false;
    uint32_t encodedLen = 0;
    if (moduleStart != NULL)
    {
        lenFieldOk = LoadLe32FromBytes((const unsigned char *)moduleStart, &encodedLen);
    }

    bool encodedLenMatches = false;
    bool moduleSizeOk = false;
    if (lenFieldOk)
    {
        FileIOSizeT expectedEncoded = DDS_STAGE1_AES_IV_BYTES + inputBytes;
        encodedLenMatches = ((FileIOSizeT)encodedLen == expectedEncoded);
        if ((FileIOSizeT)encodedLen >= DDS_STAGE1_AES_IV_BYTES &&
            ((FileIOSizeT)encodedLen <= (FileIOSizeT)(UINT32_MAX - DDS_STAGE1_AES_LEN_FIELD_BYTES)))
        {
            moduleSizeOk = true;
        }
    }

    FileIOSizeT moduleRingOff = 0;
    FileIOSizeT inputRingOff = 0;
    FileIOSizeT outputRingOff = 0;
    bool moduleInRing = false;
    bool inputInRing = false;
    bool outputInRing = false;
    FileIOSizeT moduleBytes = 0;
    if (moduleStart != NULL && lenFieldOk && moduleSizeOk)
    {
        moduleBytes = (FileIOSizeT)DDS_STAGE1_AES_LEN_FIELD_BYTES + (FileIOSizeT)encodedLen;
        moduleInRing = BufferRangeWithinResponseRing(SlotContext, moduleStart, moduleBytes, &moduleRingOff);
    }
    if (ptrsOk)
    {
        inputInRing = BufferRangeWithinResponseRing(SlotContext, inputPtr, inputBytes, &inputRingOff);
        outputInRing = BufferRangeWithinResponseRing(SlotContext, outputPtr, outputBytes, &outputRingOff);
    }

    bool allOk = ptrsOk && sizeContractOk && lenFieldOk && moduleSizeOk && encodedLenMatches && moduleInRing &&
                 inputInRing && outputInRing;
    if (forceLog || !allOk)
    {
        uint32_t moduleFp = (moduleInRing && moduleStart != NULL && moduleBytes > 0) ?
            Crc32Fingerprint((const unsigned char *)moduleStart, moduleBytes) : 0;
        uint32_t ivFp = (moduleInRing && ivPtr != NULL) ? Crc32Fingerprint(ivPtr, DDS_STAGE1_AES_IV_BYTES) : 0;
        uint32_t cipherFp = (inputInRing && cipherPtr != NULL && inputBytes >= DDS_STAGE1_AES_TAG_BYTES) ?
            Crc32Fingerprint(cipherPtr, inputBytes - DDS_STAGE1_AES_TAG_BYTES) : 0;
        uint32_t tagFp = (inputInRing && tagPtr != NULL) ? Crc32Fingerprint(tagPtr, DDS_STAGE1_AES_TAG_BYTES) : 0;
        unsigned stageInputOff = 0;
        unsigned stageInputLen = 0;
        if (SlotContext != NULL)
        {
            stageInputOff = (unsigned)SlotContext->StageInputOffsets[0];
            stageInputLen = (unsigned)SlotContext->StageInputBytes[0];
        }

        fprintf(stderr,
                "[DDS-DPK-STAGE1-AES-LAYOUT] reqId=%u status=%s reason=%s fileId=%u fileOffset=%llu "
                "inputBytes=%u outputBytes=%u stageInputOff=%u stageInputLen=%u sizeContract=%d "
                "encoded=%u encodedExpected=%u moduleSizeOk=%d encodedMatch=%d "
                "moduleInRing=%d inputInRing=%d outputInRing=%d "
                "moduleOff=%u inputOff=%u outputOff=%u moduleBytes=%u "
                "fp_module=0x%08x fp_iv=0x%08x fp_cipher=0x%08x fp_tag=0x%08x\n",
                reqId, allOk ? "PASS" : "FAIL", reason ? reason : "n/a", (unsigned)fileId, (unsigned long long)fileOffset,
                (unsigned)inputBytes, (unsigned)outputBytes, stageInputOff, stageInputLen, (int)sizeContractOk,
                (unsigned)encodedLen, (unsigned)(DDS_STAGE1_AES_IV_BYTES + inputBytes), (int)moduleSizeOk,
                (int)encodedLenMatches,
                (int)moduleInRing, (int)inputInRing, (int)outputInRing, (unsigned)moduleRingOff, (unsigned)inputRingOff,
                (unsigned)outputRingOff, (unsigned)moduleBytes, moduleFp, ivFp, cipherFp, tagFp);
    }
    return allOk;
}

//
// Failure-path-only software AES-GCM decrypt check for stage-1.
// Search tag:
//   [DDS-DPK-STAGE1-SW-AES-CHECK]
//
static inline void RunStage1SoftwareAesCheck(struct PerSlotContext *SlotContext, const char *inputPtr,
                                             FileIOSizeT inputBytes, FileIOSizeT outputBytes, const char *reason)
{
    unsigned reqId = 0;
    if (SlotContext && SlotContext->Ctx && SlotContext->Ctx->Response)
    {
        reqId = (unsigned)SlotContext->Ctx->Response->RequestId;
    }
    if (inputPtr == NULL || inputBytes <= DDS_STAGE1_AES_TAG_BYTES || outputBytes == 0)
    {
        fprintf(stderr,
                "[DDS-DPK-STAGE1-SW-AES-CHECK] reqId=%u status=SKIP reason=invalid-args "
                "inputPtr=%p inputBytes=%u outputBytes=%u trigger=%s\n",
                reqId, (const void *)inputPtr, (unsigned)inputBytes, (unsigned)outputBytes, reason ? reason : "n/a");
        return;
    }
    if (inputBytes > INT_MAX)
    {
        fprintf(stderr,
                "[DDS-DPK-STAGE1-SW-AES-CHECK] reqId=%u status=SKIP reason=input-too-large "
                "inputBytes=%u trigger=%s\n",
                reqId, (unsigned)inputBytes, reason ? reason : "n/a");
        return;
    }

    uint8_t key[32];
    uint8_t keySize = 0;
    if (dpk_embed_copy_global_aes_key(key, (uint8_t)sizeof(key), &keySize) != 0)
    {
        fprintf(stderr,
                "[DDS-DPK-STAGE1-SW-AES-CHECK] reqId=%u status=SKIP reason=key-unavailable trigger=%s\n",
                reqId, reason ? reason : "n/a");
        return;
    }
    if (keySize != 16 && keySize != 32)
    {
        fprintf(stderr,
                "[DDS-DPK-STAGE1-SW-AES-CHECK] reqId=%u status=FAIL reason=bad-key-size keyBytes=%u trigger=%s\n",
                reqId, (unsigned)keySize, reason ? reason : "n/a");
        return;
    }

    FileIOSizeT cipherBytes = inputBytes - DDS_STAGE1_AES_TAG_BYTES;
    const unsigned char *ivPtr = (const unsigned char *)((uintptr_t)inputPtr - (uintptr_t)DDS_STAGE1_AES_IV_BYTES);
    const unsigned char *cipherPtr = (const unsigned char *)inputPtr;
    const unsigned char *tagPtr = (const unsigned char *)(inputPtr + cipherBytes);
    FileIOSizeT ivRingOff = 0;
    FileIOSizeT inputRingOff = 0;
    bool ivInRing = BufferRangeWithinResponseRing(SlotContext, (const char *)ivPtr, DDS_STAGE1_AES_IV_BYTES, &ivRingOff);
    bool inputInRing = BufferRangeWithinResponseRing(SlotContext, inputPtr, inputBytes, &inputRingOff);
    if (!ivInRing || !inputInRing)
    {
        fprintf(stderr,
                "[DDS-DPK-STAGE1-SW-AES-CHECK] reqId=%u status=SKIP reason=buffer-out-of-ring "
                "ivInRing=%d inputInRing=%d ivOff=%u inputOff=%u trigger=%s\n",
                reqId, (int)ivInRing, (int)inputInRing, (unsigned)ivRingOff, (unsigned)inputRingOff,
                reason ? reason : "n/a");
        return;
    }

    if (cipherBytes > INT_MAX)
    {
        fprintf(stderr,
                "[DDS-DPK-STAGE1-SW-AES-CHECK] reqId=%u status=SKIP reason=cipher-too-large cipherBytes=%u trigger=%s\n",
                reqId, (unsigned)cipherBytes, reason ? reason : "n/a");
        return;
    }

    unsigned char *plain = (unsigned char *)malloc((size_t)cipherBytes);
    if (plain == NULL)
    {
        fprintf(stderr,
                "[DDS-DPK-STAGE1-SW-AES-CHECK] reqId=%u status=SKIP reason=malloc-failed cipherBytes=%u trigger=%s\n",
                reqId, (unsigned)cipherBytes, reason ? reason : "n/a");
        return;
    }

    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    int rcInit = 0;
    int rcIvLen = 0;
    int rcInitKey = 0;
    int rcUpdate = 0;
    int rcSetTag = 0;
    int rcFinal = 0;
    int out1 = 0;
    int out2 = 0;
    bool ctxCreated = (ctx != NULL);
    if (ctx != NULL)
    {
        const EVP_CIPHER *cipher = (keySize == 16) ? EVP_aes_128_gcm() : EVP_aes_256_gcm();
        rcInit = EVP_DecryptInit_ex(ctx, cipher, NULL, NULL, NULL);
        rcIvLen = EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, (int)DDS_STAGE1_AES_IV_BYTES, NULL);
        rcInitKey = EVP_DecryptInit_ex(ctx, NULL, NULL, key, ivPtr);
        // Match BF3 kernel contract: no AAD, input buffer is [ciphertext||tag], tag provided via GCM ctrl.
        rcUpdate = EVP_DecryptUpdate(ctx, plain, &out1, cipherPtr, (int)cipherBytes);
        rcSetTag = EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, (int)DDS_STAGE1_AES_TAG_BYTES, (void *)tagPtr);
        rcFinal = EVP_DecryptFinal_ex(ctx, plain + out1, &out2);
        EVP_CIPHER_CTX_free(ctx);
    }

    int produced = out1 + ((rcFinal == 1) ? out2 : 0);
    bool pass = ctxCreated && (rcInit == 1) && (rcIvLen == 1) && (rcInitKey == 1) && (rcUpdate == 1) &&
                (rcSetTag == 1) && (rcFinal == 1) && ((FileIOSizeT)produced == outputBytes);
    uint32_t keyFp = Crc32Fingerprint(key, keySize);
    uint32_t ivFp = Crc32Fingerprint(ivPtr, DDS_STAGE1_AES_IV_BYTES);
    uint32_t cipherFp = Crc32Fingerprint(cipherPtr, cipherBytes);
    uint32_t tagFp = Crc32Fingerprint(tagPtr, DDS_STAGE1_AES_TAG_BYTES);
    uint32_t plainFp = pass ? Crc32Fingerprint(plain, (FileIOSizeT)produced) : 0;

    fprintf(stderr,
            "[DDS-DPK-STAGE1-SW-AES-CHECK] reqId=%u status=%s trigger=%s keyBytes=%u "
            "rc={init:%d ivlen:%d initkey:%d update:%d settag:%d final:%d} "
            "inputBytes=%u cipherBytes=%u outputExpected=%u produced=%d "
            "fp_key=0x%08x fp_iv=0x%08x fp_cipher=0x%08x fp_tag=0x%08x fp_plain=0x%08x\n",
            reqId, pass ? "PASS" : "FAIL", reason ? reason : "n/a", (unsigned)keySize, rcInit, rcIvLen, rcInitKey,
            rcUpdate, rcSetTag, rcFinal, (unsigned)inputBytes, (unsigned)cipherBytes, (unsigned)outputBytes, produced,
            keyFp, ivFp, cipherFp, tagFp, plainFp);

    free(plain);
}

//
// Failure-path-only diagnostic: verify whether the exact stage-2 input slice is a valid
// raw-deflate stream using software inflate (zlib, windowBits = -MAX_WBITS).
//
// This helps separate:
// - "DPU/DOCA decompress rejected otherwise valid deflate"
// from
// - "stage-2 input bytes are malformed/corrupted".
//
// Log prefix is intentionally stable/searchable:
//   [DDS-DPK-SW-RAW-DEFLATE-CHECK]
//
static inline void RunStage2RawDeflateSoftwareCheck(struct PerSlotContext *SlotContext, const char *inputPtr,
                                                    FileIOSizeT inputBytes, FileIOSizeT expectedOutputBytes)
{
    unsigned reqId = 0;
    if (SlotContext && SlotContext->Ctx && SlotContext->Ctx->Response)
    {
        reqId = (unsigned)SlotContext->Ctx->Response->RequestId;
    }
    if (inputPtr == NULL || inputBytes == 0 || expectedOutputBytes == 0)
    {
        fprintf(stderr,
                "[DDS-DPK-SW-RAW-DEFLATE-CHECK] reqId=%u status=SKIP reason=invalid-args "
                "inputPtr=%p inputBytes=%u expectedOut=%u\n",
                reqId, (const void *)inputPtr, (unsigned)inputBytes, (unsigned)expectedOutputBytes);
        return;
    }
    if (inputBytes > UINT_MAX || expectedOutputBytes > UINT_MAX)
    {
        fprintf(stderr,
                "[DDS-DPK-SW-RAW-DEFLATE-CHECK] reqId=%u status=SKIP reason=size-overflow "
                "inputBytes=%u expectedOut=%u\n",
                reqId, (unsigned)inputBytes, (unsigned)expectedOutputBytes);
        return;
    }

    unsigned char *tmpOut = (unsigned char *)malloc((size_t)expectedOutputBytes);
    if (tmpOut == NULL)
    {
        fprintf(stderr,
                "[DDS-DPK-SW-RAW-DEFLATE-CHECK] reqId=%u status=SKIP reason=malloc-failed "
                "expectedOut=%u\n",
                reqId, (unsigned)expectedOutputBytes);
        return;
    }

    z_stream zs;
    memset(&zs, 0, sizeof(zs));
    int initRc = inflateInit2(&zs, -MAX_WBITS);
    if (initRc != Z_OK)
    {
        fprintf(stderr,
                "[DDS-DPK-SW-RAW-DEFLATE-CHECK] reqId=%u status=INIT-FAIL zrc=%d (%s)\n",
                reqId, initRc, zError(initRc));
        free(tmpOut);
        return;
    }

    zs.next_in = (Bytef *)inputPtr;
    zs.avail_in = (uInt)inputBytes;
    zs.next_out = (Bytef *)tmpOut;
    zs.avail_out = (uInt)expectedOutputBytes;
    int inflateRc = inflate(&zs, Z_FINISH);

    unsigned out0 = 0, out1 = 0, out2 = 0, out3 = 0;
    if (zs.total_out >= 1)
    {
        out0 = (unsigned)tmpOut[0];
    }
    if (zs.total_out >= 2)
    {
        out1 = (unsigned)tmpOut[1];
    }
    if (zs.total_out >= 3)
    {
        out2 = (unsigned)tmpOut[2];
    }
    if (zs.total_out >= 4)
    {
        out3 = (unsigned)tmpOut[3];
    }

    unsigned long totalIn = (unsigned long)zs.total_in;
    unsigned long totalOut = (unsigned long)zs.total_out;
    int endRc = inflateEnd(&zs);

    // "PASS" requires complete stream consumption and exact expected output size.
    const bool pass = (inflateRc == Z_STREAM_END) &&
                      (totalIn == (unsigned long)inputBytes) &&
                      (totalOut == (unsigned long)expectedOutputBytes);
    fprintf(stderr,
            "[DDS-DPK-SW-RAW-DEFLATE-CHECK] reqId=%u status=%s zrc=%d (%s) end_rc=%d "
            "in=%u consumed=%lu out_expected=%u produced=%lu out_head=%02x %02x %02x %02x\n",
            reqId, pass ? "PASS" : "FAIL", inflateRc, zError(inflateRc), endRc, (unsigned)inputBytes, totalIn,
            (unsigned)expectedOutputBytes, totalOut, out0, out1, out2, out3);

    free(tmpOut);
}

//
// Submit one DPK stage asynchronously on the target worker submit-thread index.
// Completion is drained by per-worker SPDK pollers (StageCompletionPoller).
//
static inline bool SubmitStageWithDPKAsync(struct PerSlotContext *SlotContext, uint16_t stageIndex,
                                           uint32_t submitWorkerIndex, char *inputPtr, FileIOSizeT inputBytes,
                                           char *outputPtr, FileIOSizeT outputBytes)
{
    if (SlotContext == NULL || inputPtr == NULL || outputPtr == NULL || inputBytes > UINT32_MAX || outputBytes > UINT32_MAX)
    {
        return false;
    }

    if (!EnsureStageTaskBound(SlotContext, stageIndex))
    {
        return false;
    }

    FileIOSizeT inputRingOffset = 0;
    FileIOSizeT outputRingOffset = 0;
    char *ctxRingBase = NULL;
    FileIOSizeT ctxRingBytes = 0;
    if (SlotContext->Ctx)
    {
        ctxRingBase = SlotContext->Ctx->ResponseRingBase;
        ctxRingBytes = SlotContext->Ctx->ResponseRingBytes;
    }
    if (SlotContext->ResponseRingBase == NULL || SlotContext->ResponseRingBytes == 0)
    {
        fprintf(stderr,
                "%s [error]: stage%u slot ring descriptor is invalid reqId=%u slotRingBase=%p slotRingBytes=%u "
                "ctxRingBase=%p ctxRingBytes=%u\n",
                __func__, (unsigned)stageIndex + 1u, (unsigned)SlotContext->Ctx->Response->RequestId,
                SlotContext->ResponseRingBase, (unsigned)SlotContext->ResponseRingBytes, ctxRingBase,
                (unsigned)ctxRingBytes);
    }
    bool inputInRing = BufferRangeWithinResponseRing(SlotContext, inputPtr, inputBytes, &inputRingOffset);
    bool outputInRing = BufferRangeWithinResponseRing(SlotContext, outputPtr, outputBytes, &outputRingOffset);
    if (!inputInRing || !outputInRing)
    {
        fprintf(stderr,
                "%s [error]: stage%u buffer out of response-ring bounds reqId=%u "
                "slotRingBase=%p slotRingBytes=%u ctxRingBase=%p ctxRingBytes=%u "
                "input=%p/%u inOff=%u inputInRing=%d output=%p/%u outOff=%u outputInRing=%d\n",
                __func__, (unsigned)stageIndex + 1u, (unsigned)SlotContext->Ctx->Response->RequestId,
                SlotContext->ResponseRingBase, (unsigned)SlotContext->ResponseRingBytes, ctxRingBase,
                (unsigned)ctxRingBytes, inputPtr, (unsigned)inputBytes, (unsigned)inputRingOffset, (int)inputInRing,
                outputPtr, (unsigned)outputBytes, (unsigned)outputRingOffset, (int)outputInRing);
        return false;
    }
    if (inputBytes > 0 && outputBytes > 0)
    {
        uintptr_t inStart = (uintptr_t)inputPtr;
        uintptr_t inEnd = inStart + (uintptr_t)inputBytes;
        uintptr_t outStart = (uintptr_t)outputPtr;
        uintptr_t outEnd = outStart + (uintptr_t)outputBytes;
        if (!(inEnd <= outStart || outEnd <= inStart))
        {
            fprintf(stderr,
                    "%s [warn]: stage%u input/output overlap reqId=%u input=%p/%u output=%p/%u\n", __func__,
                    (unsigned)stageIndex + 1u, (unsigned)SlotContext->Ctx->Response->RequestId, inputPtr,
                    (unsigned)inputBytes, outputPtr, (unsigned)outputBytes);
        }
    }

    dpk_embed_task_handle *task = (dpk_embed_task_handle *)SlotContext->DpkStageTaskHandles[stageIndex];
    if (stageIndex == 0)
    {
        // Stage-1 layout sanity check before DOCA submit.
        // We keep this lightweight and only emit logs on mismatch.
        if (!ValidateAndLogStage1AesLayout(SlotContext, inputPtr, inputBytes, outputPtr, outputBytes, false,
                                           "pre-submit"))
        {
            fprintf(stderr, "%s [error]: stage1 AES layout validation failed before submit reqId=%u\n", __func__,
                    (unsigned)SlotContext->Ctx->Response->RequestId);
            return false;
        }

        // DDS read2 AES contract:
        // stage-1 input window is [ciphertext||tag], and the 12-byte nonce/IV must be
        // located immediately before that window in stage0 bytes.
        // This layout is provided by the caller via stage input offsets/lengths.
        uint64_t keyShm = 0;
        uint8_t keySize = 0;
        if (dpk_embed_get_global_aes_key(&keyShm, &keySize) != 0)
        {
            fprintf(stderr, "%s [error]: AES key is not configured for stage1 reqId=%u\n", __func__,
                    (unsigned)SlotContext->Ctx->Response->RequestId);
            return false;
        }
        if (dpk_embed_task_set_aes_key_shm(task, keyShm, keySize) != 0)
        {
            fprintf(stderr, "%s [error]: dpk_embed_task_set_aes_key_shm failed reqId=%u\n", __func__,
                    (unsigned)SlotContext->Ctx->Response->RequestId);
            return false;
        }
    }

    // Original path (kept for reference) submitted on owner worker index:
    // if (dpk_embed_task_submit_and_wait(task, SlotContext->OwnerWorkerIndex, ... ) != 0)
    //
    // Updated: submit on the stage target worker index. The task was preprepared per worker
    // at backend startup, so submit thread id must match the execution worker.
    int submitRet = dpk_embed_task_submit_async(task, submitWorkerIndex, (uint64_t)(uintptr_t)inputPtr,
                                                (uint32_t)inputBytes, (uint64_t)(uintptr_t)outputPtr,
                                                (uint32_t)outputBytes);
    if (submitRet != 0)
    {
        if (stageIndex == 0)
        {
            // Failure-path diagnostics for decrypt stage:
            // 1) re-log strict layout invariants
            // 2) run software AES-GCM on the exact same input slice and key
            //    so we can prove whether payload+key are valid while DOCA fails.
            (void)ValidateAndLogStage1AesLayout(SlotContext, inputPtr, inputBytes, outputPtr, outputBytes, true,
                                                "doca-submit-failed");
            RunStage1SoftwareAesCheck(SlotContext, inputPtr, inputBytes, outputBytes, "doca-submit-failed");
        }
        if (stageIndex == 1)
        {
            if (inputBytes >= 4)
            {
                // Additional diagnostics for read2 stage-2 failures:
                // - stage2 window contract (offset/length) as consumed by this request
                // - stage1 output head/footer bytes to verify decrypt output resembles gzip payload
                // Defensive diagnostics: stage-2 expects raw deflate bytes. If these begin with 0x1f8b,
                // caller is likely passing full GZIP payload (header+deflate+footer) instead of deflate body.
                const unsigned char *in = (const unsigned char *)inputPtr;
                FileIOSizeT stage2Off = SlotContext->StageInputOffsets[1];
                FileIOSizeT stage2Len = SlotContext->StageInputBytes[1];
                FileIOSizeT stage1Bytes = SlotContext->StageSizes[0];
                const unsigned char *stage1 = (const unsigned char *)SlotContext->StageBuffers[0].FirstAddr;
                FileIdT fileId = 0;
                FileSizeT fileOffset = 0;
                FileIOSizeT logicalReqBytes = 0;
                if (SlotContext->Ctx && SlotContext->Ctx->Request)
                {
                    fileId = SlotContext->Ctx->Request->FileId;
                    fileOffset = SlotContext->Ctx->Request->Offset;
                    logicalReqBytes = SlotContext->Ctx->LogicalBytes;
                }
                fprintf(stderr,
                        "[DDS-DPK-STAGE2-HW-FAIL-CONTEXT] reqId=%u fileId=%u fileOffset=%llu "
                        "logicalReqBytes=%u logicalIssued=%u copyStart=%u "
                        "stage1Bytes=%u stage2WindowOff=%u stage2WindowLen=%u "
                        "inputRingOff=%u outputRingOff=%u\n",
                        (unsigned)SlotContext->Ctx->Response->RequestId, (unsigned)fileId,
                        (unsigned long long)fileOffset, (unsigned)logicalReqBytes,
                        (unsigned)SlotContext->LogicalBytesIssued, (unsigned)SlotContext->CopyStart,
                        (unsigned)stage1Bytes, (unsigned)stage2Off, (unsigned)stage2Len,
                        (unsigned)inputRingOffset, (unsigned)outputRingOffset);
                if (stage1 != NULL && stage1Bytes >= 4)
                {
                    if (stage1Bytes >= 8)
                    {
                        fprintf(stderr,
                                "%s [error]: stage2 window reqId=%u off=%u len=%u stage1Bytes=%u "
                                "stage1 head=%02x %02x %02x %02x tail=%02x %02x %02x %02x\n",
                                __func__, (unsigned)SlotContext->Ctx->Response->RequestId, (unsigned)stage2Off,
                                (unsigned)stage2Len, (unsigned)stage1Bytes, (unsigned)stage1[0], (unsigned)stage1[1],
                                (unsigned)stage1[2], (unsigned)stage1[3], (unsigned)stage1[stage1Bytes - 4],
                                (unsigned)stage1[stage1Bytes - 3], (unsigned)stage1[stage1Bytes - 2],
                                (unsigned)stage1[stage1Bytes - 1]);
                    }
                    else
                    {
                        fprintf(stderr,
                                "%s [error]: stage2 window reqId=%u off=%u len=%u stage1Bytes=%u "
                                "stage1 head=%02x %02x %02x %02x\n",
                                __func__, (unsigned)SlotContext->Ctx->Response->RequestId, (unsigned)stage2Off,
                                (unsigned)stage2Len, (unsigned)stage1Bytes, (unsigned)stage1[0], (unsigned)stage1[1],
                                (unsigned)stage1[2], (unsigned)stage1[3]);
                    }
                }
                fprintf(stderr,
                        "%s [error]: stage2 input head bytes=%02x %02x %02x %02x tail bytes=%02x %02x %02x %02x\n",
                        __func__, (unsigned)in[0], (unsigned)in[1], (unsigned)in[2], (unsigned)in[3],
                        (unsigned)in[inputBytes - 4], (unsigned)in[inputBytes - 3], (unsigned)in[inputBytes - 2],
                        (unsigned)in[inputBytes - 1]);
                // Additional software cross-check on failure only (not hot path).
                RunStage2RawDeflateSoftwareCheck(SlotContext, inputPtr, inputBytes, outputBytes);
            }
        }
        fprintf(stderr,
                "%s [error]: dpk_embed_task_submit_async failed stage=%u reqId=%u submitWorker=%u "
                "targetWorker=%u ownerWorker=%u slotPos=%d inputBytes=%u outputBytes=%u inOff=%u outOff=%u "
                "inAlign64=%u outAlign64=%u\n",
                __func__, (unsigned)stageIndex, (unsigned)SlotContext->Ctx->Response->RequestId,
                (unsigned)submitWorkerIndex, (unsigned)SlotContext->StageTasks[stageIndex].TargetWorkerIndex,
                (unsigned)SlotContext->OwnerWorkerIndex, SlotContext->Position, (unsigned)inputBytes, (unsigned)outputBytes,
                (unsigned)inputRingOffset, (unsigned)outputRingOffset,
                (unsigned)((uintptr_t)inputPtr & (uintptr_t)63), (unsigned)((uintptr_t)outputPtr & (uintptr_t)63));
        return false;
    }
    return true;
}

//
// Poll one asynchronously submitted stage task for completion.
// Returns true when poll state was obtained; IsDone/IsSuccess describe completion outcome.
//
static inline bool CheckStageWithDPKAsync(struct PerSlotContext *SlotContext, uint16_t stageIndex,
                                          uint32_t submitWorkerIndex, FileIOSizeT expectedOutputBytes,
                                          FileIOSizeT *actualOutBytes, bool *isDone, bool *isSuccess)
{
    if (actualOutBytes)
    {
        *actualOutBytes = 0;
    }
    if (isDone)
    {
        *isDone = false;
    }
    if (isSuccess)
    {
        *isSuccess = false;
    }
    if (SlotContext == NULL || actualOutBytes == NULL || isDone == NULL || isSuccess == NULL)
    {
        return false;
    }
    if (!EnsureStageTaskBound(SlotContext, stageIndex))
    {
        *isDone = true;
        *isSuccess = false;
        return false;
    }

    dpk_embed_task_handle *task = (dpk_embed_task_handle *)SlotContext->DpkStageTaskHandles[stageIndex];
    uint32_t actualOut = 0;
    uint8_t completionState = DPK_EMBED_TASK_COMPLETION_PENDING;
    int checkRet = dpk_embed_task_check_completion(task, submitWorkerIndex, &actualOut, &completionState);
    if (checkRet != 0)
    {
        fprintf(stderr,
                "%s [error]: dpk_embed_task_check_completion failed stage=%u reqId=%u submitWorker=%u expectedOut=%u\n",
                __func__, (unsigned)stageIndex, (unsigned)SlotContext->Ctx->Response->RequestId,
                (unsigned)submitWorkerIndex, (unsigned)expectedOutputBytes);
        *isDone = true;
        *isSuccess = false;
        return false;
    }

    if (completionState == DPK_EMBED_TASK_COMPLETION_PENDING)
    {
        *isDone = false;
        *isSuccess = false;
        return true;
    }

    *isDone = true;
    if (completionState != DPK_EMBED_TASK_COMPLETION_SUCCESS)
    {
        *isSuccess = false;
        fprintf(stderr, "%s [error]: stage completion returned error stage=%u reqId=%u submitWorker=%u\n", __func__,
                (unsigned)stageIndex, (unsigned)SlotContext->Ctx->Response->RequestId, (unsigned)submitWorkerIndex);
        return true;
    }

    if (actualOut > expectedOutputBytes)
    {
        fprintf(stderr,
                "%s [error]: dpk actual_out_size exceeds output buffer stage=%u actual=%u expectedOut=%u reqId=%u\n",
                __func__, (unsigned)stageIndex, (unsigned)actualOut, (unsigned)expectedOutputBytes,
                (unsigned)SlotContext->Ctx->Response->RequestId);
        *isSuccess = false;
        return true;
    }
    *actualOutBytes = (FileIOSizeT)actualOut;
    *isSuccess = true;
    return true;
}

//
// Fill a ring-resident splittable buffer with a byte value.
// Used by the read2 stage stubs to simulate async offload completion.
//
static inline void FillSplittableBuffer(SplittableBufferT *Buffer, unsigned char Value)
{
    if (!Buffer || Buffer->TotalSize == 0)
    {
        return;
    }
    memset(Buffer->FirstAddr, (int)Value, (size_t)Buffer->FirstSize);
    if (Buffer->SecondAddr)
    {
        size_t secondSize = (size_t)(Buffer->TotalSize - Buffer->FirstSize);
        memset(Buffer->SecondAddr, (int)Value, secondSize);
    }
}

//
// Slice a splittable buffer by offset/size (no copy).
// Returns a view into the original buffer for the requested range.
//
static inline void SliceSplittableBuffer(const SplittableBufferT *Buffer, FileIOSizeT Offset, FileIOSizeT SliceBytes,
                                         SplittableBufferT *Out)
{
    if (!Out)
    {
        return;
    }
    memset(Out, 0, sizeof(*Out));
    if (!Buffer || Buffer->TotalSize == 0)
    {
        return;
    }
    if (Offset >= Buffer->TotalSize)
    {
        return;
    }
    if (Offset + SliceBytes > Buffer->TotalSize)
    {
        SliceBytes = Buffer->TotalSize - Offset;
    }
    Out->TotalSize = SliceBytes;
    if (SliceBytes == 0)
    {
        return;
    }

    const char *first = (const char *)Buffer->FirstAddr;
    const char *second = (const char *)Buffer->SecondAddr;
    if (Offset < Buffer->FirstSize)
    {
        Out->FirstAddr = (char *)(first + Offset);
        FileIOSizeT firstAvail = Buffer->FirstSize - Offset;
        if (SliceBytes <= firstAvail)
        {
            Out->FirstSize = SliceBytes;
            Out->SecondAddr = NULL;
        }
        else
        {
            Out->FirstSize = firstAvail;
            Out->SecondAddr = Buffer->SecondAddr;
        }
        return;
    }

    FileIOSizeT secondOffset = Offset - Buffer->FirstSize;
    if (second)
    {
        Out->FirstAddr = (char *)(second + secondOffset);
    }
    else
    {
        Out->FirstAddr = (char *)(first + Offset);
    }
    Out->FirstSize = SliceBytes;
    Out->SecondAddr = NULL;
}

//
// Copy Bytes from a source splittable buffer into a destination splittable buffer.
// Assumes both buffers have at least Bytes available.
//
static inline void CopySplittableBuffer(const SplittableBufferT *Src, SplittableBufferT *Dst, FileIOSizeT Bytes)
{
    if (!Src || !Dst || Bytes == 0)
    {
        return;
    }
    const char *src1 = (const char *)Src->FirstAddr;
    size_t src1Size = (size_t)Src->FirstSize;
    const char *src2 = (const char *)Src->SecondAddr;
    size_t src2Size = (Src->SecondAddr != NULL) ? (size_t)(Src->TotalSize - Src->FirstSize) : 0;

    char *dst1 = (char *)Dst->FirstAddr;
    size_t dst1Size = (size_t)Dst->FirstSize;
    char *dst2 = (char *)Dst->SecondAddr;
    size_t dst2Size = (Dst->SecondAddr != NULL) ? (size_t)(Dst->TotalSize - Dst->FirstSize) : 0;

    size_t remaining = (size_t)Bytes;
    while (remaining > 0)
    {
        if (src1Size == 0 && src2Size > 0)
        {
            src1 = src2;
            src1Size = src2Size;
            src2Size = 0;
        }
        if (dst1Size == 0 && dst2Size > 0)
        {
            dst1 = dst2;
            dst1Size = dst2Size;
            dst2Size = 0;
        }
        if (src1Size == 0 || dst1Size == 0)
        {
            break;
        }
        size_t chunk = remaining;
        if (chunk > src1Size)
        {
            chunk = src1Size;
        }
        if (chunk > dst1Size)
        {
            chunk = dst1Size;
        }
        memcpy(dst1, src1, chunk);
        src1 += chunk;
        dst1 += chunk;
        src1Size -= chunk;
        dst1Size -= chunk;
        remaining -= chunk;
    }
}

//
// Publish a read failure if the response is still pending.
//
static inline void PublishReadFailure(struct PerSlotContext *SlotContext, ErrorCodeT Code)
{
    BuffMsgB2FAckHeader *resp = SlotContext->Ctx->Response;
    ErrorCodeT cur = DDS_ATOMIC_ERRORCODE_LOAD(&resp->Result, DDS_ATOMIC_ORDER_ACQUIRE);
    if (cur != DDS_ERROR_CODE_IO_PENDING)
    {
        return;
    }
    resp->BytesServiced = 0;
    resp->LogicalBytes = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    resp->OffloadReadTimeNs = SlotContext->OffloadReadTimeNs;
    resp->OffloadStage1TimeNs = SlotContext->OffloadStage1TimeNs;
    resp->OffloadStage2TimeNs = SlotContext->OffloadStage2TimeNs;
#endif
    DDS_ATOMIC_ERRORCODE_STORE(&resp->Result, Code, DDS_ATOMIC_ORDER_RELEASE);
}

//
// Publish read success using the final stage/output sizing.
//
static inline void PublishReadSuccessFinal(struct PerSlotContext *SlotContext)
{
    BuffMsgB2FAckHeader *resp = SlotContext->Ctx->Response;
    ErrorCodeT cur = DDS_ATOMIC_ERRORCODE_LOAD(&resp->Result, DDS_ATOMIC_ORDER_ACQUIRE);
    if (cur != DDS_ERROR_CODE_IO_PENDING)
    {
        return;
    }
    resp->BytesServiced = SlotContext->BytesIssued;
    resp->LogicalBytes = SlotContext->LogicalBytesIssued;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    resp->OffloadReadTimeNs = SlotContext->OffloadReadTimeNs;
    resp->OffloadStage1TimeNs = SlotContext->OffloadStage1TimeNs;
    resp->OffloadStage2TimeNs = SlotContext->OffloadStage2TimeNs;
#endif
    DDS_ATOMIC_ERRORCODE_STORE(&resp->Result, DDS_ERROR_CODE_SUCCESS, DDS_ATOMIC_ORDER_RELEASE);
}

static inline void NotifyOwnerStageCompletion(struct Read2StageTaskCtx *task)
{
    if (task == NULL || task->SlotContext == NULL)
    {
        return;
    }
    if (task->SlotContext->OwnerWorkerThread == NULL)
    {
        fprintf(stderr, "%s [error]: missing owner thread stage=%u reqId=%u\n", __func__, (unsigned)task->StageIndex,
                (unsigned)task->SlotContext->Ctx->Response->RequestId);
        PublishReadFailure(task->SlotContext, DDS_ERROR_CODE_IO_FAILURE);
        return;
    }

    int ret = spdk_thread_send_msg(task->SlotContext->OwnerWorkerThread, StageCompleteOnOwner, task);
    if (ret)
    {
        fprintf(stderr, "%s [warn]: send_msg to owner failed (%d), retrying\n", __func__, ret);
        while (ret)
        {
            ret = spdk_thread_send_msg(task->SlotContext->OwnerWorkerThread, StageCompleteOnOwner, task);
        }
    }
}

static inline void CompleteStageAndNotifyOwner(struct Read2StageTaskCtx *task, bool success)
{
    if (task == NULL || task->SlotContext == NULL)
    {
        return;
    }
    struct PerSlotContext *SlotContext = task->SlotContext;
    uint16_t stageIndex = task->StageIndex;
    atomic_store_explicit(&SlotContext->StageSuccess[stageIndex], success, memory_order_relaxed);
    atomic_store_explicit(&SlotContext->StageDone[stageIndex], true, memory_order_relaxed);
    atomic_thread_fence(memory_order_release);
    NotifyOwnerStageCompletion(task);
}

static int StageCompletionPoller(void *Ctx)
{
    DataPlaneStagePollerCtx *pollerCtx = (DataPlaneStagePollerCtx *)Ctx;
    if (pollerCtx == NULL)
    {
        return SPDK_POLLER_IDLE;
    }
    uint32_t workerIndex = pollerCtx->WorkerIndex;
    struct WorkerStageInflightQueue *queue = GetWorkerStageInflightQueue(workerIndex);
    if (queue == NULL || !queue->Initialized)
    {
        return SPDK_POLLER_IDLE;
    }
    if (queue->ActiveHead < 0)
    {
        return SPDK_POLLER_IDLE;
    }

    int completed = 0;
    int prev = -1;
    int cur = queue->ActiveHead;
    while (cur >= 0)
    {
        struct StageInflightEntry *entry = &queue->Entries[cur];
        int next = entry->Next;
        struct Read2StageTaskCtx *task = entry->Task;
        bool removeEntry = false;
        bool stageSuccess = false;

        if (task == NULL || task->SlotContext == NULL)
        {
            fprintf(stderr, "%s [error]: null inflight task on worker=%u entry=%d\n", __func__, (unsigned)workerIndex,
                    cur);
            removeEntry = true;
        }
        else
        {
            struct PerSlotContext *SlotContext = task->SlotContext;
            uint16_t stageIndex = task->StageIndex;
            FileIOSizeT actualOut = 0;
            bool isDone = false;
            bool isSuccess = false;
            bool checkOk = CheckStageWithDPKAsync(SlotContext, stageIndex, workerIndex, entry->ExpectedOutputBytes,
                                                  &actualOut, &isDone, &isSuccess);
            if (!checkOk && !isDone)
            {
                isDone = true;
                isSuccess = false;
            }
            if (!isDone)
            {
                prev = cur;
                cur = next;
                continue;
            }

            stageSuccess = isSuccess;
            if (stageSuccess)
            {
                if (actualOut != entry->ExpectedOutputBytes)
                {
                    fprintf(stderr,
                            "%s [error]: stage%u actual output mismatch reqId=%u actual=%u expected=%u inputBytes=%u\n",
                            __func__, (unsigned)stageIndex + 1u, (unsigned)SlotContext->Ctx->Response->RequestId,
                            (unsigned)actualOut, (unsigned)entry->ExpectedOutputBytes, (unsigned)entry->InputBytes);
                    stageSuccess = false;
                }
                else
                {
                    SlotContext->StageSizes[stageIndex] = actualOut;
                    if (stageIndex + 1u == SlotContext->StageCount)
                    {
                        SlotContext->BytesIssued = actualOut;
                    }
                }
            }
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
            if (stageSuccess)
            {
                uint64_t stageElapsedNs = OffloadNowNs() - entry->SubmitStartNs;
                if (stageIndex == 0)
                {
                    SlotContext->OffloadStage1TimeNs = stageElapsedNs;
                }
                else if (stageIndex == 1)
                {
                    SlotContext->OffloadStage2TimeNs = stageElapsedNs;
                }
            }
#endif
            CompleteStageAndNotifyOwner(task, stageSuccess);
            removeEntry = true;
        }

        if (removeEntry)
        {
            if (prev < 0)
            {
                queue->ActiveHead = next;
            }
            else
            {
                queue->Entries[prev].Next = next;
            }
            if (queue->ActiveCount > 0)
            {
                queue->ActiveCount -= 1;
            }
            ReleaseReservedWorkerStageInflightEntry(queue, cur);
            completed += 1;
            cur = next;
            continue;
        }

        prev = cur;
        cur = next;
    }

    return completed > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

//
// Stage worker offload path: submit DPK stage asynchronously on ring-resident buffers.
// Completion is handled in StageCompletionPoller and then posted to owner.
//
static void StageStubWorker(void *Ctx)
{
    struct Read2StageTaskCtx *task = (struct Read2StageTaskCtx *)Ctx;
    struct PerSlotContext *SlotContext = task->SlotContext;
    uint16_t stageIndex = task->StageIndex;
    uint32_t submitWorkerIndex = task->TargetWorkerIndex;
    atomic_thread_fence(memory_order_acquire);
    FileIOSizeT inputBytes = SlotContext->StageInputBytes[stageIndex];
    FileIOSizeT inputOffset = SlotContext->StageInputOffsets[stageIndex];
    FileIOSizeT outputBytes = SlotContext->StageSizes[stageIndex];

    bool success = true;
    if (inputBytes == 0 && outputBytes == 0)
    {
        SlotContext->StageSizes[stageIndex] = 0;
        if (stageIndex + 1u == SlotContext->StageCount)
        {
            SlotContext->BytesIssued = 0;
        }
    }
    if (inputBytes > 0 || outputBytes > 0)
    {
        SplittableBufferT inputSlice = {};
        SplittableBufferT outputSlice = {};
        if (stageIndex == 0)
        {
            // Enforce AES layout precondition from caller-provided stage window:
            // stage input must include at least [ciphertext||16B tag], and there must be
            // 12 bytes immediately before stage input for nonce/IV.
            if (inputOffset < 12 || inputBytes < 16)
            {
                fprintf(stderr,
                        "%s [error]: stage1 AES window invalid for nonce/tag contract "
                        "(inputOff=%u inputLen=%u reqId=%u)\n",
                        __func__, (unsigned)inputOffset, (unsigned)inputBytes,
                        (unsigned)SlotContext->Ctx->Response->RequestId);
                success = false;
            }
            if (!SlotContext->DestBuffer)
            {
                fprintf(stderr, "%s [error]: stage1 missing dest buffer (reqId=%u)\n", __func__,
                        (unsigned)SlotContext->Ctx->Response->RequestId);
                success = false;
            }
            else if (SlotContext->CopyStart + inputOffset > SlotContext->DestBuffer->TotalSize ||
                     inputBytes > SlotContext->DestBuffer->TotalSize - (SlotContext->CopyStart + inputOffset))
            {
                fprintf(stderr,
                        "%s [error]: stage1 input slice exceeds dest buffer (copyStart=%u inputOff=%u inputLen=%u total=%u reqId=%u)\n",
                        __func__, (unsigned)SlotContext->CopyStart, (unsigned)inputOffset, (unsigned)inputBytes,
                        (unsigned)SlotContext->DestBuffer->TotalSize,
                        (unsigned)SlotContext->Ctx->Response->RequestId);
                success = false;
            }
            else
            {
                SliceSplittableBuffer(SlotContext->DestBuffer, SlotContext->CopyStart + inputOffset, inputBytes,
                                      &inputSlice);
            }
        }
        else
        {
            if (inputOffset > SlotContext->StageBuffers[stageIndex - 1].TotalSize ||
                inputBytes > SlotContext->StageBuffers[stageIndex - 1].TotalSize - inputOffset)
            {
                fprintf(stderr,
                        "%s [error]: stage%u input slice exceeds stage buffer (inputOff=%u inputLen=%u total=%u reqId=%u)\n",
                        __func__, (unsigned)stageIndex + 1u, (unsigned)inputOffset, (unsigned)inputBytes,
                        (unsigned)SlotContext->StageBuffers[stageIndex - 1].TotalSize,
                        (unsigned)SlotContext->Ctx->Response->RequestId);
                success = false;
            }
            else
            {
                SliceSplittableBuffer(&SlotContext->StageBuffers[stageIndex - 1], inputOffset, inputBytes, &inputSlice);
            }
        }

        if (success)
        {
            if (outputBytes > SlotContext->StageBuffers[stageIndex].TotalSize)
            {
                fprintf(stderr, "%s [error]: stage%u output exceeds buffer (output=%u total=%u reqId=%u)\n", __func__,
                        (unsigned)stageIndex + 1u, (unsigned)outputBytes,
                        (unsigned)SlotContext->StageBuffers[stageIndex].TotalSize,
                        (unsigned)SlotContext->Ctx->Response->RequestId);
                success = false;
            }
            else
            {
                SliceSplittableBuffer(&SlotContext->StageBuffers[stageIndex], 0, outputBytes, &outputSlice);
            }
        }

        if (success && (inputSlice.SecondAddr != NULL || outputSlice.SecondAddr != NULL))
        {
            fprintf(stderr, "%s [error]: stage%u got split input/output buffer (no-split required) reqId=%u\n", __func__,
                    (unsigned)stageIndex + 1u, (unsigned)SlotContext->Ctx->Response->RequestId);
            success = false;
        }

        if (success)
        {
            if (FS && submitWorkerIndex < (uint32_t)FS->WorkerThreadCount &&
                FS->WorkerThreads[submitWorkerIndex] != spdk_get_thread())
            {
                fprintf(stderr,
                        "%s [warn]: stage worker thread mismatch stage=%u reqId=%u submitWorker=%u "
                        "ownerWorker=%u\n",
                        __func__, (unsigned)stageIndex, (unsigned)SlotContext->Ctx->Response->RequestId,
                        (unsigned)submitWorkerIndex, (unsigned)SlotContext->OwnerWorkerIndex);
            }

            struct WorkerStageInflightQueue *queue = GetWorkerStageInflightQueue(submitWorkerIndex);
            int entryIndex = -1;
            if (queue == NULL || !queue->Initialized || queue->Poller == NULL)
            {
                fprintf(stderr,
                        "%s [error]: async stage queue is not initialized worker=%u stage=%u reqId=%u initialized=%d poller=%p\n",
                        __func__, (unsigned)submitWorkerIndex, (unsigned)stageIndex,
                        (unsigned)SlotContext->Ctx->Response->RequestId, queue ? (int)queue->Initialized : 0,
                        queue ? (void *)queue->Poller : NULL);
                success = false;
            }
            else if (!ReserveWorkerStageInflightEntry(queue, &entryIndex))
            {
                fprintf(stderr,
                        "%s [error]: async stage queue is full worker=%u stage=%u reqId=%u active=%u capacity=%u\n",
                        __func__, (unsigned)submitWorkerIndex, (unsigned)stageIndex,
                        (unsigned)SlotContext->Ctx->Response->RequestId, (unsigned)queue->ActiveCount,
                        (unsigned)DDS_DPK_STAGE_INFLIGHT_CAPACITY);
                success = false;
            }
            if (success)
            {
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
                uint64_t submitStartNs = OffloadNowNs();
#endif
                success = SubmitStageWithDPKAsync(SlotContext, stageIndex, submitWorkerIndex, inputSlice.FirstAddr,
                                                  inputBytes, outputSlice.FirstAddr, outputBytes);
                if (success)
                {
                    ActivateWorkerStageInflightEntry(queue, entryIndex, task, outputBytes, inputBytes
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
                                                     ,
                                                     submitStartNs
#endif
                    );
                    // Async path: completion poller will notify owner.
                    return;
                }
                ReleaseReservedWorkerStageInflightEntry(queue, entryIndex);
            }
        }
    }

    // Zero-byte fast path and all error paths complete immediately on this worker.
    CompleteStageAndNotifyOwner(task, success);
}

//
// Schedule a post-read stage on a worker thread (round-robin).
// Returns false if scheduling fails. Stage input windows are clamped by source bytes.
//
static bool ScheduleStage(struct PerSlotContext *SlotContext, uint16_t stageIndex)
{
    if (!IsRead2WithStages(SlotContext) || stageIndex >= SlotContext->StageCount)
    {
        return false;
    }
    if (!FS || !SlotContext->OwnerWorkerThread)
    {
        PublishReadFailure(SlotContext, DDS_ERROR_CODE_IO_FAILURE);
        return false;
    }

    FileIOSizeT sourceBytes =
        (stageIndex == 0) ? SlotContext->LogicalBytesIssued : SlotContext->StageSizes[stageIndex - 1];
    FileIOSizeT inputOffset = SlotContext->StageInputOffsets[stageIndex];
    FileIOSizeT inputBytes = SlotContext->StageInputLengths[stageIndex];
    if (inputOffset > sourceBytes)
    {
        // jason: If EOF clamp shortens source bytes, treat out-of-range offset as empty input.
        inputOffset = sourceBytes;
        inputBytes = 0;
    }
    else if (inputBytes > sourceBytes - inputOffset)
    {
        // jason: If EOF clamp shortens source bytes, truncate stage input to available bytes.
        inputBytes = sourceBytes - inputOffset;
    }
    SlotContext->StageInputOffsets[stageIndex] = inputOffset;
    SlotContext->StageInputBytes[stageIndex] = inputBytes;

    uint32_t workerIndex = SelectStageWorkerIndex();
    if (workerIndex >= (uint32_t)FS->WorkerThreadCount || FS->WorkerThreads[workerIndex] == NULL)
    {
        // jason: fall back to the owner worker if the selected worker is unavailable.
        printf("%s [error]: selected stage worker %u unavailable, using owner worker %u\n", __func__,
               (unsigned)workerIndex, (unsigned)SlotContext->OwnerWorkerIndex);
        workerIndex = SlotContext->OwnerWorkerIndex;
    }
    if (FS->WorkerThreads[workerIndex] == NULL)
    {
        PublishReadFailure(SlotContext, DDS_ERROR_CODE_IO_FAILURE);
        return false;
    }

    struct Read2StageTaskCtx *task = &SlotContext->StageTasks[stageIndex];
    task->SlotContext = SlotContext;
    task->StageIndex = stageIndex;
    task->TargetWorkerIndex = workerIndex;

    atomic_fetch_add_explicit(&SlotContext->CallbacksToRun, 1, memory_order_relaxed);
    atomic_thread_fence(memory_order_release);
    int ret = spdk_thread_send_msg(FS->WorkerThreads[workerIndex], StageStubWorker, task);
    if (ret)
    {
        fprintf(stderr, "%s [warn]: send_msg to worker %u failed (%d), retrying\n", __func__, (unsigned)workerIndex,
                ret);
        while (ret)
        {
            ret = spdk_thread_send_msg(FS->WorkerThreads[workerIndex], StageStubWorker, task);
        }
    }
    return true;
}

//
// Owner-thread stage completion: chain to the next stage or publish final success.
//
static void StageCompleteOnOwner(void *Ctx)
{
    struct Read2StageTaskCtx *task = (struct Read2StageTaskCtx *)Ctx;
    struct PerSlotContext *SlotContext = task->SlotContext;
    uint16_t stageIndex = task->StageIndex;
    atomic_thread_fence(memory_order_acquire);
    atomic_fetch_add_explicit(&SlotContext->CallbacksRan, 1, memory_order_relaxed);

    bool success = atomic_load_explicit(&SlotContext->StageSuccess[stageIndex], memory_order_relaxed);
    assert(atomic_load_explicit(&SlotContext->StageDone[stageIndex], memory_order_relaxed));
    if (!success)
    {
        PublishReadFailure(SlotContext, DDS_ERROR_CODE_IO_FAILURE);
        return;
    }

    uint16_t nextStage = (uint16_t)(stageIndex + 1u);
    if (nextStage < SlotContext->StageCount)
    {
        ScheduleStage(SlotContext, nextStage);
        return;
    }

    PublishReadSuccessFinal(SlotContext);
}

//
// Start a read2 stage chain from the owner worker thread.
//
static void StartRead2StageChain(struct PerSlotContext *SlotContext)
{
    if (!IsRead2WithStages(SlotContext))
    {
        return;
    }
    SlotContext->StageChainRequiredWithoutIO = false;
    ScheduleStage(SlotContext, 0);
}

void DataPlaneRequestHandler(
    void* Ctx
) {
    struct PerSlotContext* HeadSlotContext = (struct PerSlotContext*) Ctx;  // head of batch
    RequestIdT batchSize = HeadSlotContext->BatchSize;
    
    // Original arithmetic batch walk retained for reference:
    // RequestIdT ioSlotBase = HeadSlotContext->IndexBase;
    // for (RequestIdT i = 0; i < batchSize; i++) {
    //     RequestIdT selfIndex = ioSlotBase + (HeadSlotContext->Position - ioSlotBase + i) % DDS_MAX_OUTSTANDING_IO;
    //     struct PerSlotContext* ThisSlotContext = &HeadSlotContext->SPDKContext->SPDKSpace[selfIndex];
    //     ...
    // }
    //
    // Updated: follow explicit request-id batch chain from DataPlaneRequestContext::BatchNextRequestId.
    // This preserves strict RequestId->slot ownership and avoids slot reuse collisions.
    struct PerSlotContext* ThisSlotContext = HeadSlotContext;
    for (RequestIdT i = 0; i < batchSize; i++) {
        if (ThisSlotContext == NULL || ThisSlotContext->Ctx == NULL) {
            fprintf(stderr, "%s [error]: null slot/context while walking batch (i=%u batch=%u)\n", __func__,
                    (unsigned)i, (unsigned)batchSize);
            break;
        }
        if (ThisSlotContext->Ctx->Response == NULL)
        {
            fprintf(stderr, "%s [error]: null response while walking batch (i=%u batch=%u)\n", __func__,
                    (unsigned)i, (unsigned)batchSize);
            goto next_slot;
        }
        // jason: skip requests that have already been completed during parsing (e.g., invalid read2 payload).
        ErrorCodeT parseResult =
            DDS_ATOMIC_ERRORCODE_LOAD(&ThisSlotContext->Ctx->Response->Result, DDS_ATOMIC_ORDER_ACQUIRE);
        if (parseResult != DDS_ERROR_CODE_IO_PENDING)
        {
            goto next_slot;
        }
        if (ThisSlotContext->Ctx->IsRead) {
            ReadHandler(ThisSlotContext);
        }
        else {
            WriteHandler(ThisSlotContext);
        }

next_slot:
        if (i + 1u >= batchSize) {
            break;
        }
        RequestIdT nextReqId = ThisSlotContext->Ctx->BatchNextRequestId;
        if (!DDS_ValidateRequestId(nextReqId)) {
            fprintf(stderr,
                    "%s [error]: invalid next request id while walking batch "
                    "(i=%u batch=%u nextReqId=%u)\n",
                    __func__, (unsigned)i, (unsigned)batchSize, (unsigned)nextReqId);
            break;
        }
        int nextChannelIndex = (int)ThisSlotContext->Ctx->ChannelIndex;
        uint32_t nextGlobalSlot = DDS_GlobalSlotFromChannelRequest(nextChannelIndex, nextReqId);
        if (nextGlobalSlot == UINT32_MAX)
        {
            fprintf(stderr,
                    "%s [error]: invalid next global slot while walking batch "
                    "(i=%u batch=%u nextReqId=%u channel=%d)\n",
                    __func__, (unsigned)i, (unsigned)batchSize, (unsigned)nextReqId, nextChannelIndex);
            break;
        }
        ThisSlotContext = &HeadSlotContext->SPDKContext->SPDKSpace[nextGlobalSlot];
    }
}

//
// Handler for a read request.
// Updated: publish Result with atomic store after BytesServiced/LogicalBytes are set.
//
void ReadHandler(
    void* Ctx
) {
    struct PerSlotContext* SlotContext = (struct PerSlotContext*)Ctx;
    DataPlaneRequestContext* Context = SlotContext->Ctx;
    bool isRead2 = IsRead2WithStages(SlotContext);
#ifndef NDEBUG
    if (isRead2)
    {
        assert(SlotContext->StageCount >= 1 && SlotContext->StageCount <= 2);
    }
#endif

    SlotContext->CallbacksRan = 0;  // incremented in callbacks
    SlotContext->CallbacksToRun = 0;  // incremented in ReadFile when async writes are issued successfully
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    SlotContext->OffloadReadTimeNs = 0;
    SlotContext->OffloadStage1TimeNs = 0;
    SlotContext->OffloadStage2TimeNs = 0;
    SlotContext->Stage0ReadStartNs = isRead2 ? OffloadNowNs() : 0;
#endif

    ErrorCodeT ret;
#ifdef OPT_FILE_SERVICE_ZERO_COPY
    ret = ReadFile(Context->Request->FileId, Context->Request->Offset, &Context->DataBuffer,
        ReadHandlerZCCallback, SlotContext, Sto, SlotContext->SPDKContext);
#else
    ret = ReadFile(Context->Request->FileId, Context->Request->Offset, &Context->DataBuffer,
        ReadHandlerNonZCCallback, SlotContext, Sto, SlotContext->SPDKContext);
#endif

    if (ret) {
        //
        // Fatal, some callbacks won't be called
        //
        //
        SPDK_ERRLOG("ReadFile() Failed: %d\n", ret);
        Context->Response->BytesServiced = 0;
        // jason: mirror logical bytes for error paths.
        Context->Response->LogicalBytes = 0;
        // Context->Response->LogicalBytes = 0;  // jason: duplicate assignment retained for reference.
        // Context->Response->LogicalBytes = 0;  // jason: duplicate assignment retained for reference.
        // Context->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
        // jason: publish Result after BytesServiced so poller sees a complete response.
        DDS_ATOMIC_ERRORCODE_STORE(&Context->Response->Result, DDS_ERROR_CODE_IO_FAILURE, DDS_ATOMIC_ORDER_RELEASE);
        return;
    }
    if (isRead2 && SlotContext->StageChainRequiredWithoutIO)
    {
        // jason: EOF-clamped reads still run the stage chain without stage0 I/O.
        StartRead2StageChain(SlotContext);
    }
}

//
// Similar to WriteHandler.
// Updated: acquire Result and publish completion with atomic ordering (BytesServiced/LogicalBytes).
//
void ReadHandlerZCCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    struct PerSlotContext* SlotContext = Context;
    bool isRead2 = IsRead2WithStages(SlotContext);

    spdk_bdev_free_io(bdev_io);
    SlotContext->CallbacksRan += 1;
    
    // if (SlotContext->Ctx->Response->Result != DDS_ERROR_CODE_IO_FAILURE) {
    if (DDS_ATOMIC_ERRORCODE_LOAD(&SlotContext->Ctx->Response->Result, DDS_ATOMIC_ORDER_ACQUIRE) != DDS_ERROR_CODE_IO_FAILURE) {
        //
        // Did not previously fail, should actually == IO_PENDING here
        //
        //
        if (Success) {
            if (SlotContext->CallbacksRan == SlotContext->CallbacksToRun) {
                //
                // All callbacks done and successful, mark resp success
                //
                //
                if (isRead2)
                {
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
                    SlotContext->OffloadReadTimeNs = OffloadNowNs() - SlotContext->Stage0ReadStartNs;
#endif
                    // jason: defer completion publishing to the read2 stage chain.
                    StartRead2StageChain(SlotContext);
                    return;
                }
                // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_SUCCESS;
                // SlotContext->Ctx->Response->BytesServiced = SlotContext->BytesIssued;
                SlotContext->Ctx->Response->BytesServiced = SlotContext->BytesIssued;
                // jason: record logical bytes alongside output bytes.
                SlotContext->Ctx->Response->LogicalBytes = SlotContext->LogicalBytesIssued;
                // SlotContext->Ctx->Response->LogicalBytes = SlotContext->LogicalBytesIssued;  // jason: duplicate
                // retained for reference. jason: publish Result after BytesServiced so poller sees a complete response.
                DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_SUCCESS, DDS_ATOMIC_ORDER_RELEASE);
            }
            //
            // else this isn't the last, nothing more to do
            //
            //
        }
        else {
            //
            // Unsuccessful, mark resp with failure
            //
            //
            // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            // SlotContext->Ctx->Response->BytesServiced = 0;
            SlotContext->Ctx->Response->BytesServiced = 0;
            // jason: mirror logical bytes for error paths.
            SlotContext->Ctx->Response->LogicalBytes = 0;
            // SlotContext->Ctx->Response->LogicalBytes = 0;  // jason: duplicate retained for reference.
            // jason: publish Result after BytesServiced so poller sees a complete response.
            DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_IO_FAILURE, DDS_ATOMIC_ORDER_RELEASE);
        }
    }
    //
    // Else previously failed, no useful work to do
    //
    //
}

//
// Non-zero-copy read callback.
// Updated: acquire Result and publish completion with atomic ordering (BytesServiced/LogicalBytes).
//
void ReadHandlerNonZCCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    struct PerSlotContext* SlotContext = Context;
    bool isRead2 = IsRead2WithStages(SlotContext);
    spdk_bdev_free_io(bdev_io);
    SlotContext->CallbacksRan += 1;

    // if (SlotContext->Ctx->Response->Result != DDS_ERROR_CODE_IO_FAILURE) {
    if (DDS_ATOMIC_ERRORCODE_LOAD(&SlotContext->Ctx->Response->Result, DDS_ATOMIC_ORDER_ACQUIRE) != DDS_ERROR_CODE_IO_FAILURE) {
        //
        // Did not previously fail, should actually == IO_PENDING here
        //
        //
        if (Success) {
            if (SlotContext->CallbacksRan == SlotContext->CallbacksToRun) {
                //
                // All callbacks done and successful, mark resp success
                // Non zc, we need to copy read data from our buff to the dest splittable buffer
                //
                //
                char *toCopy = &SlotContext->SPDKContext->buff[SlotContext->Position * DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE];
                memcpy(SlotContext->DestBuffer->FirstAddr, toCopy, SlotContext->DestBuffer->FirstSize);
                toCopy += SlotContext->DestBuffer->FirstSize;
                memcpy(SlotContext->DestBuffer->SecondAddr, toCopy, SlotContext->DestBuffer->TotalSize - SlotContext->DestBuffer->FirstSize);
                if (isRead2)
                {
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
                    SlotContext->OffloadReadTimeNs = OffloadNowNs() - SlotContext->Stage0ReadStartNs;
#endif
                    StartRead2StageChain(SlotContext);
                    return;
                }
                // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_SUCCESS;
                // SlotContext->Ctx->Response->BytesServiced = SlotContext->BytesIssued;
                SlotContext->Ctx->Response->BytesServiced = SlotContext->BytesIssued;
                // jason: record logical bytes alongside output bytes.
                SlotContext->Ctx->Response->LogicalBytes = SlotContext->LogicalBytesIssued;
                // jason: publish Result after BytesServiced so poller sees a complete response.
                DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_SUCCESS, DDS_ATOMIC_ORDER_RELEASE);
            }
            //
            // Else this isn't the last, nothing more to do
            //
            //
        }
        else {
            //
            // Unsuccessful, mark resp with failure
            //
            //
            SPDK_WARNLOG("failed bdev read (callback ran with fail param) encountered for RequestId %hu\n",
                SlotContext->Ctx->Response->RequestId);
            // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            // SlotContext->Ctx->Response->BytesServiced = 0;
            SlotContext->Ctx->Response->BytesServiced = 0;
            // jason: mirror logical bytes for error paths.
            SlotContext->Ctx->Response->LogicalBytes = 0;
            // jason: publish Result after BytesServiced so poller sees a complete response.
            DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_IO_FAILURE, DDS_ATOMIC_ORDER_RELEASE);
        }
    }
    //
    // Else previously failed, no useful work to do
    //
    //
}

//
// The cache on write function called in WriteHandler
//
//
/*
void CacheOnWrite(
    SplittableBufferT *WriteBuffer,
    FileSizeT Offset,
    FileIdT FileId
) {
    int ret;
    // the offset within the write, of a record
    RingSizeT offsetInBuffer = Offset == 0 ? 64 : 0;  // for some reason the very first 64 bytes are unused (should == 0)

    uint64_t recordKey;
    uint64_t recordValue;

    CacheItemT cacheItem;
    char* recordKeyPtr;
    char* recordValuePtr;
    RingSizeT recordKeyLocation;
    RingSizeT recordValueLocation;

    RingSizeT recordKeyOffset;
    RingSizeT recordValueOffset;

    while (offsetInBuffer + 24 < WriteBuffer->TotalSize) {
        recordKeyLocation = Offset + offsetInBuffer + sizeof(uint64_t);
        recordValueLocation = recordKeyLocation + sizeof(uint64_t);

        //
        // in case it's split buffer
        //
        //
        recordKeyOffset = offsetInBuffer + sizeof(uint64_t);  // skip the record header
        recordKeyPtr = recordKeyOffset < WriteBuffer->FirstSize ? WriteBuffer->FirstAddr + recordKeyOffset : 
            WriteBuffer->SecondAddr + (recordKeyOffset - WriteBuffer->FirstSize);
        
        recordValueOffset = recordKeyOffset + sizeof(uint64_t);
        recordValuePtr = recordValueOffset < WriteBuffer->FirstSize ? WriteBuffer->FirstAddr + recordValueOffset : 
            WriteBuffer->SecondAddr + (recordValueOffset - WriteBuffer->FirstSize);

        // if an uint64 is actually split in mem
        if (recordKeyOffset < WriteBuffer->FirstSize && recordKeyOffset + sizeof(uint64_t) > WriteBuffer->FirstSize) {
            printf("split buffer, key is split\n");
            RingSizeT firstPartLen = WriteBuffer->FirstSize - recordKeyOffset;
            memcpy(&recordKey, recordKeyPtr, firstPartLen);
            memcpy(&recordKey + firstPartLen, recordKeyPtr + firstPartLen, sizeof(uint64_t) - firstPartLen);
        }
        else {  // not split
            recordKey = *((uint64_t*) recordKeyPtr);
        }

        if (recordValueOffset < WriteBuffer->FirstSize && recordValueOffset + sizeof(uint64_t) > WriteBuffer->FirstSize) {
            printf("split buffer, value is split\n");
            RingSizeT firstPartLen = WriteBuffer->FirstSize - recordValueOffset;
            memcpy(&recordValue, recordValuePtr, firstPartLen);
            memcpy(&recordValue + firstPartLen, recordValuePtr + firstPartLen, sizeof(uint64_t) - firstPartLen);
        }
        else {
            recordValue = *((uint64_t*) recordValuePtr);
        }

        if (recordValue != 42) {
            printf("!!!!!!!! ERROR CacheOnWrite: recordValue %llu != 42, recordValueOffset: %u\n", recordValue, recordValueOffset);
        }

        cacheItem->Version = 0;
        cacheItem->Key = recordKey;
        cacheItem->FileId = FileId;
        cacheItem->Offset = recordValueLocation;
        cacheItem->Size = sizeof(uint64_t);

        if (AddToCacheTable(cacheItem)) {
            printf("cache on write AddToCacheTable() FAILED!!!\n");
        }

        offsetInBuffer += 24;  // a record consists of 3 uint64's
    }
}
*/


//
// Handler for a write request.
// Updated: publish Result with atomic store after BytesServiced is set.
//
void WriteHandler(
    void* Ctx
) {
    struct PerSlotContext* SlotContext = (struct PerSlotContext*)Ctx;
    DataPlaneRequestContext* Context = SlotContext->Ctx;

    SlotContext->CallbacksRan = 0;  // incremented in callbacks
    SlotContext->CallbacksToRun = 0;  // incremented in WriteFile when async writes are issued successfully

    //
    // TODO: CacheOnWrite(&Context->DataBuffer, Context->Request->Offset, Context->Request->FileId);
    //
    //
    ErrorCodeT ret = WriteFile(Context->Request->FileId, Context->Request->Offset, &Context->DataBuffer,
        WriteHandlerCallback, SlotContext, Sto, SlotContext->SPDKContext);
    if (ret) {
        //
        // Fatal, some callbacks won't be called
        //
        //
        SPDK_ERRLOG("WriteFile failed: %d\n", ret);
        Context->Response->BytesServiced = 0;
        // jason: mirror logical bytes for error paths.
        Context->Response->LogicalBytes = 0;
        // Context->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
        // jason: publish Result after BytesServiced so poller sees a complete response.
        DDS_ATOMIC_ERRORCODE_STORE(&Context->Response->Result, DDS_ERROR_CODE_IO_FAILURE, DDS_ATOMIC_ORDER_RELEASE);
        return;
    }
}

//
// To continue the work of Update response buffer tail
//
//
#if 0
void WriteHandlerCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    struct PerSlotContext* SlotContext = Context;
    spdk_bdev_free_io(bdev_io);
    SlotContext->CallbacksRan += 1;

    if (SlotContext->Ctx->Response->Result != DDS_ERROR_CODE_IO_FAILURE) {
        //
        // Did not previously fail, should actually == IO_PENDING here
        //
        //
        if (Success) {
            if (SlotContext->CallbacksRan == SlotContext->CallbacksToRun) {
                //
                // All callbacks done and successful, mark resp success
                //
                //
                SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_SUCCESS;
                SlotContext->Ctx->Response->BytesServiced = SlotContext->BytesIssued;
            }
            //
            // Else this isn't the last, nothing more to do
            //
            //
        }
        else {
            //
            // Unsuccessful, mark resp with failure
            // TODO: actually getting run when zc, what happened?
            //
            //
            SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            SlotContext->Ctx->Response->BytesServiced = 0;
        }
    }
    //
    // Else previously failed, no useful work to do
    //
    //
}
#endif

//
// Callback for WriteHandler async write.
// Updated: acquire Result and publish completion with atomic ordering.
//
void WriteHandlerCallback(struct spdk_bdev_io *bdev_io, bool Success, ContextT Context)
{
    struct PerSlotContext *SlotContext = Context;
    spdk_bdev_free_io(bdev_io);
    SlotContext->CallbacksRan += 1;

    // if (SlotContext->Ctx->Response->Result != DDS_ERROR_CODE_IO_FAILURE)
    if (DDS_ATOMIC_ERRORCODE_LOAD(&SlotContext->Ctx->Response->Result, DDS_ATOMIC_ORDER_ACQUIRE) != DDS_ERROR_CODE_IO_FAILURE)
    {
        //
        // Did not previously fail, should actually == IO_PENDING here
        //
        //
        if (Success)
        {
            if (SlotContext->CallbacksRan == SlotContext->CallbacksToRun)
            {
                //
                // jason: schedule metadata sync after all data I/O completes, so one more callback to run
                //
                //
                if (SlotContext->NeedMetadataSync && !SlotContext->MetadataSyncIssued)
                {
                    struct DPUFile *file = Sto->AllFiles[SlotContext->Ctx->Request->FileId];
                    if (!file)
                    {
                        // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_FILE_NOT_FOUND;
                        // SlotContext->Ctx->Response->BytesServiced = 0;
                        SlotContext->Ctx->Response->BytesServiced = 0;
                        // jason: mirror logical bytes for error paths.
                        SlotContext->Ctx->Response->LogicalBytes = 0;
                        // jason: publish Result after BytesServiced so poller sees a complete response.
                        DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_FILE_NOT_FOUND,
                                                   DDS_ATOMIC_ORDER_RELEASE);
                        return;
                    }
                    SlotContext->MetadataSyncIssued = true;
                    SlotContext->CallbacksToRun += 1;
                    ErrorCodeT result =
                        SyncFileToDisk(file, Sto, SlotContext->SPDKContext, WriteHandlerCallback, SlotContext);
                    if (result != DDS_ERROR_CODE_SUCCESS)
                    {
                        // jason: roll back the callback count on scheduling failure
                        printf("WriteHandlerCallback: SyncFileToDisk() failed: %d\n", result);
                        SlotContext->CallbacksToRun -= 1;
                        // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
                        // SlotContext->Ctx->Response->BytesServiced = 0;
                        SlotContext->Ctx->Response->BytesServiced = 0;
                        // jason: mirror logical bytes for error paths.
                        SlotContext->Ctx->Response->LogicalBytes = 0;
                        // jason: publish Result after BytesServiced so poller sees a complete response.
                        DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_IO_FAILURE,
                                                   DDS_ATOMIC_ORDER_RELEASE);
                    }
                    return;
                }

                //
                // All callbacks done and successful, mark resp success
                //
                //
                // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_SUCCESS;
                // SlotContext->Ctx->Response->BytesServiced = SlotContext->BytesIssued;
                SlotContext->Ctx->Response->BytesServiced = SlotContext->BytesIssued;
                // jason: writes report logical bytes equal to output bytes.
                SlotContext->Ctx->Response->LogicalBytes = SlotContext->BytesIssued;
                // jason: publish Result after BytesServiced so poller sees a complete response.
                DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_SUCCESS, DDS_ATOMIC_ORDER_RELEASE);
            }
            //
            // Else this isn't the last, nothing more to do
            //
            //
        }
        else
        {
            //
            // Unsuccessful, mark resp with failure
            // TODO: actually getting run when zc, what happened?
            //
            //
            // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            // SlotContext->Ctx->Response->BytesServiced = 0;
            SlotContext->Ctx->Response->BytesServiced = 0;
            // jason: mirror logical bytes for error paths.
            SlotContext->Ctx->Response->LogicalBytes = 0;
            // jason: publish Result after BytesServiced so poller sees a complete response.
            DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_IO_FAILURE, DDS_ATOMIC_ORDER_RELEASE);
        }
    }
    //
    // Else previously failed, no useful work to do
    //
    //
}
