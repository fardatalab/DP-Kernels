#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "spdk/env.h"

#include "CacheTable.h"
#include "DDSTypes.h"
#include "FileBackEnd.h"
#include "DPKEmbedShim.h"

// jason: Alignment helpers for backend I/O and response ring payloads.
/**
 * Align a FileSizeT value down to the provided alignment.
 *
 * Used to compute block-aligned offsets for backend I/O.
 */
static inline FileSizeT AlignDownFileSize(FileSizeT value, FileSizeT align)
{
    if (align == 0)
    {
        return value;
    }
    return value - (value % align);
}

/**
 * Align a FileSizeT value up to the provided alignment.
 *
 * Used to compute block-aligned end offsets for backend I/O.
 */
static inline FileSizeT AlignUpFileSize(FileSizeT value, FileSizeT align)
{
    if (align == 0)
    {
        return value;
    }
    return ((value + align - 1) / align) * align;
}

/**
 * Align the response payload start within the response ring buffer.
 *
 * Returns the payload start offset and the padding inserted between header and payload.
 * Assumes the ring base address is aligned to the payload alignment.
 */
// jason: Align payload start on the response ring; returns new offset and pad bytes.
static inline int AlignResponsePayloadStart(const char *ringBase, int startOffset, FileIOSizeT align,
                                            FileIOSizeT *padOut)
{
    if (align == 0)
    {
        align = 1;
    }
    uintptr_t base = (uintptr_t)ringBase;
    uintptr_t candidate = base + (uintptr_t)startOffset;
    uintptr_t aligned = AlignUpFileSize(candidate, (FileSizeT)align);
    if (aligned < base + BACKEND_RESPONSE_BUFFER_SIZE)
    {
        if (padOut)
        {
            *padOut = (FileIOSizeT)(aligned - candidate);
        }
        return startOffset + (int)(aligned - candidate);
    }
    // jason: wrap to base; assume ring base is aligned.
    if (padOut)
    {
        *padOut = (FileIOSizeT)(BACKEND_RESPONSE_BUFFER_SIZE - startOffset);
    }
    return 0;
}

/**
 * Mark producer-side response-wrap slack with a zero-sized record marker.
 *
 * Host consumers use this marker to skip end-of-ring slack in no-split mode.
 */
static inline void MarkResponseWrapSlack(char *ringBase, int ringOffset)
{
    if (ringOffset < 0 || ringOffset >= BACKEND_RESPONSE_BUFFER_SIZE)
    {
        return;
    }
    if ((BACKEND_RESPONSE_BUFFER_SIZE - ringOffset) < (int)sizeof(FileIOSizeT))
    {
        return;
    }
    *(FileIOSizeT *)(ringBase + ringOffset) = 0;
}
#include "DPUBackEndStorage.h"

#ifdef PRELOAD_CACHE_TABLE_ITEMS
#include <stdlib.h>
#endif

#define TRUE 1
#define FALSE 0

#undef DEBUG_FILE_BACKEND
#ifdef DEBUG_FILE_BACKEND
#define DebugPrint(Fmt, ...) fprintf(stderr, Fmt, __VA_ARGS__)
#else
static inline void DebugPrint(const char *Fmt, ...) {}
#endif

#ifndef DDS_RING_DIAGNOSTICS
#define DDS_RING_DIAGNOSTICS 0
#endif

#ifndef DDS_RING_BATCH_META_DIAGNOSTICS
#define DDS_RING_BATCH_META_DIAGNOSTICS 0
#endif

#if DDS_RING_BATCH_META_DIAGNOSTICS
// jason: Batch-meta diagnostics.
// The batch meta is a FileIOSizeT stored at the start of the batch (TailC). When it is zero,
// TailC likely points at non-meta bytes (mis-advance), or the meta word was overwritten.
static inline uint32_t ReadU32AtRingOffset(const char *ring, int offset)
{
    uint32_t v = 0;
    if (!ring)
    {
        return 0;
    }
    if (offset < 0)
    {
        offset %= BACKEND_RESPONSE_BUFFER_SIZE;
        if (offset < 0)
        {
            offset += BACKEND_RESPONSE_BUFFER_SIZE;
        }
    }
    if (offset >= BACKEND_RESPONSE_BUFFER_SIZE)
    {
        offset %= BACKEND_RESPONSE_BUFFER_SIZE;
    }
    memcpy(&v, ring + offset, sizeof(v));
    return v;
}

static inline void DumpResponseRingU32Window(const char *ring, int baseOffset, const char *tag)
{
    if (!ring)
    {
        return;
    }
    // Print 8 dwords from baseOffset.
    uint32_t w0 = ReadU32AtRingOffset(ring, baseOffset + 0);
    uint32_t w1 = ReadU32AtRingOffset(ring, baseOffset + 4);
    uint32_t w2 = ReadU32AtRingOffset(ring, baseOffset + 8);
    uint32_t w3 = ReadU32AtRingOffset(ring, baseOffset + 12);
    uint32_t w4 = ReadU32AtRingOffset(ring, baseOffset + 16);
    uint32_t w5 = ReadU32AtRingOffset(ring, baseOffset + 20);
    uint32_t w6 = ReadU32AtRingOffset(ring, baseOffset + 24);
    uint32_t w7 = ReadU32AtRingOffset(ring, baseOffset + 28);
    fprintf(stderr,
            "RingU32Dump(%s) off=%d u32[0..7]=%08x %08x %08x %08x %08x %08x %08x %08x\n",
            (tag ? tag : "?"), baseOffset, w0, w1, w2, w3, w4, w5, w6, w7);
}
#endif

/**
 * Reset per-channel posted-response transfer tracking.
 *
 * TailC remains the "posted to host" frontier. ResponseReclaimTail is the
 * local-source reuse frontier and advances only after response data WR CQEs
 * confirm the RNIC has finished reading the corresponding source bytes.
 */
static inline void ResetResponseTransferTracking(BuffConnChannelState *Ch)
{
    if (!Ch)
    {
        return;
    }

    Ch->ResponseReclaimTail = 0;
    Ch->ResponseTransferHead = 0;
    Ch->ResponseTransferTail = 0;
    Ch->ResponseTransferCount = 0;
    Ch->ResponseTransferSequence = 0;
    memset(Ch->ResponseTransfers, 0, sizeof(Ch->ResponseTransfers));
}

/**
 * Enqueue one posted response transfer for later reclaim on response-data CQEs.
 *
 * Design assumption: each logical channel uses exactly one RC QP with
 * sq_sig_all = 1, so send CQEs are observed in the same order as the WRs are
 * posted on that channel. The FIFO below therefore matches data-WR CQEs to the
 * oldest still-in-flight response transfer without encoding a unique transfer id
 * into wr_id.
 */
static inline int EnqueueResponseTransfer(BuffConnChannelState *Ch, int Start, FileIOSizeT Bytes,
                                          uint8_t PendingDataWrs)
{
    if (!Ch)
    {
        return -1;
    }
    if (Bytes == 0 || PendingDataWrs == 0)
    {
        fprintf(stderr, "%s [error][%s]: invalid transfer enqueue (start=%d bytes=%u pending=%u)\n", __func__,
                DDS_ChannelName(Ch->ChannelIndex), Start, (unsigned)Bytes, (unsigned)PendingDataWrs);
        return -1;
    }
    if (Ch->ResponseTransferCount >= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL)
    {
        fprintf(stderr,
                "%s [error][%s]: response transfer FIFO full (count=%u max=%u start=%d bytes=%u)\n",
                __func__, DDS_ChannelName(Ch->ChannelIndex), (unsigned)Ch->ResponseTransferCount,
                (unsigned)DDS_MAX_OUTSTANDING_IO_PER_CHANNEL, Start, (unsigned)Bytes);
        return -1;
    }

    ResponseTransferDescriptor *Desc = &Ch->ResponseTransfers[Ch->ResponseTransferTail];
    memset(Desc, 0, sizeof(*Desc));
    Desc->Valid = 1;
    Desc->PendingDataWrs = PendingDataWrs;
    Desc->Start = Start;
    Desc->End = (Start + (int)Bytes) % DDS_RESPONSE_RING_BYTES;
    Desc->Bytes = Bytes;
    Desc->Sequence = ++Ch->ResponseTransferSequence;

    Ch->ResponseTransferTail = (uint16_t)((Ch->ResponseTransferTail + 1) % DDS_MAX_OUTSTANDING_IO_PER_CHANNEL);
    Ch->ResponseTransferCount++;
    return 0;
}

/**
 * Consume one response-data CQE from the head posted-transfer descriptor.
 *
 * A split response transfer posts two data WRs. We reclaim the local source
 * bytes only after all of that transfer's data WR CQEs have been observed.
 */
static inline int CompleteResponseTransferDataWr(BuffConnChannelState *Ch, uint64_t WrId)
{
    if (!Ch)
    {
        return -1;
    }
    if (Ch->ResponseTransferCount == 0)
    {
        fprintf(stderr, "%s [error][%s]: response data CQE with empty transfer FIFO (wrId=%lu)\n", __func__,
                DDS_ChannelName(Ch->ChannelIndex), (unsigned long)WrId);
        return -1;
    }

    ResponseTransferDescriptor *Desc = &Ch->ResponseTransfers[Ch->ResponseTransferHead];
    if (!Desc->Valid || Desc->PendingDataWrs == 0)
    {
        fprintf(stderr,
                "%s [error][%s]: invalid response transfer descriptor at head=%u (valid=%u pending=%u wrId=%lu)\n",
                __func__, DDS_ChannelName(Ch->ChannelIndex), (unsigned)Ch->ResponseTransferHead,
                (unsigned)Desc->Valid, (unsigned)Desc->PendingDataWrs, (unsigned long)WrId);
        return -1;
    }

    Desc->PendingDataWrs--;
    if (Desc->PendingDataWrs == 0)
    {
        // Original local-reuse boundary was ResponseRing.TailC, but TailC now
        // means "posted to host". ReclaimTail advances only after data CQEs.
        DebugPrint("%s: ch=%s reclaim %d -> %d (seq=%lu bytes=%u)\n", __func__, DDS_ChannelName(Ch->ChannelIndex),
                   Ch->ResponseReclaimTail, Desc->End, (unsigned long)Desc->Sequence, (unsigned)Desc->Bytes);
        Ch->ResponseReclaimTail = Desc->End;
        memset(Desc, 0, sizeof(*Desc));
        Ch->ResponseTransferHead = (uint16_t)((Ch->ResponseTransferHead + 1) % DDS_MAX_OUTSTANDING_IO_PER_CHANNEL);
        Ch->ResponseTransferCount--;
    }
    return 0;
}

extern volatile int ForceQuitStorageEngine;
bool G_INITIALIZATION_DONE = false;

//
// Global cache table
//
//
CacheTableT *GlobalCacheTable;
static char *GlobalDpkIoPool = NULL;
static FileIOSizeT GlobalDpkIoPoolBytes = 0;
static uint32_t GlobalDpkIoPoolSlots = 0;
static bool GlobalDpkRuntimeStarted = false;
static void CleanupSlotDpkTasks(void);
static int InitializeSlotDpkTasksAtBackendStartup(BackEndConfig *config);
static void CleanupDpkRuntimeAndIoPool(void);

// Original widened slot-count macro retained for reference:
// #ifndef DDS_BACKEND_SLOT_COUNT
// #define DDS_BACKEND_SLOT_COUNT (DDS_MAX_OUTSTANDING_IO * (DDS_DPU_IO_PARALLELISM + 1))
// #endif

static int EnsureGlobalDpkIoPool(uint32_t maxBuffs, FileIOSizeT bufAlign)
{
    if (GlobalDpkIoPool != NULL)
    {
        if (maxBuffs > GlobalDpkIoPoolSlots)
        {
            fprintf(stderr, "%s [error]: existing IO pool slots=%u smaller than requested=%u\n", __func__,
                    GlobalDpkIoPoolSlots, maxBuffs);
            return -1;
        }
        return 0;
    }
    if (maxBuffs == 0)
    {
        fprintf(stderr, "%s [error]: maxBuffs must be > 0\n", __func__);
        return -1;
    }
    if (bufAlign == 0)
    {
        bufAlign = 4096;
    }

    uint64_t totalBytes = (uint64_t)maxBuffs * (uint64_t)BACKEND_RESPONSE_BUFFER_SIZE;
    // NOTE: dpk_embed_task_bind_io_region currently accepts uint32_t io_bytes.
    // Keep the externally-managed IO pool within that API limit.
    if (totalBytes > UINT32_MAX)
    {
        fprintf(stderr, "%s [error]: response IO pool too large for DPK bind API (%lu > %u)\n", __func__,
                (unsigned long)totalBytes, (unsigned)UINT32_MAX);
        return -1;
    }

    GlobalDpkIoPool = (char *)spdk_dma_malloc((size_t)totalBytes, bufAlign, NULL);
    if (GlobalDpkIoPool == NULL)
    {
        fprintf(stderr, "%s [error]: spdk_dma_malloc failed for DPK IO pool (%lu bytes)\n", __func__,
                (unsigned long)totalBytes);
        return -1;
    }
    GlobalDpkIoPoolBytes = (FileIOSizeT)totalBytes;
    GlobalDpkIoPoolSlots = maxBuffs;
    return 0;
}

static int EnsureDpkRuntimeStarted(void)
{
    if (GlobalDpkRuntimeStarted)
    {
        return 0;
    }
    if (GlobalDpkIoPool == NULL || GlobalDpkIoPoolBytes == 0)
    {
        fprintf(stderr, "%s [error]: DPK IO pool is not initialized\n", __func__);
        return -1;
    }

    dpk_embed_runtime_config cfg = {};
    cfg.io_base = (void *)GlobalDpkIoPool;
    cfg.io_bytes = (size_t)GlobalDpkIoPoolBytes;
    cfg.enable_thread_pinning = 1;
    cfg.startup_timeout_ms = 30000;

    if (dpk_embed_runtime_start(&cfg) != 0)
    {
        fprintf(stderr, "%s [error]: dpk_embed_runtime_start failed\n", __func__);
        return -1;
    }
    GlobalDpkRuntimeStarted = true;
    return 0;
}

static int InitializeDpkRuntimeAtBackendStartup(BackEndConfig *config)
{
    if (config == NULL)
    {
        fprintf(stderr, "%s [error]: config is NULL\n", __func__);
        return -1;
    }

    FileIOSizeT bufAlign = 4096;
    bool gotSpdkAlign = false;

    for (int waitedSec = 0; waitedSec < 60 && ForceQuitStorageEngine == 0; waitedSec++)
    {
        FileService *fs = config->FS;
        if (fs != NULL && fs->MasterSPDKContext != NULL)
        {
            FileIOSizeT candidateAlign = (FileIOSizeT)fs->MasterSPDKContext->buf_align;
            if (candidateAlign != 0)
            {
                bufAlign = candidateAlign;
            }
            gotSpdkAlign = true;
            break;
        }
        sleep(1);
    }

    if (!gotSpdkAlign)
    {
        fprintf(stderr,
                "%s [warn]: timed out waiting for MasterSPDKContext; using default bufAlign=%u for DPK IO pool\n",
                __func__, (unsigned)bufAlign);
    }

    int ret = EnsureGlobalDpkIoPool(config->MaxBuffs, bufAlign);
    if (ret != 0)
    {
        fprintf(stderr, "%s [error]: EnsureGlobalDpkIoPool failed\n", __func__);
        return ret;
    }

    ret = EnsureDpkRuntimeStarted();
    if (ret != 0)
    {
        fprintf(stderr, "%s [error]: EnsureDpkRuntimeStarted failed\n", __func__);
        CleanupDpkRuntimeAndIoPool();
        return ret;
    }

    ret = InitializeSlotDpkTasksAtBackendStartup(config);
    if (ret != 0)
    {
        fprintf(stderr, "%s [error]: InitializeSlotDpkTasksAtBackendStartup failed\n", __func__);
        CleanupDpkRuntimeAndIoPool();
        return ret;
    }

    return 0;
}

static void CleanupSlotDpkTasks(void)
{
    if (FS == NULL || FS->MasterSPDKContext == NULL || FS->MasterSPDKContext->SPDKSpace == NULL)
    {
        return;
    }

    // Original widened cleanup range retained for reference:
    // size_t slotCount = (size_t)DDS_MAX_OUTSTANDING_IO * (size_t)(DDS_DPU_IO_PARALLELISM + 1);
    //
    // Updated: strict request-id ownership uses explicit (channel, reqId) slots.
    size_t slotCount = (size_t)DDS_MAX_OUTSTANDING_IO_TOTAL;
    for (size_t i = 0; i < slotCount; i++)
    {
        struct PerSlotContext *slot = &FS->MasterSPDKContext->SPDKSpace[i];
        if (slot->DpkStageTaskHandles[0] != NULL)
        {
            dpk_embed_task_destroy((dpk_embed_task_handle *)slot->DpkStageTaskHandles[0]);
            slot->DpkStageTaskHandles[0] = NULL;
        }
        if (slot->DpkStageTaskHandles[1] != NULL)
        {
            dpk_embed_task_destroy((dpk_embed_task_handle *)slot->DpkStageTaskHandles[1]);
            slot->DpkStageTaskHandles[1] = NULL;
        }
        slot->DpkTaskStateInitialized = false;
    }
}

static int InitializeSlotDpkTasksAtBackendStartup(BackEndConfig *config)
{
    if (config == NULL || config->FS == NULL)
    {
        fprintf(stderr, "%s [error]: invalid config/FS\n", __func__);
        return -1;
    }
    if (!GlobalDpkRuntimeStarted || dpk_embed_runtime_is_ready() == 0)
    {
        fprintf(stderr, "%s [error]: DPK runtime is not ready\n", __func__);
        return -1;
    }
    if (GlobalDpkIoPool == NULL || GlobalDpkIoPoolBytes == 0 || GlobalDpkIoPoolBytes > UINT32_MAX)
    {
        fprintf(stderr, "%s [error]: invalid DPK IO pool base=%p bytes=%u\n", __func__, (void *)GlobalDpkIoPool,
                (unsigned)GlobalDpkIoPoolBytes);
        return -1;
    }

    SPDKContextT *masterCtx = NULL;
    for (int waitedSec = 0; waitedSec < 60 && ForceQuitStorageEngine == 0; waitedSec++)
    {
        if (config->FS->MasterSPDKContext != NULL && config->FS->MasterSPDKContext->SPDKSpace != NULL)
        {
            masterCtx = config->FS->MasterSPDKContext;
            break;
        }
        sleep(1);
    }
    if (masterCtx == NULL)
    {
        fprintf(stderr, "%s [error]: timed out waiting for MasterSPDKContext->SPDKSpace\n", __func__);
        return -1;
    }

    // Original widened provisioned slot count retained for reference:
    // size_t provisionedSlotCount = (size_t)DDS_MAX_OUTSTANDING_IO * (size_t)(DDS_DPU_IO_PARALLELISM + 1);
    //
    // Updated: prebind all active global slots across all channels.
    size_t provisionedSlotCount = (size_t)DDS_MAX_OUTSTANDING_IO_TOTAL;
    size_t slotCount = provisionedSlotCount;
    if (slotCount == 0 || slotCount > provisionedSlotCount)
    {
        fprintf(stderr, "%s [error]: invalid prebind slot count requested=%lu provisioned=%lu\n", __func__,
                (unsigned long)slotCount, (unsigned long)provisionedSlotCount);
        return -1;
    }
    uint32_t preprepareThreadCount = (config->FS->WorkerThreadCount > 0) ? (uint32_t)config->FS->WorkerThreadCount : 1u;
    int dpkMaxSubmitThreads = dpk_embed_get_max_submit_threads();
    if (dpkMaxSubmitThreads <= 0)
    {
        fprintf(stderr, "%s [error]: invalid dpk_embed_get_max_submit_threads=%d\n", __func__, dpkMaxSubmitThreads);
        return -1;
    }
    if (preprepareThreadCount > (uint32_t)dpkMaxSubmitThreads)
    {
        fprintf(stderr,
                "%s [error]: workerThreadCount=%u exceeds DPK submit threads=%d; adjust WORKER_THREAD_COUNT or "
                "N_DPM_THREADS\n",
                __func__, (unsigned)preprepareThreadCount, dpkMaxSubmitThreads);
        return -1;
    }
    fprintf(stdout,
            "%s [info]: prebinding DPK stage tasks for slots=%lu (provisioned=%lu, preprepareThreads=%u, ioBase=%p "
            "ioBytes=%u)\n",
            __func__, (unsigned long)slotCount, (unsigned long)provisionedSlotCount, (unsigned)preprepareThreadCount,
            (void *)GlobalDpkIoPool, (unsigned)GlobalDpkIoPoolBytes);

    for (size_t i = 0; i < slotCount; i++)
    {
        struct PerSlotContext *slot = &masterCtx->SPDKSpace[i];
        if (!slot->DpkTaskStateInitialized)
        {
            slot->DpkStageTaskHandles[0] = NULL;
            slot->DpkStageTaskHandles[1] = NULL;
            slot->DpkTaskStateInitialized = true;
        }

        for (uint16_t stageIndex = 0; stageIndex < 2; stageIndex++)
        {
            dpk_embed_task_handle *task = (dpk_embed_task_handle *)slot->DpkStageTaskHandles[stageIndex];
            if (task == NULL)
            {
                uint8_t stageType = (stageIndex == 0) ? DPK_EMBED_STAGE_AES_GCM : DPK_EMBED_STAGE_DECOMPRESS_DEFLATE;
                if (dpk_embed_task_create(stageType, &task) != 0 || task == NULL)
                {
                    fprintf(stderr, "%s [error]: dpk_embed_task_create failed slot=%lu stage=%u\n", __func__,
                            (unsigned long)i, (unsigned)stageIndex);
                    CleanupSlotDpkTasks();
                    return -1;
                }
                slot->DpkStageTaskHandles[stageIndex] = task;
            }

            // Bind once at backend startup against the full externally-owned response IO pool.
            // Per-request submit only updates in/out offsets and sizes inside this bound region.
            if (dpk_embed_task_bind_io_region(task, (uint64_t)(uintptr_t)GlobalDpkIoPool,
                                              (uint32_t)GlobalDpkIoPoolBytes) != 0)
            {
                fprintf(stderr, "%s [error]: dpk_embed_task_bind_io_region failed slot=%lu stage=%u\n", __func__,
                        (unsigned long)i, (unsigned)stageIndex);
                CleanupSlotDpkTasks();
                return -1;
            }

            // Preallocate kernel-side DOCA task objects per storage worker thread at backend startup.
            // This keeps ALLOC_TASK/ALLOC_IO control-plane requests out of the read2 data path.
            for (uint32_t workerIndex = 0; workerIndex < preprepareThreadCount; workerIndex++)
            {
                if (dpk_embed_task_prepare_for_thread(task, workerIndex) != 0)
                {
                    fprintf(stderr, "%s [error]: dpk_embed_task_prepare_for_thread failed slot=%lu stage=%u worker=%u\n",
                            __func__, (unsigned long)i, (unsigned)stageIndex, (unsigned)workerIndex);
                    CleanupSlotDpkTasks();
                    return -1;
                }
            }
        }
    }

    fprintf(stdout, "%s [info]: prebound DPK stage tasks successfully\n", __func__);
    return 0;
}

static void CleanupDpkRuntimeAndIoPool(void)
{
    CleanupSlotDpkTasks();

    if (GlobalDpkRuntimeStarted)
    {
        dpk_embed_runtime_stop();
        GlobalDpkRuntimeStarted = false;
    }
    if (GlobalDpkIoPool)
    {
        spdk_dma_free(GlobalDpkIoPool);
        GlobalDpkIoPool = NULL;
    }
    GlobalDpkIoPoolBytes = 0;
    GlobalDpkIoPoolSlots = 0;
}

//
// Set a CM channel to be non-blocking
//
//
int SetNonblocking(struct rdma_event_channel *Channel)
{
    int flags = fcntl(Channel->fd, F_GETFL, 0);
    if (flags == -1)
    {
        perror("fcntl F_GETFL");
        return -1;
    }

    if (fcntl(Channel->fd, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        perror("fcntl F_SETFL O_NONBLOCK");
        return -1;
    }

    return 0;
}

//
// Initialize DMA
//
//
static int InitDMA(DMAConfig *Config, uint32_t Ip, uint16_t Port)
{
    int ret = 0;
    struct sockaddr_in sin;

    Config->CmChannel = rdma_create_event_channel();
    if (!Config->CmChannel)
    {
        ret = errno;
        fprintf(stderr, "rdma_create_event_channel error %d\n", ret);
        return ret;
    }

    ret = SetNonblocking(Config->CmChannel);
    if (ret)
    {
        fprintf(stderr, "failed to set non-blocking\n");
        rdma_destroy_event_channel(Config->CmChannel);
        return ret;
    }

#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
    fprintf(stdout, "Created CmChannel %p\n", Config->CmChannel);
    fprintf(stdout, "ServerIP: %u, Port: %u\n", Ip, Port);
#endif

    ret = rdma_create_id(Config->CmChannel, &Config->CmId, Config, RDMA_PS_TCP);
    if (ret)
    {
        ret = errno;
        fprintf(stderr, "rdma_create_id error %d\n", ret);
        rdma_destroy_event_channel(Config->CmChannel);
        return ret;
    }

#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
    fprintf(stdout, "Created CmId %p\n", Config->CmId);
#endif

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = Ip;
    sin.sin_port = Port;

    ret = rdma_bind_addr(Config->CmId, (struct sockaddr *)&sin);
    if (ret)
    {
        ret = errno;
        fprintf(stderr, "rdma_bind_addr error %d\n", ret);
        rdma_destroy_event_channel(Config->CmChannel);
        rdma_destroy_id(Config->CmId);
        return ret;
    }

#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
    fprintf(stdout, "rdma_bind_addr succeeded\n");
#endif

    return ret;
}

//
// Terminate DMA
//
//
static void TermDMA(DMAConfig *Config)
{
    if (Config->CmId)
    {
        rdma_destroy_id(Config->CmId);
    }

    if (Config->CmChannel)
    {
        rdma_destroy_event_channel(Config->CmChannel);
    }
}

//
// Allocate connections
//
//
static int AllocConns(BackEndConfig *Config)
{
    Config->CtrlConns = (CtrlConnConfig *)malloc(sizeof(CtrlConnConfig) * Config->MaxClients);
    if (!Config->CtrlConns)
    {
        fprintf(stderr, "Failed to allocate CtrlConns\n");
        return ENOMEM;
    }
    memset(Config->CtrlConns, 0, sizeof(CtrlConnConfig) * Config->MaxClients);
    for (int c = 0; c < Config->MaxClients; c++)
    {
        Config->CtrlConns[c].CtrlId = c;

        //
        // Initialize the pending control plane request
        //
        //
        Config->CtrlConns[c].PendingControlPlaneRequest.RequestId = DDS_REQUEST_INVALID;
        Config->CtrlConns[c].PendingControlPlaneRequest.Request = NULL;
        Config->CtrlConns[c].PendingControlPlaneRequest.Response = NULL;
#ifdef CREATE_DEFAULT_DPU_FILE
        Config->CtrlConns[c].DefaultDpuFileCreationState = FILE_NULL;
#endif
    }

    // Original single-channel allocation retained for reference:
    // Config->BuffConns = (BuffConnConfig *)malloc(sizeof(BuffConnConfig) * Config->MaxClients);
    //
    // Updated: allocate one BuffConn per IO channel connection.
    Config->BuffConns = (BuffConnConfig *)malloc(sizeof(BuffConnConfig) * Config->MaxBuffs);
    if (!Config->BuffConns)
    {
        fprintf(stderr, "Failed to allocate BuffConns\n");
        free(Config->CtrlConns);
        return ENOMEM;
    }
    memset(Config->BuffConns, 0, sizeof(BuffConnConfig) * Config->MaxBuffs);
    for (int c = 0; c < Config->MaxBuffs; c++)
    {
        Config->BuffConns[c].BuffId = c;
    }

    return 0;
}

//
// Deallocate connections
//
//
static void DeallocConns(BackEndConfig *Config)
{
    if (Config->CtrlConns)
    {
        free(Config->CtrlConns);
    }

    if (Config->BuffConns)
    {
        free(Config->BuffConns);
    }
}

//
// Handle signals
//
//
static void SignalHandler(int SigNum)
{
    if (SigNum == SIGINT || SigNum == SIGTERM)
    {
        fprintf(stdout, "Received signal to exit\n");
        ForceQuitStorageEngine = 1;
    }
}

//
// Set up queue pairs for a control connection
//
//
static int SetUpCtrlQPair(CtrlConnConfig *CtrlConn)
{
    int ret = 0;
    struct ibv_qp_init_attr initAttr;

    CtrlConn->PDomain = ibv_alloc_pd(CtrlConn->RemoteCmId->verbs);
    if (!CtrlConn->PDomain)
    {
        fprintf(stderr, "%s [error]: ibv_alloc_pd failed\n", __func__);
        ret = -1;
        goto SetUpCtrlQPairReturn;
    }

    CtrlConn->Channel = ibv_create_comp_channel(CtrlConn->RemoteCmId->verbs);
    if (!CtrlConn->Channel)
    {
        fprintf(stderr, "%s [error]: ibv_create_comp_channel failed\n", __func__);
        ret = -1;
        goto DeallocPdReturn;
    }

    CtrlConn->CompQ = ibv_create_cq(CtrlConn->RemoteCmId->verbs, CTRL_COMPQ_DEPTH * 2, CtrlConn, CtrlConn->Channel, 0);
    if (!CtrlConn->CompQ)
    {
        fprintf(stderr, "%s [error]: ibv_create_cq failed\n", __func__);
        ret = -1;
        goto DestroyCommChannelReturn;
    }

    ret = ibv_req_notify_cq(CtrlConn->CompQ, 0);
    if (ret)
    {
        fprintf(stderr, "%s [error]: ibv_req_notify_cq failed\n", __func__);
        goto DestroyCompQReturn;
    }

    memset(&initAttr, 0, sizeof(initAttr));
    initAttr.cap.max_send_wr = CTRL_SENDQ_DEPTH;
    initAttr.cap.max_recv_wr = CTRL_RECVQ_DEPTH;
    initAttr.cap.max_recv_sge = 1;
    initAttr.cap.max_send_sge = 1;
    initAttr.qp_type = IBV_QPT_RC;
    initAttr.send_cq = CtrlConn->CompQ;
    initAttr.recv_cq = CtrlConn->CompQ;

    ret = rdma_create_qp(CtrlConn->RemoteCmId, CtrlConn->PDomain, &initAttr);
    if (!ret)
    {
        CtrlConn->QPair = CtrlConn->RemoteCmId->qp;
    }
    else
    {
        fprintf(stderr, "%s [error]: rdma_create_qp failed\n", __func__);
        ret = -1;
        goto DestroyCompQReturn;
    }

    return 0;

DestroyCompQReturn:
    ibv_destroy_cq(CtrlConn->CompQ);

DestroyCommChannelReturn:
    ibv_destroy_comp_channel(CtrlConn->Channel);

DeallocPdReturn:
    ibv_dealloc_pd(CtrlConn->PDomain);

SetUpCtrlQPairReturn:
    return ret;
}

//
// Destrory queue pairs for a control connection
//
//
static void DestroyCtrlQPair(CtrlConnConfig *CtrlConn)
{
    rdma_destroy_qp(CtrlConn->RemoteCmId);
    ibv_destroy_cq(CtrlConn->CompQ);
    ibv_destroy_comp_channel(CtrlConn->Channel);
    ibv_dealloc_pd(CtrlConn->PDomain);
}

//
// Set up regions and buffers for a control connection
//
//
static int SetUpCtrlRegionsAndBuffers(CtrlConnConfig *CtrlConn)
{
    int ret = 0;
    CtrlConn->RecvMr = ibv_reg_mr(CtrlConn->PDomain, CtrlConn->RecvBuff, CTRL_MSG_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if (!CtrlConn->RecvMr)
    {
        fprintf(stderr, "%s [error]: ibv_reg_mr for receive failed\n", __func__);
        ret = -1;
        goto SetUpCtrlRegionsAndBuffersReturn;
    }

    CtrlConn->SendMr = ibv_reg_mr(CtrlConn->PDomain, CtrlConn->SendBuff, CTRL_MSG_SIZE, 0);
    if (!CtrlConn->SendMr)
    {
        fprintf(stderr, "%s [error]: ibv_reg_mr for send failed\n", __func__);
        ret = -1;
        goto DeregisterRecvMrReturn;
    }

    //
    // Set up work requests
    //
    //
    CtrlConn->RecvSgl.addr = (uint64_t)CtrlConn->RecvBuff;
    CtrlConn->RecvSgl.length = CTRL_MSG_SIZE;
    CtrlConn->RecvSgl.lkey = CtrlConn->RecvMr->lkey;
    CtrlConn->RecvWr.sg_list = &CtrlConn->RecvSgl;
    CtrlConn->RecvWr.num_sge = 1;
    CtrlConn->RecvWr.wr_id = CTRL_RECV_WR_ID;

    CtrlConn->SendSgl.addr = (uint64_t)CtrlConn->SendBuff;
    CtrlConn->SendSgl.length = CTRL_MSG_SIZE;
    CtrlConn->SendSgl.lkey = CtrlConn->SendMr->lkey;
    CtrlConn->SendWr.opcode = IBV_WR_SEND;
    CtrlConn->SendWr.send_flags = IBV_SEND_SIGNALED;
    CtrlConn->SendWr.sg_list = &CtrlConn->SendSgl;
    CtrlConn->SendWr.num_sge = 1;
    CtrlConn->SendWr.wr_id = CTRL_SEND_WR_ID;

    return 0;

DeregisterRecvMrReturn:
    ibv_dereg_mr(CtrlConn->RecvMr);

SetUpCtrlRegionsAndBuffersReturn:
    return ret;
}

//
// Destrory regions and buffers for a control connection
//
//
static void DestroyCtrlRegionsAndBuffers(CtrlConnConfig *CtrlConn)
{
    ibv_dereg_mr(CtrlConn->SendMr);
    ibv_dereg_mr(CtrlConn->RecvMr);
    memset(&CtrlConn->SendSgl, 0, sizeof(CtrlConn->SendSgl));
    memset(&CtrlConn->RecvSgl, 0, sizeof(CtrlConn->RecvSgl));
    memset(&CtrlConn->SendWr, 0, sizeof(CtrlConn->SendWr));
    memset(&CtrlConn->RecvWr, 0, sizeof(CtrlConn->RecvWr));
}

//
// Set up queue pairs for a buffer connection
//
//
// ---------------------------------------------------------------------------
// SetUpBuffQPair — allocate PD and create one QP/CQ for this connection.
//
// Design invariant: one BuffConnConfig is one logical IO channel connection.
// There is no secondary in-struct channel lane. The second logical channel is
// represented by a second BuffConnConfig accepted by the same DMA agent thread.
//
// Each BuffConnConfig represents a SINGLE channel. The frontend creates one
// RDMA connection (DMABuffer) per channel, each arriving as a separate BuffConn.
// SetUpBuffQPair creates this connection's QP/CQ (bound to the cm_id connection).
// ChannelState.ChannelIndex is set later by BuffMsgHandler from registration.
// ---------------------------------------------------------------------------
static int SetUpBuffQPair(BuffConnConfig *BuffConn)
{
    int ret = 0;

    BuffConn->PDomain = ibv_alloc_pd(BuffConn->RemoteCmId->verbs);
    if (!BuffConn->PDomain)
    {
        fprintf(stderr, "%s [error]: ibv_alloc_pd failed\n", __func__);
        return -1;
    }

    // Each BuffConnConfig owns exactly one ChannelState.
    {
        BuffConnChannelState *ch = &BuffConn->ChannelState;
        // ChannelIndex will be set by BuffMsgHandler when the registration
        // message arrives (carries the logical channel identity).
        ch->ChannelIndex = 0;

        ch->Channel = ibv_create_comp_channel(BuffConn->RemoteCmId->verbs);
        if (!ch->Channel)
        {
            fprintf(stderr, "%s [error]: ibv_create_comp_channel failed\n", __func__);
            ret = -1;
            goto DeallocBuffPdReturn;
        }

        ch->CompQ = ibv_create_cq(BuffConn->RemoteCmId->verbs, BUFF_COMPQ_DEPTH * 2, BuffConn, ch->Channel, 0);
        if (!ch->CompQ)
        {
            fprintf(stderr, "%s [error]: ibv_create_cq failed\n", __func__);
            ret = -1;
            goto DestroyBuffCommChannel;
        }

        ret = ibv_req_notify_cq(ch->CompQ, 0);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_req_notify_cq failed\n", __func__);
            goto DestroyBuffCompQ;
        }

        struct ibv_qp_init_attr initAttr;
        memset(&initAttr, 0, sizeof(initAttr));
        initAttr.cap.max_send_wr = BUFF_SENDQ_DEPTH;
        initAttr.cap.max_recv_wr = BUFF_RECVQ_DEPTH;
        initAttr.cap.max_recv_sge = 1;
        initAttr.cap.max_send_sge = 1;
        initAttr.qp_type = IBV_QPT_RC;
        initAttr.send_cq = ch->CompQ;
        initAttr.recv_cq = ch->CompQ;
        // Response-transfer reclaim logic assumes SQ CQEs are observed in post
        // order on this per-channel RC QP, so request CQEs for every SQ WR.
        initAttr.sq_sig_all = 1;

        ret = rdma_create_qp(BuffConn->RemoteCmId, BuffConn->PDomain, &initAttr);
        if (!ret)
        {
            ch->QPair = BuffConn->RemoteCmId->qp;
        }
        else
        {
            fprintf(stderr, "%s [error]: rdma_create_qp failed\n", __func__);
            ret = -1;
            goto DestroyBuffCompQ;
        }
    }

    return 0;

DestroyBuffCompQ:
    ibv_destroy_cq(BuffConn->ChannelState.CompQ);
DestroyBuffCommChannel:
    ibv_destroy_comp_channel(BuffConn->ChannelState.Channel);
DeallocBuffPdReturn:
    ibv_dealloc_pd(BuffConn->PDomain);
    return ret;
}

//
// Destroy queue pair for a buffer connection.
// Each BuffConnConfig represents a single channel (one RDMA connection).
//
static void DestroyBuffQPair(BuffConnConfig *BuffConn)
{
    // ChannelState: cm_id-bound QP
    rdma_destroy_qp(BuffConn->RemoteCmId);
    BuffConn->ChannelState.QPair = NULL;
    if (BuffConn->ChannelState.CompQ)
    {
        ibv_destroy_cq(BuffConn->ChannelState.CompQ);
        BuffConn->ChannelState.CompQ = NULL;
    }
    if (BuffConn->ChannelState.Channel)
    {
        ibv_destroy_comp_channel(BuffConn->ChannelState.Channel);
        BuffConn->ChannelState.Channel = NULL;
    }
    ibv_dealloc_pd(BuffConn->PDomain);
}

//
// Set up RDMA regions for control messages
//
//
static int SetUpForCtrlMsgs(BuffConnConfig *BuffConn)
{
    int ret = 0;

    //
    // Receive buffer and region
    //
    //
    BuffConn->RecvMr = ibv_reg_mr(BuffConn->PDomain, BuffConn->RecvBuff, CTRL_MSG_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if (!BuffConn->RecvMr)
    {
        fprintf(stderr, "%s [error]: ibv_reg_mr for receive failed\n", __func__);
        ret = -1;
        goto SetUpForCtrlMsgsReturn;
    }

    //
    // Send buffer and region
    //
    //
    BuffConn->SendMr = ibv_reg_mr(BuffConn->PDomain, BuffConn->SendBuff, CTRL_MSG_SIZE, 0);
    if (!BuffConn->SendMr)
    {
        fprintf(stderr, "%s [error]: ibv_reg_mr for send failed\n", __func__);
        ret = -1;
        goto DeregisterBuffRecvMrForCtrlMsgsReturn;
    }

    //
    // Set up work requests
    //
    //
    BuffConn->RecvSgl.addr = (uint64_t)BuffConn->RecvBuff;
    BuffConn->RecvSgl.length = CTRL_MSG_SIZE;
    BuffConn->RecvSgl.lkey = BuffConn->RecvMr->lkey;
    BuffConn->RecvWr.sg_list = &BuffConn->RecvSgl;
    BuffConn->RecvWr.num_sge = 1;
    BuffConn->RecvWr.wr_id = BUFF_SEND_WR_ID;

    BuffConn->SendSgl.addr = (uint64_t)BuffConn->SendBuff;
    BuffConn->SendSgl.length = CTRL_MSG_SIZE;
    BuffConn->SendSgl.lkey = BuffConn->SendMr->lkey;
    BuffConn->SendWr.opcode = IBV_WR_SEND;
    BuffConn->SendWr.send_flags = IBV_SEND_SIGNALED;
    BuffConn->SendWr.sg_list = &BuffConn->SendSgl;
    BuffConn->SendWr.num_sge = 1;
    BuffConn->SendWr.wr_id = BUFF_RECV_WR_ID;

    return 0;

DeregisterBuffRecvMrForCtrlMsgsReturn:
    ibv_dereg_mr(BuffConn->RecvMr);

SetUpForCtrlMsgsReturn:
    return ret;
}

//
// Destroy the setup for control messages
//
//
static void DestroyForCtrlMsgs(BuffConnConfig *BuffConn)
{
    ibv_dereg_mr(BuffConn->SendMr);
    ibv_dereg_mr(BuffConn->RecvMr);
    memset(&BuffConn->SendSgl, 0, sizeof(BuffConn->SendSgl));
    memset(&BuffConn->RecvSgl, 0, sizeof(BuffConn->RecvSgl));
    memset(&BuffConn->SendWr, 0, sizeof(BuffConn->SendWr));
    memset(&BuffConn->RecvWr, 0, sizeof(BuffConn->RecvWr));
}

//
// Set up regions and buffers for the request buffer (per-channel).
// Initializes deferred request head state for safe consumption ordering.
// Updated: allocate request ring buffers from SPDK DMA-capable memory.
// Refactored: operates on a single BuffConnChannelState; called once per channel.
//
static int SetUpForRequests(BuffConnChannelState *Ch, struct ibv_pd *PDomain, FileIOSizeT BufAlign)
{
    int ret = 0;
    const char *chName = DDS_ChannelName(Ch->ChannelIndex);

    //
    // Read data buffer and region
    //
    //
    // BuffConn->RequestDMAReadDataBuff = malloc(BACKEND_REQUEST_BUFFER_SIZE);
    // jason: allocate SPDK DMA-capable memory so ZC writes meet bdev alignment.
    if (BufAlign == 0)
    {
        BufAlign = 4096;
    }
    Ch->RequestDMAReadDataBuff = (char *)spdk_dma_malloc(BACKEND_REQUEST_BUFFER_SIZE, BufAlign, NULL);
    if (!Ch->RequestDMAReadDataBuff)
    {
        fprintf(stderr, "%s [error][%s]: OOM for DMA read data buffer\n", __func__, chName);
        ret = -1;
        goto SetUpForRequestsReturn;
    }

    Ch->RequestDMAReadDataMr =
        ibv_reg_mr(PDomain, Ch->RequestDMAReadDataBuff, BACKEND_REQUEST_BUFFER_SIZE,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!Ch->RequestDMAReadDataMr)
    {
        fprintf(stderr, "%s [error][%s]: ibv_reg_mr for DMA read data failed\n", __func__, chName);
        ret = -1;
        goto FreeBuffDMAReadDataBuffForRequestsReturn;
    }

    //
    // Read meta buffer and region
    //
    //
    Ch->RequestDMAReadMetaMr =
        ibv_reg_mr(PDomain, Ch->RequestDMAReadMetaBuff, RING_BUFFER_REQUEST_META_DATA_SIZE,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!Ch->RequestDMAReadMetaMr)
    {
        fprintf(stderr, "%s [error][%s]: ibv_reg_mr for DMA read meta failed\n", __func__, chName);
        ret = -1;
        goto DeregisterBuffReadDataMrForRequestsReturn;
    }

    //
    // Write meta buffer and region
    //
    //
    Ch->RequestDMAWriteMetaBuff = (char *)&Ch->RequestRing.Head;
    Ch->RequestDMAWriteMetaMr =
        ibv_reg_mr(PDomain, Ch->RequestDMAWriteMetaBuff, sizeof(int),
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!Ch->RequestDMAWriteMetaMr)
    {
        fprintf(stderr, "%s [error][%s]: ibv_reg_mr for DMA write meta failed\n", __func__, chName);
        ret = -1;
        goto DeregisterBuffReadMetaMrForRequestsReturn;
    }

    //
    // Set up work requests
    //
    //
    Ch->RequestDMAReadDataSgl.addr = (uint64_t)Ch->RequestDMAReadDataBuff;
    Ch->RequestDMAReadDataSgl.length = BACKEND_REQUEST_MAX_DMA_SIZE;
    Ch->RequestDMAReadDataSgl.lkey = Ch->RequestDMAReadDataMr->lkey;
    Ch->RequestDMAReadDataWr.opcode = IBV_WR_RDMA_READ;
    Ch->RequestDMAReadDataWr.send_flags = IBV_SEND_SIGNALED;
    Ch->RequestDMAReadDataWr.sg_list = &Ch->RequestDMAReadDataSgl;
    Ch->RequestDMAReadDataWr.num_sge = 1;
    Ch->RequestDMAReadDataWr.wr_id = BUFF_READ_REQUEST_DATA_WR_ID;

    Ch->RequestDMAReadDataSplitSgl.addr = (uint64_t)Ch->RequestDMAReadDataBuff;
    Ch->RequestDMAReadDataSplitSgl.length = BACKEND_REQUEST_MAX_DMA_SIZE;
    Ch->RequestDMAReadDataSplitSgl.lkey = Ch->RequestDMAReadDataMr->lkey;
    Ch->RequestDMAReadDataSplitWr.opcode = IBV_WR_RDMA_READ;
    Ch->RequestDMAReadDataSplitWr.send_flags = IBV_SEND_SIGNALED;
    Ch->RequestDMAReadDataSplitWr.sg_list = &Ch->RequestDMAReadDataSplitSgl;
    Ch->RequestDMAReadDataSplitWr.num_sge = 1;
    Ch->RequestDMAReadDataSplitWr.wr_id = BUFF_READ_REQUEST_DATA_SPLIT_WR_ID;

    Ch->RequestDMAReadMetaSgl.addr = (uint64_t)Ch->RequestDMAReadMetaBuff;
    Ch->RequestDMAReadMetaSgl.length = RING_BUFFER_REQUEST_META_DATA_SIZE;
    Ch->RequestDMAReadMetaSgl.lkey = Ch->RequestDMAReadMetaMr->lkey;
    Ch->RequestDMAReadMetaWr.opcode = IBV_WR_RDMA_READ;
    Ch->RequestDMAReadMetaWr.send_flags = IBV_SEND_SIGNALED;
    Ch->RequestDMAReadMetaWr.sg_list = &Ch->RequestDMAReadMetaSgl;
    Ch->RequestDMAReadMetaWr.num_sge = 1;
    Ch->RequestDMAReadMetaWr.wr_id = BUFF_READ_REQUEST_META_WR_ID;

    Ch->RequestDMAWriteMetaSgl.addr = (uint64_t)Ch->RequestDMAWriteMetaBuff;
    Ch->RequestDMAWriteMetaSgl.length = sizeof(int);
    Ch->RequestDMAWriteMetaSgl.lkey = Ch->RequestDMAWriteMetaMr->lkey;
    Ch->RequestDMAWriteMetaWr.opcode = IBV_WR_RDMA_WRITE;
    Ch->RequestDMAWriteMetaWr.send_flags = IBV_SEND_SIGNALED;
    Ch->RequestDMAWriteMetaWr.sg_list = &Ch->RequestDMAWriteMetaSgl;
    Ch->RequestDMAWriteMetaWr.num_sge = 1;
    Ch->RequestDMAWriteMetaWr.wr_id = BUFF_WRITE_REQUEST_META_WR_ID;

    // jason: initialize deferred head state; host head is published after consumption.
    Ch->RequestPendingHead = DDS_REQUEST_RING_HEADER_PHASE;
    Ch->RequestPendingHeadValid = FALSE;

    return 0;

DeregisterBuffReadMetaMrForRequestsReturn:
    ibv_dereg_mr(Ch->RequestDMAReadMetaMr);

DeregisterBuffReadDataMrForRequestsReturn:
    ibv_dereg_mr(Ch->RequestDMAReadDataMr);

FreeBuffDMAReadDataBuffForRequestsReturn:
    // free(Ch->RequestDMAReadDataBuff);
    spdk_dma_free(Ch->RequestDMAReadDataBuff);

SetUpForRequestsReturn:
    return ret;
}

//
// Destroy regions and buffers for the request buffer (per-channel).
// Resets deferred request head state.
//
static void DestroyForRequests(BuffConnChannelState *Ch)
{
    // free(Ch->RequestDMAReadDataBuff);
    spdk_dma_free(Ch->RequestDMAReadDataBuff);

    ibv_dereg_mr(Ch->RequestDMAWriteMetaMr);
    ibv_dereg_mr(Ch->RequestDMAReadMetaMr);
    ibv_dereg_mr(Ch->RequestDMAReadDataMr);

    memset(&Ch->RequestDMAWriteMetaSgl, 0, sizeof(Ch->RequestDMAReadMetaSgl));
    memset(&Ch->RequestDMAReadMetaSgl, 0, sizeof(Ch->RequestDMAReadMetaSgl));
    memset(&Ch->RequestDMAReadDataSgl, 0, sizeof(Ch->RequestDMAReadDataSgl));
    memset(&Ch->RequestDMAReadDataSplitSgl, 0, sizeof(Ch->RequestDMAReadDataSplitSgl));

    memset(&Ch->RequestDMAWriteMetaWr, 0, sizeof(Ch->RequestDMAWriteMetaWr));
    memset(&Ch->RequestDMAReadMetaWr, 0, sizeof(Ch->RequestDMAReadMetaWr));
    memset(&Ch->RequestDMAReadDataWr, 0, sizeof(Ch->RequestDMAReadDataWr));
    memset(&Ch->RequestDMAReadDataSplitWr, 0, sizeof(Ch->RequestDMAReadDataSplitWr));

    // BuffConn->RequestRing.Head = 0;
    // jason: reset ring head and deferred head state together.
    Ch->RequestRing.Head = DDS_REQUEST_RING_HEADER_PHASE;
    Ch->RequestPendingHead = DDS_REQUEST_RING_HEADER_PHASE;
    Ch->RequestPendingHeadValid = FALSE;
}

//
// Set up regions and buffers for the response buffer (per-channel).
// Updated: configure response meta writes to use write-with-immediate for CQE gating.
// Updated: allocate response ring buffers from SPDK DMA-capable memory.
// Refactored: operates on a single BuffConnChannelState; called once per channel.
//
static int SetUpForResponses(BuffConnChannelState *Ch, struct ibv_pd *PDomain, FileIOSizeT BufAlign, uint32_t MaxBuffs, uint32_t BuffId)
{
    int ret = 0;
    const char *chName = DDS_ChannelName(Ch->ChannelIndex);

    //
    // Read data buffer and region
    //
    //
    // BuffConn->ResponseDMAWriteDataBuff = malloc(BACKEND_RESPONSE_BUFFER_SIZE);
    (void)BufAlign;
    (void)MaxBuffs;
    if (GlobalDpkIoPool == NULL || GlobalDpkIoPoolSlots == 0)
    {
        fprintf(stderr, "%s [error][%s]: DPK IO pool not initialized at backend startup\n", __func__, chName);
        ret = -1;
        goto SetUpForResponsesReturn;
    }
    // Original pool-slot mapping retained for reference:
    // uint32_t poolSlotIndex = BuffId * DDS_NUM_IO_CHANNELS + Ch->ChannelIndex;
    //
    // Updated transport model: one BuffConn == one logical channel connection.
    // Therefore BuffId already identifies the unique response IO pool slice.
    uint32_t poolSlotIndex = BuffId;
    if (poolSlotIndex >= GlobalDpkIoPoolSlots)
    {
        fprintf(stderr, "%s [error][%s]: poolSlotIndex=%u exceeds response IO pool slots=%u (BuffId=%u ch=%d)\n",
                __func__, chName, poolSlotIndex, GlobalDpkIoPoolSlots, BuffId, Ch->ChannelIndex);
        ret = -1;
        goto SetUpForResponsesReturn;
    }
    Ch->ResponseDMAWriteDataBuff = GlobalDpkIoPool + ((size_t)poolSlotIndex * BACKEND_RESPONSE_BUFFER_SIZE);

    if (!GlobalDpkRuntimeStarted || dpk_embed_runtime_is_ready() == 0)
    {
        fprintf(stderr, "%s [error][%s]: embedded DPK runtime is not ready during response setup\n", __func__, chName);
        ret = -1;
        goto SetUpForResponsesReturn;
    }

    Ch->ResponseDMAWriteDataMr =
        ibv_reg_mr(PDomain, Ch->ResponseDMAWriteDataBuff, BACKEND_RESPONSE_BUFFER_SIZE,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!Ch->ResponseDMAWriteDataMr)
    {
        fprintf(stderr, "%s [error][%s]: ibv_reg_mr for DMA read data failed\n", __func__, chName);
        ret = -1;
        goto FreeBuffDMAReadDataBuffForResponsesReturn;
    }

    //
    // Read meta buffer and region
    //
    //
    Ch->ResponseDMAReadMetaMr =
        ibv_reg_mr(PDomain, Ch->ResponseDMAReadMetaBuff, RING_BUFFER_RESPONSE_META_DATA_SIZE,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!Ch->ResponseDMAReadMetaMr)
    {
        fprintf(stderr, "%s [error][%s]: ibv_reg_mr for DMA read meta failed\n", __func__, chName);
        ret = -1;
        goto DeregisterBuffReadDataMrForResponsesReturn;
    }

    //
    // Write meta buffer and region
    //
    //
    Ch->ResponseDMAWriteMetaBuff = (char *)&Ch->ResponseRing.TailC;
    Ch->ResponseDMAWriteMetaMr =
        ibv_reg_mr(PDomain, Ch->ResponseDMAWriteMetaBuff, sizeof(int),
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!Ch->ResponseDMAWriteMetaMr)
    {
        fprintf(stderr, "%s [error][%s]: ibv_reg_mr for DMA write meta failed\n", __func__, chName);
        ret = -1;
        goto DeregisterBuffReadMetaMrForResponsesReturn;
    }

    //
    // Set up work requests
    //
    //
    Ch->ResponseDMAWriteDataSgl.addr = (uint64_t)Ch->ResponseDMAWriteDataBuff;
    Ch->ResponseDMAWriteDataSgl.length = BACKEND_RESPONSE_MAX_DMA_SIZE;
    Ch->ResponseDMAWriteDataSgl.lkey = Ch->ResponseDMAWriteDataMr->lkey;
    Ch->ResponseDMAWriteDataWr.opcode = IBV_WR_RDMA_WRITE;
    Ch->ResponseDMAWriteDataWr.send_flags = IBV_SEND_SIGNALED;
    Ch->ResponseDMAWriteDataWr.sg_list = &Ch->ResponseDMAWriteDataSgl;
    Ch->ResponseDMAWriteDataWr.num_sge = 1;
    Ch->ResponseDMAWriteDataWr.wr_id = BUFF_WRITE_RESPONSE_DATA_WR_ID;

    Ch->ResponseDMAWriteDataSplitSgl.addr = (uint64_t)Ch->ResponseDMAWriteDataBuff;
    Ch->ResponseDMAWriteDataSplitSgl.length = BACKEND_RESPONSE_MAX_DMA_SIZE;
    Ch->ResponseDMAWriteDataSplitSgl.lkey = Ch->ResponseDMAWriteDataMr->lkey;
    Ch->ResponseDMAWriteDataSplitWr.opcode = IBV_WR_RDMA_WRITE;
    Ch->ResponseDMAWriteDataSplitWr.send_flags = IBV_SEND_SIGNALED;
    Ch->ResponseDMAWriteDataSplitWr.sg_list = &Ch->ResponseDMAWriteDataSplitSgl;
    Ch->ResponseDMAWriteDataSplitWr.num_sge = 1;
    Ch->ResponseDMAWriteDataSplitWr.wr_id = BUFF_WRITE_RESPONSE_DATA_SPLIT_WR_ID;

    Ch->ResponseDMAReadMetaSgl.addr = (uint64_t)Ch->ResponseDMAReadMetaBuff;
    Ch->ResponseDMAReadMetaSgl.length = RING_BUFFER_RESPONSE_META_DATA_SIZE;
    Ch->ResponseDMAReadMetaSgl.lkey = Ch->ResponseDMAReadMetaMr->lkey;
    Ch->ResponseDMAReadMetaWr.opcode = IBV_WR_RDMA_READ;
    Ch->ResponseDMAReadMetaWr.send_flags = IBV_SEND_SIGNALED;
    Ch->ResponseDMAReadMetaWr.sg_list = &Ch->ResponseDMAReadMetaSgl;
    Ch->ResponseDMAReadMetaWr.num_sge = 1;
    Ch->ResponseDMAReadMetaWr.wr_id = BUFF_READ_RESPONSE_META_WR_ID;

    Ch->ResponseDMAWriteMetaSgl.addr = (uint64_t)Ch->ResponseDMAWriteMetaBuff;
    Ch->ResponseDMAWriteMetaSgl.length = sizeof(int);
    Ch->ResponseDMAWriteMetaSgl.lkey = Ch->ResponseDMAWriteMetaMr->lkey;
    // jason: always use write-with-immediate so host can gate on CQE before reading.
    Ch->ResponseDMAWriteMetaWr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    Ch->ResponseDMAWriteMetaWr.send_flags = IBV_SEND_SIGNALED;
    Ch->ResponseDMAWriteMetaWr.sg_list = &Ch->ResponseDMAWriteMetaSgl;
    Ch->ResponseDMAWriteMetaWr.num_sge = 1;
    Ch->ResponseDMAWriteMetaWr.wr_id = BUFF_WRITE_RESPONSE_META_WR_ID;

    ResetResponseTransferTracking(Ch);

    return 0;

DeregisterBuffReadMetaMrForResponsesReturn:
    ibv_dereg_mr(Ch->ResponseDMAReadMetaMr);

DeregisterBuffReadDataMrForResponsesReturn:
    ibv_dereg_mr(Ch->ResponseDMAWriteDataMr);

FreeBuffDMAReadDataBuffForResponsesReturn:
    // jason: shared IO pool is process-level; freed once during backend shutdown.

SetUpForResponsesReturn:
    return ret;
}

//
// Destroy regions and buffers for the response buffer (per-channel).
//
static void DestroyForResponses(BuffConnChannelState *Ch)
{
    // jason: shared IO pool is process-level; freed once during backend shutdown.
    Ch->ResponseDMAWriteDataBuff = NULL;

    ibv_dereg_mr(Ch->ResponseDMAWriteMetaMr);
    ibv_dereg_mr(Ch->ResponseDMAReadMetaMr);
    ibv_dereg_mr(Ch->ResponseDMAWriteDataMr);

    memset(&Ch->ResponseDMAWriteMetaSgl, 0, sizeof(Ch->ResponseDMAReadMetaSgl));
    memset(&Ch->ResponseDMAReadMetaSgl, 0, sizeof(Ch->ResponseDMAReadMetaSgl));
    memset(&Ch->ResponseDMAWriteDataSgl, 0, sizeof(Ch->ResponseDMAWriteDataSgl));

    memset(&Ch->ResponseDMAWriteMetaWr, 0, sizeof(Ch->ResponseDMAWriteMetaWr));
    memset(&Ch->ResponseDMAReadMetaWr, 0, sizeof(Ch->ResponseDMAReadMetaWr));
    memset(&Ch->ResponseDMAWriteDataWr, 0, sizeof(Ch->RequestDMAReadDataWr));

    Ch->ResponseRing.TailC = 0;
    Ch->ResponseRing.TailB = 0;
    Ch->ResponseRing.TailA = 0;
    ResetResponseTransferTracking(Ch);
}

//
// Set up regions and buffers for a buffer connection.
// Updated: use SPDK buf alignment for request/response ring buffers.
// Refactored: one BuffConn == one logical channel connection.
//
static int SetUpBuffRegionsAndBuffers(BuffConnConfig *BuffConn, BackEndConfig *Config)
{
    int ret = 0;
    FileIOSizeT bufAlign = 4096;
    FileService *FS = Config->FS;
    if (FS && FS->MasterSPDKContext)
    {
        bufAlign = FS->MasterSPDKContext->buf_align;
    }
    if (bufAlign == 0)
    {
        bufAlign = 4096;
    }

    //
    // Set up control messages for this connection.
    //
    //
    ret = SetUpForCtrlMsgs(BuffConn);
    if (ret)
    {
        fprintf(stderr, "%s [error]: SetUpForCtrlMsgs failed\n", __func__);
        goto SetUpBuffRegionsAndBuffersReturn;
    }

    //
    // Set up this connection's request/response rings.
    // ChannelState.ChannelIndex will be overwritten by BuffMsgHandler from
    // the registration message.
    //
    BuffConn->ChannelState.ChannelIndex = DDS_IO_CHANNEL_PRIMARY;

    ret = SetUpForRequests(&BuffConn->ChannelState, BuffConn->PDomain, bufAlign);
    if (ret)
    {
        fprintf(stderr, "%s [error]: SetUpForRequests failed for BuffConn#%u\n", __func__, (unsigned)BuffConn->BuffId);
        goto DeregisterBuffRecvMrForCtrlMsgsReturn;
    }

    ret = SetUpForResponses(&BuffConn->ChannelState, BuffConn->PDomain, bufAlign, Config->MaxBuffs, BuffConn->BuffId);
    if (ret)
    {
        fprintf(stderr, "%s [error]: SetUpForResponses failed for BuffConn#%u\n", __func__, (unsigned)BuffConn->BuffId);
        DestroyForRequests(&BuffConn->ChannelState);
        goto DeregisterBuffRecvMrForCtrlMsgsReturn;
    }

    return 0;

DeregisterBuffRecvMrForCtrlMsgsReturn:
    DestroyForCtrlMsgs(BuffConn);

SetUpBuffRegionsAndBuffersReturn:
    return ret;
}

//
// Destroy regions and buffers for a buffer connection.
//
static void DestroyBuffRegionsAndBuffers(BuffConnConfig *BuffConn)
{
    DestroyForCtrlMsgs(BuffConn);
    DestroyForRequests(&BuffConn->ChannelState);
    DestroyForResponses(&BuffConn->ChannelState);
}

//
// Find the id of a control connection
//
//
static int FindConnId(BackEndConfig *Config, struct rdma_cm_id *CmId, uint8_t *IsCtrl)
{
    int i;
    for (i = 0; i < Config->MaxClients; i++)
    {
        if (Config->CtrlConns[i].RemoteCmId == CmId)
        {
            *IsCtrl = TRUE;
            return i;
        }
    }
    for (i = 0; i < Config->MaxBuffs; i++)
    {
        if (Config->BuffConns[i].RemoteCmId == CmId)
        {
            *IsCtrl = FALSE;
            return i;
        }
    }

    return -1;
}

//
// Process communication channel events
//
//
static inline int ProcessCmEvents(BackEndConfig *Config, struct rdma_cm_event *Event)
{
    int ret = 0;

    switch (Event->event)
    {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
    {
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
        fprintf(stdout, "CM: RDMA_CM_EVENT_ADDR_RESOLVED\n");
#endif
        ret = rdma_resolve_route(Event->id, RESOLVE_TIMEOUT_MS);
        if (ret)
        {
            fprintf(stderr, "rdma_resolve_route error %d\n", ret);
        }
        rdma_ack_cm_event(Event);
        break;
    }
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
    {
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
        fprintf(stdout, "CM: RDMA_CM_EVENT_ROUTE_RESOLVED\n");
#endif
        rdma_ack_cm_event(Event);
        break;
    }
    case RDMA_CM_EVENT_CONNECT_REQUEST:
    {
        //
        // Check the type of this connection
        //
        //
        uint8_t privData = *(uint8_t *)Event->param.conn.private_data;
        switch (privData)
        {
        case CTRL_CONN_PRIV_DATA:
        {
            CtrlConnConfig *ctrlConn = NULL;

            for (int index = 0; index < Config->MaxClients; index++)
            {
                if (Config->CtrlConns[index].State == CONN_STATE_AVAILABLE)
                {
                    ctrlConn = &Config->CtrlConns[index];
                    break;
                }
            }

            if (ctrlConn)
            {
                struct rdma_conn_param connParam;
                struct ibv_recv_wr *badRecvWr;
                ctrlConn->RemoteCmId = Event->id;
                rdma_ack_cm_event(Event);

                //
                // Set up QPair
                //
                //
                ret = SetUpCtrlQPair(ctrlConn);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: SetUpCtrlQPair failed\n", __func__);
                    break;
                }

                //
                // Set up regions and buffers
                //
                //
                ret = SetUpCtrlRegionsAndBuffers(ctrlConn);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: SetUpCtrlRegionsAndBuffers failed\n", __func__);
                    DestroyCtrlQPair(ctrlConn);
                    break;
                }

                //
                // Post a receive
                //
                //
                ret = ibv_post_recv(ctrlConn->QPair, &ctrlConn->RecvWr, &badRecvWr);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: ibv_post_recv failed %d\n", __func__, ret);
                    DestroyCtrlRegionsAndBuffers(ctrlConn);
                    DestroyCtrlQPair(ctrlConn);
                    break;
                }

                //
                // Accept the connection
                //
                //
                memset(&connParam, 0, sizeof(connParam));
                connParam.responder_resources = CTRL_RECVQ_DEPTH;
                connParam.initiator_depth = CTRL_SENDQ_DEPTH;
                ret = rdma_accept(ctrlConn->RemoteCmId, &connParam);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: rdma_accept failed %d\n", __func__, ret);
                    DestroyCtrlRegionsAndBuffers(ctrlConn);
                    DestroyCtrlQPair(ctrlConn);
                    break;
                }

                //
                // Mark this connection unavailable
                //
                //
                ctrlConn->State = CONN_STATE_OCCUPIED;
                fprintf(stdout, "Control connection #%d is accepted\n", ctrlConn->CtrlId);
            }
            else
            {
                fprintf(stderr, "%s [error]: no available control connection\n", __func__);
                rdma_ack_cm_event(Event);
            }

            break;
        }
        case BUFF_CONN_PRIV_DATA:
        {
            BuffConnConfig *buffConn = NULL;
            int index;

            // Original single-channel loop retained for reference:
            // for (index = 0; index < Config->MaxClients; index++)
            //
            // Updated: BuffConns capacity is MaxBuffs (one per channel connection).
            for (index = 0; index < Config->MaxBuffs; index++)
            {
                if (Config->BuffConns[index].State == CONN_STATE_AVAILABLE)
                {
                    buffConn = &Config->BuffConns[index];
                    break;
                }
            }

            if (buffConn)
            {
                struct rdma_conn_param connParam;
                struct ibv_recv_wr *badRecvWr;
                buffConn->RemoteCmId = Event->id;
                rdma_ack_cm_event(Event);

                //
                // Set up QPair
                //
                //
                ret = SetUpBuffQPair(buffConn);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: SetUpBuffQPair failed\n", __func__);
                    break;
                }

                //
                // Set up regions and buffers
                //
                //
                ret = SetUpBuffRegionsAndBuffers(buffConn, Config);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: SetUpCtrlRegionsAndBuffers failed\n", __func__);
                    DestroyBuffQPair(buffConn);
                    break;
                }

                //
                // Post a receive on this connection's QPair.
                //
                //
                ret = ibv_post_recv(buffConn->ChannelState.QPair, &buffConn->RecvWr, &badRecvWr);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: ibv_post_recv failed %d\n", __func__, ret);
                    DestroyBuffRegionsAndBuffers(buffConn);
                    DestroyBuffQPair(buffConn);
                    break;
                }

                //
                // Accept the connection
                //
                //
                memset(&connParam, 0, sizeof(connParam));
                connParam.responder_resources = BUFF_RECVQ_DEPTH;
                connParam.initiator_depth = BUFF_SENDQ_DEPTH;
                ret = rdma_accept(buffConn->RemoteCmId, &connParam);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: rdma_accept failed %d\n", __func__, ret);
                    DestroyBuffRegionsAndBuffers(buffConn);
                    DestroyBuffQPair(buffConn);
                    break;
                }

                //
                // Mark this connection unavailable
                //
                //
                buffConn->State = CONN_STATE_OCCUPIED;
                fprintf(stdout, "Buffer connection #%d is accepted\n", buffConn->BuffId);
            }
            else
            {
                fprintf(stderr, "No available buffer connection\n");
                rdma_ack_cm_event(Event);
            }

            break;
        }
        default:
        {
            fprintf(stderr, "CM: unrecognized connection type\n");
            rdma_ack_cm_event(Event);
            break;
        }
        }
        break;
    }
    case RDMA_CM_EVENT_ESTABLISHED:
    {
        uint8_t isCtrl;
        int connId = FindConnId(Config, Event->id, &isCtrl);
        if (connId >= 0)
        {
            if (isCtrl)
            {
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
                fprintf(stdout, "CM: RDMA_CM_EVENT_ESTABLISHED for Control Conn#%d\n", connId);
                CtrlConnConfig *ctrlConn = &Config->CtrlConns[connId];
                ctrlConn->State = CONN_STATE_CONNECTED;
#endif
            }
            else
            {
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
                fprintf(stdout, "CM: RDMA_CM_EVENT_ESTABLISHED for Buffer Conn#%d\n", connId);
                BuffConnConfig *buffConn = &Config->BuffConns[connId];
                buffConn->State = CONN_STATE_CONNECTED;
            }
#endif
        }
        else
        {
            fprintf(stderr, "CM: RDMA_CM_EVENT_ESTABLISHED with unrecognized connection\n");
        }

        rdma_ack_cm_event(Event);
        break;
    }
    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_CONNECT_ERROR:
    case RDMA_CM_EVENT_UNREACHABLE:
    case RDMA_CM_EVENT_REJECTED:
    {
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
        fprintf(stderr, "cma event %s, error %d\n", rdma_event_str(Event->event), Event->status);
#endif
        ret = -1;
        rdma_ack_cm_event(Event);
        break;
    }
    case RDMA_CM_EVENT_DISCONNECTED:
    {
        uint8_t isCtrl;
        int connId = FindConnId(Config, Event->id, &isCtrl);
        if (connId >= 0)
        {
            if (isCtrl)
            {
                if (Config->CtrlConns[connId].State != CONN_STATE_AVAILABLE)
                {
                    CtrlConnConfig *ctrlConn = &Config->CtrlConns[connId];
                    DestroyCtrlRegionsAndBuffers(ctrlConn);
                    DestroyCtrlQPair(ctrlConn);
                    ctrlConn->State = CONN_STATE_AVAILABLE;
                }
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
                fprintf(stdout, "CM: RDMA_CM_EVENT_DISCONNECTED for Control Conn#%d\n", connId);
#endif
            }
            else
            {
                if (Config->BuffConns[connId].State != CONN_STATE_AVAILABLE)
                {
                    BuffConnConfig *buffConn = &Config->BuffConns[connId];
                    DestroyBuffRegionsAndBuffers(buffConn);
                    DestroyBuffQPair(buffConn);
                    buffConn->State = CONN_STATE_AVAILABLE;
                }
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
                fprintf(stdout, "CM: RDMA_CM_EVENT_DISCONNECTED for Buffer Conn#%d\n", connId);
#endif
            }
        }
        else
        {
            fprintf(stderr, "CM: RDMA_CM_EVENT_DISCONNECTED with unrecognized connection\n");
        }

        rdma_ack_cm_event(Event);
        break;
    }
    case RDMA_CM_EVENT_TIMEWAIT_EXIT:
    {
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
        fprintf(stdout, "CM: RDMA_CM_EVENT_TIMEWAIT_EXIT\n");
#endif
        // jason: TIMEWAIT_EXIT is expected after DISCONNECTED; do not treat as fatal.
        rdma_ack_cm_event(Event);
        break;
    }
    case RDMA_CM_EVENT_DEVICE_REMOVAL:
    {
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
        fprintf(stderr, "CM: RDMA_CM_EVENT_DEVICE_REMOVAL\n");
#endif
        ret = -1;
        rdma_ack_cm_event(Event);
        break;
    }
    default:
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
        fprintf(stderr, "oof bad type!\n");
#endif
        ret = -1;
        rdma_ack_cm_event(Event);
        break;
    }

    return ret;
}

static const char *basename_of(const char *path)
{
    const char *p = strrchr(path, '/');
    return p ? p + 1 : path;
}

//
// Control message handler
//
// jason: FIND_FIRST_FILE requests now route through the control-plane
// handler so completion responses are posted in one consistent path.
//
static inline int CtrlMsgHandler(CtrlConnConfig *CtrlConn, FileService *FS)
{
    int ret = 0;
    MsgHeader *msgIn = (MsgHeader *)CtrlConn->RecvBuff;
    MsgHeader *msgOut = (MsgHeader *)CtrlConn->SendBuff;
    printf("CtrlMsgHandler: %d\n", msgIn->MsgId);
    switch (msgIn->MsgId)
    {
    //
    // Request client id
    //
    //
    case CTRL_MSG_F2B_REQUEST_ID:
    {
        CtrlMsgB2FRespondId *resp = (CtrlMsgB2FRespondId *)(msgOut + 1);
        struct ibv_send_wr *badSendWr = NULL;
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Send the request id
        //
        //
        msgOut->MsgId = CTRL_MSG_B2F_RESPOND_ID;
        resp->ClientId = CtrlConn->CtrlId;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FRespondId);
        ret = ibv_post_send(CtrlConn->QPair, &CtrlConn->SendWr, &badSendWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
            ret = -1;
        }
    }
    break;
    //
    // The client wants to terminate
    //
    //
    case CTRL_MSG_F2B_TERMINATE:
    {
        CtrlMsgF2BTerminate *req = (CtrlMsgF2BTerminate *)(msgIn + 1);

        if (req->ClientId == CtrlConn->CtrlId)
        {
            // DestroyCtrlRegionsAndBuffers(CtrlConn);
            // DestroyCtrlQPair(CtrlConn);
            // CtrlConn->State = CONN_STATE_AVAILABLE;
            //
            // jason: Request a CM disconnect so the host side can observe a
            // RDMA_CM_EVENT_DISCONNECTED event. Cleanup is deferred to the
            // CM event handler (ProcessCmEvents) to avoid double-destroy.
            if (CtrlConn->RemoteCmId)
            {
                int discRet = rdma_disconnect(CtrlConn->RemoteCmId);
                if (discRet)
                {
                    fprintf(stderr, "%s [error]: rdma_disconnect failed: %d\n", __func__, discRet);
                    // jason: Fallback to the original immediate cleanup if
                    // rdma_disconnect cannot be issued.
                    DestroyCtrlRegionsAndBuffers(CtrlConn);
                    DestroyCtrlQPair(CtrlConn);
                    CtrlConn->State = CONN_STATE_AVAILABLE;
                }
                else
                {
                    CtrlConn->State = CONN_STATE_DISCONNECTING;
                }
            }
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
            fprintf(stdout, "%s [info]: Control Conn#%d disconnect requested\n", __func__, req->ClientId);
#endif
        }
        else
        {
            fprintf(stderr, "%s [error]: mismatched client id\n", __func__);
        }
    }
    break;
    //
    // CreateDirectory request
    //
    //
    case CTRL_MSG_F2B_REQ_CREATE_DIR:
    {
        CtrlMsgF2BReqCreateDirectory *req = (CtrlMsgF2BReqCreateDirectory *)(msgIn + 1);
        CtrlMsgB2FAckCreateDirectory *resp = (CtrlMsgB2FAckCreateDirectory *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Create the directory
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_CREATE_DIR;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SPDK_NOTICELOG("submit create dir control plane request, id: %d, req: %p, result: %d\n",
                       CtrlConn->PendingControlPlaneRequest.RequestId, CtrlConn->PendingControlPlaneRequest.Request,
                       resp->Result);
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_CREATE_DIR;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckCreateDirectory);
    }
    break;
    //
    // RemoveDirectory request
    //
    //
    case CTRL_MSG_F2B_REQ_REMOVE_DIR:
    {
        CtrlMsgF2BReqRemoveDirectory *req = (CtrlMsgF2BReqRemoveDirectory *)(msgIn + 1);
        CtrlMsgB2FAckRemoveDirectory *resp = (CtrlMsgB2FAckRemoveDirectory *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Remove the directory
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_REMOVE_DIR;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_REMOVE_DIR;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckRemoveDirectory);
    }
    break;
    case CTRL_MSG_F2B_REQ_FIND_FIRST_FILE:
    {
        printf("CTRL_MSG_F2B_REQ_FIND_FIRST_FILE\n");
        CtrlMsgF2BReqFindFirstFile *req = (CtrlMsgF2BReqFindFirstFile *)(msgIn + 1);
        CtrlMsgB2FAckFindFirstFile *resp = (CtrlMsgB2FAckFindFirstFile *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            printf("%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        // jason: Use the control-plane handler path for consistency with
        // other control operations; response is sent by completion logic.
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_FIND_FIRST_FILE;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        // jason: Reset FileId while the request is pending to avoid stale values.
        resp->FileId = DDS_FILE_INVALID;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_FIND_FIRST_FILE;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckFindFirstFile);
    }
    break;
    //
    // CreateFile request
    //
    //
    case CTRL_MSG_F2B_REQ_CREATE_FILE:
    {
        CtrlMsgF2BReqCreateFile *req = (CtrlMsgF2BReqCreateFile *)(msgIn + 1);
        CtrlMsgB2FAckCreateFile *resp = (CtrlMsgB2FAckCreateFile *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;
        printf("CTRL_MSG_F2B_REQ_CREATE_FILE FileName: %s\n", req->FileName);

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Create the file
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_CREATE_FILE;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_CREATE_FILE;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckCreateFile);
    }
    break;
    //
    // DeleteFile request
    //
    //
    case CTRL_MSG_F2B_REQ_DELETE_FILE:
    {
        CtrlMsgF2BReqDeleteFile *req = (CtrlMsgF2BReqDeleteFile *)(msgIn + 1);
        CtrlMsgB2FAckDeleteFile *resp = (CtrlMsgB2FAckDeleteFile *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Delete the file
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_DELETE_FILE;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_DELETE_FILE;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckDeleteFile);
    }
    break;
    //
    // ChangeFileSize request
    //
    //
    case CTRL_MSG_F2B_REQ_CHANGE_FILE_SIZE:
    {
        CtrlMsgF2BReqChangeFileSize *req = (CtrlMsgF2BReqChangeFileSize *)(msgIn + 1);
        CtrlMsgB2FAckChangeFileSize *resp = (CtrlMsgB2FAckChangeFileSize *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Change the file size
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_CHANGE_FILE_SIZE;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_CHANGE_FILE_SIZE;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckChangeFileSize);
    }
    break;
    //
    // GetFileSize request
    //
    //
    case CTRL_MSG_F2B_REQ_GET_FILE_SIZE:
    {
        CtrlMsgF2BReqGetFileSize *req = (CtrlMsgF2BReqGetFileSize *)(msgIn + 1);
        CtrlMsgB2FAckGetFileSize *resp = (CtrlMsgB2FAckGetFileSize *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Get the file size
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_GET_FILE_SIZE;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;

        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_GET_FILE_SIZE;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckGetFileSize);

        if (Sto == NULL)
        {
            printf("Storage is undefined\n");
        }
        struct DPUFile *file = Sto->AllFiles[req->FileId];
        if (file == NULL)
        {
            printf("File %d is undefined\n", req->FileId);
        }

        printf("FileId: %d, FileSize: %ld\n", req->FileId, GetSize(file));
        resp->Result = DDS_ERROR_CODE_SUCCESS;
        resp->FileSize = GetSize(file);
    }
    break;
    //
    // GetFileInformationById request
    //
    //
    case CTRL_MSG_F2B_REQ_GET_FILE_INFO:
    {
        CtrlMsgF2BReqGetFileInfo *req = (CtrlMsgF2BReqGetFileInfo *)(msgIn + 1);
        CtrlMsgB2FAckGetFileInfo *resp = (CtrlMsgB2FAckGetFileInfo *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Get the file info
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_GET_FILE_INFO;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_GET_FILE_INFO;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckGetFileInfo);
    }
    break;
    //
    // GetFileAttributes request
    //
    //
    case CTRL_MSG_F2B_REQ_GET_FILE_ATTR:
    {
        CtrlMsgF2BReqGetFileAttr *req = (CtrlMsgF2BReqGetFileAttr *)(msgIn + 1);
        CtrlMsgB2FAckGetFileAttr *resp = (CtrlMsgB2FAckGetFileAttr *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Get the file attributes
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_GET_FILE_ATTR;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_GET_FILE_ATTR;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckGetFileAttr);
    }
    break;
    //
    // GetStorageFreeSpace request
    //
    //
    case CTRL_MSG_F2B_REQ_GET_FREE_SPACE:
    {
        CtrlMsgF2BReqGetFreeSpace *req = (CtrlMsgF2BReqGetFreeSpace *)(msgIn + 1);
        CtrlMsgB2FAckGetFreeSpace *resp = (CtrlMsgB2FAckGetFreeSpace *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Get the free storage space
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_GET_FREE_SPACE;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_GET_FREE_SPACE;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckGetFreeSpace);
    }
    break;
    //
    // GetIoAlign request
    //
    //
    case CTRL_MSG_F2B_REQ_GET_IO_ALIGN:
    {
        CtrlMsgF2BReqGetIoAlign *req = (CtrlMsgF2BReqGetIoAlign *)(msgIn + 1);
        CtrlMsgB2FAckGetIoAlign *resp = (CtrlMsgB2FAckGetIoAlign *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Get the I/O alignment
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_GET_IO_ALIGN;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_GET_IO_ALIGN;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckGetIoAlign);
    }
    break;
    //
    // SetRead2AesKey request
    //
    //
    case CTRL_MSG_F2B_REQ_SET_READ2_AES_KEY:
    {
        CtrlMsgF2BReqSetRead2AesKey *req = (CtrlMsgF2BReqSetRead2AesKey *)(msgIn + 1);
        CtrlMsgB2FAckSetRead2AesKey *resp = (CtrlMsgB2FAckSetRead2AesKey *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Submit key setup to control-plane worker
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_SET_READ2_AES_KEY;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_SET_READ2_AES_KEY;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckSetRead2AesKey);
    }
    break;
    //
    // MoveFile request
    //
    //
    case CTRL_MSG_F2B_REQ_MOVE_FILE:
    {
        CtrlMsgF2BReqMoveFile *req = (CtrlMsgF2BReqMoveFile *)(msgIn + 1);
        CtrlMsgB2FAckMoveFile *resp = (CtrlMsgB2FAckMoveFile *)(msgOut + 1);
        struct ibv_recv_wr *badRecvWr = NULL;

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(CtrlConn->QPair, &CtrlConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed: %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Move the file
        //
        //
        CtrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_MOVE_FILE;
        CtrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
        CtrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
        resp->Result = DDS_ERROR_CODE_IO_PENDING;
        SubmitControlPlaneRequest(FS, &CtrlConn->PendingControlPlaneRequest);

        msgOut->MsgId = CTRL_MSG_B2F_ACK_MOVE_FILE;
        CtrlConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(CtrlMsgB2FAckMoveFile);
    }
    break;
    default:
        fprintf(stderr, "%s [error]: unrecognized control message1\n", __func__);
        ret = -1;
        break;
    }

    return ret;
}

//
// Process communication channel events for contorl connections
//
//
static int inline ProcessCtrlCqEvents(BackEndConfig *Config)
{
    int ret = 0;
    CtrlConnConfig *ctrlConn = NULL;
    struct ibv_wc wc;

    for (int i = 0; i != Config->MaxClients; i++)
    {
        ctrlConn = &Config->CtrlConns[i];
        if (ctrlConn->State != CONN_STATE_CONNECTED)
        {
            continue;
        }

        if ((ret = ibv_poll_cq(ctrlConn->CompQ, 1, &wc)) == 1)
        {
            ret = 0;
            if (wc.status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "%s [error]: ibv_poll_cq failed status %d\n", __func__, wc.status);
                ret = -1;
                continue;
            }

            //
            // Only receive events are expected
            //
            //
            switch (wc.opcode)
            {
            case IBV_WC_RECV:
            {
                ret = CtrlMsgHandler(ctrlConn, Config->FS);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: CtrlMsgHandler failed\n", __func__);
                    goto ProcessCtrlCqEventsReturn;
                }
            }
            break;
            case IBV_WC_SEND:
            case IBV_WC_RDMA_WRITE:
            case IBV_WC_RDMA_READ:
                break;

            default:
                fprintf(stderr, "%s [error]: unknown completion\n", __func__);
                ret = -1;
                break;
            }
        }
    }

ProcessCtrlCqEventsReturn:
    return ret;
}

//
// Buffer message handler
//
//
static inline int BuffMsgHandler(BuffConnConfig *BuffConn)
{
    int ret = 0;
    // Each BuffConnConfig owns one channel state; the logical channel identity
    // is set from the registration message's ChannelIndex field.
    BuffConnChannelState *ch = &BuffConn->ChannelState;
    MsgHeader *msgIn = (MsgHeader *)BuffConn->RecvBuff;
    MsgHeader *msgOut = (MsgHeader *)BuffConn->SendBuff;

    switch (msgIn->MsgId)
    {
    case BUFF_MSG_F2B_REQUEST_ID:
    {
        BuffMsgF2BRequestId *req = (BuffMsgF2BRequestId *)(msgIn + 1);
        BuffMsgB2FRespondId *resp = (BuffMsgB2FRespondId *)(msgOut + 1);
        struct ibv_send_wr *badSendWr = NULL;
        struct ibv_recv_wr *badRecvWr = NULL;

        // Set logical channel identity from the registration message.
        // The frontend sends a separate RDMA connection per channel, each
        // tagged with its ChannelIndex. This lets the backend identify which
        // logical IO channel this BuffConnConfig serves.
        if (req->ChannelIndex >= 0 && req->ChannelIndex < DDS_NUM_IO_CHANNELS)
        {
            ch->ChannelIndex = req->ChannelIndex;
        }
        else
        {
            fprintf(stderr, "%s [warn]: invalid ChannelIndex=%d in registration, defaulting to 0\n",
                    __func__, req->ChannelIndex);
            ch->ChannelIndex = 0;
        }
        fprintf(stdout, "%s [info]: BuffConn#%u registered as %s (ClientId=%d)\n",
                __func__, (unsigned)BuffConn->BuffId, DDS_ChannelName(ch->ChannelIndex), req->ClientId);

        //
        // Post a receive first
        //
        //
        ret = ibv_post_recv(ch->QPair, &BuffConn->RecvWr, &badRecvWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_recv failed %d\n", __func__, ret);
            ret = -1;
        }

        //
        // Update config and send the buffer id
        //
        //
        BuffConn->CtrlId = req->ClientId;
        InitializeRingBufferBackEnd(&ch->RequestRing, &ch->ResponseRing, req->BufferAddress,
                                    req->AccessToken, req->Capacity);
        // jason: buffer reconnect can reuse BuffConn while previous query contexts still hold pointers.
        // Reset per-request backend contexts here so stale IO_PENDING from a prior ring cannot poison new requests.
        memset(ch->PendingDataPlaneRequests, 0, sizeof(ch->PendingDataPlaneRequests));
        // Task 5.2: Use per-channel constant since PendingDataPlaneRequests is sized per-channel.
        for (RequestIdT i = 0; i < DDS_MAX_OUTSTANDING_IO_PER_CHANNEL; i++)
        {
            ch->PendingDataPlaneRequests[i].BatchNextRequestId = DDS_REQUEST_INVALID;
            ch->PendingDataPlaneRequests[i].ChannelIndex = -1;
        }
        ch->RequestPendingHead = 0;
        ch->RequestPendingHeadValid = 0;
        ch->ResponseRing.TailA = 0;
        ch->ResponseRing.TailB = 0;
        ch->ResponseRing.TailC = 0;
        ResetResponseTransferTracking(ch);
        fprintf(stdout, "%s [info]: reset pending request contexts for Buffer Conn#%u (attach)\n", __func__,
                (unsigned)BuffConn->BuffId);

        ch->RequestDMAReadMetaWr.wr.rdma.remote_addr = ch->RequestRing.ReadMetaAddr;
        ch->RequestDMAReadMetaWr.wr.rdma.rkey = ch->RequestRing.AccessToken;
        ch->RequestDMAWriteMetaWr.wr.rdma.remote_addr = ch->RequestRing.WriteMetaAddr;
        ch->RequestDMAWriteMetaWr.wr.rdma.rkey = ch->RequestRing.AccessToken;
        ch->RequestDMAReadDataWr.wr.rdma.rkey = ch->RequestRing.AccessToken;
        ch->RequestDMAReadDataSplitWr.wr.rdma.rkey = ch->RequestRing.AccessToken;

        ch->ResponseDMAReadMetaWr.wr.rdma.remote_addr = ch->ResponseRing.ReadMetaAddr;
        ch->ResponseDMAReadMetaWr.wr.rdma.rkey = ch->ResponseRing.AccessToken;
        ch->ResponseDMAWriteMetaWr.wr.rdma.remote_addr = ch->ResponseRing.WriteMetaAddr;
        ch->ResponseDMAWriteMetaWr.wr.rdma.rkey = ch->ResponseRing.AccessToken;
        ch->ResponseDMAWriteDataWr.wr.rdma.rkey = ch->ResponseRing.AccessToken;
        ch->ResponseDMAWriteDataSplitWr.wr.rdma.rkey = ch->ResponseRing.AccessToken;

        msgOut->MsgId = BUFF_MSG_B2F_RESPOND_ID;
        resp->BufferId = BuffConn->BuffId;
        BuffConn->SendWr.sg_list->length = sizeof(MsgHeader) + sizeof(BuffMsgB2FRespondId);
        ret = ibv_post_send(ch->QPair, &BuffConn->SendWr, &badSendWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
            ret = -1;
        }

#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
        fprintf(stdout, "%s [info]: Buffer Conn#%d is for Client#%d\n", __func__, BuffConn->BuffId, BuffConn->CtrlId);
        fprintf(stdout, "- Buffer address: %p\n", (void *)ch->RequestRing.RemoteAddr);
        fprintf(stdout, "- Buffer capacity: %u\n", ch->RequestRing.Capacity);
        fprintf(stdout, "- Access token: %x\n", ch->RequestRing.AccessToken);
        fprintf(stdout, "- Request ring data base address: %p\n", (void *)ch->RequestRing.DataBaseAddr);
        fprintf(stdout, "- Response ring data base address: %p\n", (void *)ch->ResponseRing.DataBaseAddr);
#endif
        //
        // Start polling requests
        //
        //
        ret = ibv_post_send(ch->QPair, &ch->RequestDMAReadMetaWr, &badSendWr);
        if (ret)
        {
            fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
            ret = -1;
        }
    }
    break;
    case BUFF_MSG_F2B_RELEASE:
    {
        BuffMsgF2BRelease *req = (BuffMsgF2BRelease *)(msgIn + 1);

        if (req->BufferId == BuffConn->BuffId && req->ClientId == BuffConn->CtrlId)
        {
            // jason: clear pending contexts before tearing down ring buffers to avoid stale-pointer reuse on next attach.
            memset(ch->PendingDataPlaneRequests, 0, sizeof(ch->PendingDataPlaneRequests));
            // Task 5.2: Use per-channel constant for release/detach path.
            for (RequestIdT i = 0; i < DDS_MAX_OUTSTANDING_IO_PER_CHANNEL; i++)
            {
                ch->PendingDataPlaneRequests[i].BatchNextRequestId = DDS_REQUEST_INVALID;
                ch->PendingDataPlaneRequests[i].ChannelIndex = -1;
            }
            ch->RequestPendingHead = 0;
            ch->RequestPendingHeadValid = 0;
            DestroyBuffRegionsAndBuffers(BuffConn);
            DestroyBuffQPair(BuffConn);
            BuffConn->State = CONN_STATE_AVAILABLE;
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
            fprintf(stdout, "%s [info]: Buffer Conn#%d (Client#%d) is disconnected\n", __func__, req->BufferId,
                    req->ClientId);
#endif
        }
        else
        {
            fprintf(stderr, "%s [error]: mismatched client id\n", __func__);
        }
    }
    break;
    default:
        fprintf(stderr, "%s [error]: unrecognized control message2\n", __func__);
        ret = -1;
        break;
    }

    return ret;
}

//
// Read request payload bytes from the request ring, handling wrap-around.
// Used to parse read2 stage descriptors appended after the request header.
//
static inline void ReadRequestPayloadBytes(const char *RequestRing, int PayloadOffset, void *Dst, FileIOSizeT Bytes)
{
    if (!Dst || Bytes == 0)
    {
        return;
    }
    int bytesToEnd = BACKEND_REQUEST_BUFFER_SIZE - PayloadOffset;
    if ((int)Bytes <= bytesToEnd)
    {
        memcpy(Dst, RequestRing + PayloadOffset, (size_t)Bytes);
        return;
    }
    memcpy(Dst, RequestRing + PayloadOffset, (size_t)bytesToEnd);
    memcpy((char *)Dst + bytesToEnd, RequestRing, (size_t)Bytes - (size_t)bytesToEnd);
}

//
// Slice a contiguous region on the response ring into a SplittableBufferT.
// The slice begins at PayloadStart + SliceOffset and may wrap across the ring end.
//
static inline void BuildResponseSlice(char *ResponseRing, int PayloadStart, FileIOSizeT SliceOffset,
                                      FileIOSizeT SliceBytes, SplittableBufferT *Out)
{
    if (!Out)
    {
        return;
    }
    Out->TotalSize = SliceBytes;
    if (SliceBytes == 0)
    {
        Out->FirstAddr = ResponseRing + PayloadStart;
        Out->FirstSize = 0;
        Out->SecondAddr = NULL;
        return;
    }
    int start = (PayloadStart + (int)SliceOffset) % BACKEND_RESPONSE_BUFFER_SIZE;
    int bytesToEnd = BACKEND_RESPONSE_BUFFER_SIZE - start;
    if ((int)SliceBytes <= bytesToEnd)
    {
        Out->FirstAddr = ResponseRing + start;
        Out->FirstSize = SliceBytes;
        Out->SecondAddr = NULL;
        return;
    }
    Out->FirstAddr = ResponseRing + start;
    Out->FirstSize = (FileIOSizeT)bytesToEnd;
    Out->SecondAddr = ResponseRing;
}

//
// Returns true when a response header pointer belongs to the current response ring buffer.
// Used to reject stale pointers cached in PendingDataPlaneRequests across reconnect/error paths.
//
static inline bool ResponsePointerWithinRing(const char *ResponseRingBase, const BuffMsgB2FAckHeader *Resp)
{
    if (ResponseRingBase == NULL || Resp == NULL)
    {
        return false;
    }
    uintptr_t ringStart = (uintptr_t)ResponseRingBase;
    uintptr_t ringEnd = ringStart + (uintptr_t)BACKEND_RESPONSE_BUFFER_SIZE;
    uintptr_t respPtr = (uintptr_t)Resp;
    if (respPtr < ringStart || respPtr >= ringEnd)
    {
        return false;
    }
    uintptr_t respEnd = respPtr + (uintptr_t)sizeof(BuffMsgB2FAckHeader);
    if (respEnd < respPtr || respEnd > ringEnd)
    {
        return false;
    }
    return true;
}

//
// Marks a request-id context as completed after its response Result is no longer IO_PENDING.
// This is the authoritative reuse gate (instead of probing stale response-ring memory by pointer).
//
//
// Mark a request context as completed on a specific channel.
// Refactored: takes BuffConnChannelState* instead of BuffConnConfig*.
//
static inline void MarkRequestContextCompleted(BuffConnChannelState *Ch, const BuffMsgB2FAckHeader *Resp, const char *PathTag)
{
    if (Ch == NULL || Resp == NULL)
    {
        return;
    }
    RequestIdT reqId = Resp->RequestId;
    if (!DDS_ValidateRequestId(reqId))
    {
        fprintf(stderr, "%s [warn][%s]: completion carried invalid reqId=%u (path=%s)\n", __func__,
                DDS_ChannelName(Ch->ChannelIndex), (unsigned)reqId, PathTag ? PathTag : "unknown");
        return;
    }
    DataPlaneRequestContext *ctx = &Ch->PendingDataPlaneRequests[reqId];
    if (ctx->ChannelIndex != -1 && ctx->ChannelIndex != Ch->ChannelIndex)
    {
        fprintf(stderr,
                "%s [warn][%s]: completion channel mismatch for reqId=%u (ctxChannel=%d path=%s)\n",
                __func__, DDS_ChannelName(Ch->ChannelIndex), (unsigned)reqId, (int)ctx->ChannelIndex,
                PathTag ? PathTag : "unknown");
    }
    ctx->IsPending = 0;
    ctx->Request = NULL;
    ctx->Response = NULL;
    ctx->BatchNextRequestId = DDS_REQUEST_INVALID;
    ctx->ChannelIndex = -1;

    // Per-channel debug counter: one completion processed.
    Ch->CompletionsProcessed++;
}

//
// Execute received requests.
// jason: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
// jason: use deferred request head to avoid releasing host space before consumption.
// jason: publish response Result atomically so poller sees BytesServiced first.
// jason: align read payloads to bdev buffer alignment with padding after headers.
// jason: request type parsing uses OpCode; read2 stage descriptors are validated after header parse.
// jason: persist CopyStart and stage buffers for unaligned reads and read2 stage chaining.
// Diagnostic: logs ring/header state when a request header looks corrupted.
//
static inline void ExecuteRequests(BuffConnChannelState *Ch, FileService *FS)
{
    char *buffReq;
    char *buffResp;
    char *curReq;
    BuffMsgF2BReqHeader *curReqObj;
    FileIOSizeT curReqSize;
    FileIOSizeT bytesParsed = 0;
    FileIOSizeT bytesTotal = Ch->RequestDMAReadDataSize;
    FileIOSizeT requestHeaderBytes = (FileIOSizeT)(sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader));
    FileIOSizeT responseHeaderBytes = (FileIOSizeT)(sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));
    FileIOSizeT requestAlign = (FileIOSizeT)DDS_ALIGNOF(BuffMsgF2BReqHeader);
#ifndef RING_BUFFER_RESPONSE_BATCH_ENABLED
    (void)responseHeaderBytes;
#endif

    // int tailReq = Ch->RequestRing.Head;
    // int headReq = tailReq >= bytesTotal ? tailReq - bytesTotal : BACKEND_REQUEST_BUFFER_SIZE + tailReq - bytesTotal;
    // jason: use pending head when deferred, so headReq maps to the original read start.
    int tailReq = Ch->RequestPendingHeadValid ? Ch->RequestPendingHead : Ch->RequestRing.Head;
    int headReq = tailReq >= bytesTotal ? tailReq - bytesTotal : BACKEND_REQUEST_BUFFER_SIZE + tailReq - bytesTotal;
    int tailResp = Ch->ResponseRing.TailA;
    // int headResp = Ch->ResponseRing.TailB;
    // Original reclaim boundary retained for reference:
    // int headResp = Ch->ResponseRing.TailC;
    // Reuse local response-source bytes only after response data WR CQEs
    // confirm the RNIC has finished reading them.
    int headResp = Ch->ResponseReclaimTail;
    int respRingCapacity =
        tailResp >= headResp ? (BACKEND_RESPONSE_BUFFER_SIZE - tailResp + headResp) : (headResp - tailResp);

    int progressReqForParsing;
    FileIOSizeT reqSize;
    FileIOSizeT paddedReqSize;
    FileIOSizeT respSize = 0;
    FileIOSizeT totalRespSize = 0;
    int progressReq = headReq;
    int progressResp = tailResp;

    DataPlaneRequestContext *ctxt = NULL;
    SplittableBufferT *dataBuff = NULL;

    buffReq = Ch->RequestDMAReadDataBuff;
    buffResp = Ch->ResponseDMAWriteDataBuff;

#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
    //
    // We only need |FileIOSizeT| bytes but use |FileIOSizeT| + |BuffMsgB2FAckHeader| for alignment
    //
    //

    // jason: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
    if (progressResp + (int)responseHeaderBytes > BACKEND_RESPONSE_BUFFER_SIZE)
    {
        FileIOSizeT slackBytes = (FileIOSizeT)(BACKEND_RESPONSE_BUFFER_SIZE - progressResp);
        totalRespSize += slackBytes;
        progressResp = 0;
    }

    BufferT batchMeta = buffResp + progressResp;
    progressResp += (sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));
    totalRespSize += (sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));
    if (progressResp >= BACKEND_RESPONSE_BUFFER_SIZE)
    {
        progressResp %= BACKEND_RESPONSE_BUFFER_SIZE;
    }
#endif

    DebugPrint("Requests have been received: total request bytes = %d\n", bytesTotal);

    //
    // Enables batching, will only submit once, will include a batch size with it
    // the submitted context will be the first one, subsequent processing should handle all in the batch
    // will also need to handle the wrap around of slots/indices
    //
    //
    RequestIdT batchSize = 0;

    //
    // Batch-chain head/tail keyed by RequestId.
    // firstIndex is the first request id in this parsed batch chain.
    //
    RequestIdT firstIndex = DDS_REQUEST_INVALID;
    RequestIdT prevIndex = DDS_REQUEST_INVALID;

    //
    // Parse all file requests in the batch
    //
    //
    while (bytesParsed != bytesTotal)
    {
        curReq = buffReq + progressReq;

        // jason: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
        if (progressReq + (int)requestHeaderBytes > BACKEND_REQUEST_BUFFER_SIZE)
        {
            // jason: wrap to the request header phase so the header's 8-byte fields stay aligned.
            FileIOSizeT slackBytes =
                (FileIOSizeT)(BACKEND_REQUEST_BUFFER_SIZE - progressReq) + (FileIOSizeT)DDS_REQUEST_RING_HEADER_PHASE;
            bytesParsed += slackBytes;
            progressReq = (progressReq + (int)slackBytes) % BACKEND_REQUEST_BUFFER_SIZE;
            continue;
        }

        reqSize = *(FileIOSizeT *)(curReq);
        if (reqSize == 0)
        {
            // jason: zero size marks producer-side wrap slack for no-split records.
            FileIOSizeT slackBytes =
                (FileIOSizeT)(BACKEND_REQUEST_BUFFER_SIZE - progressReq) + (FileIOSizeT)DDS_REQUEST_RING_HEADER_PHASE;
            if (slackBytes > (bytesTotal - bytesParsed))
            {
                fprintf(stderr, "%s [error]: request wrap slack %u exceeds remaining bytes %u\n", __func__,
                        (unsigned)slackBytes, (unsigned)(bytesTotal - bytesParsed));
                break;
            }
            bytesParsed += slackBytes;
            progressReq = (progressReq + (int)slackBytes) % BACKEND_REQUEST_BUFFER_SIZE;
            continue;
        }
        if (reqSize < requestHeaderBytes)
        {
            // jason: padding records are not part of the current ring semantics; treat this as a hard error.
            fprintf(stderr, "%s [error]: request size %u smaller than header %u\n", __func__, reqSize,
                    requestHeaderBytes);
#if DDS_RING_DIAGNOSTICS
            fprintf(stderr,
                    "RingDiag DPU: short header reqSize=%u headerBytes=%u bytesParsed=%u bytesTotal=%u headReq=%d "
                    "tailReq=%d progressReq=%d\n",
                    reqSize, (unsigned)requestHeaderBytes, (unsigned)bytesParsed, (unsigned)bytesTotal, headReq,
                    tailReq, progressReq);
#endif
            // Original padding-record skip behavior retained for reference:
            // bytesParsed += reqSize;
            // progressReq += reqSize;
            // if (progressReq >= BACKEND_REQUEST_BUFFER_SIZE) {
            //     progressReq %= BACKEND_REQUEST_BUFFER_SIZE;
            // }
            // continue;
            break;
        }
        // jason: ring advancement is padded to the request alignment to preserve the phased header offset.
        paddedReqSize = reqSize;
        if (paddedReqSize % requestAlign != 0)
        {
            paddedReqSize += (requestAlign - (paddedReqSize % requestAlign));
        }
        if (paddedReqSize > (bytesTotal - bytesParsed))
        {
            fprintf(stderr, "%s [error]: padded request size %u (logical=%u) exceeds remaining %u\n", __func__,
                    paddedReqSize, reqSize, (bytesTotal - bytesParsed));
            break;
        }

        progressReqForParsing = progressReq;
        bytesParsed += paddedReqSize;
        progressReq += paddedReqSize;
        if (progressReq >= BACKEND_REQUEST_BUFFER_SIZE)
        {
            progressReq %= BACKEND_REQUEST_BUFFER_SIZE;
        }
        progressReqForParsing += sizeof(FileIOSizeT) + sizeof(BuffMsgF2BReqHeader);
        if (progressReqForParsing >= BACKEND_REQUEST_BUFFER_SIZE)
        {
            progressReqForParsing %= BACKEND_REQUEST_BUFFER_SIZE;
        }
        // jason: defensive check that the request header start remains naturally aligned on the ring.
        int reqStart = progressReqForParsing - (int)requestHeaderBytes;
        if (reqStart < 0)
        {
            reqStart += BACKEND_REQUEST_BUFFER_SIZE;
        }
        if (((reqStart + (int)sizeof(FileIOSizeT)) % (int)DDS_ALIGNOF(BuffMsgF2BReqHeader)) != 0)
        {
            fprintf(stderr, "%s [warn]: request header misaligned (reqStart=%d headerAlign=%u reqSize=%u)\n", __func__,
                    reqStart, (unsigned)DDS_ALIGNOF(BuffMsgF2BReqHeader), (unsigned)reqSize);
        }

        curReqSize = reqSize - sizeof(FileIOSizeT);
        curReq += sizeof(FileIOSizeT);

        //
        //
        // jason: Use explicit opcode for request type parsing (read/read2/write).
        //
        BuffMsgF2BReqHeader *baseReqObj = (BuffMsgF2BReqHeader *)curReq;
uint16_t reqOpCode = baseReqObj->OpCode;
FileIOSizeT logicalBytes = baseReqObj->Bytes;
FileIOSizeT outputBytes = baseReqObj->BufferBytes;
if (outputBytes == 0 && logicalBytes > 0)
{
    // jason: Guard legacy clients that don't populate BufferBytes.
    outputBytes = logicalBytes;
}
// jason: read2 carries stage sizes and stage input windows as a payload after the header.
FileIOSizeT stageSizes[2] = {(FileIOSizeT)0, (FileIOSizeT)0};
FileIOSizeT stageInputOffsets[2] = {(FileIOSizeT)0, (FileIOSizeT)0};
FileIOSizeT stageInputLengths[2] = {(FileIOSizeT)0, (FileIOSizeT)0};
uint16_t stageCount = 0;
bool read2StageParseError = false;
FileIOSizeT reqPayloadBytes = curReqSize > sizeof(BuffMsgF2BReqHeader)
                                  ? (FileIOSizeT)(curReqSize - (FileIOSizeT)sizeof(BuffMsgF2BReqHeader))
                                  : (FileIOSizeT)0;
if (reqOpCode == BUFF_MSG_F2B_REQ_OP_READ2)
{
    FileIOSizeT payloadValues[6] = {(FileIOSizeT)0, (FileIOSizeT)0, (FileIOSizeT)0,
                                    (FileIOSizeT)0, (FileIOSizeT)0, (FileIOSizeT)0};
    if (reqPayloadBytes == 0)
    {
        fprintf(stderr, "%s [error]: READ2 payload is empty (reqId=%u)\n", __func__,
                (unsigned)baseReqObj->RequestId);
        read2StageParseError = true;
    }
    else
    {
        if ((reqPayloadBytes % sizeof(FileIOSizeT)) != 0)
        {
            fprintf(stderr,
                    "%s [error]: READ2 payload bytes=%u not value-aligned (reqId=%u)\n", __func__,
                    (unsigned)reqPayloadBytes, (unsigned)baseReqObj->RequestId);
            read2StageParseError = true;
        }
        else
        {
            FileIOSizeT payloadValueCount = reqPayloadBytes / (FileIOSizeT)sizeof(FileIOSizeT);
            if ((payloadValueCount % 3) != 0)
            {
                fprintf(stderr,
                        "%s [error]: READ2 payload value count=%u is not divisible by 3 (reqId=%u)\n", __func__,
                        (unsigned)payloadValueCount, (unsigned)baseReqObj->RequestId);
                read2StageParseError = true;
            }
            else
            {
                stageCount = (uint16_t)(payloadValueCount / 3);
                if (stageCount < 1 || stageCount > 2)
                {
                    fprintf(stderr, "%s [error]: READ2 stageCount=%u is out of range [1,2] (reqId=%u)\n", __func__,
                            (unsigned)stageCount, (unsigned)baseReqObj->RequestId);
                    read2StageParseError = true;
                }
                else
                {
                    ReadRequestPayloadBytes(buffReq, progressReqForParsing, payloadValues, reqPayloadBytes);
                    for (uint16_t stageIndex = 0; stageIndex < stageCount; stageIndex++)
                    {
                        stageSizes[stageIndex] = payloadValues[stageIndex];
                        stageInputOffsets[stageIndex] = payloadValues[stageCount + stageIndex];
                        stageInputLengths[stageIndex] = payloadValues[(2 * stageCount) + stageIndex];
                    }
                    outputBytes = stageSizes[stageCount - 1];
                    // jason: validate stage windows against previous source bytes.
                    FileIOSizeT sourceBytes = logicalBytes;
                    for (uint16_t stageIndex = 0; stageIndex < stageCount; stageIndex++)
                    {
                        if (stageInputOffsets[stageIndex] > sourceBytes ||
                            stageInputLengths[stageIndex] > sourceBytes ||
                            stageInputLengths[stageIndex] > sourceBytes - stageInputOffsets[stageIndex])
                        {
                            fprintf(stderr,
                                    "%s [error]: READ2 stage%u window out of range "
                                    "(source=%u off=%u len=%u reqId=%u)\n",
                                    __func__, (unsigned)stageIndex + 1u, (unsigned)sourceBytes,
                                    (unsigned)stageInputOffsets[stageIndex], (unsigned)stageInputLengths[stageIndex],
                                    (unsigned)baseReqObj->RequestId);
                            read2StageParseError = true;
                            break;
                        }
                        sourceBytes = stageSizes[stageIndex];
                    }
                }
            }
        }
    }
}

if (reqOpCode == BUFF_MSG_F2B_REQ_OP_WRITE || reqOpCode == BUFF_MSG_F2B_REQ_OP_WRITE_GATHER)
{
    //
    // Process a write request
    // Allocate a response first, no need to check alignment
    //
    //
    DebugPrint("%s: get a write request\n", __func__);
    respSize = sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader);

    //
    // Map backend request context strictly by host request id.
    // This enforces one backend slot per outstanding RequestId and avoids context aliasing.
    //
    RequestIdT currIndex = baseReqObj->RequestId;
    bool requestIdValid = true;
    // Task 5.2: Validate against per-channel outstanding limit (array dimension).
    if (!DDS_ValidateRequestId(currIndex))
    {
        fprintf(stderr, "%s [error]: invalid write request id=%u (max=%u)\n", __func__, (unsigned)currIndex,
                (unsigned)DDS_MAX_OUTSTANDING_IO_PER_CHANNEL);
        requestIdValid = false;
    }
    if (requestIdValid)
    {
        ctxt = &Ch->PendingDataPlaneRequests[currIndex];
        if (ctxt->Response && !ResponsePointerWithinRing(buffResp, ctxt->Response))
        {
            fprintf(stderr,
                    "%s [warn]: clearing stale write context response pointer (reqId=%u resp=%p ring=[%p,%p))\n",
                    __func__, (unsigned)currIndex, (void *)ctxt->Response, (void *)buffResp,
                    (void *)(buffResp + BACKEND_RESPONSE_BUFFER_SIZE));
            ctxt->Response = NULL;
        }
        if (ctxt->IsPending)
        {
            RequestIdT prevReqId = DDS_REQUEST_INVALID;
            if (ctxt->Response && ResponsePointerWithinRing(buffResp, ctxt->Response))
            {
                prevReqId = ctxt->Response->RequestId;
            }
            fprintf(stderr,
                    "%s [error]: write context reuse while pending (reqId=%u prevReqId=%u pending=%u resp=%p)\n",
                    __func__, (unsigned)currIndex, (unsigned)prevReqId, (unsigned)ctxt->IsPending,
                    (void *)ctxt->Response);
            requestIdValid = false;
        }
    }
    if (requestIdValid)
    {
        dataBuff = &ctxt->DataBuffer;
    }
    else
    {
        dataBuff = NULL;
    }

    curReqObj = baseReqObj;
#if DDS_RING_DIAGNOSTICS
    if (curReqObj->Bytes == 0)
    {
        fprintf(stderr,
                "RingDiag DPU: zero-bytes reqId=%u fileId=%u offset=%llu reqSize=%u bytesParsed=%u "
                "bytesTotal=%u headReq=%d tailReq=%d progressReq=%d progressReqForParsing=%d\n",
                (unsigned)curReqObj->RequestId, (unsigned)curReqObj->FileId, (unsigned long long)curReqObj->Offset,
                (unsigned)reqSize, (unsigned)bytesParsed, (unsigned)bytesTotal, headReq, tailReq, progressReq,
                progressReqForParsing);
    }
#endif
    if (requestIdValid)
    {
        dataBuff->TotalSize = curReqObj->Bytes;
        dataBuff->FirstAddr = buffReq + progressReqForParsing;
        if (progressReqForParsing + dataBuff->TotalSize >= BACKEND_REQUEST_BUFFER_SIZE)
        {
            dataBuff->FirstSize = BACKEND_REQUEST_BUFFER_SIZE - progressReqForParsing;
            dataBuff->SecondAddr = buffReq;
        }
        else
        {
            dataBuff->FirstSize = curReqObj->Bytes;
            dataBuff->SecondAddr = NULL;
        }
    }

    //
    // Record the size of the this response on the response ring
    //
    //
    if (progressResp + (int)respSize > BACKEND_RESPONSE_BUFFER_SIZE)
    {
        // jason: implicit wrap rule (no size+header at tail -> skip tail slack and wrap).
        /* fprintf(stderr, "%s [error]: response header would wrap (write)\n", __func__);
        exit(-1); */
        FileIOSizeT slackBytes = (FileIOSizeT)(BACKEND_RESPONSE_BUFFER_SIZE - progressResp);
        MarkResponseWrapSlack(buffResp, progressResp);
        totalRespSize += slackBytes;
        progressResp = 0;
    }
    *(FileIOSizeT *)(buffResp + progressResp) = respSize;

    BuffMsgB2FAckHeader *resp = (BuffMsgB2FAckHeader *)(buffResp + progressResp + sizeof(FileIOSizeT));
    resp->RequestId = curReqObj->RequestId;
    // Original ordering retained for reference:
    // resp->Result = DDS_ERROR_CODE_IO_PENDING;
    //
    // Updated ordering: initialize response fields first, then publish IO_PENDING only after
    // request-id/context-reuse validation. Publishing too early can cause false-positive
    // "reuse while pending" when old/new response ring slots alias by address.
    resp->BytesServiced = 0;
    resp->LogicalBytes = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    resp->OffloadReadTimeNs = 0;
    resp->OffloadStage1TimeNs = 0;
    resp->OffloadStage2TimeNs = 0;
#endif

    progressResp += respSize;
    totalRespSize += respSize;
    if (progressResp >= BACKEND_RESPONSE_BUFFER_SIZE)
    {
        progressResp %= BACKEND_RESPONSE_BUFFER_SIZE;
    }
    if (requestIdValid)
    {
        // Publish pending only after request-id validity and context-reuse checks pass.
        DDS_ATOMIC_ERRORCODE_STORE(&resp->Result, DDS_ERROR_CODE_IO_PENDING, DDS_ATOMIC_ORDER_RELAXED);
    }

#ifdef OPT_FILE_SERVICE_BATCHING
    //
    // Don't submit this request yet, will be batched
    //
    //
    if (!requestIdValid)
    {
        resp->BytesServiced = 0;
        resp->LogicalBytes = 0;
        DDS_ATOMIC_ERRORCODE_STORE(&resp->Result, DDS_ERROR_CODE_INVALID_PARAM, DDS_ATOMIC_ORDER_RELEASE);
    }
    else
    {
        ctxt->Request = curReqObj;
        ctxt->Response = resp;
        ctxt->IsPending = 1;
        ctxt->IsRead = false;
        ctxt->OpCode = reqOpCode;
        ctxt->ChannelIndex = (int16_t)Ch->ChannelIndex;
        ctxt->LogicalBytes = logicalBytes;
        ctxt->OutputBytes = outputBytes;
        // jason: clear read2 stage metadata for writes.
        ctxt->StageCount = 0;
        ctxt->StageSizes[0] = 0;
        ctxt->StageSizes[1] = 0;
        ctxt->StageInputOffsets[0] = 0;
        ctxt->StageInputOffsets[1] = 0;
        ctxt->StageInputLengths[0] = 0;
        ctxt->StageInputLengths[1] = 0;
        memset(&ctxt->StageBuffers[0], 0, sizeof(ctxt->StageBuffers));
        ctxt->CopyStart = 0;
        ctxt->ResponseRingBase = buffResp;
        ctxt->ResponseRingBytes = BACKEND_RESPONSE_BUFFER_SIZE;
        ctxt->BatchNextRequestId = DDS_REQUEST_INVALID;

        if (firstIndex == DDS_REQUEST_INVALID)
        {
            firstIndex = currIndex;
        }
        else
        {
            Ch->PendingDataPlaneRequests[prevIndex].BatchNextRequestId = currIndex;
        }
        prevIndex = currIndex;
        batchSize++;
    }
#else
        //
        // Submit this request
        //
        //
        if (!requestIdValid)
        {
            resp->BytesServiced = 0;
            resp->LogicalBytes = 0;
            DDS_ATOMIC_ERRORCODE_STORE(&resp->Result, DDS_ERROR_CODE_INVALID_PARAM, DDS_ATOMIC_ORDER_RELEASE);
        }
        else
        {
            ctxt->Request = curReqObj;
            ctxt->Response = resp;
            ctxt->IsPending = 1;
            ctxt->IsRead = false;
            ctxt->OpCode = reqOpCode;
            ctxt->ChannelIndex = (int16_t)Ch->ChannelIndex;
            ctxt->LogicalBytes = logicalBytes;
            ctxt->OutputBytes = outputBytes;
            ctxt->StageCount = 0;
            ctxt->StageSizes[0] = 0;
            ctxt->StageSizes[1] = 0;
            ctxt->StageInputOffsets[0] = 0;
            ctxt->StageInputOffsets[1] = 0;
            ctxt->StageInputLengths[0] = 0;
            ctxt->StageInputLengths[1] = 0;
            memset(&ctxt->StageBuffers[0], 0, sizeof(ctxt->StageBuffers));
            ctxt->CopyStart = 0;
            ctxt->ResponseRingBase = buffResp;
            ctxt->ResponseRingBytes = BACKEND_RESPONSE_BUFFER_SIZE;
            ctxt->BatchNextRequestId = DDS_REQUEST_INVALID;
            SubmitDataPlaneRequest(FS, ctxt, false, currIndex);
        }
#endif
}
else if (reqOpCode == BUFF_MSG_F2B_REQ_OP_READ || reqOpCode == BUFF_MSG_F2B_REQ_OP_READ2)
{
    //
    // Process a read request
    // Allocate a response first, need to check alignment
    //
    //
    DebugPrint("%s: get a read request\n", __func__);
    // RingSizeT alignment = sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader);
    // jason: Keep next response header aligned to header size while aligning payload to bdev buf align.
    FileIOSizeT headerBytes = (FileIOSizeT)(sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));
    RingSizeT alignment = headerBytes;
    FileSizeT blockSize = (FileSizeT)FS->MasterSPDKContext->block_size;
    FileIOSizeT payloadAlign = (FileIOSizeT)FS->MasterSPDKContext->buf_align;
    if (blockSize == 0)
    {
        blockSize = 1;
    }
    if (payloadAlign == 0)
    {
        payloadAlign = 1;
    }
    curReqObj = baseReqObj;
    // jason: Align the payload size and offset for backend I/O using logical bytes.
    FileSizeT alignedOffset = AlignDownFileSize(curReqObj->Offset, blockSize);
    FileSizeT alignedEnd = AlignUpFileSize(curReqObj->Offset + (FileSizeT)logicalBytes, blockSize);
    FileIOSizeT alignedBytes = (FileIOSizeT)(alignedEnd - alignedOffset);
    FileIOSizeT copyStart = (FileIOSizeT)(curReqObj->Offset - alignedOffset);
    // jason: include read2 stage buffers after the aligned stage0 read buffer.
    FileIOSizeT stagePayloadBytes = 0;
    if (reqOpCode == BUFF_MSG_F2B_REQ_OP_READ2 && stageCount > 0 && stageCount <= 2 && !read2StageParseError)
    {
        for (uint16_t stageIndex = 0; stageIndex < stageCount; stageIndex++)
        {
            stagePayloadBytes += stageSizes[stageIndex];
        }
    }
    FileIOSizeT totalPayloadBytes = alignedBytes + stagePayloadBytes;
    FileIOSizeT payloadPad = 0;
    int payloadStart = 0;
    while (true)
    {
        payloadStart = AlignResponsePayloadStart(buffResp, progressResp + (int)headerBytes, payloadAlign, &payloadPad);
        // Original: respSize = headerBytes + payloadPad + alignedBytes;
        respSize = headerBytes + payloadPad + totalPayloadBytes;
        // this alignment is to make sure the next header is aligned
        if (respSize % alignment != 0)
        {
            respSize += (alignment - (respSize % alignment));
        }
        if (respSize > BACKEND_RESPONSE_BUFFER_SIZE)
        {
            fprintf(stderr, "%s [error]: response record too large (%u bytes)\n", __func__, (unsigned)respSize);
            break;
        }
        if (progressResp + (int)respSize <= BACKEND_RESPONSE_BUFFER_SIZE)
        {
            break;
        }

        // jason: no-split mode; skip end-of-ring slack and retry layout at offset 0.
        FileIOSizeT slackBytes = (FileIOSizeT)(BACKEND_RESPONSE_BUFFER_SIZE - progressResp);
        MarkResponseWrapSlack(buffResp, progressResp);
        totalRespSize += slackBytes;
        progressResp = 0;
    }
    if (respSize > BACKEND_RESPONSE_BUFFER_SIZE)
    {
        break;
    }

    //
    // Record the size of the this response on the response ring
    //
    //
    *(FileIOSizeT *)(buffResp + progressResp) = respSize;

    BuffMsgB2FAckHeader *resp = (BuffMsgB2FAckHeader *)(buffResp + progressResp + sizeof(FileIOSizeT));
    resp->RequestId = curReqObj->RequestId;
    // Original eager publish retained for reference:
    // DDS_ATOMIC_ERRORCODE_STORE(&resp->Result, DDS_ERROR_CODE_IO_PENDING, DDS_ATOMIC_ORDER_RELAXED);
    //
    // Updated ordering mirrors write path: publish IO_PENDING only after request-id/context checks.
    resp->BytesServiced = 0;
    resp->LogicalBytes = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    resp->OffloadReadTimeNs = 0;
    resp->OffloadStage1TimeNs = 0;
    resp->OffloadStage2TimeNs = 0;
#endif

    //
    // Extract read destination buffer from the response ring
    //
    //
    RequestIdT currIndex = baseReqObj->RequestId;
    bool requestIdValid = true;
    // Task 5.2: Validate against per-channel outstanding limit (array dimension).
    if (!DDS_ValidateRequestId(currIndex))
    {
        fprintf(stderr, "%s [error]: invalid read request id=%u (max=%u)\n", __func__, (unsigned)currIndex,
                (unsigned)DDS_MAX_OUTSTANDING_IO_PER_CHANNEL);
        requestIdValid = false;
    }
    if (requestIdValid)
    {
        ctxt = &Ch->PendingDataPlaneRequests[currIndex];
        if (ctxt->Response && !ResponsePointerWithinRing(buffResp, ctxt->Response))
        {
            fprintf(stderr,
                    "%s [warn]: clearing stale read context response pointer (reqId=%u resp=%p ring=[%p,%p))\n",
                    __func__, (unsigned)currIndex, (void *)ctxt->Response, (void *)buffResp,
                    (void *)(buffResp + BACKEND_RESPONSE_BUFFER_SIZE));
            ctxt->Response = NULL;
        }
        if (ctxt->IsPending)
        {
            RequestIdT prevReqId = DDS_REQUEST_INVALID;
            if (ctxt->Response && ResponsePointerWithinRing(buffResp, ctxt->Response))
            {
                prevReqId = ctxt->Response->RequestId;
            }
            fprintf(stderr,
                    "%s [error]: read context reuse while pending (reqId=%u prevReqId=%u pending=%u resp=%p)\n",
                    __func__, (unsigned)currIndex, (unsigned)prevReqId, (unsigned)ctxt->IsPending,
                    (void *)ctxt->Response);
            requestIdValid = false;
        }
    }
    if (requestIdValid)
    {
        // Publish pending only after request-id validity and context-reuse checks pass.
        DDS_ATOMIC_ERRORCODE_STORE(&resp->Result, DDS_ERROR_CODE_IO_PENDING, DDS_ATOMIC_ORDER_RELAXED);
        dataBuff = &ctxt->DataBuffer;

        // jason: Use aligned payload size and aligned payload start.
        dataBuff->TotalSize = alignedBytes;
        if (payloadStart + (int)alignedBytes <= BACKEND_RESPONSE_BUFFER_SIZE)
        {
            dataBuff->FirstAddr = buffResp + payloadStart;
            dataBuff->FirstSize = dataBuff->TotalSize;
            dataBuff->SecondAddr = NULL;
        }
        else
        {
            dataBuff->FirstAddr = buffResp + payloadStart;
            dataBuff->FirstSize = BACKEND_RESPONSE_BUFFER_SIZE - payloadStart;
            dataBuff->SecondAddr = buffResp;
        }
        // jason: override stage0 slice using a ring-aware slicer (handles wrap consistently).
        BuildResponseSlice(buffResp, payloadStart, 0, alignedBytes, dataBuff);
        assert(dataBuff->SecondAddr == NULL);
        // jason: persist the aligned prefix so ReadFile can validate/copy correctly for unaligned offsets.
        ctxt->CopyStart = copyStart;
        ctxt->ResponseRingBase = buffResp;
        ctxt->ResponseRingBytes = BACKEND_RESPONSE_BUFFER_SIZE;
        if (reqOpCode == BUFF_MSG_F2B_REQ_OP_READ2 && stageCount > 0 && stageCount <= 2 && !read2StageParseError)
        {
            ctxt->StageCount = stageCount;
            ctxt->StageSizes[0] = 0;
            ctxt->StageSizes[1] = 0;
            ctxt->StageInputOffsets[0] = 0;
            ctxt->StageInputOffsets[1] = 0;
            ctxt->StageInputLengths[0] = 0;
            ctxt->StageInputLengths[1] = 0;
            memset(&ctxt->StageBuffers[0], 0, sizeof(ctxt->StageBuffers));
            FileIOSizeT stagePayloadOffset = alignedBytes;
            for (uint16_t stageIndex = 0; stageIndex < stageCount; stageIndex++)
            {
                ctxt->StageSizes[stageIndex] = stageSizes[stageIndex];
                ctxt->StageInputOffsets[stageIndex] = stageInputOffsets[stageIndex];
                ctxt->StageInputLengths[stageIndex] = stageInputLengths[stageIndex];
                BuildResponseSlice(buffResp, payloadStart, stagePayloadOffset, stageSizes[stageIndex],
                                   &ctxt->StageBuffers[stageIndex]);
                assert(ctxt->StageBuffers[stageIndex].SecondAddr == NULL);
                stagePayloadOffset += stageSizes[stageIndex];
            }
        }
        else
        {
            // jason: clear stage metadata when not using read2.
            ctxt->StageCount = 0;
            ctxt->StageSizes[0] = 0;
            ctxt->StageSizes[1] = 0;
            ctxt->StageInputOffsets[0] = 0;
            ctxt->StageInputOffsets[1] = 0;
            ctxt->StageInputLengths[0] = 0;
            ctxt->StageInputLengths[1] = 0;
            memset(&ctxt->StageBuffers[0], 0, sizeof(ctxt->StageBuffers));
        }
    }
    else
    {
        dataBuff = NULL;
    }
    if (!requestIdValid)
    {
        resp->BytesServiced = 0;
        resp->LogicalBytes = 0;
        DDS_ATOMIC_ERRORCODE_STORE(&resp->Result, DDS_ERROR_CODE_INVALID_PARAM, DDS_ATOMIC_ORDER_RELEASE);
    }
    else if (reqOpCode == BUFF_MSG_F2B_REQ_OP_READ2 && read2StageParseError)
    {
        // jason: mark malformed read2 requests as failures and let the handler skip them.
        resp->BytesServiced = 0;
        resp->LogicalBytes = 0;
        fprintf(stderr,
                "%s [warn]: rejecting READ2 as INVALID_PARAM (reqId=%u logical=%u output=%u stageCount=%u stage1=%u "
                "stage2=%u off1=%u len1=%u off2=%u len2=%u payloadBytes=%u)\n",
                __func__, (unsigned)baseReqObj->RequestId, (unsigned)logicalBytes, (unsigned)outputBytes,
                (unsigned)stageCount, (unsigned)stageSizes[0], (unsigned)stageSizes[1],
                (unsigned)stageInputOffsets[0], (unsigned)stageInputLengths[0], (unsigned)stageInputOffsets[1],
                (unsigned)stageInputLengths[1], (unsigned)reqPayloadBytes);
        DDS_ATOMIC_ERRORCODE_STORE(&resp->Result, DDS_ERROR_CODE_INVALID_PARAM, DDS_ATOMIC_ORDER_RELEASE);
    }

    progressResp += respSize;
    totalRespSize += respSize;
    if (progressResp >= BACKEND_RESPONSE_BUFFER_SIZE)
    {
        progressResp %= BACKEND_RESPONSE_BUFFER_SIZE;
    }

#ifdef OPT_FILE_SERVICE_BATCHING
    //
    // Don't submit just yet
    //
    //
    if (requestIdValid && !(reqOpCode == BUFF_MSG_F2B_REQ_OP_READ2 && read2StageParseError))
    {
        ctxt->Request = curReqObj;
        ctxt->Response = resp;
        ctxt->IsPending = 1;
        ctxt->IsRead = true;
        ctxt->OpCode = reqOpCode;
        ctxt->ChannelIndex = (int16_t)Ch->ChannelIndex;
        ctxt->LogicalBytes = logicalBytes;
        ctxt->OutputBytes = outputBytes;
        ctxt->BatchNextRequestId = DDS_REQUEST_INVALID;

        if (firstIndex == DDS_REQUEST_INVALID)
        {
            firstIndex = currIndex;
        }
        else
        {
            Ch->PendingDataPlaneRequests[prevIndex].BatchNextRequestId = currIndex;
        }
        prevIndex = currIndex;
        batchSize++;
    }
#else
        //
        // Submit this request
        //
        //
        if (requestIdValid && !(reqOpCode == BUFF_MSG_F2B_REQ_OP_READ2 && read2StageParseError))
        {
            ctxt->Request = curReqObj;
            ctxt->Response = resp;
            ctxt->IsPending = 1;
            ctxt->IsRead = true;
            ctxt->OpCode = reqOpCode;
            ctxt->ChannelIndex = (int16_t)Ch->ChannelIndex;
            ctxt->LogicalBytes = logicalBytes;
            ctxt->OutputBytes = outputBytes;
            ctxt->BatchNextRequestId = DDS_REQUEST_INVALID;
            SubmitDataPlaneRequest(FS, ctxt, true, currIndex);
        }
#endif
}
else
{
    fprintf(stderr, "%s [error]: unrecognized request opcode %u\n", __func__, (unsigned)reqOpCode);
    break;
}
}

#ifdef OPT_FILE_SERVICE_BATCHING
//
// Now submit in a batch, this is the host, IoSlotBase should be 0; for the DPU it would be DDS_DPU_IO_SLOT_NUMBER_BASE
//
//
if (batchSize > 0 && firstIndex != DDS_REQUEST_INVALID)
{
    SubmitDataPlaneRequest(FS, Ch->PendingDataPlaneRequests, firstIndex, batchSize, 0);
}
#endif

    //
    // Update response buffer tail
    //
    //
if (totalRespSize >= respRingCapacity)
{
    //
    // If this happens, increase response buffer size
    // TODO: a mechanism that holds the execution of requests
    //       until responses are drained and enough space is available
    //
    //
    fprintf(stderr, "%s [error]: Response buffer is corrupted!\n", __func__);
    exit(-1);
}
    // Per-channel debug counter: total requests dispatched during this ExecuteRequests call.
    Ch->RequestsDispatched += batchSize;

    DebugPrint("All requests have been executed. Response size = %d\n", totalRespSize);
    DebugPrint("%s: AggressiveTail %d -> %d\n", __func__, Ch->ResponseRing.TailA, progressResp);
    Ch->ResponseRing.TailA = progressResp;

#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
    // jason: publish batch meta last so consumers see a fully-formed batch.
    // Diagnostic: stamp a recognizable pattern in the unused meta header space.
    *((FileIOSizeT *)batchMeta) = totalRespSize;
#if DDS_RING_BATCH_META_DIAGNOSTICS
    {
        int metaOff = (int)(batchMeta - buffResp);
        // The meta reservation is (sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader)).
        // Store a signature in the bytes after the meta size field so we can detect overwrites.
        const uint32_t sig0 = 0xDD5B4D31u; // "DDSBM1"
        const uint32_t sig1 = (uint32_t)metaOff;
        const uint32_t sig2 = (uint32_t)totalRespSize;
        memcpy(batchMeta + sizeof(FileIOSizeT) + 0, &sig0, sizeof(sig0));
        memcpy(batchMeta + sizeof(FileIOSizeT) + 4, &sig1, sizeof(sig1));
        memcpy(batchMeta + sizeof(FileIOSizeT) + 8, &sig2, sizeof(sig2));
        // Print only when debugging is enabled; avoids hot-path noise.
        DebugPrint("BatchMetaPublish off=%d total=%u tailA=%d tailB=%d tailC=%d\n", metaOff, (unsigned)totalRespSize,
                   Ch->ResponseRing.TailA, Ch->ResponseRing.TailB, Ch->ResponseRing.TailC);
    }
#endif
#endif
}

//
// Compute the distance between two pointers on a ring buffer
//
//
static inline int DistanceBetweenPointers(int Tail, int Head, size_t Capacity)
{
    if (Tail >= Head)
    {
        return Tail - Head;
    }
    else
    {
        return Capacity - Head + Tail;
    }
}

//
// DpuDmaVisibilityFence
//
// Ensure CPU writes to the response buffer and response meta are visible
// to the RNIC before we post RDMA writes. This is a CPU->device ordering
// barrier; if the platform is non-coherent, this may need a stronger
// cache-management primitive.
//
static inline void DpuDmaVisibilityFence(void) { __sync_synchronize(); }

//
// Publish a deferred request head after the request data has been consumed.
// Safe variant: host space is released only after ExecuteRequests finishes.
//
//
// Publish a deferred request head after the request data has been consumed (per-channel).
// Safe variant: host space is released only after ExecuteRequests finishes.
// Refactored: operates on a single BuffConnChannelState.
//
static inline int PublishDeferredRequestHead(BuffConnChannelState *Ch, struct ibv_send_wr **BadSendWr)
{
    if (!Ch->RequestPendingHeadValid)
    {
        return 0;
    }

    Ch->RequestRing.Head = Ch->RequestPendingHead;
    Ch->RequestPendingHeadValid = FALSE;

    int ret = ibv_post_send(Ch->QPair, &Ch->RequestDMAWriteMetaWr, BadSendWr);
    if (ret)
    {
        fprintf(stderr, "%s [error][%s]: ibv_post_send failed: %d (%s)\n", __func__,
                DDS_ChannelName(Ch->ChannelIndex), ret, strerror(ret));
        return -1;
    }

    // Per-channel debug counter: one deferred head publication.
    Ch->DeferredHeadPublishes++;

    return 0;
}

//
// Process communication channel events for buffer connections.
// jason: defer request head update until data is consumed for safety.
// jason: publish response tail with write-with-immediate for CQE gating.
//
// Process RDMA completions from all buffer connections (one per IO channel).
//
// Dual-channel invariants (Tasks 4.1-4.3):
//   - WR-ID channel encoding is NOT needed: each BuffConnConfig has its own QP/CQ,
//     so completions from a BuffConn's CQ inherently belong to that channel. The
//     WR-ID constants (BUFF_READ_REQUEST_META_WR_ID, etc.) identify the operation
//     class; channel identity comes from the BuffConn iteration context.
//   - Completion processing is channel-local: `ch = &buffConn->ChannelState` scopes all
//     state mutations to the owning channel. No cross-channel pointer or index
//     references exist in any switch case.
//   - Deferred request head is channel-local: PublishDeferredRequestHead takes
//     BuffConnChannelState*, each BuffConn's ChannelState.RequestPendingHead/Valid is
//     independent.
//
static inline int ProcessBuffCqEvents(BackEndConfig *Config)
{
    int ret = 0;
    BuffConnConfig *buffConn = NULL;
    struct ibv_send_wr *badSendWr = NULL;
    struct ibv_wc wc;

    for (int i = 0; i != Config->MaxBuffs; i++)
    {
        buffConn = &Config->BuffConns[i];
        if (buffConn->State != CONN_STATE_CONNECTED)
        {
            continue;
        }

        // Each BuffConnConfig represents a single IO channel. The outer loop
        // over all BuffConns naturally polls both channels' CQs independently.
        BuffConnChannelState *ch = &buffConn->ChannelState;
        if (!DDS_ValidateChannelIndex(ch->ChannelIndex))
        {
            fprintf(stderr, "%s [error]: invalid channel index for BuffConn=%d (channel=%d)\n", __func__, i,
                    ch->ChannelIndex);
            ret = -1;
            continue;
        }

        if ((ret = ibv_poll_cq(ch->CompQ, 1, &wc)) == 1)
        {
            ret = 0;
            if (wc.status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "%s [error]: ibv_poll_cq failed status %d (%s) [buffConn=%d ch=%s]\n",
                        __func__, wc.status, ibv_wc_status_str(wc.status),
                        i, DDS_ChannelName(ch->ChannelIndex));
                ret = -1;
                continue;
            }

            switch (wc.opcode)
            {
            case IBV_WC_RECV:
            {
                ret = BuffMsgHandler(buffConn);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: BuffMsgHandler failed [buffConn=%d ch=%s]\n",
                            __func__, i, DDS_ChannelName(ch->ChannelIndex));
                    goto ProcessBuffCqEventsReturn;
                }
            }
            break;
            case IBV_WC_RDMA_READ:
            {
                switch (wc.wr_id)
                {
                case BUFF_READ_REQUEST_META_WR_ID:
                {
                    //
                    // Process a meta read
                    //
                    //
                    int *pointers = (int *)ch->RequestDMAReadMetaBuff;
                    int progress = pointers[0];
                    int tail = pointers[DDS_CACHE_LINE_SIZE_BY_INT];
                    if (tail == ch->RequestRing.Head || tail != progress)
                    {
                        //
                        // Not ready to read, poll again
                        //
                        //
                        ret = ibv_post_send(ch->QPair, &ch->RequestDMAReadMetaWr, &badSendWr);
                        if (ret)
                        {
                            fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                            ret = -1;
                        }
                    }
                    else
                    {
                        //
                        // Ready to read
                        //
                        //
                        RingSizeT availBytes = 0;
                        uint64_t sourceBuffer1 = 0;
                        uint64_t sourceBuffer2 = 0;
                        uint64_t destBuffer1 = 0;
                        uint64_t destBuffer2 = 0;
                        int head = ch->RequestRing.Head;

                        if (progress > head)
                        {
                            availBytes = progress - head;
                            ch->RequestDMAReadDataSize = availBytes;
                            sourceBuffer1 = ch->RequestRing.DataBaseAddr + head;
                            destBuffer1 = (uint64_t)(ch->RequestDMAReadDataBuff + head);
                        }
                        else
                        {
                            availBytes = DDS_REQUEST_RING_BYTES - head;
                            ch->RequestDMAReadDataSize = availBytes + progress;
                            sourceBuffer1 = ch->RequestRing.DataBaseAddr + head;
                            sourceBuffer2 = ch->RequestRing.DataBaseAddr;
                            destBuffer1 = (uint64_t)(ch->RequestDMAReadDataBuff + head);
                            destBuffer2 = (uint64_t)(ch->RequestDMAReadDataBuff);
                        }

                        //
                        // Post a DMA read, by making DPU buffer a mirror of the host buffer
                        //
                        //
                        ch->RequestDMAReadDataWr.wr.rdma.remote_addr = sourceBuffer1;
                        ch->RequestDMAReadDataWr.sg_list->addr = destBuffer1;
                        ch->RequestDMAReadDataWr.sg_list->length = availBytes;

                        if (sourceBuffer2)
                        {
                            ch->RequestDMAReadDataSplitState = BUFF_READ_DATA_SPLIT_STATE_SPLIT;

                            ch->RequestDMAReadDataSplitWr.sg_list->addr = destBuffer2;
                            ch->RequestDMAReadDataSplitWr.sg_list->length = progress;
                            ch->RequestDMAReadDataSplitWr.wr.rdma.remote_addr = sourceBuffer2;
                            ret = ibv_post_send(ch->QPair, &ch->RequestDMAReadDataSplitWr, &badSendWr);
                            if (ret)
                            {
                                fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                                ret = -1;
                            }

                            ret = ibv_post_send(ch->QPair, &ch->RequestDMAReadDataWr, &badSendWr);
                            if (ret)
                            {
                                fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                                ret = -1;
                            }
                        }
                        else
                        {
                            ch->RequestDMAReadDataSplitState = BUFF_READ_DATA_SPLIT_STATE_NOT_SPLIT;

                            ret = ibv_post_send(ch->QPair, &ch->RequestDMAReadDataWr, &badSendWr);
                            if (ret)
                            {
                                fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                                ret = -1;
                            }
                        }

                        // ch->RequestRing.Head = progress;
                        // jason: defer host head update until data is consumed (ExecuteRequests).
                        ch->RequestPendingHead = progress;
                        ch->RequestPendingHeadValid = TRUE;
                        /*
                        //
                        // Immediately update remote head, assuming DMA requests are exected in order
                        //
                        //
                        ret = ibv_post_send(ch->QPair, &ch->RequestDMAWriteMetaWr, &badSendWr);
                        if (ret) {
                            fprintf(stderr, "%s [error]: ibv_post_send failed: %d (%s)\n", __func__, ret,
                        strerror(ret)); ret = -1;
                        }
                        */
                    }
                }
                break;
                case BUFF_READ_REQUEST_DATA_WR_ID:
                {
                    //
                    // Check splitting and update the head
                    //
                    //
                    if (ch->RequestDMAReadDataSplitState == BUFF_READ_DATA_SPLIT_STATE_NOT_SPLIT)
                    {
                        //
                        // Execute all the requests
                        //
                        //
                        ExecuteRequests(ch, Config->FS);
                        // Original behavior released request head immediately after request consumption.
                        // This allows host to reuse request ring indices before async read2 stage completion.
                        // if (PublishDeferredRequestHead(ch, &badSendWr))
                        // {
                        //     ret = -1;
                        // }
                        //
                        // Updated behavior: defer request-head publication until response completion path.
                        // This avoids reuse of PerSlotContext/request index while stage tasks are still in-flight.
                    }
                    else
                    {
                        ch->RequestDMAReadDataSplitState++;
                    }
                }
                break;
                case BUFF_READ_REQUEST_DATA_SPLIT_WR_ID:
                {
                    //
                    // Check splitting and update the head
                    //
                    //
                    if (ch->RequestDMAReadDataSplitState == BUFF_READ_DATA_SPLIT_STATE_NOT_SPLIT)
                    {
                        //
                        // Execute all the requests
                        //
                        //
                        ExecuteRequests(ch, Config->FS);
                        // Original behavior released request head immediately after request consumption.
                        // This allows host to reuse request ring indices before async read2 stage completion.
                        // if (PublishDeferredRequestHead(ch, &badSendWr))
                        // {
                        //     ret = -1;
                        // }
                        //
                        // Updated behavior: defer request-head publication until response completion path.
                        // This avoids reuse of PerSlotContext/request index while stage tasks are still in-flight.
                    }
                    else
                    {
                        ch->RequestDMAReadDataSplitState++;
                    }
                }
                break;
                case BUFF_READ_RESPONSE_META_WR_ID:
                {
                    //
                    // Process a response meta read
                    //
                    //
                    int *pointers = (int *)ch->ResponseDMAReadMetaBuff;
                    int progress = pointers[0];
                    int head = pointers[DDS_CACHE_LINE_SIZE_BY_INT];
                    int tailStart = ch->ResponseRing.TailC;
                    int tailEnd = ch->ResponseRing.TailB;

                    DebugPrint("head = %d, progress = %d, tail = %d\n", head, progress, tailStart);

                    if (tailStart == tailEnd)
                    {
                        //
                        // No response to send
                        //
                        //
                        break;
                    }

                    const FileIOSizeT totalResponseBytes =
                        DistanceBetweenPointers(tailEnd, tailStart, BACKEND_RESPONSE_BUFFER_SIZE);

#if DDS_RING_BATCH_META_DIAGNOSTICS
                    // jason: batched path expects TailC to always point at a batch meta word.
                    // If it is zero, TailC is likely desynchronized or the meta word was overwritten.
                    {
                        FileIOSizeT meta = *(FileIOSizeT *)(ch->ResponseDMAWriteDataBuff + tailStart);
                        if (meta == 0)
                        {
                            fprintf(stderr,
                                    "%s [error]: TailC batch meta is zero (tailC=%d tailB=%d tailA=%d totalBytes=%u)\n",
                                    __func__, tailStart, tailEnd, ch->ResponseRing.TailA, (unsigned)totalResponseBytes);
                            DumpResponseRingU32Window(ch->ResponseDMAWriteDataBuff, tailStart, "TailC(meta==0)");
                        }
                        else if (meta != totalResponseBytes)
                        {
                            fprintf(stderr,
                                    "%s [warn]: TailC batch meta mismatch (meta=%u dist=%u tailC=%d tailB=%d tailA=%d)\n",
                                    __func__, (unsigned)meta, (unsigned)totalResponseBytes, tailStart, tailEnd,
                                    ch->ResponseRing.TailA);
                            DumpResponseRingU32Window(ch->ResponseDMAWriteDataBuff, tailStart, "TailC(meta!=dist)");
                        }
                    }
#endif

                    RingSizeT distance = 0;
                    if (head != progress)
                    {
                        //
                        // Not ready to write, poll again
                        //
                        //
                        DebugPrint("progress %d != head %d, keep polling\n", progress, head);
                        ret = ibv_post_send(ch->QPair, &ch->ResponseDMAReadMetaWr, &badSendWr);
                        if (ret)
                        {
                            fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                            ret = -1;
                        }
                        break;
                    }

                    if (tailStart >= head)
                    {
                        distance = head + DDS_RESPONSE_RING_BYTES - tailStart;
                    }
                    else
                    {
                        distance = head - tailStart;
                    }

                    if (distance < totalResponseBytes)
                    {
                        //
                        // Not ready to write, poll again
                        //
                        //
                        ret = ibv_post_send(ch->QPair, &ch->ResponseDMAReadMetaWr, &badSendWr);
                        if (ret)
                        {
                            fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                            ret = -1;
                        }
                        break;
                    }
                    else
                    {
                        //
                        // Ready to write
                        //
                        //
                        RingSizeT availBytes = 0;
                        uint64_t sourceBuffer1 = 0;
                        uint64_t sourceBuffer2 = 0;
                        uint64_t destBuffer1 = 0;
                        uint64_t destBuffer2 = 0;
                        uint8_t pendingDataWrs = 1;
                        int postedTail = 0;

                        DebugPrint("Total response bytes = %d\n", totalResponseBytes);

                        if (tailStart + totalResponseBytes <= DDS_RESPONSE_RING_BYTES)
                        {
                            //
                            // No split
                            //
                            //
                            availBytes = totalResponseBytes;
                            sourceBuffer1 = (uint64_t)(ch->ResponseDMAWriteDataBuff + tailStart);
                            destBuffer1 = (uint64_t)(ch->ResponseRing.DataBaseAddr + tailStart);
                        }
                        else
                        {
                            //
                            // Split
                            //
                            //
                            availBytes = DDS_RESPONSE_RING_BYTES - tailStart;
                            sourceBuffer1 = (uint64_t)(ch->ResponseDMAWriteDataBuff + tailStart);
                            sourceBuffer2 = (uint64_t)(ch->ResponseDMAWriteDataBuff);
                            destBuffer1 = (uint64_t)(ch->ResponseRing.DataBaseAddr + tailStart);
                            destBuffer2 = (uint64_t)(ch->ResponseRing.DataBaseAddr);
                            pendingDataWrs = 2;
                        }

                        //
                        // Post DMA writes
                        //
                        //
                        ch->ResponseDMAWriteDataWr.wr.rdma.remote_addr = destBuffer1;
                        ch->ResponseDMAWriteDataWr.sg_list->addr = sourceBuffer1;
                        ch->ResponseDMAWriteDataWr.sg_list->length = availBytes;

                        if (sourceBuffer2)
                        {
                            // Legacy split-state scalar retained for reference:
                            // ch->ResponseDMAWriteDataSplitState = BUFF_READ_DATA_SPLIT_STATE_SPLIT;
                            ch->ResponseDMAWriteDataSplitWr.sg_list->addr = sourceBuffer2;
                            ch->ResponseDMAWriteDataSplitWr.sg_list->length = totalResponseBytes - availBytes;
                            ch->ResponseDMAWriteDataSplitWr.wr.rdma.remote_addr = destBuffer2;

                            // Original split posting retained for reference:
                            // DpuDmaVisibilityFence();
                            // ret = ibv_post_send(ch->QPair, &ch->ResponseDMAWriteDataSplitWr, &badSendWr);
                            // if (ret)
                            // {
                            //     fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                            //     ret = -1;
                            //     break;
                            // }
                            //
                            // Updated: post the first logical segment (tailStart..end) before the wrapped
                            // segment (0..rest). The host parser always starts at tailStart, so publishing the
                            // wrapped second half first can expose a transient invalid header at the start of a
                            // cached batch even though the batch later becomes valid without being discarded.
                            // Keep the transfer FIFO unchanged; it only depends on per-QP post/CQE order.
                            DpuDmaVisibilityFence();
                            ret = ibv_post_send(ch->QPair, &ch->ResponseDMAWriteDataWr, &badSendWr);
                            if (ret)
                            {
                                fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                                ret = -1;
                                break;
                            }

                            DpuDmaVisibilityFence();
                            ret = ibv_post_send(ch->QPair, &ch->ResponseDMAWriteDataSplitWr, &badSendWr);
                            if (ret)
                            {
                                fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                                ret = -1;
                                break;
                            }
                        }
                        else
                        {
                            // Legacy split-state scalar retained for reference:
                            // ch->ResponseDMAWriteDataSplitState = BUFF_READ_DATA_SPLIT_STATE_NOT_SPLIT;
                            // jason: fence before posting RDMA writes so RNIC sees latest response bytes.
                            DpuDmaVisibilityFence();
                            ret = ibv_post_send(ch->QPair, &ch->ResponseDMAWriteDataWr, &badSendWr);
                            if (ret)
                            {
                                fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                                ret = -1;
                                break;
                            }
                        }

                        if (EnqueueResponseTransfer(ch, tailStart, totalResponseBytes, pendingDataWrs))
                        {
                            fprintf(stderr,
                                    "%s [error][%s]: failed to enqueue response transfer (tailStart=%d tailEnd=%d bytes=%u)\n",
                                    __func__, DDS_ChannelName(ch->ChannelIndex), tailStart, tailEnd,
                                    (unsigned)totalResponseBytes);
                            ret = -1;
                            break;
                        }

                        postedTail = (tailStart + totalResponseBytes) % DDS_RESPONSE_RING_BYTES;
                        DebugPrint("%s: ch->ResponseRing.TailC %d -> %d\n", __func__,
                                   ch->ResponseRing.TailC, postedTail);
                        ch->ResponseRing.TailC = postedTail;

                        //
                        // Publish the new posted frontier to the host. Reclaim is intentionally
                        // decoupled and advances only on response-data CQEs.
                        //
                        //
                        // jason: fence before posting tail update so RNIC sees updated TailC value.
                        DpuDmaVisibilityFence();
                        // jason: publish the new tail via immediate data for host CQE gating.
                        ch->ResponseDMAWriteMetaWr.imm_data = htonl((uint32_t)ch->ResponseRing.TailC);
                        ret = ibv_post_send(ch->QPair, &ch->ResponseDMAWriteMetaWr, &badSendWr);
                        if (ret)
                        {
                            fprintf(stderr, "%s [error]: ibv_post_send failed: %d (%s)\n", __func__, ret,
                                    strerror(ret));
                            ret = -1;
                        }
                    }
                }
                break;
                default:
                    fprintf(stderr, "%s [error]: unknown read completion\n", __func__);
                    break;
                }
            }
            break;
            case IBV_WC_RDMA_WRITE:
            {
                switch (wc.wr_id)
                {
                case BUFF_WRITE_REQUEST_META_WR_ID:
                {
                    //
                    // Ready to poll
                    //
                    //
                    ret = ibv_post_send(ch->QPair, &ch->RequestDMAReadMetaWr, &badSendWr);
                    if (ret)
                    {
                        fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                        ret = -1;
                    }
                }
                break;
                case BUFF_WRITE_RESPONSE_META_WR_ID:
                {
                    //
                    // There is nothing to do here because response completions are checked in the big loop
                    //
                    //
                }
                break;
                case BUFF_WRITE_RESPONSE_DATA_WR_ID:
                {
                    //
                    // Response data write completions reclaim local source bytes in
                    // FIFO post order. TailC remains the posted frontier; reclaim
                    // happens only when the head transfer's data WRs are all done.
                    //
                    //
                    ret = CompleteResponseTransferDataWr(ch, wc.wr_id);
                    if (ret)
                    {
                        break;
                    }
                    DebugPrint("Responses data CQE: TailA = %d, TailB = %d, TailC = %d, Reclaim = %d\n",
                               ch->ResponseRing.TailA, ch->ResponseRing.TailB, ch->ResponseRing.TailC,
                               ch->ResponseReclaimTail);
                }
                break;
                case BUFF_WRITE_RESPONSE_DATA_SPLIT_WR_ID:
                {
                    //
                    // Split response transfers still reclaim through the same FIFO.
                    // The head descriptor tracks whether one or two data WR CQEs are
                    // still outstanding for that transfer.
                    //
                    //
                    ret = CompleteResponseTransferDataWr(ch, wc.wr_id);
                    if (ret)
                    {
                        break;
                    }
                    DebugPrint("Responses split-data CQE: TailA = %d, TailB = %d, TailC = %d, Reclaim = %d\n",
                               ch->ResponseRing.TailA, ch->ResponseRing.TailB, ch->ResponseRing.TailC,
                               ch->ResponseReclaimTail);
                }
                break;
                default:
                    fprintf(stderr, "%s [error]: unknown write completion\n", __func__);
                    ret = -1;
                    break;
                }
            }
            break;
            case IBV_WC_SEND:
                break;
            default:
                fprintf(stderr, "%s [error]: unknown completion\n", __func__);
                ret = -1;
                break;
            }
        }
    }

ProcessBuffCqEventsReturn:
    return ret;
}

struct runFileBackEndArgs
{
    const char *ServerIpStr;
    const int ServerPort;
    const uint32_t MaxClients;
    const uint32_t MaxBuffs;
};

//
// Check and process I/O completions.
// Updated: acquire Result atomically to ensure BytesServiced is visible.
//
// Check and process IO completions across all buffer connections.
//
// Slot reuse safety (Task 4.4):
//   - Backend side: Each slot's IsPending flag is checked before allocation in
//     ExecuteRequests (write: L2996, read: L3286). MarkRequestContextCompleted
//     clears IsPending only after the response Result is no longer IO_PENDING.
//   - Frontend side: Deferred request head (PublishDeferredRequestHead) is only
//     published after the entire response batch completes, preventing the host
//     from resubmitting to request ring space still occupied by in-flight requests.
//   - Net effect: a slot cannot be reallocated until its response is fully written
//     to the local DMA buffer AND the batch containing it passes IO completion check.
//
static inline int CheckAndProcessIOCompletions(BackEndConfig *Config)
{
    // SPDK_NOTICELOG("CheckAndProcessIOCompletions() running\n");
    int ret = 0;
    BuffConnConfig *buffConn = NULL;

    for (int i = 0; i != Config->MaxBuffs; i++)
    {
        buffConn = &Config->BuffConns[i];
        if (buffConn->State != CONN_STATE_CONNECTED)
        {
            continue;
        }

        // Each BuffConnConfig represents a single IO channel. The outer loop
        // over all BuffConns naturally checks all channels' IO completions.
        BuffConnChannelState *ch = &buffConn->ChannelState;
        if (!DDS_ValidateChannelIndex(ch->ChannelIndex))
        {
            fprintf(stderr, "%s [error]: invalid channel index while checking completions (BuffConn=%d channel=%d)\n",
                    __func__, i, ch->ChannelIndex);
            ret = -1;
            continue;
        }

        //
        // Check the error code of each response, sequentially
        //
        //
#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
        int head1 = ch->ResponseRing.TailB;
        int head2 = ch->ResponseRing.TailC;
        int tail = ch->ResponseRing.TailA;
        char *buffResp = ch->ResponseDMAWriteDataBuff;
        char *curResp;
        FileIOSizeT curRespSize;
        const FileIOSizeT responseHeaderBytes = (FileIOSizeT)(sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));
        const FileIOSizeT totalRespSize = *(FileIOSizeT *)(buffResp + head2);

        if (tail == head1)
        {
            continue;
        }

#if DDS_RING_BATCH_META_DIAGNOSTICS
        // jason: validate batch meta before parsing; if totalRespSize is zero here,
        // we are not looking at a real batch header (or it was overwritten).
        if (totalRespSize == 0)
        {
            FileIOSizeT available = (FileIOSizeT)DistanceBetweenPointers(tail, head2, BACKEND_RESPONSE_BUFFER_SIZE);
            fprintf(stderr,
                    "%s [error]: invalid batch meta (total=0 available=%u head2=%d tail=%d tailB=%d) [buffConn=%d ch=%s]\n",
                    __func__, (unsigned)available, head2, tail, head1,
                    i, DDS_ChannelName(ch->ChannelIndex));
            DumpResponseRingU32Window(buffResp, head2, "BatchMeta(total==0)");
            // Also dump the first bytes at the current tail; helps detect whether TailC points inside payload.
            DumpResponseRingU32Window(buffResp, tail, "TailA(window)");
            // Fail fast to avoid infinite spam; caller main loop will tear down.
            return -1;
        }
#endif

        if (head1 == head2)
        {
            head1 += (sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));
            if (head1 >= BACKEND_RESPONSE_BUFFER_SIZE)
            {
                head1 %= BACKEND_RESPONSE_BUFFER_SIZE;
            }
        }

        while (DistanceBetweenPointers(head1, head2, BACKEND_RESPONSE_BUFFER_SIZE) != totalRespSize)
        {
            // SPDK_NOTICELOG("head1: %d, head2: %d\n", head1, head2);
            curResp = buffResp + head1;
            curRespSize = *(FileIOSizeT *)curResp;
            if (curRespSize == 0)
            {
                // jason: no-split response allocation can inject wrap slack markers.
                // Consume remaining tail bytes and continue at ring offset 0.
                FileIOSizeT advanced = (FileIOSizeT)DistanceBetweenPointers(head1, head2, BACKEND_RESPONSE_BUFFER_SIZE);
                FileIOSizeT remaining = totalRespSize > advanced ? (totalRespSize - advanced) : 0;
                FileIOSizeT slackBytes = (FileIOSizeT)(BACKEND_RESPONSE_BUFFER_SIZE - head1);
                if (slackBytes == 0 || slackBytes > remaining)
                {
                    SPDK_ERRLOG("invalid wrap marker [buffConn=%d ch=%s]: head1=%d head2=%d total=%u advanced=%u slack=%u\n",
                                i, DDS_ChannelName(ch->ChannelIndex), head1, head2,
                                (unsigned)totalRespSize, (unsigned)advanced, (unsigned)slackBytes);
                    break;
                }
                head1 = 0;
                continue;
            }

            {
                FileIOSizeT advanced = (FileIOSizeT)DistanceBetweenPointers(head1, head2, BACKEND_RESPONSE_BUFFER_SIZE);
                FileIOSizeT remaining = totalRespSize > advanced ? (totalRespSize - advanced) : 0;
                if (curRespSize < responseHeaderBytes || curRespSize > remaining)
                {
                    SPDK_ERRLOG("invalid response size in batch [buffConn=%d ch=%s]: size=%u remaining=%u head1=%d head2=%d total=%u\n",
                                i, DDS_ChannelName(ch->ChannelIndex),
                                (unsigned)curRespSize, (unsigned)remaining, head1, head2, (unsigned)totalRespSize);
                    break;
                }
            }

            // if (curRespSize != 12) {
            //     SPDK_NOTICELOG("curRespSize: %u\n", curRespSize);
            // }

            curResp += sizeof(FileIOSizeT);
            // if (((BuffMsgB2FAckHeader*)curResp)->Result == DDS_ERROR_CODE_IO_PENDING) {
            if (DDS_ATOMIC_ERRORCODE_LOAD(&((BuffMsgB2FAckHeader *)curResp)->Result, DDS_ATOMIC_ORDER_ACQUIRE) ==
                DDS_ERROR_CODE_IO_PENDING)
            {
                //
                // TODO: testing
                //
                //
                // ((BuffMsgB2FAckHeader*)curResp)->Result = DDS_ERROR_CODE_SUCCESS;
                // ((BuffMsgB2FAckHeader*)curResp)->BytesServiced = 4;

                // SPDK_NOTICELOG("pending req id: %llu, bytes serviced: %u\n",
                //     ((BuffMsgB2FAckHeader*)curResp)->RequestId, ((BuffMsgB2FAckHeader*)curResp)->BytesServiced);
                break;
            }
            // jason: response completed; release RequestId reuse gate immediately.
            MarkRequestContextCompleted(ch, (BuffMsgB2FAckHeader *)curResp, "batch");

            head1 += curRespSize;
            if (head1 >= BACKEND_RESPONSE_BUFFER_SIZE)
            {
                head1 %= BACKEND_RESPONSE_BUFFER_SIZE;
            }
        }

        if (head1 != ch->ResponseRing.TailB)
        {
            //
            // Update TailB immediately
            //
            //
            ch->ResponseRing.TailB = head1;

            if (DistanceBetweenPointers(head1, head2, BACKEND_RESPONSE_BUFFER_SIZE) == totalRespSize)
            {
                //
                // Send the response back to the host
                //
                //
                DebugPrint("A response batch of %d bytes have finished. Polling host response progress\n",
                           totalRespSize);
                struct ibv_send_wr *badSendWr = NULL;

                // Release deferred request head only after the full response batch is complete.
                // This preserves slot/request-index lifetime until async stage chains have finished.
                if (PublishDeferredRequestHead(ch, &badSendWr))
                {
                    ret = -1;
                }

                //
                // Poll the distance from the host
                //
                //
                ret = ibv_post_send(ch->QPair, &ch->ResponseDMAReadMetaWr, &badSendWr);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                    ret = -1;
                }
            }
        }
#else
        int head = ch->ResponseRing.TailB;
        int tail = ch->ResponseRing.TailA;
        char *buffResp = ch->ResponseDMAWriteDataBuff;
        char *curResp;
        FileIOSizeT curRespSize;
        const FileIOSizeT responseHeaderBytes = (FileIOSizeT)(sizeof(FileIOSizeT) + sizeof(BuffMsgB2FAckHeader));

        if (head == tail)
        {
            continue;
        }

        while (head != tail)
        {
            curResp = buffResp + head;
            curRespSize = *(FileIOSizeT *)curResp;
            if (curRespSize == 0)
            {
                // jason: consume producer-side wrap slack marker in no-split mode.
                FileIOSizeT slackBytes = (FileIOSizeT)(BACKEND_RESPONSE_BUFFER_SIZE - head);
                if (slackBytes == 0)
                {
                    SPDK_ERRLOG("invalid wrap marker in non-batch path [buffConn=%d ch=%s] (head=%d tail=%d)\n",
                                i, DDS_ChannelName(ch->ChannelIndex), head, tail);
                    break;
                }
                head = 0;
                continue;
            }
            if (curRespSize < responseHeaderBytes)
            {
                SPDK_ERRLOG("invalid response size in non-batch path [buffConn=%d ch=%s]: size=%u head=%d tail=%d\n",
                            i, DDS_ChannelName(ch->ChannelIndex), (unsigned)curRespSize, head, tail);
                break;
            }
            curResp += sizeof(FileIOSizeT);
            // if (((BuffMsgB2FAckHeader*)curResp)->Result == DDS_ERROR_CODE_IO_PENDING) {
            if (DDS_ATOMIC_ERRORCODE_LOAD(&((BuffMsgB2FAckHeader *)curResp)->Result, DDS_ATOMIC_ORDER_ACQUIRE) ==
                DDS_ERROR_CODE_IO_PENDING)
            {
                //
                // TODO: testing
                //
                //
                // ((BuffMsgB2FAckHeader*)curResp)->Result = DDS_ERROR_CODE_SUCCESS;
                // ((BuffMsgB2FAckHeader*)curResp)->BytesServiced = 4;
                ((BuffMsgB2FAckHeader *)curResp)->BytesServiced = 4;
                // jason: publish Result after BytesServiced so poller sees a complete response.
                DDS_ATOMIC_ERRORCODE_STORE(&((BuffMsgB2FAckHeader *)curResp)->Result, DDS_ERROR_CODE_SUCCESS,
                                           DDS_ATOMIC_ORDER_RELEASE);

                // break;
            }
            if (DDS_ATOMIC_ERRORCODE_LOAD(&((BuffMsgB2FAckHeader *)curResp)->Result, DDS_ATOMIC_ORDER_ACQUIRE) !=
                DDS_ERROR_CODE_IO_PENDING)
            {
                // jason: response completed; release RequestId reuse gate immediately.
                MarkRequestContextCompleted(ch, (BuffMsgB2FAckHeader *)curResp, "non-batch");
            }

            head += curRespSize;
            if (head >= BACKEND_RESPONSE_BUFFER_SIZE)
            {
                head %= BACKEND_RESPONSE_BUFFER_SIZE;
            }
        }

        //
        // No batching needed here
        //
        //
        if (head != ch->ResponseRing.TailB)
        {
            //
            // Send the response back to the host
            //
            //
            struct ibv_send_wr *badSendWr = NULL;

            //
            // Update TailB immediately
            //
            //
            ch->ResponseRing.TailB = head;

            //
            // Poll the distance from the host
            //
            //
            // Release deferred request head only after responses are complete.
            // This preserves slot/request-index lifetime until async stage chains have finished.
            if (PublishDeferredRequestHead(ch, &badSendWr))
            {
                ret = -1;
            }

            printf("%s: Polling response ring meta\n", __func__);
            ret = ibv_post_send(ch->QPair, &ch->ResponseDMAReadMetaWr, &badSendWr);
            if (ret)
            {
                fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                ret = -1;
            }
        }
#endif
    }
    // SPDK_NOTICELOG("CheckAndProcessIOCompletions() returned\n");
    return ret;
}

//
// Check and process control plane completions
//
//
static inline int CheckAndProcessControlPlaneCompletions(BackEndConfig *Config)
{
    int ret = 0;
    CtrlConnConfig *ctrlConn = NULL;
    struct ibv_send_wr *badSendWr = NULL;

    for (int i = 0; i != Config->MaxClients; i++)
    {
        ctrlConn = &Config->CtrlConns[i];
#ifdef CREATE_DEFAULT_DPU_FILE
        if (ctrlConn->State != CONN_STATE_CONNECTED && i != DEFAULT_DPU_FILE_CREATION_CTRL_CONN)
        {
            continue;
        }
#else
        if (ctrlConn->State != CONN_STATE_CONNECTED)
        {
            continue;
        }
#endif

        if (ctrlConn->PendingControlPlaneRequest.RequestId == DDS_REQUEST_INVALID)
        {
            continue;
        }

        //
        // The first field of response should always be the error code
        //
        //
        if (*(ErrorCodeT *)(ctrlConn->PendingControlPlaneRequest.Response) != DDS_ERROR_CODE_IO_PENDING)
        {
            // SPDK_NOTICELOG("Responding back... and NULLing ctrl plane request\n");
            ctrlConn->PendingControlPlaneRequest.RequestId = DDS_REQUEST_INVALID;
            ctrlConn->PendingControlPlaneRequest.Request = NULL;
            ctrlConn->PendingControlPlaneRequest.Response = NULL;

#ifdef CREATE_DEFAULT_DPU_FILE
            if (i == DEFAULT_DPU_FILE_CREATION_CTRL_CONN)
            {
                switch (ctrlConn->DefaultDpuFileCreationState)
                {
                case FILE_CREATION_SUBMITTED:
                {
                    CtrlMsgF2BReqChangeFileSize *req = &ctrlConn->DefaultChangeFileRequest;
                    CtrlMsgB2FAckChangeFileSize *resp = &ctrlConn->DefaultChangeFileResponse;
                    CtrlMsgB2FAckCreateFile *createResp = &ctrlConn->DefaultCreateFileResponse;

                    // ctrlConn->DefaultDpuFileCreationState = FILE_CREATED;
                    // fprintf(stdout, "DPU default file has been created\n");
                    // Guard the log and follow-up request on create success.
                    if (createResp->Result != DDS_ERROR_CODE_SUCCESS)
                    {
                        fprintf(stderr, "DPU default file creation failed: %hu\n", createResp->Result);
                        ctrlConn->DefaultDpuFileCreationState = FILE_NULL;
                        break;
                    }

                    ctrlConn->DefaultDpuFileCreationState = FILE_CREATED;
                    fprintf(stdout, "DPU default file has been created\n");

                    //
                    // Change file size
                    //
                    //
                    req->FileId = DEFAULT_DPU_FILE_ID;
                    req->NewSize = DEFAULT_DPU_FILE_SIZE;

                    ctrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_CHANGE_FILE_SIZE;
                    ctrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
                    ctrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
                    resp->Result = DDS_ERROR_CODE_IO_PENDING;
                    SubmitControlPlaneRequest(Config->FS, &ctrlConn->PendingControlPlaneRequest);
                }
                break;
                case FILE_CREATED:
                {
                    ctrlConn->DefaultDpuFileCreationState = FILE_CHANGED;
                    fprintf(stdout, "DPU default file size has been changed to %llu\n", DEFAULT_DPU_FILE_SIZE);
                }
                break;
                default:
                {
                    //
                    // Regular control operation. Respond back to the host
                    //
                    //
                    ret = ibv_post_send(ctrlConn->QPair, &ctrlConn->SendWr, &badSendWr);
                    if (ret)
                    {
                        fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                        ret = -1;
                    }
                }
                break;
                }
            }
            else
            {
                //
                // It's complete. Respond back to the host
                //
                //
                ret = ibv_post_send(ctrlConn->QPair, &ctrlConn->SendWr, &badSendWr);
                if (ret)
                {
                    fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                    ret = -1;
                }
            }
#else
            //
            // It's complete. Respond back to the host
            //
            //
            ret = ibv_post_send(ctrlConn->QPair, &ctrlConn->SendWr, &badSendWr);
            if (ret)
            {
                fprintf(stderr, "%s [error]: ibv_post_send failed: %d\n", __func__, ret);
                ret = -1;
            }
#endif
        }
    }

    return ret;
}

//
// CPU affinity
//
//
int AffinitizeCurrentThread(int CoreId)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(CoreId, &cpuset);

    pthread_t currentThread = pthread_self();
    return pthread_setaffinity_np(currentThread, sizeof(cpu_set_t), &cpuset);
}

//
// DMA agent thread is a pthread
//
//

// Task 5.1: Periodic channel-scoped telemetry dump.
// Prints per-channel queue depth, deferred head distance, and debug counters
// for all connected BuffConns. This is debug instrumentation only and is
// compiled out by default (DDS_CHANNEL_TELEMETRY_INTERVAL == 0).
// When enabled, it is called every DDS_CHANNEL_TELEMETRY_INTERVAL
// dataPlaneCounter resets on the DMA agent thread.
//
static void DumpChannelTelemetry(BackEndConfig *Config)
{
#if DDS_CHANNEL_TELEMETRY_INTERVAL > 0
    for (int i = 0; i < (int)Config->MaxBuffs; i++)
    {
        BuffConnConfig *bc = &Config->BuffConns[i];
        if (bc->State != CONN_STATE_CONNECTED)
        {
            continue;
        }
        BuffConnChannelState *ch = &bc->ChannelState;
        // Request ring queue depth: distance from Head to pending head (requests consumed but head not published).
        int reqQueueDepth = 0;
        if (ch->RequestPendingHeadValid)
        {
            reqQueueDepth = DistanceBetweenPointers(ch->RequestPendingHead, ch->RequestRing.Head,
                                                     BACKEND_REQUEST_BUFFER_SIZE);
        }
        // Response ring occupancy: produced bytes not yet posted to host.
        int respOccupancy = DistanceBetweenPointers(ch->ResponseRing.TailA, ch->ResponseRing.TailC,
                                                     BACKEND_RESPONSE_BUFFER_SIZE);
        // Local response source bytes still not reclaimable because response data
        // WR CQEs have not all arrived yet.
        int respInFlight = DistanceBetweenPointers(ch->ResponseRing.TailC, ch->ResponseReclaimTail,
                                                   BACKEND_RESPONSE_BUFFER_SIZE);
        fprintf(stderr,
                "[telemetry] buffConn=%d ch=%s: dispatched=%lu completions=%lu deferredPubs=%lu "
                "reqQueueDepth=%d respOccupancy=%d respInFlight=%d deferredHeadValid=%d\n",
                i, DDS_ChannelName(ch->ChannelIndex),
                (unsigned long)ch->RequestsDispatched,
                (unsigned long)ch->CompletionsProcessed,
                (unsigned long)ch->DeferredHeadPublishes,
                reqQueueDepth, respOccupancy, respInFlight,
                ch->RequestPendingHeadValid);
    }
#else
    (void)Config;
#endif
}

void *DMAAgentThread(void *Arg)
{
    struct rdma_cm_event *event;
    int ret = 0;
    int dataPlaneCounter = 0;

    BackEndConfig *config = (BackEndConfig *)Arg;

    //
    // Affinitize the current thread
    //
    //
    AffinitizeCurrentThread(CORE_ALLOCATION_STORAGE_ENGINE_AGENT_CORE);

    //
    // Initialize DMA
    //
    //
    ret = InitDMA(&config->DMAConf, config->ServerIp, config->ServerPort);
    if (ret)
    {
        fprintf(stderr, "InitDMA failed with %d\n", ret);
        return NULL;
    }

    //
    // Allocate connections
    //
    //
    ret = AllocConns(config);
    if (ret)
    {
        fprintf(stderr, "AllocConns failed with %d\n", ret);
        TermDMA(&config->DMAConf);
        return NULL;
    }

    ret = InitializeDpkRuntimeAtBackendStartup(config);
    if (ret)
    {
        fprintf(stderr, "InitializeDpkRuntimeAtBackendStartup failed with %d\n", ret);
        DeallocConns(config);
        CleanupDpkRuntimeAndIoPool();
        TermDMA(&config->DMAConf);
        return NULL;
    }

    //
    // Listen for incoming connections
    //
    //
    ret = rdma_listen(config->DMAConf.CmId, LISTEN_BACKLOG);
    if (ret)
    {
        ret = errno;
        fprintf(stderr, "rdma_listen error %d\n", ret);
        DeallocConns(config);
        CleanupDpkRuntimeAndIoPool();
        TermDMA(&config->DMAConf);
        return NULL;
    }

    while (ForceQuitStorageEngine == 0)
    {
        if (dataPlaneCounter == 0)
        {
            //
            // Process connection events
            //
            //
            ret = rdma_get_cm_event(config->DMAConf.CmChannel, &event);
            if (ret && errno != EAGAIN)
            {
                ret = errno;
                fprintf(stderr, "rdma_get_cm_event error %d\n", ret);
                SignalHandler(SIGTERM);
            }
            else if (!ret)
            {
#ifdef DDS_STORAGE_FILE_BACKEND_VERBOSE
                fprintf(stdout, "cma_event type %s cma_id %p (%s)\n", rdma_event_str(event->event), event->id,
                        (event->id == config->DMAConf.CmId) ? "parent" : "child");
#endif

                ret = ProcessCmEvents(config, event);
                if (ret)
                {
                    fprintf(stderr, "ProcessCmEvents error %d\n", ret);
                    SignalHandler(SIGTERM);
                }
            }
            // SPDK_NOTICELOG("Process connection events completed\n");

            //
            // Process RDMA events for control connections
            //
            //
            ret = ProcessCtrlCqEvents(config);
            if (ret)
            {
                fprintf(stderr, "ProcessCtrlCqEvents error %d\n", ret);
                SignalHandler(SIGTERM);
            }
            // SPDK_NOTICELOG("Process control conn completed\n");

            //
            // Check and process control plane completions
            //
            //
            ret = CheckAndProcessControlPlaneCompletions(config);
            if (ret)
            {
                fprintf(stderr, "CheckAndProcessControlPlaneCompletions error %d\n", ret);
                SignalHandler(SIGTERM);
            }
            // SPDK_NOTICELOG("control plane completion completed\n");

#ifdef CREATE_DEFAULT_DPU_FILE
            if (config->CtrlConns[DEFAULT_DPU_FILE_CREATION_CTRL_CONN].DefaultDpuFileCreationState == FILE_NULL)
            {
                CtrlConnConfig *ctrlConn = &config->CtrlConns[DEFAULT_DPU_FILE_CREATION_CTRL_CONN];
                CtrlMsgF2BReqCreateFile *req = &ctrlConn->DefaultCreateFileRequest;
                CtrlMsgB2FAckCreateFile *resp = &ctrlConn->DefaultCreateFileResponse;

                //
                // Wait for the file service to get ready
                //
                //
                sleep(2);

                //
                // Create the file
                //
                //
                req->DirId = DDS_DIR_ROOT;
                req->FileAttributes = 0;
                req->FileId = DEFAULT_DPU_FILE_ID;
                strcpy(req->FileName, "DpuDefaulFile");

                ctrlConn->PendingControlPlaneRequest.RequestId = CTRL_MSG_F2B_REQ_CREATE_FILE;
                ctrlConn->PendingControlPlaneRequest.Request = (BufferT)req;
                ctrlConn->PendingControlPlaneRequest.Response = (BufferT)resp;
                resp->Result = DDS_ERROR_CODE_IO_PENDING;
                SubmitControlPlaneRequest(config->FS, &ctrlConn->PendingControlPlaneRequest);
                ctrlConn->DefaultDpuFileCreationState = FILE_CREATION_SUBMITTED;
            }
#endif
        }

        //
        // Process RDMA events for buffer connections
        //
        //
        // SPDK_NOTICELOG("buffer connections started\n");
        ret = ProcessBuffCqEvents(config);
        if (ret)
        {
            fprintf(stderr, "ProcessBuffCqEvents error %d\n", ret);
            SignalHandler(SIGTERM);
        }
        // SPDK_NOTICELOG("buffer connections completed\n");

        //
        // Check and process I/O completions
        //
        //
        ret = CheckAndProcessIOCompletions(config);
        if (ret)
        {
            fprintf(stderr, "CheckAndProcessIOCompletions error %d\n", ret);
            SignalHandler(SIGTERM);
        }
        // SPDK_NOTICELOG("io completions completed\n");

        dataPlaneCounter++;
        if (dataPlaneCounter == DATA_PLANE_WEIGHT)
        {
            dataPlaneCounter = 0;
#if DDS_CHANNEL_TELEMETRY_INTERVAL > 0
            // Task 5.1: Periodic telemetry dump (debug-only, opt-in).
            static int telemetryCounter = 0;
            telemetryCounter++;
            if (telemetryCounter >= DDS_CHANNEL_TELEMETRY_INTERVAL)
            {
                telemetryCounter = 0;
                DumpChannelTelemetry(config);
            }
#endif
        }
    }

    //
    // Clean up
    //
    //
    DeallocConns(config);
    CleanupDpkRuntimeAndIoPool();
    TermDMA(&config->DMAConf);
    StopFileService(config->FS);
    sleep(1);
    DeallocateFileService(config->FS);

    return NULL;
}

//
// The entry point for the back end
// This is on the main thread not app thread, i.e. app start not called when entering this func
//
//
int RunFileBackEnd(const char *ServerIpStr, const int ServerPort, const uint32_t MaxClients, const uint32_t MaxBuffs,
                   int Argc, char **Argv)
{
    BackEndConfig config;
    pthread_t dmaAgent;
    int ret;

    //
    // Initialize the back end configuration
    //
    //
    config.ServerIp = inet_addr(ServerIpStr);
    config.ServerPort = htons(ServerPort);
    ;
    config.MaxClients = MaxClients;
    config.MaxBuffs = MaxBuffs;
    config.CtrlConns = NULL;
    config.BuffConns = NULL;
    config.DMAConf.CmChannel = NULL;
    config.DMAConf.CmId = NULL;
    config.FS = NULL;

    //
    // Initialize Cache Table
    //
    //
    //     ret = InitCacheTable(&GlobalCacheTable);
    //     if (ret) {
    //         fprintf(stderr, "InitCacheTable failed\n");
    //         return -1;
    //     }

    // #ifdef PRELOAD_CACHE_TABLE_ITEMS
    //     FILE *file = fopen(CACHE_TABLE_FILE_PATH, "rb");
    //     if (!file) {
    //         fprintf(stderr, "Failed to open the file to preload\n");
    //         return -1;
    //     }

    //     size_t chunkSize = sizeof(CacheItemT) * 1000;
    //     char* readBuffer = malloc(chunkSize);
    //     if (!readBuffer) {
    //         fprintf(stderr, "Failed to allocate buffer for preloading\n");
    //         return -1;
    //     }

    //     size_t bytesRead, numItemsInABatch, totalItems = 0;
    //     CacheItemT* items;
    //     fprintf(stdout, "Populating cache table by preloading %s...\n", CACHE_TABLE_FILE_PATH);
    //     while ((bytesRead = fread(readBuffer, 1, chunkSize, file)) > 0) {
    //         if (bytesRead % sizeof(CacheItemT)) {
    //             fprintf(stderr, "Failed to load a chunk %lu\n", bytesRead);
    //             fclose(file);
    //             return -1;
    //         }
    //         numItemsInABatch = bytesRead / sizeof(CacheItemT);
    //         items = (CacheItemT*)readBuffer;
    //         for (int i = 0; i != numItemsInABatch; i++) {
    //             if (AddToCacheTable(GlobalCacheTable, &items[i])) {
    //                 fprintf(stderr, "Failed to add item %lu into cache table\n", items[i].Key);
    //                 fclose(file);
    //                 return -1;
    //             }
    //             totalItems++;
    //         }
    //     }
    //     fprintf(stdout, "Cache table has been populated with %lu items\n", totalItems);

    //     fclose(file);
    //     free(readBuffer);
    // #endif

    //
    // Allocate the file service object
    //
    //

    // Task 1.3: Validate dual-channel outstanding capacity at startup.
    // Each channel has DDS_MAX_OUTSTANDING_IO_PER_CHANNEL slots; total across
    // all channels = DDS_MAX_OUTSTANDING_IO_PER_CHANNEL * DDS_NUM_IO_CHANNELS.
    // Fail-fast if configuration is inconsistent or insufficient.
    {
        const int totalSlots = DDS_MAX_OUTSTANDING_IO_TOTAL;
        fprintf(stdout, "[capacity] DDS_NUM_IO_CHANNELS=%d  DDS_MAX_OUTSTANDING_IO_PER_CHANNEL=%d  total_slots=%d\n",
                DDS_NUM_IO_CHANNELS, DDS_MAX_OUTSTANDING_IO_PER_CHANNEL, totalSlots);
        if (DDS_NUM_IO_CHANNELS < 1 || DDS_NUM_IO_CHANNELS > 4)
        {
            fprintf(stderr, "[capacity] ERROR: DDS_NUM_IO_CHANNELS=%d out of valid range [1,4]\n",
                    DDS_NUM_IO_CHANNELS);
            return -1;
        }
        if (DDS_MAX_OUTSTANDING_IO_PER_CHANNEL < 1)
        {
            fprintf(stderr, "[capacity] ERROR: DDS_MAX_OUTSTANDING_IO_PER_CHANNEL=%d must be >= 1\n",
                    DDS_MAX_OUTSTANDING_IO_PER_CHANNEL);
            return -1;
        }
        if (DDS_MAX_OUTSTANDING_IO_TOTAL < DDS_MAX_OUTSTANDING_IO_PER_CHANNEL)
        {
            fprintf(stderr, "[capacity] ERROR: total slot count %d is smaller than per-channel limit %d\n",
                    DDS_MAX_OUTSTANDING_IO_TOTAL, DDS_MAX_OUTSTANDING_IO_PER_CHANNEL);
            return -1;
        }
        uint32_t requiredBuffConns = MaxClients * (uint32_t)DDS_NUM_IO_CHANNELS;
        if (MaxBuffs < requiredBuffConns)
        {
            fprintf(stderr,
                    "[capacity] ERROR: MaxBuffs=%u is too small for MaxClients=%u and channels=%d "
                    "(require at least %u BuffConns)\n",
                    (unsigned)MaxBuffs, (unsigned)MaxClients, DDS_NUM_IO_CHANNELS, (unsigned)requiredBuffConns);
            return -1;
        }
        fprintf(stdout, "[capacity] Dual-channel capacity validation passed (MaxBuffs=%u)\n",
                (unsigned)MaxBuffs);
    }

    config.FS = AllocateFileService();
    if (!config.FS)
    {
        fprintf(stderr, "AllocateFileService failed\n");
        return -1;
    }

    //
    // Run DMA agent in a new thread
    //
    //
    ret = pthread_create(&dmaAgent, NULL, DMAAgentThread, (void *)&config);
    if (ret)
    {
        fprintf(stderr, "Failed to start DMA agent thread\n");
        return ret;
    }

    //
    // Run file service on the current thread
    //
    //
    SPDK_NOTICELOG("Starting file service...\n");
    printf("Starting file service...\n");
    StartFileService(Argc, Argv, config.FS);

    printf("Waiting for the agent thread to exit\n");
    pthread_join(dmaAgent, NULL);
    printf("Agent thread exited\n");

    //
    // Destroy Cache Table
    //

    // DestroyCacheTable(GlobalCacheTable);

    return ret;
}

//
// Stop the back end
//
//
int StopFileBackEnd()
{
    ForceQuitStorageEngine = 1;

    return 0;
}
