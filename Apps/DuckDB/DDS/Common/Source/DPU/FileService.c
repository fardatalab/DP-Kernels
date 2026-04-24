#define _GNU_SOURCE

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <sched.h>
#include <stdbool.h>

#include "FileService.h"
#include "Zmalloc.h"
#include "CacheTable.h"
#include "Protocol.h"
#include "DDSChannelDefs.h"

// #undef DEBUG_FILE_SERVICE
#define DEBUG_FILE_SERVICE
#ifdef DEBUG_FILE_SERVICE
#include <stdio.h>
#define DebugPrint(Fmt, ...) fprintf(stderr, Fmt, ##__VA_ARGS__)
#else
static inline void DebugPrint(const char* Fmt, ...) { }
#endif

#define SPDK_LOG_LEVEL SPDK_LOG_NOTICE

//
// The global cache table
//
//
CacheTableT* CacheTable;

struct DPUStorage *Sto;
char *G_BDEV_NAME = "nvme0n1";
FileService* FS;
extern bool G_INITIALIZATION_DONE;
extern volatile int ForceQuitStorageEngine;
int WorkerId = 0;
// jason: round-robin selector for data-plane owner worker selection.
// We intentionally keep control-plane submission on WorkerId for now.
static atomic_uint g_dataPlaneOwnerWorkerRR = 0;
static DataPlaneStagePollerCtx g_dataPlaneStagePollerCtx[WORKER_THREAD_COUNT];

// Original widened slot-count macro retained for reference:
// #ifndef DDS_BACKEND_SLOT_COUNT
// #define DDS_BACKEND_SLOT_COUNT (DDS_MAX_OUTSTANDING_IO * (DDS_DPU_IO_PARALLELISM + 1))
// #endif

static int PinCurrentPthreadToCore(int coreId, const char *tag)
{
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(coreId, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
        SPDK_ERRLOG("%s: pthread_setaffinity_np failed for core %d (rc=%d)\n", tag ? tag : "PinThread", coreId, rc);
    }
    else
    {
        SPDK_NOTICELOG("%s pinned to core %d\n", tag ? tag : "Thread", coreId);
    }
    return rc;
}

static uint32_t GetStorageWorkerCoreId(int workerIndex)
{
    if (workerIndex <= 0)
    {
        return (uint32_t)CORE_ALLOCATION_STORAGE_ENGINE_WORKER0_CORE;
    }
    if (workerIndex == 1)
    {
        return (uint32_t)CORE_ALLOCATION_STORAGE_ENGINE_WORKER1_CORE;
    }
    return (uint32_t)(CORE_ALLOCATION_STORAGE_ENGINE_WORKER1_CORE + (workerIndex - 1));
}

//
// Select owner worker for data-plane request submission.
// This spreads request orchestration across all storage workers while preserving
// stage-chain correctness (owner still serializes stage progression/publication).
//
static inline uint32_t SelectDataPlaneOwnerWorkerIndex(FileService *FS)
{
    if (FS == NULL || FS->WorkerThreadCount <= 0 || FS->WorkerThreads == NULL)
    {
        return 0;
    }
    unsigned rr = atomic_fetch_add_explicit(&g_dataPlaneOwnerWorkerRR, 1u, memory_order_relaxed);
    uint32_t selected = (uint32_t)(rr % (unsigned)FS->WorkerThreadCount);
    if (selected >= (uint32_t)FS->WorkerThreadCount || FS->WorkerThreads[selected] == NULL)
    {
        // jason: defensive fallback in case worker table is not fully initialized.
        selected = 0;
    }
    return selected;
}

//
// Initialize per-slot stage metadata from the parsed data-plane request context.
// This mirrors host-side stage sizes/input windows so DataPlaneHandlers can run a stage chain.
//
static inline void
InitializeSlotStageMetadata(
    struct PerSlotContext* SlotContext,
    DataPlaneRequestContext* Context,
    uint32_t OwnerWorkerIndex,
    struct spdk_thread* OwnerWorkerThread
) {
    if (SlotContext == NULL || Context == NULL)
    {
        SPDK_ERRLOG("%s: invalid SlotContext/Context\n", __func__);
        return;
    }
    SlotContext->OwnerWorkerIndex = OwnerWorkerIndex;
    SlotContext->OwnerWorkerThread = OwnerWorkerThread;
    SlotContext->StageCount = Context->StageCount;
    SlotContext->CopyStart = Context->CopyStart;
    // jason: Mirror per-request response ring bounds into slot context.
    // Stage execution validates in/out pointers against these slot fields.
    SlotContext->ResponseRingBase = Context->ResponseRingBase;
    SlotContext->ResponseRingBytes = Context->ResponseRingBytes;
    SlotContext->StageChainRequiredWithoutIO = false;
    if (SlotContext->ResponseRingBase == NULL || SlotContext->ResponseRingBytes == 0)
    {
        RequestIdT reqId = DDS_REQUEST_INVALID;
        if (Context->Response)
        {
            reqId = Context->Response->RequestId;
        }
        SPDK_ERRLOG(
            "%s: request has invalid response ring bounds (reqId=%u slotBase=%p slotBytes=%u ctxBase=%p ctxBytes=%u)\n",
            __func__, (unsigned)reqId, SlotContext->ResponseRingBase, (unsigned)SlotContext->ResponseRingBytes,
            Context->ResponseRingBase, (unsigned)Context->ResponseRingBytes);
    }
    if (!SlotContext->DpkTaskStateInitialized) {
        SlotContext->DpkStageTaskHandles[0] = NULL;
        SlotContext->DpkStageTaskHandles[1] = NULL;
        SlotContext->DpkTaskStateInitialized = true;
    }
    for (int i = 0; i < 2; i++) {
        SlotContext->StageSizes[i] = Context->StageSizes[i];
        SlotContext->StageInputOffsets[i] = Context->StageInputOffsets[i];
        SlotContext->StageInputLengths[i] = Context->StageInputLengths[i];
        SlotContext->StageBuffers[i] = Context->StageBuffers[i];
        SlotContext->StageInputBytes[i] = 0;
        atomic_store_explicit(&SlotContext->StageDone[i], false, memory_order_relaxed);
        atomic_store_explicit(&SlotContext->StageSuccess[i], true, memory_order_relaxed);
        SlotContext->StageTasks[i].SlotContext = SlotContext;
        SlotContext->StageTasks[i].StageIndex = (uint16_t)i;
        SlotContext->StageTasks[i].TargetWorkerIndex = OwnerWorkerIndex;
    }
}

//
// Usage function for printing parameters that are specific to this application, needed for SPDK app start
//
//
static void
DDSCustomArgsUsage(void) {
    printf("if there is any custom cmdline params, add a description for it here\n");
}

//
// This function is called to parse the parameters that are specific to this application
//
//
static int
DDSParseArg(
    int ch,
    char *arg
) {
    printf("parse custom arg func called, currently doing nothing...\n");
    return 0;
}

//
// Allocate the file service object
//
//
FileService*
AllocateFileService() {
    DebugPrint("Allocating the file service object...\n");
    FS = (FileService*)malloc(sizeof(FileService));
    // TODO: anything to do here?
    return FS;
}


void GetIOChannel(void* Ctx) {
    SPDKContextT* ctx = (SPDKContextT*)Ctx;
    SPDK_NOTICELOG("Initializing per thread IO Channel, thread %lu...\n", spdk_thread_get_id(spdk_get_thread()));
    ctx->bdev_io_channel = spdk_bdev_get_io_channel(ctx->bdev_desc);
    if (ctx->bdev_io_channel == NULL) {
        SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
        exit(-1);
        return;
    }
}

//
// This is also on app thread, can we wait in here??
//
//
void InitializeWorkerThreadIOChannel(FileService *FS) {
    for (size_t i = 0; i < FS->WorkerThreadCount; i++)
    {
        struct spdk_thread *worker = FS->WorkerThreads[i];
        SPDKContextT *ctx = &FS->WorkerSPDKContexts[i];
        SPDK_NOTICELOG("worker thread no. %lu, has poller: %d\n", i, spdk_thread_has_pollers(worker));
        SPDK_NOTICELOG("thread is idle: %d\n", spdk_thread_is_idle(worker));
        int result = spdk_thread_send_msg(worker, GetIOChannel, ctx);
        if (result) {
            SPDK_ERRLOG("failed for i %lu, result: %s\n", i, spdk_strerror(-result));
        }
        else {
            SPDK_NOTICELOG("sent GetIOChannel() to thread no. %lu, ptr: %p\n", i, worker);
        }
    }

    // DON'T wait for all workers to finish
    /* int i = 0;
    bool allDone = true;
    while (i < 10) {
        SPDK_NOTICELOG("waiting for all worker threads...\n");
        sleep(1);
        for (size_t i = 0; i < FS->WorkerThreadCount; i++) {
            if (FS->WorkerSPDKContexts[i].bdev_io_channel == NULL) {
                allDone = false;
                SPDK_NOTICELOG("thread %d hasn't yet got io channel!\n", i);
                break;
            }
        }
        if (allDone) {
            SPDK_NOTICELOG("all threads got io channel!\n");
            break;
        }
        i++;
    }

    if (!allDone) {
        // TODO: FS app stop
        SPDK_ERRLOG("waiting for all worker threads timed out, EXITING...\n");
        exit(-1);
    } */
}

//
// Register one per-worker stage completion poller on each worker thread.
// Pollers are used by async DPK stage execution to avoid blocking worker callbacks.
//
void InitializeWorkerThreadStagePollers(FileService *FS) {
    for (size_t i = 0; i < FS->WorkerThreadCount; i++)
    {
        struct spdk_thread *worker = FS->WorkerThreads[i];
        g_dataPlaneStagePollerCtx[i].WorkerIndex = (uint32_t)i;
        int result = spdk_thread_send_msg(worker, RegisterDataPlaneStagePoller, &g_dataPlaneStagePollerCtx[i]);
        if (result) {
            SPDK_ERRLOG("failed to register stage poller on worker %lu: %s\n", i, spdk_strerror(-result));
        }
        else {
            SPDK_NOTICELOG("sent RegisterDataPlaneStagePoller() to worker %lu, ptr: %p\n", i, worker);
        }
    }
}


//
// This is the func supplied to `spdk_app_start()`, will do the actual init work asynchronously
//
//
void StartSPDKFileService(void* Ctx) {
    PinCurrentPthreadToCore(CORE_ALLOCATION_STORAGE_ENGINE_APP_CORE, "DDS app thread");

    //
    // Initialize worker threads and their SPDKContexts
    //
    //
    struct StartFileServiceCtx* StartCtx = (struct StartFileServiceCtx*)Ctx;
    FileService* FS = StartCtx->FS;
    FS->WorkerThreadCount = WORKER_THREAD_COUNT;
    FS->WorkerThreads = calloc(FS->WorkerThreadCount, sizeof(struct spdk_thread*));
    memset(FS->WorkerThreads, 0, FS->WorkerThreadCount * sizeof(struct spdk_thread*));
    FS->WorkerSPDKContexts = calloc(FS->WorkerThreadCount, sizeof(SPDKContextT));

    int ret;
    struct spdk_bdev_desc *bdev_desc;
    ret = spdk_bdev_open_ext(G_BDEV_NAME, true, SpdkBdevEventCb, NULL,
            &bdev_desc);
    if (ret) {
        DebugPrint("spdk_bdev_open_ext FAILED!!! FATAL, EXITING...\n");
        exit(-1);
    }
    struct spdk_bdev *bdev = spdk_bdev_desc_get_bdev(bdev_desc);

    FS->MasterSPDKContext = malloc(sizeof(*FS->MasterSPDKContext));
    FS->MasterSPDKContext->bdev_io_channel = NULL;
    FS->MasterSPDKContext->bdev_name = G_BDEV_NAME;
    FS->MasterSPDKContext->bdev_desc = bdev_desc;
    FS->MasterSPDKContext->bdev = bdev;

    // SPDK_WARNLOG("bdev buf align = %lu\n", spdk_bdev_get_buf_align(bdev));
    // jason: Cache bdev alignment constraints for data-plane use.
    uint32_t bdev_block_size = spdk_bdev_get_block_size(bdev);
    uint32_t bdev_buf_align = spdk_bdev_get_buf_align(bdev);
    if (bdev_block_size == 0) {
        bdev_block_size = 1;
    }
    if (bdev_buf_align == 0) {
        bdev_buf_align = 1;
    }
    FS->MasterSPDKContext->block_size = bdev_block_size;
    FS->MasterSPDKContext->buf_align = bdev_buf_align;
    SPDK_WARNLOG("bdev block size = %u, buf align = %u\n", bdev_block_size, bdev_buf_align);

    SPDK_NOTICELOG("AllocateSpace()...\n");
    AllocateSpace(FS->MasterSPDKContext);  // allocated SPDKSpace will be copied/shared to all worker contexts


    struct spdk_cpuset tmp_cpumask = {};
    for (int i = 0; i < FS->WorkerThreadCount; i++) {
        SPDK_NOTICELOG("creating worker thread %d\n", i);
        char threadName[32];
        snprintf(threadName, 32, "worker_thread%d", i);
        spdk_cpuset_zero(&tmp_cpumask);
        uint32_t workerCore = GetStorageWorkerCoreId(i);
        spdk_cpuset_set_cpu(&tmp_cpumask, workerCore, true);
        FS->WorkerThreads[i] = spdk_thread_create(threadName, &tmp_cpumask);
        if (FS->WorkerThreads[i] == NULL) {
            SPDK_ERRLOG("CANNOT CREATE WORKER THREAD %d on core %u!!! FATAL, EXITING...\n", i, workerCore);
            exit(-1);  // TODO: FS app stop
        }
        SPDK_NOTICELOG("thread id: %lu, thread ptr: %p\n", spdk_thread_get_id(FS->WorkerThreads[i]), FS->WorkerThreads[i]);
        
        SPDKContextT *SPDKContext = &FS->WorkerSPDKContexts[i];
        memcpy(SPDKContext, FS->MasterSPDKContext, sizeof(*SPDKContext));
    }
    FS->MasterSPDKContext->bdev_io_channel = spdk_bdev_get_io_channel(FS->MasterSPDKContext->bdev_desc);
    
    if (FS->MasterSPDKContext->bdev_io_channel == NULL) {
        DebugPrint("master context can't get IO channel!!! Exiting...\n");
        exit(-1);
    }
    FS->AppThread = spdk_get_thread();
    SPDK_NOTICELOG("FS->AppThread id: %lu\n", spdk_thread_get_id(FS->AppThread));
    SPDK_NOTICELOG("FS->AppThread : %lu\n", spdk_thread_get_id(FS->AppThread));
    SPDK_NOTICELOG("master thread has poller: %d\n", spdk_thread_has_pollers(FS->AppThread));
    SPDK_NOTICELOG("master thread is idle: %d\n", spdk_thread_is_idle(FS->AppThread));

    SPDK_NOTICELOG("Calling InitializeWorkerThreadStagePollers()...\n");
    InitializeWorkerThreadStagePollers(FS);

    SPDK_NOTICELOG("Calling InitializeWorkerThreadIOChannel()...\n");
    InitializeWorkerThreadIOChannel(FS);

    //
    // Initialize Storage async, we are now using a global DPUStorage *, potentially cross thread
    //
    //
    Sto = BackEndStorage();

    ErrorCodeT result = Initialize(Sto, FS->MasterSPDKContext);
    if (result != DDS_ERROR_CODE_SUCCESS){
        fprintf(stderr, "InitStorage failed with %d\n", result);
        
        //
        // FS app stop
        //
        //
        exit(-1);
        return;
    }

    //
    // `Initialize` will be completed async, returning here doesn't mean it's actually started, but assume it's ok.
    //
    //
    DebugPrint("File service started\n");
}

//
// On the top level, this will call spdk_app_start(), supplying the func `StartSPDKFileService`
// which will initialize threads and storage
//
//
void *
StartFileServiceWrapper(
    void *Ctx
) {
    printf("StartFileServiceWrapper() running...\n");
    struct StartFileServiceCtx *StartCtx = Ctx;

    int argc = StartCtx->Argc;
    char **argv = StartCtx->Argv;

    int rc;

    spdk_log_set_level(SPDK_LOG_LEVEL);

    struct spdk_app_opts opts = {};
    /* Set default values in opts structure. */
    spdk_app_opts_init(&opts, sizeof(opts));
    opts.name = "dds_bdev";
    
    //
    // Parse built-in SPDK command line parameters as well
    // as our custom one(s).
    //
    //
    if (
        (rc = spdk_app_parse_args(argc, argv, &opts, "b:", NULL, DDSParseArg, DDSCustomArgsUsage))
        != SPDK_APP_PARSE_ARGS_SUCCESS
    ) {
        printf("spdk_app_parse_args() failed with: %d\n", rc);
        exit(rc);
    }

    printf("starting FS, StartSPDKFileService()...\n");
    spdk_app_start(&opts, StartSPDKFileService, StartCtx);
    printf("spdk_app_start returns\n");
    return NULL;
}

//
// Start the file service (called by main thread), this will then call pthread_create.
//
//
void
StartFileService(
    int Argc,
    char **Argv,
    FileService *FS
) {
    G_INITIALIZATION_DONE = false;
    struct StartFileServiceCtx *StartCtx = malloc(sizeof(*StartCtx));
    StartCtx->Argc = Argc;
    StartCtx->Argv = Argv;
    StartCtx->FS = FS;
    
    StartFileServiceWrapper(StartCtx);
}

//
// Call this on app thread
//
//
void AppThreadExit(void *Ctx) {
    FileService *FS = Ctx;
    spdk_put_io_channel(FS->MasterSPDKContext->bdev_io_channel);
    spdk_bdev_close(FS->MasterSPDKContext->bdev_desc);
    
    //
    // will get "spdk_app_stop() called twice" if we call it here (after Ctrl-C)
    //
    //
    SPDK_NOTICELOG("App thread exited\n");
}

//
// Call on each of all worker threads
//
//
void WorkerThreadExit(void *Ctx) {
    struct WorkerThreadExitCtx *ExitCtx = Ctx;
    DataPlaneStagePollerCtx pollerCtx;
    pollerCtx.WorkerIndex = (uint32_t)ExitCtx->i;
    UnregisterDataPlaneStagePoller(&pollerCtx);
    spdk_put_io_channel(ExitCtx->Channel);
    spdk_thread_exit(spdk_get_thread());
    SPDK_NOTICELOG("Worker thread %d exited!\n", ExitCtx->i);
    free(Ctx);
}

//
// Stop the file service, called on agent thread; this will eventually make app thread call spdk_app_stop()
//
//
void
StopFileService(
    FileService* FS
) {
    //
    // clean up and free contexts, then worker threads exit
    //
    //
    printf("Worker thread count = %d\n", FS->WorkerThreadCount);
    for (size_t i = 0; i < FS->WorkerThreadCount; i++) {
        struct WorkerThreadExitCtx *ExitCtx = malloc(sizeof(*ExitCtx));
        ExitCtx->i = i;
        ExitCtx->Channel = FS->WorkerSPDKContexts[i].bdev_io_channel;
        spdk_thread_send_msg(FS->WorkerThreads[i], WorkerThreadExit, ExitCtx);
    }

    spdk_thread_send_msg(FS->AppThread, AppThreadExit, FS);

    //
    // TODO: maybe do sth more, maybe thread exit for app thread?
    //
    //
    DebugPrint("File service stopped\n");
}

//
// Stop, then Deallocate the file service object, called on agent thread
//
//
void
DeallocateFileService(
    FileService* FS
) {
    free(FS);
    DebugPrint("File service object deallocated\n");
}

//
// Send a control plane request to the file service, called from app thread
//
//
void
SubmitControlPlaneRequest(
    FileService* FS,
    ControlPlaneRequestContext* Context
) {
    //
    // TODO: thread selection?
    //
    //
    struct spdk_thread *worker = FS->WorkerThreads[WorkerId];
    Context->SPDKContext = &FS->WorkerSPDKContexts[WorkerId];

    int ret = spdk_thread_send_msg(worker, ControlPlaneHandler, Context);

    if (ret) {
        fprintf(stderr, "SubmitControlPlaneRequest() initial thread send msg failed with %d, retrying\n", ret);
        
        //
        // TODO: maybe limited retries?
        //
        //
        while (1) {
            ret = spdk_thread_send_msg(worker, ControlPlaneHandler, Context);
            if (ret == 0) {
                fprintf(stderr, "SubmitControlPlaneRequest() retry finished\n");
                return;
            }
        }
    }
}

#ifdef OPT_FILE_SERVICE_BATCHING
void
SubmitDataPlaneRequest(
    FileService* FS,
    DataPlaneRequestContext* ContextArray,
    RequestIdT Index,
    RequestIdT BatchSize,
    RequestIdT IoSlotBase
) {
    // Original fixed-owner selection retained for reference:
    // struct spdk_thread *worker = FS->WorkerThreads[WorkerId];
    // uint32_t ownerWorkerIndex = (uint32_t)WorkerId;
    //
    // Updated: distribute owner workers across all storage workers.
    uint32_t ownerWorkerIndex = SelectDataPlaneOwnerWorkerIndex(FS);
    struct spdk_thread *worker = FS->WorkerThreads[ownerWorkerIndex];
    if (worker == NULL)
    {
        fprintf(stderr, "%s [error]: selected owner worker %u is unavailable\n", __func__, (unsigned)ownerWorkerIndex);
        return;
    }
    if (BatchSize > DDS_MAX_OUTSTANDING_IO_PER_CHANNEL)
    {
        fprintf(stderr, "%s [warn]: batch size %u exceeds per-channel max outstanding %u, clamping\n", __func__,
                (unsigned)BatchSize, (unsigned)DDS_MAX_OUTSTANDING_IO_PER_CHANNEL);
        BatchSize = DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    }
    
    //
    // Original arithmetic batch indexing retained for reference:
    // SPDKContextT *SPDKContext = &FS->WorkerSPDKContexts[WorkerId];
    // struct PerSlotContext *headSlotContext = GetFreeSpace(SPDKContext, &ContextArray[Index - IoSlotBase], Index);
    // ...
    // for (RequestIdT i = 1; i < BatchSize; i++) {
    //     RequestIdT curIndex = (Index - IoSlotBase + i) % DDS_MAX_OUTSTANDING_IO;
    //     ...
    // }
    //
    // Updated: strict RequestId ownership model.
    // Follow the explicit batch chain built by ExecuteRequests (BatchNextRequestId), so each backend slot/context
    // is keyed by request id and we never alias contexts by opportunistic arithmetic wrapping.
    SPDKContextT *SPDKContext = &FS->WorkerSPDKContexts[ownerWorkerIndex];
    RequestIdT batchRequestIds[DDS_MAX_OUTSTANDING_IO_PER_CHANNEL];
    bool seenRequestId[DDS_MAX_OUTSTANDING_IO_PER_CHANNEL];
    memset(seenRequestId, 0, sizeof(seenRequestId));
    RequestIdT actualBatchSize = 0;
    RequestIdT curReqId = Index;
    int batchChannelIndex = -1;
    (void)IoSlotBase;

    while (actualBatchSize < BatchSize) {
        if (!DDS_ValidateRequestId(curReqId)) {
            fprintf(stderr,
                    "%s [error]: invalid request id in batch chain (start=%u cur=%u requestedBatch=%u actual=%u)\n",
                    __func__, (unsigned)Index, (unsigned)curReqId, (unsigned)BatchSize, (unsigned)actualBatchSize);
            break;
        }
        if (ContextArray[curReqId].Request == NULL || ContextArray[curReqId].Response == NULL) {
            fprintf(stderr,
                    "%s [error]: null request/response in batch chain (reqId=%u requestedBatch=%u actual=%u)\n",
                    __func__, (unsigned)curReqId, (unsigned)BatchSize, (unsigned)actualBatchSize);
            break;
        }
        if (seenRequestId[curReqId]) {
            fprintf(stderr,
                    "%s [error]: duplicate request id in batch chain (start=%u dup=%u requestedBatch=%u actual=%u)\n",
                    __func__, (unsigned)Index, (unsigned)curReqId, (unsigned)BatchSize, (unsigned)actualBatchSize);
            break;
        }
        seenRequestId[curReqId] = true;

        int reqChannelIndex = (int)ContextArray[curReqId].ChannelIndex;
        if (!DDS_ValidateChannelIndex(reqChannelIndex))
        {
            fprintf(stderr,
                    "%s [error]: invalid channel index in batch chain (start=%u reqId=%u channel=%d)\n",
                    __func__, (unsigned)Index, (unsigned)curReqId, reqChannelIndex);
            break;
        }
        if (batchChannelIndex < 0)
        {
            batchChannelIndex = reqChannelIndex;
        }
        else if (batchChannelIndex != reqChannelIndex)
        {
            fprintf(stderr,
                    "%s [error]: cross-channel batch chain detected (start=%u reqId=%u prevChannel=%d curChannel=%d)\n",
                    __func__, (unsigned)Index, (unsigned)curReqId, batchChannelIndex, reqChannelIndex);
            break;
        }

        batchRequestIds[actualBatchSize] = curReqId;
        actualBatchSize++;

        curReqId = ContextArray[curReqId].BatchNextRequestId;
        if (curReqId == DDS_REQUEST_INVALID) {
            break;
        }
    }

    if (actualBatchSize == 0) {
        fprintf(stderr, "%s [warn]: empty batch chain (start=%u requestedBatch=%u)\n", __func__, (unsigned)Index,
                (unsigned)BatchSize);
        return;
    }
    if (actualBatchSize != BatchSize) {
        fprintf(stderr, "%s [warn]: batch chain length mismatch (requested=%u actual=%u start=%u)\n", __func__,
                (unsigned)BatchSize, (unsigned)actualBatchSize, (unsigned)Index);
    }

    struct PerSlotContext *headSlotContext = NULL;
    for (RequestIdT i = 0; i < actualBatchSize; i++) {
        RequestIdT reqId = batchRequestIds[i];
        int reqChannelIndex = (int)ContextArray[reqId].ChannelIndex;
        uint32_t globalSlot = DDS_GlobalSlotFromChannelRequest(reqChannelIndex, reqId);
        if (globalSlot == UINT32_MAX)
        {
            fprintf(stderr, "%s [error]: invalid global slot mapping (reqId=%u channel=%d)\n", __func__,
                    (unsigned)reqId, reqChannelIndex);
            return;
        }
        struct PerSlotContext *curSlotContext = GetFreeSpace(SPDKContext, &ContextArray[reqId], globalSlot);
        if (curSlotContext == NULL) {
            fprintf(stderr, "%s [error]: GetFreeSpace failed for reqId=%u globalSlot=%u (batch start=%u i=%u)\n",
                    __func__, (unsigned)reqId, (unsigned)globalSlot, (unsigned)Index, (unsigned)i);
            return;
        }
        curSlotContext->SPDKContext = SPDKContext;
        curSlotContext->BatchSize = actualBatchSize;
        curSlotContext->IndexBase = (RequestIdT)globalSlot;
        InitializeSlotStageMetadata(curSlotContext, curSlotContext->Ctx, ownerWorkerIndex, worker);
        if (i == 0) {
            headSlotContext = curSlotContext;
        }
    }
    
    //
    // Send the head of batch RequestHandler, which will call ReadHandler and WriteHandler accordingly
    //
    //
    int ret = spdk_thread_send_msg(worker, DataPlaneRequestHandler, headSlotContext);
    if (ret) {
        fprintf(stderr, "SubmitDataPlaneRequest() initial thread send msg failed with %d, retrying\n", ret);
        while (1) {
            ret = spdk_thread_send_msg(worker, DataPlaneRequestHandler, headSlotContext);
            if (ret == 0) {
                fprintf(stderr, "SubmitDataPlaneRequest() read retry finished\n");
                return;
            }
        }
    }
}
#else
//
// Send a data plane request to the file service, called from app thread
//
//
void
SubmitDataPlaneRequest(
    FileService* FS,
    DataPlaneRequestContext* Context,
    bool IsRead,
    RequestIdT Index
) {
    // Original fixed-owner selection retained for reference:
    // struct spdk_thread *worker = FS->WorkerThreads[WorkerId];
    // uint32_t ownerWorkerIndex = (uint32_t)WorkerId;
    //
    // Updated: distribute owner workers across all storage workers.
    uint32_t ownerWorkerIndex = SelectDataPlaneOwnerWorkerIndex(FS);
    struct spdk_thread *worker = FS->WorkerThreads[ownerWorkerIndex];
    if (worker == NULL) {
        fprintf(stderr, "%s [error]: selected owner worker %u is unavailable\n", __func__,
                (unsigned)ownerWorkerIndex);
        return;
    }

    //
    // TODO: no need for thread spdk ctx?
    //
    //
    int channelIndex = (int)Context->ChannelIndex;
    uint32_t globalSlot = DDS_GlobalSlotFromChannelRequest(channelIndex, Index);
    if (globalSlot == UINT32_MAX)
    {
        fprintf(stderr, "%s [error]: invalid global slot mapping for request (reqId=%u channel=%d)\n", __func__,
                (unsigned)Index, channelIndex);
        return;
    }
    struct PerSlotContext *SlotContext = GetFreeSpace(&FS->WorkerSPDKContexts[ownerWorkerIndex], Context, globalSlot);
    if (SlotContext == NULL) {
        fprintf(stderr, "%s [error]: GetFreeSpace failed for reqId=%u globalSlot=%u\n", __func__, (unsigned)Index,
                (unsigned)globalSlot);
        return;
    }
    SlotContext->SPDKContext = &FS->WorkerSPDKContexts[ownerWorkerIndex];
    InitializeSlotStageMetadata(SlotContext, Context, ownerWorkerIndex, worker);
    
    //
    // TODO: limited retries?
    //
    //
    int ret;
    if (IsRead) {
        ret = spdk_thread_send_msg(worker, ReadHandler, SlotContext);
        
        if (ret) {
            fprintf(stderr, "SubmitDataPlaneRequest() initial thread send msg failed with %d, retrying\n", ret);
            while (1) {
                ret = spdk_thread_send_msg(worker, ReadHandler, SlotContext);
                if (ret == 0) {
                    fprintf(stderr, "SubmitDataPlaneRequest() read retry finished\n");
                    return;
                }
            }
        }
    }
    else {
        ret = spdk_thread_send_msg(worker, WriteHandler, SlotContext);

        if (ret) {
            fprintf(stderr, "SubmitDataPlaneRequest() initial thread send msg failed with %d, retrying\n", ret);
            while (1) {
                ret = spdk_thread_send_msg(worker, WriteHandler, SlotContext);
                if (ret == 0) {
                    fprintf(stderr, "SubmitDataPlaneRequest() write retry finished\n");
                    return;
                }
            }
        }
    }
}
#endif
