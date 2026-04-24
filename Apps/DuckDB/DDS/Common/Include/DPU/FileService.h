#pragma once

#include "BackEndControl.h"
#include "BackEndTypes.h"
#include "Protocol.h"
#include "Zmalloc.h"
#include "DataPlaneHandlers.h"

// #define WORKER_THREAD_COUNT 1
// Original fixed worker count retained for reference:
// #define WORKER_THREAD_COUNT 2
// jason: keep worker-thread count in sync with DDS_DPU_IO_PARALLELISM so stage scheduling,
// pre-prepare, and DPK submit-thread assumptions stay aligned.
#define WORKER_THREAD_COUNT DDS_DPU_IO_PARALLELISM

//
// File service running on the DPU
//
//
typedef struct {
    //
    // Spawned worker threads, each will have its corresponding SPDKContext,
    // this should be initialized at startup and remain unchanged
    //
    //
    struct spdk_thread **WorkerThreads;  // array of thread ptrs
    SPDKContextT *WorkerSPDKContexts;  // array of ctxs per thread
    int WorkerThreadCount;

    struct spdk_thread *AppThread;
    SPDKContextT *MasterSPDKContext;
} FileService;

extern FileService* FS;


//
// Allocate the file service object
//
//
FileService*
AllocateFileService();


//
// the context/params to start the file service
//
//
struct StartFileServiceCtx {
    int Argc;
    char **Argv;
    FileService *FS;
};

//
// Start the file service 
//
//
void
StartFileService(
    int Argc,
    char **Argv,
    FileService *FS
);

struct WorkerThreadExitCtx {
    struct spdk_io_channel *Channel;
    int i;
};

//
// Stop the file service
//
//
void
StopFileService(
    FileService* FS
);

//
// Deallocate the file service object
//
//
void
DeallocateFileService(
    FileService* FS
);

//
// Send a control plane request to the file service 
//
//
void
SubmitControlPlaneRequest(
    FileService* FS,
    ControlPlaneRequestContext* Context
);

//
// Send a data plane request to the file service
//
//
#ifdef OPT_FILE_SERVICE_BATCHING
// Index is the per-channel RequestId head. Channel identity is carried in
// DataPlaneRequestContext::ChannelIndex and used to compute the global slot.
void
SubmitDataPlaneRequest(
    FileService* FS,
    DataPlaneRequestContext* Context,
    RequestIdT Index,
    RequestIdT BatchSize,
    RequestIdT IoSlotBase
);
#else
// Index is the per-channel RequestId. Channel identity is carried in
// DataPlaneRequestContext::ChannelIndex and used to compute the global slot.
void
SubmitDataPlaneRequest(
    FileService* FS,
    DataPlaneRequestContext* Context,
    bool IsRead,
    RequestIdT Index
);
#endif
