#pragma once

#include "MsgTypes.h"
#include "bdev.h"
#include "DPUBackEndStorage.h"
#include <stdbool.h>
#include <stdint.h>
#include "Zmalloc.h"

// Context payload used by FileService to register/unregister per-worker stage pollers.
typedef struct DataPlaneStagePollerCtx {
    uint32_t WorkerIndex;
} DataPlaneStagePollerCtx;

//
// To support batching request submission, instead of directly spdk send msg with R/W Handler, send msg with this,
// which will then call the corresponding RW Handler
//
//
void DataPlaneRequestHandler(
    void* Ctx
);

//
// Handler for a read request
//
//
void ReadHandler(
    void* Ctx
);

//
// Callback for ReadHandler async read
//
//
void ReadHandlerZCCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
);

void ReadHandlerNonZCCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
);

//
// Handler for a write request
//
//
void WriteHandler(
    void* Ctx
);

//
// Callback for WriteHandler async write
//
//
void WriteHandlerCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
);

//
// Register/unregister the per-worker poller used to complete async DPK stage submissions.
// These are invoked on worker threads via spdk_thread_send_msg from FileService.
//
void RegisterDataPlaneStagePoller(void *Ctx);
void UnregisterDataPlaneStagePoller(void *Ctx);
