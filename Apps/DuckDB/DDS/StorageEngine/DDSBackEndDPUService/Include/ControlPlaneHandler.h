#pragma once

#include "BackEndTypes.h"
#include "MsgTypes.h"

//
// Handler for a control plane request
//
//
void ControlPlaneHandler(
    void *ReqContext
);

typedef struct ControlPlaneHandlerContext {
    ErrorCodeT *Result;  // it's from the request context's response
    DirIdT DirId;
    struct DPUDir* dir;
    FileIdT FileId;
    struct DPUFile* File;
    struct DPUDir* NewDir; // only used in MoveFile() as NewDir pointer
    char* NewFileName; // only used in MoveFile() as new file name
    void *SPDKContext;
} ControlPlaneHandlerCtx;
