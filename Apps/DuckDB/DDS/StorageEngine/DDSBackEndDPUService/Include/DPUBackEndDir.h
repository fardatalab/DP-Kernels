#pragma once

#include <pthread.h>
#include <stdbool.h>

#include "DPUBackEnd.h"

// using Mutex = std::mutex;


//
// Persistent properties of DPU dir
//
//
typedef struct DPUDirProperties {
    DirIdT Id;
    DirIdT Parent;
    FileNumberT NumFiles;
    char Name[DDS_MAX_FILE_PATH];
    FileIdT Files[DDS_MAX_FILES_PER_DIR];
    // Align on-disk metadata to sector size to satisfy SPDK alignment.
    char __pad[488];
} DPUDirPropertiesT;
// Ensure directory metadata slots are sector-aligned for reserved segment I/O.
AssertStaticDDSTypes((sizeof(DPUDirPropertiesT) % DDS_BACKEND_SECTOR_SIZE) == 0, 101);

//
// DPU dir, highly similar with DDSBackEndDir
// since C doesn't have package mutex, I used pthread_mutex_t to replace it
//
//
struct DPUDir {
    DPUDirPropertiesT Properties;
    pthread_mutex_t ModificationMutex;
    //remove const before SegmentSizeT
    SegmentSizeT AddressOnSegment;
};
//
// Since C doesn't support overload, I separate these two constructors by 
// BackEndDirX (without input) and BackEndDirI (with input)
//
//
struct DPUDir* BackEndDirX();

//
// BackEndDir constructor with input
//
//
struct DPUDir* BackEndDirI(
    DirIdT Id,
    DirIdT Parent,
    const char* Name
);

SegmentSizeT GetDirAddressOnSegment(struct DPUDir* Dir);

DPUDirPropertiesT* GetDirProperties(struct DPUDir* Dir);

//
// Add a file
//
//
ErrorCodeT AddFile(
    FileIdT FileId,
    struct DPUDir* Dir
);

//
// Delete a file
//
//
ErrorCodeT DeleteFile(
    FileIdT FileId,
    struct DPUDir* Dir
);

//
// Lock the directory
//
//
void Lock(struct DPUDir* Dir);

//
// Unlock the directory
//
//
void Unlock(struct DPUDir* Dir);
