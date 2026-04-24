#pragma once
#include <time.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "spdk/stdinc.h"
#include "spdk/thread.h"
#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/bdev_zone.h"

#include "BackEndControl.h"
#include "DDSTypes.h"

#define ONE_KB 1024ULL
#define ONE_MB 1048576ULL
#define ONE_GB 1073741824ULL
#define DDS_BACKEND_RESERVED_SEGMENT 0
#define DDS_BACKEND_PAGE_SIZE DDS_PAGE_SIZE
#define DDS_BACKEND_SEGMENT_SIZE 134217728ULL // ==128MB  //ONE_GB

#ifndef TESTING_FS
#define DDS_BACKEND_CAPACITY (ONE_GB * 130)
#else
#define DDS_BACKEND_CAPACITY (ONE_GB * 9)
#endif

#define DDS_BACKEND_QUEUE_DEPTH_PAGE_IO_DEFAULT 1024
#define DDS_BACKEND_ROOT_DIR_NAME ""
#define DDS_BACKEND_INITIALIZATION_MARK "Initialized by DDS"
#define DDS_BACKEND_INITIALIZATION_MARK_LENGTH 19
#define DDS_BACKEND_MAX_SEGMENTS_PER_FILE 1000
#define DDS_BACKEND_SEGMENT_INVALID -1
#define DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE 32 * ONE_MB

#define min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

#define min3(x,y,z) \
    ({ __typeof__ (x) _x = (x); \
       __typeof__ (y) _y = (y); \
       __typeof__ (z) _z = (z); \
     min(min(x,y),z); })

//
// Types used in DPU
//
//
typedef unsigned long long DiskSizeT;
typedef int SegmentIdT;
typedef unsigned int SegmentSizeT;
typedef unsigned int FileNumberT;

//
// DPU segment, same with Segement in DDSBackEnd.h
//
//
typedef struct DPUSegment {
    SegmentIdT Id;
    FileIdT FileId;
    FileSizeT DiskAddress;
    bool Allocatable;
} SegmentT;

//
// DPU storage, highly similar with BackEndStorage
//
//
struct DPUStorage {
    SegmentT* AllSegments;
    SegmentIdT TotalSegments;
    SegmentIdT AvailableSegments;
    
    struct DPUDir* AllDirs[DDS_MAX_DIRS];
    struct DPUFile* AllFiles[DDS_MAX_FILES];
    int TotalDirs;
    int TotalFiles;

    pthread_mutex_t SectorModificationMutex;
    pthread_mutex_t SegmentAllocationMutex;
    
    //
    // these are used during `Initialize`, move them to their own context
    //
    //

    // atomic_size_t TargetProgress;
    // atomic_size_t CurrentProgress;
};

//
// DPU Storage var should be a global singleton
//
//
extern struct DPUStorage *Sto;

extern bool G_INITIALIZATION_DONE;  // will be set to true when reserved seg is initialized
