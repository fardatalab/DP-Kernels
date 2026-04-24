#pragma once

#include "DPUBackEnd.h"
#include <stdbool.h>

//
// Persistent properties of DPU file
//
// NOTE: Front-end specific fields (access/share/position) are intentionally
// excluded; DPU persists backend-owned metadata only.
//
typedef struct DPUFilePropertiesCore
{
    FileAttributesT FileAttributes;
    time_t CreationTime;
    time_t LastAccessTime;
    time_t LastWriteTime;
    FileSizeT FileSize;
    char FileName[DDS_MAX_FILE_PATH];
} DPUFilePropertiesCoreT;

//
// Persistent properties of DPU file (with segment map)
//
//
typedef struct DPUFileProperties {
        FileIdT Id;
        // FilePropertiesT FProperties;
        DPUFilePropertiesCoreT FProperties;
        SegmentIdT Segments[DDS_BACKEND_MAX_SEGMENTS_PER_FILE];
        // char __pad[472];
        // Align on-disk metadata to sector size to satisfy SPDK alignment.
        char __pad[496];
} DPUFilePropertiesT;

// Ensure file metadata slots are sector-aligned for reserved segment I/O.
AssertStaticDDSTypes((sizeof(DPUFilePropertiesT) % DDS_BACKEND_SECTOR_SIZE) == 0, 100);

//
// DPU file, highly similar with DDSBackEndFile
//
//
struct DPUFile {
    DPUFilePropertiesT Properties;
    SegmentIdT NumSegments;
    //remove const before SegmentIdT
    SegmentSizeT AddressOnSegment;
};

//
// BackEndFile constructor without input
//
//
struct DPUFile* BackEndFileX();

//
// BackEndFile constructor with input
//
//
struct DPUFile* BackEndFileI(
    FileIdT FileId,
    const char* FileName,
    FileAttributesT FileAttributes
);

const char* GetName(struct DPUFile* File);

FileAttributesT GetAttributes(struct DPUFile* File);

FileSizeT GetSize(struct DPUFile* File);

SegmentSizeT GetFileAddressOnSegment(struct DPUFile* File);

DPUFilePropertiesT* GetFileProperties(struct DPUFile* File);

SegmentIdT GetNumSegments(struct DPUFile* File);

void SetName(
    const char* FileName,
    struct DPUFile* File
);

void SetSize(
    FileSizeT FileSize,
    struct DPUFile* File
);

//
// Set number of segments based on the allocation
//
//
void SetNumAllocatedSegments(struct DPUFile* File);

//
// Allocate a segment
// Assuming boundries were taken care of when the function is invoked
// Not thread-safe
//
//
void AllocateSegment(
    SegmentIdT NewSegment,
    struct DPUFile* File
);

//
// Deallocate a segment
// Assuming boundries were taken care of when the function is invoked
// Not thread-safe
//
//
void DeallocateSegment(struct DPUFile* File);
