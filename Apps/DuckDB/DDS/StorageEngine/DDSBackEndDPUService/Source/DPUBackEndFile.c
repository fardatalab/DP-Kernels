#include <string.h>
#include <stdlib.h>

#include "../Include/DPUBackEndFile.h"

/**
 * Allocate a DPUFile with backend-owned metadata initialized to defaults.
 */
struct DPUFile* BackEndFileX(){
    struct DPUFile *tmp;
    tmp = malloc(sizeof(struct DPUFile));
    tmp->Properties.Id = DDS_FILE_INVALID;
    tmp->Properties.FProperties.FileAttributes = 0;
    tmp->Properties.FProperties.FileSize = 0;
    // Initialize backend-owned timestamps/name
    tmp->Properties.FProperties.CreationTime = 0;
    tmp->Properties.FProperties.LastAccessTime = 0;
    tmp->Properties.FProperties.LastWriteTime = 0;
    tmp->Properties.FProperties.FileName[0] = '\0';

    for (size_t s = 0; s != DDS_BACKEND_MAX_SEGMENTS_PER_FILE; s++) {
        tmp->Properties.Segments[s] = DDS_BACKEND_SEGMENT_INVALID;
    }

    tmp->NumSegments = 0;
    tmp->AddressOnSegment = 0;
    return tmp;
}

/**
 * Allocate a DPUFile with the provided ID/name/attributes and default metadata.
 * AddressOnSegment uses a sector-aligned slot and 1-based indexing from segment end.
 */
struct DPUFile* BackEndFileI(
    FileIdT FileId,
    const char* FileName,
    FileAttributesT FileAttributes
){
    struct DPUFile *tmp;
    tmp = malloc(sizeof(struct DPUFile));
    tmp->Properties.Id = FileId;
    strcpy(tmp->Properties.FProperties.FileName, FileName);
    tmp->Properties.FProperties.FileAttributes = FileAttributes;
    tmp->Properties.FProperties.FileSize = 0;
    // jason: Reserved access/share are removed; DPU persists backend-only fields.
    // tmp->Properties.FProperties.ReservedAccess = 0;
    // tmp->Properties.FProperties.ReservedShareMode = 0;
    tmp->Properties.FProperties.CreationTime = 0;
    tmp->Properties.FProperties.LastAccessTime = 0;
    tmp->Properties.FProperties.LastWriteTime = 0;

    for (size_t s = 0; s != DDS_BACKEND_MAX_SEGMENTS_PER_FILE; s++) {
        tmp->Properties.Segments[s] = DDS_BACKEND_SEGMENT_INVALID;
    }

    tmp->NumSegments = 0;
    // tmp->AddressOnSegment = DDS_BACKEND_SEGMENT_SIZE - FileId * sizeof(DPUFilePropertiesT);
    // jason: use 1-based slot indexing to keep metadata inside the reserved segment.
    tmp->AddressOnSegment = DDS_BACKEND_SEGMENT_SIZE - ((FileId + 1) * sizeof(DPUFilePropertiesT));
    return tmp;
}

const char* GetName(
    struct DPUFile* File
){
    return File->Properties.FProperties.FileName;
}

FileAttributesT GetAttributes(
    struct DPUFile* File
){
    return File->Properties.FProperties.FileAttributes;
}

FileSizeT GetSize(
    struct DPUFile* File
){
    return File->Properties.FProperties.FileSize;
}

SegmentSizeT GetFileAddressOnSegment(
    struct DPUFile* File
){
    return File->AddressOnSegment;
}

DPUFilePropertiesT* GetFileProperties(
    struct DPUFile* File
){
    return &File->Properties;
}

SegmentIdT GetNumSegments(
    struct DPUFile* File
){
    return File->NumSegments;
}

void SetName(
    const char* FileName,
    struct DPUFile* File
){
    strcpy(File->Properties.FProperties.FileName, FileName);
}

void SetSize(
    FileSizeT FileSize,
    struct DPUFile* File
){
    File->Properties.FProperties.FileSize = FileSize;
}

void SetNumAllocatedSegments(
    struct DPUFile* File
){
    File->NumSegments = 0;
    for (size_t s = 0; s != DDS_BACKEND_MAX_SEGMENTS_PER_FILE; s++) {
        if (File->Properties.Segments[s] == DDS_BACKEND_SEGMENT_INVALID) {
            break;
        }
        File->NumSegments++;
    }
}

void AllocateSegment(
    SegmentIdT NewSegment,
    struct DPUFile* File
){
    File->Properties.Segments[File->NumSegments] = NewSegment;
    File->NumSegments++;
}

void DeallocateSegment(
    struct DPUFile* File
){
    File->NumSegments--;
    File->Properties.Segments[File->NumSegments] = DDS_BACKEND_SEGMENT_INVALID;
}
