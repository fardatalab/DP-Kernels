#include <string.h>

#include "DDSBackEndFile.h"

namespace DDS_BackEnd {

BackEndFile::BackEndFile() :
    AddressOnSegment(0) {
    this->Properties.Id = DDS_FILE_INVALID;
    this->Properties.FProperties.FileAttributes = 0;
    this->Properties.FProperties.FileSize = 0;

    for (size_t s = 0; s != DDS_BACKEND_MAX_SEGMENTS_PER_FILE; s++) {
        this->Properties.Segments[s] = DDS_BACKEND_SEGMENT_INVALID;
    }

    this->NumSegments = 0;
}

BackEndFile::BackEndFile(
    FileIdT FileId,
    const char* FileName,
    FileAttributesT FileAttributes
) :
    AddressOnSegment(DDS_BACKEND_SEGMENT_SIZE - FileId * sizeof(PersistentPropertiesT)) {
    this->Properties.Id = FileId;
    strcpy(this->Properties.FProperties.FileName, FileName);
    this->Properties.FProperties.FileAttributes = FileAttributes;
    this->Properties.FProperties.FileSize = 0;

    for (size_t s = 0; s != DDS_BACKEND_MAX_SEGMENTS_PER_FILE; s++) {
        this->Properties.Segments[s] = DDS_BACKEND_SEGMENT_INVALID;
    }

    this->NumSegments = 0;
}

const char*
BackEndFile::GetName() {
	return this->Properties.FProperties.FileName;
}

FileAttributesT
BackEndFile::GetAttributes() {
    return this->Properties.FProperties.FileAttributes;
}

FileSizeT
BackEndFile::GetSize() {
    return this->Properties.FProperties.FileSize;
}

SegmentSizeT
BackEndFile::GetAddressOnSegment() {
    return this->AddressOnSegment;
}

BackEndFile::PersistentPropertiesT*
BackEndFile::GetProperties() {
    return &this->Properties;
}

SegmentIdT
BackEndFile::GetNumSegments() {
    return this->NumSegments;
}

void
BackEndFile::SetName(
    const char* FileName
) {
    strcpy(this->Properties.FProperties.FileName, FileName);
}

void
BackEndFile::SetSize(FileSizeT FileSize) {
    this->Properties.FProperties.FileSize = FileSize;
}

//
// Set number of segments based on the allocation
//
//
void
BackEndFile::SetNumAllocatedSegments() {
    this->NumSegments = 0;
    for (size_t s = 0; s != DDS_BACKEND_MAX_SEGMENTS_PER_FILE; s++) {
        if (this->Properties.Segments[s] == DDS_BACKEND_SEGMENT_INVALID) {
            break;
        }
        this->NumSegments++;
    }
}

//
// Allocate a segment
// Assuming boundries were taken care of when the function is invoked
// Not thread-safe
//
//
inline
void
BackEndFile::AllocateSegment(
    SegmentIdT NewSegment
) {
    this->Properties.Segments[this->NumSegments] = NewSegment;
    this->NumSegments++;
}

//
// Deallocate a segment
// Assuming boundries were taken care of when the function is invoked
// Not thread-safe
//
//
inline
void
BackEndFile::DeallocateSegment() {
    this->NumSegments--;
    this->Properties.Segments[this->NumSegments] = DDS_BACKEND_SEGMENT_INVALID;
}

}