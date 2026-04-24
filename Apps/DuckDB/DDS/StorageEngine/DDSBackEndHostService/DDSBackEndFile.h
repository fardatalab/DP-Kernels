#pragma once

#include "DDSBackEnd.h"

namespace DDS_BackEnd {

class BackEndFile {
public:
    //
    // Properties that will be made persistent on disk
    // Note: Id must be the index
    //
    //
    typedef struct PersistentProperties {
        FileIdT Id;
        FileProperties FProperties;
        SegmentIdT Segments[DDS_BACKEND_MAX_SEGMENTS_PER_FILE];
    } PersistentPropertiesT;
    
private:
    PersistentPropertiesT Properties;
    SegmentIdT NumSegments;
    const SegmentSizeT AddressOnSegment;

public:
    BackEndFile();

    BackEndFile(
        FileIdT FileId,
        const char* FileName,
        FileAttributesT FileAttributes
    );

    const char*
    GetName();

    FileAttributesT
    GetAttributes();

    FileSizeT
    GetSize();

    SegmentSizeT
    GetAddressOnSegment();

    PersistentPropertiesT*
    GetProperties();

    SegmentIdT
    GetNumSegments();

    void
    SetName(
        const char* FileName
    );

    void
    SetSize(
        FileSizeT FileSize
    );

    //
    // Set number of segments based on the allocation
    //
    //
    void
    SetNumAllocatedSegments();
    
    //
    // Allocate a segment
    // Assuming boundries were taken care of when the function is invoked
    // Not thread-safe
    //
    //
    inline
    void
    AllocateSegment(
        SegmentIdT NewSegment
    );

    //
    // Deallocate a segment
    // Assuming boundries were taken care of when the function is invoked
    // Not thread-safe
    //
    //
    inline
    void
    DeallocateSegment();
};

}