#pragma once

#include <mutex>

#include "DDSBackEnd.h"

namespace DDS_BackEnd {

using Mutex = std::mutex;

//
// Flat directories that have only two layers: root and its children
//
//
class BackEndDir {
public:
    //
    // Properties that will be made persistent on disk
    // Note: Id must be the index
    //
    //
    typedef struct PersistentProperties {
        DirIdT Id;
        DirIdT Parent;
        FileNumberT NumFiles;
        char Name[DDS_MAX_FILE_PATH];
        FileIdT Files[DDS_MAX_FILES_PER_DIR];
    } PersistentPropertiesT;

private:
    PersistentPropertiesT Properties;
    Mutex ModificationMutex;
    const SegmentSizeT AddressOnSegment;

public:
    BackEndDir();

    BackEndDir(
        DirIdT Id,
        DirIdT Parent,
        const char* Name
    );

    SegmentSizeT
    GetAddressOnSegment();

    PersistentPropertiesT*
    GetProperties();

    //
    // Add a file
    //
    //
    ErrorCodeT
    AddFile(
        FileIdT FileId
    );

    //
    // Delete a file
    //
    //
    ErrorCodeT
    DeleteFile(
        FileIdT FileId
    );

    //
    // Lock the directory
    //
    //
    inline
    void
    Lock();

    //
    // Unlock the directory
    //
    //
    inline
    void
    Unlock();
};

}