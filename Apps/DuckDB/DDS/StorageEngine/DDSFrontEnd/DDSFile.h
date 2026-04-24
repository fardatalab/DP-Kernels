#pragma once

#include <atomic>

#include "DDSFrontEndInterface.h"

namespace DDS_FrontEnd {

class DDSFile {
private:
    FileIdT Id;
    FileProperties Properties;
    // Track how many POSIX FDs refer to this file for safe DeleteFile behavior.
    std::atomic<uint32_t> OpenCount;

public:
    PollIdT PollId;
    ContextT PollContext;

public:
    DDSFile();

    DDSFile(
        FileIdT FileId,
        const char* FileName,
        FileAttributesT FileAttributes,
        FileAccessT FileAccess,
        FileShareModeT FileShareMode
    );

    const char*
    GetName();

    FileAttributesT
    GetAttributes();

    FileAccessT
    GetAccess();

    FileSizeT
    GetSize();

    FileSizeT
    GetPointer();

    FileShareModeT
    GetShareMode();

    time_t
    GetCreationTime();

    time_t
    GetLastAccessTime();

    time_t
    GetLastWriteTime();

    void
    SetName(
        const char* FileName
    );

    void
    SetSize(
        FileSizeT FileSize
    );

    void
    SetPointer(
        FileSizeT FilePointer
    );

    void
    IncrementPointer(
        FileSizeT Delta
    );

    void
    DecrementPointer(
        FileSizeT Delta
    );

    void
    SetLastAccessTime(
        time_t NewTime
    );

    void
    SetLastWriteTime(
        time_t NewTime
    );

    /**
     * Get the current open handle count for this file.
     */
    uint32_t
    GetOpenCount();

    /**
     * Increment the open handle count for this file.
     */
    void
    IncrementOpenCount();

    /**
     * Decrement the open handle count for this file.
     */
    void
    DecrementOpenCount();
};

}
