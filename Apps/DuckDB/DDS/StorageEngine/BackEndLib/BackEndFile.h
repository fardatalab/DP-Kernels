#pragma once

#include "BackEnd.h"

namespace DDS_BackEnd {

class BackEndFile {
private:
    FileIdT Id;
    FileProperties Properties;
    size_t StartAddressOnStorage;

public:
    BackEndFile();

    BackEndFile(
        FileIdT FileId,
        const char* FileName,
        FileAttributesT FileAttributes,
        size_t StartAddressOnStorage
    );

    const char*
    GetName();

    FileAttributesT
    GetAttributes();

    FileSizeT
    GetSize();

    time_t
    GetCreationTime();

    time_t
    GetLastAccessTime();

    time_t
    GetLastWriteTime();

    FileSizeT
    GetStartAddressOnStorage();

    void
    SetName(
        const char* FileName
    );

    void
    SetSize(
        FileSizeT FileSize
    );

    void
    SetLastAccessTime(
        time_t NewTime
    );

    void
    SetLastWriteTime(
        time_t NewTime
    );
};

}