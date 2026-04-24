#pragma once

#include <time.h>

#include "DDSTypes.h"
#include "Protocol.h"

namespace DDS_BackEnd {

//
// Properties of a file
// Differences from front end:
// 1. No file pointer
// 2. No access
// 3. No share mode
// These are not needed in back end
//
//
typedef struct FileProperties {
    FileAttributesT FileAttributes;
    time_t CreationTime;
    time_t LastAccessTime;
    time_t LastWriteTime;
    FileSizeT FileSize;
    char FileName[DDS_MAX_FILE_PATH];
} FilePropertiesT;

//
// Callbacks for async operations
//
//
typedef void (*ReadWriteCallback)(
    ErrorCodeT ErrorCode,
    FileSizeT BytesServiced,
    ContextT Context
);

}