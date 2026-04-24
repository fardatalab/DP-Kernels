#pragma once

#include <time.h>

#define DDS_MAX_DIRS 1000
#define DDS_MAX_FILE_PATH 64
#define DDS_MAX_FILES 1000
#define DDS_MAX_FILES_PER_DIR 1000
#define DDS_DIR_INVALID -1
#define DDS_DIR_ROOT 0
#define DDS_FILE_INVALID -1
#define DDS_PAGE_SIZE 4096

#define DDS_ERROR_CODE_SUCCESS 0
#define DDS_ERROR_CODE_TOO_MANY_DIRS 1
#define DDS_ERROR_CODE_TOO_MANY_FILES 2
#define DDS_ERROR_CODE_DIR_EXISTS 3
#define DDS_ERROR_CODE_DIR_NOT_FOUND 4
#define DDS_ERROR_CODE_DIR_NOT_EMPTY 5
#define DDS_ERROR_CODE_FILE_EXISTS 6
#define DDS_ERROR_CODE_FILE_NOT_FOUND 7
#define DDS_ERROR_CODE_READ_OVERFLOW 8
#define DDS_ERROR_CODE_WRITE_OVERFLOW 9
#define DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE 16
#define DDS_ERROR_CODE_INVALID_FILE_POSITION 17
#define DDS_ERROR_CODE_SEGMENT_RETRIEVAL_FAILURE 18
#define DDS_ERROR_CODE_RESERVED_SEGMENT_ERROR 19
#define DDS_ERROR_CODE_OUT_OF_MEMORY 20

#define ONE_KB 1024ULL
#define ONE_MB 1048576ULL
#define ONE_GB 1073741824ULL
#define DDS_BACKEND_RESERVED_SEGMENT 0
#define DDS_BACKEND_SECTOR_SIZE 512
#define DDS_BACKEND_PAGE_SIZE DDS_PAGE_SIZE
#define DDS_BACKEND_SEGMENT_SIZE ONE_GB
#define DDS_BACKEND_CAPACITY (ONE_GB * 130)
#define DDS_BACKEND_QUEUE_DEPTH_PAGE_IO_DEFAULT 1024
#define DDS_BACKEND_ROOT_DIR_NAME ""
#define DDS_BACKEND_INITIALIZATION_MARK "Initialized by DDS"
#define DDS_BACKEND_INITIALIZATION_MARK_LENGTH 19
#define DDS_BACKEND_MAX_SEGMENTS_PER_FILE 1000
#define DDS_BACKEND_SEGMENT_INVALID -1

namespace DDS_BackEnd {

//
// Types used in DDS
//
//
typedef char* BufferT;
typedef void* ContextT;
typedef int DirIdT;
typedef int ErrorCodeT;
typedef unsigned long FileAttributesT;
typedef int FileIdT;
typedef unsigned long FileIOSizeT;
typedef unsigned long long FileSizeT;
typedef unsigned long long DiskSizeT;
typedef int SegmentIdT;
typedef unsigned int SegmentSizeT;
typedef unsigned int FileNumberT;

//
// Properties of a file
// Differences from front end:
// 1. No file pointer
// 2. No access
// 3. No share mode
// These are not needed in back end
// We further removed timestamps to simplify the implementaiton
//
//
typedef struct FileProperties {
    FileAttributesT FileAttributes;
    FileSizeT FileSize;
    char FileName[DDS_MAX_FILE_PATH];
} FilePropertiesT;

//
// A segment on disk
//
//
typedef struct Segement {
    SegmentIdT Id;
    FileIdT FileId;
    FileSizeT DiskAddress;
    bool Allocatable;
} SegmentT;

}