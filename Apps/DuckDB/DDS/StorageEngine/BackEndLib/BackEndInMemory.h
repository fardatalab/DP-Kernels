#pragma once

#include <time.h>

#include "BackEndDir.h"
#include "BackEndFile.h"

#define ONE_GB 1073741824ULL
#define DDS_BACKEND_CAPACITY ONE_GB * 1

namespace DDS_BackEnd {

class BackEndInMemory {
private:
    const size_t Capacity = DDS_BACKEND_CAPACITY;
    BufferT Data;
    BackEndDir* AllDirs[DDS_MAX_DIRS];
    BackEndFile* AllFiles[DDS_MAX_FILES];

private:
    void
    RemoveDirectoryRecursive(
        BackEndDir* Dir
    );

public:
    BackEndInMemory();

    ~BackEndInMemory();

    //
    // Create a diretory
    // Assuming id and parent id have been computed by host
    // 
    //
    ErrorCodeT
    CreateDirectory(
        const char* PathName,
        DirIdT DirId,
        DirIdT ParentId
    );

    //
    // Delete a directory
    // 
    //
    ErrorCodeT
    RemoveDirectory(
        DirIdT DirId
    );

    //
    // Create a file
    // 
    //
    ErrorCodeT
    CreateFile(
        const char* FileName,
        FileAttributesT FileAttributes,
        FileIdT FileId,
        DirIdT DirId
    );

    //
    // Delete a file
    // 
    //
    ErrorCodeT
    DeleteFile(
        FileIdT FileId,
        DirIdT DirId
    );

    //
    // Change the size of a file
    // 
    //
    ErrorCodeT
    ChangeFileSize(
        FileIdT FileId,
        FileSizeT NewSize
    );

    //
    // Get file size
    // 
    //
    ErrorCodeT
    GetFileSize(
        FileIdT FileId,
        FileSizeT* FileSize
    );

    //
    // Async read from a file
    // 
    //
    ErrorCodeT
    ReadFile(
        FileIdT FileId,
        FileSizeT Offset,
        BufferT DestBuffer,
        FileIOSizeT BytesToRead,
        FileIOSizeT* BytesRead,
        ReadWriteCallback Callback,
        ContextT Context
    );

    //
    // Async read from a file with scattering
    // 
    //
    ErrorCodeT
    ReadFileScatter(
        FileIdT FileId,
        FileSizeT Offset,
        BufferT* DestBufferArray,
        FileIOSizeT BytesToRead,
        FileIOSizeT* BytesRead,
        ReadWriteCallback Callback,
        ContextT Context
    );

    //
    // Async write to a file
    // 
    //
    ErrorCodeT
    WriteFile(
        FileIdT FileId,
        FileSizeT Offset,
        BufferT SourceBuffer,
        FileIOSizeT BytesToWrite,
        FileIOSizeT* BytesWritten,
        ReadWriteCallback Callback,
        ContextT Context
    );

    //
    // Async write to a file with gathering
    // 
    //
    ErrorCodeT
    WriteFileGather(
        FileIdT FileId,
        FileSizeT Offset,
        BufferT* SourceBufferArray,
        FileIOSizeT BytesToWrite,
        FileIOSizeT* BytesWritten,
        ReadWriteCallback Callback,
        ContextT Context
    );

    //
    // Get file properties by file id
    // 
    //
    ErrorCodeT
    GetFileInformationById(
        FileIdT FileId,
        FilePropertiesT* FileProperties
    );

    //
    // Get file attributes by file name
    // 
    //
    ErrorCodeT
    GetFileAttributes(
        FileIdT FileId,
        FileAttributesT* FileAttributes
    );

    //
    // Get the size of free space on the storage
    // 
    //
    ErrorCodeT
    GetStorageFreeSpace(
        FileSizeT* StorageFreeSpace
    );

    //
    // Move an existing file or a directory,
    // including its children
    // 
    //
    ErrorCodeT
    MoveFile(
        FileIdT FileId,
        const char* NewFileName
    );
};

}