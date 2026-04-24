#pragma once

#include <atomic>
#include <mutex>
#include <time.h>

#include "DDSBackEndDir.h"
#include "DDSBackEndFile.h"

namespace DDS_BackEnd {

template<class T>
using Atomic = std::atomic<T>;

using Mutex = std::mutex;

//
// Context for each I/O operation in the back end
//
//
typedef struct BackEndIOContext {
    ContextT FrontEndBufferContext;
    ContextT BackEndBufferContext;
    FileIOSizeT BytesIssued;
    Atomic<FileIOSizeT> BytesCompleted;
    Atomic<bool> IsAvailable;
    ErrorCodeT ErrorCode;
    bool IsRead;
} BackEndIOContextT;

//
// Callback for async disk operations
//
//
typedef void (*DiskIOCallback)(
    bool Success,
    ContextT Context
);

class BackEndStorage {
private:
    SegmentT* AllSegments;
    const SegmentIdT TotalSegments;
    SegmentIdT AvailableSegments;
    
    BackEndDir* AllDirs[DDS_MAX_DIRS];
    BackEndFile* AllFiles[DDS_MAX_FILES];
    int TotalDirs;
    int TotalFiles;

    Mutex SectorModificationMutex;
    Mutex SegmentAllocationMutex;
    
    Atomic<size_t> TargetProgress;
    Atomic<size_t> CurrentProgress;

private:
    //
    // Read from disk synchronously
    //
    //
    ErrorCodeT
    ReadFromDiskSync(
        BufferT DstBuffer,
        SegmentIdT SegmentId,
        SegmentSizeT SegmentOffset,
        FileIOSizeT Bytes
    );

    //
    // Write to disk synchronously
    //
    //
    ErrorCodeT
    WriteToDiskSync(
        BufferT SrcBuffer,
        SegmentIdT SegmentId,
        SegmentSizeT SegmentOffset,
        FileIOSizeT Bytes
    );

    //
    // Read from disk asynchronously
    //
    //
    ErrorCodeT
    ReadFromDiskAsync(
        BufferT DstBuffer,
        SegmentIdT SegmentId,
        SegmentSizeT SegmentOffset,
        FileIOSizeT Bytes,
        DiskIOCallback Callback,
        ContextT Context
    );

    //
    // Write to disk asynchronously
    //
    //
    ErrorCodeT
    WriteToDiskAsync(
        BufferT SrcBuffer,
        SegmentIdT SegmentId,
        SegmentSizeT SegmentOffset,
        FileIOSizeT Bytes,
        DiskIOCallback Callback,
        ContextT Context
    );

    //
    // Retrieve all segments on the disk
    //
    //
    ErrorCodeT
    RetrieveSegments();

    //
    // Return all segments back to the disk
    //
    //
    void
    ReturnSegments();

    //
    // Load all directories and files from the reserved segment
    //
    //
    ErrorCodeT
    LoadDirectoriesAndFiles();

    //
    // Synchronize a directory to the disk
    //
    //
    inline
    ErrorCodeT
    SyncDirToDisk(BackEndDir* Dir);

    //
    // Synchronize a file to the disk
    //
    //
    inline
    ErrorCodeT
    SyncFileToDisk(BackEndFile* File);

    //
    // Synchronize the first sector on the reserved segment
    //
    //
    inline
    ErrorCodeT
    SyncReservedInformationToDisk();

public:
    BackEndStorage();

    ~BackEndStorage();
    
    //
    // Initialize the backend service
    //
    //
    ErrorCodeT
    Initialize();

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
        DiskIOCallback Callback,
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
        DiskIOCallback Callback,
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
        DirIdT OldDirId,
        DirIdT NewDirId,
        const char* NewFileName
    );
};

}