#pragma once

#include <stdint.h>

#include "DDSTypes.h"

#undef CreateDirectory
#undef RemoveDirectory
#undef CreateFile
#undef DeleteFile
#undef FindFirstFile
#undef FindNextFile
#undef GetFileAttributes
#undef GetCurrentDirectory
#undef MoveFile

namespace DDS_FrontEnd {

//
// Callbacks for async operations
//
//
typedef void (*ReadWriteCallback)(
    ErrorCodeT ErrorCode,
    FileIOSizeT BytesServiced,
    ContextT Context
);

// jason: Callback for read2 operations (exposes logical bytes (read) separately).
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
typedef void (*ReadWriteCallback2)(ErrorCodeT ErrorCode, FileIOSizeT BytesServiced, FileIOSizeT LogicalBytes,
                                   uint64_t OffloadReadTimeNs, uint64_t OffloadStage1TimeNs,
                                   uint64_t OffloadStage2TimeNs, ContextT Context);
#else
typedef void (*ReadWriteCallback2)(ErrorCodeT ErrorCode, FileIOSizeT BytesServiced, FileIOSizeT LogicalBytes,
                                   ContextT Context);
#endif

//
// The interface of file system control plane
//
//
class DDSFrontEndInterface
{
public:

    //
    // Intialize the front end, including connecting to back end
    // and setting up the root directory and default poll
    //
    //
    virtual
    ErrorCodeT
    Initialize() = 0;
    
    //
    // Create a diretory
    // 
    //
    virtual
    ErrorCodeT
    CreateDirectory(
        const char* PathName,
        DirIdT* DirId
    ) = 0;

    //
    // Delete a directory
    // 
    //
    virtual
    ErrorCodeT
    RemoveDirectory(
        const char* PathName
    ) = 0;

    //
    // Create a file
    // 
    //
    virtual
    ErrorCodeT
    CreateFile(
        const char* FileName,
        FileAccessT DesiredAccess,
        FileShareModeT ShareMode,
        FileAttributesT FileAttributes,
        FileIdT* FileId
    ) = 0;

    //
    // Delete a file
    // 
    //
    virtual
    ErrorCodeT
    DeleteFile(
        const char* FileName
    ) = 0;

    //
    // Change the size of a file
    // 
    //
    virtual
    ErrorCodeT
    ChangeFileSize(
        FileIdT FileId,
        FileSizeT NewSize
    ) = 0;
    
    //
    // Set the physical file size for the specified file
    // to the current position of the file pointer
    // 
    //
    virtual
    ErrorCodeT
    SetEndOfFile(
        FileIdT FileId
    ) = 0;

    //
    // Move the file pointer of the specified file
    // 
    //
    virtual
    ErrorCodeT
    SetFilePointer(
        FileIdT FileId,
        PositionInFileT DistanceToMove,
        FilePointerPosition MoveMethod
    ) = 0;

    //
    // Get file size
    // 
    //
    virtual
    ErrorCodeT
    GetFileSize(
        FileIdT FileId,
        FileSizeT* FileSize
    ) = 0;

    virtual
    ErrorCodeT
    SetRead2AesKey(
        const uint8_t* KeyBytes,
        uint8_t KeySizeBytes
    ) = 0;

    //
    // Async read from a file
    // 
    //
    virtual
    ErrorCodeT
    ReadFile(
        FileIdT FileId,
        BufferT DestBuffer,
        FileIOSizeT BytesToRead,
        FileIOSizeT* BytesRead,
        ReadWriteCallback Callback,
        ContextT Context
    ) = 0;

    //
    // Async read from a file with explicit stage sizes and stage input windows (read2)
    //
    virtual ErrorCodeT ReadFile2(FileIdT FileId, BufferT DestBuffer, FileIOSizeT LogicalBytesToRead,
                                 const FileIOSizeT *StageSizes, const FileIOSizeT *StageInputOffsets,
                                 const FileIOSizeT *StageInputLengths, uint16_t StageCount, FileIOSizeT *BytesRead,
                                 FileIOSizeT *LogicalBytesRead, ReadWriteCallback2 Callback, ContextT Context) = 0;

    //
    // Async read from a file with scattering
    // 
    //
    virtual
    ErrorCodeT
    ReadFileScatter(
        FileIdT FileId,
        BufferT* DestBufferArray,
        FileIOSizeT BytesToRead,
        FileIOSizeT* BytesRead,
        ReadWriteCallback Callback,
        ContextT Context
    ) = 0;

    //
    // Async write to a file
    // 
    //
    virtual
    ErrorCodeT
    WriteFile(
        FileIdT FileId,
        BufferT SourceBuffer,
        FileIOSizeT BytesToWrite,
        FileIOSizeT* BytesWritten,
        ReadWriteCallback Callback,
        ContextT Context
    ) = 0;

    //
    // Async write to a file with gathering
    // 
    //
    virtual
    ErrorCodeT
    WriteFileGather(
        FileIdT FileId,
        BufferT* SourceBufferArray,
        FileIOSizeT BytesToWrite,
        FileIOSizeT* BytesWritten,
        ReadWriteCallback Callback,
        ContextT Context
    ) = 0;

    //
    // Read/write with offset
    //
    //

    //
    // Async read from a file
    // 
    //
    virtual
    ErrorCodeT
    ReadFile(
        FileIdT FileId,
        BufferT DestBuffer,
        FileSizeT Offset,
        FileIOSizeT BytesToRead,
        FileIOSizeT* BytesRead,
        ReadWriteCallback Callback,
        ContextT Context
    ) = 0;

    //
    // Async read from a file with explicit stage sizes and stage input windows (read2)
    //
    virtual ErrorCodeT ReadFile2(FileIdT FileId, BufferT DestBuffer, FileSizeT Offset, FileIOSizeT LogicalBytesToRead,
                                 const FileIOSizeT *StageSizes, const FileIOSizeT *StageInputOffsets,
                                 const FileIOSizeT *StageInputLengths, uint16_t StageCount, FileIOSizeT *BytesRead,
                                 FileIOSizeT *LogicalBytesRead, ReadWriteCallback2 Callback, ContextT Context) = 0;

    //
    // Async read from a file with scattering
    // 
    //
    virtual
    ErrorCodeT
    ReadFileScatter(
        FileIdT FileId,
        BufferT* DestBufferArray,
        FileSizeT Offset,
        FileIOSizeT BytesToRead,
        FileIOSizeT* BytesRead,
        ReadWriteCallback Callback,
        ContextT Context
    ) = 0;

    //
    // Async write to a file
    // 
    //
    virtual
    ErrorCodeT
    WriteFile(
        FileIdT FileId,
        BufferT SourceBuffer,
        FileSizeT Offset,
        FileIOSizeT BytesToWrite,
        FileIOSizeT* BytesWritten,
        ReadWriteCallback Callback,
        ContextT Context
    ) = 0;

    //
    // Async write to a file with gathering
    // 
    //
    virtual
    ErrorCodeT
    WriteFileGather(
        FileIdT FileId,
        BufferT* SourceBufferArray,
        FileSizeT Offset,
        FileIOSizeT BytesToWrite,
        FileIOSizeT* BytesWritten,
        ReadWriteCallback Callback,
        ContextT Context
    ) = 0;

    //
    // Flush buffered data to storage
    // 
    //
    virtual
    ErrorCodeT
    FlushFileBuffers(
        FileIdT FileId
    ) = 0;

    //
    // Search a directory for a file or subdirectory
    // with a name that matches a specific name
    // (or partial name if wildcards are used)
    // 
    //
    virtual
    ErrorCodeT
    FindFirstFile(
        const char* FileName,
        FileIdT* FileId
    ) = 0;

    //
    // Continue a file search from a previous call
    // to the FindFirstFile
    // 
    //
    virtual
    ErrorCodeT
    FindNextFile(
        FileIdT LastFileId,
        FileIdT* FileId
    ) = 0;

    //
    // Get file properties by file id
    // 
    //
    virtual
    ErrorCodeT
    GetFileInformationById(
        FileIdT FileId,
        FilePropertiesT* FileProperties
    ) = 0;
    
    //
    // Get file attributes by file name
    // 
    //
    virtual
    ErrorCodeT
    GetFileAttributes(
        const char* FileName,
        FileAttributesT* FileAttributes
    ) = 0;

    //
    // Retrieve the current directory for the
    // current process
    // 
    //
    virtual
    ErrorCodeT
    GetCurrentDirectory(
        unsigned BufferLength,
        BufferT DirectoryBuffer
    ) = 0;

    //
    // Get the size of free space on the storage
    // 
    //
    virtual
    ErrorCodeT
    GetStorageFreeSpace(
        FileSizeT* StorageFreeSpace
    ) = 0;

    //
    // Lock the specified file for exclusive
    // access by the calling process
    // 
    //
    virtual
    ErrorCodeT
    LockFile(
        FileIdT FileId,
        PositionInFileT FileOffset,
        PositionInFileT NumberOfBytesToLock
    ) = 0;

    //
    // Unlock a region in an open file;
    // Unlocking a region enables other
    // processes to access the region
    // 
    //
    virtual
    ErrorCodeT
    UnlockFile(
        FileIdT FileId,
        PositionInFileT FileOffset,
        PositionInFileT NumberOfBytesToUnlock
    ) = 0;

    //
    // Move an existing file or a directory,
    // including its children
    // 
    //
    virtual
    ErrorCodeT
    MoveFile(
        const char* ExistingFileName,
        const char* NewFileName
    ) = 0;

    //
    // Get the default poll structure for async I/O
    //
    //
    virtual
    ErrorCodeT
    GetDefaultPoll(
        PollIdT* PollId
    ) = 0;

    //
    // Create a poll structure for async I/O
    //
    //
    virtual
    ErrorCodeT
    PollCreate(
        PollIdT* PollId
    ) = 0;

    //
    // Delete a poll structure
    //
    //
    virtual
    ErrorCodeT
    PollDelete(
        PollIdT PollId
    ) = 0;

    //
    // Add a file to a pool
    //
    //
    virtual
    ErrorCodeT
    PollAdd(
        FileIdT FileId,
        PollIdT PollId,
        ContextT FileContext
    ) = 0;

    //
    // Poll a completion event
    //
    //
    virtual
    ErrorCodeT
    PollWait(
        PollIdT PollId,
        FileIOSizeT* BytesServiced,
        ContextT* FileContext,
        ContextT* IOContext,
        size_t WaitTime,
        bool* PollResult
    ) = 0;
};

}
