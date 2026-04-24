#pragma once

#include "DDSFrontEndInterface.h"
#include "DDSFrontEndTypes.h"
#include "MsgTypes.h"
#include "Protocol.h"

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
// Connector that fowards requests to and receives responses from the back end
//
//
class DDSBackEndBridgeBase {
public:
    //
    // Connect to the backend
    //
    //
    virtual ErrorCodeT
    Connect() = 0;

    //
    // Disconnect from the backend
    //
    //
    virtual ErrorCodeT
    Disconnect() = 0;

	//
    // Create a diretory
    // 
    //
    virtual ErrorCodeT
    CreateDirectory(
        const char* PathName,
        DirIdT DirId,
        DirIdT ParentId
    ) = 0;

    //
    // Delete a directory
    //
    //
    virtual ErrorCodeT
    RemoveDirectory(
        DirIdT DirId
    ) = 0;

    //
    // Find First FIle
    // 
    //
    virtual ErrorCodeT
    FindFirstFile(
        const char* FileName,
        FileIdT* FileId
    ) = 0;

    //
    // Create a file
    // 
    //
    virtual ErrorCodeT
    CreateFile(
        const char* FileName,
        FileAttributesT FileAttributes,
        FileIdT FileId,
        DirIdT DirId
    ) = 0;

    //
    // Delete a file
    // 
    //
    virtual ErrorCodeT
    DeleteFile(
        FileIdT FileId,
        DirIdT DirId
    ) = 0;

    //
    // Change the size of a file
    // 
    //
    virtual ErrorCodeT
    ChangeFileSize(
        FileIdT FileId,
        FileSizeT NewSize
    ) = 0;

    //
    // Get file size
    // 
    //
    virtual ErrorCodeT
    GetFileSize(
        FileIdT FileId,
        FileSizeT* FileSize
    ) = 0;

    //
    // Get backend I/O alignment parameters
    //
    //
    virtual ErrorCodeT
    GetIoAlignment(
        FileIOSizeT* BlockSize,
        FileIOSizeT* BufAlign
    ) = 0;

    //
    // Configure read2 AES key bytes on backend DPK stage-1 decrypt.
    //
    virtual ErrorCodeT
    SetRead2AesKey(
        const uint8_t* KeyBytes,
        uint8_t KeySizeBytes
    ) = 0;

    //
    // Async read from a file
    // 
    //
    virtual ErrorCodeT
    ReadFile(
        FileIdT FileId,
        FileSizeT Offset,
        BufferT DestBuffer,
        FileIOSizeT BytesToRead,
        FileIOSizeT* BytesRead,
        ContextT Context,
        PollT* Poll
    ) = 0;

    //
    // Async read from a file with explicit stage sizes and stage input windows (read2)
    //
    virtual ErrorCodeT
    ReadFile2(
        FileIdT FileId,
        FileSizeT Offset,
        BufferT DestBuffer,
        FileIOSizeT LogicalBytesToRead,
        const FileIOSizeT* StageSizes,
        const FileIOSizeT* StageInputOffsets,
        const FileIOSizeT* StageInputLengths,
        uint16_t StageCount,
        FileIOSizeT* BytesRead,
        FileIOSizeT* LogicalBytesRead,
        ContextT Context,
        PollT* Poll
    ) = 0;

    //
    // Async read from a file with scattering
    // 
    //
    virtual ErrorCodeT
    ReadFileScatter(
        FileIdT FileId,
        FileSizeT Offset,
        BufferT* DestBufferArray,
        FileIOSizeT BytesToRead,
        FileIOSizeT* BytesRead,
        ContextT Context,
        PollT* Poll
    ) = 0;

    //
    // Async write to a file
    // 
    //
    virtual ErrorCodeT
    WriteFile(
        FileIdT FileId,
        FileSizeT Offset,
        BufferT SourceBuffer,
        FileIOSizeT BytesToWrite,
        FileIOSizeT* BytesWritten,
        ContextT Context,
        PollT* Poll
    ) = 0;

    //
    // Async write to a file with gathering
    // 
    //
    virtual ErrorCodeT
    WriteFileGather(
        FileIdT FileId,
        FileSizeT Offset,
        BufferT* SourceBufferArray,
        FileIOSizeT BytesToWrite,
        FileIOSizeT* BytesWritten,
        ContextT Context,
        PollT* Poll
    ) = 0;

    //
    // Get file properties by file id
    // 
    //
    virtual ErrorCodeT
    GetFileInformationById(
        FileIdT FileId,
        FilePropertiesT* FileProperties
    ) = 0;

    //
    // Get file attributes by file name
    // 
    //
    virtual ErrorCodeT
    GetFileAttributes(
        FileIdT FileId,
        FileAttributesT* FileAttributes
    ) = 0;

    //
    // Get the size of free space on the storage
    // 
    //
    virtual ErrorCodeT
    GetStorageFreeSpace(
        FileSizeT* StorageFreeSpace
    ) = 0;

    //
    // Move an existing file or a directory,
    // including its children
    // 
    //
    virtual ErrorCodeT
    MoveFile(
        FileIdT FileId,
        const char* NewFileName
    ) = 0;

    //
    // Retrieve a response
    // Task 2.2: added ChannelOut to identify which IO channel produced the response.
    //
    virtual ErrorCodeT GetResponse(PollT *Poll, size_t WaitTime, FileIOSizeT *BytesServiced, RequestIdT *ReqId,
                                   int *ChannelOut = nullptr) = 0;
};

}
