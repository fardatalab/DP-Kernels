#include <string.h>

#include "DDSBackEndBridgeForLocalMemory.h"

namespace DDS_FrontEnd {

#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
//
// A callback for back end
//
//
void
BackEndReadWriteCallbackHandler(
    ErrorCodeT ErrorCode,
    FileIOSizeT BytesServiced,
    ContextT Context
) {
    FileIOT* pIO = (FileIOT*)Context;

    if (pIO->AppCallback) {
        pIO->AppCallback(
            ErrorCode,
            BytesServiced,
            pIO->Context
        );

        pIO->IsComplete = true;
    }
    else {
        pIO->IsReadyForPoll = true;
    }
}

DDSBackEndBridgeForLocalMemory::DDSBackEndBridgeForLocalMemory()
    : BackEnd(NULL) { }

//
// Connect to the backend
//
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::Connect() {
    if (!BackEnd) {
        BackEnd = new DDS_BackEnd::BackEndInMemory();
    }

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Disconnect from the backend
//
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::Disconnect() {
    if (BackEnd) {
        delete BackEnd;
        BackEnd = nullptr;
    }

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Create a diretory
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::CreateDirectory(
    const char* PathName,
    DirIdT DirId,
    DirIdT ParentId
) {
    return BackEnd->CreateDirectory(
        PathName,
        DirId,
        ParentId
    );
}

//
// Delete a directory
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::RemoveDirectory(
    DirIdT DirId
) {
    return BackEnd->RemoveDirectory(
        DirId
    );
}

//
// Create a file
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::CreateFile(
    const char* FileName,
    FileAttributesT FileAttributes,
    FileIdT FileId,
    DirIdT DirId
) {
    return BackEnd->CreateFile(
        FileName,
        FileAttributes,
        FileId,
        DirId
    );
}

//
// Delete a file
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::DeleteFile(
    FileIdT FileId,
    DirIdT DirId
) {
    return BackEnd->DeleteFile(
        FileId,
        DirId
    );
}

//
// Change the size of a file
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::ChangeFileSize(
    FileIdT FileId,
    FileSizeT NewSize
) {
    return BackEnd->ChangeFileSize(
        FileId,
        NewSize
    );
}

//
// Get file size
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::GetFileSize(
    FileIdT FileId,
    FileSizeT* FileSize
) {
    return BackEnd->GetFileSize(
        FileId,
        FileSize
    );
}

ErrorCodeT
DDSBackEndBridgeForLocalMemory::SetRead2AesKey(
    const uint8_t* KeyBytes,
    uint8_t KeySizeBytes
) {
    (void)KeyBytes;
    (void)KeySizeBytes;
    return DDS_ERROR_CODE_NOT_IMPLEMENTED;
}

//
// Async read from a file
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::ReadFile(
    FileIdT FileId,
    FileSizeT Offset,
    BufferT DestBuffer,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ContextT Context,
    PollT* Poll
) {
    return BackEnd->ReadFile(
        FileId,
        Offset,
        DestBuffer,
        BytesToRead,
        BytesRead,
        (DDS_BackEnd::ReadWriteCallback)BackEndReadWriteCallbackHandler,
        Context
    );
}

//
// Async read from a file with scattering
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::ReadFileScatter(
    FileIdT FileId,
    FileSizeT Offset,
    BufferT* DestBufferArray,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ContextT Context,
    PollT* Poll
) {
    return BackEnd->ReadFileScatter(
        FileId,
        Offset,
        DestBufferArray,
        BytesToRead,
        BytesRead,
        (DDS_BackEnd::ReadWriteCallback)BackEndReadWriteCallbackHandler,
        Context
    );
}

//
// Async write to a file
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::WriteFile(
    FileIdT FileId,
    FileSizeT Offset,
    BufferT SourceBuffer,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ContextT Context,
    PollT* Poll
) {
    return BackEnd->WriteFile(
        FileId,
        Offset,
        SourceBuffer,
        BytesToWrite,
        BytesWritten,
        (DDS_BackEnd::ReadWriteCallback)BackEndReadWriteCallbackHandler,
        Context
    );
}

//
// Async write to a file with gathering
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::WriteFileGather(
    FileIdT FileId,
    FileSizeT Offset,
    BufferT* SourceBufferArray,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ContextT Context,
    PollT* Poll
) {
    return BackEnd->WriteFileGather(
        FileId,
        Offset,
        SourceBufferArray,
        BytesToWrite,
        BytesWritten,
        (DDS_BackEnd::ReadWriteCallback)BackEndReadWriteCallbackHandler,
        Context
    );
}

//
// Get file properties by file id
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::GetFileInformationById(
    FileIdT FileId,
    FilePropertiesT* FileProperties
) {
    DDS_BackEnd::FilePropertiesT fileProperties;
    ErrorCodeT result = BackEnd->GetFileInformationById(
        FileId,
        &fileProperties
    );
    strcpy(FileProperties->FileName, fileProperties.FileName);
    FileProperties->FileSize = fileProperties.FileSize;
    FileProperties->FileAttributes = fileProperties.FileAttributes;
    FileProperties->CreationTime = fileProperties.CreationTime;
    FileProperties->LastAccessTime = fileProperties.LastAccessTime;
    FileProperties->LastWriteTime = fileProperties.LastWriteTime;

    return result;
}

//
// Get file attributes by file name
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::GetFileAttributes(
    FileIdT FileId,
    FileAttributesT* FileAttributes
) {
    return BackEnd->GetFileAttributes(
        FileId,
        FileAttributes
    );
}

//
// Get the size of free space on the storage
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::GetStorageFreeSpace(
    FileSizeT* StorageFreeSpace
) {
    return BackEnd->GetStorageFreeSpace(
        StorageFreeSpace
    );
}

//
// Move an existing file or a directory,
// including its children
// 
//
ErrorCodeT
DDSBackEndBridgeForLocalMemory::MoveFile(
    FileIdT FileId,
    const char* NewFileName
) {
    return BackEnd->MoveFile(FileId, NewFileName);
}

//
// Retrieve a response
// Task 2.2: ChannelOut always 0 for local memory (no DPU channels).
//
ErrorCodeT GetResponse(PollT *Poll, size_t WaitTime, FileIOSizeT *BytesServiced, RequestIdT *ReqId, int *ChannelOut)
{
    if (ChannelOut)
        *ChannelOut = 0;
}

#endif

}
