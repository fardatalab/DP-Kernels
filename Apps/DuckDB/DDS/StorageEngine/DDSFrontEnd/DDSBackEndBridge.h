#pragma once

#include "DDSBackEndBridgeBase.h"
// #include "RDMC.h"

#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <mutex>


/* Newly Added */
#define RESOLVE_TIMEOUT_MS 2000
#define INLINE_THREASHOLD 256
#define INFINITE ((unsigned long)-1)


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
class DDSBackEndBridge : public DDSBackEndBridgeBase {
private:
    // RDMA connection resources
    void cleanup();
    std::mutex ctrl_mutex;


    // Helper function
    ErrorCodeT resolveBackendAddress();

public:



    //
    // Back end configuration
    //
    //
    char BackEndAddr[16];
    unsigned short BackEndPort;
    struct sockaddr_in BackEndSock;

    //
    // RNIC configuration
    //
    //
    // IND2Adapter* Adapter;
    // HANDLE AdapterFileHandle;
    // ND2_ADAPTER_INFO AdapterInfo;
    // OVERLAPPED Ov;
    size_t QueueDepth;
    size_t MaxSge;
    size_t InlineThreshold;
    
    struct sockaddr_in LocalSock;

    // IND2Connector* CtrlConnector;
    // IND2CompletionQueue* CtrlCompQ;
    // IND2QueuePair* CtrlQPair;
    // IND2MemoryRegion* CtrlMemRegion;
    // ND2_SGE* CtrlSgl;

    struct rdma_event_channel *ec;
    struct rdma_cm_id *cm_id;
    struct ibv_cq *cq;
    struct ibv_mr *mr;  
    char CtrlMsgBuf[CTRL_MSG_SIZE];
    int ClientId;

public:
    DDSBackEndBridge();

    //
    // Connect to the backend
    //
    //
    ErrorCodeT
    Connect();

    //
    // Disconnect from the backend
    //
    //
    ErrorCodeT
    Disconnect();

	//
    // Create a diretory
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
    // Find First File
    //
    //
    ErrorCodeT
    FindFirstFile(
        const char* FileName,
        FileIdT* FileId
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
    // Get backend I/O alignment parameters
    //
    //
    ErrorCodeT
    GetIoAlignment(
        FileIOSizeT* BlockSize,
        FileIOSizeT* BufAlign
    );

    ErrorCodeT
    SetRead2AesKey(
        const uint8_t* KeyBytes,
        uint8_t KeySizeBytes
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
        ContextT Context,
        PollT* Poll
    );

    //
    // Async read from a file with explicit stage sizes and stage input windows (read2)
    //
    ErrorCodeT
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
        ContextT Context,
        PollT* Poll
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
        ContextT Context,
        PollT* Poll
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
        ContextT Context,
        PollT* Poll
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

    //
    // Retrieve a response from the response ring
    // Task 2.2: added ChannelOut for per-channel completion routing.
    //
    ErrorCodeT GetResponse(PollT *Poll, size_t WaitTime, FileIOSizeT *BytesServiced, RequestIdT *ReqId,
                           int *ChannelOut = nullptr);

#ifdef RING_BUFFER_RESPONSE_BATCH_ENABLED
    //
    // Retrieve a response from the cached batch (channel-aware).
    // chIdx selects which channel's batch state and ring to process.
    //
    ErrorCodeT GetResponseFromCachedBatch(PollT *Poll, FileIOSizeT *BytesServiced, RequestIdT *ReqId,
                                          int *ChannelOut = nullptr, int chIdx = 0);
#endif
};

}
