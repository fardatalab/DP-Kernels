#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string.h>
#include <thread>

#include "DDSFrontEnd.h"

using std::cout;
using std::endl;

namespace DDS_FrontEnd {

/**
 * Validate read2 stage descriptors.
 *
 * Stage window contract:
 * - stage0 input window is relative to logical read bytes
 * - stageN input window is relative to stage(N-1) output bytes
 *
 * Current implementation supports one or two post-read stages.
 */
static inline bool ValidateRead2StageSpec(const FileIOSizeT* StageSizes, const FileIOSizeT* StageInputOffsets,
                                          const FileIOSizeT* StageInputLengths, uint16_t StageCount,
                                          FileIOSizeT LogicalBytesToRead) {
    if (!StageSizes || !StageInputOffsets || !StageInputLengths || StageCount < 1 || StageCount > 2) {
        return false;
    }
    FileIOSizeT sourceBytes = LogicalBytesToRead;
    for (uint16_t stageIndex = 0; stageIndex < StageCount; stageIndex++) {
        FileIOSizeT inputOffset = StageInputOffsets[stageIndex];
        FileIOSizeT inputLength = StageInputLengths[stageIndex];
        if (inputOffset > sourceBytes || inputLength > sourceBytes || inputLength > sourceBytes - inputOffset) {
            return false;
        }
        sourceBytes = StageSizes[stageIndex];
    }
    return true;
}

//
// Initialize a poll structure
//
//
PollT::PollT() {
    for (size_t i = 0; i != DDS_MAX_OUTSTANDING_IO; i++) {
        OutstandingRequests[i] = nullptr;
    }
    // PollChannelT default constructors handle per-channel field init.
#if BACKEND_TYPE == BACKEND_TYPE_DPU
    // Original flat-field init removed; fields now live in Channels[].
    // MsgBuffer = NULL;
    // RequestRing = NULL;
    // ResponseRing = NULL;
    // PendingNotifications = 0;
    for (int ch = 0; ch < DDS_NUM_IO_CHANNELS; ch++)
    {
        Channels[ch].ChannelIndex = ch;
    }
#endif
}

#if BACKEND_TYPE == BACKEND_TYPE_DPU
//
// Set up the DMA buffer
//
//
ErrorCodeT PollT::SetUpDMABuffer(void* BackEnd) {
    DDSBackEndBridge* backEndDPU = (DDSBackEndBridge*)BackEnd;
    printf("Setting up DMA Buffer...with ClientID %d\n", backEndDPU->ClientId);

    // Task 1.2: allocate a DMA buffer per channel.
    // For now all channels share the same backend/port/size; Task 3.x may
    // differentiate per-channel ring sizes.
    for (int ch = 0; ch < DDS_NUM_IO_CHANNELS; ch++)
    {
        Channels[ch].MsgBuffer =
            new DMABuffer(DDS_BACKEND_ADDR, DDS_BACKEND_PORT, DDS_REQUEST_RING_BYTES + DDS_RESPONSE_RING_BYTES + 1024,
                          backEndDPU->ClientId,
                          ch); // Pass logical channel index for backend registration
        if (!Channels[ch].MsgBuffer)
        {
            cout << __func__ << " [error]: Failed to allocate DMABuffer for channel " << ch << endl;
            // Tear down any channels already allocated.
            for (int j = 0; j < ch; j++)
            {
                Channels[j].MsgBuffer->Release();
                delete Channels[j].MsgBuffer;
                Channels[j].MsgBuffer = NULL;
            }
            return DDS_ERROR_CODE_OOM;
        }

        bool allocResult =
            Channels[ch].MsgBuffer->Allocate(&backEndDPU->LocalSock, &backEndDPU->BackEndSock, backEndDPU->QueueDepth,
                                             backEndDPU->MaxSge, backEndDPU->InlineThreshold);
        if (!allocResult)
        {
            cout << __func__ << " [error]: Failed to allocate DMA buffer for channel " << ch << endl;
            delete Channels[ch].MsgBuffer;
            Channels[ch].MsgBuffer = NULL;
            // Tear down any channels already allocated.
            for (int j = 0; j < ch; j++)
            {
                Channels[j].MsgBuffer->Release();
                delete Channels[j].MsgBuffer;
                Channels[j].MsgBuffer = NULL;
            }
            return DDS_ERROR_CODE_FAILED_BUFFER_ALLOCATION;
        }
        fprintf(stdout, "%s [info]: DMA buffer allocated for channel %d\n", __func__, ch);
    }

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Destroy the DMA buffer
//
//
void PollT::DestroyDMABuffer() {
    // Task 1.2: tear down per-channel DMA buffers.
    for (int ch = 0; ch < DDS_NUM_IO_CHANNELS; ch++)
    {
        if (Channels[ch].MsgBuffer)
        {
            Channels[ch].MsgBuffer->Release();
            delete Channels[ch].MsgBuffer;
            Channels[ch].MsgBuffer = NULL;
        }
    }
}

//
// Initialize request and response rings
//
//
void PollT::InitializeRings() {
    // Original: allocate rings without an explicit reset.
    // RequestRing = AllocateRequestBufferProgressive(MsgBuffer->BufferAddress);
    // ResponseRing = AllocateResponseBufferProgressive(RequestRing->Buffer + DDS_REQUEST_RING_BYTES);
    // jason: allocate and then reset ring state so reconnects re-establish the phase/alignment invariants.
    // Task 1.2: initialize rings per channel from each channel's own DMA buffer.
    for (int ch = 0; ch < DDS_NUM_IO_CHANNELS; ch++)
    {
        Channels[ch].RequestRing = AllocateRequestBufferProgressive(Channels[ch].MsgBuffer->BufferAddress);
        Channels[ch].ResponseRing =
            AllocateResponseBufferProgressive(Channels[ch].RequestRing->Buffer + DDS_REQUEST_RING_BYTES);
        ResetRequestRingBufferProgressive(Channels[ch].RequestRing);
        ResetResponseRingBufferProgressive(Channels[ch].ResponseRing);
        Channels[ch].PublishedTail.store(Channels[ch].ResponseRing->Tail[0], std::memory_order_relaxed);
        fprintf(stdout, "%s [info]: channel %d: request ring base=%p, response ring base=%p\n", __func__, ch,
                (void *)Channels[ch].RequestRing->Buffer, (void *)Channels[ch].ResponseRing->Buffer);
    }
}
#endif
    
//
// A dummy callback for app
//
//
void
DummyReadWriteCallback(
    ErrorCodeT ErrorCode,
    FileIOSizeT BytesServiced,
    ContextT Context
) { }

DDSFrontEnd::DDSFrontEnd(
    const char* StoreName
) : BackEnd(NULL) {
    //
    // Set the name of the store
    //
    //
    strcpy(this->StoreName, StoreName);

    //
    // Prepare directories, files, and polls
    //
    //
    for (size_t d = 0; d != DDS_MAX_DIRS; d++) {
        AllDirs[d] = nullptr;
    }
    DirIdEnd = 0;

    for (size_t f = 0; f != DDS_MAX_FILES; f++) {
        AllFiles[f] = nullptr;
    }
    FileIdEnd = 0;

    for (size_t p = 0; p != DDS_MAX_POLLS; p++) {
        AllPolls[p] = nullptr;
    }
    PollIdEnd = 0;
    // jason: default alignment to 1 until fetched from backend.
    IoBlockSize = 1;
    IoBufAlign = 1;

    //
    // Cannot allocate heap here otherwise SQL server would crash
    //
}

DDSFrontEnd::~DDSFrontEnd() {
    for (size_t d = 0; d != DDS_MAX_DIRS; d++) {
        if (AllDirs[d] != nullptr) {
            delete AllDirs[d];
        }
    }

    for (size_t f = 0; f != DDS_MAX_FILES; f++) {
        if (AllFiles[f] != nullptr) {
            delete AllFiles[f];
        }
    }

    for (size_t p = 0; p != DDS_MAX_POLLS; p++) {
        if (AllPolls[p]) {
#if BACKEND_TYPE == BACKEND_TYPE_DPU
            AllPolls[p]->DestroyDMABuffer();
            // Task 2.1: clean up per-channel FileIOT objects.
            for (int ch = 0; ch < DDS_NUM_IO_CHANNELS; ch++)
            {
                for (size_t i = 0; i < DDS_MAX_OUTSTANDING_IO_PER_CHANNEL; i++)
                {
                    delete AllPolls[p]->Channels[ch].OutstandingRequests[i];
                }
            }
#endif
            for (size_t i = 0; i != DDS_MAX_OUTSTANDING_IO; i++) {
                delete AllPolls[p]->OutstandingRequests[i];
            }

            delete AllPolls[p];
        }
    }

    if (BackEnd) {
        BackEnd->Disconnect();
        delete BackEnd;
    }
}

/**
 * Compute aligned offset/length and copy start for backend-aligned reads.
 *
 * Uses backend block size alignment to derive the aligned range and the
 * copy-start (prefix bytes to skip in the aligned payload).
 */
//
// Compute aligned offset/length and copy-start for backend-aligned reads.
//
void
DDSFrontEnd::ComputeReadAlignment(
    FileSizeT Offset,
    FileIOSizeT Bytes,
    FileSizeT* AlignedOffset,
    FileIOSizeT* AlignedBytes,
    FileIOSizeT* CopyStart
) {
    FileSizeT blockSize = (IoBlockSize == 0) ? 1 : (FileSizeT)IoBlockSize;
    FileSizeT alignedOffset = Offset - (Offset % blockSize);
    FileSizeT endOffset = Offset + (FileSizeT)Bytes;
    FileSizeT alignedEnd = ((endOffset + blockSize - 1) / blockSize) * blockSize;
    FileSizeT alignedSize = alignedEnd - alignedOffset;
    if (AlignedOffset) {
        *AlignedOffset = alignedOffset;
    }
    if (AlignedBytes) {
        *AlignedBytes = (FileIOSizeT)alignedSize;
    }
    if (CopyStart) {
        *CopyStart = (FileIOSizeT)(Offset - alignedOffset);
    }
}

//
// Intialize the front end, including connecting to back end
// and setting up the root directory and default poll
// Updated: fetch backend I/O alignment and configure response payload parsing.
//
//
ErrorCodeT
DDSFrontEnd::Initialize() {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
        
    //
    // Set up back end
    //
    //
#if BACKEND_TYPE == BACKEND_TYPE_DPU
    BackEnd = new DDSBackEndBridge();
#elif BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
    BackEnd = new DDSBackEndBridgeForLocalMemory();
#else
#error "Unknown backend type"
#endif
    result = BackEnd->Connect();
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << __func__ << " [error]: Failed to connect to the back end (" << result << ")" << endl;
        return result;
    }

    //
    // Fetch backend I/O alignment parameters for read alignment and payload parsing.
    //
    //
    FileIOSizeT blockSize = 1;
    FileIOSizeT bufAlign = 1;
    result = BackEnd->GetIoAlignment(&blockSize, &bufAlign);
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << __func__ << " [warn]: Failed to fetch I/O alignment (" << result << ")" << endl;
    } else {
        IoBlockSize = blockSize == 0 ? 1 : blockSize;
        IoBufAlign = bufAlign == 0 ? 1 : bufAlign;
        SetResponsePayloadAlignment(IoBufAlign);
    }

    //
    // Set up root directory
    //
    //
    DDSDir* rootDir = new DDSDir(DDS_DIR_ROOT, DDS_DIR_INVALID, "/");
    AllDirs[DDS_DIR_ROOT] = rootDir;

    //
    // Rehydrate persisted backend files into the front-end table.
    // This avoids stale/empty AllFiles[] after a front-end restart.
    //
    result = RehydrateFileTableFromBackEnd();
    if (result != DDS_ERROR_CODE_SUCCESS)
    {
        return result;
    }

    //
    // Set up default poll
    //
    //

    // Task 1.3: Validate dual-channel outstanding capacity at startup (frontend).
#if BACKEND_TYPE == BACKEND_TYPE_DPU
    {
        const int totalSlots = DDS_MAX_OUTSTANDING_IO_TOTAL;
        fprintf(stdout, "[capacity] frontend: DDS_NUM_IO_CHANNELS=%d  per_ch=%d  total=%d  legacy=%d\n",
                DDS_NUM_IO_CHANNELS, DDS_MAX_OUTSTANDING_IO_PER_CHANNEL, totalSlots, DDS_MAX_OUTSTANDING_IO);
        if (DDS_NUM_IO_CHANNELS < 1 || DDS_NUM_IO_CHANNELS > 4)
        {
            fprintf(stderr, "[capacity] ERROR: DDS_NUM_IO_CHANNELS=%d out of range\n", DDS_NUM_IO_CHANNELS);
            return DDS_ERROR_CODE_INVALID_PARAM;
        }
        if (DDS_MAX_OUTSTANDING_IO_PER_CHANNEL < 1 || DDS_MAX_OUTSTANDING_IO_TOTAL < DDS_MAX_OUTSTANDING_IO_PER_CHANNEL)
        {
            fprintf(stderr, "[capacity] ERROR: per-channel limit %d invalid (total=%d)\n",
                    DDS_MAX_OUTSTANDING_IO_PER_CHANNEL, DDS_MAX_OUTSTANDING_IO_TOTAL);
            return DDS_ERROR_CODE_INVALID_PARAM;
        }
        fprintf(stdout, "[capacity] frontend dual-channel capacity validation passed\n");
    }
#endif

    PollT* poll = new PollT();
    // Legacy flat array: still allocated for LOCAL_MEMORY compatibility.
    for (size_t i = 0; i != DDS_MAX_OUTSTANDING_IO; i++) {
        poll->OutstandingRequests[i] = new FileIOT();
        if (!poll->OutstandingRequests[i]) {
            fprintf(stderr, "%s [error]: Failed to allocate a file I/O object\n", __func__);
            for (size_t j = 0; j != i; j++) {
                delete poll->OutstandingRequests[j];
            }
            return DDS_ERROR_CODE_OOM;
        }

        poll->OutstandingRequests[i]->RequestId = (RequestIdT)i;
    }
    poll->NextRequestSlot = 0;

#if BACKEND_TYPE == BACKEND_TYPE_DPU
    // Task 2.1: Allocate per-channel FileIOT objects for DPU submission/completion.
    // Each channel has DDS_MAX_OUTSTANDING_IO_PER_CHANNEL slots with per-channel RequestId 0..N-1.
    for (int ch = 0; ch < DDS_NUM_IO_CHANNELS; ch++)
    {
        for (size_t i = 0; i < DDS_MAX_OUTSTANDING_IO_PER_CHANNEL; i++)
        {
            poll->Channels[ch].OutstandingRequests[i] = new FileIOT();
            if (!poll->Channels[ch].OutstandingRequests[i])
            {
                fprintf(stderr, "%s [error]: Failed to allocate FileIOT for channel %d slot %zu\n", __func__, ch, i);
                // Cleanup already-allocated per-channel slots.
                for (int cj = 0; cj <= ch; cj++)
                {
                    size_t limit = (cj < ch) ? DDS_MAX_OUTSTANDING_IO_PER_CHANNEL : i;
                    for (size_t j = 0; j < limit; j++)
                    {
                        delete poll->Channels[cj].OutstandingRequests[j];
                        poll->Channels[cj].OutstandingRequests[j] = nullptr;
                    }
                }
                // Also clean up flat array.
                for (size_t j = 0; j < DDS_MAX_OUTSTANDING_IO; j++)
                {
                    delete poll->OutstandingRequests[j];
                }
                delete poll;
                return DDS_ERROR_CODE_OOM;
            }
            // Per-channel RequestId: 0 to DDS_MAX_OUTSTANDING_IO_PER_CHANNEL-1.
            poll->Channels[ch].OutstandingRequests[i]->RequestId = (RequestIdT)i;
        }
        poll->Channels[ch].NextRequestSlot = 0;
        poll->Channels[ch].ChannelIndex = ch;
        fprintf(stdout, "%s [info]: allocated %d FileIOT slots for channel %d\n", __func__,
                DDS_MAX_OUTSTANDING_IO_PER_CHANNEL, ch);
    }
#endif

    AllPolls[DDS_POLL_DEFAULT] = poll;
#if BACKEND_TYPE == BACKEND_TYPE_DPU
    DDSBackEndBridge* backEndDPU = (DDSBackEndBridge*)BackEnd;
    poll->SetUpDMABuffer(backEndDPU);
    poll->InitializeRings();
    // Ring base addresses are now printed inside InitializeRings() per channel.
#endif

    return result;
}

//
// Create a diretory
// 
//
ErrorCodeT
DDSFrontEnd::CreateDirectory(
    const char* PathName,
    DirIdT* DirId
) {
    //
    // Check existance of the path
    //
    //
    bool exists = false;
    for (size_t i = 0; i != DirIdEnd; i++) {
        if (AllDirs[i] && strcmp(PathName, AllDirs[i]->GetName()) == 0) {
            exists = true;
            break;
        }
    }
    if (exists) {
        return DDS_ERROR_CODE_DIR_EXISTS;
    }

    //
    // Retrieve an id
    // TODO: concurrent support
    //
    //
    DirIdT id = 0;
    for (; id != DDS_MAX_DIRS; id++) {
        if (!AllDirs[id]) {
            break;
        }
    }

    if (id == DDS_MAX_DIRS) {
        return DDS_ERROR_CODE_TOO_MANY_DIRS;
    }

    //
    // TODO: find the parent directory
    //
    //
    DirIdT parentId = DDS_DIR_ROOT;

    DDSDir* dir = new DDSDir(id, parentId, PathName);
    AllDirs[parentId]->AddChild(id);

    AllDirs[id] = dir;
    if (id >= DirIdEnd) {
        //
        // End is the next to the last valid directory ID
        //
        //
        DirIdEnd = id + 1;
    }

    *DirId = id;

    //
    // Reflect the update on back end
    //
    //
    return BackEnd->CreateDirectory(PathName, *DirId, parentId);
}

void
DDSFrontEnd::RemoveDirectoryRecursive(
    DDSDir* Dir
) {
    LinkedList<DirIdT>* pChildren = Dir->GetChildren();
    for (auto cur = pChildren->Begin(); cur != pChildren->End(); cur = cur->Next) {
        RemoveDirectoryRecursive(AllDirs[cur->Value]);
        delete AllDirs[cur->Value];
        AllDirs[cur->Value] = nullptr;
    }
    pChildren->DeleteAll();

    LinkedList<FileIdT>* pFiles = Dir->GetFiles();
    for (auto cur = pFiles->Begin(); cur != pFiles->End(); cur = cur->Next) {
        delete AllFiles[cur->Value];
        AllFiles[cur->Value] = nullptr;
    }
    pFiles->DeleteAll();
}

/**
 * Rehydrate front-end file table from backend metadata.
 *
 * This clears any existing AllFiles[] entries and rebuilds them by querying
 * the backend for each possible FileId. It is intended to run once during
 * initialization when the backend persists file metadata across front-end
 * restarts.
 *
 * This uses PopulateFrontendFileFromBackend to share population logic with
 * FindFirstFile.
 */
ErrorCodeT DDSFrontEnd::RehydrateFileTableFromBackEnd()
{
#if BACKEND_TYPE != BACKEND_TYPE_DPU
    // Local memory backend does not persist across restarts; no rehydration.
    return DDS_ERROR_CODE_SUCCESS;
#else
    // Clear existing front-end file table to avoid stale metadata.
    for (FileIdT id = 0; id != DDS_MAX_FILES; id++)
    {
        if (AllFiles[id])
        {
            delete AllFiles[id];
            AllFiles[id] = nullptr;
        }
    }
    FileIdEnd = 0;

    // Rebuild file table by probing backend metadata per FileId.
    for (FileIdT id = 0; id != DDS_MAX_FILES; id++)
    {
        ErrorCodeT result = PopulateFrontendFileFromBackend(id);
        if (result == DDS_ERROR_CODE_FILE_NOT_FOUND)
        {
            continue;
        }
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            printf("%s [critical]: Failed to get file information for FileId %u from backend (%hu)\n", __func__, id,
                   result);
            return result;
        }
    }

    return DDS_ERROR_CODE_SUCCESS;
#endif
}

/**
 * Populate a single AllFiles[] entry from backend metadata.
 *
 * This avoids duplicating the front-end population logic across call sites.
 */
ErrorCodeT DDSFrontEnd::PopulateFrontendFileFromBackend(FileIdT FileId)
{
    if (FileId >= DDS_MAX_FILES)
    {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    if (AllFiles[FileId])
    {
        return DDS_ERROR_CODE_SUCCESS;
    }

    FilePropertiesT props;
    memset(&props, 0, sizeof(props));

    ErrorCodeT result = BackEnd->GetFileInformationById(FileId, &props);
    if (result != DDS_ERROR_CODE_SUCCESS)
    {
        return result;
    }

    // Access/share are front-end-only; default to 0 for populated entries.
    DDSFile *file = new DDSFile(FileId, props.FileName, props.FileAttributes, 0, 0);
    if (!file)
    {
        return DDS_ERROR_CODE_OOM;
    }

    // Seed size and timestamps from persisted backend metadata.
    file->SetSize(props.FileSize);
    if (props.LastAccessTime != 0)
    {
        file->SetLastAccessTime(props.LastAccessTime);
    }
    if (props.LastWriteTime != 0)
    {
        file->SetLastWriteTime(props.LastWriteTime);
    }

    AllFiles[FileId] = file;
    if (FileId >= FileIdEnd)
    {
        //
        // End is the next to the last valid file ID
        //
        //
        FileIdEnd = FileId + 1;
    }

    //
    // Track membership in the root directory (single-dir flat file name model)
    //
    //
    if (AllDirs[DDS_DIR_ROOT])
    {
        AllDirs[DDS_DIR_ROOT]->AddFile(FileId);
    }

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Delete a directory
// 
//
ErrorCodeT
DDSFrontEnd::RemoveDirectory(
    const char* PathName
) {
    DirIdT id = DDS_DIR_INVALID;

    for (DirIdT i = 0; i != DirIdEnd; i++) {
        if (AllDirs[i] && strcmp(PathName, AllDirs[i]->GetName()) == 0) {
            id = i;
            break;
        }
    }

    if (id == DDS_DIR_INVALID) {
        return DDS_ERROR_CODE_DIR_NOT_FOUND;
    }

    //
    // Recursively delete all files and child directories
    //
    //
    RemoveDirectoryRecursive(AllDirs[id]);

    delete AllDirs[id];
    AllDirs[id] = nullptr;

    //
    // Reflect the update on back end
    //
    //
    return BackEnd->RemoveDirectory(id);
}

//
// Create a file
// 
//
ErrorCodeT
DDSFrontEnd::CreateFile(
    const char* FileName,
    FileAccessT DesiredAccess,
    FileShareModeT ShareMode,
    FileAttributesT FileAttributes,
    FileIdT* FileId
) {
    //
    // Check existance of the file
    //
    //
    bool exists = false;
    for (size_t i = 0; i != FileIdEnd; i++) {
        if (AllFiles[i] && strcmp(FileName, AllFiles[i]->GetName()) == 0) {
            exists = true;
            break;
        }
    }
    if (exists) {
        return DDS_ERROR_CODE_FILE_EXISTS;
    }

    //
    // Retrieve an id
    // TODO: concurrent support
    //
    //
    FileIdT id = 0;
    for (; id != DDS_MAX_FILES; id++) {
        if (!AllFiles[id]) {
            break;
        }
    }
    if (id == DDS_MAX_FILES) {
        return DDS_ERROR_CODE_TOO_MANY_FILES;
    }

    //
    // Create the file
    //
    //
    DDSFile* file = new DDSFile(id, FileName, FileAttributes, DesiredAccess, ShareMode);

    //
    // TODO: find the directory
    //
    //
    DirIdT dirId = DDS_DIR_ROOT;
    AllDirs[dirId]->AddFile(id);

    AllFiles[id] = file;
    if (id >= FileIdEnd) {
        //
        // End is the next to the last valid file ID
        //
        //
        FileIdEnd = id + 1;
    }

    *FileId = id;

    //
    // Reflect the update on back end
    //
    //
    return BackEnd->CreateFile(FileName, FileAttributes, *FileId, dirId);
}

//
// Delete a file
// - Reject deletion while any POSIX fd remains open for this file.
//   Note: Only DDSFrontEnd::DeleteFile is guarded; other deletion paths
//   (if introduced later) must enforce the same rule.
//
//
ErrorCodeT
DDSFrontEnd::DeleteFile(
    const char* FileName
) {
    FileIdT id = DDS_FILE_INVALID;
    DDSFile* pFile = nullptr;

    for (FileIdT i = 0; i != FileIdEnd; i++) {
        if (AllFiles[i] && strcmp(FileName, AllFiles[i]->GetName()) == 0) {
            id = i;
            pFile = AllFiles[i];
            break;
        }
    }

    if (id == DDS_FILE_INVALID) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    // jason: Fail delete if any fd is still open for this file.
    // jason: DDS_ERROR_CODE_FILE_EXISTS is reused here to indicate "file busy".
    if (pFile && pFile->GetOpenCount() > 0)
    {
        return DDS_ERROR_CODE_FILE_EXISTS;
    }

    //
    // NOTE: Assumes single-dir flat file name model, so everything is in root dir
    //
    //
    DirIdT dirId = DDS_DIR_ROOT;
    AllDirs[dirId]->DeleteFile(id);

    delete AllFiles[id];
    AllFiles[id] = nullptr;

    //
    // Reflect the update on back end
    //
    //
    return BackEnd->DeleteFile(id, dirId);
}

//
// Change the size of a file
// 
//
ErrorCodeT
DDSFrontEnd::ChangeFileSize(
    FileIdT FileId,
    FileSizeT NewSize
) {
    //
    // Change file size on back end first
    //
    //
    ErrorCodeT result = BackEnd->ChangeFileSize(FileId, NewSize);
    if (result == DDS_ERROR_CODE_SUCCESS) {
        AllFiles[FileId]->SetSize(NewSize);
    }

    return result;
}

//
// Set the physical file size for the specified file
// to the current position of the file pointer
// 
//
ErrorCodeT
DDSFrontEnd::SetEndOfFile(
    FileIdT FileId
) {
    DDSFile* pFile = AllFiles[FileId];

    //
    // Change file size on back end first
    //
    //
    ErrorCodeT result = BackEnd->ChangeFileSize(FileId, pFile->GetPointer());
    
    if (result == DDS_ERROR_CODE_SUCCESS) {
        pFile->SetSize(pFile->GetPointer());
    }

    return result;
}


//
// Move the file pointer of the specified file
// 
//
ErrorCodeT
DDSFrontEnd::SetFilePointer(
    FileIdT FileId,
    PositionInFileT DistanceToMove,
    FilePointerPosition MoveMethod
) {
    DDSFile* pFile = AllFiles[FileId];
    PositionInFileT newPosition = 0;

    switch (MoveMethod)
    {
    case FilePointerPosition::BEGIN:
        newPosition = DistanceToMove;
        break;
    case FilePointerPosition::CURRENT:
        newPosition = pFile->GetPointer() + DistanceToMove;
        break;
    case FilePointerPosition::END:
        newPosition = pFile->GetSize() + DistanceToMove;
        break;
    default:
        return DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE;
    }

    /*FileSizeT storageFreeSpace;
    GetStorageFreeSpace(&storageFreeSpace);
    if ((newPosition - (PositionInFileT)pFile->GetSize()) > (PositionInFileT)storageFreeSpace) {
        return DDS_ERROR_CODE_INVALID_FILE_POSITION;
    }*/

    pFile->SetPointer(newPosition);

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get file size
// - For DPU backend, use the front-end size maintained on completion/truncate.
//
ErrorCodeT
DDSFrontEnd::GetFileSize(
    FileIdT FileId,
    FileSizeT* FileSize
) {
    // if (FileId >= DDS_MAX_FILES || AllFiles[FileId] == nullptr) {
    //     return DDS_ERROR_CODE_FILE_NOT_FOUND;
    // }

    //
    // No writes on the DPU, so it's safe to get file size here
    //
    //
    // *FileSize = AllFiles[FileId]->GetSize();
    // ErrorCodeT result = BackEnd->GetFileSize(FileId, FileSize);
    // return result;

#if BACKEND_TYPE == BACKEND_TYPE_DPU
    if (FileId >= DDS_MAX_FILES || AllFiles[FileId] == nullptr) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }
    *FileSize = AllFiles[FileId]->GetSize();
    return DDS_ERROR_CODE_SUCCESS;
#else
    ErrorCodeT result = BackEnd->GetFileSize(FileId, FileSize);
    return result;
#endif
}

ErrorCodeT
DDSFrontEnd::SetRead2AesKey(
    const uint8_t* KeyBytes,
    uint8_t KeySizeBytes
) {
    if (BackEnd == NULL)
    {
        return DDS_ERROR_CODE_FAILED_CONNECTION;
    }
    return BackEnd->SetRead2AesKey(KeyBytes, KeySizeBytes);
}

#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
// Async read from a file 


//
// Async read from a file
// - RequestId is unused for local memory backend.
//
ErrorCodeT
DDSFrontEnd::ReadFile(
    FileIdT FileId,
    BufferT DestBuffer,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollT* poll = AllPolls[AllFiles[FileId]->PollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    //
    // Lock-free busy polling for completion
    //
    //
    bool expectedCompletion = true;
    while (pIO->IsComplete.compare_exchange_weak(
        expectedCompletion,
        false,
        std::memory_order_relaxed
    ) == false) {
        if (pIO->AppCallback == nullptr) {
            return DDS_ERROR_CODE_REQUIRE_POLLING;
        }
    }

    pIO->IsRead = true;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = AllFiles[FileId]->GetPointer();
    pIO->AppBuffer = DestBuffer;
    pIO->AppBufferArray = nullptr;
    pIO->BytesDesired = BytesToRead;
    pIO->AppCallback = Callback;
    pIO->Context = Context;

    result = BackEnd->ReadFile(
        pIO->FileId,
        pIO->Offset,
        pIO->AppBuffer,
        pIO->BytesDesired,
        BytesRead,
        pIO,
        poll
    );

    //
    // TODO: better handle file pointer
    //
    //
    FileSizeT newPointer = ((DDSFile*)pIO->FileReference)->GetPointer() + BytesToRead;
    ((DDSFile*)pIO->FileReference)->SetPointer(newPointer);

    if (!Callback) {
        return DDS_ERROR_CODE_IO_PENDING;
    }

    return result;
}

//
// Async read from a file with explicit output buffer size (read2)
//
ErrorCodeT
DDSFrontEnd::ReadFile2(
    FileIdT FileId,
    BufferT DestBuffer,
    FileIOSizeT LogicalBytesToRead,
    const FileIOSizeT* StageSizes,
    const FileIOSizeT* StageInputOffsets,
    const FileIOSizeT* StageInputLengths,
    uint16_t StageCount,
    FileIOSizeT* BytesRead,
    FileIOSizeT* LogicalBytesRead,
    ReadWriteCallback2 Callback,
    ContextT Context
) {
    // jason: local-memory backend does not support read2 yet.
    (void)FileId;
    (void)DestBuffer;
    (void)LogicalBytesToRead;
    (void)StageSizes;
    (void)StageInputOffsets;
    (void)StageInputLengths;
    (void)StageCount;
    (void)BytesRead;
    (void)LogicalBytesRead;
    (void)Callback;
    (void)Context;
    return DDS_ERROR_CODE_NOT_IMPLEMENTED;
}
#elif BACKEND_TYPE == BACKEND_TYPE_DPU

// Async pread from a file
// - RequestId is pre-initialized per slot; assert slot mapping.
// - Uses the file's poll ID instead of hardcoding DDS_POLL_DEFAULT.
// - Publishes per-request metadata with IsAvailable (release) before submission.
// - Prefer the offset-based ReadFile overload when possible (same behavior).
// - Updated: compute alignment metadata for backend-aligned reads.
ErrorCodeT
DDSFrontEnd::PReadFile(
    FileIdT FileId,
    FileSizeT Offset,
    BufferT DestBuffer,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    // jason: Defensive check to avoid dereferencing missing file entries.
    DDSFile* file = GetFile(FileId);
    if (!file) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }
    // PollIdT pollId = DDS_POLL_DEFAULT;
    PollIdT pollId = file->PollId;

    PollT* poll = AllPolls[pollId];

    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];
    // TODO context->reqid = mySlot 

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        //
        // If this is a callback-based completion,
        // perform polling once
        //
        //
        // if (pIO->AppCallback) {
        //     FileIOSizeT bytesServiced;
        //     ContextT fileCtxt;
        //     ContextT ioCtxt;
        //     bool pollResult;
        //
        //     PollWait(
        //         pollId,
        //         &bytesServiced,
        //         &fileCtxt,
        //         &ioCtxt,
        //         0,
        //         &pollResult
        //     );
        // }
        // jason: PollWait is serialized by the DDSPosix poller thread.
        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }
    pIO->IsRead = true;
    // jason: reset read2 state for standard pread.
    pIO->IsRead2 = false;
    pIO->StageCount = 0;
    pIO->StageSizes[0] = 0;
    pIO->StageSizes[1] = 0;
    pIO->FileReference = file;
    pIO->FileId = FileId;
    pIO->Offset = Offset;
    pIO->BytesDesired = BytesToRead;
    pIO->OutputBytes = BytesToRead;
    pIO->LogicalBytesServiced = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    pIO->OffloadReadTimeNs = 0;
    pIO->OffloadStage1TimeNs = 0;
    pIO->OffloadStage2TimeNs = 0;
#endif
    pIO->AppBuffer = DestBuffer;
    pIO->AppBufferArray = nullptr;
    pIO->AppCallback = Callback;
    pIO->AppCallback2 = nullptr;
    pIO->Context = Context;
    // jason: compute aligned range and copy-start for backend-aligned reads.
    ComputeReadAlignment(pIO->Offset, pIO->BytesDesired, &pIO->AlignedOffset, &pIO->AlignedBytes, &pIO->CopyStart);
    // jason: duplicate alignment computation removed to avoid double work.
    // ComputeReadAlignment(pIO->Offset, pIO->BytesDesired, &pIO->AlignedOffset, &pIO->AlignedBytes, &pIO->CopyStart);
    // pIO->RequestId = mySlot; // TODO verify this works 
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->ReadFile(
        FileId,
        pIO->Offset,
        DestBuffer,
        BytesToRead,
        nullptr,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    //
    // Update file pointer
    //
    //
    // ((DDSFile*)pIO->FileReference)->IncrementPointer(BytesToRead);
    // printf("After Read: FileId=%d, Offset=%lu, BytesToRead=%u\n", 
    //     FileId, pIO->Offset, BytesToRead);
    return DDS_ERROR_CODE_IO_PENDING;
}

//
// Async pread from a file with explicit stage sizes (read2)
//
ErrorCodeT
DDSFrontEnd::PReadFile2(
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
    ReadWriteCallback2 Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    // jason: Defensive check to avoid dereferencing missing file entries.
    DDSFile* file = GetFile(FileId);
    if (!file) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }
    PollIdT pollId = file->PollId;

    PollT* poll = AllPolls[pollId];

    // Task 2.1: route to channel pread2.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PREAD2];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        // jason: PollWait is serialized by the DDSPosix poller thread.
        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }
    // jason: read2 supports one or two explicit stage descriptors.
    if (!ValidateRead2StageSpec(StageSizes, StageInputOffsets, StageInputLengths, StageCount, LogicalBytesToRead)) {
        pIO->IsAvailable = true;
        return DDS_ERROR_CODE_INVALID_PARAM;
    }
    FileIOSizeT outputBytes = StageSizes[StageCount - 1];
    pIO->IsRead = true;
    pIO->IsRead2 = true;
    pIO->FileReference = file;
    pIO->FileId = FileId;
    pIO->Offset = Offset;
    pIO->BytesDesired = LogicalBytesToRead;
    // Original: pIO->OutputBytes = OutputBytes;
    // jason: OutputBytes is now the final stage size.
    pIO->OutputBytes = outputBytes;
    pIO->StageCount = StageCount;
    pIO->StageSizes[0] = StageSizes[0];
    // jason: clear stage2 size when running single-stage read2 to avoid stale slot metadata.
    pIO->StageSizes[1] = (StageCount > 1) ? StageSizes[1] : 0;
    pIO->LogicalBytesServiced = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    pIO->OffloadReadTimeNs = 0;
    pIO->OffloadStage1TimeNs = 0;
    pIO->OffloadStage2TimeNs = 0;
#endif
    pIO->AppBuffer = DestBuffer;
    pIO->AppBufferArray = nullptr;
    pIO->AppCallback = nullptr;
    pIO->AppCallback2 = Callback;
    pIO->Context = Context;
    // jason: compute aligned range and copy-start for backend-aligned reads.
    ComputeReadAlignment(pIO->Offset, pIO->BytesDesired, &pIO->AlignedOffset, &pIO->AlignedBytes, &pIO->CopyStart);
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->ReadFile2(
        FileId,
        pIO->Offset,
        DestBuffer,
        LogicalBytesToRead,
        StageSizes,
        StageInputOffsets,
        StageInputLengths,
        StageCount,
        BytesRead,
        LogicalBytesRead,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    // jason: BytesRead/LogicalBytesRead are populated on completion.
    return DDS_ERROR_CODE_IO_PENDING;
}

//
// Async read from a file
// - Publishes per-request metadata with IsAvailable (release) before submission.
// - Updated: compute alignment metadata for backend-aligned reads (aligned offset/bytes and copy-start).
//
ErrorCodeT
DDSFrontEnd::ReadFile(
    FileIdT FileId,
    BufferT DestBuffer,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollIdT pollId = AllFiles[FileId]->PollId;
    PollT* poll = AllPolls[pollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];
    // TODO context->reqid = mySlot 

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        //
        // If this is a callback-based completion,
        // perform polling once
        //
        //
        if (pIO->AppCallback) {
            FileIOSizeT bytesServiced;
            ContextT fileCtxt;
            ContextT ioCtxt;
            bool pollResult;

            PollWait(
                pollId,
                &bytesServiced,
                &fileCtxt,
                &ioCtxt,
                0,
                &pollResult
            );
        }
        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }

    pIO->IsRead = true;
    // jason: reset read2 state for standard read.
    pIO->IsRead2 = false;
    pIO->StageCount = 0;
    pIO->StageSizes[0] = 0;
    pIO->StageSizes[1] = 0;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = AllFiles[FileId]->GetPointer();
    pIO->BytesDesired = BytesToRead;
    pIO->OutputBytes = BytesToRead;
    pIO->LogicalBytesServiced = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    pIO->OffloadReadTimeNs = 0;
    pIO->OffloadStage1TimeNs = 0;
    pIO->OffloadStage2TimeNs = 0;
#endif
    pIO->AppBuffer = DestBuffer;
    pIO->AppBufferArray = nullptr;
    pIO->AppCallback = Callback;
    pIO->AppCallback2 = nullptr;
    pIO->Context = Context;
    // jason: compute aligned range and copy-start for backend-aligned reads.
    ComputeReadAlignment(pIO->Offset, pIO->BytesDesired, &pIO->AlignedOffset, &pIO->AlignedBytes, &pIO->CopyStart);
    // pIO->RequestId = mySlot; // TODO verify this works
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->ReadFile(
        FileId,
        pIO->Offset,
        DestBuffer,
        BytesToRead,
        nullptr,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    //
    // Update file pointer
    //
    //
    ((DDSFile*)pIO->FileReference)->IncrementPointer(BytesToRead);
    // printf("After Read: FileId=%d, Offset=%lu, BytesToRead=%u\n", 
    //     FileId, pIO->Offset, BytesToRead);
    return DDS_ERROR_CODE_IO_PENDING;
}

//
// Async read from a file with explicit output buffer size (read2)
//
ErrorCodeT
DDSFrontEnd::ReadFile2(
    FileIdT FileId,
    BufferT DestBuffer,
    FileIOSizeT LogicalBytesToRead,
    const FileIOSizeT* StageSizes,
    const FileIOSizeT* StageInputOffsets,
    const FileIOSizeT* StageInputLengths,
    uint16_t StageCount,
    FileIOSizeT* BytesRead,
    FileIOSizeT* LogicalBytesRead,
    ReadWriteCallback2 Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollIdT pollId = AllFiles[FileId]->PollId;
    PollT* poll = AllPolls[pollId];
    // Task 2.1: route to channel pread2.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PREAD2];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        //
        // If this is a callback-based completion,
        // perform polling once
        //
        //
        if (pIO->AppCallback) {
            FileIOSizeT bytesServiced;
            ContextT fileCtxt;
            ContextT ioCtxt;
            bool pollResult;

            PollWait(
                pollId,
                &bytesServiced,
                &fileCtxt,
                &ioCtxt,
                0,
                &pollResult
            );
        }
        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }
    if (!ValidateRead2StageSpec(StageSizes, StageInputOffsets, StageInputLengths, StageCount, LogicalBytesToRead)) {
        pIO->IsAvailable = true;
        return DDS_ERROR_CODE_INVALID_PARAM;
    }
    FileIOSizeT outputBytes = StageSizes[StageCount - 1];

    pIO->IsRead = true;
    pIO->IsRead2 = true;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = AllFiles[FileId]->GetPointer();
    pIO->BytesDesired = LogicalBytesToRead;
    pIO->OutputBytes = outputBytes;
    pIO->StageCount = StageCount;
    pIO->StageSizes[0] = StageSizes[0];
    // jason: clear stage2 size when running single-stage read2 to avoid stale slot metadata.
    pIO->StageSizes[1] = (StageCount > 1) ? StageSizes[1] : 0;
    pIO->LogicalBytesServiced = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    pIO->OffloadReadTimeNs = 0;
    pIO->OffloadStage1TimeNs = 0;
    pIO->OffloadStage2TimeNs = 0;
#endif
    pIO->AppBuffer = DestBuffer;
    pIO->AppBufferArray = nullptr;
    pIO->AppCallback = nullptr;
    pIO->AppCallback2 = Callback;
    pIO->Context = Context;
    // jason: compute aligned range and copy-start for backend-aligned reads.
    ComputeReadAlignment(pIO->Offset, pIO->BytesDesired, &pIO->AlignedOffset, &pIO->AlignedBytes, &pIO->CopyStart);
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->ReadFile2(
        FileId,
        pIO->Offset,
        DestBuffer,
        LogicalBytesToRead,
        StageSizes,
        StageInputOffsets,
        StageInputLengths,
        StageCount,
        BytesRead,
        LogicalBytesRead,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    //
    // Update file pointer using logical bytes (POSIX semantics)
    //
    //
    ((DDSFile*)pIO->FileReference)->IncrementPointer(LogicalBytesToRead);

    return DDS_ERROR_CODE_IO_PENDING;
}

#else
#error "Unknown backend type"
#endif

#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
//
// Async read from a file with scattering
// - RequestId is unused for local memory backend.
//
ErrorCodeT
DDSFrontEnd::ReadFileScatter(
    FileIdT FileId,
    BufferT* DestBufferArray,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollT* poll = AllPolls[AllFiles[FileId]->PollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    //
    // Lock-free busy polling for completion
    //
    //
    bool expectedCompletion = true;
    while (pIO->IsComplete.compare_exchange_weak(
        expectedCompletion,
        false,
        std::memory_order_relaxed
    ) == false) {
        if (pIO->AppCallback == nullptr) {
            return DDS_ERROR_CODE_REQUIRE_POLLING;
        }
    }

    pIO->IsRead = true;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = AllFiles[FileId]->GetPointer();
    pIO->AppBuffer = nullptr;
    pIO->AppBufferArray = DestBufferArray;
    pIO->BytesDesired = BytesToRead;
    pIO->AppCallback = Callback;
    pIO->Context = Context;
    // RequestId is unused for local memory backend.
    // assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));

    result = BackEnd->ReadFileScatter(
        pIO->FileId,
        pIO->Offset,
        pIO->AppBufferArray,
        pIO->BytesDesired,
        BytesRead,
        pIO,
        poll
    );

    //
    // TODO: better handle file pointer
    //
    //
    FileSizeT newPointer = ((DDSFile*)pIO->FileReference)->GetPointer() + BytesToRead;
    ((DDSFile*)pIO->FileReference)->SetPointer(newPointer);

    if (!Callback) {
        return DDS_ERROR_CODE_IO_PENDING;
    }

    return result;
}

//
// Async read from a file with explicit output buffer size (read2)
//
ErrorCodeT
DDSFrontEnd::ReadFile2(
    FileIdT FileId,
    BufferT DestBuffer,
    FileSizeT Offset,
    FileIOSizeT LogicalBytesToRead,
    const FileIOSizeT* StageSizes,
    const FileIOSizeT* StageInputOffsets,
    const FileIOSizeT* StageInputLengths,
    uint16_t StageCount,
    FileIOSizeT* BytesRead,
    FileIOSizeT* LogicalBytesRead,
    ReadWriteCallback2 Callback,
    ContextT Context
) {
    // jason: local-memory backend does not support read2 yet.
    (void)FileId;
    (void)DestBuffer;
    (void)Offset;
    (void)LogicalBytesToRead;
    (void)StageSizes;
    (void)StageInputOffsets;
    (void)StageInputLengths;
    (void)StageCount;
    (void)BytesRead;
    (void)LogicalBytesRead;
    (void)Callback;
    (void)Context;
    return DDS_ERROR_CODE_NOT_IMPLEMENTED;
}
#elif BACKEND_TYPE == BACKEND_TYPE_DPU
//
// Async read from a file with scattering
// - RequestId is pre-initialized per slot; assert slot mapping.
// - Publishes per-request metadata with IsAvailable (release) before submission.
// - Updated: compute alignment metadata for backend-aligned reads.
//
ErrorCodeT
DDSFrontEnd::ReadFileScatter(
    FileIdT FileId,
    BufferT* DestBufferArray,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollIdT pollId = AllFiles[FileId]->PollId;
    PollT* poll = AllPolls[pollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        //
        // If this is a callback-based completion,
        // perform polling once
        //
        //
        if (pIO->AppCallback) {
            FileIOSizeT bytesServiced;
            ContextT fileCtxt;
            ContextT ioCtxt;
            bool pollResult;

            PollWait(
                pollId,
                &bytesServiced,
                &fileCtxt,
                &ioCtxt,
                0,
                &pollResult
            );
        }

        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }

    pIO->IsRead = true;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = AllFiles[FileId]->GetPointer();
    pIO->BytesDesired = BytesToRead;
    pIO->AppBuffer = nullptr;
    pIO->AppBufferArray = DestBufferArray;
    pIO->AppCallback = Callback;
    pIO->Context = Context;
    // jason: compute aligned range and copy-start for backend-aligned reads.
    ComputeReadAlignment(pIO->Offset, pIO->BytesDesired, &pIO->AlignedOffset, &pIO->AlignedBytes, &pIO->CopyStart);
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->ReadFileScatter(
        FileId,
        pIO->Offset,
        DestBufferArray,
        BytesToRead,
        nullptr,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    //
    // Update file pointer
    //
    //
    ((DDSFile*)pIO->FileReference)->IncrementPointer(BytesToRead);

    return DDS_ERROR_CODE_IO_PENDING;
}
#else
#error "Unknown backend type"
#endif

#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
//
// Async write to a file
// - RequestId is unused for local memory backend.
//
ErrorCodeT
DDSFrontEnd::WriteFile(
    FileIdT FileId,
    BufferT SourceBuffer,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollT* poll = AllPolls[AllFiles[FileId]->PollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    //
    // Lock-free busy polling for completion
    //
    //
    bool expectedCompletion = true;
    while (pIO->IsComplete.compare_exchange_weak(
        expectedCompletion,
        false,
        std::memory_order_relaxed
    ) == false) {
        if (pIO->AppCallback == nullptr) {
            return DDS_ERROR_CODE_REQUIRE_POLLING;
        }
    }

    pIO->IsRead = false;
    // jason: clear read2 state for write requests.
    pIO->IsRead2 = false;
    // jason: clear read2 state for write requests.
    pIO->IsRead2 = false;
    pIO->StageCount = 0;
    pIO->StageSizes[0] = 0;
    pIO->StageSizes[1] = 0;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = AllFiles[FileId]->GetPointer();
    pIO->AppBuffer = DestBuffer;
    pIO->AppBufferArray = nullptr;
    pIO->BytesDesired = BytesToWrite;
    pIO->AppCallback = Callback;
    pIO->Context = Context;
    // RequestId is unused for local memory backend.
    // assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));

    result = BackEnd->WriteFile(
        pIO->FileId,
        pIO->Offset,
        pIO->AppBuffer,
        pIO->BytesDesired,
        BytesWritten,
        pIO,
        poll
    );

    //
    // Update file pointer
    // TODO: better handle file pointer
    //
    //
    FileSizeT newPointer = ((DDSFile*)pIO->FileReference)->GetPointer() + BytesToWrite;
    ((DDSFile*)pIO->FileReference)->SetPointer(newPointer);

    if (!Callback) {
        return DDS_ERROR_CODE_IO_PENDING;
    }

    return result;
}
#elif BACKEND_TYPE == BACKEND_TYPE_DPU
//
// Async write to a file
// - RequestId is pre-initialized per slot; assert slot mapping.
// - PollWait is expected to be serialized by the caller (e.g., DDSPosix poller).
// - Publishes per-request metadata with IsAvailable (release) before submission.
//
ErrorCodeT
DDSFrontEnd::WriteFile(
    FileIdT FileId,
    BufferT SourceBuffer,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    /*
    PollIdT pollId = AllFiles[FileId]->PollId;
    PollT* poll = AllPolls[pollId];
    size_t mySlot;
    FileIOT* pIO;
    bool expectedAvail;
    bool found = false;

    for (int i = DDS_MAX_OUTSTANDING_IO; i != 0; i--) {
        mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
        pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
        mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
        pIO = poll->OutstandingRequests[mySlot];

        expectedAvail = true;
        if (pIO->IsAvailable.compare_exchange_weak(
            expectedAvail,
            false,
            std::memory_order_relaxed
        ) == false) {
            //
            // If this is a callback-based completion,
            // perform polling once
            //
            //
            if (pIO->AppCallback) {
                FileIOSizeT bytesServiced;
                ContextT fileCtxt;
                ContextT ioCtxt;
                bool pollResult;

                PollWait(
                    pollId,
                    &bytesServiced,
                    &fileCtxt,
                    &ioCtxt,
                    0,
                    &pollResult
                );
            }
        }
        else {
            found = true;
            break;
        }
    }
    if (!found) {
        //
        // If this is a callback-based completion,
        // perform polling once
        //
        //
        // if (pIO->AppCallback) {
        //     FileIOSizeT bytesServiced;
        //     ContextT fileCtxt;
        //     ContextT ioCtxt;
        //     bool pollResult;
        //
        //     PollWait(
        //         pollId,
        //         &bytesServiced,
        //         &fileCtxt,
        //         &ioCtxt,
        //         0,
        //         &pollResult
        //     );
        // }
        // jason: PollWait is serialized by the DDSPosix poller thread.

        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }
    */

    PollIdT pollId = AllFiles[FileId]->PollId;
    PollT* poll = AllPolls[pollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        //
        // If this is a callback-based completion,
        // perform polling once
        //
        //
        if (pIO->AppCallback) {
            FileIOSizeT bytesServiced;
            ContextT fileCtxt;
            ContextT ioCtxt;
            bool pollResult;

            PollWait(
                pollId,
                &bytesServiced,
                &fileCtxt,
                &ioCtxt,
                0,
                &pollResult
            );
        }

        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }

    pIO->IsRead = false;
    // jason: clear read2 state for write requests.
    pIO->IsRead2 = false;
    pIO->StageCount = 0;
    pIO->StageSizes[0] = 0;
    pIO->StageSizes[1] = 0;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = AllFiles[FileId]->GetPointer();
    pIO->BytesDesired = BytesToWrite;
    pIO->AppBuffer = nullptr;
    pIO->AppBufferArray = nullptr;
    pIO->AppCallback = Callback;
    pIO->AppCallback2 = nullptr;
    pIO->Context = Context;
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->WriteFile(
        FileId,
        pIO->Offset,
        SourceBuffer,
        BytesToWrite,
        BytesWritten,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    //
    // Update file pointer
    //
    //
    ((DDSFile*)pIO->FileReference)->IncrementPointer(BytesToWrite);

    return DDS_ERROR_CODE_IO_PENDING;
}
#else
#error "Unknown backend type"
#endif

#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
//
// Async write to a file with gathering
// - RequestId is unused for local memory backend.
//
ErrorCodeT
DDSFrontEnd::WriteFileGather(
    FileIdT FileId,
    BufferT* SourceBufferArray,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollT* poll = AllPolls[AllFiles[FileId]->PollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    //
    // Lock-free busy polling for completion
    //
    //
    bool expectedCompletion = true;
    while (pIO->IsComplete.compare_exchange_weak(
        expectedCompletion,
        false,
        std::memory_order_relaxed
    ) == false) {
        if (pIO->AppCallback == nullptr) {
            return DDS_ERROR_CODE_REQUIRE_POLLING;
        }
    }

    pIO->IsRead = false;
    pIO->IsSegmented = true;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = AllFiles[FileId]->GetPointer();
    pIO->AppBuffer = nullptr;
    pIO->AppBufferArray = SourceBufferArray;
    pIO->BytesDesired = BytesToWrite;
    pIO->BytesServiced = 0;
    pIO->AppCallback = Callback;
    pIO->Context = Context;
    // RequestId is unused for local memory backend.
    // assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));

    result = BackEnd->WriteFileGather(
        pIO->FileId,
        pIO->Offset,
        pIO->AppBufferArray,
        pIO->BytesDesired,
        &pIO->BytesServiced,
        pIO,
        poll
    );

    if (BytesWritten) {
        *BytesWritten = pIO->BytesServiced;
    }

    //
    // Update file pointer
    // TODO: better handle file pointer
    //
    //
    FileSizeT newPointer = ((DDSFile*)pIO->FileReference)->GetPointer() + BytesToWrite;
    ((DDSFile*)pIO->FileReference)->SetPointer(newPointer);

    if (!Callback) {
        return DDS_ERROR_CODE_IO_PENDING;
    }

    return result;
}
#elif BACKEND_TYPE == BACKEND_TYPE_DPU
//
// Async write to a file with gathering
// - RequestId is pre-initialized per slot; assert slot mapping.
// - Publishes per-request metadata with IsAvailable (release) before submission.
//
ErrorCodeT
DDSFrontEnd::WriteFileGather(
    FileIdT FileId,
    BufferT* SourceBufferArray,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollIdT pollId = AllFiles[FileId]->PollId;
    PollT* poll = AllPolls[pollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        //
        // If this is a callback-based completion,
        // perform polling once
        //
        //
        if (pIO->AppCallback) {
            FileIOSizeT bytesServiced;
            ContextT fileCtxt;
            ContextT ioCtxt;
            bool pollResult;

            PollWait(
                pollId,
                &bytesServiced,
                &fileCtxt,
                &ioCtxt,
                0,
                &pollResult
            );
        }
        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }

    pIO->IsRead = false;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = AllFiles[FileId]->GetPointer();
    pIO->BytesDesired = BytesToWrite;
    pIO->AppBuffer = nullptr;
    pIO->AppBufferArray = nullptr;
    pIO->AppCallback = Callback;
    pIO->AppCallback2 = nullptr;
    pIO->Context = Context;
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->WriteFileGather(
        FileId,
        pIO->Offset,
        SourceBufferArray,
        BytesToWrite,
        nullptr,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    //
    // Update file pointer
    //
    //
    ((DDSFile*)pIO->FileReference)->IncrementPointer(BytesToWrite);

    return DDS_ERROR_CODE_IO_PENDING;
}
#else
#error "Unknown backend type"
#endif

//
// Read/write with offset
//
//

#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
//
// Async read from a file
// 
//
ErrorCodeT
DDSFrontEnd::ReadFile(
    FileIdT FileId,
    BufferT DestBuffer,
    FileSizeT Offset,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollT* poll = AllPolls[AllFiles[FileId]->PollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    //
    // Lock-free busy polling for completion
    //
    //
    bool expectedCompletion = true;
    while (pIO->IsComplete.compare_exchange_weak(
        expectedCompletion,
        false,
        std::memory_order_relaxed
    ) == false) {
        if (pIO->AppCallback == nullptr) {
            return DDS_ERROR_CODE_REQUIRE_POLLING;
        }
    }

    pIO->IsRead = true;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = Offset;
    pIO->AppBuffer = DestBuffer;
    pIO->AppBufferArray = nullptr;
    pIO->BytesDesired = BytesToRead;
    pIO->AppCallback = Callback;
    pIO->Context = Context;

    result = BackEnd->ReadFile(
        pIO->FileId,
        pIO->Offset,
        pIO->AppBuffer,
        pIO->BytesDesired,
        BytesRead,
        pIO,
        poll
    );

    if (!Callback) {
        return DDS_ERROR_CODE_IO_PENDING;
    }

    return result;
}
#elif BACKEND_TYPE == BACKEND_TYPE_DPU
//
// Async read from a file
// - Offset-based DPU read keeps RequestId in sync with slot index.
// - PollWait is expected to be serialized by the caller (e.g., DDSPosix poller).
// - Publishes per-request metadata with IsAvailable (release) before submission.
// - Defensive check for missing file entries.
// - Updated: compute alignment metadata for backend-aligned reads.
//
ErrorCodeT
DDSFrontEnd::ReadFile(
    FileIdT FileId,
    BufferT DestBuffer,
    FileSizeT Offset,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    DDSFile* file = GetFile(FileId);
    if (!file) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }
    PollIdT pollId = file->PollId;
    PollT* poll = AllPolls[pollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        //
        // If this is a callback-based completion,
        // perform polling once
        //
        //
        // if (pIO->AppCallback) {
        //     FileIOSizeT bytesServiced;
        //     ContextT fileCtxt;
        //     ContextT ioCtxt;
        //     bool pollResult;
        //
        //     PollWait(
        //         pollId,
        //         &bytesServiced,
        //         &fileCtxt,
        //         &ioCtxt,
        //         0,
        //         &pollResult
        //     );
        // }
        // jason: PollWait is serialized by the DDSPosix poller thread.
        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }

    // include alignment metadata so frontend copies the right subrange.
    pIO->IsRead = true;
    // jason: reset read2 state for standard offset read.
    pIO->IsRead2 = false;
    pIO->StageCount = 0;
    pIO->StageSizes[0] = 0;
    pIO->StageSizes[1] = 0;
    pIO->FileReference = file;
    pIO->FileId = FileId;
    pIO->Offset = Offset;
    pIO->BytesDesired = BytesToRead;
    pIO->OutputBytes = BytesToRead;
    pIO->LogicalBytesServiced = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    pIO->OffloadReadTimeNs = 0;
    pIO->OffloadStage1TimeNs = 0;
    pIO->OffloadStage2TimeNs = 0;
#endif
    pIO->AppBuffer = DestBuffer;
    pIO->AppBufferArray = nullptr;
    pIO->AppCallback = Callback;
    pIO->AppCallback2 = nullptr;
    pIO->Context = Context;
    // Track aligned range and copy-start for backend-aligned read payloads.
    ComputeReadAlignment(pIO->Offset, pIO->BytesDesired, &pIO->AlignedOffset, &pIO->AlignedBytes, &pIO->CopyStart);
    // pIO->RequestId = mySlot; // TODO verify this works 
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->ReadFile(
        FileId,
        Offset,
        DestBuffer,
        BytesToRead,
        nullptr,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    return DDS_ERROR_CODE_IO_PENDING;
}

//
// Async read from a file with explicit output buffer size (read2)
// - Offset-based DPU read keeps RequestId in sync with slot index.
// - PollWait is expected to be serialized by the caller (e.g., DDSPosix poller).
// - Publishes per-request metadata with IsAvailable (release) before submission.
//
ErrorCodeT
DDSFrontEnd::ReadFile2(
    FileIdT FileId,
    BufferT DestBuffer,
    FileSizeT Offset,
    FileIOSizeT LogicalBytesToRead,
    const FileIOSizeT* StageSizes,
    const FileIOSizeT* StageInputOffsets,
    const FileIOSizeT* StageInputLengths,
    uint16_t StageCount,
    FileIOSizeT* BytesRead,
    FileIOSizeT* LogicalBytesRead,
    ReadWriteCallback2 Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    DDSFile* file = GetFile(FileId);
    if (!file) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }
    PollIdT pollId = file->PollId;
    PollT* poll = AllPolls[pollId];
    // Task 2.1: route to channel pread2.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PREAD2];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        // jason: PollWait is serialized by the DDSPosix poller thread.
        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }
    if (!ValidateRead2StageSpec(StageSizes, StageInputOffsets, StageInputLengths, StageCount, LogicalBytesToRead)) {
        pIO->IsAvailable = true;
        return DDS_ERROR_CODE_INVALID_PARAM;
    }
    FileIOSizeT outputBytes = StageSizes[StageCount - 1];

    // include alignment metadata so frontend copies the right subrange.
    pIO->IsRead = true;
    pIO->IsRead2 = true;
    pIO->FileReference = file;
    pIO->FileId = FileId;
    pIO->Offset = Offset;
    pIO->BytesDesired = LogicalBytesToRead;
    pIO->OutputBytes = outputBytes;
    pIO->StageCount = StageCount;
    pIO->StageSizes[0] = StageSizes[0];
    // jason: clear stage2 size when running single-stage read2 to avoid stale slot metadata.
    pIO->StageSizes[1] = (StageCount > 1) ? StageSizes[1] : 0;
    pIO->LogicalBytesServiced = 0;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    pIO->OffloadReadTimeNs = 0;
    pIO->OffloadStage1TimeNs = 0;
    pIO->OffloadStage2TimeNs = 0;
#endif
    pIO->AppBuffer = DestBuffer;
    pIO->AppBufferArray = nullptr;
    pIO->AppCallback = nullptr;
    pIO->AppCallback2 = Callback;
    pIO->Context = Context;
    // Track aligned range and copy-start for backend-aligned read payloads.
    ComputeReadAlignment(pIO->Offset, pIO->BytesDesired, &pIO->AlignedOffset, &pIO->AlignedBytes, &pIO->CopyStart);
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->ReadFile2(
        FileId,
        Offset,
        DestBuffer,
        LogicalBytesToRead,
        StageSizes,
        StageInputOffsets,
        StageInputLengths,
        StageCount,
        BytesRead,
        LogicalBytesRead,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    return DDS_ERROR_CODE_IO_PENDING;
}
#else
#error "Unknown backend type"
#endif

#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
//
// Async read from a file with scattering
// 
//
ErrorCodeT
DDSFrontEnd::ReadFileScatter(
    FileIdT FileId,
    BufferT* DestBufferArray,
    FileSizeT Offset,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollT* poll = AllPolls[AllFiles[FileId]->PollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    //
    // Lock-free busy polling for completion
    //
    //
    bool expectedCompletion = true;
    while (pIO->IsComplete.compare_exchange_weak(
        expectedCompletion,
        false,
        std::memory_order_relaxed
    ) == false) {
        if (pIO->AppCallback == nullptr) {
            return DDS_ERROR_CODE_REQUIRE_POLLING;
        }
    }

    pIO->IsRead = true;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = Offset;
    pIO->AppBuffer = nullptr;
    pIO->AppBufferArray = DestBufferArray;
    pIO->BytesDesired = BytesToRead;
    pIO->AppCallback = Callback;
    pIO->Context = Context;

    result = BackEnd->ReadFileScatter(
        pIO->FileId,
        pIO->Offset,
        pIO->AppBufferArray,
        pIO->BytesDesired,
        BytesRead,
        pIO,
        poll
    );

    if (!Callback) {
        return DDS_ERROR_CODE_IO_PENDING;
    }

    return result;
}
#elif BACKEND_TYPE == BACKEND_TYPE_DPU
//
// Async read from a file with scattering
// - RequestId is pre-initialized per slot; assert slot mapping.
// - Publishes per-request metadata with IsAvailable (release) before submission.
// - Updated: compute alignment metadata for backend-aligned reads.
//
ErrorCodeT
DDSFrontEnd::ReadFileScatter(
    FileIdT FileId,
    BufferT* DestBufferArray,
    FileSizeT Offset,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollIdT pollId = AllFiles[FileId]->PollId;
    PollT* poll = AllPolls[pollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        //
        // If this is a callback-based completion,
        // perform polling once
        //
        //
        if (pIO->AppCallback) {
            FileIOSizeT bytesServiced;
            ContextT fileCtxt;
            ContextT ioCtxt;
            bool pollResult;

            PollWait(
                pollId,
                &bytesServiced,
                &fileCtxt,
                &ioCtxt,
                0,
                &pollResult
            );
        }

        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }

    pIO->IsRead = true;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = Offset;
    pIO->BytesDesired = BytesToRead;
    pIO->AppBuffer = nullptr;
    pIO->AppBufferArray = DestBufferArray;
    pIO->AppCallback = Callback;
    pIO->Context = Context;
    // jason: compute aligned range and copy-start for backend-aligned reads.
    ComputeReadAlignment(pIO->Offset, pIO->BytesDesired, &pIO->AlignedOffset, &pIO->AlignedBytes, &pIO->CopyStart);
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->ReadFileScatter(
        FileId,
        Offset,
        DestBufferArray,
        BytesToRead,
        nullptr,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    return DDS_ERROR_CODE_IO_PENDING;
}
#else
#error "Unknown backend type"
#endif

#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
//
// Async write to a file
// 
//
ErrorCodeT
DDSFrontEnd::WriteFile(
    FileIdT FileId,
    BufferT SourceBuffer,
    FileSizeT Offset,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollT* poll = AllPolls[AllFiles[FileId]->PollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    //
    // Lock-free busy polling for completion
    //
    //
    bool expectedCompletion = true;
    while (pIO->IsComplete.compare_exchange_weak(
        expectedCompletion,
        false,
        std::memory_order_relaxed
    ) == false) {
        if (pIO->AppCallback == nullptr) {
            return DDS_ERROR_CODE_REQUIRE_POLLING;
        }
    }

    pIO->IsRead = false;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = Offset;
    pIO->AppBuffer = DestBuffer;
    pIO->AppBufferArray = nullptr;
    pIO->BytesDesired = BytesToWrite;
    pIO->AppCallback = Callback;
    pIO->Context = Context;

    result = BackEnd->WriteFile(
        pIO->FileId,
        pIO->Offset,
        pIO->AppBuffer,
        pIO->BytesDesired,
        BytesWritten,
        pIO,
        poll
    );

    if (!Callback) {
        return DDS_ERROR_CODE_IO_PENDING;
    }

    return result;
}
#elif BACKEND_TYPE == BACKEND_TYPE_DPU
//
// Async write to a file
// - RequestId is pre-initialized per slot; assert slot mapping.
// - Publishes per-request metadata with IsAvailable (release) before submission.
//
ErrorCodeT
DDSFrontEnd::WriteFile(
    FileIdT FileId,
    BufferT SourceBuffer,
    FileSizeT Offset,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollIdT pollId = AllFiles[FileId]->PollId;
    PollT* poll = AllPolls[pollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        //
        // If this is a callback-based completion,
        // perform polling once
        //
        //
        if (pIO->AppCallback) {
            FileIOSizeT bytesServiced;
            ContextT fileCtxt;
            ContextT ioCtxt;
            bool pollResult;

            PollWait(
                pollId,
                &bytesServiced,
                &fileCtxt,
                &ioCtxt,
                0,
                &pollResult
            );
        }

        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }

    pIO->IsRead = false;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = Offset;
    pIO->BytesDesired = BytesToWrite;
    pIO->AppBuffer = nullptr;
    pIO->AppBufferArray = nullptr;
    pIO->AppCallback = Callback;
    pIO->Context = Context;
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->WriteFile(
        FileId,
        Offset,
        SourceBuffer,
        BytesToWrite,
        nullptr,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    return DDS_ERROR_CODE_IO_PENDING;
}
#else
#error "Unknown backend type"
#endif

#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
//
// Async write to a file with gathering
// 
//
ErrorCodeT
DDSFrontEnd::WriteFileGather(
    FileIdT FileId,
    BufferT* SourceBufferArray,
    FileSizeT Offset,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollT* poll = AllPolls[AllFiles[FileId]->PollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    //
    // Lock-free busy polling for completion
    //
    //
    bool expectedCompletion = true;
    while (pIO->IsComplete.compare_exchange_weak(
        expectedCompletion,
        false,
        std::memory_order_relaxed
    ) == false) {
        if (pIO->AppCallback == nullptr) {
            return DDS_ERROR_CODE_REQUIRE_POLLING;
        }
    }

    pIO->IsRead = false;
    pIO->IsSegmented = true;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = Offset;
    pIO->AppBuffer = nullptr;
    pIO->AppBufferArray = SourceBufferArray;
    pIO->BytesDesired = BytesToWrite;
    pIO->BytesServiced = 0;
    pIO->AppCallback = Callback;
    pIO->Context = Context;

    result = BackEnd->WriteFileGather(
        pIO->FileId,
        pIO->Offset,
        pIO->AppBufferArray,
        pIO->BytesDesired,
        &pIO->BytesServiced,
        pIO,
        poll
    );

    if (BytesWritten) {
        *BytesWritten = pIO->BytesServiced;
    }

    if (!Callback) {
        return DDS_ERROR_CODE_IO_PENDING;
    }

    return result;
}
#elif BACKEND_TYPE == BACKEND_TYPE_DPU
//
// Async write to a file with gathering
// - RequestId is pre-initialized per slot; assert slot mapping.
// - Publishes per-request metadata with IsAvailable (release) before submission.
//
ErrorCodeT
DDSFrontEnd::WriteFileGather(
    FileIdT FileId,
    BufferT* SourceBufferArray,
    FileSizeT Offset,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    PollIdT pollId = AllFiles[FileId]->PollId;
    PollT* poll = AllPolls[pollId];
    // Task 2.1: route to channel primary.
    PollChannelT &pch = poll->Channels[DDS_IO_CHANNEL_PRIMARY];
    size_t mySlot = pch.NextRequestSlot.fetch_add(1, std::memory_order_relaxed);
    pch.EnqueueCount.fetch_add(1, std::memory_order_relaxed);
    mySlot %= DDS_MAX_OUTSTANDING_IO_PER_CHANNEL;
    FileIOT *pIO = pch.OutstandingRequests[mySlot];

    bool expectedAvail = true;
    if (pIO->IsAvailable.compare_exchange_weak(
        expectedAvail,
        false,
        std::memory_order_relaxed
    ) == false) {
        //
        // If this is a callback-based completion,
        // perform polling once
        //
        //
        if (pIO->AppCallback) {
            FileIOSizeT bytesServiced;
            ContextT fileCtxt;
            ContextT ioCtxt;
            bool pollResult;

            PollWait(
                pollId,
                &bytesServiced,
                &fileCtxt,
                &ioCtxt,
                0,
                &pollResult
            );
        }
        return DDS_ERROR_CODE_TOO_MANY_REQUESTS;
    }

    pIO->IsRead = false;
    pIO->FileReference = AllFiles[FileId];
    pIO->FileId = FileId;
    pIO->Offset = Offset;
    pIO->BytesDesired = BytesToWrite;
    pIO->AppBuffer = nullptr;
    pIO->AppBufferArray = nullptr;
    pIO->AppCallback = Callback;
    pIO->Context = Context;
    // RequestId is pre-initialized per slot in poll setup.
    assert(pIO->RequestId == static_cast<RequestIdT>(mySlot));
    // jason: Publish request metadata before submission for PollWait to consume.
    pIO->IsAvailable.store(false, std::memory_order_release);

    result = BackEnd->WriteFileGather(
        FileId,
        Offset,
        SourceBufferArray,
        BytesToWrite,
        nullptr,
        pIO,
        poll
    );

    if (result != DDS_ERROR_CODE_IO_PENDING) {
        pIO->IsAvailable = true;
        return result;
    }

    //
    // Update file pointer
    //
    //
    ((DDSFile*)pIO->FileReference)->IncrementPointer(BytesToWrite);

    return DDS_ERROR_CODE_IO_PENDING;
}
#else
#error "Unknown backend type"
#endif

//
// Flush buffered data to storage
// Note: buffering is disabled
// 
//
ErrorCodeT
DDSFrontEnd::FlushFileBuffers(
    FileIdT FileId
) {
    return DDS_ERROR_CODE_SUCCESS;
}

//
// Search a directory for a file or subdirectory
// with a name that matches a specific name
// (or partial name if wildcards are used)
// jason: Prefer the front-end AllFiles[] cache when metadata is in sync to avoid
// extra backend FindFirstFile calls; fallback to backend on a cache miss.
// jason: Opportunistically populate AllFiles[] when the backend returns a match,
// using backend file properties to avoid defaulted access/share metadata.
// jason: Uses PopulateFrontendFileFromBackend to share population logic.
//
ErrorCodeT DDSFrontEnd::FindFirstFile(const char *FileName, FileIdT *FileId)
{
    // jason: Original backend-only lookup preserved for reference.
    // ErrorCodeT result = BackEnd->FindFirstFile(FileName, FileId);
    // if (result != DDS_ERROR_CODE_SUCCESS) {
    //     return result;
    // }
    //
    // if (*FileId >= DDS_MAX_FILES) {
    //     return DDS_ERROR_CODE_FILE_NOT_FOUND;
    // }
    //
    // //
    // // Opportunistically populate the front-end file table if missing.
    // //
    // //
    // if (!AllFiles[*FileId])
    // {
    //     ErrorCodeT infoResult = PopulateFrontendFileFromBackend(*FileId);
    //     if (infoResult != DDS_ERROR_CODE_SUCCESS)
    //     {
    //         return infoResult;
    //     }
    // }
    //
    // return result;

    // jason: Prefer the front-end file table to avoid extra backend calls when metadata is in sync.
    if (FileName && FileId)
    {
        for (FileIdT id = 0; id != FileIdEnd; id++)
        {
            // NOTE: Only consider populated entries; names are tracked in DDSFile.
            if (AllFiles[id] && strcmp(FileName, AllFiles[id]->GetName()) == 0)
            {
                *FileId = id;
                return DDS_ERROR_CODE_SUCCESS;
            }
        }
    }

    // jason: Fallback to backend lookup if not found in the front-end table.
    ErrorCodeT result = BackEnd->FindFirstFile(FileName, FileId);
    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    if (*FileId >= DDS_MAX_FILES) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    //
    // Opportunistically populate the front-end file table if missing.
    //
    //
    if (!AllFiles[*FileId])
    {
        ErrorCodeT infoResult = PopulateFrontendFileFromBackend(*FileId);
        if (infoResult != DDS_ERROR_CODE_SUCCESS)
        {
            return infoResult;
        }
    }

    return result;
}

//
// Continue a file search from a previous call
// to the FindFirstFile
// 
//
ErrorCodeT
DDSFrontEnd::FindNextFile(
    FileIdT LastFileId,
    FileIdT* FileId
) {
    //
    // TODO: support wild card search
    //
    //
    *FileId = DDS_FILE_INVALID;

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get file properties by file id
// 
//
ErrorCodeT
DDSFrontEnd::GetFileInformationById(
    FileIdT FileId,
    FilePropertiesT* FileProperties
) {
    DDSFile* pFile = AllFiles[FileId];
    FileProperties->Access = pFile->GetAccess();
    FileProperties->Position = pFile->GetPointer();
    FileProperties->ShareMode = pFile->GetShareMode();
    FileProperties->FileAttributes = pFile->GetAttributes();
    FileProperties->CreationTime = pFile->GetCreationTime();
    FileProperties->LastAccessTime = pFile->GetLastAccessTime();
    FileProperties->LastWriteTime = pFile->GetLastWriteTime();

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get file attributes by file name
// 
//
ErrorCodeT
DDSFrontEnd::GetFileAttributes(
    const char* FileName,
    FileAttributesT* FileAttributes
) {
    FileIdT id = DDS_FILE_INVALID;

    for (FileIdT i = 0; i != FileIdEnd; i++) {
        if (AllFiles[i] && strcmp(FileName, AllFiles[i]->GetName()) == 0) {
            id = i;
            break;
        }
    }

    if (id == DDS_FILE_INVALID) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    //
    // File attributes might be updated on the DPU, so get it from back end
    //
    //
    return BackEnd->GetFileAttributes(id, FileAttributes);
}

//
// Retrieve the current directory for the
// current process
// 
//
ErrorCodeT
DDSFrontEnd::GetCurrentDirectory(
    unsigned BufferLength,
    BufferT DirectoryBuffer
) {
    //
    // TODO: this function is more like an OS feature
    // check whether it is necessary
    //
    //
    DDSDir* pDir = AllDirs[DDS_DIR_ROOT];
    if (strlen(pDir->GetName()) > BufferLength) {
        return DDS_ERROR_CODE_BUFFER_OVERFLOW;
    }

    strcpy((char*)DirectoryBuffer, pDir->GetName());

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get the size of free space on the storage
// 
//
ErrorCodeT
DDSFrontEnd::GetStorageFreeSpace(
    FileSizeT* StorageFreeSpace
) {
    return BackEnd->GetStorageFreeSpace(StorageFreeSpace);
}

//
// Lock the specified file for exclusive
// access by the calling process
// 
//
ErrorCodeT
DDSFrontEnd::LockFile(
    FileIdT FileId,
    PositionInFileT FileOffset,
    PositionInFileT NumberOfBytesToLock
) {
    return DDS_ERROR_CODE_NOT_IMPLEMENTED;
}

//
// Unlock a region in an open file;
// Unlocking a region enables other
// processes to access the region
// 
//
ErrorCodeT
DDSFrontEnd::UnlockFile(
    FileIdT FileId,
    PositionInFileT FileOffset,
    PositionInFileT NumberOfBytesToUnlock
) {
    return DDS_ERROR_CODE_NOT_IMPLEMENTED;
}

//
// Move an existing file or a directory,
// including its children
// 
//
ErrorCodeT
DDSFrontEnd::MoveFile(
    const char* ExistingFileName,
    const char* NewFileName
) {
    FileIdT id = DDS_FILE_INVALID;
    DDSFile* pFile = NULL;

    for (FileIdT i = 0; i != FileIdEnd; i++) {
        if (AllFiles[i] && strcmp(ExistingFileName, AllFiles[i]->GetName()) == 0) {
            id = i;
            pFile = AllFiles[i];
            break;
        }
    }

    if (id == DDS_FILE_INVALID) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    pFile->SetName(NewFileName);

    //
    // Reflect the update on back end
    //
    //
    return BackEnd->MoveFile(id, NewFileName);
}

//
// Get the default poll structure for async I/O
//
//
ErrorCodeT
DDSFrontEnd::GetDefaultPoll(
    PollIdT* PollId
) {
    *PollId = DDS_POLL_DEFAULT;

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Create a poll structure for async I/O
// - Initialize per-slot RequestId for DPU ring correlation.
//
ErrorCodeT
DDSFrontEnd::PollCreate(
    PollIdT* PollId
) {
    //
    // Retrieve an id
    // TODO: concurrent support
    //
    //
    PollIdT id = 0;
    for (; id != DDS_MAX_POLLS; id++) {
        if (!AllPolls[id]) {
            break;
        }
    }

    if (id == DDS_MAX_POLLS) {
        return DDS_ERROR_CODE_TOO_MANY_POLLS;
    }

    PollT* poll = new PollT();
    for (size_t i = 0; i != DDS_MAX_OUTSTANDING_IO; i++) {
        poll->OutstandingRequests[i] = new FileIOT();
        // Per-slot request id is used to correlate ring responses.
        // poll->OutstandingRequests[i]->RequestId = (RequestIdT)i;
#if BACKEND_TYPE == BACKEND_TYPE_DPU
        poll->OutstandingRequests[i]->RequestId = (RequestIdT)i;
#endif
    }
    poll->NextRequestSlot = 0;

#if BACKEND_TYPE == BACKEND_TYPE_DPU
    // Task 2.1: allocate per-channel FileIOT objects for new polls.
    for (int ch = 0; ch < DDS_NUM_IO_CHANNELS; ch++)
    {
        for (size_t i = 0; i < DDS_MAX_OUTSTANDING_IO_PER_CHANNEL; i++)
        {
            poll->Channels[ch].OutstandingRequests[i] = new FileIOT();
            poll->Channels[ch].OutstandingRequests[i]->RequestId = (RequestIdT)i;
        }
        poll->Channels[ch].NextRequestSlot = 0;
        poll->Channels[ch].ChannelIndex = ch;
    }
#endif

    AllPolls[id] = poll;

    *PollId = id;

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Delete a poll structure
//
//
ErrorCodeT
DDSFrontEnd::PollDelete(
    PollIdT PollId
) {
    if (PollId == DDS_POLL_DEFAULT) {
        return DDS_ERROR_CODE_INVALID_POLL_DELETION;
    }

    PollT* poll = AllPolls[PollId];
    if (!poll) {
        return DDS_ERROR_CODE_INVALID_POLL_DELETION;
    }

    for (size_t i = 0; i != DDS_MAX_OUTSTANDING_IO; i++) {
        delete poll->OutstandingRequests[i];
    }
#if BACKEND_TYPE == BACKEND_TYPE_DPU
    // Task 2.1: clean up per-channel FileIOT objects.
    for (int ch = 0; ch < DDS_NUM_IO_CHANNELS; ch++)
    {
        for (size_t i = 0; i < DDS_MAX_OUTSTANDING_IO_PER_CHANNEL; i++)
        {
            delete poll->Channels[ch].OutstandingRequests[i];
            poll->Channels[ch].OutstandingRequests[i] = nullptr;
        }
    }
#endif
    delete poll;
    AllPolls[PollId] = nullptr;

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Add a file to a pool
//
//
ErrorCodeT
DDSFrontEnd::PollAdd(
    FileIdT FileId,
    PollIdT PollId,
    ContextT FileContext
) {
    DDSFile* pFile = AllFiles[FileId];

    if (PollId != pFile->PollId) {
        pFile->PollId = PollId;
    }

    pFile->PollContext = FileContext;

    return DDS_ERROR_CODE_SUCCESS;
}

#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
//
// Poll a completion event
// - For DPU backend, update front-end file size on successful write completion.
// - Acquire published request metadata before reading per-IO context/callback.
//
ErrorCodeT
DDSFrontEnd::PollWait(
    PollIdT PollId,
    FileIOSizeT* BytesServiced,
    ContextT* FileContext,
    ContextT* IOContext,
    size_t WaitTime,
    bool* PollResult
) {
    std::chrono::time_point<std::chrono::system_clock> begin, cur;

    PollT* poll = AllPolls[PollId];

    *PollResult = false;

    begin = std::chrono::system_clock::now();

    size_t sleepTimeInUs = 1;

    while (true) {
        for (size_t i = 0; i != DDS_MAX_OUTSTANDING_IO; i++) {
            FileIOT* io = poll->OutstandingRequests[i];

            bool expectedCompletion = true;
            if (io->IsReadyForPoll.compare_exchange_weak(
                expectedCompletion,
                false,
                std::memory_order_relaxed
            )) {
                *BytesServiced = io->BytesServiced;
                *FileContext = AllFiles[io->FileId]->PollContext;
                *IOContext = io->Context;
                *PollResult = true;

                io->IsComplete = true;

                break;
            }
        }

        if (*PollResult) {
            break;
        }

        cur = std::chrono::system_clock::now();
        if ((size_t)std::chrono::duration_cast<std::chrono::milliseconds>(cur - begin).count() >= WaitTime) {
            break;
        }

        //
        // Adaptively yeild the CPU usage until the tolerated latency upper bound
        //
        //
        std::this_thread::sleep_for(std::chrono::microseconds(sleepTimeInUs));
        if (sleepTimeInUs < DDS_POLL_MAX_LATENCY_MICROSECONDS) {
            sleepTimeInUs *= 2;
        }
    }

    if (!(*PollResult)) {
        *FileContext = nullptr;
        *IOContext = nullptr;
    }

    return DDS_ERROR_CODE_SUCCESS;
}
#elif BACKEND_TYPE == BACKEND_TYPE_DPU
//
// Poll a completion event
// - For DPU backend, update front-end file size on successful write completion.
// - Acquire published request metadata before reading per-IO context/callback.
//
ErrorCodeT
DDSFrontEnd::PollWait(
    PollIdT PollId,
    FileIOSizeT* BytesServiced,
    ContextT* FileContext,
    ContextT* IOContext,
    size_t WaitTime,
    bool* PollResult
) {
    std::chrono::time_point<std::chrono::system_clock> begin, cur;

    PollT* poll = AllPolls[PollId];
    RequestIdT reqId;
    // Task 2.3: GetResponse returns channel index for per-channel OutstandingRequests lookup.
    int completionChannel = DDS_IO_CHANNEL_PRIMARY;

    // printf("poll wait 1\n");
    ErrorCodeT result = BackEnd->GetResponse(poll, WaitTime, BytesServiced, &reqId, &completionChannel);

    if (result == DDS_ERROR_CODE_NO_COMPLETION) {
        *PollResult = false;
        *FileContext = nullptr;
        *IOContext = nullptr;
    }
    else {
        // Task 2.3: look up from the channel that produced the completion.
        FileIOT *io = poll->Channels[completionChannel].OutstandingRequests[reqId];
        // Task 2.4: increment per-channel completion counter.
        poll->Channels[completionChannel].CompletionCount.fetch_add(1, std::memory_order_relaxed);
        // jason: Acquire request metadata published by submitter (Context/AppCallback).
        (void)io->IsAvailable.load(std::memory_order_acquire);

        // *FileContext = AllFiles[io->FileId]->PollContext;
        *FileContext = nullptr;

        *IOContext = io->Context;
        *PollResult = true;

        //
        // Update front-end size on successful write completion.
        // Use max(currentSize, offset + bytes) to handle out-of-order completions.
        //
        //
        if (result == DDS_ERROR_CODE_SUCCESS && !io->IsRead) {
            DDSFile* pFile = (DDSFile*)io->FileReference;
            if (pFile) {
                FileSizeT completedEnd = io->Offset + (FileSizeT)(*BytesServiced);
                FileSizeT currentSize = pFile->GetSize();
                if (completedEnd > currentSize) {
                    pFile->SetSize(completedEnd);
                }
            }
        }

        //
        // If the application provides a callback, we should handle it here
        // because this poll might be invoked by a read/write call
        //
        //
        if (io->AppCallback2) {
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
            io->AppCallback2(result, *BytesServiced, io->LogicalBytesServiced, io->OffloadReadTimeNs,
                             io->OffloadStage1TimeNs, io->OffloadStage2TimeNs, io->Context);
#else
            io->AppCallback2(result, *BytesServiced, io->LogicalBytesServiced, io->Context);
#endif
        } else if (io->AppCallback) {
            // jason: legacy callback uses BytesServiced only.
            io->AppCallback(result, *BytesServiced, io->Context); // pass reqId to the call back
        }

        //
        // Mark this slot as available
        //
        //
        io->IsAvailable.store(true);
    }
    // printf("Pollwait returns\n");
    return DDS_ERROR_CODE_SUCCESS;
}
#else
#error "Unknown backend type"
#endif

}
