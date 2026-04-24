#include "DPUBackEndStorage.h"
#include <assert.h>

//
// Helper to translate file offsets into physical segments using the per-file map.
//
/**
 * Translate a file offset to a physical segment id and segment-local offset.
 * Returns DDS_ERROR_CODE_INVALID_FILE_POSITION if the segment map is missing.
 */
static ErrorCodeT TranslateFileOffsetToSegment(struct DPUFile *File, FileSizeT Offset, SegmentIdT *OutSegmentId,
                                               SegmentSizeT *OutSegmentOffset)
{
    // Guard against invalid inputs and overflow in logical index.
    if (!File || !OutSegmentId || !OutSegmentOffset)
    {
        return DDS_ERROR_CODE_INVALID_FILE_POSITION;
    }

    size_t logicalIndex = (size_t)(Offset / DDS_BACKEND_SEGMENT_SIZE);
    if (logicalIndex >= DDS_BACKEND_MAX_SEGMENTS_PER_FILE)
    {
        return DDS_ERROR_CODE_INVALID_FILE_POSITION;
    }

    SegmentIdT segId = File->Properties.Segments[logicalIndex];
    if (segId == DDS_BACKEND_SEGMENT_INVALID)
    {
        return DDS_ERROR_CODE_INVALID_FILE_POSITION;
    }

    *OutSegmentId = segId;
    *OutSegmentOffset = (SegmentSizeT)(Offset % DDS_BACKEND_SEGMENT_SIZE);
    return DDS_ERROR_CODE_SUCCESS;
}

//
// Helper to grow/shrink a file's segment map with proper bookkeeping.
//
/**
 * Ensure the file has enough segments to cover NewSize, allocate (or deallocate) segments as needed.
 * Updates Sto->AllSegments ownership and Sto->AvailableSegments.
 * SegmentsChanged is set when the segment list changes.
 */
static ErrorCodeT EnsureFileSegmentsForSize(struct DPUStorage *Sto, struct DPUFile *File, FileIdT FileId,
                                            FileSizeT NewSize, bool *SegmentsChanged)
{
    if (!Sto || !File)
    {
        return DDS_ERROR_CODE_INVALID_FILE_POSITION;
    }

    if (SegmentsChanged)
    {
        *SegmentsChanged = false;
    }

    FileSizeT currentFileSize = GetFileProperties(File)->FProperties.FileSize;
    long long sizeToChange = (long long)NewSize - (long long)currentFileSize;

    if (sizeToChange >= 0)
    {
        size_t numSeg = GetNumSegments(File);
        // Use FileSizeT to avoid overflow when segments exceed 4GB.
        FileSizeT remainingAllocatedSize = (FileSizeT)numSeg * (FileSizeT)DDS_BACKEND_SEGMENT_SIZE - currentFileSize;
        long long bytesToBeAllocated = sizeToChange - (long long)remainingAllocatedSize;

        if (bytesToBeAllocated > 0)
        {
            SegmentIdT numSegmentsToAllocate =
                (SegmentIdT)(bytesToBeAllocated / DDS_BACKEND_SEGMENT_SIZE +
                             (bytesToBeAllocated % DDS_BACKEND_SEGMENT_SIZE == 0 ? 0 : 1));

            // Cap by per-file segment map size.
            if ((size_t)numSegmentsToAllocate + numSeg > DDS_BACKEND_MAX_SEGMENTS_PER_FILE)
            {
                return DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE;
            }

            pthread_mutex_lock(&Sto->SegmentAllocationMutex);

            if (numSegmentsToAllocate > Sto->AvailableSegments)
            {
                pthread_mutex_unlock(&Sto->SegmentAllocationMutex);
                return DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE;
            }

            SegmentIdT allocatedSegments = 0;
            for (SegmentIdT s = 0; s != Sto->TotalSegments; s++)
            {
                SegmentT *seg = &Sto->AllSegments[s];
                // jason: NOTE: seg->FileId is the real check if the seg is allocated; seg->Allocatable is just to not
                // touch (the) reserved segment
                if (seg->Allocatable && seg->FileId == DDS_FILE_INVALID)
                {
                    AllocateSegment(s, File);
                    seg->FileId = FileId;
                    Sto->AvailableSegments--;
                    allocatedSegments++;
                    if (SegmentsChanged)
                    {
                        *SegmentsChanged = true;
                    }
                    if (allocatedSegments == numSegmentsToAllocate)
                    {
                        break;
                    }
                }
            }

            if (allocatedSegments != numSegmentsToAllocate)
            {
                // Roll back partial allocation to keep metadata consistent.
                for (SegmentIdT i = 0; i < allocatedSegments; i++)
                {
                    SegmentIdT segId = File->Properties.Segments[File->NumSegments - 1];
                    DeallocateSegment(File);
                    if (segId != DDS_BACKEND_SEGMENT_INVALID)
                    {
                        Sto->AllSegments[segId].FileId = DDS_FILE_INVALID;
                        Sto->AvailableSegments++;
                    }
                }
                pthread_mutex_unlock(&Sto->SegmentAllocationMutex);
                return DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE;
            }

            pthread_mutex_unlock(&Sto->SegmentAllocationMutex);
        }
    }
    else
    {
        SegmentIdT newSegments =
            (SegmentIdT)(NewSize / DDS_BACKEND_SEGMENT_SIZE + (NewSize % DDS_BACKEND_SEGMENT_SIZE == 0 ? 0 : 1));
        SegmentIdT numSegmentsToDeallocate = GetNumSegments(File) - newSegments;
        if (numSegmentsToDeallocate > 0)
        {
            pthread_mutex_lock(&Sto->SegmentAllocationMutex);

            for (SegmentIdT s = 0; s != numSegmentsToDeallocate; s++)
            {
                SegmentIdT segId = File->Properties.Segments[File->NumSegments - 1];
                DeallocateSegment(File);
                if (segId != DDS_BACKEND_SEGMENT_INVALID)
                {
                    if (Sto->AllSegments[segId].FileId == FileId)
                    {
                        Sto->AllSegments[segId].FileId = DDS_FILE_INVALID;
                        Sto->AvailableSegments++;
                        if (SegmentsChanged)
                        {
                            *SegmentsChanged = true;
                        }
                    }
                }
            }

            pthread_mutex_unlock(&Sto->SegmentAllocationMutex);
        }
    }

    return DDS_ERROR_CODE_SUCCESS;
}

/*
 * GetControlPlaneDmaAlign
 *
 * Return the DMA alignment for control-plane buffers, using the cached bdev
 * buffer alignment when available, with a conservative fallback.
 */
static inline FileIOSizeT
GetControlPlaneDmaAlign(
    SPDKContextT *SPDKContext
){
    if (SPDKContext && SPDKContext->buf_align) {
        return (FileIOSizeT)SPDKContext->buf_align;
    }
    return 4096;
}

/*
 * AllocateControlPlaneDmaBuffer
 *
 * Allocate a DMA-safe buffer for control-plane I/O using SPDK DMA APIs,
 * optionally zeroing the buffer for read/initialize use.
 */
static inline void*
AllocateControlPlaneDmaBuffer(
    SPDKContextT *SPDKContext,
    FileIOSizeT Size,
    bool ZeroFill
){
    FileIOSizeT align = GetControlPlaneDmaAlign(SPDKContext);
    if (ZeroFill) {
        return spdk_dma_zmalloc(Size, align, NULL);
    }
    return spdk_dma_malloc(Size, align, NULL);
}

/*
 * ControlPlaneDmaWriteCtx
 *
 * Tracks a DMA staging buffer for control-plane writes and forwards the
 * completion to the original callback after releasing DMA resources.
 */
typedef struct ControlPlaneDmaWriteCtx {
    DiskIOCallback UserCallback;
    ContextT UserContext;
    void *DmaBuffer;
} ControlPlaneDmaWriteCtx;

/*
 * ControlPlaneDmaWriteCallback
 *
 * Release the DMA staging buffer and invoke the original completion callback.
 */
static void
ControlPlaneDmaWriteCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
){
    ControlPlaneDmaWriteCtx *DmaCtx = (ControlPlaneDmaWriteCtx *)Context;
    DiskIOCallback UserCallback = DmaCtx->UserCallback;
    ContextT UserContext = DmaCtx->UserContext;

    // jason: free DMA staging buffer before invoking the user callback.
    spdk_dma_free(DmaCtx->DmaBuffer);
    free(DmaCtx);

    if (UserCallback) {
        UserCallback(bdev_io, Success, UserContext);
    } else {
        // jason: avoid leaking I/O if caller passed no callback.
        spdk_bdev_free_io(bdev_io);
    }
}

/*
 * InitializeInvalidEntryCtx
 *
 * Tracks a DMA staging buffer for writing invalid directory/file entries
 * during initialization and coordinates completion for the async chain.
 */
typedef struct InitializeInvalidEntryCtx
{
    struct InitializeCtx *InitCtx;
    atomic_int *PendingWrites;
    void *DmaBuffer;
} InitializeInvalidEntryCtx;

/*
 * BuildInvalidDirProperties
 *
 * Populate a DPUDirPropertiesT buffer with invalid sentinel values so
 * zeroed slots do not appear as valid root directories on restart.
 */
static void BuildInvalidDirProperties(DPUDirPropertiesT *Props)
{
    if (!Props)
    {
        return;
    }

    memset(Props, 0, sizeof(*Props));
    Props->Id = DDS_DIR_INVALID;
    Props->Parent = DDS_DIR_INVALID;
    Props->NumFiles = 0;
    Props->Name[0] = '\0';
    for (size_t f = 0; f != DDS_MAX_FILES_PER_DIR; f++)
    {
        Props->Files[f] = DDS_FILE_INVALID;
    }
}

/*
 * BuildInvalidFileProperties
 *
 * Populate a DPUFilePropertiesT buffer with invalid sentinel values so
 * zeroed slots do not appear as valid files on restart.
 */
static void BuildInvalidFileProperties(DPUFilePropertiesT *Props)
{
    if (!Props)
    {
        return;
    }

    memset(Props, 0, sizeof(*Props));
    Props->Id = DDS_FILE_INVALID;
    Props->FProperties.FileAttributes = 0;
    Props->FProperties.FileSize = 0;
    Props->FProperties.CreationTime = 0;
    Props->FProperties.LastAccessTime = 0;
    Props->FProperties.LastWriteTime = 0;
    Props->FProperties.FileName[0] = '\0';
    for (size_t s = 0; s != DDS_BACKEND_MAX_SEGMENTS_PER_FILE; s++)
    {
        Props->Segments[s] = DDS_BACKEND_SEGMENT_INVALID;
    }
}

/*
 * InitializeInvalidEntryWriteCallback
 *
 * Releases the DMA buffer and advances the initialization chain once all
 * invalid entries have been written.
 */
static void InitializeInvalidEntryWriteCallback(struct spdk_bdev_io *bdev_io, bool Success, ContextT Context)
{
    InitializeInvalidEntryCtx *Ctx = Context;
    spdk_bdev_free_io(bdev_io);
    if (!Success)
    {
        Ctx->InitCtx->FailureStatus->HasFailed = true;
        Ctx->InitCtx->FailureStatus->HasAborted = true;
        SPDK_ERRLOG("InitializeInvalidEntryWriteCallback: IO failed\n");
        exit(-1);
    }

    if (Ctx->DmaBuffer)
    {
        spdk_dma_free(Ctx->DmaBuffer);
        Ctx->DmaBuffer = NULL;
    }

    int remaining = atomic_fetch_sub(Ctx->PendingWrites, 1) - 1;
    if (remaining == 0)
    {
        // Last invalid entry write finished; continue initialization.
        free(Ctx->PendingWrites);
        Ctx->PendingWrites = NULL;

        // Create root directory after invalid entries are in place.
        struct DPUDir *rootDir = BackEndDirI(DDS_DIR_ROOT, DDS_DIR_INVALID, DDS_BACKEND_ROOT_DIR_NAME);
        Ctx->InitCtx->RootDir = rootDir;
        ErrorCodeT result =
            SyncDirToDisk(rootDir, Sto, Ctx->InitCtx->SPDKContext, InitializeSyncDirToDiskCallback, Ctx->InitCtx);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            Ctx->InitCtx->FailureStatus->HasFailed = true;
            Ctx->InitCtx->FailureStatus->HasAborted = true;
            SPDK_ERRLOG("InitializeInvalidEntryWriteCallback: SyncDirToDisk failed\n");
            exit(-1);
        }
    }

    free(Ctx);
}

/*
 * InitializeWriteInvalidEntries
 *
 * Write invalid directory/file entries to reserved segment so empty slots
 * are explicitly marked as invalid rather than zeroed.
 */
static ErrorCodeT InitializeWriteInvalidEntries(struct DPUStorage *Sto, struct InitializeCtx *Ctx)
{
    if (!Sto || !Ctx)
    {
        SPDK_ERRLOG("InitializeWriteInvalidEntries: invalid args\n");
        return DDS_ERROR_CODE_IO_FAILURE;
    }

    const int totalWrites = DDS_MAX_DIRS + DDS_MAX_FILES;
    atomic_int *pending = malloc(sizeof(*pending));
    if (!pending)
    {
        SPDK_ERRLOG("InitializeWriteInvalidEntries: pending alloc failed\n");
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }
    atomic_init(pending, totalWrites);

    // Initialize all directory slots as invalid.
    for (size_t d = 0; d != DDS_MAX_DIRS; d++)
    {
        InitializeInvalidEntryCtx *EntryCtx = malloc(sizeof(*EntryCtx));
        if (!EntryCtx)
        {
            SPDK_ERRLOG("InitializeWriteInvalidEntries: dir ctx alloc failed\n");
            free(pending);
            return DDS_ERROR_CODE_OUT_OF_MEMORY;
        }
        EntryCtx->InitCtx = Ctx;
        EntryCtx->PendingWrites = pending;
        EntryCtx->DmaBuffer = AllocateControlPlaneDmaBuffer(Ctx->SPDKContext, sizeof(DPUDirPropertiesT), true);
        if (!EntryCtx->DmaBuffer)
        {
            free(EntryCtx);
            SPDK_ERRLOG("InitializeWriteInvalidEntries: dir DMA alloc failed\n");
            free(pending);
            return DDS_ERROR_CODE_OUT_OF_MEMORY;
        }

        BuildInvalidDirProperties((DPUDirPropertiesT *)EntryCtx->DmaBuffer);
        SegmentSizeT dirOffset = DDS_BACKEND_SECTOR_SIZE + (SegmentSizeT)(d * sizeof(DPUDirPropertiesT));
        ErrorCodeT result = WriteToDiskAsyncZC((BufferT)EntryCtx->DmaBuffer, DDS_BACKEND_RESERVED_SEGMENT, dirOffset,
                                               sizeof(DPUDirPropertiesT), InitializeInvalidEntryWriteCallback, EntryCtx,
                                               Sto, Ctx->SPDKContext);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            SPDK_ERRLOG("InitializeWriteInvalidEntries: dir write failed\n");
            return result;
        }
    }

    // Initialize all file slots as invalid.
    for (size_t f = 0; f != DDS_MAX_FILES; f++)
    {
        InitializeInvalidEntryCtx *EntryCtx = malloc(sizeof(*EntryCtx));
        if (!EntryCtx)
        {
            SPDK_ERRLOG("InitializeWriteInvalidEntries: file ctx alloc failed\n");
            free(pending);
            return DDS_ERROR_CODE_OUT_OF_MEMORY;
        }
        EntryCtx->InitCtx = Ctx;
        EntryCtx->PendingWrites = pending;
        EntryCtx->DmaBuffer = AllocateControlPlaneDmaBuffer(Ctx->SPDKContext, sizeof(DPUFilePropertiesT), true);
        if (!EntryCtx->DmaBuffer)
        {
            free(EntryCtx);
            SPDK_ERRLOG("InitializeWriteInvalidEntries: file DMA alloc failed\n");
            free(pending);
            return DDS_ERROR_CODE_OUT_OF_MEMORY;
        }

        BuildInvalidFileProperties((DPUFilePropertiesT *)EntryCtx->DmaBuffer);
        SegmentSizeT fileOffset = DDS_BACKEND_SEGMENT_SIZE - (SegmentSizeT)((f + 1) * sizeof(DPUFilePropertiesT));
        ErrorCodeT result = WriteToDiskAsyncZC((BufferT)EntryCtx->DmaBuffer, DDS_BACKEND_RESERVED_SEGMENT, fileOffset,
                                               sizeof(DPUFilePropertiesT), InitializeInvalidEntryWriteCallback,
                                               EntryCtx, Sto, Ctx->SPDKContext);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            SPDK_ERRLOG("InitializeWriteInvalidEntries: file write failed\n");
            return result;
        }
    }

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Constructor
// 
//
struct DPUStorage* BackEndStorage(){
    struct DPUStorage *tmp;
    tmp = malloc(sizeof(struct DPUStorage));
    tmp->AllSegments = NULL;
    tmp->AvailableSegments = 0;

    for (size_t d = 0; d != DDS_MAX_DIRS; d++) {
        tmp->AllDirs[d] = NULL;
    }

    for (size_t f = 0; f != DDS_MAX_FILES; f++) {
        tmp->AllFiles[f] = NULL;
    }

    tmp->TotalDirs = 0;
    tmp->TotalFiles = 0;

    tmp->TotalSegments = DDS_BACKEND_CAPACITY / DDS_BACKEND_SEGMENT_SIZE;
    pthread_mutex_init(&tmp->SectorModificationMutex, NULL);
    pthread_mutex_init(&tmp->SegmentAllocationMutex, NULL);
    return tmp;
}

//
// Destructor
// 
//
void DeBackEndStorage(
    struct DPUStorage* Sto
){
    //
    // Free all bufffers
    //
    //
    for (size_t d = 0; d != DDS_MAX_DIRS; d++) {
        if (Sto->AllDirs[d] != NULL) {
            free(Sto->AllDirs[d]);
        }
    }

    for (size_t f = 0; f != DDS_MAX_FILES; f++) {
        if (Sto->AllFiles[f] != NULL) {
            free(Sto->AllFiles[f]);
        }
    }

    //
    // Return all segments
    //
    //
    ReturnSegments(Sto);
}

ErrorCodeT ReadvFromDiskAsyncZC(
    struct iovec *Iov,
    int IovCnt,
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes,
    DiskIOCallback Callback,
    ContextT Context,
    struct DPUStorage* Sto,
    void *SPDKContext
) {
    SegmentT* seg = &Sto->AllSegments[SegmentId];
    int rc;
    rc = BdevReadV(SPDKContext, Iov, IovCnt, seg->DiskAddress + SegmentOffset, Bytes, 
        Callback, Context);

    if (rc)
    {
        printf("ReadFromDiskAsync called BdevRead(), but failed with: %d\n", rc);
        return DDS_ERROR_CODE_INVALID_FILE_POSITION;  // there should be an SPDK error log before this
    }

    return DDS_ERROR_CODE_SUCCESS; // since it's async, we don't know if the actual read will be successful
}

ErrorCodeT ReadvFromDiskAsyncNonZC(
    struct iovec *Iov,
    int IovCnt,
    FileIOSizeT NonZCBuffOffset,
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes,
    DiskIOCallback Callback,
    struct PerSlotContext *SlotContext,
    struct DPUStorage* Sto,
    void *SPDKContext
) {
    SegmentT* seg = &Sto->AllSegments[SegmentId];
    int rc;
    int position = SlotContext->Position;
    SPDKContextT *arg = SPDKContext;
    if (Bytes > DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE) {
        SPDK_WARNLOG("A read with %u bytes exceeds buff block space: %llu\n", Bytes, DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE);
    }

    rc = BdevRead(SPDKContext, (&arg->buff[position * DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE]), 
    seg->DiskAddress + SegmentOffset, Bytes, Callback, SlotContext);

    if (rc)
    {
        printf("ReadFromDiskAsync called BdevRead(), but failed with: %d\n", rc);
        return DDS_ERROR_CODE_INVALID_FILE_POSITION;  // there should be an SPDK error log before this
    }

    return DDS_ERROR_CODE_SUCCESS; // since it's async, we don't know if the actual read will be successful
}


ErrorCodeT ReadFromDiskAsyncZC(
    BufferT DstBuffer,
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes,
    DiskIOCallback Callback,
    ContextT Context,
    struct DPUStorage* Sto,
    void *SPDKContext
){
    SegmentT* seg = &Sto->AllSegments[SegmentId];
    int rc;
    rc = BdevRead(SPDKContext, DstBuffer, seg->DiskAddress + SegmentOffset, Bytes, 
        Callback, Context);

    if (rc)
    {
        printf("ReadFromDiskAsync called BdevRead(), but failed with: %d\n", rc);
        return DDS_ERROR_CODE_INVALID_FILE_POSITION;  // there should be an SPDK error log before this
    }

    return DDS_ERROR_CODE_SUCCESS; // since it's async, we don't know if the actual read will be successful
}

//
// Read from disk asynchronously, FileIOSizeT BytesRead is how many we already read, only used for non zero copy
//
//
ErrorCodeT ReadFromDiskAsyncNonZC(
    FileIOSizeT NonZCBuffOffset,  // how many we already read
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes,
    DiskIOCallback Callback,
    struct PerSlotContext *SlotContext,
    struct DPUStorage* Sto,
    void *SPDKContext
){
    SegmentT* seg = &Sto->AllSegments[SegmentId];
    int rc;
    int position = SlotContext->Position;
    SPDKContextT *arg = SPDKContext;
    if (Bytes > DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE) {
        SPDK_WARNLOG("A read with %u bytes exceeds buff block space: %llu\n", Bytes, DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE);
    }
    rc = BdevRead(SPDKContext, (&arg->buff[position * DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE]) + NonZCBuffOffset, 
    seg->DiskAddress + SegmentOffset, Bytes, Callback, SlotContext);

    if (rc)
    {
        printf("ReadFromDiskAsync called BdevRead(), but failed with: %d\n", rc);
        return DDS_ERROR_CODE_INVALID_FILE_POSITION;  // there should be an SPDK error log before this
    }

    return DDS_ERROR_CODE_SUCCESS; // since it's async, we don't know if the actual read will be successful
}


ErrorCodeT WritevToDiskAsyncZC(
    struct iovec *Iov,
    int IovCnt,
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes,
    DiskIOCallback Callback,
    struct PerSlotContext *SlotContext,  // this should be the callback arg
    struct DPUStorage* Sto,
    void *SPDKContext
){
    SegmentT* seg = &Sto->AllSegments[SegmentId];
    int rc;
    
    rc = BdevWriteV(SPDKContext, Iov, IovCnt, seg->DiskAddress + SegmentOffset, Bytes, 
        Callback, SlotContext);

    if (rc)
    {
        printf("WritevToDiskAsyncZC called BdevWrite(), but failed with: %d\n", rc);
        return rc;  // there should be an SPDK error log before this?
    }

    return DDS_ERROR_CODE_SUCCESS;
}

ErrorCodeT WritevToDiskAsyncNonZC(
    struct iovec *Iov,
    int IovCnt,
    FileIOSizeT NonZCBuffOffset,  // how many we already read
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes,
    DiskIOCallback Callback,
    struct PerSlotContext *SlotContext,  // this should be the callback arg
    struct DPUStorage* Sto,
    void *SPDKContext
){
    SegmentT* seg = &Sto->AllSegments[SegmentId];
    int rc;

    int position = SlotContext->Position;
    SPDKContextT *arg = SPDKContext;
    if (Bytes > DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE) {
        SPDK_WARNLOG("A write with %u bytes exceeds buff block space: %llu\n", Bytes, DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE);
    }
    char *toCopy = (&arg->buff[position * DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE]) + NonZCBuffOffset;
    memcpy(toCopy, Iov[0].iov_base, Iov[0].iov_len);
    memcpy(toCopy + Iov[0].iov_len, Iov[1].iov_base, Iov[1].iov_len);
    rc = BdevWrite(SPDKContext, toCopy,
        seg->DiskAddress + SegmentOffset, Bytes, Callback, SlotContext);

    if (rc)
    {
        printf("WritevToDiskAsyncNonZC called BdevWrite(), but failed with: %d\n", rc);
        return rc;  // there should be an SPDK error log before this?
    }

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Write to disk asynchronously with zero copy
//
//
ErrorCodeT WriteToDiskAsyncZC(
    BufferT SrcBuffer,
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes,
    DiskIOCallback Callback,
    ContextT Context,  // this should be the callback arg
    struct DPUStorage* Sto,
    void *SPDKContext
){
    SegmentT* seg = &Sto->AllSegments[SegmentId];
    int rc;

    rc = BdevWrite(SPDKContext, SrcBuffer, seg->DiskAddress + SegmentOffset, Bytes, 
        Callback, Context);

    if (rc)
    {
        printf("WriteToDiskAsyncZC called BdevWrite(), but failed with: %d\n", rc);
        return rc;  // there should be an SPDK error log before this?
    }

    return DDS_ERROR_CODE_SUCCESS;
}

ErrorCodeT WriteToDiskAsyncNonZC(
    BufferT SrcBuffer,
    FileIOSizeT NonZCBuffOffset,  // how many we've already written (i.e. the progress)
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes,
    DiskIOCallback Callback,
    // ContextT Context,  // this should be the callback arg
    struct PerSlotContext *SlotContext,
    struct DPUStorage* Sto,
    void *SPDKContext
){
    SegmentT* seg = &Sto->AllSegments[SegmentId];
    int rc;
    
    int position = SlotContext->Position;
    SPDKContextT *arg = SPDKContext;
    if (Bytes > DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE) {
        SPDK_WARNLOG("A write with %u bytes exceeds buff block space: %llu\n", Bytes, DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE);
    }

    // copy into our own buffer, then write
    char *toCopy = (&arg->buff[position * DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE]) + NonZCBuffOffset;
    memcpy(toCopy, SrcBuffer, Bytes);
    rc = BdevWrite(SPDKContext, toCopy, 
        seg->DiskAddress + SegmentOffset, Bytes, Callback, SlotContext);

    if (rc)
    {
        printf("WriteToDiskAsyncNonZC called BdevWrite(), but failed with: %d\n", rc);
        return rc;  // there should be an SPDK error log before this?
    }

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Retrieve all segments on the disk, replace all new... by malloc() inside
// This initializes Sto->AllSegments
//
//
ErrorCodeT RetrieveSegments(
    struct DPUStorage* Sto
){
    //
    // We should get data from the disk,
    // but just create all segments using memory for now
    //
    //
    Sto->AllSegments = malloc(sizeof(SegmentT) * Sto->TotalSegments);

    for (SegmentIdT i = 0; i != Sto->TotalSegments; i++) {
        Sto->AllSegments[i].Id = i;
        Sto->AllSegments[i].FileId = DDS_FILE_INVALID;
        
        //
        // We dont allocate memory to newSegment. Instead, we calculate 
        // the start position of each segment by i*DDS_BACKEND_SEGMENT_SIZE
        // and use this position as disk address
        //
        //
        DiskSizeT newSegment = i * DDS_BACKEND_SEGMENT_SIZE;

        Sto->AllSegments[i].DiskAddress = newSegment;
        
        if (i == DDS_BACKEND_RESERVED_SEGMENT) {
            Sto->AllSegments[i].Allocatable = false;
        }
        else {
            Sto->AllSegments[i].Allocatable = true;
            Sto->AvailableSegments++;
        }
    }

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Return all segments back to the disk
//
//
void ReturnSegments(
    struct DPUStorage* Sto
){
    //
    // Release the memory buffers
    //
    //
    if (Sto->AllSegments) {
        for (size_t i = 0; i != Sto->TotalSegments; i++) {

            if (Sto->AllSegments[i].Allocatable) {
                Sto->AvailableSegments--;
            }
        }

        free(Sto->AllSegments);
    }
}

//
// LoadFilesCallback
// Updated: free DMA-backed file property buffers with spdk_dma_free.
//
void LoadFilesCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    struct LoadDirectoriesAndFilesCtx *Ctx = Context;


    if (Ctx->FileOnDisk->Id != DDS_DIR_INVALID) {
        SPDK_NOTICELOG("Initializing file with ID %hu\n", Ctx->FileOnDisk->Id);
        Sto->AllFiles[Ctx->FileLoopIndex] =
            BackEndFileI(Ctx->FileOnDisk->Id, Ctx->FileOnDisk->FProperties.FileName, Ctx->FileOnDisk->FProperties.FileAttributes);
        printf("********** Sto->AllFiles[%d] = %p\n", Ctx->FileOnDisk->Id, Sto->AllFiles[Ctx->FileLoopIndex]);
        if (Sto->AllFiles[Ctx->FileLoopIndex]) {
            memcpy(GetFileProperties(Sto->AllFiles[Ctx->FileLoopIndex]), Ctx->FileOnDisk, sizeof(DPUFilePropertiesT));
            SetNumAllocatedSegments(Sto->AllFiles[Ctx->FileLoopIndex]);
        }
        else {
            Ctx->FailureStatus->HasFailed = true;
            Ctx->FailureStatus->HasAborted = true;
            // TODO: call FS app stop func
        }

        *(Ctx->LoadedFiles)++;
        if (*(Ctx->LoadedFiles) == Sto->TotalFiles) {
            SPDK_NOTICELOG("Loaded all files, total: %d\n", Sto->TotalFiles);
        }
    }
    // free(Ctx->FileOnDisk);
    // jason: file properties were read into a DMA-safe buffer.
    spdk_dma_free(Ctx->FileOnDisk);
    Ctx->FileOnDisk = NULL;

    if (Ctx->FileLoopIndex == DDS_MAX_FILES - 1) {
        // jason: last file callback, safe to release context.
        free(Ctx);
        return;
    }
    free(Ctx);
}

//
// LoadDirectoriesCallback
// Updated: free DMA-backed directory property buffers with spdk_dma_free.
//
void LoadDirectoriesCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    struct LoadDirectoriesAndFilesCtx *Ctx = Context;
    SPDKContextT *SPDKContext = Ctx->SPDKContext;

    // Original: if (Ctx->DirOnDisk->Id != DDS_DIR_INVALID) { ... }
    // jason: treat zeroed slots as invalid by requiring Id to match the slot index.
    if (Ctx->DirOnDisk->Id != DDS_DIR_INVALID && Ctx->DirOnDisk->Id == (DirIdT)Ctx->DirLoopIndex)
    {
        SPDK_NOTICELOG("Initializing Dir with ID: %hu\n", Ctx->DirOnDisk->Id);
        Sto->AllDirs[Ctx->DirLoopIndex] = BackEndDirI(Ctx->DirOnDisk->Id, DDS_DIR_ROOT, Ctx->DirOnDisk->Name);
        if (Sto->AllDirs[Ctx->DirLoopIndex]) {
            memcpy(GetDirProperties(Sto->AllDirs[Ctx->DirLoopIndex]), Ctx->DirOnDisk, sizeof(DPUDirPropertiesT));
        }
        else {
            //
            // This should be very unlikely
            //
            //
            // return DDS_ERROR_CODE_OUT_OF_MEMORY;
            // TODO: call app stop func
        }

        *(Ctx->LoadedDirs) += 1;
        if (*(Ctx->LoadedDirs) == Sto->TotalDirs) {
            SPDK_NOTICELOG("Loaded all directories, total: %d\n", Sto->TotalDirs);
        }
    }
    else if (Ctx->DirOnDisk->Id != DDS_DIR_INVALID && Ctx->DirOnDisk->Id != (DirIdT)Ctx->DirLoopIndex)
    {
        SPDK_NOTICELOG("LoadDirectoriesCallback: skipping mismatched dir id=%hu at slot=%zu\n", Ctx->DirOnDisk->Id,
                       Ctx->DirLoopIndex);
    }
    // free(Ctx->DirOnDisk);
    // jason: directory properties were read into a DMA-safe buffer.
    spdk_dma_free(Ctx->DirOnDisk);
    Ctx->DirOnDisk = NULL;

    //
    // This is the last callback
    //
    //
    if (Ctx->DirLoopIndex == DDS_MAX_DIRS - 1) {
        //
        // Load the files
        //
        //
        int result;
        // DPUFilePropertiesT *fileOnDisk = malloc(sizeof(*fileOnDisk));
        // jason: file metadata buffers are allocated per callback to avoid sharing across async reads.
        SegmentSizeT nextAddress = DDS_BACKEND_SEGMENT_SIZE - sizeof(DPUFilePropertiesT);
        // Ctx->FileOnDisk = fileOnDisk;
        // jason: each callback owns its own DMA buffer for file metadata.
        // Original: Ctx->LoadedFiles = 0;
        // jason: allocate a shared counter for file loads.
        Ctx->LoadedFiles = malloc(sizeof(*Ctx->LoadedFiles));
        if (!Ctx->LoadedFiles)
        {
            Ctx->FailureStatus->HasFailed = true;
            Ctx->FailureStatus->HasAborted = true;
            SPDK_ERRLOG("LoadDirectoriesCallback: LoadedFiles allocation failed\n");
            exit(-1);
        }
        *(Ctx->LoadedFiles) = 0;
        for (size_t f = 0; f != DDS_MAX_FILES; f++) {
            struct LoadDirectoriesAndFilesCtx *CallbackCtx = malloc(sizeof(*CallbackCtx));
            memcpy(CallbackCtx, Ctx, sizeof(*Ctx));
            CallbackCtx->FileLoopIndex = f;
            // CallbackCtx->FileOnDisk = malloc(sizeof(*CallbackCtx->FileOnDisk));
            // jason: file metadata buffer must be DMA-safe for SPDK reads.
            CallbackCtx->FileOnDisk = AllocateControlPlaneDmaBuffer(
                SPDKContext,
                sizeof(*CallbackCtx->FileOnDisk),
                true
            );
            if (!CallbackCtx->FileOnDisk) {
                Ctx->FailureStatus->HasFailed = true;
                Ctx->FailureStatus->HasAborted = true;
                SPDK_ERRLOG("LoadDirectoriesCallback: DMA buffer allocation failed\n");
                exit(-1);
            }

            // result = ReadFromDiskAsyncZC(
            //     (BufferT) fileOnDisk,
            //     DDS_BACKEND_RESERVED_SEGMENT,
            //     nextAddress,
            //     sizeof(DPUFilePropertiesT),
            //     LoadFilesCallback,
            //     CallbackCtx,
            //     Sto,
            //     SPDKContext
            // );
            // jason: read into per-callback DMA buffer to avoid sharing across async reads.
            result = ReadFromDiskAsyncZC(
                (BufferT) CallbackCtx->FileOnDisk,
                DDS_BACKEND_RESERVED_SEGMENT,
                nextAddress,
                sizeof(DPUFilePropertiesT),
                LoadFilesCallback,
                CallbackCtx,
                Sto,
                SPDKContext
            );

            if (result != DDS_ERROR_CODE_SUCCESS) {
                Ctx->FailureStatus->HasFailed = true;
                Ctx->FailureStatus->HasAborted = true;
                SPDK_ERRLOG("Read failed with %d, exiting...\n", result);
                exit(-1);
            }

            nextAddress -= sizeof(DPUFilePropertiesT);
        }
        // jason: file callbacks now own their contexts; safe to free the dir ctx.
        free(Ctx);
        return;
    }
    // jason: not the last directory callback; safe to free the context now.
    free(Ctx);
}

//
// Load the directories, in other future callbacks will then load the files.
// Updated: free DMA buffer used for the reserved sector header.
//
void LoadDirectoriesAndFilesCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    struct LoadDirectoriesAndFilesCtx *Ctx = Context;
    SPDKContextT *SPDKContext = Ctx->SPDKContext;

    Sto->TotalDirs = *((int*)(Ctx->TmpSectorBuffer + DDS_BACKEND_INITIALIZATION_MARK_LENGTH));
    Sto->TotalFiles = *((int*)(Ctx->TmpSectorBuffer + DDS_BACKEND_INITIALIZATION_MARK_LENGTH + sizeof(int)));
    SPDK_NOTICELOG("Got Sto->TotalDirs: %d, Sto->TotalFiles: %d\n", Sto->TotalDirs, Sto->TotalFiles);
    // jason: reserved header buffer is DMA-safe and no longer needed after parsing.
    spdk_dma_free(Ctx->TmpSectorBuffer);
    Ctx->TmpSectorBuffer = NULL;

    //
    // Load the directories
    //
    //
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    SegmentSizeT nextAddress = DDS_BACKEND_SECTOR_SIZE;

    // Original: Ctx->LoadedDirs = malloc(sizeof(Ctx->LoadedDirs));
    // jason: allocate the counter size, not the pointer size.
    Ctx->LoadedDirs = malloc(sizeof(*Ctx->LoadedDirs));
    if (!Ctx->LoadedDirs)
    {
        Ctx->FailureStatus->HasFailed = true;
        Ctx->FailureStatus->HasAborted = true;
        SPDK_ERRLOG("LoadDirectoriesAndFilesCallback: LoadedDirs allocation failed\n");
        exit(-1);
    }
    *(Ctx->LoadedDirs) = 0;
    for (size_t d = 0; d != DDS_MAX_DIRS; d++) {
        struct LoadDirectoriesAndFilesCtx *CallbackCtx = malloc(sizeof(*CallbackCtx));
        memcpy(CallbackCtx, Ctx, sizeof(*Ctx));
        CallbackCtx->DirLoopIndex = d;
        // CallbackCtx->DirOnDisk = malloc(sizeof(*CallbackCtx->DirOnDisk));
        // jason: directory metadata buffer must be DMA-safe for SPDK reads.
        CallbackCtx->DirOnDisk = AllocateControlPlaneDmaBuffer(
            SPDKContext,
            sizeof(*CallbackCtx->DirOnDisk),
            true
        );
        if (!CallbackCtx->DirOnDisk) {
            Ctx->FailureStatus->HasFailed = true;
            Ctx->FailureStatus->HasAborted = true;
            SPDK_ERRLOG("LoadDirectoriesAndFilesCallback: DMA buffer allocation failed\n");
            exit(-1);
        }
        result = ReadFromDiskAsyncZC(
            (BufferT) CallbackCtx->DirOnDisk,
            DDS_BACKEND_RESERVED_SEGMENT,
            nextAddress,
            sizeof(DPUDirPropertiesT),
            LoadDirectoriesCallback,
            CallbackCtx,
            Sto,
            SPDKContext
        );

        if (result != DDS_ERROR_CODE_SUCCESS) {
            Ctx->FailureStatus->HasFailed = true;
            Ctx->FailureStatus->HasAborted = true;
            SPDK_ERRLOG("Read failed with %d, exiting...\n", result);
            exit(-1);
        }
       
        nextAddress += sizeof(DPUDirPropertiesT);
    }
}

//
// Load all directories and files from the reserved segment.
// Updated: allocate a DMA-safe buffer for the reserved sector read.
//
ErrorCodeT LoadDirectoriesAndFiles(
    struct DPUStorage* Sto,
    void *Arg,
    struct InitializeCtx *InitializeCtx
){
    if (!Sto->AllSegments || Sto->TotalSegments == 0) {
        return DDS_ERROR_CODE_RESERVED_SEGMENT_ERROR;
    }

    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    //
    // Read the first sector
    //
    //
    // char tmpSectorBuffer[DDS_BACKEND_SECTOR_SIZE];
    struct LoadDirectoriesAndFilesCtx *Ctx = malloc(sizeof(*Ctx));
    // Ctx->TmpSectorBuffer = tmpSectorBuffer;
    // jason: use DMA-safe buffer for SPDK read of reserved sector.
    Ctx->TmpSectorBuffer = AllocateControlPlaneDmaBuffer(
        (SPDKContextT *)Arg,
        DDS_BACKEND_SECTOR_SIZE,
        true
    );
    if (!Ctx->TmpSectorBuffer) {
        SPDK_ERRLOG("LoadDirectoriesAndFiles: DMA buffer allocation failed\n");
        free(Ctx);
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }
    Ctx->FailureStatus = InitializeCtx->FailureStatus;
    Ctx->SPDKContext = InitializeCtx->SPDKContext;
    result = ReadFromDiskAsyncZC(
        Ctx->TmpSectorBuffer,
        DDS_BACKEND_RESERVED_SEGMENT,
        0,
        DDS_BACKEND_SECTOR_SIZE,
        LoadDirectoriesAndFilesCallback,
        Ctx,
        Sto,
        Arg
    );

    if (result != DDS_ERROR_CODE_SUCCESS) {
        Ctx->FailureStatus->HasFailed = true;
        Ctx->FailureStatus->HasAborted = true;
        SPDK_ERRLOG("ReadFromDiskAsync reserved seg failed with %d, EXITING...\n", result);
        exit(-1);
    }
    return DDS_ERROR_CODE_SUCCESS;
}

//
// Synchronize a directory to the disk.
// Updated: stage metadata in a DMA-safe buffer for SPDK I/O.
//
ErrorCodeT SyncDirToDisk(
    struct DPUDir* Dir, 
    struct DPUStorage* Sto,
    void *SPDKContext,
    DiskIOCallback Callback,
    ContextT Context
){
    // return WriteToDiskAsyncZC(
    //     (BufferT)GetDirProperties(Dir),
    //     DDS_BACKEND_RESERVED_SEGMENT,
    //     GetDirAddressOnSegment(Dir),
    //     sizeof(DPUDirPropertiesT),
    //     Callback,
    //     Context,
    //     Sto,
    //     SPDKContext
    // );
    // jason: use a DMA-safe staging buffer to satisfy SPDK RDMA requirements.
    void *dmaBuf = AllocateControlPlaneDmaBuffer(
        (SPDKContextT *)SPDKContext,
        sizeof(DPUDirPropertiesT),
        false
    );
    if (!dmaBuf) {
        SPDK_ERRLOG("SyncDirToDisk: DMA buffer allocation failed\n");
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }
    memcpy(dmaBuf, GetDirProperties(Dir), sizeof(DPUDirPropertiesT));

    ControlPlaneDmaWriteCtx *dmaCtx = malloc(sizeof(*dmaCtx));
    if (!dmaCtx) {
        spdk_dma_free(dmaBuf);
        SPDK_ERRLOG("SyncDirToDisk: DMA ctx allocation failed\n");
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }
    dmaCtx->UserCallback = Callback;
    dmaCtx->UserContext = Context;
    dmaCtx->DmaBuffer = dmaBuf;

    return WriteToDiskAsyncZC(
        (BufferT)dmaBuf,
        DDS_BACKEND_RESERVED_SEGMENT,
        GetDirAddressOnSegment(Dir),
        sizeof(DPUDirPropertiesT),
        ControlPlaneDmaWriteCallback,
        dmaCtx,
        Sto,
        SPDKContext
    );
}

//
// Synchronize a file to the disk.
// Updated: stage metadata in a DMA-safe buffer for SPDK I/O.
//
ErrorCodeT SyncFileToDisk(
    struct DPUFile* File,
    struct DPUStorage* Sto,
    void *SPDKContext,
    DiskIOCallback Callback,
    ContextT Context
){
    // return WriteToDiskAsyncZC(
    //     (BufferT)GetFileProperties(File),
    //     DDS_BACKEND_RESERVED_SEGMENT,
    //     GetFileAddressOnSegment(File),
    //     sizeof(DPUFilePropertiesT),
    //     Callback,
    //     Context,
    //     Sto,
    //     SPDKContext
    // );
    // jason: use a DMA-safe staging buffer to satisfy SPDK RDMA requirements.
    void *dmaBuf = AllocateControlPlaneDmaBuffer(
        (SPDKContextT *)SPDKContext,
        sizeof(DPUFilePropertiesT),
        false
    );
    if (!dmaBuf) {
        SPDK_ERRLOG("SyncFileToDisk: DMA buffer allocation failed\n");
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }
    memcpy(dmaBuf, GetFileProperties(File), sizeof(DPUFilePropertiesT));

    ControlPlaneDmaWriteCtx *dmaCtx = malloc(sizeof(*dmaCtx));
    if (!dmaCtx) {
        spdk_dma_free(dmaBuf);
        SPDK_ERRLOG("SyncFileToDisk: DMA ctx allocation failed\n");
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }
    dmaCtx->UserCallback = Callback;
    dmaCtx->UserContext = Context;
    dmaCtx->DmaBuffer = dmaBuf;

    return WriteToDiskAsyncZC(
        (BufferT)dmaBuf,
        DDS_BACKEND_RESERVED_SEGMENT,
        GetFileAddressOnSegment(File),
        sizeof(DPUFilePropertiesT),
        ControlPlaneDmaWriteCallback,
        dmaCtx,
        Sto,
        SPDKContext
    );
}

//
// Synchronize the first sector on the reserved segment.
// Updated: build the sector in a DMA-safe buffer and free it on completion.
//
ErrorCodeT SyncReservedInformationToDisk(
    struct DPUStorage* Sto,
    void *SPDKContext,
    DiskIOCallback Callback,
    ContextT Context
){
    // char tmpSectorBuf[DDS_BACKEND_SECTOR_SIZE];
    // memset(tmpSectorBuf, 0, DDS_BACKEND_SECTOR_SIZE);
    // strcpy(tmpSectorBuf, DDS_BACKEND_INITIALIZATION_MARK);
    //
    // int* numDirs = (int*)(tmpSectorBuf + DDS_BACKEND_INITIALIZATION_MARK_LENGTH);
    // *numDirs = Sto->TotalDirs;
    // int* numFiles = numDirs + 1;
    // *numFiles = Sto->TotalFiles;
    //
    // SPDK_NOTICELOG("Sto->TotalDirs %d, Sto->TotalFiles: %d\n", Sto->TotalDirs, Sto->TotalFiles);
    //
    // return WriteToDiskAsyncZC(
    //     tmpSectorBuf,
    //     DDS_BACKEND_RESERVED_SEGMENT,
    //     0,
    //     DDS_BACKEND_SECTOR_SIZE,
    //     Callback,
    //     Context,
    //     Sto,
    //     SPDKContext
    // );
    // jason: build reserved-sector contents in a DMA-safe buffer.
    char *tmpSectorBuf = AllocateControlPlaneDmaBuffer(
        (SPDKContextT *)SPDKContext,
        DDS_BACKEND_SECTOR_SIZE,
        true
    );
    if (!tmpSectorBuf) {
        SPDK_ERRLOG("SyncReservedInformationToDisk: DMA buffer allocation failed\n");
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }

    memset(tmpSectorBuf, 0, DDS_BACKEND_SECTOR_SIZE);
    strcpy(tmpSectorBuf, DDS_BACKEND_INITIALIZATION_MARK);

    int* numDirs = (int*)(tmpSectorBuf + DDS_BACKEND_INITIALIZATION_MARK_LENGTH);
    *numDirs = Sto->TotalDirs;
    int* numFiles = numDirs + 1;
    *numFiles = Sto->TotalFiles;

    SPDK_NOTICELOG("Sto->TotalDirs %d, Sto->TotalFiles: %d\n", Sto->TotalDirs, Sto->TotalFiles);

    ControlPlaneDmaWriteCtx *dmaCtx = malloc(sizeof(*dmaCtx));
    if (!dmaCtx) {
        spdk_dma_free(tmpSectorBuf);
        SPDK_ERRLOG("SyncReservedInformationToDisk: DMA ctx allocation failed\n");
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }
    dmaCtx->UserCallback = Callback;
    dmaCtx->UserContext = Context;
    dmaCtx->DmaBuffer = tmpSectorBuf;

    return WriteToDiskAsyncZC(
        tmpSectorBuf,
        DDS_BACKEND_RESERVED_SEGMENT,
        0,
        DDS_BACKEND_SECTOR_SIZE,
        ControlPlaneDmaWriteCallback,
        dmaCtx,
        Sto,
        SPDKContext
    );
}

void InitializeSyncReservedInfoCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    spdk_bdev_free_io(bdev_io);
    if (!Success) {
        SPDK_ERRLOG("Initialize() SyncReservedInformationToDisk IO failed...\n");
        exit(-1);
    }

    SPDK_NOTICELOG("Initialize() done!\n");
    G_INITIALIZATION_DONE = true;
}

void InitializeSyncDirToDiskCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    spdk_bdev_free_io(bdev_io);
    struct InitializeCtx *Ctx = Context;
    if (!Success) {
        SPDK_NOTICELOG("Initialize() SyncDirToDisk IO failed...\n");
        exit(-1);
    }

    Sto->AllDirs[DDS_DIR_ROOT] = Ctx->RootDir;

    //
    // Set the formatted mark and the numbers of dirs and files
    //
    //
    ErrorCodeT result;
    Sto->TotalDirs = 1;
    Sto->TotalFiles = 0;
    result = SyncReservedInformationToDisk(Sto, Ctx->SPDKContext, InitializeSyncReservedInfoCallback, Ctx);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        SPDK_ERRLOG("Initialize() SyncReservedInformationToDisk failed!!!\n");
        exit(-1);
    }

}

void RemainingPagesProgressCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    spdk_bdev_free_io(bdev_io);
    struct InitializeCtx *Ctx = Context;
    if (Ctx->FailureStatus->HasAborted) {
        SPDK_ERRLOG("HasAborted, exiting\n");
        exit(-1);
        return;
    }
    if (!Success) {
        Ctx->FailureStatus->HasFailed = true;
        SPDK_ERRLOG("HasFailed!\n");
    }

    if (Ctx->CurrentProgress == Ctx->TargetProgress) {
        SPDK_NOTICELOG("Initialize() RemainingPagesProgressCallback last one running\n");
        if (Ctx->FailureStatus->HasFailed) {
            //
            // something went wrong, can't continue to do work
            //
            //
            SPDK_ERRLOG("Initialize() RemainingPagesProgressCallback has failed IO, stopping...\n");
            exit(-1);
        }
        else {
            //
            // Create the root directory, which is on the second sector on the segment
            //
            //
            // Original: create the root directory immediately after zeroing.
            // ErrorCodeT result;
            // struct DPUDir* rootDir = BackEndDirI(DDS_DIR_ROOT, DDS_DIR_INVALID, DDS_BACKEND_ROOT_DIR_NAME);
            // Ctx->RootDir = rootDir;
            // result = SyncDirToDisk(rootDir, Sto, Ctx->SPDKContext, InitializeSyncDirToDiskCallback, Ctx);
            // if (result != DDS_ERROR_CODE_SUCCESS) {
            //     Ctx->FailureStatus->HasFailed = true;
            //     Ctx->FailureStatus->HasAborted = true;
            //     SPDK_ERRLOG("Initialize() SyncDirToDisk early fail!\n");
            //     exit(-1);
            // }
            // jason: write invalid entries before creating the root dir.
            ErrorCodeT result = InitializeWriteInvalidEntries(Sto, Ctx);
            if (result != DDS_ERROR_CODE_SUCCESS) {
                Ctx->FailureStatus->HasFailed = true;
                Ctx->FailureStatus->HasAborted = true;
                SPDK_ERRLOG("Initialize() InitializeWriteInvalidEntries failed!\n");
                exit(-1);
            }
        }
    }
}

//
// Increment progress callback.
// Updated: release DMA zero-page buffer when initialization completes.
//
void
IncrementProgressCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context)
{
    spdk_bdev_free_io(bdev_io);
    struct InitializeCtx *Ctx = Context;
    
    if (!Success) {
        SPDK_ERRLOG("HasFailed!\n");
        Ctx->FailureStatus->HasFailed = true;
        exit(-1);
    }
    ErrorCodeT result;
    if (Ctx->PagesLeft == 0) {
        //
        // All done, continue initialization
        // Create the root directory, which is on the second sector on the segment
        //
        //
        // Original: sync the root directory immediately after zeroing.
        // SPDK_NOTICELOG("Last of writing zeroed page to reserved seg!\n");
        // if (Ctx->tmpPageBuf) {
        //     spdk_dma_free(Ctx->tmpPageBuf);
        //     Ctx->tmpPageBuf = NULL;
        // }
        // struct DPUDir* rootDir = BackEndDirI(DDS_DIR_ROOT, DDS_DIR_INVALID, DDS_BACKEND_ROOT_DIR_NAME);
        // Ctx->RootDir = rootDir;
        // result = SyncDirToDisk(rootDir, Sto, Ctx->SPDKContext, InitializeSyncDirToDiskCallback, Ctx);
        // if (result != DDS_ERROR_CODE_SUCCESS) {
        //     Ctx->FailureStatus->HasFailed = true;
        //     Ctx->FailureStatus->HasAborted = true;
        //     SPDK_ERRLOG("Initialize() SyncDirToDisk early fail!!\n");
        //     exit(-1);
        // }
        // jason: after zeroing, write invalid entries before creating the root dir.
        SPDK_NOTICELOG("Last of writing zeroed page to reserved seg!\n");
        if (Ctx->tmpPageBuf) {
            spdk_dma_free(Ctx->tmpPageBuf);
            Ctx->tmpPageBuf = NULL;
        }
        result = InitializeWriteInvalidEntries(Sto, Ctx);
        if (result != DDS_ERROR_CODE_SUCCESS) {
            Ctx->FailureStatus->HasFailed = true;
            Ctx->FailureStatus->HasAborted = true;
            SPDK_ERRLOG("Initialize() InitializeWriteInvalidEntries failed!!\n");
            exit(-1);
        }
    }
    else {
        //
        // Else not last, issue more zeroing writes
        //
        //
        Ctx->NumPagesWritten += 1;
        Ctx->PagesLeft -= 1;
        
        result = WriteToDiskAsyncZC(
            Ctx->tmpPageBuf,
            DDS_BACKEND_RESERVED_SEGMENT,
            (SegmentSizeT)((Ctx->NumPagesWritten - 1) * DDS_BACKEND_PAGE_SIZE),
            DDS_BACKEND_PAGE_SIZE,
            IncrementProgressCallback,
            Ctx,
            Sto,
            Ctx->SPDKContext
        );
    }
}

//
// InitializeReadReservedSectorCallback
// Updated: use DMA-safe buffers for reserved sector and zero-page writes.
//
void InitializeReadReservedSectorCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    struct InitializeCtx *Ctx = Context;
    ErrorCodeT result;

    if (!Success) {
        SPDK_ERRLOG("Initialization failed! Exiting...\n");
        exit(-1);
    }
    if (strcmp(DDS_BACKEND_INITIALIZATION_MARK, Ctx->tmpSectorBuf)) {
        SPDK_NOTICELOG("Backend is NOT initialized!\n");
        //
        // Empty every byte on the reserved segment
        //
        //
        // char *tmpPageBuf = malloc(DDS_BACKEND_PAGE_SIZE);
        // memset(tmpPageBuf, 0, DDS_BACKEND_PAGE_SIZE);
        // Ctx->tmpPageBuf = tmpPageBuf;
        // jason: zero-page buffer must be DMA-safe for SPDK writes.
        char *tmpPageBuf = AllocateControlPlaneDmaBuffer(
            Ctx->SPDKContext,
            DDS_BACKEND_PAGE_SIZE,
            true
        );
        if (!tmpPageBuf) {
            Ctx->FailureStatus->HasFailed = true;
            Ctx->FailureStatus->HasAborted = true;
            SPDK_ERRLOG("InitializeReadReservedSectorCallback: DMA buffer allocation failed\n");
            exit(-1);
        }
        memset(tmpPageBuf, 0, DDS_BACKEND_PAGE_SIZE);
        Ctx->tmpPageBuf = tmpPageBuf;
        
        size_t pagesPerSegment = DDS_BACKEND_SEGMENT_SIZE / DDS_BACKEND_PAGE_SIZE;

        size_t numPagesWritten = 1;
        Ctx->NumPagesWritten = numPagesWritten;
        Ctx->PagesLeft = pagesPerSegment - numPagesWritten;

        //
        // Issue writes with the maximum queue depth
        //
        //
        Ctx->TargetProgress = pagesPerSegment;
        
        
        Ctx->CurrentProgress = 1;

        result = WriteToDiskAsyncZC(
            tmpPageBuf,
            DDS_BACKEND_RESERVED_SEGMENT,
            (SegmentSizeT)(numPagesWritten * DDS_BACKEND_PAGE_SIZE),
            DDS_BACKEND_PAGE_SIZE,
            IncrementProgressCallback,
            Ctx,
            Sto,
            Ctx->SPDKContext
        );
        if (result != DDS_ERROR_CODE_SUCCESS) {
            Ctx->FailureStatus->HasFailed = true;
            Ctx->FailureStatus->HasAborted = true;  // don't need to run callbacks anymore
            SPDK_ERRLOG("Clearing backend pages failed with %d\n", result);
            exit(-1);
        }
    }
    else {
        SPDK_NOTICELOG("Backend is initialized! Load directories and files...\n");
        //
        // Load directories and files
        //
        //
        result = LoadDirectoriesAndFiles(Sto, Ctx->SPDKContext, Ctx);

        if (result != DDS_ERROR_CODE_SUCCESS) {
            return;
        }
        // jason: existing storage path; emit a ready marker for restart scenarios.
        SPDK_NOTICELOG("Initialize() existing storage detected; backend ready (loading metadata)\n");

        //
        // Update segment information
        //
        //
        int checkedFiles = 0;
        for (FileIdT f = 0; f != DDS_MAX_FILES; f++) {
            struct DPUFile* file = Sto->AllFiles[f];
            if (file) {
                SegmentIdT* segments = GetFileProperties(file)->Segments;
                for (size_t s = 0; s != DDS_BACKEND_MAX_SEGMENTS_PER_FILE; s++) {
                    SegmentIdT currentSeg = segments[s];
                    if (currentSeg == DDS_BACKEND_SEGMENT_INVALID) {
                        break;
                    }
                    else {
                        // Original: Sto->AllSegments[currentSeg].FileId = f;
                        // Updated: track AvailableSegments when rehydrating segment ownership.
                        if (Sto->AllSegments[currentSeg].Allocatable &&
                            Sto->AllSegments[currentSeg].FileId == DDS_FILE_INVALID)
                        {
                            Sto->AllSegments[currentSeg].FileId = f;
                            Sto->AvailableSegments--;
                        }
                        else if (!Sto->AllSegments[currentSeg].Allocatable)
                        {
                            // Reserved segment should never be claimed by file segment maps.
                            SPDK_ERRLOG("Reserved segment %d referenced by file %u\n", currentSeg, f);
                        }
                    }
                }

                checkedFiles++;
                if (checkedFiles == Sto->TotalFiles) {
                    break;
                }
            }
        }
    }
    // jason: release DMA buffer for reserved-sector read after parsing.
    if (Ctx->tmpSectorBuf) {
        spdk_dma_free(Ctx->tmpSectorBuf);
        Ctx->tmpSectorBuf = NULL;
    }
    spdk_bdev_free_io(bdev_io);
}

//
// Initialize the backend service.
// This needs to be async, so using callback chains to accomplish this.
// Updated: allocate DMA-safe buffer for reserved sector reads.
//
ErrorCodeT Initialize(
    struct DPUStorage* Sto,
    void *Arg // NOTE: this is currently an spdkContext, but depending on the callbacks, they need different arg than this
){
    SPDKContextT *SPDKContext = Arg;
    //
    // Retrieve all segments on disk
    //
    //
    ErrorCodeT result = RetrieveSegments(Sto);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    //
    // The first sector on the reserved segment contains initialization information
    //
    //
    // char *tmpSectorBuf = malloc(DDS_BACKEND_SECTOR_SIZE + 1);
    // memset(tmpSectorBuf, 0, DDS_BACKEND_SECTOR_SIZE + 1);
    // jason: reserved sector buffer must be DMA-safe for SPDK reads.
    char *tmpSectorBuf = AllocateControlPlaneDmaBuffer(
        SPDKContext,
        DDS_BACKEND_SECTOR_SIZE + 1,
        true
    );
    if (!tmpSectorBuf) {
        SPDK_ERRLOG("Initialize: DMA buffer allocation failed\n");
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }
    memset(tmpSectorBuf, 0, DDS_BACKEND_SECTOR_SIZE + 1);

    // read reserved sector
    struct InitializeCtx *InitializeCtx = malloc(sizeof(*InitializeCtx));
    InitializeCtx->tmpSectorBuf = tmpSectorBuf;
    struct InitializeFailureStatus *FailureStatus = malloc(sizeof(*FailureStatus));
    InitializeCtx->FailureStatus = FailureStatus;
    InitializeCtx->FailureStatus->HasFailed = false;
    InitializeCtx->FailureStatus->HasStopped = false;
    InitializeCtx->FailureStatus->HasStopped = false;
    InitializeCtx->CurrentProgress = 0;
    InitializeCtx->TargetProgress = 0;
    InitializeCtx->SPDKContext = SPDKContext;
    result = ReadFromDiskAsyncZC(
        tmpSectorBuf,
        DDS_BACKEND_RESERVED_SEGMENT,
        0,
        DDS_BACKEND_SECTOR_SIZE,
        InitializeReadReservedSectorCallback,
        InitializeCtx,
        Sto,
        Arg
    );
    SPDK_NOTICELOG("Reserved segment has been read\n");

    if (result != DDS_ERROR_CODE_SUCCESS) {
        printf("Initialize read reserved sector failed!!!\n");
        return result;
    }
    return DDS_ERROR_CODE_SUCCESS;
}

void CreateDirectorySyncReservedInformationToDiskCallback(struct spdk_bdev_io *bdev_io, bool Success, ContextT Context) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {
        //
        // This is basically the last line of the original CreateDirectory()
        //
        //
        *(HandlerCtx->Result) = DDS_ERROR_CODE_SUCCESS;
    }
    else {
        SPDK_ERRLOG("CreateDirectorySyncReservedInformationToDiskCallback failed\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }

    //
    // We should free the handler ctx at the end of the last callback
    //
    //
    free(HandlerCtx);
    pthread_mutex_unlock(&Sto->SectorModificationMutex);
}

void CreateDirectorySyncDirToDiskCallback(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {
        Sto->AllDirs[HandlerCtx->DirId] = HandlerCtx->dir;

        pthread_mutex_lock(&Sto->SectorModificationMutex);
        Sto->TotalDirs++;
        ErrorCodeT result = SyncReservedInformationToDisk(Sto, HandlerCtx->SPDKContext,
            CreateDirectorySyncReservedInformationToDiskCallback, HandlerCtx);
        if (result != 0) {
            //
            // Fatal, can't continue, and the callback won't run
            //
            //
            SPDK_ERRLOG("SyncReservedInformationToDisk() returned %hu\n", result);
            pthread_mutex_unlock(&Sto->SectorModificationMutex);
            *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
        }
        //
        // Else, the callback passed to it should be guaranteed to run
        //
        //
    }
    else {
        SPDK_ERRLOG("CreateDirectorySyncDirToDisk failed, can't create directory\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }
}

//
// Create a diretory
// Assuming id and parent id have been computed by host
// 
//
ErrorCodeT CreateDirectory(
    const char* PathName,
    DirIdT DirId,
    DirIdT ParentId,
    struct DPUStorage* Sto,
    void *SPDKContext,
    ControlPlaneHandlerCtx *HandlerCtx
){
    HandlerCtx->SPDKContext = SPDKContext;

    struct DPUDir* dir = BackEndDirI(DirId, ParentId, PathName);
    if (!dir) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_OUT_OF_MEMORY;
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }
    HandlerCtx->DirId = DirId;
    HandlerCtx->dir = dir;
    ErrorCodeT result = SyncDirToDisk(dir, Sto, SPDKContext, CreateDirectorySyncDirToDiskCallback, HandlerCtx);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
        return result;
    }
    return result;
}

void RemoveDirectoryCallback2(struct spdk_bdev_io *bdev_io, bool Success, ContextT Context) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_SUCCESS;
        free(HandlerCtx);
    }
    else {
        SPDK_ERRLOG("RemoveDirectoryCallback2() failed\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }

    pthread_mutex_unlock(&Sto->SectorModificationMutex);
}

void RemoveDirectoryCallback1(struct spdk_bdev_io *bdev_io, bool Success, ContextT Context) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {  
        //
        // Continue to do work
        //
        //
        free(Sto->AllDirs[HandlerCtx->DirId]);
        Sto->AllDirs[HandlerCtx->DirId] = NULL;

        pthread_mutex_lock(&Sto->SectorModificationMutex);

        Sto->TotalDirs--;
        ErrorCodeT result = SyncReservedInformationToDisk(Sto, HandlerCtx->SPDKContext,
            RemoveDirectoryCallback2, HandlerCtx);
        if (result != 0) {
            //
            // Fatal, can't continue, and the callback won't run
            //
            //
            SPDK_ERRLOG("RemoveDirectoryCallback1() returned %hu\n", result);
            pthread_mutex_unlock(&Sto->SectorModificationMutex);
            *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
        }
        //
        // else, the callback passed to it should be guaranteed to run
        //
        //
    }
    else {
        SPDK_ERRLOG("RemoveDirectoryCallback1() failed, can't create directory\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }
}

//
// Delete a directory
// 
//
ErrorCodeT RemoveDirectory(
    DirIdT DirId,
    struct DPUStorage* Sto,
    void *SPDKContext,
    ControlPlaneHandlerCtx *HandlerCtx
){
    HandlerCtx->SPDKContext = SPDKContext;
    if (!Sto->AllDirs[DirId]) {
       *(HandlerCtx->Result) = DDS_ERROR_CODE_DIR_NOT_FOUND;
       return DDS_ERROR_CODE_DIR_NOT_FOUND;
    }

    GetDirProperties(Sto->AllDirs[DirId])->Id = DDS_DIR_INVALID;
    HandlerCtx->DirId = DirId;
    ErrorCodeT result = SyncDirToDisk(Sto->AllDirs[DirId], Sto, SPDKContext, RemoveDirectoryCallback1, HandlerCtx);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
        return DDS_ERROR_CODE_IO_FAILURE;
    }
    return result;
}

void CreateFileCallback3(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_SUCCESS;
        free(HandlerCtx);
    }
    else {
        SPDK_ERRLOG("CreateFileCallback3() failed\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }
    pthread_mutex_unlock(&Sto->SectorModificationMutex);
}

void CreateFileCallback2(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {
        Unlock(HandlerCtx->dir);
        //
        // Finally, file is added
        //
        //
        Sto->AllFiles[HandlerCtx->FileId] = HandlerCtx->File;

        Sto->TotalFiles++;
        ErrorCodeT result = SyncReservedInformationToDisk(Sto, HandlerCtx->SPDKContext,
            CreateFileCallback3, HandlerCtx);
        if (result != 0) {
            //
            // Fatal, can't continue, and the callback won't run
            //
            //
            SPDK_ERRLOG("CreateFileCallback2() returned %hu\n", result);
            pthread_mutex_unlock(&Sto->SectorModificationMutex);
            *(HandlerCtx->Result) =  DDS_ERROR_CODE_IO_FAILURE;
        }
        //
        // Else, the callback passed to it should be guaranteed to run
        //
        //
    }
    else {
        SPDK_ERRLOG("CreateFileCallback2() failed\n");
        *(HandlerCtx->Result) =  DDS_ERROR_CODE_IO_FAILURE;
    }
        
    pthread_mutex_lock(&Sto->SectorModificationMutex);
}

void CreateFileCallback1(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {
        //
        // Continue to do work
        // Add this file to its directory and make the update persistent
        //
        //
        Lock(HandlerCtx->dir);
    
        AddFile(HandlerCtx->FileId, HandlerCtx->dir);
        ErrorCodeT result = SyncDirToDisk(HandlerCtx->dir, Sto, HandlerCtx->SPDKContext,
            CreateFileCallback2, HandlerCtx);
        if (result != 0) {
            //
            // Fatal, can't continue, and the callback won't run
            //
            //
            SPDK_ERRLOG("CreateFileCallback1() returned %hu\n", result);
            Unlock(HandlerCtx->dir);
            *(HandlerCtx->Result) =  DDS_ERROR_CODE_IO_FAILURE;
        }
        //
        // Else, the callback passed to it should be guaranteed to run
        //
        //
    }
    else {
        SPDK_ERRLOG("CreateFileCallback1() failed, can't create directory\n");
        *(HandlerCtx->Result) =  DDS_ERROR_CODE_IO_FAILURE;
    }
}

// Deprecated: control-plane FindFirstFile is disabled; inline scan in
// FileBackEnd.c handles FIND_FIRST_FILE.
// jason: DEPERCATED, NO LONGER USED
/* ErrorCodeT FindFirstFile(
    const char* FileName,
    FileIdT* FileId,
    struct DPUStorage* Sto
) {
    if (!FileName) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    // for (FileIdT i = 0; i != DDS_MAX_FILES; i++) {
    //     struct DPUFile* file = Sto->AllFiles[i];
    //     if (file && strcmp(GetName(file), FileName) == 0) {
    //         *FileId = i;
    //         HandlerCtx->FileId = i;
    //         *(HandlerCtx->Result) = DDS_ERROR_CODE_SUCCESS;
    //         return DDS_ERROR_CODE_SUCCESS;
    //     }
    // }
    // *(HandlerCtx->Result) = DDS_ERROR_CODE_FILE_NOT_FOUND;
    return DDS_ERROR_CODE_FILE_NOT_FOUND;
} */

/*
 * CreateFile
 *
 * Create a new file if it does not already exist. If a file with the same
 * name is present, return DDS_ERROR_CODE_FILE_EXISTS so the host can resolve
 * the existing ID via FIND_FIRST_FILE and apply O_TRUNC semantics.
 *
 * Notes:
 * - Access/share metadata is front-end only and not persisted here.
 * - File name comparisons are exact string matches on backend-stored names.
 */
ErrorCodeT CreateFile(
    const char* FileName,
    FileAttributesT FileAttributes,
    FileIdT FileId,
    DirIdT DirId,
    struct DPUStorage* Sto,
    void *SPDKContext,
    ControlPlaneHandlerCtx *HandlerCtx
){
    // Defensive checks with explicit logs for early diagnosis.
    if (!FileName || !HandlerCtx || !Sto)
    {
        SPDK_ERRLOG("CreateFile() invalid args: HandlerCtx=%p Sto=%p FileName=%p\n", (void *)HandlerCtx, (void *)Sto,
                    (void *)FileName);
        if (HandlerCtx && HandlerCtx->Result)
        {
            *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
        }
        return DDS_ERROR_CODE_IO_FAILURE;
    }
    if (!HandlerCtx->Result)
    {
        SPDK_ERRLOG("CreateFile() invalid handler result pointer\n");
        assert(HandlerCtx->Result != NULL);
        return DDS_ERROR_CODE_IO_FAILURE;
    }

    printf("CreateFile() FileName: %s\n", FileName);

    HandlerCtx->SPDKContext = SPDKContext;
    struct DPUDir* dir = Sto->AllDirs[DirId];
    if (!dir) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_DIR_NOT_FOUND;
        SPDK_ERRLOG("DDS_ERROR_CODE_DIR_NOT_FOUND\n");
        return DDS_ERROR_CODE_DIR_NOT_FOUND;
    }

    // Check for existing file name to honor POSIX O_CREAT semantics on restart.
    for (FileIdT i = 0; i != DDS_MAX_FILES; i++)
    {
        struct DPUFile *existing = Sto->AllFiles[i];
        if (existing)
        {
            const char *existingName = GetName(existing);
            if (existingName && strcmp(existingName, FileName) == 0)
            {
                *(HandlerCtx->Result) = DDS_ERROR_CODE_FILE_EXISTS;
                SPDK_NOTICELOG("CreateFile(): file already exists (id=%u, name=%s)\n", i, FileName);
                return DDS_ERROR_CODE_FILE_EXISTS;
            }
        }
    }

    struct DPUFile* file = BackEndFileI(FileId, FileName, FileAttributes);
    if (!file) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_OUT_OF_MEMORY;
        SPDK_ERRLOG("DDS_ERROR_CODE_OUT_OF_MEMORY\n");
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }

    // Persist timestamps; access/share are front-end only and not stored.
    // file->Properties.FProperties.Access = Access;
    // file->Properties.FProperties.ShareMode = ShareMode;
    file->Properties.FProperties.CreationTime = time(NULL);
    file->Properties.FProperties.LastAccessTime = file->Properties.FProperties.CreationTime;
    file->Properties.FProperties.LastWriteTime = file->Properties.FProperties.CreationTime;

    //
    // Make file persistent
    //
    //
    HandlerCtx->DirId = DirId;
    HandlerCtx->dir = dir;
    HandlerCtx->FileId = FileId;
    HandlerCtx->File = file;
    ErrorCodeT result = SyncFileToDisk(file, Sto, SPDKContext, CreateFileCallback1, HandlerCtx);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        SPDK_ERRLOG("DDS_ERROR_CODE_IO_FAILURE\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }
    return result;
}
void DeleteFileCallback3(struct spdk_bdev_io *bdev_io, bool Success, ContextT Context) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_SUCCESS;
        free(HandlerCtx);
    }
    else {
        SPDK_ERRLOG("DeleteFileCallback3() failed\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }

    pthread_mutex_unlock(&Sto->SectorModificationMutex);
}

void DeleteFileCallback2(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {
        Unlock(HandlerCtx->dir);

        //
        // Finally, delete the file
        //
        //
        free(HandlerCtx->File);
        Sto->AllFiles[HandlerCtx->FileId] = NULL;

        pthread_mutex_lock(&Sto->SectorModificationMutex);

        Sto->TotalFiles--;
        ErrorCodeT result = SyncReservedInformationToDisk(Sto, HandlerCtx->SPDKContext,
            DeleteFileCallback3, HandlerCtx);
        if (result != 0) {
            //
            // Fatal, can't continue, and the callback won't run
            //
            //
            SPDK_ERRLOG("DeleteFileCallback2() returned %hu\n", result);
            pthread_mutex_unlock(&Sto->SectorModificationMutex);
            *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
        }
        // 
        // Else, the callback passed to it should be guaranteed to run
        //
        //
    }
    else {
        SPDK_ERRLOG("DeleteFileCallback2() failed\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }
}

void DeleteFileCallback1(
    struct spdk_bdev_io *bdev_io,
    bool Success,
    ContextT Context
) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {
        //
        // Continue to do work
        // Delete this file from its directory and make the update persistent
        //
        //
        Lock(HandlerCtx->dir);

        DeleteFile(HandlerCtx->FileId, HandlerCtx->dir);
        ErrorCodeT result = SyncDirToDisk(HandlerCtx->dir, Sto, HandlerCtx->SPDKContext,
            DeleteFileCallback2, HandlerCtx);
        if (result != 0) {
            //
            // Fatal, can't continue, and the callback won't run
            //
            //
            SPDK_ERRLOG("DeleteFileCallback1() returned %hu\n", result);
            Unlock(HandlerCtx->dir);
            *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
        }
        //
        // Else, the callback passed to it should be guaranteed to run
        //
        //
    }
    else {
        SPDK_ERRLOG("DeleteFileCallback1() failed, can't create directory\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }
}

//
// Delete a file
// 
//
ErrorCodeT DeleteFileOnSto(
    FileIdT FileId,
    DirIdT DirId,
    struct DPUStorage* Sto,
    void *SPDKContext,
    ControlPlaneHandlerCtx *HandlerCtx
){
    HandlerCtx->SPDKContext = SPDKContext;
    struct DPUFile* file = Sto->AllFiles[FileId];
    struct DPUDir* dir = Sto->AllDirs[DirId];
    if (!file) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_FILE_NOT_FOUND;
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }
    if (!dir) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_DIR_NOT_FOUND;
        return DDS_ERROR_CODE_DIR_NOT_FOUND;
    }

    GetFileProperties(file)->Id = DDS_FILE_INVALID;
    
    //
    // Make the change persistent
    //
    //
    HandlerCtx->DirId = DirId;
    HandlerCtx->dir = dir;
    HandlerCtx->FileId = FileId;
    HandlerCtx->File = file;
    ErrorCodeT result = SyncFileToDisk(file, Sto, SPDKContext, DeleteFileCallback1, HandlerCtx);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }
    return result;
}

//
// Change the size of a file
//
#if 0
ErrorCodeT ChangeFileSize(
    FileIdT FileId,
    FileSizeT NewSize,
    struct DPUStorage* Sto,
    CtrlMsgB2FAckChangeFileSize *Resp
){
    struct DPUFile* file = Sto->AllFiles[FileId];

    if (!file) {
        Resp->Result = DDS_ERROR_CODE_FILE_NOT_FOUND;
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    //
    // There might be concurrent writes, hoping apps will handle the concurrency
    //
    //
    FileSizeT currentFileSize = GetFileProperties(file)->FProperties.FileSize;
    long long sizeToChange = NewSize - currentFileSize;
    if (sizeToChange >= 0) {
        //
        // The previous code was "(size_t)GetNumSegments(file)", but it cannot be
        // understood by compiler, so I rewrite in this format
        // 
        //
        size_t numSeg = GetNumSegments(file); 
        SegmentSizeT remainingAllocatedSize = (SegmentSizeT)(numSeg * DDS_BACKEND_SEGMENT_SIZE - currentFileSize);
        long long bytesToBeAllocated = sizeToChange - (long long)remainingAllocatedSize;
        
        if (bytesToBeAllocated > 0) {
            SegmentIdT numSegmentsToAllocate = (SegmentIdT)(
                bytesToBeAllocated / DDS_BACKEND_SEGMENT_SIZE +
                (bytesToBeAllocated % DDS_BACKEND_SEGMENT_SIZE == 0 ? 0 : 1)
            );

            //
            // Allocate segments
            //
            //
            pthread_mutex_lock(&Sto->SegmentAllocationMutex);

            if (numSegmentsToAllocate > Sto->AvailableSegments) {
                pthread_mutex_unlock(&Sto->SegmentAllocationMutex);
                Resp->Result = DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE;
                return DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE;
            }

            SegmentIdT allocatedSegments = 0;
            for (SegmentIdT s = 0; s != Sto->TotalSegments; s++) {
                SegmentT* seg = &Sto->AllSegments[s];
                if (seg->Allocatable && seg->FileId == DDS_FILE_INVALID) {
                    AllocateSegment(s,file);
                    seg->FileId = FileId;
                    allocatedSegments++;
                    if (allocatedSegments == numSegmentsToAllocate) {
                        break;
                    }
                }
            }

            pthread_mutex_unlock(&Sto->SegmentAllocationMutex);
        }
    }
    else {
        SegmentIdT newSegments = (SegmentIdT)(NewSize / DDS_BACKEND_SEGMENT_SIZE + (NewSize % DDS_BACKEND_SEGMENT_SIZE == 0 ? 0 : 1));
        SegmentIdT numSegmentsToDeallocate = GetNumSegments(file) - newSegments;
        if (numSegmentsToDeallocate > 0) {

            //
            // Deallocate segments
            //
            //
            pthread_mutex_lock(&Sto->SegmentAllocationMutex);

            for (SegmentIdT s = 0; s != numSegmentsToDeallocate; s++) {
                DeallocateSegment(file);
            }

            pthread_mutex_unlock(&Sto->SegmentAllocationMutex);
        }
    }

    //
    // Segment (de)allocation has been taken care of
    //
    //
    SetSize(NewSize, file);
    Resp->Result = DDS_ERROR_CODE_SUCCESS;
    return DDS_ERROR_CODE_SUCCESS;
}
#endif

//
// Control-plane callback for ChangeFileSizeControlPlane()
//
void ChangeFileSizeSyncCallback(struct spdk_bdev_io *bdev_io, bool Success, ContextT Context)
{
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);

    if (Success)
    {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_SUCCESS;
    }
    else
    {
        SPDK_ERRLOG("ChangeFileSizeSyncCallback failed\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }

    // Consistent with other control-plane callbacks.
    free(HandlerCtx);
}

/**
 * Control-plane version of ChangeFileSize that persists metadata to disk.
 */
ErrorCodeT ChangeFileSizeControlPlane(FileIdT FileId, FileSizeT NewSize, struct DPUStorage *Sto, void *SPDKContext,
                                      ControlPlaneHandlerCtx *HandlerCtx)
{
    HandlerCtx->SPDKContext = SPDKContext;
    struct DPUFile *file = Sto->AllFiles[FileId];
    if (!file)
    {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_FILE_NOT_FOUND;
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    HandlerCtx->FileId = FileId;
    HandlerCtx->File = file;

    bool segmentsChanged = false;
    ErrorCodeT result = EnsureFileSegmentsForSize(Sto, file, FileId, NewSize, &segmentsChanged);
    // segmentsChanged reserved for future persistence decisions.
    (void)segmentsChanged;
    if (result != DDS_ERROR_CODE_SUCCESS)
    {
        *(HandlerCtx->Result) = result;
        return result;
    }

    // Updated: size change happens before persisting file metadata.
    SetSize(NewSize, file);

    result = SyncFileToDisk(file, Sto, SPDKContext, ChangeFileSizeSyncCallback, HandlerCtx);
    if (result != DDS_ERROR_CODE_SUCCESS)
    {
        SPDK_ERRLOG("ChangeFileSizeControlPlane SyncFileToDisk failed: %hu\n", result);
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
        return result;
    }

    return DDS_ERROR_CODE_SUCCESS;
}

/**
 * Change the size of a file (in-memory only).
 * Control plane uses a separate variant that persists metadata to disk.
 */
ErrorCodeT ChangeFileSize(FileIdT FileId, FileSizeT NewSize, struct DPUStorage *Sto, CtrlMsgB2FAckChangeFileSize *Resp)
{
    struct DPUFile *file = Sto->AllFiles[FileId];
    if (!file)
    {
        Resp->Result = DDS_ERROR_CODE_FILE_NOT_FOUND;
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    bool segmentsChanged = false;
    ErrorCodeT result = EnsureFileSegmentsForSize(Sto, file, FileId, NewSize, &segmentsChanged);
    // segmentsChanged reserved for future persistence decisions.
    (void)segmentsChanged;
    if (result != DDS_ERROR_CODE_SUCCESS)
    {
        Resp->Result = result;
        return result;
    }

    // Updated: size change now happens after segment bookkeeping.
    SetSize(NewSize, file);
    Resp->Result = DDS_ERROR_CODE_SUCCESS;
    return DDS_ERROR_CODE_SUCCESS;
}

/**
 * Change the size of a file (data plane) and mark metadata for persistence.
 */
ErrorCodeT ChangeFileSizeDataPlane(FileIdT FileId, FileSizeT NewSize, struct DPUStorage *Sto,
                                   struct PerSlotContext *SlotContext)
{
    if (!SlotContext)
    {
        return DDS_ERROR_CODE_INVALID_FILE_POSITION;
    }

    SlotContext->NeedMetadataSync = false;
    SlotContext->MetadataSyncIssued = false;

    struct DPUFile *file = Sto->AllFiles[FileId];
    if (!file)
    {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    FileSizeT currentFileSize = GetFileProperties(file)->FProperties.FileSize;
    if (NewSize == currentFileSize)
    {
        return DDS_ERROR_CODE_SUCCESS;
    }

    bool segmentsChanged = false;
    ErrorCodeT result = EnsureFileSegmentsForSize(Sto, file, FileId, NewSize, &segmentsChanged);
    if (result != DDS_ERROR_CODE_SUCCESS)
    {
        return result;
    }

    // Updated: data-plane size changes should be persisted after data I/O completes.
    SetSize(NewSize, file);
    SlotContext->NeedMetadataSync = true;
    SlotContext->MetadataSyncIssued = false;
    (void)segmentsChanged;
    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get file size
// 
//
ErrorCodeT GetFileSize(
    FileIdT FileId,
    FileSizeT* FileSize,
    struct DPUStorage* Sto,
    CtrlMsgB2FAckGetFileSize *Resp
) {
    struct DPUFile* file = Sto->AllFiles[FileId];

    if (!file) {
        Resp->Result = DDS_ERROR_CODE_FILE_NOT_FOUND;
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    *FileSize = GetSize(file);
    Resp->Result = DDS_ERROR_CODE_SUCCESS;
    return DDS_ERROR_CODE_SUCCESS;
}

//
// Async read from a file
// TODO: Currently doesn't handle non block sized IO, deals with split buffer with readv/writev but logic still relies
// on block sized IO
//
#if 0
ErrorCodeT ReadFile(
    FileIdT FileId,
    FileSizeT Offset,
    SplittableBufferT *DestBuffer,
    DiskIOCallback Callback,
    ContextT Context,
    struct DPUStorage* Sto,
    void *SPDKContext
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    struct DPUFile* file = Sto->AllFiles[FileId];
    struct PerSlotContext* SlotContext = (struct PerSlotContext*)Context;
    SlotContext->DestBuffer = DestBuffer;
    FileSizeT remainingBytes = GetSize(file) - Offset;
    // jason: Use bdev block size to align the disk I/O offset.
    SPDKContextT *spdkContext = (SPDKContextT *)SPDKContext;
    FileSizeT blockSize = spdkContext ? (FileSizeT)spdkContext->block_size : 1;
    if (blockSize == 0) {
        blockSize = 1;
    }
    FileSizeT alignedOffset = Offset - (Offset % blockSize);

    FileIOSizeT bytesLeftToRead = DestBuffer->TotalSize;
    FileSizeT fileSize = GetSize(file);

    if (Offset > GetSize(file)) {
        SPDK_WARNLOG("Offset %lu > GetSize(file) %lu !\n", Offset, GetSize(file));
        return DDS_ERROR_CODE_IO_FAILURE;
    }

    if (remainingBytes < DestBuffer->TotalSize) {
        //
        // TODO: bytes serviced can be smaller than requested, probably not a problem?
        //
        //
        SPDK_WARNLOG("found remainingBytes < DestBuffer->TotalSize, GetSize(file): %lu, Offset: %lu\n", fileSize, Offset);
        bytesLeftToRead = (FileIOSizeT)remainingBytes;
    }
    
	const FileIOSizeT totalBytesToRead = bytesLeftToRead;

    SlotContext->BytesIssued = bytesLeftToRead;

    // FileSizeT curOffset = Offset;
    // jason: Issue disk I/O from the aligned offset while reporting logical bytes.
    FileSizeT curOffset = alignedOffset;
    SegmentIdT curSegment;
    SegmentSizeT offsetOnSegment;
    SegmentSizeT bytesToIssue;
    SegmentSizeT remainingBytesOnCurSeg;
    BufferT DestAddr;

    /* if (DestBuffer->SecondAddr) {
        printf("read buffer has second split!\n");
    } */
    
    while (bytesLeftToRead) {
        //
        // Cross-boundary detection
        //
        //
        curSegment = (SegmentIdT)(curOffset / DDS_BACKEND_SEGMENT_SIZE);
        offsetOnSegment = curOffset % DDS_BACKEND_SEGMENT_SIZE;
        remainingBytesOnCurSeg = DDS_BACKEND_SEGMENT_SIZE - offsetOnSegment;

        FileIOSizeT bytesRead = totalBytesToRead - bytesLeftToRead;
        int firstSplitLeftToRead = DestBuffer->FirstSize - bytesRead;

        if (DestBuffer->FirstSize <= bytesRead) {
            printf("firstSplitLeftToWrite should be <= 0, is: %d\n", firstSplitLeftToRead);
            printf("total size is: %u\n", DestBuffer->TotalSize);
        }

        if (DestBuffer->FirstSize - bytesRead < 0) {
            // 
            // TODO: check
            //
            //
            SPDK_ERRLOG("int firstSplitLeftToRead = DestBuffer->FirstSize - bytesRead, underflow, %u - %u = %d\n",
                DestBuffer->FirstSize, bytesRead, firstSplitLeftToRead);
        }

        //
        // May be 1st or 2nd split, the no. of bytes left to read onto it
        //
        //
        FileIOSizeT bytesLeftOnCurrSplit;
        FileIOSizeT splitBufferOffset;

        //
        // XXX: it is assumed that both are block size aligned, so that bytesToIssue is always block size aligned
        // also, bytesToIssue >= bytesLeftOnCurrSplit should always hold true
        //
        //
        bytesToIssue = min(bytesLeftToRead, remainingBytesOnCurSeg);


        if (firstSplitLeftToRead > 0) {
            //
            // First addr
            //
            //
            DestAddr = DestBuffer->FirstAddr;
            bytesLeftOnCurrSplit = firstSplitLeftToRead;
            splitBufferOffset = bytesRead;
        }
        else {
            //
            // Second addr
            //
            //
            SPDK_NOTICELOG("reading into second split, bytesLeftToRead: %d\n", bytesLeftToRead);
            DestAddr = DestBuffer->SecondAddr;
            bytesLeftOnCurrSplit = bytesLeftToRead;
            splitBufferOffset = bytesRead - DestBuffer->FirstSize;
        }

        //
        // If bytes to issue can all fit on curr split, then we don't need sg IO
        //
        //
        if (bytesLeftOnCurrSplit >= bytesToIssue) {
#ifdef OPT_FILE_SERVICE_ZERO_COPY
                result = ReadFromDiskAsyncZC(
                    DestAddr + splitBufferOffset,
                    curSegment,
                    offsetOnSegment,
                    bytesToIssue,
                    Callback,
                    Context,
                    Sto,
                    SPDKContext
                );
#else
                result = ReadFromDiskAsyncNonZC(
                    bytesRead,
                    curSegment,
                    offsetOnSegment,
                    bytesToIssue,
                    Callback,
                    Context,
                    Sto,
                    SPDKContext
                );
#endif
        }
        else {
            //
            // We need sg, and exactly 2 iovec's, one on 1st split, the other on 2nd split
            //
            //
            if (!(DestAddr == DestBuffer->FirstAddr)) {
                SPDK_ERRLOG("ERROR: SG start addr is not FirstAddr\n");
            }
            SlotContext->Iov[0].iov_base = DestAddr + splitBufferOffset;
            SlotContext->Iov[0].iov_len = bytesLeftOnCurrSplit;

            SlotContext->Iov[1].iov_base = DestBuffer->SecondAddr;
            SlotContext->Iov[1].iov_len = bytesToIssue - bytesLeftOnCurrSplit;

#ifdef OPT_FILE_SERVICE_ZERO_COPY
                result = ReadvFromDiskAsyncZC(
                    SlotContext->Iov,
                    2,
                    curSegment,
                    offsetOnSegment,
                    bytesToIssue,
                    Callback,
                    Context,
                    Sto,
                    SPDKContext
                );
#else
                result = ReadvFromDiskAsyncNonZC(
                    SlotContext->Iov,
                    2,
                    bytesRead,
                    curSegment,
                    offsetOnSegment,
                    bytesToIssue,
                    Callback,
                    Context,
                    Sto,
                    SPDKContext
                );
#endif
        }

        if (result != DDS_ERROR_CODE_SUCCESS) {
            SPDK_WARNLOG("ReadFromDiskAsync() failed with ret: %d\n", result);
            SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            SlotContext->Ctx->Response->BytesServiced = 0;
            SlotContext->Ctx->Response->LogicalBytes = 0;
            return result;
        }
        else {
            SlotContext->CallbacksToRun += 1;
        }

        curOffset += bytesToIssue;
        bytesLeftToRead -= bytesToIssue;
    }

    return result;
}
#endif

/**
 * Async read from a file using the per-file segment map.
 * Updated: publish Result with atomic store after BytesServiced is set.
 * Updated: keep disk I/O aligned while reporting logical bytes for EOF tails.
 * Updated: align disk I/O offset using the bdev block size while preserving logical offsets.
 * Updated: derive logical bytes from the original request to avoid over-copying for unaligned reads.
 */
ErrorCodeT ReadFile(FileIdT FileId, FileSizeT Offset, SplittableBufferT *DestBuffer, DiskIOCallback Callback,
                    ContextT Context, struct DPUStorage *Sto, void *SPDKContext)
{
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    struct DPUFile *file = Sto->AllFiles[FileId];
    struct PerSlotContext *SlotContext = (struct PerSlotContext *)Context;
    SlotContext->DestBuffer = DestBuffer;

    if (!file)
    {
        SPDK_WARNLOG("ReadFile() FileId %u not found\n", FileId);
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    FileSizeT remainingBytes = GetSize(file) - Offset;
    // jason: Align disk I/O offset down to bdev block size; payload buffer already includes aligned bytes.
    SPDKContextT *spdkContext = (SPDKContextT *)SPDKContext;
    FileSizeT blockSize = spdkContext ? (FileSizeT)spdkContext->block_size : 1;
    if (blockSize == 0)
    {
        blockSize = 1;
    }
    FileSizeT alignedOffset = Offset - (Offset % blockSize);
    // Original: bytesLeftToRead derived immediately after alignedOffset.
    // FileIOSizeT bytesLeftToRead = DestBuffer->TotalSize;
    // jason: ensure the request-time CopyStart matches the run-time aligned offset.

    FileIOSizeT computedCopyStart = (FileIOSizeT)(Offset - alignedOffset);
    if (SlotContext->CopyStart != computedCopyStart)
    {
        fprintf(stderr, "%s [error]: CopyStart mismatch (req=%u computed=%u file=%u offset=%llu)\n", __func__,
                (unsigned)SlotContext->CopyStart, (unsigned)computedCopyStart, (unsigned)FileId,
                (unsigned long long)Offset);
        assert(SlotContext->CopyStart == computedCopyStart);
    }

    FileIOSizeT bytesLeftToRead = DestBuffer->TotalSize;
    // jason: Track logical bytes separately so I/O stays aligned while BytesServiced matches the request.
    // FileIOSizeT logicalBytesToRead = bytesLeftToRead;
    FileIOSizeT requestedBytes = DestBuffer->TotalSize;
    FileIOSizeT outputBytes = DestBuffer->TotalSize;
    // Original request byte extraction preserved for reference.
    // if (SlotContext->Ctx && SlotContext->Ctx->Request) {
    //     requestedBytes = SlotContext->Ctx->Request->Bytes;
    // }
    if (SlotContext->Ctx)
    {
        requestedBytes = SlotContext->Ctx->LogicalBytes;
        outputBytes = SlotContext->Ctx->OutputBytes;
    }
    // jason: read2 stage metadata lives on the slot context and overrides output sizing.
    bool hasStages =
        SlotContext->Ctx && SlotContext->Ctx->OpCode == BUFF_MSG_F2B_REQ_OP_READ2 && SlotContext->StageCount > 0;
    SlotContext->StageChainRequiredWithoutIO = false;
    if (hasStages)
    {
        uint16_t stageCount = SlotContext->StageCount;
        if (stageCount <= 2)
        {
            outputBytes = SlotContext->StageSizes[stageCount - 1];
        }
    }
    if (outputBytes == 0 && requestedBytes > 0)
    {
        // jason: guard legacy callers that don't populate OutputBytes.
        outputBytes = requestedBytes;
    }
    if (requestedBytes > DestBuffer->TotalSize) {
        // jason: guard against request size mismatches to avoid over-reporting.
        requestedBytes = DestBuffer->TotalSize;
    }
    FileIOSizeT logicalBytesToRead = requestedBytes;
    FileSizeT fileSize = GetSize(file);

    if (Offset > GetSize(file))
    {
        SPDK_WARNLOG("Offset %lu > GetSize(file) %lu !\n", Offset, GetSize(file));
        return DDS_ERROR_CODE_IO_FAILURE;
    }

    // if (remainingBytes < DestBuffer->TotalSize)
    if (remainingBytes < requestedBytes)
    {
        //
        // TODO: bytes serviced can be smaller than requested, probably not a problem?
        //
        //
        // SPDK_WARNLOG("found remainingBytes < DestBuffer->TotalSize, GetSize(file): %lu, Offset: %lu\n", fileSize,
        //              Offset);
        SPDK_WARNLOG("found remainingBytes < requestedBytes, GetSize(file): %lu, Offset: %lu\n", fileSize, Offset);
        // bytesLeftToRead = (FileIOSizeT)remainingBytes;
        // jason: keep I/O aligned; only clamp the logical bytes reported back.
        logicalBytesToRead = (FileIOSizeT)remainingBytes;
    }

    // jason: If the request asked for zero logical bytes, return early (no padding).
    if (requestedBytes == 0)
    {
        printf("WARNING: ReadFile() zero logical bytes to read\n");
        SlotContext->BytesIssued = 0;
        SlotContext->LogicalBytesIssued = 0;
        SlotContext->Ctx->Response->BytesServiced = 0;
        SlotContext->Ctx->Response->LogicalBytes = 0;
        // jason: publish Result after BytesServiced so poller sees a complete response.
        DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_SUCCESS,
                                   DDS_ATOMIC_ORDER_RELEASE);
        return DDS_ERROR_CODE_SUCCESS;
    }
    // jason: If EOF clamped to zero but request was non-zero, complete without I/O.
    if (logicalBytesToRead == 0)
    {
        if (hasStages)
        {
            // jason: read2 still needs to run its stage chain even without disk I/O.
            SlotContext->BytesIssued = outputBytes;
            SlotContext->LogicalBytesIssued = 0;
            SlotContext->StageChainRequiredWithoutIO = true;
            return DDS_ERROR_CODE_SUCCESS;
        }
        SlotContext->BytesIssued = outputBytes;
        SlotContext->LogicalBytesIssued = 0;
        SlotContext->Ctx->Response->BytesServiced = outputBytes;
        SlotContext->Ctx->Response->LogicalBytes = 0;
        DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_SUCCESS,
                                   DDS_ATOMIC_ORDER_RELEASE);
        return DDS_ERROR_CODE_SUCCESS;
    }

    const FileIOSizeT totalBytesToRead = bytesLeftToRead;
    // SlotContext->BytesIssued = bytesLeftToRead;
    // jason: report output bytes while tracking logical bytes separately.
    SlotContext->BytesIssued = outputBytes;
    SlotContext->LogicalBytesIssued = logicalBytesToRead;

    // FileSizeT curOffset = Offset;
    // jason: Issue disk I/O from aligned offset while reporting logical bytes.
    FileSizeT curOffset = alignedOffset;
    SegmentIdT physicalSegment;
    SegmentSizeT offsetOnSegment;
    SegmentSizeT bytesToIssue;
    SegmentSizeT remainingBytesOnCurSeg;
    BufferT DestAddr;

    /* if (DestBuffer->SecondAddr) {
        printf("read buffer has second split!\n");
    } */

    while (bytesLeftToRead)
    {
        //
        // Cross-boundary detection using the file segment map.
        //
        //
        result = TranslateFileOffsetToSegment(file, curOffset, &physicalSegment, &offsetOnSegment);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            SPDK_WARNLOG("TranslateFileOffsetToSegment failed with ret: %d\n", result);
            SlotContext->Ctx->Response->BytesServiced = 0;
            SlotContext->Ctx->Response->LogicalBytes = 0;
            // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            // jason: publish Result after BytesServiced so poller sees a complete response.
            DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_IO_FAILURE,
                                       DDS_ATOMIC_ORDER_RELEASE);
            return result;
        }
        if (physicalSegment < 0 || physicalSegment >= Sto->TotalSegments)
        {
            SPDK_ERRLOG("Physical segment %d out of range\n", physicalSegment);
            SlotContext->Ctx->Response->BytesServiced = 0;
            SlotContext->Ctx->Response->LogicalBytes = 0;
            // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            // jason: publish Result after BytesServiced so poller sees a complete response.
            DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_IO_FAILURE,
                                       DDS_ATOMIC_ORDER_RELEASE);
            return DDS_ERROR_CODE_IO_FAILURE;
        }
        remainingBytesOnCurSeg = DDS_BACKEND_SEGMENT_SIZE - offsetOnSegment;

        FileIOSizeT bytesRead = totalBytesToRead - bytesLeftToRead;
        int firstSplitLeftToRead = DestBuffer->FirstSize - bytesRead;

        if (DestBuffer->FirstSize <= bytesRead)
        {
            printf("firstSplitLeftToWrite should be <= 0, is: %d\n", firstSplitLeftToRead);
            printf("total size is: %u\n", DestBuffer->TotalSize);
        }

        if (DestBuffer->FirstSize - bytesRead < 0)
        {
            //
            // TODO: check
            //
            //
            SPDK_ERRLOG("int firstSplitLeftToRead = DestBuffer->FirstSize - bytesRead, underflow, %u - %u = %d\n",
                        DestBuffer->FirstSize, bytesRead, firstSplitLeftToRead);
        }

        //
        // May be 1st or 2nd split, the no. of bytes left to read onto it
        //
        //
        FileIOSizeT bytesLeftOnCurrSplit;
        FileIOSizeT splitBufferOffset;

        //
        // XXX: it is assumed that both are block size aligned, so that bytesToIssue is always block size aligned
        // also, bytesToIssue >= bytesLeftOnCurrSplit should always hold true
        //
        //
        bytesToIssue = min(bytesLeftToRead, remainingBytesOnCurSeg);

        if (firstSplitLeftToRead > 0)
        {
            //
            // First addr
            //
            //
            DestAddr = DestBuffer->FirstAddr;
            bytesLeftOnCurrSplit = firstSplitLeftToRead;
            splitBufferOffset = bytesRead;
        }
        else
        {
            //
            // Second addr
            //
            //
            SPDK_NOTICELOG("reading into second split, bytesLeftToRead: %d\n", bytesLeftToRead);
            DestAddr = DestBuffer->SecondAddr;
            bytesLeftOnCurrSplit = bytesLeftToRead;
            splitBufferOffset = bytesRead - DestBuffer->FirstSize;
        }

        //
        // If bytes to issue can all fit on curr split, then we don't need sg IO
        //
        //
        if (bytesLeftOnCurrSplit >= bytesToIssue)
        {
#ifdef OPT_FILE_SERVICE_ZERO_COPY
            result = ReadFromDiskAsyncZC(DestAddr + splitBufferOffset, physicalSegment, offsetOnSegment, bytesToIssue,
                                         Callback, Context, Sto, SPDKContext);
#else
            result = ReadFromDiskAsyncNonZC(bytesRead, physicalSegment, offsetOnSegment, bytesToIssue, Callback,
                                            Context, Sto, SPDKContext);
#endif
        }
        else
        {
            //
            // We need sg, and exactly 2 iovec's, one on 1st split, the other on 2nd split
            //
            //
            if (!(DestAddr == DestBuffer->FirstAddr))
            {
                SPDK_ERRLOG("ERROR: SG start addr is not FirstAddr\n");
            }
            SlotContext->Iov[0].iov_base = DestAddr + splitBufferOffset;
            SlotContext->Iov[0].iov_len = bytesLeftOnCurrSplit;

            SlotContext->Iov[1].iov_base = DestBuffer->SecondAddr;
            SlotContext->Iov[1].iov_len = bytesToIssue - bytesLeftOnCurrSplit;

#ifdef OPT_FILE_SERVICE_ZERO_COPY
            result = ReadvFromDiskAsyncZC(SlotContext->Iov, 2, physicalSegment, offsetOnSegment, bytesToIssue, Callback,
                                          Context, Sto, SPDKContext);
#else
            result = ReadvFromDiskAsyncNonZC(SlotContext->Iov, 2, bytesRead, physicalSegment, offsetOnSegment,
                                             bytesToIssue, Callback, Context, Sto, SPDKContext);
#endif
        }

        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            SPDK_WARNLOG("ReadFromDiskAsync() failed with ret: %d\n", result);
            SlotContext->Ctx->Response->BytesServiced = 0;
            SlotContext->Ctx->Response->LogicalBytes = 0;
            // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            // jason: publish Result after BytesServiced so poller sees a complete response.
            DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_IO_FAILURE,
                                       DDS_ATOMIC_ORDER_RELEASE);
            return result;
        }
        else
        {
            SlotContext->CallbacksToRun += 1;
        }

        curOffset += bytesToIssue;
        bytesLeftToRead -= bytesToIssue;
    }

    return result;
}

//
// Async write to a file
//
#if 0
ErrorCodeT WriteFile(
    FileIdT FileId,
    FileSizeT Offset,
    SplittableBufferT *SourceBuffer,
    DiskIOCallback Callback,
    ContextT Context,
    struct DPUStorage* Sto,
    void *SPDKContext
){
    struct DPUFile* file = Sto->AllFiles[FileId];
    FileSizeT newSize = Offset + SourceBuffer->TotalSize;
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    struct PerSlotContext* SlotContext = (struct PerSlotContext*)Context;

    //
    // Check if allocation is needed
    //
    //
    FileSizeT currentFileSize = GetFileProperties(file)->FProperties.FileSize;
    long long sizeToChange = newSize - currentFileSize;

    if (sizeToChange >= 0) {
        // SPDK_NOTICELOG("file size changed, new size: %llu\n", newSize);
        size_t numSeg = GetNumSegments(file);
        SegmentSizeT remainingAllocatedSize = (SegmentSizeT)(numSeg * DDS_BACKEND_SEGMENT_SIZE - currentFileSize);
        long long bytesToBeAllocated = sizeToChange - (long long)remainingAllocatedSize;

        if (bytesToBeAllocated > 0) {
            SegmentIdT numSegmentsToAllocate = (SegmentIdT)(
                bytesToBeAllocated / DDS_BACKEND_SEGMENT_SIZE +
                (bytesToBeAllocated % DDS_BACKEND_SEGMENT_SIZE == 0 ? 0 : 1)
                );

            //
            // Allocate segments
            //
            //
            pthread_mutex_lock(&Sto->SegmentAllocationMutex);

            if (numSegmentsToAllocate > Sto->AvailableSegments) {
                SPDK_ERRLOG("Need to allocate seg for file, but not enough available segs left!\n");
                pthread_mutex_unlock(&Sto->SegmentAllocationMutex);
                return DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE;
            }

            SegmentIdT allocatedSegments = 0;
            for (SegmentIdT s = 0; s != Sto->TotalSegments; s++) {
                SegmentT* seg = &Sto->AllSegments[s];
                if (seg->Allocatable && seg->FileId == DDS_FILE_INVALID) {
                    AllocateSegment(s,file);
                    seg->FileId = FileId;
                    allocatedSegments++;
                    if (allocatedSegments == numSegmentsToAllocate) {
                        break;
                    }
                }
            }

            pthread_mutex_unlock(&Sto->SegmentAllocationMutex);
        }
        SetSize(newSize,file);
    }

    FileIOSizeT bytesLeftToWrite = SourceBuffer->TotalSize;
    SlotContext->BytesIssued = bytesLeftToWrite;

    FileSizeT curOffset = Offset;
    SegmentIdT curSegment;
    SegmentSizeT offsetOnSegment;
    SegmentSizeT bytesToIssue;
    SegmentSizeT remainingBytesOnCurSeg;
    BufferT SourceAddr = SourceBuffer->FirstAddr;
    
    /* char expected[8192];
    memset(expected, 42, 8192);
    if (memcmp(SourceBuffer->FirstAddr, expected, SourceBuffer->FirstSize)) {
        printf("write got first split that doesn't look correct, offset: %p, 1st size: %u, total size: %u\n",
            Offset, SourceBuffer->FirstSize, SourceBuffer->TotalSize);
        if (SourceBuffer->SecondAddr == NULL) {
            printf("and got no second split\n");
        }
    }

    if (SourceBuffer->SecondAddr) {
        printf("write buffer has second split!, first size: %u, offset: %p\n", SourceBuffer->FirstSize, Offset);
        if (memcmp(SourceBuffer->SecondAddr, expected, SourceBuffer->TotalSize - SourceBuffer->FirstSize)) {
            printf("write got second split that doesn't look correct???\n");
        }
    } */

    while (bytesLeftToWrite) {
        //
        // Cross-boundary detection
        //
        //
        curSegment = (SegmentIdT)(curOffset / DDS_BACKEND_SEGMENT_SIZE);
        offsetOnSegment = curOffset % DDS_BACKEND_SEGMENT_SIZE;
        remainingBytesOnCurSeg = DDS_BACKEND_SEGMENT_SIZE - offsetOnSegment;

        FileIOSizeT bytesWritten = SourceBuffer->TotalSize - bytesLeftToWrite;
        int firstSplitLeftToWrite = SourceBuffer->FirstSize - bytesWritten;

        if (SourceBuffer->FirstSize <= bytesWritten) {
            printf("firstSplitLeftToWrite should be <= 0, is: %d\n", firstSplitLeftToWrite);
            printf("total size is: %u\n", SourceBuffer->TotalSize);
        }

        //
        // May be 1st or 2nd split, the no. of bytes left to write on it
        //
        //
        FileIOSizeT bytesLeftOnCurrSplit;
        FileIOSizeT splitBufferOffset;

        bytesToIssue = min(bytesLeftToWrite, remainingBytesOnCurSeg);
        
        if (firstSplitLeftToWrite > 0) {
            //
            // Writing from first addr
            //
            //
            SourceAddr = SourceBuffer->FirstAddr;
            bytesLeftOnCurrSplit = firstSplitLeftToWrite;
            splitBufferOffset = bytesWritten;
        }
        else {
            //
            // Writing from second addr
            //
            //
            SPDK_NOTICELOG("Writing from second split, bytesLeftToWrite: %d\n", bytesLeftToWrite);
            SourceAddr = SourceBuffer->SecondAddr;
            bytesLeftOnCurrSplit = bytesLeftToWrite;
            splitBufferOffset = bytesWritten - SourceBuffer->FirstSize;
        }
        
        if (bytesLeftOnCurrSplit >= bytesToIssue) {
            //
            // If bytes to issue can all fit on curr split, then we don't need sg IO
            //
            //
#ifdef OPT_FILE_SERVICE_ZERO_COPY
            result = WriteToDiskAsyncZC(
                SourceAddr + splitBufferOffset,
                curSegment,
                offsetOnSegment,
                bytesToIssue,
                Callback,
                SlotContext,
                Sto,
                SPDKContext
            );
#else
                result = WriteToDiskAsyncNonZC(
                    SourceAddr + splitBufferOffset,
                    bytesWritten,
                    curSegment,
                    offsetOnSegment,
                    bytesToIssue,
                    Callback,
                    SlotContext,
                    Sto,
                    SPDKContext
                );
#endif
        }
        else {
            //
            // We need sg, and exactly 2 iovec's, one on 1st split, the other on 2nd split
            //
            //
            assert (SourceAddr == SourceBuffer->FirstAddr);
            SlotContext->Iov[0].iov_base = SourceAddr + splitBufferOffset;
            SlotContext->Iov[0].iov_len = bytesLeftOnCurrSplit;
            SlotContext->Iov[1].iov_base = SourceBuffer->SecondAddr;
            SlotContext->Iov[1].iov_len = bytesToIssue - bytesLeftOnCurrSplit;
            /* printf("bytesToIssue: %u, FirstSize: %u, TotalSize: %u, first len: %u, second len: %u\n", bytesToIssue,
                SourceBuffer->FirstSize, SourceBuffer->TotalSize, SlotContext->Iov[0].iov_len, SlotContext->Iov[1].iov_len); */
            
#ifdef OPT_FILE_SERVICE_ZERO_COPY
                result = WritevToDiskAsyncZC(
                    &SlotContext->Iov[0],
                    2,
                    curSegment,
                    offsetOnSegment,
                    bytesToIssue,
                    Callback,
                    Context,
                    Sto,
                    SPDKContext
                );
#else
                result = WritevToDiskAsyncNonZC(
                    &SlotContext->Iov[0],
                    2,
                    bytesWritten,
                    curSegment,
                    offsetOnSegment,
                    bytesToIssue,
                    Callback,
                    Context,
                    Sto,
                    SPDKContext
                );
#endif
        }

        if (result != DDS_ERROR_CODE_SUCCESS) {
            SPDK_WARNLOG("WriteToDiskAsync() failed with ret: %d\n", result);
            SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            SlotContext->Ctx->Response->BytesServiced = 0;
            SlotContext->Ctx->Response->LogicalBytes = 0;
            return result;
        }
        else {
            SlotContext->CallbacksToRun += 1;
        }

        curOffset += bytesToIssue;
        bytesLeftToWrite -= bytesToIssue;
    }

    return result;
}
#endif

/**
 * Async write to a file using the per-file segment map.
 * Updated: uses ChangeFileSizeDataPlane to mark metadata sync after data I/O.
 * Updated: publish Result with atomic store after BytesServiced is set.
 */
ErrorCodeT WriteFile(FileIdT FileId, FileSizeT Offset, SplittableBufferT *SourceBuffer, DiskIOCallback Callback,
                     ContextT Context, struct DPUStorage *Sto, void *SPDKContext)
{
    struct DPUFile *file = Sto->AllFiles[FileId];
    FileSizeT newSize = Offset + SourceBuffer->TotalSize;
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    struct PerSlotContext *SlotContext = (struct PerSlotContext *)Context;

    if (!file)
    {
        SPDK_WARNLOG("WriteFile() FileId %u not found\n", FileId);
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    // jason: reset data-plane metadata sync flags per request.
    SlotContext->NeedMetadataSync = false;
    SlotContext->MetadataSyncIssued = false;

    //
    // Check if allocation is needed
    //
    //
    FileSizeT currentFileSize = GetFileProperties(file)->FProperties.FileSize;
    long long sizeToChange = (long long)newSize - (long long)currentFileSize;

    if (sizeToChange > 0)
    {
        result = ChangeFileSizeDataPlane(FileId, newSize, Sto, SlotContext);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            SPDK_ERRLOG("ChangeFileSizeDataPlane failed: %d\n", result);
            return result;
        }
    }

    FileIOSizeT bytesLeftToWrite = SourceBuffer->TotalSize;
    SlotContext->BytesIssued = bytesLeftToWrite;

    FileSizeT curOffset = Offset;
    SegmentIdT physicalSegment;
    SegmentSizeT offsetOnSegment;
    SegmentSizeT bytesToIssue;
    SegmentSizeT remainingBytesOnCurSeg;
    BufferT SourceAddr = SourceBuffer->FirstAddr;

    while (bytesLeftToWrite)
    {
        //
        // Cross-boundary detection using the file segment map.
        //
        //
        result = TranslateFileOffsetToSegment(file, curOffset, &physicalSegment, &offsetOnSegment);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            SPDK_WARNLOG("TranslateFileOffsetToSegment failed with ret: %d\n", result);
            SlotContext->Ctx->Response->BytesServiced = 0;
            SlotContext->Ctx->Response->LogicalBytes = 0;
            // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            // jason: publish Result after BytesServiced so poller sees a complete response.
            DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_IO_FAILURE,
                                       DDS_ATOMIC_ORDER_RELEASE);
            return result;
        }
        if (physicalSegment < 0 || physicalSegment >= Sto->TotalSegments)
        {
            SPDK_ERRLOG("Physical segment %d out of range\n", physicalSegment);
            SlotContext->Ctx->Response->BytesServiced = 0;
            SlotContext->Ctx->Response->LogicalBytes = 0;
            // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            // jason: publish Result after BytesServiced so poller sees a complete response.
            DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_IO_FAILURE,
                                       DDS_ATOMIC_ORDER_RELEASE);
            return DDS_ERROR_CODE_IO_FAILURE;
        }
        remainingBytesOnCurSeg = DDS_BACKEND_SEGMENT_SIZE - offsetOnSegment;

        FileIOSizeT bytesWritten = SourceBuffer->TotalSize - bytesLeftToWrite;
        int firstSplitLeftToWrite = SourceBuffer->FirstSize - bytesWritten;

        if (SourceBuffer->FirstSize <= bytesWritten)
        {
            printf("firstSplitLeftToWrite should be <= 0, is: %d\n", firstSplitLeftToWrite);
            printf("total size is: %u\n", SourceBuffer->TotalSize);
        }

        //
        // May be 1st or 2nd split, the no. of bytes left to write on it
        //
        //
        FileIOSizeT bytesLeftOnCurrSplit;
        FileIOSizeT splitBufferOffset;

        bytesToIssue = min(bytesLeftToWrite, remainingBytesOnCurSeg);

        if (firstSplitLeftToWrite > 0)
        {
            //
            // Writing from first addr
            //
            //
            SourceAddr = SourceBuffer->FirstAddr;
            bytesLeftOnCurrSplit = firstSplitLeftToWrite;
            splitBufferOffset = bytesWritten;
        }
        else
        {
            //
            // Writing from second addr
            //
            //
            SPDK_NOTICELOG("Writing from second split, bytesLeftToWrite: %d\n", bytesLeftToWrite);
            SourceAddr = SourceBuffer->SecondAddr;
            bytesLeftOnCurrSplit = bytesLeftToWrite;
            splitBufferOffset = bytesWritten - SourceBuffer->FirstSize;
        }

        if (bytesLeftOnCurrSplit >= bytesToIssue)
        {
            //
            // If bytes to issue can all fit on curr split, then we don't need sg IO
            //
            //
#ifdef OPT_FILE_SERVICE_ZERO_COPY
            result = WriteToDiskAsyncZC(SourceAddr + splitBufferOffset, physicalSegment, offsetOnSegment, bytesToIssue,
                                        Callback, SlotContext, Sto, SPDKContext);
#else
            result = WriteToDiskAsyncNonZC(SourceAddr + splitBufferOffset, bytesWritten, physicalSegment,
                                           offsetOnSegment, bytesToIssue, Callback, SlotContext, Sto, SPDKContext);
#endif
        }
        else
        {
            //
            // We need sg, and exactly 2 iovec's, one on 1st split, the other on 2nd split
            //
            //
            assert(SourceAddr == SourceBuffer->FirstAddr);
            SlotContext->Iov[0].iov_base = SourceAddr + splitBufferOffset;
            SlotContext->Iov[0].iov_len = bytesLeftOnCurrSplit;
            SlotContext->Iov[1].iov_base = SourceBuffer->SecondAddr;
            SlotContext->Iov[1].iov_len = bytesToIssue - bytesLeftOnCurrSplit;
            /* printf("bytesToIssue: %u, FirstSize: %u, TotalSize: %u, first len: %u, second len: %u\n", bytesToIssue,
                SourceBuffer->FirstSize, SourceBuffer->TotalSize, SlotContext->Iov[0].iov_len,
               SlotContext->Iov[1].iov_len); */

#ifdef OPT_FILE_SERVICE_ZERO_COPY
            result = WritevToDiskAsyncZC(&SlotContext->Iov[0], 2, physicalSegment, offsetOnSegment, bytesToIssue,
                                         Callback, Context, Sto, SPDKContext);
#else
            result = WritevToDiskAsyncNonZC(&SlotContext->Iov[0], 2, bytesWritten, physicalSegment, offsetOnSegment,
                                            bytesToIssue, Callback, Context, Sto, SPDKContext);
#endif
        }

        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            SPDK_WARNLOG("WriteToDiskAsync() failed with ret: %d\n", result);
            SlotContext->Ctx->Response->BytesServiced = 0;
            SlotContext->Ctx->Response->LogicalBytes = 0;
            // SlotContext->Ctx->Response->Result = DDS_ERROR_CODE_IO_FAILURE;
            // jason: publish Result after BytesServiced so poller sees a complete response.
            DDS_ATOMIC_ERRORCODE_STORE(&SlotContext->Ctx->Response->Result, DDS_ERROR_CODE_IO_FAILURE,
                                       DDS_ATOMIC_ORDER_RELEASE);
            return result;
        }
        else
        {
            SlotContext->CallbacksToRun += 1;
        }

        curOffset += bytesToIssue;
        bytesLeftToWrite -= bytesToIssue;
    }

    return result;
}

//
// Get file properties by file id (backend-only metadata)
//
//
ErrorCodeT GetFileInformationById(FileIdT FileId,
                                  // FilePropertiesT* FileProperties,
                                  DPUFileInfoT *FileProperties, struct DPUStorage *Sto, CtrlMsgB2FAckGetFileInfo *Resp)
{
    struct DPUFile* file = Sto->AllFiles[FileId];
    if (!file) {
        Resp->Result = DDS_ERROR_CODE_FILE_NOT_FOUND;
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    /* FileProperties->FileAttributes = GetAttributes(file);
    strcpy(FileProperties->FileName, GetName(file));
    FileProperties->FileSize = GetSize(file); */

    // Return persisted backend properties for front-end rehydration/mapping.
    FileProperties->FileAttributes = GetAttributes(file);
    FileProperties->CreationTime = file->Properties.FProperties.CreationTime;
    FileProperties->LastAccessTime = file->Properties.FProperties.LastAccessTime;
    FileProperties->LastWriteTime = file->Properties.FProperties.LastWriteTime;
    FileProperties->FileSize = GetSize(file);
    strcpy(FileProperties->FileName, GetName(file));
    Resp->Result = DDS_ERROR_CODE_SUCCESS;
    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get file attributes by file name
// 
//
ErrorCodeT
GetFileAttributes(
    FileIdT FileId,
    FileAttributesT* FileAttributes,
    struct DPUStorage* Sto,
    CtrlMsgB2FAckGetFileAttr *Resp
){
    struct DPUFile* file = Sto->AllFiles[FileId];
    if (!file) {
        Resp->Result = DDS_ERROR_CODE_FILE_NOT_FOUND;
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    *FileAttributes = GetAttributes(file);
    Resp->Result = DDS_ERROR_CODE_SUCCESS;
    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get the size of free space on the storage
// 
//
ErrorCodeT GetStorageFreeSpace(
    FileSizeT* StorageFreeSpace,
    struct DPUStorage* Sto,
    CtrlMsgB2FAckGetFreeSpace *Resp
){
    pthread_mutex_lock(&Sto->SegmentAllocationMutex);

    *StorageFreeSpace = Sto->AvailableSegments * (FileSizeT)DDS_BACKEND_SEGMENT_SIZE;

    pthread_mutex_unlock(&Sto->SegmentAllocationMutex);
    Resp->Result = DDS_ERROR_CODE_SUCCESS;
    return DDS_ERROR_CODE_SUCCESS;
}

void MoveFileCallback2(struct spdk_bdev_io *bdev_io, bool Success, ContextT Context) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {
        Unlock(HandlerCtx->NewDir);
        SetName(HandlerCtx->NewFileName, HandlerCtx->File);
        *(HandlerCtx->Result) = DDS_ERROR_CODE_SUCCESS;
        free(HandlerCtx);
    }
    else {
        SPDK_ERRLOG("RemoveDirectoryCallback2() failed\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }
}

void MoveFileCallback1(struct spdk_bdev_io *bdev_io, bool Success, ContextT Context) {
    ControlPlaneHandlerCtx *HandlerCtx = Context;
    spdk_bdev_free_io(bdev_io);
    if (Success) {  
        Unlock(HandlerCtx->dir);

        //
        // Update new directory
        //
        //
        Lock(HandlerCtx->NewDir);
    
        ErrorCodeT result = AddFile(HandlerCtx->FileId, HandlerCtx->NewDir);

        if (result != DDS_ERROR_CODE_SUCCESS) {
            Unlock(HandlerCtx->NewDir);
            *(HandlerCtx->Result) = result;
        }
        result = SyncDirToDisk(HandlerCtx->NewDir, Sto, HandlerCtx->SPDKContext,
            MoveFileCallback2, HandlerCtx);
        if (result != 0) {
            //
            // Fatal, can't continue, and the callback won't run
            //
            //
            SPDK_ERRLOG("MoveFileCallback1() returned %hu\n", result);
            Unlock(HandlerCtx->NewDir);
            *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
        }
        //
        // Else, the callback passed to it should be guaranteed to run
        //
        //
    }
    else {
        Unlock(HandlerCtx->dir);
        SPDK_ERRLOG("MoveFileCallback1() failed, can't create directory\n");
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }
}

//
// Move an existing file or a directory,
// including its children
// 
//
ErrorCodeT MoveFile(
    FileIdT FileId,
    DirIdT OldDirId,
    DirIdT NewDirId,
    const char* NewFileName,
    struct DPUStorage* Sto,
    void *SPDKContext,
    ControlPlaneHandlerCtx *HandlerCtx
){
    HandlerCtx->SPDKContext = SPDKContext;
    struct DPUFile* file = Sto->AllFiles[FileId];
    struct DPUDir* OldDir = Sto->AllDirs[OldDirId];
    struct DPUDir* NewDir = Sto->AllDirs[NewDirId];
    if (!file) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_DIR_NOT_FOUND;
        return DDS_ERROR_CODE_DIR_NOT_FOUND;
    }
    if (!OldDir || !NewDir) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_FILE_NOT_FOUND;
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    //
    // Update old directory
    //
    //
    Lock(OldDir);

    ErrorCodeT result = DeleteFile(FileId, OldDir);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        Unlock(OldDir);
        *(HandlerCtx->Result) = result;
        return result;
    }
    HandlerCtx->dir = OldDir;
    HandlerCtx->NewDir = NewDir;
    HandlerCtx->FileId = FileId;
    HandlerCtx->File = file;
    strcpy(HandlerCtx->NewFileName, NewFileName);
    result = SyncDirToDisk(OldDir, Sto, SPDKContext, MoveFileCallback1, HandlerCtx);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        *(HandlerCtx->Result) = DDS_ERROR_CODE_IO_FAILURE;
    }
    return result;
}
