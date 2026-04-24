#include <iostream>
#include <thread>
#include <string.h>

#include "DDSBackEndStorage.h"

namespace DDS_BackEnd {
    
//
// Constructor
// 
//
BackEndStorage::BackEndStorage() :
    TotalSegments(DDS_BACKEND_CAPACITY / DDS_BACKEND_SEGMENT_SIZE) {
    AllSegments = nullptr;
    AvailableSegments = 0;

    for (size_t d = 0; d != DDS_MAX_DIRS; d++) {
        AllDirs[d] = nullptr;
    }

    for (size_t f = 0; f != DDS_MAX_FILES; f++) {
        AllFiles[f] = nullptr;
    }

    TotalDirs = 0;
    TotalFiles = 0;

    TargetProgress = 0;
    CurrentProgress = 0;
}

//
// Destructor
// 
//
BackEndStorage::~BackEndStorage() {
    //
    // Free all bufffers
    //
    //
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

    //
    // Return all segments
    //
    //
    ReturnSegments();
}

//
// Read from disk synchronously
//
//
ErrorCodeT
BackEndStorage::ReadFromDiskSync(
    BufferT DstBuffer,
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes
) {
    SegmentT* seg = &AllSegments[SegmentId];
    memcpy(DstBuffer, (char*)seg->DiskAddress + SegmentOffset, Bytes);
    
    return DDS_ERROR_CODE_SUCCESS;
}

//
// Write to disk synchronously
//
//
ErrorCodeT
BackEndStorage::WriteToDiskSync(
    BufferT SrcBuffer,
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes
) {
    SegmentT* seg = &AllSegments[SegmentId];
    memcpy((char*)seg->DiskAddress + SegmentOffset, SrcBuffer, Bytes);

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Read from disk asynchronously
//
//
ErrorCodeT
BackEndStorage::ReadFromDiskAsync(
    BufferT DstBuffer,
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes,
    DiskIOCallback Callback,
    ContextT Context
) {
    SegmentT* seg = &AllSegments[SegmentId];
    memcpy(DstBuffer, (char*)seg->DiskAddress + SegmentOffset, Bytes);

    Callback(true, Context);

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Write to disk asynchronously
//
//
ErrorCodeT
BackEndStorage::WriteToDiskAsync(
    BufferT SrcBuffer,
    SegmentIdT SegmentId,
    SegmentSizeT SegmentOffset,
    FileIOSizeT Bytes,
    DiskIOCallback Callback,
    ContextT Context
) {
    SegmentT* seg = &AllSegments[SegmentId];
    memcpy((char*)seg->DiskAddress + SegmentOffset, SrcBuffer, Bytes);

    Callback(true, Context);

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Retrieve all segments on the disk
//
//
ErrorCodeT
BackEndStorage::RetrieveSegments() {
    //
    // We should get data from the disk,
    // but just create all segments using memory for now
    //
    //
    AllSegments = new SegmentT[TotalSegments];

    for (SegmentIdT i = 0; i != TotalSegments; i++) {
        AllSegments[i].Id = i;
        AllSegments[i].FileId = DDS_FILE_INVALID;
        
        char* newSegment = new char[DDS_BACKEND_SEGMENT_SIZE];

        if (!newSegment) {
            return DDS_ERROR_CODE_SEGMENT_RETRIEVAL_FAILURE;
        }

        memset(newSegment, 0, sizeof(char) * DDS_BACKEND_SEGMENT_SIZE);
        AllSegments[i].DiskAddress = (DiskSizeT)newSegment;
        
        if (i == DDS_BACKEND_RESERVED_SEGMENT) {
            AllSegments[i].Allocatable = false;
        }
        else {
            AllSegments[i].Allocatable = true;
            AvailableSegments++;
        }
    }

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Return all segments back to the disk
//
//
void
BackEndStorage::ReturnSegments() {
    //
    // Release the memory buffers
    //
    //
    if (AllSegments) {
        for (size_t i = 0; i != TotalSegments; i++) {
            if (AllSegments[i].DiskAddress) {
                delete[] (char*)AllSegments[i].DiskAddress;
            }

            if (AllSegments[i].Allocatable) {
                AvailableSegments--;
            }
        }

        delete[] AllSegments;
    }
}

//
// Load all directories from the reserved segment
//
//
ErrorCodeT
BackEndStorage::LoadDirectoriesAndFiles() {
    if (!AllSegments || TotalSegments == 0) {
        return DDS_ERROR_CODE_RESERVED_SEGMENT_ERROR;
    }

    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

    //
    // Read the first sector
    //
    //
    char tmpSectorBuffer[DDS_BACKEND_SECTOR_SIZE];
    result = ReadFromDiskSync(
        tmpSectorBuffer,
        DDS_BACKEND_RESERVED_SEGMENT,
        0,
        DDS_BACKEND_SECTOR_SIZE
    );

    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    TotalDirs = *((int*)(tmpSectorBuffer + DDS_BACKEND_INITIALIZATION_MARK_LENGTH));
    TotalFiles = *((int*)(tmpSectorBuffer + DDS_BACKEND_INITIALIZATION_MARK_LENGTH + sizeof(int)));

    //
    // Load the directories
    //
    //
    BackEndDir::PersistentPropertiesT dirOnDisk;
    SegmentSizeT nextAddress = DDS_BACKEND_SECTOR_SIZE;
    int loadedDirs = 0;
    for (size_t d = 0; d != DDS_MAX_DIRS; d++) {
        result = ReadFromDiskSync(
            (BufferT)&dirOnDisk,
            DDS_BACKEND_RESERVED_SEGMENT,
            nextAddress,
            sizeof(BackEndDir::PersistentPropertiesT)
        );

        if (result != DDS_ERROR_CODE_SUCCESS) {
            return result;
        }

        if (dirOnDisk.Id != DDS_DIR_INVALID) {
            AllDirs[d] = new BackEndDir(dirOnDisk.Id, DDS_DIR_ROOT, dirOnDisk.Name);
            if (AllDirs[d]) {
                memcpy(AllDirs[d]->GetProperties(), &dirOnDisk, sizeof(sizeof(BackEndDir::PersistentPropertiesT)));
            }
            else {
                return DDS_ERROR_CODE_OUT_OF_MEMORY;
            }

            loadedDirs++;
            if (loadedDirs == TotalDirs) {
                break;
            }
        }
        
        nextAddress += sizeof(BackEndDir::PersistentPropertiesT);
    }

    //
    // Load the files
    //
    //
    BackEndFile::PersistentPropertiesT fileOnDisk;
    nextAddress = DDS_BACKEND_SEGMENT_SIZE - sizeof(BackEndFile::PersistentPropertiesT);
    int loadedFiles = 0;
    for (size_t f = 0; f != DDS_MAX_FILES; f++) {
        result = ReadFromDiskSync(
            (BufferT)&fileOnDisk,
            DDS_BACKEND_RESERVED_SEGMENT,
            nextAddress,
            sizeof(BackEndFile::PersistentPropertiesT)
        );

        if (result != DDS_ERROR_CODE_SUCCESS) {
            return result;
        }

        if (fileOnDisk.Id != DDS_DIR_INVALID) {
            AllFiles[f] = new BackEndFile(fileOnDisk.Id, fileOnDisk.FProperties.FileName, fileOnDisk.FProperties.FileAttributes);
            if (AllFiles[f]) {
                memcpy(AllFiles[f]->GetProperties(), &fileOnDisk, sizeof(sizeof(BackEndFile::PersistentPropertiesT)));
                AllFiles[f]->SetNumAllocatedSegments();
            }
            else {
                return DDS_ERROR_CODE_OUT_OF_MEMORY;
            }

            loadedFiles++;
            if (loadedFiles == TotalFiles) {
                break;
            }
        }
        
        nextAddress -= sizeof(BackEndFile::PersistentPropertiesT);
    }

    return result;
}

//
// Synchronize a directory to the disk
//
//
inline
ErrorCodeT
BackEndStorage::SyncDirToDisk(BackEndDir* Dir) {
    return WriteToDiskSync(
        (BufferT)Dir->GetProperties(),
        DDS_BACKEND_RESERVED_SEGMENT,
        Dir->GetAddressOnSegment(),
        sizeof(BackEndDir::PersistentPropertiesT)
    );
}

//
// Synchronize a file to the disk
//
//
inline
ErrorCodeT
BackEndStorage::SyncFileToDisk(BackEndFile* File) {
    return WriteToDiskSync(
        (BufferT)File->GetProperties(),
        DDS_BACKEND_RESERVED_SEGMENT,
        File->GetAddressOnSegment(),
        sizeof(BackEndFile::PersistentPropertiesT)
    );
}

//
// Synchronize the first sector on the reserved segment
//
//
inline
ErrorCodeT
BackEndStorage::SyncReservedInformationToDisk() {
    char tmpSectorBuf[DDS_BACKEND_SECTOR_SIZE];

    memset(tmpSectorBuf, 0, DDS_BACKEND_SECTOR_SIZE);
    strcpy(tmpSectorBuf, DDS_BACKEND_INITIALIZATION_MARK);

    int* numDirs = (int*)(tmpSectorBuf + DDS_BACKEND_INITIALIZATION_MARK_LENGTH);
    *numDirs = TotalDirs;
    int* numFiles = numDirs + 1;
    *numFiles = TotalFiles;

    return WriteToDiskSync(
        tmpSectorBuf,
        DDS_BACKEND_RESERVED_SEGMENT,
        0,
        DDS_BACKEND_SECTOR_SIZE
    );
}

//
// Increment progress callback
//
//
void
IncrementProgressCallback(
    bool Success,
    ContextT Context) {
    Atomic<size_t>* progress = (Atomic<size_t>*)Context;
    (*progress) += 1;
}

//
// Initialize the backend service
//
//
ErrorCodeT
BackEndStorage::Initialize() {
    //
    // Retrieve all segments on disk
    //
    //
    ErrorCodeT result = RetrieveSegments();

    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    //
    // The first sector on the reserved segement contains initialization information
    //
    //
    bool diskInitialized = false;
    SegmentT* rSeg = &AllSegments[DDS_BACKEND_RESERVED_SEGMENT];
    char tmpSectorBuf[DDS_BACKEND_SECTOR_SIZE];
    memset(tmpSectorBuf, 0, DDS_BACKEND_SECTOR_SIZE);

    result = ReadFromDiskSync(tmpSectorBuf, DDS_BACKEND_RESERVED_SEGMENT, 0, DDS_BACKEND_SECTOR_SIZE);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    if (strcmp(DDS_BACKEND_INITIALIZATION_MARK, tmpSectorBuf)) {
        //
        // Empty every byte on the reserved segment
        //
        //
        char tmpPageBuf[DDS_BACKEND_PAGE_SIZE];
        memset(tmpPageBuf, 0, DDS_BACKEND_SECTOR_SIZE);
        
        size_t pagesPerSegment = DDS_BACKEND_SEGMENT_SIZE / DDS_BACKEND_PAGE_SIZE;
        size_t numPagesWritten = 0;

        //
        // Issue writes with the maximum queue depth
        //
        //
        while ((numPagesWritten + DDS_BACKEND_QUEUE_DEPTH_PAGE_IO_DEFAULT) <= pagesPerSegment) {
            TargetProgress = pagesPerSegment;
            CurrentProgress = 0;
            for (size_t q = 0; q != DDS_BACKEND_QUEUE_DEPTH_PAGE_IO_DEFAULT; q++) {
                result = WriteToDiskAsync(
                    tmpPageBuf,
                    0,
                    (SegmentSizeT)((numPagesWritten + q) * DDS_BACKEND_PAGE_SIZE),
                    DDS_BACKEND_PAGE_SIZE,
                    IncrementProgressCallback,
                    &CurrentProgress
                );

                if (result != DDS_ERROR_CODE_SUCCESS) {
                    return result;
                }
            }

            while (CurrentProgress != TargetProgress) {
                std::this_thread::yield();
            }

            numPagesWritten += DDS_BACKEND_QUEUE_DEPTH_PAGE_IO_DEFAULT;
        }

        //
        // Handle the remaining pages
        //
        //
        size_t pagesLeft = pagesPerSegment - numPagesWritten;
        TargetProgress = pagesLeft;
        CurrentProgress = 0;
        for (size_t q = 0; q != pagesLeft; q++) {
            result = WriteToDiskAsync(
                tmpPageBuf,
                0,
                (SegmentSizeT)((numPagesWritten + q) * DDS_BACKEND_PAGE_SIZE),
                DDS_BACKEND_PAGE_SIZE,
                IncrementProgressCallback,
                &CurrentProgress
            );

            if (result != DDS_ERROR_CODE_SUCCESS) {
                return result;
            }
        }

        while (CurrentProgress != TargetProgress) {
            std::this_thread::yield();
        }

        //
        // Create the root directory, which is on the second sector on the segment
        //
        //
        BackEndDir* rootDir = new BackEndDir(DDS_DIR_ROOT, DDS_DIR_INVALID, DDS_BACKEND_ROOT_DIR_NAME);
        result = SyncDirToDisk(rootDir);

        if (result != DDS_ERROR_CODE_SUCCESS) {
            return result;
        }

        AllDirs[DDS_DIR_ROOT] = rootDir;

        //
        // Set the formatted mark and the numbers of dirs and files
        //
        //
        TotalDirs = 1;
        TotalFiles = 0;
        result = SyncReservedInformationToDisk();

        if (result != DDS_ERROR_CODE_SUCCESS) {
            return result;
        }
    }
    else {
        //
        // Load directories and files
        //
        //
        result = LoadDirectoriesAndFiles();

        if (result != DDS_ERROR_CODE_SUCCESS) {
            return result;
        }

        //
        // Update segment information
        //
        //
        int checkedFiles = 0;
        for (FileIdT f = 0; f != DDS_MAX_FILES; f++) {
            BackEndFile* file = AllFiles[f];
            if (file) {
                SegmentIdT* segments = file->GetProperties()->Segments;
                for (size_t s = 0; s != DDS_BACKEND_MAX_SEGMENTS_PER_FILE; s++) {
                    SegmentIdT currentSeg = segments[s];
                    if (currentSeg == DDS_BACKEND_SEGMENT_INVALID) {
                        break;
                    }
                    else {
                        AllSegments[currentSeg].FileId = f;
                    }
                }

                checkedFiles++;
                if (checkedFiles == TotalFiles) {
                    break;
                }
            }
        }
    }
}

//
// Create a diretory
// 
//
ErrorCodeT
BackEndStorage::CreateDirectory(
    const char* PathName,
    DirIdT DirId,
    DirIdT ParentId
) {
    BackEndDir* dir = new BackEndDir(DirId, ParentId, PathName);
    if (!dir) {
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }
    
    ErrorCodeT result = SyncDirToDisk(dir);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    AllDirs[DirId] = dir;

    SectorModificationMutex.lock();

    TotalDirs++;
    result = SyncReservedInformationToDisk();

    SectorModificationMutex.unlock();

    return result;
}

//
// Delete a directory
// 
//
ErrorCodeT
BackEndStorage::RemoveDirectory(
    DirIdT DirId
) {
    if (!AllDirs[DirId]) {
        return DDS_ERROR_CODE_SUCCESS;
    }

    AllDirs[DirId]->GetProperties()->Id = DDS_DIR_INVALID;
    ErrorCodeT result = SyncDirToDisk(AllDirs[DirId]);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    delete AllDirs[DirId];
    AllDirs[DirId] = nullptr;

    SectorModificationMutex.lock();

    TotalDirs--;
    result = SyncReservedInformationToDisk();

    SectorModificationMutex.unlock();

    return result;
}

//
// Create a file
// 
//
ErrorCodeT
BackEndStorage::CreateFile(
    const char* FileName,
    FileAttributesT FileAttributes,
    FileIdT FileId,
    DirIdT DirId
) {
    BackEndDir* dir = AllDirs[DirId];
    if (!dir) {
        return DDS_ERROR_CODE_DIR_NOT_FOUND;
    }

    BackEndFile* file = new BackEndFile(FileId, FileName, FileAttributes);
    if (!file) {
        return DDS_ERROR_CODE_OUT_OF_MEMORY;
    }

    //
    // Make file persistent
    //
    //
    ErrorCodeT result = SyncFileToDisk(file);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    //
    // Add this file to its directory and make the update persistent
    //
    //
    dir->Lock();
    
    dir->AddFile(FileId);
    result = SyncDirToDisk(dir);
    
    dir->Unlock();

    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    //
    // Finally, file is added
    //
    //
    AllFiles[DirId] = file;

    SectorModificationMutex.lock();

    TotalFiles++;
    result = SyncReservedInformationToDisk();

    SectorModificationMutex.unlock();

    return result;
}

//
// Delete a file
// 
//
ErrorCodeT
BackEndStorage::DeleteFile(
    FileIdT FileId,
    DirIdT DirId
) {
    BackEndFile* file = AllFiles[FileId];
    BackEndDir* dir = AllDirs[DirId];

    if (!file || !dir) {
        return DDS_ERROR_CODE_SUCCESS;
    }

    file->GetProperties()->Id = DDS_FILE_INVALID;
    
    //
    // Make the change persistent
    //
    //
    ErrorCodeT result = SyncFileToDisk(file);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    //
    // Delete this file from its directory and make the update persistent
    //
    //
    dir->Lock();

    dir->DeleteFile(FileId);
    result = SyncDirToDisk(dir);

    dir->Unlock();

    //
    // Finally, delete the file
    //
    //
    delete file;
    AllFiles[FileId] = nullptr;

    SectorModificationMutex.lock();

    TotalFiles--;
    result = SyncReservedInformationToDisk();

    SectorModificationMutex.unlock();

    return result;
}

//
// Change the size of a file
// 
//
ErrorCodeT
BackEndStorage::ChangeFileSize(
    FileIdT FileId,
    FileSizeT NewSize
) {
    BackEndFile* file = AllFiles[FileId];

    if (!file) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    //
    // There might be concurrent writes, hoping apps will handle the concurrency
    //
    //
    FileSizeT currentFileSize = file->GetProperties()->FProperties.FileSize;
    long long sizeToChange = NewSize - currentFileSize;

    if (sizeToChange >= 0) {
        SegmentSizeT remainingAllocatedSize = (SegmentSizeT)((size_t)file->GetNumSegments() * DDS_BACKEND_SEGMENT_SIZE - currentFileSize);
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
            SegmentAllocationMutex.lock();

            if (numSegmentsToAllocate > AvailableSegments) {
                SegmentAllocationMutex.unlock();
                return DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE;
            }

            SegmentIdT allocatedSegments = 0;
            for (SegmentIdT s = 0; s != TotalSegments; s++) {
                SegmentT* seg = &AllSegments[s];
                if (seg->Allocatable && seg->FileId == DDS_FILE_INVALID) {
                    file->AllocateSegment(s);
                    seg->FileId = FileId;
                    allocatedSegments++;
                    if (allocatedSegments == numSegmentsToAllocate) {
                        break;
                    }
                }
            }

            SegmentAllocationMutex.unlock();
        }
    }
    else {
        SegmentIdT newSegments = (SegmentIdT)(NewSize / DDS_BACKEND_SEGMENT_SIZE + (NewSize % DDS_BACKEND_SEGMENT_SIZE == 0 ? 0 : 1));
        SegmentIdT numSegmentsToDeallocate = file->GetNumSegments() - newSegments;
        if (numSegmentsToDeallocate > 0) {
            //
            // Deallocate segments
            //
            //
            SegmentAllocationMutex.lock();

            for (SegmentIdT s = 0; s != numSegmentsToDeallocate; s++) {
                file->DeallocateSegment();
            }

            SegmentAllocationMutex.unlock();
        }
    }

    //
    // Segment (de)allocation has been taken care of
    //
    //
    file->SetSize(NewSize);

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get file size
// 
//
ErrorCodeT
BackEndStorage::GetFileSize(
    FileIdT FileId,
    FileSizeT* FileSize
) {
    BackEndFile* file = AllFiles[FileId];

    if (!file) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    *FileSize = file->GetSize();

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Async read from a file
// 
//
ErrorCodeT
BackEndStorage::ReadFile(
    FileIdT FileId,
    FileSizeT Offset,
    BufferT DestBuffer,
    FileIOSizeT BytesToRead,
    DiskIOCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    BackEndFile* file = AllFiles[FileId];
    BackEndIOContextT* ioContext = (BackEndIOContextT*)Context;
    FileSizeT remainingBytes = file->GetSize() - Offset;

    FileIOSizeT bytesLeftToRead = BytesToRead;
    if (remainingBytes < BytesToRead) {
        bytesLeftToRead = (FileIOSizeT)remainingBytes;
    }

    ioContext->BytesIssued = bytesLeftToRead;

    FileSizeT curOffset = Offset;
    SegmentIdT curSegment;
    SegmentSizeT offsetOnSegment;
    SegmentSizeT bytesToIssue;
    SegmentSizeT remainingBytesOnCurSeg;
    while (bytesLeftToRead) {
        //
        // Cross-boundary detection
        //
        //
        curSegment = (SegmentIdT)(curOffset / DDS_BACKEND_SEGMENT_SIZE);
        offsetOnSegment = curOffset % DDS_BACKEND_SEGMENT_SIZE;
        remainingBytesOnCurSeg = DDS_BACKEND_SEGMENT_SIZE - offsetOnSegment;
        
        if (remainingBytesOnCurSeg >= bytesLeftToRead) {
            bytesToIssue = bytesLeftToRead;
        }
        else {
            bytesToIssue = remainingBytesOnCurSeg;
        }

        result = ReadFromDiskAsync(
            DestBuffer + (curOffset - Offset),
            curSegment,
            offsetOnSegment,
            bytesToIssue,
            Callback,
            Context
        );

        if (result != DDS_ERROR_CODE_SUCCESS) {
            return result;
        }

        curOffset += bytesToIssue;
        bytesLeftToRead -= bytesToIssue;
    }

    return result;
}

//
// Async write to a file
// 
//
ErrorCodeT
BackEndStorage::WriteFile(
    FileIdT FileId,
    FileSizeT Offset,
    BufferT SourceBuffer,
    FileIOSizeT BytesToWrite,
    DiskIOCallback Callback,
    ContextT Context
) {
    BackEndFile* file = AllFiles[FileId];
    FileSizeT newSize = Offset + BytesToWrite;
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    BackEndIOContextT* ioContext = (BackEndIOContextT*)Context;

    //
    // Check if allocation is needed
    //
    //
    FileSizeT currentFileSize = file->GetProperties()->FProperties.FileSize;
    long long sizeToChange = newSize - currentFileSize;

    if (sizeToChange >= 0) {
        SegmentSizeT remainingAllocatedSize = (SegmentSizeT)((size_t)file->GetNumSegments() * DDS_BACKEND_SEGMENT_SIZE - currentFileSize);
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
            SegmentAllocationMutex.lock();

            if (numSegmentsToAllocate > AvailableSegments) {
                SegmentAllocationMutex.unlock();
                return DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE;
            }

            SegmentIdT allocatedSegments = 0;
            for (SegmentIdT s = 0; s != TotalSegments; s++) {
                SegmentT* seg = &AllSegments[s];
                if (seg->Allocatable && seg->FileId == DDS_FILE_INVALID) {
                    file->AllocateSegment(s);
                    seg->FileId = FileId;
                    allocatedSegments++;
                    if (allocatedSegments == numSegmentsToAllocate) {
                        break;
                    }
                }
            }

            SegmentAllocationMutex.unlock();
        }

        file->SetSize(newSize);
    }

    FileIOSizeT bytesLeftToWrite = BytesToWrite;
    ioContext->BytesIssued = bytesLeftToWrite;

    FileSizeT curOffset = Offset;
    SegmentIdT curSegment;
    SegmentSizeT offsetOnSegment;
    SegmentSizeT bytesToIssue;
    SegmentSizeT remainingBytesOnCurSeg;
    while (bytesLeftToWrite) {
        //
        // Cross-boundary detection
        //
        //
        curSegment = (SegmentIdT)(curOffset / DDS_BACKEND_SEGMENT_SIZE);
        offsetOnSegment = curOffset % DDS_BACKEND_SEGMENT_SIZE;
        remainingBytesOnCurSeg = DDS_BACKEND_SEGMENT_SIZE - offsetOnSegment;

        if (remainingBytesOnCurSeg >= bytesLeftToWrite) {
            bytesToIssue = bytesLeftToWrite;
        }
        else {
            bytesToIssue = remainingBytesOnCurSeg;
        }

        result = WriteToDiskAsync(
            SourceBuffer + (curOffset - Offset),
            curSegment,
            offsetOnSegment,
            bytesToIssue,
            Callback,
            Context
        );

        if (result != DDS_ERROR_CODE_SUCCESS) {
            return result;
        }

        curOffset += bytesToIssue;
        bytesLeftToWrite -= bytesToIssue;
    }

    return result;
}

//
// Get file properties by file id
// 
//
ErrorCodeT
BackEndStorage::GetFileInformationById(
    FileIdT FileId,
    FilePropertiesT* FileProperties
) {
    BackEndFile* file = AllFiles[FileId];
    if (file) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    FileProperties->FileAttributes = file->GetAttributes();
    strcpy(FileProperties->FileName, file->GetName());
    FileProperties->FileSize = file->GetSize();

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get file attributes by file name
// 
//
ErrorCodeT
BackEndStorage::GetFileAttributes(
    FileIdT FileId,
    FileAttributesT* FileAttributes
) {
    BackEndFile* file = AllFiles[FileId];
    if (file) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    *FileAttributes = file->GetAttributes();

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get the size of free space on the storage
// 
//
ErrorCodeT
BackEndStorage::GetStorageFreeSpace(
    FileSizeT* StorageFreeSpace
) {
    SegmentAllocationMutex.lock();

    *StorageFreeSpace = AvailableSegments * (FileSizeT)DDS_BACKEND_SEGMENT_SIZE;

    SegmentAllocationMutex.unlock();

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Move an existing file or a directory,
// including its children
// 
//
ErrorCodeT
BackEndStorage::MoveFile(
    FileIdT FileId,
    DirIdT OldDirId,
    DirIdT NewDirId,
    const char* NewFileName
) {
    BackEndFile* file = AllFiles[FileId];
    BackEndDir* oldDir = AllDirs[OldDirId];
    BackEndDir* newDir = AllDirs[NewDirId];
    if (!file) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }
    if (!oldDir || !newDir) {
        return DDS_ERROR_CODE_DIR_NOT_FOUND;
    }

    //
    // Update old directory
    //
    //
    oldDir->Lock();

    ErrorCodeT result = oldDir->DeleteFile(FileId);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        oldDir->Unlock();
        return result;
    }

    result = SyncDirToDisk(oldDir);

    oldDir->Unlock();

    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    //
    // Update new directory
    //
    //
    newDir->Lock();
    
    result = newDir->AddFile(FileId);

    if (result != DDS_ERROR_CODE_SUCCESS) {
        newDir->Unlock();
        return result;
    }

    result = SyncDirToDisk(newDir);

    newDir->Unlock();

    if (result != DDS_ERROR_CODE_SUCCESS) {
        return result;
    }

    file->SetName(NewFileName);
    
    return DDS_ERROR_CODE_SUCCESS;
}

}