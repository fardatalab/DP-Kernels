#include <string.h>
#include <iostream>

#include "BackEndInMemory.h"

namespace DDS_BackEnd {

//
// Constructor
// 
//
BackEndInMemory::BackEndInMemory() {
    for (size_t d = 0; d != DDS_MAX_DIRS; d++) {
        AllDirs[d] = nullptr;
    }

    for (size_t f = 0; f != DDS_MAX_FILES; f++) {
        AllFiles[f] = nullptr;
    }
    
	//
	// Create the root directory
	//
	//
	BackEndDir* rootDir = new BackEndDir(DDS_DIR_ROOT, DDS_DIR_INVALID, "/");
	AllDirs[DDS_DIR_ROOT] = rootDir;

    //
    // Allocate memory for storing data
    //
    //
	Data = new char[Capacity];
    memset(Data, 0, sizeof(char) * Capacity);
}

//
// Destructor
// 
//
BackEndInMemory::~BackEndInMemory() {
    //
    // Free all bufffers
    //
    //
    for (size_t i = 0; i != DDS_MAX_DIRS; i++) {
        if (AllDirs[i] != nullptr) {
            delete AllDirs[i];
        }
    }

    for (size_t i = 0; i != DDS_MAX_FILES; i++) {
        if (AllFiles[i] != nullptr) {
            delete AllFiles[i];
        }
    }

    delete[] Data;
}

//
// Create a diretory
// 
//
ErrorCodeT
BackEndInMemory::CreateDirectory(
    const char* PathName,
    DirIdT DirId,
    DirIdT ParentId
) {
    BackEndDir* dir = new BackEndDir(DirId, ParentId, PathName);
    AllDirs[ParentId]->AddChild(DirId);
    AllDirs[DirId] = dir;

    return DDS_ERROR_CODE_SUCCESS;
}

void
BackEndInMemory::RemoveDirectoryRecursive(
    BackEndDir* Dir
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
}

//
// Delete a directory
// 
//
ErrorCodeT
BackEndInMemory::RemoveDirectory(
    DirIdT DirId
) {
    //
    // Recursively delete all files and child directories
    //
    //
    RemoveDirectoryRecursive(AllDirs[DirId]);

    delete AllDirs[DirId];
    AllDirs[DirId] = nullptr;

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Create a file
// 
//
ErrorCodeT
BackEndInMemory::CreateFile(
    const char* FileName,
    FileAttributesT FileAttributes,
    FileIdT FileId,
    DirIdT DirId
) {
    //
    // Create the file
    // TODO: allocate storage space for files
    //
    //
    FileSizeT startAddress = 0;
    BackEndFile* file = new BackEndFile(FileId, FileName, FileAttributes, startAddress);

    AllDirs[DirId]->AddFile(FileId);
    AllFiles[DirId] = file;

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Delete a file
// 
//
ErrorCodeT
BackEndInMemory::DeleteFile(
    FileIdT FileId,
    DirIdT DirId
) {
    AllDirs[DirId]->DeleteFile(FileId);
    
    delete AllFiles[FileId];
    AllFiles[FileId] = nullptr;

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Change the size of a file
// 
//
ErrorCodeT
BackEndInMemory::ChangeFileSize(
    FileIdT FileId,
    FileSizeT NewSize
) {
    FileSizeT storageFreeSpace;
    GetStorageFreeSpace(&storageFreeSpace);
    if (NewSize - AllFiles[FileId]->GetSize() > storageFreeSpace) {
        return DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE;
    }

    AllFiles[FileId]->SetSize(NewSize);

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get file size
// 
//
ErrorCodeT
BackEndInMemory::GetFileSize(
    FileIdT FileId,
    FileSizeT* FileSize
) {
    if (FileId >= DDS_MAX_FILES || AllFiles[FileId] == nullptr) {
        return DDS_ERROR_CODE_FILE_NOT_FOUND;
    }

    *FileSize = AllFiles[FileId]->GetSize();

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Async read from a file
// 
//
ErrorCodeT
BackEndInMemory::ReadFile(
    FileIdT FileId,
    FileSizeT Offset,
    BufferT DestBuffer,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    BackEndFile* pFile = AllFiles[FileId];
    size_t remainingBytes = pFile->GetSize() - Offset;
    FileIOSizeT bytesRead = 0;

    if (remainingBytes < BytesToRead) {
        result = DDS_ERROR_CODE_READ_OVERFLOW;
        if (remainingBytes > 0) {
            char* dataPointer = this->Data + pFile->GetStartAddressOnStorage() + Offset;
            memcpy(DestBuffer, dataPointer, remainingBytes);
            bytesRead = (FileIOSizeT)remainingBytes;
            pFile->SetLastAccessTime(time(NULL));
        }
    }
    else {
        char* dataPointer = this->Data + pFile->GetStartAddressOnStorage() + Offset;
        memcpy(DestBuffer, dataPointer, BytesToRead);
        bytesRead = BytesToRead;
        pFile->SetLastAccessTime(time(NULL));
    }

    if (BytesRead != NULL) {
        *BytesRead = bytesRead;
    }
    
    Callback(
        result,
        bytesRead,
        Context
    );

    return result;
}

//
// Async read from a file with scattering
// 
//
ErrorCodeT
BackEndInMemory::ReadFileScatter(
    FileIdT FileId,
    FileSizeT Offset,
    BufferT* DestBufferArray,
    FileIOSizeT BytesToRead,
    FileIOSizeT* BytesRead,
    ReadWriteCallback Callback,
    ContextT Context
) {
    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    BackEndFile* pFile = AllFiles[FileId];
    size_t remainingBytes = pFile->GetSize() - Offset;

    //
    // Caculate the actual bytes to read
    //
    //
    size_t actualReadBytes = remainingBytes < BytesToRead ? remainingBytes : BytesToRead;
    size_t numSegments = actualReadBytes / DDS_PAGE_SIZE;
    FileIOSizeT bytesRead = 0;
    size_t p = 0;
    
    for (; p != numSegments; p++) {
        char* dataPointer = this->Data + pFile->GetStartAddressOnStorage() + Offset;
        memcpy(DestBufferArray[p], dataPointer, DDS_PAGE_SIZE);
        Offset += DDS_PAGE_SIZE;
    }
    bytesRead += (FileIOSizeT)(DDS_PAGE_SIZE * numSegments);

    //
    // Handle the residual
    //
    //
    if (numSegments * DDS_PAGE_SIZE < actualReadBytes) {
        size_t restBytes = actualReadBytes - (numSegments * DDS_PAGE_SIZE);
        char* dataPointer = this->Data + pFile->GetStartAddressOnStorage() + Offset;
        memcpy(DestBufferArray[p], dataPointer, restBytes);
        bytesRead += (FileIOSizeT)restBytes;
        Offset += restBytes;
    }
    pFile->SetLastAccessTime(time(NULL));

    if (remainingBytes < BytesToRead) {
        result = DDS_ERROR_CODE_READ_OVERFLOW;
    }

    if (BytesRead != NULL) {
        *BytesRead = bytesRead;
    }

    Callback(
        result,
        bytesRead,
        Context
    );

    return result;
}

//
// Async write to a file
// 
//
ErrorCodeT
BackEndInMemory::WriteFile(
    FileIdT FileId,
    FileSizeT Offset,
    BufferT SourceBuffer,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ReadWriteCallback Callback,
    ContextT Context
) {
    BackEndFile* pFile = AllFiles[FileId];
    FileSizeT newSize = Offset + BytesToWrite;
    FileSizeT freeStorageSpace;
    GetStorageFreeSpace(&freeStorageSpace);
    if (newSize > pFile->GetSize()) {
        if (newSize - pFile->GetSize() > freeStorageSpace) {
            return DDS_ERROR_CODE_WRITE_OVERFLOW;
        }
        pFile->SetSize(newSize);
    }

    FileIOSizeT bytesWritten = 0;

    char* dataPointer = this->Data + pFile->GetStartAddressOnStorage() + Offset;
    memcpy(dataPointer, SourceBuffer, BytesToWrite);
    time_t now = time(NULL);
    pFile->SetLastWriteTime(now);
    pFile->SetLastAccessTime(now);
    bytesWritten = BytesToWrite;

    if (BytesWritten != NULL) {
        *BytesWritten = bytesWritten;
    }

    Callback(
        DDS_ERROR_CODE_SUCCESS,
        bytesWritten,
        Context
    );

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Async write to a file
// 
//
ErrorCodeT
BackEndInMemory::WriteFileGather(
    FileIdT FileId,
    FileSizeT Offset,
    BufferT* SourceBufferArray,
    FileIOSizeT BytesToWrite,
    FileIOSizeT* BytesWritten,
    ReadWriteCallback Callback,
    ContextT Context
) {
    BackEndFile* pFile = AllFiles[FileId];
    FileSizeT newSize = Offset + BytesToWrite;
    FileSizeT freeStorageSpace;
    GetStorageFreeSpace(&freeStorageSpace);
    if (newSize > pFile->GetSize()) {
        if (newSize - pFile->GetSize() > freeStorageSpace) {
            return DDS_ERROR_CODE_WRITE_OVERFLOW;
        }
        pFile->SetSize(newSize);
    }

    size_t numSegments = BytesToWrite / DDS_PAGE_SIZE;
    FileIOSizeT bytesWritten = 0;
    size_t p = 0;

    for (; p != numSegments; p++) {
        char* dataPointer = this->Data + pFile->GetStartAddressOnStorage() + Offset;
        memcpy(dataPointer, SourceBufferArray[p], DDS_PAGE_SIZE);
        Offset += DDS_PAGE_SIZE;
    }
    bytesWritten += (FileIOSizeT)(DDS_PAGE_SIZE * numSegments);

    //
    // Handle the residual
    //
    //
    if (numSegments * DDS_PAGE_SIZE < BytesToWrite) {
        size_t restBytes = BytesToWrite - (numSegments * DDS_PAGE_SIZE);
        char* dataPointer = this->Data + pFile->GetStartAddressOnStorage() + Offset;
        memcpy(dataPointer, SourceBufferArray[p], restBytes);
        bytesWritten += (FileIOSizeT)restBytes;
        Offset += restBytes;
    }
    time_t now = time(NULL);
    pFile->SetLastWriteTime(now);
    pFile->SetLastAccessTime(now);

    if (BytesWritten != NULL) {
        *BytesWritten = bytesWritten;
    }

    Callback(
        DDS_ERROR_CODE_SUCCESS,
        bytesWritten,
        Context
    );

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get file properties by file id
// 
//
ErrorCodeT
BackEndInMemory::GetFileInformationById(
    FileIdT FileId,
    FilePropertiesT* FileProperties
) {
    BackEndFile* pFile = AllFiles[FileId];
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
BackEndInMemory::GetFileAttributes(
    FileIdT FileId,
    FileAttributesT* FileAttributes
) {
    *FileAttributes = AllFiles[FileId]->GetAttributes();

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Get the size of free space on the storage
// 
//
ErrorCodeT
BackEndInMemory::GetStorageFreeSpace(
    FileSizeT* StorageFreeSpace
) {
    //
    // A upperbound of free storage space
    // TODO: a better space management with inodes etc.
    //
    //
    FileSizeT totalFileSize = 0;
    for (size_t i = 0; i != DDS_MAX_FILES; i++) {
        if (AllFiles[i]) {
            totalFileSize += AllFiles[i]->GetSize();
        }
    }

    *StorageFreeSpace = Capacity - totalFileSize;

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Move an existing file or a directory,
// including its children
// 
//
ErrorCodeT
BackEndInMemory::MoveFile(
    FileIdT FileId,
    const char* NewFileName
) {
    AllFiles[FileId]->SetName(NewFileName);

    return DDS_ERROR_CODE_SUCCESS;
}

}