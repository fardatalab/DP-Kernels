#include <string.h>

#include "BackEndFile.h"

namespace DDS_BackEnd {

BackEndFile::BackEndFile() {
    this->Id = DDS_FILE_INVALID;
    this->Properties.FileAttributes = 0;
    this->Properties.CreationTime = time(NULL);
    this->Properties.LastAccessTime = time(NULL);
    this->Properties.LastWriteTime = time(NULL);
    this->Properties.FileSize = 0;
    this->StartAddressOnStorage = 0;
}

BackEndFile::BackEndFile(
    FileIdT FileId,
    const char* FileName,
    FileAttributesT FileAttributes,
    size_t StartAddressOnStorage
) {
    this->Id = FileId;
    strcpy(this->Properties.FileName, FileName);
    this->Properties.FileAttributes = FileAttributes;
    this->Properties.CreationTime = time(NULL);
    this->Properties.LastAccessTime = time(NULL);
    this->Properties.LastWriteTime = time(NULL);
    this->Properties.FileSize = 0;

    //
    // TODO: find a location on storage to store this file
    //
    //
    this->StartAddressOnStorage = StartAddressOnStorage;
}

const char*
BackEndFile::GetName() {
	return this->Properties.FileName;
}

FileAttributesT
BackEndFile::GetAttributes() {
    return this->Properties.FileAttributes;
}

FileSizeT
BackEndFile::GetSize() {
    return this->Properties.FileSize;
}

time_t
BackEndFile::GetCreationTime() {
    return this->Properties.CreationTime;
}

time_t
BackEndFile::GetLastAccessTime() {
    return this->Properties.LastAccessTime;
}

time_t
BackEndFile::GetLastWriteTime() {
    return this->Properties.LastWriteTime;
}

FileSizeT
BackEndFile::GetStartAddressOnStorage() {
    return this->StartAddressOnStorage;
}

void
BackEndFile::SetName(
    const char* FileName
) {
    strcpy(this->Properties.FileName, FileName);
}

void
BackEndFile::SetSize(FileSizeT FileSize) {
    this->Properties.FileSize = FileSize;
}

void
BackEndFile::SetLastAccessTime(
    time_t NewTime
) {
    this->Properties.LastAccessTime = NewTime;
}

void
BackEndFile::SetLastWriteTime(
    time_t NewTime
) {
    this->Properties.LastWriteTime = NewTime;
}

}