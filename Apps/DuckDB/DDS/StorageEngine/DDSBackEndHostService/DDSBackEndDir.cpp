#include <string.h>

#include "DDSBackEndDir.h"

namespace DDS_BackEnd {

//
// Constructors
//
//
BackEndDir::BackEndDir() :
    AddressOnSegment(0) {
    this->Properties.Id = DDS_DIR_INVALID;
    this->Properties.Parent = DDS_DIR_INVALID;
    this->Properties.NumFiles = 0;
    this->Properties.Name[0] = '\0';

    for (size_t f = 0; f != DDS_MAX_FILES_PER_DIR; f++) {
        this->Properties.Files[f] = DDS_FILE_INVALID;
    }
}

BackEndDir::BackEndDir(
    DirIdT Id,
    DirIdT Parent,
    const char* Name
) :
    AddressOnSegment(DDS_BACKEND_SECTOR_SIZE + Id * sizeof(PersistentPropertiesT)) {
    this->Properties.Id = Id;
    this->Properties.Parent = Parent;
    this->Properties.NumFiles = 0;
    strcpy(this->Properties.Name, Name);
    
    for (size_t f = 0; f != DDS_MAX_FILES_PER_DIR; f++) {
        this->Properties.Files[f] = DDS_FILE_INVALID;
    }
}

BackEndDir::PersistentPropertiesT*
BackEndDir::GetProperties() {
    return &this->Properties;
}

SegmentSizeT
BackEndDir::GetAddressOnSegment() {
    return this->AddressOnSegment;
}

//
// Add a file
//
//
ErrorCodeT
BackEndDir::AddFile(
    FileIdT FileId
) {
    ErrorCodeT result = DDS_ERROR_CODE_TOO_MANY_FILES;

    for (size_t f = 0; f != DDS_MAX_FILES_PER_DIR; f++) {
        if (this->Properties.Files[f] == DDS_FILE_INVALID) {
            this->Properties.Files[f] = FileId;
            this->Properties.NumFiles++;
            result = DDS_ERROR_CODE_SUCCESS;
            break;
        }
    }
    
    return result;
}

//
// Delete a file
//
//
ErrorCodeT
BackEndDir::DeleteFile(
    FileIdT FileId
) {
    ErrorCodeT result = DDS_ERROR_CODE_FILE_NOT_FOUND;

    for (size_t f = 0; f != DDS_MAX_FILES_PER_DIR; f++) {
        if (this->Properties.Files[f] == FileId) {
            this->Properties.Files[f] = DDS_FILE_INVALID;
            this->Properties.NumFiles--;
            result = DDS_ERROR_CODE_SUCCESS;
            break;
        }
    }

    return result;
}

//
// Lock the directory
//
//
inline
void
BackEndDir::Lock() {
    ModificationMutex.lock();
}

//
// Unlock the directory
//
//
#pragma warning (disable:26110)
inline
void
BackEndDir::Unlock() {
    ModificationMutex.unlock();
}

}