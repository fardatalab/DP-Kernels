#include <string.h>
#include <stdlib.h>

#include "../Include/DPUBackEndDir.h"

struct DPUDir* BackEndDirX(){
    struct DPUDir *tmp;
    tmp = malloc(sizeof(struct DPUDir));
    tmp->Properties.Id = DDS_DIR_INVALID;
    tmp->Properties.Parent = DDS_DIR_INVALID;
    tmp->Properties.NumFiles = 0;
    tmp->Properties.Name[0] = '\0';
    tmp->AddressOnSegment = 0;
    for (size_t f = 0; f != DDS_MAX_FILES_PER_DIR; f++) {
        tmp->Properties.Files[f] = DDS_FILE_INVALID;
    }
    pthread_mutex_init(&tmp->ModificationMutex, NULL);
    return tmp;
}

struct DPUDir* BackEndDirI(
    DirIdT Id,
    DirIdT Parent,
    const char* Name
){
    struct DPUDir *tmp;
    tmp = malloc(sizeof(struct DPUDir));
    tmp->Properties.Id = Id;
    tmp->Properties.Parent = Parent;
    tmp->Properties.NumFiles = 0;
    strcpy(tmp->Properties.Name, Name);
    tmp->AddressOnSegment = DDS_BACKEND_SECTOR_SIZE + Id * sizeof(DPUDirPropertiesT);
    for (size_t f = 0; f != DDS_MAX_FILES_PER_DIR; f++) {
        tmp->Properties.Files[f] = DDS_FILE_INVALID;
    }
    pthread_mutex_init(&tmp->ModificationMutex, NULL);
    return tmp;
}

DPUDirPropertiesT* GetDirProperties(
    struct DPUDir* Dir
){
    return &Dir->Properties;
}

SegmentSizeT GetDirAddressOnSegment(
    struct DPUDir* Dir
){
    return Dir->AddressOnSegment;
}

ErrorCodeT AddFile(
    FileIdT FileId,
    struct DPUDir* Dir
){
    ErrorCodeT result = DDS_ERROR_CODE_TOO_MANY_FILES;

    for (size_t f = 0; f != DDS_MAX_FILES_PER_DIR; f++) {
        if (Dir->Properties.Files[f] == DDS_FILE_INVALID) {
            Dir->Properties.Files[f] = FileId;
            Dir->Properties.NumFiles++;
            result = DDS_ERROR_CODE_SUCCESS;
            break;
        }
    }
    
    return result;
}

ErrorCodeT DeleteFile(
    FileIdT FileId,
    struct DPUDir* Dir
){
    ErrorCodeT result = DDS_ERROR_CODE_FILE_NOT_FOUND;

    for (size_t f = 0; f != DDS_MAX_FILES_PER_DIR; f++) {
        if (Dir->Properties.Files[f] == FileId) {
            Dir->Properties.Files[f] = DDS_FILE_INVALID;
            Dir->Properties.NumFiles--;
            result = DDS_ERROR_CODE_SUCCESS;
            break;
        }
    }

    return result;
}

void Lock(
    struct DPUDir* Dir
){
    pthread_mutex_lock(&Dir->ModificationMutex);
}

void Unlock(
    struct DPUDir* Dir
){
    pthread_mutex_unlock(&Dir->ModificationMutex);
}
