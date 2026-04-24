#include <string.h>

#include "BackEndDir.h"

namespace DDS_BackEnd {

//
// Constructors
//
//
BackEndDir::BackEndDir() {
    this->Id = DDS_DIR_INVALID;
    this->Parent = DDS_DIR_INVALID;
    this->Name[0] = '\0';
}

BackEndDir::BackEndDir(
    DirIdT Id,
    DirIdT Parent,
    const char* Name
) {
    this->Id = Id;
    this->Parent = Parent;
    strcpy(this->Name, Name);
}

const char*
BackEndDir::GetName() {
    return this->Name;
}

LinkedList<DirIdT>*
BackEndDir::GetChildren() {
    return &this->Children;
}

LinkedList<FileIdT>*
BackEndDir::GetFiles() {
    return &this->Files;
}

//
// Add a child directory
//
//
ErrorCodeT
BackEndDir::AddChild(
    DirIdT ChildId
) {
    this->Children.Insert(ChildId);

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Delete a child directory
//
//
ErrorCodeT
BackEndDir::DeleteChild(
    DirIdT ChildId
) {
    this->Children.Delete(ChildId);

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Add a file
//
//
ErrorCodeT
BackEndDir::AddFile(
    FileIdT FileId
) {
    this->Files.Insert(FileId);

    return DDS_ERROR_CODE_SUCCESS;
}

//
// Delete a file
//
//
ErrorCodeT
BackEndDir::DeleteFile(
    FileIdT FileId
) {
    this->Files.Delete(FileId);

    return DDS_ERROR_CODE_SUCCESS;
}

}