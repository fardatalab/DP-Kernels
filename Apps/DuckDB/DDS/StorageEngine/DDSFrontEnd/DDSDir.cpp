#include <string.h>

#include "DDSDir.h"

namespace DDS_FrontEnd {

//
// Constructors
//
//
    DDSDir::DDSDir() {
    this->Id = DDS_DIR_INVALID;
    this->Parent = DDS_DIR_INVALID;
    this->Name[0] = '\0';
}

DDSDir::DDSDir(
    DirIdT Id,
    DirIdT Parent,
    const char* Name
) {
    this->Id = Id;
    this->Parent = Parent;
    strcpy(this->Name, Name);
}

const char*
DDSDir::GetName() {
    return this->Name;
}

LinkedList<DirIdT>*
DDSDir::GetChildren() {
    return &this->Children;
}

LinkedList<FileIdT>*
DDSDir::GetFiles() {
    return &this->Files;
}

//
// Add a child directory
//
//
ErrorCodeT
DDSDir::AddChild(
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
DDSDir::DeleteChild(
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
DDSDir::AddFile(
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
DDSDir::DeleteFile(
    FileIdT FileId
) {
    this->Files.Delete(FileId);

    return DDS_ERROR_CODE_SUCCESS;
}

}