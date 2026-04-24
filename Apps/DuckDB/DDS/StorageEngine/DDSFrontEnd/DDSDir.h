#pragma once

#include "DDSFrontEndInterface.h"
#include "LinkedList.h"

namespace DDS_FrontEnd {

//
// A simple implementation of directory
//
//
class DDSDir {
private:
    DirIdT Id;
    DirIdT Parent;
    LinkedList<DirIdT> Children;
    LinkedList<FileIdT> Files;
    char Name[DDS_MAX_FILE_PATH];

public:
    DDSDir();

    DDSDir(
        DirIdT Id,
        DirIdT Parent,
        const char* Name
    );

    const char*
    GetName();

    LinkedList<DirIdT>*
    GetChildren();

    LinkedList<FileIdT>*
    GetFiles();

    //
    // Add a child directory
    //
    //
    ErrorCodeT
    AddChild(
        DirIdT ChildId
    );

    //
    // Delete a child directory
    //
    //
    ErrorCodeT
    DeleteChild(
        DirIdT ChildId
    );

    //
    // Add a file
    //
    //
    ErrorCodeT
    AddFile(
        FileIdT FileId
    );

    //
    // Delete a file
    //
    //
    ErrorCodeT
    DeleteFile(
        FileIdT FileId
    );
};

}