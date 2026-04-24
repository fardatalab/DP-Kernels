#pragma once

#include "BackEnd.h"
#include "LinkedList.h"

namespace DDS_BackEnd {

//
// A simple implementation of directory
//
//
class BackEndDir {
private:
    DirIdT Id;
    DirIdT Parent;
    LinkedList<DirIdT> Children;
    LinkedList<FileIdT> Files;
    char Name[DDS_MAX_FILE_PATH];

public:
    BackEndDir();

    BackEndDir(
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