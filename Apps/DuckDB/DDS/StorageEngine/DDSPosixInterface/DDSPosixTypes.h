#pragma once
#include "DDSTypes.h"
#include <fcntl.h>      // For O_* flags
#include <sys/stat.h>   // For mode_t and stat structure
#include <errno.h>      // For errno values
#include <atomic>
#include <condition_variable>
#include <dirent.h>
#include <mutex>
#include <vector>
#include <time.h>

#include "LinkedList.h"

#define DDS_POSIX_MAX_OPEN_FILES 1024
// First usable POSIX fd; 0/1/2 are reserved for standard streams.
#define DDS_POSIX_FD_START 3

namespace DDS_FrontEnd {

// internal representation of an open file 
struct DDSPosixFileHandle {
    FileIdT fileId;
    FileAccessT flags;
    // Track per-FD file offset for POSIX semantics.
    off_t offset;
    std::atomic<bool> isOpen;
    // Track inflight I/O and close coordination.
    std::atomic<uint32_t> inflight;
    std::atomic<bool> closing;
    std::mutex offsetMutex;
    std::mutex inflightMutex;
    std::condition_variable inflightCv;
};

struct DDSPosixDirHandle {
    DirIdT dirId;
    LinkedList<DirIdT>::iterator dirIter; // use LinkedList or vector?? if LL, need to implement iterator
    LinkedList<FileIdT>::iterator fileIter;
    bool iteratingFiles;  // true = iterating files, false = iterating dirs
    bool isOpen;
};

struct DIR {
    int handleIndex;  // Index into the openDirs array
    dirent entry;     // Current directory entry
};

// struct DDSPosixStat {
//     dev_t     st_dev;      // ID of device
//     ino_t     st_ino;      // Inode number (using file/dir ID)
//     mode_t    st_mode;     // File type and mode
//     nlink_t   st_nlink;    // Number of hard links
//     uid_t     st_uid;      // User ID
//     gid_t     st_gid;      // Group ID
//     off_t     st_size;     // File size
//     time_t    st_atime;    // Last access time
//     time_t    st_mtime;    // Last modification time
//     time_t    st_ctime;    // Last status change time
// };

}
