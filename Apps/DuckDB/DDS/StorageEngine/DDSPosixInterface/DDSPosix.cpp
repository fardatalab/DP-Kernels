#include "DDSPosix.h"
#include <atomic>
#include <bitset>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <errno.h>
#include <iostream>
#include <limits>
#include <mutex>
#include <string.h>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <vector>

// pin thread
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include "DDSFrontEnd.h"
#include "RingBufferProgressive.h"
// Note: DDS_MAX_FILES and FileIdT come from DDSTypes.h
// which is included via DDSPosix.h -> DDSPosixTypes.h -> DDSTypes.h

namespace DDSPosix = DDS_FrontEnd;

namespace DDS_FrontEnd{

/* Global DDSFrontEnd instance */
static DDSFrontEnd* ddsInstance = nullptr;

// global fd table to track open files
static DDSPosixFileHandle openFiles[DDS_MAX_FD];
static std::bitset<DDS_MAX_FD> g_fdInUse;
// Protects global fd allocation/release tables for control-plane calls.
static std::mutex g_fdTableMutex;

// NOTE: DDSFile::OpenCount is the front-end source of truth for DeleteFile gating.

// DEPRECATED/BUGGY: These maps are not properly maintained
// maps pathname to FileId. close() doesn't release DDS FileId; only releases DDSPosix fd
static std::unordered_map<std::string, int> g_pathToFdMap;
static std::unordered_map<int, std::string> g_fdToPathMap;

// global dir table to track dir usage
static DDSPosixDirHandle openDirs[DDS_MAX_DIR];
static std::bitset<DDS_MAX_DIR> g_dirInUse;
static std::unordered_map<DIR*, int> g_dirHandleMap;

// Background poller state for serialized PollWait calls.
static std::atomic<bool> g_pollerStop{true};
static std::thread g_pollerThread;
// jason: Thread-safe, one-time initialization state for initialize_posix.
enum class PosixInitState { Uninitialized, Initializing, Initialized, Failed };
static std::mutex g_initMutex;
static std::condition_variable g_initCv;
static PosixInitState g_initState = PosixInitState::Uninitialized;
static int g_initErrno = 0;

/* // temporarily tracking file size since DDS file size is not implemented
// static FileSizeT fileSize[DDS_MAX_FD] = {0}; */
// jason: File size tracking is mirrored and updated in the front end, and also updated in the backend

/* Initialization/Cleanup Functions */

/**
 * Initialize the POSIX interface with the given storage name
 *
 * @param storeName Name of the storage to initialize
 * @return true if successful, false otherwise
 *
 * Starts the background poller thread used by async reads/writes.
 * Updated: thread-safe, single-pass initialization with waiting callers.
 */
bool initialize_posix(const char* storeName) {
    // jason: New thread-safe init path; only one thread performs initialization.
    {
        std::unique_lock<std::mutex> lock(g_initMutex);
        if (g_initState == PosixInitState::Initialized) {
            return true;
        }
        if (g_initState == PosixInitState::Failed) {
            // jason: Propagate the first init error to callers.
            if (g_initErrno != 0) {
                errno = g_initErrno;
            }
            return false;
        }
        if (g_initState == PosixInitState::Initializing) {
            // jason: Wait for the in-flight initializer to complete.
            g_initCv.wait(lock, [&]() { return g_initState != PosixInitState::Initializing; });
            if (g_initState == PosixInitState::Initialized) {
                return true;
            }
            if (g_initErrno != 0) {
                errno = g_initErrno;
            }
            return false;
        }
        g_initState = PosixInitState::Initializing;
    }

    bool initOk = false;
    int initErrno = 0;

    DDSFrontEnd* newInstance = new DDSFrontEnd(storeName);
    if (!newInstance) {
        initErrno = ENOMEM;
    } else {
        ErrorCodeT result = newInstance->Initialize();
        if (result != DDS_ERROR_CODE_SUCCESS) {
            delete newInstance;
            newInstance = nullptr;
            initErrno = EIO;
        } else {
            ddsInstance = newInstance;
            initOk = true;
        }
    }

    if (initOk) {
        // Initialize file descriptor and directory tracking.
        g_fdInUse.reset();
        g_dirInUse.reset();

        // jason: Originally relied on implicit C++ static zero-initialization of openFiles array.
        // jason: make initialization explicit and also sets fileId to DDS_FILE_INVALID (-1) instead of relying on zero
        // (which is a valid FileId).
        for (int fd = 0; fd < DDS_MAX_FD; fd++)
        {
            openFiles[fd].fileId = DDS_FILE_INVALID;
            openFiles[fd].flags = 0;
            openFiles[fd].offset = 0;
            // jason: Initialize concurrency tracking for this fd.
            openFiles[fd].isOpen.store(false, std::memory_order_relaxed);
            openFiles[fd].inflight.store(0, std::memory_order_relaxed);
            openFiles[fd].closing.store(false, std::memory_order_relaxed);
        }

        // jason: Start background poller after successful initialization.
        g_pollerStop.store(false, std::memory_order_release);
        g_pollerThread = std::thread(
            []()
            {
                // Pin this poller thread to the last online CPU core.
                long cpuCount = sysconf(_SC_NPROCESSORS_ONLN);
                if (cpuCount > 0)
                {
                    int lastCpu = static_cast<int>(cpuCount - 1);
                    cpu_set_t cpuset;
                    CPU_ZERO(&cpuset);
                    CPU_SET(lastCpu, &cpuset);
                    (void)pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
                }

                // Poller thread serializes PollWait for the shared poll.
                while (!g_pollerStop.load(std::memory_order_acquire))
                {
                    if (!ddsInstance)
                    {
                        std::this_thread::yield();
                        continue;
                    }
                    FileIOSizeT bytesServiced = 0;
                    ContextT ioCtxt = nullptr;
                    ContextT fileCtxt = nullptr;
                    bool pollResult = false;
                    // NOTE: DDSPosix assumes DDS_POLL_DEFAULT for data-path I/O.
                    ErrorCodeT result =
                    ddsInstance->PollWait(DDS_POLL_DEFAULT, &bytesServiced, &fileCtxt, &ioCtxt, 0, &pollResult);
                    if (result != DDS_ERROR_CODE_SUCCESS)
                    {
                        // std::this_thread::yield();
                        continue;
                    }
                    if (!pollResult)
                    {
                        // std::this_thread::yield();
                    }
                }
                });
    }

    {
        std::lock_guard<std::mutex> lock(g_initMutex);
        g_initState = initOk ? PosixInitState::Initialized : PosixInitState::Failed;
        g_initErrno = initOk ? 0 : (initErrno != 0 ? initErrno : EIO);
    }
    g_initCv.notify_all();
    if (!initOk && g_initErrno != 0) {
        errno = g_initErrno;
    }
    return initOk;
}

/**
 * Shutdown the POSIX interface and clean up resources
 *
 * Stops the background poller thread after draining inflight I/O.
 */
bool shutdown_posix() {
    if (ddsInstance != nullptr) {
        // jason: Stop the poller before closing handles and deleting DDS.
        // if (g_pollerThread.joinable())
        // {
        //     g_pollerStop.store(true, std::memory_order_release);
        //     g_pollerThread.join();
        // }

        // Close all open file descriptors
        for (int fd = DDS_POSIX_FD_START; fd < DDS_MAX_FD; fd++)
        {
            if (g_fdInUse[fd])
                DDSPosix::close(fd);
        }
        // jason: Stop the poller after inflight I/O drains.
        if (g_pollerThread.joinable())
        {
            g_pollerStop.store(true, std::memory_order_release);
            g_pollerThread.join();
        }

        // // jason: Original code didn't clean up open directories, causing resource leak on re-initialization.
        // // jason: Close all open directory handles to free DIR* structures and clear g_dirHandleMap.
        // std::vector<DIR *> openDirPtrs;
        // for (const auto &entry : g_dirHandleMap)
        // {
        //     openDirPtrs.push_back(entry.first);
        // }
        // for (DIR *dirp : openDirPtrs)
        // {
        //     DDSPosix::closedir(dirp);
        // }

        delete ddsInstance;
        ddsInstance = nullptr;
    }
    // jason: Allow future initialize_posix calls after shutdown.
    {
        std::lock_guard<std::mutex> lock(g_initMutex);
        g_initState = PosixInitState::Uninitialized;
        g_initErrno = 0;
    }
    return true;
}


/* Helper Functions */

/**
 * Allocate a file descriptor for the given file ID
 *
 * @param fileId The file ID to associate with the descriptor
 * @param access The access flags
 * @return A file descriptor or -1 on failure
 *
 * Note: File size tracking mirrored in the front end.
 * Serializes fd table updates for control-plane callers.
 */
static int allocateFileDescriptor(FileIdT fileId, FileAccessT access) {
    // jason: Serialize fd table updates for control-plane operations.
    std::lock_guard<std::mutex> lock(g_fdTableMutex);
    /* fd >= DDS_POSIX_FD_START is file opened via DDSFrontEnd::open */
    int fd = DDS_POSIX_FD_START;
    while (fd < DDS_MAX_FD && g_fdInUse[fd]) fd++;

    if (fd >= DDS_MAX_FD) {
        errno = EMFILE;
        return -1;
    }

    g_fdInUse[fd] = true;
    openFiles[fd].fileId = fileId;
    openFiles[fd].flags = access;
    // Initialize per-FD offset to the start; open() can override for O_APPEND.
    openFiles[fd].offset = 0;
    // jason: Reset concurrency tracking for a newly allocated fd.
    openFiles[fd].inflight.store(0, std::memory_order_relaxed);
    openFiles[fd].closing.store(false, std::memory_order_relaxed);
    openFiles[fd].isOpen.store(true, std::memory_order_release);

    // jason: Track open handles in the front-end file table for DeleteFile gating.
    if (ddsInstance)
    {
        DDSFile *file = ddsInstance->GetFile(fileId);
        if (file)
        {
            file->IncrementOpenCount();
        }
    }

    return fd;
}

/**
 * Release a file descriptor
 *
 * @param fd The file descriptor to release
 *
 * Decrements the open-handle count for the associated FileId.
 * When the last handle closes, the FileId slot becomes available for reuse,
 * but we do NOT delete the DDS file (no CloseFile operation exists).
 * Serializes fd table updates for control-plane callers.
 */
static void releaseFileDescriptor(int fd) {
    // jason: Serialize fd table updates for control-plane operations.
    std::lock_guard<std::mutex> lock(g_fdTableMutex);
    if (fd >= DDS_POSIX_FD_START && fd < DDS_MAX_FD)
    {
        FileIdT fileId = openFiles[fd].fileId;

        if (fileId < DDS_MAX_FILES)
        {
            // jason: Track open handles in the front-end file table for DeleteFile gating.
            if (ddsInstance)
            {
                DDSFile *file = ddsInstance->GetFile(fileId);
                if (file)
                {
                    file->DecrementOpenCount();
                }
            }
        }

        g_fdInUse[fd] = false;
        // jason: defensive programming just in case...
        // openFiles[fd].isOpen = false;
        openFiles[fd].fileId = DDS_FILE_INVALID;
        openFiles[fd].flags = 0;
        openFiles[fd].offset = 0;
        // jason: Reset concurrency tracking after final release.
        openFiles[fd].inflight.store(0, std::memory_order_relaxed);
        openFiles[fd].closing.store(false, std::memory_order_relaxed);
        openFiles[fd].isOpen.store(false, std::memory_order_release);
    }
}

/**
 * Check if a file descriptor is valid
 * 
 * @param fd The file descriptor to check
 * @return true if valid, false otherwise
 */
static bool isValidFileDescriptor(int fd) {
    if (fd < DDS_POSIX_FD_START || fd >= DDS_MAX_FD)
    {
        return false;
    }
    // printf("g_fdInUse[fd]: %d openFiles[fd].isOpen: %d\n", g_fdInUse[fd], openFiles[fd].isOpen);
    // jason: Use atomic open state to avoid data races with concurrent close.
    return g_fdInUse[fd] && openFiles[fd].isOpen.load(std::memory_order_acquire);
}

/**
 * Get the file ID associated with a file descriptor
 *
 * @param fd The file descriptor
 * @return The file ID or DDS_FILE_INVALID on error
 *
 * Serializes fd table lookups for control-plane callers.
 */
static FileIdT getFileId(int fd) {
    // jason: Serialize fd table lookups for control-plane callers.
    std::lock_guard<std::mutex> lock(g_fdTableMutex);
    if (!isValidFileDescriptor(fd)) {
        errno = EBADF;
        return DDS_FILE_INVALID;
    }
    
    return openFiles[fd].fileId;
}

/**
 * Acquire a file descriptor for an in-flight POSIX I/O call.
 *
 * Increments the inflight counter and rejects new operations when closing.
 */
static bool acquireFdForIo(int fd, DDSPosixFileHandle **handleOut)
{
    if (fd < DDS_POSIX_FD_START || fd >= DDS_MAX_FD)
    {
        errno = EBADF;
        return false;
    }
    DDSPosixFileHandle *handle = &openFiles[fd];
    if (!handle->isOpen.load(std::memory_order_acquire) || handle->closing.load(std::memory_order_acquire))
    {
        errno = EBADF;
        return false;
    }
    handle->inflight.fetch_add(1, std::memory_order_acq_rel);
    // jason: Re-check closing state after increment to avoid racing close().
    if (!handle->isOpen.load(std::memory_order_acquire) || handle->closing.load(std::memory_order_acquire))
    {
        handle->inflight.fetch_sub(1, std::memory_order_acq_rel);
        errno = EBADF;
        return false;
    }
    *handleOut = handle;
    return true;
}

/**
 * Release an inflight I/O reference and notify a waiting close().
 */
static void releaseFdForIo(DDSPosixFileHandle *handle)
{
    if (!handle)
    {
        return;
    }
    uint32_t remaining = handle->inflight.fetch_sub(1, std::memory_order_acq_rel) - 1;
    if (remaining == 0 && handle->closing.load(std::memory_order_acquire))
    {
        std::lock_guard<std::mutex> lock(handle->inflightMutex);
        handle->inflightCv.notify_all();
    }
}

/**
 * Scoped inflight guard for DDSPosix I/O paths.
 *
 * Ensures inflight reference is released on all return paths.
 */
class DDSPosixInflightGuard
{
public:
    explicit DDSPosixInflightGuard(DDSPosixFileHandle *handle) : handle_(handle) {}
    ~DDSPosixInflightGuard() { releaseFdForIo(handle_); }
    DDSPosixInflightGuard(const DDSPosixInflightGuard &) = delete;
    DDSPosixInflightGuard &operator=(const DDSPosixInflightGuard &) = delete;

private:
    DDSPosixFileHandle *handle_ = nullptr;
};

/**
 * Check whether the open flags permit reads.
 */
static bool isReadAllowed(FileAccessT flags)
{
    int accmode = flags & O_ACCMODE;
    bool allowed = (accmode == O_RDONLY) || (accmode == O_RDWR);
    if (!allowed) {
        fprintf(stdout, "Warning: read not permitted (flags=0x%x)\n", (unsigned int)flags);
    }
    return allowed;
}

/**
 * Check whether the open flags permit writes.
 */
static bool isWriteAllowed(FileAccessT flags)
{
    int accmode = flags & O_ACCMODE;
    bool allowed = (accmode == O_WRONLY) || (accmode == O_RDWR);
    if (!allowed) {
        fprintf(stdout, "Warning: write not permitted (flags=0x%x)\n", (unsigned int)flags);
    }
    return allowed;
}

/**
 * Allocate a directory handle
 * 
 * @param dirId The directory ID
 * @return A directory handle or -1 on failure
 */
 static int allocateDirectoryHandle(DirIdT dirId) {
    int dirHandle = 0;
    while (dirHandle < DDS_MAX_DIR && g_dirInUse[dirHandle]) dirHandle++;

    if (dirHandle >= DDS_MAX_DIR) {
        errno = EMFILE;
        return -1;
    }

    g_dirInUse[dirHandle] = true;
    openDirs[dirHandle].dirId = dirId;
    openDirs[dirHandle].iteratingFiles = false;
    openDirs[dirHandle].isOpen = true;
    
    return dirHandle;
}

/**
 * Release a directory handle
 * 
 * @param dirHandle The directory handle to release
 */
static void releaseDirectoryHandle(int dirHandle) {
    if (dirHandle >= 0 && dirHandle < DDS_MAX_DIR) {
        g_dirInUse[dirHandle] = false;
        openDirs[dirHandle].isOpen = false;
    }
}



/* Directory Operations */

/**
 * Open a directory for reading
 * 
 * @param pathname The path to the directory
 * @return A pointer to a DIR structure or nullptr on failure
 */
DIR* opendir(const char* pathname) {
    if (!ddsInstance) {
        if (!initialize_posix("DDSFrontEnd")) return nullptr;
    }

    if (!pathname) {
        errno = EINVAL;
        return nullptr;
    }
    
    // Find the directory
    DirIdT dirId = DDS_DIR_INVALID;
    for (DirIdT i = 0; i < ddsInstance->GetDirIdEnd(); i++) {
        if (ddsInstance->GetDir(i) && strcmp(pathname, ddsInstance->GetDir(i)->GetName()) == 0) {
            dirId = i;
            break;
        }
    }
    
    if (dirId == DDS_DIR_INVALID) {
        errno = ENOENT;
        return nullptr;
    }
    
    // Allocate a directory handle
    int dirHandle = allocateDirectoryHandle(dirId);
    if (dirHandle < 0) {
        // errno is set by allocateDirectoryHandle
        return nullptr;
    }
    
    // Set up the iterators
    DDSDir* dir = ddsInstance->GetDir(dirId);
    LinkedList<DirIdT>* childDirs = dir->GetChildren();
    LinkedList<FileIdT>* childFiles = dir->GetFiles();
    
    openDirs[dirHandle].dirIter = childDirs->Begin();
    openDirs[dirHandle].fileIter = childFiles->Begin();
    
    // Allocate and initialize DIR structure
    DIR* dirp = (DIR*)malloc(sizeof(DIR));
    if (!dirp) {
        releaseDirectoryHandle(dirHandle);
        errno = ENOMEM;
        return nullptr;
    }
    
    // Initialize the DIR structure
    dirp->handleIndex = dirHandle;
    
    // Register the mapping
    g_dirHandleMap[dirp] = dirHandle;
    
    return dirp;
}

/**
 * Read an entry from a directory
 * 
 * @param dirp The directory pointer
 * @return A pointer to a dirent structure or nullptr on end of directory
 */
struct dirent* readdir(DIR* dirp) {
    if (!ddsInstance) {
        errno = ENOSYS;
        return nullptr;
    }
    
    // Validate the DIR pointer
    auto it = g_dirHandleMap.find(dirp);
    if (it == g_dirHandleMap.end()) {
        errno = EBADF;
        return nullptr;
    }
    
    int dirHandle = it->second;
    if (!g_dirInUse[dirHandle] || !openDirs[dirHandle].isOpen) {
        errno = EBADF;
        return nullptr;
    }
    
    DDSPosixDirHandle* handle = &openDirs[dirHandle];
    DDSDir* dir = ddsInstance->GetDir(handle->dirId);
    
    // First process all subdirectories
    if (!handle->iteratingFiles) {
        LinkedList<DirIdT>* childDirs = dir->GetChildren();
        
        if (handle->dirIter != childDirs->End()) {
            // Get current directory
            DirIdT childDirId = handle->dirIter.GetValue();
            DDSDir* childDir = ddsInstance->GetDir(childDirId);
            
            // Fill in dirent structure
            memset(&dirp->entry, 0, sizeof(dirent));
            strncpy(dirp->entry.d_name, childDir->GetName(), sizeof(dirp->entry.d_name) - 1);
            dirp->entry.d_type = DT_DIR;
            dirp->entry.d_reclen = sizeof(dirent);
            
            // Move to next directory
            handle->dirIter.Next();
            
            return &dirp->entry;
        } else {
            // Done with directories, switch to files
            handle->iteratingFiles = true;
        }
    }
    
    // Process files
    if (handle->iteratingFiles) {
        LinkedList<FileIdT>* childFiles = dir->GetFiles();
        
        if (handle->fileIter != childFiles->End()) {
            // Get current file
            FileIdT childFileId = handle->fileIter.GetValue();
            DDSFile* childFile = ddsInstance->GetFile(childFileId);
            
            // Fill in dirent structure
            memset(&dirp->entry, 0, sizeof(dirent));
            strncpy(dirp->entry.d_name, childFile->GetName(), sizeof(dirp->entry.d_name) - 1);
            dirp->entry.d_type = DT_REG;
            dirp->entry.d_reclen = sizeof(dirent);
            
            // Move to next file
            handle->fileIter.Next();
            
            return &dirp->entry;
        }
    }
    
    // No more entries
    return nullptr;
}

/**
 * Close a directory
 * 
 * @param dirp The directory pointer
 * @return 0 on success, -1 on failure
 */
int closedir(DIR* dirp) {
    if (!ddsInstance) {
        errno = ENOSYS;
        return -1;
    }

    // Validate the DIR pointer
    auto it = g_dirHandleMap.find(dirp);
    if (it == g_dirHandleMap.end()) {
        errno = EBADF;
        return -1;
    }
    
    int dirHandle = it->second;
    if (!g_dirInUse[dirHandle] || !openDirs[dirHandle].isOpen) {
        errno = EBADF;
        return -1;
    }
    
    // Release the directory handle
    releaseDirectoryHandle(dirHandle);
    g_dirHandleMap.erase(it);
    free(dirp);
    
    return 0;
}

/**
 * Rewind a directory stream
 * 
 * @param dirp The directory pointer
 */
void rewinddir(DIR* dirp) {
    if (!ddsInstance) {
        return;
    }
    
    // Validate the DIR pointer
    auto it = g_dirHandleMap.find(dirp);
    if (it == g_dirHandleMap.end()) {
        return;
    }
    
    int dirHandle = it->second;
    if (!g_dirInUse[dirHandle] || !openDirs[dirHandle].isOpen) {
        return;
    }
    
    DDSPosixDirHandle* handle = &openDirs[dirHandle];
    DDSDir* dir = ddsInstance->GetDir(handle->dirId);
    
    // Reset iterators
    handle->iteratingFiles = false;
    
    LinkedList<DirIdT>* childDirs = dir->GetChildren();
    LinkedList<FileIdT>* childFiles = dir->GetFiles();
    
    handle->dirIter = childDirs->Begin();
    handle->fileIter = childFiles->Begin();
}



/* File Operations */

/**
 * Open or create a file using POSIX-style semantics.
 *
 * @param pathname The path to the file
 * @param flags Access flags
 * @param mode File access mode (0644)
 * @return A file descriptor or -1 on failure
 */
int open(const char* pathname, int flags, mode_t mode) {
    if (!ddsInstance) {
        if (!initialize_posix("DDSFrontEnd")) {
            printf("DDSFrontEnd not initialized\n");
            return -1;
        };
    }

    if (!pathname) {
        printf("path DNE\n");
        errno = EINVAL;
        return -1;
    }

    std::string path(pathname);
    FileIdT fileId;
    ErrorCodeT result = DDS_ERROR_CODE_OOM;
    bool fileCreated = false;
    bool fileFound = false;
    FileAccessT access = (uint32_t) flags;
    FileAttributesT attrs = (uint32_t) mode;
    // Track the initial per-FD offset; updated after fd allocation.
    off_t initialOffset = 0;

    // auto it = g_pathToFdMap.find(path);
    // if (it != g_pathToFdMap.end()) {
    //     // file exists; was opened before
    //     fileCreated = true;
    //     fileId = it->second;
    // } else {

    // needs to create file
    // POSIX: allow open on existing file unless O_EXCL is set.
    if (flags & O_CREAT) {
        result = ddsInstance->CreateFile(pathname, access, 0, attrs, &fileId);
        if (result == DDS_ERROR_CODE_SUCCESS) {
            fileCreated = true;
            fileFound = true;
        } else if (result == DDS_ERROR_CODE_FILE_EXISTS) {
          if (flags & O_EXCL) {
            errno = EEXIST;
            printf("O_CREAT and O_EXCL, EEXIST\n");
            return -1;
          }
            // fall through to FindFirstFile for existing file
        } else {
          errno = EIO;
          return -1;
        }
    }

    // file already exists / couldn't create file
    if (!fileFound) {
        //   printf("\n=====Triggering FindFirstFile()=====\n");
        result = ddsInstance->FindFirstFile(pathname, &fileId);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            errno = ENOENT;
            printf("line 457 couldn't create file. Errno: %d\n", ENOENT);
            return -1;
        }
      fileFound = true;
      //   printf("=====End of FindFirstFile()=====\n");
    }

    // Truncate file
    if ((flags & O_TRUNC) && (flags & (O_WRONLY | O_RDWR))) {
        result = ddsInstance->ChangeFileSize(fileId, 0);
        if (result != DDS_ERROR_CODE_SUCCESS) {
            if (fileCreated) {
                ddsInstance->DeleteFile(pathname);
            }
            errno = EIO;
            return -1;
        }
    }

    // Set the per-FD offset to the end if requested
    if (flags & O_APPEND) {
        FileSizeT size;
        result = ddsInstance->GetFileSize(fileId, &size);
        if (result != DDS_ERROR_CODE_SUCCESS) {
            if (fileCreated) {
                ddsInstance->DeleteFile(pathname);
            }
            errno = EIO;
            printf("line 484\n");
            return -1;
        }
        // Size is fetched for append error checking; per-FD offset remains
        // unchanged.
        (void)size;
        // NOTE: jason: Don't update DDS file pointer; O_APPEND is enforced on
        // write. Keep per-FD offset at its default (start of file) for POSIX
        // semantics.
        // initialOffset = (off_t)size;
        // result = ddsInstance->SetFilePointer(fileId, size,
        // FilePointerPosition::BEGIN);
        // if (result != DDS_ERROR_CODE_SUCCESS) {
        //     if (fileCreated) {
        //         ddsInstance->DeleteFile(pathname);
        //     }
        //     errno = EIO;
        //     printf("line 494\n");
        //     return -1;
        // }
    }

    // jason: Use per-FD offset for normal opens; no global file pointer update.
    // if (!(flags & O_RDONLY)) {
    //     result = ddsInstance->SetFilePointer(fileId, 0,
    //     FilePointerPosition::BEGIN); if (result != DDS_ERROR_CODE_SUCCESS) {
    //         if (fileCreated) {
    //             ddsInstance->DeleteFile(pathname);
    //         }
    //         errno = EIO;
    //         printf("line 506\n");
    //         return -1;
    //     }
    // }

    // Allocate fd
    int fd;
    // if (fileCreated) {
        fd = allocateFileDescriptor(fileId, access);
        // jason: FileIdToFd array declaration was commented out, and this mapping is broken for multiple opens
        // (overwrites). jason: DDSFile::OpenCount tracks open handles for DeleteFile gating.
        // FileIdToFd[fileId] = fd;
        // printf("First time creating a file: fileId=%d, FileIdToFd[fileId]=%d\n", fileId, fd);
        // }
        // Apply initial (0) per-FD offset after allocation.
        openFiles[fd].offset = initialOffset;
        // jason: g_pathToFdMap is never read (read code commented at lines 474-478), causes memory leak.
        // jason: FindFirstFile already handles file lookup efficiently, no need for caching.
        // g_pathToFdMap[path] = fileId;
        // jason: g_fdToPathMap is never read anywhere, pure dead code causing memory leak.
        // g_fdToPathMap[fileId] = path;

        // printf("file id: %d, fd: %d\n", fileId, fd);

        return fd;
}

/**
 * Context for DDSPosix async I/O completion.
 *
 * Stores completion status and bytes serviced for a single request.
 */
typedef struct DDSPosixIoContext
{
    std::atomic<bool> done{false};
    std::atomic<FileIOSizeT> bytes{0};
    std::atomic<FileIOSizeT> logicalBytes{0};
    std::atomic<ErrorCodeT> ddsResult{DDS_ERROR_CODE_SUCCESS};
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    std::atomic<uint64_t> offloadReadNs{0};
    std::atomic<uint64_t> offloadStage1Ns{0};
    std::atomic<uint64_t> offloadStage2Ns{0};
#endif
} DDSPosixIoContext;

void
DummyCallback(
    ErrorCodeT ErrorCode,
    FileIOSizeT BytesServiced,
    ContextT Context // add reqId
) {
    size_t* completedOperations = (size_t*)Context; // make context a struct 
    (*completedOperations)++;

    // set flag 
}

/**
 * Completion callback for DDSPosix async I/O.
 *
 * Marks the per-call context as complete and stores the result/bytes.
 */
void DDSPosixCompletionCallback(ErrorCodeT ErrorCode, FileIOSizeT BytesServiced, ContextT Context)
{
    DDSPosixIoContext *ioContext = static_cast<DDSPosixIoContext *>(Context);
    if (!ioContext)
    {
        return;
    }
    ioContext->ddsResult.store(ErrorCode, std::memory_order_relaxed);
    ioContext->bytes.store(BytesServiced, std::memory_order_relaxed);
    // jason: legacy callback has no logical bytes, mirror BytesServiced.
    ioContext->logicalBytes.store(BytesServiced, std::memory_order_relaxed);
    ioContext->done.store(true, std::memory_order_release);
}

/**
 * Completion callback for DDSPosix async read2.
 *
 * Marks the per-call context as complete and stores the result/bytes.
 */
void DDSPosixCompletionCallback2(ErrorCodeT ErrorCode, FileIOSizeT BytesServiced, FileIOSizeT LogicalBytes,
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
                                 uint64_t OffloadReadTimeNs, uint64_t OffloadStage1TimeNs,
                                 uint64_t OffloadStage2TimeNs,
#endif
                                 ContextT Context)
{
    DDSPosixIoContext *ioContext = static_cast<DDSPosixIoContext *>(Context);
    if (!ioContext)
    {
        return;
    }
    ioContext->ddsResult.store(ErrorCode, std::memory_order_relaxed);
    ioContext->bytes.store(BytesServiced, std::memory_order_relaxed);
    ioContext->logicalBytes.store(LogicalBytes, std::memory_order_relaxed);
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    ioContext->offloadReadNs.store(OffloadReadTimeNs, std::memory_order_relaxed);
    ioContext->offloadStage1Ns.store(OffloadStage1TimeNs, std::memory_order_relaxed);
    ioContext->offloadStage2Ns.store(OffloadStage2TimeNs, std::memory_order_relaxed);
#endif
    ioContext->done.store(true, std::memory_order_release);
}

/**
 * Truncate a file to a specified length
 * 
 * @param fd The file descriptor
 * @param length The desired length of the file
 * @return 0 on success, -1 on error with errno set
 * 
 */
int ftruncate(int fd, off_t length) {
    if (!ddsInstance) {
        errno = ENOSYS;
        return -1;
    }

    FileIdT fileId = getFileId(fd);
    if (fileId == DDS_FILE_INVALID) {
        errno = EBADF;
        return -1;
    }

    // Update the file size in our tracking array
    // fileSize[fileId] = length;

    // Call the DDS API to change the file size
    ErrorCodeT result = ddsInstance->ChangeFileSize(fileId, length);
    if (result != DDS_ERROR_CODE_SUCCESS) {
        errno = EIO;
        return -1;
    }

    return 0;
}



/**
 * Get file status information
 *
 * @param path Path to the file
 * @param buf Pointer to a stat structure to be filled
 * @return 0 on success, -1 on failure with errno set
 */
 int dds_stat(const char* path, struct stat* buf) {
    if (!ddsInstance) {
        if (!initialize_posix("DDSFrontEnd")) {
            errno = ENOSYS;
            return -1;
        }
    }

    if (!path || !buf) {
        errno = EINVAL;
        return -1;
    }

    // Find the file
    FileIdT fileId = DDS_FILE_INVALID;
    ErrorCodeT result = ddsInstance->FindFirstFile(path, &fileId);
    if (result != DDS_ERROR_CODE_SUCCESS) {        
        errno = ENOENT;
        printf("600\n");
        return -1;
    }

    // Get file information
    // DDSFile* file = ddsInstance->GetFile(fileId);
    // if (!file) {
    //     errno = EIO;
    //     printf("608\n");
    //     return -1;
    // }

    // Get file size
    FileSizeT fileSize;
    int ret = ddsInstance->GetFileSize(fileId, &fileSize);
    // if (fileSize == 55980544) fileSize = 55980115;

    if (ret != DDS_ERROR_CODE_SUCCESS) {
        printf("Failed to get file size from dpu %d\n", ret);
        errno = EIO;
        return -1;
    }
    // FileSizeT size = fileSize[fileId];

    // Get file attributes
    // FileAttributesT attrs = file->GetAttributes();

    // Fill in the stat structure
    memset(buf, 0, sizeof(struct stat));
    // buf->st_mode = S_IFREG | (attrs & 0777); // Regular file with permissions from attributes
    // buf->st_nlink = 1;
    // buf->st_uid = NULL; // TODO
    // buf->st_gid = NULL; // TODO
    buf->st_size = fileSize;
    // buf->st_blocks = (size + 511) / 512; // Calculate number of 512-byte blocks
    // buf->st_atime = buf->st_mtime = buf->st_ctime = time(NULL);

    return 0;
}

/**
 * @brief Reads from a file at a specific offset without changing the file pointer
 *
 * Uses a per-call completion context and the background poller thread.
 * Tracks inflight operations to coordinate close() semantics.
 * Handles backend 512-byte alignment by rounding the I/O range and using a
 * thread-local scratch buffer for unaligned reads; aligned requests read
 * directly into the caller buffer (assumes backend alignment constraints are
 * offset/count only, not buffer address).
 * Updated: backend handles alignment; pread passes the original offset/count.
 * Legacy note: the rounding/bounce-buffer description above is retained for context only.
 *
 * @param fd The file descriptor to read from
 * @param buf Pointer to the buffer where data should be stored
 * @param count Number of bytes to read
 * @param offset Offset in the file where reading should begin
 * @return On success, returns the number of bytes read; on failure, returns -1 and sets errno
 */
ssize_t pread(int fd, void *buf, size_t count, off_t offset)
{

    if (!ddsInstance) {
        errno = ENOSYS;
        return -1;
    }

    if (offset < 0)
    {
        errno = EINVAL;
        return -1;
    }
    if (count == 0)
    {
        return 0;
    }

    // Thread-local scratch avoids per-call malloc/free for unaligned reads.
    // void *temp_buf = buf;
    // bool use_bounce = !is_aligned;
    // if (use_bounce)
    // {
    //     static thread_local void *tls_buf = nullptr;
    //     static thread_local size_t tls_capacity = 0;
    //     if (aligned_count > tls_capacity || tls_buf == nullptr)
    //     {
    //         // Grow TLS buffer as needed; alignment is for backend constraints.
    //         void *new_buf = nullptr;
    //         int alloc_rc = posix_memalign(&new_buf, kAlignment, aligned_count);
    //         if (alloc_rc != 0 || !new_buf)
    //         {
    //             errno = ENOMEM;
    //             return -1;
    //         }
    //         if (tls_buf)
    //         {
    //             free(tls_buf);
    //         }
    //         tls_buf = new_buf;
    //         tls_capacity = aligned_count;
    //     }
    //     temp_buf = tls_buf;
    // }
    // jason: Backend handles alignment; use caller buffer directly.
    void *temp_buf = buf;
    bool use_bounce = false;
    (void)use_bounce;

    // Enforce access mode for pread after validating the descriptor.
    DDSPosixFileHandle *handle = nullptr;
    if (!acquireFdForIo(fd, &handle))
    {
        // NOTE: bounce buffer is reused; no per-call free on error.
        // free(temp_buf);
        return -1;
    }
    DDSPosixInflightGuard inflightGuard(handle);
    // FileIdT fileId = getFileId(fd);
    FileIdT fileId = handle->fileId;
    /* if (fileId == DDS_FILE_INVALID) {
        errno = EBADF;
        return -1;
    } */
    if (fileId == DDS_FILE_INVALID) {
        // NOTE: bounce buffer is reused; no per-call free on error.
        // free(temp_buf);
        errno = EBADF;
        return -1;
    }
    if (!isReadAllowed(handle->flags))
    {
        // NOTE: bounce buffer is reused; no per-call free on error.
        // free(temp_buf);
        errno = EBADF;
        return -1;
    }

    // Legacy PReadFile path retained for reference.
    /*
    ErrorCodeT result;
    // size_t ioCount = 0;
    // jason: Use a per-call completion context instead of inline PollWait.
    DDSPosixIoContext ioContext;

    // Use the default poll ID
    PollIdT pollId = DDS_POLL_DEFAULT;

    // Start read operation
    FileIOSizeT bytesRead = 0;
    result = ddsInstance->PReadFile(
        fileId,
        offset,
        (BufferT)temp_buf,
        (FileIOSizeT)count,
        &bytesRead,
        DummyCallback,
        &ioCount  // Pass the counter as context
    );

    if (result != DDS_ERROR_CODE_IO_PENDING &&
        result != DDS_ERROR_CODE_SUCCESS) {
        // Restore original file pointer position
        // ddsInstance->SetFilePointer(fileId, originalPosition, FilePointerPosition::BEGIN);
        free(temp_buf);
        errno = EIO;
        return -1;
    }

    // Poll until completion
    ContextT ioCtxt, fileCtxt;
    bool pollResult = false;


    while (!pollResult) {
        result = ddsInstance->PollWait(
            pollId,
            &bytesRead,
            &fileCtxt,
            &ioCtxt,
            0,  // No timeout
            &pollResult
        );

        if (result != DDS_ERROR_CODE_SUCCESS &&
            result != DDS_ERROR_CODE_NO_COMPLETION) {
            // Restore original file pointer position
            // ddsInstance->SetFilePointer(fileId, originalPosition, FilePointerPosition::BEGIN);
            free(temp_buf);
            errno = EIO;
            return -1;
        }
    }
    */

    // Use per-file poll and offset-based ReadFile; completion is handled by the poller thread.
    ErrorCodeT result;
    // size_t ioCount = 0;
    // jason: Use a per-call completion context instead of inline PollWait.
    DDSPosixIoContext ioContext;
    // Start read operation
    FileIOSizeT bytesRead = 0;
    DDSFile* file = ddsInstance->GetFile(fileId);
    if (!file) {
        // NOTE: bounce buffer is reused; no per-call free on error.
        // free(temp_buf);
        errno = EBADF;
        return -1;
    }
    // PollIdT pollId = file->PollId;
    // jason: Polling is handled by the background poller thread.

    // result = ddsInstance->ReadFile(
    //     fileId,
    //     (BufferT)temp_buf,
    //     (FileSizeT)offset,
    //     (FileIOSizeT)count,
    //     &bytesRead,
    //     DummyCallback,
    //     &ioCount  // Pass the counter as context
    // );
    // result = ddsInstance->ReadFile(fileId, (BufferT)temp_buf, (FileSizeT)offset, (FileIOSizeT)count, &bytesRead,
    //                                DDSPosixCompletionCallback, &ioContext);
    // result = ddsInstance->ReadFile(fileId, (BufferT)temp_buf, (FileSizeT)aligned_offset, (FileIOSizeT)aligned_count,
    //                                &bytesRead, DDSPosixCompletionCallback, &ioContext);
    // jason: backend handles alignment; submit original offset/count.
    result = ddsInstance->ReadFile(fileId, (BufferT)temp_buf, (FileSizeT)offset, (FileIOSizeT)count, &bytesRead,
                                   DDSPosixCompletionCallback, &ioContext);

    while (result != DDS_ERROR_CODE_IO_PENDING &&
           result != DDS_ERROR_CODE_SUCCESS) {
        // Retry on ring full
        result = ddsInstance->ReadFile(fileId, (BufferT)temp_buf, (FileSizeT)offset, (FileIOSizeT)count, &bytesRead,
                                       DDSPosixCompletionCallback, &ioContext);
    }

    /* if (result != DDS_ERROR_CODE_IO_PENDING &&
        result != DDS_ERROR_CODE_SUCCESS) {
        // NOTE: TLS bounce buffer is reused; no per-call free on error.
        // free(temp_buf);
        // jason: should be ring full, try again later
        //errno = EIO;
        //return -1;
    } */
    // jason: Handle synchronous completion without waiting on the poller.
    if (result == DDS_ERROR_CODE_SUCCESS)
    {
        ioContext.ddsResult.store(result, std::memory_order_relaxed);
        ioContext.bytes.store(bytesRead, std::memory_order_relaxed);
        ioContext.done.store(true, std::memory_order_release);
    }

    // jason: Wait for the poller thread to signal this request's completion.
    while (!ioContext.done.load(std::memory_order_acquire))
    {
        // std::this_thread::yield();
    }
    if (ioContext.ddsResult.load(std::memory_order_relaxed) != DDS_ERROR_CODE_SUCCESS)
    {
        // NOTE: bounce buffer is reused; no per-call free on error.
        // free(temp_buf);
        errno = EIO;
        printf("pread got error on completion: %d\n", ioContext.ddsResult.load(std::memory_order_relaxed));
        return -1;
    }
    bytesRead = ioContext.bytes.load(std::memory_order_relaxed);

    //
    // // NOTE: bounce buffer is reused; no per-call free on success.
    // // free(temp_buf);
    // return (ssize_t)to_copy;
    //
    // // return bytesRead;
    // jason: backend/host handle alignment; return logical bytes read.
    return (ssize_t)bytesRead;
}

/**
 * @brief Reads from a file at a specific offset with explicit stage sizing/windows (pread2)
 *
 * The logical read size (count) may differ from the final output size; the backend returns
 * both logical and output byte counts, and only the final stage is delivered to the app buffer.
 *
 * @param fd The file descriptor to read from
 * @param buf Pointer to the buffer where data should be stored
 * @param count Logical number of bytes to read
 * @param offset Offset in the file where reading should begin
 * @param stageSizes Per-stage output sizes (stage1 decrypt, optional stage2 decompress)
 * @param stageInputOffsets Per-stage input window offsets from previous-stage output
 * @param stageInputLengths Per-stage input window lengths from previous-stage output
 * @param stageCount Number of stages; supported range is [1,2]
 * @return On success, returns the number of bytes written into the app buffer (final stage size);
 *         on failure, returns -1 and sets errno
 */
ssize_t pread2(int fd, void *buf, size_t count, off_t offset, const size_t *stageSizes,
               const size_t *stageInputOffsets, const size_t *stageInputLengths, uint16_t stageCount
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
               , DDSOffloadStageTimings *timings
#endif
               )
{
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    if (timings)
    {
        timings->read_ns = 0;
        timings->stage1_ns = 0;
        timings->stage2_ns = 0;
    }
#endif
    if (!ddsInstance) {
        errno = ENOSYS;
        return -1;
    }

    if (offset < 0)
    {
        errno = EINVAL;
        return -1;
    }
    if (count == 0)
    {
        // jason: logical count 0 returns immediately.
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
        if (timings)
        {
            timings->read_ns = 0;
            timings->stage1_ns = 0;
            timings->stage2_ns = 0;
        }
#endif
        return 0;
    }
    if (!stageSizes || !stageInputOffsets || !stageInputLengths || stageCount < 1 || stageCount > 2)
    {
        errno = EINVAL;
        return -1;
    }
    constexpr FileIOSizeT maxSize = std::numeric_limits<FileIOSizeT>::max();
    if (count > maxSize)
    {
        errno = EINVAL;
        return -1;
    }
    FileIOSizeT stageSizesLocal[2] = {(FileIOSizeT)0, (FileIOSizeT)0};
    FileIOSizeT stageInputOffsetsLocal[2] = {(FileIOSizeT)0, (FileIOSizeT)0};
    FileIOSizeT stageInputLengthsLocal[2] = {(FileIOSizeT)0, (FileIOSizeT)0};
    for (uint16_t stageIndex = 0; stageIndex < stageCount; stageIndex++)
    {
        if (stageSizes[stageIndex] > maxSize || stageInputOffsets[stageIndex] > maxSize ||
            stageInputLengths[stageIndex] > maxSize)
        {
            errno = EINVAL;
            return -1;
        }
        stageSizesLocal[stageIndex] = (FileIOSizeT)stageSizes[stageIndex];
        stageInputOffsetsLocal[stageIndex] = (FileIOSizeT)stageInputOffsets[stageIndex];
        stageInputLengthsLocal[stageIndex] = (FileIOSizeT)stageInputLengths[stageIndex];
    }
    // Stage[i] consumes a window from source bytes:
    // - stage0 source is logical read bytes (count)
    // - stageN source is stage(N-1) output bytes
    FileIOSizeT sourceBytes = (FileIOSizeT)count;
    for (uint16_t stageIndex = 0; stageIndex < stageCount; stageIndex++)
    {
        FileIOSizeT inputOffset = stageInputOffsetsLocal[stageIndex];
        FileIOSizeT inputLen = stageInputLengthsLocal[stageIndex];
        if (inputOffset > sourceBytes || inputLen > sourceBytes || inputLen > sourceBytes - inputOffset)
        {
            errno = EINVAL;
            return -1;
        }
        sourceBytes = stageSizesLocal[stageIndex];
    }

    // Enforce access mode for pread2 after validating the descriptor.
    DDSPosixFileHandle *handle = nullptr;
    if (!acquireFdForIo(fd, &handle))
    {
        return -1;
    }
    DDSPosixInflightGuard inflightGuard(handle);
    FileIdT fileId = handle->fileId;
    if (fileId == DDS_FILE_INVALID) {
        errno = EBADF;
        return -1;
    }
    if (!isReadAllowed(handle->flags))
    {
        errno = EBADF;
        return -1;
    }

    ErrorCodeT result;
    DDSPosixIoContext ioContext;
    FileIOSizeT bytesRead = 0;
    FileIOSizeT logicalBytesRead = 0;
    DDSFile* file = ddsInstance->GetFile(fileId);
    if (!file) {
        errno = EBADF;
        return -1;
    }

    result = ddsInstance->ReadFile2(fileId,
                                    (BufferT)buf,
                                    (FileSizeT)offset,
                                    (FileIOSizeT)count,
                                    stageSizesLocal,
                                    stageInputOffsetsLocal,
                                    stageInputLengthsLocal,
                                    stageCount,
                                    &bytesRead,
                                    &logicalBytesRead,
                                    DDSPosixCompletionCallback2,
                                    &ioContext);

    while (result != DDS_ERROR_CODE_IO_PENDING &&
           result != DDS_ERROR_CODE_SUCCESS) {
        // Retry on ring full
        result = ddsInstance->ReadFile2(fileId,
                                        (BufferT)buf,
                                        (FileSizeT)offset,
                                        (FileIOSizeT)count,
                                        stageSizesLocal,
                                        stageInputOffsetsLocal,
                                        stageInputLengthsLocal,
                                        stageCount,
                                        &bytesRead,
                                        &logicalBytesRead,
                                        DDSPosixCompletionCallback2,
                                        &ioContext);
    }

    // jason: Handle synchronous completion without waiting on the poller.
    if (result == DDS_ERROR_CODE_SUCCESS)
    {
        ioContext.ddsResult.store(result, std::memory_order_relaxed);
        ioContext.bytes.store(bytesRead, std::memory_order_relaxed);
        ioContext.logicalBytes.store(logicalBytesRead, std::memory_order_relaxed);
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
        ioContext.offloadReadNs.store(0, std::memory_order_relaxed);
        ioContext.offloadStage1Ns.store(0, std::memory_order_relaxed);
        ioContext.offloadStage2Ns.store(0, std::memory_order_relaxed);
#endif
        ioContext.done.store(true, std::memory_order_release);
    }

    // jason: Wait for the poller thread to signal this request's completion.
    while (!ioContext.done.load(std::memory_order_acquire))
    {
        // std::this_thread::yield();
    }
    if (ioContext.ddsResult.load(std::memory_order_relaxed) != DDS_ERROR_CODE_SUCCESS)
    {
        errno = EIO;
        printf("pread2 got error on completion: %d\n", ioContext.ddsResult.load(std::memory_order_relaxed));
        return -1;
    }
    bytesRead = ioContext.bytes.load(std::memory_order_relaxed);
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    if (timings)
    {
        timings->read_ns = ioContext.offloadReadNs.load(std::memory_order_relaxed);
        timings->stage1_ns = ioContext.offloadStage1Ns.load(std::memory_order_relaxed);
        timings->stage2_ns = ioContext.offloadStage2Ns.load(std::memory_order_relaxed);
    }
#endif

    return (ssize_t)bytesRead;
}

int set_read2_aes_key(const void *key, size_t keySizeBytes)
{
    if (!ddsInstance)
    {
        errno = ENOSYS;
        return -1;
    }
    if (key == nullptr || (keySizeBytes != 16 && keySizeBytes != 32))
    {
        errno = EINVAL;
        return -1;
    }
    ErrorCodeT result = ddsInstance->SetRead2AesKey((const uint8_t *)key, (uint8_t)keySizeBytes);
    if (result != DDS_ERROR_CODE_SUCCESS)
    {
        errno = EIO;
        return -1;
    }
    return 0;
}

/**
 * Read data from a file using the per-FD offset.
 *
 * Uses a per-call completion context and the background poller thread.
 * Serializes per-FD offset updates and tracks inflight operations.
 *
 * @param fd The file descriptor
 * @param buf The buffer to read into
 * @param count The number of bytes to read
 * @return The number of bytes read or -1 on failure
 */
ssize_t read(int fd, void* buf, size_t count) {

    if (!ddsInstance) {
        errno = ENOSYS;
        return -1;
    }

    DDSPosixFileHandle *handle = nullptr;
    if (!acquireFdForIo(fd, &handle))
    {
        errno = EBADF;
        return -1;
    }
    DDSPosixInflightGuard inflightGuard(handle);
    // Serialize per-FD offset updates for POSIX semantics.
    std::lock_guard<std::mutex> offsetLock(handle->offsetMutex);
    // FileIdT fileId = getFileId(fd);
    FileIdT fileId = handle->fileId;
    if (fileId == DDS_FILE_INVALID) {
        printf("FileId Invalid\n");
        errno = EBADF;
        return -1;
    }

    // Enforce access mode for read.
    if (!isReadAllowed(handle->flags))
    {
        errno = EBADF;
        return -1;
    }

    // size_t ioCount = 0; // contains reqid
    // jason: Use a per-call completion context instead of inline PollWait.
    DDSPosixIoContext ioContext;

    // Use per-file poll ID for consistency with the front end.
    // PollIdT pollId = DDS_POLL_DEFAULT;
    DDSFile *file = ddsInstance->GetFile(fileId);
    if (!file)
    {
        errno = EBADF;
        return -1;
    }
    // PollIdT pollId = file->PollId;
    // jason: Polling is handled by the background poller thread.

    // Start read operation using the per-FD offset for POSIX semantics.
    FileIOSizeT bytesRead = 0;
    off_t currentOffset = handle->offset;
    // ErrorCodeT result = ddsInstance->ReadFile(
    //     fileId,
    //     (BufferT)buf,
    //     (FileIOSizeT)count,
    //     &bytesRead,
    //     DummyCallback,
    //     &ioCount  // Pass the counter as context
    // );
    // ErrorCodeT result =
    //     ddsInstance->ReadFile(fileId, (BufferT)buf, (FileSizeT)currentOffset,
    //                           (FileIOSizeT)count, &bytesRead, DummyCallback,
    //                           &ioCount // Pass the counter as context
    //     );
    ErrorCodeT result = ddsInstance->ReadFile(fileId, (BufferT)buf, (FileSizeT)currentOffset, (FileIOSizeT)count,
                                              &bytesRead, DDSPosixCompletionCallback, &ioContext);

    if (result != DDS_ERROR_CODE_IO_PENDING && 
        result != DDS_ERROR_CODE_SUCCESS) {
        printf("ReadFile error: %d\n", result);
        errno = EIO;
        return -1;
    }
    // jason: Handle synchronous completion without waiting on the poller.
    if (result == DDS_ERROR_CODE_SUCCESS)
    {
        ioContext.ddsResult.store(result, std::memory_order_relaxed);
        ioContext.bytes.store(bytesRead, std::memory_order_relaxed);
        ioContext.done.store(true, std::memory_order_release);
    }

    // Poll until completion
    // size_t id;
    // ContextT ioCtxt, fileCtxt; // ContextT = void*
    // bool pollResult = false;
    //
    // while (!pollResult) {
    //     result = ddsInstance->PollWait(
    //         pollId,
    //         &bytesRead,
    //         &fileCtxt,
    //         &ioCtxt,
    //         0,
    //         &pollResult
    //     );
    //
    //     if (result != DDS_ERROR_CODE_SUCCESS &&
    //         result != DDS_ERROR_CODE_NO_COMPLETION) {
    //         printf("PollWait error: %d\n", result);
    //         errno = EIO;
    //         return -1;
    //     }
    // }
    // jason: Wait for the poller thread to signal this request's completion.
    while (!ioContext.done.load(std::memory_order_acquire))
    {
        // std::this_thread::yield();
    }
    if (ioContext.ddsResult.load(std::memory_order_relaxed) != DDS_ERROR_CODE_SUCCESS)
    {
        errno = EIO;
        return -1;
    }
    bytesRead = ioContext.bytes.load(std::memory_order_relaxed);
    // Update per-FD offset after successful completion.
    if (bytesRead > 0) {
        handle->offset = currentOffset + (off_t)bytesRead;
    }
    // have reqid via ioCtxt, update flag, wait flag, proceed 
    // while loop flag array req id for its own is set 
    // printf("Read completed, bytesRead: %lu\n", bytesRead);
    
    return bytesRead;
}

/**
 * @brief Writes to a file at a specified offset without changing the per-FD
 * offset
 *
 * Uses a per-call completion context and the background poller thread.
 * Tracks inflight operations to coordinate close() semantics.
 *
 * @param fd File descriptor of the target file
 * @param buf Pointer to the buffer containing data to be written
 * @param count Number of bytes to write
 * @param offset Position in the file where writing should begin
 * @return On success, returns the number of bytes written; on failure, returns
 * -1 and sets errno
 *
 */
ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset) {

    // assert(count <= 1048576);
    if (!ddsInstance) {
        errno = ENOSYS;
        return -1;
    }

    DDSPosixFileHandle *handle = nullptr;
    if (!acquireFdForIo(fd, &handle))
    {
        errno = EBADF;
        return -1;
    }
    DDSPosixInflightGuard inflightGuard(handle);
    // FileIdT fileId = getFileId(fd);
    FileIdT fileId = handle->fileId;
    if (fileId == DDS_FILE_INVALID) {
        errno = EBADF;
        return -1;
    }
    // Enforce access mode for pwrite.
    if (!isWriteAllowed(handle->flags))
    {
        errno = EBADF;
        return -1;
    }
    // FileSizeT oldsize = fileSize[fileId];
    // File size tracking is delegated to the backend; no local cache.

    // manually update file size
    // fileSize[fileId] = fileSize[fileId] + count;

    // Use the offset-based write path to avoid changing the global file
    // pointer. DDSFile* file = ddsInstance->GetFile(fileId); if (!file) {
    //     errno = EBADF;
    //     return -1;
    // }
    // PositionInFileT originalPosition = file->GetPointer();
    //
    // ErrorCodeT result = ddsInstance->SetFilePointer(fileId, offset,
    // FilePointerPosition::BEGIN); if (result != DDS_ERROR_CODE_SUCCESS) {
    //     errno = EIO;
    //     return -1;
    // }

    // Use a completion counter
    size_t ioCount = 0;
    // jason: Use a per-call completion context instead of inline PollWait.
    DDSPosixIoContext ioContext;

    // Use per-file poll ID for consistency with the front end.
    // PollIdT pollId = DDS_POLL_DEFAULT;
    DDSFile *file = ddsInstance->GetFile(fileId);
    if (!file)
    {
        errno = EBADF;
        return -1;
    }
    // PollIdT pollId = file->PollId;
    // jason: Polling is handled by the background poller thread.

    ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
    FileIOSizeT bytesWritten = 0;

    // result =
    //     ddsInstance->WriteFile(fileId, (BufferT)buf, (FileSizeT)offset,
    //                            (FileIOSizeT)count, &bytesWritten, DummyCallback,
    //                            &ioCount // Pass the counter as context
    //     );
    result = ddsInstance->WriteFile(fileId, (BufferT)buf, (FileSizeT)offset, (FileIOSizeT)count, &bytesWritten,
                                    DDSPosixCompletionCallback, &ioContext);

    if (result != DDS_ERROR_CODE_IO_PENDING && 
        result != DDS_ERROR_CODE_SUCCESS) {
        // Restore original file pointer position
        // ddsInstance->SetFilePointer(fileId, originalPosition,
        // FilePointerPosition::BEGIN);
        // errno = result;
        errno = EIO;
        return -1;
    }
    // jason: Handle synchronous completion without waiting on the poller.
    if (result == DDS_ERROR_CODE_SUCCESS)
    {
        ioContext.ddsResult.store(result, std::memory_order_relaxed);
        ioContext.bytes.store(bytesWritten, std::memory_order_relaxed);
        ioContext.done.store(true, std::memory_order_release);
    }

    // Poll until completion
    // ContextT ioCtxt, fileCtxt;
    // bool pollResult = false;
    //
    // while (!pollResult) {
    //     result = ddsInstance->PollWait(
    //         pollId,
    //         &bytesWritten,
    //         &fileCtxt,
    //         &ioCtxt,
    //         0,  // No timeout
    //         &pollResult
    //     );
    //
    //     if (result != DDS_ERROR_CODE_SUCCESS &&
    //         result != DDS_ERROR_CODE_NO_COMPLETION) {
    //         // Restore original file pointer position
    //         // ddsInstance->SetFilePointer(fileId, originalPosition,
    //         // FilePointerPosition::BEGIN);
    //         // errno = result;
    //         errno = EIO;
    //         return -1;
    //     }
    // }
    // jason: Wait for the poller thread to signal this request's completion.
    while (!ioContext.done.load(std::memory_order_acquire))
    {
        // std::this_thread::yield();
    }
    if (ioContext.ddsResult.load(std::memory_order_relaxed) != DDS_ERROR_CODE_SUCCESS)
    {
        errno = EIO;
        return -1;
    }
    bytesWritten = ioContext.bytes.load(std::memory_order_relaxed);

    // Restore the file pointer to its original position
    // ddsInstance->SetFilePointer(fileId, originalPosition,
    // FilePointerPosition::BEGIN);
    // FileSizeT newSize = offset + bytesWritten;
    // if (newSize > oldsize) {
    //   fileSize[fileId] = newSize;
    // }

    return bytesWritten;

}

/**
 * Write data to a file using the per-FD offset.
 *
 * Uses a per-call completion context and the background poller thread.
 * Serializes per-FD offset updates and tracks inflight operations.
 *
 * @param fd The file descriptor
 * @param buf The buffer to write from
 * @param count The number of bytes to write
 * @return The number of bytes written or -1 on failure
 *
 */
ssize_t write(int fd, const void* buf, size_t count) {

    // assert(count <= 1048576);
    if (!ddsInstance) {
        errno = ENOSYS;
        return -1;
    }

    DDSPosixFileHandle *handle = nullptr;
    if (!acquireFdForIo(fd, &handle))
    {
        errno = EBADF;
        return -1;
    }
    DDSPosixInflightGuard inflightGuard(handle);
    // Serialize per-FD offset updates for POSIX semantics.
    std::lock_guard<std::mutex> offsetLock(handle->offsetMutex);
    // FileIdT fileId = getFileId(fd);
    FileIdT fileId = handle->fileId;
    if (fileId == DDS_FILE_INVALID) {
        errno = EBADF;
        return -1;
    }

    // Enforce access mode for write.
    if (!isWriteAllowed(handle->flags))
    {
        errno = EBADF;
        return -1;
    }

    // manually update file size

    // size_t ioCount = 0;
    // jason: Use a per-call completion context instead of inline PollWait.
    DDSPosixIoContext ioContext;

    // Use per-file poll ID instead of the default for consistency.
    // PollIdT pollId = DDS_POLL_DEFAULT;
    DDSFile *file = ddsInstance->GetFile(fileId);
    if (!file)
    {
        errno = EBADF;
        return -1;
    }
    // PollIdT pollId = file->PollId;
    // jason: Polling is handled by the background poller thread.

    ErrorCodeT result;
    FileIOSizeT bytesWritten = 0;

    // Start write operation using per-FD offset (and O_APPEND if set).
    FileSizeT fileEnd = 0;
    off_t currentOffset = handle->offset;
    if (handle->flags & O_APPEND)
    {
        result = ddsInstance->GetFileSize(fileId, &fileEnd);
        if (result != DDS_ERROR_CODE_SUCCESS)
        {
            errno = EIO;
            return -1;
        }
        currentOffset = (off_t)fileEnd;
    }
    // result = ddsInstance->WriteFile(
    //     fileId,
    //     (BufferT)buf,
    //     (FileIOSizeT)count,
    //     &bytesWritten,
    //     DummyCallback,
    //     &ioCount  // Pass the counter as context
    // );
    // result =
    //     ddsInstance->WriteFile(fileId, (BufferT)buf, (FileSizeT)currentOffset,
    //                            (FileIOSizeT)count, &bytesWritten, DummyCallback,
    //                            &ioCount // Pass the counter as context
    //     );
    result = ddsInstance->WriteFile(fileId, (BufferT)buf, (FileSizeT)currentOffset, (FileIOSizeT)count, &bytesWritten,
                                    DDSPosixCompletionCallback, &ioContext);

    if (result != DDS_ERROR_CODE_IO_PENDING && 
        result != DDS_ERROR_CODE_SUCCESS) {
        // errno = result;
        errno = EIO;
        return -1;
    }
    // jason: Handle synchronous completion without waiting on the poller.
    if (result == DDS_ERROR_CODE_SUCCESS)
    {
        ioContext.ddsResult.store(result, std::memory_order_relaxed);
        ioContext.bytes.store(bytesWritten, std::memory_order_relaxed);
        ioContext.done.store(true, std::memory_order_release);
    }

    // Poll until completion like the working example
    // ContextT ioCtxt, fileCtxt;
    // bool pollResult = false;
    //
    // while (!pollResult) {
    //     result = ddsInstance->PollWait(
    //         pollId,  // Use default poll ID
    //         &bytesWritten,
    //         &fileCtxt,
    //         &ioCtxt,
    //         0,  // No timeout - will return immediately if no completion
    //         &pollResult
    //     );
    //
    //     if (result != DDS_ERROR_CODE_SUCCESS &&
    //         result != DDS_ERROR_CODE_NO_COMPLETION) {
    //         printf("PollWait error: %d\n", result);
    //         // errno = result;
    //         errno = EIO;
    //         return -1;
    //     }
    // }
    // jason: Wait for the poller thread to signal this request's completion.
    while (!ioContext.done.load(std::memory_order_acquire))
    {
        // std::this_thread::yield();
    }
    if (ioContext.ddsResult.load(std::memory_order_relaxed) != DDS_ERROR_CODE_SUCCESS)
    {
        errno = EIO;
        return -1;
    }
    bytesWritten = ioContext.bytes.load(std::memory_order_relaxed);
    // Update per-FD offset and local size tracking for POSIX semantics.
    // Update per-FD offset for POSIX semantics; size comes from backend.
    handle->offset = currentOffset + (off_t)bytesWritten;
    // if (openFiles[fd].offset > (off_t)fileSize[fileId]) {
    //   fileSize[fileId] = (FileSizeT)openFiles[fd].offset;
    // }
    // fileSize[fileId] = fileSize[fileId] + bytesWritten;

    return bytesWritten;
}

/**
 * Close a file
 *
 * @param fd The file descriptor
 * @return 0 on success, -1 on failure
 *
 * Marks the fd as closed immediately and waits for inflight I/O to finish.
 */
int close(int fd) {
    if (!ddsInstance) {
        errno = ENOSYS;
        return -1;
    }

    DDSPosixFileHandle *handle = nullptr;
    {
        std::lock_guard<std::mutex> lock(g_fdTableMutex);
        if (!isValidFileDescriptor(fd))
        {
            errno = EBADF;
            return -1;
        }
        handle = &openFiles[fd];
        // jason: Prevent new I/O from being issued on this fd.
        handle->closing.store(true, std::memory_order_release);
        handle->isOpen.store(false, std::memory_order_release);
        g_fdInUse[fd] = false;
    }
    // jason: Wait for inflight I/O to drain before releasing the descriptor.
    {
        std::unique_lock<std::mutex> lock(handle->inflightMutex);
        handle->inflightCv.wait(lock, [handle]() { return handle->inflight.load(std::memory_order_acquire) == 0; });
    }
    // releaseFileDescriptor(fd);
    releaseFileDescriptor(fd);
    return 0;
}

/**
 * Change the per-FD file offset (POSIX semantics).
 *
 * Serializes offset updates and tracks inflight operations to coordinate close().
 *
 * @param fd The file descriptor
 * @param offset The offset value
 * @param whence Where to start from
 * @return The new offset or -1 on failure
 */
off_t lseek(int fd, off_t offset, int whence) {
    if (!ddsInstance) {
        errno = ENOSYS;
        return -1;
    }

    DDSPosixFileHandle *handle = nullptr;
    if (!acquireFdForIo(fd, &handle))
    {
        errno = EBADF;
        return -1;
    }
    DDSPosixInflightGuard inflightGuard(handle);
    // Serialize per-FD offset updates for POSIX semantics.
    std::lock_guard<std::mutex> offsetLock(handle->offsetMutex);
    // FileIdT fileId = getFileId(fd);
    FileIdT fileId = handle->fileId;
    if (fileId == DDS_FILE_INVALID) {
        errno = EBADF;
        return -1;
    }

    // Use per-FD offset; avoid mutating the shared DDS file pointer.
    // DDSFile* file = nullptr;
    // if (fileId >= 0 && fileId < ddsInstance->GetFileIdEnd()) {
    //     file = ddsInstance->GetFile(fileId);
    //     if (!file) {
    //         errno = EBADF;
    //         return -1;
    //     }
    // } else {
    //     errno = EBADF;
    //     return -1;
    // }
    //
    // Calculate what the new position will be
    // PositionInFileT newPosition = 0;
    // switch (whence) {
    //     case SEEK_SET:
    //         newPosition = offset;
    //         break;
    //     case SEEK_CUR:
    //         newPosition = file->GetPointer() + offset;
    //         break;
    //     case SEEK_END:
    //         newPosition = file->GetSize() + offset;
    //         break;
    //     default:
    //         errno = EINVAL;
    //         return -1;
    // }
    //
    // Set the file pointer
    // FilePointerPosition position = static_cast<FilePointerPosition>(whence);
    // ErrorCodeT result = ddsInstance->SetFilePointer(fileId, offset,
    // position); if (result != DDS_ERROR_CODE_SUCCESS) {
    //     errno = EIO;
    //     return -1;
    // }

    off_t baseOffset = 0;
    switch (whence) {
        case SEEK_SET:
          baseOffset = 0;
          break;
        case SEEK_CUR:
            baseOffset = handle->offset;
            break;
        case SEEK_END: {
          FileSizeT size = 0;
          ErrorCodeT result = ddsInstance->GetFileSize(fileId, &size);
          if (result != DDS_ERROR_CODE_SUCCESS) {
            errno = EIO;
            return -1;
          }
          baseOffset = (off_t)size;
          break;
        }
        default:
            errno = EINVAL;
            return -1;
    }

    off_t newPosition = baseOffset + offset;
    if (newPosition < 0) {
      errno = EINVAL;
      return -1;
    }
    handle->offset = newPosition;

    // Return the calculated new position
    return newPosition;
}

} // namespace DDSFrontEnd
