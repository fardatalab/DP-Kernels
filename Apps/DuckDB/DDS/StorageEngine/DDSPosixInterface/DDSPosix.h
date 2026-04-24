#pragma once
#include <sys/types.h>
#include <cstdint>
#include "DDSPosixTypes.h"

#define DDS_MAX_FD 4096 // DDS_MAX_FD < Rocks DB (2^32) 
#define DDS_MAX_DIR 64

namespace DDSPosix = DDS_FrontEnd;

namespace DDS_FrontEnd {

#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
typedef struct DDSOffloadStageTimings {
    uint64_t read_ns;
    uint64_t stage1_ns;
    uint64_t stage2_ns;
} DDSOffloadStageTimings;
#endif

bool initialize_posix(const char* storeName);
bool shutdown_posix();

// core file functions
int open(const char* pathname, int flags, mode_t mode = 0);
int close(int fd);
ssize_t read(int fd, void* buf, size_t count);
ssize_t write(int fd, const void* buf, size_t count);
off_t lseek(int fd, off_t offset, int whence);
int dds_stat(const char* path, struct stat* buf);
ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset);
int ftruncate(int fd, off_t length);
ssize_t pread(int fd, void* buf, size_t count, off_t offset);
// read2: logical bytes + explicit per-stage output sizes and stage input windows
ssize_t pread2(int fd, void* buf, size_t count, off_t offset, const size_t* stageSizes,
               const size_t* stageInputOffsets, const size_t* stageInputLengths, uint16_t stageCount
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
               , DDSOffloadStageTimings *timings
#endif
               );
int set_read2_aes_key(const void* key, size_t keySizeBytes);
// int fstat(int fd, struct stat* buf);

// directory operations
DIR* opendir(const char* pathname);
struct dirent* readdir(DIR* dirp);
int closedir(DIR* dirp);
void rewinddir(DIR* dirp);


// needed? file manipulation 
int unlike(const char* pathname);
int rename(const char* oldpath, const char* newpath);
int truncate(const char* pathname, off_t length);
int ftruncate(int fd, off_t length);

}
