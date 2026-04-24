#pragma once

#if defined (_MSC_VER) || defined (__cplusplus)
#include <atomic>
#else
#include <stdatomic.h>
#endif
#include <stdint.h>
#include <time.h>

#include "Protocol.h"

#if defined (_MSC_VER) || defined (__cplusplus)
template <class C>
using Atomic=std::atomic<C>;
#endif

//
// Types used in DDS
//
//
typedef char* BufferT;
typedef void* ContextT;
typedef uint16_t DirIdT;
typedef uint16_t ErrorCodeT;
typedef uint32_t FileAccessT;
typedef uint32_t FileAttributesT;
typedef uint16_t FileIdT;
typedef int64_t PositionInFileT;
typedef uint32_t FileShareModeT;
typedef uint32_t FileCreationDispositionT;
typedef uint32_t FileIOSizeT;
typedef uint64_t FileSizeT;
typedef uint16_t PollIdT;
typedef uint32_t RingSizeT;
typedef uint32_t RingSlotSizeT;
typedef uint16_t RequestIdT;
typedef uint8_t BoolT;

//
// Types of position in file
//
//
enum FilePointerPosition {
    BEGIN,
    CURRENT,
    END
};

//
// Properties of a file (front end view)
//
//
typedef struct FileProperties {
  FileAttributesT FileAttributes;
  time_t CreationTime;
  time_t LastAccessTime;
  time_t LastWriteTime;
  FileSizeT FileSize;
  FileAccessT Access;
#if defined (_MSC_VER) || defined (__cplusplus)
  Atomic<PositionInFileT> Position;
#else
  _Atomic PositionInFileT Position;
#endif
  FileShareModeT ShareMode;
  char FileName[DDS_MAX_FILE_PATH];
} FilePropertiesT;

//
// Describe an object that might be split on the ring buffer
//
//
typedef struct {
    RingSizeT TotalSize;
    RingSizeT FirstSize;
    BufferT FirstAddr;
    BufferT SecondAddr;
} SplittableBufferT;


//
// Check a few parameters at the compile time
//
//
#define AssertStaticDDSTypes(e, num) \
    enum { AssertStaticDDSTypes__##num = 1/(e) }

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result" 
#else
#pragma warning(push)
#pragma warning (disable: 4804)
#endif

AssertStaticDDSTypes((1 << sizeof(FileIdT) * 8) - 1 == DDS_FILE_INVALID, 0);
AssertStaticDDSTypes((1 << sizeof(DirIdT) * 8) - 1 == DDS_DIR_INVALID, 1);
AssertStaticDDSTypes((1 << sizeof(RequestIdT) * 8) - 1 == DDS_REQUEST_INVALID, 2);

#ifdef __GNUC__
#pragma GCC diagnostic pop
#else
#pragma warning(pop)
#endif
