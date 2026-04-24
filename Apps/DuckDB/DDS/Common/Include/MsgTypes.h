#pragma once

#include <stdint.h>

#ifdef __cplusplus
#include <atomic>
#else
#include <stdatomic.h>
#endif

#include "DDSTypes.h"

#ifdef __cplusplus
#define DDS_ALIGNOF(T) alignof(T)
#else
#define DDS_ALIGNOF(T) _Alignof(T)
#endif

#define MSG_CTXT ((uint64_t)0x5000) // changed from ((void *) 0x5000);

#define CTRL_CONN_PRIV_DATA 42
#define BUFF_CONN_PRIV_DATA 24

#define CTRL_MSG_SIZE 256
#define BUFF_MSG_SIZE 64

//
// Atomic error code helpers to publish response completion ordering.
// Ensures BytesServiced is visible before Result transitions from IO_PENDING.
//
#ifdef __cplusplus
typedef std::atomic<ErrorCodeT> DDSAtomicErrorCodeT;
#define DDS_ATOMIC_ORDER_RELAXED std::memory_order_relaxed
#define DDS_ATOMIC_ORDER_ACQUIRE std::memory_order_acquire
#define DDS_ATOMIC_ORDER_RELEASE std::memory_order_release
#define DDS_ATOMIC_ERRORCODE_LOAD(ptr, order) (ptr)->load(order)
#define DDS_ATOMIC_ERRORCODE_STORE(ptr, val, order) (ptr)->store((val), (order))
#else
typedef _Atomic ErrorCodeT DDSAtomicErrorCodeT;
#define DDS_ATOMIC_ORDER_RELAXED memory_order_relaxed
#define DDS_ATOMIC_ORDER_ACQUIRE memory_order_acquire
#define DDS_ATOMIC_ORDER_RELEASE memory_order_release
#define DDS_ATOMIC_ERRORCODE_LOAD(ptr, order) atomic_load_explicit((ptr), (order))
#define DDS_ATOMIC_ERRORCODE_STORE(ptr, val, order) atomic_store_explicit((ptr), (val), (order))
#endif

#define CTRL_MSG_F2B_REQUEST_ID 0
#define CTRL_MSG_F2B_TERMINATE 1
#define CTRL_MSG_B2F_RESPOND_ID 2
#define CTRL_MSG_F2B_REQ_CREATE_DIR 3
#define CTRL_MSG_B2F_ACK_CREATE_DIR 4
#define CTRL_MSG_F2B_REQ_REMOVE_DIR 5
#define CTRL_MSG_B2F_ACK_REMOVE_DIR 6
#define CTRL_MSG_F2B_REQ_CREATE_FILE 7
#define CTRL_MSG_B2F_ACK_CREATE_FILE 8
#define CTRL_MSG_F2B_REQ_DELETE_FILE 9
#define CTRL_MSG_B2F_ACK_DELETE_FILE 10
#define CTRL_MSG_F2B_REQ_CHANGE_FILE_SIZE 11
#define CTRL_MSG_B2F_ACK_CHANGE_FILE_SIZE 12
#define CTRL_MSG_F2B_REQ_GET_FILE_SIZE 13
#define CTRL_MSG_B2F_ACK_GET_FILE_SIZE 14
#define CTRL_MSG_F2B_REQ_GET_FILE_INFO 15
#define CTRL_MSG_B2F_ACK_GET_FILE_INFO 16
#define CTRL_MSG_F2B_REQ_GET_FILE_ATTR 17
#define CTRL_MSG_B2F_ACK_GET_FILE_ATTR 18
#define CTRL_MSG_F2B_REQ_GET_FREE_SPACE 19
#define CTRL_MSG_B2F_ACK_GET_FREE_SPACE 20
#define CTRL_MSG_F2B_REQ_MOVE_FILE 21
#define CTRL_MSG_B2F_ACK_MOVE_FILE 22
#define CTRL_MSG_F2B_REQ_FIND_FIRST_FILE 25
#define CTRL_MSG_B2F_ACK_FIND_FIRST_FILE 26
// jason: DPU I/O alignment query (block size + buffer alignment).
#define CTRL_MSG_F2B_REQ_GET_IO_ALIGN 27
#define CTRL_MSG_B2F_ACK_GET_IO_ALIGN 28
// jason: Configure read2 AES key bytes for DPK stage-1 decrypt.
#define CTRL_MSG_F2B_REQ_SET_READ2_AES_KEY 29
#define CTRL_MSG_B2F_ACK_SET_READ2_AES_KEY 30

#define BUFF_MSG_F2B_REQUEST_ID 100
#define BUFF_MSG_B2F_RESPOND_ID 101
#define BUFF_MSG_F2B_RELEASE 102

typedef struct
{
    RequestIdT MsgId;
} MsgHeader;

typedef struct
{
    int Dummy;
} CtrlMsgF2BRequestId;

typedef struct
{
    int ClientId;
} CtrlMsgF2BTerminate;

typedef struct
{
    int ClientId;
} CtrlMsgB2FRespondId;

typedef struct
{
    int ClientId;
    uint64_t BufferAddress;
    uint32_t AccessToken;
    uint32_t Capacity;
    // Logical IO channel index (DDS_IO_CHANNEL_PRIMARY or DDS_IO_CHANNEL_PREAD2).
    // Each channel creates its own RDMA connection; the backend uses this field
    // to tag the BuffConnConfig with the correct logical channel identity.
    int ChannelIndex;
} BuffMsgF2BRequestId;

typedef struct
{
    int BufferId;
} BuffMsgB2FRespondId;

typedef struct
{
    int ClientId;
    int BufferId;
} BuffMsgF2BRelease;

typedef struct
{
    DirIdT DirId;
    DirIdT ParentDirId;
    char PathName[DDS_MAX_FILE_PATH];
} CtrlMsgF2BReqCreateDirectory;

typedef struct
{
    ErrorCodeT Result;
} CtrlMsgB2FAckCreateDirectory;

typedef struct
{
    DirIdT DirId;
} CtrlMsgF2BReqRemoveDirectory;

typedef struct
{
    ErrorCodeT Result;
} CtrlMsgB2FAckRemoveDirectory;

typedef struct
{
    FileIdT FileId;
    DirIdT DirId;
    FileAttributesT FileAttributes;
    // Access/share are front-end-only; not sent to backend.
    // FileAccessT Access;
    // FileShareModeT ShareMode;
    char FileName[DDS_MAX_FILE_PATH];
} CtrlMsgF2BReqCreateFile;

typedef struct
{
    ErrorCodeT Result;
} CtrlMsgB2FAckCreateFile;

typedef struct
{
    char FileName[DDS_MAX_FILE_PATH];
} CtrlMsgF2BReqFindFirstFile;

typedef struct
{
    // FileIdT FileId;
    // ErrorCodeT Result;
    //
    // jason: Result is the first field for control-plane responses so the
    // completion path can treat the first word as the error code.
    ErrorCodeT Result;
    FileIdT FileId;
} CtrlMsgB2FAckFindFirstFile;

typedef struct
{
    FileIdT FileId;
    DirIdT DirId;
} CtrlMsgF2BReqDeleteFile;

typedef struct
{
    ErrorCodeT Result;
} CtrlMsgB2FAckDeleteFile;

typedef struct
{
    FileIdT FileId;
    FileSizeT NewSize;
} CtrlMsgF2BReqChangeFileSize;

typedef struct
{
    ErrorCodeT Result;
} CtrlMsgB2FAckChangeFileSize;

typedef struct
{
    FileIdT FileId;
} CtrlMsgF2BReqGetFileSize;

typedef struct
{
    ErrorCodeT Result;
    FileSizeT FileSize;
} CtrlMsgB2FAckGetFileSize;

typedef struct
{
    FileIdT FileId;
} CtrlMsgF2BReqGetFileInfo;

//
// DPU backend file info (backend-only metadata; no access/share/position).
//
typedef struct
{
    FileAttributesT FileAttributes;
    time_t CreationTime;
    time_t LastAccessTime;
    time_t LastWriteTime;
    FileSizeT FileSize;
    char FileName[DDS_MAX_FILE_PATH];
} DPUFileInfoT;

typedef struct
{
    ErrorCodeT Result;
    // FilePropertiesT FileInfo;
    // DPU backend returns backend-only file info; front-end maps to FilePropertiesT.
    DPUFileInfoT FileInfo;
} CtrlMsgB2FAckGetFileInfo;

typedef struct
{
    FileIdT FileId;
} CtrlMsgF2BReqGetFileAttr;

typedef struct
{
    ErrorCodeT Result;
    FileAttributesT FileAttr;
} CtrlMsgB2FAckGetFileAttr;

typedef struct
{
    int Dummy;
} CtrlMsgF2BReqGetFreeSpace;

typedef struct
{
    ErrorCodeT Result;
    FileSizeT FreeSpace;
} CtrlMsgB2FAckGetFreeSpace;

// jason: Control plane request to fetch DPU I/O alignment parameters.
typedef struct
{
    int Dummy;
} CtrlMsgF2BReqGetIoAlign;

// jason: Control plane response with DPU I/O alignment parameters.
typedef struct
{
    ErrorCodeT Result;
    FileIOSizeT BlockSize;
    FileIOSizeT BufAlign;
} CtrlMsgB2FAckGetIoAlign;

// jason: Control-plane request to set read2 AES key material (16 or 32 bytes).
typedef struct
{
    uint8_t KeySizeBytes;
    uint8_t Reserved[3];
    uint8_t KeyBytes[32];
} CtrlMsgF2BReqSetRead2AesKey;

// jason: Control-plane response for read2 AES key setup.
typedef struct
{
    ErrorCodeT Result;
} CtrlMsgB2FAckSetRead2AesKey;

typedef struct
{
    FileIdT FileId;
    DirIdT OldDirId;
    DirIdT NewDirId;
    char NewFileName[DDS_MAX_FILE_PATH];
} CtrlMsgF2BReqMoveFile;

typedef struct
{
    ErrorCodeT Result;
} CtrlMsgB2FAckMoveFile;

// jason: Request opcodes for data plane operations (read/write variants).
typedef enum
{
    BUFF_MSG_F2B_REQ_OP_READ = 1,
    BUFF_MSG_F2B_REQ_OP_READ2 = 2,
    BUFF_MSG_F2B_REQ_OP_WRITE = 3,
    BUFF_MSG_F2B_REQ_OP_WRITE_GATHER = 4
} BuffMsgF2BReqOpCode;

/* Original request header preserved for reference.
typedef struct {
    RequestIdT RequestId;
    FileIdT FileId;
    FileIOSizeT Bytes;
    FileSizeT Offset;
} BuffMsgF2BReqHeader;
*/

/* Previous updated request header preserved for reference (static assert now fails).
typedef struct {
    uint16_t OpCode;
    uint16_t Flags;
    RequestIdT RequestId;
    FileIdT FileId;
    // Bytes == logical bytes to read/write for read/read2/write.
    FileIOSizeT Bytes;
    // BufferBytes == output buffer size (read2) or Bytes for non-read2.
    FileIOSizeT BufferBytes;
    FileSizeT Offset;
} BuffMsgF2BReqHeader;
*/

/* Packed-to-4 request header preserved for reference (sizeof rounded to 32 on 64-bit ABIs).
typedef struct {
    uint16_t OpCode;
    uint16_t Flags;
    RequestIdT RequestId;
    FileIdT FileId;
    // Bytes == logical bytes to read/write for read/read2/write.
    FileIOSizeT Bytes;
    // BufferBytes == output buffer size (read2) or Bytes for non-read2.
    FileIOSizeT BufferBytes;
    FileSizeT Offset;
    // jason: Pad header to 28 bytes so (header + size field) == 32 and divides the ring size.
    uint32_t RingPadding;
} BuffMsgF2BReqHeader;
*/

// jason: Natural alignment is required because the header includes 8-byte fields.
// #pragma pack(push, 4)
typedef struct
{
    uint16_t OpCode;
    uint16_t Flags;
    RequestIdT RequestId;
    FileIdT FileId;
    // Bytes == logical bytes to read/write for read/read2/write.
    FileIOSizeT Bytes;
    // BufferBytes == output buffer size (read2) or Bytes for non-read2.
    FileIOSizeT BufferBytes;
    FileSizeT Offset;
    // jason: Structural padding reserved for future flags/fields (natural size rounds to 32 bytes).
    uint32_t RingPadding;
} BuffMsgF2BReqHeader;
// #pragma pack(pop)

// jason: Read2 request payload alias (shares the same header layout).
typedef BuffMsgF2BReqHeader BuffMsgF2BReqRead2;

/* Original response header preserved for reference.
typedef struct {
    RequestIdT RequestId;
    // ErrorCodeT Result;
    // Updated: atomic Result to publish completion ordering between DPU threads.
    DDSAtomicErrorCodeT Result;
    FileIOSizeT BytesServiced;
} BuffMsgB2FAckHeader;
*/

// jason: Updated response header includes logical bytes in addition to output bytes.
typedef struct
{
    RequestIdT RequestId;
    // ErrorCodeT Result;
    // Updated: atomic Result to publish completion ordering between DPU threads.
    DDSAtomicErrorCodeT Result;
    // BytesServiced == output bytes placed into the app buffer.
    FileIOSizeT BytesServiced;
    // LogicalBytes == logical bytes read (EOF-clamped).
    FileIOSizeT LogicalBytes;
#if DDS_OFFLOAD_STAGE_TIMING_ENABLED
    // Offloaded stage timing (nanoseconds) for read2.
    uint64_t OffloadReadTimeNs;
    uint64_t OffloadStage1TimeNs;
    uint64_t OffloadStage2TimeNs;
#endif
} BuffMsgB2FAckHeader;

typedef BuffMsgF2BReqHeader OffloadWorkRequest;
typedef BuffMsgB2FAckHeader OffloadWorkResponse;

//
// Check a few parameters at the compile time
//
//
#define AssertStaticMsgTypes(e, num)                                                                                   \
    enum                                                                                                               \
    {                                                                                                                  \
        AssertStaticMsgTypes__##num = 1 / (e)                                                                          \
    }

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
#else
#pragma warning(push)
#pragma warning(disable : 4804)
#endif
//
// Ring space is allocated at the |size of a request/response| + |header size|
// The alignment is enforced once a request/response is inserted into the ring
//
//
// jason: We align request publication to the header's natural alignment (not header size).
AssertStaticMsgTypes(DDS_REQUEST_RING_HEADER_PHASE < DDS_ALIGNOF(BuffMsgF2BReqHeader), 0);
AssertStaticMsgTypes((DDS_REQUEST_RING_HEADER_PHASE + sizeof(FileIOSizeT)) % DDS_ALIGNOF(BuffMsgF2BReqHeader) == 0, 1);
AssertStaticMsgTypes(DDS_CACHE_LINE_SIZE % DDS_ALIGNOF(BuffMsgF2BReqHeader) == 0, 2);
AssertStaticMsgTypes(DDS_REQUEST_RING_BYTES % DDS_ALIGNOF(BuffMsgF2BReqHeader) == 0, 3);
AssertStaticMsgTypes(DDS_CACHE_LINE_SIZE % DDS_ALIGNOF(BuffMsgB2FAckHeader) == 0, 4);
AssertStaticMsgTypes(DDS_RESPONSE_RING_BYTES % DDS_ALIGNOF(BuffMsgB2FAckHeader) == 0, 5);
AssertStaticMsgTypes(sizeof(DDSAtomicErrorCodeT) == sizeof(ErrorCodeT), 6);
#ifdef __GNUC__
#pragma GCC diagnostic pop
#else
#pragma warning(pop)
#endif
