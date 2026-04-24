#pragma once

#include <DDSFrontEnd.h>
#include <mutex>

#define MAX_DDS_FILE_HANDLES 1024
#define MAX_DDS_POLLS 1024

#ifndef _WINDOWS_
#define DUMMYSTRUCTNAME
#define DUMMYUNIONNAME
#define ERROR_SUCCESS			0x00000000
#define ERROR_DISK_FULL			0x00000070
#define ERROR_SEEK_ON_DEVICE	0x00000084
#define ERROR_IO_PENDING		0x000003E5
#define CREATE_NEW			1
#define CREATE_ALWAYS		2
#define OPEN_EXISTING		3
#define OPEN_ALWAYS			4
#define TRUNCATE_EXISTING	5
#define STATUS_PENDING                   ((DWORD   )0x00000103L)
#define STATUS_SUCCESS                   ((DWORD   )0x00000000L)

//
// Types required to implement WinAPI functions
//
//
typedef int BOOL;
typedef unsigned long DWORD;
typedef void* HANDLE;
typedef long LONG;
typedef long long LONGLONG;
typedef unsigned long long ULONGLONG;
typedef ULONGLONG DWORDLONG;
typedef __int64 INT_PTR, * PINT_PTR;
typedef unsigned __int64 UINT_PTR, * PUINT_PTR;
typedef __int64 LONG_PTR, * PLONG_PTR;
typedef unsigned __int64 ULONG_PTR, * PULONG_PTR;
typedef const char* LPCSTR;
typedef const wchar_t* LPCWSTR;
typedef const void* LPCVOID;
typedef DWORD* LPDWORD;
typedef void* LPVOID;
typedef void* PVOID;
typedef void* PVOID64;
typedef struct _SECURITY_ATTRIBUTES {
	DWORD nLength;
	LPVOID lpSecurityDescriptor;
	BOOL bInheritHandle;
} SECURITY_ATTRIBUTES, * PSECURITY_ATTRIBUTES, * LPSECURITY_ATTRIBUTES;

typedef struct _OVERLAPPED {
	ULONG_PTR Internal;
	ULONG_PTR InternalHigh;
	union {
		struct {
			DWORD Offset;
			DWORD OffsetHigh;
		} DUMMYSTRUCTNAME;
		PVOID Pointer;
	} DUMMYUNIONNAME;

	HANDLE  hEvent;
} OVERLAPPED, * LPOVERLAPPED;

typedef union _FILE_SEGMENT_ELEMENT {
	PVOID64 Buffer;
	ULONGLONG Alignment;
}FILE_SEGMENT_ELEMENT, * PFILE_SEGMENT_ELEMENT;

typedef union _LARGE_INTEGER {
	struct {
		DWORD LowPart;
		LONG HighPart;
	} DUMMYSTRUCTNAME;
	struct {
		DWORD LowPart;
		LONG HighPart;
	} u;
	LONGLONG QuadPart;
} LARGE_INTEGER;

typedef LARGE_INTEGER* PLARGE_INTEGER;

#define INVALID_HANDLE_VALUE ((HANDLE)(UINT_PTR)-1)
#define NO_ERROR 0
#endif

class DDSFrontEndForSQL {
private:
	const char* FILE_TO_INTERCEPT = "C:\\Socrates\\PageServer\\data\\rbpex.bpe";
	const HANDLE HANDLE_BASE = (HANDLE)0xFFFF00;
	const int WIN32_FILE_HANDLE_OFFSET = 2;
	size_t HandleCounter;
	HANDLE Handles[MAX_DDS_FILE_HANDLES];
	std::mutex HandleMutex;

	const HANDLE POLL_BASE = (HANDLE)0xFF00;
	HANDLE Polls[MAX_DDS_POLLS];
	size_t PollCounter;

	//
	// DDS front end
	//
	//
	const char* DDS_STORE_NAME = "DDS-StoreForSQL";
	DDS_FrontEnd::DDSFrontEnd Store;
	bool DDSInitialized = false;

public:
	DDSFrontEndForSQL();
	~DDSFrontEndForSQL();

	//
	// Check if this file should be intercepted by DDS
	//
	//
	bool
	Intercept(
		const char* FileName
	);

	//
	// Check if this file handle was created by DDS
	//
	//
	bool
	Intercept(
		HANDLE FileHandle
	);

	//
	// Create a DDS file
	//
	//
	HANDLE
	DDSCreateFile(
		_In_ LPCSTR lpFileName,
		_In_ DWORD dwDesiredAccess,
		_In_ DWORD dwShareMode,
		_In_opt_ LPSECURITY_ATTRIBUTES lpSecurityAttributes,
		_In_ DWORD dwCreationDisposition,
		_In_ DWORD dwFlagsAndAttributes,
		_In_opt_ HANDLE hTemplateFile
	);

	//
	// Write to a DDS file through a gather list
	//
	//
	BOOL
	DDSWriteFileGather(
		_In_ HANDLE hFile,
		_In_ FILE_SEGMENT_ELEMENT aSegmentArray[],
		_In_ DWORD nNumberOfBytesToWrite,
		_Reserved_ LPDWORD lpReserved,
		_Inout_ LPOVERLAPPED lpOverlapped,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Write to a DDS file through a gather list, synchronously
	//
	//
	BOOL
	DDSWriteFileGatherSync(
		_In_ HANDLE hFile,
		_In_ FILE_SEGMENT_ELEMENT aSegmentArray[],
		_In_ DWORD nNumberOfBytesToWrite,
		_Reserved_ LPDWORD lpReserved,
		_Inout_ LPOVERLAPPED lpOverlapped,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Write to a DDS file
	//
	//
	BOOL
	DDSWriteFile(
		_In_ HANDLE hFile,
		_In_reads_bytes_opt_(nNumberOfBytesToWrite) LPCVOID lpBuffer,
		_In_ DWORD nNumberOfBytesToWrite,
		_Out_opt_ LPDWORD lpNumberOfBytesWritten,
		_Inout_opt_ LPOVERLAPPED lpOverlapped,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Write to a DDS file, synchronously
	//
	//
	BOOL
	DDSWriteFileSync(
		_In_ HANDLE hFile,
		_In_reads_bytes_opt_(nNumberOfBytesToWrite) LPCVOID lpBuffer,
		_In_ DWORD nNumberOfBytesToWrite,
		_Out_opt_ LPDWORD lpNumberOfBytesWritten,
		_Inout_opt_ LPOVERLAPPED lpOverlapped,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Read from a DDS file with scattering
	//
	//
	BOOL
	DDSReadFileScatter(
		_In_ HANDLE hFile,
		_In_ FILE_SEGMENT_ELEMENT aSegmentArray[],
		_In_ DWORD nNumberOfBytesToRead,
		_Reserved_ LPDWORD lpReserved,
		_Inout_ LPOVERLAPPED lpOverlapped,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Read from a DDS file with scattering, synchronously
	//
	//
	BOOL
	DDSReadFileScatterSync(
		_In_ HANDLE hFile,
		_In_ FILE_SEGMENT_ELEMENT aSegmentArray[],
		_In_ DWORD nNumberOfBytesToRead,
		_Reserved_ LPDWORD lpReserved,
		_Inout_ LPOVERLAPPED lpOverlapped,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Read from a DDS file
	//
	//
	BOOL
	DDSReadFile(
		_In_ HANDLE hFile,
		_Out_writes_bytes_to_opt_(nNumberOfBytesToRead, *lpNumberOfBytesRead) LPVOID lpBuffer,
		_In_ DWORD nNumberOfBytesToRead,
		_Out_opt_ LPDWORD lpNumberOfBytesRead,
		_Inout_opt_ LPOVERLAPPED lpOverlapped,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Read from a DDS file, synchronously
	//
	//
	BOOL
	DDSReadFileSync(
		_In_ HANDLE hFile,
		_Out_writes_bytes_to_opt_(nNumberOfBytesToRead, *lpNumberOfBytesRead) LPVOID lpBuffer,
		_In_ DWORD nNumberOfBytesToRead,
		_Out_opt_ LPDWORD lpNumberOfBytesRead,
		_Inout_opt_ LPOVERLAPPED lpOverlapped,
		_Out_ LPDWORD pErrorCode
	);
	
	//
	// Get the size of a file
	//
	//
	BOOL
	DDSGetFileSizeEx(
		_In_ HANDLE hFile,
		_Out_ PLARGE_INTEGER lpFileSize,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Change the size of a file
	//
	//
	DWORD
	DDSChangeFileSize(
		_In_ HANDLE fileHandle,
		_In_ DWORDLONG newSize
	);

	//
	// Set the valid data size
	//
	//
	BOOL
	DDSSetFileValidData(
		_In_ HANDLE   hFile,
		_In_ LONGLONG ValidDataLength,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Set delete on close
	//
	//
	BOOL
	DDSSetFileDeleteOnClose(
		_In_ HANDLE   hFile,
		_In_ bool DeleteOnClose,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Create a poll strucuture for async I/O
	//
	//
	HANDLE
	DDSPollCreate();

	//
	// Delete a poll strucuture
	//
	//
	BOOL
	DDSPollDelete(
		_In_ HANDLE hPoll,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Add a file handle to a poll
	//
	//
	BOOL
	DDSPollAdd(
		_In_ HANDLE hFile,
		_In_ HANDLE hPoll,
		_In_ ULONG_PTR pCompletionKey,
		_Out_ LPDWORD pErrorCode
	);

	//
	// Create a poll strucuture for async I/O
	//
	//
	BOOL
	DDSPollWait(
		_In_ HANDLE hPoll,
		_Out_ LPDWORD lpNumberOfBytesTransferred,
		_Out_ PULONG_PTR lpCompletionKey,
		_Out_ LPOVERLAPPED* lpOverlapped,
		_In_ DWORD dwMilliseconds,
		_Out_ LPDWORD pErrorCode
	);

	void
	CheckAndAdd(
		const char* FileName,
		HANDLE FileHandle
	);

	bool
	SearchHandle(
		HANDLE FileHandle
	);

	size_t
	TotalHandles();
};

extern DDSFrontEndForSQL DDSFrontEnd;