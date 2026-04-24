#include "pch.h"

#include <iostream>
#include <string.h>

#include "DDSFrontEndForSQL.h"

#pragma warning (disable : 6011)

DDSFrontEndForSQL::DDSFrontEndForSQL() : Store(DDS_STORE_NAME) {
	HandleCounter = 0;
	for (size_t i = 0; i != MAX_DDS_FILE_HANDLES; i++) {
		Handles[i] = 0;
	}

	PollCounter = 0;
	for (size_t i = 0; i != MAX_DDS_POLLS; i++) {
		Polls[i] = 0;
	}
}

DDSFrontEndForSQL::~DDSFrontEndForSQL() { }

//
// Check if this file should be intercepted by DDS
//
//
bool
DDSFrontEndForSQL::Intercept(
	const char* FileName
) {
	printf("DDS: Intercepting %s...\n", FileName);
	bool result = strcmp(FileName, FILE_TO_INTERCEPT) == 0;
	return result;
	// return false;
}

//
// Check if this file handle was created by DDS
//
//
bool
DDSFrontEndForSQL::Intercept(
	HANDLE FileHandle
) {
	bool result = SearchHandle(FileHandle);
	// printf("DDS: Intercepting %p = %d ...\n", FileHandle, result);
	return result;
}

//
// Create a DDS file
//
//
HANDLE
DDSFrontEndForSQL::DDSCreateFile(
	_In_ LPCSTR lpFileName,
	_In_ DWORD dwDesiredAccess,
	_In_ DWORD dwShareMode,
	_In_opt_ LPSECURITY_ATTRIBUTES lpSecurityAttributes,
	_In_ DWORD dwCreationDisposition,
	_In_ DWORD dwFlagsAndAttributes,
	_In_opt_ HANDLE hTemplateFile
) {
	printf("DDS (%p): Creating file %s...\n", this, lpFileName);
	DDS_FrontEnd::FileIdT fileId = 0;
	DDS_FrontEnd::ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
	HANDLE fileHandle = INVALID_HANDLE_VALUE;

	if (!DDSInitialized) {
		result = Store.Initialize();
		if (result != DDS_ERROR_CODE_SUCCESS) {
			printf("DDS: Failed to initialize\n");
			return fileHandle;
		}
		else {
			printf("DDS: Initialized\n");
			DDSInitialized = true;
		}
	}

	//
	// If creating a new file, invoke DDS for creation;
	// otherwise, reuse the previously created file
	//
	//
	if (dwCreationDisposition == CREATE_NEW || dwCreationDisposition == CREATE_ALWAYS) {
		result = Store.CreateFile(
			lpFileName,
			(DDS_FrontEnd::FileAccessT)dwDesiredAccess,
			(DDS_FrontEnd::FileShareModeT)dwShareMode,
			(DDS_FrontEnd::FileAttributesT)dwFlagsAndAttributes,
			&fileId
		);

		printf("DDS Test: Searching after creation...\n");
		DDS_FrontEnd::FileIdT tmpFileId;
		Store.FindFirstFile(lpFileName, &tmpFileId);

		if (result != DDS_ERROR_CODE_SUCCESS) {
			return INVALID_HANDLE_VALUE;
		}

		if (HandleCounter == MAX_DDS_FILE_HANDLES) {
			std::cout << "DDS: Out of files" << std::endl;
			return INVALID_HANDLE_VALUE;
		}

		fileHandle = (HANDLE)((size_t)HANDLE_BASE + ((size_t)fileId << WIN32_FILE_HANDLE_OFFSET));
		Handles[HandleCounter] = fileHandle;
		HandleCounter++;
	}
	else {
		result = Store.FindFirstFile(lpFileName, &fileId);
		if (result != DDS_ERROR_CODE_SUCCESS) {
			std::cout << "DDS: Failed to find the file with the same name" << std::endl;
		}
		else {
			fileHandle = (HANDLE)((size_t)HANDLE_BASE + ((size_t)fileId << WIN32_FILE_HANDLE_OFFSET));
		}
	}

	return fileHandle;
}

//
// Write to a DDS file through a gather list
//
//
BOOL
DDSFrontEndForSQL::DDSWriteFileGather(
	_In_ HANDLE hFile,
	_In_ FILE_SEGMENT_ELEMENT aSegmentArray[],
	_In_ DWORD nNumberOfBytesToWrite,
	_Reserved_ LPDWORD lpReserved,
	_Inout_ LPOVERLAPPED lpOverlapped,
	_Out_ LPDWORD pErrorCode
) {
	// printf("DDS: Writing file gather %p...\n", hFile);
	DDS_FrontEnd::ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
	if (pErrorCode) {
		*pErrorCode = ERROR_SUCCESS;
	}

	DDS_FrontEnd::FileIdT fileId = (DDS_FrontEnd::FileIdT)((size_t)hFile - (size_t)HANDLE_BASE);
	DDS_FrontEnd::FileSizeT offset = (DDS_FrontEnd::FileSizeT)(((DWORDLONG)(lpOverlapped->OffsetHigh) << 32) + (DWORDLONG)(lpOverlapped->Offset));

	result = Store.SetFilePointer(fileId, offset, DDS_FrontEnd::FilePointerPosition::BEGIN);
	if (DDS_ERROR_CODE_SUCCESS != result) {
		printf("DDS: Failed to set file pointer at offset %llu\n", offset);
		if (pErrorCode) {
			*pErrorCode = ERROR_SEEK_ON_DEVICE;
		}
		return false;
	}

	//
	// Set the internal error codes for the overlapped
	//
	//
	(lpOverlapped)->Internal = (ULONG_PTR)STATUS_PENDING;
	(lpOverlapped)->InternalHigh = (ULONG_PTR)0;
	
	result = Store.WriteFileGather(
		fileId,
		(DDS_FrontEnd::BufferT*)aSegmentArray,
		nNumberOfBytesToWrite,
		NULL,
		NULL,
		lpOverlapped
	);

	while (result == DDS_ERROR_CODE_REQUIRE_POLLING) {
		result = Store.WriteFileGather(
			fileId,
			(DDS_FrontEnd::BufferT*)aSegmentArray,
			nNumberOfBytesToWrite,
			NULL,
			NULL,
			lpOverlapped
		);
	}

	if (DDS_ERROR_CODE_SUCCESS == result) {
		//
		// Set the internal error codes for the overlapped
		//
		//
		(lpOverlapped)->Internal = (ULONG_PTR)STATUS_SUCCESS;
		(lpOverlapped)->InternalHigh = (ULONG_PTR)nNumberOfBytesToWrite;
		return true;
	}
	
	if (DDS_ERROR_CODE_IO_PENDING == result) {
		if (pErrorCode) {
			*pErrorCode = ERROR_IO_PENDING;
		}
	}

	return false;
}

//
// Write to a DDS file through a gather list, synchronously
//
//
BOOL
DDSFrontEndForSQL::DDSWriteFileGatherSync(
	_In_ HANDLE hFile,
	_In_ FILE_SEGMENT_ELEMENT aSegmentArray[],
	_In_ DWORD nNumberOfBytesToWrite,
	_Reserved_ LPDWORD lpReserved,
	_Inout_ LPOVERLAPPED lpOverlapped,
	_Out_ LPDWORD pErrorCode
) {
	// printf("DDS: Writing file gather %p...\n", hFile);
	DDS_FrontEnd::ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
	if (pErrorCode) {
		*pErrorCode = ERROR_SUCCESS;
	}

	DDS_FrontEnd::FileIdT fileId = (DDS_FrontEnd::FileIdT)((size_t)hFile - (size_t)HANDLE_BASE);
	DDS_FrontEnd::FileSizeT offset = (DDS_FrontEnd::FileSizeT)(((DWORDLONG)(lpOverlapped->OffsetHigh) << 32) + (DWORDLONG)(lpOverlapped->Offset));

	result = Store.SetFilePointer(fileId, offset, DDS_FrontEnd::FilePointerPosition::BEGIN);
	if (DDS_ERROR_CODE_SUCCESS != result) {
		printf("DDS: Failed to set file pointer at offset %llu\n", offset);
		if (pErrorCode) {
			*pErrorCode = ERROR_SEEK_ON_DEVICE;
		}
		return false;
	}

	//
	// Set the internal error codes for the overlapped
	//
	//
	(lpOverlapped)->Internal = (ULONG_PTR)STATUS_PENDING;
	(lpOverlapped)->InternalHigh = (ULONG_PTR)0;

	result = Store.WriteFileGather(
		fileId,
		(DDS_FrontEnd::BufferT*)aSegmentArray,
		nNumberOfBytesToWrite,
		NULL,
		DDS_FrontEnd::DummyReadWriteCallback,
		lpOverlapped
	);

	while (result == DDS_ERROR_CODE_REQUIRE_POLLING) {
		result = Store.WriteFileGather(
			fileId,
			(DDS_FrontEnd::BufferT*)aSegmentArray,
			nNumberOfBytesToWrite,
			NULL,
			DDS_FrontEnd::DummyReadWriteCallback,
			lpOverlapped
		);
	}

	if (DDS_ERROR_CODE_SUCCESS == result) {
		//
		// Set the internal error codes for the overlapped
		//
		//
		(lpOverlapped)->Internal = (ULONG_PTR)STATUS_SUCCESS;
		(lpOverlapped)->InternalHigh = (ULONG_PTR)nNumberOfBytesToWrite;
		return true;
	}

	if (DDS_ERROR_CODE_IO_PENDING == result) {
		if (pErrorCode) {
			*pErrorCode = ERROR_IO_PENDING;
		}
	}

	return false;
}

//
// Write to a DDS file
//
//
BOOL
DDSFrontEndForSQL::DDSWriteFile(
	_In_ HANDLE hFile,
	_In_reads_bytes_opt_(nNumberOfBytesToWrite) LPCVOID lpBuffer,
	_In_ DWORD nNumberOfBytesToWrite,
	_Out_opt_ LPDWORD lpNumberOfBytesWritten,
	_Inout_opt_ LPOVERLAPPED lpOverlapped,
	_Out_ LPDWORD pErrorCode
) {
	// DWORDLONG Offset = (DWORDLONG)(lpOverlapped->OffsetHigh) << 32 + (DWORDLONG)(lpOverlapped->Offset);
	// printf("DDS: Writing file %p at %llu with %lu bytes...\n", hFile, Offset, nNumberOfBytesToWrite);

	DDS_FrontEnd::ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
	if (pErrorCode) {
		*pErrorCode = ERROR_SUCCESS;
	}
	if (lpNumberOfBytesWritten != NULL) {
		*lpNumberOfBytesWritten = 0;
	}

	DDS_FrontEnd::FileIdT fileId = (DDS_FrontEnd::FileIdT)((size_t)hFile - (size_t)HANDLE_BASE);
	DDS_FrontEnd::FileSizeT offset = (DDS_FrontEnd::FileSizeT)(((DWORDLONG)(lpOverlapped->OffsetHigh) << 32) + (DWORDLONG)(lpOverlapped->Offset));

	result = Store.SetFilePointer(fileId, offset, DDS_FrontEnd::FilePointerPosition::BEGIN);
	if (DDS_ERROR_CODE_SUCCESS != result) {
		printf("DDS: Failed to set file pointer\n");
		if (pErrorCode) {
			*pErrorCode = ERROR_SEEK_ON_DEVICE;
		}
		return false;
	}

	//
	// Set the internal error codes for the overlapped
	//
	//
	(lpOverlapped)->Internal = (ULONG_PTR)STATUS_PENDING;
	(lpOverlapped)->InternalHigh = (ULONG_PTR)0;

	result = Store.WriteFile(
		fileId,
		(DDS_FrontEnd::BufferT)lpBuffer,
		nNumberOfBytesToWrite,
		lpNumberOfBytesWritten,
		// DDS_FrontEnd::DummyReadWriteCallback,
		NULL,
		lpOverlapped
	);

	while (result == DDS_ERROR_CODE_REQUIRE_POLLING) {
		result = Store.WriteFile(
			fileId,
			(DDS_FrontEnd::BufferT)lpBuffer,
			nNumberOfBytesToWrite,
			lpNumberOfBytesWritten,
			NULL,
			lpOverlapped
		);
	}

	if (DDS_ERROR_CODE_SUCCESS == result) {
		//
		// Set the internal error codes for the overlapped
		//
		//
		(lpOverlapped)->Internal = (ULONG_PTR)STATUS_SUCCESS;
		(lpOverlapped)->InternalHigh = (ULONG_PTR)nNumberOfBytesToWrite;
		return true;
	}

	if (DDS_ERROR_CODE_IO_PENDING == result) {
		if (pErrorCode) {
			*pErrorCode = ERROR_IO_PENDING;
		}
	}

	return false;
}

//
// Write to a DDS file, synchronously
//
//
BOOL
DDSFrontEndForSQL::DDSWriteFileSync(
	_In_ HANDLE hFile,
	_In_reads_bytes_opt_(nNumberOfBytesToWrite) LPCVOID lpBuffer,
	_In_ DWORD nNumberOfBytesToWrite,
	_Out_opt_ LPDWORD lpNumberOfBytesWritten,
	_Inout_opt_ LPOVERLAPPED lpOverlapped,
	_Out_ LPDWORD pErrorCode
) {
	// DWORDLONG Offset = (DWORDLONG)(lpOverlapped->OffsetHigh) << 32 + (DWORDLONG)(lpOverlapped->Offset);
	// printf("DDS: Writing file %p at %llu with %lu bytes...\n", hFile, Offset, nNumberOfBytesToWrite);
	DDS_FrontEnd::ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
	if (pErrorCode) {
		*pErrorCode = ERROR_SUCCESS;
	}
	if (lpNumberOfBytesWritten != NULL) {
		*lpNumberOfBytesWritten = 0;
	}

	DDS_FrontEnd::FileIdT fileId = (DDS_FrontEnd::FileIdT)((size_t)hFile - (size_t)HANDLE_BASE);
	DDS_FrontEnd::FileSizeT offset = (DDS_FrontEnd::FileSizeT)(((DWORDLONG)(lpOverlapped->OffsetHigh) << 32) + (DWORDLONG)(lpOverlapped->Offset));

	result = Store.SetFilePointer(fileId, offset, DDS_FrontEnd::FilePointerPosition::BEGIN);
	if (DDS_ERROR_CODE_SUCCESS != result) {
		printf("DDS: Failed to set file pointer\n");
		if (pErrorCode) {
			*pErrorCode = ERROR_SEEK_ON_DEVICE;
		}
		return false;
	}

	//
	// Set the internal error codes for the overlapped
	//
	//
	(lpOverlapped)->Internal = (ULONG_PTR)STATUS_PENDING;
	(lpOverlapped)->InternalHigh = (ULONG_PTR)0;

	result = Store.WriteFile(
		fileId,
		(DDS_FrontEnd::BufferT)lpBuffer,
		nNumberOfBytesToWrite,
		lpNumberOfBytesWritten,
		DDS_FrontEnd::DummyReadWriteCallback,
		lpOverlapped
	);

	while (result == DDS_ERROR_CODE_REQUIRE_POLLING) {
		result = Store.WriteFile(
			fileId,
			(DDS_FrontEnd::BufferT)lpBuffer,
			nNumberOfBytesToWrite,
			lpNumberOfBytesWritten,
			DDS_FrontEnd::DummyReadWriteCallback,
			lpOverlapped
		);
	}

	if (DDS_ERROR_CODE_SUCCESS == result) {
		//
		// Set the internal error codes for the overlapped
		//
		//
		(lpOverlapped)->Internal = (ULONG_PTR)STATUS_SUCCESS;
		(lpOverlapped)->InternalHigh = (ULONG_PTR)nNumberOfBytesToWrite;
		return true;
	}

	if (DDS_ERROR_CODE_IO_PENDING == result) {
		if (pErrorCode) {
			*pErrorCode = ERROR_IO_PENDING;
		}
	}

	return false;
}

//
// Write to a DDS file through a gather list
//
//
BOOL
DDSFrontEndForSQL::DDSReadFileScatter(
	_In_ HANDLE hFile,
	_In_ FILE_SEGMENT_ELEMENT aSegmentArray[],
	_In_ DWORD nNumberOfBytesToRead,
	_Reserved_ LPDWORD lpReserved,
	_Inout_ LPOVERLAPPED lpOverlapped,
	_Out_ LPDWORD pErrorCode
) {
	// printf("DDS: Reading file scatter %p...\n", hFile);
	DDS_FrontEnd::ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
	if (pErrorCode) {
		*pErrorCode = ERROR_SUCCESS;
	}

	DDS_FrontEnd::FileIdT fileId = (DDS_FrontEnd::FileIdT)((size_t)hFile - (size_t)HANDLE_BASE);
	DDS_FrontEnd::FileSizeT offset = (DDS_FrontEnd::FileSizeT)(((DWORDLONG)(lpOverlapped->OffsetHigh) << 32) + (DWORDLONG)(lpOverlapped->Offset));

	result = Store.SetFilePointer(fileId, offset, DDS_FrontEnd::FilePointerPosition::BEGIN);
	if (DDS_ERROR_CODE_SUCCESS != result) {
		printf("DDS: Failed to set file pointer at offset %llu\n", offset);
		if (pErrorCode) {
			*pErrorCode = ERROR_SEEK_ON_DEVICE;
		}
		return false;
	}

	//
	// Set the internal error codes for the overlapped
	//
	//
	(lpOverlapped)->Internal = (ULONG_PTR)STATUS_PENDING;
	(lpOverlapped)->InternalHigh = (ULONG_PTR)0;

	result = Store.ReadFileScatter(
		fileId,
		(DDS_FrontEnd::BufferT*)aSegmentArray,
		nNumberOfBytesToRead,
		NULL,
		NULL,
		lpOverlapped
	);

	while (result == DDS_ERROR_CODE_REQUIRE_POLLING) {
		result = Store.ReadFileScatter(
			fileId,
			(DDS_FrontEnd::BufferT*)aSegmentArray,
			nNumberOfBytesToRead,
			NULL,
			NULL,
			lpOverlapped
		);
	}

	if (DDS_ERROR_CODE_SUCCESS == result) {
		//
		// Set the internal error codes for the overlapped
		//
		//
		(lpOverlapped)->Internal = (ULONG_PTR)STATUS_SUCCESS;
		(lpOverlapped)->InternalHigh = (ULONG_PTR)nNumberOfBytesToRead;
		return true;
	}

	if (DDS_ERROR_CODE_IO_PENDING == result) {
		if (pErrorCode) {
			*pErrorCode = ERROR_IO_PENDING;
		}
	}

	return false;
}

//
// Write to a DDS file through a gather list, synchronously
//
//
BOOL
DDSFrontEndForSQL::DDSReadFileScatterSync(
	_In_ HANDLE hFile,
	_In_ FILE_SEGMENT_ELEMENT aSegmentArray[],
	_In_ DWORD nNumberOfBytesToRead,
	_Reserved_ LPDWORD lpReserved,
	_Inout_ LPOVERLAPPED lpOverlapped,
	_Out_ LPDWORD pErrorCode
) {
	// printf("DDS: Reading file scatter sync %p...\n", hFile);
	DDS_FrontEnd::ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
	if (pErrorCode) {
		*pErrorCode = ERROR_SUCCESS;
	}

	DDS_FrontEnd::FileIdT fileId = (DDS_FrontEnd::FileIdT)((size_t)hFile - (size_t)HANDLE_BASE);
	DDS_FrontEnd::FileSizeT offset = (DDS_FrontEnd::FileSizeT)(((DWORDLONG)(lpOverlapped->OffsetHigh) << 32) + (DWORDLONG)(lpOverlapped->Offset));

	result = Store.SetFilePointer(fileId, offset, DDS_FrontEnd::FilePointerPosition::BEGIN);
	if (DDS_ERROR_CODE_SUCCESS != result) {
		printf("DDS: Failed to set file pointer at offset %llu\n", offset);
		if (pErrorCode) {
			*pErrorCode = ERROR_SEEK_ON_DEVICE;
		}
		return false;
	}

	//
	// Set the internal error codes for the overlapped
	//
	//
	(lpOverlapped)->Internal = (ULONG_PTR)STATUS_PENDING;
	(lpOverlapped)->InternalHigh = (ULONG_PTR)0;

	result = Store.ReadFileScatter(
		fileId,
		(DDS_FrontEnd::BufferT*)aSegmentArray,
		nNumberOfBytesToRead,
		NULL,
		DDS_FrontEnd::DummyReadWriteCallback,
		lpOverlapped
	);

	while (result == DDS_ERROR_CODE_REQUIRE_POLLING) {
		result = Store.ReadFileScatter(
			fileId,
			(DDS_FrontEnd::BufferT*)aSegmentArray,
			nNumberOfBytesToRead,
			NULL,
			DDS_FrontEnd::DummyReadWriteCallback,
			lpOverlapped
		);
	}

	if (DDS_ERROR_CODE_SUCCESS == result) {
		//
		// Set the internal error codes for the overlapped
		//
		//
		(lpOverlapped)->Internal = (ULONG_PTR)STATUS_SUCCESS;
		(lpOverlapped)->InternalHigh = (ULONG_PTR)nNumberOfBytesToRead;
		return true;
	}

	if (DDS_ERROR_CODE_IO_PENDING == result) {
		if (pErrorCode) {
			*pErrorCode = ERROR_IO_PENDING;
		}
	}

	return false;
}

//
// Read from a DDS file
//
//
BOOL
DDSFrontEndForSQL::DDSReadFile(
	_In_ HANDLE hFile,
	_Out_writes_bytes_to_opt_(nNumberOfBytesToRead, *lpNumberOfBytesRead) LPVOID lpBuffer,
	_In_ DWORD nNumberOfBytesToRead,
	_Out_opt_ LPDWORD lpNumberOfBytesRead,
	_Inout_opt_ LPOVERLAPPED lpOverlapped,
	_Out_ LPDWORD pErrorCode
) {
	DDS_FrontEnd::ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
	if (pErrorCode) {
		*pErrorCode = ERROR_SUCCESS;
	}
	if (lpNumberOfBytesRead != NULL) {
		*lpNumberOfBytesRead = 0;
	}

	DDS_FrontEnd::FileIdT fileId = (DDS_FrontEnd::FileIdT)((size_t)hFile - (size_t)HANDLE_BASE);
	DDS_FrontEnd::FileSizeT offset = (DDS_FrontEnd::FileSizeT)(((DWORDLONG)(lpOverlapped->OffsetHigh) << 32) + (DWORDLONG)(lpOverlapped->Offset));

	result = Store.SetFilePointer(fileId, offset, DDS_FrontEnd::FilePointerPosition::BEGIN);
	if (DDS_ERROR_CODE_SUCCESS != result) {
		printf("DDS: Failed to set file pointer\n");
		if (pErrorCode) {
			*pErrorCode = ERROR_SEEK_ON_DEVICE;
		}
		return false;
	}

	//
	// Set the internal error codes for the overlapped
	//
	//
	(lpOverlapped)->Internal = (ULONG_PTR)STATUS_PENDING;
	(lpOverlapped)->InternalHigh = (ULONG_PTR)0;

	result = Store.ReadFile(
		fileId,
		(DDS_FrontEnd::BufferT)lpBuffer,
		nNumberOfBytesToRead,
		lpNumberOfBytesRead,
		NULL,
		lpOverlapped
	);

	while (result == DDS_ERROR_CODE_REQUIRE_POLLING) {
		result = Store.ReadFile(
			fileId,
			(DDS_FrontEnd::BufferT)lpBuffer,
			nNumberOfBytesToRead,
			lpNumberOfBytesRead,
			NULL,
			lpOverlapped
		);
	}

	if (DDS_ERROR_CODE_SUCCESS == result) {
		//
		// Set the internal error codes for the overlapped
		//
		//
		(lpOverlapped)->Internal = (ULONG_PTR)STATUS_SUCCESS;
		(lpOverlapped)->InternalHigh = (ULONG_PTR)nNumberOfBytesToRead;
		return true;
	}

	if (DDS_ERROR_CODE_IO_PENDING == result) {
		if (pErrorCode) {
			*pErrorCode = ERROR_IO_PENDING;
		}
	}

	return false;
}

//
// Read from a DDS file, synchronously
//
//
BOOL
DDSFrontEndForSQL::DDSReadFileSync(
	_In_ HANDLE hFile,
	_Out_writes_bytes_to_opt_(nNumberOfBytesToRead, *lpNumberOfBytesRead) LPVOID lpBuffer,
	_In_ DWORD nNumberOfBytesToRead,
	_Out_opt_ LPDWORD lpNumberOfBytesRead,
	_Inout_opt_ LPOVERLAPPED lpOverlapped,
	_Out_ LPDWORD pErrorCode
) {
	DDS_FrontEnd::ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;
	if (pErrorCode) {
		*pErrorCode = ERROR_SUCCESS;
	}
	if (lpNumberOfBytesRead != NULL) {
		*lpNumberOfBytesRead = 0;
	}

	DDS_FrontEnd::FileIdT fileId = (DDS_FrontEnd::FileIdT)((size_t)hFile - (size_t)HANDLE_BASE);
	DDS_FrontEnd::FileSizeT offset = (DDS_FrontEnd::FileSizeT)(((DWORDLONG)(lpOverlapped->OffsetHigh) << 32) + (DWORDLONG)(lpOverlapped->Offset));

	result = Store.SetFilePointer(fileId, offset, DDS_FrontEnd::FilePointerPosition::BEGIN);
	if (DDS_ERROR_CODE_SUCCESS != result) {
		printf("DDS: Failed to set file pointer\n");
		if (pErrorCode) {
			*pErrorCode = ERROR_SEEK_ON_DEVICE;
		}
		return false;
	}

	//
	// Set the internal error codes for the overlapped
	//
	//
	(lpOverlapped)->Internal = (ULONG_PTR)STATUS_PENDING;
	(lpOverlapped)->InternalHigh = (ULONG_PTR)0;

	result = Store.ReadFile(
		fileId,
		(DDS_FrontEnd::BufferT)lpBuffer,
		nNumberOfBytesToRead,
		lpNumberOfBytesRead,
		DDS_FrontEnd::DummyReadWriteCallback,
		lpOverlapped
	);

	while (result == DDS_ERROR_CODE_REQUIRE_POLLING) {
		result = Store.ReadFile(
			fileId,
			(DDS_FrontEnd::BufferT)lpBuffer,
			nNumberOfBytesToRead,
			lpNumberOfBytesRead,
			DDS_FrontEnd::DummyReadWriteCallback,
			lpOverlapped
		);
	}

	if (DDS_ERROR_CODE_SUCCESS == result) {
		//
		// Set the internal error codes for the overlapped
		//
		//
		(lpOverlapped)->Internal = (ULONG_PTR)STATUS_SUCCESS;
		(lpOverlapped)->InternalHigh = (ULONG_PTR)nNumberOfBytesToRead;
		return true;
	}

	if (DDS_ERROR_CODE_IO_PENDING == result) {
		if (pErrorCode) {
			*pErrorCode = ERROR_IO_PENDING;
		}
	}

	return false;
}

//
// Get the size of a file
//
//
BOOL
DDSFrontEndForSQL::DDSGetFileSizeEx(
	_In_ HANDLE hFile,
	_Out_ PLARGE_INTEGER lpFileSize,
	_Out_ LPDWORD pErrorCode
) {
	printf("DDS: Getting file size %p...\n", hFile);
	DDS_FrontEnd::FileIdT fileId = (DDS_FrontEnd::FileIdT)((size_t)hFile - (size_t)HANDLE_BASE);
	DDS_FrontEnd::FileSizeT fileSize = 0;
	lpFileSize->QuadPart = 0;

	if (pErrorCode) {
		*pErrorCode = NO_ERROR;
	}

	DDS_FrontEnd::ErrorCodeT result = Store.GetFileSize(fileId, &fileSize);
	
	if (result != DDS_ERROR_CODE_SUCCESS) {
		printf("DDS: Failed to get file size\n");
		return false;
	}

	lpFileSize->QuadPart = fileSize;

	return true;
}

//
// Change the size of a file
//
//
DWORD
DDSFrontEndForSQL::DDSChangeFileSize(
	_In_ HANDLE fileHandle,
	_In_ DWORDLONG newSize
) {
	printf("DDS: Changing file size %p...\n", fileHandle);
	DDS_FrontEnd::FileIdT fileId = (DDS_FrontEnd::FileIdT)((size_t)fileHandle - (size_t)HANDLE_BASE);

	DDS_FrontEnd::ErrorCodeT result = Store.ChangeFileSize(fileId, newSize);

	if (result != DDS_ERROR_CODE_SUCCESS) {
		printf("DDS: Failed to change file size\n");
	}

	if (result == DDS_ERROR_CODE_STORAGE_OUT_OF_SPACE) {
		return ERROR_DISK_FULL;
	}

	return ERROR_SUCCESS;
}

//
// Set the valid data size
//
//
BOOL
DDSFrontEndForSQL::DDSSetFileValidData(
	_In_ HANDLE   hFile,
	_In_ LONGLONG ValidDataLength,
	_Out_ LPDWORD pErrorCode
) {
	printf("DDS: Setting the valid data size %p...\n", hFile);

	DWORD error = DDSChangeFileSize(hFile, (DWORDLONG)ValidDataLength);
	if (pErrorCode) {
		*pErrorCode = error;
	}
	return error == ERROR_SUCCESS ? true : false;
}

//
// Set delete on close
//
//
BOOL
DDSFrontEndForSQL::DDSSetFileDeleteOnClose(
	_In_ HANDLE   hFile,
	_In_ bool DeleteOnClose,
	_Out_ LPDWORD pErrorCode
) {
	printf("DDS: Setting file delet on close...\n");
	//
	// TODO: add delete-on-close feature
	//
	//
	if (pErrorCode) {
		*pErrorCode = ERROR_SUCCESS;
	}

	return true;
}

//
// Create a poll structure for async I/O
//
//
HANDLE
DDSFrontEndForSQL::DDSPollCreate() {
	printf("DDS: Creating a poll...\n");
	DDS_FrontEnd::PollIdT pollId = 0;
	DDS_FrontEnd::ErrorCodeT result = DDS_ERROR_CODE_SUCCESS;

	if (PollCounter == 0) {
		// get DDS default poll
		printf("DDS: Getting the default DDS poll...\n");
		result = Store.GetDefaultPoll(&pollId);
		
		if (result != DDS_ERROR_CODE_SUCCESS) {
			printf("DDS: Failed to get the default poll\n");
			return INVALID_HANDLE_VALUE;
		}
	}
	else {
		printf("DDS: Creating a new poll...\n");
		result = Store.PollCreate(&pollId);

		if (result != DDS_ERROR_CODE_SUCCESS) {
			printf("DDS: Failed to create a new poll\n");
			return INVALID_HANDLE_VALUE;
		}
	}

	HANDLE newPollHandle = (HANDLE)((size_t)POLL_BASE + pollId);
	Polls[PollCounter] = newPollHandle;
	PollCounter++;

	return newPollHandle;
}

//
// Delete a poll strucuture
//
//
BOOL
DDSFrontEndForSQL::DDSPollDelete(
	_In_ HANDLE hPoll,
	_Out_ LPDWORD pErrorCode
) {
	printf("DDS: deleting poll %p...\n", hPoll);
	DDS_FrontEnd::PollIdT pollId = (DDS_FrontEnd::PollIdT)((size_t)hPoll - (size_t)POLL_BASE);

	//
	// TODO: provide more informative errors
	//
	//
	if (pErrorCode) {
		*pErrorCode = NO_ERROR;
	}

	DDS_FrontEnd::ErrorCodeT result = Store.PollDelete(pollId);

	if (result != DDS_ERROR_CODE_SUCCESS) {
		if (result == DDS_ERROR_CODE_INVALID_POLL_DELETION) {
			return true;
		}
		printf("DDS: Failed to delete poll %p\n", hPoll);
		return false;
	}

	Polls[pollId] = nullptr;

	return true;
}

//
// Add a file handle to a poll
//
//
BOOL
DDSFrontEndForSQL::DDSPollAdd(
	_In_ HANDLE hFile,
	_In_ HANDLE hPoll,
	_In_ ULONG_PTR pCompletionKey,
	_Out_ LPDWORD pErrorCode
) {
	printf("DDS: adding file (%p) to poll (%p) with context (%llu)..\n", hFile, hPoll, pCompletionKey);
	DDS_FrontEnd::FileIdT fileId = (DDS_FrontEnd::FileIdT)((DDS_FrontEnd::FileIdT)((size_t)hFile - (size_t)HANDLE_BASE));
	DDS_FrontEnd::PollIdT pollId = (DDS_FrontEnd::PollIdT)((DDS_FrontEnd::PollIdT)((size_t)hPoll - (size_t)POLL_BASE));

	//
	// TODO: provide more informative errors
	//
	//
	if (pErrorCode) {
		*pErrorCode = NO_ERROR;
	}

	DDS_FrontEnd::ErrorCodeT result = Store.PollAdd(
		fileId,
		pollId,
		(DDS_FrontEnd::ContextT)pCompletionKey
	);

	if (result != DDS_ERROR_CODE_SUCCESS) {
		printf("DDS: Failed to add file to poll\n");
		return false;
	}

	return true;
}

//
// Create a poll strucuture for async I/O
//
//
BOOL
DDSFrontEndForSQL::DDSPollWait(
	_In_ HANDLE hPoll,
	_Out_ LPDWORD lpNumberOfBytesTransferred,
	_Out_ PULONG_PTR lpCompletionKey,
	_Out_ LPOVERLAPPED* lpOverlapped,
	_In_ DWORD dwMilliseconds,
	_Out_ LPDWORD pErrorCode
) {
	// printf("DDS: performing polling (%p)..\n", hPoll);
	DDS_FrontEnd::PollIdT pollId = (DDS_FrontEnd::PollIdT)((size_t)hPoll - (size_t)POLL_BASE);
	bool pollResult = false;

	//
	// TODO: provide more informative errors
	//
	//
	if (pErrorCode) {
		*pErrorCode = NO_ERROR;
	}

	DDS_FrontEnd::ErrorCodeT result = Store.PollWait(
		pollId,
		lpNumberOfBytesTransferred,
		(DDS_FrontEnd::ContextT*)lpCompletionKey,
		(DDS_FrontEnd::ContextT*)lpOverlapped,
		dwMilliseconds,
		&pollResult
	);

	if (result != DDS_ERROR_CODE_SUCCESS) {
		printf("DDS: Failed to perform polling\n");
		return false;
	}

	if (pollResult) {
		//
		// Set the internal error codes for the overlapped
		//
		//
		(*lpOverlapped)->Internal = (ULONG_PTR)STATUS_SUCCESS;
		(*lpOverlapped)->InternalHigh = (ULONG_PTR)(*lpNumberOfBytesTransferred);
	}

	return pollResult;
}

void
DDSFrontEndForSQL::CheckAndAdd(
	const char* FileName,
	HANDLE FileHandle
) {
	if (strcmp(FileName, "C:\\Socrates\\PageServer\\data\\rbpex.bpe") == 0) {
		std::cout << "DDS: The RBPEX file is created/opened (" << FileHandle << ")..." << std::endl;

		HandleMutex.lock();
		if (HandleCounter == MAX_DDS_FILE_HANDLES) {
			std::cout << "DDS: Out of space" << std::endl;
		}
		else {
			//
			// Check if this handle has been recorded
			//
			//
			size_t i = 0;
			for (; i < HandleCounter; i++) {
				if (Handles[i] == FileHandle) {
					std::cout << "DDS: Existing handle: " << FileHandle << std::endl;
					break;
				}
			}
			if (i == HandleCounter) {
				Handles[HandleCounter] = FileHandle;
				HandleCounter++;
				std::cout << "DDS: New handle " << HandleCounter << ": " << FileHandle << std::endl;
			}
		}
		HandleMutex.unlock();
	}
}

bool
DDSFrontEndForSQL::SearchHandle(
	HANDLE FileHandle
) {
	for (size_t i = 0; i < HandleCounter; i++) {
		if (Handles[i] == FileHandle) {
			return true;
		}
	}

	return false;
}

size_t
DDSFrontEndForSQL::TotalHandles() {
	return HandleCounter;
}