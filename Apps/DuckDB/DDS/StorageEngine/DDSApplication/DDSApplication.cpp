#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

#include "DDSFrontEnd.h"
#include "Profiler.h"

#if BACKEND_TYPE == BACKEND_TYPE_LOCAL_MEMORY
#include <Windows.h>
#undef CreateDirectory
#undef CreateFile
#endif

using std::cout;
using std::endl;
using std::thread;
using std::this_thread::yield;

#define PAGE_SIZE 8192

std::mutex mtx;
std::condition_variable cv;
HANDLE eventHandle = CreateEvent(NULL, TRUE, FALSE, NULL);

void
DummyCallback(
    ErrorCodeT ErrorCode,
    FileIOSizeT BytesServiced,
    ContextT Context
) {
    size_t* completedOperations = (size_t*)Context;
    (*completedOperations)++;
}

void BenchmarkIO(
    DDS_FrontEnd::DDSFrontEnd &Store,
    FileIdT FileId,
    FileSizeT MaxFileSize
) {
    char* writeBuffer = new char[PAGE_SIZE];
    memset(writeBuffer, 0, sizeof(char) * PAGE_SIZE);
    *((int*)writeBuffer) = 42;
    FileSizeT totalWrittenSize = 0;
    const size_t totalIOs = MaxFileSize / PAGE_SIZE;
    size_t completedIOs = 0;
    FileIOSizeT bytesServiced;

    ErrorCodeT result = Store.SetFilePointer(
        FileId,
        0,
        FilePointerPosition::BEGIN
    );
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to set file pointer" << endl;
    }
    else {
        cout << "File pointer reset" << endl;
    }

    cout << "Benchmarking writes..." << endl;
    Profiler profiler(totalIOs);
    profiler.Start();
    for (size_t i = 0; i != totalIOs; i++) {
        result = Store.WriteFile(
            FileId,
            writeBuffer,
            PAGE_SIZE,
            &bytesServiced,
            DummyCallback,
            &completedIOs
        );

        if (result != DDS_ERROR_CODE_SUCCESS) {
            cout << "Failed to write file: " << FileId << endl;
        }
    }

    while (completedIOs != totalIOs) {}
    profiler.Stop();
    profiler.Report();

    result = Store.SetFilePointer(
        FileId,
        0,
        FilePointerPosition::BEGIN
    );
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to set file pointer" << endl;
    }
    else {
        cout << "File pointer reset" << endl;
    }

    cout << "Benchmarking reads..." << endl;
    char* readBuffer = new char[PAGE_SIZE * DDS_MAX_OUTSTANDING_IO];
    memset(readBuffer, 0, sizeof(char) * PAGE_SIZE * DDS_MAX_OUTSTANDING_IO);
    completedIOs = 0;
    profiler.Start();
    for (size_t i = 0; i != totalIOs; i++) {
        result = Store.ReadFile(
            FileId,
            &readBuffer[(i % DDS_MAX_OUTSTANDING_IO) * PAGE_SIZE],
            PAGE_SIZE,
            &bytesServiced,
            DummyCallback,
            &completedIOs
        );

        if (result != DDS_ERROR_CODE_SUCCESS) {
            cout << "Failed to read file: " << FileId << endl;
        }
    }

    while (completedIOs != totalIOs) {}
    profiler.Stop();
    profiler.Report();
}

void 
PollThread(
    DDS_FrontEnd::DDSFrontEnd *Store,
    FileIdT FileId,
    size_t NumOps
) {
    size_t completedOps = 0;
    size_t totalBytes = 0;
    PollIdT pollId;
    ErrorCodeT result;

    result = Store->GetDefaultPoll(&pollId);
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to get default poll" << endl;
        return;
    }

    FileIOSizeT opSize;
    ContextT opContext;
    ContextT pollContext;
    bool pollResult;
    while (completedOps != NumOps) {
        result = Store->PollWait(
            pollId,
            &opSize,
            &pollContext,
            &opContext,
            INFINITE,
            &pollResult
        );

        if (result != DDS_ERROR_CODE_SUCCESS) {
            cout << "Failed to poll an operation [" << result << "]" << endl;
            return;
        }

        if (pollResult) {
            totalBytes += opSize;
            completedOps++;
        }
    }
}

void 
BenchmarkIOWithPolling(
    DDS_FrontEnd::DDSFrontEnd& Store,
    FileIdT FileId,
    FileSizeT MaxFileSize
) {
    char* writeBuffer = new char[PAGE_SIZE];
    memset(writeBuffer, 0, sizeof(char) * PAGE_SIZE);
    *((int*)writeBuffer) = 42;
    FileSizeT totalWrittenSize = 0;
    const size_t totalIOs = MaxFileSize / PAGE_SIZE;
    FileIOSizeT bytesServiced;

    ErrorCodeT result = Store.SetFilePointer(
        FileId,
        0,
        FilePointerPosition::BEGIN
    );
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to set file pointer" << endl;
    }
    else {
        cout << "File pointer reset" << endl;
    }

    cout << "Benchmarking writes..." << endl;
    Profiler profiler(totalIOs);
    profiler.Start();

    thread* pollWorker = new thread(
        [&Store, FileId, totalIOs]
        { PollThread(&Store, FileId, totalIOs);}
    );

    for (size_t i = 0; i != totalIOs; i++) {
        result = Store.WriteFile(
            FileId,
            writeBuffer,
            PAGE_SIZE,
            &bytesServiced,
            NULL,
            NULL
        );
        while (result == DDS_ERROR_CODE_TOO_MANY_REQUESTS || result == DDS_ERROR_CODE_REQUEST_RING_FAILURE) {
            result = Store.WriteFile(
                FileId,
                writeBuffer,
                PAGE_SIZE,
                &bytesServiced,
                NULL,
                NULL
            );
        }

        if (result != DDS_ERROR_CODE_IO_PENDING) {
            cout << "Failed to write file: " << FileId << " [" << result << "]" << endl;
        }
    }

    pollWorker->join();
    
    profiler.Stop();
    profiler.Report();

    result = Store.SetFilePointer(
        FileId,
        0,
        FilePointerPosition::BEGIN
    );
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to set file pointer" << endl;
    }
    else {
        cout << "File pointer reset" << endl;
    }

    cout << "Benchmarking reads..." << endl;
    char* readBuffer = new char[PAGE_SIZE * DDS_MAX_OUTSTANDING_IO];
    memset(readBuffer, 0, sizeof(char) * PAGE_SIZE * DDS_MAX_OUTSTANDING_IO);
    
    pollWorker = new thread(
        [&Store, FileId, totalIOs]
        { PollThread(&Store, FileId, totalIOs); }
    );

    profiler.Start();
    for (size_t i = 0; i != totalIOs; i++) {
        result = Store.ReadFile(
            FileId,
            &readBuffer[(i % DDS_MAX_OUTSTANDING_IO) * PAGE_SIZE],
            PAGE_SIZE,
            &bytesServiced,
            NULL,
            NULL
        );

        while (result == DDS_ERROR_CODE_TOO_MANY_REQUESTS || result == DDS_ERROR_CODE_REQUEST_RING_FAILURE) {
            result = Store.ReadFile(
                FileId,
                writeBuffer,
                PAGE_SIZE,
                &bytesServiced,
                NULL,
                NULL
            );
        }

        if (result != DDS_ERROR_CODE_IO_PENDING) {
            cout << "Failed to read file: " << FileId << endl;
        }
    }
    pollWorker->join();
    
    profiler.Stop();
    profiler.Report();

    delete pollWorker;
}

static uint64_t file_time_2_utc(const FILETIME* ftime)
{
    LARGE_INTEGER li;

    li.LowPart = ftime->dwLowDateTime;
    li.HighPart = ftime->dwHighDateTime;
    return li.QuadPart;
}

static int get_processor_number()
{
    SYSTEM_INFO info;
    GetSystemInfo(&info);
    return (int)info.dwNumberOfProcessors;
}

void 
BenchmarkIOCPUEfficient(
    DDS_FrontEnd::DDSFrontEnd& Store,
    FileIdT FileId,
    FileSizeT MaxFileSize
) {
    char* expectedBuffer = new char[PAGE_SIZE];
    memset(expectedBuffer, 42, sizeof(char) * PAGE_SIZE);

    char* writeBuffer = new char[PAGE_SIZE];
    memset(writeBuffer, 42, sizeof(char) * PAGE_SIZE);
    //*((int*)writeBuffer) = 42;
    FileSizeT totalWrittenSize = 0;
    const size_t totalIOs = MaxFileSize / PAGE_SIZE;
    FileIOSizeT bytesServiced;

    PollIdT pollId;
    FileIOSizeT opSize;
    ContextT opContext;
    ContextT pollContext;
    bool pollResult;
    size_t completedOps = 0;

    ErrorCodeT result = Store.GetDefaultPoll(&pollId);
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to get default poll" << endl;
        return;
    }

    result = Store.SetFilePointer(
        FileId,
        0,
        FilePointerPosition::BEGIN
    );
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to set file pointer" << endl;
    }
    else {
        cout << "File pointer reset" << endl;
    }

    cout << "Benchmarking writes..." << endl;
    
    FILETIME now;
    FILETIME creation_time;
    FILETIME exit_time;
    FILETIME kernel_time;
    FILETIME user_time;
    int64_t system_time;
    int64_t time;
    int64_t system_time_delta;
    int64_t time_delta;
    int64_t last_time_ = 0;
    int64_t last_system_time_ = 0;

    GetSystemTimeAsFileTime(&now);
    GetProcessTimes(GetCurrentProcess(), &creation_time, &exit_time, &kernel_time, &user_time);

    system_time = (file_time_2_utc(&kernel_time) + file_time_2_utc(&user_time)) / get_processor_number();
    time = file_time_2_utc(&now);
    last_system_time_ = system_time;
    last_time_ = time;

    Profiler profiler(totalIOs);
    profiler.Start();

    for (size_t i = 0; i != totalIOs;) {
        result = Store.WriteFile(
            FileId,
            writeBuffer,
            PAGE_SIZE,
            &bytesServiced,
            NULL,
            NULL
        );

        if (result == DDS_ERROR_CODE_TOO_MANY_REQUESTS || result == DDS_ERROR_CODE_REQUEST_RING_FAILURE) {
            result = Store.PollWait(
                pollId,
                &opSize,
                &pollContext,
                &opContext,
                INFINITE,
                &pollResult
            );

            do {
                completedOps++;
                Store.PollWait(
                    pollId,
                    &opSize,
                    &pollContext,
                    &opContext,
                    0,
                    &pollResult
                );
            } while (pollResult);
        }
        else {
            i++;
        }
    }

    while (completedOps != totalIOs) {
        result = Store.PollWait(
            pollId,
            &opSize,
            &pollContext,
            &opContext,
            INFINITE,
            &pollResult
        );

        if (result != DDS_ERROR_CODE_SUCCESS) {
            cout << "Failed to poll an operation [" << result << "]" << endl;
            return;
        }

        if (pollResult) {
            completedOps++;
        }
    }
    
    profiler.Stop();

    GetSystemTimeAsFileTime(&now);
    GetProcessTimes(GetCurrentProcess(), &creation_time, &exit_time, &kernel_time, &user_time);

    profiler.Report();

    system_time = (file_time_2_utc(&kernel_time) + file_time_2_utc(&user_time)) / get_processor_number();
    time = file_time_2_utc(&now);
    system_time_delta = system_time - last_system_time_;
    time_delta = time - last_time_;
    cout << "CPU utilization: " << (system_time_delta * 1.0 / time_delta) * 100 << "%" << endl;

    result = Store.SetFilePointer(
        FileId,
        0,
        FilePointerPosition::BEGIN
    );
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to set file pointer" << endl;
    }
    else {
        cout << "File pointer reset" << endl;
    }

    cout << "Benchmarking reads..." << endl;
    char* readBuffer = new char[PAGE_SIZE * DDS_MAX_OUTSTANDING_IO];
    memset(readBuffer, 0, sizeof(char) * PAGE_SIZE * DDS_MAX_OUTSTANDING_IO);
    completedOps = 0;

    GetSystemTimeAsFileTime(&now);
    GetProcessTimes(GetCurrentProcess(), &creation_time, &exit_time, &kernel_time, &user_time);

    system_time = (file_time_2_utc(&kernel_time) + file_time_2_utc(&user_time)) / get_processor_number();
    time = file_time_2_utc(&now);
    last_system_time_ = system_time;
    last_time_ = time;

    char* zeroedBuffer = new char[PAGE_SIZE];
    memset(zeroedBuffer, 0, PAGE_SIZE);
    size_t wrongReadCount = 0;

    profiler.Start();
    for (size_t i = 0; i != totalIOs;) {
        memset(&readBuffer[(i % DDS_MAX_OUTSTANDING_IO) * PAGE_SIZE], 255, PAGE_SIZE);

        result = Store.ReadFile(
            FileId,
            &readBuffer[(i % DDS_MAX_OUTSTANDING_IO) * PAGE_SIZE],
            PAGE_SIZE,
            &bytesServiced,
            NULL,
            (void *) i
        );

        if (result == DDS_ERROR_CODE_TOO_MANY_REQUESTS || result == DDS_ERROR_CODE_REQUEST_RING_FAILURE) {
            result = Store.PollWait(
                pollId,
                &opSize,
                &pollContext,
                &opContext,
                INFINITE,
                &pollResult
            );
            size_t j = (size_t)opContext;
            if (memcmp(&readBuffer[(j % DDS_MAX_OUTSTANDING_IO) * PAGE_SIZE], expectedBuffer, PAGE_SIZE) != 0) {
                printf("got wrong read result!!!\n");
                wrongReadCount++;
            }

            do {
                completedOps++;
                Store.PollWait(
                    pollId,
                    &opSize,
                    &pollContext,
                    &opContext,
                    0,
                    &pollResult
                );
                size_t j = (size_t)opContext;
                if (pollResult) {
                    if (memcmp(&readBuffer[(j % DDS_MAX_OUTSTANDING_IO) * PAGE_SIZE], expectedBuffer, PAGE_SIZE) != 0) {
                        printf("got wrong read result!!!, offset: %p\n", PAGE_SIZE * j);
                        wrongReadCount++;
                    }
                }
                
            } while (pollResult);
        }
        else {
            
            i++;
        }
    }

    while (completedOps != totalIOs) {
        result = Store.PollWait(
            pollId,
            &opSize,
            &pollContext,
            &opContext,
            INFINITE,
            &pollResult
        );
        size_t j = (size_t)opContext;
        if (memcmp(&readBuffer[(j % DDS_MAX_OUTSTANDING_IO) * PAGE_SIZE], expectedBuffer, PAGE_SIZE) != 0) {
            printf("got wrong read result!!!\n");
            wrongReadCount++;
        }

        if (result != DDS_ERROR_CODE_SUCCESS) {
            cout << "Failed to poll an operation [" << result << "]" << endl;
            return;
        }

        if (pollResult) {
            completedOps++;
        }
    }
    
    printf("wrong read count: %llu, total: %llu\n", wrongReadCount, totalIOs);
    profiler.Stop();

    GetSystemTimeAsFileTime(&now);
    GetProcessTimes(GetCurrentProcess(), &creation_time, &exit_time, &kernel_time, &user_time);
    
    profiler.Report();

    system_time = (file_time_2_utc(&kernel_time) + file_time_2_utc(&user_time)) / get_processor_number();
    time = file_time_2_utc(&now);
    system_time_delta = system_time - last_system_time_;
    time_delta = time - last_time_;
    cout << "CPU utilization: " << (system_time_delta * 1.0 / time_delta) * 100 << "%" << endl;
}

DWORD WINAPI SleepThread(LPVOID lpParam) {
    SetEvent(eventHandle);
    Sleep(30000);
    return 0;
}

int main()
{
    /*
    // atomic test
    std::atomic<int> ai = 0;
    cout << "Size of atomic<int> is " << sizeof(ai) << endl;

    return 0;

    // Mutex test

    // std::thread t = std::thread(SleepThread);
    CreateThread(NULL, 0, SleepThread, NULL, 0, NULL);

    // cout << "Waiting for mutex..." << endl;
    std::unique_lock<std::mutex> lck(mtx);
    auto prevClock = high_resolution_clock::now();
    
    WaitForSingleObject(eventHandle, INFINITE);

    auto nextClock = high_resolution_clock::now();
    cout << "Event got in " << (nextClock - prevClock).count() << " ns" << endl;

    // Waiting test
    static HANDLE timer = CreateWaitableTimer(NULL, FALSE, NULL);
    LARGE_INTEGER due;
    due.QuadPart = -10LL; // *100 ns
    while (true) {
        // double sleepSecs = 0.00001;
        auto prevClock = high_resolution_clock::now();
        // this_thread::sleep_for(nanoseconds((int64_t)(sleepSecs * 1e9)));

        BOOL bSuccess = SetWaitableTimer(timer, &due, 0, NULL, NULL, NULL);
        WaitForSingleObject(timer, INFINITE);
        
        auto nextClock = high_resolution_clock::now();
        double deltaTime = (nextClock - prevClock).count() / 1e9;
        // printf("Latency: %.6lf ms\n", deltaTime * 1e3);
        prevClock = nextClock;
    }
    return 0;
    */

    const char* storeName = "DDS-Store0";
    const char* rootDirName = "/data";
    const char* fileName = "/data/example";
    // const FileSizeT maxFileSize = 1073741824ULL;
    // const FileSizeT maxFileSize = 8192000;
    // const FileSizeT maxFileSize = 163840000000;
    const FileSizeT maxFileSize = 2147483648ULL;
    const FileAccessT fileAccess = 0;
    const FileShareModeT shareMode = 0;
    const FileAttributesT fileAttributes = 0;
    DirIdT rootDirId = DDS_DIR_INVALID;
    FileIdT fileId = DDS_FILE_INVALID;

    DDS_FrontEnd::DDSFrontEnd store(storeName);

    ErrorCodeT result = store.Initialize();
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to initialize DDS front end" << std::endl;
    }
    else {
        cout << "DDS front end initialized " << std::endl;
    }

    result = store.CreateDirectory(rootDirName, &rootDirId);
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to create directory" << endl;
    }
    else {
        cout << "Directory created: " << rootDirId << endl;
    }

    result = store.CreateFile(
        fileName,
        fileAccess,
        shareMode,
        fileAttributes,
        &fileId
    );
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to create file" << endl;
    }
    else {
        cout << "File created: " << fileId << endl;
    }

    int dataToWrite[128];
    memset(dataToWrite, 0, sizeof(int) * 128);
    dataToWrite[0] = 42;
    int readBuffer[128];
    memset(readBuffer, 0, sizeof(int) * 128);
    size_t ioCount = 0;
    FileIOSizeT bytesServiced = 0;

    result = store.WriteFile(
        fileId,
        (BufferT)dataToWrite,
        sizeof(int) * 128,
        &bytesServiced,
        DummyCallback,
        &ioCount
    );
    if (result != DDS_ERROR_CODE_SUCCESS && result != DDS_ERROR_CODE_IO_PENDING) {
        cout << "Failed to write file" << endl;
    }
    else {
        cout << "Write posted" << endl;
    }

    ContextT ioCtxt, fileCtxt;
    bool pollResult = false;

    cout << "Calling PollWait..." << endl;
    while (!pollResult) {
        store.PollWait(DDS_POLL_DEFAULT, &bytesServiced, &fileCtxt, &ioCtxt, 0, &pollResult);
    }
    cout << "PollWait finished " << pollResult << " (" << bytesServiced << " bytes serviced)" << endl;

    /*
    while (ioCount != 1) {
        yield();
    }
    */

    cout << "Data has been written" << endl;

    result = store.SetFilePointer(
        fileId,
        0,
        FilePointerPosition::BEGIN
    );
    if (result != DDS_ERROR_CODE_SUCCESS) {
        cout << "Failed to set file pointer" << endl;
    }
    else {
        cout << "File pointer reset" << endl;
    }

    result = store.ReadFile(
        fileId,
        (BufferT)readBuffer,
        sizeof(int) * 128,
        &bytesServiced,
        DummyCallback,
        &ioCount
    );
    if (result != DDS_ERROR_CODE_SUCCESS && result != DDS_ERROR_CODE_IO_PENDING) {
        cout << "Failed to read file " << result << endl;
    }
    else {
        cout << "Read posted" << endl;
    }

    cout << "Calling PollWait..." << endl;
    pollResult = false;
    while (!pollResult) {
        store.PollWait(DDS_POLL_DEFAULT, &bytesServiced, &fileCtxt, &ioCtxt, 0, &pollResult);
    }
    cout << "PollWait finished " << pollResult << " (" << bytesServiced << " bytes serviced)" << endl;
    /*
    while (ioCount != 2) {
        yield();
    }
    */

    cout << "Data has been read: " << readBuffer[0] << endl;

    /*
    cout << "Benchmarking callback-based I/O" << endl;
    BenchmarkIO(
        store,
        fileId,
        maxFileSize
    );
    */
    
    /*
    cout << "Benchmarking polling-based I/O" << endl;
    BenchmarkIOWithPolling(
        store,
        fileId,
        maxFileSize
    );
    */

   cout << "Benchmarking (CPU efficient)" << endl;
   BenchmarkIOCPUEfficient(
        store,
        fileId,
        maxFileSize
    );

    return 0;
}