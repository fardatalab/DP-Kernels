#include <chrono>    // C++ time utilities
#include <iostream>  // C++ I/O streams
#include <string.h>  // C string functions (note: could use <cstring> for C++)
#include <thread>    // C++ threading support

#include <time.h>
#include <sys/times.h>
#include <sys/resource.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <mutex>
#include <condition_variable>

#include "DDSFrontEnd.h"
#include "Profiler.h" // TODO What is this? Porting needed? 
#include "DDSEvent.h"

using std::cout;
using std::endl;
using std::thread;
using std::this_thread::yield;

#define PAGE_SIZE 1024
#define INFINITE ((unsigned long)-1)

std::mutex mtx;
std::condition_variable cv;
// HANDLE eventHandle = CreateEvent(NULL, TRUE, FALSE, NULL);
Event eventHandle;
// Event_Init(&eventHandle);

// Consider path separator normalization if dealing with paths in DDSFile.cpp

// Compatible
void
DummyCallback(
    ErrorCodeT ErrorCode,
    FileIOSizeT BytesServiced,
    ContextT Context
) {
    size_t* completedOperations = (size_t*)Context;
    (*completedOperations)++;
}

// PollWait - Compatible
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

// Compatible
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

// TODO Not sure if the time conversion is correct 
// static uint64_t file_time_2_utc(const struct timespec* ts)
// {
//     // Get nanoseconds since Unix epoch (1970)
//     uint64_t nanosecs = ((uint64_t)ts->tv_sec * 1000000000ULL) + ts->tv_nsec;
//     // Convert to 100-nanosecond intervals
//     uint64_t hundred_nanosecs = nanosecs / 100ULL;
//     return hundred_nanosecs;
// }

static int get_processor_number() {
    return sysconf(_SC_NPROCESSORS_ONLN);
}

/*
 *
 *
 *
 *
 *
 */

// new helper function
int64_t timespec_to_ns(struct timespec* ts) {
    return (int64_t)ts->tv_sec * 1000000000LL + ts->tv_nsec;
}

// new helper function to get CPU time in nanoseconds
int64_t get_cpu_time() {
    struct tms tm;
    times(&tm);
    long clk_tck = sysconf(_SC_CLK_TCK);
    
    // Convert user + system time to nanoseconds
    int64_t user_ns = (int64_t)tm.tms_utime * 1000000000LL / clk_tck;
    int64_t system_ns = (int64_t)tm.tms_stime * 1000000000LL / clk_tck;
    
    return user_ns + system_ns;
}

void 
BenchmarkIOCPUEfficient (
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
    
    struct timespec now;
    struct tms process_times;

    int64_t system_time;
    int64_t time;
    int64_t system_time_delta;
    int64_t time_delta;
    int64_t last_time_ = 0;
    int64_t last_system_time_ = 0;

    clock_gettime(CLOCK_REALTIME, &now);  // Instead of GetSystemTimeAsFileTime
    system_time = get_cpu_time() / get_processor_number();  // CPU time divided by processor count
    time = timespec_to_ns(&now);
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

    clock_gettime(CLOCK_REALTIME, &now);
    system_time = get_cpu_time() / get_processor_number();
    time = timespec_to_ns(&now);
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

    clock_gettime(CLOCK_REALTIME, &now);  // Instead of GetSystemTimeAsFileTime
    system_time = get_cpu_time() / get_processor_number();  // CPU time divided by processor count
    time = timespec_to_ns(&now);
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
                        printf("got wrong read result!!!, offset: %ld\n", PAGE_SIZE * j);
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
    
    printf("wrong read count: %lu, total: %lu\n", wrongReadCount, totalIOs);
    profiler.Stop();

    clock_gettime(CLOCK_REALTIME, &now);

    profiler.Report();

    system_time = get_cpu_time() / get_processor_number();
    time = timespec_to_ns(&now);
    system_time_delta = system_time - last_system_time_;
    time_delta = time - last_time_;
    cout << "CPU utilization: " << (system_time_delta * 1.0 / time_delta) * 100 << "%" << endl;
}

// TODO Validate correctness
void* SleepThread(void* arg) {
    Event* eventHandle = (Event*)arg;
    Event_Set(eventHandle); // Signal the event
    sleep(30); // Sleep for 30 seconds
    return NULL;
}

int main() 
{
    const char* storeName = "DDS-Store0";
    const char* rootDirName = "/data";
    const char* fileName = "/data/example";

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

    printf("Before CreateDirectory\n");
    
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
        sizeof(int),
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
        sizeof(int),
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

    cout << "Benchmarking (CPU efficient)" << endl;
    BenchmarkIOCPUEfficient(
        store,
        fileId,
        maxFileSize
    );

    return 0;
}

