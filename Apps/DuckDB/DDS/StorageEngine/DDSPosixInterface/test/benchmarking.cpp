#include <iostream>
#include <chrono>
#include <string>
#include <cstring>
#include <iomanip>
#include <fcntl.h>
#include <unistd.h>
#include <sys/times.h>
#include <sys/resource.h>

#include "DDSPosix.h"
#include "Profiler.h"

using namespace std;
using namespace std::chrono;

// Match the first benchmark's buffer size
#define PAGE_SIZE 32768 // 16KB

static int fd;
// Helper for CPU time calculation
int64_t timespec_to_ns(struct timespec* ts) {
    return (int64_t)ts->tv_sec * 1000000000LL + ts->tv_nsec;
}

// Get CPU time in nanoseconds
int64_t get_cpu_time() {
    struct tms tm;
    times(&tm);
    long clk_tck = sysconf(_SC_CLK_TCK);
    
    // Convert user + system time to nanoseconds
    int64_t user_ns = (int64_t)tm.tms_utime * 1000000000LL / clk_tck;
    int64_t system_ns = (int64_t)tm.tms_stime * 1000000000LL / clk_tck;
    
    return user_ns + system_ns;
}

// Helper function to format bytes
string formatBytes(uint64_t bytes) {
    string units[] = {"B", "KB", "MB", "GB", "TB"};
    int unitIndex = 0;
    double size = bytes;
    
    while (size >= 1024 && unitIndex < 4) {
        size /= 1024;
        unitIndex++;
    }
    
    char buffer[100];
    snprintf(buffer, sizeof(buffer), "%.2f %s", size, units[unitIndex].c_str());
    return string(buffer);
}

static int get_processor_number() {
    return sysconf(_SC_NPROCESSORS_ONLN);
}

// Benchmark function for write throughput using POSIX-style wrappers
void benchmarkWrite(const char* filename, size_t totalSize) {
    cout << "\n=== WRITE BENCHMARK ===" << endl;
    cout << "File: " << filename << endl;
    cout << "Total size: " << formatBytes(totalSize) << endl;
    cout << "Block size: " << PAGE_SIZE << " bytes" << endl;

    // Prepare data buffer with pattern (using 0x42 pattern like in original)
    char* writeBuffer = new char[PAGE_SIZE];
    memset(writeBuffer, 0x42, PAGE_SIZE);
    
    // CPU timing setup
    struct timespec now;
    int64_t system_time;
    int64_t time;
    int64_t system_time_delta;
    int64_t time_delta;
    int64_t last_time_;
    int64_t last_system_time_;

    // Create/open file for writing using POSIX wrapper
    fd = DDSPosix::open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        cout << "Failed to open file for writing: " << errno << endl;
        delete[] writeBuffer;
        return;
    }
    
    const size_t totalBlocks = totalSize / PAGE_SIZE;
    size_t blocksWritten = 0;
    size_t bytesWritten = 0;
    
    // Start timing
    clock_gettime(CLOCK_REALTIME, &now);
    system_time = get_cpu_time() / get_processor_number();
    time = timespec_to_ns(&now);
    last_system_time_ = system_time;
    last_time_ = time;
    
    cout << "Benchmarking writes..." << endl;
    
    auto startTime = high_resolution_clock::now();
    
    for (size_t i = 0; i < totalBlocks; i++) {
        ssize_t result = DDSPosix::write(fd, writeBuffer, PAGE_SIZE);
        
        if (result > 0) {
            bytesWritten += result;
            blocksWritten++;
        } else {
            cout << "Write error at block " << i << ": " << errno << endl;
            break;
        }
    }
    
    auto endTime = high_resolution_clock::now();
    double elapsedSeconds = duration_cast<milliseconds>(endTime - startTime).count() / 1000.0;
    
    double throughputMBps = (bytesWritten / (1024.0 * 1024.0)) / elapsedSeconds;
    
    clock_gettime(CLOCK_REALTIME, &now);
    system_time = get_cpu_time() / get_processor_number();
    time = timespec_to_ns(&now);
    system_time_delta = system_time - last_system_time_;
    time_delta = time - last_time_;
    
    // Print results
    cout << "Blocks written: " << blocksWritten << " of " << totalBlocks << endl;
    cout << "Bytes written: " << formatBytes(bytesWritten) << endl;
    cout << "Time elapsed: " << fixed << setprecision(2) << elapsedSeconds << " seconds" << endl;
    cout << "Write throughput: " << fixed << setprecision(2) << throughputMBps << " MB/s" << endl;
    cout << "CPU utilization: " << (system_time_delta * 1.0 / time_delta) * 100 << "%" << endl;
    
    // Close file
    DDSPosix::close(fd);
    delete[] writeBuffer;
}

// Benchmark function for read throughput using POSIX-style wrappers
void benchmarkRead(const char* filename, size_t totalSize) {
    cout << "\n=== READ BENCHMARK ===" << endl;
    cout << "File: " << filename << endl;
    cout << "Total size: " << formatBytes(totalSize) << endl;
    cout << "Block size: " << PAGE_SIZE << " bytes" << endl;
    
    // Prepare data buffer
    char* readBuffer = new char[PAGE_SIZE];
    
    // CPU timing setup
    struct timespec now;
    int64_t system_time;
    int64_t time;
    int64_t system_time_delta;
    int64_t time_delta;
    int64_t last_time_;
    int64_t last_system_time_;
    
    // Open file for reading using POSIX wrapper
    int fd = DDSPosix::open(filename, O_RDONLY, 0);
    
    if (fd < 0) {
        cout << "Failed to open file for reading: " << errno << endl;
        delete[] readBuffer;
        return;
    }
    
    const size_t totalBlocks = totalSize / PAGE_SIZE;
    size_t blocksRead = 0;
    size_t bytesRead = 0;
    
    // Start timing
    clock_gettime(CLOCK_REALTIME, &now);
    system_time = get_cpu_time() / get_processor_number();
    time = timespec_to_ns(&now);
    last_system_time_ = system_time;
    last_time_ = time;
    
    cout << "Benchmarking reads..." << endl;
    DDSPosix::lseek(fd, 0, SEEK_SET);

    // Create an expected buffer for verification
    char* expectedBuffer = new char[PAGE_SIZE];
    memset(expectedBuffer, 0x42, PAGE_SIZE);
    size_t wrongReadCount = 0;
    
    // Start recording time for manual throughput calculation
    auto startTime = high_resolution_clock::now();
    
    // Perform sequential reads using the POSIX wrapper
    for (size_t i = 0; i < totalBlocks; i++) {
        // Reset read buffer to detect partial reads
        memset(readBuffer, 0, PAGE_SIZE);
        
        ssize_t result = DDSPosix::read(fd, readBuffer, PAGE_SIZE);
        
        if (result > 0) {
            bytesRead += result;
            blocksRead++;
            
            if (memcmp(readBuffer, expectedBuffer, PAGE_SIZE) != 0) {
                printf("Got wrong read result at offset: %ld\n", i * PAGE_SIZE);
                wrongReadCount++;
            }
        } else if (result == 0) {
            // End of file
            break;
        } else {
            cout << "Read error at block " << i << ": " << errno << endl;
            break;
        }
    }
    
    // End timing
    auto endTime = high_resolution_clock::now();
    double elapsedSeconds = duration_cast<milliseconds>(endTime - startTime).count() / 1000.0;
    
    // Calculate throughput in MB/s like DDSApplication
    double throughputMBps = (bytesRead / (1024.0 * 1024.0)) / elapsedSeconds;
    
    // Calculate CPU utilization
    clock_gettime(CLOCK_REALTIME, &now);
    system_time = get_cpu_time() / get_processor_number();
    time = timespec_to_ns(&now);
    system_time_delta = system_time - last_system_time_;
    time_delta = time - last_time_;
    
    // Print results
    cout << "Blocks read: " << blocksRead << " of " << totalBlocks << endl;
    cout << "Bytes read: " << formatBytes(bytesRead) << endl;
    cout << "Time elapsed: " << fixed << setprecision(2) << elapsedSeconds << " seconds" << endl;
    cout << "Read throughput: " << fixed << setprecision(2) << throughputMBps << " MB/s" << endl;
    cout << "Wrong read count: " << wrongReadCount << ", total: " << totalBlocks << endl;
    cout << "CPU utilization: " << (system_time_delta * 1.0 / time_delta) * 100 << "%" << endl;
    
    // Close file
    DDSPosix::close(fd);
    delete[] readBuffer;
    delete[] expectedBuffer;
}

// Verify data integrity
bool verifyData(const char* filename, size_t totalSize) {
    cout << "\n=== DATA VERIFICATION ===" << endl;
    
    // Prepare expected pattern buffer
    char* expectedBuffer = new char[PAGE_SIZE];
    memset(expectedBuffer, 0x42, PAGE_SIZE);
    
    // Prepare read buffer
    char* readBuffer = new char[PAGE_SIZE];
    
    // Open file for reading
    int fd = DDSPosix::open(filename, O_RDONLY, 0);
    if (fd < 0) {
        cout << "Failed to open file for verification: " << errno << endl;
        delete[] expectedBuffer;
        delete[] readBuffer;
        return false;
    }
    
    const size_t totalBlocks = totalSize / PAGE_SIZE;
    size_t mismatchedBlocks = 0;
    
    // Read and verify each block
    for (size_t i = 0; i < totalBlocks; i++) {
        ssize_t result = DDSPosix::read(fd, readBuffer, PAGE_SIZE);
        
        if (result == PAGE_SIZE) {
            if (memcmp(readBuffer, expectedBuffer, PAGE_SIZE) != 0) {
                if (mismatchedBlocks < 5) {  // Limit the number of errors we print
                    cout << "Data mismatch at block " << i << endl;
                }
                mismatchedBlocks++;
            }
        } else {
            cout << "Read error or incomplete block at " << i << endl;
            DDSPosix::close(fd);
            delete[] expectedBuffer;
            delete[] readBuffer;
            return false;
        }
    }
    
    // Print verification result
    if (mismatchedBlocks == 0) {
        cout << "All data verified successfully!" << endl;
    } else {
        cout << "Found " << mismatchedBlocks << " mismatched blocks of " << totalBlocks << endl;
    }
    
    // Clean up
    DDSPosix::close(fd);
    delete[] expectedBuffer;
    delete[] readBuffer;
    
    return (mismatchedBlocks == 0);
}

int main(int argc, char* argv[]) {
    // Parse command line arguments for file size (in MB)
    size_t fileSizeMB = 1024;  // Default 1GB
    if (argc > 1) {
        fileSizeMB = atoi(argv[1]);
    }
    size_t totalSize = fileSizeMB * 1024 * 1024;
    
    // Ensure the size is a multiple of the PAGE_SIZE
    totalSize = (totalSize / PAGE_SIZE) * PAGE_SIZE;
    
    const char* filename = "/data/throughput_benchmark.dat";
    
    cout << "==================================================" << endl;
    cout << "    DDS POSIX Throughput Benchmark" << endl;
    cout << "==================================================" << endl;
    cout << "Initializing DDS POSIX interface..." << endl;
    
    // Initialize the POSIX interface
    bool initResult = DDSPosix::initialize_posix("BenchmarkStore");
    if (!initResult) {
        cerr << "Failed to initialize DDS POSIX interface" << endl;
        return 1;
    }
    cout << "DDS POSIX interface initialized successfully" << endl;
    
    // Run the write benchmark
    benchmarkWrite(filename, totalSize);
    
    // Run the read benchmark
    benchmarkRead(filename, totalSize);
    
    // Verify data integrity
    // verifyData(filename, totalSize);
    
    // Clean up
    cout << "\nShutting down DDS POSIX interface..." << endl;
    DDSPosix::shutdown_posix();
    cout << "Benchmark completed" << endl;
    
    return 0;
}