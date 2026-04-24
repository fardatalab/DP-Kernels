#include <iostream>
#include <string>
#include <cstring>
#include <cassert>
#include <vector>
#include <fcntl.h>
#include "DDSPosix.h"

// Test tracking variables
int totalTests = 0;
int passedTests = 0;

// // Simple test assertion macros
#define TEST_START(name) do { \
    std::cout << "Running test: " << name << "... \n"; \
    totalTests++; \
} while(0)

#define TEST_ASSERT(condition) do { \
    if (!(condition)) { \
        std::cout << "FAILED at line " << __LINE__ << std::endl; \
        return false; \
    } \
} while(0)

#define TEST_END() do { \
    std::cout << "PASSED" << std::endl; \
    passedTests++; \
    return true; \
} while(0)







// Helper function to create a test file with some content
int createTestFile(const char* filename, char* buf) {

    int fd = DDSPosix::open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    TEST_ASSERT(fd >= 3);

    printf("WRITE\n");
    
    int size = 512;
    int written = DDSPosix::write(fd, buf, size);

    TEST_ASSERT(written == size);
    DDSPosix::close(fd);
    return 0;
}

// // Test lseek functionality
// bool testLseek() {
//     TEST_START("Lseek");
    
//     const char* testData = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
//     size_t dataLen = strlen(testData);
    
//     // Create a test file
//     TEST_ASSERT(createTestFile("test_seek.txt", testData) == 0);
    
//     // Open the file for reading
//     int fd = open("test_seek.txt", O_RDONLY);
//     TEST_ASSERT(fd >= 3);
    
//     // Seek to position 10 from the beginning
//     off_t newPos = lseek(fd, 10, SEEK_SET);
//     TEST_ASSERT(newPos == 10);
    
//     // Read 5 bytes
//     char buffer[6] = {0};
//     ssize_t bytesRead = read(fd, buffer, 5);
//     TEST_ASSERT(bytesRead == 5);
//     TEST_ASSERT(strcmp(buffer, "KLMNO") == 0);
    
//     // Seek 5 bytes forward from current position
//     newPos = lseek(fd, 5, SEEK_CUR);
//     TEST_ASSERT(newPos == 20);
    
//     // Read 5 bytes
//     memset(buffer, 0, sizeof(buffer));
//     bytesRead = read(fd, buffer, 5);
//     TEST_ASSERT(bytesRead == 5);
//     TEST_ASSERT(strcmp(buffer, "UVWXY") == 0);
    
//     // Seek 10 bytes backward from the end
//     newPos = lseek(fd, -10, SEEK_END);
//     TEST_ASSERT(newPos == static_cast<off_t>(dataLen - 10));
    
//     // Read 5 bytes
//     memset(buffer, 0, sizeof(buffer));
//     bytesRead = read(fd, buffer, 5);
//     TEST_ASSERT(bytesRead == 5);
//     TEST_ASSERT(strcmp(buffer, "QRSTU") == 0);
    
//     // Close the file
//     TEST_ASSERT(close(fd) == 0);
    
//     TEST_END();
// }

// // Test directory operations
bool testDirectory() {
    TEST_START("Directory");
    
    int size = 512;
    char* buf = (char *) malloc(sizeof(char) * size);
    // buf[511] = '\0';

    // Create some test files
    TEST_ASSERT(createTestFile("test_dir_file1.txt", buf) == 0);
    // TEST_ASSERT(createTestFile("test_dir_file2.txt", buf) == 0);
    
    printf("DIR\n");
    // Open the directory
    // DDSPosix::DIR* dir = DDSPosix::opendir(".");
    // TEST_ASSERT(dir != nullptr);
    
    // // Read directory entries
    // std::vector<std::string> foundFiles;
    // struct dirent* entry;
    
    // while ((entry = DDSPosix::readdir(dir)) != nullptr) {
    //     if (entry->d_type == DT_REG) {
    //         std::string filename = entry->d_name;
    //         if (filename.find("test_dir_file") != std::string::npos) {
    //             foundFiles.push_back(filename);
    //         }
    //     }
    // }
    
    // // We should have found our test files
    // TEST_ASSERT(foundFiles.size() == 2);


    // bool found1 = false, found2 = false;
    
    // for (const auto& file : foundFiles) {
    //     if (file == "test_dir_file1.txt") found1 = true;
    //     if (file == "test_dir_file2.txt") found2 = true;
    // }
    
    // TEST_ASSERT(found1);
    // TEST_ASSERT(found2);
    
    // // Rewind and check again
    // DDSPosix::rewinddir(dir);
    
    // // Close the directory
    // TEST_ASSERT(DDSPosix::closedir(dir) == 0);
    
    free(buf);
    TEST_END();
}

// // Test error conditions
// bool testErrorConditions() {
//     TEST_START("ErrorConditions");
    
//     // Try to open a non-existent file
//     int fd = open("nonexistent_file.txt", O_RDONLY);
//     TEST_ASSERT(fd == -1);
//     TEST_ASSERT(errno == ENOENT);
    
//     // Create a file and try to read from it when opened write-only
//     TEST_ASSERT(createTestFile("test_errors.txt", "Test Content") == 0);
    
//     fd = open("test_errors.txt", O_WRONLY);
//     TEST_ASSERT(fd >= 3);
    
//     char buffer[20];
//     ssize_t bytesRead = read(fd, buffer, sizeof(buffer));
//     TEST_ASSERT(bytesRead == -1);  // This should fail with write-only fd
    
//     // Close properly
//     TEST_ASSERT(close(fd) == 0);
    
//     // Try to write to a read-only file
//     fd = open("test_errors.txt", O_RDONLY);
//     TEST_ASSERT(fd >= 3);
    
//     ssize_t bytesWritten = write(fd, "New Content", 11);
//     TEST_ASSERT(bytesWritten == -1);  // This should fail with read-only fd
    
//     // Close properly
//     TEST_ASSERT(close(fd) == 0);
    
//     TEST_END();
// }

// // Test append mode
// bool testAppend() {
//     TEST_START("Append");
    
//     // Create file with initial content
//     const char* initialContent = "Initial Content\n";
//     TEST_ASSERT(createTestFile("test_append.txt", initialContent) == 0);
    
//     // Open in append mode
//     int fd = open("test_append.txt", O_WRONLY | O_APPEND);
//     TEST_ASSERT(fd >= 3);
    
//     // Append more content
//     const char* appendContent = "Appended Content\n";
//     ssize_t bytesWritten = write(fd, appendContent, strlen(appendContent));
//     TEST_ASSERT(bytesWritten == static_cast<ssize_t>(strlen(appendContent)));
//     TEST_ASSERT(close(fd) == 0);
    
//     // Read back the entire file
//     fd = open("test_append.txt", O_RDONLY);
//     TEST_ASSERT(fd >= 3);
    
//     char buffer[100] = {0};
//     ssize_t bytesRead = read(fd, buffer, sizeof(buffer) - 1);
    
//     std::string expectedContent = std::string(initialContent) + std::string(appendContent);
//     TEST_ASSERT(bytesRead == static_cast<ssize_t>(expectedContent.length()));
//     TEST_ASSERT(strcmp(buffer, expectedContent.c_str()) == 0);
    
//     TEST_ASSERT(close(fd) == 0);
    
//     TEST_END();
// }

// // Test multiple files
// bool testMultipleFiles() {
//     TEST_START("MultipleFiles");
    
//     // Open multiple files
//     int fd1 = open("test_multi_1.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
//     TEST_ASSERT(fd1 >= 3);
    
//     int fd2 = open("test_multi_2.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
//     TEST_ASSERT(fd2 >= 3);
    
//     // Write to both files
//     const char* content1 = "Content for file 1";
//     const char* content2 = "Content for file 2";
    
//     TEST_ASSERT(write(fd1, content1, strlen(content1)) == (ssize_t)strlen(content1));
//     TEST_ASSERT(write(fd2, content2, strlen(content2)) == (ssize_t)strlen(content2));
    
//     // Close both files
//     TEST_ASSERT(close(fd1) == 0);
//     TEST_ASSERT(close(fd2) == 0);
    
//     // Reopen files for reading
//     fd1 = open("test_multi_1.txt", O_RDONLY);
//     fd2 = open("test_multi_2.txt", O_RDONLY);
    
//     TEST_ASSERT(fd1 >= 3);
//     TEST_ASSERT(fd2 >= 3);
    
//     // Read and verify content
//     char buffer[100] = {0};
    
//     TEST_ASSERT(read(fd1, buffer, sizeof(buffer) - 1) == (ssize_t)strlen(content1));
//     TEST_ASSERT(strcmp(buffer, content1) == 0);
    
//     memset(buffer, 0, sizeof(buffer));
//     TEST_ASSERT(read(fd2, buffer, sizeof(buffer) - 1) == (ssize_t)strlen(content2));
//     TEST_ASSERT(strcmp(buffer, content2) == 0);
    
//     // Close files
//     TEST_ASSERT(close(fd1) == 0);
//     TEST_ASSERT(close(fd2) == 0);
    
//     TEST_END();
// }

// // Test for truncation of files
// bool testTruncate() {
//     TEST_START("Truncate");
    
//     // Create file with initial content
//     const char* initialContent = "This is a long content that will be truncated";
//     TEST_ASSERT(createTestFile("test_truncate.txt", initialContent) == 0);
    
//     // Open with truncate flag
//     int fd = open("test_truncate.txt", O_WRONLY | O_TRUNC);
//     TEST_ASSERT(fd >= 3);
    
//     // Write new shorter content
//     const char* newContent = "Short";
//     ssize_t bytesWritten = write(fd, newContent, strlen(newContent));
//     TEST_ASSERT(bytesWritten == static_cast<ssize_t>(strlen(newContent)));
//     TEST_ASSERT(close(fd) == 0);
    
//     // Verify file was truncated
//     fd = open("test_truncate.txt", O_RDONLY);
//     TEST_ASSERT(fd >= 3);
    
//     char buffer[100] = {0};
//     ssize_t bytesRead = read(fd, buffer, sizeof(buffer) - 1);
    
//     TEST_ASSERT(bytesRead == static_cast<ssize_t>(strlen(newContent)));
//     TEST_ASSERT(strcmp(buffer, newContent) == 0);
    
//     TEST_ASSERT(close(fd) == 0);
    
//     TEST_END();
// }


// Initialize POSIX interface before testing
bool setupTest() {
    TEST_START("Setup");
    int ret = DDSPosix::initialize_posix("TestStorage");
    TEST_ASSERT(ret == 1);
    TEST_END();
}

// Test open/close functionality
bool testOpenClose() {
    TEST_START("OpenClose");
    
    // Open a new file for writing
    int fd1 = DDSPosix::open("test_file1.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    printf("fd: %d\n", fd1);
    TEST_ASSERT(fd1 >= 3); // File descriptors typically start at 3 (0,1,2 are reserved)
    
    int fd2 = DDSPosix::open("test_file2.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    printf("fd: %d\n", fd2);

    fd1 = DDSPosix::open("test_file1.txt", O_WRONLY, 0644);
    printf("fd: %d\n", fd1);

    // Close the file
    int closeResult1 = DDSPosix::close(fd1);
    int closeResult2 = DDSPosix::close(fd2);
    TEST_ASSERT(closeResult1 == 0 && closeResult2 == 0);
    
    // Try to close an invalid fd
    int closeResult = DDSPosix::close(999);
    printf("closeResult: %d\n", closeResult);
    // TEST_ASSERT(closeResult == -1);
    TEST_ASSERT(errno == EBADF);
    
    TEST_END();
}



// // Test read/write functionality
bool testReadWrite() {
    printf("==============================================\n");
    TEST_START("ReadWrite");
    
    char* testData = (char*) malloc(sizeof(char) * 1048576);
    char* testData2 = (char*) malloc(sizeof(char) * 512);
    int dataLen = 1048576;
    int dataLen2 = 512;
    memset(testData, 0, dataLen);
    memset(testData2, 0, dataLen2);
    const char* message = "I love computer architecture!";
    const char* message2 = "I love computer engineering!";
    memcpy(testData, message, strlen(message));
    memcpy(testData2, message2, strlen(message2));

    // Create a file for writing
    int fd = DDSPosix::open("test_rw.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    TEST_ASSERT(fd >= 3);
    printf("fd: %d\n", fd);

    // Write to the file
    int written = DDSPosix::write(fd, testData, dataLen);
    TEST_ASSERT(written == dataLen);
    
    // Write to the file
    written = DDSPosix::write(fd, testData2, dataLen2);
    TEST_ASSERT(written == dataLen2);

    DDSPosix::lseek(fd, 0, SEEK_SET);
    
    // Read
    char buffer[1048577] = {0};
    ssize_t bytesRead = DDSPosix::read(fd, buffer, sizeof(buffer) - 1);
    printf("bytes read: %ld\n", bytesRead);
    printf("Content: %s\n", buffer);

    char buffer2[513] = {0};
    bytesRead = DDSPosix::read(fd, buffer2, sizeof(buffer2) - 1);
    printf("bytes read: %ld\n", bytesRead);
    printf("Content: %s\n", buffer2);
    
    TEST_ASSERT(bytesRead == static_cast<ssize_t>(dataLen2));
    TEST_ASSERT(strcmp(buffer, testData) == 0);
    
    TEST_ASSERT(DDSPosix::close(fd) == 0);

    free(testData);
    free(testData2);
    TEST_END();
}

bool testRocksDB() {
    printf("==============================================\n");
    TEST_START("RocksDB");

    // Read
    char buffer[513] = {0};
    ssize_t bytesRead = DDSPosix::read(999, buffer, sizeof(buffer) - 1);
    printf("bytes read: %ld\n", bytesRead);
    printf("Content: %s\n", buffer);
    
    TEST_END();
}



// // Clean up after testing
bool teardownTest() {
    TEST_START("Teardown");

    int ret = DDSPosix::shutdown_posix();
    TEST_ASSERT(ret == 1);
    TEST_END();
}

void benchmark() {
    
}

// Run all tests
void runAllTests() {
    std::cout << "============= DDSPosix Test Suite ============" << std::endl;
    
    // setupTest();
    // testRocksDB();
    // testOpenClose();
    testReadWrite();
    // benchmark();
    // testReadWrite();
    // testLseek();
    // testDirectory();
    // testErrorConditions();
    // testAppend();
    // testMultipleFiles();
    // testTruncate();
    
    // teardownTest();
    
    std::cout << "==============================================" << std::endl;
    std::cout << "Test results: " << passedTests << " passed out of " << totalTests << std::endl;
    std::cout << (passedTests == totalTests ? "ALL TESTS PASSED" : "SOME TESTS FAILED") << std::endl;
}

int main() {
    runAllTests();
    return (passedTests == totalTests) ? 0 : 1; // Return non-zero if any tests failed
}