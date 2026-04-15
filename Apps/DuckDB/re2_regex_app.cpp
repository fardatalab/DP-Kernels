#include "re2_regex_app.h"
#include <re2/re2.h>
#include <chrono>
#include <fstream>
#include <numeric>
#include <iostream>
#include <thread>
#include <vector>

static char** contig_strings_ptr = nullptr;

void worker_func(int tid, char* pattern, size_t start, size_t end, size_t* result) {
    re2::RE2 pattern_obj(pattern);
    size_t temp = 0;
    for (size_t i = start; i < end; i++) {
        re2::StringPiece sp(contig_strings_ptr[i], strlen(contig_strings_ptr[i]));
        while (re2::RE2::FindAndConsume(&sp, pattern_obj)) {
            temp++;
        }
    }
    *result = temp;
}

extern "C" bool re2_regex_app_driver(char* path, char** contig_strings, size_t n_bufs, size_t num_threads) {
    char* pattern = ".*regular.*\n";
    contig_strings_ptr = contig_strings;
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file) {
        printf("[re2_regex_app_driver] failed to open %s, abort\n", path);
        return false;
    }
    size_t file_size = file.tellg();
    file.close();
    
    printf("no. threads: %lu\n", num_threads);
    std::vector<std::thread> threads;
    std::vector<size_t> per_thread_counts(num_threads);
    size_t workload = n_bufs / num_threads;

    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_threads; i++) {
        size_t start = i * workload;
        size_t end = (i == num_threads-1) ? n_bufs : (i+1) * workload;
        threads.emplace_back(worker_func, i, pattern, start, end, &per_thread_counts[i]);
    }
    for (auto& t : threads) {
        t.join();
    }
    size_t total_match_count = std::accumulate(per_thread_counts.begin(), per_thread_counts.end(), 0);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    double latency_sec = elapsed.count();
    double throughput_GB_sec = (file_size * 1.0) / (1024 * 1024 * 1024) / latency_sec;
    
    printf("%lu matches found\nlatency: %.6f sec\nthroughput: %.6f GB/sec\n",
            total_match_count, latency_sec, throughput_GB_sec);
    return true;
}