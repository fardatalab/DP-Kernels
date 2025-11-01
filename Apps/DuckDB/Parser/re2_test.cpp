#include <chrono>
#include <fstream>
#include <iostream>
#include <numeric>
#include <re2/stringpiece.h>
#include <thread>

#include <re2/re2.h>

using namespace std;

vector<string> strings;
char** strings_buf;

void worker(size_t start, size_t end, re2::RE2& pattern, size_t* count) {
    size_t temp_count = 0;
    auto st = chrono::high_resolution_clock::now();
    for (size_t i = start; i < end; i++) {
        re2::StringPiece sp(strings_buf[i]);
        temp_count += re2::RE2::PartialMatch(sp, pattern);
    }
    auto et = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = et - st;
    printf("thread %lu spent %.6f sec\n", pthread_self(), elapsed.count());
    *count = temp_count;
}

int main() {
    string dump_path("dump/strings.txt");
    size_t num_threads = 16;
    // char* pattern = ".*regular.*\n";

    size_t num_tuples = 6001215;
    ifstream dump_file(dump_path);
    // vector<char*> strings;
    strings_buf = new char*[num_tuples];
    size_t i = 0;
    string line;
    while (getline(dump_file, line)) {
        // char* str = new char[line.size()+1];
        strings_buf[i] = new char[line.size()+1];
        memcpy(strings_buf[i], line.c_str(), line.size());
        strings_buf[i][line.size()] = 0;
        i++;
    }
    
    vector<thread> threads;
    vector<size_t> counts(num_threads);
    size_t workload = num_tuples / num_threads;
    char* contig_strings[num_threads];
    // re2::StringPiece sps[num_threads];

    re2::RE2 pattern("regular");
    
    auto start = chrono::high_resolution_clock::now();
    for (int i = 0; i < num_threads; i++) {
        size_t st = i * workload;
        size_t et = (i == num_threads-1) ? num_tuples : (i+1) * workload;
        threads.emplace_back(worker, st, et, ref(pattern), &counts[i]);
    }
    for (auto& t : threads) {
        t.join();
    }
    size_t total = accumulate(counts.begin(), counts.end(), 0);
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end - start;
    printf("%lu matches (%.6f)\n", total, elapsed.count());
    
    return 0;
}