#include "interface.h"
#include "include/database/database.hpp"
#include <chrono>
#include <fstream>
#include <sched.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

Database* db = nullptr;
Table* table = nullptr;
size_t str_count = 0;

extern "C" void db_init(char* path) {
    db = new Database(path);
    db->LoadExistingDatabase();
}

extern "C" size_t scan_table(char* path, size_t batch_size, size_t lengths_buf_size, char*** strings) {
    std::vector<char*> parsed_strings = {};
    // std::vector<uint8_t*> string_lengths = {};
    // std::vector<size_t> string_counts = {};
    char* strings_buf;
    uint8_t* lengths_buf;
    size_t n_strings;
    int n_bufs = 0;
    while (1) {
        strings_buf = new char[batch_size];  // includes null terminator
        lengths_buf = new uint8_t[lengths_buf_size];
        n_strings = load_strings_in_batch(path, batch_size, strings_buf, lengths_buf);
        if (n_strings == 0) {
            break;
        }
        parsed_strings.push_back(strings_buf);
        // string_lengths.push_back(lengths_buf);
        // string_counts.push_back(n_strings);
        n_bufs++;
    }
    *strings = new char*[n_bufs];
    // *lengths = new uint8_t*[n_bufs];
    // *string_counts_per_buf = new size_t[n_bufs];
    for (int i = 0; i < n_bufs; i++) {
        char* parsed_string = parsed_strings[i];
        // uint8_t* string_length = string_lengths[i];
        (*strings)[i] = new char[strlen(parsed_string)+1];
        memcpy((*strings)[i], parsed_string, strlen(parsed_string));
        // (*lengths)[i] = new uint8_t[string_counts[i]];
        // memcpy((*lengths)[i], string_length, string_counts[i]);
        // (*string_counts_per_buf)[i] = string_counts[i];
    }
    return n_bufs;
}

void regex_worker(re2::RE2* pattern, char** strings, uint8_t** lengths, size_t* strcnt_per_thread, size_t start, size_t end, size_t* partial_count) {
    auto st = chrono::high_resolution_clock::now();
    // re2::RE2 pattern_obj(pattern);
    size_t temp = 0;
    for (size_t i = start; i < end; i++) {
        re2::StringPiece sp(strings[i], strlen(strings[i]));
        while (re2::RE2::FindAndConsume(&sp, *pattern)) {
            temp++;
        }
        // char* cont_strs = strings[i];
        // uint8_t* partial_lens = lengths[i];
        // size_t strcnt = strcnt_per_thread[i];
        // size_t offset = 0;
        // for (size_t j = 0; j < strcnt; j++) {
        //     string src(cont_strs + offset, partial_lens[j]);
        //     temp += re2::RE2::FullMatch(src, *pattern);
        //     offset += partial_lens[j];
        // }
    }
    *partial_count = temp;
    auto et = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = et - st;
    std::cout << "[regex_worker] " << elapsed.count() << " sec\n";
}

extern "C" size_t count_matches(const char* pattern, size_t nbufs, char** strings, uint8_t** lengths, size_t* strcnt_per_thread) {
    re2::RE2 pattern_obj(pattern);
    size_t nbufs_per_thread = nbufs / NTHREADS;
    // size_t partial_counts[NTHREADS] = {0};
    vector<size_t> partial_counts(NTHREADS);
    vector<thread> threads;

    auto start = chrono::high_resolution_clock::now();
    // get partial counts
    for (int i = 0; i < NTHREADS; i++) {
        size_t start = i * nbufs_per_thread;
        size_t end = (i == NTHREADS - 1) ? nbufs : (i+1) * nbufs_per_thread;
        // regex_worker(&pattern_obj, strings, lengths, strcnt_per_thread, start, end, &partial_counts[i]);
        threads.emplace_back(regex_worker, &pattern_obj, strings, lengths, strcnt_per_thread, start, end, &partial_counts[i]);
    }
    for (auto& t : threads) {
        t.join();
    }

    // aggregate for total number of matches
    size_t match_count = accumulate(partial_counts.begin(), partial_counts.end(), 0);
    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end - start;
    std::cout << "[count_matches] " << elapsed.count() << " sec\n";
    return match_count;
}

extern "C" size_t load_strings_in_batch(char* path, size_t nbytes, char* buf, uint8_t* lengths) {
    assert(db);
    table = db->GetTable(0);
    assert(table);
    memset(buf, 0, nbytes);
	size_t n_strs = table->LoadData(path, nbytes, buf, lengths);
    return n_strs;
}

extern "C" size_t get_tuple_count(size_t tbl_idx) {
    /// @todo for now just assume tbl_idx == 0
    /// @note don't want to touch the table class, so the cast here...
    assert(table);
    return static_cast<size_t>(table->GetRowCount());
}

// extern "C" void dump_strings_to(const char* dest) {
//     ofstream dump_file(dest);
//     assert(dump_file);
//     size_t nstrs;
//     char buf[BATCH_SIZE] = {0};
//     uint8_t lengths[STRLEN_ARR_SIZE] = {0};

//     while ((nstrs = load_strings_in_batch(BATCH_SIZE, buf, lengths))) {
//         size_t offset = 0;
//         for (size_t i = 0; i < nstrs; i++) {
//             string curr_str(buf + offset, lengths[i]);
//             dump_file << curr_str;  // newline character assume
//             offset += lengths[i];
//         }
//     }

//     dump_file.close();
//     printf("strings dumped to %s, file stream closed\n", dest);
// }
