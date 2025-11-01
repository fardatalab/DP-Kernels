#include "interface.h"
#include "include/database/database.hpp"
#include <chrono>
#include <fstream>
#include <re2/re2.h>
#include <re2/stringpiece.h>

Database* db = nullptr;
Table* table = nullptr;
size_t str_count = 0;

extern "C" void db_init() {
    db = new Database(DBFILE);
    db->LoadExistingDatabase();
}

extern "C" size_t scan_table(char*** strings, uint8_t*** lengths, size_t** string_counts_per_buf) {
    std::vector<char*> parsed_strings = {};
    std::vector<uint8_t*> string_lengths = {};
    std::vector<size_t> string_counts = {};
    char* strings_buf;
    uint8_t* lengths_buf;
    size_t n_strings;
    int n_bufs = 0;
    while (1) {
        strings_buf = new char[BATCH_SIZE];  // includes null terminator
        lengths_buf = new uint8_t[STRLEN_ARR_SIZE];
        n_strings = load_strings_in_batch(BATCH_SIZE, strings_buf, lengths_buf);
        if (n_strings == 0) {
            break;
        }
        parsed_strings.push_back(strings_buf);
        string_lengths.push_back(lengths_buf);
        string_counts.push_back(n_strings);
        n_bufs++;
    }
    *strings = new char*[n_bufs];
    *lengths = new uint8_t*[n_bufs];
    *string_counts_per_buf = new size_t[n_bufs];
    for (int i = 0; i < n_bufs; i++) {
        char* parsed_string = parsed_strings[i];
        uint8_t* string_length = string_lengths[i];
        (*strings)[i] = new char[strlen(parsed_string)+1];
        memcpy((*strings)[i], parsed_string, strlen(parsed_string));
        (*lengths)[i] = new uint8_t[string_counts[i]];
        memcpy((*lengths)[i], string_length, string_counts[i]);
        (*string_counts_per_buf)[i] = string_counts[i];
    }
    return n_bufs;
}

void regex_worker(const char* pattern, char** strings, uint8_t** lengths, size_t* strcnt_per_thread, size_t start, size_t end, size_t* partial_count) {
    re2::RE2 pattern_obj(pattern);
    size_t temp = 0;
    for (size_t i = start; i < end; i++) {
        re2::StringPiece sp(strings[i]);
        while (re2::RE2::FindAndConsume(&sp, pattern_obj)) {
            temp++;
        }
    }
    *partial_count = temp;
}

extern "C" size_t count_matches(const char* pattern, size_t nbufs, char** strings, uint8_t** lengths, size_t* strcnt_per_thread) {
    size_t nbufs_per_thread = nbufs / NTHREADS;
    size_t partial_counts[NTHREADS] = {0};
    vector<thread> threads;

    // auto start = chrono::high_resolution_clock::now();
    // get partial counts
    for (int i = 0; i < NTHREADS; i++) {
        size_t start = i * nbufs_per_thread;
        size_t end = (i == NTHREADS - 1) ? nbufs : (i+1) * nbufs_per_thread;
        threads.emplace_back(regex_worker, pattern, strings, lengths, strcnt_per_thread, start, end, &partial_counts[i]);
    }
    for (auto& t : threads) {
        t.join();
    }

    // aggregate for total number of matches
    size_t match_count = 0;
    for (int i = 0; i < NTHREADS; i++) {
        match_count += partial_counts[i];
    }

    // auto end = chrono::high_resolution_clock::now();
    // chrono::duration<double> elapsed = end - start;
    // std::cout << "[count_matches] " << elapsed.count() << " sec\n";
    return match_count;
}

extern "C" void dump_strings_to(const char* dest) {
    ofstream dump_file(dest);
    assert(dump_file);
    size_t nstrs;
    char buf[BATCH_SIZE] = {0};
    uint8_t lengths[STRLEN_ARR_SIZE] = {0};

    while ((nstrs = load_strings_in_batch(BATCH_SIZE, buf, lengths))) {
        size_t offset = 0;
        for (size_t i = 0; i < nstrs; i++) {
            string curr_str(buf + offset, lengths[i]);
            dump_file << curr_str;  // newline character assume
            offset += lengths[i];
        }
    }

    dump_file.close();
    printf("strings dumped to %s, file stream closed\n", dest);
}

extern "C" size_t load_strings_in_batch(size_t nbytes, char* buf, uint8_t* lengths) {
    assert(db);
    table = db->GetTable(0);
    assert(table);
    memset(buf, 0, nbytes);
	size_t n_strs = table->LoadData(DBFILE, nbytes, buf, lengths);
    return n_strs;
}

extern "C" size_t get_tuple_count(size_t tbl_idx) {
    /// @todo for now just assume tbl_idx == 0
    /// @note don't want to touch the table class, so the cast here...
    assert(table);
    return static_cast<size_t>(table->GetRowCount());
}
