#include "src/interface.h"
#include <iostream>
#include <vector>
#include <re2/re2.h>

static size_t strings_buf_size = 16 * 1024;  // 16 KB
static size_t lengths_buf_size = 1024;
static char* file_path = "/home/ubuntu/dbfiles/sf1.db";
static size_t max_bufs_per_batch = 1000;
static std::vector<char*> contig_strings;
static char* pattern = ".*regular.*\n";

size_t re2_count_matches(size_t bid, size_t start_buf, size_t end_buf) {
    size_t total = 0;
    for (size_t i = start_buf; i < end_buf; i++) {
        re2::StringPiece sp(contig_strings[i], strlen(contig_strings[i]));
        while (re2::RE2::FindAndConsume(&sp, pattern)) {
            total++;
        }
    }
    return total;
}

int main() {
    db_init(file_path);
    while (true) {
        char* strings = new char[strings_buf_size];
        uint8_t* lengths = new uint8_t[lengths_buf_size];
        size_t nstrs = load_strings_in_batch(file_path, strings_buf_size, strings, lengths);
        if (nstrs == 0) {
            break;
        }
        contig_strings.push_back(strings);
    }
    printf("no. buffers: %lu\n", contig_strings.size());

    size_t n_batches = contig_strings.size() / max_bufs_per_batch;
    if (n_batches * max_bufs_per_batch < contig_strings.size()) {
        n_batches++;
    }
    size_t last_batch_size = contig_strings.size() - (n_batches-1) * max_bufs_per_batch;
    printf("no. batches: %lu\n", n_batches);

    for (size_t b = 0; b < n_batches; b++) {
        size_t batch_size = (b == n_batches-1) ? last_batch_size : max_bufs_per_batch;
        size_t start_buf = b * max_bufs_per_batch;
        size_t end_buf = start_buf + batch_size;
        size_t partial_match_count = re2_count_matches(b, start_buf, end_buf);
        printf("%lu matches found in batch %lu\n", partial_match_count, b);
    }

    for (size_t i = 0; i < contig_strings.size(); i++) {
        delete[] contig_strings[i];
    }
    
    return 0;
}