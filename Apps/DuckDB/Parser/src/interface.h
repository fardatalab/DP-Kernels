#ifndef INTERFACE_H
#define INTERFACE_H

#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>

#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#define NTHREADS (16)

#ifdef __cplusplus
extern "C" {
#endif

    // initializes metadata of a database and its entries
    void db_init(char* path);

    // populates *buf* with complete strings that are stored contiguously
    // and returns the number of complete strings copied into *buf*
    size_t load_strings_in_batch(char* path, size_t nbytes, char* buf, uint8_t* lengths);

    // scans rows of an entire table into memory (assuming 1st table)
    // implemented using batched loading
    size_t scan_table(char* path, size_t batch_size, size_t lengths_buf_size, char*** strings);

    // persists table rows into a file
    void dump_strings_to(const char* dest);

    // returns number of strings that matches the given regex pattern
    size_t count_matches(const char* pattern, size_t nbufs, char** strings, uint8_t** lengths, size_t* strcnt_per_thread);

    // returns the number of tuples table no. tbl_idx has
    size_t get_tuple_count(size_t tbl_idx);

#ifdef __cplusplus
}
#endif

#endif  // INTERFACE_H
