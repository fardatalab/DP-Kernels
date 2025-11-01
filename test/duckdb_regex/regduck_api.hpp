#pragma once

#include "parser/include/database/database.hpp"

#define MAX_BATCH_SIZE (1 << 14)

extern Table *table;
/*
    Returns multiple strings as contiguous bytes. The total size of returned strings
    is limited by *batch_size*, and all returned strings are complete, so it's possible
    that the actual number of bytes returned is less than *batch_size*
*/
size_t load_strings_in_batch(char *batch_buf, size_t batch_size);