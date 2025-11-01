#ifndef RE2_REGEX_APP_H
#define RE2_REGEX_APP_H

#include <stdio.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

    bool re2_regex_app_driver(
        char* path, 
        char** contig_strings, 
        size_t n_bufs, 
        size_t num_threads
    );
    
#ifdef __cplusplus
}
#endif

#endif  // RE2_REGEX_APP_H