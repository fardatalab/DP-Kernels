#ifndef REGEX_APP_H
#define REGEX_APP_H

#include <stdio.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

    bool bf3_regex_app_driver(
        char** contig_strings,
        size_t n_bufs,
        size_t num_threads,
        size_t pipeline_depth
    );

#ifdef __cplusplus
}
#endif

#endif  // REGEX_APP_H
