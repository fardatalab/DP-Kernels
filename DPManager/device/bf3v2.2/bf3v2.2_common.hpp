#pragma once

#include "device_common.hpp"
#include <doca_buf.h>
#include <doca_regex.h>

/// the common bf3 (DOCA v2.5) extension from the base task struct
struct dpkernel_task_bf3 : dpkernel_task_base
{
    doca_buf *src_doca_buf;
    doca_buf *dst_doca_buf;
};

/// common bf3v2.2 (DOCA v2.2) task struct is the same as bf3 v2.5
struct dpkernel_task_bf3v2_2 : dpkernel_task_bf3
{
};

/// bf3v2.2 regex task struct
struct dpkernel_task_bf3v2_2_regex : dpkernel_task_base
{
    doca_buf *src_doca_buf;
    struct doca_regex_search_result results;

    // uint32_t n_results;
};

/* /// the common bf3 (DOCA v2.5) extension from the base task struct
struct dpkernel_task_bf3 : dpkernel_task_base
{
    doca_buf *src_doca_buf;
    doca_buf *dst_doca_buf;
}; */