#include "regex_sw.hpp"
#include "common.hpp"
#include "memory.hpp"
#include "memory_common.hpp"
#include <cstddef>
#include <cstdint>
#include <string>

static int n_cpus = 0;

static RE2 *pattern_obj = nullptr;

static long __get_estimated_processing_time(uint32_t input_size, int thread_id)
{
    // TODO
    return (input_size / (100 * MB)) * NANOSECONDS_PER_SECOND;
}

static bool __is_coalescing_enabled()
{
    return true;
}

static uint32_t __get_max_single_task_size()
{
    // NO actual ceiling
    return 1 * GB;
}

bool sw_regex_deflate_get_catalogue(dpm_kernel_catalogue *catalogue)
{
    // set info in catalogue
    catalogue->is_coalescing_enabled = __is_coalescing_enabled;
    catalogue->get_max_single_task_size = __get_max_single_task_size;
    catalogue->get_estimated_processing_time_ns = __get_estimated_processing_time;

    return true;
}

bool sw_regex_deflate_kernel_init(struct dpm_kernel_catalogue *result, int n_threads)
{
    // set info in catalogue
    result->is_coalescing_enabled = __is_coalescing_enabled;
    result->get_max_single_task_size = __get_max_single_task_size;
    result->get_estimated_processing_time_ns = __get_estimated_processing_time;

    n_cpus = n_threads;

    const std::string pattern_str = REGEX_PATTERN;

    // compile the pattern
    RE2::Options options;
    options.set_log_errors(false);
    pattern_obj = new RE2(pattern_str, options);

    if (!pattern_obj->ok())
    {
        std::cerr << "RE2: Failed to compile pattern: " << pattern_str << " | Error: " << pattern_obj->error()
                  << std::endl;
        delete pattern_obj;
        pattern_obj = nullptr;
        return false;
    }

    return true;
}

dpkernel_error sw_regex_deflate_kernel_execute(dpkernel_task *task, int thread_id)
{
    dpkernel_task_base *dpk_task = &task->base;
    char *input_buffer = dpm_get_input_ptr_from_shmptr(dpk_task->in);
    char *output_buffer = dpm_get_output_ptr_from_shmptr(dpk_task->out);

    // Assuming buffer is null-terminated. If you have a length, use re2::StringPiece(buffer, len).
    re2::StringPiece input_sp(input_buffer, dpk_task->in_size);
    re2::StringPiece matched_text_sp; // This will be filled with the text of each match

    uint32_t match_count = 0;
    std::vector<struct regex_match_details> all_matches;
    uint32_t output_buf_offset = sizeof(uint32_t); // the first 4 bytes are reserved for the number of matches
    // Iterate with FindAndConsume
    while (RE2::FindAndConsume(&input_sp, *pattern_obj, &matched_text_sp))
    {
        match_count++;
        *((uint32_t *)(output_buffer + output_buf_offset)) = static_cast<uint32_t>(matched_text_sp.length());
        output_buf_offset += sizeof(uint32_t);
        *((uint32_t *)(output_buffer + output_buf_offset)) =
            static_cast<uint32_t>(matched_text_sp.data() - input_buffer);
        output_buf_offset += sizeof(uint32_t);
    }
    // Store the number of matches at the beginning of the output buffer
    *((uint32_t *)output_buffer) = match_count;

    dpk_task->actual_out_size = output_buf_offset;
    dpk_task->completion.store(DPK_SUCCESS, std::memory_order_release);

    return DPK_SUCCESS;
}

bool sw_regex_deflate_kernel_cleanup()
{
    if (pattern_obj)
    {
        delete pattern_obj;
        pattern_obj = nullptr;
    }
    return true;
}