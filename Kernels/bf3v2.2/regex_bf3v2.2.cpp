#include "regex_bf3v2.2.hpp"
#include "common.hpp"
#include "doca_buf_inventory.h"
#include "doca_error.h"
#include "doca_regex.h"
#include "memory.hpp"
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <pthread.h>

static struct dpm_doca_regex_bf3v2_2 bf3v2_2_regex_state;

static int n_cpus = 0;

static uint32_t *kernel_queue_remaining_capacity;

static uint32_t __get_max_single_task_size()
{
    uint64_t max_size;
    if (doca_regex_get_maximum_non_huge_job_size(doca_dev_as_devinfo(bf3v2_2_regex_state.global_doca_state->state.dev),
                                                 &max_size) == DOCA_SUCCESS)
    {
        printf("bf3v2_2_regex max single task size: %lu\n", max_size);
        // return max_size;
        return 16 * KB;
    }
    else
    {
        printf("bf3v2_2_regex failed to get max single task size failed\n");
        return 0;
    }
}

static long __get_estimated_processing_time(uint32_t input_size, int thread_id)
{
    // TODO
    return 0;
}

static bool __is_coalescing_enabled()
{
    return true;
}

bool bf3v2_2_regex_get_catalogue(dpm_kernel_catalogue *catalogue)
{
    // set info in catalogue
    // catalogue->is_coalescing_enabled = __is_coalescing_enabled;
    catalogue->is_coalescing_enabled = __is_coalescing_enabled();
    // catalogue->get_max_single_task_size = __get_max_single_task_size;
    catalogue->max_single_task_size = __get_max_single_task_size();
    catalogue->get_estimated_processing_time_ns = __get_estimated_processing_time;
    catalogue->kernel_capacity = BF3v2_2_REGEX_WORKQ_DEPTH; // 1 queue per cpu
    return true;
}

/// read the regex rules file from path
/// @param [out] rule_file_size the size of the file
/// @param [out] rule_file_buffer will malloc a buffer which contain the binary regex rule data
bool _read_regex_rule_file(size_t *rule_file_size, char **rule_file_buffer)
{
    FILE *file = fopen(BF3v2_2_REGEX_RULES_FILE, "rb");
    if (!file)
    {
        printf("Failed to open regex rules file\n");
        return false;
    }
    fseek(file, 0, SEEK_END);
    *rule_file_size = ftell(file);
    fseek(file, 0, SEEK_SET);
    *rule_file_buffer = (char *)malloc(*rule_file_size);
    if (!rule_file_buffer)
    {
        printf("Failed to allocate memory for regex rules buffer\n");
        fclose(file);
        return false;
    }
    size_t read_size = fread(*rule_file_buffer, 1, *rule_file_size, file);
    if (read_size != *rule_file_size)
    {
        printf("Failed to read regex rules file\n");
        free(*rule_file_buffer);
        fclose(file);
        return false;
    }
    fclose(file);
    return true;
}

bool bf3v2_2_regex_kernel_init(struct dpm_kernel_catalogue *catalogue, int n_threads)
{
    // set info in catalogue
    // catalogue->is_coalescing_enabled = __is_coalescing_enabled;
    // catalogue->is_coalescing_enabled = __is_coalescing_enabled();
    // // catalogue->get_max_single_task_size = __get_max_single_task_size;
    // // catalogue->max_single_task_size = __get_max_single_task_size(); // BUG segfault, dev not initialized?
    // catalogue->max_single_task_size = 16 * KB;
    // catalogue->get_estimated_processing_time_ns = __get_estimated_processing_time;

    bf3v2_2_regex_state.global_doca_state = &dpm_doca_state;

    // set the cpu mask for the kernel
    n_cpus = n_threads;

    // CPU_ZERO(&result->cpu_mask);
    // for (int i = 0; i < 32; ++i)
    // {
    //     if ((BF3v2_2_REGEX_CPU_MASK >> i) & 1)
    //     {
    //         CPU_SET(i, &result->cpu_mask);
    //         n_cpus++;
    //     }
    // }
    printf("bf3v2_2_regex_kernel_init cpu mask: %x, n_cpus = %d\n", BF3v2_2_REGEX_CPU_MASK, n_cpus);

    // malloc the multithreaded ctxs etc
    kernel_queue_remaining_capacity = (uint32_t *)calloc(n_cpus, sizeof(uint32_t));
    bf3v2_2_regex_state.regexes = (struct doca_regex **)calloc(n_cpus, sizeof(struct doca_regex *));
    bf3v2_2_regex_state.workqs = (struct doca_workq **)calloc(n_cpus, sizeof(struct doca_workq *));
    bf3v2_2_regex_state.ctxs = (struct doca_ctx **)calloc(n_cpus, sizeof(struct doca_ctx *));
    if (bf3v2_2_regex_state.regexes == NULL || bf3v2_2_regex_state.ctxs == NULL || bf3v2_2_regex_state.workqs == NULL ||
        kernel_queue_remaining_capacity == NULL)
    {
        printf("Unable to allocate memory for bf3v2_2 regex state\n");
        return false;
    }

    for (int i = 0; i < n_cpus; i++)
    {
        kernel_queue_remaining_capacity[i] = BF3v2_2_REGEX_WORKQ_DEPTH;
    }

    doca_error_t ret;

    // read the regex rules file
    size_t rule_file_size = 0;
    char *rule_file_buffer = NULL;
    if (_read_regex_rule_file(&rule_file_size, &rule_file_buffer))
    {
        printf("Read regex rules file %s, size %zu\n", BF3v2_2_REGEX_RULES_FILE, rule_file_size);
    }
    else
    {
        printf("Failed to read regex rules file\n");
        return false;
    }

    // for each cpu, create a doca regex engine and ctx and workq
    for (int i = 0; i < n_cpus; i++)
    {
        ret = doca_regex_create(&bf3v2_2_regex_state.regexes[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create regex engine %d: %s", i, doca_get_error_string(ret));
            return false;
        }
        bf3v2_2_regex_state.ctxs[i] = doca_regex_as_ctx(bf3v2_2_regex_state.regexes[i]);

        ret = doca_ctx_dev_add(bf3v2_2_regex_state.ctxs[i], bf3v2_2_regex_state.global_doca_state->state.dev);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3v2.2 regex unable to register device with lib context: %s\n", doca_get_error_string(ret));
            bf3v2_2_regex_state.ctxs[i] = NULL;
            return ret;
        }

        /* Size per workq memory pool */
        ret = doca_regex_set_workq_matches_memory_pool_size(bf3v2_2_regex_state.regexes[i],
                                                            BF3V2_2_REGEX_MATCHES_POOL_SIZE);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable set matches mempool size for ctx %d. Reason: %s\n", i, doca_get_error_string(ret));
            return ret;
        }

        /* Load compiled rules into the RegEx */
        ret = doca_regex_set_hardware_compiled_rules(bf3v2_2_regex_state.regexes[i], rule_file_buffer, rule_file_size);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to set regex rule. Reason: %s\n", doca_get_error_string(ret));
            return ret;
        }

        uint64_t max_size = 0;
        doca_regex_get_maximum_job_size(doca_dev_as_devinfo(bf3v2_2_regex_state.global_doca_state->state.dev),
                                        &max_size);
        printf("regex max job size: %lu\n", max_size);
        doca_regex_get_maximum_non_huge_job_size(doca_dev_as_devinfo(bf3v2_2_regex_state.global_doca_state->state.dev),
                                                 &max_size);
        printf("regex max non-huge job size: %lu\n", max_size);

        ret = doca_ctx_start(bf3v2_2_regex_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Failed to start context %d: %s\n", i, doca_get_error_string(ret));
            return false;
        }

        ret = doca_workq_create(BF3v2_2_REGEX_WORKQ_DEPTH, &bf3v2_2_regex_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to create work queue for regex: %s", doca_get_error_string(ret));
            return false;
        }
        ret = doca_ctx_workq_add(bf3v2_2_regex_state.ctxs[i], bf3v2_2_regex_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to add work queue to context %d: %s\n", i, doca_get_error_string(ret));
            return false;
        }
    }
    return true;
}

bool bf3v2_2_regex_kernel_cleanup()
{
    doca_error_t ret;
    // int n_cpus = 0;
    // for (int i = 0; i < 32; ++i)
    // {
    //     if ((BF3v2_2_REGEX_CPU_MASK >> i) & 1)
    //     {
    //         n_cpus++;
    //     }
    // }

    // cleanup the workq and ctxs
    for (int i = 0; i < n_cpus; i++)
    {
        ret = doca_ctx_workq_rm(bf3v2_2_regex_state.ctxs[i], bf3v2_2_regex_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to remove work queue from context %d: %s", i, doca_get_error_string(ret));
            return false;
        }
        ret = doca_workq_destroy(bf3v2_2_regex_state.workqs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to destroy work queue %d: %s", i, doca_get_error_string(ret));
            return false;
        }
        bf3v2_2_regex_state.workqs[i] = NULL;

        ret = doca_ctx_stop(bf3v2_2_regex_state.ctxs[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to stop context %d: %s", i, doca_get_error_string(ret));
            return false;
        }
        ret = doca_regex_destroy(bf3v2_2_regex_state.regexes[i]);
        if (ret != DOCA_SUCCESS)
        {
            printf("Unable to destroy regex engine: %s", doca_get_error_string(ret));
            return false;
        }
    }

    free(bf3v2_2_regex_state.regexes);
    free(bf3v2_2_regex_state.workqs);
    free(bf3v2_2_regex_state.ctxs);
    free(kernel_queue_remaining_capacity);

    return true;
}

bool bf3v2_2_regex_handle_mem_req(struct dpm_mem_req *req)
{
    doca_error_t ret;
    dpkernel_task_bf3v2_2_regex *dpk_task;

    switch (req->type)
    {
    case DPM_MEM_REQ_ALLOC_INPUT: {
        dpk_task = &((dpkernel_task *)dpm_get_task_ptr_from_shmptr(req->task))->bf3v2_2_regex;
        print_debug("bf3 regex handle mem req alloc input\n");
        char *input_buffer = dpm_get_input_ptr_from_shmptr(dpk_task->in);

        pthread_spin_lock(&dpm_doca_state.buf_inventory_lock);
        ret = doca_buf_inventory_buf_by_addr(bf3v2_2_regex_state.global_doca_state->state.buf_inv,
                                             bf3v2_2_regex_state.global_doca_state->state.src_mmap, input_buffer,
                                             dpk_task->in_size, &dpk_task->src_doca_buf);
        pthread_spin_unlock(&dpm_doca_state.buf_inventory_lock);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 regex failed to get input buf from inventory: %s\n", doca_get_error_string(ret));
            return false;
        }
        ret = doca_buf_set_data(dpk_task->src_doca_buf, input_buffer, dpk_task->in_size);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 regex failed to set input buf data: %s\n", doca_get_error_string(ret));
            return false;
        }
        return true;
    }
    case DPM_MEM_REQ_ALLOC_OUTPUT: {
        dpk_task = &((dpkernel_task *)dpm_get_task_ptr_from_shmptr(req->task))->bf3v2_2_regex;
        // no need for output doca buf, output is regex results
        print_debug("bf3 regex handle mem req alloc output\n");
        // dpk_task->results = (struct doca_regex_search_result *)calloc(1, sizeof(*dpk_task->results));
        return true;
    }
    case DPM_MEM_REQ_ALLOC_COALESCED: { // this is only supposed to be called within DPM process, so no shm_ptr for task
                                        // itself
        dpk_task = &((dpkernel_task *)(req->task))->bf3v2_2_regex;
        print_debug("bf3 regex handle mem req alloc coalesced\n");
        pthread_spin_lock(&dpm_doca_state.buf_inventory_lock);
        ret = doca_buf_inventory_buf_by_addr(
            bf3v2_2_regex_state.global_doca_state->state.buf_inv, bf3v2_2_regex_state.global_doca_state->state.src_mmap,
            dpm_get_input_ptr_from_shmptr(dpk_task->in), dpk_task->in_size, &dpk_task->src_doca_buf);
        if (ret != DOCA_SUCCESS) [[unlikely]]
        {
            printf("bf3 regex failed to get input buf for coalesced task: %s\n", doca_get_error_name(ret));
            pthread_spin_unlock(&dpm_doca_state.buf_inventory_lock);
            return false;
        }
        ret = doca_buf_set_data(dpk_task->src_doca_buf, dpm_get_input_ptr_from_shmptr(dpk_task->in), dpk_task->in_size);
        if (ret != DOCA_SUCCESS) [[unlikely]]
        {
            printf("bf3 regex failed to set input buf data for coalesced task: %s\n", doca_get_error_name(ret));
            pthread_spin_unlock(&dpm_doca_state.buf_inventory_lock);
            return false;
        }
        // also allocate the output buffer
        // dpk_task->results = (struct doca_regex_search_result *)calloc(1, sizeof(*dpk_task->results));
        pthread_spin_unlock(&dpm_doca_state.buf_inventory_lock);

        return true;
    }
    case DPM_MEM_REQ_FREE_INPUT: {
        dpk_task = &((dpkernel_task *)dpm_get_task_ptr_from_shmptr(req->task))->bf3v2_2_regex;
        print_debug("bf3 regex handle mem req free input\n");
        // free the input buffer
        ret = doca_buf_refcount_rm(dpk_task->src_doca_buf, NULL);
        if (ret != DOCA_SUCCESS)
        {
            printf("bf3 regex failed to free input buf: %s\n", doca_get_error_string(ret));
            return false;
        }
        dpk_task->src_doca_buf = NULL;
        return true;
    }
    case DPM_MEM_REQ_FREE_OUTPUT: {
        dpk_task = &((dpkernel_task *)dpm_get_task_ptr_from_shmptr(req->task))->bf3v2_2_regex;
        // free the output matches?? Prob not... have to do it after completion?

        ////incomplete...
        /* int offset = 0;
        struct doca_regex_match *regex_match;
        struct doca_regex_search_result *const result = dpk_task->results;

        if (result->num_matches == 0)
            return DPK_SUCCESS;

        // free the matches one by one, chasing the linked list
        regex_match = result->matches;
        while (regex_match != NULL)
        {
            uint32_t *match_length = (uint32_t *)out_ptr + offset;
            *match_length = regex_match->length;
            offset += sizeof(uint32_t);
            uint32_t *match_start = (uint32_t *)out_ptr + offset;
            *match_start = regex_match->match_start;
            struct doca_regex_match *const to_release_match = regex_match;

            regex_match = regex_match->next;
            doca_regex_mempool_put_obj(result->matches_mempool, to_release_match);
        } */

        // free(dpk_task->results);
        // dpk_task->results = NULL;
        // char *out_ptr = dpm_get_output_ptr_from_shmptr(dpk_task->out);
        // free(out_ptr);
        return true;
    }
    case DPM_MEM_REQ_ALLOC_TASK: {
        [[unlikely]] // should never happen
        // nothing to do
        printf("bf3 decompress deflate handle mem req alloc task, nothing to do\n");
        return true;
    }
    case DPM_MEM_REQ_FREE_TASK: {
        [[unlikely]] // should never happen
        // nothing to do
        printf("bf3 decompress deflate handle mem req free task, nothing to do\n");
        return true;
    }
    }
}

uint32_t bf3v2_2_regex_hw_kernel_remaining_capacity(int thread_id, uint32_t *max_capacity)
{
    if (max_capacity != NULL)
    {
        *max_capacity = BF3v2_2_REGEX_WORKQ_DEPTH;
    }
    return kernel_queue_remaining_capacity[thread_id];
}

dpkernel_error bf3v2_2_regex_kernel_execute(dpkernel_task *task, int thread_id)
{
    dpkernel_task_bf3v2_2_regex *dpk_task = &task->bf3v2_2_regex;
    // get local ptr from shm ptr of in and out buffers
    // char *in = dpm_get_input_ptr_from_shmptr(dpk_task->in);
    // char *out = dpm_get_output_ptr_from_shmptr(dpk_task->out);

    doca_error_t ret;

    // construct the task
    struct doca_regex_job_search job_request;
    job_request = {
        .base =
            (struct doca_job){
                .type = DOCA_REGEX_JOB_SEARCH,
                .flags = DOCA_JOB_FLAGS_NONE,
                .ctx = bf3v2_2_regex_state.ctxs[thread_id],
                .user_data = {.ptr = (void *)task}, // use this to get the actual out size after completion
            },
        .rule_group_ids = {0},
        .buffer = dpk_task->src_doca_buf,
        .result = &dpk_task->results,
        .allow_batching = false,
    };
    ret = doca_workq_submit(bf3v2_2_regex_state.workqs[thread_id], &job_request.base);
    if (ret == DOCA_ERROR_NO_MEMORY)
    {
        print_debug("regex doca queue full, try again later\n");
        // doca_buf_refcount_rm(src_doca_buf, NULL);
        return DPK_ERROR_AGAIN;
    }
    if (ret != DOCA_SUCCESS)
    {
        printf("Failed to submit regex job: %s\n", doca_get_error_string(ret));
        // doca_buf_refcount_rm(src_doca_buf, NULL);
        // doca_buf_refcount_rm(dst_doca_buf, NULL);
        return DPK_ERROR_FAILED;
    }
    kernel_queue_remaining_capacity[thread_id] -= 1;
    return DPK_SUCCESS;
}

dpkernel_error bf3v2_2_regex_kernel_poll(dpkernel_task **task, int thread_id)
{
    dpkernel_task_bf3v2_2_regex *dpk_task;
    doca_error_t ret;
    struct doca_event event = {0};
    ret = doca_workq_progress_retrieve(bf3v2_2_regex_state.workqs[thread_id], &event, DOCA_WORKQ_RETRIEVE_FLAGS_NONE);
    if (ret == DOCA_ERROR_AGAIN)
    {
        // normal case, try again later
        return DPK_ERROR_AGAIN;
    }
    else if (ret == DOCA_SUCCESS)
    {
        kernel_queue_remaining_capacity[thread_id] += 1;
        print_debug("bf3 regex kernel poll got completion\n");
        // got a completion
        *task = (dpkernel_task *)event.user_data.ptr; // we store the task in user_data during execute
        dpk_task = &(*task)->bf3v2_2_regex;
        char *out_ptr = dpm_get_output_ptr_from_shmptr(dpk_task->out);

        // the out buffer should contain the number of matches
        // maybe followed by the matches themselves? in the form of [(len, offset)]?

        int offset = 0;
        struct doca_regex_match *regex_match;
        struct doca_regex_search_result *const result = (struct doca_regex_search_result *)event.result.ptr;
        if (result->status_flags != 0)
        {
            printf("regex search has abnormal status: %lx\n", result->status_flags);
        }

        // write detected matches to the output buffer
        int *num_matches = ((int *)out_ptr);
        *num_matches = result->detected_matches;
        offset += sizeof(int);

        ////
        // if (result->detected_matches != result->num_matches)
        // {
        //     printf("bf3 regex detected matches %d != num matches %d\n", result->detected_matches,
        //     result->num_matches); printf("regex mempool free count = %zu\n",
        //     doca_regex_mempool_get_free_count(result->matches_mempool));
        // }
        ////

        // write the matches to the output buffer

        regex_match = result->matches;
        // XXX: TODO: DON'T FREE regex matches here, do them as a memory request???
        // while (regex_match != NULL)
        // {
        //     if (offset + sizeof(uint32_t) * 2 > dpk_task->out_size) [[unlikely]]
        //     {
        //         printf("Output buffer is too small for regex matches\n");
        //         return DPK_ERROR_FAILED;
        //     }
        //     // uint32_t *match_length = (uint32_t *)out_ptr + offset;
        //     // *match_length = regex_match->length;
        //     // offset += sizeof(uint32_t);
        //     // uint32_t *match_start = (uint32_t *)out_ptr + offset;
        //     // *match_start = regex_match->match_start;
        //     struct doca_regex_match *const to_release_match = regex_match;

        //     regex_match = regex_match->next;
        //     doca_regex_mempool_put_obj(result->matches_mempool, to_release_match);
        // }
        dpk_task->actual_out_size = offset;
        // Let DPM own final task completion signaling for the task returned by poll.
        // The old direct write duplicated dpm_poll_completion's write.
        // dpk_task->completion.store(DPK_SUCCESS, std::memory_order_release);

        ////
#ifdef COALESCING
        dpkernel_task *task_in_chain = (dpkernel_task *)(dpk_task->user_data);
        print_debug("bf3 regex kernel poll set completion for coalesced chain head = %p\n", dpk_task);

        while (task_in_chain != NULL)
        {
            // dpkernel_task *task = (dpkernel_task *)(dpk_task->user_data);
            task_in_chain->base.completion.store(DPK_SUCCESS, std::memory_order_release);
            task_in_chain->base.actual_out_size = dpk_task->actual_out_size;
            print_debug("original task = %p, original in size = %d\n", task_in_chain, task_in_chain->base.in_size);
            task_in_chain = (dpkernel_task *)(task_in_chain->bf3v2_2_regex.user_data);
        }
#endif

// if this is chained, then free the constructed coalesced task
// NO NEED TO free the coalesced task, since it's the head of the chain now, not acquired from the pool
#ifdef COALESCING
        if (dpk_task->is_coalesced_head)
        {
            // coalesce_task_pools[thread_id]->release(*task);
            print_debug("bf3 regex kernel poll free coalesced task head %p\n", *task);
            // NOTE: probably CAN'T mi_free from a dynamically loaded lib, not sure though, could be a mimalloc issue
            // mi_free(dpk_task);
            // mi_free(*task);
        }
        else
        {
            printf("bf3 regex kernel poll got non-head task%p\n", *task);
        }
#endif
        ////

        return DPK_SUCCESS;
    }
    else
    {
        // got a completion
        *task = (dpkernel_task *)event.user_data.ptr; // we store the task in user_data during execute
        printf("Failed to retrieve workq progress: %s", doca_get_error_string(ret));
        return DPK_ERROR_FAILED;
    }
}
