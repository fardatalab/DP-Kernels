#include "dp_manager_msgq.hpp"
#include <iostream>

std::atomic<bool> polling_loop{true};
// extern std::thread submission_thread;
// extern std::thread completion_thread;
// use a single thread
// std::thread manager_thread;

/// boost message queue for submission
boost::interprocess::message_queue *dpm_submission_queue;
////boost::interprocess::message_queue *dpm_completion_queue;

/// MPMC queue for submission and completion
using rigtorp::MPMCQueue;
MPMCQueue<shm_ptr> *dpm_submission_queue_mpmc;

// will be nullptr if successfully consumed an item from submission queue, otherwise will be `_mq_polled_submission`
// dpkernel_task_base *mq_polled_submission;
shm_ptr *mq_polled_submission;
// dpkernel_task_base _mq_polled_submission;
shm_ptr _mq_polled_submission;

uint active_kernels[dpkernel_name::KERNEL_NAME_LAST] = {0, 0, 0, 0};
// let the application poll on dpkernel_task_base in shared memory for completion
/* dpkernel_completion *mq_polled_completion;
dpkernel_completion _mq_polled_completion; */

int dp_kernel_manager_msgq_start(void *args)
{
    ////_create_msg_queue_boost();
    _create_msg_queue_mpmc();

    mq_polled_submission = NULL;
    _mq_polled_submission = {};

    // mq_polled_completion = nullptr;
    // _mq_polled_completion;

    /* submission_thread = std::thread([]() {
        while (polling_loop)
        {
            dpm_poll_submission_msgq();
        }
    });

    completion_thread = std::thread([]() {
        while (polling_loop)
        {
            dpm_poll_completion_msgq();
        }
    }); */

    printf("dpm_mem_init\n");
    dpm_mem_init();
    printf("dpm_mem_init done\n");

    int i = 0;
    while (polling_loop)
    {
        dpm_poll_mem_req();
        while (i < 1000000)
        {
            i++;
            dpm_poll_submission_msgq();
        }
        i = 0;
        /// DPM polls for completion and sets the flag in dpkernel_task_base, then the application can poll for
        /// completion
        dpm_poll_completion_msgq();
    }
    // manager_thread = std::thread([]() {

    // });
    // manager_thread.detach();

    return 0;
}

void _create_msg_queue_boost()
{
    boost::interprocess::message_queue::remove(SUBMISSION_QUEUE_NAME);
    boost::interprocess::message_queue::remove(COMPLETION_QUEUE_NAME);
    dpm_submission_queue = new boost::interprocess::message_queue(boost::interprocess::create_only,
                                                                  SUBMISSION_QUEUE_NAME, // name
                                                                  DPM_SUBMISSION_QUEUE_SIZE,
                                                                  // sizeof(dpkernel_task_base) // max message size
                                                                  sizeof(shm_ptr) // only pass shm_ptr now
    );
    /* dpm_completion_queue = new boost::interprocess::message_queue(boost::interprocess::create_only,
                                                                  COMPLETION_QUEUE_NAME, // name
                                                                  DPM_SUBMISSION_QUEUE_SIZE,
                                                                  sizeof(dpkernel_completion) // max message size
    ); */

    printf("submission queue get_max_msg_size: %lu\n", dpm_submission_queue->get_max_msg_size());
    printf("get_max_msg_size: %lu\n", dpm_submission_queue->get_max_msg_size());
}

bool _create_msg_queue_mpmc()
{
    std::string name = std::string(SUBMISSION_QUEUE_BACKING_SHM_NAME);
    std::string q_name = std::string(SUBMISSION_QUEUE_NAME);
    // Create shared memory object
    int shm_fd = shm_open(q_name.c_str(), O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1)
    {
        perror("create mpmc queue shm_open failed");
        return false;
    }

    // Define the size of shared memory object
    size_t shm_size = sizeof(rigtorp::MPMCQueue<shm_ptr>);
    if (ftruncate(shm_fd, shm_size) == -1)
    {
        perror("create mpmc queue ftruncate failed");
        return false;
    }

    // Map shared memory object
    void *ptr = mmap(0, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (ptr == MAP_FAILED)
    {
        perror("create mpmc queue mmap failed");
        return false;
    }

    // Construct MPMCQueue in-place in shared memory
    new (ptr) rigtorp::MPMCQueue<shm_ptr>(DPM_SUBMISSION_QUEUE_SIZE, name);
    dpm_submission_queue_mpmc = (rigtorp::MPMCQueue<shm_ptr> *)ptr;
    // printf("dpm_submission_queue_mpmc created\n", dpm_submission_queue_mpmc->slots_);
    return true;
}

bool _dpm_execute_task_msgq()
{
    print_debug("mq_polled_submission shm_ptr = %lu\n", *mq_polled_submission);
    dpkernel_task_base *task = dpm_get_task_ptr_from_shmptr(*mq_polled_submission);
    print_debug("got task from shm_ptr: %p\n", task);
    // task->completion.store(DPK_ONGOING); // TODO: shouldn't need this here, submission should have set this already
    switch ((enum dpkernel_name)task->name)
    {
    case DECOMPRESS_DEFLATE_DPKERNEL:
        print_debug("decompress deflate\n");
        if (decompress_executor_shm_execute(task))
        {
            active_kernels[DECOMPRESS_DEFLATE_DPKERNEL]++;
            return true;
        }
        else
        {
            print_debug("decompress deflate failed\n");
            return false;
        }
    case NULL_KERNEL:
        print_debug("null kernel\n");
        if (null_executor_shm_execute(task))
        {
            active_kernels[NULL_KERNEL]++;
            return true;
        }
        else
        {
            print_debug("null kernel failed\n");
            return false;
        }
    default:
        print_debug("unknown kernel %d\n", (int)task->name);
        return false;
    }
}

// TODO: returned value shouldn't really matter??
/* bool dpm_poll_submission_msgq()
{
    ulong recvd_size;
    uint priority;
    bool ret;

    // common case
    if (mq_polled_submission == nullptr)
    {
        mq_polled_submission = &_mq_polled_submission;
        if (dpm_submission_queue->try_receive(mq_polled_submission, sizeof(dpkernel_task_base), recvd_size, priority))
        {
            printf("got submission\n");
            if (mq_polled_submission->in != (void *)0xdeadbeef)
            {
                printf("dpm_poll_submission_msgq unexpected in: %p\n", mq_polled_submission->in);
            }
            ret = _dpm_execute_task_msgq();
            if (ret)
            {
                // printf("executed task\n");
                mq_polled_submission = nullptr;
            }
            // else: couldn't enqueue/execute task, try again next time (next poll goes to the next else branch)
            else
            {
                printf("couldn't enqueue/execute task\n");
            }
        }
        else
        {
            // printf("no submission\n");
            mq_polled_submission = nullptr;
        }
    }
    else // didn't finish execute task
    {
        printf("didn't finish execute task, try again in this poll\n");
        ret = _dpm_execute_task_msgq();
        if (ret)
        {
            mq_polled_submission = nullptr;
        }
    }
    return ret;
} */

// now uses shared memory of dpkernel_task_base for both submission and completion (polling)
bool dpm_poll_submission_msgq()
{
    ulong recvd_size;
    uint priority;
    bool ret = false;

    // common case, last time got a submission and successfully executed it
    if (mq_polled_submission == NULL)
    {
        mq_polled_submission = &_mq_polled_submission;

        // if (dpm_submission_queue->try_receive(mq_polled_submission, sizeof(dpkernel_task_base *), recvd_size,
        // priority))
        if (dpm_submission_queue_mpmc->try_pop(*mq_polled_submission))
        {
            print_debug("got submission\n");
            ret = _dpm_execute_task_msgq();
            if (ret)
            {
                mq_polled_submission = NULL;
            }
            else
            {
                print_debug("couldn't enqueue/execute task\n");
            }
        }
        else // didn't receive anything
        {
            mq_polled_submission = NULL;
        }
    }
    else // didn't finish execute task
    {
        // printf("didn't finish execute task, try again in this poll\n");
        ret = _dpm_execute_task_msgq();
        if (ret)
        {
            mq_polled_submission = NULL;
        }
        // else, the next time we still try to execute the task
        else
        {
            print_debug("couldn't enqueue/execute task (again)\n");
        }
    }
    return ret;
}

// XXX: deprecated, let the application poll for completion on dpkernel_task_base in shared memory
/* bool dpm_poll_completion_msgq()
{
    bool ret;
    if (mq_polled_completion == nullptr)
    {
        mq_polled_completion = &_mq_polled_completion;
        if (null_executor_msgq_poller(mq_polled_completion))
        {
            // printf("got completion\n");

            ret = dpm_completion_queue->try_send(mq_polled_completion, sizeof(dpkernel_completion), 0);
            if (ret)
            {
                mq_polled_completion = nullptr;
            }
        }
        else
        {
            // printf("no completion\n");
            mq_polled_completion = nullptr;
        }
    }
    else
    {
        ret = dpm_completion_queue->try_send(mq_polled_completion, sizeof(dpkernel_completion), 0);
        if (ret)
        {
            mq_polled_completion = nullptr;
        }
    }

    return ret;
} */

// the polling loop should complete the task, and then set the flag in dpkernel_task_base, then the application
// can poll for completion
bool dpm_poll_completion_msgq()
{
    // TODO: for each ACTIVE kernel, poll for completion

    struct dpkernel_task_base *task = NULL;

    if (active_kernels[dpkernel_name::DECOMPRESS_DEFLATE_DPKERNEL] && decompress_executor_shm_poller(&task))
    {
        print_debug("poll got decompress completion\n");
        print_debug("actual_out_size: %lu\n", task->actual_out_size);
        active_kernels[dpkernel_name::DECOMPRESS_DEFLATE_DPKERNEL]--;
        return true;
    }
    if (active_kernels[dpkernel_name::NULL_KERNEL] && null_executor_shm_poller(&task))
    {
        print_debug("poll got null completion\n");
        print_debug("actual_out_size: %lu\n", task->actual_out_size);
        active_kernels[dpkernel_name::NULL_KERNEL]--;
        return true;
    }

    return false; // TODO: can just return void
}

int dp_kernel_manager_msgq_stop()
{
    polling_loop.store(false);
    nanosleep((const struct timespec[]){{0, 100000000L}}, NULL); // 100ms

    return cleanup_dpm_msgq();
}

bool cleanup_dpm_msgq()
{
    bool ret;
    ret = boost::interprocess::message_queue::remove(SUBMISSION_QUEUE_NAME);
    ret &= boost::interprocess::message_queue::remove(COMPLETION_QUEUE_NAME);

    /// close shared memory
    if (munmap(dpm_submission_queue_mpmc, sizeof(*dpm_submission_queue_mpmc)) == -1)
    {
        perror("munmap dpm_submission_queue_mpmc");
        // return -1;
    }
    // close(dpm_submission_queue_mpmc->allocator_.shm_fd);
    if (shm_unlink(SUBMISSION_QUEUE_BACKING_SHM_NAME) == -1)
    {
        // perror("shm_unlink");
        return -1;
    }

    // cleanup memory
    printf("cleanup bluefield\n");
    ret &= cleanup_bluefield();

    /// cleanup shared memory
    printf("cleanup shared memory\n");
    printf("teardown_mimalloc dpm_req_ctx_shm\n");
    teardown_mimalloc(&dpm_req_ctx_shm, DPM_REQ_CTX_SHM_NAME);
    printf("teardown_mimalloc dpm_own_mem_region.input_region\n");
    teardown_mimalloc(&dpm_own_mem_region.input_region, DPM_SHM_INPUT_REGION_NAME);
    printf("teardown_mimalloc dpm_own_mem_region.output_region\n");
    teardown_mimalloc(&dpm_own_mem_region.output_region, DPM_SHM_OUTPUT_REGION_NAME);
    printf("cleaned up shared memory\n");
    return ret;
}
