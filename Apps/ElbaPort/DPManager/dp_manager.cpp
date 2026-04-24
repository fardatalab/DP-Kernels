#include "dp_manager.hpp"
#include <iostream>
#include <thread>

int shmFd;
dpm_submission_completion *submission_completion_shm;

std::atomic<bool> polling_loop(true);
std::thread submission_thread;
std::thread completion_thread;

int dp_kernel_manager_start(void *args)
{
    setup_submission_completion_shm();
    bool ret;

    submission_thread = std::thread([]() {
        while (polling_loop)
        {
            dpm_poll_submission();
        }
    });

    completion_thread = std::thread([]() {
        while (polling_loop)
        {
            dpm_poll_completion();
        }
    });
    submission_thread.detach();
    completion_thread.detach();

    return 0;
}

int setup_submission_completion_shm()
{
    shmFd = shm_open(SHM_NAME_SUBMISSION_COMPLETION, O_CREAT | O_RDWR, 0666);
    if (shmFd == -1)
    {
        perror("shm_open");
        return -1;
    }

    if (ftruncate(shmFd, sizeof(dpm_submission_completion)) == -1)
    {
        perror("ftruncate");
        close(shmFd);
        return -1;
    }

    submission_completion_shm = (dpm_submission_completion *)mmap(nullptr, sizeof(dpm_submission_completion),
                                                                  PROT_READ | PROT_WRITE, MAP_SHARED, shmFd, 0);

    if (submission_completion_shm == MAP_FAILED)
    {
        perror("mmap");
        close(shmFd);
        return -1;
    }
    // memset(submission_completion_shm, 0, sizeof(*submission_completion_shm));
    // initialize submission_completion_shm using constructor
    new (submission_completion_shm) dpm_submission_completion();

    return 0;
}

bool _dpm_execute_task()
{
    // use the shared mem to get the task
    auto &sub = submission_completion_shm->submission;

    // // lock the mutex // already locked by poll_submission
    // sub.mutex.lock();

    if (sub.task.device == dpkernel_device::DEVICE_NULL)
    {
        // invoke null executor, it is async
        // this is just passing on (copying) the arguments
        // the executor may not be able to actually execute the task (is full, etc)
        while (
            !null_executor_execute(sub.task.in, sub.task.in_size, sub.task.out, &sub.task.out_size, sub.task.user_data))
        {
            // Retry until null_executor returns true
        }
    }
    /* else if (submission->task.device == dpkernel_device::DEVICE_SOFTWARE)
    {

    }
    else {

    } */

    // already unlocked by poll_submission
    // sub.mutex.unlock();
    return true;
}

bool dpm_poll_submission()
{
    auto &shm_sub = submission_completion_shm->submission;

    // execute the task
    shm_sub.mutex.lock();

    if (shm_sub.is_ready)
    {
        shm_sub.is_ready = false;
        // execute the task
        _dpm_execute_task();
        shm_sub.is_ready = true;
        shm_sub.mutex.unlock();
        return true;
    }

    shm_sub.mutex.unlock();
    return false;
}

bool dpm_poll_completion()
{
    dpm_completion &shm_comp = submission_completion_shm->completion;

    // TODO: poll on all pollers, currently only null executor?
    // shouldn't poll on device??
    // for all pollers
    // {
    dpkernel_completion *completion;

    completion = null_executor_poller();
    if (completion != nullptr) // got a completion
    {
        // if shm_comp is free to accommodate a new completion
        if (shm_comp.mutex.try_lock())
        {
            // if it is not in use
            if (shm_comp.completion.error.load() == DPK_NONE)
            {
                // copy the completion to shared mem
                shm_comp.completion.user_data = completion->user_data;
                shm_comp.completion.out = completion->out;
                shm_comp.completion.out_size = completion->out_size;
                shm_comp.completion.error = completion->error.load();

                // executor can using ONGOING to indicate that its own completions can be used (again)
                completion->error.store(DPK_ONGOING); // restore, clear the completion of the executor's

                shm_comp.mutex.unlock();
                return true;
            }
            else
            {
                // if it is in use, return false
                shm_comp.mutex.unlock();
                return false;
            }
        }
        else // someone else (consume_completion) has the lock
        {
            return false;
        }
    }
    /* else  // other pollers */
    return false;
    // }
}

int cleanup_submission_completion_shm()
{
    if (munmap(submission_completion_shm, sizeof(dpm_submission_completion)) == -1)
    {
        perror("munmap");
        return -1;
    }
    close(shmFd);
    if (shm_unlink(SHM_NAME_SUBMISSION_COMPLETION) == -1)
    {
        perror("shm_unlink");
        return -1;
    }
    return 0;
}

int dp_kernel_manager_stop()
{
    polling_loop.store(false);
    try
    {
        submission_thread.join();
        completion_thread.join();
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << '\n';
        // TODO: mutex problem??
    }
    // submission_thread.join();
    // completion_thread.join();
    printf("threads joined\n");

    return cleanup_submission_completion_shm();
}
