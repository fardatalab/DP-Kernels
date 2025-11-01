// #include "dp_manager.hpp"
#include "dp_manager_msgq.hpp"
#include <csignal>
#include <iostream>

// std::atomic<bool> stop_prog(false);

void signal_handler(int signal)
{
    if (signal == SIGINT)
    {
        dp_kernel_manager_msgq_stop();
        // stop_prog.store(true);
    }
}

int main()
{
    // Pin dpm main thread to core 0 (client threads should be pinned to other cores)
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
        std::cerr << "Error calling pthread_setaffinity_np for thread " << " on core 0"
                  << ": " << strerror(rc) << std::endl;
        return -1;
    }

    std::signal(SIGINT, signal_handler);

    printf("DPManager started\n");
    dp_kernel_manager_msgq_start(nullptr);
    // while (!stop_prog)
    // {
    //     }
    printf("DPManager stopped\n");
    return 0;
}