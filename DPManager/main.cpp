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

    std::signal(SIGINT, signal_handler);

    printf("DPManager started\n");
    dp_kernel_manager_msgq_start(nullptr);
    // while (!stop_prog)
    // {
    //     }
    printf("DPManager stopped\n");
    return 0;
}