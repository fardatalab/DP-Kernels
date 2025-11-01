#include "page_server.hpp"

#include <atomic>
#include <csignal>
#include <cstddef>
#include <iostream>

// using namespace pd3::network_hashmap;
using namespace dpk::page_server;

std::atomic_bool running{true};

void SignalHandler(int signal)
{
    running = false;
}

int main(int argc, char *argv[])
{
    std::cout << "Starting page server" << std::endl;

    PageServer page_server;

    Config config;
    // make sure local capacity is the same as database size
    config.local_capacity = __LOCAL_CAPACITY; // 1_GiB;
    config.database_size = __DATABASE_SIZE;   // 4_GiB;
    config.num_threads = N_SERVER_THREADS;
    config.server_ip = "0.0.0.0";
    config.server_port = 12345;
    config.batch_size = 1024;
    page_server.Configure(config);

    page_server.Run();

    signal(SIGINT, SignalHandler);
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGTERM, SignalHandler);

    while (running)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    page_server.Stop();
    std::cout << "Server stopped" << std::endl;

    return 0;
}