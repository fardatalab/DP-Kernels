#include "dpm_interface.hpp"
#include <iostream>

#define LOOPS 10
#define DEPTH 32

int main()
{
    dpkernel_completion *completion = new dpkernel_completion();

    // start the manager
    dpm_initialize();
    printf("dpm init completed\n");

    // create a task
    dpkernel_task_base task;
    task.device = dpkernel_device::DEVICE_NULL;
    task.in = (void *)0xdeadbeef;
    task.in_size = 0x1024;
#ifdef TEST_USE_MEMCPY
    memset(task.out, 42, TEST_PAYLOAD_SIZE);
#else
    task.out = (void *)0xfeadbeed;
#endif
    task.out_size = 0x512;
    task.user_data = nullptr;

    // measure the time
    printf("Submitting task\n");
    auto start = std::chrono::high_resolution_clock::now();

    bool ret;
    ulong cnt = 0;
    ulong submitted_cnt = 0;
    ulong completed_cnt = 0;

    while (completed_cnt < LOOPS * DEPTH)
    {
        while (1)
        {
            ret = dpm_submit_task_msgq(&task);
            if (!ret)
            {
                break;
            }
            else
            {
                submitted_cnt++;
            }
        }

        while (1)
        {
            ret = dpm_try_get_completion_msgq(completion);
            if (ret)
            {
                // printf("Completed\n");
                completed_cnt++;
            }
            else
            {
                break;
            }
        }
    }

    // time end
    auto end = std::chrono::high_resolution_clock::now();

    printf("Submitted %lu tasks\n", submitted_cnt);
    printf("Completed %lu tasks\n", completed_cnt);

    std::chrono::duration<double> elapsed = end - start;
    std::cout << "Time taken: " << elapsed.count() << " s\n";

    double throughput_million_ops = (double)cnt / elapsed.count() / 1000000;
    printf("Throughput: %.6f million ops/s\n", throughput_million_ops);

    sleep(1);
    while (1)
    {
        ret = dpm_try_get_completion_msgq(completion);
        if (ret)
        {
            // printf("Completed\n");
            completed_cnt++;
        }
        else
        {
            break;
        }
    }
    printf("Submitted %lu tasks\n", cnt);
    printf("Completed %lu tasks\n", completed_cnt);

    return 0;
}

/* #include <boost/interprocess/ipc/message_queue.hpp>
#include <chrono>
#include <iostream>
#include <thread>

static void run_server(const char *mq_name, unsigned int depth)
{
    boost::interprocess::message_queue::remove(mq_name);
    boost::interprocess::message_queue mq(boost::interprocess::create_only, mq_name, depth, sizeof(int));

    auto start = std::chrono::high_resolution_clock::now();
    unsigned int received = 0;
    int buffer;
    size_t rec_size;
    unsigned priority;

    while (received < depth)
    {
        if (mq.try_receive(&buffer, sizeof(buffer), rec_size, priority))
        {
            ++received;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Server received " << depth << " messages in " << std::chrono::duration<double>(end - start).count()
              << " s\n";
}

static void run_client(const char *mq_name, unsigned int depth)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    boost::interprocess::message_queue mq(boost::interprocess::open_only, mq_name);

    auto start = std::chrono::high_resolution_clock::now();
    for (unsigned int i = 0; i < depth; ++i)
    {
        mq.send(&i, sizeof(i), 0);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Client sent " << depth << " messages in " << std::chrono::duration<double>(end - start).count()
              << " s\n";
}

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        std::cerr << "Usage: " << argv[0] << " [server|client] <queue_depth>\n";
        return 1;
    }

    std::string mode = argv[1];
    unsigned int depth = std::stoi(argv[2]);
    const char *name = "boost_mq_test";

    if (mode == "server")
    {
        run_server(name, depth);
    }
    else if (mode == "client")
    {
        run_client(name, depth);
    }
    else
    {
        std::cerr << "Invalid mode.\n";
    }

    return 0;
} */