#include "common.hpp"
#include "dpm_interface.hpp"
#include <iostream>

#define LOOPS 10
#define DEPTH 32

// #define INPUT_SIZE 4096
#define OUTPUT_SIZE 4096
#define INPUT_FILE "4K.deflate"

int INPUT_SIZE = -1;
char *input_buf = NULL;

int main()
{
    // start the manager
    dpm_initialize();
    printf("dpm init completed\n");

    dpkernel_task_base *task;

    // read input deflate file
    FILE *fp = fopen(INPUT_FILE, "rb");
    if (fp == NULL)
    {
        printf("Failed to open file\n");
        return -1;
    }
    fseek(fp, 0, SEEK_END);
    INPUT_SIZE = ftell(fp);
    input_buf = (char *)malloc(INPUT_SIZE);
    fseek(fp, 0, SEEK_SET);
    fread(input_buf, 1, INPUT_SIZE, fp);
    fclose(fp);

#ifdef TEST_USE_MEMCPY
    memset(task->out, 42, TEST_PAYLOAD_SIZE);
#else
    // task->out = (void *)0xfeadbeed;
#endif
    // task->out_size = 0x512;
    // task->user_data = nullptr;

    // measure the time
    printf("Submitting task\n");
    auto start = std::chrono::high_resolution_clock::now();

    ulong cnt = 0;
    ulong submitted_cnt = 0;
    ulong completed_cnt = 0;

    while (completed_cnt < 4)
    {
        bool ret = app_alloc_task_request(&task);
        if (!ret)
        {
            printf("Failed to allocate task\n");
            return -1;
        }
        else
        {
            printf("Allocated task\n");
        }

        task->in_size = INPUT_SIZE;
        task->out_size = OUTPUT_SIZE;

        printf("alloc input buf with len: %lu\n", task->in_size);
        ret = dpm_alloc_input_buf(task->in_size, &task->in);
        if (ret)
            printf("Allocated input buf shm: %lu: \n", task->in);
        else
        {
            printf("Failed to allocate input buf\n");
            break;
        }

        ret = dpm_alloc_output_buf(OUTPUT_SIZE, &task->out);
        if (ret)
            printf("Allocated output buf shm: %lu: \n", task->out);
        else
        {
            printf("Failed to allocate output buf\n");
            break;
        }

        // task->device = dpkernel_device::DEVICE_NULL;
        task->task = dpm_task_name::TASK_NULL;

        // get local ptr from shm ptr of in and out buffers
        char *in = app_get_input_ptr_from_shmptr(task->in);
        char *out = app_get_output_ptr_from_shmptr(task->out);

        task->user_data = nullptr;
        memset(in, 42, INPUT_SIZE);
        printf("Filled input buf\n");
        memset(out, 0, OUTPUT_SIZE);
        printf("Filled output buf\n");

        // now submit the NULL kernel task
        printf("Submitting NULL task\n");
        ret = dpm_submit_task_msgq(task);

        if (!ret)
        {
            printf("Failed to submit task\n");
            break;
        }
        else
        {
            submitted_cnt++;
        }

        // wait on the completion flag in the task
        // TODO: should be a function in the interface
        while (task->completion.load() == DPK_ONGOING)
            ;
        printf("Task completed with flag: %d\n", static_cast<int>(task->completion.load()));

        completed_cnt++;

        // do decompress kernel
        task->device = dpm_device::DEVICE_NULL;
        task->task = dpm_task_name::TASK_DECOMPRESS_DEFLATE;
        memcpy(in, input_buf, INPUT_SIZE);
        memset(out, 0, OUTPUT_SIZE);
        task->in_size = INPUT_SIZE;
        task->out_size = OUTPUT_SIZE;

        // now submit the decompress kernel task
        printf("Submitting decompress task\n");
        ret = dpm_submit_task_msgq(task);
        if (!ret)
        {
            printf("Failed to submit decompress task\n");
            break;
        }
        else
        {
            submitted_cnt++;
        }

        // wait on the completion flag in the task
        while (task->completion.load() == DPK_ONGOING)
            ;
        printf("Task completed with flag: %d\n", static_cast<int>(task->completion.load()));
        // print the first 10 chars of the output
        printf("decompressed output: ");
        for (int i = 0; i < 10; ++i)
        {
            printf("%c", ((out))[i]);
        }
        printf("\n");

        dpm_free_input_buf(in);
        printf("Freed input buf\n");
        dpm_free_output_buf(out);
        printf("Freed output buf\n");
        app_free_task_request(task);
    }

    // time end
    auto end = std::chrono::high_resolution_clock::now();

    printf("Submitted %lu tasks\n", submitted_cnt);
    printf("Completed %lu tasks\n", completed_cnt);

    std::chrono::duration<double> elapsed = end - start;
    std::cout << "Time taken: " << elapsed.count() << " s\n";

    double throughput_million_ops = (double)completed_cnt / elapsed.count() / 1000000;
    printf("Throughput: %.6f million ops/s\n", throughput_million_ops);

    /* sleep(1);
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
 */
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