#include "../DPManager/mpmc_queue.hpp"
#include <chrono>
#include <iostream>
#include <thread>

void producer(std::string queue_name, size_t queue_size, int id, int count)
{
    int shm_fd = shm_open(queue_name.c_str(), O_RDWR, 0666);
    if (shm_fd == -1)
    {
        perror("shm_open in producer failed");
        return;
    }

    size_t shm_size = sizeof(rigtorp::MPMCQueue<int>);

    void *shm_ptr = mmap(0, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shm_ptr == MAP_FAILED)
    {
        perror("mmap in producer failed");
        return;
    }

    rigtorp::MPMCQueue<int> &queue = *static_cast<rigtorp::MPMCQueue<int> *>(shm_ptr);
    for (int i = 0; i < count; ++i)
    {
        int value = i * 1000 + id;
        while (!queue.try_push(value))
        {
            std::this_thread::yield();
        }
        std::cout << "Producer " << id << " pushed: " << value << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (munmap(shm_ptr, shm_size) == -1)
    {
        perror("munmap in producer failed");
    }
    close(shm_fd);
}

std::atomic_bool run_consumer{true};

void consumer(rigtorp::MPMCQueue<int> &queue, int id, int &items_processed)
{
    while (run_consumer)
    {
        int value;
        if (queue.try_pop(value))
        {
            std::cout << "Consumer " << id << " got: " << value << std::endl;
            items_processed++;
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }
        else
        {
            std::this_thread::yield();
        }
    }
}

int main()
{
    const size_t queue_size = 128;
    const int items_per_producer = 5;

    std::string name = std::string("test_queue");
    std::string q_name = std::string("test_queue_name");
    // Create shared memory object
    int shm_fd = shm_open(q_name.c_str(), O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1)
    {
        perror("shm_open failed");
        return 1;
    }

    // Define the size of shared memory object
    size_t shm_size = sizeof(rigtorp::MPMCQueue<int>);
    if (ftruncate(shm_fd, shm_size) == -1)
    {
        perror("ftruncate failed");
        return 1;
    }

    // Map shared memory object
    void *shm_ptr = mmap(0, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (shm_ptr == MAP_FAILED)
    {
        perror("mmap failed");
        return 1;
    }
    std::cout << "Queue sizeof: " << sizeof(rigtorp::MPMCQueue<int>) << std::endl;
    std::cout << "shm_size: " << shm_size << std::endl;

    // Construct MPMCQueue in-place in shared memory
    rigtorp::MPMCQueue<int> *queue = new (shm_ptr) rigtorp::MPMCQueue<int>(queue_size, name);

    int consumer1_count = 0, consumer2_count = 0;

    std::thread prod1(producer, q_name, queue_size, 1, items_per_producer);
    std::thread prod2(producer, q_name, queue_size, 2, items_per_producer);
    std::thread cons1(consumer, std::ref(*queue), 1, std::ref(consumer1_count));
    std::thread cons2(consumer, std::ref(*queue), 2, std::ref(consumer2_count));

    prod1.join();
    prod2.join();

    // Let consumers process remaining items for a while
    std::this_thread::sleep_for(std::chrono::seconds(2));
    run_consumer.store(false);

    cons1.join();
    cons2.join();

    std::cout << "Consumer 1 processed: " << consumer1_count << " items\n";
    std::cout << "Consumer 2 processed: " << consumer2_count << " items\n";
    std::cout << "Total items processed: " << (consumer1_count + consumer2_count) << "\n";

    return 0;
}
