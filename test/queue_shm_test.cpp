// #include "boost/lockfree/queue.hpp"
#include "boost_queue_shm.hpp"
#include <iostream>

#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

boost::lockfree::queue<boost::lockfree::capacity<32>> m_queue;

int main()
{
    boost::lockfree::queue<int, boost::lockfree::capacity<16>> m_queue;

    std::cout << "Type of m_queue.pool: " << typeid(m_queue.pool).name() << std::endl;

    size_t pool_size;
    char *pool_addr = m_queue.get_pool(&pool_size);

    // int shm_fd = shm_open("/my_shm", O_CREAT | O_RDWR, 0666);
    // if (shm_fd == -1) {
    //     perror("shm_open");
    //     return 1;
    // }

    // if (ftruncate(shm_fd, pool_size) == -1) {
    //     perror("ftruncate");
    //     return 1;
    // }

    // void *shm_addr = mmap(NULL, pool_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    // if (shm_addr == MAP_FAILED) {
    //     perror("mmap");
    //     return 1;
    // }

    // std::memcpy(shm_addr, pool_addr, pool_size);

    /* for (int i = 0; i < 100; i++)
    {
        m_queue.push(i);
    }

    int val;
    while (m_queue.pop(val))
    {
        std::cout << val << std::endl;
    } */

    return 0;
}