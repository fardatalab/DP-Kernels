#include "common.hpp"
#include "helpers.hpp"
#include "rdma_comm.hpp"
#include <cstddef>

int main(int argc, char *argv[])
{
    // load keys from file
    size_t count = load_keys(KEYS_FILE);
    size_t per_thread_read_num = count / N_THREADS;

    setup_rdma_connections(N_THREADS, PORT_BASE, QUEUE_DEPTH, BATCH_SIZE, per_thread_read_num, sizeof(MessageHeader),
                           sizeof(CompressedValue));

    return 0;
}