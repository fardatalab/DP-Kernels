///
/// @file net_hashmap.hpp
/// @brief The hashmap reads requests from the network and processes them
///
#pragma once

#include <deque>
#define MB (1024 * 1024)
#define KB (1024)
#define GB (1024 * 1024 * 1024)

#include "types.hpp"

// #include "literals/memory_literals.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <libdeflate.h>

#include "dpm_interface.hpp"

// #include <rigtorp/SPSCQueue.h>

// #include <rdma/rdma_verbs.h>

// #include <tbb/concurrent_queue.h>

// #define PD3

#define INPUT_FILE "/home/ubuntu/deflate/4K.deflate"

namespace pd3
{
namespace network_hashmap
{

// using namespace dpf::literals::memory_literals;
static constexpr size_t BUFFER_SIZE = 1 * MB;
// MUST be a multiple of Response size
static constexpr size_t SEND_BUFFER_SIZE = 8 * sizeof(Response); // 64 * MB / __UNCOMPRESSED_SIZE * sizeof(Response);

// value size is for compressed size
static constexpr size_t VALUE_SIZE = __VALUE_SIZE;

static constexpr size_t REMOTE_SEGMENT_SIZE = 1 * GB; // 1_GiB;

// Define fixed sizes for on-wire structs from types.hpp
static constexpr size_t ON_WIRE_REQUEST_SIZE = sizeof(pd3::network_hashmap::Request);
static constexpr size_t ON_WIRE_RESPONSE_SIZE = sizeof(pd3::network_hashmap::Response);

static constexpr int N_SEND_BUFFERS = SEND_BUFFER_SIZE / ON_WIRE_RESPONSE_SIZE;

struct Config
{
    size_t local_capacity;
    size_t database_size;
    size_t num_threads;
    size_t batch_size;
    std::string server_ip = "0.0.0.0"; // Default IP
    int server_port = 12345;           // Default port
    bool run_pd3 = false;
    // std::string remote_memory_server_ip = "10.10.1.1";
    // std::string remote_memory_server_port = "51216";
};

class NetworkHashMap
{

    // forward declarations
    struct ClientState;

  public:
    NetworkHashMap();
    ~NetworkHashMap();

    void Configure(const Config &config);

    void Run();

    void Stop();

    // Helper for network setup
    bool InitializeServerSocket();

    // Helper for RDMA setup
    bool InitializeRdma();

  private:
    // New method to handle individual client connections
    void ClientConnectionThread(int client_fd);

    // Modified network thread to only handle accepting connections
    void AcceptorThread();

    // RDMA thread for handling remote requests
    void RdmaThread();

    // Helper methods for client connection handling
    void HandleClientRead(ClientState *client_state, bool &error);
    void PollDPKCompletions(ClientState *client_state);
    void HandleClientRequests(ClientState *client_state);
    void CompactReadBuffer(ClientState *client_state);
    void SendClientData(ClientState *client_state, bool &error);

    int AwaitCompletion();

  private:
    Config config_;

    std::mutex threads_mutex_;
    std::vector<std::thread> threads_;
    std::atomic<bool> running_;

    std::unordered_map<uint64_t, uint64_t> local_hashmap_;
    char *local_store_;
    char *local_output_buffer_;
    size_t local_store_size_;
    size_t local_output_buffer_size_;

    // RDMA resources
    // bool use_rdma_ = false;
    // struct rdma_cm_id *rdma_cm_id_ = nullptr;
    // struct ibv_comp_channel *comp_channel_ = nullptr;
    // struct ibv_cq *cq_ = nullptr;
    // struct ibv_qp *qp_ = nullptr;

    // std::vector<MemoryBuffer> remote_buffers_;

    // remote worker request queue
    struct RemoteRequest
    {
        uint64_t key;
        uint64_t offset;
        uint64_t segment_id;
        uint32_t thread_id;
    };
    // tbb::concurrent_queue<RemoteRequest> remote_request_queue_;

    // per thread state
    struct alignas(64) PerThreadState
    {
        char *local_buffer;
        struct ibv_mr *local_mr;
        // tbb::concurrent_queue<uint64_t> response_queue;
    };
    std::vector<PerThreadState> per_thread_states_;
    std::atomic<uint64_t> next_thread_state_idx_ = 0;

    int listen_fd_ = -1;

    struct ClientState
    {
        int client_fd;
        size_t send_start = 0;
        size_t send_end = 0;
        size_t recv_start = 0;
        size_t recv_end = 0;
        size_t total_received_requests = 0;
        size_t total_processed_requests = 0;
        size_t total_sent_responses = 0;
        size_t thread_state_idx = 0;

        // decompression
        struct libdeflate_decompressor *decompressor;

        // dpk related stuff
        std::vector<dpkernel_task *> dpk_tasks;
        std::vector<dpkernel_task *> output_tasks;
        size_t next_poll_key = 0;
        size_t send_used_size = 0;
        size_t send_completed = 0;      // offset
        size_t send_completed_size = 0; // size in bytes of completed
        // std::deque<PendingDecompressionOp> pending_decompression_ops;
        std::array<PendingDecompressionOp, SEND_BUFFER_SIZE / sizeof(Response)> pending_decompression_ops;

        std::array<char, BUFFER_SIZE> recv_buffer;
        // std::array<char, SEND_BUFFER_SIZE> send_buffer;
        char *send_buffer;
    };
    std::mutex client_states_mutex_;
    std::unordered_map<int, std::unique_ptr<ClientState>> client_states_;

    // #ifdef PD3
    //     // add PD3 related members
    // #endif
};

} // namespace network_hashmap
} // namespace pd3