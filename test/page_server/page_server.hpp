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

#define INPUT_FILE "/home/ubuntu/deflate/4K.deflate"

namespace dpk
{
namespace page_server
{

using Request = pd3::network_hashmap::Request;
using Response = pd3::network_hashmap::Response;
using PendingDecompressionOp = pd3::network_hashmap::PendingDecompressionOp;

static constexpr size_t BUFFER_SIZE = 1 * MB;
// MUST be a multiple of Response size
static constexpr size_t SEND_BUFFER_SIZE = 1024 * sizeof(Response); // 64 * MB / __UNCOMPRESSED_SIZE * sizeof(Response);

// value size is for compressed size
static constexpr size_t VALUE_SIZE = __VALUE_SIZE;

static constexpr size_t REMOTE_SEGMENT_SIZE = 1 * GB;
static constexpr size_t SEGMENT_FILE_SIZE = 128 * MB;

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
};

class PageServer  
{

    // forward declarations
    struct ClientState;

  public:
    PageServer();
    ~PageServer();

    void Configure(const Config &config);
    
    // this function initializes the index for segmented files on disk
    bool InitializeIndex(uint32_t num_pages);

    void Run();

    void Stop();

    // Helper for network setup
    bool InitializeServerSocket();

  private:
    // New method to handle individual client connections
    void ClientConnectionThread(int client_fd);

    // Modified network thread to only handle accepting connections
    void AcceptorThread();

    // Helper methods for client connection handling
    void HandleClientRead(ClientState *client_state, bool &error);
    void PollDPKCompletions(ClientState *client_state);
    void HandleClientRequests(ClientState *client_state);
    void CompactReadBuffer(ClientState *client_state);
    void SendClientData(ClientState *client_state, bool &error);

  private:
    Config config_;

    std::mutex threads_mutex_;
    std::vector<std::thread> threads_;
    std::atomic<bool> running_;
    std::atomic_uint32_t next_thread_state_idx_{0};

    /* Page Server Items */
    // page metadata
    struct PageMeta {
      uint32_t seg_idx;
      uint32_t off;
    };
    size_t num_pages_in_segment_;
    size_t segment_size_;
    // segment fds
    std::vector<int> seg_fd_;
    // page index
    std::vector<PageMeta> page_index_;


    char *local_store_;
    char *local_output_buffer_;
    size_t local_store_size_;
    size_t local_output_buffer_size_;

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
        char* scratch;

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
        char *send_buffer;
    };
    std::mutex client_states_mutex_;
    std::unordered_map<int, std::unique_ptr<ClientState>> client_states_;
};

} // namespace network_hashmap
} // namespace pd3