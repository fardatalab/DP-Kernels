#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "memory_common.hpp"
#include "types.hpp"

// Configuration
const std::string SERVER_IP = "10.10.1.42";
const int SERVER_PORT = 12345;
const size_t NUM_THREADS = N_CLIENT_THREADS;
// const uint64_t DEFAULT_NUM_REQUESTS_TO_SEND = 10'000'000;
const uint64_t DEFAULT_NUM_REQUESTS_TO_SEND = __DEFAULT_NUM_REQUESTS_TO_SEND;
const size_t REQUEST_BATCH_SIZE = 128;             // Number of requests to batch before sending
const size_t DATABASE_SIZE = __DATABASE_SIZE;      // size_t(16) * size_t(1024) * size_t(1024) * size_t(1024);
const size_t LOCAL_MEMORY_SIZE = __LOCAL_CAPACITY; // size_t(1) * 1024 * 1024 * 1024;
const size_t VALUE_SIZE = __VALUE_SIZE;

const size_t ON_WIRE_REQUEST_SIZE = sizeof(pd3::network_hashmap::Request);
const size_t ON_WIRE_RESPONSE_SIZE = sizeof(pd3::network_hashmap::Response);

// Global counters
std::atomic<uint64_t> responses_received_count(0);
std::atomic<bool> all_senders_finished(false);

// Thread-local state
// struct ThreadState {
//     int sock_fd;
//     uint64_t requests_to_send;
//     char* send_buffer;
//     char* recv_buffer;
//     std::atomic<bool> sender_finished{false};
//     std::atomic<bool> receiver_finished{false};
// };

// Function to connect to the server
int connect_to_server(const std::string &ip, int port)
{
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0)
    {
        perror("Client: Socket creation failed");
        return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) <= 0)
    {
        perror("Client: Invalid address/ Address not supported");
        close(sock_fd);
        return -1;
    }

    if (connect(sock_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        perror("Client: Connection Failed");
        close(sock_fd);
        return -1;
    }

    // Set TCP_NODELAY for the client socket
    int optval = 1;
    if (setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval)) < 0)
    {
        perror("Client: setsockopt TCP_NODELAY failed");
        // Proceeding without it, but log the error
    }

    std::cout << "Client: Connected to server " << ip << ":" << port << std::endl;
    return sock_fd;
}

void sender_thread_func(int sock_fd, uint64_t requests_to_send, char *send_buffer)
{
    // Pin this thread to a core
    static std::atomic<int> next_core_id{0};
    auto core_id = next_core_id++;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0)
    {
        std::cerr << "Failed to pin thread to core" << std::endl;
    }

    std::cout << "Client Sender Thread " << core_id << ": Starting to send " << requests_to_send
              << " requests in batches of " << REQUEST_BATCH_SIZE << "." << std::endl;

    // Populate send buffer
    // TODO: pre-populate the send buffer with request

    // Send requests in batches
    auto start_time = std::chrono::high_resolution_clock::now();
    const uint64_t total_size = requests_to_send * ON_WIRE_REQUEST_SIZE;
    uint64_t offset = 0;
    while (offset < total_size)
    {
        auto remaining_bytes = total_size - offset;
        auto batch_size = REQUEST_BATCH_SIZE * sizeof(pd3::network_hashmap::Request);
        auto bytes_to_send = std::min(remaining_bytes, batch_size);
        auto ret = send(sock_fd, send_buffer + offset, bytes_to_send, 0);
        if (ret < 0)
        {
            perror("Client Sender Thread: send failed");
            return;
        }
        if (ret == 0)
        {
            std::cerr << "Client Sender Thread: send returned 0" << std::endl;
            return;
        }
        offset += ret;
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> duration_us = end_time - start_time;
    std::cout << "Client Sender Thread " << core_id
              << ": Finished sending all requests. Time taken: " << duration_us.count() << " us" << std::endl;
}

void receiver_thread_func(int sock_fd, uint64_t requests_to_recv)
{
    // std::cout << "Requests to receive: " << requests_to_recv << std::endl;
    // Pin this thread to a core
    char *recv_buffer = new char[requests_to_recv * ON_WIRE_RESPONSE_SIZE];
    static std::atomic<int> next_core_id{NUM_THREADS};
    auto core_id = next_core_id++;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0)
    {
        std::cerr << "Failed to pin thread to core" << std::endl;
    }

    // std::cout << "Client Receiver Thread " << core_id << ": Starting to receive responses." << std::endl;

    const uint64_t total_responses_size = requests_to_recv * ON_WIRE_RESPONSE_SIZE;
    uint64_t offset = 0;
    uint64_t responses_received = 0;

    while (offset < total_responses_size)
    {
        auto remaining_bytes = total_responses_size - offset;
        auto bytes_to_recv = std::min(remaining_bytes, static_cast<uint64_t>(REQUEST_BATCH_SIZE));
        auto ret = recv(sock_fd, recv_buffer + offset, bytes_to_recv, 0);
        if (ret < 0)
        {
            perror("Client Receiver Thread: recv failed");
            break;
        }
        if (ret == 0)
        {
            std::cout << "Client Receiver Thread: Server closed connection" << std::endl;
            break;
        }
        offset += ret;
        responses_received = offset / ON_WIRE_RESPONSE_SIZE;
        // std::cout << "Client Receiver Thread " << core_id << ": Received " << responses_received << " responses." <<
        // std::endl;
    }

    // std::cout << "Client Receiver Thread " << core_id << ": Finished. Received "
    //           << responses_received << " responses." << std::endl;
    responses_received_count.fetch_add(responses_received);
}

void GenerateTestRequests(char *send_buffer, uint64_t requests_to_send)
{
    uint64_t offset = 0;
    srand(0);
    uint64_t num_keys = DATABASE_SIZE / VALUE_SIZE;
    uint64_t num_local_keys = LOCAL_MEMORY_SIZE / VALUE_SIZE;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, num_keys - 1);
    for (uint64_t i = 0; i < requests_to_send; ++i)
    {
        auto req = reinterpret_cast<pd3::network_hashmap::Request *>(send_buffer + offset);
        req->type = pd3::network_hashmap::RequestType::kGet;
        req->key = i;
        // req->local = req->key < num_local_keys;
        req->local = true; // always use local requests
        offset += ON_WIRE_REQUEST_SIZE;
    }
    return;
}

/* void GenerateUniformRequests(char *send_buffer, uint64_t requests_to_send)
{
    uint64_t offset = 0;
    for (uint64_t i = 0; i < requests_to_send; ++i)
    {
        auto req = reinterpret_cast<pd3::network_hashmap::Request *>(send_buffer + offset);
        req->type = pd3::network_hashmap::RequestType::kGet;
        req->key = i;
        req->local = true;
        offset += ON_WIRE_REQUEST_SIZE;
    }
    return;
}

void GenerateZipfianRequests(char *send_buffer, uint64_t requests_to_send)
{
    uint64_t offset = 0;
    for (uint64_t i = 0; i < requests_to_send; ++i)
    {
        auto req = reinterpret_cast<pd3::network_hashmap::Request *>(send_buffer + offset);
        req->type = pd3::network_hashmap::RequestType::kGet;
        req->key = i;
        req->local = true;
        offset += ON_WIRE_REQUEST_SIZE;
    }
    return;
}

void GenerateRequestsWithLeap(char *send_buffer, uint64_t requests_to_send)
{
    uint64_t offset = 0;
    for (uint64_t i = 0; i < requests_to_send; ++i)
    {
        auto req = reinterpret_cast<pd3::network_hashmap::Request *>(send_buffer + offset);
        req->type = pd3::network_hashmap::RequestType::kGet;
        req->key = i;
        req->local = true;
        offset += ON_WIRE_REQUEST_SIZE;
    }
    return;
} */

int main(int argc, char *argv[])
{
    uint64_t num_total_requests = DEFAULT_NUM_REQUESTS_TO_SEND;
    if (argc > 1)
    {
        try
        {
            num_total_requests = std::stoull(argv[1]);
        }
        catch (const std::exception &e)
        {
            std::cerr << "Invalid number of requests: " << argv[1]
                      << ". Using default: " << DEFAULT_NUM_REQUESTS_TO_SEND << std::endl;
        }
    }

    if (num_total_requests == 0)
    {
        std::cout << "Client: Number of requests is zero. Exiting." << std::endl;
        return 0;
    }

    // Calculate requests per thread
    uint64_t requests_per_thread = num_total_requests / NUM_THREADS;
    uint64_t remaining_requests = num_total_requests % NUM_THREADS;

    // Create thread states and start threads
    std::vector<std::thread> threads;
    std::vector<char *> send_buffers(NUM_THREADS);

    // first connect to all the servers and generate all the requests
    std::vector<int> sock_fds(NUM_THREADS);
    for (size_t i = 0; i < NUM_THREADS; ++i)
    {
        sock_fds[i] = connect_to_server(SERVER_IP, SERVER_PORT);
        if (sock_fds[i] < 0)
        {
            std::cerr << "Failed to connect thread " << i << " to server" << std::endl;
            continue;
        }
        // generate requests
        auto requests_to_send = requests_per_thread + (i < remaining_requests ? 1 : 0);
        char *send_buffer = new char[requests_to_send * ON_WIRE_REQUEST_SIZE];
        GenerateTestRequests(send_buffer, requests_to_send);
        send_buffers[i] = send_buffer;
    }

    sleep(5);

    for (size_t i = 0; i < NUM_THREADS; ++i)
    {

        auto fd = sock_fds[i];
        auto requests_to_send = requests_per_thread + (i < remaining_requests ? 1 : 0);
        auto requests_to_recv = requests_to_send;

        threads.emplace_back(sender_thread_func, fd, requests_to_send, send_buffers[i]);
        threads.emplace_back(receiver_thread_func, fd, requests_to_recv);
    }

    std::cout << "Client: Started " << threads.size() << " threads (" << NUM_THREADS
              << " connections, each with sender and receiver)." << std::endl;
    auto start_time = std::chrono::high_resolution_clock::now();

    // Wait for all threads to complete
    for (auto &thread : threads)
    {
        thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> duration_us = end_time - start_time;

    // Cleanup and report results
    uint64_t total_responses = responses_received_count.load();

    std::cout << "Client: All threads finished." << std::endl;
    std::cout << "Client: Total requests sent: " << num_total_requests << std::endl;
    std::cout << "Client: Total responses received: " << total_responses << std::endl;
    std::cout << "Client: Total time taken: " << duration_us.count() << " us" << std::endl;

    if (duration_us.count() > 0 && total_responses == num_total_requests)
    {
        double throughput = (static_cast<double>(total_responses) / duration_us.count()); // Responses per microsecond
        std::cout << "Client: Throughput: " << throughput << " MOps" << std::endl;
    }
    else if (total_responses != num_total_requests)
    {
        std::cerr << "Client: Mismatch in sent requests and received responses." << std::endl;
    }

    // cleanup
    for (size_t i = 0; i < NUM_THREADS; ++i)
    {
        delete[] send_buffers[i];
    }

    return 0;
}