#include "net_hashmap.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
// #include <rdma/rdma_verbs.h>
#include <sched.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace pd3
{
namespace network_hashmap
{

NetworkHashMap::NetworkHashMap() : running_(false)
{
}

NetworkHashMap::~NetworkHashMap()
{
    if (running_)
    {
        Stop();
    }
    if (local_store_)
    {
        delete[] local_store_;
    }
    for (auto &&thread_state : per_thread_states_)
    {
        /* if (thread_state.local_buffer)
        {
            free(thread_state.local_buffer);
        }
        if (thread_state.local_mr)
        {
            ibv_dereg_mr(thread_state.local_mr);
        } */
    }
    /* if (rdma_cm_id_)
    {
        rdma_destroy_id(rdma_cm_id_);
    }
    if (comp_channel_)
    {
        ibv_destroy_comp_channel(comp_channel_);
    }
    if (cq_)
    {
        ibv_destroy_cq(cq_);
    }
    if (qp_)
    {
        ibv_destroy_qp(qp_);
    } */
    if (listen_fd_ != -1)
    {
        close(listen_fd_);
    }
}

void NetworkHashMap::Configure(const Config &config)
{
    config_ = config;
    auto num_keys = std::min(config_.database_size, config_.local_capacity) / VALUE_SIZE;
    std::cout << "num_keys: " << num_keys << std::endl;
    local_store_ = new char[config_.local_capacity];
    std::memset(local_store_, 0, config_.local_capacity);

    local_output_buffer_size_ = num_keys * __UNCOMPRESSED_SIZE;
    local_output_buffer_ = new char[local_output_buffer_size_];

    // read from file and copy into the local_store_
    std::ifstream file(INPUT_FILE, std::ios::binary | std::ios::ate);
    if (!file)
    {
        std::cerr << "Error: Cannot open file " << INPUT_FILE << std::endl;
        exit(EXIT_FAILURE);
    }
    std::streamsize file_size = file.tellg();
    if (file_size < 0)
    {
        std::cerr << "Error: Failed to determine file size." << std::endl;
        exit(EXIT_FAILURE);
    }
    file.seekg(0, std::ios::beg);
    char *input_buf = new char[file_size];
    if (!file.read(input_buf, file_size))
    {
        std::cerr << "Error: Failed to read file data." << std::endl;
        delete[] input_buf;
        exit(EXIT_FAILURE);
    }
    file.close();
    // copy the input buffer into the local store repeteadly
    size_t bytes_to_copy = std::min(static_cast<size_t>(file_size), config_.local_capacity);
    size_t bytes_copied = 0;
    while (bytes_copied < config_.local_capacity)
    {
        size_t bytes_left = config_.local_capacity - bytes_copied;
        size_t bytes_to_copy_now = std::min(bytes_left, bytes_to_copy);
        std::memcpy(local_store_ + bytes_copied, input_buf, bytes_to_copy_now);
        bytes_copied += bytes_to_copy_now;
    }

    local_store_size_ = config_.local_capacity;
    // local_hashmap_.reserve(config_.local_capacity);

    // initialize the local hashmap
    for (auto i = 0; i < num_keys; i++)
    {
        size_t memory_addr = i * VALUE_SIZE;
        local_hashmap_.emplace(i, memory_addr);
    }
}

bool NetworkHashMap::InitializeServerSocket()
{
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0)
    {
        perror("socket creation failed");
        return false;
    }

    int opt = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        perror("setsockopt SO_REUSEADDR failed");
        close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, config_.server_ip.c_str(), &server_addr.sin_addr) <= 0)
    {
        perror("inet_pton failed for server_ip");
        close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    server_addr.sin_port = htons(config_.server_port);

    if (bind(listen_fd_, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        perror("bind failed");
        close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    if (listen(listen_fd_, 5) < 0)
    { // Listen queue size 5
        perror("listen failed");
        close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    // Set listen_fd_ to non-blocking for the accept loop in NetworkThread
    int flags = fcntl(listen_fd_, F_GETFL, 0);
    if (flags == -1 || fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        perror("fcntl O_NONBLOCK on listen_fd_ failed");
        close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    std::cout << "Server listening on " << config_.server_ip << ":" << config_.server_port << std::endl;
    return true;
}

// initialize the RDMA here
/* bool NetworkHashMap::InitializeRdma()
{
    int ret;
    struct ibv_wc wc;
    struct ibv_qp_init_attr qp_attr;

    // rdma address info
    struct rdma_addrinfo *rai = nullptr;
    struct rdma_addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_port_space = RDMA_PS_TCP;
    std::cout << "Initializing RDMA for remote memory server " << config_.remote_memory_server_ip << ":"
              << config_.remote_memory_server_port << std::endl;
    ret = rdma_getaddrinfo(config_.remote_memory_server_ip.c_str(), config_.remote_memory_server_port.c_str(), &hints,
                           &rai);
    if (ret)
    {
        std::cerr << "RDMAServer: failed to get address info: " << strerror(errno) << std::endl;
        return false;
    }

    RemoteMemoryDetails remote_memory_details;
    ret = rdma_create_id(NULL, &rdma_cm_id_, NULL, RDMA_PS_TCP);
    if (ret)
    {
        std::cerr << "RDMAServer: failed to create ID" << strerror(errno) << std::endl;
        return false;
    }
    ret = rdma_resolve_addr(rdma_cm_id_, NULL, rai->ai_dst_addr, 1000);
    if (ret)
    {
        std::cerr << "RDMAServer: failed to resolve address" << strerror(errno) << std::endl;
        return false;
    }
    ret = rdma_resolve_route(rdma_cm_id_, 1000);
    if (ret)
    {
        std::cerr << "RDMAServer: failed to resolve route" << strerror(errno) << std::endl;
        return false;
    }
    comp_channel_ = ibv_create_comp_channel(rdma_cm_id_->verbs);
    if (!comp_channel_)
    {
        std::cerr << "RDMAServer: failed to create completion channel" << std::endl;
        return false;
    }
    cq_ = ibv_create_cq(rdma_cm_id_->verbs, 1024, NULL, comp_channel_, 0);
    if (!cq_)
    {
        std::cerr << "RDMAServer: failed to create completion queue" << std::endl;
        return false;
    }
    ret = ibv_req_notify_cq(cq_, 0);
    if (ret)
    {
        std::cerr << "RDMAServer: failed to request CQ notification" << std::endl;
        return false;
    }
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.cap.max_send_wr = 16;
    qp_attr.cap.max_recv_wr = 16;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.send_cq = cq_;
    qp_attr.recv_cq = cq_;
    ret = rdma_create_ep(&rdma_cm_id_, rai, NULL, &qp_attr);
    if (ret)
    {
        std::cerr << "RDMAServer: failed to create QP" << strerror(errno) << std::endl;
        return false;
    }
    ret = rdma_connect(rdma_cm_id_, NULL);

    // rdma_dest_buffer_ = reinterpret_cast<char*>(malloc(128_KiB));
    // rdma_dest_mr_ = ibv_reg_mr(rdma_cm_id_->pd, rdma_dest_buffer_, 128_KiB, IBV_ACCESS_LOCAL_WRITE);
    // if (!rdma_dest_mr_) {
    //   std::cerr << "RDMAServer: failed to register memory" << std::endl;
    //   return;
    // }

    for (auto &&thread_state : per_thread_states_)
    {
        thread_state.local_buffer = reinterpret_cast<char *>(malloc(128_KiB));
        thread_state.local_mr = ibv_reg_mr(rdma_cm_id_->pd, thread_state.local_buffer, 128_KiB, IBV_ACCESS_LOCAL_WRITE);
        if (!thread_state.local_mr)
        {
            std::cerr << "RDMAServer: failed to register memory" << std::endl;
            return false;
        }
    }

    struct ibv_mr *temp_mr;
    temp_mr =
        ibv_reg_mr(rdma_cm_id_->pd, &remote_memory_details, sizeof(remote_memory_details), IBV_ACCESS_LOCAL_WRITE);
    if (!temp_mr)
    {
        std::cerr << "RDMAServer: failed to register memory" << std::endl;
        return false;
    }
    ret = rdma_post_recv(rdma_cm_id_, NULL, &remote_memory_details, sizeof(remote_memory_details), temp_mr);
    if (ret)
    {
        std::cerr << "RDMAServer: failed to post receive" << strerror(errno) << std::endl;
        return false;
    }
    AwaitCompletion();

    while (true)
    {
        ret = ibv_poll_cq(cq_, 1, &wc);
        if (ret > 0)
        {
            break;
        }
        if (ret < 0)
        {
            std::cerr << "RDMAServer: failed to poll CQ" << std::endl;
            return false;
        }
        std::this_thread::yield();
    }

    std::cout << "Remote memory details received" << std::endl;
    std::cout << "Num segments: " << remote_memory_details.num_entries << std::endl;
    for (auto i = 0; i < remote_memory_details.num_entries; ++i)
    {
        remote_buffers_.emplace_back(remote_memory_details.buffers[i]);
    }

    // clean up the temp mr
    ibv_dereg_mr(temp_mr);
    return true;
} */

void NetworkHashMap::Run()
{
    if (!InitializeServerSocket())
    {
        std::cerr << "Failed to initialize server socket. Aborting Run." << std::endl;
        return;
    }

    per_thread_states_.resize(config_.num_threads + 10);
    if (config_.database_size > config_.local_capacity)
    {
        // use_rdma_ = true;
        // use_rdma_ = false; // disable RDMA for now
        printf("config_.database_size > config_.local_capacity, but not using RDMA for now\n");
        /* if (!InitializeRdma())
        {
            std::cerr << "Failed to initialize RDMA. Aborting Run." << std::endl;
            return;
        } */
    }
    running_ = true;

    // Start the acceptor thread
    threads_.emplace_back(&NetworkHashMap::AcceptorThread, this);

    // disable RDMA for now
    /* if (use_rdma_)
    {
        threads_.emplace_back(&NetworkHashMap::RdmaThread, this);
    } */
}

void NetworkHashMap::Stop()
{
    running_ = false;
    if (listen_fd_ != -1)
    {
        shutdown(listen_fd_, SHUT_RDWR);
        close(listen_fd_);
        listen_fd_ = -1;
    }

    // Clean up all client connections
    {
        std::lock_guard<std::mutex> lock(client_states_mutex_);
        for (auto &&[fd, state] : client_states_)
        {
            shutdown(fd, SHUT_RDWR);
            close(fd);
        }
        client_states_.clear();
    }

    for (auto &thread : threads_)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
    threads_.clear();
}

void NetworkHashMap::AcceptorThread()
{
    if (listen_fd_ == -1)
    {
        std::cerr << "AcceptorThread: Listen socket not initialized." << std::endl;
        return;
    }

    // Pin this thread to a core

    while (running_)
    {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);

        int client_fd = accept(listen_fd_, (struct sockaddr *)&client_addr, &client_addr_len);

        if (client_fd < 0)
        {
            if (errno == EWOULDBLOCK || errno == EAGAIN)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            if (!running_)
                break;
            perror("AcceptorThread: accept failed");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        // Set client socket to non-blocking
        int flags = fcntl(client_fd, F_GETFL, 0);
        if (flags == -1 || fcntl(client_fd, F_SETFL, flags | O_NONBLOCK) == -1)
        {
            perror("AcceptorThread: fcntl O_NONBLOCK failed on client_fd");
            close(client_fd);
            continue;
        }

        // Set TCP_NODELAY for the client socket
        int optval = 1;
        if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval)) < 0)
        {
            perror("AcceptorThread: setsockopt TCP_NODELAY failed");
            // Continue without it, or close and log error
        }

        // Create client state
        {
            std::lock_guard<std::mutex> lock(client_states_mutex_);
            client_states_[client_fd] = std::make_unique<ClientState>();
            client_states_[client_fd]->client_fd = client_fd;
            client_states_[client_fd]->thread_state_idx =
                next_thread_state_idx_.fetch_add(1) % (config_.num_threads + 10);
            client_states_[client_fd]->decompressor = libdeflate_alloc_decompressor();
            client_states_[client_fd]->send_buffer = new char[SEND_BUFFER_SIZE];
        }

        // Start client connection thread
        {
            std::lock_guard<std::mutex> lock(threads_mutex_);
            threads_.emplace_back(&NetworkHashMap::ClientConnectionThread, this, client_fd);
        }

        char client_ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip_str, INET_ADDRSTRLEN);
        std::cout << "AcceptorThread: Accepted new connection from " << client_ip_str << ":"
                  << ntohs(client_addr.sin_port) << " on fd: " << client_fd << std::endl;
    }
}

void NetworkHashMap::ClientConnectionThread(int client_fd)
{
    // Pin this thread to a core
    static std::atomic<int> next_core_id{0}; // Start after acceptor thread
    auto core_id = N_CLIENT_THREADS * 2 + (next_core_id++ % config_.num_threads);
    std::cout << "ClientConnectionThread: Pinning thread to core " << core_id << std::endl;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0)
    {
        std::cerr << "Failed to pin thread to core" << std::endl;
    }

    ClientState *client_state = nullptr;
    {
        std::lock_guard<std::mutex> lock(client_states_mutex_);
        client_state = client_states_[client_fd].get();
    }
    std::cout << "ClientConnectionThread: ThreadIDX: " << client_state->thread_state_idx << std::endl;

    while (running_)
    {
        fd_set read_fds;
        fd_set write_fds;
        FD_ZERO(&read_fds);
        FD_ZERO(&write_fds);

        FD_SET(client_fd, &read_fds);

        // Check if we have data to send
        if (client_state->send_end > client_state->send_start)
        {
            FD_SET(client_fd, &write_fds);
        }

        struct timeval select_timeout;
        select_timeout.tv_sec = 0;
        select_timeout.tv_usec = 100; // 10ms timeout

        int activity = select(client_fd + 1, &read_fds, &write_fds, nullptr, &select_timeout);

        if (activity < 0 && errno != EINTR)
        {
            if (!running_)
                break;
            perror("ClientConnectionThread: select error");
            break;
        }

        if (!running_)
            break;

        // Handle incoming data
        bool error = false;
        if (activity > 0 && FD_ISSET(client_fd, &read_fds))
        {
            HandleClientRead(client_state, error);
            if (error)
            {
                break;
            }
        }

        // Process any requests in the buffer
        HandleClientRequests(client_state);
        CompactReadBuffer(client_state);

        // Handle outgoing data
        if (activity > 0 && FD_ISSET(client_fd, &write_fds))
        {
            SendClientData(client_state, error);
            if (error)
            {
                break;
            }
        }
    }

    std::cout << "ClientConnectionThread: Client fd " << client_state->client_fd << " disconnected." << std::endl;

    // Clean up
    {
        std::lock_guard<std::mutex> lock(client_states_mutex_);
        client_states_.erase(client_fd);
    }
    close(client_fd);
    std::cout << "Thread " << pthread_self() << " exiting." << std::endl;
}

void NetworkHashMap::HandleClientRead(ClientState *client_state, bool &error)
{
    error = false;
    auto remaining_space = BUFFER_SIZE - client_state->recv_end;
    if (remaining_space == 0)
    {
        // std::cerr << "ClientConnectionThread: Receive buffer full for fd " << client_state->client_fd << std::endl;
        return;
    }

    auto bytes_read =
        recv(client_state->client_fd, client_state->recv_buffer.data() + client_state->recv_end, remaining_space, 0);
    if (bytes_read > 0)
    {
        client_state->total_received_requests += bytes_read / ON_WIRE_REQUEST_SIZE;
        client_state->recv_end += bytes_read;
    }
    else if (bytes_read == 0)
    {
        error = true;
    }
    else
    {
        if (errno != EWOULDBLOCK && errno != EAGAIN)
        {
            if (!running_)
                return;
            perror("ClientConnectionThread: recv error");
            error = true;
        }
        return;
    }
}

void NetworkHashMap::HandleClientRequests(ClientState *client_state)
{
    uint64_t requests_processed = 0;
    while (running_)
    {
        auto remaining_bytes = client_state->recv_end - client_state->recv_start;
        if (remaining_bytes < ON_WIRE_REQUEST_SIZE)
        {
            break;
        }
        auto &per_thread_state = per_thread_states_[client_state->thread_state_idx];

        Request *current_request =
            reinterpret_cast<Request *>(client_state->recv_buffer.data() + client_state->recv_start);

        // Process the request
        ////if (current_request->local)
        if (true)
        {
            // XXX: get the requested bytes from local hashmap
            // auto addr = local_hashmap_[current_request->key] % config_.local_capacity;

            auto addr = (current_request->key * VALUE_SIZE) % local_store_size_;
            char *local_output_buf_ptr =
                local_output_buffer_ + (current_request->key * __UNCOMPRESSED_SIZE) % local_output_buffer_size_;

            if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > SEND_BUFFER_SIZE)
            {
                break;
            }
            auto response = reinterpret_cast<Response *>(client_state->send_buffer + client_state->send_end);

            // libdeflate decompression
            size_t actual_out_size = 0;
            if (auto result = libdeflate_deflate_decompress(client_state->decompressor, local_store_ + addr, VALUE_SIZE,
                                                            &response->value, __UNCOMPRESSED_SIZE,
                                                            &actual_out_size) != LIBDEFLATE_SUCCESS)
            {
                printf("Decompression failed for key %lu, result: %d\n", current_request->key, result);
            }

            client_state->send_end += ON_WIRE_RESPONSE_SIZE;
            response->key = current_request->key;
            // don't do memcpy on the critical path
            // std::memcpy(response->value, local_output_buf_ptr, __UNCOMPRESSED_SIZE);
        }
        // don't use remote for now
        /* else
        {
            if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > BUFFER_SIZE)
            {
                break;
            }
            auto response = reinterpret_cast<Response *>(client_state->send_buffer.data() + client_state->send_end);
            client_state->send_end += ON_WIRE_RESPONSE_SIZE;
            response->key = current_request->key;

            uint64_t address = current_request->key * VALUE_SIZE;
            uint64_t segment = address / REMOTE_SEGMENT_SIZE;
            uint64_t offset = address % REMOTE_SEGMENT_SIZE;
            uint32_t thread_id = client_state->thread_state_idx;
            // std::cout << "thread id: " << thread_id << std::endl;
            remote_request_queue_.push({current_request->key, offset, segment, thread_id});
            uint64_t response_offset;
            while (true)
            {
                if (per_thread_state.response_queue.try_pop(response_offset))
                {
                    break;
                }
            }
            // always at the start of the local buffer
            std::memcpy(response->value, per_thread_state.local_buffer, VALUE_SIZE);
        } */
        client_state->recv_start += ON_WIRE_REQUEST_SIZE;
        requests_processed++;
    }
    client_state->total_processed_requests += requests_processed;
}

void NetworkHashMap::CompactReadBuffer(ClientState *client_state)
{
    if (client_state->recv_start == client_state->recv_end)
    {
        client_state->recv_start = 0;
        client_state->recv_end = 0;
    }
    else if (client_state->recv_start > 0)
    {
        memmove(client_state->recv_buffer.data(), client_state->recv_buffer.data() + client_state->recv_start,
                client_state->recv_end - client_state->recv_start);
        client_state->recv_end -= client_state->recv_start;
        client_state->recv_start = 0;
    }
}

void NetworkHashMap::SendClientData(ClientState *client_state, bool &error)
{
    size_t current_send_data_len = client_state->send_end - client_state->send_start;

    if (current_send_data_len > 0)
    {
        ssize_t bytes_sent = send(client_state->client_fd, client_state->send_buffer + client_state->send_start,
                                  current_send_data_len, 0);
        if (bytes_sent > 0)
        {
            size_t num_responses_sent = bytes_sent / ON_WIRE_RESPONSE_SIZE;
            client_state->total_sent_responses += num_responses_sent;
            client_state->send_start += bytes_sent;
            if (client_state->send_start == client_state->send_end)
            {
                client_state->send_start = 0;
                client_state->send_end = 0;
            }
        }
        else if (bytes_sent < 0)
        {
            if (errno != EWOULDBLOCK && errno != EAGAIN)
            {
                if (!running_)
                    return;
                perror("ClientConnectionThread: send error");
                error = true;
            }
            // If EWOULDBLOCK, can't send more now, will retry
        }
        // compact if needed
        if (client_state->send_start > 0)
        {
            memmove(client_state->send_buffer, client_state->send_buffer + client_state->send_start,
                    client_state->send_end - client_state->send_start);
            client_state->send_end -= client_state->send_start;
            client_state->send_start = 0;
        }
    }
}

// Don't use RDMA for now
/* void NetworkHashMap::RdmaThread()
{
    auto core_id = config_.num_threads + 8; // TODO: see if this is necessary
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0)
    {
        std::cerr << "Failed to pin thread to core" << std::endl;
    }
    uint64_t remote_requests = 0;

    struct ibv_wc wc;
    struct ibv_send_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));
    struct ibv_sge sge;

    while (running_)
    {
        RemoteRequest request;
        if (remote_request_queue_.try_pop(request))
        {
            if (!running_)
                break;

            auto thread_id = request.thread_id;
            // std::cout << "Recieved request: " << request.key << " from thread " << thread_id << std::endl;

            wr.wr_id = 0;
            wr.opcode = IBV_WR_RDMA_READ;
            wr.num_sge = 1;
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.wr.rdma.remote_addr = (uint64_t)(remote_buffers_[request.segment_id].address + request.offset);
            wr.wr.rdma.rkey = remote_buffers_[request.segment_id].rkey;
            sge.addr = (uint64_t)per_thread_states_[request.thread_id].local_buffer;
            sge.length = RECORD_SIZE;
            sge.lkey = per_thread_states_[request.thread_id].local_mr->lkey;

            wr.sg_list = &sge;
            wr.num_sge = 1;

            auto ret = ibv_post_send(rdma_cm_id_->qp, &wr, &bad_wr);
            if (ret < 0)
            {
                std::cerr << "RDMAServer: failed to post send" << std::endl;
                return;
            }

            while (running_)
            {
                ret = ibv_poll_cq(cq_, 1, &wc);
                if (ret < 0)
                {
                    std::cerr << "RDMAServer: failed to poll CQ" << std::endl;
                    return;
                }
                if (ret > 0)
                {
                    break;
                }
                if (wc.status != IBV_WC_SUCCESS)
                {
                    std::cerr << "RDMAServer: failed to poll CQ" << std::endl;
                    return;
                }
            }
            per_thread_states_[thread_id].response_queue.push(request.offset);
            // std::cout << "Pushed response to thread " << thread_id << std::endl;
        }
    }
} */

// Don't use RDMA for now
/* int NetworkHashMap::AwaitCompletion()
{
    int ret;
    struct ibv_cq *ev_cq;
    void *ev_ctx;

    ret = ibv_get_cq_event(comp_channel_, &ev_cq, &ev_ctx);
    if (ret)
    {
        std::cerr << "RDMAServer: failed to get CQ event" << std::endl;
        return ret;
    }
    ibv_ack_cq_events(ev_cq, 1);
    ret = ibv_req_notify_cq(cq_, 0);
    if (ret)
    {
        std::cerr << "RDMAServer: failed to request CQ notification" << std::endl;
        return ret;
    }
    return 0;
} */

} // namespace network_hashmap
} // namespace pd3