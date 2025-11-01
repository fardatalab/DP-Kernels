#include "common.hpp"
#include "dpm_interface.hpp"
#include "memory.hpp"
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
#include <doca_buf.h>
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
    // init DPK
    if (!dpm_frontend_initialize())
    {
        std::cerr << "Failed to initialize DPM frontend. Aborting Run." << std::endl;
        return;
    }
    std::cout << "DPM frontend initialized successfully." << std::endl;

    config_ = config;
    auto num_keys = std::min(config_.database_size, config_.local_capacity) / VALUE_SIZE;
    std::cout << "num_keys: " << num_keys << std::endl;

    local_store_size_ = config_.local_capacity;

    // allocate local store and output buffer with dpm
    dpkernel_task *alloc_task = nullptr;
    if (!app_alloc_task_request(&alloc_task))
    {
        printf("Failed to allocate task during configure\n");
        return;
    }
    dpkernel_task_base *temp_task = &alloc_task->base;
    temp_task->out_size = 0;
    temp_task->in_size = local_store_size_;
    temp_task->task = dpm_task_name::TASK_DECOMPRESS_DEFLATE;
    temp_task->device = dpm_device::DEVICE_NULL;

    auto in_req = dpm_alloc_input_buf_async(local_store_size_, alloc_task);
    if (in_req == NULL)
    {
        printf("Failed to allocate input buf local_store_\n");
        exit(-1);
    }

    local_store_ = app_get_input_ptr_from_shmptr(temp_task->in);
    // local_store_ = new char[config_.local_capacity];
    std::memset(local_store_, 0, config_.local_capacity);

    // local_output_buffer_size_ = num_keys * __UNCOMPRESSED_SIZE;
    // local_output_buffer_ = new char[local_output_buffer_size_];

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

        // Calculate requests per thread
        uint64_t requests_per_thread = __DEFAULT_NUM_REQUESTS_TO_SEND / N_CLIENT_THREADS;

        std::vector<struct dpm_mem_req *> in_reqs;
        std::vector<struct dpm_mem_req *> out_reqs;

        // Create client state
        {
            std::lock_guard<std::mutex> lock(client_states_mutex_);
            client_states_[client_fd] = std::make_unique<ClientState>();
            client_states_[client_fd]->client_fd = client_fd;
            client_states_[client_fd]->thread_state_idx =
                next_thread_state_idx_.fetch_add(1) % (config_.num_threads + 10);
            // client_states_[client_fd]->decompressor = libdeflate_alloc_decompressor();

            int thread_id = client_states_[client_fd]->thread_state_idx;

            // DPK related init
            auto client_state = client_states_[client_fd].get();
            client_state->dpk_tasks.resize(requests_per_thread);

            for (int i = 0; i < requests_per_thread; ++i)
            {
                // init for each task, use input buf from local store
                dpkernel_task *task = nullptr;
                if (!app_alloc_task_request(&task))
                {
                    printf("Thread %d: Failed to allocate task\n", thread_id);
                    return;
                }
                dpkernel_task_base *temp_task = &task->base;
                temp_task->out_size = __UNCOMPRESSED_SIZE;
                temp_task->in_size = __COMPRESSED_SIZE;
                temp_task->in = app_get_shm_ptr_for_input_buf(local_store_ + (i * __COMPRESSED_SIZE));
                temp_task->task = dpm_task_name::TASK_DECOMPRESS_DEFLATE;
                temp_task->device = dpm_device::DEVICE_BLUEFIELD_3;

                client_state->dpk_tasks[i] = task;
                auto in_req = dpm_alloc_input_buf_async(__COMPRESSED_SIZE, task);
                if (in_req == NULL)
                {
                    printf("Thread %d: Failed to allocate input buf for task %d\n", thread_id, i);
                    exit(-1);
                }
                in_reqs.push_back(in_req);

                // auto out_req = dpm_alloc_output_buf_async(__UNCOMPRESSED_SIZE, task);
                // if (out_req == NULL)
                // {
                //     printf("Thread %d: Failed to allocate output buf for task %d\n", thread_id, i);
                //     exit(-1);
                // }
                // out_reqs.push_back(out_req);
            }
            // wait for all mem reqs to be done
            for (int i = 0; i < requests_per_thread; ++i)
            {
                auto task = client_state->dpk_tasks[i];
                auto in_req = in_reqs[i];
                if (!dpm_wait_for_mem_req_completion(in_req))
                {
                    printf("Thread %d: Failed for input buf for task %d\n", thread_id, i);
                    exit(-1);
                }
                // auto out_req = out_reqs[i];
                // if (!dpm_wait_for_mem_req_completion(out_req))
                // {
                //     printf("Thread %d: Failed for output buf for task %d\n", thread_id, i);
                //     exit(-1);
                // }
            }
            printf("Thread %d: Allocated %lu input bufs for tasks\n", thread_id, requests_per_thread);

            // allocate output buf, the no. of output buffers == SEND_BUFFER_SIZE / __UNCOMPRESSED_SIZE
            // we will use the same output buffer for all tasks, so we can just allocate it once

            // buf first, allocate the full send buffer
            dpkernel_task *send_buf_task = nullptr;
            if (!app_alloc_task_request(&send_buf_task))
            {
                printf("Thread %d: Failed to allocate send buffer task\n", thread_id);
                return;
            }
            dpkernel_task_base *send_buf_temp_task = &send_buf_task->base;
            send_buf_temp_task->out_size = SEND_BUFFER_SIZE;
            send_buf_temp_task->in_size = 0; // no input for send buffer task
            send_buf_temp_task->task = dpm_task_name::TASK_DECOMPRESS_DEFLATE;
            send_buf_temp_task->device = dpm_device::DEVICE_NULL;
            auto out_req = dpm_alloc_output_buf_async(SEND_BUFFER_SIZE, send_buf_task);
            if (out_req == NULL)
            {
                printf("Thread %d: Failed to allocate output buf for send buffer task\n", thread_id);
                exit(-1);
            }
            // wait for the whole output buffer to be allocated
            if (!dpm_wait_for_mem_req_completion(out_req))
            {
                printf("Thread %d: Failed to allocate output buf for send buffer task\n", thread_id);
                exit(-1);
            }
            client_state->send_buffer = app_get_output_ptr_from_shmptr(send_buf_temp_task->out);

            // then, allocate for each output task (of __UNCOMPRESSED_SIZE)
            std::vector<dpm_mem_req *> out_bufs_reqs;
            const size_t num_output_buffers = SEND_BUFFER_SIZE / ON_WIRE_RESPONSE_SIZE;
            client_state->output_tasks.resize(num_output_buffers);
            for (size_t i = 0; i < num_output_buffers; ++i)
            {
                dpkernel_task *output_task = nullptr;
                if (!app_alloc_task_request(&output_task))
                {
                    printf("Thread %d: Failed to allocate output task %lu\n", thread_id, i);
                    return;
                }
                dpkernel_task_base *temp_output_task = &output_task->base;
                temp_output_task->out_size = __UNCOMPRESSED_SIZE;
                Response *this_resp =
                    reinterpret_cast<Response *>(client_state->send_buffer + (i * ON_WIRE_RESPONSE_SIZE));

                temp_output_task->out = app_get_shm_ptr_for_output_buf(
                    (char *)&this_resp->value); // (i * ON_WIRE_RESPONSE_SIZE) + send_buf_temp_task->out;
                temp_output_task->in_size = 0;  // no input for output tasks
                temp_output_task->task = dpm_task_name::TASK_DECOMPRESS_DEFLATE;
                temp_output_task->device = dpm_device::DEVICE_BLUEFIELD_3;

                auto out_req = dpm_alloc_output_buf_async(__UNCOMPRESSED_SIZE, output_task);
                if (out_req == NULL)
                {
                    printf("Thread %d: Failed to allocate output buf for task %lu\n", thread_id, i);
                    exit(-1);
                }
                out_bufs_reqs.push_back(out_req);
                client_state->output_tasks[i] = output_task;
            }
            // wait for all output tasks to be allocated
            for (size_t i = 0; i < num_output_buffers; ++i)
            {
                auto out_buf_req = out_bufs_reqs[i];
                if (!dpm_wait_for_mem_req_completion(out_buf_req))
                {
                    printf("Thread %d: Failed for output buf for task %lu\n", thread_id, i);
                    exit(-1);
                }
            }
            printf("Thread %d: Allocated %lu output bufs for tasks\n", thread_id, num_output_buffers);
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

        PollDPKCompletions(client_state);

        // Process any requests in the buffer
        HandleClientRequests(client_state);
        CompactReadBuffer(client_state);

        // Handle outgoing data
        // if (activity > 0 && FD_ISSET(client_fd, &write_fds))
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
    printf("Thread %zu: Received %zd bytes\n", client_state->thread_state_idx, bytes_read);
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

void NetworkHashMap::PollDPKCompletions(ClientState *client_state)
{
    // if (!running_ || client_state->pending_decompression_ops.empty())
    if (!running_)
    {
        return;
    }

    while (true)
    {
        PendingDecompressionOp &op =
            client_state->pending_decompression_ops[client_state->send_completed / ON_WIRE_RESPONSE_SIZE];
        // poll for the completion of the DPK task
        dpkernel_task *dpk_task = op.pending_task;
        if (dpk_task == nullptr)
        {
            // No pending task for this key, break
            break;
        }

        // if polled a completion
        if (dpk_task->base.completion.load(std::memory_order_release) != DPK_ONGOING)
        {
            printf("Thread %zu: DPK task for key %lu completed\n", client_state->thread_state_idx, op.key);
            // bool is_consecutive = op.key == client_state->next_poll_key;
            // if (client_state->send_end + ON_WIRE_RESPONSE_SIZE <= SEND_BUFFER_SIZE) // is_consecutive &&
            {
                client_state->next_poll_key++;
                auto response = reinterpret_cast<Response *>(client_state->send_buffer + client_state->send_completed);
                response->key = op.key;

                // task completed
                // client_state->pending_decompression_ops.pop_front();
                client_state->send_completed_size += ON_WIRE_RESPONSE_SIZE;
                client_state->send_used_size -= ON_WIRE_RESPONSE_SIZE;
                printf("Thread %zu: Response for key %lu ready at send_completed %zu\n", client_state->thread_state_idx,
                       op.key, client_state->send_completed);
                printf("Thread %zu: completed dpk task %zu\n", client_state->thread_state_idx,
                       app_get_shm_ptr_for_task(dpk_task));
            }
            // reset op
            op.key = 0;                // reset key
            op.pending_task = nullptr; // reset task pointer
        }
        else
        {
            // task not completed yet, break and wait for next poll
            break;
        }

        // move to next pending task slot (tentative), after getting a completion
        client_state->send_completed = (client_state->send_completed + ON_WIRE_RESPONSE_SIZE) % SEND_BUFFER_SIZE;

        // send completed should trail behind send_end, we move one slot at a time
        if (client_state->send_completed == client_state->send_end)
        {
            // No completed tasks to process
            break;
        }
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

            dpkernel_task *dpk_task =
                client_state->dpk_tasks[current_request->key % client_state->dpk_tasks.size()]; // DON'T NEED mod
            // printf("Thread %zu: Processing request for key %lu\n", client_state->thread_state_idx,
            //        current_request->key);

            size_t available_space = SEND_BUFFER_SIZE - client_state->send_used_size;
            size_t bytes_to_write = std::min(ON_WIRE_RESPONSE_SIZE, available_space);
            if (bytes_to_write < ON_WIRE_RESPONSE_SIZE)
            {
                // Not enough space in the send buffer
                break;
            }

            if (client_state->send_end + bytes_to_write <= SEND_BUFFER_SIZE) // no wrap around
            {
                int output_tasks_idx = client_state->send_end / ON_WIRE_RESPONSE_SIZE;
                printf("Thread %zu: Using output task idx %d for key %lu\n", client_state->thread_state_idx,
                       output_tasks_idx, current_request->key);

                dpk_task->base.out = client_state->output_tasks[output_tasks_idx]->base.out;
                dpk_task->bf3.dst_doca_buf = client_state->output_tasks[output_tasks_idx]->bf3.dst_doca_buf;
                printf("dst doca buf: %p\n", client_state->output_tasks[output_tasks_idx]->bf3.dst_doca_buf);
                Response *this_resp = reinterpret_cast<Response *>(client_state->send_buffer +
                                                                   (output_tasks_idx * ON_WIRE_RESPONSE_SIZE));
                if (dpk_task->base.out != app_get_shm_ptr_for_output_buf((char *)&this_resp->value))
                {
                    printf("Thread %zu: Output task out pointer mismatch for key %lu\n", client_state->thread_state_idx,
                           current_request->key);
                    printf("Expected: %zu, Got: %zu\n", app_get_shm_ptr_for_output_buf((char *)&this_resp->value),
                           dpk_task->base.out);
                    exit(-1);
                }
                // doca_buf_set_data(dpk_task->bf3.dst_doca_buf, &(this_resp->value), bytes_to_write);

                dpm_submit_task_msgq_blocking(client_state->thread_state_idx, dpk_task, DEVICE_BLUEFIELD_3);
                printf("Thread %zu: Submitted task %zu for key %lu\n", client_state->thread_state_idx,
                       app_get_shm_ptr_for_task(dpk_task), current_request->key);

                // client_state->pending_decompression_ops.push_back({current_request->key, dpk_task});
                client_state->pending_decompression_ops[output_tasks_idx] = {current_request->key, dpk_task};
            }
            else
            {
                printf("wrap around should not happen, send_end: %zu, bytes_to_write: %zu\n", client_state->send_end,
                       bytes_to_write);
                // size_t first_chunk_size = SEND_BUFFER_SIZE - client_state->send_end;
                // std::memcpy(buffer_.data() + write_ptr_, data, first_chunk_size);

                // size_t second_chunk_size = bytes_to_write - first_chunk_size;
                // std::memcpy(buffer_.data(), data + first_chunk_size, second_chunk_size);
            }
            client_state->send_end = (client_state->send_end + bytes_to_write) % SEND_BUFFER_SIZE;
            client_state->send_used_size += bytes_to_write;

            ////
            /* if (client_state->send_end_tentative + ON_WIRE_RESPONSE_SIZE > SEND_BUFFER_SIZE)
            {
                break;
            }


            int output_tasks_idx = client_state->send_end_tentative / ON_WIRE_RESPONSE_SIZE;
            client_state->send_end_tentative += ON_WIRE_RESPONSE_SIZE;

            printf("Thread %zu: Using output task idx %d for key %lu\n", client_state->thread_state_idx,
                   output_tasks_idx, current_request->key);
            // dpk_task->bf3.dst_doca_buf = client_state->output_tasks[output_tasks_idx]->bf3.dst_doca_buf;
            doca_buf_set_data(dpk_task->bf3.dst_doca_buf, data, len);
                // dpk_task->base.out_size = __UNCOMPRESSED_SIZE; */

            // auto addr = (current_request->key * VALUE_SIZE) % local_store_size_;
            // char *local_output_buf_ptr =
            //     local_output_buffer_ + (current_request->key * __UNCOMPRESSED_SIZE) % local_output_buffer_size_;

            // DON'T handle the send buffer here, it will be done when polling completion
            /* auto response = reinterpret_cast<Response *>(client_state->send_buffer + client_state->send_end);

            client_state->send_end += ON_WIRE_RESPONSE_SIZE;
            response->key = current_request->key; */
            // don't do memcpy on the critical path
            // std::memcpy(response->value, local_output_buf_ptr, __UNCOMPRESSED_SIZE);
        }
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
    // if is empty, return
    // if (client_state->send_used_size == 0)
    if (client_state->send_completed_size == 0)
    {
        return;
    }

    size_t total_processed_this_call = 0;
    // --- Attempt to process the first (potentially only) contiguous block ---
    // This block starts at read_ptr_.
    size_t first_block_len;
    if (client_state->send_start < client_state->send_completed)
    {
        // Data is not wrapped
        // first_block_len = client_state->send_end - client_state->send_start;
        first_block_len = client_state->send_completed - client_state->send_start;
    }
    else
    {
        // read_ptr_ >= write_ptr_
        // Data is wrapped
        // Or buffer is full and read_ptr_ can be anywhere.
        // The first contiguous block is from read_ptr_ to the end of the buffer.
        first_block_len = SEND_BUFFER_SIZE - client_state->send_start;
    }

    if (first_block_len > 0)
    {
        printf("Thread %zu: first_block_len: %zu, send_start: %zu, send_completed: %zu\n",
               client_state->thread_state_idx, first_block_len, client_state->send_start, client_state->send_completed);
        const char *data_chunk1 = client_state->send_buffer + client_state->send_start;
        int processed_count1 = send(client_state->client_fd, data_chunk1, first_block_len, 0);
        // processor(data_chunk1, first_block_len);

        // The processor should not report consuming more than offered.
        // Clamp to be safe, or add error handling if it does.
        // processed_count1 = std::min(processed_count1, first_block_len);

        if (processed_count1 > 0)
        {
            client_state->send_start = (client_state->send_start + processed_count1) % SEND_BUFFER_SIZE;
            // client_state->send_used_size -= processed_count1;
            client_state->send_completed_size -= processed_count1;
            total_processed_this_call += processed_count1;
        }
        else
        {
            if (errno != EWOULDBLOCK && errno != EAGAIN)
            {
                if (!running_)
                    return;
                perror("ClientConnectionThread: send error");
                error = true;
                return; // Exit on error
            }
            // If EWOULDBLOCK, can't send more now, will retry later.
            // return total_processed_this_call; // No data sent this call.
            return;
        }

        // If the processor didn't consume the entire first offered block,
        // we stop and don't offer the second (if any).
        // The consumer signals it's not ready for more from this call.
        if (processed_count1 < first_block_len)
        {
            // return total_processed_this_call;
            return;
        }

        // --- If the first block was fully consumed AND data was wrapped AND there's still data ---
        // This situation arises if:
        // 1. `processed_count1 == first_block_len` (the consumer took all of the first chunk).
        // 2. `read_ptr_` is now `0` (meaning the first chunk ended exactly at `capacity_`).
        // 3. `used_size_ > 0` (there's still data left, which must be at the start of the buffer).
        if (
            // client_state->send_used_size > 0
            client_state->send_completed_size > 0 && client_state->send_start == 0 &&
            total_processed_this_call > 0 /*ensures first block was attempted & fully consumed*/)
        {
            // The length of this second block is simply the remaining used_size_,
            // which is guaranteed to be contiguous from the start of the buffer up to the write_ptr_.
            size_t second_block_len =
                // client_state->send_used_size; // This is equivalent to write_ptr_ in this wrapped scenario.
                client_state->send_completed_size;

            if (second_block_len > 0)
            {                                                        // Should be true if used_size_ > 0
                const char *data_chunk2 = client_state->send_buffer; // Starts from index 0
                size_t processed_count2 = send(client_state->client_fd, data_chunk2, second_block_len, 0);

                processed_count2 = std::min(processed_count2, second_block_len); // Clamp

                if (processed_count2 > 0)
                {
                    printf("Thread %zu: second_block_len: %zu, processed_count2: %zu\n", client_state->thread_state_idx,
                           second_block_len, processed_count2);
                    client_state->send_start =
                        (client_state->send_start + processed_count2) % SEND_BUFFER_SIZE; // read_ptr_ was 0
                    // client_state->send_used_size -= processed_count2;
                    client_state->send_completed_size -= processed_count2;
                    total_processed_this_call += processed_count2;
                }
                else
                {
                    if (errno != EWOULDBLOCK && errno != EAGAIN)
                    {
                        if (!running_)
                            return;
                        perror("ClientConnectionThread: send error");
                        error = true;
                        return; // Exit on error
                    }
                    // If EWOULDBLOCK, can't send more now, will retry later.
                    // return total_processed_this_call; // No data sent this call.
                    return;
                }
            }
        }
    }
    // return total_processed_this_call;
    printf("Thread %zu: Sent %zu bytes to client fd %d\n", client_state->thread_state_idx, total_processed_this_call,
           client_state->client_fd);

    ////
    /* size_t current_send_data_len = client_state->send_end - client_state->send_start;

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
    } */
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