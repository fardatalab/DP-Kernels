#include "page_server.hpp"

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
#include <sched.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <vector>

namespace dpk
{
namespace page_server
{

PageServer::PageServer() : running_(false)
{
}

PageServer::~PageServer()
{
    if (running_)
    {
        Stop();
    }
    if (local_store_)
    {
        delete[] local_store_;
    }
    if (listen_fd_ != -1)
    {
        close(listen_fd_);
    }
    for (auto fd : seg_fd_)
    {
        close(fd);
    }
}

void PageServer::Configure(const Config &config)
{
    config_ = config;
    auto num_pages = std::min(config_.database_size, config_.local_capacity) / VALUE_SIZE;
    std::cout << "num_pages: " << num_pages << std::endl;
    InitializeIndex(num_pages);
}

// num_pages is the number of pages we want in the final
bool PageServer::InitializeIndex(uint32_t num_pages)
{
    // for now, we just use the INPUT_FILE as the page server storage
    // get the size of the file
    std::ifstream file(INPUT_FILE, std::ios::binary | std::ios::ate);
    if (!file)
    {
        std::cerr << "Error: Cannot open file " << INPUT_FILE << '\n';
        return false;
    }
    std::streamsize file_streamsize = file.tellg();
    if (file_streamsize < 0)
    {
        std::cerr << "Error: Failed to determine file size\n";
        return false;
    }
    size_t file_size = static_cast<size_t>(file_streamsize);
    segment_size_ = file_size;
    file.close();

    auto num_pages_in_segment = file_size / VALUE_SIZE;
    num_pages_in_segment_ = num_pages_in_segment;
    auto num_replications = num_pages / num_pages_in_segment;

    store_ = new DDS_FrontEnd::DDSFrontEnd("DDS-Store0");
    const char* dir_name = "/data";
    const char* file_name = "/data/4k.deflate";
    DirIdT root_dir_id = DDS_DIR_INVALID;
    FileIdT file_id = DDS_FILE_INVALID;

    store_->Initialize();
    auto result = store_->CreateDirectory(dir_name, &root_dir_id);
    if (result != DDS_ERROR_CODE_SUCCESS) {
        std::cerr << "Failed to create directory" << std::endl;
        return false;
    }
    result = store_->CreateFile(file_name, 0, 0, 0, &file_id);

    for (size_t i = 0; i < num_replications; ++i)
    {
        // open the file this many times. Note that this is not necessary, we can just open it once and just compute
        // offset
        auto fd = open(INPUT_FILE, O_RDONLY | O_NOATIME | O_CLOEXEC);
        if (fd == -1)
        {
            std::cerr << "Error: failed to open database files: " << INPUT_FILE << '\n';
            return false;
        }
        seg_fd_.push_back(fd);
        seg_file_ids_.push_back(file_id);
        // open the DDS equivalent
    }

    return true;
}

bool PageServer::InitializeServerSocket()
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

void PageServer::Run()
{
    if (!InitializeServerSocket())
    {
        std::cerr << "Failed to initialize server socket. Aborting Run." << std::endl;
        return;
    }

    running_ = true;

    // Start the acceptor thread
    threads_.emplace_back(&PageServer::AcceptorThread, this);
}

void PageServer::Stop()
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

void PageServer::AcceptorThread()
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
            // client_states_[client_fd]->scratch = new char[VALUE_SIZE * MAX_OUTSTANDING_IO]; // buffer
            for (int i = 0; i != MAX_OUTSTANDING_IO; ++i) {
                client_states_[client_fd]->scratch_buffers_.push(new char[VALUE_SIZE]);
            }
        }

        // Start client connection thread
        {
            std::lock_guard<std::mutex> lock(threads_mutex_);
            threads_.emplace_back(&PageServer::ClientConnectionThread, this, client_fd);
        }

        char client_ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip_str, INET_ADDRSTRLEN);
        std::cout << "AcceptorThread: Accepted new connection from " << client_ip_str << ":"
                  << ntohs(client_addr.sin_port) << " on fd: " << client_fd << std::endl;
    }
}

void PageServer::ClientConnectionThread(int client_fd)
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
        select_timeout.tv_usec = 100; // 100us timeout

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
        PollDDSCompletions(client_state);

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

void PageServer::HandleClientRead(ClientState *client_state, bool &error)
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

void PageServer::HandleClientRequests(ClientState *client_state)
{
    uint64_t requests_processed = 0;
    while (running_)
    {
        auto remaining_bytes = client_state->recv_end - client_state->recv_start;
        if (remaining_bytes < ON_WIRE_REQUEST_SIZE)
        {
            break;
        }

        Request *current_request =
            reinterpret_cast<Request *>(client_state->recv_buffer.data() + client_state->recv_start);

        // if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > SEND_BUFFER_SIZE)
        // {
        //     break;
        // }
        // auto response = reinterpret_cast<Response *>(client_state->send_buffer + client_state->send_end);
        // handle the request (we might have to replace these with DDS file operations)
        // find the segment and offset
        if (client_state->scratch_buffers_.empty()) {
            // we need to wait for the reads to complete
            break;
        }
      
        auto read_buffer = client_state->scratch_buffers_.front();
        client_state->scratch_buffers_.pop();
        size_t page_logical_addr = current_request->key * VALUE_SIZE;
        size_t segment = current_request->key / num_pages_in_segment_;
        size_t offset = page_logical_addr % segment_size_;
        // pread the data from the segment and offset
        FileIOSizeT bytes_serviced;
        auto result = store_->PReadFile(seg_file_ids_[segment], 
                          offset, 
                          read_buffer, 
                          VALUE_SIZE, 
                          &bytes_serviced, 
                          nullptr, 
                          read_buffer); // context is the read buffer pointer
        if (result == DDS_ERROR_CODE_TOO_MANY_REQUESTS || 
            result == DDS_ERROR_CODE_REQUEST_RING_FAILURE) {
            // the outbound functions will poll the store for completions
            break;
        }
        client_state->recv_start += ON_WIRE_REQUEST_SIZE;
        requests_processed++;
        // auto n = pread(seg_fd_[segment], (void *)client_state->scratch, VALUE_SIZE, offset);
        // if (n != VALUE_SIZE)
        // {
        //     std::cerr << "Error read file " << INPUT_FILE << " at offset: " << offset << '\n';
        //     continue;
        // }
        // libdeflate decompression
        // size_t actual_out_size = 0;
        // if (auto result = libdeflate_deflate_decompress(client_state->decompressor, client_state->scratch, VALUE_SIZE,
        //                                                 &response->value, __UNCOMPRESSED_SIZE,
        //                                                 &actual_out_size) != LIBDEFLATE_SUCCESS)
        // {
        //     printf("Decompression failed for key %lu, result: %d\n", current_request->key, result);
        // }

        // client_state->send_end += ON_WIRE_RESPONSE_SIZE;
        // response->key = current_request->key;
        // client_state->recv_start += ON_WIRE_REQUEST_SIZE;
        // requests_processed++;
    }
    client_state->total_processed_requests += requests_processed;
}

void PageServer::CompactReadBuffer(ClientState *client_state)
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

void PageServer::PollDDSCompletions(ClientState *client_state) {
    PollIdT poll_id;
    ErrorCodeT result = store_->GetDefaultPoll(&poll_id);
    if (result != DDS_ERROR_CODE_SUCCESS) {
        std::cerr << "Failed to get default poll" << std::endl;
        return;
    }
    bool poll_result = false;
    ContextT poll_context = nullptr;
    FileIOSizeT op_size = 0;
    while (true) {
        if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > SEND_BUFFER_SIZE) break;
        store_->PollWait(poll_id, &op_size, &poll_context, nullptr, 0, &poll_result);
        if (poll_result) {
#ifdef DPK_ACTIVE
            // no need to decompress, just send back
            auto read_buffer = (char*)poll_context;
            client_state->scratch_buffers_.push(read_buffer);
            auto response = reinterpret_cast<Response *>(client_state->send_buffer + client_state->send_end);
            response->key = 0; // dummy key, can change if needed
            std::memcpy(response->value, read_buffer, VALUE_SIZE);
            client_state->send_end += ON_WIRE_RESPONSE_SIZE;
#else
            // decompress and send back
            auto read_buffer = (char*)poll_context;
            client_state->scratch_buffers_.push(read_buffer);
            // decompress
            auto response = reinterpret_cast<Response *>(client_state->send_buffer + client_state->send_end);
            size_t actual_out_size = 0;
            if (auto result = libdeflate_deflate_decompress(client_state->decompressor, 
                                                            read_buffer, 
                                                            VALUE_SIZE,
                                                            &response->value, 
                                                            __UNCOMPRESSED_SIZE,
                                                            &actual_out_size) != LIBDEFLATE_SUCCESS) {
                printf("Decompression failed for key %lu, result: %d\n", response->key, result);
            }
            client_state->send_end += ON_WIRE_RESPONSE_SIZE;
#endif
        } else break;
    }
}   

void PageServer::SendClientData(ClientState *client_state, bool &error)
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

} // namespace page_server
} // namespace dpk