#include "socket.hpp"

#define QUERY "SELECT COUNT(l_comment) FROM comment WHERE l_comment like '%regular%';"
#define SERVER_IPADDR "128.110.216.201"
#define CHECKSUM 0xdeadbeef

ssize_t client_send_query(int server_sock, const char* query);
ssize_t client_fetch_result(int server_sock, size_t* result_ptr);

int main() {
    uint64_t recv_checksum;
    ssize_t nbytes_read, nbytes_sent;

    std::cout << "[client] setting up server's socket...\n";
    int server_sock = client_init(PORT, SERVER_IPADDR);
    std::cout << "[client] server socket is " << server_sock << ", about to send a query to the server...\n";
    
    // wait until the server has brought tables into memory
    nbytes_read = recv(server_sock, &recv_checksum, sizeof(uint64_t), 0);
    if (nbytes_read < 0) {
        printf("error: failed to read from server socket %d\n", server_sock);
        close(server_sock);
        exit(1);
    }
    if (nbytes_read == 0) {
        printf("server socket %d already closed\n", server_sock);
        exit(1);
    }
    recv_checksum = ntohl(recv_checksum);
    if (recv_checksum != CHECKSUM) {
        printf("[client] error: wrong checksum bytes received from server\n");
        close(server_sock);
        exit(1);
    }
    
    // start timing the query latency here
    auto start = chrono::high_resolution_clock::now();
    nbytes_sent = client_send_query(server_sock, QUERY);
    if (nbytes_sent < 0) {
        printf("[client] error: failed to write to server socket %d\n", server_sock);
        close(server_sock);
        exit(1);
    }
    if (nbytes_sent == 0) {
        printf("server socket %d already closed\n", server_sock);
        exit(1);
    }

    size_t num_matches;
    nbytes_read = client_fetch_result(server_sock, &num_matches);
    if (nbytes_read < 0) {
        printf("[client] error: failed to read from server socket %d\n", server_sock);
        close(server_sock);
        exit(1);
    }
    if (nbytes_read == 0) {
        printf("server socket %d already closed\n", server_sock);
        exit(1);
    }
    auto end = chrono::high_resolution_clock::now();
    std::chrono::duration<double> rrt = end - start;
    std::cout << "[client] server returns " << num_matches << " matches (" << rrt.count() << " sec)\n";

    close(server_sock);
    return 0;
}

ssize_t client_send_query(int server_sock, const char* query) {
    char buffer[BUF_SIZE] = {0};
    memcpy(buffer, query, strlen(query));
    ssize_t nbytes_sent = send(server_sock, buffer, strlen(query), 0);
    return nbytes_sent;
}

ssize_t client_fetch_result(int server_sock, size_t* result_ptr) {
    return recv(server_sock, result_ptr, sizeof(size_t), 0);
}