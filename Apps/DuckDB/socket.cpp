#include "socket.hpp"

/*******************************************
*                helpers
********************************************/
int set_up_connetion(int port, const char* ip, struct sockaddr_in* peer) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        exit(1);
    }
    peer->sin_family = AF_INET;
    peer->sin_port = htons(port);
    inet_pton(AF_INET, ip, &peer->sin_addr);
    return sock;
}

/*******************************************
*        client-specific functions
********************************************/
int client_init(int port, const char* ip) {
    struct sockaddr_in server;
    int server_sock = set_up_connetion(port, ip, &server);

    printf("[client] trying to connect with the server %d...\n", server_sock);
    if (connect(server_sock, (struct sockaddr *)&server, sizeof(server))) {
        perror("connect");
        close(server_sock);
        exit(1);
    }

    return server_sock;
}

/*******************************************
*        server-specific functions
********************************************/
int server_init(int port, const char* ip) {
    struct sockaddr_in server;
    int sockfd = set_up_connetion(port, ip, &server);
    if (bind(sockfd, (struct sockaddr *)&server, sizeof(server))) {
        perror("bind");
        close(sockfd);
        exit(1);
    }
    if (listen(sockfd, 1) < 0) {
        perror("listen");
        close(sockfd);
        exit(1);
    }
    return sockfd;
}

int accept_connection(int listen_fd) {
    struct sockaddr_in peer;
    socklen_t peer_len = sizeof(peer);
    int client_sock = accept(listen_fd, (struct sockaddr *)&peer, &peer_len);
    if (client_sock < 0) {
        perror("accept");
        close(listen_fd);
        return -1;
    } else {
        return client_sock;
    }
}