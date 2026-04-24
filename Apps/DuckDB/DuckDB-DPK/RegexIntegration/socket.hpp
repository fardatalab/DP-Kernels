#pragma once

#include <assert.h>
#include <iostream>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <chrono>

#define PORT 3232 
#define IPADDR "127.0.0.1"
#define BUF_SIZE 1024

using namespace std;

// helpers
int set_up_connetion(int port, const char* ip, struct sockaddr_in* peer);

// client-specific functions
int client_init(int port, const char* ip);

// server-specific functions
int server_init(int port, const char* ip);
int accept_connection(int listen_fd);
