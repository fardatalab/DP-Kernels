#pragma once

#include <chrono>

#define CLIENT_IP "192.168.100.2"//"10.1.0.6"
#define SERVER_IP "10.10.1.42"//"10.1.0.6"
#define SERVER_PORT 3232

#define SECTOR_SIZE 4096

#define SEND_PACKET_SIZE 1024

using namespace std;
using namespace std::chrono;

struct MessageHeader {
	long long TimeSend;
	uint16_t BatchId;
	uint64_t Key;
	uint64_t Value;
	uint8_t Operation;
};