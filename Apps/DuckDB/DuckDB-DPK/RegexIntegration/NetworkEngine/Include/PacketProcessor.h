#ifndef PACKETPROCESSOR_H
#define PACKETPROCESSOR_H

#include <stdio.h>
#include <stdint.h>
#include <unistd.h>

#define DUCKDB_DB_FILE_PATH "/home/ubuntu/dbfiles/sf10.db"

extern char** contig_strings;
extern size_t n_bufs;
extern size_t file_size;

// Tentative Packet Processor Interface
int ProcessPacketMsg(char *msg, int msg_len);

#endif  // PACKETPROCESSOR_H
