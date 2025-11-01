#include "faster_bench_common.hpp"
#include "rdma_comm.hpp"
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>

extern vector<char *> contig_input_buf_per_thread;  // for DPU threads, each thread has its own input buffer
extern vector<char *> contig_output_buf_per_thread; // for DPU threads, each thread has its own output buffer

// no. of threads for submitting to DPM, this can be diff from N_THREADS (which are client <-> server threads)
#define N_DPK_THREADS 2

/// act as a server to the client threads (sending back responses), each thread will have its own RDMA connection
int setup_client_connections(int NumConnections, int PortBase, int QueueDepth, int BatchSize, int MaxSendMsgSize,
                             int MaxRecvMsgSize);

/// act as a client (relaying requests to the server threads), each thread will have its own RDMA connection
int setup_server_connections(int NumConnections, int PortBase, int BatchSize, int QueueDepth);

void dpu_thread_func(RDMAConnectionResource &conn, int ThreadNum, int BatchSize, const uint64_t ReadNum,
                     const int QueueDepth);

bool setup_rdma_dpu_server_qp_and_buffers(rdma_cm_id *id, RDMAConnectionResource &conn, int QueueDepth, int BatchSize,
                                          int MaxSendMsgSize, int MaxRecvMsgSize, int thread_idx);