#pragma once

#include "ndcommon.h"
// ntestutil is the main dependency, define server and client base classes
#include "ndtestutil.h"
#include <logging.h>
#include <iostream>

// rpc state machine: context msg flags
#define RECV_CTXT ((void *) 0x1000) // e.g., peer info msg
#define SEND_CTXT ((void *) 0x2000)
#define READ_CTXT ((void *) 0x3000) // RDMA read cmd
#define WRITE_CTXT ((void *) 0x4000)  // RDMA write cmd


/*
QZ: RDMC is the name of the project.
*/

#define RDMC_VERBOSE 1
// #undef RDMC_VERBOSE

#ifdef RDMC_VERBOSE
#define RDMC_DEBUG(msg) \
do { \
    cout << msg << endl; \
} while(0)
#else
#define RDMC_DEBUG(msg) \
do { } while(0)
#endif

#define RDMC_DEFAUL_PORT 54328
#define RDMC_MANAGER_PORT 54329

// #define RDMC_MEASURE_LATENCY 1
#undef RDMC_MEASURE_LATENCY

// #define RDMC_FRONT_MEASURE_LATENCY 1
#undef RDMC_FRONT_MEASURE_LATENCY

void RDMC_WaitForEventNotification(
    IND2CompletionQueue * p_cq,
    OVERLAPPED * p_ov);

void RDMC_WaitForCompletion(
    IND2CompletionQueue* p_cq,
    OVERLAPPED* p_ov,
    const std::function<void(ND2_RESULT*)>& process_completion_fn,
    bool b_blocking);

// wait for CQ entry and get the result
void RDMC_WaitForCompletion(
    IND2CompletionQueue* p_cq,
    OVERLAPPED* p_ov, ND2_RESULT* p_result,
    bool b_blocking = false);

// wait for CQ entry and check context
void RDMC_WaitForCompletionAndCheckContext(
    IND2CompletionQueue* p_cq,
    OVERLAPPED* p_ov,
    void* expected_context,
    bool b_blocking = false);

void RDMC_OpenAdapter(
    IND2Adapter * *p_adapter,
    struct sockaddr_in* p_v4_src,
    HANDLE * p_adapter_file,
    OVERLAPPED * p_ov,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to OpenAdapter");

void RDMC_GetAdapterFile(
    IND2Adapter* p_adapter,
    HANDLE* p_adapter_file,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to GetAdapterFile");

void RDMC_GetAdapterInfo(IND2Adapter* p_dapter,
    ND2_ADAPTER_INFO* p_dapter_info,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to GetAdapterInfo");

void RDMC_CreateListener(IND2Adapter* p_adapter,
    HANDLE h_adapter_file,
    IND2Listener** pp_listener,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to CreateListener");

void RDMC_Listen(IND2Listener* p_listener,
    const sockaddr_in& v4_src,
    HRESULT expectedResult = ND_SUCCESS,
    const char* errorMessage = "Failed to Listen");

void RDMC_StopListening(IND2Listener* p_listener,
    HRESULT expectedResult = ND_SUCCESS,
    const char* errorMessage = "Failed to StopListening");

void RDMC_CreateConnector(
    IND2Adapter* p_adapter,
    HANDLE h_adapter_file,
    IND2Connector** pp_connector,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to CreateConnector");

void RDMC_GetConnectionRequest(
    IND2Listener* p_listener,
    IND2Connector* p_connector,
    OVERLAPPED* p_ov,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to GetConnectionRequest");

void RDMC_CreateCQ(
    IND2Adapter* p_adapter,
    HANDLE h_adapter_file,
    DWORD depth,
    IND2CompletionQueue** pp_cq,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to CreateCQ");

void RDMC_CreateQueuePair(
    IND2Adapter* p_adapter,
    IND2CompletionQueue* p_cq,
    DWORD queue_depth, DWORD n_sge,
    DWORD inline_data_size,
    IND2QueuePair** pp_qp,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to CreateQueuePair");

void RDMC_CreateQueuePairWithSeparateCQ(
    IND2Adapter* p_adapter,
    IND2CompletionQueue* p_cq_send,
    IND2CompletionQueue* p_cq_recv,
    DWORD queue_depth, DWORD n_sge,
    DWORD inline_data_size,
    IND2QueuePair** pp_qp,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to CreateQueuePairWithSeparateCQ");

void RDMC_CreateMR(
    IND2Adapter* p_adapter,
    HANDLE h_adapter_file,
    IND2MemoryRegion** pp_mr,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to CreateMR");

void RDMC_RegisterDataBuffer(
    IND2MemoryRegion* p_mr,
    void* p_buf,
    DWORD buffer_length,
    ULONG type,
    OVERLAPPED* p_ov,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to RegisterDataBuffer");

void RDMC_Accept(
    IND2Connector* p_connector,
    IND2QueuePair* p_qp,
    OVERLAPPED* p_ov,
    DWORD inbound_read_limit,
    DWORD outbound_read_limit,
    const VOID* p_private_data = (void*)nullptr,
    DWORD cb_private_data = 0UL,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to Accept");

void RDMC_CreateMW(
    IND2Adapter* p_adapter,
    IND2MemoryWindow** pp_mw,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to CreateMemoryWindow");

void RDMC_Bind(
    IND2QueuePair* p_qp,
    IND2MemoryRegion* p_mr,
    IND2MemoryWindow* p_mw,
    const void* p_buf,
    DWORD buffer_length,
    ULONG flags,
    IND2CompletionQueue* p_cq,
    OVERLAPPED* p_ov,
    void* context = (void*)nullptr,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to Bind");

void RDMC_Send(
    IND2QueuePair* p_qp,
    const ND2_SGE* sge,
    const ULONG n_sge,
    ULONG flags,
    void* request_context = (void*)nullptr,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to Send");

void RDMC_PostReceive(
    IND2QueuePair* p_qp,
    const ND2_SGE* sge,
    const DWORD n_sge,
    void* request_context = (void*)nullptr,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to PostReceive");

void RDMC_Connect(
    IND2Connector* p_connector,
    IND2QueuePair* p_qp,
    OVERLAPPED* p_ov,
    _In_ const sockaddr_in& v4_src,
    _In_ const sockaddr_in& v4_dst,
    DWORD inbound_read_limit,
    DWORD outbound_read_limit,
    const VOID* p_private_data = (void*)nullptr,
    DWORD cb_private_data = 0UL,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to Connect");

void RDMC_CompleteConnect(
    IND2Connector* p_connector,
    OVERLAPPED* p_ov,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to CompleteConnect");

void RDMC_Read(
    IND2QueuePair* p_qp,
    const ND2_SGE* sge,
    const ULONG n_sge,
    UINT64 remote_address,
    UINT32 remote_token,
    DWORD flag,
    void* request_context,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to Read");

void RDMC_Write(
    IND2QueuePair* p_qp,
    const ND2_SGE* sge,
    const ULONG n_sge,
    UINT64 remote_address,
    UINT32 remote_token,
    DWORD flag,
    void* request_context,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to Write");

void RDMC_Write_Debugging(
    IND2QueuePair* p_qp,
    const ND2_SGE* sge,
    const ULONG n_sge,
    UINT64 remote_address,
    UINT32 remote_token,
    DWORD flag,
    void* request_context,
    HRESULT expected_result = ND_SUCCESS,
    const char* error_message = "Failed to Write");

void RDMC_DisconnectConnector(IND2Connector* p_connector, OVERLAPPED* p_ov);

void RDMC_DeregisterMemory(IND2MemoryRegion* p_mr, OVERLAPPED* p_ov);