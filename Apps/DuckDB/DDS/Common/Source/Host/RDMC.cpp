#include "RDMC.h"

void RDMC_WaitForEventNotification(
    IND2CompletionQueue* p_cq,
    OVERLAPPED* p_ov) {
    HRESULT hr = p_cq->Notify(ND_CQ_NOTIFY_ANY, p_ov);
    if (hr == ND_PENDING)
    {
        hr = p_cq->GetOverlappedResult(p_ov, TRUE);
    }
}

void RDMC_WaitForCompletion(
    IND2CompletionQueue* p_cq,
    OVERLAPPED* p_ov,
    const std::function<void(ND2_RESULT*)>& process_completion_fn,
    bool b_blocking) {
    for (;;)
    {
        ND2_RESULT nd_res;
        if (p_cq->GetResults(&nd_res, 1) == 1)
        {
            process_completion_fn(&nd_res);
            break;
        }
        if (b_blocking)
        {
            RDMC_WaitForEventNotification(p_cq, p_ov);
        }
    };
}

// wait for CQ entry and get the result
void RDMC_WaitForCompletion(
    IND2CompletionQueue* p_cq,
    OVERLAPPED* p_ov,
    ND2_RESULT* p_result,
    bool b_blocking) {
    RDMC_WaitForCompletion(p_cq, p_ov, [&p_result](ND2_RESULT* p_comp_res)
        {
            *p_result = *p_comp_res;
        }, b_blocking);
}

// wait for CQ entry and check context
void RDMC_WaitForCompletionAndCheckContext(
    IND2CompletionQueue* p_cq,
    OVERLAPPED* p_ov,
    void* expected_context,
    bool b_blocking) {
    RDMC_WaitForCompletion(p_cq, p_ov, [&expected_context](ND2_RESULT* p_comp)
        {
            if (ND_SUCCESS != p_comp->Status)
            {
                LogIfErrorExit(p_comp->Status, ND_SUCCESS, "Unexpected completion status", __LINE__);
            }
            if (expected_context != p_comp->RequestContext)
            {
                std::cout << "Wrong context = " << p_comp->RequestContext << ", expected context = " << expected_context << std::endl;
                LogErrorExit("Unexpected completion\n", __LINE__);
            }
        }, b_blocking);
}

void RDMC_OpenAdapter(
    IND2Adapter** p_adapter,
    struct sockaddr_in* p_v4_src,
    HANDLE* p_adapter_file,
    OVERLAPPED* p_ov,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = NdOpenAdapter(
        IID_IND2Adapter,
        reinterpret_cast<const struct sockaddr*>(p_v4_src),
        sizeof(*p_v4_src),
        reinterpret_cast<void**>(p_adapter)
    );
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);

    p_ov->hEvent = CreateEvent(nullptr, FALSE, FALSE, nullptr);
    if (p_ov->hEvent == nullptr) {
        LogErrorExit(hr, "Failed to CreateEvent", __LINE__);
    }

    // Get the file handle for overlapped operations on this adapter.
    hr = (*p_adapter)->CreateOverlappedFile(p_adapter_file);
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_GetAdapterFile(
    IND2Adapter* p_adapter,
    HANDLE* p_adapter_file,
    HRESULT expected_result,
    const char* error_message) {
    // Get the file handle for overlapped operations on this adapter.
    HRESULT hr = p_adapter->CreateOverlappedFile(p_adapter_file);
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_GetAdapterInfo(IND2Adapter* p_dapter,
    ND2_ADAPTER_INFO* p_dapter_info,
    HRESULT expected_result,
    const char* error_message) {
    ULONG adapter_info_size = sizeof(*p_dapter_info);
    memset(p_dapter_info, 0, adapter_info_size);
    p_dapter_info->InfoVersion = ND_VERSION_2;
    HRESULT hr = p_dapter->Query(p_dapter_info, &adapter_info_size);

    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_CreateListener(IND2Adapter* p_adapter,
    HANDLE h_adapter_file,
    IND2Listener** pp_listener,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_adapter->CreateListener(
        IID_IND2Listener,
        h_adapter_file,
        reinterpret_cast<VOID**>(pp_listener)
    );
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_Listen(IND2Listener* p_listener,
    const sockaddr_in& v4_src,
    HRESULT expectedResult,
    const char* errorMessage) {
    HRESULT hr = p_listener->Bind(
        reinterpret_cast<const sockaddr*>(&v4_src),
        sizeof(v4_src)
    );
    LogIfErrorExit(hr, expectedResult, "Bind failed", __LINE__);
    hr = p_listener->Listen(0);
    LogIfErrorExit(hr, expectedResult, errorMessage, __LINE__);
}

void RDMC_StopListening(IND2Listener* p_listener,
    HRESULT expectedResult,
    const char* errorMessage) {
    HRESULT hr = p_listener->Release();
    LogIfErrorExit(hr, expectedResult, errorMessage, __LINE__);
}

void RDMC_CreateConnector(
    IND2Adapter* p_adapter,
    HANDLE h_adapter_file,
    IND2Connector** pp_connector,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_adapter->CreateConnector(
        IID_IND2Connector,
        h_adapter_file,
        reinterpret_cast<VOID**>(pp_connector)
    );
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_GetConnectionRequest(
    IND2Listener* p_listener,
    IND2Connector* p_connector,
    OVERLAPPED* p_ov,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_listener->GetConnectionRequest(p_connector, p_ov);
    while (hr == ND_PENDING)
    {
        hr = p_listener->GetOverlappedResult(p_ov, TRUE);
    }
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_CreateCQ(
    IND2Adapter* p_adapter,
    HANDLE h_adapter_file,
    DWORD depth,
    IND2CompletionQueue** pp_cq,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_adapter->CreateCompletionQueue(
        IID_IND2CompletionQueue,
        h_adapter_file,
        depth,
        0,
        0,
        reinterpret_cast<VOID**>(pp_cq)
    );
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_CreateQueuePair(
    IND2Adapter* p_adapter,
    IND2CompletionQueue* p_cq,
    DWORD queue_depth, DWORD n_sge,
    DWORD inline_data_size,
    IND2QueuePair** pp_qp,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_adapter->CreateQueuePair(
        IID_IND2QueuePair,
        p_cq,
        p_cq,
        nullptr,
        queue_depth,
        queue_depth,
        n_sge,
        n_sge,
        inline_data_size,
        reinterpret_cast<VOID**>(pp_qp)
    );
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_CreateQueuePairWithSeparateCQ(
    IND2Adapter* p_adapter,
    IND2CompletionQueue* p_cq_send,
    IND2CompletionQueue* p_cq_recv,
    DWORD queue_depth, DWORD n_sge,
    DWORD inline_data_size,
    IND2QueuePair** pp_qp,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_adapter->CreateQueuePair(
        IID_IND2QueuePair,
        p_cq_recv,
        p_cq_send,
        nullptr,
        queue_depth,
        queue_depth,
        n_sge,
        n_sge,
        inline_data_size,
        reinterpret_cast<VOID**>(pp_qp)
    );
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_CreateMR(
    IND2Adapter* p_adapter,
    HANDLE h_adapter_file,
    IND2MemoryRegion** pp_mr,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_adapter->CreateMemoryRegion(
        IID_IND2MemoryRegion,
        h_adapter_file,
        reinterpret_cast<VOID**>(pp_mr)
    );
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_RegisterDataBuffer(
    IND2MemoryRegion* p_mr,
    void* p_buf,
    DWORD buffer_length,
    ULONG type,
    OVERLAPPED* p_ov,
    HRESULT expected_result,
    const char* error_message) {
#pragma warning(suppress: 6001)
    HRESULT hr = p_mr->Register(
        p_buf,
        buffer_length,
        type,
        p_ov
    );
    if (hr == ND_PENDING) {
        hr = p_mr->GetOverlappedResult(p_ov, TRUE);
    }
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_Accept(
    IND2Connector* p_connector,
    IND2QueuePair* p_qp,
    OVERLAPPED* p_ov,
    DWORD inbound_read_limit,
    DWORD outbound_read_limit,
    const VOID* p_private_data,
    DWORD cb_private_data,
    HRESULT expected_result,
    const char* error_message) {
    //
    // Accept the connection.
    //
    HRESULT hr = p_connector->Accept(
        p_qp,
        inbound_read_limit,
        outbound_read_limit,
        p_private_data,
        cb_private_data,
        p_ov
    );
    if (hr == ND_PENDING)
    {
        hr = p_connector->GetOverlappedResult(p_ov, TRUE);
    }
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_CreateMW(
    IND2Adapter* p_adapter,
    IND2MemoryWindow** pp_mw,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_adapter->CreateMemoryWindow(
        IID_IND2MemoryWindow,
        reinterpret_cast<VOID**>(pp_mw)
    );

    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_Bind(
    IND2QueuePair* p_qp,
    IND2MemoryRegion* p_mr,
    IND2MemoryWindow* p_mw,
    const void* p_buf,
    DWORD buffer_length,
    ULONG flags,
    IND2CompletionQueue* p_cq,
    OVERLAPPED* p_ov,
    void* context,
    HRESULT expected_result,
    const char* error_message) {
#pragma warning(suppress: 6001)
    HRESULT hr = p_qp->Bind(context, p_mr, p_mw, p_buf, buffer_length, flags);
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);

    ND2_RESULT nd_res;
    while (true) {
        RDMC_WaitForCompletion(p_cq, p_ov, &nd_res, true);
        if (nd_res.Status == expected_result)
            break;
    }
    LogIfErrorExit(nd_res.Status, expected_result, error_message, -1);
    if (nd_res.Status == ND_SUCCESS && nd_res.RequestContext != context)
    {
        LogErrorExit("Invalid context", __LINE__);
    }
}

void RDMC_Send(
    IND2QueuePair* p_qp,
    const ND2_SGE* sge,
    const ULONG n_sge,
    ULONG flags,
    void* request_context,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_qp->Send(request_context, sge, n_sge, flags);
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_PostReceive(
    IND2QueuePair* p_qp,
    const ND2_SGE* sge,
    const DWORD n_sge,
    void* request_context,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_qp->Receive(request_context, sge, n_sge);
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_Connect(
    IND2Connector* p_connector,
    IND2QueuePair* p_qp,
    OVERLAPPED* p_ov,
    _In_ const sockaddr_in& v4_src,
    _In_ const sockaddr_in& v4_dst,
    DWORD inbound_read_limit,
    DWORD outbound_read_limit,
    const VOID* p_private_data,
    DWORD cb_private_data,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_connector->Bind(
        reinterpret_cast<const sockaddr*>(&v4_src),
        sizeof(v4_src)
    );
    if (hr == ND_PENDING)
    {
        hr = p_connector->GetOverlappedResult(p_ov, TRUE);
    }

    hr = p_connector->Connect(
        p_qp,
        reinterpret_cast<const sockaddr*>(&v4_dst),
        sizeof(v4_dst),
        inbound_read_limit,
        outbound_read_limit,
        p_private_data,
        cb_private_data,
        p_ov
    );
    if (hr == ND_PENDING)
    {
        hr = p_connector->GetOverlappedResult(p_ov, TRUE);
    }
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_CompleteConnect(
    IND2Connector* p_connector,
    OVERLAPPED* p_ov,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr = p_connector->CompleteConnect(p_ov);
    if (hr == ND_PENDING)
    {
        hr = p_connector->GetOverlappedResult(p_ov, TRUE);
    }
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_Read(
    IND2QueuePair* p_qp,
    const ND2_SGE* sge,
    const ULONG n_sge,
    UINT64 remote_address,
    UINT32 remote_token,
    DWORD flag,
    void* request_context,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr;
    hr = p_qp->Read(request_context, sge, n_sge, remote_address, remote_token, flag);
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_Write(
    IND2QueuePair* p_qp,
    const ND2_SGE* sge,
    const ULONG n_sge,
    UINT64 remote_address,
    UINT32 remote_token,
    DWORD flag,
    void* request_context,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr;
    hr = p_qp->Write(request_context, sge, n_sge, remote_address, remote_token, flag);
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_Write_Debugging(
    IND2QueuePair* p_qp,
    const ND2_SGE* sge,
    const ULONG n_sge,
    UINT64 remote_address,
    UINT32 remote_token,
    DWORD flag,
    void* request_context,
    HRESULT expected_result,
    const char* error_message) {
    HRESULT hr(0);
    LogIfErrorExit(hr, expected_result, error_message, __LINE__);
}

void RDMC_DisconnectConnector(
    IND2Connector* p_connector,
    OVERLAPPED* p_ov) {
    if (p_connector != nullptr)
    {
        p_connector->Disconnect(p_ov);
    }
}

void RDMC_DeregisterMemory(
    IND2MemoryRegion* p_mr,
    OVERLAPPED* p_ov) {
    p_mr->Deregister(p_ov);
}