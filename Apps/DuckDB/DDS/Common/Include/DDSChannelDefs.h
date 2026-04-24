#pragma once
// ---------------------------------------------------------------------------
// DDSChannelDefs.h — Deterministic IO channel mapping for dual-channel DDS.
//
// Provides:
//   DDS_OpToChannel(opCode)  — returns channel index (0 or 1) for a given
//                              request opcode.
//   DDS_ChannelName(ch)      — returns a human-readable label for logging.
//
// The mapping is used by both frontend (C++) and backend (C) submission and
// completion paths.  pread2 (BUFF_MSG_F2B_REQ_OP_READ2) is routed exclusively
// to channel-1; all other operations use channel-0.
// ---------------------------------------------------------------------------

#include <stdint.h>

#include "MsgTypes.h"
#include "Protocol.h"

#ifdef __cplusplus
extern "C"
{
#endif

    // ---------------------------------------------------------------------------
    // DDS_OpToChannel — map a BuffMsgF2BReqHeader.OpCode to a channel index.
    //
    //   Returns DDS_IO_CHANNEL_PREAD2 (1) for BUFF_MSG_F2B_REQ_OP_READ2,
    //   Returns DDS_IO_CHANNEL_PRIMARY (0) for all other opcodes.
    //
    //   This is a hot-path helper; keep it branch-free when possible.
    // ---------------------------------------------------------------------------
    static inline int DDS_OpToChannel(uint16_t opCode)
    {
        // Single comparison — evaluates to 1 when opCode == READ2, 0 otherwise.
        return (opCode == BUFF_MSG_F2B_REQ_OP_READ2) ? DDS_IO_CHANNEL_PREAD2 : DDS_IO_CHANNEL_PRIMARY;
    }

    // ---------------------------------------------------------------------------
    // DDS_IsRead2Channel — convenience predicate: is this the pread2 channel?
    // ---------------------------------------------------------------------------
    static inline int DDS_IsRead2Channel(int channel) { return channel == DDS_IO_CHANNEL_PREAD2; }

    // ---------------------------------------------------------------------------
    // DDS_ChannelName — human-readable channel label for debug/log prints.
    // ---------------------------------------------------------------------------
    static inline const char *DDS_ChannelName(int channel)
    {
        switch (channel)
        {
        case DDS_IO_CHANNEL_PRIMARY:
            return "ch0-primary";
        case DDS_IO_CHANNEL_PREAD2:
            return "ch1-pread2";
        default:
            return "ch?-unknown";
        }
    }

    // ---------------------------------------------------------------------------
    // DDS_ValidateChannelIndex — defensive check for valid channel index.
    // ---------------------------------------------------------------------------
    static inline int DDS_ValidateChannelIndex(int channel) { return (channel >= 0 && channel < DDS_NUM_IO_CHANNELS); }

    // ---------------------------------------------------------------------------
    // DDS_ValidateRequestId — defensive check for request-id validity on one channel.
    // ---------------------------------------------------------------------------
    static inline int DDS_ValidateRequestId(RequestIdT reqId)
    {
        return (reqId != DDS_REQUEST_INVALID && reqId < DDS_MAX_OUTSTANDING_IO_PER_CHANNEL);
    }

    // ---------------------------------------------------------------------------
    // DDS_GlobalSlotFromChannelRequest — map (channel, requestId) to global slot.
    //
    // This is the canonical mapping used by backend SPDK slot ownership:
    //   globalSlot = channel * DDS_MAX_OUTSTANDING_IO_PER_CHANNEL + requestId
    // ---------------------------------------------------------------------------
    static inline uint32_t DDS_GlobalSlotFromChannelRequest(int channel, RequestIdT reqId)
    {
        if (!DDS_ValidateChannelIndex(channel) || !DDS_ValidateRequestId(reqId))
        {
            return UINT32_MAX;
        }
        return (uint32_t)channel * (uint32_t)DDS_MAX_OUTSTANDING_IO_PER_CHANNEL + (uint32_t)reqId;
    }

#ifdef __cplusplus
}
#endif
