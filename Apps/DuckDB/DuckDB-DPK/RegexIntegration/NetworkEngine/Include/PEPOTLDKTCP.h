#ifndef PEPO_TLDK_TCP_H
#define PEPO_TLDK_TCP_H

#include <arpa/inet.h>
#include <assert.h>
#include <getopt.h>
#include <inttypes.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include <rte_common.h>
#include <rte_config.h>
#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_hash.h>
#include <rte_ip.h>
#include <rte_ip_frag.h>
#include <rte_kvargs.h>
#include <rte_lcore.h>
#include <rte_lpm.h>
#include <rte_lpm6.h>
#include <rte_malloc.h>
#include <rte_tcp.h>
#include <tle_event.h>
#include <tle_tcp.h>

// #include "FileService.h"
// #include "OffloadTypes.h"
// #include "Protocol.h"
// #include "UDF.h"

#define TLE_DEFAULT_MSS 536
#define MAX_PKT_BURST 0x20
#define RSS_HASH_KEY_LENGTH 64

#undef DDS_VERBOSE

#define NETFE_DEBUG
#define NETBE_DEBUG
#undef NETFE_DEBUG
#undef NETBE_DEBUG

#define MAX_REQS_PER_MESSAGE 64
#define MAX_READ_BUFFER_MBUF_SIZE 65535
#define MAX_READ_BUFFER_SIZE 65024
#define MAX_READ_BUFFER_NUM 0x1000

//
// TLDK back end
//
//
struct NetbePort
{
    uint32_t Id;
    uint32_t Mtu;
    uint64_t RxOffload;
    uint64_t TxOffload;
    uint32_t Ipv4;
    struct rte_ether_addr Mac;
};

struct NetbeDest
{
    uint32_t Port;
    uint32_t Prfx;
    struct in_addr Ipv4;
    struct rte_ether_addr Mac;
};

struct NetbeDestParam
{
    uint32_t NumDests;
    struct NetbeDest Dest;
};

struct PktBuf
{
    uint32_t Num;
    struct rte_mbuf *Pkt[MAX_PKT_BURST];
};

struct NetbeDev
{
    uint16_t RxQid;
    uint16_t TxQid;
    struct NetbePort Port;
    struct tle_dev *Dev;
    struct PktBuf TxBuf;
#ifdef DDS_VERBOSE
    struct
    {
        uint64_t In;
        uint64_t Up;
        uint64_t Drop;
    } RxStat;
    struct
    {
        uint64_t Down;
        uint64_t Out;
        uint64_t Drop;
    } TxStat;
#endif
};

struct NetbeLcore
{
    uint32_t Id;
    struct rte_lpm *Lpm4;
    struct rte_ip_frag_tbl *Ftbl;
    struct tle_ctx *Ctx;
    uint32_t NumPortQueues;
    struct NetbeDev *PortQueues;
    struct tle_dest Dst4;
    struct rte_ip_frag_death_row DeathRow;
#ifdef DDS_VERBOSE
    struct
    {
        uint64_t Flags[UINT8_MAX + 1];
    } TcpStat;
#endif
};

struct NetbeCfg
{
    uint32_t Promisc;
    uint32_t NumPorts;
    uint32_t NumCores;
    uint32_t NumMempoolBufs;
    struct NetbePort *Ports;
    struct NetbeLcore *Cores;
};

//
// TLDK front end
//
//
struct NetfeSParam
{
    uint32_t Bidx;                      /* BE index to use. */
    struct sockaddr_storage LocalAddr;  /**< stream local address. */
    struct sockaddr_storage RemoteAddr; /**< stream remote address. */
};

struct NetfeStreamParam
{
    uint32_t Lcore;
    struct NetfeSParam DPUStreamParam;
    struct NetfeSParam FwdStreamParam;
};

//
// In addition to TLDK streams, every front-end thread maintains
// the offload predicate and offload function libraries
//
//
struct NetfeLcoreParam
{
    uint32_t MaxStreams;
    uint32_t NumStreams;
    struct NetfeStreamParam *Streams;
    // void *OffloadPredLib;
    // void *OffloadFuncLib;
    // OffloadPred OffloadPred;
    // OffloadFunc OffloadFunc;
};

enum NetfeStreamType
{
    LISTENING_STREAM,
    DPU_STREAM,
    HOST_STREAM
};

struct NetfeFwdStream;

struct NetfeListenStream
{
    struct tle_stream *TleStream;
    struct tle_event *ErEv;
    struct tle_event *RxEv;
    struct tle_event *TxEv;
    uint16_t PostErr; /* # of time error event handling was postponed */
#ifdef DDS_VERBOSE
    struct
    {
        uint64_t RxPkts;
        uint64_t RxBytes;
        uint64_t TxPkts;
        uint64_t TxBytes;
        uint64_t Drops;
        uint64_t RxEv[TLE_SEV_NUM];
        uint64_t TxEv[TLE_SEV_NUM];
        uint64_t ErEv[TLE_SEV_NUM];
    } Stat;
#endif
    struct PktBuf Pbuf;
    struct sockaddr_storage Laddr;
    struct sockaddr_storage Raddr;
};

struct NetfeStream
{
    struct tle_stream *TleStream;
    struct tle_event *ErEv;
    struct tle_event *RxEv;
    struct tle_event *TxEv;
    uint16_t PostErr; /* # of time error event handling was postponed */
#ifdef DDS_VERBOSE
    struct
    {
        uint64_t RxPkts;
        uint64_t RxBytes;
        uint64_t TxPkts;
        uint64_t TxBytes;
        uint64_t FwdPkts;
        uint64_t Drops;
        uint64_t RxEv[TLE_SEV_NUM];
        uint64_t TxEv[TLE_SEV_NUM];
        uint64_t ErEv[TLE_SEV_NUM];
    } Stat;
#endif
    struct PktBuf Pbuf;
    struct sockaddr_storage Laddr;
    struct sockaddr_storage Raddr;
    struct NetfeFwdStream *HostStream;
    LIST_ENTRY(NetfeStream) Link;
};

struct NetfeFwdStream
{
    struct tle_stream *TleStream;
    struct tle_event *ErEv;
    struct tle_event *RxEv;
    struct tle_event *TxEv;
    uint16_t PostErr; /* # of time error event handling was postponed */
#ifdef DDS_VERBOSE
    struct
    {
        uint64_t RxPkts;
        uint64_t RxBytes;
        uint64_t TxPkts;
        uint64_t TxBytes;
        uint64_t Drops;
        uint64_t RxEv[TLE_SEV_NUM];
        uint64_t TxEv[TLE_SEV_NUM];
        uint64_t ErEv[TLE_SEV_NUM];
    } Stat;
#endif
    struct PktBuf Pbuf;
    struct sockaddr_storage Laddr;
    struct sockaddr_storage Raddr;
    struct NetfeListenStream *ListenStream;
    struct NetfeStream *DPUStream;
    uint8_t IsConnected;
    LIST_ENTRY(NetfeFwdStream) Link;
};

struct NetfeStreamList
{
    uint32_t Num;
    LIST_HEAD(, NetfeStream) Head;
};

struct NetfeFwdStreamList
{
    uint32_t Num;
    LIST_HEAD(, NetfeFwdStream) Head;
};

struct NetfeLcore
{
    uint32_t NumStreams; /* max number of streams */
    uint32_t NumListeners;
    struct NetfeListenStream *Listeners;
    struct tle_evq *ListenErEq;
    struct tle_evq *ListenRxEq;
    struct tle_evq *ListenTxEq;
    struct tle_evq *ErEq;
    struct tle_evq *RxEq;
    struct tle_evq *TxEq;
    struct tle_evq *HostErEq;
    struct tle_evq *HostRxEq;
    struct tle_evq *HostTxEq;
#ifdef DDS_VERBOSE
    struct
    {
        uint64_t Acc;
        uint64_t Rej;
        uint64_t Ter;
    } TcpStat;
#endif
    struct NetfeStreamList Free;
    struct NetfeStreamList Use;
    struct NetfeFwdStreamList HostFree;
    struct NetfeFwdStreamList HostUse;

    // OffloadPred OffloadPred;
    // OffloadFunc OffloadFunc;

    // RequestDescriptorT ReqsForHost[MAX_REQS_PER_MESSAGE];
    // RequestDescriptorT ReqsForDPU[MAX_REQS_PER_MESSAGE];
    // uint16_t NumReqsForHost;
    // uint16_t NumReqsForDPU;

    // ReadOpDescriptorT ReadOps[DDS_MAX_OUTSTANDING_IO];
    // DataPlaneRequestContext ReadCtxts[DDS_MAX_OUTSTANDING_IO];
    // uint16_t ReadOpsHeadPointer;
    // uint16_t ReadOpsTailPointer;
};

struct LcoreParam
{
    struct
    {
        struct NetbeLcore *Lc;
    } Be;
    struct NetfeLcoreParam Fe;
};

//
// Debug/trace macros
//
//

#define DUMMY_MACRO                                                                                                    \
    do                                                                                                                 \
    {                                                                                                                  \
    } while (0)

#ifdef NETFE_DEBUG
#define NETFE_TRACE(fmt, arg...) printf(fmt, ##arg)
#define NETFE_PKT_DUMP(p) rte_pktmbuf_dump(stdout, (p), 64)
#else
#define NETFE_TRACE(fmt, arg...) DUMMY_MACRO
#define NETFE_PKT_DUMP(p) DUMMY_MACRO
#endif

#ifdef NETBE_DEBUG
#define NETBE_TRACE(fmt, arg...) printf(fmt, ##arg)
#define NETBE_PKT_DUMP(p) rte_pktmbuf_dump(stdout, (p), 64)
#else
#define NETBE_TRACE(fmt, arg...) DUMMY_MACRO
#define NETBE_PKT_DUMP(p) DUMMY_MACRO
#endif

#define FUNC_STAT(v, c)                                                                                                \
    do                                                                                                                 \
    {                                                                                                                  \
        static uint64_t nb_call, nb_data;                                                                              \
        nb_call++;                                                                                                     \
        nb_data += (v);                                                                                                \
        if ((nb_call & ((c) - 1)) == 0)                                                                                \
        {                                                                                                              \
            printf("%s#%d@%u: nb_call=%lu, avg(" #v ")=%#Lf\n", __func__, __LINE__, rte_lcore_id(), nb_call,           \
                   (long double)nb_data / nb_call);                                                                    \
            nb_call = 0;                                                                                               \
            nb_data = 0;                                                                                               \
        }                                                                                                              \
    } while (0)

#define FUNC_TM_STAT(v, c)                                                                                             \
    do                                                                                                                 \
    {                                                                                                                  \
        static uint64_t nb_call, nb_data;                                                                              \
        static uint64_t cts, pts, sts;                                                                                 \
        cts = rte_rdtsc();                                                                                             \
        if (pts != 0)                                                                                                  \
            sts += cts - pts;                                                                                          \
        pts = cts;                                                                                                     \
        nb_call++;                                                                                                     \
        nb_data += (v);                                                                                                \
        if ((nb_call & ((c) - 1)) == 0)                                                                                \
        {                                                                                                              \
            printf("%s#%d@%u: nb_call=%lu, "                                                                           \
                   "avg(" #v ")=%#Lf, "                                                                                \
                   "avg(cycles)=%#Lf, "                                                                                \
                   "avg(cycles/" #v ")=%#Lf\n",                                                                        \
                   __func__, __LINE__, rte_lcore_id(), nb_call, (long double)nb_data / nb_call,                        \
                   (long double)sts / nb_call, (long double)sts / nb_data);                                            \
            nb_call = 0;                                                                                               \
            nb_data = 0;                                                                                               \
            sts = 0;                                                                                                   \
        }                                                                                                              \
    } while (0)

int SetupRxCb(const struct NetbePort *Port, struct NetbeLcore *Lc, uint16_t Qid);

//
// Application main function type
//
//
typedef int (*LCORE_MAIN_FUNCTYPE)(void *Arg);

//
// Initialize the PEPO based on TLDK TCP/IP
//
//
int PEPOTLDKTCPInit(uint32_t NumCores, const char *CoreList, const char *HostIPv4Addr, uint16_t HostTCPPort,
                    const char *DPUIPv4Addr, const char *UdfPath, uint32_t MaxStreams);

//
// Run
//
//
int PEPOTLDKTCPRun(
    // void (*StorageEngine)(void*),
    // void* StorageEngineArg
);

//
// Destroy the PEPO
//
//
int PEPOTLDKTCPDestroy(void);

#endif /* PEPO_LINUX_TCP_H */
