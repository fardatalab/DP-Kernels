// #include "CacheTable.h"
#include "PEPOTLDKTCP.h"
#include "PacketProcessor.h"
// #include <stdlib.h>

#define MAX_RULES 0x100
#define MAX_TBL8 0x800
#define RX_RING_SIZE 0x400
#define TX_RING_SIZE 0x800
#define MPOOL_CACHE_SIZE 0x100
#define MPOOL_NB_BUF 0x10000
#define FRAG_MBUF_BUF_SIZE (RTE_PKTMBUF_HEADROOM + TLE_DST_MAX_HDR)
#define FRAG_TBL_BUCKET_ENTRIES 16
#define FRAG_TTL MS_PER_S
#define RSS_HASH_KEY_DEST_PORT_LOC_IPV4 15
#define RSS_RETA_CONF_ARRAY_SIZE (RTE_ETH_RSS_RETA_SIZE_512 / RTE_ETH_RETA_GROUP_SIZE)
#define TCP_SEQ_HASH_SEC_KEY "deadbeefbeefdead"
#define TCP_MAX_PROCESS 0x20

RTE_DEFINE_PER_LCORE(struct NetbeLcore *, _be);
RTE_DEFINE_PER_LCORE(struct NetfeLcore *, _fe);

typedef uint16_t dpdk_port_t;

static const uint64_t PEPORxOffload = RTE_ETH_RX_OFFLOAD_IPV4_CKSUM | RTE_ETH_RX_OFFLOAD_UDP_CKSUM | RTE_ETH_RX_OFFLOAD_TCP_CKSUM | RTE_ETH_RX_OFFLOAD_TCP_LRO;
static const uint64_t PEPOTxOffload = RTE_ETH_TX_OFFLOAD_IPV4_CKSUM | RTE_ETH_TX_OFFLOAD_UDP_CKSUM | RTE_ETH_TX_OFFLOAD_TCP_CKSUM | RTE_ETH_TX_OFFLOAD_TCP_TSO;
static const uint32_t PEPOMtuSize = RTE_ETHER_MAX_LEN - RTE_ETHER_CRC_LEN;
static const uint32_t PEPONumPorts = 1;
static const uint32_t PEPOPortId = 0;
static const char *ClientIPv4Addr = "0.0.0.0";
static const uint16_t ClientTCPPort = 0;
static const char *ClientEtherAddr = "ff:ff:ff:ff:ff:ff";

static char PEPOIPv4Addr[16];
static char GlobalHostIPv4Addr[16];
static uint16_t GlobalHostTCPPort;

static const struct rte_eth_conf PortConfDefault = {0};

static struct rte_mempool *Mpool[RTE_MAX_NUMA_NODES + 1];
static struct rte_mempool *FragMpool[RTE_MAX_NUMA_NODES + 1];
static struct rte_mempool *ReadBufMpool[RTE_MAX_NUMA_NODES + 1];

static struct tle_ctx_param TleCtxParam;
static struct NetbeCfg BeCfg;
static struct NetfeLcoreParam FeParam;
static struct LcoreParam CoreParam[RTE_MAX_LCORE];

static int PEPOVerbose = 0;
volatile int ForceQuitNetworkEngine;

char** contig_strings = NULL;
size_t n_bufs = 0;

//
// Check out the paper below for the mapping between the hash key and TCP header fields
// and how to make the hashing symmetric:
// https://www.ndsl.kaist.edu/~kyoungsoo/papers/TR-symRSS.pdf
//
//
uint8_t SymmetricRssKey[40] = {
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6D, 0x5A,
    0x6D, 0x5A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
};

//
// Handle signals
//
//
static void SigHandler(int SigNum)
{
    RTE_LOG(ERR, USER1, "%s(%d)\n", __func__, SigNum);
    ForceQuitNetworkEngine = 1;
}

//
// Set RSS configuration for the port
//
//
static int UpdateRSSConf(struct NetbePort *Port, const struct rte_eth_dev_info *DevInfo, struct rte_eth_conf *PortConf)
{
    PortConf->rxmode.mq_mode = RTE_ETH_MQ_RX_RSS;
    PortConf->rx_adv_conf.rss_conf.rss_hf = RTE_ETH_RSS_NONFRAG_IPV4_TCP | RTE_ETH_RSS_L4_DST_ONLY | RTE_ETH_RSS_L4_SRC_ONLY;
    PortConf->rx_adv_conf.rss_conf.rss_key = SymmetricRssKey;
    PortConf->rx_adv_conf.rss_conf.rss_key_len = sizeof(SymmetricRssKey);

    return 0;
}

//
// Initilise DPDK port
// In current version, multi-queue per port is used
//
static int PortInit(struct NetbePort *Port)
{
    int32_t result = 0;
    struct rte_eth_conf portConf = {0};
    struct rte_eth_dev_info devInfo = {0};

    rte_eth_dev_info_get(Port->Id, &devInfo);
    portConf = PortConfDefault;

    //
    // Handle hardware offloading
    //
    //
    if ((devInfo.rx_offload_capa & Port->RxOffload) != Port->RxOffload)
    {
        RTE_LOG(ERR, USER1,
                "Port#%u supported/requested RX offloads don't match, "
                "supported: %#" PRIx64 ", requested: %#" PRIx64 "\n",
                Port->Id, (uint64_t)devInfo.rx_offload_capa, (uint64_t)Port->RxOffload);
        return -EINVAL;
    }
    if ((devInfo.tx_offload_capa & Port->TxOffload) != Port->TxOffload)
    {
        RTE_LOG(ERR, USER1,
                "Port#%u supported/requested TX offloads don't match, "
                "supported: %#" PRIx64 ", requested: %#" PRIx64 "\n",
                Port->Id, (uint64_t)devInfo.tx_offload_capa, (uint64_t)Port->TxOffload);
        return -EINVAL;
    }

    RTE_LOG(ERR, USER1, "%s(%u): Enabling RX csum offload\n", __func__, Port->Id);
    portConf.rxmode.offloads |= Port->RxOffload;

    portConf.rxmode.max_lro_pkt_size = Port->Mtu + RTE_ETHER_CRC_LEN;
    if (portConf.rxmode.max_lro_pkt_size > RTE_ETHER_MAX_LEN)
    {
        // NOTE: quote from DPDK patch:
        // ethdev: remove jumbo offload flag
        // Removing 'DEV_RX_OFFLOAD_JUMBO_FRAME' offload flag.

        //     Instead of drivers announce this capability,
        //     application can deduct the
        //             capability by checking reported 'dev_info.max_mtu' or
        //         'dev_info.max_rx_pktlen'.

        //         And instead of application setting this flag explicitly to enable jumbo
        //             frames,
        //     this can be deduced by driver by comparing requested 'mtu' to 'RTE_ETHER_MTU'.

        // portConf.rxmode.offloads |= DEV_RX_OFFLOAD_JUMBO_FRAME;
        printf("Port#%u: max_lro_pkt_size=%u\n", Port->Id, portConf.rxmode.max_lro_pkt_size);
        portConf.rxmode.offloads |= RTE_ETH_RX_OFFLOAD_TCP_LRO;
    }

    RTE_LOG(ERR, USER1, "%s(%u): Enabling TX csum offload\n", __func__, Port->Id);
    portConf.txmode.offloads |= Port->TxOffload;

    //
    // Handle RSS
    //
    //
    result = UpdateRSSConf(Port, &devInfo, &portConf);
    if (result != 0)
    {
        return result;
    }

    //
    // Configure the port
    //
    //
    result = rte_eth_dev_configure(Port->Id, BeCfg.NumCores, BeCfg.NumCores, &portConf);
    if (result != 0)
    {
        return result;
    }

    RTE_LOG(NOTICE, USER1, "%s (%u): rte_eth_dev_configure (nb_rxq = %u, nb_txq = %u)\n", __func__, Port->Id,
            BeCfg.NumCores, BeCfg.NumCores);

    return 0;
}

//
// Check that lcore is enabled, not master, and not in use already.
//
//
static int CheckLcore(uint32_t LcId)
{
    if (rte_lcore_is_enabled(LcId) == 0)
    {
        RTE_LOG(ERR, USER1, "lcore %u is not enabled\n", LcId);
        return -EINVAL;
    }
    if (rte_eal_get_lcore_state(LcId) == RUNNING)
    {
        RTE_LOG(ERR, USER1, "lcore %u already in use\n", LcId);
        return -EINVAL;
    }
    return 0;
}

//
// Init mempools
//
//
static int PoolInit(uint32_t Sid, uint32_t NumMpoolBufs)
{
    int32_t result;
    struct rte_mempool *mp;
    char name[RTE_MEMPOOL_NAMESIZE];

    snprintf(name, sizeof(name), "MP%u", Sid);
    mp = rte_pktmbuf_pool_create(name, NumMpoolBufs, MPOOL_CACHE_SIZE, 0, RTE_MBUF_DEFAULT_BUF_SIZE, Sid - 1);
    if (mp == NULL)
    {
        result = -rte_errno;
        RTE_LOG(ERR, USER1, "%s(%d) failed with error code: %d\n", __func__, Sid - 1, result);
        return result;
    }

    Mpool[Sid] = mp;

    return 0;
}

static int FragPoolInit(uint32_t Sid, uint32_t NumMpoolBufs)
{
    int32_t result;
    struct rte_mempool *fragMp;
    char fragName[RTE_MEMPOOL_NAMESIZE];

    snprintf(fragName, sizeof(fragName), "FragMP%u", Sid);
    fragMp = rte_pktmbuf_pool_create(fragName, NumMpoolBufs, MPOOL_CACHE_SIZE, 0, FRAG_MBUF_BUF_SIZE, Sid - 1);
    if (fragMp == NULL)
    {
        result = -rte_errno;
        RTE_LOG(ERR, USER1, "%s(%d) failed with error code: %d\n", __func__, Sid - 1, result);
        return result;
    }

    FragMpool[Sid] = fragMp;
    return 0;
}

static int ReadBufPoolInit(uint32_t Sid, uint32_t NumMpoolBufs)
{
    int32_t result;
    struct rte_mempool *mp;
    char name[RTE_MEMPOOL_NAMESIZE];

    snprintf(name, sizeof(name), "ReadBufMP%u", Sid);
    mp = rte_pktmbuf_pool_create(name, NumMpoolBufs, MPOOL_CACHE_SIZE, 0, MAX_READ_BUFFER_MBUF_SIZE, Sid - 1);
    if (mp == NULL)
    {
        result = -rte_errno;
        RTE_LOG(ERR, USER1, "%s(%d) failed with error code: %d\n", __func__, Sid - 1, result);
        return result;
    }

    ReadBufMpool[Sid] = mp;

    return 0;
}

//
// Init queues
//
//
static int QueueInit(struct NetbePort *Port, struct rte_mempool *Mp)
{
    int32_t sid, result;
    uint16_t q;
    uint32_t numRxd, numTxd;
    struct rte_eth_dev_info devInfo;

    rte_eth_dev_info_get(Port->Id, &devInfo);

    sid = rte_eth_dev_socket_id(Port->Id);

    devInfo.default_rxconf.rx_drop_en = 1;

    numRxd = RTE_MIN(RX_RING_SIZE, devInfo.rx_desc_lim.nb_max);
    numTxd = RTE_MIN(TX_RING_SIZE, devInfo.tx_desc_lim.nb_max);

    devInfo.default_txconf.tx_free_thresh = numTxd / 2;

    for (q = 0; q < BeCfg.NumCores; q++)
    {
        result = rte_eth_rx_queue_setup(Port->Id, q, numRxd, sid, &devInfo.default_rxconf, Mp);
        if (result < 0)
        {
            RTE_LOG(ERR, USER1,
                    "%s: rx queue=%u setup failed with error "
                    "code: %d\n",
                    __func__, q, result);
            return result;
        }
    }

    for (q = 0; q < BeCfg.NumCores; q++)
    {
        result = rte_eth_tx_queue_setup(Port->Id, q, numTxd, sid, &devInfo.default_txconf);
        if (result < 0)
        {
            RTE_LOG(ERR, USER1,
                    "%s: tx queue=%u setup failed with error "
                    "code: %d\n",
                    __func__, q, result);
            return result;
        }
    }

    return 0;
}

//
// Find the core object for the given lcore id
//
//
static struct NetbeLcore *FindInitilizedLcore(uint32_t LcId)
{
    uint32_t i;

    for (i = 0; i < BeCfg.NumCores; i++)
    {
        if (BeCfg.Cores[i].Id == LcId)
        {
            return &BeCfg.Cores[i];
        }
    }
    return NULL;
}

//
// Log the port configuration
//
//
static void LogNetbePort(const struct NetbePort *Port)
{
    uint32_t i;
    char corelist[2 * RTE_MAX_LCORE + 1];

    memset(corelist, 0, sizeof(corelist));
    for (i = 0; i < BeCfg.NumCores; i++)
    {
        if (i < BeCfg.NumCores - 1)
        {
            sprintf(corelist + (2 * i), "%u,", BeCfg.Cores[i].Id);
        }
        else
        {
            sprintf(corelist + (2 * i), "%u", BeCfg.Cores[i].Id);
        }
    }

    RTE_LOG(NOTICE, USER1,
            "Port %p = <Id = %u, Lcore = <%s>, Mtu = %u, "
            "RxOffload = %#" PRIx64 ", TxOffload = %#" PRIx64 ","
            "Ipv4 = %u.%u.%u.%u, "
            "Mac = %02hhx:%02hhx:%02hhx:%02hhx:%02hhx:%02hhx>\n",
            Port, Port->Id, corelist, Port->Mtu, Port->RxOffload, Port->TxOffload, Port->Ipv4 & 0xff,
            Port->Ipv4 >> 8 & 0xff, Port->Ipv4 >> 16 & 0xff, Port->Ipv4 >> 24 & 0xff, Port->Mac.addr_bytes[0],
            Port->Mac.addr_bytes[1], Port->Mac.addr_bytes[2], Port->Mac.addr_bytes[3], Port->Mac.addr_bytes[4],
            Port->Mac.addr_bytes[5]);
}

//
// Log the TLDK back end configuration
//
//
static void LogNetbeCfg()
{
    uint32_t i;

    RTE_LOG(NOTICE, USER1, "TLDK back end config <#ports = %u>:\n", BeCfg.NumPorts);

    for (i = 0; i != BeCfg.NumPorts; i++)
    {
        LogNetbePort(&BeCfg.Ports[i]);
    }
}

//
// Set up PEPO port
//
//
static int PEPOPortInit()
{
    int32_t result;
    uint32_t sid, j;
    struct NetbePort *port;
    struct NetbeLcore *lcore;

    port = &BeCfg.Ports[PEPOPortId];
    result = PortInit(port);
    if (result != 0)
    {
        RTE_LOG(ERR, USER1, "%s: port=%u init failed with error code: %d\n", __func__, port->Id, result);
        return result;
    }

    rte_eth_macaddr_get(port->Id, &port->Mac);
    if (BeCfg.Promisc)
    {
        result = rte_eth_promiscuous_enable(port->Id);
        if (result != 0)
        {
            RTE_LOG(ERR, USER1, "%s: port=%u enable promiscuous failed with error code: %d\n", __func__, port->Id,
                    result);
            return result;
        }
    }

    for (j = 0; j < BeCfg.NumCores; j++)
    {
        result = CheckLcore(BeCfg.Cores[j].Id);
        if (result != 0)
        {
            RTE_LOG(ERR, USER1, "%s: lcore %d is unavailable\n", __func__, BeCfg.Cores[j].Id);
            return result;
        }

        sid = rte_lcore_to_socket_id(BeCfg.Cores[j].Id) + 1;
        assert(sid < RTE_DIM(Mpool));

        if (Mpool[sid] == NULL)
        {
            result = PoolInit(sid, BeCfg.NumMempoolBufs);
            if (result != 0)
                return result;
        }

        if (FragMpool[sid] == NULL)
        {
            result = FragPoolInit(sid, BeCfg.NumMempoolBufs);
            if (result != 0)
                return result;
        }

        if (ReadBufMpool[sid] == NULL)
        {
            result = ReadBufPoolInit(sid, MAX_READ_BUFFER_NUM);
            if (result != 0)
                return result;
        }

        result = QueueInit(port, Mpool[sid]);

        if (result != 0)
        {
            RTE_LOG(ERR, USER1,
                    "%s: lcore=%u queue init failed with "
                    "err: %d\n",
                    __func__, BeCfg.Cores[j].Id, result);
            return result;
        }

        lcore = FindInitilizedLcore(BeCfg.Cores[j].Id);

        if (lcore == NULL)
        {
            RTE_LOG(ERR, USER1,
                    "%s: lcore=%u find initialized lcore failed with "
                    "err: %d\n",
                    __func__, BeCfg.Cores[j].Id, result);
            return result;
        }

        lcore->PortQueues = (struct NetbeDev *)rte_realloc(
            lcore->PortQueues, sizeof(struct NetbeDev) * (lcore->NumPortQueues + 1), RTE_CACHE_LINE_SIZE);

        if (lcore->PortQueues == NULL)
        {
            RTE_LOG(ERR, USER1, "%s: failed to reallocate memory\n", __func__);
            return -ENOMEM;
        }

        lcore->PortQueues[lcore->NumPortQueues].RxQid = j;
        lcore->PortQueues[lcore->NumPortQueues].TxQid = j;
        lcore->PortQueues[lcore->NumPortQueues].Port = *port;
        lcore->NumPortQueues++;
    }

    LogNetbeCfg();

    return 0;
}

//
// Split a string into an array of strings
//
//
static char **StrSplit(char *Str, const char Delim)
{
    char **result = 0;
    size_t count = 0;
    char *tmp = Str;
    char *lastIndex = 0;
    char delim[2];
    delim[0] = Delim;
    delim[1] = 0;

    while (*tmp)
    {
        if (Delim == *tmp)
        {
            count++;
            lastIndex = tmp;
        }
        tmp++;
    }

    count += lastIndex < (Str + strlen(Str) - 1);
    count++;
    result = (char **)malloc(sizeof(char *) * count);

    if (result)
    {
        size_t idx = 0;
        char *token = strtok(Str, delim);

        while (token)
        {
            *(result + idx++) = strdup(token);
            token = strtok(0, delim);
        }
        *(result + idx) = 0;
    }

    return result;
}

//
// Create blocked ports
//
//
static uint16_t CreateBlockList(uint16_t *BlockedPorts, uint32_t Q, bool BlockByQueue)
{
    uint32_t i, j, Qid, alignNumQ;

    if (BlockByQueue)
    {
        alignNumQ = rte_align32pow2(BeCfg.NumCores);
        for (i = 0, j = 0; i < (UINT16_MAX + 1); i++)
        {
            Qid = (i % alignNumQ) % BeCfg.NumCores;
            if (Qid != Q)
            {
                BlockedPorts[j++] = i;
            }
        }

        return j;
    }
    else
    {
        return 0;
    }
}

//
// Init longest prefix match for this lcore
//
//
static int LcoreLpmInit(struct NetbeLcore *Lc)
{
    int32_t sid;
    char str[RTE_LPM_NAMESIZE];
    const struct rte_lpm_config lpm4Cfg = {
        .max_rules = MAX_RULES,
        .number_tbl8s = MAX_TBL8,
    };

    sid = rte_lcore_to_socket_id(Lc->Id);

    snprintf(str, sizeof(str), "LPM4%u\n", Lc->Id);
    Lc->Lpm4 = rte_lpm_create(str, sid, &lpm4Cfg);
    RTE_LOG(NOTICE, USER1, "%s(lcore = %u): lpm4 = %p\n", __func__, Lc->Id, Lc->Lpm4);
    if (Lc->Lpm4 == NULL)
    {
        return -ENOMEM;
    }

    return 0;
}

//
// IPv4 destination lookup callback
//
//
static int Lpm4DstLookup(void *Data, const struct in_addr *Addr, struct tle_dest *Des)
{
    struct NetbeLcore *Lc;
    struct tle_dest *Dst;

    Lc = (struct NetbeLcore *)Data;

    Dst = &Lc->Dst4;
    rte_memcpy(Des, Dst, Dst->l2_len + Dst->l3_len + offsetof(struct tle_dest, hdr));

    return 0;
}

//
// Create context
//
//
static int CreateContext(struct NetbeLcore *Lc)
{
    uint32_t result = 0, sid;
    uint64_t fragCycles;
    struct tle_ctx_param ctxParam;

    if (Lc->Ctx == NULL)
    {
        sid = rte_lcore_to_socket_id(Lc->Id);

        result = LcoreLpmInit(Lc);
        if (result != 0)
        {
            RTE_LOG(ERR, USER1, "%s (lcore = %u): failed to init lpm4 with error code: %u\n", __func__, Lc->Id, result);
            return result;
        }

        ctxParam = TleCtxParam;
        ctxParam.socket_id = sid;
        ctxParam.lookup4 = Lpm4DstLookup;
        ctxParam.lookup4_data = Lc;
        if (ctxParam.secret_key.u64[0] == 0 && ctxParam.secret_key.u64[1] == 0)
        {
            ctxParam.secret_key.u64[0] = rte_rand();
            ctxParam.secret_key.u64[1] = rte_rand();
        }

        fragCycles = (rte_get_tsc_hz() + MS_PER_S - 1) / MS_PER_S * FRAG_TTL;

        Lc->Ftbl = rte_ip_frag_table_create(ctxParam.max_streams, FRAG_TBL_BUCKET_ENTRIES,
                                            ctxParam.max_streams * FRAG_TBL_BUCKET_ENTRIES * 2, fragCycles, sid);

        if (Lc->Ftbl == NULL)
        {
            RTE_LOG(ERR, USER1, "%s (lcore = %u): failed to create frag_tbl\n", __func__, Lc->Id);
            return -1;
        }

        RTE_LOG(NOTICE, USER1, "%s (lcore = %u): frag_tbl has been created\n", __func__, Lc->Id);

        Lc->Ctx = tle_ctx_create(&ctxParam);

        if (Lc->Ctx == NULL)
        {
            RTE_LOG(ERR, USER1, "%s (lcore = %u): failed to create tle_ctx\n", __func__, Lc->Id);
            return -1;
        }

        RTE_LOG(NOTICE, USER1, "%s (lcore = %u): tle_ctx has been created\n", __func__, Lc->Id);
    }

    return result;
}

//
// Back end lcore setup routine
//
//
static int LcoreInit(struct NetbeLcore *Lc, const uint32_t PortQid, const uint16_t *BlockedPorts,
                     uint32_t NumBlockedPorts)
{
    int32_t result = 0;
    struct tle_dev_param devParam;

    result = CreateContext(Lc);
    if (result)
    {
        RTE_LOG(ERR, USER1, "%s (lcore = %u): failed to create context with error code: %d\n", __func__, Lc->Id,
                result);
        return result;
    }

    if (Lc->Ctx != NULL)
    {
        memset(&devParam, 0, sizeof(devParam));
        devParam.rx_offload = Lc->PortQueues[PortQid].Port.RxOffload;
        devParam.tx_offload = Lc->PortQueues[PortQid].Port.TxOffload;
        devParam.local_addr4.s_addr = Lc->PortQueues[PortQid].Port.Ipv4;
        devParam.bl4.nb_port = NumBlockedPorts;
        devParam.bl4.port = BlockedPorts;

        Lc->PortQueues[PortQid].Dev = tle_add_dev(Lc->Ctx, &devParam);

        if (Lc->PortQueues[PortQid].Dev == NULL)
        {
            RTE_LOG(ERR, USER1, "%s (lcore = %u) failed to add dev with error code: %d\n", __func__, Lc->Id, result);
            tle_ctx_destroy(Lc->Ctx);
            rte_ip_frag_table_destroy(Lc->Ftbl);
            rte_lpm_free(Lc->Lpm4);
            rte_free(Lc->PortQueues);
            Lc->NumPortQueues = 0;
            return -rte_errno;
        }

        RTE_LOG(NOTICE, USER1, "%s (lcore = %u, port = %u, Qid = %u) has added dev\n", __func__, Lc->Id,
                Lc->PortQueues[PortQid].Port.Id, Lc->PortQueues[PortQid].RxQid);
    }

    return result;
}

//
// Init lcores for the PEPO
//
//
static int PEPOLcoreInit()
{
    int32_t result;
    uint32_t i, j, numBlockedPorts = 0, sz;
    struct NetbeLcore *Lc;
    static uint16_t *blockedPorts;

    result = 0;
    sz = sizeof(uint16_t) * UINT16_MAX;
    blockedPorts = (uint16_t *)rte_zmalloc(NULL, sz, RTE_CACHE_LINE_SIZE);
    for (i = 0; i < BeCfg.NumCores; i++)
    {
        Lc = &BeCfg.Cores[i];
        for (j = 0; j < Lc->NumPortQueues; j++)
        {
            memset((uint8_t *)blockedPorts, 0, sz);

            numBlockedPorts = CreateBlockList(blockedPorts, Lc->PortQueues[j].RxQid, false);
            RTE_LOG(NOTICE, USER1, "Lc = %u, q = %u, numBlockedPorts = %u\n", Lc->Id, Lc->PortQueues[j].RxQid,
                    numBlockedPorts);

            result = LcoreInit(Lc, j, blockedPorts, numBlockedPorts);
            if (result != 0)
            {
                RTE_LOG(ERR, USER1, "%s: failed to init lcores with error code: %d\n", __func__, result);
                rte_free(blockedPorts);
                return result;
            }
        }
    }
    rte_free(blockedPorts);

    return 0;
}

//
// Fill the L4 destination
//
//
static void FillDst(struct tle_dest *Dst, struct NetbeDev *BeDev, const struct NetbeDest *BeDest, int32_t Sid)
{
    struct rte_ether_hdr *eth;
    struct rte_ipv4_hdr *ip4h;

    Dst->dev = BeDev->Dev;
    Dst->head_mp = FragMpool[Sid + 1];
    Dst->mtu = BeDev->Port.Mtu;
    Dst->l2_len = sizeof(*eth);

    eth = (struct rte_ether_hdr *)Dst->hdr;

    rte_ether_addr_copy(&BeDev->Port.Mac, &eth->src_addr); //->s_addr
    rte_ether_addr_copy(&BeDest->Mac, &eth->dst_addr);     //->d_addr
    eth->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

    Dst->l3_len = sizeof(*ip4h);
    ip4h = (struct rte_ipv4_hdr *)(eth + 1);
    ip4h->version_ihl = 4 << 4 | sizeof(*ip4h) / RTE_IPV4_IHL_MULTIPLIER;
    ip4h->time_to_live = 64;
    ip4h->next_proto_id = IPPROTO_TCP;
}

//
// Add IPv4 routing entry
//
//
static int NetbeAddIpv4Route(struct NetbeLcore *Lc, const struct NetbeDest *Dst)
{
    int32_t result;
    uint32_t addr, depth;
    char str[INET_ADDRSTRLEN];

    depth = Dst->Prfx;
    addr = rte_be_to_cpu_32(Dst->Ipv4.s_addr);

    inet_ntop(AF_INET, &Dst->Ipv4, str, sizeof(str));
    result = rte_lpm_add(Lc->Lpm4, addr, depth, 0);
    RTE_LOG(NOTICE, USER1,
            "%s (lcore = %u, port = %u, dev = %p, "
            "ipv4 = %s/%u, mtu = %u, "
            "mac = %02hhx:%02hhx:%02hhx:%02hhx:%02hhx:%02hhx) "
            "returns %d\n",
            __func__, Lc->Id, Dst->Port, Lc->Dst4.dev, str, depth, Lc->Dst4.mtu, Dst->Mac.addr_bytes[0],
            Dst->Mac.addr_bytes[1], Dst->Mac.addr_bytes[2], Dst->Mac.addr_bytes[3], Dst->Mac.addr_bytes[4],
            Dst->Mac.addr_bytes[5], result);
    return result;
}

//
// Add destination for the back end
//
//
static int NetbeAddDest(struct NetbeLcore *Lc, uint32_t DevIdx, const struct NetbeDest *Dst)
{
    int32_t result, sid;
    struct tle_dest *dp;

    dp = &Lc->Dst4;
    sid = rte_lcore_to_socket_id(Lc->Id);
    FillDst(dp, Lc->PortQueues + DevIdx, Dst, sid);
    result = NetbeAddIpv4Route(Lc, Dst);

    return result;
}

//
// Init back end destination
//
//
static int NetbeDestInit()
{
    int32_t result;
    uint32_t k, l, cnt;
    struct NetbeLcore *Lc;
    struct NetbeDestParam param;

    //
    // Configure the only destination in PEPO
    //
    //
    param.NumDests = 1;
    param.Dest.Port = PEPOPortId;
    inet_pton(AF_INET, PEPOIPv4Addr, &param.Dest.Ipv4);
    param.Dest.Prfx = 32;
    rte_ether_unformat_addr(ClientEtherAddr, &param.Dest.Mac);

    cnt = 0;
    for (k = 0; k != BeCfg.NumCores; k++)
    {
        Lc = &BeCfg.Cores[k];
        for (l = 0; l != Lc->NumPortQueues; l++)
        {
            if (Lc->PortQueues[l].Port.Id == PEPOPortId)
            {
                result = NetbeAddDest(Lc, l, &param.Dest);
                if (result != 0)
                {
                    RTE_LOG(ERR, USER1, "%s (Lc = %u) could not add the PEPO destination\n", __func__, Lc->Id);
                    return -ENOSPC;
                }
                cnt++;
            }
        }
    }

    if (cnt == 0)
    {
        RTE_LOG(ERR, USER1, "%s: Port %u not managed by any lcore\n", __func__, param.Dest.Port);
    }

    return 0;
}

//
// Update RSS redirection table
//
//
static int UpdateRssReta(struct NetbePort *Port, const struct rte_eth_dev_info *DevInfo)
{
    struct rte_eth_rss_reta_entry64 retaConf[RSS_RETA_CONF_ARRAY_SIZE];
    int32_t i, result;
    int32_t qIdx, idx, shift;

    if (BeCfg.NumCores > 1)
    {
        if (DevInfo->reta_size == 0)
        {
            RTE_LOG(ERR, USER1,
                    "%s: Redirection table size 0 is invalid for "
                    "RSS\n",
                    __func__);
            return -EINVAL;
        }
        RTE_LOG(NOTICE, USER1, "%s: The reta size of port %d is %u\n", __func__, Port->Id, DevInfo->reta_size);

        if (DevInfo->reta_size > RTE_ETH_RSS_RETA_SIZE_512)
        {
            RTE_LOG(ERR, USER1, "%s: More than %u entries of Reta not supported\n", __func__, RTE_ETH_RSS_RETA_SIZE_512);
            return -EINVAL;
        }

        memset(retaConf, 0, sizeof(retaConf));
        for (i = 0; i < DevInfo->reta_size; i++)
        {
            qIdx = i % BeCfg.NumCores;

            idx = i / RTE_ETH_RETA_GROUP_SIZE;
            shift = i % RTE_ETH_RETA_GROUP_SIZE;
            retaConf[idx].mask |= (1ULL << shift);
            retaConf[idx].reta[shift] = qIdx;
            RTE_LOG(NOTICE, USER1, "%s: port = %u RSS reta conf: hash = %u, q = %u\n", __func__, Port->Id, i, qIdx);
        }

        result = rte_eth_dev_rss_reta_update(Port->Id, retaConf, DevInfo->reta_size);
        if (result != 0)
        {
            RTE_LOG(ERR, USER1,
                    "%s: Bad redirection table parameter, "
                    "result = %d\n",
                    __func__, result);
            return result;
        }
    }

    return 0;
}

//
// Fill IPv4 address
//
//
static void FillAddr(struct sockaddr_storage *SS, const char *Addr4, uint16_t Port)
{
    struct in_addr Ip;
    struct sockaddr_in *si = (struct sockaddr_in *)SS;
    inet_pton(AF_INET, Addr4, &Ip);
    si->sin_addr = Ip;
    si->sin_port = rte_cpu_to_be_16(Port);
    SS->ss_family = AF_INET;
}

//
// Compare two NetfeStreamParam objects by lcore
//
//
static int NetfeLcoreCmp(const void *S1, const void *S2)
{
    const struct NetfeStreamParam *p1, *p2;

    p1 = (const struct NetfeStreamParam *)S1;
    p2 = (const struct NetfeStreamParam *)S2;
    return p1->Lcore - p2->Lcore;
}

//
// Fill lcore parameters for the front end
//
//
static int NetfeLcoreFill()
/* static int NetfeLcoreFill(void *OffloadPredLib, void *OffloadFuncLib, OffloadPred OffloadPredFunc,
                          OffloadFunc OffloadFuncFunc) */
{
    uint32_t lc;
    uint32_t i, j;

    //
    // Map each stream to it's backend
    // Support multiple streams and multiple backends
    //
    //
    for (i = 0; i != FeParam.NumStreams; i++)
    {
        FeParam.Streams[i].DPUStreamParam.Bidx = i % BeCfg.NumCores;
        FeParam.Streams[i].FwdStreamParam.Bidx = i % BeCfg.NumCores;
    }

    qsort(FeParam.Streams, FeParam.NumStreams, sizeof(struct NetfeStreamParam), NetfeLcoreCmp);

    for (i = 0; i != FeParam.NumStreams; i = j)
    {

        lc = FeParam.Streams[i].Lcore;

        if (rte_lcore_is_enabled(lc) == 0)
        {
            RTE_LOG(ERR, USER1, "%s: lcore %u is not enabled\n", __func__, lc);
            return -EINVAL;
        }

        if (rte_get_main_lcore() != lc && rte_eal_get_lcore_state(lc) == RUNNING)
        {
            RTE_LOG(ERR, USER1, "%s: lcore %u already in use\n", __func__, lc);
            return -EINVAL;
        }

        for (j = i + 1; j != FeParam.NumStreams && lc == FeParam.Streams[j].Lcore; j++)
        {
        }

        CoreParam[lc].Fe.MaxStreams = FeParam.MaxStreams;
        CoreParam[lc].Fe.NumStreams = j - i;
        CoreParam[lc].Fe.Streams = FeParam.Streams + i;

        //
        // Offload predicate and function libraries and functions
        // get rid of offloading in network engine for now
        //
        //
        /* CoreParam[lc].Fe.OffloadPredLib = OffloadPredLib;
        CoreParam[lc].Fe.OffloadFuncLib = OffloadFuncLib;
        CoreParam[lc].Fe.OffloadPred = OffloadPredFunc;
        CoreParam[lc].Fe.OffloadFunc = OffloadFuncFunc; */
    }

    return 0;
}

//
// Initialize the PEPO based on TLDK TCP/IP
//
//
int PEPOTLDKTCPInit(uint32_t NumCores, const char *CoreList, const char *HostIPv4Addr, uint16_t HostTCPPort,
                    const char *DPUIPv4Addr, const char *UdfPath, uint32_t MaxStreams)
{
    //
    // DPDK and TLDK initialization
    //
    //
    int32_t result;
    uint32_t i;
    struct rte_eth_dev_info devInfo;

    strcpy(PEPOIPv4Addr, DPUIPv4Addr);

    memset(CoreParam, 0, sizeof(CoreParam));
    memset(&TleCtxParam, 0, sizeof(TleCtxParam));
    memset(&BeCfg, 0, sizeof(BeCfg));

    //
    // Set fixed parameters
    //
    //
    TleCtxParam.send_bulk_size = 0;
    TleCtxParam.flags = TLE_CTX_FLAG_ST;
    TleCtxParam.max_stream_rbufs = MaxStreams;
    TleCtxParam.max_stream_sbufs = MaxStreams;
    TleCtxParam.max_streams = MaxStreams;
    TleCtxParam.timewait = TLE_TCP_TIMEWAIT_DEFAULT;
    TleCtxParam.hash_alg = TLE_SIPHASH;
    TleCtxParam.proto = TLE_PROTO_TCP;
    memcpy(&TleCtxParam.secret_key, TCP_SEQ_HASH_SEC_KEY, sizeof(TleCtxParam.secret_key));
    TleCtxParam.icw = 0;

    BeCfg.Promisc = 1;
    BeCfg.NumMempoolBufs = MPOOL_NB_BUF;

    //
    // Set configurable parameters
    //
    //
    BeCfg.NumPorts = PEPONumPorts;
    BeCfg.Ports = (struct NetbePort *)rte_zmalloc(NULL, sizeof(struct NetbePort) * PEPONumPorts, RTE_CACHE_LINE_SIZE);

    BeCfg.NumCores = NumCores;
    BeCfg.Cores = (struct NetbeLcore *)rte_zmalloc(NULL, sizeof(struct NetbeLcore) * NumCores, RTE_CACHE_LINE_SIZE);

    char coreList[32];
    strcpy(coreList, CoreList);
    char **coreListArray = StrSplit(coreList, ',');
    for (i = 0; i < NumCores; i++)
    {
        BeCfg.Cores[i].Id = atoi(coreListArray[i]);
        BeCfg.Cores[i].NumPortQueues = 0;
    }

    PEPOVerbose = 9;

    //
    // Configure the PEPO interface
    //
    //
    BeCfg.Ports[PEPOPortId].Id = PEPOPortId;
    BeCfg.Ports[PEPOPortId].Mtu = PEPOMtuSize;
    BeCfg.Ports[PEPOPortId].RxOffload = PEPORxOffload;
    BeCfg.Ports[PEPOPortId].TxOffload = PEPOTxOffload;

    struct in_addr addr4;
    if (inet_pton(AF_INET, PEPOIPv4Addr, &addr4) != 1)
    {
        RTE_LOG(ERR, USER1, "%s: failed to retrieve IPv4\n", __func__);
        SigHandler(SIGQUIT);
        return -EINVAL;
    }

    BeCfg.Ports[PEPOPortId].Ipv4 = addr4.s_addr;

    result = PEPOPortInit();
    if (result != 0)
    {
        RTE_LOG(ERR, USER1, "%s: PEPO port init failed with error code: %d\n", __func__, result);
        SigHandler(SIGQUIT);
        return result;
    }

    //
    // Configure lcores
    //
    //
    result = PEPOLcoreInit();
    if (result != 0)
    {
        RTE_LOG(ERR, USER1, "%s: PEPO port init failed with error code: %d\n", __func__, result);
        SigHandler(SIGQUIT);
        return result;
    }

    //
    // Init back end destination, i.e., the front end
    //
    //
    result = NetbeDestInit();
    if (result != 0)
    {
        SigHandler(SIGQUIT);
        return result;
    }

    RTE_LOG(NOTICE, USER1, "%s: DPDK (back end) configured\n", __func__);

    //
    // Start back end ports
    //
    //
    for (i = 0; i != BeCfg.NumPorts && result == 0; i++)
    {
        RTE_LOG(NOTICE, USER1, "%s: Starting port %u\n", __func__, BeCfg.Ports[i].Id);
        result = rte_eth_dev_start(BeCfg.Ports[i].Id);
        if (result != 0)
        {
            RTE_LOG(ERR, USER1,
                    "%s: rte_eth_dev_start (%u) returned "
                    "error code: %d\n",
                    __func__, BeCfg.Ports[i].Id, result);
            SigHandler(SIGQUIT);
            return result;
        }
        rte_eth_dev_info_get(BeCfg.Ports[i].Id, &devInfo);
        UpdateRssReta(&BeCfg.Ports[i], &devInfo);
        if (result != 0)
        {
            RTE_LOG(ERR, USER1,
                    "%s: UpdateRssReta returned "
                    "error code: %d\n",
                    __func__, result);
            SigHandler(SIGQUIT);
            return result;
        }
    }

    RTE_LOG(NOTICE, USER1, "%s: DPDK (back end) interfaces started\n", __func__);

    //
    // Set front end parameters
    //
    //
    FeParam.MaxStreams = TleCtxParam.max_streams * BeCfg.NumCores;
    FeParam.NumStreams = 1 * BeCfg.NumCores;

    FeParam.Streams = (struct NetfeStreamParam *)rte_zmalloc(NULL, sizeof(struct NetfeStreamParam) * FeParam.NumStreams,
                                                             RTE_CACHE_LINE_SIZE);
    if (FeParam.Streams == NULL)
    {
        RTE_LOG(ERR, USER1, "%s: Failed to allocate memory for streams\n", __func__);
        SigHandler(SIGQUIT);
        return -ENOMEM;
    }

    GlobalHostTCPPort = HostTCPPort;
    strcpy(GlobalHostIPv4Addr, HostIPv4Addr);

    //
    // Set DPU stream address
    // Local address: local IP + host TCP port (reuse host port for symmetric RSS)
    // Remote address: any IP + any port
    //
    //
    for (i = 0; i != FeParam.NumStreams; i++)
    {
        FeParam.Streams[i].Lcore = BeCfg.Cores[i % BeCfg.NumCores].Id;
        FillAddr(&FeParam.Streams[i].DPUStreamParam.LocalAddr, PEPOIPv4Addr, GlobalHostTCPPort);
        FillAddr(&FeParam.Streams[i].DPUStreamParam.RemoteAddr, ClientIPv4Addr, ClientTCPPort);
    }

    RTE_LOG(NOTICE, USER1, "%s: TLDK (front end) configured\n", __func__);

    //
    // Set back end lcore parameters
    //
    //
    for (i = 0; result == 0 && i != BeCfg.NumCores; i++)
    {
        CoreParam[BeCfg.Cores[i].Id].Be.Lc = &BeCfg.Cores[i];
    }

    RTE_LOG(NOTICE, USER1, "%s: Lcores for back end configured\n", __func__);

    //
    // Load offload predicate and function
    // get rid of it for now
    //
    //
    /* bool predRet = CompileUDF(UdfPath, OFFLOAD_PRED);
    if (!predRet)
    {
        RTE_LOG(ERR, USER1, "%s: Failed to compile offload predicate\n", __func__);
        SigHandler(SIGQUIT);
        return -EINVAL;
    }

    void *predLib = LoadLibrary(UdfPath, OFFLOAD_PRED);
    if (predLib == NULL)
    {
        RTE_LOG(ERR, USER1, "%s: Failed to load offload predicate library\n", __func__);
        SigHandler(SIGQUIT);
        return -EINVAL;
    }

    OffloadPred offloadPred = GetOffloadPred(predLib);
    if (offloadPred == NULL)
    {
        RTE_LOG(ERR, USER1, "%s: Failed to get offload predicate\n", __func__);
        SigHandler(SIGQUIT);
        return -EINVAL;
    }

    bool funcRet = CompileUDF(UdfPath, OFFLOAD_FUNC);
    if (!funcRet)
    {
        RTE_LOG(ERR, USER1, "%s: Failed to compile offload function\n", __func__);
        SigHandler(SIGQUIT);
        return -EINVAL;
    }

    void *funcLib = LoadLibrary(UdfPath, OFFLOAD_FUNC);
    if (funcLib == NULL)
    {
        RTE_LOG(ERR, USER1, "%s: Failed to load offload function library\n", __func__);
        SigHandler(SIGQUIT);
        return -EINVAL;
    }

    OffloadFunc offloadFunc = GetOffloadFunc(funcLib);
    if (offloadFunc == NULL)
    {
        RTE_LOG(ERR, USER1, "%s: Failed to get offload function\n", __func__);
        SigHandler(SIGQUIT);
        return -EINVAL;
    } */

    //
    // Set front end lcore parameters
    //
    //
    // result = NetfeLcoreFill(predLib, funcLib, offloadPred, offloadFunc);
    result = NetfeLcoreFill();
    if (result != 0)
    {
        RTE_LOG(ERR, USER1,
                "%s: NetfeLcoreFill returned "
                "error code: %d\n",
                __func__, result);
        SigHandler(SIGQUIT);
        return result;
    }

    RTE_LOG(NOTICE, USER1, "%s: Lcores for front end configured\n", __func__);

    return result;
}

//
// Put n streams into the stream list
//
//
static inline void NetfePutStreams(struct NetfeLcore *FeLcore, struct NetfeStreamList *StreamList,
                                   struct NetfeStream *FeStream[], uint32_t Num)
{
    uint32_t i, n;

    n = RTE_MIN(FeLcore->NumStreams - StreamList->Num, Num);
    if (n != Num)
    {
        RTE_LOG(ERR, USER1, "%s: StreamList overflow by %u\n", __func__, Num - n);
    }

    for (i = 0; i != n; i++)
    {
        LIST_INSERT_HEAD(&StreamList->Head, FeStream[i], Link);
    }
    StreamList->Num += n;
}

//
// Put one stream into the stream list
//
//
static inline void NetfePutStream(struct NetfeLcore *FeLcore, struct NetfeStreamList *StreamList,
                                  struct NetfeStream *Stream)
{
    if (StreamList->Num == FeLcore->NumStreams)
    {
        RTE_LOG(ERR, USER1, "%s: StreamList is full\n", __func__);
        return;
    }

    NetfePutStreams(FeLcore, StreamList, &Stream, 1);
}

//
// Get n streams from the stream list
//
//
static inline uint32_t NetfeGetStreams(struct NetfeStreamList *List, struct NetfeStream *Result[], uint32_t Num)
{
    struct NetfeStream *s;
    uint32_t i, n;

    n = RTE_MIN(List->Num, Num);
    for (i = 0, s = LIST_FIRST(&List->Head); i != n; i++, s = LIST_NEXT(s, Link))
    {
        Result[i] = s;
    }

    if (s == NULL)
    {
        LIST_INIT(&List->Head);
    }
    else
    {
        LIST_FIRST(&List->Head) = s;
    }

    List->Num -= n;

    return n;
}

//
// Get one stream from the stream list
//
//
static inline struct NetfeStream *NetfeGetStream(struct NetfeStreamList *List)
{
    struct NetfeStream *s = NULL;

    if (List->Num == 0)
    {
        return s;
    }

    NetfeGetStreams(List, &s, 1);

    return s;
}

//
// Put n streams into the host stream list
//
//
static inline void NetfePutHostStreams(struct NetfeLcore *FeLcore, struct NetfeFwdStreamList *StreamList,
                                       struct NetfeFwdStream *FeStream[], uint32_t Num)
{
    uint32_t i, n;

    n = RTE_MIN(FeLcore->NumStreams - StreamList->Num, Num);
    if (n != Num)
    {
        RTE_LOG(ERR, USER1, "%s: HostStreamList overflow by %u\n", __func__, Num - n);
    }

    for (i = 0; i != n; i++)
    {
        LIST_INSERT_HEAD(&StreamList->Head, FeStream[i], Link);
    }
    StreamList->Num += n;
}

//
// Put one stream into the host stream list
//
//
static inline void NetfePutHostStream(struct NetfeLcore *FeLcore, struct NetfeFwdStreamList *StreamList,
                                      struct NetfeFwdStream *Stream)
{
    if (StreamList->Num == FeLcore->NumStreams)
    {
        RTE_LOG(ERR, USER1, "%s: StreamList is full\n", __func__);
        return;
    }

    NetfePutHostStreams(FeLcore, StreamList, &Stream, 1);
}

//
// Get n streams from the host stream list
//
//
static inline uint32_t NetfeGetHostStreams(struct NetfeFwdStreamList *List, struct NetfeFwdStream *Result[],
                                           uint32_t Num)
{
    struct NetfeFwdStream *s;
    uint32_t i, n;

    n = RTE_MIN(List->Num, Num);
    for (i = 0, s = LIST_FIRST(&List->Head); i != n; i++, s = LIST_NEXT(s, Link))
    {
        Result[i] = s;
    }

    if (s == NULL)
    {
        LIST_INIT(&List->Head);
    }
    else
    {
        LIST_FIRST(&List->Head) = s;
    }

    List->Num -= n;

    return n;
}

//
// Get one stream from the host stream list
//
//
static inline struct NetfeFwdStream *NetfeGetHostStream(struct NetfeFwdStreamList *List)
{
    struct NetfeFwdStream *s = NULL;

    if (List->Num == 0)
    {
        return s;
    }

    NetfeGetHostStreams(List, &s, 1);

    return s;
}

//
// Empty the packet buffer
//
//
static inline size_t PktBufEmpty(struct PktBuf *PB)
{
    uint32_t i;
    size_t x;

    x = 0;
    for (i = 0; i != PB->Num; i++)
    {
        x += PB->Pkt[i]->pkt_len;
        NETFE_PKT_DUMP(PB->Pkt[i]);
        rte_pktmbuf_free(PB->Pkt[i]);
    }

    PB->Num = 0;
    return x;
}

//
// Terminate a listening stream
//
//
static inline void NetfeListenStreamTerm(struct NetfeLcore *FeLcore, struct NetfeListenStream *FeStream)
{
    FeStream->TleStream = NULL;
    FeStream->PostErr = 0;
#ifdef DDS_VERBOSE
    memset(&FeStream->Stat, 0, sizeof(FeStream->Stat));
#endif
    PktBufEmpty(&FeStream->Pbuf);
}

//
// Terminate a DPU TCP stream
//
//
static inline void NetfeStreamTerm(struct NetfeLcore *FeLcore, struct NetfeStream *FeStream)
{
    RTE_LOG(NOTICE, USER1, "%s: TCP DPU stream closed (%p): <stream = %p>\n", __func__, FeStream, FeStream->TleStream);

    FeStream->TleStream = NULL;
    FeStream->HostStream = NULL;
    FeStream->PostErr = 0;
#ifdef DDS_VERBOSE
    memset(&FeStream->Stat, 0, sizeof(FeStream->Stat));
#endif
    PktBufEmpty(&FeStream->Pbuf);
    NetfePutStream(FeLcore, &FeLcore->Free, FeStream);
}

//
// Terminate a host TCP stream
//
//
static inline void NetfeHostStreamTerm(struct NetfeLcore *FeLcore, struct NetfeFwdStream *FeStream)
{
    RTE_LOG(NOTICE, USER1, "%s: TCP host stream closed (%p): <stream = %p>\n", __func__, FeStream, FeStream->TleStream);

    FeStream->TleStream = NULL;
    FeStream->DPUStream = NULL;
    FeStream->PostErr = 0;
    FeStream->IsConnected = 0;
#ifdef DDS_VERBOSE
    memset(&FeStream->Stat, 0, sizeof(FeStream->Stat));
#endif
    PktBufEmpty(&FeStream->Pbuf);
    NetfePutHostStream(FeLcore, &FeLcore->HostFree, FeStream);
}

//
// Close a TCP stream
//
//
static inline void NetfeStreamClose(struct NetfeLcore *FeLcore, void *FeStream, enum NetfeStreamType SType)
{
    switch (SType)
    {
    case LISTENING_STREAM: {
        struct NetfeListenStream *feStream = (struct NetfeListenStream *)FeStream;
        tle_tcp_stream_close(feStream->TleStream);
        NetfeListenStreamTerm(FeLcore, feStream);
        break;
    }
    case DPU_STREAM: {
        struct NetfeStream *feStream = (struct NetfeStream *)FeStream;
        tle_tcp_stream_close(feStream->TleStream);
        NetfeStreamTerm(FeLcore, feStream);
        break;
    }
    case HOST_STREAM: {
        struct NetfeFwdStream *feStream = (struct NetfeFwdStream *)FeStream;
        tle_tcp_stream_close(feStream->TleStream);
        NetfeHostStreamTerm(FeLcore, feStream);
        break;
    }
    }
}

//
// Open a listening stream
//
//
static int32_t NetfeListenStreamOpen(struct NetfeLcore *FeLcore, struct NetfeListenStream *FeListenStream,
                                     struct NetfeSParam *SParam, uint32_t Lcore, uint32_t BeIdx)
{
    int32_t result;
    struct sockaddr_in *l4;
    uint16_t errport;
    struct tle_tcp_stream_param tcpStreamParam;

    //
    // Activate events by moving the state of the event to DOWN
    //
    //
    tle_event_active(FeListenStream->TxEv, TLE_SEV_DOWN);
#ifdef DDS_VERBOSE
    FeListenStream->Stat.TxEv[TLE_SEV_DOWN]++;
#endif

    tle_event_active(FeListenStream->RxEv, TLE_SEV_DOWN);
#ifdef DDS_VERBOSE
    FeListenStream->Stat.RxEv[TLE_SEV_DOWN]++;
#endif

    tle_event_active(FeListenStream->ErEv, TLE_SEV_DOWN);
#ifdef DDS_VERBOSE
    FeListenStream->Stat.ErEv[TLE_SEV_DOWN]++;
#endif

    memset(&tcpStreamParam, 0, sizeof(tcpStreamParam));
    tcpStreamParam.addr.local = SParam->LocalAddr;
    tcpStreamParam.addr.remote = SParam->RemoteAddr;
    tcpStreamParam.cfg.err_ev = FeListenStream->ErEv;
    tcpStreamParam.cfg.recv_ev = FeListenStream->RxEv;
    tcpStreamParam.cfg.send_ev = FeListenStream->TxEv;

    FeListenStream->TleStream = tle_tcp_stream_open(BeCfg.Cores[BeIdx].Ctx, &tcpStreamParam);

    if (FeListenStream->TleStream == NULL)
    {
        result = rte_errno;
        NetfeStreamClose(FeLcore, FeListenStream, LISTENING_STREAM);
        rte_errno = result;

        l4 = (struct sockaddr_in *)&SParam->LocalAddr;
        errport = ntohs(l4->sin_port);

        RTE_LOG(ERR, USER1,
                "TCP listening tream open failed for port %u with error "
                "code = %u, BeIdx = %u, lcore = %u\n",
                errport, result, BeIdx, BeCfg.Cores[BeIdx].Id);

        return result;
    }

    RTE_LOG(NOTICE, USER1,
            "%s (lcore = %u) TCP listening stream (%p) created: <stream = %p, rxev = %p, txev = %p>, belcore = %u\n",
            __func__, Lcore, FeListenStream, FeListenStream->TleStream, FeListenStream->RxEv, FeListenStream->TxEv,
            BeCfg.Cores[BeIdx].Id);

    FeListenStream->Laddr = SParam->LocalAddr;

    return 0;
}

//
// Open IPv4 streams to connect to host
//
//
static struct NetfeFwdStream *NetfeHostStreamOpen(struct NetfeLcore *FeLcore, struct NetbeLcore *BeLcore, uint16_t Port)
{
    int32_t result;
    struct NetfeFwdStream *feStream;
    struct sockaddr_in *l4;
    uint16_t errport;
    struct tle_tcp_stream_param tcpStreamParam;

    feStream = NetfeGetHostStream(&FeLcore->HostFree);
    if (feStream == NULL)
    {
        RTE_LOG(ERR, USER1, "%s: Failed to get free host streams on lcore %u\n", __func__, BeLcore->Id);
        rte_errno = ENOBUFS;
        return NULL;
    }

    //
    // Activate events by moving the state of the event to DOWN
    //
    //
    tle_event_active(feStream->TxEv, TLE_SEV_UP);
#ifdef DDS_VERBOSE
    feStream->Stat.TxEv[TLE_SEV_UP]++;
#endif

    tle_event_active(feStream->RxEv, TLE_SEV_DOWN);
#ifdef DDS_VERBOSE
    feStream->Stat.RxEv[TLE_SEV_DOWN]++;
#endif

    tle_event_active(feStream->ErEv, TLE_SEV_DOWN);
#ifdef DDS_VERBOSE
    feStream->Stat.ErEv[TLE_SEV_DOWN]++;
#endif

    FillAddr(&feStream->Laddr, PEPOIPv4Addr, Port);

    memset(&tcpStreamParam, 0, sizeof(tcpStreamParam));
    tcpStreamParam.addr.local = feStream->Laddr;
    tcpStreamParam.addr.remote = feStream->Raddr;
    tcpStreamParam.cfg.err_ev = feStream->ErEv;
    tcpStreamParam.cfg.recv_ev = feStream->RxEv;
    tcpStreamParam.cfg.send_ev = feStream->TxEv;

    //
    // Open the TCP stream
    //
    //
    feStream->TleStream = tle_tcp_stream_open(BeLcore->Ctx, &tcpStreamParam);

    if (feStream->TleStream == NULL)
    {
        result = rte_errno;
        NetfeStreamClose(FeLcore, feStream, HOST_STREAM);
        rte_errno = result;

        l4 = (struct sockaddr_in *)&feStream->Laddr;
        errport = ntohs(l4->sin_port);

        RTE_LOG(ERR, USER1,
                "TCP host stream open failed for port %u with error "
                "code = %u, lcore = %u\n",
                errport, result, BeLcore->Id);

        return NULL;
    }

    RTE_LOG(NOTICE, USER1, "%s (lcore = %u) TCP host stream created (%p): <stream = %p, rxev = %p, txev = %p>\n",
            __func__, BeLcore->Id, feStream, feStream->TleStream, feStream->RxEv, feStream->TxEv);

    NetfePutHostStream(FeLcore, &FeLcore->HostUse, feStream);

    return feStream;
}

//
// Format an IP address
//
//
static const char *FormatAddr(const struct sockaddr_storage *Addr, char Buf[], size_t Len)
{
    const struct sockaddr_in *i4;
    const struct sockaddr_in6 *i6;
    const void *addr;

    if (Addr->ss_family == AF_INET)
    {
        i4 = (const struct sockaddr_in *)Addr;
        addr = &i4->sin_addr;
    }
    else if (Addr->ss_family == AF_INET6)
    {
        i6 = (const struct sockaddr_in6 *)Addr;
        addr = &i6->sin6_addr;
    }
    else
    {
        return NULL;
    }

    return inet_ntop(Addr->ss_family, addr, Buf, Len);
}

//
// Dump a TCP stream
//
//
static void NetfeStreamDump(const void *FeStream, enum NetfeStreamType SType, struct sockaddr_storage *LocalAddr,
                            struct sockaddr_storage *RemoteAddr)
{
    struct sockaddr_in *l4, *r4;
    struct sockaddr_in6 *l6, *r6;
    uint16_t lport, rport;
    char laddr[INET6_ADDRSTRLEN];
    char raddr[INET6_ADDRSTRLEN];

    if (LocalAddr->ss_family == AF_INET)
    {

        l4 = (struct sockaddr_in *)LocalAddr;
        r4 = (struct sockaddr_in *)RemoteAddr;

        lport = l4->sin_port;
        rport = r4->sin_port;
    }
    else if (LocalAddr->ss_family == AF_INET6)
    {

        l6 = (struct sockaddr_in6 *)LocalAddr;
        r6 = (struct sockaddr_in6 *)RemoteAddr;

        lport = l6->sin6_port;
        rport = r6->sin6_port;
    }
    else
    {
        RTE_LOG(ERR, USER1, "unknown family=%hu\n", LocalAddr->ss_family);
        return;
    }

    FormatAddr(LocalAddr, laddr, sizeof(laddr));
    FormatAddr(RemoteAddr, raddr, sizeof(raddr));

    switch (SType)
    {
    case LISTENING_STREAM: {
        struct NetfeListenStream *feListenStream = (struct NetfeListenStream *)FeStream;
#ifdef DDS_VERBOSE
        RTE_LOG(INFO, USER1,
                "TCP listening stream@%p (lcore = %d) = { TLE stream = %p, "
                "Laddr = %s, Lport = %hu, Raddr = %s, Rport = %hu;"
                "Stats = { "
                "RxPkts = %" PRIu64 ", "
                "Drops = %" PRIu64 ", "
                "RxEv[IDLE, DOWN, UP] = [%" PRIu64 ", %" PRIu64 ", %" PRIu64 "]"
                "} }\n",
                feListenStream, rte_lcore_id(), feListenStream->TleStream, laddr, ntohs(lport), raddr, ntohs(rport),
                feListenStream->Stat.RxPkts, feListenStream->Stat.Drops, feListenStream->Stat.RxEv[TLE_SEV_IDLE],
                feListenStream->Stat.RxEv[TLE_SEV_DOWN], feListenStream->Stat.RxEv[TLE_SEV_UP]);
#else
        RTE_LOG(INFO, USER1,
                "TCP listening stream@%p (lcore = %d) = { TLE stream = %p, "
                "Laddr = %s, Lport = %hu, Raddr = %s, Rport = %hu }\n",
                feListenStream, rte_lcore_id(), feListenStream->TleStream, laddr, ntohs(lport), raddr, ntohs(rport));
#endif
        break;
    }
    case DPU_STREAM: {
        struct NetfeStream *feStream = (struct NetfeStream *)FeStream;
#ifdef DDS_VERBOSE
        RTE_LOG(INFO, USER1,
                "TCP DPU stream@%p (lcore = %d) = { TLE stream = %p, "
                "Laddr = %s, Lport = %hu, Raddr = %s, Rport = %hu;"
                "Stats = { "
                "RxPkts = %" PRIu64 ", "
                "TxPkts = %" PRIu64 ", "
                "FwdPkts = %" PRIu64 ", Drops = %" PRIu64 ", "
                "RxEv[IDLE, DOWN, UP] = [%" PRIu64 ", %" PRIu64 ", %" PRIu64 "], "
                "TxEv[IDLE, DOWN, UP] = [%" PRIu64 ", %" PRIu64 ", %" PRIu64 "] "
                "} }\n",
                feStream, rte_lcore_id(), feStream->TleStream, laddr, ntohs(lport), raddr, ntohs(rport),
                feStream->Stat.RxPkts, feStream->Stat.TxPkts, feStream->Stat.FwdPkts, feStream->Stat.Drops,
                feStream->Stat.RxEv[TLE_SEV_IDLE], feStream->Stat.RxEv[TLE_SEV_DOWN], feStream->Stat.RxEv[TLE_SEV_UP],
                feStream->Stat.TxEv[TLE_SEV_IDLE], feStream->Stat.TxEv[TLE_SEV_DOWN], feStream->Stat.TxEv[TLE_SEV_UP]);
#else
        RTE_LOG(INFO, USER1,
                "TCP DPU stream@%p (lcore = %d) = { TLE stream = %p, "
                "Laddr = %s, Lport = %hu, Raddr = %s, Rport = %hu }\n",
                feStream, rte_lcore_id(), feStream->TleStream, laddr, ntohs(lport), raddr, ntohs(rport));
#endif
        break;
    }
    case HOST_STREAM: {
        struct NetfeFwdStream *feStream = (struct NetfeFwdStream *)FeStream;
#ifdef DDS_VERBOSE
        RTE_LOG(INFO, USER1,
                "TCP host stream@%p (lcore = %d) = { TLE stream = %p, "
                "Laddr = %s, Lport = %hu, Raddr = %s, Rport = %hu;"
                "Stats = { "
                "RxPkts = %" PRIu64 ", "
                "TxPkts = %" PRIu64 ", "
                "Drops = %" PRIu64 ", "
                "RxEv[IDLE, DOWN, UP] = [%" PRIu64 ", %" PRIu64 ", %" PRIu64 "], "
                "TxEv[IDLE, DOWN, UP] = [%" PRIu64 ", %" PRIu64 ", %" PRIu64 "] "
                "} }\n",
                feStream, rte_lcore_id(), feStream->TleStream, laddr, ntohs(lport), raddr, ntohs(rport),
                feStream->Stat.RxPkts, feStream->Stat.TxPkts, feStream->Stat.Drops, feStream->Stat.RxEv[TLE_SEV_IDLE],
                feStream->Stat.RxEv[TLE_SEV_DOWN], feStream->Stat.RxEv[TLE_SEV_UP], feStream->Stat.TxEv[TLE_SEV_IDLE],
                feStream->Stat.TxEv[TLE_SEV_DOWN], feStream->Stat.TxEv[TLE_SEV_UP]);
#else
        RTE_LOG(INFO, USER1,
                "TCP host stream@%p (lcore = %d) = { TLE stream = %p, "
                "Laddr = %s, Lport = %hu, Raddr = %s, Rport = %hu }\n",
                feStream, rte_lcore_id(), feStream->TleStream, laddr, ntohs(lport), raddr, ntohs(rport));
#endif
        break;
    }
    default: {
        RTE_LOG(ERR, USER1, "unknown stream type=%d\n", SType);
        break;
    }
    }
}

//
// Init per-core front-end state
//
//
static int NetfeLcoreInit(const struct NetfeLcoreParam *Param)
{
    size_t sz;
    int32_t result;
    uint32_t i, lcore, snum, sid;
    struct NetfeLcore *fe;
    struct tle_evq_param evtParam;
    struct NetfeListenStream *feListenStream;
    struct NetfeStream *feStream;
    struct NetfeFwdStream *feHostStream;
    struct NetfeSParam *sParam;

    lcore = rte_lcore_id();
    sid = rte_lcore_to_socket_id(lcore);

    snum = Param->MaxStreams;
    RTE_LOG(NOTICE, USER1, "%s (lcore = %u, listen_streams = %u, max_streams = %u)\n", __func__, lcore,
            Param->NumStreams, snum);

    memset(&evtParam, 0, sizeof(evtParam));
    evtParam.socket_id = sid;
    evtParam.max_events = snum;

    sz = sizeof(struct NetfeLcore) + snum * sizeof(struct NetfeStream) + snum * sizeof(struct NetfeFwdStream);
    fe = (struct NetfeLcore *)rte_zmalloc_socket(NULL, sz, RTE_CACHE_LINE_SIZE, sid);
    if (fe == NULL)
    {
        RTE_LOG(ERR, USER1, "%s:%d failed to allocate %zu bytes for NetfeLcore\n", __func__, __LINE__, sz);
        return -ENOMEM;
    }

    RTE_PER_LCORE(_fe) = fe;

    fe->NumStreams = snum;
    fe->NumListeners = Param->NumStreams;

    sz = sizeof(struct NetfeListenStream) * fe->NumListeners;
    fe->Listeners = (struct NetfeListenStream *)rte_zmalloc_socket(NULL, sz, RTE_CACHE_LINE_SIZE, sid);
    if (fe->Listeners == NULL)
    {
        RTE_LOG(ERR, USER1, "%s:%d failed to allocate %zu bytes for Listeners\n", __func__, __LINE__, sz);
        rte_free(fe);
        return -ENOMEM;
    }

    LIST_INIT(&fe->Free.Head);
    LIST_INIT(&fe->Use.Head);
    LIST_INIT(&fe->HostFree.Head);
    LIST_INIT(&fe->HostUse.Head);

    //
    // Set up event queues and events
    //
    //
    fe->ErEq = tle_evq_create(&evtParam);
    fe->RxEq = tle_evq_create(&evtParam);
    fe->TxEq = tle_evq_create(&evtParam);
    fe->HostErEq = tle_evq_create(&evtParam);
    fe->HostRxEq = tle_evq_create(&evtParam);
    fe->HostTxEq = tle_evq_create(&evtParam);
    fe->ListenErEq = tle_evq_create(&evtParam);
    fe->ListenRxEq = tle_evq_create(&evtParam);
    fe->ListenTxEq = tle_evq_create(&evtParam);

    // get rid of offload
    /* fe->OffloadPred = Param->OffloadPred;
    fe->OffloadFunc = Param->OffloadFunc; */

    RTE_LOG(INFO, USER1, "%s (%u) ErEq = %p, RxEq = %p, TxEq = %p, \
        HostErEq = %p, HostRxEq = %p, HostTxEq = %p, \
        ListenErEq = %p, ListenRxEq = %p, ListenTxEq = %p \n",
            __func__, lcore, fe->ErEq, fe->RxEq, fe->TxEq, fe->HostErEq, fe->HostRxEq, fe->HostTxEq, fe->ListenErEq,
            fe->ListenRxEq, fe->ListenTxEq);
    if (fe->ErEq == NULL || fe->RxEq == NULL || fe->TxEq == NULL || fe->HostErEq == NULL || fe->HostRxEq == NULL ||
        fe->HostTxEq == NULL || fe->ListenErEq == NULL || fe->ListenRxEq == NULL || fe->ListenTxEq == NULL)
    {
        RTE_LOG(ERR, USER1, "%s:%d failed to create event queues\n", __func__, __LINE__);
        result = -rte_errno;
        return result;
    }

    //
    // Allocate events for all DPU streams
    //
    //
    feStream = (struct NetfeStream *)(fe + 1);
    for (i = 0; i != snum; i++)
    {
        feStream[i].RxEv = tle_event_alloc(fe->RxEq, feStream + i);
        feStream[i].TxEv = tle_event_alloc(fe->TxEq, feStream + i);
        feStream[i].ErEv = tle_event_alloc(fe->ErEq, feStream + i);

        NetfePutStream(fe, &fe->Free, &feStream[i]);
    }

    //
    // Allocate events for all host streams
    //
    //
    feHostStream = (struct NetfeFwdStream *)((struct NetfeStream *)(fe + 1) + snum);
    for (i = 0; i != snum; i++)
    {
        feHostStream[i].RxEv = tle_event_alloc(fe->HostRxEq, feHostStream + i);
        feHostStream[i].TxEv = tle_event_alloc(fe->HostTxEq, feHostStream + i);
        feHostStream[i].ErEv = tle_event_alloc(fe->HostErEq, feHostStream + i);

        //
        // We need to fill in the port later
        //
        //
        FillAddr(&feHostStream[i].Laddr, PEPOIPv4Addr, 0);
        FillAddr(&feHostStream[i].Raddr, GlobalHostIPv4Addr, GlobalHostTCPPort);

        NetfePutHostStream(fe, &fe->HostFree, &feHostStream[i]);
    }

    //
    // Allocate events for all listening streams and open these streams
    //
    //
    feListenStream = fe->Listeners;
    for (i = 0; i != fe->NumListeners; i++)
    {
        feListenStream[i].RxEv = tle_event_alloc(fe->ListenRxEq, feListenStream + i);
        feListenStream[i].TxEv = tle_event_alloc(fe->ListenTxEq, feListenStream + i);
        feListenStream[i].ErEv = tle_event_alloc(fe->ListenErEq, feListenStream + i);

        sParam = &Param->Streams[i].DPUStreamParam;

        result = NetfeListenStreamOpen(fe, &feListenStream[i], sParam, lcore, sParam->Bidx);
        if (result)
        {
            RTE_LOG(ERR, USER1, "%s (lcore = %u) failed to open listening stream %u\n", __func__, lcore, i);
            break;
        }

        NetfeStreamDump(&feListenStream[i], LISTENING_STREAM, &sParam->LocalAddr, &sParam->RemoteAddr);

        //
        // Start listening on the stream
        //
        //
        result = tle_tcp_stream_listen(feListenStream[i].TleStream);
        RTE_LOG(INFO, USER1,
                "%s(%u) tle_tcp_stream_listen (stream = %p) "
                "returns %d\n",
                __func__, lcore, feListenStream[i].TleStream, result);

        if (result != 0)
        {
            break;
        }
    }

    //
    // Initialization for offloading
    // get rid of offload
    //
    //
    /* for (i = 0; i != DDS_MAX_OUTSTANDING_IO; i++)
    {
        fe->ReadOps[i].ReadReq.RequestId = i;
        fe->ReadOps[i].ReadResp.RequestId = i;
        fe->ReadOps[i].StreamCtxt = NULL;
#if OFFLOAD_ENGINE_ZERO_COPY == OFFLOAD_ENGINE_ZERO_COPY_NONE
        fe->ReadCtxts[i].DataBuffer.FirstAddr = fe->ReadOps[i].RespBuf;
#elif OFFLOAD_ENGINE_ZERO_COPY == OFFLOAD_ENGINE_ZERO_COPY_STATIC
        fe->ReadOps[i].RespBuf = (void *)rte_pktmbuf_alloc(ReadBufMpool[sid + 1]);
        fe->ReadCtxts[i].DataBuffer.FirstAddr =
            rte_pktmbuf_append((struct rte_mbuf *)fe->ReadOps[i].RespBuf, MAX_READ_BUFFER_SIZE);
#endif
        fe->ReadCtxts[i].DataBuffer.SecondAddr = NULL;
        fe->ReadCtxts[i].Request = &fe->ReadOps[i].ReadReq;
        fe->ReadCtxts[i].Response = &fe->ReadOps[i].ReadResp;
        fe->ReadCtxts[i].IsRead = true;
    }

    fe->ReadOpsHeadPointer = 0;
    fe->ReadOpsTailPointer = 0; */

    return result;
}

struct PType2Cb
{
    uint32_t mask;
    const char *name;
    rte_rx_callback_fn fn;
};

enum
{
    ETHER_PTYPE = 0x1,
    IPV4_PTYPE = 0x2,
    IPV4_EXT_PTYPE = 0x4,
    IPV6_PTYPE = 0x8,
    IPV6_EXT_PTYPE = 0x10,
    TCP_PTYPE = 0x20,
    UDP_PTYPE = 0x40,
};

//
// Get packet type mask supported by the port
//
//
static uint32_t GetPTypes(const struct NetbePort *BePort)
{
    uint32_t smask;
    int32_t i, result;
    const uint32_t pmask = RTE_PTYPE_L2_MASK | RTE_PTYPE_L3_MASK | RTE_PTYPE_L4_MASK;

    smask = 0;
    result = rte_eth_dev_get_supported_ptypes(BePort->Id, pmask, NULL, 0);
    if (result < 0)
    {
        RTE_LOG(ERR, USER1, "%s (port = %u) failed to get supported ptypes\n", __func__, BePort->Id);
        return smask;
    }

    uint32_t ptype[result];
    result = rte_eth_dev_get_supported_ptypes(BePort->Id, pmask, ptype, result);

    for (i = 0; i != result; i++)
    {
        switch (ptype[i])
        {
        case RTE_PTYPE_L2_ETHER:
            smask |= ETHER_PTYPE;
            break;
        case RTE_PTYPE_L3_IPV4:
        case RTE_PTYPE_L3_IPV4_EXT_UNKNOWN:
            smask |= IPV4_PTYPE;
            break;
        case RTE_PTYPE_L3_IPV4_EXT:
            smask |= IPV4_EXT_PTYPE;
            break;
        case RTE_PTYPE_L3_IPV6:
        case RTE_PTYPE_L3_IPV6_EXT_UNKNOWN:
            smask |= IPV6_PTYPE;
            break;
        case RTE_PTYPE_L3_IPV6_EXT:
            smask |= IPV6_EXT_PTYPE;
            break;
        case RTE_PTYPE_L4_TCP:
            smask |= TCP_PTYPE;
            break;
        case RTE_PTYPE_L4_UDP:
            smask |= UDP_PTYPE;
            break;
        }
    }

    return smask;
}

//
// Set offload flags
//
//
static inline uint64_t MbufTxOffload(uint64_t Il2, uint64_t Il3, uint64_t Il4, uint64_t Tso, uint64_t Ol3, uint64_t Ol2)
{
    return Il2 | Il3 << 7 | Il4 << 16 | Tso << 24 | Ol3 << 40 | Ol2 << 49;
}

//
// Fill packet header length
//
//
static inline void FillPktHdrLen(struct rte_mbuf *Pkt, uint32_t L2, uint32_t L3, uint32_t L4)
{
    Pkt->tx_offload = MbufTxOffload(L2, L3, L4, 0, 0, 0);
}

//
// Get TCP header size
//
//
static inline uint32_t GetTcpHeaderSize(struct rte_mbuf *Pkt, uint32_t l2Len, uint32_t l3Len)
{
    const struct rte_tcp_hdr *tcp;

    tcp = rte_pktmbuf_mtod_offset(Pkt, struct rte_tcp_hdr *, l2Len + l3Len);
    return (tcp->data_off >> 4) * 4;
}

//
// Adjust IPv4 packet length
//
//
static inline void AdjustIpv4Pktlen(struct rte_mbuf *Pkt, uint32_t L2Len)
{
    uint32_t plen, trim;
    const struct rte_ipv4_hdr *iph;

    iph = rte_pktmbuf_mtod_offset(Pkt, const struct rte_ipv4_hdr *, L2Len);
    plen = rte_be_to_cpu_16(iph->total_length) + L2Len;
    if (plen < Pkt->pkt_len)
    {
        trim = Pkt->pkt_len - plen;
        rte_pktmbuf_trim(Pkt, trim);
    }
}

//
// Check if packet is IPv4 fragment
//
//
static inline int IsIpv4Frag(const struct rte_ipv4_hdr *Iph)
{
    const uint16_t mask = rte_cpu_to_be_16(~RTE_IPV4_HDR_DF_FLAG);

    return ((mask & Iph->fragment_offset) != 0);
}

static inline uint32_t GetIpv4HdrLen(struct rte_mbuf *Pkt, uint32_t L2, uint32_t Frag)
{
    const struct rte_ipv4_hdr *iph;
    int32_t dlen, len;

    dlen = rte_pktmbuf_data_len(Pkt);
    dlen -= L2;

    iph = rte_pktmbuf_mtod_offset(Pkt, const struct rte_ipv4_hdr *, L2);
    len = (iph->version_ihl & RTE_IPV4_HDR_IHL_MASK) * RTE_IPV4_IHL_MULTIPLIER;

    if (Frag != 0 && IsIpv4Frag(iph))
    {
        Pkt->packet_type &= ~RTE_PTYPE_L4_MASK;
        Pkt->packet_type |= RTE_PTYPE_L4_FRAG;
    }

    return len;
}

//
// HW can recognize L2/L3 with/without extensions/L4 (ixgbe/igb/fm10k)
//
//
static uint16_t Type0TcpRxCallback(__rte_unused dpdk_port_t Port, __rte_unused uint16_t Queue, struct rte_mbuf *Pkt[],
                                   uint16_t NumPkts, __rte_unused uint16_t MaxPkts, void *UserParam)
{
    uint32_t j, tp;
    struct NetbeLcore *lc;
    uint32_t l4Len, l3Len, l2Len;

    lc = (struct NetbeLcore *)UserParam;
    l2Len = sizeof(struct rte_ether_hdr);

    RTE_SET_USED(lc);

    for (j = 0; j != NumPkts; j++)
    {

        NETBE_PKT_DUMP(Pkt[j]);

        tp = Pkt[j]->packet_type & (RTE_PTYPE_L4_MASK | RTE_PTYPE_L3_MASK | RTE_PTYPE_L2_MASK);

        switch (tp)
        {
        /* non fragmented tcp packets. */
        case (RTE_PTYPE_L4_TCP | RTE_PTYPE_L3_IPV4 | RTE_PTYPE_L2_ETHER):
            l4Len = GetTcpHeaderSize(Pkt[j], l2Len, sizeof(struct rte_ipv4_hdr));
            FillPktHdrLen(Pkt[j], l2Len, sizeof(struct rte_ipv4_hdr), l4Len);
            AdjustIpv4Pktlen(Pkt[j], l2Len);
            break;
        case (RTE_PTYPE_L4_TCP | RTE_PTYPE_L3_IPV4_EXT | RTE_PTYPE_L2_ETHER):
            l3Len = GetIpv4HdrLen(Pkt[j], l2Len, 0);
            l4Len = GetTcpHeaderSize(Pkt[j], l2Len, l3Len);
            FillPktHdrLen(Pkt[j], l2Len, l3Len, l4Len);
            AdjustIpv4Pktlen(Pkt[j], l2Len);
            break;
        default:
            /* treat packet types as invalid. */
            Pkt[j]->packet_type = RTE_PTYPE_UNKNOWN;
            break;
        }
    }

    return NumPkts;
}

#ifdef DDS_VERBOSE
//
// Update TCP statistics
//
//
static inline void TcpStatUpdate(struct NetbeLcore *BeLcore, const struct rte_mbuf *Pkt, uint32_t L2Len, uint32_t L3Len)
{
    const struct rte_tcp_hdr *th;

    th = rte_pktmbuf_mtod_offset(Pkt, struct rte_tcp_hdr *, L2Len + L3Len);
    BeLcore->TcpStat.Flags[th->tcp_flags]++;
}
#endif

//
// HW can recognize L2/L3/L4 and fragments (i40e).
//
//
static uint16_t Type1TcpRxCallback(__rte_unused dpdk_port_t Port, __rte_unused uint16_t Queue, struct rte_mbuf *Pkt[],
                                   uint16_t NumPkts, __rte_unused uint16_t MaxPkts, void *UserParam)
{
    uint32_t j, tp;
    struct NetbeLcore *lc;
    uint32_t l4Len, l3Len, l2Len;
    const struct rte_ether_hdr *eth;

    lc = (struct NetbeLcore *)UserParam;
    l2Len = sizeof(*eth);

    RTE_SET_USED(lc);

    for (j = 0; j != NumPkts; j++)
    {

        NETBE_PKT_DUMP(Pkt[j]);

        tp = Pkt[j]->packet_type & (RTE_PTYPE_L4_MASK | RTE_PTYPE_L3_MASK | RTE_PTYPE_L2_MASK);

        switch (tp)
        {
        case (RTE_PTYPE_L4_TCP | RTE_PTYPE_L3_IPV4_EXT_UNKNOWN | RTE_PTYPE_L2_ETHER):
            l3Len = GetIpv4HdrLen(Pkt[j], l2Len, 0);
            l4Len = GetTcpHeaderSize(Pkt[j], l2Len, l3Len);
            FillPktHdrLen(Pkt[j], l2Len, l3Len, l4Len);
            AdjustIpv4Pktlen(Pkt[j], l2Len);
#ifdef DDS_VERBOSE
            TcpStatUpdate(lc, Pkt[j], l2Len, l3Len);
#endif
            break;
        default:
            /* treat packet types as invalid. */
            Pkt[j]->packet_type = RTE_PTYPE_UNKNOWN;
            break;
        }
    }

    return NumPkts;
}

//
// Fill TCP packet header lengths
//
//
static inline void FillEthTcpHdrLen(struct rte_mbuf *Pkt)
{
    uint32_t dlen, l2Len, l3Len, l4Len;
    uint16_t etp;
    const struct rte_ether_hdr *eth;

    dlen = rte_pktmbuf_data_len(Pkt);

    if (dlen < sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_tcp_hdr))
    {
        Pkt->packet_type = RTE_PTYPE_UNKNOWN;
        return;
    }

    l2Len = sizeof(*eth);

    eth = rte_pktmbuf_mtod(Pkt, const struct rte_ether_hdr *);
    etp = eth->ether_type;
    if (etp == rte_be_to_cpu_16(RTE_ETHER_TYPE_VLAN))
        l2Len += sizeof(struct rte_vlan_hdr);

    if (etp == rte_be_to_cpu_16(RTE_ETHER_TYPE_IPV4))
    {
        Pkt->packet_type = RTE_PTYPE_L4_TCP | RTE_PTYPE_L3_IPV4_EXT_UNKNOWN | RTE_PTYPE_L2_ETHER;
        l3Len = GetIpv4HdrLen(Pkt, l2Len, 1);
        l4Len = GetTcpHeaderSize(Pkt, l2Len, l3Len);
        FillPktHdrLen(Pkt, l2Len, l3Len, l4Len);
        AdjustIpv4Pktlen(Pkt, l2Len);
    }
    else
    {
        Pkt->packet_type = RTE_PTYPE_UNKNOWN;
    }
}

//
// TCP no HW packet type
//
//
static uint16_t TypenTcpRxCallback(__rte_unused dpdk_port_t Port, __rte_unused uint16_t Queue, struct rte_mbuf *Pkt[],
                                   uint16_t NumPkts, __rte_unused uint16_t MaxPkts, void *UserParam)
{
    uint32_t j;
    struct NetbeLcore *lc;

    lc = (struct NetbeLcore *)UserParam;

    RTE_SET_USED(lc);

    for (j = 0; j != NumPkts; j++)
    {

        NETBE_PKT_DUMP(Pkt[j]);
        FillEthTcpHdrLen(Pkt[j]);
    }

    return NumPkts;
}

//
// Setup RX callback for the port
//
//
int SetupRxCb(const struct NetbePort *Port, struct NetbeLcore *BeLcore, uint16_t Qid)
{
    int32_t result;
    uint32_t i, n, smask;
    const void *cb;
    const struct PType2Cb *ptype2cb;

    static const struct PType2Cb tcpPType2Cb[] = {
        {
            .mask = ETHER_PTYPE | IPV4_PTYPE | IPV4_EXT_PTYPE | IPV6_PTYPE | IPV6_EXT_PTYPE | TCP_PTYPE,
            .name = "HW l2/l3x/l4-tcp ptype",
            .fn = Type0TcpRxCallback,
        },
        {
            .mask = ETHER_PTYPE | IPV4_PTYPE | IPV6_PTYPE | TCP_PTYPE,
            .name = "HW l2/l3/l4-tcp ptype",
            .fn = Type1TcpRxCallback,
        },
        {
            .mask = 0,
            .name = "tcp no HW ptype",
            .fn = TypenTcpRxCallback,
        },
    };

    smask = GetPTypes(Port);

    //
    // Only TCP is supported
    //
    //
    ptype2cb = tcpPType2Cb;
    n = RTE_DIM(tcpPType2Cb);

    for (i = 0; i != n; i++)
    {
        if ((smask & ptype2cb[i].mask) == ptype2cb[i].mask)
        {
            cb = rte_eth_add_rx_callback(Port->Id, Qid, ptype2cb[i].fn, BeLcore);
            result = -rte_errno;
            RTE_LOG(ERR, USER1,
                    "%s (port = %u), setup RX callback \"%s\" "
                    "returns %p\n",
                    __func__, Port->Id, ptype2cb[i].name, cb);
            return ((cb == NULL) ? result : 0);
        }
    }

    RTE_LOG(ERR, USER1, "%s (port = %u) failed to find an appropriate callback\n", __func__, Port->Id);
    return -ENOENT;
}

//
// Set up lcore for back end
//
//
static int NetbeLcoreSetup(struct NetbeLcore *BeLcore)
{
    uint32_t i;
    int32_t result;

    RTE_LOG(NOTICE, USER1, "%s: (lcore = %u, ctx = %p) start\n", __func__, BeLcore->Id, BeLcore->Ctx);

    /*
     * ???????
     * wait for FE lcores to start, so BE dont' drop any packets
     * because corresponding streams not opened yet by FE.
     * useful when used with pcap PMDS.
     * think better way, or should this timeout be a cmdlien parameter.
     * ???????
     */
    rte_delay_ms(10);

    result = 0;
    for (i = 0; i != BeLcore->NumPortQueues && result == 0; i++)
    {
        result = SetupRxCb(&BeLcore->PortQueues[i].Port, BeLcore, BeLcore->PortQueues[i].RxQid);
        if (result < 0)
        {
            return result;
        }
    }

    if (result == 0)
    {
        RTE_PER_LCORE(_be) = BeLcore;
    }

    return result;
}

//
// Clear front end before terminating TCP
//
//
static void NetfeLcoreFiniTcp(void)
{
    struct NetfeLcore *fe;
    uint32_t i, snum;
    struct tle_tcp_stream_addr addr;
    void *fes;
#ifdef DDS_VERBOSE
    uint32_t acc, rej, ter;
#endif

    fe = RTE_PER_LCORE(_fe);
    if (fe == NULL)
    {
        return;
    }

    //
    // Close listening streams
    //
    //
    snum = fe->NumListeners;
    for (i = 0; i != snum; i++)
    {
        fes = &fe->Listeners[i];
        tle_tcp_stream_get_addr(fe->Listeners[i].TleStream, &addr);
        NetfeStreamDump(fes, LISTENING_STREAM, &addr.local, &addr.remote);
        NetfeStreamClose(fe, fes, LISTENING_STREAM);
    }

    //
    // Close DPU streams
    //
    //
    snum = fe->Use.Num;
    for (i = 0; i != snum; i++)
    {
        fes = NetfeGetStream(&fe->Use);
        tle_tcp_stream_get_addr(((struct NetfeStream *)fes)->TleStream, &addr);
        NetfeStreamDump(fes, DPU_STREAM, &addr.local, &addr.remote);
        NetfeStreamClose(fe, fes, DPU_STREAM);
    }

    //
    // Close host streams
    //
    //
    snum = fe->HostUse.Num;
    for (i = 0; i != snum; i++)
    {
        fes = NetfeGetHostStream(&fe->HostUse);
        tle_tcp_stream_get_addr(((struct NetfeFwdStream *)fes)->TleStream, &addr);
        NetfeStreamDump(fes, HOST_STREAM, &addr.local, &addr.remote);
        NetfeStreamClose(fe, fes, HOST_STREAM);
    }

#ifdef DDS_VERBOSE
    acc = fe->TcpStat.Acc;
    rej = fe->TcpStat.Rej;
    ter = fe->TcpStat.Ter;

    RTE_LOG(NOTICE, USER1, "TcpStat = {con_acc = %u, con_rej = %u, con_ter = %u}\n", acc, rej, ter);
#endif

    tle_evq_destroy(fe->ListenTxEq);
    tle_evq_destroy(fe->ListenRxEq);
    tle_evq_destroy(fe->ListenErEq);
    tle_evq_destroy(fe->HostTxEq);
    tle_evq_destroy(fe->HostRxEq);
    tle_evq_destroy(fe->HostErEq);
    tle_evq_destroy(fe->TxEq);
    tle_evq_destroy(fe->RxEq);
    tle_evq_destroy(fe->ErEq);

    //
    // Release read buffers
    // get rid of offload now
    //
    //
    /* #if OFFLOAD_ENGINE_ZERO_COPY == OFFLOAD_ENGINE_ZERO_COPY_STATIC
        for (i = 0; i != DDS_MAX_OUTSTANDING_IO; i++)
        {
            rte_pktmbuf_free((struct rte_mbuf *)fe->ReadOps[i].RespBuf);
        }
    #endif */
}

//
// Clear back end before terminating the front end
//
//
static void NetbeLcoreClear(void)
{
    uint32_t i, j;
    struct NetbeLcore *lc;

    lc = RTE_PER_LCORE(_be);
    if (lc == NULL)
    {
        return;
    }

    RTE_LOG(NOTICE, USER1, "%s (lcore = %u, ctx: %p) finish\n", __func__, lc->Id, lc->Ctx);

    for (i = 0; i != lc->NumPortQueues; i++)
    {
#ifdef DDS_VERBOSE
        RTE_LOG(NOTICE, USER1,
                "%s: %u (port = %u, q = %u, lcore = %u, dev = %p) "
                "rx_stats = {"
                "in = %" PRIu64 ", up = %" PRIu64 ", drop = %" PRIu64 "}, "
                "tx_stats = {"
                "in = %" PRIu64 ", up = %" PRIu64 ", drop = %" PRIu64 "}\n",
                __func__, i, lc->PortQueues[i].Port.Id, lc->PortQueues[i].RxQid, lc->Id, lc->PortQueues[i].Dev,
                lc->PortQueues[i].RxStat.In, lc->PortQueues[i].RxStat.Up, lc->PortQueues[i].RxStat.Drop,
                lc->PortQueues[i].TxStat.Down, lc->PortQueues[i].TxStat.Out, lc->PortQueues[i].TxStat.Drop);
#else
        RTE_LOG(NOTICE, USER1, "%s: %u (port = %u, q = %u, lcore = %u, dev = %p)\n", __func__, i,
                lc->PortQueues[i].Port.Id, lc->PortQueues[i].RxQid, lc->Id, lc->PortQueues[i].Dev);
#endif
    }

#ifdef DDS_VERBOSE
    RTE_LOG(NOTICE, USER1, "tcp_stat = {\n");
    for (i = 0; i != RTE_DIM(lc->TcpStat.Flags); i++)
    {
        if (lc->TcpStat.Flags[i] != 0)
        {
            RTE_LOG(NOTICE, USER1, "[flag = %#x] == %" PRIu64 "\n", i, lc->TcpStat.Flags[i]);
        }
    }
    RTE_LOG(NOTICE, USER1, "}\n");
#endif

    for (i = 0; i != lc->NumPortQueues; i++)
    {
        for (j = 0; j != lc->PortQueues[i].TxBuf.Num; j++)
        {
            rte_pktmbuf_free(lc->PortQueues[i].TxBuf.Pkt[j]);
        }
    }

    RTE_PER_LCORE(_be) = NULL;
}

//
// Remove a stream from a list
//
//
static inline void NetfeRemStream(struct NetfeStreamList *List, struct NetfeStream *Stream)
{
    LIST_REMOVE(Stream, Link);
    List->Num--;
}

//
// Remove a host stream from a list
//
//
static inline void NetfeRemHostStream(struct NetfeFwdStreamList *List, struct NetfeFwdStream *Stream)
{
    LIST_REMOVE(Stream, Link);
    List->Num--;
}

//
// Process TCP send and receive events
//
//
static inline int NetfeRxTxProcess(__rte_unused uint32_t Lcore, struct NetfeStream *FeStream,
                                   struct NetfeLcore *FeLcore)
{
    uint32_t i, k, n;
    struct rte_mbuf **pkts;
    struct rte_mbuf *curPkt;
    char *rawMsg;

    // uint16_t offsetInMsg, totalBytesOfDPUReqs;
    // uint16_t reqSize;

    // get rid of offload
    /* #if OFFLOAD_ENGINE_ZERO_COPY == OFFLOAD_ENGINE_ZERO_COPY_DYNAMIC
        uint32_t sid = rte_lcore_to_socket_id(Lcore);
    #endif */
    // uint32_t sid = rte_lcore_to_socket_id(Lcore);

    n = FeStream->Pbuf.Num;
    pkts = FeStream->Pbuf.Pkt;

    if (n == 0)
    {
        tle_event_idle(FeStream->TxEv);
#ifdef DDS_VERBOSE
        FeStream->Stat.TxEv[TLE_SEV_IDLE]++;
#endif
        return 0;
    }

    //
    // Process the received packets
    //
    //
    for (k = 0; k != n; k++)
    {
        NETFE_TRACE("%s (%u): packet %d (%p) / %d has %u bytes\n", __func__, Lcore, k, pkts[k], n, pkts[k]->data_len);
        curPkt = pkts[k];
        rawMsg = (char *)rte_pktmbuf_mtod(curPkt, void *);
        ProcessPacketMsg(rawMsg, curPkt->data_len);
        //
        // Apply the offload predicate
        // get rid of offload (and the cache table)
        //
        //
        /* FeLcore->NumReqsForHost = 0;
        FeLcore->NumReqsForDPU = 0;

        FeLcore->OffloadPred(rawMsg, curPkt->data_len, GlobalCacheTable, FeLcore->ReqsForHost, FeLcore->ReqsForDPU,
                             &FeLcore->NumReqsForHost, &FeLcore->NumReqsForDPU); */

        /* if (FeLcore->NumReqsForDPU)
        {
            //
            // At least one request is offloaded to the DPU, so we need to split the message
            // For efficiency, we process offloaded requests first
            // Then we override offloaded requests in the original message
            //
            //
            offsetInMsg = 0;
            totalBytesOfDPUReqs = 0;

            RequestIdT firstIndex = DDS_DPU_IO_SLOT_NUMBER_BASE * (8 - Lcore) + FeLcore->ReadOpsTailPointer;

            for (i = 0; i != FeLcore->NumReqsForDPU; i++)
            {
                ReadOpDescriptorT *readOp = &FeLcore->ReadOps[FeLcore->ReadOpsTailPointer];
                DataPlaneRequestContext *readCtxt = &FeLcore->ReadCtxts[FeLcore->ReadOpsTailPointer];
                readOp->ReadResp.Result = DDS_ERROR_CODE_IO_PENDING;

                FeLcore->OffloadFunc(rawMsg, &FeLcore->ReqsForDPU[i], GlobalCacheTable, &readOp->ReadReq);

#ifdef OFFLOAD_ENGINE_ALLOW_NONALIGNED_IO
                //
                // Check alignment
                //
                //
                readOp->OffsetAlignment = readOp->ReadReq.Offset % DDS_BACKEND_SECTOR_SIZE;
                readOp->ReadReq.Offset -= readOp->OffsetAlignment;
                readOp->BytesAlignment = readOp->ReadReq.Bytes % DDS_BACKEND_SECTOR_SIZE;
                readOp->ReadReq.Bytes +=
                    (readOp->BytesAlignment ? (DDS_BACKEND_SECTOR_SIZE - readOp->BytesAlignment) : 0);
#endif

                reqSize = FeLcore->ReqsForDPU[i].Bytes;

                totalBytesOfDPUReqs += reqSize;
                readCtxt->DataBuffer.TotalSize = readCtxt->Request->Bytes;
                readCtxt->DataBuffer.FirstSize = readCtxt->DataBuffer.TotalSize;
#if OFFLOAD_ENGINE_ZERO_COPY == OFFLOAD_ENGINE_ZERO_COPY_DYNAMIC
                readOp->RespBuf = (void *)rte_pktmbuf_alloc(ReadBufMpool[sid + 1]);
#ifdef OFFLOAD_ENGINE_RESPONSE_INCLUDES_REQUEST
                readCtxt->DataBuffer.FirstAddr =
                    rte_pktmbuf_append((struct rte_mbuf *)readOp->RespBuf, readCtxt->DataBuffer.FirstSize + reqSize);
#else
                readCtxt->DataBuffer.FirstAddr =
                    rte_pktmbuf_append((struct rte_mbuf *)readOp->RespBuf, readCtxt->DataBuffer.FirstSize);
#endif
#endif

#ifdef OFFLOAD_ENGINE_RESPONSE_INCLUDES_REQUEST
                memcpy(readCtxt->DataBuffer.FirstAddr, rawMsg + FeLcore->ReqsForDPU[i].Offset, reqSize);
                readCtxt->DataBuffer.FirstAddr = readCtxt->DataBuffer.FirstAddr + reqSize;
#if OFFLOAD_ENGINE_ZERO_COPY == OFFLOAD_ENGINE_ZERO_COPY_NONE
                readOp->ReqSize = reqSize;
#endif
#endif
                readOp->StreamCtxt = (ContextT)FeStream;

#ifndef OPT_FILE_SERVICE_BATCHING
                SubmitDataPlaneRequest(FS, readCtxt, true, DDS_DPU_IO_SLOT_NUMBER_BASE + FeLcore->ReadOpsTailPointer);
#endif

                FeLcore->ReadOpsTailPointer++;
                if (FeLcore->ReadOpsTailPointer == DDS_MAX_OUTSTANDING_IO)
                {
                    FeLcore->ReadOpsTailPointer = 0;
                }
            }

#ifdef OPT_FILE_SERVICE_BATCHING
            //
            // Batched SubmitDataPlaneRequest() call here
            //
            //
            SubmitDataPlaneRequest(FS, FeLcore->ReadCtxts, firstIndex, FeLcore->NumReqsForDPU,
                                   DDS_DPU_IO_SLOT_NUMBER_BASE * (8 - Lcore));
#endif

            if (FeLcore->NumReqsForHost)
            {
                //
                // Partial offloading
                // Consolidate the requests for the host, trim the packet buffer, and send it to the host
                //
                //
                for (i = 0; i != FeLcore->NumReqsForHost; i++)
                {
                    RequestDescriptorT *reqForHost = &FeLcore->ReqsForHost[i];
                    memcpy(&rawMsg[offsetInMsg], &rawMsg[reqForHost->Offset], reqForHost->Bytes);
                    offsetInMsg += reqForHost->Bytes;
                }

                if (rte_pktmbuf_trim(curPkt, totalBytesOfDPUReqs))
                {
                    NETFE_TRACE("%s: failed to trim the packet buffer\n", __func__);
                }

                if (tle_tcp_stream_send(FeStream->HostStream->TleStream, &pkts[k], 1) != 1)
                {
                    NETFE_TRACE("%s: failed to forward the packet\n", __func__);
                    break;
                }
            }
            else
            {
                //
                // Full offloading
                // Just delete this packet
                //
                //
                rte_pktmbuf_free(curPkt);
            }
        }
        else
        {
            //
            // No offloading, so send the message to the host
            //
            //
            if (tle_tcp_stream_send(FeStream->HostStream->TleStream, &pkts[k], 1) != 1)
            {
                NETFE_TRACE("%s: failed to forward the packet\n", __func__);
                break;
            }
        } */

        // XXX: forward every packet to the host for now
        // TODO: maybe we can do a bit of batching here, will it be helpful?
        if (tle_tcp_stream_send(FeStream->HostStream->TleStream, &pkts[k], 1) != 1)
        {
            NETFE_TRACE("%s: failed to forward the packet\n", __func__);
            break;
        }
    }

    NETFE_TRACE("%s: tle_tcp_stream_send(%p, %u) returns %u\n", __func__, FeStream->TleStream, n, k);

#ifdef DDS_VERBOSE
    FeStream->Stat.TxPkts += k;
    FeStream->Stat.Drops += n - k;
#endif

    if (k == 0)
    {
        return 0;
    }

    //
    // Mark stream for reading if:
    // ECHO: Buffer full
    // RXTX: All outbound packets successfully dispatched
    // TODO: handle post-send processing based on PEPO needs
    //
    //
    if (n == RTE_DIM(FeStream->Pbuf.Pkt))
    {
        tle_event_active(FeStream->RxEv, TLE_SEV_UP);
        // tle_event_raise(FeStream->RxEv);
#ifdef DDS_VERBOSE
        FeStream->Stat.RxEv[TLE_SEV_UP]++;
#endif
    }

    //
    // Adjust pbuf array
    //
    //
    FeStream->Pbuf.Num = n - k;
    for (i = 0; i != n - k; i++)
    {
        pkts[i] = pkts[i + k];
    }

    if (FeStream->Pbuf.Num == 0)
    {
        tle_event_idle(FeStream->TxEv);
    }

    return k;
}

//
// Process TCP send and receive events for a host stream
//
//
static inline int NetfeHostRxTxProcess(__rte_unused uint32_t Lcore, struct NetfeFwdStream *FeStream)
{
    uint32_t i, k, n;
    struct rte_mbuf **pkts;

    n = FeStream->Pbuf.Num;
    pkts = FeStream->Pbuf.Pkt;

    if (n == 0)
    {
        tle_event_idle(FeStream->TxEv);
#ifdef DDS_VERBOSE
        FeStream->Stat.TxEv[TLE_SEV_IDLE]++;
#endif
        return 0;
    }

    //
    // Forward it to the DPU stream
    //
    //
    k = tle_tcp_stream_send(FeStream->DPUStream->TleStream, pkts, n);

    NETFE_TRACE("%s: tle_tcp_stream_send(%p, %u) returns %u\n", __func__, FeStream->TleStream, n, k);

#ifdef DDS_VERBOSE
    FeStream->Stat.TxPkts += k;
    FeStream->Stat.Drops += n - k;
#endif

    if (k == 0)
    {
        return 0;
    }

    //
    // Mark stream for reading if:
    // ECHO: Buffer full
    // RXTX: All outbound packets successfully dispatched
    // TODO: handle post-send processing based on PEPO needs
    //
    //
    if (n == RTE_DIM(FeStream->Pbuf.Pkt))
    {
        tle_event_active(FeStream->RxEv, TLE_SEV_UP);
#ifdef DDS_VERBOSE
        FeStream->Stat.RxEv[TLE_SEV_UP]++;
#endif
    }

    //
    // Adjust pbuf array
    //
    //
    FeStream->Pbuf.Num = n - k;
    for (i = 0; i != n - k; i++)
    {
        pkts[i] = pkts[i + k];
    }

    return k;
}

//
// PEPO main processing
//
//
static int PEPOLcoreMain(void *Arg)
{
    int32_t result;
    struct LcoreParam *param;
    uint32_t j, jj, n, k, lcore, processedEvents;

    //
    // Front-end state
    //
    //
    struct NetfeLcore *feLcore;
    struct NetfeStream *feStreams[MAX_PKT_BURST];
    struct NetfeFwdStream *feHostStreams[MAX_PKT_BURST];
    struct NetfeListenStream *feListenStreams[MAX_PKT_BURST];
    struct tle_stream *newTleStream[1];
    struct NetfeStream *newFeStream[1];
    struct NetfeFwdStream *newFeHostStream[1];
    struct tle_tcp_stream_cfg TleStreamCfg[1];

    //
    // Back-end state
    //
    //
    struct NetbeLcore *beLcore;
    struct rte_mbuf *bePkts[MAX_PKT_BURST];
    struct rte_mbuf *returnedPkts[MAX_PKT_BURST];
    int32_t errorCodes[MAX_PKT_BURST];
    struct rte_mbuf **memBufs;

    // get rid of offload
    /* #if OFFLOAD_ENGINE_ZERO_COPY == OFFLOAD_ENGINE_ZERO_COPY_DYNAMIC || \ OFFLOAD_ENGINE_ZERO_COPY ==
    OFFLOAD_ENGINE_ZERO_COPY_STATIC struct rte_mbuf *readRespBufs[OFFLOAD_ENGINE_BATCH_SIZE]; #else struct iovec
    iov[OFFLOAD_ENGINE_BATCH_SIZE]; struct rte_mempool *localMpool = Mpool[rte_lcore_to_socket_id(rte_lcore_id()) + 1];
        ssize_t bytesToSendBack = 0;
    #endif */
    // uint32_t completedOffloadedReqs = 0;

    param = (struct LcoreParam *)Arg;
    lcore = rte_lcore_id();
    result = 0;

    RTE_LOG(NOTICE, USER1, "%s (lcore = %u) running...\n", __func__, lcore);

    //
    // Init front-end for this lcore
    //
    //
    if (param->Fe.MaxStreams != 0)
    {
        result = NetfeLcoreInit(&param->Fe);
        if (result != 0)
        {
            RTE_LOG(ERR, USER1, "%s (lcore = %u) failed to init front end\n", __func__, lcore);
        }
        else
        {
            RTE_LOG(NOTICE, USER1, "%s (lcore = %u) front end initialized\n", __func__, lcore);
        }
    }

    //
    // Init back-end for this lcore
    //
    //
    if (result == 0 && param->Be.Lc != NULL)
    {
        result = NetbeLcoreSetup(param->Be.Lc);
        RTE_LOG(NOTICE, USER1, "%s (lcore = %u) back end initialized\n", __func__, lcore);
    }

    if (result != 0)
    {
        SigHandler(SIGQUIT);
    }

    feLcore = RTE_PER_LCORE(_fe);
    if (feLcore == NULL)
    {
        RTE_LOG(ERR, USER1, "%s (lcore = %u) failed to find front-end lcore obj\n", __func__, lcore);
        SigHandler(SIGQUIT);
    }

    beLcore = RTE_PER_LCORE(_be);
    if (beLcore == NULL)
    {
        RTE_LOG(ERR, USER1, "%s (lcore = %u) failed to find back-end lcore obj\n", __func__, lcore);
        SigHandler(SIGQUIT);
    }

    signal(SIGINT, SigHandler);
    signal(SIGTERM, SigHandler);
    
    // Load strings into memory
    size_t batch_size = 16 * 1024;
    size_t lengths_buf_size = 1024;  // 1024
    db_init(DUCKDB_DB_FILE_PATH);
    n_bufs = scan_table(DUCKDB_DB_FILE_PATH, batch_size, lengths_buf_size, &contig_strings);
    
    while (ForceQuitNetworkEngine == 0)
    {
        ProcessPacketMsg("regex", strlen("regex"));
        break;
//         //
//         // 1. Process TCP syn events
//         //
//         //
//         n = tle_evq_get(feLcore->ListenRxEq, (const void **)(uintptr_t)feListenStreams, RTE_DIM(feListenStreams));

//         if (n > 0)
//         {
//             NETFE_TRACE("\n\n%s (%u): tle_evq_get (ListenRxEq = %p) returns %u\n", __func__, lcore, feLcore->ListenRxEq,
//                         n);

//             for (j = 0; j != n; j++)
//             {
//                 //
//                 // Process a new TCP connection
//                 // a. Get the source port
//                 //
//                 uint16_t srcPort;
//                 struct tle_tcp_stream_addr addr;
//                 struct sockaddr_storage *rAddr;
//                 char strAddr[INET6_ADDRSTRLEN];

//                 k = tle_tcp_stream_accept(feListenStreams[j]->TleStream, newTleStream, 1);
//                 if (k == 0)
//                 {
//                     RTE_LOG(ERR, USER1, "%s (%u): failed to accept a new connection\n", __func__, lcore);
//                     tle_event_raise(feListenStreams[j]->ErEv);
// #ifdef DDS_VERBOSE
//                     feLcore->TcpStat.Rej++;
// #endif
//                     continue;
//                 }

//                 tle_tcp_stream_get_addr(newTleStream[0], &addr);
//                 rAddr = &addr.remote;
//                 FormatAddr(rAddr, strAddr, sizeof(strAddr));
//                 srcPort = ntohs(((const struct sockaddr_in *)rAddr)->sin_port);

//                 printf("Got a new connection from %s:%u\n", strAddr, srcPort);

//                 k = NetfeGetStreams(&feLcore->Free, newFeStream, 1);
//                 if (k == 0)
//                 {
//                     RTE_LOG(ERR, USER1, "%s (%u): failed to get a free DPU stream\n", __func__, lcore);
//                     tle_tcp_stream_close_bulk(&newTleStream[0], 1);
//                     tle_event_raise(feHostStreams[j]->ErEv);
// #ifdef DDS_VERBOSE
//                     feLcore->TcpStat.Rej++;
// #endif
//                     continue;
//                 }

//                 newFeStream[0]->TleStream = newTleStream[0];

//                 tle_event_active(newFeStream[0]->ErEv, TLE_SEV_DOWN);
//                 tle_event_active(newFeStream[0]->TxEv, TLE_SEV_UP);
//                 tle_event_active(newFeStream[0]->RxEv, TLE_SEV_DOWN);
// #ifdef DDS_VERBOSE
//                 newFeStream[0]->Stat.ErEv[TLE_SEV_DOWN]++;
//                 newFeStream[0]->Stat.TxEv[TLE_SEV_UP]++;
//                 newFeStream[0]->Stat.RxEv[TLE_SEV_DOWN]++;
// #endif
//                 memset(&TleStreamCfg[0], 0, sizeof(TleStreamCfg[0]));
//                 TleStreamCfg[0].recv_ev = newFeStream[0]->RxEv;
//                 TleStreamCfg[0].send_ev = newFeStream[0]->TxEv;
//                 TleStreamCfg[0].err_ev = newFeStream[0]->ErEv;
//                 tle_tcp_stream_update_cfg(newTleStream, TleStreamCfg, 1);

//                 NetfePutStream(feLcore, &feLcore->Use, newFeStream[0]);

//                 //
//                 // b. Get a free host stream and connect to the host
//                 //
//                 //
//                 newFeHostStream[0] = NetfeHostStreamOpen(feLcore, beLcore, srcPort);
//                 if (newFeHostStream[0] == NULL)
//                 {
//                     RTE_LOG(ERR, USER1, "%s (%u): failed to open a host stream\n", __func__, lcore);
//                     continue;
//                 }

//                 result = tle_tcp_stream_connect(newFeHostStream[0]->TleStream,
//                                                 (const struct sockaddr *)&newFeHostStream[0]->Raddr);

// #ifdef DDS_VERBOSE
//                 NETFE_TRACE("%s (lcore = %u, TCP host stream = %p): tle_tcp_stream_connect() "
//                             "returns %d\n",
//                             __func__, rte_lcore_id(), newFeHostStream[0], result);
// #endif

//                 if (result != 0)
//                 {
//                     NetfeHostStreamTerm(feLcore, newFeHostStream[0]);

//                     RTE_LOG(ERR, USER1, "%s (lcore = %u) failed to connect to host\n", __func__, lcore);
//                     continue;
//                 }

//                 newFeHostStream[0]->ListenStream = feListenStreams[j];
//                 newFeHostStream[0]->DPUStream = newFeStream[0];
//                 newFeHostStream[0]->IsConnected = 0;
//                 newFeStream[0]->HostStream = newFeHostStream[0];

//                 //
//                 // Keep checking if the connection is established
//                 //
//                 //
//                 tle_event_raise(newFeHostStream[0]->RxEv);
//             }
//         }

//         //
//         // 2. Process TCP Reset events
//         //
//         //
//         n = tle_evq_get(feLcore->ErEq, (const void **)(uintptr_t)feStreams, RTE_DIM(feStreams));

//         if (n > 0)
//         {
//             NETFE_TRACE("%s (%u): tle_evq_get (ErEq = %p) returns %u\n", __func__, lcore, feLcore->ErEq, n);

//             for (j = 0; j != n; j++)
//             {
//                 //
//                 // Process a reset event
//                 //
//                 //
// #ifdef DDS_VERBOSE
//                 struct tle_tcp_stream_addr addr;
//                 tle_tcp_stream_get_addr(feStreams[j]->TleStream, &addr);
//                 NetfeStreamDump(feStreams[j], DPU_STREAM, &addr.local, &addr.remote);
// #endif

//                 if (feStreams[j]->PostErr == 0 && (tle_event_state(feStreams[j]->RxEv) == TLE_SEV_UP ||
//                                                    tle_event_state(feStreams[j]->TxEv) == TLE_SEV_UP))
//                 {
//                     feStreams[j]->PostErr++;
//                 }
//                 else
//                 {
//                     newTleStream[0] = feStreams[j]->TleStream;

//                     tle_event_idle(feStreams[j]->RxEv);
//                     tle_event_idle(feStreams[j]->TxEv);
//                     tle_event_idle(feStreams[j]->ErEv);
//                     tle_tcp_stream_close_bulk(newTleStream, 1);

//                     //
//                     // Terminate the stream receiving the Reset event
//                     // As well as the forwarding stream
//                     //
//                     //
//                     if (feStreams[j]->HostStream != NULL && feStreams[j]->HostStream->TleStream != NULL)
//                     {
//                         feStreams[j]->HostStream->DPUStream = NULL;
//                         tle_event_raise(feStreams[j]->HostStream->ErEv);
//                     }

//                     NetfeRemStream(&feLcore->Use, feStreams[j]);
//                     NetfeStreamTerm(feLcore, feStreams[j]);
// #ifdef DDS_VERBOSE
//                     feLcore->TcpStat.Ter++;
// #endif
//                 }
//             }
//         }

//         //
//         // 3. Process TCP Receive events
//         //
//         //
//         n = tle_evq_get(feLcore->RxEq, (const void **)(uintptr_t)feStreams, RTE_DIM(feStreams));

//         if (n != 0)
//         {
//             NETFE_TRACE("%s (%u): tle_evq_get (RxEq = %p) returns %u\n", __func__, lcore, feLcore->RxEq, n);

//             processedEvents = 0;

//             for (j = 0; j != n; j++)
//             {
//                 uint32_t numPkts, numCurrentPkts, numAvailPkts;

//                 //
//                 // Wait until TCP splitting is done
//                 //
//                 //
//                 if (!feStreams[j]->HostStream->IsConnected)
//                 {
//                     tle_event_idle(feStreams[j]->RxEv);
// #ifdef DDS_VERBOSE
//                     feStreams[j]->Stat.RxEv[TLE_SEV_IDLE]++;
// #endif
//                     goto CheckTermination;
//                 }

//                 numCurrentPkts = feStreams[j]->Pbuf.Num;
//                 numAvailPkts = RTE_DIM(feStreams[j]->Pbuf.Pkt) - numCurrentPkts;

//                 //
//                 // No more space in the receive buffer
//                 //
//                 //
//                 if (numAvailPkts == 0)
//                 {
//                     tle_event_idle(feStreams[j]->RxEv);
// #ifdef DDS_VERBOSE
//                     feStreams[j]->Stat.RxEv[TLE_SEV_IDLE]++;
// #endif
//                     goto CheckTermination;
//                 }

//                 //
//                 // Receive packets from the TCP stream
//                 //
//                 //
//                 numPkts =
//                     tle_tcp_stream_recv(feStreams[j]->TleStream, &feStreams[j]->Pbuf.Pkt[numCurrentPkts], numAvailPkts);

//                 if (numPkts == 0)
//                 {
//                     goto CheckTermination;
//                 }

//                 NETFE_TRACE("%s (%u): tle_tcp_stream_recv(%p, %u) returns %u\n", __func__, lcore,
//                             feStreams[j]->TleStream, numAvailPkts, numPkts);
//                 NETFE_TRACE("Received %d packets with %d existing packets\n", numPkts, numCurrentPkts);

//                 //
//                 // Trigger the send event
//                 //
//                 //
//                 if (feStreams[j]->Pbuf.Num == 0)
//                 {
//                     //
//                     // If there was no packet, TxEv was idle
//                     //
//                     //
//                     tle_event_active(feStreams[j]->TxEv, TLE_SEV_UP);
//                 }
//                 else
//                 {
//                     //
//                     // Otherwise, TxEv was down
//                     //
//                     //
//                     tle_event_raise(feStreams[j]->TxEv);
//                 }

//                 feStreams[j]->Pbuf.Num += numPkts;
// #ifdef DDS_VERBOSE
//                 feStreams[j]->Stat.TxEv[TLE_SEV_UP]++;
//                 feStreams[j]->Stat.RxPkts += numPkts;
// #endif
//                 processedEvents += numPkts;

//             CheckTermination:
//                 //
//                 // Check if we need to terminate the stream
//                 //
//                 //
//                 if (processedEvents == 0 && feStreams[j]->PostErr != 0)
//                 {
//                     tle_event_raise(feStreams[j]->ErEv);
//                 }
//             }
//         }

//         //
//         // 4. Process TCP Send events
//         //
//         //
//         n = tle_evq_get(feLcore->TxEq, (const void **)(uintptr_t)feStreams, RTE_DIM(feStreams));

//         if (n != 0)
//         {
//             NETFE_TRACE("%s(%u): tle_evq_get (TxEq = %p) returns %u\n", __func__, lcore, feLcore->TxEq, n);

//             for (j = 0; j != n; j++)
//             {

//                 processedEvents = NetfeRxTxProcess(lcore, feStreams[j], feLcore);

//                 //
//                 // Check if we need to terminate the stream
//                 //
//                 //
//                 if (processedEvents == 0 && feStreams[j]->PostErr != 0)
//                 {
//                     tle_event_raise(feStreams[j]->ErEv);
//                 }
//             }
//         }

//         //
//         // 5. Process TCP Reset events from the host or timeout
//         //
//         //
//         n = tle_evq_get(feLcore->HostErEq, (const void **)(uintptr_t)feHostStreams, RTE_DIM(feHostStreams));

//         if (n > 0)
//         {
//             NETFE_TRACE("%s (%u): tle_evq_get (HostErEq = %p) returns %u\n", __func__, lcore, feLcore->HostErEq, n);

//             for (j = 0; j != n; j++)
//             {
//                 //
//                 // Process a reset event
//                 //
//                 //
// #ifdef DDS_VERBOSE
//                 struct tle_tcp_stream_addr addr;
//                 tle_tcp_stream_get_addr(feHostStreams[j]->TleStream, &addr);
//                 NetfeStreamDump(feHostStreams[j], HOST_STREAM, &addr.local, &addr.remote);
// #endif

//                 if (feHostStreams[j]->PostErr == 0 && (tle_event_state(feHostStreams[j]->RxEv) == TLE_SEV_UP ||
//                                                        tle_event_state(feHostStreams[j]->TxEv) == TLE_SEV_UP))
//                 {
//                     feHostStreams[j]->PostErr++;
//                 }
//                 else
//                 {
//                     newTleStream[0] = feHostStreams[j]->TleStream;

//                     tle_event_idle(feHostStreams[j]->RxEv);
//                     tle_event_idle(feHostStreams[j]->TxEv);
//                     tle_event_idle(feHostStreams[j]->ErEv);

//                     tle_tcp_stream_close_bulk(newTleStream, 1);

//                     //
//                     // Terminate the stream receiving the Reset event
//                     // As well as the forwarding stream
//                     //
//                     //
//                     if (feHostStreams[j]->DPUStream != NULL && feHostStreams[j]->DPUStream->TleStream != NULL)
//                     {
//                         feHostStreams[j]->DPUStream->HostStream = NULL;
//                         tle_event_raise(feHostStreams[j]->DPUStream->ErEv);
//                     }

//                     NetfeRemHostStream(&feLcore->HostUse, feHostStreams[j]);
//                     NetfeHostStreamTerm(feLcore, feHostStreams[j]);
// #ifdef DDS_VERBOSE
//                     feLcore->TcpStat.Ter++;
// #endif
//                 }
//             }
//         }

//         //
//         // 6. Process TCP Receive events from the host
//         //
//         //
//         n = tle_evq_get(feLcore->HostRxEq, (const void **)(uintptr_t)feHostStreams, RTE_DIM(feHostStreams));

//         if (n != 0)
//         {
//             NETFE_TRACE("%s (%u): tle_evq_get (HostRxEq = %p) returns %u\n", __func__, lcore, feLcore->HostRxEq, n);

//             for (j = 0; j != n; j++)
//             {
//                 uint32_t numPkts, numCurrentPkts, numAvailPkts, p;

//                 if (!feHostStreams[j]->IsConnected)
//                 {
//                     if (tle_tcp_stream_get_established(feHostStreams[j]->TleStream))
//                     {
//                         feHostStreams[j]->IsConnected = 1;
//                         tle_event_active(feHostStreams[j]->DPUStream->RxEv, TLE_SEV_UP);

//                         RTE_LOG(NOTICE, USER1, "%s (lcore = %u) TCP DPU stream created, belcore = %u\n", __func__,
//                                 lcore, beLcore->Id);
// #ifdef DDS_VERBOSE
//                         feLcore->TcpStat.Acc++;
// #endif
//                     }
//                     else
//                     {
//                         //
//                         // Keep polling until the connection is established or timeout
//                         // In case of timeout, Er event will be raised
//                         //
//                         //
//                         tle_event_raise(feHostStreams[j]->RxEv);
//                     }
//                 }

//                 //
//                 // Now, forward whatever is received from the host to the client
//                 //
//                 //
//                 processedEvents = 0;
//                 numCurrentPkts = feHostStreams[j]->Pbuf.Num;
//                 numAvailPkts = RTE_DIM(feHostStreams[j]->Pbuf.Pkt) - numCurrentPkts;

//                 if (numAvailPkts == 0)
//                 {
//                     tle_event_idle(feHostStreams[j]->RxEv);
// #ifdef DDS_VERBOSE
//                     feHostStreams[j]->Stat.RxEv[TLE_SEV_IDLE]++;
// #endif
//                     goto CheckTerminationForHost;
//                 }

//                 //
//                 // Put the received packets in send buffer
//                 //
//                 //
//                 numPkts = tle_tcp_stream_recv(feHostStreams[j]->TleStream, &feHostStreams[j]->Pbuf.Pkt[numCurrentPkts],
//                                               numAvailPkts);

//                 if (numPkts == 0)
//                 {
//                     goto CheckTerminationForHost;
//                 }

//                 NETFE_TRACE("%s (%u): tle_tcp_stream_recv(%p, %u) returns %u\n", __func__, lcore,
//                             feHostStreams[j]->TleStream, numAvailPkts, numPkts);
//                 NETFE_TRACE("Received %d packets from host with %d existing packets in the buffer\n", numPkts,
//                             numCurrentPkts);

//                 for (p = 0; p != numPkts; p++)
//                 {
//                     NETFE_TRACE("%s (%u): packet %d (%p) / %d has %u bytes\n", __func__, lcore, p,
//                                 feHostStreams[j]->Pbuf.Pkt[numCurrentPkts + p], numPkts,
//                                 feHostStreams[j]->Pbuf.Pkt[numCurrentPkts + p]->data_len);

//                     feHostStreams[j]->Pbuf.Num++;

//                     tle_event_active(feHostStreams[j]->TxEv, TLE_SEV_UP);
// #ifdef DDS_VERBOSE
//                     feHostStreams[j]->Stat.TxEv[TLE_SEV_UP]++;
//                     feHostStreams[j]->Stat.RxPkts++;
// #endif
//                     processedEvents++;
//                 }

//             CheckTerminationForHost:
//                 //
//                 // Check if we need to terminate the stream
//                 //
//                 //
//                 if (processedEvents == 0 && feHostStreams[j]->PostErr != 0)
//                 {
//                     tle_event_raise(feHostStreams[j]->ErEv);
//                 }
//             }
//         }

//         //
//         // 7. Process TCP Send events to the host
//         //
//         //
//         n = tle_evq_get(feLcore->HostTxEq, (const void **)(uintptr_t)feHostStreams, RTE_DIM(feHostStreams));

//         if (n != 0)
//         {
//             NETFE_TRACE("%s(%u): tle_evq_get (HostTxEq = %p) returns %u\n", __func__, lcore, feLcore->HostTxEq, n);

//             for (j = 0; j != n; j++)
//             {

//                 processedEvents = NetfeHostRxTxProcess(lcore, feHostStreams[j]);

//                 //
//                 // Check if we need to terminate the stream
//                 //
//                 //
//                 if (processedEvents == 0 && feHostStreams[j]->PostErr != 0)
//                 {
//                     tle_event_raise(feHostStreams[j]->ErEv);
//                 }
//             }
//         }

//         //
//         // 8. Process back end for TCP
//         //
//         //
//         for (j = 0; j != beLcore->NumPortQueues; j++)
//         {
//             //
//             // Back-end receive
//             //
//             //
//             n = rte_eth_rx_burst(beLcore->PortQueues[j].Port.Id, beLcore->PortQueues[j].RxQid, bePkts, RTE_DIM(bePkts));

//             if (n != 0)
//             {
// #ifdef DDS_VERBOSE
//                 beLcore->PortQueues[j].RxStat.In += n;
//                 for (int p = 0; p != n; p++)
//                 {
//                     printf("RTE receives a packet of %u bytes\n", bePkts[p]->pkt_len);
//                 }
// #endif

//                 NETFE_TRACE("%s (%u): rte_eth_rx_burst(%u, %u) returns %u\n", __func__, beLcore->Id,
//                             beLcore->PortQueues[j].Port.Id, beLcore->PortQueues[j].RxQid, n);

//                 k = tle_tcp_rx_bulk(beLcore->PortQueues[j].Dev, bePkts, returnedPkts, errorCodes, n);
// #ifdef DDS_VERBOSE
//                 beLcore->PortQueues[j].RxStat.Up += k;
//                 beLcore->PortQueues[j].RxStat.Drop += n - k;
// #endif

//                 NETFE_TRACE("%s (%u): tle_tcp_rx_bulk(%p, %u) returns %u\n", __func__, beLcore->Id,
//                             beLcore->PortQueues[j].Dev, n, k);

//                 for (jj = 0; jj != n - k; jj++)
//                 {
//                     NETFE_TRACE("%s: %d (port = %u) rp[%u] = {%p, %d}\n", __func__, __LINE__,
//                                 beLcore->PortQueues[j].Port.Id, j, returnedPkts[jj], errorCodes[jj]);
//                     rte_pktmbuf_free(returnedPkts[jj]);
//                 }
//             }

//             tle_tcp_process(beLcore->Ctx, TCP_MAX_PROCESS);

//             //
//             // Back-end send
//             //
//             //
//             n = beLcore->PortQueues[j].TxBuf.Num;
//             k = RTE_DIM(beLcore->PortQueues[j].TxBuf.Pkt) - n;
//             memBufs = beLcore->PortQueues[j].TxBuf.Pkt;

//             if (k >= RTE_DIM(beLcore->PortQueues[j].TxBuf.Pkt) / 2)
//             {
//                 jj = tle_tcp_tx_bulk(beLcore->PortQueues[j].Dev, memBufs + n, k);
//                 n += jj;
// #ifdef DDS_VERBOSE
//                 beLcore->PortQueues[j].TxStat.Down += jj;
// #endif
//             }

//             if (n == 0)
//             {
//                 continue;
//             }

//             NETFE_TRACE("%s (%u): tle_tcp_tx_bulk(%p) returns %u, "
//                         "total pkts to send: %u\n",
//                         __func__, beLcore->Id, beLcore->PortQueues[j].Dev, jj, n);

// #ifdef DDS_VERBOSE
//             for (jj = 0; jj != n; jj++)
//             {
//                 NETBE_PKT_DUMP(memBufs[jj]);
//             }
// #endif

//             k = rte_eth_tx_burst(beLcore->PortQueues[j].Port.Id, beLcore->PortQueues[j].TxQid, memBufs, n);

// #ifdef DDS_VERBOSE
//             beLcore->PortQueues[j].TxStat.Out += k;
//             beLcore->PortQueues[j].TxStat.Drop += n - k;
// #endif

//             NETFE_TRACE("%s (%u): rte_eth_tx_burst(%u, %u, %u) returns %u\n", __func__, beLcore->Id,
//                         beLcore->PortQueues[j].Port.Id, beLcore->PortQueues[j].TxQid, n, k);

//             beLcore->PortQueues[j].TxBuf.Num = n - k;
//             if (k != 0)
//             {
//                 for (jj = k; jj != n; jj++)
//                 {
//                     memBufs[jj - k] = memBufs[jj];
//                 }
//             }
//         }

//         //
//         // 9. Check offloaded requests
//         // get rid of offload
//         //
//         //
//         /* for (j = feLcore->ReadOpsHeadPointer; j != feLcore->ReadOpsTailPointer;)
//         {
//             if (feLcore->ReadOps[j].ReadResp.Result != DDS_ERROR_CODE_IO_PENDING)
//             {
//                 //
//                 // Return the message back to the client
//                 //
//                 //
//                 ReadOpDescriptorT *readOp = &feLcore->ReadOps[j];
//                 struct NetfeStream *tleStream = (struct NetfeStream *)readOp->StreamCtxt;

// #if OFFLOAD_ENGINE_ZERO_COPY == OFFLOAD_ENGINE_ZERO_COPY_DYNAMIC
//                 readRespBufs[completedOffloadedReqs] = (struct rte_mbuf *)readOp->RespBuf;
// #ifdef OFFLOAD_ENGINE_ALLOW_NONALIGNED_IO
//                 if (readOp->OffsetAlignment)
//                 {
//                     rte_pktmbuf_adj(readRespBufs[completedOffloadedReqs], readOp->OffsetAlignment);
//                 }

//                 if (readOp->BytesAlignment)
//                 {
//                     rte_pktmbuf_trim(readRespBufs[completedOffloadedReqs],
//                                      DDS_BACKEND_SECTOR_SIZE - readOp->BytesAlignment);
//                 }
// #endif

//                 completedOffloadedReqs++;
//                 if (completedOffloadedReqs == OFFLOAD_ENGINE_BATCH_SIZE)
//                 {
//                     uint16_t numPktsSent =
//                         tle_tcp_stream_send(tleStream->TleStream, readRespBufs, OFFLOAD_ENGINE_BATCH_SIZE);
//                     if (numPktsSent != OFFLOAD_ENGINE_BATCH_SIZE)
//                     {
//                         RTE_LOG(ERR, USER1,
//                                 "%s (lcore = %u) failed to send a batch of responses back to the client (%u)\n",
//                                 __func__, lcore, numPktsSent);
//                         SigHandler(SIGQUIT);
//                     }
//                     completedOffloadedReqs = 0;
//                 }
// #elif OFFLOAD_ENGINE_ZERO_COPY == OFFLOAD_ENGINE_ZERO_COPY_STATIC
//                 readRespBufs[completedOffloadedReqs] = (struct rte_mbuf *)readOp->RespBuf;
//                 uint16_t numBytesToTrim = MAX_READ_BUFFER_SIZE - readOp->ReadResp.BytesServiced;

//                 //
//                 // TODO: append numBytesToTrim back to each of the mbufs after a batch of responses is sent back
//                 //
//                 //
//                 rte_pktmbuf_trim(readRespBufs[completedOffloadedReqs], numBytesToTrim);

// #ifdef OFFLOAD_ENGINE_ALLOW_NONALIGNED_IO
//                 if (readOp->OffsetAlignment)
//                 {
//                     rte_pktmbuf_adj(readRespBufs[completedOffloadedReqs], readOp->OffsetAlignment);
//                 }

//                 if (readOp->BytesAlignment)
//                 {
//                     rte_pktmbuf_trim(readRespBufs[completedOffloadedReqs],
//                                      DDS_BACKEND_SECTOR_SIZE - readOp->BytesAlignment);
//                 }
// #endif

//                 completedOffloadedReqs++;
//                 if (completedOffloadedReqs == OFFLOAD_ENGINE_BATCH_SIZE)
//                 {
//                     uint16_t numPktsSent =
//                         tle_tcp_stream_send_no_free(tleStream->TleStream, readRespBufs, OFFLOAD_ENGINE_BATCH_SIZE);
//                     if (numPktsSent != OFFLOAD_ENGINE_BATCH_SIZE)
//                     {
//                         RTE_LOG(ERR, USER1,
//                                 "%s (lcore = %u) failed to send a batch of responses back to the client (%u)\n",
//                                 __func__, lcore, numPktsSent);
//                         SigHandler(SIGQUIT);
//                     }
//                     completedOffloadedReqs = 0;
//                 }
// #else
//                 DataPlaneRequestContext *readCtxt = &feLcore->ReadCtxts[j];
//                 FileIOSizeT actualReadSize = readOp->ReadResp.BytesServiced;

// #ifdef OFFLOAD_ENGINE_ALLOW_NONALIGNED_IO
//                 //
//                 // Handle unaligned I/O
//                 //
//                 //
//                 actualReadSize = readOp->BytesAlignment == 0
//                                      ? readOp->ReadReq.Bytes
//                                      : (readOp->ReadReq.Bytes - (DDS_BACKEND_SECTOR_SIZE - readOp->BytesAlignment));
//                 if (actualReadSize > readOp->ReadResp.BytesServiced)
//                 {
//                     actualReadSize = readOp->ReadResp.BytesServiced;
//                 }

//                 if (readOp->OffsetAlignment)
//                 {
//                     memmove(readCtxt->DataBuffer.FirstAddr, readCtxt->DataBuffer.FirstAddr + readOp->OffsetAlignment,
//                             actualReadSize);
//                 }
// #endif
//                 //
//                 // Reset the address of the buffer
//                 //
//                 //
//                 readCtxt->DataBuffer.FirstAddr = readOp->RespBuf;

//                 //
//                 // Prepare to send responses
//                 //
//                 //
//                 iov[completedOffloadedReqs].iov_base = readOp->RespBuf;
// #ifdef OFFLOAD_ENGINE_RESPONSE_INCLUDES_REQUEST
//                 iov[completedOffloadedReqs].iov_len = actualReadSize + readOp->ReqSize;
// #else
//                 iov[completedOffloadedReqs].iov_len = actualReadSize;
// #endif

//                 bytesToSendBack += iov[completedOffloadedReqs].iov_len;
//                 completedOffloadedReqs++;
//                 if (completedOffloadedReqs == OFFLOAD_ENGINE_BATCH_SIZE)
//                 {
//                     ssize_t sz =
//                         tle_tcp_stream_writev(tleStream->TleStream, localMpool, iov, OFFLOAD_ENGINE_BATCH_SIZE);

//                     if (sz < 0)
//                     {
//                         RTE_LOG(ERR, USER1,
//                                 "%s (lcore = %u) failed to send a batch of responses back to the client (%ld)\n",
//                                 __func__, lcore, sz);
//                         SigHandler(SIGQUIT);
//                     }

//                     bytesToSendBack = 0;
//                     completedOffloadedReqs = 0;
//                 }
// #endif
//             }
//             else
//             {
//                 break;
//             }

//             j++;
//             if (j == DDS_MAX_OUTSTANDING_IO)
//             {
//                 j = 0;
//             }
//         }

//         feLcore->ReadOpsHeadPointer = j; */
    }

    RTE_LOG(NOTICE, USER1, "%s (lcore = %u) finish\n", __func__, lcore);

    NetfeLcoreFiniTcp();
    NetbeLcoreClear();

    return result;
}

//
// Run the PEPO
// getting rid of the storage engine (which uses DPDK/SPDK)
//
//
int PEPOTLDKTCPRun(
    // void (*StorageEngine)(void*),
    // void* StorageEngineArg
)
{
    int32_t result;
    uint32_t i;

    RTE_LOG(NOTICE, USER1, "%s: Launching PEPO on all configured cores...\n", __func__);

    //
    // Launch all worker lcores
    // Network engine is run worker lcores only (7 of them on BF-2)
    //
    //
    RTE_LCORE_FOREACH_WORKER(i)
    {
#ifdef DDS_VERBOSE
        printf("Running worker %d\n", i);
#endif
        if (CoreParam[i].Be.Lc != NULL || CoreParam[i].Fe.MaxStreams != 0)
        {
            result = rte_eal_remote_launch(PEPOLcoreMain, &CoreParam[i], i);
            if (result != 0)
            {
                RTE_LOG(ERR, USER1, "rte_eal_remote_launch failed: err = %d, lcore = %u\n", result, i);
            }
        }
    }

    //
    // Launch the storage engine on the master lcore
    //
    //
#ifdef DDS_VERBOSE
    i = rte_get_main_lcore();
    printf("Master lcore = %d\n", i);
#endif

    // StorageEngine(StorageEngineArg);

    //
    // Wait for exit signal and sleep to avoid burning CPUs
    //
    //
    while (!ForceQuitNetworkEngine)
    {
        sleep(1);
    }

    //
    // Wait for all network lcores to exit
    //
    //
    fprintf(stdout, "Waiting for all network lcores to exit...\n");

    RTE_LCORE_FOREACH_WORKER(i)
    {
#ifdef DDS_VERBOSE
        printf("Waiting for worker %d\n", i);
#endif
        if (CoreParam[i].Be.Lc != NULL || CoreParam[i].Fe.MaxStreams != 0)
        {
            result = rte_eal_wait_lcore(i);
            if (result != 0)
            {
                RTE_LOG(ERR, USER1, "rte_eal_wait_lcore returns %d, lcore = %u\n", result, i);
            }
        }
    }
    fprintf(stdout, "All network lcores have exited\n");

    return 0;
}

//
// Clean all lcores for the front end
//
//
static void NetfeLcoreFini()
{
    //
    // Unload UDF libraries
    //
    //
    /* if (CoreParam[0].Fe.OffloadPredLib != NULL) {
        UnloadLibrary(CoreParam[0].Fe.OffloadPredLib);
        UnloadLibrary(CoreParam[0].Fe.OffloadFuncLib);
    } */

    RTE_LOG(NOTICE, USER1, "****** Lcore frontend has finished ******\n");
}

//
// Clean all lcores for the backend
//
//
static void NetbeLcoreFini()
{
    uint32_t i;

    for (i = 0; i != BeCfg.NumCores; i++)
    {
        tle_ctx_destroy(BeCfg.Cores[i].Ctx);
        rte_ip_frag_table_destroy(BeCfg.Cores[i].Ftbl);
        rte_lpm_free(BeCfg.Cores[i].Lpm4);

        rte_free(BeCfg.Cores[i].PortQueues);
        BeCfg.Cores[i].NumPortQueues = 0;
    }

    rte_free(BeCfg.Cores);
    BeCfg.NumCores = 0;

    rte_free(BeCfg.Ports);
    BeCfg.NumPorts = 0;
    RTE_LOG(NOTICE, USER1, "****** Lcore backend has finished ******\n");
}

//
// Destroy the PEPO
//
//
int PEPOTLDKTCPDestroy(void)
{
    uint32_t i, result;
    struct rte_eth_stats stats;

    for (i = 0; i != BeCfg.NumPorts; i++)
    {
        RTE_LOG(NOTICE, USER1, "%s: Stoping port %u\n", __func__, BeCfg.Ports[i].Id);
        rte_eth_stats_get(BeCfg.Ports[i].Id, &stats);
        RTE_LOG(NOTICE, USER1,
                "port %u stats = {\n"
                "ipackets = %" PRIu64 ", "
                "ibytes = %" PRIu64 ", "
                "ierrors = %" PRIu64 ", "
                "imissed = %" PRIu64 ", "
                "opackets = %" PRIu64 ", "
                "obytes = %" PRIu64 ", "
                "oerrors = %" PRIu64 "\n"
                "}\n",
                BeCfg.Ports[i].Id, stats.ipackets, stats.ibytes, stats.ierrors, stats.imissed, stats.opackets,
                stats.obytes, stats.oerrors);
        result = rte_eth_dev_stop(BeCfg.Ports[i].Id);
        if (result != 0)
        {
            RTE_LOG(ERR, USER1, "rte_eth_dev_stop failed: err = %d, port = %u\n", result, BeCfg.Ports[i].Id);
        }
        result = rte_eth_dev_close(BeCfg.Ports[i].Id);
        if (result != 0)
        {
            RTE_LOG(ERR, USER1, "rte_eth_dev_close failed: err = %d, port = %u\n", result, BeCfg.Ports[i].Id);
        }
    }

    NetfeLcoreFini();
    NetbeLcoreFini();

    RTE_LOG(NOTICE, USER1, "PEPO (TLDK TCP) has been stopped\n");

    return 0;
}
