#include <rte_eal.h>
#include <rte_common.h>
#include <rte_malloc.h>
#include <rte_ether.h>
#include <rte_ethdev.h>
#include <rte_mempool.h>
#include <rte_mbuf.h>
#include <rte_net.h>
#include <rte_flow.h>

#include "AppEchoTCP.h"

#define DDSBOW_APP_PKT_L2(M) rte_pktmbuf_mtod(M, uint8_t *)
#define DDSBOW_APP_PKT_LEN(M) rte_pktmbuf_pkt_len(M)
#define L4_PROTO_TCP 6
#define START_SEQ_NUM 1603421721

static int MessageSize = 0;
static uint16_t IpPacketId = 0x8989;

struct MessageHeader {
    bool OffloadedToDPU;
    bool LastMessage;
    int MessageSize;
    char Timestamp[10];
};

//
// Echo TCP packets back to the sender
//
//
struct rte_mbuf*
EchoTCPDirect(
    struct rte_mbuf *mbuf
) {
    uint8_t *data = DDSBOW_APP_PKT_L2(mbuf);
    struct rte_ether_hdr *eth = NULL;
    struct rte_ipv4_hdr *ipHdr;
    struct rte_ether_addr ethSrc, ethDst;
    struct rte_mbuf* newPkt = NULL;

    int l3Off = 0;
    int l4Off = 0;
    int l7Off = 0;

    eth = (struct rte_ether_hdr *)data;
    
    switch (rte_be_to_cpu_16(eth->ether_type)) {
    case RTE_ETHER_TYPE_IPV4:
        l3Off = sizeof(struct rte_ether_hdr);
        break;
    default:
        goto free_pkt_and_return;
    }
    
    ipHdr = (struct rte_ipv4_hdr *)(data + l3Off);
    if ((ipHdr->version_ihl >> 4) != 4) {
        goto free_pkt_and_return;
    }
    if (ipHdr->src_addr == 0 || ipHdr->dst_addr == 0) {
        goto free_pkt_and_return;
    }

    l4Off = l3Off + rte_ipv4_hdr_len(ipHdr);
    
    switch (ipHdr->next_proto_id) {
    case L4_PROTO_TCP:
    {
        struct rte_tcp_hdr *tcpHdr = (struct rte_tcp_hdr *)(data + l4Off);
        struct rte_ether_hdr *newEthHdr;
        struct rte_ipv4_hdr *newIpHdr;
        struct rte_tcp_hdr *newTcpHdr;
        int ipPktSize = rte_cpu_to_be_16(ipHdr->total_length);
        int ipHdrSize = 0;
        int tcpHdrSize = 0;
        int payloadSize = 0;
        
        newPkt = rte_pktmbuf_alloc(mbuf->pool);
        if (newPkt == NULL) {
            rte_pktmbuf_free(mbuf);
            printf("Failed to allocate a new packet");
            break;
        }

        l7Off = l4Off + ((tcpHdr->data_off & 0xf0) >> 2);
        ipHdrSize = (int)ipHdr->ihl * 4;
        tcpHdrSize = l7Off - l4Off;
        payloadSize = ipPktSize - ipHdrSize - tcpHdrSize;
        
        //
        // Emulate a TCP server
        // First, check if this packet is a SYN packet
        //
        //
        if (tcpHdr->tcp_flags & RTE_TCP_SYN_FLAG) {
            //
            // Send a SYN-ACK packet
            //
            //
            newEthHdr = rte_pktmbuf_mtod(newPkt, struct rte_ether_hdr *);
            newIpHdr = (struct rte_ipv4_hdr *)(newEthHdr + 1);
            newTcpHdr = (struct rte_tcp_hdr *)(newIpHdr + 1);

            rte_memcpy(newEthHdr, eth, sizeof(struct rte_ether_hdr));
            rte_memcpy(newIpHdr, ipHdr, sizeof(struct rte_ipv4_hdr));
            rte_memcpy(newTcpHdr, tcpHdr, sizeof(struct rte_tcp_hdr));

            rte_ether_addr_copy(&eth->src_addr, &ethSrc);
            rte_ether_addr_copy(&eth->dst_addr, &ethDst);
            rte_ether_addr_copy(&ethDst, &newEthHdr->src_addr);
            rte_ether_addr_copy(&ethSrc, &newEthHdr->dst_addr);

            newIpHdr->src_addr = ipHdr->dst_addr;
            newIpHdr->dst_addr = ipHdr->src_addr;

            newIpHdr->packet_id = rte_cpu_to_be_16(IpPacketId);
            IpPacketId++;
            newIpHdr->hdr_checksum = 0;

            newTcpHdr->src_port = tcpHdr->dst_port;
            newTcpHdr->dst_port = tcpHdr->src_port;
            newTcpHdr->tcp_flags = RTE_TCP_SYN_FLAG | RTE_TCP_ACK_FLAG | RTE_TCP_ECE_FLAG;
            newTcpHdr->sent_seq = rte_cpu_to_be_32(START_SEQ_NUM);
            newTcpHdr->recv_ack = rte_cpu_to_be_32(rte_be_to_cpu_32(tcpHdr->sent_seq) + 1);

            //
            // Add TCP options
            //
            //
            // Op #1: MSS
            int bytesForOptions = 0;
            uint8_t *tcpOptions = (uint8_t *)(newTcpHdr + 1);
            *tcpOptions = 0x02;
            *(tcpOptions + 1) = 0x04;
            *(tcpOptions + 2) = 0x05;
            *(tcpOptions + 3) = 0x42;
            bytesForOptions += 4;

            // Op #2: no op
            *(tcpOptions + bytesForOptions) = 0x01;
            bytesForOptions += 1;

            // Op #3: Window scale
            *(tcpOptions + bytesForOptions) = 0x03;
            *(tcpOptions + bytesForOptions + 1) = 0x03;
            *(tcpOptions + bytesForOptions + 2) = 0x08;
            bytesForOptions += 3;

            // Op #4: no op
            *(tcpOptions + bytesForOptions) = 0x01;
            bytesForOptions += 1;
            
            // Op #5: no op
            *(tcpOptions + bytesForOptions) = 0x01;
            bytesForOptions += 1;
            
            // Op #6: SACK
            *(tcpOptions + bytesForOptions) = 0x04;
            *(tcpOptions + bytesForOptions + 1) = 0x02;
            bytesForOptions += 2;
            
            //
            // Set the TCP window size
            //
            //
            newTcpHdr->rx_win = rte_cpu_to_be_16(65535);

            //
            // Offload the checksum calculation to hardware
            //
            //
            newTcpHdr->cksum = 0;

            //
            // Note: checksum offloading for IP and TCP must be explicitly specified
            // when the TX Ethernet port is initialized.
            // Add the following line to the port_init() function in 
            // /opt/mellanox/doca/applications/common/src/dpdk_utils.c, e.g., line 422:
            // port_conf.txmode.offloads = DEV_TX_OFFLOAD_IPV4_CKSUM | DEV_TX_OFFLOAD_TCP_CKSUM;
            //
            //
            newPkt->ol_flags = RTE_MBUF_F_TX_IPV4 | RTE_MBUF_F_TX_IP_CKSUM | RTE_MBUF_F_TX_TCP_CKSUM;
            newPkt->l2_len = sizeof(newEthHdr);
            newPkt->l3_len = sizeof(newIpHdr);

            newPkt->pkt_len = sizeof(struct rte_ether_hdr) +
                sizeof(struct rte_ipv4_hdr) +
                sizeof(struct rte_tcp_hdr) +
                bytesForOptions;
            newPkt->data_len = newPkt->pkt_len;

            goto free_pkt_and_return;
        }

        //
        // Then, check if this packet is an ACK packet
        //
        //
        if (tcpHdr->tcp_flags & RTE_TCP_ACK_FLAG) {
            //
            // Check if this is the last ACK packet
            //
            //
            if (tcpHdr->tcp_flags & RTE_TCP_FIN_FLAG) {
                //
                // Send a FIN-ACK packet
                //
                //
                newEthHdr = rte_pktmbuf_mtod(newPkt, struct rte_ether_hdr *);
                newIpHdr = (struct rte_ipv4_hdr *)(newEthHdr + 1);
                newTcpHdr = (struct rte_tcp_hdr *)(newIpHdr + 1);

                rte_memcpy(newEthHdr, eth, sizeof(struct rte_ether_hdr));
                rte_memcpy(newIpHdr, ipHdr, sizeof(struct rte_ipv4_hdr));
                rte_memcpy(newTcpHdr, tcpHdr, sizeof(struct rte_tcp_hdr));

                newIpHdr->src_addr = ipHdr->dst_addr;
                newIpHdr->dst_addr = ipHdr->src_addr;
                newTcpHdr->src_port = tcpHdr->dst_port;
                newTcpHdr->dst_port = tcpHdr->src_port;
                newTcpHdr->tcp_flags = RTE_TCP_ACK_FLAG;
                newTcpHdr->sent_seq = rte_cpu_to_be_32(
                    rte_be_to_cpu_32(tcpHdr->recv_ack));
                newTcpHdr->recv_ack = rte_cpu_to_be_32(
                    rte_be_to_cpu_32(tcpHdr->sent_seq) + 1);

                rte_ether_addr_copy(&eth->src_addr, &ethSrc);
                rte_ether_addr_copy(&eth->dst_addr, &ethDst);
                rte_ether_addr_copy(&ethDst, &newEthHdr->src_addr);
                rte_ether_addr_copy(&ethSrc, &newEthHdr->dst_addr);

                newPkt->pkt_len = sizeof(struct rte_ether_hdr) +
                    sizeof(struct rte_ipv4_hdr) +
                    sizeof(struct rte_tcp_hdr);
                newPkt->data_len = newPkt->pkt_len;

                goto free_pkt_and_return;
            }

            if (payloadSize == 0) {
                //
                // This is an ACK packet without any data and
                // thus can be discarded
                //
                //
                rte_pktmbuf_free(newPkt);
                newPkt = NULL;

                goto free_pkt_and_return;
            }
        }

        //
        // Check if this is the first message
        //
        //
        if (payloadSize == sizeof(int)) {
            MessageSize = *(int *)(data + l7Off);

            //
            // Send an ACK packet
            //
            //
            newEthHdr = rte_pktmbuf_mtod(newPkt, struct rte_ether_hdr *);
            newIpHdr = (struct rte_ipv4_hdr *)(newEthHdr + 1);
            newTcpHdr = (struct rte_tcp_hdr *)(newIpHdr + 1);

            rte_memcpy(newEthHdr, eth, sizeof(struct rte_ether_hdr));
            rte_memcpy(newIpHdr, ipHdr, sizeof(struct rte_ipv4_hdr));
            rte_memcpy(newTcpHdr, tcpHdr, sizeof(struct rte_tcp_hdr));

            rte_ether_addr_copy(&eth->src_addr, &ethSrc);
            rte_ether_addr_copy(&eth->dst_addr, &ethDst);
            rte_ether_addr_copy(&ethDst, &newEthHdr->src_addr);
            rte_ether_addr_copy(&ethSrc, &newEthHdr->dst_addr);

            newIpHdr->src_addr = ipHdr->dst_addr;
            newIpHdr->dst_addr = ipHdr->src_addr;

            newIpHdr->packet_id = rte_cpu_to_be_16(IpPacketId);
            IpPacketId++;
            newIpHdr->hdr_checksum = 0;
            newIpHdr->total_length = rte_cpu_to_be_16(sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_tcp_hdr));
            
            newTcpHdr->src_port = tcpHdr->dst_port;
            newTcpHdr->dst_port = tcpHdr->src_port;
            newTcpHdr->tcp_flags = RTE_TCP_ACK_FLAG;
            newTcpHdr->sent_seq = tcpHdr->recv_ack;
            newTcpHdr->recv_ack = rte_cpu_to_be_32(rte_be_to_cpu_32(tcpHdr->sent_seq) + payloadSize);
            
            //
            // Set the TCP window size
            //
            //
            newTcpHdr->rx_win = rte_cpu_to_be_16(49155);

            //
            // Offload the checksum calculation to hardware
            //
            //
            newTcpHdr->cksum = 0;

            //
            // Note: checksum offloading for IP and TCP must be explicitly specified
            // when the TX Ethernet port is initialized.
            // Add the following line to the port_init() function in 
            // /opt/mellanox/doca/applications/common/src/dpdk_utils.c, e.g., line 422:
            // port_conf.txmode.offloads = DEV_TX_OFFLOAD_IPV4_CKSUM | DEV_TX_OFFLOAD_TCP_CKSUM;
            //
            //
            newPkt->ol_flags = RTE_MBUF_F_TX_IPV4 | RTE_MBUF_F_TX_IP_CKSUM | RTE_MBUF_F_TX_TCP_CKSUM;
            newPkt->l2_len = sizeof(newEthHdr);
            newPkt->l3_len = sizeof(newIpHdr);

            newPkt->pkt_len = sizeof(struct rte_ether_hdr) +
                sizeof(struct rte_ipv4_hdr) +
                sizeof(struct rte_tcp_hdr);
            newPkt->data_len = newPkt->pkt_len;

            goto free_pkt_and_return;
        }

        //
        // Check if this is a regular message
        //
        //
        if (payloadSize == MessageSize) {
            //
            // Check if this is the last message
            //
            //
            struct MessageHeader* header = (struct MessageHeader*)(data + l7Off);
            if (header->LastMessage) {
                //
                // Send back a FIN-ACK packet
                //
                //
                newEthHdr = rte_pktmbuf_mtod(newPkt, struct rte_ether_hdr *);
                newIpHdr = (struct rte_ipv4_hdr *)(newEthHdr + 1);
                newTcpHdr = (struct rte_tcp_hdr *)(newIpHdr + 1);

                rte_memcpy(newEthHdr, eth, sizeof(struct rte_ether_hdr));
                rte_memcpy(newIpHdr, ipHdr, sizeof(struct rte_ipv4_hdr));
                rte_memcpy(newTcpHdr, tcpHdr, sizeof(struct rte_tcp_hdr));

                rte_ether_addr_copy(&eth->src_addr, &ethSrc);
                rte_ether_addr_copy(&eth->dst_addr, &ethDst);
                rte_ether_addr_copy(&ethDst, &newEthHdr->src_addr);
                rte_ether_addr_copy(&ethSrc, &newEthHdr->dst_addr);

                newIpHdr->src_addr = ipHdr->dst_addr;
                newIpHdr->dst_addr = ipHdr->src_addr;

                newIpHdr->packet_id = rte_cpu_to_be_16(IpPacketId);
                IpPacketId++;
                newIpHdr->hdr_checksum = 0;
                newIpHdr->total_length = rte_cpu_to_be_16(sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_tcp_hdr));

                newTcpHdr->src_port = tcpHdr->dst_port;
                newTcpHdr->dst_port = tcpHdr->src_port;
                newTcpHdr->tcp_flags = RTE_TCP_ACK_FLAG | RTE_TCP_FIN_FLAG;
                newTcpHdr->sent_seq = tcpHdr->recv_ack;
                newTcpHdr->recv_ack = rte_cpu_to_be_32(rte_be_to_cpu_32(tcpHdr->sent_seq) + payloadSize);

                //
                // Set the TCP window size
                //
                //
                newTcpHdr->rx_win = rte_cpu_to_be_16(49152);

                //
                // Offload the checksum calculation to hardware
                //
                //
                newTcpHdr->cksum = 0;

                //
                // Note: checksum offloading for IP and TCP must be explicitly specified
                // when the TX Ethernet port is initialized.
                // Add the following line to the port_init() function in 
                // /opt/mellanox/doca/applications/common/src/dpdk_utils.c, e.g., line 422:
                // port_conf.txmode.offloads = DEV_TX_OFFLOAD_IPV4_CKSUM | DEV_TX_OFFLOAD_TCP_CKSUM;
                //
                //
                newPkt->ol_flags = RTE_MBUF_F_TX_IPV4 | RTE_MBUF_F_TX_IP_CKSUM | RTE_MBUF_F_TX_TCP_CKSUM;
                newPkt->l2_len = sizeof(newEthHdr);
                newPkt->l3_len = sizeof(newIpHdr);

                newPkt->pkt_len = sizeof(struct rte_ether_hdr) +
                    sizeof(struct rte_ipv4_hdr) +
                    sizeof(struct rte_tcp_hdr);
                newPkt->data_len = newPkt->pkt_len;

                goto free_pkt_and_return;
            }

            //
            // Copy the payload and send an ACK + payload packet
            //
            //
            // printf("This is a regular app message\n");
            newEthHdr = rte_pktmbuf_mtod(newPkt, struct rte_ether_hdr *);
            newIpHdr = (struct rte_ipv4_hdr *)(newEthHdr + 1);
            newTcpHdr = (struct rte_tcp_hdr *)(newIpHdr + 1);

            rte_memcpy(newEthHdr, eth, sizeof(struct rte_ether_hdr));
            rte_memcpy(newIpHdr, ipHdr, sizeof(struct rte_ipv4_hdr));
            rte_memcpy(newTcpHdr, tcpHdr, sizeof(struct rte_tcp_hdr));

            rte_ether_addr_copy(&eth->src_addr, &ethSrc);
            rte_ether_addr_copy(&eth->dst_addr, &ethDst);
            rte_ether_addr_copy(&ethDst, &newEthHdr->src_addr);
            rte_ether_addr_copy(&ethSrc, &newEthHdr->dst_addr);

            newIpHdr->src_addr = ipHdr->dst_addr;
            newIpHdr->dst_addr = ipHdr->src_addr;
            
            newIpHdr->packet_id = rte_cpu_to_be_16(IpPacketId);
            IpPacketId++;
            newIpHdr->hdr_checksum = 0;
            newIpHdr->total_length = rte_cpu_to_be_16(sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_tcp_hdr) + MessageSize);

            newTcpHdr->src_port = tcpHdr->dst_port;
            newTcpHdr->dst_port = tcpHdr->src_port;
            newTcpHdr->tcp_flags = RTE_TCP_ACK_FLAG | RTE_TCP_PSH_FLAG;
            newTcpHdr->sent_seq = tcpHdr->recv_ack;
            newTcpHdr->recv_ack = rte_cpu_to_be_32(rte_be_to_cpu_32(tcpHdr->sent_seq) + payloadSize);

            //
            // Copy the payload
            //
            //
            rte_memcpy(DDSBOW_APP_PKT_L2(newPkt) + sizeof(struct rte_ether_hdr) +
                sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_tcp_hdr),
                data + l7Off, MessageSize);
            
            //
            // Set the TCP window size
            //
            //
            newTcpHdr->rx_win = rte_cpu_to_be_16(49155);

            //
            // Offload the checksum calculation to hardware
            //
            //
            newTcpHdr->cksum = 0;

            //
            // Note: checksum offloading for IP and TCP must be explicitly specified
            // when the TX Ethernet port is initialized.
            // Add the following line to the port_init() function in 
            // /opt/mellanox/doca/applications/common/src/dpdk_utils.c, e.g., line 422:
            // port_conf.txmode.offloads = DEV_TX_OFFLOAD_IPV4_CKSUM | DEV_TX_OFFLOAD_TCP_CKSUM;
            //
            //
            newPkt->ol_flags = RTE_MBUF_F_TX_IPV4 | RTE_MBUF_F_TX_IP_CKSUM | RTE_MBUF_F_TX_TCP_CKSUM;
            newPkt->l2_len = sizeof(newEthHdr);
            newPkt->l3_len = sizeof(newIpHdr);

            newPkt->pkt_len = sizeof(struct rte_ether_hdr) +
                sizeof(struct rte_ipv4_hdr) +
                sizeof(struct rte_tcp_hdr) + MessageSize;
            newPkt->data_len = newPkt->pkt_len;

            goto free_pkt_and_return;
        }

        //
        // Otherwise, this is an unrecognized message
        //
        //
        printf("Unrecognized message!\n");
        rte_pktmbuf_free(newPkt);
        newPkt = NULL;

        break;
    }
    default:
        break;
    }

free_pkt_and_return:
    rte_pktmbuf_free(mbuf);
    return newPkt;
}
