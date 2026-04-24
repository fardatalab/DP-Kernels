#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "DDSTrafficDirecting.h"

extern char DPU_INTERFACE[16];    // = "p0";
extern char BOW_INTERFACE[16];    // = "en3f0pf0sf0"; //"en3f0pf0sf1";

//
// Apply the application signature
//
//
int ApplyAppSignatureRedirectPackets(
    struct DDSBOWAppSignature *Sig
) {
    char cmdProtocol[32];
    char cmdDstPort[32];
    char cmdSrcPort[32];

    if (strcmp(Sig->Protocol, "*") == 0) {
        strcpy(cmdProtocol, "");
    } else {
        sprintf(cmdProtocol, "ip_proto %s", Sig->Protocol);
    }

    if (strcmp(Sig->DestinationPort, "*") == 0) {
        strcpy(cmdDstPort, "");
    } else {
        sprintf(cmdDstPort, "dst_port %s", Sig->DestinationPort);
    }

    if (strcmp(Sig->SourcePort, "*") == 0) {
        strcpy(cmdSrcPort, "");
    } else {
        sprintf(cmdSrcPort, "src_port %s", Sig->SourcePort);
    }

    char cmd[1024];
    sprintf(cmd,
            "/usr/sbin/tc filter add dev %s ingress protocol ip pref 1 flower dst_ip %s src_ip %s %s %s %s action "
            "mirred egress redirect dev %s 2>&1",
            DPU_INTERFACE, Sig->DestinationIPv4, Sig->SourceIPv4, cmdProtocol, cmdDstPort, cmdSrcPort, BOW_INTERFACE);

    FILE *fp;
    char output[1024];

    fp = popen(cmd, "r");
    if (fp == NULL) {
        return -1;
    }

    while (fgets(output, sizeof(output), fp) != NULL) {
        if (strlen(output) > 0) {
            printf("Unexpected output as follows:\n");
            printf("%s\n", output);
            return -1;
        }
    }

    pclose(fp);

    return 0;
}

//
// Remove the application signature
//
//
int RemoveAppSignatureRedirectPackets(
    struct DDSBOWAppSignature *Sig
) {
    char cmdProtocol[32];
    char cmdDstPort[32];
    char cmdSrcPort[32];

    if (strcmp(Sig->Protocol, "*") == 0) {
        strcpy(cmdProtocol, "");
    } else {
        sprintf(cmdProtocol, "ip_proto %s", Sig->Protocol);
    }

    if (strcmp(Sig->DestinationPort, "*") == 0) {
        strcpy(cmdDstPort, "");
    } else {
        sprintf(cmdDstPort, "dst_port %s", Sig->DestinationPort);
    }

    if (strcmp(Sig->SourcePort, "*") == 0) {
        strcpy(cmdSrcPort, "");
    } else {
        sprintf(cmdSrcPort, "src_port %s", Sig->SourcePort);
    }

    char cmd[1024];
    sprintf(cmd,
            "/usr/sbin/tc filter del dev %s ingress protocol ip pref 1 flower dst_ip %s src_ip %s %s %s %s action "
            "mirred egress redirect dev %s 2>&1",
            DPU_INTERFACE, Sig->DestinationIPv4, Sig->SourceIPv4, cmdProtocol, cmdDstPort, cmdSrcPort, BOW_INTERFACE);

    FILE *fp;
    char output[1024];

    fp = popen(cmd, "r");
    if (fp == NULL) {
        return -1;
    }

    while (fgets(output, sizeof(output), fp) != NULL) {
        if (strlen(output) > 0) {
            printf("Unexpected output as follows:\n");
            printf("%s\n", output);
            return -1;
        }
    }

    pclose(fp);

    return 0;
}
