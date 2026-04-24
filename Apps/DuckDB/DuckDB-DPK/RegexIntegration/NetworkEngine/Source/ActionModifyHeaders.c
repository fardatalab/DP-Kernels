#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "DDSTrafficDirecting.h"

char DPU_INTERFACE[16];     // = "p0";
char BOW_INTERFACE[16];     // = "en3f0pf0sf0"; //"en3f0pf0sf1";
char HOST_INTERFACE[16];    // = "pf0hpf";
char BOW_L2_ADDRESS[18];    // = "02:86:36:58:e9:81"; //"00:00:00:00:01:00";

char HostL2Address[18];
char HostL3Address[16];
char HostL4Port[6];

//
// Apply the application signature
//
//
int ApplyDirectingRules(
    struct DDSBOWAppSignature *Sig,
    struct DDSBOWFwdRuleList* RuleList,
    char* HostMac,
    char* DPUIP
) {
    char cmdProtocol[32];
    char cmdDstPort[32];
    char cmdSrcPort[32];
    char cmd[1024];
    FILE *fp;
    char output[1024];
    unsigned int r, appliedRules;

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

    //
    // Modify the headers for ingress packets
    //
    //
    sprintf(cmd, "/usr/sbin/tc filter add dev %s ingress protocol ip pref 1 flower skip_sw \
        dst_ip %s src_ip %s %s %s %s \
        action pedit ex \
        munge eth dst set %s \
        munge ip dst set %s \
        action mirred egress redirect dev %s 2>&1",
            DPU_INTERFACE, Sig->DestinationIPv4, Sig->SourceIPv4, cmdProtocol, cmdDstPort, cmdSrcPort, BOW_L2_ADDRESS,
            DPUIP, BOW_INTERFACE);

    printf("Traffic directing rule #1: %s\n", cmd);

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

    memcpy(HostL2Address, HostMac, 17); 
    HostL2Address[17]='\0';
    memcpy(HostL3Address, Sig->DestinationIPv4, 15);
    HostL3Address[15]='\0';
    memcpy(HostL4Port, Sig->DestinationPort, 5);
    HostL4Port[5]='\0';

    //
    // Modify the headers for egress packets to the host
    //
    //
    sprintf(cmd, "/usr/sbin/tc filter add dev %s ingress protocol ip pref 1 flower skip_sw \
        src_ip %s dst_ip %s \
        action pedit ex \
        munge eth src set %s \
        munge eth dst set %s \
        action mirred egress redirect dev %s 2>&1",
            BOW_INTERFACE, DPUIP, HostL3Address, BOW_L2_ADDRESS, HostL2Address, HOST_INTERFACE);

    printf("Traffic directing rule #2: %s\n", cmd);

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
        printf("Traffic directing rule #2 is applied\n");
    }

    pclose(fp);
    
    //
    // Modify the headers for packets from the host to the DPU
    //
    //
    sprintf(cmd, "/usr/sbin/tc filter add dev %s ingress protocol ip pref 1 flower skip_sw \
        src_ip %s dst_ip %s \
        action mirred egress redirect dev %s 2>&1",
            HOST_INTERFACE, HostL3Address, DPUIP, BOW_INTERFACE);

    printf("Traffic directing rule #3: %s\n", cmd);

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
        printf("Traffic directing rule #3 is applied\n");
    }

    pclose(fp);

    //
    // Modify the headers for egress packets to clients
    //
    //
    if (RuleList->NumRules == 0) {
        sprintf(cmd, "/usr/sbin/tc filter add dev %s ingress protocol ip pref 2 flower skip_sw \
            src_ip %s %s \
            action pedit ex \
            munge eth src set %s \
            munge eth dst set ff:ff:ff:ff:ff:ff \
            munge ip src set %s \
            action mirred egress redirect dev %s 2>&1",
                BOW_INTERFACE, DPUIP, cmdProtocol, HostL2Address, HostL3Address, DPU_INTERFACE);

        printf("Traffic directing rule #4: %s\n", cmd);

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
            printf("Traffic directing rule #4 is applied\n");
        }

        pclose(fp);

	return 0;
    } else {
        for (r = 0, appliedRules = 0; r != RuleList->NumRules; r++, appliedRules++) {
            sprintf(cmd, "/usr/sbin/tc filter add dev %s ingress protocol ip pref 2 flower skip_sw \
                src_ip %s dst_ip %s %s \
                action pedit ex \
                munge eth src set %s \
                munge eth dst set %s \
                munge ip src set %s \
                action mirred egress redirect dev %s 2>&1",
                    BOW_INTERFACE, DPUIP, RuleList->Rules[r].RemoteIPv4, cmdProtocol, HostL2Address,
                    RuleList->Rules[r].RemoteMAC, HostL3Address, DPU_INTERFACE);

            printf("Traffic directing rule #%d: %s\n", r + 4, cmd);

            fp = popen(cmd, "r");
            if (fp == NULL) {
                goto CleanUp;
            }

            while (fgets(output, sizeof(output), fp) != NULL) {
                if (strlen(output) > 0) {
                    printf("Unexpected output as follows:\n");
                    printf("%s\n", output);
                    goto CleanUp;
                }
                printf("Traffic directing rule #%d is applied\n", r + 4);
            }

            pclose(fp);
        }

        return 0;

CleanUp:
        for (r = 0; r != appliedRules; r++) {
            //
            // delete the rule
            //
            //
            sprintf(cmd, "/usr/sbin/tc filter del dev %s ingress protocol ip pref 1 flower skip_sw \
                src_ip %s dst_ip %s %s \
                action pedit ex \
                munge eth src set %s \
                munge eth dst set %s \
                munge ip src set %s \
                action mirred egress redirect dev %s 2>&1",
                    BOW_INTERFACE, DPUIP, RuleList->Rules[r].RemoteIPv4, cmdProtocol, HostL2Address,
                    RuleList->Rules[r].RemoteMAC, HostL3Address, DPU_INTERFACE);

            fp = popen(cmd, "r");
            if (fp != NULL) {
                while (fgets(output, sizeof(output), fp) != NULL) {
                    if (strlen(output) > 0) {
                        printf("Unexpected output when deleting a rule as follows:\n");
                        printf("%s\n", output);
                    }
                }
                printf("Traffic directing rule #%d is deleted\n", r + 3);
                pclose(fp);
            }
        }

        return -1;
    }

    return 0;
}

//
// Remove the application signature
//
//
int RemoveDirectingRules(
    struct DDSBOWAppSignature *Sig,
    struct DDSBOWFwdRuleList* RuleList,
    char* HostMac,
    char* DPUIP
) {
    char cmdProtocol[32];
    char cmdDstPort[32];
    char cmdSrcPort[32];
    char cmd[1024];
    FILE *fp;
    char output[1024];
    unsigned int r;

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

    sprintf(cmd, "/usr/sbin/tc filter del dev %s ingress protocol ip pref 1 flower skip_sw \
        dst_ip %s src_ip %s %s %s %s \
        action pedit ex \
        munge eth dst set %s \
        munge ip dst set %s \
        action mirred egress redirect dev %s 2>&1",
            DPU_INTERFACE, Sig->DestinationIPv4, Sig->SourceIPv4, cmdProtocol, cmdDstPort, cmdSrcPort, BOW_L2_ADDRESS,
            DPUIP, BOW_INTERFACE);

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

    memcpy(HostL2Address, HostMac, 17); 
    HostL2Address[17]='\0';
    memcpy(HostL3Address, Sig->DestinationIPv4, 15);
    HostL3Address[15]='\0';
    memcpy(HostL4Port, Sig->DestinationPort, 5);
    HostL4Port[5]='\0';

    //
    // Modify the headers for egress packets to the host
    //
    //
    sprintf(cmd, "/usr/sbin/tc filter del dev %s ingress protocol ip pref 1 flower skip_sw \
        src_ip %s dst_ip %s \
        action pedit ex \
        munge eth src set %s \
        munge eth dst set %s \
        action mirred egress redirect dev %s 2>&1",
            BOW_INTERFACE, DPUIP, HostL3Address, BOW_L2_ADDRESS, HostL2Address, HOST_INTERFACE);

    fp = popen(cmd, "r");
    if (fp == NULL) {
        return -1;
    }

    while (fgets(output, sizeof(output), fp) != NULL) {
        if (strlen(output) > 0) {
            printf("Unexpected output 1 as follows:\n");
            printf("%s\n", output);
            return -1;
        }
    }

    pclose(fp);
    
    //
    // Modify the headers for packets from the host to the DPU
    //
    //
    sprintf(cmd, "/usr/sbin/tc filter del dev %s ingress protocol ip pref 1 flower skip_sw \
        src_ip %s dst_ip %s \
        action mirred egress redirect dev %s 2>&1",
            HOST_INTERFACE, HostL3Address, DPUIP, BOW_INTERFACE);

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

    //
    // Modify the headers for egress packets to clients
    //
    //
    if (RuleList->NumRules == 0) {
        sprintf(cmd, "/usr/sbin/tc filter del dev %s ingress protocol ip pref 2 flower skip_sw \
            src_ip %s %s \
            action pedit ex \
            munge eth src set %s \
            munge eth dst set ff:ff:ff:ff:ff:ff \
            munge ip src set %s \
            action mirred egress redirect dev %s 2>&1",
                BOW_INTERFACE, DPUIP, cmdProtocol, HostL2Address, HostL3Address, DPU_INTERFACE);

        fp = popen(cmd, "r");
        if (fp == NULL) {
            return 0;
        }

        while (fgets(output, sizeof(output), fp) != NULL) {
            if (strlen(output) > 0) {
                printf("Unexpected output as follows:\n");
                printf("%s\n", output);
            }
        }

        pclose(fp);
    }
    else  {
        for (r = 0; r != RuleList->NumRules; r++) {
            sprintf(cmd, "/usr/sbin/tc filter del dev %s ingress protocol ip pref 2 flower skip_sw \
                src_ip %s dst_ip %s %s \
                action pedit ex \
                munge eth src set %s \
                munge eth dst set %s \
                munge ip src set %s \
                action mirred egress redirect dev %s 2>&1",
                    BOW_INTERFACE, DPUIP, RuleList->Rules[r].RemoteIPv4, cmdProtocol, HostL2Address,
                    RuleList->Rules[r].RemoteMAC, HostL3Address, DPU_INTERFACE);

            fp = popen(cmd, "r");
            if (fp == NULL) {
                continue;
            }

            while (fgets(output, sizeof(output), fp) != NULL) {
                if (strlen(output) > 0) {
                    printf("Unexpected output as follows:\n");
                    printf("%s\n", output);
                }
            }

            pclose(fp);
        }
    }

    return 0;
}
