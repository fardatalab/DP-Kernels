#ifndef DDS_BOW_APP_SIGNATURE_H_
#define DDS_BOW_APP_SIGNATURE_H_

extern char DPU_INTERFACE[16];     // = "p0";
extern char BOW_INTERFACE[16];     // = "en3f0pf0sf0"; //"en3f0pf0sf1";
extern char HOST_INTERFACE[16];    // = "pf0hpf";
extern char BOW_L2_ADDRESS[18];    // = "02:86:36:58:e9:81"; //"00:00:00:00:01:00";

//
// We use IP 5-tuple to identify an application
// Example 1: "192.168.200.133/32:65535 192.168.200.132/32:65535 TCP"
// Example 2: "0.0.0.0/0:* 0.0.0.0/0:* *"
//
//
struct DDSBOWAppSignature {
    char SourceIPv4[19];
    char DestinationIPv4[19];
    char SourcePort[6];
    char DestinationPort[6];
    char Protocol[5];
};

//
// We push forwarding rules to the hardware
// Example: "192.168.200.133/32 b8:3f:d2:04:e2:54;192.168.300.133/32 de:ad:be:ef:00:00"
//
//
struct DDSBOWFwdRule {
    char RemoteIPv4[19];
    char RemoteMAC[18];
};

struct DDSBOWFwdRuleList {
    struct DDSBOWFwdRule* Rules;
    unsigned int NumRules;
};

//
// Parse an application signature from a string
//
//
int
ParseAppSignature(
    char* Str,
    struct DDSBOWAppSignature* Sig
);

//
// Return the application signature as a string
//
//
void
GetAppSignatureStr(
    struct DDSBOWAppSignature* Sig,
    char* Str
);

//
// Apply the application signature to redirect packets
//
//
int ApplyAppSignatureRedirectPackets(
    struct DDSBOWAppSignature* Sig
);

//
// Remove the application signature
//
//
int RemoveAppSignatureRedirectPackets(
    struct DDSBOWAppSignature* Sig
);

//
// Parse fwd rules from a string
//
//
int
ParseFwdRules(
    char* Str,
    struct DDSBOWFwdRuleList* RuleList
);

//
// Release rules
//
//
int
ReleaseFwdRules(
    struct DDSBOWFwdRuleList* RuleList
);

//
// Apply directing rules to modify headers
//
//
int ApplyDirectingRules(
    struct DDSBOWAppSignature* Sig,
    struct DDSBOWFwdRuleList* RuleList,
    char* HostMac,
    char* DPUIP
);

//
// Remove applied directing rules
//
//
int RemoveDirectingRules(
    struct DDSBOWAppSignature* Sig,
    struct DDSBOWFwdRuleList* RuleList,
    char* HostMac,
    char* DPUIP
);

#endif /* DDS_BOW_APP_SIGNATURE_H_ */
