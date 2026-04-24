#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include "DDSTrafficDirecting.h"

//
// Split a string into an array of strings
//
//
char** StrSplit(char* Str, const char Delim) {
    char** result    = 0;
    size_t count     = 0;
    char* tmp        = Str;
    char* lastIndex = 0;
    char delim[2];
    delim[0] = Delim;
    delim[1] = 0;

    while (*tmp) {
        if (Delim == *tmp)
        {
            count++;
            lastIndex = tmp;
        }
        tmp++;
    }

    count += lastIndex < (Str + strlen(Str) - 1);

    count++;

    result = (char**)malloc(sizeof(char*) * count);

    if (result) {
        size_t idx  = 0;
        char* token = strtok(Str, delim);

        while (token) {
            *(result + idx++) = strdup(token);
            token = strtok(0, delim);
        }
        *(result + idx) = 0;
    }

    return result;
}

//
//  Convert a string to lower case
//
//
void ToLower(
    char* Str
) {
    for(char *p=Str; *p; p++) {
        *p=tolower(*p);
    }
}

//
// Parse an application signature from a string
// Example 1: "192.168.200.133/32:65535 192.168.200.132/32:65535 TCP"
// Example 2: "0.0.0.0/0:* 0.0.0.0/0:* *"
//
//
int
ParseAppSignature(
    char* Str,
    struct DDSBOWAppSignature* Sig
) { 
    char** elements = StrSplit(Str, ' ');

    //
    // Process the source address
    //
    //
    char** srcAddrElements = StrSplit(elements[0], ':');
    strcpy(Sig->SourceIPv4, srcAddrElements[0]);
    strcpy(Sig->SourcePort, srcAddrElements[1]);

    //
    // Process the destination address
    //
    //
    char** dstAddrElements = StrSplit(elements[1], ':');
    strcpy(Sig->DestinationIPv4, dstAddrElements[0]);
    strcpy(Sig->DestinationPort, dstAddrElements[1]);
    
    //
    // Process the protocol
    //
    //
    strcpy(Sig->Protocol, elements[2]);
    ToLower(Sig->Protocol);

    //
    // Release memory
    //
    //
    free(dstAddrElements[0]);
    free(dstAddrElements[1]);
    free(dstAddrElements);

    free(srcAddrElements[0]);
    free(srcAddrElements[1]);
    free(srcAddrElements);

    free(elements[0]);
    free(elements[1]);
    free(elements);
    
    return 0;
}

//
// Return the application signature as a string
//
//
void
GetAppSignatureStr(
    struct DDSBOWAppSignature* Sig,
    char* Str
) {
    strcpy(Str, Sig->SourceIPv4);
    strcat(Str, ":");
    strcat(Str, Sig->SourcePort);
    strcat(Str, " ");
    strcat(Str, Sig->DestinationIPv4);
    strcat(Str, ":");
    strcat(Str, Sig->DestinationPort);
    strcat(Str, " ");
    strcat(Str, Sig->Protocol);
}

//
// Split a string into an array of strings
//
//
char**
StrSplitWithCount(
    char* Str,
    const char Delim,
    int* Count
) {
    char** result    = 0;
    size_t count     = 0;
    char* tmp        = Str;
    char* lastIndex = 0;
    char delim[2];
    delim[0] = Delim;
    delim[1] = 0;

    while (*tmp) {
        if (Delim == *tmp) {
            count++;
            lastIndex = tmp;
        }
        tmp++;
    }

    count += (lastIndex > 0) && (lastIndex < (Str + strlen(Str) - 1));

    if (count == 0) {
    	count = 1;
    }

    result = (char**)malloc(sizeof(char*) * count);

    if (result) {
        size_t idx  = 0;
        char* token = strtok(Str, delim);

        while (token) {
            *(result + idx++) = strdup(token);
            token = strtok(0, delim);
        }
    }

    *Count = count;

    return result;
}

//
// Parse fwd rules from a string
//
//
int
ParseFwdRules(
    char* Str,
    struct DDSBOWFwdRuleList* RuleList
) {
    int numRules = 0;
    char** rules = StrSplitWithCount(Str, ';', &numRules);
    RuleList->NumRules = numRules;
    RuleList->Rules = (struct DDSBOWFwdRule*)malloc(sizeof(struct DDSBOWFwdRule) * numRules);

    if (RuleList->Rules == NULL) {
        if (rules) {
            for (int j = 0; j != numRules; j++) {
                free(rules[j]);
            }
            free(rules);
        }
        return -1;
    }

    for (int i = 0; i != numRules; i++) {
        int tmp;
        char** ruleElements = StrSplitWithCount(rules[i], ' ', &tmp);
        
        strcpy(RuleList->Rules[i].RemoteIPv4, ruleElements[0]);
        strcpy(RuleList->Rules[i].RemoteMAC, ruleElements[1]);

        for (int j = 0; j != tmp; j++) {
            free(ruleElements[j]);
        }
        free(ruleElements);
    }

    for (int j = 0; j != numRules; j++) {
        free(rules[j]);
    }
    free(rules);

    return 0;
}

//
// Release rules
//
//
int
ReleaseFwdRules(
    struct DDSBOWFwdRuleList* RuleList
) {
    if (RuleList->Rules != NULL) {
        free(RuleList->Rules);
    }
    return 0;
}
