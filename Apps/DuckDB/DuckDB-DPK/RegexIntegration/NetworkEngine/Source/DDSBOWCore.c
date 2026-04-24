#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/time.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/queue.h>
#include <netinet/in.h>
#include <setjmp.h>
#include <stdarg.h>
#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <stdbool.h>

#include <rte_eal.h>
#include <rte_common.h>
#include <rte_malloc.h>
#include <rte_ether.h>
#include <rte_ethdev.h>
#include <rte_mempool.h>
#include <rte_mbuf.h>
#include <rte_net.h>
#include <rte_flow.h>

#include <doca_argp.h>
#include <doca_flow.h>
#include <doca_log.h>

#include <dpdk_utils.h>
#include <utils.h>

#include "AppEchoTCP.h"
#include "DDSBOWCore.h"

DOCA_LOG_REGISTER(DDS_BOW_Core);
#define doca_get_error_string(x) doca_get_error_string(x)

extern char DPU_INTERFACE[16];    // = "p0";
extern char BOW_INTERFACE[16];    // = "en3f0pf0sf0"; //"en3f0pf0sf1";
extern char HOST_INTERFACE[16];   // = "pf0hpf";
extern char BOW_L2_ADDRESS[18];

static doca_error_t DPUInterfaceCallback(void *Param, void *Config)
{
    strncpy(DPU_INTERFACE, (char *)Param, sizeof(DPU_INTERFACE) - 1);
    DPU_INTERFACE[sizeof(DPU_INTERFACE) - 1] = '\0';
    RTE_LOG(NOTICE, USER1, "DPU Interface = %s\n", DPU_INTERFACE);
    return DOCA_SUCCESS;
}

static doca_error_t BOWInterfaceCallback(void *Param, void *Config)
{
    strncpy(BOW_INTERFACE, (char *)Param, sizeof(BOW_INTERFACE) - 1);
    BOW_INTERFACE[sizeof(BOW_INTERFACE) - 1] = '\0';
    RTE_LOG(NOTICE, USER1, "DDSBOW Interface = %s\n", BOW_INTERFACE);
    return DOCA_SUCCESS;
}

static doca_error_t HostInterfaceCallback(void *Param, void *Config)
{
    strncpy(HOST_INTERFACE, (char *)Param, sizeof(HOST_INTERFACE) - 1);
    HOST_INTERFACE[sizeof(HOST_INTERFACE) - 1] = '\0';
    RTE_LOG(NOTICE, USER1, "Host Interface = %s\n", HOST_INTERFACE);
    return DOCA_SUCCESS;
}

static doca_error_t BOWL2AddressCallback(void *Param, void *Config)
{
    strncpy(BOW_L2_ADDRESS, (char *)Param, sizeof(BOW_L2_ADDRESS) - 1);
    BOW_L2_ADDRESS[sizeof(BOW_L2_ADDRESS) - 1] = '\0';
    RTE_LOG(NOTICE, USER1, "BOW L2 Address = %s\n", BOW_L2_ADDRESS);
    return DOCA_SUCCESS;
}

/*
 * Callback function for the app signature
 *
 * @Param [in]: parameter indicates whther or not to set the app signature
 * @Config [out]: application configuration to set the app signature
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t
AppSignatureCallback(
    void *Param,
    void *Config
) {
    struct DDSBOWConfig *appConfig = (struct DDSBOWConfig *)Config;

    if (ParseAppSignature((char *)Param, &appConfig->AppSig) == 0) {
        char sigStr[64] = {0};
        GetAppSignatureStr(&appConfig->AppSig, sigStr);
        RTE_LOG(NOTICE, USER1,
                "PEPO parameter: application signature = %s\n",
                sigStr);

        return DOCA_SUCCESS;
    } else {
        DOCA_LOG_ERR("Invalid application signature");
        return DOCA_ERROR_INVALID_VALUE;
    }
}

/*
 * Callback function for the path to the offload predicate and function
 *
 * @Param [in]: parameter indicates whther or not to set the path
 * @Config [out]: application configuration to set the path
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t
UdfPathCallback(
    void *Param,
    void *Config
) {
    struct DDSBOWConfig *appConfig = (struct DDSBOWConfig *)Config;

    strcpy(appConfig->UdfPath, (char *)Param);
    RTE_LOG(NOTICE, USER1,
                "UDF code path = %s\n",
                appConfig->UdfPath);
    return DOCA_SUCCESS;
}

/*
 * Callback function for fwd rules
 *
 * @Param [in]: parameter indicates whther or not to set the app signature
 * @Config [out]: application configuration to set the app signature
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t
FwdRulesCallback(
    void *Param,
    void *Config
) {
    struct DDSBOWConfig *appConfig = (struct DDSBOWConfig *)Config;

    printf("Parsing fwd rules %s\n", (char*)Param);
    if (ParseFwdRules((char *)Param, &appConfig->FwdRules) == 0) {
        return DOCA_SUCCESS;
    } else {
        DOCA_LOG_ERR("Invalid fwd rules");
        return DOCA_ERROR_INVALID_VALUE;
    }
}

/*
 * Callback function for determining if DPDK is used
 *
 * @Param [in]: parameter indicates whther or not to set UseDPDK
 * @Config [out]: application configuration to set the app signature
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t
UseDPDKCallback(
    void *Param,
    void *Config
) {
    struct DDSBOWConfig *appConfig = (struct DDSBOWConfig *)Config;
    int useDPDK = *(int *)Param;
    appConfig->UseDPDK = useDPDK;
    RTE_LOG(NOTICE, USER1,
                "PEPO parameter: use DPDK = %u\n",
                useDPDK);

    return DOCA_SUCCESS;
}

/*
 * Callback function for setting number of cores
 *
 * @Param [in]: number of cores to set
 * @Config [out]: application configuration for setting the number of cores
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t
NumCoresCallback(
    void *Param,
    void *Config
) {
    struct DDSBOWConfig *appConfig = (struct DDSBOWConfig *)Config;
    int numCores = *(int *)Param;

    appConfig->NumCores = numCores;
    RTE_LOG(NOTICE, USER1,
                "Number of cores = %u\n",
                numCores);
    return DOCA_SUCCESS;
}

/*
 * Callback function for setting host port
 *
 * @Param [in]: host port to set
 * @Config [out]: application configuration for setting the host port
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t
HostPortCallback(
    void *Param,
    void *Config
) {
    struct DDSBOWConfig *appConfig = (struct DDSBOWConfig *)Config;
    int port = *(int *)Param;

    appConfig->HostPort = (uint16_t)port;
    RTE_LOG(NOTICE, USER1,
                "Host port = %d\n",
                port);
    return DOCA_SUCCESS;
}

/*
 * Callback function for setting core list
 *
 * @Param [in]: core list string to set
 * @Config [out]: application configuration for setting the core list string
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t
CoreListCallback(
    void *Param,
    void *Config
) {
    struct DDSBOWConfig *appConfig = (struct DDSBOWConfig *)Config;
    
    strcpy(appConfig->CoreList, (char *)Param);
    RTE_LOG(NOTICE, USER1,
                "Core list = %s\n",
                appConfig->CoreList);
    return DOCA_SUCCESS;
}

/*
 * Callback function for setting host IPv4 address
 *
 * @Param [in]: host IPv4 string to set
 * @Config [out]: application configuration for setting the host IPv4 string
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t
HostIPv4Callback(
    void *Param,
    void *Config
) {
    struct DDSBOWConfig *appConfig = (struct DDSBOWConfig *)Config;
    
    strcpy(appConfig->HostIPv4, (char *)Param);
    RTE_LOG(NOTICE, USER1,
                "Host IPv4 = %s\n",
                appConfig->HostIPv4);
    return DOCA_SUCCESS;
}

/*
 * Callback function for setting DPU IPv4 address
 *
 * @Param [in]: DPU IPv4 string to set
 * @Config [out]: application configuration for setting the DPU IPv4 string
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t
DPUIPv4Callback(
    void *Param,
    void *Config
) {
    struct DDSBOWConfig *appConfig = (struct DDSBOWConfig *)Config;
    
    strcpy(appConfig->DPUIPv4, (char *)Param);
    RTE_LOG(NOTICE, USER1,
                "DPU IPv4 = %s\n",
                appConfig->DPUIPv4);
    return DOCA_SUCCESS;
}

/*
 * Callback function for setting host Mac address
 *
 * @Param [in]: host Mac string to set
 * @Config [out]: application configuration for setting the host Mac string
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t
HostMacCallback(
    void *Param,
    void *Config
) {
    struct DDSBOWConfig *appConfig = (struct DDSBOWConfig *)Config;
    
    strcpy(appConfig->HostMac, (char *)Param);
    RTE_LOG(NOTICE, USER1,
                "Host Mac = %s\n",
                appConfig->HostMac);
    return DOCA_SUCCESS;
}

/*
 * Callback function for setting max streams
 *
 * @Param [in]: max streams to set
 * @Config [out]: application configuration for setting max streams
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
static doca_error_t
MaxStreamsCallback(
    void *Param,
    void *Config
) {
    struct DDSBOWConfig *appConfig = (struct DDSBOWConfig *)Config;
    int maxStreams = *(int *)Param;

    appConfig->MaxStreams = (uint32_t)maxStreams;
    RTE_LOG(NOTICE, USER1,
                "Max streams = %d\n",
                maxStreams);
    return DOCA_SUCCESS;
}

/*
 * Registers all flags used by the application for DOCA argument parser, so that when parsing
 * it can be parsed accordingly
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t
RegisterDDSBOWParams() {
    doca_error_t result;
    struct doca_argp_param *appSignatureParam, *udfPathParam, *fwdRulesParam;
    struct doca_argp_param *useDPDKParam, *numCoresParam, *coreListParam;
    struct doca_argp_param *hostIPv4Param, *hostPortParam, *hostMacParam;
    struct doca_argp_param *dpuIPv4Param, *maxStreamsParam;
    struct doca_argp_param *dpuInterfaceParam, *ddsbowInterfaceParam, *hostInterfaceParam, *ddsbowL2AddressParam;

    /* Create and register DPU interface param */
    result = doca_argp_param_create(&dpuInterfaceParam);
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to create DPU interface param: %s", doca_get_error_string(result));
        return result;
    }
    doca_argp_param_set_short_name(dpuInterfaceParam, "D");
    doca_argp_param_set_long_name(dpuInterfaceParam, "dpu-interface");
    doca_argp_param_set_arguments(dpuInterfaceParam, "<str>");
    doca_argp_param_set_description(dpuInterfaceParam, "Set DPU interface");
    doca_argp_param_set_callback(dpuInterfaceParam, DPUInterfaceCallback);
    doca_argp_param_set_type(dpuInterfaceParam, DOCA_ARGP_TYPE_STRING);
    result = doca_argp_register_param(dpuInterfaceParam);
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to register DPU interface param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register DDSBOW interface param */
    result = doca_argp_param_create(&ddsbowInterfaceParam);
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to create DDSBOW interface param: %s", doca_get_error_string(result));
        return result;
    }
    doca_argp_param_set_short_name(ddsbowInterfaceParam, "B");
    doca_argp_param_set_long_name(ddsbowInterfaceParam, "bow-interface");
    doca_argp_param_set_arguments(ddsbowInterfaceParam, "<str>");
    doca_argp_param_set_description(ddsbowInterfaceParam, "Set DPU BOW interface");
    doca_argp_param_set_callback(ddsbowInterfaceParam, BOWInterfaceCallback);
    doca_argp_param_set_type(ddsbowInterfaceParam, DOCA_ARGP_TYPE_STRING);
    result = doca_argp_register_param(ddsbowInterfaceParam);
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to register DDSBOW interface param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register Host interface param */
    result = doca_argp_param_create(&hostInterfaceParam);
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to create host interface param: %s", doca_get_error_string(result));
        return result;
    }
    doca_argp_param_set_short_name(hostInterfaceParam, "I");
    doca_argp_param_set_long_name(hostInterfaceParam, "host-interface");
    doca_argp_param_set_arguments(hostInterfaceParam, "<str>");
    doca_argp_param_set_description(hostInterfaceParam, "Set host interface");
    doca_argp_param_set_callback(hostInterfaceParam, HostInterfaceCallback);
    doca_argp_param_set_type(hostInterfaceParam, DOCA_ARGP_TYPE_STRING);
    result = doca_argp_register_param(hostInterfaceParam);
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to register host interface param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register DDSBOW L2 address param */
    result = doca_argp_param_create(&ddsbowL2AddressParam);
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to create DDSBOW L2 address param: %s", doca_get_error_string(result));
        return result;
    }
    doca_argp_param_set_short_name(ddsbowL2AddressParam, "L");
    doca_argp_param_set_long_name(ddsbowL2AddressParam, "bow-l2-address");
    doca_argp_param_set_arguments(ddsbowL2AddressParam, "<str>");
    doca_argp_param_set_description(ddsbowL2AddressParam, "Set BOW interface MAC address");
    doca_argp_param_set_callback(ddsbowL2AddressParam, BOWL2AddressCallback);
    doca_argp_param_set_type(ddsbowL2AddressParam, DOCA_ARGP_TYPE_STRING);
    result = doca_argp_register_param(ddsbowL2AddressParam);
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to register DDSBOW L2 address param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register HW offload param */
    result = doca_argp_param_create(&appSignatureParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_get_error_string(result));
        return result;
    }
    doca_argp_param_set_short_name(appSignatureParam, "s");
    doca_argp_param_set_long_name(appSignatureParam, "app-sig");
    doca_argp_param_set_description(appSignatureParam, "Set application signature");
    doca_argp_param_set_callback(appSignatureParam, AppSignatureCallback);
    doca_argp_param_set_type(appSignatureParam, DOCA_ARGP_TYPE_STRING);
    result = doca_argp_register_param(appSignatureParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register app sig param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register offload predicate param */
    /* result = doca_argp_param_create(&udfPathParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_get_error_string(result));
        return result;
    }
    doca_argp_param_set_short_name(udfPathParam, "u");
    doca_argp_param_set_long_name(udfPathParam, "udf-path");
    doca_argp_param_set_description(udfPathParam, "Set path to UDFs");
    doca_argp_param_set_callback(udfPathParam, UdfPathCallback);
    doca_argp_param_set_type(udfPathParam, DOCA_ARGP_TYPE_STRING);
    result = doca_argp_register_param(udfPathParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register udf param: %s", doca_get_error_string(result));
        return result;
    } */

    /* Create and register fwd rules param */
    result = doca_argp_param_create(&fwdRulesParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_get_error_string(result));
        return result;
    }
    doca_argp_param_set_short_name(fwdRulesParam, "f");
    doca_argp_param_set_long_name(fwdRulesParam, "fwd-rules");
    doca_argp_param_set_description(fwdRulesParam, "Set forwarding rules");
    doca_argp_param_set_callback(fwdRulesParam, FwdRulesCallback);
    doca_argp_param_set_type(fwdRulesParam, DOCA_ARGP_TYPE_STRING);
    result = doca_argp_register_param(fwdRulesParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register fwd rules param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register use DPDK param */
    result = doca_argp_param_create(&useDPDKParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_get_error_string(result));
        return result;
    }
    doca_argp_param_set_short_name(useDPDKParam, "d");
    doca_argp_param_set_long_name(useDPDKParam, "use-dpdk");
    doca_argp_param_set_arguments(useDPDKParam, "<num>");
    doca_argp_param_set_description(useDPDKParam, "Set if use DPDK");
    doca_argp_param_set_callback(useDPDKParam, UseDPDKCallback);
    doca_argp_param_set_type(useDPDKParam, DOCA_ARGP_TYPE_INT);
    result = doca_argp_register_param(useDPDKParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register use dpdk param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register number of cores param */
    result = doca_argp_param_create(&numCoresParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_get_error_string(result));
        return result;
    }
    doca_argp_param_set_short_name(numCoresParam, "c");
    doca_argp_param_set_long_name(numCoresParam, "num-cores");
    doca_argp_param_set_arguments(numCoresParam, "<num>");
    doca_argp_param_set_description(numCoresParam, "Set the number of cores to use");
    doca_argp_param_set_callback(numCoresParam, NumCoresCallback);
    doca_argp_param_set_type(numCoresParam, DOCA_ARGP_TYPE_INT);
    result = doca_argp_register_param(numCoresParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register num cores param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register core list param */
    result = doca_argp_param_create(&coreListParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_get_error_string(result));
        return result;
    }
    // doca_argp_param_set_short_name(coreListParam, "l"); // XXX: some how this causes an error
    doca_argp_param_set_long_name(coreListParam, "core-list");
    doca_argp_param_set_arguments(coreListParam, "<str>");
    doca_argp_param_set_description(coreListParam, "Set the list of cores to use");
    doca_argp_param_set_callback(coreListParam, CoreListCallback);
    doca_argp_param_set_type(coreListParam, DOCA_ARGP_TYPE_STRING);
    result = doca_argp_register_param(coreListParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register core list param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register host IPv4 param */
    result = doca_argp_param_create(&hostIPv4Param);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_get_error_string(result));
        return result;
    }
    doca_argp_param_set_short_name(hostIPv4Param, "i");
    doca_argp_param_set_long_name(hostIPv4Param, "host-ipv4");
    doca_argp_param_set_arguments(hostIPv4Param, "<str>");
    doca_argp_param_set_description(hostIPv4Param, "Set host IPv4 address");
    doca_argp_param_set_callback(hostIPv4Param, HostIPv4Callback);
    doca_argp_param_set_type(hostIPv4Param, DOCA_ARGP_TYPE_STRING);
    result = doca_argp_register_param(hostIPv4Param);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register host ipv4 param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register DPU IPv4 param */
    result = doca_argp_param_create(&dpuIPv4Param);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_get_error_string(result));
        return result;
    }
    doca_argp_param_set_short_name(dpuIPv4Param, "u");
    doca_argp_param_set_long_name(dpuIPv4Param, "dpu-ipv4");
    doca_argp_param_set_arguments(dpuIPv4Param, "<str>");
    doca_argp_param_set_description(dpuIPv4Param, "Set DPU IPv4 address");
    doca_argp_param_set_callback(dpuIPv4Param, DPUIPv4Callback);
    doca_argp_param_set_type(dpuIPv4Param, DOCA_ARGP_TYPE_STRING);
    result = doca_argp_register_param(dpuIPv4Param);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register dpu ipv4 param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register host MAC param */
    result = doca_argp_param_create(&hostMacParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_get_error_string(result));
        return result;
    }
    // doca_argp_param_set_short_name(hostMacParam, "h");
    doca_argp_param_set_long_name(hostMacParam, "host-mac");
    doca_argp_param_set_arguments(hostMacParam, "<str>");
    doca_argp_param_set_description(hostMacParam, "Set host MAC address");
    doca_argp_param_set_callback(hostMacParam, HostMacCallback);
    doca_argp_param_set_type(hostMacParam, DOCA_ARGP_TYPE_STRING);
    result = doca_argp_register_param(hostMacParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register host mac param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register host port param */
    result = doca_argp_param_create(&hostPortParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_get_error_string(result));
        return result;
    }
    // doca_argp_param_set_short_name(hostPortParam, "p");
    doca_argp_param_set_long_name(hostPortParam, "host-port");
    doca_argp_param_set_arguments(hostPortParam, "<num>");
    doca_argp_param_set_description(hostPortParam, "Set host IPv4 port");
    doca_argp_param_set_callback(hostPortParam, HostPortCallback);
    doca_argp_param_set_type(hostPortParam, DOCA_ARGP_TYPE_INT);
    result = doca_argp_register_param(hostPortParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register host port param: %s", doca_get_error_string(result));
        return result;
    }

    /* Create and register max streams param */
    result = doca_argp_param_create(&maxStreamsParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to create ARGP param: %s", doca_get_error_string(result));
        return result;
    }
    // doca_argp_param_set_short_name(maxStreamsParam, "m");
    doca_argp_param_set_long_name(maxStreamsParam, "max-streams");
    doca_argp_param_set_arguments(maxStreamsParam, "<num>");
    doca_argp_param_set_description(maxStreamsParam, "Set maximum concurrent streams and packets TLDK can create");
    doca_argp_param_set_callback(maxStreamsParam, MaxStreamsCallback);
    doca_argp_param_set_type(maxStreamsParam, DOCA_ARGP_TYPE_INT);
    result = doca_argp_register_param(maxStreamsParam);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register max streams param: %s", doca_get_error_string(result));
        return result;
    }

    /* Register version callback for DOCA SDK & RUNTIME */
    result = doca_argp_register_version_callback(sdk_version_callback);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to register version callback: %s", doca_get_error_string(result));
        return result;
    }
    return DOCA_SUCCESS;
}
