#include <signal.h>
#include <stdint.h>

#include <rte_cycles.h>
#include <rte_ethdev.h>
#include <rte_launch.h>

#include <doca_argp.h>
#include <doca_log.h>

#include <dpdk_utils.h>
#include <utils.h>

#include "DDSBOWCore.h"
#include "DDSTrafficDirecting.h"
#include "PEPOLinuxTCP.h"
#include "PEPOTLDKTCP.h"

#define doca_get_error_string(x) doca_get_error_string(x)
// #define DOCA_LOG_ERR(...) printf(__VA_ARGS__)
// #define DOCA_LOG_INFO(...) printf(__VA_ARGS__)

DOCA_LOG_REGISTER(DDSBOW_PIPELINE);

struct DDSBOWConfig AppCfg = {0};

/*
 * DDS BOW pipeline main function
 *
 * @Argc [in]: command line arguments size
 * @Argv [in]: array of command line arguments
 * @StorageEngine [in]: the function to start storage engine
 * @StorageEngineArg [in]: the argument to the function above
 * @return: EXIT_SUCCESS on success and EXIT_FAILURE otherwise
 */
int RunBOW(int Argc, char **Argv
           // void (*StorageEngine)(void*),
           // void* StorageEngineArg
)
{
    doca_error_t result;
    int exitStatus = EXIT_SUCCESS;
    struct doca_logger_backend *logger;
    int resultInt;

    memset(&AppCfg.AppSig, 0, sizeof(struct DDSBOWAppSignature));

    /* Parse cmdline/json arguments */
    result = doca_argp_init("DDSBOWPipeline", &AppCfg);
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to init ARGP resources: %s", doca_get_error_string(result));
        return EXIT_FAILURE;
    }
    doca_argp_set_dpdk_program(dpdk_init);
    result = RegisterDDSBOWParams();
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to register application params: %s", doca_get_error_string(result));
        doca_argp_destroy();
        return EXIT_FAILURE;
    }

    result = doca_argp_start(Argc, Argv);
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to parse application input: %s", doca_get_error_string(result));
        doca_argp_destroy();
        return EXIT_FAILURE;
    }

    /* result = doca_log_create_syslog_backend("doca_core", &logger);
    if (result != DOCA_SUCCESS)
    {
        DOCA_LOG_ERR("Failed to allocate the logger");
        doca_argp_destroy();
        return EXIT_FAILURE;
    } */

    if (AppCfg.UseDPDK)
    {
        //
        // Apply application signature to direct traffic to DDS BOW
        //
        //
        resultInt = ApplyDirectingRules(&AppCfg.AppSig, &AppCfg.FwdRules, AppCfg.HostMac, AppCfg.DPUIPv4);
        if (resultInt)
        {
            DOCA_LOG_ERR("Failed to apply traffic directing rules");
            exitStatus = EXIT_FAILURE;
            goto exit_app;
        }

        //
        // Init and run TLDK TCP PEPO
        //
        //
        resultInt = PEPOTLDKTCPInit(AppCfg.NumCores, AppCfg.CoreList, AppCfg.HostIPv4, AppCfg.HostPort, AppCfg.DPUIPv4,
                                    AppCfg.UdfPath, AppCfg.MaxStreams);

        if (resultInt)
        {
            DOCA_LOG_ERR("Failed to init TLDK TCP PEPO");
            exitStatus = EXIT_FAILURE;
            goto exit_app;
        }

        // resultInt = PEPOTLDKTCPRun(StorageEngine, StorageEngineArg);
        resultInt = PEPOTLDKTCPRun();
        if (resultInt)
        {
            DOCA_LOG_ERR("Failed to run TLDK TCP PEPO");
            exitStatus = EXIT_FAILURE;
            goto exit_app;
        }
    }
    else
    {
        //
        // Apply traffic directing rules in NIC HW
        //
        //
        resultInt = ApplyDirectingRules(&AppCfg.AppSig, &AppCfg.FwdRules, AppCfg.HostMac, AppCfg.DPUIPv4);
        if (resultInt)
        {
            DOCA_LOG_ERR("Failed to apply traffic directing rules");
            exitStatus = EXIT_FAILURE;
            goto exit_app;
        }

        //
        // Init and run Linux TCP PEPO
        //
        //
        resultInt = PEPOLinuxTCPInit();
        if (resultInt)
        {
            DOCA_LOG_ERR("Failed to init Linux TCP PEPO");
            exitStatus = EXIT_FAILURE;
            goto exit_app;
        }

        resultInt = PEPOLinuxTCPRun();
        if (resultInt)
        {
            DOCA_LOG_ERR("Failed to run Linux TCP PEPO");
            exitStatus = EXIT_FAILURE;
            goto exit_app;
        }
    }

exit_app:
    if (AppCfg.UseDPDK)
    {
        //
        // Stop and destroy TLDK TCP PEPO
        //
        //
        resultInt = PEPOTLDKTCPDestroy();
        if (resultInt)
        {
            DOCA_LOG_ERR("Failed to destroy TLDK TCP PEPO");
            exitStatus = EXIT_FAILURE;
        }

        //
        // Delete traffic directing rules
        //
        //
        resultInt = RemoveDirectingRules(&AppCfg.AppSig, &AppCfg.FwdRules, AppCfg.HostMac, AppCfg.DPUIPv4);
        if (resultInt)
        {
            DOCA_LOG_ERR("Failed to remove traffic directing rules");
        }
        else
            DOCA_LOG_INFO("Traffic directing rules removed");

        ReleaseFwdRules(&AppCfg.FwdRules);

        // /* DPDK cleanup resources */
        // dpdk_queues_and_ports_fini(&dpdkConfig);

        dpdk_fini();
    }
    else
    {
        resultInt = PEPOLinuxTCPDestroy();
        if (resultInt)
        {
            DOCA_LOG_ERR("Failed to destroy Linux TCP PEPO");
            exitStatus = EXIT_FAILURE;
        }

        //
        // Delete traffic directing rules
        //
        //
        RemoveDirectingRules(&AppCfg.AppSig, &AppCfg.FwdRules, AppCfg.HostMac, AppCfg.DPUIPv4);
        ReleaseFwdRules(&AppCfg.FwdRules);
    }

    /* ARGP cleanup */
    doca_argp_destroy();

    return exitStatus;
}
