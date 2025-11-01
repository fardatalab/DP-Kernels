#include "fe_interface.h"
#include "Common/common.hpp"
#include "DPManager/dpm_interface.hpp"
#include "DPManager/memory.hpp"
#include "DPManager/memory_common.hpp"

extern "C" void dpm_frontend_init() {
    dpm_frontend_initialize();
}