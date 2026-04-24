#include "DPUBackEnd.h"
#include "DPUBackEndStorage.h"
#include <stdint.h>

void AllocateSpace(void *arg);

void FreeSingleSpace(struct PerSlotContext* Ctx);

void FreeAllSpace(void *arg);

struct PerSlotContext*
FindFreeSpace(
    SPDKContextT *SPDKContext,
    DataPlaneRequestContext* Context
);

struct PerSlotContext*
GetFreeSpace(
    SPDKContextT *SPDKContext,
    DataPlaneRequestContext* Context,
    uint32_t Index
);
