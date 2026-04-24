#include "Zmalloc.h"

// Original widened backend slot depth retained for reference:
// #define DDS_BACKEND_SLOT_COUNT (DDS_MAX_OUTSTANDING_IO * (DDS_DPU_IO_PARALLELISM + 1))
//
// Updated: strict request-id ownership model uses explicit (channel, request-id) ownership.
// Global slot range is [0, DDS_MAX_OUTSTANDING_IO_TOTAL).
#define DDS_BACKEND_SLOT_COUNT DDS_MAX_OUTSTANDING_IO_TOTAL

//
// AllocateSpace
// Updated: allocate SPDK DMA-safe buffers for data-plane staging.
// Updated: skip non-ZC staging buffer allocation when OPT_FILE_SERVICE_ZERO_COPY is set.
//
void AllocateSpace(void *arg){
    SPDKContextT *SPDKContext = arg;
    // Original widened staging space retained for reference:
    // SPDKContext->buff_size = (DDS_MAX_OUTSTANDING_IO * (DDS_DPU_IO_PARALLELISM + 1)) * DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE;
#ifdef OPT_FILE_SERVICE_ZERO_COPY
    // jason: ZC path does not need the per-request staging buffer.
    SPDKContext->buff_size = 0;
    SPDKContext->buff = NULL;
#else
    // Keep staging space sized to the active slot pool.
    SPDKContext->buff_size = DDS_BACKEND_SLOT_COUNT * DDS_BACKEND_SPDK_BUFF_BLOCK_SPACE;
    // SPDKContext->buff = malloc(SPDKContext->buff_size);
    // jason: SPDK bdev I/O buffers must be DMA-safe for RDMA-backed devices.
    FileIOSizeT bufAlign = SPDKContext->buf_align ? (FileIOSizeT)SPDKContext->buf_align : 4096;
    SPDKContext->buff = spdk_dma_zmalloc(SPDKContext->buff_size, bufAlign, NULL);
		if (!SPDKContext->buff) {
		SPDK_ERRLOG("Failed to allocate buffer\n");
        exit(-1);
		return;
	}
#endif

    // Original widened slot allocation retained for reference:
    // SPDKContext->SPDKSpace =
    //     calloc((DDS_MAX_OUTSTANDING_IO * (DDS_DPU_IO_PARALLELISM + 1)), sizeof(struct PerSlotContext));
    SPDKContext->SPDKSpace = calloc(DDS_BACKEND_SLOT_COUNT, sizeof(struct PerSlotContext));
    for (int i = 0; i < DDS_BACKEND_SLOT_COUNT; i++){
        SPDKContext->SPDKSpace[i].Available = true;
        SPDKContext->SPDKSpace[i].Position = i;
    }
    if(spdk_bdev_is_zoned(SPDKContext->bdev)){
        BdevResetZone(arg);
        return;
    }

    pthread_mutex_init(&SPDKContext->SpaceMutex, NULL);
}

void FreeSingleSpace(
    struct PerSlotContext* Ctx
){
    Ctx->Available = true;

}

//
// FreeAllSpace
// Updated: only free staging buffers when they are allocated (non-ZC).
//
void FreeAllSpace(void *arg){
    SPDKContextT *SPDKContext = arg;
    // spdk_dma_free(SPDKContext->buff);
    // jason: staging buffer is only allocated in non-ZC mode.
    if (SPDKContext->buff) {
        spdk_dma_free(SPDKContext->buff);
    }
    free(SPDKContext->SPDKSpace);
}

struct PerSlotContext*
FindFreeSpace(
    SPDKContextT *SPDKContext,
    DataPlaneRequestContext* Context
){
    pthread_mutex_lock(&SPDKContext->SpaceMutex);
    // Original widened scan retained for reference:
    // for (int i = 0; i < (DDS_MAX_OUTSTANDING_IO * 2); i++){
    for (int i = 0; i < DDS_BACKEND_SLOT_COUNT; i++){
        if(SPDKContext->SPDKSpace[i].Available){
            SPDKContext->SPDKSpace[i].Available = false;
            SPDKContext->SPDKSpace[i].Ctx = Context;
            pthread_mutex_unlock(&SPDKContext->SpaceMutex);
            return &SPDKContext->SPDKSpace[i];
        }
    }
    pthread_mutex_unlock(&SPDKContext->SpaceMutex);
    return NULL;
}

//
// Puts the Request Context into a slot context, and returns the slot ctx ptr
// Note: should be always called from app thread.
// Strict mode for active usage: global slot range [0, DDS_MAX_OUTSTANDING_IO_TOTAL) maps 1:1 to active slots.
// for per slot context, since they have a 1 to 1 relation, therefore no need for searching and freeing;
//
//
struct PerSlotContext*
GetFreeSpace(
    SPDKContextT *SPDKContext,
    DataPlaneRequestContext* Context,
    uint32_t Index
){
    if (Index >= DDS_MAX_OUTSTANDING_IO_TOTAL) {
        SPDK_ERRLOG("%s: invalid active slot index %u (active-max=%u, provisioned=%u)\n", __func__, (unsigned)Index,
                    (unsigned)DDS_MAX_OUTSTANDING_IO_TOTAL, (unsigned)DDS_BACKEND_SLOT_COUNT);
        return NULL;
    }
    SPDKContext->SPDKSpace[Index].Ctx = Context;
    return &SPDKContext->SPDKSpace[Index];
}
