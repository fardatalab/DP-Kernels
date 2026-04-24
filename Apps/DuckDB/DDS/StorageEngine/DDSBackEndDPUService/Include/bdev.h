#pragma once

#include <stdint.h>
#include <pthread.h>

#include "DPUBackEnd.h"

extern char *G_BDEV_NAME;

typedef struct SPDKContext {
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *bdev_desc;
	struct spdk_io_channel *bdev_io_channel;  // channel would be per thread unique, meaning this whole context should also be
	char *buff;
	unsigned long long buff_size;
	char *bdev_name;
	struct spdk_bdev_io_wait_entry bdev_io_wait;
	void *cookie;  // just in case, a completion cookie that could be anything
	struct PerSlotContext *SPDKSpace; // an array to record the status of each slot memory inside buff.
	pthread_mutex_t SpaceMutex; // used for SPDKSpace
	// jason: Cached bdev alignment constraints for offset/length and buffer address.
	uint32_t block_size;
	uint32_t buf_align;
} SPDKContextT;

//
// A single SPDK read.
// Sync read should set position = 0
//
//
int BdevRead(
    void *arg,
    char* DstBuffer,
    uint64_t offset,
    uint64_t nbytes,
    spdk_bdev_io_completion_cb cb,
	void *cb_arg
);

//
// A scattered SPDK read.
// Sync read should set position = 0
//
//
int BdevReadV(
	void *arg,
    struct iovec *iov,
	int iovcnt,
    uint64_t offset,
    uint64_t nbytes,
	spdk_bdev_io_completion_cb cb,
	void *cb_arg
);

//
// Callback function for read io completion
//
//
void write_complete(
    struct spdk_bdev_io *bdev_io, 
    bool success, 
    void *cb_arg
);

//
// A single SPDK write
// position is used for async only and should between belongs
// [0, ONE_GB). But the actual position will be added ONE_GB to 
// separate space from read.
// Sync write should set position = 0
//
//
int BdevWrite(
    void *arg,
    char* SrcBuffer,
    uint64_t offset,
    uint64_t nbytes,
    spdk_bdev_io_completion_cb cb,
	void *cb_arg
);

//
// Bdev write function
// position is used for async only and should between belongs
// [0, ONE_GB). But the actual position will be added ONE_GB to 
// separate space from read.
// Sync write should set position = 0
//
//
int BdevWriteV(
    void *arg,
    struct iovec *iov,
    int iovcnt,
    uint64_t offset,
    uint64_t nbytes,
	spdk_bdev_io_completion_cb cb,
	void *cb_arg
);

void SpdkBdevEventCb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		    void *event_ctx);

void BdevResetZone(void *arg);
