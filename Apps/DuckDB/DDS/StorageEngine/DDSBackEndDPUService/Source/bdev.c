#include "../Include/bdev.h"

//
// Perform a list of reads (scattered reads)
//
//
int
BdevReadV(
    void *Arg,
    struct iovec *Iov,
    int IovCnt,
    uint64_t Offset,
    uint64_t NBytes,
    spdk_bdev_io_completion_cb Cb,
    void *CbArg
) {
    SPDKContextT *spdkContext = (SPDKContextT *)Arg;
    int rc = 0;

    rc = spdk_bdev_readv(
        spdkContext->bdev_desc,
        spdkContext->bdev_io_channel,
        Iov,
        IovCnt,
        Offset,
        NBytes,
        Cb,
        CbArg
    );

    if (rc == 0) {
        return 0;
    } else if ((rc == -ENOMEM)) {
        // 
        // In case we cannot perform I/O now, queue I/O
        //
        //
        SPDK_NOTICELOG("Queueing IO\n");
        spdkContext->bdev_io_wait.bdev = spdkContext->bdev;
        spdkContext->bdev_io_wait.cb_fn = BdevRead;
        spdkContext->bdev_io_wait.cb_arg = spdkContext;
        spdk_bdev_queue_io_wait(
            spdkContext->bdev,
            spdkContext->bdev_io_channel,
            &spdkContext->bdev_io_wait
        );

        //
        // Eventually this IO should finish successfully
        //
        //
        return 0;
    }
    else if ((rc == -EINVAL)) {
        SPDK_ERRLOG("Offset: %lu and/or NBytes: %lu are not aligned or out of range\n", Offset, NBytes);
        return rc;
    }
    else {
        //
        // Unknown error
        //
        //
        SPDK_ERRLOG("%s error while reading from bdev: %d\n", spdk_strerror(-rc), rc);
        return rc;
    }
}

//
// Perform a single read
//
//
int
BdevRead(
    void *Arg,
    char* DstBuffer,
    uint64_t Offset,
    uint64_t NBytes,
    spdk_bdev_io_completion_cb Cb,
    void *CbArg
) {
    SPDKContextT *spdkContext = (SPDKContextT *)Arg;
    int rc = 0;

    rc = spdk_bdev_read(
        spdkContext->bdev_desc,
        spdkContext->bdev_io_channel,
        DstBuffer,
        Offset,
        NBytes,
        Cb,
        CbArg
    );
    if (rc == 0) {
        return 0;
    }
    else if (rc == -ENOMEM) {
        //
        // In case we cannot perform I/O now, queue I/O
        //
        //
        SPDK_NOTICELOG("Queueing IO\n");
        spdkContext->bdev_io_wait.bdev = spdkContext->bdev;
        spdkContext->bdev_io_wait.cb_fn = BdevRead;
        spdkContext->bdev_io_wait.cb_arg = spdkContext;
        spdk_bdev_queue_io_wait(
            spdkContext->bdev,
            spdkContext->bdev_io_channel,
            &spdkContext->bdev_io_wait
        );
        
        //
        // Eventually this IO should finish successfully
        //
        //
        return 0;
    } else if (rc == -EINVAL) {
        SPDK_ERRLOG("Offset: %lu and/or NBytes: %lu are not aligned or out of range\n", Offset, NBytes);
        return rc;
    } else {
        //
        // Unknown error
        //
        //
        SPDK_ERRLOG("%s error while reading from bdev: %d\n", spdk_strerror(-rc), rc);
        return rc;
    }
}

//
// Perform a list of writes (gathered writes)
//
//
int
BdevWriteV(
    void *Arg,
    struct iovec *Iov,
    int IovCnt,
    uint64_t Offset,
    uint64_t NBytes,
    spdk_bdev_io_completion_cb Cb,
    void *CbArg
){
    SPDKContextT *spdkContext = (SPDKContextT *)Arg;
    int rc = 0;

    rc = spdk_bdev_writev(
        spdkContext->bdev_desc,
        spdkContext->bdev_io_channel,
        Iov,
        IovCnt,
        Offset,
        NBytes,
        Cb,
        CbArg
    );

    if (rc == 0) {
        return 0;
    }
    else if (rc == -ENOMEM) {
        //
        // In case we cannot perform I/O now, queue I/O
        //
        //
        SPDK_NOTICELOG("Queueing IO\n");
        spdkContext->bdev_io_wait.bdev = spdkContext->bdev;
        spdkContext->bdev_io_wait.cb_fn = BdevWrite;
        spdkContext->bdev_io_wait.cb_arg = spdkContext;
        spdk_bdev_queue_io_wait(
            spdkContext->bdev,
            spdkContext->bdev_io_channel,
            &spdkContext->bdev_io_wait
        );

        //
        // Eventually this IO should finish successfully
        //
        //
        return 0;
    } else if (rc == -EINVAL) {
        SPDK_ERRLOG("Offset: %lu and/or NBytes: %lu are not aligned or out of range\n", Offset, NBytes);
        return rc;
    } else {
        //
        // Unknown error
        //
        //
        SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
        return rc;
    }
}

//
// Perform a single write
//
//
int
BdevWrite(
    void *Arg,
    char* SrcBuffer,
    uint64_t Offset,
    uint64_t NBytes,
    spdk_bdev_io_completion_cb Cb,
    void *CbArg
) {
    SPDKContextT *spdkContext = (SPDKContextT *)Arg;
    int rc = 0;

    rc = spdk_bdev_write(
        spdkContext->bdev_desc,
	spdkContext->bdev_io_channel,
        SrcBuffer,
	Offset,
	NBytes,
	Cb,
	CbArg
    );

    if (rc == 0) {
        return 0;
    } else if (rc == -ENOMEM) {
        //
        // In case we cannot perform I/O now, queue I/O
        //
        //
        SPDK_NOTICELOG("Queueing IO\n");
        spdkContext->bdev_io_wait.bdev = spdkContext->bdev;
        spdkContext->bdev_io_wait.cb_fn = BdevWrite;
        spdkContext->bdev_io_wait.cb_arg = spdkContext;
        spdk_bdev_queue_io_wait(
            spdkContext->bdev,
            spdkContext->bdev_io_channel,
            &spdkContext->bdev_io_wait
        );

        //
        // Eventually this IO should finish successfully
        //
        //
        return 0;
    } else if (rc == -EINVAL) {
        SPDK_ERRLOG("Offset: %lu and/or NBytes: %lu are not aligned or out of range\n", Offset, NBytes);
        return rc;
    } else {
        //
        // Unknown error
        //
        //
        SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
        return rc;
    }
}

void
ResetZoneComplete(
    struct spdk_bdev_io *BdevIo,
    bool Success,
    void *CbArg
) {
    SPDKContextT *SPDKContext = (SPDKContextT *)CbArg;

    //
    // Complete the I/O
    //
    //
    spdk_bdev_free_io(BdevIo);

    if (!Success) {
        SPDK_ERRLOG("bdev io reset zone error: %d\n", EIO);
        spdk_put_io_channel(SPDKContext->bdev_io_channel);
        spdk_bdev_close(SPDKContext->bdev_desc);
        spdk_app_stop(-1);
        return;
    }
}

void
BdevResetZone(
	void *Arg
) {
    SPDKContextT *SPDKContext = (SPDKContextT *)Arg;
    int rc = 0;

    rc = spdk_bdev_zone_management(
        SPDKContext->bdev_desc,
        SPDKContext->bdev_io_channel,
        0,
        SPDK_BDEV_ZONE_RESET,
        ResetZoneComplete,
        SPDKContext
    );

    if (rc == -ENOMEM) {
        //
        // In case we cannot perform I/O now, queue I/O
        //
        //
        SPDK_NOTICELOG("Queueing io\n");
        SPDKContext->bdev_io_wait.bdev = SPDKContext->bdev;
        SPDKContext->bdev_io_wait.cb_fn = BdevResetZone;
        SPDKContext->bdev_io_wait.cb_arg = SPDKContext;
        spdk_bdev_queue_io_wait(
            SPDKContext->bdev,
            SPDKContext->bdev_io_channel,
            &SPDKContext->bdev_io_wait
        );
    } else if (rc) {
        SPDK_ERRLOG("%s error while resetting zone: %d\n", spdk_strerror(-rc), rc);
        spdk_put_io_channel(SPDKContext->bdev_io_channel);
        spdk_bdev_close(SPDKContext->bdev_desc);
        spdk_app_stop(-1);
    }
}

//
// This seems to be called when Ctrl-C'd, and gets type 0
//
//
void
SpdkBdevEventCb(
    enum spdk_bdev_event_type type,
    struct spdk_bdev *bdev,
    void *event_ctx
) {
    if (type == 0) {
        SPDK_NOTICELOG("bdev event: type 0, exiting...\n");
        ForceQuitStorageEngine = 1;
        ForceQuitNetworkEngine = 1;
        return;
    }
    SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}
