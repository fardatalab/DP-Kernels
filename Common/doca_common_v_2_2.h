/*
 * Copyright (c) 2022-2023 NVIDIA CORPORATION & AFFILIATES, ALL RIGHTS RESERVED.
 *
 * This software product is a proprietary product of NVIDIA CORPORATION &
 * AFFILIATES (the "Company") and all right, title, and interest in and to the
 * software product, including all associated intellectual property rights, are
 * and shall remain exclusively with the Company.
 *
 * This software product is governed by the End User License Agreement
 * provided with the software product.
 *
 */

#ifndef COMMON_H_
#define COMMON_H_

#include <doca_dev.h>
#include <doca_error.h>

#ifdef __cplusplus
extern "C"
{
#endif

    /* Function to check if a given device is capable of executing some job */
    typedef doca_error_t (*jobs_check)(struct doca_devinfo *);

    /* DOCA core objects used by the samples / applications */
    struct program_core_objects
    {
        struct doca_dev *dev;               /* doca device */
        struct doca_mmap *src_mmap;         /* doca mmap for source buffer */
        struct doca_mmap *dst_mmap;         /* doca mmap for destination buffer */
        struct doca_buf_inventory *buf_inv; /* doca buffer inventory */
        // struct doca_ctx *ctx;               /* doca context */  // should be per kernel!!
        // struct doca_workq *workq;           /* doca work queue */  // should also be per kernel!!
    };

    /*
     * Open a DOCA device according to a given PCI address
     *
     * @pci_addr [in]: PCI address
     * @func [in]: pointer to a function that checks if the device have some job capabilities (Ignored if set to NULL)
     * @retval [out]: pointer to doca_dev struct, NULL if not found
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    doca_error_t open_doca_device_with_pci(const char *pci_addr, jobs_check func, struct doca_dev **retval);

    /*
     * Open a DOCA device according to a given IB device name
     *
     * @value [in]: IB device name
     * @val_size [in]: input length, in bytes
     * @func [in]: pointer to a function that checks if the device have some job capabilities (Ignored if set to NULL)
     * @retval [out]: pointer to doca_dev struct, NULL if not found
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    doca_error_t open_doca_device_with_ibdev_name(const uint8_t *value, size_t val_size, jobs_check func,
                                                  struct doca_dev **retval);

    /*
     * Open a DOCA device according to a given interface name
     *
     * @value [in]: interface name
     * @val_size [in]: input length, in bytes
     * @func [in]: pointer to a function that checks if the device have some job capabilities (Ignored if set to NULL)
     * @retval [out]: pointer to doca_dev struct, NULL if not found
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    doca_error_t open_doca_device_with_iface_name(const uint8_t *value, size_t val_size, jobs_check func,
                                                  struct doca_dev **retval);

    /*
     * Open a DOCA device with a custom set of capabilities
     *
     * @func [in]: pointer to a function that checks if the device have some job capabilities
     * @retval [out]: pointer to doca_dev struct, NULL if not found
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    doca_error_t open_doca_device_with_capabilities(jobs_check func, struct doca_dev **retval);

    /*
     * Open a DOCA device representor according to a given VUID string
     *
     * @local [in]: queries represtors of the given local doca device
     * @filter [in]: bitflags filter to narrow the represetors in the search
     * @value [in]: IB device name
     * @val_size [in]: input length, in bytes
     * @retval [out]: pointer to doca_dev_rep struct, NULL if not found
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    doca_error_t open_doca_device_rep_with_vuid(struct doca_dev *local, enum doca_dev_rep_filter filter,
                                                const uint8_t *value, size_t val_size, struct doca_dev_rep **retval);

    /*
     * Open a DOCA device according to a given PCI address
     *
     * @local [in]: queries representors of the given local doca device
     * @filter [in]: bitflags filter to narrow the representors in the search
     * @pci_addr [in]: PCI address
     * @retval [out]: pointer to doca_dev_rep struct, NULL if not found
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    doca_error_t open_doca_device_rep_with_pci(struct doca_dev *local, enum doca_dev_rep_filter filter,
                                               const char *pci_addr, struct doca_dev_rep **retval);

    /*
     * Create and start a series of DOCA Core objects needed for the program's execution.
     * See also @ref create_core_objects and @ref start_context.
     *
     * @state [in]: struct containing the set of initialized DOCA Core objects
     * @workq_depth [in]: depth for the created Work Queue
     * @max_bufs [in]: maximum number of buffers for DOCA Inventory
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    // doca_error_t init_core_objects(struct program_core_objects *state, uint32_t workq_depth, uint32_t max_bufs);

    /*
     * Initialize a series of DOCA Core objects needed for the program's execution
     *
     * @state [in]: struct containing the set of initialized DOCA Core objects
     * @max_bufs [in]: maximum number of buffers for DOCA Inventory
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    doca_error_t create_core_objects(struct program_core_objects *state, uint32_t max_bufs);

    /*
     * Start context and work queue needed for the program's execution
     *
     * @state [in]: struct containing the set of initialized DOCA Core objects
     * @workq_depth [in]: depth for the created Work Queue
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    // doca_error_t start_context(struct program_core_objects *state, uint32_t workq_depth);

    /*
     * Cleanup the series of DOCA Core objects created by init_core_objects
     *
     * @state [in]: struct containing the set of initialized DOCA Core objects
     * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
     */
    doca_error_t destroy_core_objects(struct program_core_objects *state);

    /*
     * Create a string Hex dump representation of the given input buffer
     *
     * @data [in]: Pointer to the input buffer
     * @size [in]: Number of bytes to be analyzed
     * @return: pointer to the string representation, or NULL if an error was encountered
     */
    char *hex_dump(const void *data, size_t size);

#endif

#ifdef __cplusplus
}
#endif