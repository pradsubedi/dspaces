/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers
 * University
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef __DSPACES_CLIENT_H
#define __DSPACES_CLIENT_H

#include <dspaces-common.h>
#include <mpi.h>
#include <stdint.h>

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct dspaces_client *dspaces_client_t;
#define dspaces_CLIENT_NULL ((dspaces_client_t)NULL)

#define META_MODE_SPEC 1
#define META_MODE_NEXT 2
#define META_MODE_LAST 3

/**
 * @brief Creates a dspaces client.
 * @param[in] rank: rank of this process relative in the application
 * @param[out] client dspaces client
 *
 * @return dspaces_SUCCESS or error code defined in dspaces-common.h
 */
int dspaces_init(int rank, dspaces_client_t *client);

/**
 * @brief Creates a dspaces client. Uses MPI primitives for scalable initialization.
 * @param[in] comm: MPI communicator for reading configuration collectively.
 * @param[out] client dspaces client
 *
 * @return dspaces_SUCCESS or error code defined in dspaces-common.h
 */
int dspaces_init_mpi(MPI_Comm comm, dspaces_client_t *c);

void dspaces_define_gdim(dspaces_client_t client, const char *var_name,
                         int ndim, uint64_t *gdim);

/**
 * @brief Finalizes a dspaces client.
 *
 * @param[in] client dspaces client to finalize
 *
 * @return dspaces_SUCCESS or error code defined in dspaces-common.h
 */
int dspaces_fini(dspaces_client_t client);

/**
 * @brief Query the space to insert data specified by a geometric
 *    descriptor.
 *
 * Memory buffer pointed by pointer "data" is a sub-region of the
 * global n-dimensional array in user application, which is described
 * by the local bounding box {(lb[0],lb[1],..,lb[n-1]),
 * (ub[0],ub[1],..,ub[n-1])}.
 *
 * This routine is non-blocking, and successful return of the routine does not
 * guarantee the completion of data transfer from client process to dataspaces
 * staging server.
 *
 * Note: ordering of dimension (fast->slow) is 0, 1, ..., n-1. For C row-major
 * array, the dimensions need to be reordered to construct the bounding box. For
 * example, the bounding box for C array c[2][4] is lb: {0,0}, ub: {3,1}.
 *
 * @param[in] client dspaces client
 * @param[in] var_name:     Name of the variable.
 * @param[in] ver:      Version of the variable.
 * @param[in] size:     Size (in bytes) for each element of the global
 *              array.
 * @param[in] ndim:     the number of dimensions for the local bounding
 *              box.
 * @param[in] lb:       coordinates for the lower corner of the local
 *                  bounding box.
 * @param[in] ub:       coordinates for the upper corner of the local
 *                  bounding box.
 * @param[in] data:     Pointer to user data buffer.
 *
 * @return  0 indicates success.
 */
int dspaces_put(dspaces_client_t client, const char *var_name, unsigned int ver,
                int size, int ndim, uint64_t *lb, uint64_t *ub, const void *data);

/**
 * @brief Query the space to insert data specified by a geometric
 *    descriptor.
 *
 * Memory buffer pointed by pointer "data" is a sub-region of the
 * global n-dimensional array in user application, which is described
 * by the local bounding box {(lb[0],lb[1],..,lb[n-1]),
 * (ub[0],ub[1],..,ub[n-1])}.
 *
 * This routine is non-blocking, and successful return of the routine does not
 * guarantee the completion of data transfer from client process to dataspaces
 * staging server.
 *
 * Note: ordering of dimension (fast->slow) is 0, 1, ..., n-1. For C row-major
 * array, the dimensions need to be reordered to construct the bounding box. For
 * example, the bounding box for C array c[2][4] is lb: {0,0}, ub: {3,1}.
 *
 * @param[in] client dspaces client
 * @param[in] var_name:     Name of the variable.
 * @param[in] ver:      Version of the variable.
 * @param[in] size:     Size (in bytes) for each element of the global
 *              array.
 * @param[in] ndim:     the number of dimensions for the local bounding
 *              box.
 * @param[in] lb:       coordinates for the lower corner of the local
 *                  bounding box.
 * @param[in] ub:       coordinates for the upper corner of the local
 *                  bounding box.
 * @param[in] data:     Pointer to user data buffer.
 *
 * @return  0 indicates success.
 */
int dspaces_put_local(dspaces_client_t client, const char *var_name,
                      unsigned int ver, int size, int ndim, uint64_t *lb,
                      uint64_t *ub, void *data);

/**
 * @brief Query the space to get data specified by a geometric
 *    descriptor.
 *
 * Memory buffer pointed by pointer "data" is a sub-region of the
 * global n-dimensional array in user application, which is described
 * by the local bounding box {(lb[0],lb[1],..,lb[n-1]),
 * (ub[0],ub[1],..,ub[n-1])}.
 *
 * This routine is non-blocking, and successful return of the routine does not
 * guarantee the completion of data transfer from client process to dataspaces
 * staging server. User applications need to call dspaces_put_sync to check if
 * the most recent dspaces_put is complete or not.
 *
 * Note: ordering of dimension (fast->slow) is 0, 1, ..., n-1. For C row-major
 * array, the dimensions need to be reordered to construct the bounding box. For
 * example, the bounding box for C array c[2][4] is lb: {0,0}, ub: {3,1}.
 *
 * @param[in] client dspaces client
 * @param[in] var_name:     Name of the variable.
 * @param[in] ver:      Version of the variable.
 * @param[in] size:     Size (in bytes) for each element of the global
 *              array.
 * @param[in] ndim:     the number of dimensions for the local bounding
 *              box.
 * @param[in] lb:       coordinates for the lower corner of the local
 *                  bounding box.
 * @param[in] ub:       coordinates for the upper corner of the local
 *                  bounding box.
 * @param[in] data:     Pointer to user data buffer.
 * @param[in] timeout:  Timeout value: -1 is never, 0 is immediate.
 *
 * @return  0 indicates success.
 */
int dspaces_get(dspaces_client_t client, const char *var_name, unsigned int ver,
                int size, int ndim, uint64_t *lb, uint64_t *ub, void *data,
                int timeout);

/**
 * @brief Allocating get - the library allocates the data buffers based on
 * available metadata.
 *
 * This function is similar to dspaces_get, but allocates the buffer pointed to
 * by data. The element size is inferred from available metadata.
 *
 * @param[in] client dspaces client
 * @param[in] var_name:     Name of the variable.
 * @param[in] ver:      Version of the variable.
 *              array.
 * @param[in] ndim:     the number of dimensions for the local bounding
 *              box.
 * @param[in] lb:       coordinates for the lower corner of the local
 *                  bounding box.
 * @param[in] ub:       coordinates for the upper corner of the local
 *                  bounding box.
 * @param[in] data:     Pointer to user data buffer.
 * @param[in] timeout:  Timeout value: -1 is never, 0 is immediate.
 *
 * @return  0 indicates success.
 */
int dspaces_aget(dspaces_client_t client, const char *var_name,
                 unsigned int ver, int ndim, uint64_t *lb, uint64_t *ub,
                 void **data, int timeout);

struct dspaces_req {
    char *var_name;
    int ver;
    int elem_size;
    int ndim;
    uint64_t *lb, *ub;
    void *buf;
};

typedef int (*dspaces_sub_fn)(dspaces_client_t, struct dspaces_req *, void *);
typedef struct dspaces_sub_handle *dspaces_sub_t;
#define DSPACES_SUB_FAIL NULL

#define DSPACES_SUB_DONE 0
#define DSPACES_SUB_WAIT 1
#define DSPACES_SUB_ERR 2
#define DSPACES_SUB_RUNNING 3
#define DSPACES_SUB_INVALID 4
#define DSPACES_SUB_CANCELLED 5

/**
 * @brief subscribe to data objects with callback
 *
 * A client can subscribe to a data object. When the object is received, the
 * callback function will be run on the data object with the passed argument.
 * sub_cb receives the request, a data buffer, and the user-suppied argument.
 * Aftr sub_cb is run, the request structure is free'd. The user should free
 * the data buffer.
 *
 * @param[in] client dspaces client
 * @param[in] var_name:     Name of the variable.
 * @param[in] ver:      Version of the variable.
 * @param[in] size:     Size (in bytes) for each element of the global array.
 * @param[in] ndim:     the number of dimensions for the local bounding box.
 * @param[in] lb:       coordinates for the lower corner of the local
 *                  bounding box.
 * @param[in] ub:       coordinates for the upper corner of the local
 *                  bounding box.
 * @param[in] sub_cb: function to run on subscribed data.
 * @param[in] arg: user argument to pass to sub_cb when run.
 *
 * @return subscription handle, DSPACES_SUB_FAIL on failure.
 */
dspaces_sub_t dspaces_sub(dspaces_client_t client, const char *var_name,
                          unsigned int ver, int elem_size, int ndim,
                          uint64_t *lb, uint64_t *ub, dspaces_sub_fn sub_cb,
                          void *arg);
/**
 * @brief check data subscription status
 *
 * Check the status of a subscription handle returned by a previous data
 * subscription.
 *
 * @param[in] client: dspaces client.
 * @param[in] subh: the subscription handle to check.
 * @param[in] wait: wait for subscription to either complete or fail
 * @param[out] results: the return value of the user callback, if the
 *    subscription has fired.
 *
 * @return subscription status
 */
int dspaces_check_sub(dspaces_client_t client, dspaces_sub_t subh, int wait,
                      int *result);

/**
 * @brief cancels pending subscription
 *
 * Stops a pending subscription from being processed, ignoring any future
 * notification for this subscription.
 *
 * @param[in] client: dspaces client
 * @param[in] subh: handle for the subscription to be cancelled.
 *
 * @return zero for success, non-zero for failure (probably invalid subh)
 */
int dspaces_cancel_sub(dspaces_client_t client, dspaces_sub_t subh);

/**
 * @brief send signal to kill server group.
 *
 * Only one client process needs to send the kill process.
 * The kill signal is propagated between server processes.
 *
 * @param[in] client dspaces client
 */
void dspaces_kill(dspaces_client_t client);

/**
 * @brief store metadata to dataspaces.
 *
 * Metadata is a byte array indexed by name and version. Only one metadata array
 * per name/version pair should be stored.
 *
 * @param[in] client: dspaces client
 * @param[in] name: the metadata object name
 * @param[in] version: the metadata object version
 * @param[in] data: the buffer containing the metadata
 * @param[in] len: the number of bytes to store from data
 *
 * @return zero for success, non-zero for failure
 */
int dspaces_put_meta(dspaces_client_t client, const char *name, int version,
                     const void *data, unsigned int len);

/**
 * @brief access stored metadata
 *
 * Access stored metadata. There are several modes of metadata access:
 *  META_MODE_SPEC - specify the exact name/version pair of metadata to
 * retrieve. Will not block. META_MODE_NEXT - retrieve the lowest stored version
 * of metadata with a given name that is more recent than the current version.
 * If nothing newer exists, block until new metadata arrives. META_MODE_LAST -
 * get the highest version metadata available for the given name. If nothing
 * higher than the current version exists, blocks waiting for new metadata.
 *
 *  dspaces_get_meta allocates a buffer to store the results.
 *
 * @param[in] client: dspaces client
 * @param[in] name: the metadata object name
 * @param[in] mode: the access mode
 * @param[in] current: the version to be queried (in META_MODE_SPEC mode) or be
 * counted as current for other modes.
 * @param[out] version: the version of the returned metadata
 * @param[out] data: the results buffer
 * @param[out] len: the size of the results buffer in bytes
 */
int dspaces_get_meta(dspaces_client_t client, const char *name, int mode,
                     int current, int *version, void **data, unsigned int *len);

#if defined(__cplusplus)
}
#endif

#endif
