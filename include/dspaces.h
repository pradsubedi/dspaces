/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers University
 *
 * See COPYRIGHT in top-level directory.
 */


#ifndef __DSPACES_CLIENT_H
#define __DSPACES_CLIENT_H

#include <margo.h>
#include <dspaces-common.h>


#if defined(__cplusplus)
extern "C" {
#endif

typedef struct dspaces_client* dspaces_client_t;
#define dspaces_CLIENT_NULL ((dspaces_client_t)NULL)

/**
 * @brief Creates a dspaces client.
 * @param[in] rank: rank of this process relative in the application
 * @param[out] client dspaces client
 *
 * @return dspaces_SUCCESS or error code defined in dspaces-common.h
 */
int dspaces_init(int rank, dspaces_client_t* client);


void dspaces_define_gdim (dspaces_client_t client, 
	const char *var_name, int ndim, uint64_t *gdim);

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
 * by the local bounding box {(lb[0],lb[1],..,lb[n-1]), (ub[0],ub[1],..,ub[n-1])}.
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
int dspaces_put (dspaces_client_t client,
		const char *var_name,
        unsigned int ver, int size,
        int ndim, uint64_t *lb, uint64_t *ub, 
        void *data); 

/**
 * @brief Query the space to insert data specified by a geometric
 *    descriptor.
 * 
 * Memory buffer pointed by pointer "data" is a sub-region of the
 * global n-dimensional array in user application, which is described
 * by the local bounding box {(lb[0],lb[1],..,lb[n-1]), (ub[0],ub[1],..,ub[n-1])}.
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
int dspaces_put_local (dspaces_client_t client,
		const char *var_name,
        unsigned int ver, int size,
        int ndim, uint64_t *lb, uint64_t *ub, 
        void *data); 

/**
 * @brief Query the space to get data specified by a geometric
 *    descriptor.
 * 
 * Memory buffer pointed by pointer "data" is a sub-region of the
 * global n-dimensional array in user application, which is described
 * by the local bounding box {(lb[0],lb[1],..,lb[n-1]), (ub[0],ub[1],..,ub[n-1])}.
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
int dspaces_get (dspaces_client_t client,
		const char *var_name,
        unsigned int ver, int size,
        int ndim, uint64_t *lb, uint64_t *ub, 
        void *data, int timeout);

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
dspaces_sub_t dspaces_sub(dspaces_client_t client,
        const char *var_name,
        unsigned int ver, int elem_size,
        int ndim, uint64_t *lb, uint64_t *ub,
        dspaces_sub_fn sub_cb, void *arg);
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
int dspaces_check_sub(dspaces_client_t client, dspaces_sub_t subh, int wait, int *result);

/**
 * @brief cancels pending subscription
 *
 * Stops a pending subscription from being processed, ignoring any future notification 
 * for this subscription.
 *
 * @param[in] client: dspaces client
 * @param[in] subh: handle for the subscription to be cancelled.
 *
 * @return zero for success, for failure (probably invalid subh)
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

#if defined(__cplusplus)
}
#endif

#endif
