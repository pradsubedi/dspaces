/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers University
 *
 * See COPYRIGHT in top-level directory.
 */


#ifndef __DSPACES_SERVER_H
#define __DSPACES_SERVER_H

#include <margo.h>
#include <dspaces-common.h>
#include <assert.h>
#include <stdio.h>
#include <ssg.h>
#include <mpi.h>

#if defined(__cplusplus)
extern "C" {
#endif

#define dspaces_ABT_POOL_DEFAULT ABT_POOL_NULL

typedef struct dspaces_provider* dspaces_provider_t;
#define dspaces_PROVIDER_NULL ((dspaces_provider_t)NULL)

/**
 * @brief Creates a MESSAGING server.
 *
 * @param[in] comm MPI Comminicator
 * @param[out] server MESSAGING server
  * @return MESSAGING_SUCCESS or error code defined in messaging-common.h
 */
int server_init(char *listen_addr_str, MPI_Comm comm, dspaces_provider_t* server);
	

/**
 * @brief Destroys the Messaging server and deregisters its RPC.
 *
 * @param[in] server Messaging server
 *
 * @return MESSAGING_SUCCESS or error code defined in messaging-common.h
 */
int server_destroy(dspaces_provider_t server);


#if defined(__cplusplus)
}
#endif

#endif
