/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers
 * University
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef __DSPACES_SERVER_H
#define __DSPACES_SERVER_H

#include <assert.h>
#include <dspaces-common.h>
#include <margo.h>
#include <mpi.h>
#include <stdio.h>

#if defined(__cplusplus)
extern "C" {
#endif

#define dspaces_ABT_POOL_DEFAULT ABT_POOL_NULL

typedef struct dspaces_provider *dspaces_provider_t;
#define dspaces_PROVIDER_NULL ((dspaces_provider_t)NULL)

/**
 * @brief Creates a MESSAGING server.
 *
 * @param[in] comm MPI Comminicator
 * @param[out] server MESSAGING server
 * @param[in] debug enable debugging
 * @return MESSAGING_SUCCESS or error code defined in messaging-common.h
 */
int dspaces_server_init(char *listen_addr_str, MPI_Comm comm,
                        dspaces_provider_t *server);

/**
 * @brief Waits for the dataspaces server to finish (be killed.)
 *
 * @param[in] server Messaging server
 *
 */
void dspaces_server_fini(dspaces_provider_t server);

#if defined(__cplusplus)
}
#endif

#endif
