/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers University
 *
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <margo.h>
#include <dspaces-client.h>
#include "timer.h"
#include "mpi.h"

int main(int argc, char **argv)
{
    dspaces_client_t ds;
    int rank;
    char *listen_addr_str;

    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(rank == 0 && argc != 2) {
        fprintf(stderr, "Usage: terminator transport_string\n"); 
        return(-1);
    }

    listen_addr_str = argv[1];

    client_init(listen_addr_str, rank, &ds);

    if(rank == 0) {
        dspaces_kill(ds);
    }

    client_finalize(ds);            

    MPI_Finalize();

    return(0);

}
