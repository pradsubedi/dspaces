/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers
 * University
 *
 * See COPYRIGHT in top-level directory.
 */

#include <dspaces-server.h>
#include <margo.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv)
{
    if(argc != 2) {
        fprintf(stderr, "Usage: %s <listen-address>\n", argv[0]);
        return -1;
    }

    char *listen_addr_str = argv[1];

    dspaces_provider_t s = dspaces_PROVIDER_NULL;

    int rank;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm gcomm = MPI_COMM_WORLD;

    int color = 1;
    MPI_Comm_split(MPI_COMM_WORLD, color, rank, &gcomm);

    int ret = dspaces_server_init(listen_addr_str, gcomm, &s);
    if(ret != 0)
        return ret;

    // make margo wait for finalize
    dspaces_server_fini(s);

    if(rank == 0) {
        fprintf(stderr, "Server is all done!\n");
    }

    MPI_Finalize();
    return 0;
}
