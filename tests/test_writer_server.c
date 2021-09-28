/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers
 * University
 *
 * See COPYRIGHT in top-level directory.
 */

#include <dspaces-server.h>
#include <dspaces.h>
#include <margo.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int test_put_run(int dims, int *npdim, uint64_t *spdim, int timestep,
                        size_t elem_size, int num_vars, int local_mode,
                        int terminate, int nonblock, MPI_Comm gcomm);

void print_usage()
{
    fprintf(
        stderr,
        "Usage: test_writer_server <listen-address> <dims> np[0] .. np[dims-1] "
        "sp[0] ... sp[dims-1] "
        "<timesteps> [-s <elem_size>] [-m (server|local)] [-c <var_count>] "
        "[-t]\n"
        "   listen-address    - the address or fabric to which the server "
        "should attach\n"
        "   dims              - number of data dimensions. Must be at least "
        "one\n"
        "   np[i]             - the number of processes in the ith dimension. "
        "The product of np[0],...,np[dim-1] must be the number of MPI ranks\n"
        "   sp[i]             - the per-process data size in the ith "
        "dimension\n"
        "   timesteps         - the number of timestep iterations written\n"
        "   -s <elem_size>    - the number of bytes in each element. Defaults "
        "to 8\n"
        "   -m (server|local) - the storage mode (stage to server or stage in "
        "process memory). Defaults to server\n"
        "   -c <var_count>    - the number of variables written in each "
        "iteration. Defaults to one\n"
        "   -t                - send server termination after writing is "
        "complete\n"
        "   -i                - use nonblocking puts (dspaces_iput)\n");
}

int parse_args(int argc, char **argv, int *dims, int *npdim, uint64_t *spdim,
               int *timestep, size_t *elem_size, int *num_vars,
               int *store_local, int *terminate, int *nonblock)
{
    char **argp;
    int i;

    *elem_size = 8;
    *num_vars = 1;
    *store_local = 0;
    *dims = 1;
    *terminate = 0;
    *nonblock = 0;
    if(argc > 1) {
        *dims = atoi(argv[2]);
    }

    if(argc < 4 + (*dims * 2)) {
        fprintf(stderr, "Not enough arguments.\n");
        print_usage();
        return (-1);
    }

    argp = &argv[3];

    for(i = 0; i < *dims; i++) {
        npdim[i] = atoi(*argp);
        argp++;
    }
    for(i = 0; i < *dims; i++) {
        spdim[i] = strtoull(*argp, NULL, 10);
        argp++;
    }

    *timestep = atoi(*argp);
    argp++;

    while(argp < (argv + argc)) {
        if(strcmp(*argp, "-s") == 0) {
            if(argp == ((argv + argc) - 1)) {
                fprintf(stderr, "%s takes an argument.\n", *argp);
                print_usage();
                return (-1);
            }
            *elem_size = atoi(*(argp + 1));
            argp += 2;
        } else if(strcmp(*argp, "-m") == 0) {
            if(argp == ((argv + argc) - 1)) {
                fprintf(stderr, "%s takes an argument.\n", *argp);
                print_usage();
                return (-1);
            }
            if(strcmp(*(argp + 1), "local") == 0) {
                *store_local = 1;
            }
            argp += 2;
        } else if(strcmp(*argp, "-c") == 0) {
            if(argp == ((argv + argc) - 1)) {
                fprintf(stderr, "%s takes an argument.\n", *argp);
                print_usage();
                return (-1);
            }
            *num_vars = atoi(*(argp + 1));
            argp += 2;
        } else if(strcmp(*argp, "-t") == 0) {
            *terminate = 1;
            argp++;
        } else if(strcmp(*argp, "-i") == 0) {
            *nonblock = 1;
            argp++;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", *argp);
            print_usage();
            return (-1);
        }
    }

    if(argp < (argv + argc)) {
        fprintf(stderr, "Warning: ignoring extraneous argument '%s'.\n", *argp);
    }

    return (0);
}

int main(int argc, char **argv)
{
    char *listen_addr_str;
    int dims, timestep, num_vars, local_mode, terminate, nonblock, npapp,
        nprocs;
    int np[10] = {0};
    uint64_t sp[10] = {0};
    size_t elem_size;
    int rank;
    int color;
    MPI_Comm gcomm;
    int i, ret;

    if(parse_args(argc, argv, &dims, np, sp, &timestep, &elem_size, &num_vars,
                  &local_mode, &terminate, &nonblock) != 0) {
        return (-1);
    }

    listen_addr_str = argv[1];

    dspaces_provider_t s = dspaces_PROVIDER_NULL;

    MPI_Init(&argc, &argv);
    gcomm = MPI_COMM_WORLD;

    color = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_split(MPI_COMM_WORLD, color, rank, &gcomm);
    MPI_Comm_size(gcomm, &nprocs);
    MPI_Comm_rank(gcomm, &rank);

    npapp = 1;
    for(i = 0; i < dims; i++) {
        npapp *= np[i];
    }
    if(npapp != nprocs) {
        fprintf(stderr,
                "Product of np[i] args must equal number of MPI processes!\n");
        print_usage();
        return (-1);
    }

    ret = dspaces_server_init(listen_addr_str, gcomm, &s);
    if(ret != 0)
        return ret;

    test_put_run(dims, np, sp, timestep, elem_size, num_vars, local_mode,
                 terminate, nonblock, gcomm);

    MPI_Barrier(gcomm);
    if(rank == 0) {
        fprintf(stderr, "Writer is all done!\n");
    }

    // make margo wait for finalize
    dspaces_server_fini(s);

    MPI_Barrier(gcomm);
    if(rank == 0) {
        fprintf(stderr, "Server is all done!\n");
    }

    MPI_Finalize();
    return 0;
}
