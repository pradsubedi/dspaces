/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers University
 *
 * See COPYRIGHT in top-level directory.
 */


#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"

extern int test_put_run(char *listen_addr, int dims, 
	int* npdim, uint64_t *spdim, int timestep,
	size_t elem_size, int num_vars, int local_mode, MPI_Comm gcomm);

void print_usage()
{
    fprintf(stderr, "Usage: test_writer <transport> <dims> np[0] .. np[dims-1] sp[0] ... sp[dims-1] <timesteps> [-s <elem_size>] [-m (server|local)] [-c <var_count>]\n"
"   transport         - mercury transport string (e.g. sockets, sm, verbs, etc.)\n"
"   dims              - number of data dimensions. Must be at least one\n"
"   np[i]             - the number of processes in the ith dimension. The product of np[0],...,np[dim-1] must be the number of MPI ranks\n"
"   sp[i]             - the per-process data size in the ith dimension\n"
"   timesteps         - the number of timestep iterations written\n"
"   -s <elem_size>    - the number of bytes in each element. Defaults to 8\n"
"   -m (server|local) - the storage mode (stage to server or stage in process memory). Defaults to server\n"
"   -c <var_count>    - the number of variables written in each iteration. Defaults to one\n");
}

int parse_args(int argc, char **argv,
                int *dims, int *npdim, uint64_t *spdim, int *timestep,
                size_t *elem_size, int *num_vars, int *store_local)
{
    char **argp;
    int i;


    *elem_size = 8;
    *num_vars = 1;
    *store_local = 0;
    *dims = 1;
    if(argc > 2) {
        *dims = atoi(argv[2]);    
    }

    if(argc < 4 + (*dims * 2)) {
        fprintf(stderr, "Not enough arguments.\n");
        print_usage();
        return(-1);
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

    while(argp < ((argv + argc) - 1)) {
        if(strcmp(*argp, "-s") == 0) {
            *elem_size = atoi(*(argp+1));
            argp += 2;
        } else if(strcmp(*argp, "-m") == 0) {
            if(strcmp(*(argp+1), "local") == 0) {
                *store_local = 1;
            }
            argp += 2;            
        } else if(strcmp(*argp, "-c") == 0) {
            *num_vars = atoi(*(argp+1));
            argp += 2;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", *argp);
            print_usage();
            return(-1);
        }
    }

    if(argp < (argv + argc)) {
        fprintf(stderr, "Warning: ignoring extraneous argument '%s'.\n", *argp);
    }

    return(0);
}

int main(int argc, char **argv)
{
	int err;
	int nprocs, rank;
	MPI_Comm gcomm;

    int npapp; // number of application processes
    int np[10] = {0};	//number of processes in each dimension
    uint64_t sp[10] = {0}; //block size per process in each dimension
    int timestep; // number of iterations
    int dims; // number of dimensions
    size_t elem_size; // Optional: size of one element in the global array. Default value is 8 (bytes).
    int num_vars; // Optional: number of variables to be shared in the testing. Default value is 1.
    int local_mode;
    char *listen_addr = argv[1];

	if (parse_args(argc, argv, &dims, np, sp,
    		&timestep, &elem_size, &num_vars, &local_mode) != 0) {
		goto err_out;
	}

	// Using SPMD style programming
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Barrier(MPI_COMM_WORLD);
	gcomm = MPI_COMM_WORLD;

	int color = 1;
	MPI_Comm_split(MPI_COMM_WORLD, color, rank, &gcomm);

	// Run as data writer

	test_put_run(listen_addr, dims, np,
		sp, timestep, elem_size, num_vars, local_mode, gcomm);

	MPI_Barrier(gcomm);
	MPI_Finalize();

	return 0;	
err_out:
	fprintf(stderr, "error out!\n");
	return -1;	
}

