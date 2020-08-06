/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers University
 *
 * See COPYRIGHT in top-level directory.
 */


#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include "mpi.h"

extern int test_put_run(char *listen_addr, int dims, 
	int* npdim, uint64_t *spdim, int timestep,
	size_t elem_size, int num_vars, MPI_Comm gcomm);

int parse_args(int argc, char** argv, 
	int *dims, int* npdim, uint64_t* spdim, int *timestep, 
	size_t *elem_size, int *num_vars)
{
	int i = 0, j = 0, count = 0;
	*dims = atoi(argv[2]);
	count = 2;

	if(argc < 2 + (*dims)*2 + 2){
		fprintf(stderr, "Wrong number of arguments!\n Usage: ./test_writer transport_layer dims np[0] ... np[dims-1] sp[0] ... sp[dims-1] timestep elem_size\n");
		return -1;
	}

	for(i = count + 1, j = 0; j < *dims; i++, j++){
		*(npdim+j) = atoi(argv[i]);
	}
	count += *dims;

	for(i = count + 1, j = 0; j < *dims; i++, j++){
		*(spdim+j) = strtoull(argv[i], NULL, 10); 
	}
	count += *dims;

	*timestep = atoi(argv[++count]);

	if(argc >= ++count + 1)
		*elem_size = atoi(argv[count]);
	else
		*elem_size = sizeof(double);

	if(argc >= ++count + 1)
		*num_vars = atoi(argv[count]);
	else
		*num_vars = 1;

	return 0;
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
    char *listen_addr = argv[1];

	if (parse_args(argc, argv, &dims, np, sp,
    		&timestep, &elem_size, &num_vars) != 0) {
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
		sp, timestep, elem_size, num_vars, gcomm);

	MPI_Barrier(gcomm);
	MPI_Finalize();

	return 0;	
err_out:
	fprintf(stderr, "error out!\n");
	return -1;	
}

