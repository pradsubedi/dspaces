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

//# of processors in fortran direction
static int np[10] = {0};
//block size per processor per direction
static uint64_t sp[10] = {0};
//# of interations
static int timesteps_;
//# of processors in the application
static int npapp_;

static int rank_, nproc_;

static uint64_t off[10] = {0};

static struct timer timer_;

static MPI_Comm gcomm_;

static size_t elem_size_;

static double* allocate_nd(int dims)
{
        double* tmp = NULL;
        int i = 0;
        uint64_t size = 1;
        for(i = 0; i < dims; i++){
                size *= sp[i];
        }
        tmp = (double*)malloc(elem_size_ * size);
        return tmp;
}

static void set_offset_nd(int rank, int dims)
{
	int i = 0, j = 0;
	for(i = 0; i < dims; i++){
		int tmp = rank;
		for(j = 0; j < i; j++)
			tmp /= np[j];
		off[i] = tmp % np[i] * sp[i];
	}
}


static int generate_nd(double *mnd, unsigned int ts, int dims)
{
    //double value = 1.0*(rank_) + 0.0001*ts;
	double value = ts;
    int i;
	uint64_t mnd_size = 1;
    for(i = 0; i < dims; i++)
        mnd_size *= sp[i];
	mnd_size = mnd_size * elem_size_ / sizeof(double);
    for(i = 0; i < mnd_size; i++)
        *(mnd+i) = value;
    return 0;
}

static int couple_write_nd(dspaces_client_t ndph, unsigned int ts, int num_vars, int dims)
{
	double **data_tab = (double **)malloc(sizeof(double *) * num_vars);
	char var_name[128];
	int i;
	int err;
	for(i = 0; i < num_vars; i++)
		data_tab[i] = NULL;

	set_offset_nd(rank_, dims);
	int elem_size = elem_size_;
	uint64_t lb[10] = {0}, ub[10] = {0};
	for(i = 0; i < dims; i++){
		lb[i] = off[i];
		ub[i] = off[i] + sp[i] - 1;
	}
	double tm_st, tm_end, tm_max, tm_diff;
	int root = 0;

	//allocate data
	double *data = NULL;
	for(i = 0; i < num_vars; i++){
		data = allocate_nd(dims);
		if(data == NULL){
			fprintf(stderr, "%s(): allocate_nd() failed.\n", __func__);
            return -1; // TODO: free buffers
		}
		
		generate_nd(data, ts, dims);
		data_tab[i] = data;
	}

	MPI_Barrier(gcomm_);
    tm_st = timer_read(&timer_);

	for(i = 0; i < num_vars; i++){
		sprintf(var_name, "mnd_%d", i);
		//if(rank_%2 == 0)
			err = dspaces_put(ndph, var_name, ts, elem_size, dims, lb, ub, data_tab[i]);
		//else
		//	err = dspaces_put_local(ndph, var_name, ts, elem_size, dims, lb, ub, data_tab[i]);
		if(err!=0){
			fprintf(stderr, "dspaces_put returned error %d", err);
			return err;
		}
		
	}
	tm_end = timer_read(&timer_);


	tm_diff = tm_end-tm_st;
	MPI_Reduce(&tm_diff, &tm_max, 1, MPI_DOUBLE, MPI_MAX, root, gcomm_);

    if (rank_ == root) {
        fprintf(stdout, "TS= %u write MAX time= %lf\n",
                ts, tm_max);
    }

	for (i = 0; i < num_vars; i++) {
        if (data_tab[i]) {
            free(data_tab[i]);
        }
    }
    free(data_tab);

    return 0;
}

int test_put_run(char *listen_addr_str, int ndims, int* npdim, 
	uint64_t *spdim, int timestep, size_t elem_size, int num_vars, 
	MPI_Comm gcomm)
{
	gcomm_ = gcomm;
	elem_size_ = elem_size;
	timesteps_ = timestep;

	dspaces_client_t ndcl = dspaces_CLIENT_NULL;

    int ret = 0;


	int i;
	for(i = 0; i < ndims; i++){
        np[i] = npdim[i];
        sp[i] = spdim[i];
	}

	timer_init(&timer_, 1);
    timer_start(&timer_);

	double tm_st, tm_end;
	tm_st = timer_read(&timer_);

	MPI_Comm_rank(gcomm_, &rank_);
	
    ret = client_init(listen_addr_str, rank_, &ndcl);

	tm_end = timer_read(&timer_);
	fprintf(stdout, "TIMING_PERF Init_server_connection peer %d time= %lf\n", rank_, tm_end-tm_st);

    MPI_Comm_size(gcomm_, &nproc_);


	unsigned int ts;
	for(ts = 1; ts <= timesteps_; ts++){
		ret = couple_write_nd(ndcl, ts, num_vars, ndims);
		if(ret!=0){
			ret = -1;
			goto error;
		}

	}

	MPI_Barrier(gcomm_);
	if(rank_ == 0){
		fprintf(stdout, "%s(): done\n", __func__);
	}
	tm_st = timer_read(&timer_);

    client_finalize(ndcl);

	tm_end = timer_read(&timer_);


	fprintf(stdout, "TIMING_PERF Close_server_connection peer %d time= %lf\n", rank_, tm_end-tm_st);


        return 0;

 error:

    client_finalize(ndcl);

    return ret;

}
