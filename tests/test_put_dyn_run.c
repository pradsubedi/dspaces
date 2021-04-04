/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers
 * University
 *
 * See COPYRIGHT in top-level directory.
 */

#include "mpi.h"
#include "timer.h"
#include <dspaces.h>
#include <margo.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

//# of processors in fortran direction
static int np[10] = {0};
// block size per processor per direction
static uint64_t sp[10] = {0};
// global data dimensions
static uint64_t gdim[10] = {0};
// lower bound
static uint64_t lb[10];
// upper bound
static uint64_t ub[10];
// left neighbor
static int left;
// right neighbor
static int right;

//# of interations
static int timesteps_;
//# of processors in the application
static int npapp_;

static int rank_, nproc_;

static uint64_t off[10] = {0};

static struct timer timer_;

static MPI_Comm gcomm_;

static size_t elem_size_;

static double *allocate_nd(int dims)
{
    double *tmp = NULL;
    int i = 0;
    uint64_t size = 1;
    for(i = 0; i < dims; i++) {
        size *= (ub[i] - lb[i]) + 1;
    }
    tmp = (double *)malloc(elem_size_ * size);
    return tmp;
}

static void set_offset_nd(int rank, int dims)
{
    int i = 0, j = 0;
    for(i = 0; i < dims; i++) {
        int tmp = rank;
        for(j = 0; j < i; j++)
            tmp /= np[j];
        off[i] = tmp % np[i] * sp[i];
    }
}

static int generate_nd(double *mnd, unsigned int ts, int dims)
{
    // double value = 1.0*(rank_) + 0.0001*ts;
    double value = ts;
    int i;
    uint64_t mnd_size = 1;
    for(i = 0; i < dims; i++)
        mnd_size *= (ub[i] - lb[i]) + 1;
    mnd_size = mnd_size * elem_size_ / sizeof(double);
    for(i = 0; i < mnd_size; i++)
        *(mnd + i) = value;
    return 0;
}

static void update_bounds(unsigned int dyn[3])
{
    int send_left, send_right, recv_left, recv_right;
    int row_count;
    MPI_Request req[2];
    int i;

    send_left = send_right = 0;
    row_count = (ub[0] - lb[0]) + 1;

    for(i = 0; i < row_count; i++) {
        int x = rand() % 1000;
        if(x < dyn[0] && left > -1) {
            send_left++;
        } else if(x < (2 * dyn[0]) && right > -1) {
            send_right++;
        }
    }

    if(left > -1) {
        MPI_Isend(&send_left, 1, MPI_INT, left, 0, gcomm_, &req[0]);
        MPI_Recv(&recv_left, 1, MPI_INT, left, 0, gcomm_, MPI_STATUS_IGNORE);
        lb[0] -= recv_left - send_left;
    }

    if(right > -1) {
        MPI_Isend(&send_right, 1, MPI_INT, right, 0, gcomm_, &req[1]);
        MPI_Recv(&recv_right, 1, MPI_INT, right, 0, gcomm_, MPI_STATUS_IGNORE);
        ub[0] += recv_right - send_right;
    }

    MPI_Waitall(2, req, MPI_STATUSES_IGNORE);

    if(dyn[0] < dyn[1]) {
        dyn[0] += dyn[2];
    } else if(dyn[0] > dyn[1]) {
        dyn[0] -= dyn[2];
    }
}

static int couple_write_nd(dspaces_client_t ndph, unsigned int ts, int num_vars,
                           int dims, int local_mode, unsigned int dyn[3])
{
    double **data_tab = (double **)malloc(sizeof(double *) * num_vars);
    char var_name[128];
    int i;
    int err;
    for(i = 0; i < num_vars; i++)
        data_tab[i] = NULL;

    // set_offset_nd(rank_, dims);
    int elem_size = elem_size_;
    double tm_st, tm_end, tm_max, tm_diff;
    int root = 0;

    // allocate data
    double *data = NULL;
    for(i = 0; i < num_vars; i++) {
        data = allocate_nd(dims);
        if(data == NULL) {
            fprintf(stderr, "%s(): allocate_nd() failed.\n", __func__);
            return -1; // TODO: free buffers
        }

        generate_nd(data, ts, dims);
        data_tab[i] = data;
    }

    if(rank_ == root) {
        err = dspaces_put_meta(ndph, "mnd", ts, &elem_size, sizeof(elem_size));
        if(err != 0) {
            fprintf(stderr, "dspaces_put_meta returned error %d", err);
            return (err);
        }
    }

    MPI_Barrier(gcomm_);
    tm_st = timer_read(&timer_);

    for(i = 0; i < num_vars; i++) {
        sprintf(var_name, "mnd_%d", i);
        if(!local_mode)
            err = dspaces_put(ndph, var_name, ts, elem_size, dims, lb, ub,
                              data_tab[i]);
        else
            err = dspaces_put_local(ndph, var_name, ts, elem_size, dims, lb, ub,
                                    data_tab[i]);
        if(err != 0) {
            fprintf(stderr, "dspaces_put returned error %d", err);
            return err;
        }
    }
    tm_end = timer_read(&timer_);

    tm_diff = tm_end - tm_st;
    MPI_Reduce(&tm_diff, &tm_max, 1, MPI_DOUBLE, MPI_MAX, root, gcomm_);

    if(rank_ == root) {
        fprintf(stdout, "TS= %u write MAX time= %lf\n", ts, tm_max);
    }

    for(i = 0; i < num_vars; i++) {
        if(data_tab[i]) {
            free(data_tab[i]);
        }
    }
    free(data_tab);

    update_bounds(dyn);

    return 0;
}

int set_gdims(dspaces_client_t client, int num_vars, int ndims, uint64_t *dims)
{
    char var_name[128];
    int i, ret;

    for(i = 0; i < num_vars; i++) {
        sprintf(var_name, "mnd_%d", i);
        dspaces_define_gdim(client, var_name, ndims, dims);
    }
    if(rank_ == 0) {
        ret = dspaces_put_meta(client, "gdim", 0, dims, sizeof(*dims) * 10);
        if(ret != 0) {
            fprintf(stderr, "dspaces_put_meta() failed with %d.\n", ret);
            return (ret);
        }
    }

    return (0);
}

int test_put_dyn_run(int ndims, int *npdim, uint64_t *spdim, int timestep,
                     unsigned int dyn[3], size_t elem_size, int num_vars,
                     int local_mode, int terminate, unsigned int cwait[2],
                     MPI_Comm gcomm)
{
    elem_size_ = elem_size;
    timesteps_ = timestep;
    unsigned int wait_time;
    int periods[10] = {0};
    int coords[10] = {0};
    int i, ret;

    dspaces_client_t ndcl = dspaces_CLIENT_NULL;

    ret = 0;

    for(i = 0; i < ndims; i++) {
        np[i] = npdim[i];
        sp[i] = spdim[i];
        gdim[i] = np[i] * sp[i];
    }

    timer_init(&timer_, 1);
    timer_start(&timer_);

    double tm_st, tm_end;
    tm_st = timer_read(&timer_);

    MPI_Cart_create(gcomm, ndims, np, periods, 0, &gcomm_);
    MPI_Comm_rank(gcomm_, &rank_);
    MPI_Cart_coords(gcomm_, rank_, ndims, coords);
    for(i = 0; i < ndims; i++) {
        lb[i] = sp[i] * coords[i];
        ub[i] = (lb[i] + sp[i]) - 1;
    }
    if(coords[0] > 0) {
        coords[0]--;
        MPI_Cart_rank(gcomm_, coords, &left);
    } else {
        left = -1;
    }

    if(coords[0] < (np[0] - 1)) {
        coords[0]++;
        MPI_Cart_rank(gcomm_, coords, &right);
    } else {
        right = -1;
    }

    ret = dspaces_init_mpi(gcomm_, &ndcl);
    if(ret != dspaces_SUCCESS) {
        fprintf(stderr, "dspaces_init_mpi() failed with %d.\n", ret);
        goto error;
    }

    tm_end = timer_read(&timer_);
    fprintf(stdout, "TIMING_PERF Init_server_connection peer %d time= %lf\n",
            rank_, tm_end - tm_st);

    ret = set_gdims(ndcl, num_vars, ndims, gdim);
    if(ret != 0) {
        ret = -1;
        goto error;
    }

    MPI_Comm_size(gcomm_, &nproc_);

    unsigned int ts;
    for(ts = 1; ts <= timesteps_; ts++) {
        wait_time = 1000 * (cwait[0] + (rand() % (cwait[1] - cwait[0])));
        usleep(wait_time);
        ret = couple_write_nd(ndcl, ts, num_vars, ndims, local_mode, dyn);
        if(ret != 0) {
            ret = -1;
            goto error;
        }
    }

    MPI_Barrier(gcomm_);
    if(rank_ == 0) {
        fprintf(stdout, "%s(): done\n", __func__);
    }
    tm_st = timer_read(&timer_);

    if(rank_ == 0 && terminate) {
        fprintf(stderr, "Writer sending kill signal to server.\n");
        dspaces_kill(ndcl);
    }

    dspaces_fini(ndcl);

    tm_end = timer_read(&timer_);

    fprintf(stdout, "TIMING_PERF Close_server_connection peer %d time= %lf\n",
            rank_, tm_end - tm_st);

    return 0;

error:

    dspaces_fini(ndcl);

    return ret;
}
