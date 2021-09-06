/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers
 * University
 *
 * See COPYRIGHT in top-level directory.
 */

#include "mpi.h"
#include "timer.h"
#include <dspaces.h>
#include <errno.h>
#include <inttypes.h>
#include <margo.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

//# of processors in fortran direction
static int np[10] = {0};
// block size per processor per direction
static uint64_t sp[10] = {0};
// global dimensions
static uint64_t gdim[10] = {0};
//# of interations
static int timesteps_;

static int rank_, nproc_;

static uint64_t off[10] = {0};

static struct timer timer_;

static MPI_Comm gcomm_;

static size_t elem_size_;

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

int check_data(const char *var_name, double *buf, int num_elem, int rank,
               int ts)
{
    double max, min, sum, avg;
    int i;
    int cnt = 0;

    if(num_elem <= 0) {
        return -EINVAL;
    }
    max = min = sum = buf[0];
    for(i = 0; i < num_elem; i++) {
        if(max < buf[i])
            max = buf[i];
        if(min > buf[i])
            min = buf[i];
        sum += buf[i];
        if(buf[i] != ts) {
            cnt++;
        }
    }
    avg = sum / num_elem;
    if(cnt > 0) {
        fprintf(stderr,
                "%s(): var= %s, rank= %d, ts= %d, avg=%lf"
                "error elem cnt= %d, total elem= %d\n",
                __func__, var_name, rank, ts, avg, cnt, num_elem);
    }

    free(buf);

    return cnt;
}

int check_data_cb(dspaces_client_t client, struct dspaces_req *req, void *rankv)
{
    int rank = *(int *)rankv;
    int num_elem = 1;
    int i;

    (void)client;
    fprintf(stderr, "executing %s on rank %d for version %d.\n", __func__, rank,
            req->ver);

    for(i = 0; i < req->ndim; i++) {
        num_elem *= (req->ub[i] - req->lb[i]) + 1;
    }

    return (check_data(req->var_name, req->buf, num_elem, rank, req->ver));
}

static int couple_sub_nd(dspaces_client_t client, unsigned int ts, int num_vars,
                         int dims, dspaces_sub_t *subh)
{
    double **data_tab = (double **)malloc(sizeof(double *) * num_vars);
    char var_name[128];
    int i;
    int ret = 0;
    uint64_t dims_size = 1;
    int elem_size = elem_size_;
    uint64_t lb[10] = {0}, ub[10] = {0};
    double tm_st, tm_end, tm_max, tm_diff;
    int root = 0;

    for(i = 0; i < num_vars; i++) {
        data_tab[i] = NULL;
    }

    set_offset_nd(rank_, dims);
    for(i = 0; i < dims; i++) {
        lb[i] = off[i];
        ub[i] = off[i] + sp[i] - 1;
        dims_size *= sp[i];
    }

    MPI_Barrier(gcomm_);
    tm_st = timer_read(&timer_);

    for(i = 0; i < num_vars; i++) {
        sprintf(var_name, "mnd_%d", i);
        *subh = dspaces_sub(client, var_name, ts, elem_size, dims, lb, ub,
                            check_data_cb, &rank_);
        if(*subh == DSPACES_SUB_FAIL) {
            fprintf(stderr, "dspaces_sub() failed.\n");
            return (-1);
        }
    }
    tm_end = timer_read(&timer_);

    tm_diff = tm_end - tm_st;
    MPI_Reduce(&tm_diff, &tm_max, 1, MPI_DOUBLE, MPI_MAX, root, gcomm_);

    if(rank_ == root) {
        fprintf(stdout, "TS= %u read MAX time= %lf\n", ts, tm_max);
    }

    free(data_tab);

    return ret;
}

int get_gdims(dspaces_client_t client, int num_vars, int ndims, MPI_Comm gcomm)
{
    char var_name[128];
    int mver;
    unsigned int mlen;
    void *mdata;
    int i, ret;
    int root = 0;

    if(rank_ == root) {
        ret = dspaces_get_meta(client, "gdim", META_MODE_NEXT, -1, &mver,
                               &mdata, &mlen);
        if(ret != 0) {
            fprintf(stderr, "dspaces_get_meta() failed with %d\n", ret);
            return (ret);
        }
        if(mver != 0) {
            fprintf(stderr, "gdim metadata is version %d, shoud be 0!\n", mver);
            return (-1);
        }
        if(mlen != sizeof(*gdim) * 10) {
            fprintf(stderr,
                    "gdim metadata is the wrong size. Expected %li bytes and "
                    "got %i!\n",
                    sizeof(*gdim) * 1, mlen);
            return (0);
        }
        memcpy(gdim, mdata, sizeof(*gdim) * 10);
        free(mdata);
        fprintf(stdout, "Global writer dimensions: (");
        for(i = 0; i < 10; i++) {
            if(i < ndims) {
                fprintf(stdout, "%" PRIu64 ", ", gdim[i]);
            } else if(gdim[i] != 0) {
                fprintf(stderr,
                        "ndim mismatch between writer and reader, or metadata "
                        "corruption. Non-zero entry in dimension %d.\n",
                        i);
                return (-1);
            }
        }
        fprintf(stdout, ")\n");
    }

    MPI_Bcast(gdim, sizeof(*gdim) * 10, MPI_BYTE, root, gcomm);

    for(i = 0; i < num_vars; i++) {
        sprintf(var_name, "mnd_%d", i);
        dspaces_define_gdim(client, var_name, ndims, gdim);
    }

    return (0);
}

int test_sub_run(int ndims, int *npdim, uint64_t *spdim, int timestep,
                 size_t elem_size, int num_vars, int terminate, MPI_Comm gcomm)
{
    dspaces_sub_t *sub_handles;
    gcomm_ = gcomm;
    elem_size_ = elem_size;
    timesteps_ = timestep;
    int result;

    dspaces_client_t ndcl = dspaces_CLIENT_NULL;

    int err, ret = 0;

    int i;
    for(i = 0; i < ndims; i++) {
        np[i] = npdim[i];
        sp[i] = spdim[i];
    }

    timer_init(&timer_, 1);
    timer_start(&timer_);

    double tm_st, tm_end;
    tm_st = timer_read(&timer_);

    MPI_Comm_rank(gcomm_, &rank_);
    MPI_Comm_size(gcomm_, &nproc_);

    ret = dspaces_init(rank_, &ndcl);
    if(ret != dspaces_SUCCESS) {
        fprintf(stderr, "%s: dspaces_init() failed with %d.\n", __func__, ret);
    }

    tm_end = timer_read(&timer_);
    fprintf(stdout, "TIMING_PERF Init_server_connection peer %d time= %lf\n",
            rank_, tm_end - tm_st);

    get_gdims(ndcl, num_vars, ndims, gcomm);

    sub_handles = malloc(sizeof(*sub_handles) * timesteps_);

    unsigned int ts;
    for(ts = 1; (int)ts <= timesteps_; ts++) {
        err = couple_sub_nd(ndcl, ts, num_vars, ndims, &sub_handles[ts - 1]);
        if(err != 0) {
            fprintf(stderr, "couple_sub_nd failed on ts %d with %d.\n", ts,
                    err);
            ret = -1;
        }
    }

    for(ts = 1; (int)ts <= timesteps_; ts++) {
        err = dspaces_check_sub(ndcl, sub_handles[ts - 1], 1, &result);
        if((err != DSPACES_SUB_DONE) || result > 0) {
            fprintf(stderr, "subscription tailed for ts %d with %d.\n", ts,
                    err);
            ret = -1;
        }
    }

    MPI_Barrier(gcomm_);

    if(rank_ == 0) {
        fprintf(stdout, "%s(): done\n", __func__);
    }
    tm_st = timer_read(&timer_);

    if(rank_ == 0 && terminate) {
        fprintf(stderr, "Subscriber sending kill signal to server.\n");
        dspaces_kill(ndcl);
    }

    dspaces_fini(ndcl);
    tm_end = timer_read(&timer_);

    fprintf(stdout, "TIMING_PERF Close_server_connection peer %d time= %lf\n",
            rank_, tm_end - tm_st);

    return ret;

}
