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
//# of processors in the application
static int npapp_;

static int rank_, nproc_;

static uint64_t off[10] = {0};

static struct timer timer_;

static MPI_Comm gcomm_;

static size_t elem_size_;

static char transport_type_str_[256];

static double *allocate_nd(int dims)
{
    double *tmp = NULL;
    int i = 0;
    uint64_t size = 1;
    for(i = 0; i < dims; i++) {
        size *= sp[i];
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
                "%s(): var= %s, rank= %d, ts= %d, "
                "error elem cnt= %d, total elem= %d\n",
                __func__, var_name, rank, ts, cnt, num_elem);
    }

    return cnt;
}

static int couple_read_nd(dspaces_client_t client, unsigned int ts,
                          int num_vars, int dims, int allocate)
{
    double **data_tab = (double **)malloc(sizeof(double *) * num_vars);
    char var_name[128];
    int ret = 0;
    int err = 0;
    int i;
    for(i = 0; i < num_vars; i++) {
        data_tab[i] = NULL;
    }

    set_offset_nd(rank_, dims);
    uint64_t dims_size = 1;
    int elem_size = elem_size_;
    void *meta;
    int meta_elem_size;
    unsigned int meta_len;
    int next_ver;
    uint64_t lb[10] = {0}, ub[10] = {0};
    for(i = 0; i < dims; i++) {
        lb[i] = off[i];
        ub[i] = off[i] + sp[i] - 1;
        dims_size *= sp[i];
    }
    double tm_st, tm_end, tm_max, tm_diff;
    int root = 0;

    if(elem_size == 0) {
        fprintf(stdout, "learning elem_size from metadata.\n");
        err = dspaces_get_meta(client, "mnd", META_MODE_NEXT, ts - 1, &next_ver,
                               &meta, &meta_len);
        if(err != 0) {
            fprintf(stderr, "dspaces_get_meta() returned error %d\n", err);
            return err;
        }
        if(next_ver != ts) {
            fprintf(
                stderr,
                "Missing metadata step! Expected version %i, got version %i\n",
                ts, next_ver);
            return -1;
        }
        if(meta_len != sizeof(meta_elem_size)) {
            fprintf(
                stderr,
                "Metadata is misformated. Expected %zi bytes, got %d bytes.\n",
                sizeof(meta_elem_size), meta_len);
            return -1;
        }

        meta_elem_size = *(int *)meta;
        elem_size = meta_elem_size;
        elem_size_ = elem_size;
        fprintf(stdout, "elem_size is %d.\n", elem_size);
    }

    // allocate data
    double *data = NULL;
    if(!allocate) {
        for(i = 0; i < num_vars; i++) {
            data = allocate_nd(dims);
            if(data == NULL) {
                fprintf(stderr, "%s(): allocate_nd() failed.\n", __func__);
                return -1; // TODO: free buffers
            }
            memset(data, 0, elem_size_ * dims_size);
            data_tab[i] = data;
        }
    }

    MPI_Barrier(gcomm_);
    tm_st = timer_read(&timer_);

    for(i = 0; i < num_vars; i++) {
        sprintf(var_name, "mnd_%d", i);
        if(allocate) {
            err = dspaces_aget(client, var_name, ts, dims, lb, ub,
                               (void **)&data_tab[i], -1);
        } else {
            err = dspaces_get(client, var_name, ts, elem_size, dims, lb, ub,
                              data_tab[i], -1);
        }
        if(err != 0) {
            fprintf(stderr, "dspaces_get() returned error %d\n", err);
            return err;
        }
    }
    tm_end = timer_read(&timer_);

    tm_diff = tm_end - tm_st;
    MPI_Reduce(&tm_diff, &tm_max, 1, MPI_DOUBLE, MPI_MAX, root, gcomm_);

    if(rank_ == root) {
        fprintf(stdout, "TS= %u read MAX time= %lf\n", ts, tm_max);
    }

    for(i = 0; i < num_vars; i++) {
        sprintf(var_name, "mnd_%d", i);
        err = check_data(var_name, data_tab[i],
                         dims_size * elem_size_ / sizeof(double), rank_, ts);
        if(err > 0) {
            ret = -EIO;
        }
        if(data_tab[i]) {
            free(data_tab[i]);
        }
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

int test_get_run(int ndims, int *npdim, uint64_t *spdim, int timestep,
                 size_t elem_size, int num_vars, int terminate, int allocate,
                 MPI_Comm gcomm)
{
    gcomm_ = gcomm;
    elem_size_ = elem_size;
    timesteps_ = timestep;

    dspaces_client_t ndcl = dspaces_CLIENT_NULL;

    hg_return_t hret = HG_SUCCESS;
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
        fprintf(stderr, "dspaces_init() failed with %d.\n", ret);
        goto error;
    }

    tm_end = timer_read(&timer_);
    fprintf(stdout, "TIMING_PERF Init_server_connection peer %d time= %lf\n",
            rank_, tm_end - tm_st);

    //get_gdims(ndcl, num_vars, ndims, gcomm);

    unsigned int ts;
    for(ts = 1; ts <= timesteps_; ts++) {
        err = couple_read_nd(ndcl, ts, num_vars, ndims, allocate);
        if(err != 0) {
            ret = -1;
        }
    }

    MPI_Barrier(gcomm_);

    if(rank_ == 0) {
        fprintf(stdout, "%s(): done\n", __func__);
    }
    tm_st = timer_read(&timer_);

    if(rank_ == 0 && terminate) {
        fprintf(stderr, "Reader sending kill signal to server.\n");
        dspaces_kill(ndcl);
    }

    dspaces_fini(ndcl);
    tm_end = timer_read(&timer_);

    fprintf(stdout, "TIMING_PERF Close_server_connection peer %d time= %lf\n",
            rank_, tm_end - tm_st);

    return ret;

error:
    dspaces_fini(ndcl);

    return ret;
}
