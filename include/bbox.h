/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers University
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef __BBOX_H_
#define __BBOX_H_

#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include "util.h"

#define max(a,b) (a) > (b) ? (a):(b)
#define min(a,b) (a) < (b) ? (a):(b)

#define BBOX_MAX_NDIM 10

enum bb_dim {
        bb_x = 0,
        bb_y = 1,
        bb_z = 2
};

struct coord {
        uint64_t c[BBOX_MAX_NDIM];
};

struct bbox {
        int num_dims;
        struct coord lb, ub;
};

struct intv {
        uint64_t lb, ub;
};

uint64_t bbox_dist(struct bbox *, int);
void bbox_divide(struct bbox *b0, struct bbox *b_tab);
int bbox_include(const struct bbox *, const struct bbox *);
int bbox_does_intersect(const struct bbox *, const struct bbox *);
void bbox_intersect(struct bbox *, const struct bbox *, struct bbox *);
int bbox_equals(const struct bbox *, const struct bbox *);

uint64_t bbox_volume(struct bbox *);
void bbox_to_intv(const struct bbox *, uint64_t, int, struct intv **, int *);
void bbox_to_intv2(const struct bbox *, uint64_t, int, struct intv **, int *);
void bbox_to_origin(struct bbox *, const struct bbox *);

int intv_do_intersect(struct intv *, struct intv *);
uint64_t intv_size(struct intv *);

void bbox_divide_in2_ondim(const struct bbox *b0, struct bbox *b_tab, int dim);

static uint64_t next_pow_2(uint64_t n)
{
        uint64_t i;

        if (n < 0)
                return 0;

        i = ~(~0ULL >> 1);
        while (i && !(i&n)) {
                i = i >>1;
        }

        i = i << 1;

        return i;
}


extern char *str_append_const(char *, const char *);
extern char *str_append(char *, char *);

void coord_print(struct coord *c, int num_dims);
char * coord_sprint(const struct coord *c, int num_dims);
void bbox_print(struct bbox *bb);
char * bbox_sprint(const struct bbox *bb);
#endif /* __BBOX_H_ */
