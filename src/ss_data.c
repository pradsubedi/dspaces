/*
 * Copyright (c) 2009, NSF Cloud and Autonomic Computing Center, Rutgers
 * University All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * - Neither the name of the NSF Cloud and Autonomic Computing Center, Rutgers
 * University, nor the names of its contributors may be used to endorse or
 * promote products derived from this software without specific prior written
 * permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

/*
 *  Ciprian Docan (2009)  TASSL Rutgers University
 *  docan@cac.rutgers.edu
 *
 *  Pradeep Subedi (2020) Rutgers University
 *  pradeep.subedi@rutgers.edu
 */

#include "ss_data.h"
#include "queue.h"
#include <errno.h>
#include <math.h>

#include <abt.h>

/*
  A view in  the matrix allows to extract any subset  of values from a
  matrix.
*/

struct matrix_view {
    uint64_t lb[BBOX_MAX_NDIM];
    uint64_t ub[BBOX_MAX_NDIM];
};

/* Generic matrix representation. */
struct matrix {
    uint64_t dist[BBOX_MAX_NDIM];
    int num_dims;
    size_t size_elem;
    enum storage_type mat_storage;
    struct matrix_view mat_view;
    void *pdata;
};

/*
  Cache structure to "map" a bounding box to corresponding nodes in
  the space.
*/
struct sfc_hash_cache {
    struct list_head sh_entry;

    struct bbox sh_bb;

    struct dht_entry **sh_de_tab;
    int sh_nodes;
};

static LIST_HEAD(sfc_hash_list);
static int is_sfc_hash_list_free = 0;

static uint64_t next_pow_2_v2(uint64_t n)
{
    uint64_t i;

    i = 1;
    while(i < n) {
        i = i << 1;
    }

    return i;
}

static int compute_bits_v2(uint64_t n)
{
    int nr_bits = 0;

    while(n > 1) {
        n = n >> 1;
        nr_bits++;
    }

    return nr_bits;
}

static int compute_bits(uint64_t n)
{
    int nr_bits = 0;

    while(n) {
        n = n >> 1;
        nr_bits++;
    }

    return nr_bits;
}

int sh_add(const struct bbox *bb, struct dht_entry *de_tab[], int n)
{
    struct sfc_hash_cache *shc;
    int i, err = -ENOMEM;

    shc = malloc(sizeof(*shc) + sizeof(de_tab[0]) * n);
    if(!shc)
        goto err_out;

    shc->sh_bb = *bb;
    shc->sh_nodes = n;

    shc->sh_de_tab = (struct dht_entry **)(shc + 1);
    for(i = 0; i < n; i++)
        shc->sh_de_tab[i] = de_tab[i];

    list_add_tail(&shc->sh_entry, &sfc_hash_list);

    return 0;
err_out:
    fprintf(stderr, "'%s()': failed with %d.\n", __func__, err);
    return err;
}

int sh_find(const struct bbox *bb, struct dht_entry *de_tab[])
{
    struct sfc_hash_cache *shc;
    int i;

    list_for_each_entry(shc, &sfc_hash_list, struct sfc_hash_cache, sh_entry)
    {
        if(bbox_equals(bb, &shc->sh_bb)) {
            for(i = 0; i < shc->sh_nodes; i++)
                de_tab[i] = shc->sh_de_tab[i];
            return shc->sh_nodes;
        }
    }

    return -1;
}

void sh_free(void)
{
    if(is_sfc_hash_list_free)
        return;

    struct sfc_hash_cache *l, *t;
    int n = 0;

    list_for_each_entry_safe(l, t, &sfc_hash_list, struct sfc_hash_cache,
                             sh_entry)
    {
        free(l);
        n++;
    }
#ifdef DEBUG
    fprintf(stderr, "'%s()': SFC cached %d object descriptors, size = %zu.\n",
            __func__, n, sizeof(*l) * n);
#endif
    is_sfc_hash_list_free = 1;
}
static void get_bbox_max_dim(const struct bbox *bb, uint64_t *out_max_dim,
                             int *out_dim)
{
    int i;
    uint64_t max_dim = 0;
    for(i = 0; i < bb->num_dims; i++) {
        if(max_dim < bb->ub.c[i]) {
            max_dim = bb->ub.c[i];
            *out_dim = i;
        }
    }
    *out_max_dim = max_dim;
}

static void get_bbox_max_dim_size(const struct bbox *bb,
                                  uint64_t *out_max_dim_size, int *out_dim)
{
    int i;
    uint64_t max_dim_size = 0;
    for(i = 0; i < bb->num_dims; i++) {
        uint64_t size = bb->ub.c[i] - bb->lb.c[i] + 1;
        if(max_dim_size < size) {
            max_dim_size = size;
            *out_dim = i;
        }
    }
    *out_max_dim_size = max_dim_size;
}

static struct dht_entry *dht_entry_alloc(struct sspace *ssd, int size_hash)
{
    struct dht_entry *de;
    int i;

    de = malloc(sizeof(*de) + sizeof(struct obj_desc_list) * (size_hash - 1));
    if(!de) {
        errno = ENOMEM;
        return de;
    }
    memset(de, 0, sizeof(*de));

    de->ss = ssd;
    de->odsc_size = size_hash;
    de->hash_cond = malloc(sizeof(*(de->hash_cond)) * size_hash);
    de->hash_mutex = malloc(sizeof(*(de->hash_mutex)) * size_hash);
    de->dht_subs = malloc(sizeof(*(de->dht_subs)) * size_hash);

    for(i = 0; i < size_hash; i++) {
        INIT_LIST_HEAD(&de->odsc_hash[i]);
        ABT_cond_create(&de->hash_cond[i]);
        ABT_mutex_create(&de->hash_mutex[i]);
        INIT_LIST_HEAD(&de->dht_subs[i]);
    }

    de->num_bbox = 0;
    de->size_bb_tab = 0;
    de->bb_tab = NULL;
    return de;
}

static void dht_entry_free(struct dht_entry *de)
{
    struct obj_desc_list *l, *t;
    int i;

    // TODO: free the *intv and other resources.
    free(de->i_tab);
    for(i = 0; i < de->odsc_size; i++) {
        list_for_each_entry_safe(l, t, &de->odsc_hash[i], struct obj_desc_list,
                                 odsc_entry) free(l);
    }

    free(de);
}

static struct dht *dht_alloc(struct sspace *ssd, const struct bbox *bb_domain,
                             int num_nodes, int size_hash)
{
    struct dht *dht;
    int i;

    dht = malloc(sizeof(*dht) + sizeof(struct dht_entry) * (num_nodes - 1));
    if(!dht) {
        errno = ENOMEM;
        return dht;
    }
    memset(dht, 0, sizeof(*dht));

    dht->bb_glb_domain = *bb_domain;
    dht->num_entries = num_nodes;

    for(i = 0; i < num_nodes; i++) {
        dht->ent_tab[i] = dht_entry_alloc(ssd, size_hash);
        if(!dht->ent_tab[i])
            break;
    }

    if(i != num_nodes) {
        errno = ENOMEM;
        while(--i > 0)
            free(dht->ent_tab[i]);
        free(dht);
        dht = 0;
    }

    return dht;
}

static void dht_free(struct dht *dht)
{
    int i;

    for(i = 0; i < dht->num_entries; i++)
        free(dht->ent_tab[i]);

    free(dht);
}

static void dht_free_v2(struct dht *dht)
{
    int i;

    for(i = 0; i < dht->num_entries; i++) {
        if(dht->ent_tab[i]->bb_tab) {
            free(dht->ent_tab[i]->bb_tab);
        }
        free(dht->ent_tab[i]);
    }

    free(dht);
}

static int dht_intersect(struct dht_entry *de, struct intv *itv)
{
    int i;

    if(de->i_virt.lb > itv->ub || de->i_virt.ub < itv->lb)
        return 0;

    for(i = 0; i < de->num_intv; i++)
        if(intv_do_intersect(&de->i_tab[i], itv))
            return 1;
    return 0;
}

static uint64_t ssd_get_max_dim(struct sspace *ss) { return ss->max_dim; }

static int ssd_get_bpd(struct sspace *ss) { return ss->bpd; }

/*
  Hash the global geometric domain space to 1d index, and map a piece
  to each entry in the dht->ent_tab.
*/
static int dht_construct_hash(struct dht *dht, struct sspace *ssd)
{
    uint64_t vol = bbox_volume(&dht->bb_glb_domain);
    uint64_t *sn = malloc(sizeof(*sn) * dht->num_entries);
    struct intv *i_tab, intv;
    struct dht_entry *de;
    uint64_t len;
    int num_intv, i, j;
    int err = -ENOMEM;

    for(i = 0; i < dht->num_entries; i++) {
        sn[i] = vol / dht->num_entries;
        if(i < vol % dht->num_entries) {
            sn[i]++;
        }
    }

    bbox_to_intv(&dht->bb_glb_domain, ssd->max_dim, ssd->bpd, &i_tab,
                 &num_intv);
    for(i = 0, j = 0; i < dht->num_entries; i++) {
        len = sn[i];

        de = dht->ent_tab[i];
        de->rank = i;
        de->i_tab = malloc(sizeof(struct intv) * num_intv);
        if(!de->i_tab)
            break;

        while(len > 0) {
            if(intv_size(&i_tab[j]) > len) {
                intv.lb = i_tab[j].lb;
                intv.ub = intv.lb + len - 1;
                i_tab[j].lb += len;
            } else {
                intv = i_tab[j++];
            }
            len -= intv_size(&intv);
            de->i_tab[de->num_intv++] = intv;
        }

        de->i_virt.lb = de->i_tab[0].lb;
        de->i_virt.ub = de->i_tab[de->num_intv - 1].ub;
        de->i_tab = realloc(de->i_tab, sizeof(intv) * de->num_intv);
        if(!de->i_tab && de->num_intv)
            break;
    }

    free(i_tab);
    free(sn);

    if(i == dht->num_entries)
        return 0;

    fprintf(stderr, "'%s()': failed at entry %d.\n", __func__, i);
    return err;
}

static struct sspace *ssd_alloc_v1(const struct bbox *bb_domain, int num_nodes,
                                   int max_versions)
{
    struct sspace *ssd;
    uint64_t max_dim;
    int err = -ENOMEM;

    ssd = malloc(sizeof(*ssd));
    if(!ssd)
        goto err_out;
    memset(ssd, 0, sizeof(*ssd));

    ssd->dht = dht_alloc(ssd, bb_domain, num_nodes, max_versions);
    if(!ssd->dht) {
        free(ssd);
        goto err_out;
    }

    int i;
    max_dim = bb_domain->ub.c[0];
    for(i = 1; i < bb_domain->num_dims; i++) {
        max_dim = max(bb_domain->ub.c[i], max_dim);
    }
    if(max_dim == 0)
        max_dim = 1; // Note: max_dim as 0 would not work...

    ssd->max_dim = next_pow_2(max_dim);
    ssd->bpd = compute_bits(ssd->max_dim);

    err = dht_construct_hash(ssd->dht, ssd);
    if(err < 0) {
        dht_free(ssd->dht);
        free(ssd);
        goto err_out;
    }

    ssd->hash_version = ssd_hash_version_v1;
    return ssd;
err_out:
    fprintf(stderr, "'%s()': failed with %d\n", __func__, err);
    return NULL;
}

static void ssd_free_v1(struct sspace *ssd)
{
    dht_free(ssd->dht);
    free(ssd);
    sh_free();
}

static int ssd_hash_v1(struct sspace *ss, const struct bbox *bb,
                       struct dht_entry *de_tab[])
{
    struct intv *i_tab;
    int i, k, n, num_nodes;

    num_nodes = sh_find(bb, de_tab);
    if(num_nodes > 0)
        /* This is great, I hit the cache. */
        return num_nodes;

    num_nodes = 0;

    bbox_to_intv2(bb, ss->max_dim, ss->bpd, &i_tab, &n);

    for(k = 0; k < ss->dht->num_entries; k++) {
        for(i = 0; i < n; i++) {
            if(dht_intersect(ss->dht->ent_tab[k], &i_tab[i])) {
                de_tab[num_nodes++] = ss->dht->ent_tab[k];
                break;
            }
        }
    }

    /* Cache the results for later use. */
    sh_add(bb, de_tab, num_nodes);

    free(i_tab);
    return num_nodes;
}

struct sspace *ssd_alloc_v2(const struct bbox *bb_domain, int num_nodes,
                            int max_versions)
{
    struct sspace *ssd = NULL;
    int err = -ENOMEM;
    int i, j, k;
    int dim;
    int nbits_max_dim = 0;
    uint64_t max_dim = 0;
    get_bbox_max_dim(bb_domain, &max_dim, &dim);
    max_dim = next_pow_2_v2(max_dim);
    nbits_max_dim = compute_bits_v2(max_dim);

    // decompose the global bbox
    int num_divide_iteration = compute_bits_v2(next_pow_2_v2(num_nodes));
    struct bbox *bb, *b1, *b2;
    struct queue q1, q2;
    queue_init(&q1);
    queue_init(&q2);

    bb = malloc(sizeof(struct bbox));
    memcpy(bb, bb_domain, sizeof(struct bbox));
    queue_enqueue(&q1, bb);
    for(i = 0; i < num_divide_iteration; i++) {
        struct queue *src_q, *dst_q;
        if(!queue_is_empty(&q1) && queue_is_empty(&q2)) {
            src_q = &q1;
            dst_q = &q2;
        } else if(!queue_is_empty(&q2) && queue_is_empty(&q1)) {
            src_q = &q2;
            dst_q = &q1;
        } else {
            printf("%s(): error, both q1 and q2 is (non)empty.\n", __func__);
        }
        while(!queue_is_empty(src_q)) {
            uint64_t max_dim_size;
            struct bbox b_tab[2];

            bb = queue_dequeue(src_q);
            get_bbox_max_dim_size(bb, &max_dim_size, &dim);
            bbox_divide_in2_ondim(bb, b_tab, dim);
            free(bb);
            bb = NULL;
            b1 = malloc(sizeof(struct bbox));
            b2 = malloc(sizeof(struct bbox));
            *b1 = b_tab[0];
            *b2 = b_tab[1];
            queue_enqueue(dst_q, b1);
            queue_enqueue(dst_q, b2);
        }
    }

    struct queue *q;
    if(!queue_is_empty(&q1))
        q = &q1;
    else if(!queue_is_empty(&q2))
        q = &q2;

    ssd = malloc(sizeof(*ssd));
    if(!ssd)
        goto err_out;
    memset(ssd, 0, sizeof(*ssd));

    ssd->max_dim = max_dim;
    ssd->bpd = nbits_max_dim;

    ssd->total_num_bbox = queue_size(q);
    ssd->dht = dht_alloc(ssd, bb_domain, num_nodes, max_versions);
    if(!ssd->dht) {
        free(ssd);
        goto err_out;
    }
    for(i = 0; i < num_nodes; i++) {
        ssd->dht->ent_tab[i]->rank = i;
    }

    int n = ceil(ssd->total_num_bbox * 1.0 / ssd->dht->num_entries);
    for(i = 0; i < ssd->dht->num_entries; i++) {
        ssd->dht->ent_tab[i]->size_bb_tab = n;
        ssd->dht->ent_tab[i]->bb_tab = malloc(sizeof(struct bbox) * n);
    }

    // simple round-robing mapping of decomposed bbox to dht entries
    i = 0;
    while(!queue_is_empty(q)) {
        bb = queue_dequeue(q);
        j = i++ % ssd->dht->num_entries;
        k = ssd->dht->ent_tab[j]->num_bbox++;
        ssd->dht->ent_tab[j]->bb_tab[k] = *bb;
        free(bb);
    }

    ssd->hash_version = ssd_hash_version_v2;
    return ssd;
err_out:
    fprintf(stderr, "'%s()': failed with %d\n", __func__, err);
    return NULL;
}

void ssd_free_v2(struct sspace *ssd)
{
    dht_free_v2(ssd->dht);
    free(ssd);
    sh_free();
}

int ssd_hash_v2(struct sspace *ss, const struct bbox *bb,
                struct dht_entry *de_tab[])
{
    int i, j, num_nodes;

    num_nodes = sh_find(bb, de_tab);
    if(num_nodes > 0)
        /* This is great, I hit the cache. */
        return num_nodes;

    num_nodes = 0;
    for(i = 0; i < ss->dht->num_entries; i++) {
        for(j = 0; j < ss->dht->ent_tab[i]->num_bbox; j++) {
            if(bbox_does_intersect(bb, &ss->dht->ent_tab[i]->bb_tab[j])) {
                de_tab[num_nodes++] = ss->dht->ent_tab[i];
                break;
            }
        }
    }

    /* Cache the results for later use. */
    sh_add(bb, de_tab, num_nodes);

    return num_nodes;
}

static void matrix_init(struct matrix *mat, enum storage_type st,
                        struct bbox *bb_glb, struct bbox *bb_loc, void *pdata,
                        size_t se)
{
    int i;
    int ndims = bb_glb->num_dims;
    memset(mat, 0, sizeof(struct matrix));

    for(i = 0; i < ndims; i++) {
        mat->dist[i] = bbox_dist(bb_glb, i);
        mat->mat_view.lb[i] = bb_loc->lb.c[i] - bb_glb->lb.c[i];
        mat->mat_view.ub[i] = bb_loc->ub.c[i] - bb_glb->lb.c[i];
    }

    mat->num_dims = ndims;
    mat->mat_storage = st;
    mat->pdata = pdata;
    mat->size_elem = se;
}

static int matrix_copy(struct matrix *a, struct matrix *b)
{
    char *A = a->pdata;
    char *B = b->pdata;

    uint64_t a0, a1, a2, a3, a4, a5, a6, a7, a8, a9;
    uint64_t aloc = 0, aloc1 = 0, aloc2 = 0, aloc3 = 0, aloc4 = 0, aloc5 = 0,
             aloc6 = 0, aloc7 = 0, aloc8 = 0, aloc9 = 0;
    uint64_t b0, b1, b2, b3, b4, b5, b6, b7, b8, b9;
    uint64_t bloc = 0, bloc1 = 0, bloc2 = 0, bloc3 = 0, bloc4 = 0, bloc5 = 0,
             bloc6 = 0, bloc7 = 0, bloc8 = 0, bloc9 = 0;
    uint64_t numelem;
    uint64_t num_copied_elem = 0;

    switch(a->num_dims) {
    case(1):
        goto dim1;
        break;
    case(2):
        goto dim2;
        break;
    case(3):
        goto dim3;
        break;
    case(4):
        goto dim4;
        break;
    case(5):
        goto dim5;
        break;
    case(6):
        goto dim6;
        break;
    case(7):
        goto dim7;
        break;
    case(8):
        goto dim8;
        break;
    case(9):
        goto dim9;
        break;
    case(10):
        goto dim10;
        break;
    default:
        break;
    }

dim10:
    for(a9 = a->mat_view.lb[9], b9 = b->mat_view.lb[9]; // TODO-Q
        a9 <= a->mat_view.ub[9]; a9++, b9++) {
        aloc9 = a9 * a->dist[8];
        bloc9 = a9 * b->dist[8];
    dim9:
        for(a8 = a->mat_view.lb[8], b8 = b->mat_view.lb[8]; // TODO-Q
            a8 <= a->mat_view.ub[8]; a8++, b8++) {
            aloc8 = (aloc9 + a8) * a->dist[7];
            bloc8 = (bloc9 + b8) * b->dist[7];
        dim8:
            for(a7 = a->mat_view.lb[7], b7 = b->mat_view.lb[7]; // TODO-Q
                a7 <= a->mat_view.ub[7]; a7++, b7++) {
                aloc7 = (aloc8 + a7) * a->dist[6];
                bloc7 = (bloc8 + b7) * b->dist[6];
            dim7:
                for(a6 = a->mat_view.lb[6], b6 = b->mat_view.lb[6]; // TODO-Q
                    a6 <= a->mat_view.ub[6]; a6++, b6++) {
                    aloc6 = (aloc7 + a6) * a->dist[5];
                    bloc6 = (bloc7 + b6) * b->dist[5];
                dim6:
                    for(a5 = a->mat_view.lb[5],
                    b5 = b->mat_view.lb[5]; // TODO-Q
                        a5 <= a->mat_view.ub[5]; a5++, b5++) {
                        aloc5 = (aloc6 + a5) * a->dist[4];
                        bloc5 = (bloc6 + b5) * b->dist[4];
                    dim5:
                        for(a4 = a->mat_view.lb[4], b4 = b->mat_view.lb[4];
                            a4 <= a->mat_view.ub[4]; a4++, b4++) {
                            aloc4 = (aloc5 + a4) * a->dist[3];
                            bloc4 = (bloc5 + b4) * b->dist[3];
                        dim4:
                            for(a3 = a->mat_view.lb[3], b3 = b->mat_view.lb[3];
                                a3 <= a->mat_view.ub[3]; a3++, b3++) {
                                aloc3 = (aloc4 + a3) * a->dist[2];
                                bloc3 = (bloc4 + b3) * b->dist[2];
                            dim3:
                                for(a2 = a->mat_view.lb[2],
                                b2 = b->mat_view.lb[2];
                                    a2 <= a->mat_view.ub[2]; a2++, b2++) {
                                    aloc2 = (aloc3 + a2) * a->dist[1];
                                    bloc2 = (bloc3 + b2) * b->dist[1];
                                dim2:
                                    for(a1 = a->mat_view.lb[1],
                                    b1 = b->mat_view.lb[1];
                                        a1 <= a->mat_view.ub[1]; a1++, b1++) {
                                        aloc1 = (aloc2 + a1) * a->dist[0];
                                        bloc1 = (bloc2 + b1) * b->dist[0];
                                    dim1:
                                        numelem = (a->mat_view.ub[0] -
                                                   a->mat_view.lb[0]) +
                                                  1;
                                        aloc = aloc1 + a->mat_view.lb[0];
                                        bloc = bloc1 + b->mat_view.lb[0];
                                        memcpy(&A[aloc * a->size_elem],
                                               &B[bloc * a->size_elem],
                                               (a->size_elem * numelem));
                                        num_copied_elem += numelem;
                                        if(a->num_dims == 1)
                                            return num_copied_elem;
                                    }
                                    if(a->num_dims == 2)
                                        return num_copied_elem;
                                }
                                if(a->num_dims == 3)
                                    return num_copied_elem;
                            }
                            if(a->num_dims == 4)
                                return num_copied_elem;
                        }
                        if(a->num_dims == 5)
                            return num_copied_elem;
                    }
                    if(a->num_dims == 6)
                        return num_copied_elem;
                }
                if(a->num_dims == 7)
                    return num_copied_elem;
            }
            if(a->num_dims == 8)
                return num_copied_elem;
        }
        if(a->num_dims == 9)
            return num_copied_elem;
    }
}

char *obj_desc_sprint(obj_descriptor *odsc)
{
    char *str;
    int nb;

    str = alloc_sprintf("obj_descriptor = {\n"
                        "\t.name = %s,\n"
                        "\t.owner = %s,\n"
                        "\t.version = %d,\n"
                        "\t.bb = ",
                        odsc->name, odsc->owner, odsc->version);
    str = str_append_const(str_append(str, bbox_sprint(&odsc->bb)), "}\n");

    return str;
}
/*
 */
int ssd_copy(struct obj_data *to_obj, struct obj_data *from_obj)
{
    struct matrix to_mat, from_mat;
    struct bbox bbcom;
    int copied_elems = 0;

    bbox_intersect(&to_obj->obj_desc.bb, &from_obj->obj_desc.bb, &bbcom);

    matrix_init(&from_mat, from_obj->obj_desc.st, &from_obj->obj_desc.bb,
                &bbcom, from_obj->data, from_obj->obj_desc.size);

    matrix_init(&to_mat, to_obj->obj_desc.st, &to_obj->obj_desc.bb, &bbcom,
                to_obj->data, to_obj->obj_desc.size);

    copied_elems = matrix_copy(&to_mat, &from_mat);
    return copied_elems;
}

/*
  Allocate and init the local storage structure.
*/
ss_storage *ls_alloc(int max_versions)
{
    ss_storage *ls = 0;
    int i;

    ls = malloc(sizeof(*ls) + sizeof(struct list_head) * max_versions);
    if(!ls) {
        errno = ENOMEM;
        return ls;
    }

    memset(ls, 0, sizeof(*ls));
    ls->meta_hash = malloc(sizeof(struct list_head) * max_versions);
    // extra cond/mutex/sub for the no version subs
    ls->meta_cond = malloc(sizeof(*ls->meta_cond) * (max_versions + 1));
    ls->meta_mutex = malloc(sizeof(*ls->meta_mutex) * (max_versions + 1));
    ls->meta_subs = malloc(sizeof(struct list_head) * (max_versions + 1));

    for(i = 0; i <= max_versions; i++) {
        if(i < max_versions) {
            INIT_LIST_HEAD(&ls->meta_hash[i]);
            INIT_LIST_HEAD(&ls->obj_hash[i]);
        }
        INIT_LIST_HEAD(&ls->meta_subs[i]);
        ABT_cond_create(&ls->meta_cond[i]);
        ABT_mutex_create(&ls->meta_mutex[i]);
    }
    ls->size_hash = max_versions;

    return ls;
}

void ls_free(ss_storage *ls)
{
    if(!ls)
        return;

    struct obj_data *od, *t;
    struct meta_data *mdata, *tm;
    struct list_head *list;
    int i;

    for(i = 0; i < ls->size_hash; i++) {
        list = &ls->obj_hash[i];
        list_for_each_entry_safe(od, t, list, struct obj_data, obj_entry)
        {
            ls_remove(ls, od);
            obj_data_free(od);
        }
        list = &ls->meta_hash[i];
        list_for_each_entry_safe(mdata, tm, list, struct meta_data, entry)
        {
            list_del(&mdata->entry);
            meta_data_free(mdata);
        }
    }

    if(ls->num_obj != 0) {
        fprintf(stderr, "%s(): ERROR ls->num_obj is %d not 0\n", __func__,
                ls->num_obj);
    }
    free(ls);
}

struct meta_data *ls_find_meta(ss_storage *ls, const char *name, int version)
{
    struct meta_data *mdata;
    struct list_head *list;
    int index;

    index = version % ls->size_hash;
    list = &ls->meta_hash[index];
    list_for_each_entry(mdata, list, struct meta_data, entry)
    {
        if(strcmp(name, mdata->name) == 0 && mdata->version == version) {
            return (mdata);
        }
    }

    return (NULL);
}

void ls_add_meta(ss_storage *ls, struct meta_data *mdata)
{
    struct meta_sub_list_entry *msub, *mt;
    struct list_head *list;
    int msub_found = 0;
    int index;

    if(ls_find_meta(ls, mdata->name, mdata->version)) {
        fprintf(stderr,
                "WARNING: metadata already received for \"%s\", version %d. "
                "Ignoring.\n",
                mdata->name, mdata->version);
        return;
    }
    index = mdata->version % ls->size_hash;
    list = &ls->meta_hash[index];

    ABT_mutex_lock(ls->meta_mutex[index]);
    list_add(&mdata->entry, list);

    list_for_each_entry_safe(msub, mt, &ls->meta_subs[index],
                             struct meta_sub_list_entry, entry)
    {
        if(strcmp(msub->name, mdata->name) == 0 &&
           msub->version == mdata->version) {
            msub->mdata = mdata;
            msub_found = 1;
            list_del(&msub->entry);
        }
    }

    if(msub_found) {
        ABT_cond_broadcast(ls->meta_cond[index]);
    }
    ABT_mutex_unlock(ls->meta_mutex[index]);

    // check for no version matches
    msub_found = 0;
    ABT_mutex_lock(ls->meta_mutex[ls->size_hash]);
    list_for_each_entry_safe(msub, mt, &ls->meta_subs[ls->size_hash],
                             struct meta_sub_list_entry, entry)
    {
        if(strcmp(msub->name, mdata->name) == 0) {
            msub->mdata = mdata;
            msub_found = 1;
            list_del(&msub->entry);
        }
    }

    if(msub_found) {
        ABT_cond_broadcast(ls->meta_cond[ls->size_hash]);
    }
    ABT_mutex_unlock(ls->meta_mutex[ls->size_hash]);
}

/*
  Add an object to the local storage.
*/
void ls_add_obj(ss_storage *ls, struct obj_data *od)
{
    int index;
    struct list_head *bin;
    struct obj_data *od_existing;

    // ABT_rwlock_create(&od->lock);
    // ABT_rwlock_wrlock(&od->lock);

    od_existing = ls_find_no_version(ls, &od->obj_desc);
    if(od_existing) {
        od_existing->f_free = 1;
        if(od_existing->refcnt == 0) {
            ls_remove(ls, od_existing);
            obj_data_free(od_existing);
        } else {
            fprintf(stderr, "'%s()': object eviction delayed.\n", __func__);
        }
    }
    index = od->obj_desc.version % ls->size_hash;
    bin = &ls->obj_hash[index];

    /* NOTE: new object comes first in the list. */
    list_add(&od->obj_entry, bin);
    ls->num_obj++;
    // ABT_rwlock_unlock(&od->lock);
}

struct obj_data *ls_lookup(ss_storage *ls, char *name)
{
    struct obj_data *od;
    struct list_head *list;
    int i;

    for(i = 0; i < ls->size_hash; i++) {
        list = &ls->obj_hash[i];

        list_for_each_entry(od, list, struct obj_data, obj_entry)
        {
            if(strcmp(od->obj_desc.name, name) == 0)
                return od;
        }
    }

    return NULL;
}

void ls_remove(ss_storage *ls, struct obj_data *od)
{
    list_del(&od->obj_entry);
    ls->num_obj--;
}

void ls_try_remove_free(ss_storage *ls, struct obj_data *od)
{
    /* Note:  we   assume  the  object  data   is  allocated  with
       obj_data_alloc(), i.e., the data follows the structure.  */
    if(od->refcnt == 0) {
        ls_remove(ls, od);
        obj_data_free(od);
    }
}

/*
  Find  an object  in the  local storage  that has  the same  name and
  version with the object descriptor 'odsc'.
*/
struct obj_data *ls_find(ss_storage *ls, obj_descriptor *odsc)
{
    struct obj_data *od;
    struct list_head *list;
    int index;

    index = odsc->version % ls->size_hash;
    list = &ls->obj_hash[index];

    list_for_each_entry(od, list, struct obj_data, obj_entry)
    {
        if(obj_desc_equals_intersect(odsc, &od->obj_desc))
            return od;
    }

    return NULL;
}

/*
  Find  list of object_desriptors  in the  local storage  that has  the same
  name and version with the object descriptor 'odsc'.
*/
int ls_find_ods(ss_storage *ls, obj_descriptor *odsc, struct obj_data **od_tab)
{
    struct obj_data *od;
    struct list_head *list;
    int index;
    int num_odsc = 0;

    index = odsc->version % ls->size_hash;
    list = &ls->obj_hash[index];
    list_for_each_entry(od, list, struct obj_data, obj_entry)
    {
        if(obj_desc_equals_intersect(odsc, &od->obj_desc)) {
            od_tab[num_odsc++] = od;
        }
    }
    return num_odsc;
}

/*
  Search for an object in the local storage that is mapped to the same
  bin, and that has the same  name and object descriptor, but may have
  different version.
*/
struct obj_data *ls_find_no_version(ss_storage *ls, obj_descriptor *odsc)
{
    struct obj_data *od;
    struct list_head *list;
    int index;

    index = odsc->version % ls->size_hash;
    list = &ls->obj_hash[index];
    list_for_each_entry(od, list, struct obj_data, obj_entry)
    {
        if(obj_desc_by_name_intersect(odsc, &od->obj_desc))
            return od;
    }

    return NULL;
}

/*
//TODO: implement lock
struct lock_data *get_lock(struct list_head *lock_list, char* name)
{
    struct lock_data *lk;
    list_for_each_entry(lk, lock_list, struct lock_data, lock_entry){
        if(strcmp(lk->name, name) == 0)
            return lk;
    }
    return NULL;
}

struct lock_data *create_lock(struct list_head *lock_list, char* name)
{
    struct lock_data *lk;
    lk = malloc(sizeof(lock_data));
    lk->name = malloc(strlen(name)+1);
    strcpy(lk->name, name);
    ABT_rwlock_create(&lk->lock);
    list_add(&lk->lock_entry, lock_list);
    return lk;

}
*/

#define ALIGN_ADDR_QUAD_BYTES(a)                                               \
    unsigned long _a = (unsigned long)(a);                                     \
    _a = (_a + 7) & ~7;                                                        \
    (a) = (void *)_a;
/*
  Allocate space for an obj_data structure and the data.
*/
struct obj_data *obj_data_alloc(obj_descriptor *odsc)
{
    struct obj_data *od = 0;

    od = malloc(sizeof(*od));
    if(!od) {
        fprintf(stderr, "Malloc od error\n");
        return NULL;
    }
    memset(od, 0, sizeof(*od));

    int size = obj_data_size(odsc);
    od->data = malloc(size);
    if(!od->data) {
        fprintf(stderr, "Malloc od_data error\n");
        free(od);
        return NULL;
    }
    ALIGN_ADDR_QUAD_BYTES(od->data);
    od->obj_desc = *odsc;

    return od;
}

/*
  Allocate space for the obj_data struct only; space for data is
  externally allocated.
*/
struct obj_data *obj_data_alloc_no_data(obj_descriptor *odsc, void *data)
{
    struct obj_data *od;

    od = malloc(sizeof(*od));
    if(!od)
        return NULL;
    memset(od, 0, sizeof(*od));

    od->obj_desc = *odsc;
    od->data = data;

    return od;
}

struct obj_data *obj_data_alloc_with_data(obj_descriptor *odsc, void *data)
{
    struct obj_data *od = obj_data_alloc(odsc);
    if(!od)
        return NULL;

    memcpy(od->data, data, obj_data_size(odsc));
    // TODO: what about the descriptor ?

    return od;
}

void meta_data_free(struct meta_data *mdata)
{
    if(mdata) {
        free(mdata->name);
        if(mdata->data) {
            free(mdata->data);
        }
        free(mdata);
    }
}

void obj_data_free(struct obj_data *od)
{
    if(od) {
        if(od->data) {
            free(od->data);
        }
        free(od);
    }
}

uint64_t obj_data_size(obj_descriptor *obj_desc)
{
    return obj_desc->size * bbox_volume(&obj_desc->bb);
}

int obj_desc_equals_no_owner(const obj_descriptor *odsc1,
                             const obj_descriptor *odsc2)
{
    if(odsc1->version == odsc2->version &&
       strcmp(odsc1->name, odsc2->name) == 0 &&
       bbox_equals(&odsc1->bb, &odsc2->bb))
        return 1;
    return 0;
}

int obj_desc_equals(obj_descriptor *odsc1, obj_descriptor *odsc2)
{
    if(odsc1->owner == odsc2->owner && bbox_equals(&odsc1->bb, &odsc2->bb))
        return 1;
    else
        return 0;
}

/*
 *   Test if two object descriptors have the same name and versions and
 *     their bounding boxes intersect.
 *     */
int obj_desc_equals_intersect(obj_descriptor *odsc1, obj_descriptor *odsc2)
{
    if(strcmp(odsc1->name, odsc2->name) == 0 &&
       odsc1->version == odsc2->version &&
       bbox_does_intersect(&odsc1->bb, &odsc2->bb))
        return 1;
    return 0;
}

/*
 *   Test if two object descriptors have the same name and their bounding
 *     boxes intersect.
 *     */
int obj_desc_by_name_intersect(const obj_descriptor *odsc1,
                               const obj_descriptor *odsc2)
{
    if(strcmp(odsc1->name, odsc2->name) == 0 &&
       bbox_does_intersect(&odsc1->bb, &odsc2->bb))
        return 1;
    return 0;
}

/*
  Public API starts here.
*/

/*
 ssd hashing function v1: uses Hilbert SFC to linearize the global data domain
    and bounding box passed by put()/get().
 ssd hashing function v2: NOT use Hilbert SFC for linearization.
*/

/*
  Allocate the shared space structure.
*/
struct sspace *ssd_alloc(const struct bbox *bb_domain, int num_nodes,
                         int max_versions,
                         enum sspace_hash_version hash_version)
{
    struct sspace *ss = NULL;

#ifdef TIMING_SSD
    struct timer tm;
    double tm_st, tm_end;
    timer_init(&tm, 1);
    timer_start(&tm);
    tm_st = timer_read(&tm);
#endif

#ifdef DEBUG
    char *str = bbox_sprint(bb_domain);
    fprintf(stderr, "%s: allocating new shared space %s\n", __func__, str);
#endif

    switch(hash_version) {
    case ssd_hash_version_v1:
        ss = ssd_alloc_v1(bb_domain, num_nodes, max_versions);
        break;
    case ssd_hash_version_v2:
        ss = ssd_alloc_v2(bb_domain, num_nodes, max_versions);
        break;
    default:
        fprintf(stderr, "%s(): ERROR unknown shared space hash version %u\n",
                __func__, hash_version);
        break;
    }

#ifdef TIMING_SSD
    tm_end = timer_read(&tm);
    fprintf(stderr, "%s(): hash_version v%u time %lf seconds\n", __func__,
            hash_version, tm_end - tm_st);
#endif

    return ss;
}

void ssd_free(struct sspace *ss)
{
    switch(ss->hash_version) {
    case ssd_hash_version_v1:
        ssd_free_v1(ss);
        break;
    case ssd_hash_version_v2:
        ssd_free_v2(ss);
        break;
    default:
        fprintf(stderr, "%s(): ERROR unknown shared space hash version %u\n",
                __func__, ss->hash_version);
        break;
    }
}

long ssh_hash_elem_count_v1(struct sspace *ss, const struct bbox *bb)
{
    struct intv *i_tab, *i_self;
    long overlap, num_elem;
    int i, n;

    // TODO: cache results

    bbox_to_intv2(bb, ss->max_dim, ss->bpd, &i_tab, &n);

    i_self = &ss->ent_self->i_virt;

    num_elem = 0;
    for(i = 0; i < n; i++) {
        if(i_self->lb <= i_tab[i].ub && i_self->ub >= i_tab[i].lb) {
            overlap = (i_self->ub - i_tab[i].lb) + 1;
            if(i_tab[i].lb < i_self->lb) {
                overlap -= i_self->lb - i_tab[i].lb;
            }
            if(i_self->ub > i_tab[i].ub) {
                overlap -= i_self->ub - i_tab[i].ub;
            }
            num_elem += overlap;
        }
    }

    return (num_elem);
}

long ssh_hash_elem_count_v2(struct sspace *ss, const struct bbox *bb)
{
    long num_elem;
    struct bbox *ent_bb;
    struct bbox isect;
    int i;

    // TODO: cache results

    num_elem = 0;
    for(i = 0; i < ss->ent_self->num_bbox; i++) {
        ent_bb = &ss->ent_self->bb_tab[i];
        if(bbox_does_intersect(bb, ent_bb)) {
            bbox_intersect(bb, ent_bb, &isect);
            num_elem += bbox_volume(&isect);
        }
    }

    return (num_elem);
}

/*
 Get the number of elements of a bounding box 'bb' that hash to the
 local dht
 */
long ssh_hash_elem_count(struct sspace *ss, const struct bbox *bb)
{
    long ret;

    switch(ss->hash_version) {
    case ssd_hash_version_v1:
        ret = ssh_hash_elem_count_v1(ss, bb);
        break;
    case ssd_hash_version_v2:
        ret = ssh_hash_elem_count_v2(ss, bb);
        break;
    default:
        fprintf(stderr, "%s(): ERROR unknown shared space hash version %u\n",
                __func__, ss->hash_version);
        ret = 0;
        break;
    }

    return (ret);
}

/*
  Hash a bounding box 'bb' to the hash entries in dht; fill in the
  entries in the de_tab and return the number of entries.
*/
int ssd_hash(struct sspace *ss, const struct bbox *bb,
             struct dht_entry *de_tab[])
{
#ifdef TIMING_SSD
    struct timer tm;
    double tm_st, tm_end;
    timer_init(&tm, 1);
    timer_start(&tm);
    tm_st = timer_read(&tm);
#endif

    int ret;

    switch(ss->hash_version) {
    case ssd_hash_version_v1:
        ret = ssd_hash_v1(ss, bb, de_tab);
        break;
    case ssd_hash_version_v2:
        ret = ssd_hash_v2(ss, bb, de_tab);
        break;
    default:
        fprintf(stderr, "%s(): ERROR unknown shared space hash version %u\n",
                __func__, ss->hash_version);
        ret = 0;
        break;
    }

#ifdef TIMING_SSD
    tm_end = timer_read(&tm);
    fprintf(stderr, "%s(): hash_version v%u time %lf seconds\n", __func__,
            ss->hash_version, tm_end - tm_st);
#endif
    return ret;
}

/*
  Initialize the dht structure.
*/
int ssd_init(struct sspace *ssd, int rank)
{
    ssd->rank = rank;
    ssd->ent_self = ssd->dht->ent_tab[rank];

    return 0;
}

int ssd_filter(struct obj_data *from, obj_descriptor *odsc, double *dval)
{
    // TODO: search the matrix to find the min
    static int n = 1;

    *dval = 2.0 * n;
    n++;

    return 0;
}

/*
  Test if the  'odsc' matches any object descriptor in  a DHT entry by
  name and coordinates, but not version, and return the matching index.
*/
static struct obj_desc_list *dht_find_match(const struct dht_entry *de,
                                            const obj_descriptor *odsc)
{
    struct obj_desc_list *odscl;
    int n;

    // TODO: delete this (just an assertion for proper behaviour).
    if(odsc->version == (unsigned int)-1) {
        fprintf(stderr, "'%s()': version on object descriptor is not set!!!\n",
                __func__);
        return 0;
    }

    n = odsc->version % de->odsc_size;
    list_for_each_entry(odscl, &de->odsc_hash[n], struct obj_desc_list,
                        odsc_entry)
    {
        if(obj_desc_by_name_intersect(odsc, &odscl->odsc))
            return odscl;
    }

    return 0;
}

static obj_descriptor *dht_find_exact(const struct dht_entry *de,
                                      const obj_descriptor *odsc)
{
    struct obj_desc_list *odscl;
    int n;

    n = odsc->version % de->odsc_size;
    list_for_each_entry(odscl, &de->odsc_hash[n], struct obj_desc_list,
                        odsc_entry)
    {
        if(obj_desc_equals_no_owner(odsc, &odscl->odsc)) {
            return (&odscl->odsc);
        }
    }

    return (NULL);
}

#define array_resize(a, n) a = realloc(a, sizeof(*a) * (n))

/*`
 * subscribe to a certain number of elements, remaining, of an object descriptor
 *  q_odsc. When dht updates are added, they will be checked against
 * subscriptions created by this function. Each incoming update that overlaps
 * with q_odsc will decrement sub.remaining by the size of the overlap and
 * increment pub_count by one. The general idea is that once enough elements
 * that overlap with the query have been counted, the query must be fully
 * satisfiable.
 *
 * Updates and signals are coming from dht_add_entry
 *
 * TODO: if we ever overwrite an object that is needed to satisfy q_odsc, the
 *  subscription should fail and that failure should be propagated.
 *
 */
void dht_local_subscribe(struct dht_entry *de, obj_descriptor *q_odsc,
                         obj_descriptor ***odsc_tab, int *tab_entries,
                         long remaining, int timeout)
{
    struct dht_sub_list_entry sub;
    obj_descriptor **odsc_tab_pos;
    struct obj_desc_ptr_list *odscl, *tmp;
    int n = q_odsc->version % de->odsc_size;

    sub.odsc = q_odsc;
    sub.remaining = remaining;
    sub.pub_count = 0;
    INIT_LIST_HEAD(&sub.recv_odsc);

    list_add(&sub.entry, &de->dht_subs[n]);

    do {
        ABT_cond_wait(de->hash_cond[n], de->hash_mutex[n]);
    } while(sub.remaining > 0);

    *odsc_tab =
        realloc(*odsc_tab, sizeof(**odsc_tab) * (*tab_entries + sub.pub_count));
    odsc_tab_pos = &(*odsc_tab)[*tab_entries];
    list_for_each_entry_safe(odscl, tmp, &sub.recv_odsc,
                             struct obj_desc_ptr_list, odsc_entry)
    {
        *odsc_tab_pos = odscl->odsc;
        odsc_tab_pos++;
        list_del(&odscl->odsc_entry);
        free(odscl);
    }
    *tab_entries += sub.pub_count;
}

int dht_update_owner(struct dht_entry *de, obj_descriptor *odsc, int clear_flag)
{
    obj_descriptor *old_odsc;

    old_odsc = dht_find_exact(de, odsc);
    if(!old_odsc) {
        fprintf(stderr,
                "ERROR: (%s): no matching object found when doing update. "
                "Object being updated is %s\n",
                __func__, obj_desc_sprint(odsc));
        return (-ENOENT);
    }
    strcpy(old_odsc->owner, odsc->owner);
    if(clear_flag) {
        old_odsc->flags &= ~(DS_CLIENT_STORAGE);
    }

    return 0;
}

int dht_add_entry(struct dht_entry *de, obj_descriptor *odsc)
{
    struct obj_desc_list *odscl;
    struct obj_desc_ptr_list *sub_odscl;
    struct dht_sub_list_entry *sub, *tmp;
    struct bbox isect;
    int sub_complete = 0;
    int n, err = -ENOMEM;

    odscl = dht_find_match(de, odsc);
    if(odscl) {
        /* There  is allready  a descriptor  with  a different
           version in the DHT, so I will overwrite it. */
        if(odscl->odsc.version == odsc->version) {
            fprintf(stderr, "WARNING: the server has detected an overlapping "
                            "put (same version and some common elements with a "
                            "previous put. DataSpaces storage is intended to "
                            "be immutable, and the behavior of updates is "
                            "undefined. Proceed at your own risk...\n");
            fprintf(stderr, " New put is: \n%s\n", obj_desc_sprint(odsc));
            fprintf(stderr, " But found existing: \n%s\n",
                    obj_desc_sprint(&odscl->odsc));
        }
        memcpy(&odscl->odsc, odsc, sizeof(*odsc));
        return 0;
    }

    n = odsc->version % de->odsc_size;
    odscl = malloc(sizeof(*odscl));
    if(!odscl)
        return err;
    memcpy(&odscl->odsc, odsc, sizeof(*odsc));

    ABT_mutex_lock(de->hash_mutex[n]);
    list_add(&odscl->odsc_entry, &de->odsc_hash[n]);
    de->odsc_num++;

    list_for_each_entry_safe(sub, tmp, &de->dht_subs[n],
                             struct dht_sub_list_entry, entry)
    {
        if(bbox_does_intersect(&odsc->bb, &sub->odsc->bb)) {
            sub_odscl = malloc(sizeof(*sub_odscl));
            sub_odscl->odsc = &odscl->odsc;
            list_add(&sub_odscl->odsc_entry, &sub->recv_odsc);
            sub->pub_count++;

            bbox_intersect(&odsc->bb, &sub->odsc->bb, &isect);
            sub->remaining -= bbox_volume(&isect);
            if(sub->remaining == 0) {
                sub_complete = 1;
                list_del(&sub->entry);
            }
        }
    }
    if(sub_complete) {
        ABT_cond_broadcast(de->hash_cond[n]);
    }

    ABT_mutex_unlock(de->hash_mutex[n]);

    return 0;
}
/*
  Object descriptor 'q_odsc' can intersect multiple object descriptors
  from dht entry 'de'; find all descriptor from 'de' and return their
  number and references .
*/
int dht_find_entry_all(struct dht_entry *de, obj_descriptor *q_odsc,
                       obj_descriptor **odsc_tab[], int timeout)
{
    int n, num_odsc = 0;
    long num_elem;
    struct obj_desc_list *odscl;
    struct bbox isect;
    int sub = timeout != 0 && de == de->ss->ent_self;

    n = q_odsc->version % de->odsc_size;
    if(sub) {
        num_elem = ssh_hash_elem_count(de->ss, &q_odsc->bb);
        ABT_mutex_lock(de->hash_mutex[n]);
    }
    list_for_each_entry(odscl, &de->odsc_hash[n], struct obj_desc_list,
                        odsc_entry)
    {
        if(obj_desc_equals_intersect(&odscl->odsc, q_odsc)) {
            (*odsc_tab)[num_odsc++] = &odscl->odsc;
            if(sub) {
                bbox_intersect(&q_odsc->bb, &odscl->odsc.bb, &isect);
                num_elem -= bbox_volume(&isect);
            }
        }
    }
    if(sub) {
        if(num_elem > 0) {
            dht_local_subscribe(de, q_odsc, odsc_tab, &num_odsc, num_elem,
                                timeout);
        }
        ABT_mutex_unlock(de->hash_mutex[n]);
    }

    return num_odsc;
}

/*
  List the available versions of a data object.
*/
int dht_find_versions(struct dht_entry *de, obj_descriptor *q_odsc,
                      int odsc_vers[])
{
    struct obj_desc_list *odscl;
    int i, n = 0;

    for(i = 0; i < de->odsc_size; i++) {
        list_for_each_entry(
            odscl, &de->odsc_hash[i], struct obj_desc_list,
            odsc_entry) if(obj_desc_by_name_intersect(&odscl->odsc, q_odsc))
        {
            odsc_vers[n++] = odscl->odsc.version;
            break; /* Break the list_for_each_entry loop. */
        }
    }

    return n;
}

// meta_mutex for version hash is already locked
struct meta_data *ls_subscribe_meta(ss_storage *ls, const char *name,
                                    int version)
{
    struct meta_data *mdata;
    struct meta_sub_list_entry msub;
    int index;
    struct list_head *sub_list;

    index = version % ls->size_hash;
    sub_list = &ls->meta_subs[index];
    msub.version = version;
    msub.name = name;
    msub.mdata = NULL;
    list_add(&msub.entry, sub_list);

    do {
        ABT_cond_wait(ls->meta_cond[index], ls->meta_mutex[index]);
    } while(!msub.mdata);

    return (msub.mdata);
}

struct meta_data *ls_subscribe_meta_no_version(ss_storage *ls, const char *name)
{
    struct meta_data *mdata;
    struct meta_sub_list_entry msub;
    struct list_head *sub_list;

    sub_list = &ls->meta_subs[ls->size_hash];
    msub.version = -1;
    msub.name = name;
    msub.mdata = NULL;
    list_add(&msub.entry, sub_list);

    do {
        ABT_cond_wait(ls->meta_cond[ls->size_hash],
                      ls->meta_mutex[ls->size_hash]);
    } while(!msub.mdata);

    return (msub.mdata);
}

struct meta_data *meta_find_entry(ss_storage *ls, const char *name, int version,
                                  int wait)
{
    struct meta_data *mdata;
    int index;

    index = version % ls->size_hash;
    ABT_mutex_lock(ls->meta_mutex[index]);
    mdata = ls_find_meta(ls, name, version);
    if(mdata || !wait) {
        ABT_mutex_unlock(ls->meta_mutex[index]);
        return (mdata);
    }

    mdata = ls_subscribe_meta(ls, name, version);
    ABT_mutex_unlock(ls->meta_mutex[index]);

    return (mdata);
}

struct meta_data *meta_find_next_entry(ss_storage *ls, const char *name,
                                       int curr, int wait)
{
    int i, index;
    struct list_head *list;
    struct meta_data *mdata, *mdres;

    for(index = 0; index <= ls->size_hash; index++) {
        ABT_mutex_lock(ls->meta_mutex[index]);
    }
    mdres = NULL;
    for(i = 0; i < ls->size_hash; i++) {
        index = (curr + i + 1) % ls->size_hash;
        list = &ls->meta_hash[index];
        list_for_each_entry(mdata, list, struct meta_data, entry)
        {
            if(strcmp(mdata->name, name) == 0 && mdata->version > curr &&
               (!mdres || mdata->version < mdres->version)) {
                mdres = mdata;
                if(mdata->version < (curr + ls->size_hash)) {
                    break;
                }
            }
        }
    }
    for(index = 0; index < ls->size_hash; index++) {
        ABT_mutex_unlock(ls->meta_mutex[index]);
    }
    if(mdres || !wait) {
        ABT_mutex_unlock(ls->meta_mutex[ls->size_hash]);
        return (mdres);
    }

    mdres = ls_subscribe_meta_no_version(ls, name);
    ABT_mutex_unlock(ls->meta_mutex[ls->size_hash]);

    return (mdres);
}

void copy_global_dimension(struct global_dimension *l, int ndim,
                           const uint64_t *gdim)
{
    int i;
    l->ndim = ndim;
    memset(&l->sizes, 0, sizeof(struct coord));
    for(i = 0; i < ndim; i++) {
        l->sizes.c[i] = gdim[i];
    }
}

void init_gdim_list(struct list_head *gdim_list)
{
    if(!gdim_list)
        return;
    INIT_LIST_HEAD(gdim_list);
}

void free_gdim_list(struct list_head *gdim_list)
{
    if(!gdim_list)
        return;
    int cnt = 0;
    struct gdim_list_entry *e, *t;
    list_for_each_entry_safe(e, t, gdim_list, struct gdim_list_entry, entry)
    {
        list_del(&e->entry);
        free(e->var_name);
        free(e);
        cnt++;
    }

#ifdef DEBUG
    fprintf(stderr, "%s(): number of user-defined global dimension is %d\n",
            __func__, cnt);
#endif
}

struct gdim_list_entry *lookup_gdim_list(struct list_head *gdim_list,
                                         const char *var_name)
{
    if(!gdim_list)
        return NULL;
    struct gdim_list_entry *e;
    list_for_each_entry(e, gdim_list, struct gdim_list_entry, entry)
    {
        if(0 == strcmp(e->var_name, var_name))
            return e;
    }
    return NULL;
}

void update_gdim_list(struct list_head *gdim_list, const char *var_name,
                      int ndim, uint64_t *gdim)
{
    struct gdim_list_entry *e = lookup_gdim_list(gdim_list, var_name);
    if(!e) {
        // add new entry
        e = (struct gdim_list_entry *)malloc(sizeof(*e));
        e->var_name = malloc(strlen(var_name) + 1);
        strcpy(e->var_name, var_name);
        list_add(&e->entry, gdim_list);
    }

    // update entry
    copy_global_dimension(&e->gdim, ndim, gdim);
}

void set_global_dimension(struct list_head *gdim_list, const char *var_name,
                          const struct global_dimension *default_gdim,
                          struct global_dimension *gdim)
{
    struct gdim_list_entry *e = lookup_gdim_list(gdim_list, var_name);
    if(e) {
        memcpy(gdim, &e->gdim, sizeof(struct global_dimension));
    } else {
        memcpy(gdim, default_gdim, sizeof(struct global_dimension));
    }
}

int global_dimension_equal(const struct global_dimension *gdim1,
                           const struct global_dimension *gdim2)
{
    int i;
    for(i = 0; i < gdim1->ndim; i++) {
        if(gdim1->sizes.c[i] != gdim2->sizes.c[i])
            return 0;
    }

    return 1;
}

char **addr_str_buf_to_list(char *buf, int num_addrs)
{
    int i;
    char **ret = malloc(num_addrs * sizeof(*ret));
    if(ret == NULL)
        return NULL;

    ret[0] = (char *)buf;
    for(i = 1; i < num_addrs; i++) {
        char *a = ret[i - 1];
        ret[i] = a + strlen(a) + 1;
    }
    return ret;
}
