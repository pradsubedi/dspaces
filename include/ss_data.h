/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers University
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef __SS_DATA_H_
#define __SS_DATA_H_

#include <stdlib.h>

#include "bbox.h"
#include "list.h"
#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_atomic.h>
#include <mercury_macros.h>
#include <margo.h>

#include <abt.h>

#define MAX_VERSIONS 10

typedef struct {
	void			*iov_base;
	size_t			iov_len;
} iovec_t;

enum storage_type {row_major, column_major};

typedef struct{
        char                    name[154];

        enum storage_type       st;

        char                owner[128];
        unsigned int            version;

        /* Global bounding box descriptor. */
        struct bbox             bb;

        /* Size of one element of a data object. */
        size_t                  size;
} obj_descriptor;

struct global_dimension {
        int ndim;
        struct coord sizes;
};

struct obj_data {
        struct list_head        obj_entry;

        obj_descriptor   obj_desc;

        struct global_dimension gdim;

        void                    *data;		/* Aligned pointer */

        /* Reference to the parent object; used only for sub-objects. */
        struct obj_data         *obj_ref;

        /* Count how many references are to this data object. */
        int                     refcnt;

        /* Flag to mark if we should free this data object. */
        unsigned int            f_free:1;


};



struct gdim_list_entry {
        struct list_head    entry;
        char *var_name;       
        struct global_dimension gdim; 
};

typedef struct {
        int                     num_obj;
        int                     size_hash;
        /* List of data objects. */
        struct list_head        obj_hash[1];
} ss_storage;

struct obj_desc_list {
	struct list_head	odsc_entry;
	obj_descriptor	odsc;
};

struct obj_desc_ptr_list {
    struct list_head odsc_entry;
    obj_descriptor *odsc;
};

typedef struct{
        size_t size;
        char *raw_odsc;

} odsc_hdr;

typedef struct{
        size_t size;
        size_t gdim_size;
        char *raw_odsc;
        char *raw_gdim;

} odsc_hdr_with_gdim;

struct dht_sub_list_entry {
    obj_descriptor *odsc; //subbed object
    long remaining;
    int pub_count;
    struct list_head recv_odsc;
    struct list_head entry;
};

struct dht_entry {
        /* Global info. */
        struct sspace           *ss;
        struct bbox             bb;

        int                     rank;

        struct intv             i_virt;

        int                     num_intv;
        struct intv             *i_tab;

        int num_bbox;
        int size_bb_tab;
        struct bbox             *bb_tab;

        ABT_mutex *hash_mutex;
        ABT_cond *hash_cond;
        struct list_head *dht_subs;

        int     odsc_size, odsc_num;
        struct list_head  odsc_hash[1];
};

struct dht {
        struct bbox             bb_glb_domain;

        int                     num_entries;
        struct dht_entry        *ent_tab[1];
};

enum sspace_hash_version {
    ssd_hash_version_v1 = 1, // (default) decompose the global data domain
                             //  using hilbert SFC
    ssd_hash_version_v2, // decompose the global data domain using
                         // recursive bisection of the longest dimension   
    _ssd_hash_version_count,
};

/*
  Shared space structure.
*/
struct sspace {
        uint64_t                   max_dim;
        unsigned int            bpd;

        struct dht              *dht;

        int                     rank;
        /* Pointer into "dht.ent_tab" corresponding to this node. */
        struct dht_entry        *ent_self;

        // for v2 
        int total_num_bbox;
        enum sspace_hash_version    hash_version;
};

struct sspace_list_entry {
        struct list_head    entry;
        struct global_dimension gdim;
        struct sspace   *ssd;
};

typedef struct{
        int num_dims;
        int num_space_srv;
        int    max_versions; 
        enum sspace_hash_version    hash_version;
        struct bbox             ss_domain;
        struct global_dimension default_gdim;
        int rank;
} ss_info_hdr;


static inline hg_return_t hg_proc_odsc_hdr(hg_proc_t proc, void *arg)
{
  hg_return_t ret;
  odsc_hdr *in = (odsc_hdr*)arg;
  ret = hg_proc_hg_size_t(proc, &in->size);
  if(ret != HG_SUCCESS) return ret;
  if (in->size) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
        ret = hg_proc_raw(proc, in->raw_odsc, in->size);
        if(ret != HG_SUCCESS) return ret;
      break;
    case HG_DECODE:
      in->raw_odsc = (char*)malloc(in->size);
      ret = hg_proc_raw(proc, in->raw_odsc, in->size);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_FREE:
      free(in->raw_odsc);
      break;
    default:
      break;
    }
  }
  return HG_SUCCESS;
}

static inline hg_return_t hg_proc_odsc_hdr_with_gdim(hg_proc_t proc, void *arg)
{
  hg_return_t ret;
  odsc_hdr_with_gdim *in = (odsc_hdr_with_gdim*)arg;
  ret = hg_proc_hg_size_t(proc, &in->size);
  ret = hg_proc_hg_size_t(proc, &in->gdim_size);
  if(ret != HG_SUCCESS) return ret;
  if (in->size) {
    switch (hg_proc_get_op(proc)) {
    case HG_ENCODE:
        ret = hg_proc_raw(proc, in->raw_odsc, in->size);
        if(ret != HG_SUCCESS) return ret;
        ret = hg_proc_raw(proc, in->raw_gdim, in->gdim_size);
        if(ret != HG_SUCCESS) return ret;
      break;
    case HG_DECODE:
      in->raw_odsc = (char*)malloc(in->size);
      ret = hg_proc_raw(proc, in->raw_odsc, in->size);
      if(ret != HG_SUCCESS) return ret;
      in->raw_gdim = (char*)malloc(in->gdim_size);
      ret = hg_proc_raw(proc, in->raw_gdim, in->gdim_size);
      if(ret != HG_SUCCESS) return ret;
      break;
    case HG_FREE:
      free(in->raw_odsc);
      free(in->raw_gdim);
      break;
    default:
      break;
    }
  }
  return HG_SUCCESS;
}

MERCURY_GEN_PROC(bulk_gdim_t,
        ((odsc_hdr_with_gdim)(odsc))\
        ((hg_bulk_t)(handle)))
MERCURY_GEN_PROC(bulk_in_t,
        ((odsc_hdr)(odsc))\
        ((hg_bulk_t)(handle)))
MERCURY_GEN_PROC(bulk_out_t, ((int32_t)(ret)))
MERCURY_GEN_PROC(odsc_gdim_t, ((odsc_hdr_with_gdim)(odsc_gdim))((int32_t)(param)))
MERCURY_GEN_PROC(odsc_list_t, ((odsc_hdr)(odsc_list)))
MERCURY_GEN_PROC(ss_information, ((odsc_hdr)(ss_buf)))

char * obj_desc_sprint(obj_descriptor *);
//
struct sspace* ssd_alloc(const struct bbox *, int, int, enum sspace_hash_version);
struct sspace *ssd_alloc_v2(const struct bbox *bb_domain, int num_nodes, int max_versions);
int ssd_init(struct sspace *, int);
void ssd_free(struct sspace *);
//

int ssd_copy(struct obj_data *, struct obj_data *);
//
long ssh_hash_elem_count(struct sspace *ss, const struct bbox *bb);
//
int ssd_filter(struct obj_data *, obj_descriptor *, double *);
int ssd_hash(struct sspace *, const struct bbox *, struct dht_entry *[]);

int dht_update_owner(struct dht_entry *de, obj_descriptor *odsc);
int dht_add_entry(struct dht_entry *, obj_descriptor *);
obj_descriptor * dht_find_entry(struct dht_entry *, obj_descriptor *);
int dht_find_entry_all(struct dht_entry *, obj_descriptor *, 
                       obj_descriptor **[], int);
int dht_find_versions(struct dht_entry *, obj_descriptor *, int []);
//

ss_storage *ls_alloc(int max_versions);
void ls_free(ss_storage *);
void ls_add_obj(ss_storage *, struct obj_data *);
struct obj_data* ls_lookup(ss_storage *, char *);
void ls_remove(ss_storage *, struct obj_data *);
void ls_try_remove_free(ss_storage *, struct obj_data *);
struct obj_data * ls_find(ss_storage *, obj_descriptor *);
struct obj_data *ls_find_od(ss_storage *, obj_descriptor *);
int ls_find_ods(ss_storage *, obj_descriptor *, struct obj_data **);
struct obj_data * ls_find_no_version(ss_storage *, obj_descriptor *);

struct obj_data *obj_data_alloc(obj_descriptor *);
struct obj_data * obj_data_alloc_no_data(obj_descriptor *, void *);
struct obj_data *obj_data_alloc_with_data(obj_descriptor *, void *);

void obj_data_free(struct obj_data *od);
uint64_t obj_data_size(obj_descriptor *);

int obj_desc_equals(obj_descriptor *, obj_descriptor *);
int obj_desc_equals_no_owner(const obj_descriptor *, const obj_descriptor *);

int obj_desc_equals_intersect(obj_descriptor *odsc1,
                obj_descriptor *odsc2);

int obj_desc_by_name_intersect(const obj_descriptor *odsc1,
                const obj_descriptor *odsc2);

//void copy_global_dimension(struct global_dimension *l, int ndim, const uint64_t *gdim);
int global_dimension_equal(const struct global_dimension* gdim1,
                const struct global_dimension* gdim2);
void init_gdim_list(struct list_head *gdim_list);
void update_gdim_list(struct list_head *gdim_list,
                const char *var_name, int ndim, uint64_t *gdim);
struct gdim_list_entry* lookup_gdim_list(struct list_head *gdim_list, const char *var_name);
void free_gdim_list(struct list_head *gdim_list);
void set_global_dimension(struct list_head *gdim_list, const char *var_name,
            const struct global_dimension *default_gdim, struct global_dimension *gdim);

struct lock_data *get_lock(struct list_head *list, char* name);
struct lock_data *create_lock(struct list_head *list, char* name);

char ** addr_str_buf_to_list(char * buf, int num_addrs);





#endif /* __SS_DATA_H_ */
