/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers
 * University
 *
 * See COPYRIGHT in top-level directory.
 */
#include "dspaces-server.h"
#include "dspaces.h"
#include "dspacesp.h"
#include "gspace.h"
#include "ss_data.h"
#include <abt.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define DEBUG_OUT(args...)                                                     \
    do {                                                                       \
        if(server->f_debug) {                                                  \
            fprintf(stderr, "Rank %i: %s, line %i (%s): ", server->rank,       \
                    __FILE__, __LINE__, __func__);                             \
            fprintf(stderr, args);                                             \
        }                                                                      \
    } while(0);

#define DSPACES_DEFAULT_NUM_HANDLERS 4

static enum storage_type st = column_major;

typedef enum obj_update_type { DS_OBJ_NEW, DS_OBJ_OWNER } obj_update_t;

int cond_num = 0;

struct addr_list_entry {
    struct list_head entry;
    char *addr;
};

struct dspaces_provider {
    margo_instance_id mid;
    hg_id_t put_id;
    hg_id_t put_local_id;
    hg_id_t put_meta_id;
    hg_id_t query_id;
    hg_id_t query_meta_id;
    hg_id_t get_id;
    hg_id_t get_local_id;
    hg_id_t obj_update_id;
    hg_id_t odsc_internal_id;
    hg_id_t ss_id;
    hg_id_t drain_id;
    hg_id_t kill_id;
    hg_id_t kill_client_id;
    hg_id_t sub_id;
    hg_id_t notify_id;
    struct ds_gspace *dsg;
    char **server_address;
    char **node_names;
    char *listen_addr_str;
    int rank;
    int comm_size;
    int f_debug;
    int f_drain;
    int f_kill;

    MPI_Comm comm;

    ABT_mutex odsc_mutex;
    ABT_mutex ls_mutex;
    ABT_mutex dht_mutex;
    ABT_mutex sspace_mutex;
    ABT_mutex kill_mutex;

    ABT_xstream drain_xstream;
    ABT_pool drain_pool;
    ABT_thread drain_t;
};

DECLARE_MARGO_RPC_HANDLER(put_rpc);
DECLARE_MARGO_RPC_HANDLER(put_local_rpc);
DECLARE_MARGO_RPC_HANDLER(put_meta_rpc);
DECLARE_MARGO_RPC_HANDLER(get_rpc);
DECLARE_MARGO_RPC_HANDLER(query_rpc);
DECLARE_MARGO_RPC_HANDLER(query_meta_rpc);
DECLARE_MARGO_RPC_HANDLER(obj_update_rpc);
DECLARE_MARGO_RPC_HANDLER(odsc_internal_rpc);
DECLARE_MARGO_RPC_HANDLER(ss_rpc);
DECLARE_MARGO_RPC_HANDLER(kill_rpc);
DECLARE_MARGO_RPC_HANDLER(sub_rpc);

static void put_rpc(hg_handle_t h);
static void put_local_rpc(hg_handle_t h);
static void put_meta_rpc(hg_handle_t h);
static void get_rpc(hg_handle_t h);
static void query_rpc(hg_handle_t h);
static void query_meta_rpc(hg_handle_t h);
static void obj_update_rpc(hg_handle_t h);
static void odsc_internal_rpc(hg_handle_t h);
static void ss_rpc(hg_handle_t h);
static void kill_rpc(hg_handle_t h);
static void sub_rpc(hg_handle_t h);
// static void write_lock_rpc(hg_handle_t h);
// static void read_lock_rpc(hg_handle_t h);

/* Server configuration parameters */
static struct {
    int ndim;
    struct coord dims;
    int max_versions;
    int max_readers;
    int lock_type;    /* 1 - generic, 2 - custom */
    int hash_version; /* 1 - ssd_hash_version_v1, 2 - ssd_hash_version_v2 */
    int num_apps;
} ds_conf;

static struct {
    const char *opt;
    int *pval;
} options[] = {{"ndim", &ds_conf.ndim},
               {"dims", (int *)&ds_conf.dims},
               {"max_versions", &ds_conf.max_versions},
               {"max_readers", &ds_conf.max_readers},
               {"lock_type", &ds_conf.lock_type},
               {"hash_version", &ds_conf.hash_version},
               {"num_apps", &ds_conf.num_apps}};

static void eat_spaces(char *line)
{
    char *t = line;

    while(t && *t) {
        if(*t != ' ' && *t != '\t' && *t != '\n')
            *line++ = *t;
        t++;
    }
    if(line)
        *line = '\0';
}

static int parse_line(int lineno, char *line)
{
    char *t;
    int i, n;

    /* Comment line ? */
    if(line[0] == '#')
        return 0;

    t = strstr(line, "=");
    if(!t) {
        eat_spaces(line);
        if(strlen(line) == 0)
            return 0;
        else
            return -EINVAL;
    }

    t[0] = '\0';
    eat_spaces(line);
    t++;

    n = sizeof(options) / sizeof(options[0]);

    for(i = 0; i < n; i++) {
        if(strcmp(line, options[1].opt) == 0) { /**< when "dims" */
            // get coordinates
            int idx = 0;
            char *crd;
            crd = strtok(t, ",");
            while(crd != NULL) {
                ((struct coord *)options[1].pval)->c[idx] = atoll(crd);
                crd = strtok(NULL, ",");
                idx++;
            }
            if(idx != *(int *)options[0].pval) {
                fprintf(stderr, "ERROR: (%s): dimensionality mismatch.\n",
                        __func__);
                fprintf(stderr, "ERROR: index=%d, ndims=%d\n", idx,
                        *(int *)options[0].pval);
                return -EINVAL;
            }
            break;
        }
        if(strcmp(line, options[i].opt) == 0) {
            eat_spaces(line);
            *(int *)options[i].pval = atoi(t);
            break;
        }
    }

    if(i == n) {
        fprintf(stderr, "WARNING: (%s): unknown option '%s' at line %d.\n",
                __func__, line, lineno);
    }
    return 0;
}

static int parse_conf(char *fname)
{
    FILE *fin;
    char buff[1024];
    int lineno = 1, err;

    fin = fopen(fname, "rt");
    if(!fin)
        return -errno;

    while(fgets(buff, sizeof(buff), fin) != NULL) {
        err = parse_line(lineno++, buff);
        if(err < 0) {
            fclose(fin);
            return err;
        }
    }

    fclose(fin);
    return 0;
}

static int init_sspace(struct bbox *default_domain, struct ds_gspace *dsg_l)
{
    int err = -ENOMEM;
    dsg_l->ssd = ssd_alloc(default_domain, dsg_l->size_sp, ds_conf.max_versions,
                           ds_conf.hash_version);
    if(!dsg_l->ssd)
        goto err_out;

    err = ssd_init(dsg_l->ssd, dsg_l->rank);
    if(err < 0)
        goto err_out;

    dsg_l->default_gdim.ndim = ds_conf.ndim;
    int i;
    for(i = 0; i < ds_conf.ndim; i++) {
        dsg_l->default_gdim.sizes.c[i] = ds_conf.dims.c[i];
    }

    INIT_LIST_HEAD(&dsg_l->sspace_list);
    return 0;
err_out:
    fprintf(stderr, "%s(): ERROR failed\n", __func__);
    return err;
}

static int write_conf(dspaces_provider_t server, MPI_Comm comm)
{
    hg_addr_t my_addr = HG_ADDR_NULL;
    char *my_addr_str = NULL;
    char my_node_str[HOST_NAME_MAX];
    hg_size_t my_addr_size = 0;
    int my_node_name_len = 0;
    int *str_sizes;
    hg_return_t hret = HG_SUCCESS;
    int buf_size = 0;
    int *sizes_psum;
    char *str_buf;
    FILE *fd;
    int i, ret;

    hret = margo_addr_self(server->mid, &my_addr);
    if(hret != HG_SUCCESS) {
        fprintf(stderr, "ERROR: (%s): margo_addr_self() returned %d\n",
                __func__, hret);
        ret = -1;
        goto error;
    }

    hret = margo_addr_to_string(server->mid, NULL, &my_addr_size, my_addr);
    if(hret != HG_SUCCESS) {
        fprintf(stderr, "ERROR: (%s): margo_addr_to_string() returned %d\n",
                __func__, hret);
        ret = -1;
        goto errorfree;
    }

    my_addr_str = malloc(my_addr_size);
    hret =
        margo_addr_to_string(server->mid, my_addr_str, &my_addr_size, my_addr);
    if(hret != HG_SUCCESS) {
        fprintf(stderr, "ERROR: (%s): margo_addr_to_string() returned %d\n",
                __func__, hret);
        ret = -1;
        goto errorfree;
    }

    MPI_Comm_size(comm, &server->comm_size);
    str_sizes = malloc(server->comm_size * sizeof(*str_sizes));
    sizes_psum = malloc(server->comm_size * sizeof(*sizes_psum));
    MPI_Allgather(&my_addr_size, 1, MPI_INT, str_sizes, 1, MPI_INT, comm);
    sizes_psum[0] = 0;
    for(i = 0; i < server->comm_size; i++) {
        buf_size += str_sizes[i];
        if(i) {
            sizes_psum[i] = sizes_psum[i - 1] + str_sizes[i - 1];
        }
    }
    str_buf = malloc(buf_size);
    MPI_Allgatherv(my_addr_str, my_addr_size, MPI_CHAR, str_buf, str_sizes,
                   sizes_psum, MPI_CHAR, comm);

    server->server_address =
        malloc(server->comm_size * sizeof(*server->server_address));
    for(i = 0; i < server->comm_size; i++) {
        server->server_address[i] = &str_buf[sizes_psum[i]];
    }

    gethostname(my_node_str, HOST_NAME_MAX);
    my_node_str[HOST_NAME_MAX - 1] = '\0';
    my_node_name_len = strlen(my_node_str) + 1;
    MPI_Allgather(&my_node_name_len, 1, MPI_INT, str_sizes, 1, MPI_INT, comm);
    sizes_psum[0] = 0;
    buf_size = 0;
    for(i = 0; i < server->comm_size; i++) {
        buf_size += str_sizes[i];
        if(i) {
            sizes_psum[i] = sizes_psum[i - 1] + str_sizes[i - 1];
        }
    }
    str_buf = malloc(buf_size);
    MPI_Allgatherv(my_node_str, my_node_name_len, MPI_CHAR, str_buf, str_sizes,
                   sizes_psum, MPI_CHAR, comm);
    server->node_names =
        malloc(server->comm_size * sizeof(*server->node_names));
    for(i = 0; i < server->comm_size; i++) {
        server->node_names[i] = &str_buf[sizes_psum[i]];
    }

    MPI_Comm_rank(comm, &server->rank);
    if(server->rank == 0) {
        fd = fopen("conf.ds", "w");
        if(!fd) {
            fprintf(stderr,
                    "ERROR: %s: unable to open 'conf.ds' for writing.\n",
                    __func__);
            ret = -1;
            goto errorfree;
        }
        fprintf(fd, "%d\n", server->comm_size);
        for(i = 0; i < server->comm_size; i++) {
            fprintf(fd, "%s %s\n", server->node_names[i],
                    server->server_address[i]);
        }
        fprintf(fd, "%s\n", server->listen_addr_str);
        fclose(fd);
    }

    free(my_addr_str);
    free(str_sizes);
    free(sizes_psum);
    margo_addr_free(server->mid, my_addr);

    return (ret);

errorfree:
    margo_addr_free(server->mid, my_addr);
error:
    margo_finalize(server->mid);
    return (ret);
}

static int dsg_alloc(dspaces_provider_t server, char *conf_name, MPI_Comm comm)
{
    struct ds_gspace *dsg_l;
    int err = -ENOMEM;

    /* Default values */
    ds_conf.max_versions = 1;
    ds_conf.max_readers = 1;
    ds_conf.lock_type = 1;
    ds_conf.hash_version = ssd_hash_version_v1;
    ds_conf.num_apps = 1;

    err = parse_conf(conf_name);
    if(err < 0) {
        fprintf(stderr, "%s(): ERROR failed to load config file '%s'.",
                __func__, conf_name);
        goto err_out;
    }

    // Check number of dimension
    if(ds_conf.ndim > BBOX_MAX_NDIM) {
        fprintf(
            stderr,
            "%s(): ERROR maximum number of array dimension is %d but ndim is %d"
            " in file '%s'\n",
            __func__, BBOX_MAX_NDIM, ds_conf.ndim, conf_name);
        err = -EINVAL;
        goto err_out;
    }

    // Check hash version
    if((ds_conf.hash_version < ssd_hash_version_v1) ||
       (ds_conf.hash_version >= _ssd_hash_version_count)) {
        fprintf(stderr, "%s(): ERROR unknown hash version %d in file '%s'\n",
                __func__, ds_conf.hash_version, conf_name);
        err = -EINVAL;
        goto err_out;
    }

    struct bbox domain;
    memset(&domain, 0, sizeof(struct bbox));
    domain.num_dims = ds_conf.ndim;
    int i;
    for(i = 0; i < domain.num_dims; i++) {
        domain.lb.c[i] = 0;
        domain.ub.c[i] = ds_conf.dims.c[i] - 1;
    }

    dsg_l = malloc(sizeof(*dsg_l));
    if(!dsg_l)
        goto err_out;

    MPI_Comm_size(comm, &(dsg_l->size_sp));

    MPI_Comm_rank(comm, &dsg_l->rank);

    write_conf(server, comm);

    err = init_sspace(&domain, dsg_l);
    if(err < 0) {
        goto err_free;
    }
    dsg_l->ls = ls_alloc(ds_conf.max_versions);
    if(!dsg_l->ls) {
        fprintf(stderr, "%s(): ERROR ls_alloc() failed\n", __func__);
        goto err_free;
    }

    dsg_l->num_apps = ds_conf.num_apps;

    INIT_LIST_HEAD(&dsg_l->obj_desc_drain_list);

    server->dsg = dsg_l;
    return 0;
err_free:
    free(dsg_l);
err_out:
    fprintf(stderr, "'%s()': failed with %d.\n", __func__, err);
    return err;
}

static int free_sspace(struct ds_gspace *dsg_l)
{
    ssd_free(dsg_l->ssd);
    struct sspace_list_entry *ssd_entry, *temp;
    list_for_each_entry_safe(ssd_entry, temp, &dsg_l->sspace_list,
                             struct sspace_list_entry, entry)
    {
        ssd_free(ssd_entry->ssd);
        list_del(&ssd_entry->entry);
        free(ssd_entry);
    }

    return 0;
}

static struct sspace *lookup_sspace(dspaces_provider_t server,
                                    const char *var_name,
                                    const struct global_dimension *gd)
{
    struct global_dimension gdim;
    struct ds_gspace *dsg_l = server->dsg;
    memcpy(&gdim, gd, sizeof(struct global_dimension));

    // Return the default shared space created based on
    // global data domain specified in dataspaces.conf
    if(global_dimension_equal(&gdim, &dsg_l->default_gdim)) {
        DEBUG_OUT("uses default gdim\n");
        return dsg_l->ssd;
    }

    // Otherwise, search for shared space based on the
    // global data domain specified by application in put()/get().
    struct sspace_list_entry *ssd_entry = NULL;
    list_for_each_entry(ssd_entry, &dsg_l->sspace_list,
                        struct sspace_list_entry, entry)
    {
        // compare global dimension
        if(gdim.ndim != ssd_entry->gdim.ndim)
            continue;

        if(global_dimension_equal(&gdim, &ssd_entry->gdim))
            return ssd_entry->ssd;
    }

    DEBUG_OUT("didn't find an existing shared space. Make a new one.\n");

    // If not found, add new shared space
    int i, err;
    struct bbox domain;
    memset(&domain, 0, sizeof(struct bbox));
    domain.num_dims = gdim.ndim;
    for(i = 0; i < gdim.ndim; i++) {
        domain.lb.c[i] = 0;
        domain.ub.c[i] = gdim.sizes.c[i] - 1;
    }

    ssd_entry = malloc(sizeof(struct sspace_list_entry));
    memcpy(&ssd_entry->gdim, &gdim, sizeof(struct global_dimension));

    DEBUG_OUT("allocate the ssd.\n");
    ssd_entry->ssd = ssd_alloc(&domain, dsg_l->size_sp, ds_conf.max_versions,
                               ds_conf.hash_version);
    if(!ssd_entry->ssd) {
        fprintf(stderr, "%s(): ssd_alloc failed for '%s'\n", __func__,
                var_name);
        return dsg_l->ssd;
    }

    DEBUG_OUT("doing ssd init\n");
    err = ssd_init(ssd_entry->ssd, dsg_l->rank);
    if(err < 0) {
        fprintf(stderr, "%s(): ssd_init failed\n", __func__);
        return dsg_l->ssd;
    }

    list_add(&ssd_entry->entry, &dsg_l->sspace_list);
    return ssd_entry->ssd;
}

static int obj_update_dht(dspaces_provider_t server, struct obj_data *od,
                          obj_update_t type)
{
    obj_descriptor *odsc = &od->obj_desc;
    ABT_mutex_lock(server->sspace_mutex);
    struct sspace *ssd = lookup_sspace(server, odsc->name, &od->gdim);
    ABT_mutex_unlock(server->sspace_mutex);
    struct dht_entry *dht_tab[ssd->dht->num_entries];

    int num_de, i, err;

    /* Compute object distribution to nodes in the space. */
    num_de = ssd_hash(ssd, &odsc->bb, dht_tab);
    if(num_de == 0) {
        fprintf(stderr, "'%s()': this should not happen, num_de == 0 ?!\n",
                __func__);
    }
    /* Update object descriptors on the corresponding nodes. */
    for(i = 0; i < num_de; i++) {
        if(dht_tab[i]->rank == server->dsg->rank) {
            DEBUG_OUT("Add in local_dht %d\n", server->dsg->rank);
            ABT_mutex_lock(server->dht_mutex);
            switch(type) {
            case DS_OBJ_NEW:
                dht_add_entry(ssd->ent_self, odsc);
                break;
            case DS_OBJ_OWNER:
                dht_update_owner(ssd->ent_self, odsc, 1);
                break;
            default:
                fprintf(stderr, "ERROR: (%s): unknown object update type.\n",
                        __func__);
            }
            ABT_mutex_unlock(server->dht_mutex);
            DEBUG_OUT("I am self, added in local dht %d\n", server->dsg->rank);
            continue;
        }

        // now send rpc to the server for dht_update
        hg_return_t hret;
        odsc_gdim_t in;
        DEBUG_OUT("Server %d sending object %s to dht server %d \n",
                  server->dsg->rank, obj_desc_sprint(odsc), dht_tab[i]->rank);

        in.odsc_gdim.size = sizeof(*odsc);
        in.odsc_gdim.gdim_size = sizeof(struct global_dimension);
        in.odsc_gdim.raw_odsc = (char *)(odsc);
        in.odsc_gdim.raw_gdim = (char *)(&od->gdim);
        in.param = type;

        hg_addr_t svr_addr;
        margo_addr_lookup(server->mid, server->server_address[dht_tab[i]->rank],
                          &svr_addr);

        hg_handle_t h;
        margo_create(server->mid, svr_addr, server->obj_update_id, &h);
        margo_forward(h, &in);
        DEBUG_OUT("sent obj server %d to update dht %s in \n", dht_tab[i]->rank,
                  obj_desc_sprint(odsc));

        margo_addr_free(server->mid, svr_addr);
        hret = margo_destroy(h);
        if(hret != HG_SUCCESS) {
            fprintf(stderr, "ERROR: (%s): could not destroy handle!\n",
                    __func__);
            return (dspaces_ERR_MERCURY);
        }
    }

    return dspaces_SUCCESS;
}

static int get_client_data(obj_descriptor odsc, dspaces_provider_t server)
{
    bulk_in_t in;
    bulk_out_t out;
    struct obj_data *od;
    od = malloc(sizeof(struct obj_data));
    int ret;

    od = obj_data_alloc(&odsc);
    in.odsc.size = sizeof(obj_descriptor);
    in.odsc.raw_odsc = (char *)(&odsc);

    hg_addr_t owner_addr;
    size_t owner_addr_size = 128;

    margo_addr_self(server->mid, &owner_addr);
    margo_addr_to_string(server->mid, od->obj_desc.owner, &owner_addr_size,
                         owner_addr);
    margo_addr_free(server->mid, owner_addr);

    hg_size_t rdma_size = (odsc.size) * bbox_volume(&odsc.bb);

    margo_bulk_create(server->mid, 1, (void **)(&(od->data)), &rdma_size,
                      HG_BULK_WRITE_ONLY, &in.handle);
    hg_addr_t client_addr;
    margo_addr_lookup(server->mid, odsc.owner, &client_addr);

    hg_handle_t handle;
    margo_create(server->mid, client_addr, server->drain_id, &handle);
    margo_forward(handle, &in);
    margo_get_output(handle, &out);
    if(out.ret == dspaces_SUCCESS) {
        ABT_mutex_lock(server->ls_mutex);
        ls_add_obj(server->dsg->ls, od);
        ABT_mutex_unlock(server->ls_mutex);
    }
    ret = out.ret;
    // now update the dht with new owner information
    DEBUG_OUT("Inside get_client_data\n");
    margo_addr_free(server->mid, client_addr);
    margo_bulk_free(in.handle);
    margo_free_output(handle, &out);
    margo_destroy(handle);
    obj_update_dht(server, od, DS_OBJ_OWNER);
    return ret;
}

// thread to move data between layers
static void drain_thread(void *arg)
{
    dspaces_provider_t server = arg;

    while(server->f_kill > 0) {
        int counter = 0;
        DEBUG_OUT("Thread WOKEUP\n");
        do {
            counter = 0;
            obj_descriptor odsc;
            struct obj_desc_list *odscl;
            // requires better way to get the obj_descriptor
            ABT_mutex_lock(server->odsc_mutex);
            DEBUG_OUT("Inside odsc mutex\n");
            list_for_each_entry(odscl, &(server->dsg->obj_desc_drain_list),
                                struct obj_desc_list, odsc_entry)
            {
                memcpy(&odsc, &(odscl->odsc), sizeof(obj_descriptor));
                DEBUG_OUT("Found %s in odsc_list\n", obj_desc_sprint(&odsc));
                counter = 1;
                break;
            }
            if(counter == 1) {
                list_del(&odscl->odsc_entry);
                ABT_mutex_unlock(server->odsc_mutex);
                int ret = get_client_data(odsc, server);
                DEBUG_OUT("Finished draining %s\n", obj_desc_sprint(&odsc));
                if(ret != dspaces_SUCCESS) {
                    ABT_mutex_lock(server->odsc_mutex);
                    DEBUG_OUT("Drain failed, returning object to queue...\n");
                    list_add_tail(&odscl->odsc_entry,
                                  &server->dsg->obj_desc_drain_list);
                    ABT_mutex_unlock(server->odsc_mutex);
                }
                sleep(1);
            } else {
                ABT_mutex_unlock(server->odsc_mutex);
            }

        } while(counter == 1);

        sleep(10);

        ABT_thread_yield();
    }
}

int dspaces_server_init(char *listen_addr_str, MPI_Comm comm,
                        dspaces_provider_t *sv)
{
    const char *envdebug = getenv("DSPACES_DEBUG");
    const char *envnthreads = getenv("DSPACES_NUM_HANDLERS");
    const char *envdrain = getenv("DSPACES_DRAIN");
    dspaces_provider_t server;
    hg_class_t *hg;
    static int is_initialized = 0;
    hg_bool_t flag;
    hg_id_t id;
    int num_handlers = DSPACES_DEFAULT_NUM_HANDLERS;
    int ret;

    if(is_initialized) {
        fprintf(stderr,
                "DATASPACES: WARNING: %s: multiple instantiations of the "
                "dataspaces server is not supported.\n",
                __func__);
        return (dspaces_ERR_ALLOCATION);
    }

    server = (dspaces_provider_t)calloc(1, sizeof(*server));
    if(server == NULL)
        return dspaces_ERR_ALLOCATION;

    if(envdebug) {
        server->f_debug = 1;
    }

    if(envnthreads) {
        num_handlers = atoi(envnthreads);
    }

    if(envdrain) {
        server->f_drain = 1;
    }

    MPI_Comm_dup(comm, &server->comm);
    MPI_Comm_rank(comm, &server->rank);

    ABT_init(0, NULL);

    server->mid =
        margo_init(listen_addr_str, MARGO_SERVER_MODE, 1, num_handlers);
    if(!server->mid) {
        fprintf(stderr, "ERROR: %s: margo_init() failed.\n", __func__);
        return (dspaces_ERR_MERCURY);
    }

    server->listen_addr_str = strdup(listen_addr_str);

    ret = ABT_mutex_create(&server->odsc_mutex);
    ret = ABT_mutex_create(&server->ls_mutex);
    ret = ABT_mutex_create(&server->dht_mutex);
    ret = ABT_mutex_create(&server->sspace_mutex);
    ret = ABT_mutex_create(&server->kill_mutex);

    hg = margo_get_class(server->mid);

    margo_registered_name(server->mid, "put_rpc", &id, &flag);

    if(flag == HG_TRUE) { /* RPCs already registered */
        DEBUG_OUT("RPC names already registered. Setting handlers...\n");
        margo_registered_name(server->mid, "put_rpc", &server->put_id, &flag);
        DS_HG_REGISTER(hg, server->put_id, bulk_gdim_t, bulk_out_t, put_rpc);
        margo_registered_name(server->mid, "put_local_rpc",
                              &server->put_local_id, &flag);
        DS_HG_REGISTER(hg, server->put_local_id, odsc_gdim_t, bulk_out_t,
                       put_local_rpc);
        margo_registered_name(server->mid, "put_meta_rpc", &server->put_meta_id,
                              &flag);
        DS_HG_REGISTER(hg, server->put_meta_id, put_meta_in_t, bulk_out_t,
                       put_meta_rpc);
        margo_registered_name(server->mid, "get_rpc", &server->get_id, &flag);
        DS_HG_REGISTER(hg, server->get_id, bulk_in_t, bulk_out_t, get_rpc);
        margo_registered_name(server->mid, "get_local_rpc",
                              &server->get_local_id, &flag);
        margo_registered_name(server->mid, "query_rpc", &server->query_id,
                              &flag);
        DS_HG_REGISTER(hg, server->query_id, odsc_gdim_t, odsc_list_t,
                       query_rpc);
        margo_registered_name(server->mid, "query_meta_rpc",
                              &server->query_meta_id, &flag);
        DS_HG_REGISTER(hg, server->query_meta_id, query_meta_in_t,
                       query_meta_out_t, query_meta_rpc);
        margo_registered_name(server->mid, "obj_update_rpc",
                              &server->obj_update_id, &flag);
        DS_HG_REGISTER(hg, server->obj_update_id, odsc_gdim_t, void,
                       obj_update_rpc);
        margo_registered_name(server->mid, "odsc_internal_rpc",
                              &server->odsc_internal_id, &flag);
        DS_HG_REGISTER(hg, server->odsc_internal_id, odsc_gdim_t, odsc_list_t,
                       odsc_internal_rpc);
        margo_registered_name(server->mid, "ss_rpc", &server->ss_id, &flag);
        DS_HG_REGISTER(hg, server->ss_id, void, ss_information, ss_rpc);
        margo_registered_name(server->mid, "drain_rpc", &server->drain_id,
                              &flag);
        margo_registered_name(server->mid, "kill_rpc", &server->kill_id, &flag);
        DS_HG_REGISTER(hg, server->kill_id, int32_t, void, kill_rpc);
        margo_registered_name(server->mid, "kill_client_rpc",
                              &server->kill_client_id, &flag);
        margo_registered_name(server->mid, "sub_rpc", &server->sub_id, &flag);
        DS_HG_REGISTER(hg, server->sub_id, odsc_gdim_t, void, sub_rpc);
        margo_registered_name(server->mid, "notify_rpc", &server->notify_id,
                              &flag);
    } else {
        server->put_id = MARGO_REGISTER(server->mid, "put_rpc", bulk_gdim_t,
                                        bulk_out_t, put_rpc);
        margo_register_data(server->mid, server->put_id, (void *)server, NULL);
        server->put_local_id =
            MARGO_REGISTER(server->mid, "put_local_rpc", odsc_gdim_t,
                           bulk_out_t, put_local_rpc);
        margo_register_data(server->mid, server->put_local_id, (void *)server,
                            NULL);
        server->put_meta_id =
            MARGO_REGISTER(server->mid, "put_meta_rpc", put_meta_in_t,
                           bulk_out_t, put_meta_rpc);
        margo_register_data(server->mid, server->put_meta_id, (void *)server,
                            NULL);
        server->get_id = MARGO_REGISTER(server->mid, "get_rpc", bulk_in_t,
                                        bulk_out_t, get_rpc);
        server->get_local_id = MARGO_REGISTER(server->mid, "get_local_rpc",
                                              bulk_in_t, bulk_out_t, NULL);
        margo_register_data(server->mid, server->get_id, (void *)server, NULL);
        server->query_id = MARGO_REGISTER(server->mid, "query_rpc", odsc_gdim_t,
                                          odsc_list_t, query_rpc);
        margo_register_data(server->mid, server->query_id, (void *)server,
                            NULL);
        server->query_meta_id =
            MARGO_REGISTER(server->mid, "query_meta_rpc", query_meta_in_t,
                           query_meta_out_t, query_meta_rpc);
        margo_register_data(server->mid, server->query_meta_id, (void *)server,
                            NULL);
        server->obj_update_id = MARGO_REGISTER(
            server->mid, "obj_update_rpc", odsc_gdim_t, void, obj_update_rpc);
        margo_register_data(server->mid, server->obj_update_id, (void *)server,
                            NULL);
        margo_registered_disable_response(server->mid, server->obj_update_id,
                                          HG_TRUE);
        server->odsc_internal_id =
            MARGO_REGISTER(server->mid, "odsc_internal_rpc", odsc_gdim_t,
                           odsc_list_t, odsc_internal_rpc);
        margo_register_data(server->mid, server->odsc_internal_id,
                            (void *)server, NULL);
        server->ss_id =
            MARGO_REGISTER(server->mid, "ss_rpc", void, ss_information, ss_rpc);
        margo_register_data(server->mid, server->ss_id, (void *)server, NULL);
        server->drain_id = MARGO_REGISTER(server->mid, "drain_rpc", bulk_in_t,
                                          bulk_out_t, NULL);
        server->kill_id =
            MARGO_REGISTER(server->mid, "kill_rpc", int32_t, void, kill_rpc);
        margo_registered_disable_response(server->mid, server->kill_id,
                                          HG_TRUE);
        margo_register_data(server->mid, server->kill_id, (void *)server, NULL);
        server->kill_client_id =
            MARGO_REGISTER(server->mid, "kill_client_rpc", int32_t, void, NULL);
        margo_registered_disable_response(server->mid, server->kill_client_id,
                                          HG_TRUE);

        server->sub_id =
            MARGO_REGISTER(server->mid, "sub_rpc", odsc_gdim_t, void, sub_rpc);
        margo_register_data(server->mid, server->sub_id, (void *)server, NULL);
        margo_registered_disable_response(server->mid, server->sub_id, HG_TRUE);
        server->notify_id =
            MARGO_REGISTER(server->mid, "notify_rpc", odsc_list_t, void, NULL);
        margo_registered_disable_response(server->mid, server->notify_id,
                                          HG_TRUE);
    }
    int size_sp = 1;
    int err = dsg_alloc(server, "dataspaces.conf", comm);
    if(err) {
        fprintf(stderr,
                "DATASPACES: ERROR: %s: could not allocate internal "
                "structures. (%d)\n",
                __func__, err);
        return (dspaces_ERR_ALLOCATION);
    }

    server->f_kill = server->dsg->num_apps;

    if(server->f_drain) {
        // thread to drain the data
        ABT_xstream_create(ABT_SCHED_NULL, &server->drain_xstream);
        ABT_xstream_get_main_pools(server->drain_xstream, 1,
                                   &server->drain_pool);
        ABT_thread_create(server->drain_pool, drain_thread, server,
                          ABT_THREAD_ATTR_NULL, &server->drain_t);
    }

    *sv = server;

    is_initialized = 1;

    return dspaces_SUCCESS;
}

static void kill_client(dspaces_provider_t server, char *client_addr)
{
    hg_addr_t server_addr;
    hg_handle_t h;
    int arg = -1;

    margo_addr_lookup(server->mid, client_addr, &server_addr);
    margo_create(server->mid, server_addr, server->kill_client_id, &h);
    margo_forward(h, &arg);
    margo_addr_free(server->mid, server_addr);
    margo_destroy(h);
}

/*
 * Clients with local data need to know when it's safe to finalize. Send kill
 * rpc to any clients in the drain list.
 */
static void kill_local_clients(dspaces_provider_t server)
{
    struct obj_desc_list *odscl;
    struct list_head client_list;
    struct addr_list_entry *client_addr, *temp;
    int found;

    INIT_LIST_HEAD(&client_list);

    DEBUG_OUT("Killing clients with local storage.\n");

    ABT_mutex_lock(server->odsc_mutex);
    list_for_each_entry(odscl, &(server->dsg->obj_desc_drain_list),
                        struct obj_desc_list, odsc_entry)
    {
        found = 0;
        list_for_each_entry(client_addr, &client_list, struct addr_list_entry,
                            entry)
        {
            if(strcmp(client_addr->addr, odscl->odsc.owner) == 0) {
                found = 1;
                break;
            }
        }
        if(!found) {
            DEBUG_OUT("Adding %s to kill list.\n", odscl->odsc.owner);
            client_addr = malloc(sizeof(*client_addr));
            client_addr->addr = strdup(odscl->odsc.owner);
            list_add(&client_addr->entry, &client_list);
        }
    }
    ABT_mutex_unlock(server->odsc_mutex);

    list_for_each_entry_safe(client_addr, temp, &client_list,
                             struct addr_list_entry, entry)
    {
        DEBUG_OUT("Sending kill signal to %s.\n", client_addr->addr);
        kill_client(server, client_addr->addr);
        list_del(&client_addr->entry);
        free(client_addr->addr);
        free(client_addr);
    }
}

static int server_destroy(dspaces_provider_t server)
{
    int i;

    MPI_Barrier(server->comm);
    DEBUG_OUT("Finishing up, waiting for asynchronous jobs to finish...\n");

    if(server->f_drain) {
        ABT_thread_free(&server->drain_t);
        ABT_xstream_join(server->drain_xstream);
        ABT_xstream_free(&server->drain_xstream);
        DEBUG_OUT("drain thread stopped.\n");
    }

    kill_local_clients(server);

    // Hack to avoid possible argobots race condition. Need to track this down
    // at some point.
    sleep(1);

    free_sspace(server->dsg);
    ls_free(server->dsg->ls);
    free(server->dsg);
    free(server->server_address[0]);
    free(server->server_address);
    free(server->listen_addr_str);

    MPI_Barrier(server->comm);
    MPI_Comm_free(&server->comm);
    DEBUG_OUT("finalizing server.\n");
    margo_finalize(server->mid);
}

static void put_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    bulk_gdim_t in;
    bulk_out_t out;
    hg_bulk_t bulk_handle;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info *info = margo_get_info(handle);
    dspaces_provider_t server =
        (dspaces_provider_t)margo_registered_data(mid, info->id);

    if(server->f_kill == 0) {
        fprintf(stderr, "WARNING: put rpc received when server is finalizing. "
                        "This will likely cause problems...\n");
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,
                "DATASPACES: ERROR handling %s: margo_get_input() failed with "
                "%d.\n",
                __func__, hret);
        margo_destroy(handle);
        return;
    }

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc.raw_odsc, sizeof(in_odsc));
    // set the owner to be this server address
    hg_addr_t owner_addr;
    size_t owner_addr_size = 128;

    margo_addr_self(server->mid, &owner_addr);
    margo_addr_to_string(server->mid, in_odsc.owner, &owner_addr_size,
                         owner_addr);
    margo_addr_free(server->mid, owner_addr);

    struct obj_data *od;
    od = obj_data_alloc(&in_odsc);
    memcpy(&od->gdim, in.odsc.raw_gdim, sizeof(struct global_dimension));

    if(!od)
        fprintf(stderr, "ERROR: (%s): object allocation failed!\n", __func__);

    // do write lock

    hg_size_t size = (in_odsc.size) * bbox_volume(&(in_odsc.bb));

    void *buffer = (void *)od->data;
    hret = margo_bulk_create(mid, 1, (void **)&(od->data), &size,
                             HG_BULK_WRITE_ONLY, &bulk_handle);

    if(hret != HG_SUCCESS) {
        fprintf(stderr, "ERROR: (%s): margo_bulk_create failed!\n", __func__);
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr, in.handle, 0,
                               bulk_handle, 0, size);
    if(hret != HG_SUCCESS) {
        fprintf(stderr, "ERROR: (%s): margo_bulk_transfer failed!\n", __func__);
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_bulk_free(bulk_handle);
        margo_destroy(handle);
        return;
    }

    ABT_mutex_lock(server->ls_mutex);
    ls_add_obj(server->dsg->ls, od);
    ABT_mutex_unlock(server->ls_mutex);

    DEBUG_OUT("Received obj %s\n", obj_desc_sprint(&od->obj_desc));

    // now update the dht
    out.ret = dspaces_SUCCESS;
    margo_bulk_free(bulk_handle);
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);

    obj_update_dht(server, od, DS_OBJ_NEW);
    DEBUG_OUT("Finished obj_put_update from put_rpc\n");
}
DEFINE_MARGO_RPC_HANDLER(put_rpc)

static void put_local_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    odsc_gdim_t in;
    bulk_out_t out;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info *info = margo_get_info(handle);
    dspaces_provider_t server =
        (dspaces_provider_t)margo_registered_data(mid, info->id);

    DEBUG_OUT("In the local put rpc\n");

    if(server->f_kill == 0) {
        fprintf(stderr,
                "WARNING: (%s): got put rpc with local storage, but server is "
                "shutting down. This will likely cause problems...\n",
                __func__);
    }

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,
                "DATASPACES: ERROR handling %s: margo_get_input() failed with "
                "%d.\n",
                __func__, hret);
        margo_destroy(handle);
        return;
    }

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));

    struct obj_data *od;
    od = obj_data_alloc_no_data(&in_odsc, NULL);
    memcpy(&od->gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));

    if(!od)
        fprintf(stderr, "ERROR: (%s): failed to allocate object data!\n",
                __func__);

    DEBUG_OUT("Received obj %s  in put_local_rpc\n",
              obj_desc_sprint(&od->obj_desc));

    // now update the dht
    obj_update_dht(server, od, DS_OBJ_NEW);
    DEBUG_OUT("Finished obj_put_local_update in local_put\n");

    // add to the local list for marking as to be drained data
    struct obj_desc_list *odscl;
    odscl = malloc(sizeof(*odscl));
    memcpy(&odscl->odsc, &od->obj_desc, sizeof(obj_descriptor));

    ABT_mutex_lock(server->odsc_mutex);
    DEBUG_OUT("Adding drain list entry.\n");
    list_add_tail(&odscl->odsc_entry, &server->dsg->obj_desc_drain_list);
    ABT_mutex_unlock(server->odsc_mutex);

    // TODO: wake up thread to initiate draining
    out.ret = dspaces_SUCCESS;
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);

    free(od);
}
DEFINE_MARGO_RPC_HANDLER(put_local_rpc)

static void put_meta_rpc(hg_handle_t handle)
{
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    const struct hg_info *info = margo_get_info(handle);
    dspaces_provider_t server =
        (dspaces_provider_t)margo_registered_data(mid, info->id);
    hg_return_t hret;
    put_meta_in_t in;
    bulk_out_t out;
    struct meta_data *mdata;
    hg_size_t rdma_size;
    hg_bulk_t bulk_handle;

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,
                "DATASPACES: ERROR handling %s: margo_get_input() failed with "
                "%d.\n",
                __func__, hret);
        margo_destroy(handle);
        return;
    }

    DEBUG_OUT("Received meta data of length %d, name '%s' version %d.\n",
              in.length, in.name, in.version);

    mdata = malloc(sizeof(*mdata));
    mdata->name = strdup(in.name);
    mdata->version = in.version;
    mdata->length = in.length;
    mdata->data = (in.length > 0) ? malloc(in.length) : NULL;

    rdma_size = mdata->length;
    if(rdma_size > 0) {
        hret = margo_bulk_create(mid, 1, (void **)&mdata->data, &rdma_size,
                                 HG_BULK_WRITE_ONLY, &bulk_handle);

        if(hret != HG_SUCCESS) {
            fprintf(stderr, "ERROR: (%s): margo_bulk_create failed!\n",
                    __func__);
            out.ret = dspaces_ERR_MERCURY;
            margo_respond(handle, &out);
            margo_free_input(handle, &in);
            margo_bulk_free(bulk_handle);
            margo_destroy(handle);
            return;
        }

        hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr, in.handle, 0,
                                   bulk_handle, 0, rdma_size);
        if(hret != HG_SUCCESS) {
            fprintf(stderr, "ERROR: (%s): margo_bulk_transfer failed!\n",
                    __func__);
            out.ret = dspaces_ERR_MERCURY;
            margo_respond(handle, &out);
            margo_free_input(handle, &in);
            margo_bulk_free(bulk_handle);
            margo_destroy(handle);
            return;
        }
    }

    DEBUG_OUT("adding to metaddata store.\n");

    ABT_mutex_lock(server->ls_mutex);
    ls_add_meta(server->dsg->ls, mdata);
    ABT_mutex_unlock(server->ls_mutex);

    DEBUG_OUT("successfully stored.\n");

    out.ret = dspaces_SUCCESS;
    if(rdma_size > 0) {
        margo_bulk_free(bulk_handle);
    }
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(put_meta_rpc)

static int get_query_odscs(dspaces_provider_t server, odsc_gdim_t *query,
                           int timeout, obj_descriptor **results)
{
    struct sspace *ssd;
    struct dht_entry **de_tab;
    int peer_num;
    int self_id_num = -1;
    int total_odscs = 0;
    int *odsc_nums;
    obj_descriptor **odsc_tabs, **podsc;
    obj_descriptor *odsc_curr;
    margo_request *serv_reqs;
    hg_handle_t *hndls;
    hg_addr_t server_addr;
    odsc_list_t dht_resp;
    obj_descriptor *q_odsc;
    struct global_dimension *q_gdim;
    int dup;
    int i, j, k;

    q_odsc = (obj_descriptor *)query->odsc_gdim.raw_odsc;
    q_gdim = (struct global_dimension *)query->odsc_gdim.raw_gdim;

    DEBUG_OUT("getting sspace lock.\n");
    ABT_mutex_lock(server->sspace_mutex);
    DEBUG_OUT("got lock, looking up shared space for global dimensions.\n");
    ssd = lookup_sspace(server, q_odsc->name, q_gdim);
    ABT_mutex_unlock(server->sspace_mutex);
    DEBUG_OUT("found shared space with %i entries.\n", ssd->dht->num_entries);

    de_tab = malloc(sizeof(*de_tab) * ssd->dht->num_entries);
    peer_num = ssd_hash(ssd, &(q_odsc->bb), de_tab);

    DEBUG_OUT("%d peers to query\n", peer_num);

    odsc_tabs = malloc(sizeof(*odsc_tabs) * peer_num);
    odsc_nums = calloc(sizeof(*odsc_nums), peer_num);
    serv_reqs = malloc(sizeof(*serv_reqs) * peer_num);
    hndls = malloc(sizeof(*hndls) * peer_num);

    for(i = 0; i < peer_num; i++) {
        DEBUG_OUT("dht server id %d\n", de_tab[i]->rank);
        DEBUG_OUT("self id %d\n", server->dsg->rank);

        if(de_tab[i]->rank == server->dsg->rank) {
            self_id_num = i;
            continue;
        }
        // remote servers
        margo_addr_lookup(server->mid, server->server_address[de_tab[i]->rank],
                          &server_addr);
        margo_create(server->mid, server_addr, server->odsc_internal_id,
                     &hndls[i]);
        margo_iforward(hndls[i], query, &serv_reqs[i]);
        margo_addr_free(server->mid, server_addr);
    }

    if(self_id_num > -1) {
        podsc = malloc(sizeof(*podsc) * ssd->ent_self->odsc_num);
        DEBUG_OUT("finding local entries.\n");
        odsc_nums[self_id_num] =
            dht_find_entry_all(ssd->ent_self, q_odsc, &podsc, timeout);
        DEBUG_OUT("%d odscs found in %d\n", odsc_nums[self_id_num],
                  server->dsg->rank);
        total_odscs += odsc_nums[self_id_num];
        if(odsc_nums[self_id_num]) {
            odsc_tabs[self_id_num] =
                malloc(sizeof(**odsc_tabs) * odsc_nums[self_id_num]);
            for(i = 0; i < odsc_nums[self_id_num]; i++) {
                obj_descriptor *odsc =
                    &odsc_tabs[self_id_num][i]; // readability
                *odsc = *podsc[i];
                odsc->st = q_odsc->st;
                bbox_intersect(&q_odsc->bb, &odsc->bb, &odsc->bb);
                DEBUG_OUT("%s\n", obj_desc_sprint(&odsc_tabs[self_id_num][i]));
            }
        }

        free(podsc);
    }

    for(i = 0; i < peer_num; i++) {
        if(i == self_id_num) {
            continue;
        }
        DEBUG_OUT("waiting for %d\n", i);
        margo_wait(serv_reqs[i]);
        margo_get_output(hndls[i], &dht_resp);
        if(dht_resp.odsc_list.size != 0) {
            odsc_nums[i] = dht_resp.odsc_list.size / sizeof(obj_descriptor);
            DEBUG_OUT("received %d odscs from peer %d\n", odsc_nums[i], i);
            total_odscs += odsc_nums[i];
            odsc_tabs[i] = malloc(sizeof(**odsc_tabs) * odsc_nums[i]);
            memcpy(odsc_tabs[i], dht_resp.odsc_list.raw_odsc,
                   dht_resp.odsc_list.size);

            for(j = 0; j < odsc_nums[i]; j++) {
                // readability
                obj_descriptor *odsc =
                    (obj_descriptor *)dht_resp.odsc_list.raw_odsc;
                DEBUG_OUT("remote buffer: %s\n", obj_desc_sprint(&odsc[j]));
            }
        }
        margo_free_output(hndls[i], &dht_resp);
        margo_destroy(hndls[i]);
    }

    odsc_curr = *results = malloc(sizeof(**results) * total_odscs);

    for(i = 0; i < peer_num; i++) {
        if(odsc_nums[i] == 0) {
            continue;
        }
        // dedup
        for(j = 0; j < odsc_nums[i]; j++) {
            dup = 0;
            for(k = 0; k < (odsc_curr - *results); k++) {
                if(obj_desc_equals_no_owner(&(*results)[k], &odsc_tabs[i][j])) {
                    dup = 1;
                    total_odscs--;
                    break;
                }
            }
            if(!dup) {
                *odsc_curr = odsc_tabs[i][j];
                odsc_curr++;
            }
        }
        free(odsc_tabs[i]);
    }

    for(i = 0; i < total_odscs; i++) {
        DEBUG_OUT("odscs in response: %s\n", obj_desc_sprint(&(*results)[i]));
    }

    free(de_tab);
    free(hndls);
    free(serv_reqs);
    free(odsc_tabs);
    free(odsc_nums);

    return (sizeof(obj_descriptor) * total_odscs);
}

static void query_rpc(hg_handle_t handle)
{
    margo_instance_id mid;
    const struct hg_info *info;
    dspaces_provider_t server;
    odsc_gdim_t in;
    odsc_list_t out;
    obj_descriptor in_odsc;
    struct global_dimension in_gdim;
    int timeout;
    obj_descriptor *results;
    hg_return_t hret;

    // unwrap context and input from margo
    mid = margo_hg_handle_get_instance(handle);
    info = margo_get_info(handle);
    server = (dspaces_provider_t)margo_registered_data(mid, info->id);
    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,
                "DATASPACES: ERROR handling %s: margo_get_input() failed with "
                "%d.\n",
                __func__, hret);
        margo_destroy(handle);
        return;
    }

    DEBUG_OUT("received query\n");

    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));
    memcpy(&in_gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));
    timeout = in.param;
    DEBUG_OUT("Received query for %s with timeout %d",
              obj_desc_sprint(&in_odsc), timeout);

    out.odsc_list.size = get_query_odscs(server, &in, timeout, &results);

    out.odsc_list.raw_odsc = (char *)results;
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);

    free(results);
}
DEFINE_MARGO_RPC_HANDLER(query_rpc)

static void query_meta_rpc(hg_handle_t handle)
{
    margo_instance_id mid;
    const struct hg_info *info;
    dspaces_provider_t server;
    query_meta_in_t in;
    query_meta_out_t out;
    struct meta_data *mdata, *mdlatest;
    hg_return_t hret;

    mid = margo_hg_handle_get_instance(handle);
    info = margo_get_info(handle);
    server = (dspaces_provider_t)margo_registered_data(mid, info->id);
    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,
                "DATASPACES: ERROR handling %s: margo_get_input() failed with "
                "%d.\n",
                __func__, hret);
        margo_destroy(handle);
        return;
    }

    DEBUG_OUT("received metadata query for version %d of '%s', mode %d.\n",
              in.version, in.name, in.mode);

    switch(in.mode) {
    case META_MODE_SPEC:
        DEBUG_OUT("spec query - searching without waiting...\n");
        mdata = meta_find_entry(server->dsg->ls, in.name, in.version, 0);
        break;
    case META_MODE_NEXT:
        DEBUG_OUT("find next query...\n");
        mdata = meta_find_next_entry(server->dsg->ls, in.name, in.version, 1);
        break;
    case META_MODE_LAST:
        DEBUG_OUT("find last query...\n");
        mdata = meta_find_next_entry(server->dsg->ls, in.name, in.version, 1);
        mdlatest = mdata;
        do {
            mdata = mdlatest;
            DEBUG_OUT("found version %d. Checking for newer...\n",
                      mdata->version);
            mdlatest = meta_find_next_entry(server->dsg->ls, in.name,
                                            mdlatest->version, 0);
        } while(mdlatest);
        break;
    default:
        fprintf(stderr,
                "ERROR: unkown mode %d while processing metadata query.\n",
                in.mode);
    }

    if(mdata) {
        DEBUG_OUT("found version %d, length %d.", mdata->version,
                  mdata->length);
        out.size = mdata->length;
        hret = margo_bulk_create(mid, 1, (void **)&mdata->data, &out.size,
                                 HG_BULK_READ_ONLY, &out.handle);
        out.version = mdata->version;
    } else {
        out.size = 0;
        out.version = -1;
    }

    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_bulk_free(out.handle);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(query_meta_rpc)

static void get_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    bulk_in_t in;
    bulk_out_t out;
    hg_bulk_t bulk_handle;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info *info = margo_get_info(handle);
    dspaces_provider_t server =
        (dspaces_provider_t)margo_registered_data(mid, info->id);

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,
                "DATASPACES: ERROR handling %s: margo_get_input() failed with "
                "%d.\n",
                __func__, hret);
        margo_destroy(handle);
        return;
    }

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc.raw_odsc, sizeof(in_odsc));

    DEBUG_OUT("received get request\n");

    struct obj_data *od, *from_obj;

    from_obj = ls_find(server->dsg->ls, &in_odsc);

    od = obj_data_alloc(&in_odsc);
    ssd_copy(od, from_obj);

    hg_size_t size = (in_odsc.size) * bbox_volume(&(in_odsc.bb));
    void *buffer = (void *)od->data;
    hret = margo_bulk_create(mid, 1, (void **)&buffer, &size, HG_BULK_READ_ONLY,
                             &bulk_handle);

    if(hret != HG_SUCCESS) {
        fprintf(stderr, "ERROR: (%s): margo_bulk_create() failure\n", __func__);
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, info->addr, in.handle, 0,
                               bulk_handle, 0, size);
    if(hret != HG_SUCCESS) {
        fprintf(stderr, "ERROR: (%s): margo_bulk_transfer() failure (%d)\n",
                __func__, hret);
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_bulk_free(bulk_handle);
        margo_destroy(handle);
        return;
    }
    margo_bulk_free(bulk_handle);
    out.ret = dspaces_SUCCESS;
    out.ret = dspaces_SUCCESS;
    obj_data_free(od);
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(get_rpc)

static void odsc_internal_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    odsc_gdim_t in;
    int timeout;
    odsc_list_t out;
    obj_descriptor **podsc;
    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info *info = margo_get_info(handle);
    dspaces_provider_t server =
        (dspaces_provider_t)margo_registered_data(mid, info->id);

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,
                "DATASPACES: ERROR handling %s: margo_get_input() failed with "
                "%d.\n",
                __func__, hret);
        margo_destroy(handle);
        return;
    }

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));
    timeout = in.param;

    struct global_dimension od_gdim;
    memcpy(&od_gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));

    DEBUG_OUT("Received query for %s with timeout %d\n",
              obj_desc_sprint(&in_odsc), timeout);

    obj_descriptor odsc, *odsc_tab;
    ABT_mutex_lock(server->sspace_mutex);
    struct sspace *ssd = lookup_sspace(server, in_odsc.name, &od_gdim);
    ABT_mutex_unlock(server->sspace_mutex);
    podsc = malloc(sizeof(*podsc) * ssd->ent_self->odsc_num);
    int num_odsc;
    num_odsc = dht_find_entry_all(ssd->ent_self, &in_odsc, &podsc, timeout);
    DEBUG_OUT("found %d DHT entries.\n", num_odsc);
    if(!num_odsc) {
        // need to figure out how to send that number of odscs is null
        out.odsc_list.size = 0;
        out.odsc_list.raw_odsc = NULL;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);

    } else {
        odsc_tab = malloc(sizeof(*odsc_tab) * num_odsc);
        for(int j = 0; j < num_odsc; j++) {
            obj_descriptor odsc;
            odsc = *podsc[j];
            DEBUG_OUT("including %s\n", obj_desc_sprint(&odsc));
            /* Preserve storage type at the destination. */
            odsc.st = in_odsc.st;
            bbox_intersect(&in_odsc.bb, &odsc.bb, &odsc.bb);
            odsc_tab[j] = odsc;
        }
        out.odsc_list.size = num_odsc * sizeof(obj_descriptor);
        out.odsc_list.raw_odsc = (char *)odsc_tab;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
    }
    for(int i = 0; i < num_odsc; ++i) {
        DEBUG_OUT("send odsc: %s\n", obj_desc_sprint(&odsc_tab[i]));
    }

    free(podsc);
}
DEFINE_MARGO_RPC_HANDLER(odsc_internal_rpc)

/*
  Rpc routine to update (add or insert) an object descriptor in the
  dht table.
*/
static void obj_update_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    odsc_gdim_t in;
    obj_update_t type;
    int err;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info *info = margo_get_info(handle);
    dspaces_provider_t server =
        (dspaces_provider_t)margo_registered_data(mid, info->id);

    DEBUG_OUT("Received rpc to update obj_dht\n");

    hret = margo_get_input(handle, &in);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,
                "DATASPACES: ERROR handling %s: margo_get_input() failed with "
                "%d.\n",
                __func__, hret);
        margo_destroy(handle);
        return;
    }

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));
    struct global_dimension gdim;
    memcpy(&gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));
    type = in.param;

    DEBUG_OUT("received update_rpc %s\n", obj_desc_sprint(&in_odsc));
    ABT_mutex_lock(server->sspace_mutex);
    struct sspace *ssd = lookup_sspace(server, in_odsc.name, &gdim);
    ABT_mutex_unlock(server->sspace_mutex);
    struct dht_entry *de = ssd->ent_self;

    ABT_mutex_lock(server->dht_mutex);
    switch(type) {
    case DS_OBJ_NEW:
        err = dht_add_entry(de, &in_odsc);
        break;
    case DS_OBJ_OWNER:
        err = dht_update_owner(de, &in_odsc, 1);
        break;
    default:
        fprintf(stderr, "ERROR: (%s): unknown object update type.\n", __func__);
    }
    ABT_mutex_unlock(server->dht_mutex);
    DEBUG_OUT("Updated dht %s in server %d \n", obj_desc_sprint(&in_odsc),
              server->dsg->rank);
    if(err < 0)
        fprintf(stderr, "ERROR (%s): obj_update_rpc Failed with %d\n", __func__,
                err);

    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(obj_update_rpc)

static void ss_rpc(hg_handle_t handle)
{
    ss_information out;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info *info = margo_get_info(handle);
    dspaces_provider_t server =
        (dspaces_provider_t)margo_registered_data(mid, info->id);

    ss_info_hdr ss_data;
    ss_data.num_dims = ds_conf.ndim;
    ss_data.num_space_srv = server->dsg->size_sp;
    ss_data.max_versions = ds_conf.max_versions;
    ss_data.hash_version = ds_conf.hash_version;
    ss_data.default_gdim.ndim = ds_conf.ndim;

    for(int i = 0; i < ds_conf.ndim; i++) {
        ss_data.ss_domain.lb.c[i] = 0;
        ss_data.ss_domain.ub.c[i] = ds_conf.dims.c[i] - 1;
        ss_data.default_gdim.sizes.c[i] = ds_conf.dims.c[i];
    }

    out.ss_buf.size = sizeof(ss_info_hdr);
    out.ss_buf.raw_odsc = (char *)(&ss_data);
    margo_respond(handle, &out);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(ss_rpc)

static void send_kill_rpc(dspaces_provider_t server, int target, int *rank)
{
    // TODO: error handling/reporting
    hg_addr_t server_addr;
    hg_handle_t h;

    margo_addr_lookup(server->mid, server->server_address[target],
                      &server_addr);
    margo_create(server->mid, server_addr, server->kill_id, &h);
    margo_forward(h, rank);
    margo_addr_free(server->mid, server_addr);
    margo_destroy(h);
}

static void kill_rpc(hg_handle_t handle)
{
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    const struct hg_info *info = margo_get_info(handle);
    dspaces_provider_t server =
        (dspaces_provider_t)margo_registered_data(mid, info->id);
    int32_t src, rank, parent, child1, child2;
    int do_kill = 0;
    hg_return_t hret;

    hret = margo_get_input(handle, &src);
    DEBUG_OUT("Received kill signal from %d.\n", src);

    rank = server->dsg->rank;
    parent = (rank - 1) / 2;
    child1 = (rank * 2) + 1;
    child2 = child1 + 1;

    ABT_mutex_lock(server->kill_mutex);
    DEBUG_OUT("Kill tokens remaining: %d\n",
              server->f_kill ? (server->f_kill - 1) : 0);
    if(server->f_kill == 0) {
        // already shutting down
        ABT_mutex_unlock(server->kill_mutex);
        margo_free_input(handle, &src);
        margo_destroy(handle);
        return;
    }
    if(--server->f_kill == 0) {
        DEBUG_OUT("Kill count is zero. Initiating shutdown.\n");
        do_kill = 1;
    }

    ABT_mutex_unlock(server->kill_mutex);

    if((src == -1 || src > rank) && rank > 0) {
        send_kill_rpc(server, parent, &rank);
    }
    if((child1 != src && child1 < server->dsg->size_sp)) {
        send_kill_rpc(server, child1, &rank);
    }
    if((child2 != src && child2 < server->dsg->size_sp)) {
        send_kill_rpc(server, child2, &rank);
    }

    margo_free_input(handle, &src);
    margo_destroy(handle);
    if(do_kill) {
        server_destroy(server);
    }
}
DEFINE_MARGO_RPC_HANDLER(kill_rpc)

static void sub_rpc(hg_handle_t handle)
{
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    const struct hg_info *info = margo_get_info(handle);
    dspaces_provider_t server =
        (dspaces_provider_t)margo_registered_data(mid, info->id);
    odsc_list_t notice;
    odsc_gdim_t in;
    int32_t sub_id;
    hg_return_t hret;
    obj_descriptor in_odsc;
    obj_descriptor *results;
    struct global_dimension in_gdim;
    hg_addr_t client_addr;
    hg_handle_t notifyh;

    hret = margo_get_input(handle, &in);

    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));
    memcpy(&in_gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));
    sub_id = in.param;

    DEBUG_OUT("received subscription for %s with id %d from %s\n",
              obj_desc_sprint(&in_odsc), sub_id, in_odsc.owner);

    in.param = -1; // this will be interpreted as timeout by any interal queries
    notice.odsc_list.size = get_query_odscs(server, &in, -1, &results);
    notice.odsc_list.raw_odsc = (char *)results;
    notice.param = sub_id;

    margo_addr_lookup(server->mid, in_odsc.owner, &client_addr);
    margo_create(server->mid, client_addr, server->notify_id, &notifyh);
    margo_forward(notifyh, &notice);
    margo_addr_free(server->mid, client_addr);
    margo_destroy(notifyh);

    margo_free_input(handle, &in);
    margo_destroy(handle);

    free(results);
}
DEFINE_MARGO_RPC_HANDLER(sub_rpc)

void dspaces_server_fini(dspaces_provider_t server)
{
    margo_wait_for_finalize(server->mid);
    free(server);
}
