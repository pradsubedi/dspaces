/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers University
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <abt.h>
#include "ss_data.h"
#include "dspaces-server.h"
#include "gspace.h"

#define DEBUG_OUT(args...) \
    do { \
        if(server->f_debug) { \
           fprintf(stderr, "Rank %i: %s, line %i (%s): ", server->rank, __FILE__, __LINE__, __func__); \
           fprintf(stderr, args); \
        } \
    }while(0);

#define DSPACES_DEFAULT_NUM_HANDLERS 4

static enum storage_type st = column_major;

typedef enum obj_update_type {
    DS_OBJ_NEW,
    DS_OBJ_OWNER
} obj_update_t;

int cond_num = 0;

struct dspaces_provider{
    margo_instance_id mid;
    hg_id_t put_id;
    hg_id_t put_local_id;
    hg_id_t query_id;
    hg_id_t get_id;
    hg_id_t obj_update_id;   
    hg_id_t odsc_internal_id;
    hg_id_t ss_id;
    hg_id_t drain_id;
    hg_id_t kill_id;
    struct ds_gspace *dsg; 
    char **server_address;  
    int rank;
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
DECLARE_MARGO_RPC_HANDLER(get_rpc);
DECLARE_MARGO_RPC_HANDLER(query_rpc);
DECLARE_MARGO_RPC_HANDLER(obj_update_rpc);
DECLARE_MARGO_RPC_HANDLER(odsc_internal_rpc);
DECLARE_MARGO_RPC_HANDLER(ss_rpc);
DECLARE_MARGO_RPC_HANDLER(kill_rpc);

static void put_rpc(hg_handle_t h);
static void put_local_rpc(hg_handle_t h);
static void get_rpc(hg_handle_t h);
static void query_rpc(hg_handle_t h);
static void obj_update_rpc(hg_handle_t h);
static void odsc_internal_rpc(hg_handle_t h);
static void ss_rpc(hg_handle_t h);
static void kill_rpc(hg_handle_t h);
//static void write_lock_rpc(hg_handle_t h);
//static void read_lock_rpc(hg_handle_t h);



/* Server configuration parameters */
static struct {
        int ndim;
        struct coord dims;
        int max_versions;
        int max_readers;
        int lock_type;      /* 1 - generic, 2 - custom */
        int hash_version;   /* 1 - ssd_hash_version_v1, 2 - ssd_hash_version_v2 */
} ds_conf;

static struct {
        const char      *opt;
        int             *pval;
} options[] = {
        {"ndim",                &ds_conf.ndim},
        {"dims",                (int *)&ds_conf.dims},
        {"max_versions",        &ds_conf.max_versions},
        {"max_readers",         &ds_conf.max_readers},
        {"lock_type",           &ds_conf.lock_type},
        {"hash_version",        &ds_conf.hash_version}, 
};

static void eat_spaces(char *line)
{
        char *t = line;

        while (t && *t) {
                if (*t != ' ' && *t != '\t' && *t != '\n')
                        *line++ = *t;
                t++;
        }
        if (line)
                *line = '\0';
}

static int parse_line(int lineno, char *line)
{
        char *t;
        int i, n;

        /* Comment line ? */
        if (line[0] == '#')
                return 0;

        t = strstr(line, "=");
        if (!t) {
                eat_spaces(line);
                if (strlen(line) == 0)
                        return 0;
                else    return -EINVAL;
        }

        t[0] = '\0';
        eat_spaces(line);
        t++;

        n = sizeof(options) / sizeof(options[0]);

        for (i = 0; i < n; i++) {
                if (strcmp(line, options[1].opt) == 0){ /**< when "dims" */
                    //get coordinates
                    int idx = 0;
                    char* crd;
                    crd = strtok(t, ",");
                    while(crd != NULL){
                        ((struct coord*)options[1].pval)->c[idx] = atoll(crd);
                        crd = strtok(NULL, ",");
                        idx++;
                    }
                    if(idx != *(int*)options[0].pval){
                        fprintf(stderr, "ERROR: (%s): dimensionality mismatch.\n", __func__);
                        fprintf(stderr, "ERROR: index=%d, ndims=%d\n",idx, *(int*)options[0].pval);
                        return -EINVAL;
                    }
                    break;
                }
                if (strcmp(line, options[i].opt) == 0) {
                        eat_spaces(line);
                        *(int*)options[i].pval = atoi(t);
                        break;
                }
        }

        if (i == n) {
                fprintf(stderr, "WARNING: (%s): unknown option '%s' at line %d.\n", __func__, line, lineno);
        }
        return 0;
}

static int parse_conf(char *fname)
{
        FILE *fin;
        char buff[1024];
        int lineno = 1, err;

        fin = fopen(fname, "rt");
        if (!fin)
                return -errno;

        while (fgets(buff, sizeof(buff), fin) != NULL) {
                err = parse_line(lineno++, buff);
                if (err < 0) {
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
    dsg_l->ssd = ssd_alloc(default_domain, dsg_l->size_sp,
                            ds_conf.max_versions, ds_conf.hash_version);
    if (!dsg_l->ssd)
        goto err_out;

    err = ssd_init(dsg_l->ssd, dsg_l->rank);
    if (err < 0)
        goto err_out;

    dsg_l->default_gdim.ndim = ds_conf.ndim;
    int i;
    for (i = 0; i < ds_conf.ndim; i++) {
        dsg_l->default_gdim.sizes.c[i] = ds_conf.dims.c[i];
    }

    INIT_LIST_HEAD(&dsg_l->sspace_list);
    return 0;
 err_out:
    fprintf(stderr, "%s(): ERROR failed\n", __func__);
    return err;
}

static int write_address(dspaces_provider_t server, MPI_Comm comm){

    hg_addr_t my_addr  = HG_ADDR_NULL;
    hg_return_t hret   = HG_SUCCESS;
    hg_size_t my_addr_size;

    int comm_size, rank, ret = 0;
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &rank);

    char *my_addr_str = NULL;
    int self_addr_str_size = 0;
    char *addr_str_buf = NULL;
    int *sizes = NULL;
    int *sizes_psum = NULL;
    char **addr_strs = NULL;

    hret = margo_addr_self(server->mid, &my_addr);
    if(hret != HG_SUCCESS) {
        fprintf(stderr, "ERROR: margo_addr_self() returned %d\n", hret);
        ret = -1;
        goto error;
    }

    hret = margo_addr_to_string(server->mid, NULL, &my_addr_size, my_addr);
    if(hret != HG_SUCCESS) {
        fprintf(stderr, "ERROR: margo_addr_to_string() returned %d\n", hret);
        ret = -1;
        goto errorfree;
    }

    my_addr_str = malloc(my_addr_size);
    hret = margo_addr_to_string(server->mid, my_addr_str, &my_addr_size, my_addr);
    if(hret != HG_SUCCESS) {
        fprintf(stderr, "ERROR: margo_addr_to_string() returned %d\n", hret);
        ret = -1;
        goto errorfree;
    }
    

    sizes = malloc(comm_size * sizeof(*sizes));
    self_addr_str_size = (int)strlen(my_addr_str) + 1;
    MPI_Allgather(&self_addr_str_size, 1, MPI_INT, sizes, 1, MPI_INT, comm);

    int addr_buf_size = 0;
    for (int i = 0; i < comm_size; ++i)
    {
        addr_buf_size = addr_buf_size + sizes[i];
    }

    sizes_psum = malloc((comm_size) * sizeof(*sizes_psum));
    sizes_psum[0]=0;
    for (int i = 1; i < comm_size; i++)
        sizes_psum[i] = sizes_psum[i-1] + sizes[i-1];

    addr_str_buf = malloc(addr_buf_size);
    MPI_Allgatherv(my_addr_str, self_addr_str_size, MPI_CHAR, addr_str_buf, sizes, sizes_psum, MPI_CHAR, comm);

    server->server_address = (char **)addr_str_buf_to_list(addr_str_buf, comm_size);

    if(rank==0){
        
        for (int i = 1; i < comm_size; ++i)
        {
            addr_str_buf[sizes_psum[i]-1]='\n';
        }
        addr_str_buf[addr_buf_size-1]='\n';

        int fd;
        fd = open("servids.0", O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0)
        {
            fprintf(stderr, "ERROR: unable to write server_ids into file\n");
            ret = -1;
            goto errorfree;

        }
        int bytes_written = 0;
        bytes_written = write(fd, addr_str_buf, addr_buf_size);
        if (bytes_written != addr_buf_size)
        {
            fprintf(stderr, "ERROR: unable to write server_ids into opened file\n");
            ret = -1;
            free(addr_str_buf);
            close(fd);
            goto errorfree;

        }
        close(fd);
    }
    free(my_addr_str);
    free(sizes);
    free(sizes_psum);
    margo_addr_free(server->mid, my_addr);

finish:
    return ret;
errorfree:
    margo_addr_free(server->mid, my_addr);
error:
    margo_finalize(server->mid);
    goto finish;
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

        err = parse_conf(conf_name);
        if (err < 0) {
            fprintf(stderr, "%s(): ERROR failed to load config file '%s'.", __func__, conf_name);
            goto err_out;
        }

        // Check number of dimension
        if (ds_conf.ndim > BBOX_MAX_NDIM) {
            fprintf(stderr, "%s(): ERROR maximum number of array dimension is %d but ndim is %d"
                " in file '%s'\n", __func__, BBOX_MAX_NDIM, ds_conf.ndim, conf_name);
            err = -EINVAL;
            goto err_out;
        }

        // Check hash version
        if ((ds_conf.hash_version < ssd_hash_version_v1) ||
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
        for(i = 0; i < domain.num_dims; i++){
            domain.lb.c[i] = 0;
            domain.ub.c[i] = ds_conf.dims.c[i] - 1;
        }

        dsg_l = malloc(sizeof(*dsg_l));
        if (!dsg_l)
                goto err_out;

        MPI_Comm_size(comm, &(dsg_l->size_sp));

        MPI_Comm_rank(comm, &dsg_l->rank);

        write_address(server, comm);

        err = init_sspace(&domain, dsg_l);
        if (err < 0) {
            goto err_free;
        }
        dsg_l->ls = ls_alloc(ds_conf.max_versions);
        if (!dsg_l->ls) {
            fprintf(stderr, "%s(): ERROR ls_alloc() failed\n", __func__);
            goto err_free;
        }

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


static struct sspace* lookup_sspace(struct ds_gspace *dsg_l, const char* var_name, const struct global_dimension* gd)
{
    struct global_dimension gdim;
    memcpy(&gdim, gd, sizeof(struct global_dimension));

    // Return the default shared space created based on
    // global data domain specified in dataspaces.conf 
    if (global_dimension_equal(&gdim, &dsg_l->default_gdim )) {
        return dsg_l->ssd;
    }


    // Otherwise, search for shared space based on the
    // global data domain specified by application in put()/get().
    struct sspace_list_entry *ssd_entry = NULL;
    list_for_each_entry(ssd_entry, &dsg_l->sspace_list,
        struct sspace_list_entry, entry)
    {
        // compare global dimension
        if (gdim.ndim != ssd_entry->gdim.ndim)
            continue;

        if (global_dimension_equal(&gdim, &ssd_entry->gdim))
            return ssd_entry->ssd;
    }

    // If not found, add new shared space
    int i, err;
    struct bbox domain;
    memset(&domain, 0, sizeof(struct bbox));
    domain.num_dims = gdim.ndim;
    for (i = 0; i < gdim.ndim; i++) {
        domain.lb.c[i] = 0;
        domain.ub.c[i] = gdim.sizes.c[i] - 1;
    } 

    
    ssd_entry = malloc(sizeof(struct sspace_list_entry));
    memcpy(&ssd_entry->gdim, &gdim, sizeof(struct global_dimension));

    ssd_entry->ssd = ssd_alloc(&domain, dsg_l->size_sp, 
                            ds_conf.max_versions, ds_conf.hash_version);     
    if (!ssd_entry->ssd) {
        fprintf(stderr, "%s(): ssd_alloc failed for '%s'\n", __func__, var_name);
        return dsg_l->ssd;
    }

    err = ssd_init(ssd_entry->ssd, dsg_l->rank);
    if (err < 0) {
        fprintf(stderr,"%s(): ssd_init failed\n", __func__); 
        return dsg_l->ssd;
    }

    list_add(&ssd_entry->entry, &dsg_l->sspace_list);
    return ssd_entry->ssd;
}

static int obj_update_dht(dspaces_provider_t server, struct obj_data *od, obj_update_t type)
{
    obj_descriptor *odsc = &od->obj_desc;
    ABT_mutex_lock(server->sspace_mutex);
    struct sspace* ssd = lookup_sspace(server->dsg, odsc->name, &od->gdim);
    ABT_mutex_unlock(server->sspace_mutex);
    struct dht_entry *dht_tab[ssd->dht->num_entries];

    int num_de, i, err;

    /* Compute object distribution to nodes in the space. */
    num_de = ssd_hash(ssd, &odsc->bb, dht_tab);
    if (num_de == 0) {
        fprintf(stderr, "'%s()': this should not happen, num_de == 0 ?!\n",
            __func__);
    }
    /* Update object descriptors on the corresponding nodes. */
    for (i = 0; i < num_de; i++) {
        if (dht_tab[i]->rank == server->dsg->rank) {
            DEBUG_OUT("Add in local_dht %d\n", server->dsg->rank);
            ABT_mutex_lock(server->dht_mutex);
            switch(type) {
            case DS_OBJ_NEW:
                dht_add_entry(ssd->ent_self, odsc);
                break;
            case DS_OBJ_OWNER:
                dht_update_owner(ssd->ent_self, odsc);
                break;
            default:
                fprintf(stderr, "ERROR: (%s): unknown object update type.\n", __func__);
            }
            ABT_mutex_unlock(server->dht_mutex);
            DEBUG_OUT("I am self, added in local dht %d\n", server->dsg->rank);
            continue;
        }

        //now send rpc to the server for dht_update
        hg_return_t hret;
        odsc_gdim_t in;
        DEBUG_OUT("Server %d sending object %s to dht server %d \n", server->dsg->rank, obj_desc_sprint(odsc), dht_tab[i]->rank);

        in.odsc_gdim.size = sizeof(*odsc);
        in.odsc_gdim.gdim_size = sizeof(struct global_dimension);
        in.odsc_gdim.raw_odsc = (char*)(odsc);
        in.odsc_gdim.raw_gdim = (char*)(&od->gdim);
        in.param = type;

        hg_addr_t svr_addr;
        margo_addr_lookup(server->mid, server->server_address[dht_tab[i]->rank], &svr_addr);

        hg_handle_t h;
        margo_create(server->mid, svr_addr, server->obj_update_id, &h);
        margo_forward(h, &in);
        DEBUG_OUT("sent obj server %d to update dht %s in \n", dht_tab[i]->rank, obj_desc_sprint(odsc));

        margo_addr_free(server->mid, svr_addr);
        hret = margo_destroy(h);
        if(hret != HG_SUCCESS) {
            fprintf(stderr, "ERROR: (%s): could not destroy handle!\n", __func__);
            return(dspaces_ERR_MERCURY);    
        }
        return dspaces_SUCCESS;

    }

    return 0;
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
    in.odsc.raw_odsc = (char*)(&odsc);

    hg_addr_t owner_addr;
    size_t owner_addr_size = 128;

    margo_addr_self(server->mid, &owner_addr);
    margo_addr_to_string(server->mid, od->obj_desc.owner, &owner_addr_size, owner_addr);
    margo_addr_free(server->mid, owner_addr);


    hg_size_t rdma_size = (odsc.size)*bbox_volume(&odsc.bb);

    margo_bulk_create(server->mid, 1, (void**)(&(od->data)), &rdma_size,
                            HG_BULK_WRITE_ONLY, &in.handle);
    hg_addr_t client_addr;
    margo_addr_lookup(server->mid, odsc.owner, &client_addr);

    hg_handle_t handle;
    margo_create(server->mid,
        client_addr,
        server->drain_id,
        &handle);
    margo_forward(handle, &in);
    margo_get_output(handle, &out);
    if(out.ret == dspaces_SUCCESS){
        ABT_mutex_lock(server->ls_mutex);
        ls_add_obj(server->dsg->ls, od);
        ABT_mutex_unlock(server->ls_mutex);
    }
    ret = out.ret;
    //now update the dht with new owner information
    DEBUG_OUT("Inside get_client_data\n");
    margo_addr_free(server->mid, client_addr);
    margo_bulk_free(in.handle);
    margo_free_output(handle, &out);
    margo_destroy(handle);
    obj_update_dht(server, od, DS_OBJ_OWNER);
    return ret;

}


//thread to move data between layers
static void drain_thread(void *arg)
{
    dspaces_provider_t server = arg;

    while (!server->f_kill)
    {
        int counter = 0;
        DEBUG_OUT("Thread WOKEUP\n");
        do{
            counter = 0;
            obj_descriptor odsc;
            struct obj_desc_list *odscl;
            //requires better way to get the obj_descriptor
            ABT_mutex_lock(server->odsc_mutex);
            DEBUG_OUT("Inside odsc mutex\n");
            list_for_each_entry(odscl, &(server->dsg->obj_desc_drain_list), struct obj_desc_list, odsc_entry){
                memcpy(&odsc, &(odscl->odsc), sizeof(obj_descriptor));
                DEBUG_OUT("Found %s in odsc_list\n", obj_desc_sprint(&odsc));
                counter = 1;
                break;
                
            }
            ABT_mutex_unlock(server->odsc_mutex);
            if(counter == 1) {
                int ret = get_client_data(odsc, server);
                DEBUG_OUT("Finished draining %s\n", obj_desc_sprint(&odsc));
                if(ret == dspaces_SUCCESS){
                    ABT_mutex_lock(server->odsc_mutex);
                    //delete moved obj_descriptor from the list
                    DEBUG_OUT("Deleting drain entry\n");
                    list_del(&odscl->odsc_entry);
                    ABT_mutex_unlock(server->odsc_mutex);
                }
                sleep(1);

            }
            
        }while(counter == 1); 

        sleep(10);                            
   
        ABT_thread_yield();    
    }
        
}


int dspaces_server_init(char *listen_addr_str, MPI_Comm comm, dspaces_provider_t* sv)
{
    const char *envdebug = getenv("DSPACES_DEBUG");
    const char *envnthreads = getenv("DSPACES_NUM_HANDLERS");
    const char *envdrain = getenv("DSPACES_DRAIN");
    dspaces_provider_t server;
    hg_bool_t flag;
    hg_id_t id;
    int num_handlers = DSPACES_DEFAULT_NUM_HANDLERS;
    int ret; 

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

    server->mid = margo_init(listen_addr_str, MARGO_SERVER_MODE, 1, num_handlers);
    assert(server->mid);

    ret = ABT_mutex_create(&server->odsc_mutex);
    ret = ABT_mutex_create(&server->ls_mutex);
    ret = ABT_mutex_create(&server->dht_mutex);
    ret = ABT_mutex_create(&server->sspace_mutex);
    ret = ABT_mutex_create(&server->kill_mutex);
    
    margo_registered_name(server->mid, "put_rpc", &id, &flag);

    if(flag == HG_TRUE) { /* RPCs already registered */
        margo_registered_name(server->mid, "put_rpc",           &server->put_id,            &flag);
        margo_registered_name(server->mid, "put_local_rpc",     &server->put_local_id,      &flag);
        margo_registered_name(server->mid, "get_rpc",           &server->get_id,            &flag);
        margo_registered_name(server->mid, "query_rpc",         &server->query_id,          &flag);
        margo_registered_name(server->mid, "obj_update_rpc",    &server->obj_update_id,     &flag);
        margo_registered_name(server->mid, "odsc_internal_rpc", &server->odsc_internal_id,  &flag);
        margo_registered_name(server->mid, "ss_rpc",            &server->ss_id,             &flag);
        margo_registered_name(server->mid, "drain_rpc",         &server->drain_id,          &flag);
        margo_registered_name(server->mid, "kill_rpc",          &server->kill_id,           &flag);
    } else {
        server->put_id =
            MARGO_REGISTER(server->mid, "put_rpc", bulk_gdim_t, bulk_out_t, put_rpc);
        margo_register_data(server->mid, server->put_id, (void*)server, NULL);
        server->put_local_id =
            MARGO_REGISTER(server->mid, "put_local_rpc", odsc_gdim_t, bulk_out_t, put_local_rpc);
        margo_register_data(server->mid, server->put_local_id, (void*)server, NULL);
        server->get_id =
            MARGO_REGISTER(server->mid, "get_rpc", bulk_in_t, bulk_out_t, get_rpc);
        margo_register_data(server->mid, server->get_id, (void*)server, NULL);
        server->query_id =
            MARGO_REGISTER(server->mid, "query_rpc", odsc_gdim_t, odsc_list_t, query_rpc);
        margo_register_data(server->mid, server->query_id, (void*)server, NULL);
        server->obj_update_id =
            MARGO_REGISTER(server->mid, "obj_update_rpc", odsc_gdim_t, void, obj_update_rpc);
        margo_register_data(server->mid, server->obj_update_id, (void*)server, NULL);
        margo_registered_disable_response(server->mid, server->obj_update_id, HG_TRUE);
        server->odsc_internal_id =
            MARGO_REGISTER(server->mid, "odsc_internal_rpc", odsc_gdim_t, odsc_list_t, odsc_internal_rpc);
        margo_register_data(server->mid, server->odsc_internal_id, (void*)server, NULL);
        server->ss_id =
            MARGO_REGISTER(server->mid, "ss_rpc", void, ss_information, ss_rpc);
        margo_register_data(server->mid, server->ss_id, (void*)server, NULL);
        server->drain_id =
            MARGO_REGISTER(server->mid, "drain_rpc", bulk_in_t, bulk_out_t, NULL);
        server->kill_id =
            MARGO_REGISTER(server->mid, "kill_rpc", int32_t, void, kill_rpc);
        margo_registered_disable_response(server->mid, server->kill_id, HG_TRUE);
        margo_register_data(server->mid, server->kill_id, (void*)server, NULL);
        
    }
    int size_sp = 1;
    int err = dsg_alloc(server, "dataspaces.conf", comm);
    assert(err == 0);    

    server->f_kill = 0;

    if(server->f_drain) {
        //thread to drain the data
        ABT_xstream_create(ABT_SCHED_NULL, &server->drain_xstream);
        ABT_xstream_get_main_pools(server->drain_xstream, 1, &server->drain_pool);
        ABT_thread_create(server->drain_pool, drain_thread, server, ABT_THREAD_ATTR_NULL, &server->drain_t); 
    }

    *sv = server;

    return dspaces_SUCCESS;
}

static int server_destroy(dspaces_provider_t server)
{
    int i;

    MPI_Barrier(server->comm);
    if(server->rank == 0) {
        fprintf(stderr, "Finishing up, waiting for asynchronous jobs to finish...\n");
    }

    if(server->f_drain) {
        ABT_thread_free(&server->drain_t);
        ABT_xstream_join(server->drain_xstream);
        ABT_xstream_free(&server->drain_xstream);
        DEBUG_OUT("drain thread stopped.\n");
    }

    free_sspace(server->dsg);
    ls_free(server->dsg->ls);
    free(server->dsg);
    free(server->server_address[0]);
    free(server->server_address);

    MPI_Barrier(server->comm);
    MPI_Comm_free(&server->comm);
    if(server->rank == 0) {
        fprintf(stderr, "Finalizing servers.\n");
    }
    margo_finalize(server->mid);
    free(server);
}

static void put_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    bulk_gdim_t in;
    bulk_out_t out;
    hg_bulk_t bulk_handle;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info* info = margo_get_info(handle);
    dspaces_provider_t server = (dspaces_provider_t)margo_registered_data(mid, info->id);
    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc.raw_odsc, sizeof(in_odsc));
    //set the owner to be this server address
    hg_addr_t owner_addr;
    size_t owner_addr_size = 128;

    margo_addr_self(server->mid, &owner_addr);
    margo_addr_to_string(server->mid, in_odsc.owner, &owner_addr_size, owner_addr);
    margo_addr_free(server->mid, owner_addr);

    struct obj_data *od;
    od = obj_data_alloc(&in_odsc);
    memcpy(&od->gdim, in.odsc.raw_gdim, sizeof(struct global_dimension));

    if(!od)
        fprintf(stderr, "ERROR: (%s): object allocation failed!\n", __func__);

    //do write lock

    hg_size_t size = (in_odsc.size)*bbox_volume(&(in_odsc.bb));

    void *buffer = (void*) od->data;
    hret = margo_bulk_create(mid, 1, (void**)&(od->data), &size,
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

    //now update the dht
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

    const struct hg_info* info = margo_get_info(handle);
    dspaces_provider_t server = (dspaces_provider_t)margo_registered_data(mid, info->id);

    DEBUG_OUT("In the local put rpc\n");

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));

    struct obj_data *od;
    od = obj_data_alloc_no_data(&in_odsc, NULL);
    memcpy(&od->gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));

    if(!od)
        fprintf(stderr, "ERROR: (%s): failed to allocate object data!\n", __func__);


    DEBUG_OUT("Received obj %s  in put_local_rpc\n", obj_desc_sprint(&od->obj_desc));

   
    //now update the dht
    obj_update_dht(server, od, DS_OBJ_NEW);
    DEBUG_OUT("Finished obj_put_local_update in local_put\n");

    //add to the local list for marking as to be drained data
    struct obj_desc_list *odscl;
    odscl = malloc(sizeof(*odscl));
    memcpy(&odscl->odsc, &od->obj_desc, sizeof(obj_descriptor));

    ABT_mutex_lock(server->odsc_mutex);
    DEBUG_OUT("Adding drain list entry.\n");
    list_add_tail(&odscl->odsc_entry, &server->dsg->obj_desc_drain_list);
    ABT_mutex_unlock(server->odsc_mutex);

    //TODO: wake up thread to initiate draining
    out.ret = dspaces_SUCCESS;
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);
    
    free(od);
}
DEFINE_MARGO_RPC_HANDLER(put_local_rpc)

static void query_rpc(hg_handle_t handle)
{
    margo_instance_id mid;
    const struct hg_info *info;
    dspaces_provider_t server;
    odsc_gdim_t in;
    odsc_list_t out;
    obj_descriptor in_odsc;
    int timeout;
    struct global_dimension in_gdim;
    struct sspace *ssd;
    struct dht_entry **de_tab;
    int peer_num;
    int self_id_num = -1;
    int total_odscs = 0;
    int *odsc_nums;
    obj_descriptor **odsc_tabs, **podsc;
    obj_descriptor *odsc_curr, *odsc_tab_all;
    margo_request *serv_reqs;
    hg_handle_t *hndls;
    hg_addr_t server_addr;
    odsc_list_t dht_resp;
    int i, j;
    hg_return_t hret;

    // unwrap context and input from margo
    mid = margo_hg_handle_get_instance(handle);
    info = margo_get_info(handle);
    server = (dspaces_provider_t)margo_registered_data(mid, info->id);
    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    DEBUG_OUT("received query\n");

    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));
    memcpy(&in_gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));
    timeout = in.param;
    DEBUG_OUT("Received query for %s\n",  obj_desc_sprint(&in_odsc));

    ABT_mutex_lock(server->sspace_mutex);
    ssd = lookup_sspace(server->dsg, in_odsc.name, &in_gdim);
    ABT_mutex_unlock(server->sspace_mutex);

    de_tab = malloc(sizeof(*de_tab) * ssd->dht->num_entries);
    peer_num = ssd_hash(ssd, &(in_odsc.bb), de_tab);

    DEBUG_OUT("%d peers to query\n", peer_num);

    odsc_tabs = malloc(sizeof(*odsc_tabs) * peer_num);
    odsc_nums = calloc(sizeof(*odsc_nums), peer_num);
    serv_reqs = malloc(sizeof(*serv_reqs) * peer_num);
    hndls = malloc(sizeof(*hndls) * peer_num);

    for(i = 0; i < peer_num; i++) {
        DEBUG_OUT("dht servr id %d\n", de_tab[i]->rank);
        DEBUG_OUT("self id %d\n", server->dsg->rank);

        if(de_tab[i]->rank == server->dsg->rank) {
            self_id_num = i;
            continue;
        }
        //remote servers
        margo_addr_lookup(server->mid, server->server_address[de_tab[i]->rank], &server_addr);
        margo_create(server->mid, server_addr, server->odsc_internal_id, &hndls[i]);
        margo_iforward(hndls[i], &in, &serv_reqs[i]);
        margo_addr_free(server->mid, server_addr);
    }

    if(self_id_num > -1) {
        podsc = malloc(sizeof(*podsc) * ssd->ent_self->odsc_num);
        odsc_nums[self_id_num] = dht_find_entry_all(ssd->ent_self, &in_odsc, &podsc, timeout);
        DEBUG_OUT("%d odscs found in %d\n", odsc_nums[self_id_num], server->dsg->rank);
        total_odscs += odsc_nums[self_id_num];
        if(odsc_nums[self_id_num]) {
            odsc_tabs[self_id_num] = malloc(sizeof(**odsc_tabs) * odsc_nums[self_id_num]);
            for(i = 0; i < odsc_nums[self_id_num]; i++) {
                obj_descriptor *odsc = &odsc_tabs[self_id_num][i]; //readability
                *odsc = *podsc[i];
                odsc->st = in_odsc.st;
                bbox_intersect(&in_odsc.bb, &odsc->bb, &odsc->bb);
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
            memcpy(odsc_tabs[i], dht_resp.odsc_list.raw_odsc, dht_resp.odsc_list.size);

            for(j = 0; j < odsc_nums[i]; j++) {
                //readability
                obj_descriptor *odsc = (obj_descriptor *)dht_resp.odsc_list.raw_odsc;
                DEBUG_OUT("remote buffer: %s\n", obj_desc_sprint(&odsc[j]));
            }
        }
        margo_free_output(hndls[i], &dht_resp);
        margo_destroy(hndls[i]);
    }

    odsc_curr = odsc_tab_all = malloc(sizeof(*odsc_tab_all) * total_odscs);

    for(i = 0; i < peer_num; i++) {
        memcpy(odsc_curr, odsc_tabs[i], sizeof(*odsc_curr) * odsc_nums[i]);
        odsc_curr += odsc_nums[i];
        free(odsc_tabs[i]);
    }

    for(i = 0; i < total_odscs; i++) {
        DEBUG_OUT("odscs in response: %s\n", obj_desc_sprint(&odsc_tab_all[i]));
    }

    out.odsc_list.size = sizeof(*odsc_tab_all) * total_odscs;
    out.odsc_list.raw_odsc = (char *)odsc_tab_all;
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);

    free(odsc_tab_all);
    free(hndls);
    free(serv_reqs);
    free(odsc_tabs);
    free(odsc_nums);
    free(de_tab); 
}
DEFINE_MARGO_RPC_HANDLER(query_rpc)

static void get_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    bulk_in_t in;
    bulk_out_t out;
    hg_bulk_t bulk_handle;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info* info = margo_get_info(handle);
    dspaces_provider_t server = (dspaces_provider_t)margo_registered_data(mid, info->id);

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS); 

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc.raw_odsc, sizeof(in_odsc));
     
    struct obj_data *od, *from_obj;

    from_obj = ls_find(server->dsg->ls, &in_odsc);

    od = obj_data_alloc(&in_odsc);
    ssd_copy(od, from_obj);
    
    hg_size_t size = (in_odsc.size)*bbox_volume(&(in_odsc.bb));
    void *buffer = (void*) od->data;
    hret = margo_bulk_create(mid, 1, (void**)&buffer, &size,
                HG_BULK_READ_ONLY, &bulk_handle);

    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_bulk_create() failure\n", __func__);
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
	}

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, info->addr, in.handle, 0,
            bulk_handle, 0, size);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_bulk_transfer() failure\n", __func__);
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_bulk_free(bulk_handle);
        margo_destroy(handle);
        return;
    }
    margo_bulk_free(bulk_handle);
    out.ret = dspaces_SUCCESS;
    obj_data_free(od);
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_bulk_free(bulk_handle);
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

    const struct hg_info* info = margo_get_info(handle);
    dspaces_provider_t server = (dspaces_provider_t)margo_registered_data(mid, info->id);

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));
    timeout = in.param;

    struct global_dimension od_gdim;
    memcpy(&od_gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));

    DEBUG_OUT("Received query for %s with timeout %d\n",  obj_desc_sprint(&in_odsc), timeout);
    
    obj_descriptor odsc, *odsc_tab;
    ABT_mutex_lock(server->sspace_mutex);
    struct sspace* ssd = lookup_sspace(server->dsg, in_odsc.name, &od_gdim);
    ABT_mutex_unlock(server->sspace_mutex);
    podsc = malloc(sizeof(*podsc) * ssd->ent_self->odsc_num);
    int num_odsc;
    num_odsc = dht_find_entry_all(ssd->ent_self, &in_odsc, &podsc, timeout);
    DEBUG_OUT("found %d DHT entries.\n", num_odsc);
    if (!num_odsc) {
        //need to figure out how to send that number of odscs is null
        out.odsc_list.size = 0;
        out.odsc_list.raw_odsc = NULL;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        
    }else{
        odsc_tab = malloc(sizeof(*odsc_tab) * num_odsc);
        for (int j = 0; j < num_odsc; j++) {
            obj_descriptor odsc;
            odsc = *podsc[j];
            DEBUG_OUT("including %s\n", obj_desc_sprint(&odsc));
            /* Preserve storage type at the destination. */
            odsc.st = in_odsc.st;
            bbox_intersect(&in_odsc.bb, &odsc.bb, &odsc.bb);
            odsc_tab[j] = odsc;
        }
        out.odsc_list.size = num_odsc * sizeof(obj_descriptor);
        out.odsc_list.raw_odsc = (char*)odsc_tab;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);

    }
    for (int i = 0; i < num_odsc; ++i)
    {
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

    const struct hg_info* info = margo_get_info(handle);
    dspaces_provider_t server = (dspaces_provider_t)margo_registered_data(mid, info->id);

    DEBUG_OUT("Received rpc to update obj_dht\n");

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS); 

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));
    struct global_dimension gdim;
    memcpy(&gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));
    type = in.param;

    DEBUG_OUT("received update_rpc %s\n", obj_desc_sprint(&in_odsc));
    ABT_mutex_lock(server->sspace_mutex);
    struct sspace* ssd = lookup_sspace(server->dsg, in_odsc.name, &gdim); 
    ABT_mutex_unlock(server->sspace_mutex);
    struct dht_entry *de = ssd->ent_self;

    ABT_mutex_lock(server->dht_mutex);
    switch(type) {
    case DS_OBJ_NEW: 
        err = dht_add_entry(de, &in_odsc);
        break;
    case DS_OBJ_OWNER:
        err = dht_update_owner(de, &in_odsc);
        break;
    default:
        fprintf(stderr, "ERROR: (%s): unknown object update type.\n", __func__);
    }
    ABT_mutex_unlock(server->dht_mutex);
    DEBUG_OUT("Updated dht %s in server %d \n", obj_desc_sprint(&in_odsc), server->dsg->rank);
    if (err < 0)
        fprintf(stderr, "ERROR (%s): obj_update_rpc Failed with %d\n", __func__, err);

    margo_free_input(handle, &in);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(obj_update_rpc)


static void ss_rpc(hg_handle_t handle)
{
    ss_information out;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info* info = margo_get_info(handle);
    dspaces_provider_t server = (dspaces_provider_t)margo_registered_data(mid, info->id);

    ss_info_hdr ss_data;
    ss_data.num_dims = ds_conf.ndim;
    ss_data.num_space_srv = server->dsg->size_sp;
    ss_data.max_versions = ds_conf.max_versions; 
    ss_data.hash_version = ds_conf.hash_version;
    ss_data.default_gdim.ndim = ds_conf.ndim;

    for(int i = 0; i < ds_conf.ndim; i++){
        ss_data.ss_domain.lb.c[i] = 0;
        ss_data.ss_domain.ub.c[i] = ds_conf.dims.c[i]-1;
        ss_data.default_gdim.sizes.c[i] = ds_conf.dims.c[i];
    }

    out.ss_buf.size = sizeof(ss_info_hdr);
    out.ss_buf.raw_odsc = (char*)(&ss_data);
    margo_respond(handle, &out);
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(ss_rpc)

static void send_kill_rpc(dspaces_provider_t server, int target, int *rank)
{
    //TODO: error handling/reporting
    hg_addr_t server_addr;
    hg_handle_t h;
   
    margo_addr_lookup(server->mid, server->server_address[target], &server_addr);
    margo_create(server->mid, server_addr, server->kill_id, &h);
    margo_forward(h, rank);
    margo_addr_free(server->mid, server_addr);
    margo_destroy(h);
}

static void kill_rpc(hg_handle_t handle)
{
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    const struct hg_info* info = margo_get_info(handle);
    dspaces_provider_t server = (dspaces_provider_t)margo_registered_data(mid, info->id);
    int32_t src, rank, parent, child1, child2;
    hg_return_t hret;

    hret = margo_get_input(handle, &src);
    DEBUG_OUT("Received kill signal from %d.\n", src);

    rank = server->dsg->rank;
    parent = (rank - 1) / 2;
    child1 = (rank * 2) + 1;
    child2 = child1 + 1;

    ABT_mutex_lock(server->kill_mutex);
    if(server->f_kill) {
        ABT_mutex_unlock(server->kill_mutex);
        margo_free_input(handle, &src);
        margo_destroy(handle);
        return;
    }
    DEBUG_OUT("Setting kill flag.\n");
    server->f_kill = 1;
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
    server_destroy(server);
}
DEFINE_MARGO_RPC_HANDLER(kill_rpc)

void dspaces_server_fini(dspaces_provider_t server)
{
    margo_wait_for_finalize(server->mid);
}
