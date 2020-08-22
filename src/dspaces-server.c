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
#include <ssg-mpi.h>
#include <abt.h>
#include <pthread.h>
#include "ss_data.h"
#include "dspaces-server.h"
#include "gspace.h"

static enum storage_type st = column_major;

pthread_mutex_t pmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  pcond = PTHREAD_COND_INITIALIZER;

pthread_mutex_t odscmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t ls_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t dht_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t sspace_mutex = PTHREAD_MUTEX_INITIALIZER;

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
    struct ds_gspace *dsg;   

};


DECLARE_MARGO_RPC_HANDLER(put_rpc);
DECLARE_MARGO_RPC_HANDLER(put_local_rpc);
DECLARE_MARGO_RPC_HANDLER(get_rpc);
DECLARE_MARGO_RPC_HANDLER(query_rpc);
DECLARE_MARGO_RPC_HANDLER(obj_update_rpc);
DECLARE_MARGO_RPC_HANDLER(odsc_internal_rpc);
DECLARE_MARGO_RPC_HANDLER(ss_rpc);

static void put_rpc(hg_handle_t h);
static void put_local_rpc(hg_handle_t h);
static void get_rpc(hg_handle_t h);
static void query_rpc(hg_handle_t h);
static void obj_update_rpc(hg_handle_t h);
static void odsc_internal_rpc(hg_handle_t h);
static void ss_rpc(hg_handle_t h);
//static void write_lock_rpc(hg_handle_t h);
//static void read_lock_rpc(hg_handle_t h);

static void my_membership_update_cb(void* uargs,
        ssg_member_id_t member_id,
        ssg_member_update_type_t update_type)
{
    switch(update_type) {
    case SSG_MEMBER_JOINED:
        printf("Member %lu joined\n", member_id);
        break;
    case SSG_MEMBER_LEFT:
        printf("Member %lu left\n", member_id);
        break;
    case SSG_MEMBER_DIED:
        printf("Member %lu died\n", member_id);
        break;
    }
}


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
                        fprintf(stderr, "index=%d, ndims=%d\n",idx, *(int*)options[0].pval);
                        fprintf(stderr, "The number of coordination should the same as the number of dimensions!\n");
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
                printf("Unknown option '%s' at line %d.\n", line, lineno);
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
        /*
       if((ds_conf.lock_type < lock_generic) ||
            (ds_conf.lock_type >= _lock_type_count)) {
            fprintf(stderr, "%s(): ERROR unknown lock type %d in file '%s'\n",
                __func__, ds_conf.lock_type, conf_name);
            err = -EINVAL;
            goto err_out;
        } 
        */

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


        ssg_group_config_t config = {
            .swim_period_length_ms = 1000,
            .swim_suspect_timeout_periods = 5,
            .swim_subgroup_member_count = -1,
            .ssg_credential = -1
            };

        //this group is used to store server addresses
        ssg_group_id_t gid = ssg_group_create_mpi(
                server->mid, "dspaces", comm,
                &config, NULL, NULL);

        dsg_l->gid = gid;
        dsg_l->rank = ssg_get_group_self_rank(gid);
        dsg_l->srv_ids = malloc(sizeof(ssg_member_id_t)*dsg_l->size_sp);

        for (int i = 0; i < dsg_l->size_sp; ++i)
        {
            dsg_l->srv_ids[i] = ssg_get_group_member_id_from_rank(gid, i);
        }

        //now write to a file for client access
        if(dsg_l->rank == 0){
            int fd;
            fd = open("servids.0", O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd < 0)
            {
                fprintf(stderr, "ERROR: unable to write server_ids into file\n");
                goto err_free;

            }
            int bytes_written = 0;
            bytes_written = write(fd, dsg_l->srv_ids, sizeof(ssg_member_id_t)*dsg_l->size_sp);
            if (bytes_written != sizeof(ssg_member_id_t)*dsg_l->size_sp)
            {
                fprintf(stderr, "ERROR: unable to write server_ids into opened file\n");
                free(dsg_l->srv_ids);
                close(fd);
                goto err_free;

            }
            close(fd);
        }

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


static int obj_put_update_dht(dspaces_provider_t server, struct obj_data *od)
{
    obj_descriptor *odsc = &od->obj_desc;
    pthread_mutex_lock(&sspace_mutex);
    struct sspace* ssd = lookup_sspace(server->dsg, odsc->name, &od->gdim);
    pthread_mutex_unlock(&sspace_mutex);
    struct dht_entry *dht_tab[ssd->dht->num_entries];

    int num_de, i, min_rank, err;

    fprintf(stderr, "In the update_dht\n");
    /* Compute object distribution to nodes in the space. */
    num_de = ssd_hash(ssd, &odsc->bb, dht_tab);
    if (num_de == 0) {
        fprintf(stderr, "'%s()': this should not happen, num_de == 0 ?!\n",
            __func__);
    }

    min_rank = dht_tab[0]->rank;
    /* Update object descriptors on the corresponding nodes. */
    for (i = 0; i < num_de; i++) {
        ssg_member_id_t peer = server->dsg->srv_ids[dht_tab[i]->rank];
        ssg_member_id_t self_id = server->dsg->srv_ids[server->dsg->rank];
        if (peer == self_id) {
            fprintf(stderr, "I am self, add in local dht\n");

            pthread_mutex_lock(&dht_mutex);
            dht_add_entry(ssd->ent_self, odsc);
            pthread_mutex_unlock(&dht_mutex);
            continue;
        }

        //now send rpc to the server for dht_update
        hg_return_t hret;
        odsc_gdim_t in;
        bulk_out_t out;
        fprintf(stderr, "sending object %s to dht server\n", obj_desc_sprint(odsc));

        in.odsc_gdim.size = sizeof(*odsc);
        in.odsc_gdim.gdim_size = sizeof(struct global_dimension);
        in.odsc_gdim.raw_odsc = (char*)(odsc);
        in.odsc_gdim.raw_gdim = (char*)(&od->gdim);

        hg_addr_t svr_addr = ssg_get_group_member_addr(server->dsg->gid, peer);

        hg_handle_t h;
        margo_create(server->mid, svr_addr, server->obj_update_id, &h);
        margo_forward(h, &in);

        fprintf(stderr, "sent obj %s\n", obj_desc_sprint(odsc));
        margo_destroy(h);
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

    od = obj_data_alloc(&odsc);
    in.odsc.size = sizeof(obj_descriptor);
    in.odsc.raw_odsc = (char*)(&odsc);
    od->obj_desc.owner = ssg_get_self_id(server->mid);

    hg_size_t rdma_size = (odsc.size)*bbox_volume(&odsc.bb);

    margo_bulk_create(server->mid, 1, (void**)(&(od->data)), &rdma_size,
                            HG_BULK_WRITE_ONLY, &in.handle);

    hg_addr_t client_addr;
    client_addr = ssg_get_group_member_addr(server->dsg->gid, odsc.owner);

    hg_handle_t handle;
    margo_create(server->mid,
        client_addr,
        server->drain_id,
        &handle);

    margo_forward(handle, &in);
    margo_get_output(handle, &out);
    if(out.ret == dspaces_SUCCESS){
        pthread_mutex_lock(&ls_mutex);
        ls_add_obj(server->dsg->ls, od);
        pthread_mutex_unlock(&ls_mutex);
    }
    //now update the dht with new owner information
    obj_put_update_dht(server, od);
    return out.ret;



}


//thread to move data between layers
static void *drain_thread(void*attr){
    dspaces_provider_t server = attr;
    while (1)
    {
        //sleep(10);
        pthread_mutex_lock(&pmutex);        
        if (cond_num == 0){/*sleep */
            fprintf(stderr, "THREAD SLEEPING\n");
            pthread_cond_wait(&pcond, &pmutex); //wait
        }else{ /*cond_num > 0 */
            int counter = 0;
            do{
                counter = 0;
                obj_descriptor odsc;
                struct obj_desc_list *odscl;
                //requires better way to get the obj_descriptor
                pthread_mutex_lock(&odscmutex);
                list_for_each_entry(odscl, &(server->dsg->obj_desc_drain_list), struct obj_desc_list, odsc_entry){
                    memcpy(&odsc, &(odscl->odsc), sizeof(obj_descriptor));
                    counter = 1;
                    break;
                    
                }
                pthread_mutex_unlock(&odscmutex);
                if(counter == 1){
                    int ret = get_client_data(odsc, server);
                    fprintf(stderr, "Finished draining %s\n", obj_desc_sprint(&odsc));
                    if(ret == dspaces_SUCCESS){
                        pthread_mutex_lock(&odscmutex);
                        //delete moved obj_descriptor from the list
                        list_del(&odscl->odsc_entry);
                        pthread_mutex_unlock(&odscmutex);
                    }
                    sleep(1);

                }
                
            }while(counter == 1);

            cond_num = 0;
        
        }                           
        
        pthread_mutex_unlock(&pmutex);
    }
        
}


int server_init(char *listen_addr_str, MPI_Comm comm, dspaces_provider_t* sv)
{
    int ret = ssg_init();
    assert(ret == SSG_SUCCESS);

    dspaces_provider_t server;
    server = (dspaces_provider_t)calloc(1, sizeof(*server));
    if(server == NULL)
        return dspaces_ERR_ALLOCATION; 

    server->mid = margo_init(listen_addr_str, MARGO_SERVER_MODE, 1, 2);
    assert(server->mid);

    
    hg_bool_t flag;
    hg_id_t id;
    margo_registered_name(server->mid, "put_rpc", &id, &flag);

    if(flag == HG_TRUE) { /* RPCs already registered */
        margo_registered_name(server->mid, "put_rpc",                   &server->put_id,                   &flag);
        margo_registered_name(server->mid, "put_local_rpc",                   &server->put_local_id,                   &flag);
        margo_registered_name(server->mid, "get_rpc",                   &server->get_id,                   &flag);
        margo_registered_name(server->mid, "query_rpc",                   &server->query_id,                   &flag);
        margo_registered_name(server->mid, "obj_update_rpc",                   &server->obj_update_id,                   &flag);
        margo_registered_name(server->mid, "odsc_internal_rpc",                   &server->odsc_internal_id,                   &flag);
        margo_registered_name(server->mid, "ss_rpc",                   &server->ss_id,                   &flag);
        margo_registered_name(server->mid, "drain_rpc",                   &server->drain_id,                   &flag);
   
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
        server->odsc_internal_id =
            MARGO_REGISTER(server->mid, "odsc_internal_rpc", odsc_gdim_t, odsc_list_t, odsc_internal_rpc);
        margo_register_data(server->mid, server->odsc_internal_id, (void*)server, NULL);
        server->ss_id =
            MARGO_REGISTER(server->mid, "ss_rpc", void, ss_information, ss_rpc);
        margo_register_data(server->mid, server->ss_id, (void*)server, NULL);
        server->drain_id =
            MARGO_REGISTER(server->mid, "drain_rpc", bulk_in_t, bulk_out_t, NULL);

    }
    int size_sp = 1;
    int err = dsg_alloc(server, "dataspaces.conf", comm);
    assert(err == 0);    

    *sv = server;
    if(server->dsg->rank == 0){
        ssg_group_id_store("dspaces.ssg", server->dsg->gid, server->dsg->size_sp);
    }

    //thread to drain the data
    pthread_t t_drain;
    pthread_create(&t_drain, NULL, drain_thread, (void*)server); //Create thread

    return dspaces_SUCCESS;
}

int server_destroy(dspaces_provider_t server)
{
    margo_wait_for_finalize(server->mid);

    margo_deregister(server->mid, server->put_id);
    margo_deregister(server->mid, server->put_local_id);
    margo_deregister(server->mid, server->get_id);
    margo_deregister(server->mid, server->query_id);
    margo_deregister(server->mid, server->obj_update_id);
    margo_deregister(server->mid, server->odsc_internal_id);
    margo_deregister(server->mid, server->ss_id);
    /* deregister other RPC ids ... */
    int ret = ssg_group_leave(server->dsg->gid);
    assert(ret == SSG_SUCCESS);

    free_sspace(server->dsg);
    ls_free(server->dsg->ls);
    free(server->dsg);
    margo_finalize(server->mid);
    free(server);
   
    ret = ssg_finalize();
    assert(ret == SSG_SUCCESS);
    return dspaces_SUCCESS;

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
    //set the owner to be this server id
    in_odsc.owner = ssg_get_self_id(server->mid);

    struct obj_data *od;
    od = obj_data_alloc(&in_odsc);
    memcpy(&od->gdim, in.odsc.raw_gdim, sizeof(struct global_dimension));

    if(!od)
        fprintf(stderr, "Obj_data_alloc error\n");

    //do write lock

    hg_size_t size = (in_odsc.size)*bbox_volume(&(in_odsc.bb));

    void *buffer = (void*) od->data;
    hret = margo_bulk_create(mid, 1, (void**)&(od->data), &size,
                HG_BULK_WRITE_ONLY, &bulk_handle);

    if(hret != HG_SUCCESS) {
        fprintf(stderr, "Error in margo_bulk_create\n");
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
	}
    
    hret = margo_bulk_transfer(mid, HG_BULK_PULL, info->addr, in.handle, 0,
            bulk_handle, 0, size);
    if(hret != HG_SUCCESS) {
        fprintf(stderr, "Error in margo_bulk_transfer\n");
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_bulk_free(bulk_handle);
        margo_destroy(handle);
        return;
    }

    pthread_mutex_lock(&ls_mutex);
    ls_add_obj(server->dsg->ls, od);
    pthread_mutex_unlock(&ls_mutex);

    fprintf(stderr, "Received obj %s\n", obj_desc_sprint(&od->obj_desc));

    //now update the dht
    out.ret = dspaces_SUCCESS;
    margo_bulk_free(bulk_handle);
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);

    obj_put_update_dht(server, od);
    fprintf(stderr, "Finished obj_put_update\n");

    //do write unlock;

    
}
DEFINE_MARGO_RPC_HANDLER(put_rpc)


static void put_local_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    odsc_gdim_t in;
    bulk_out_t out;
    hg_bulk_t bulk_handle;
    fprintf(stderr, "In the local put rpc\n");

    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info* info = margo_get_info(handle);
    dspaces_provider_t server = (dspaces_provider_t)margo_registered_data(mid, info->id);

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));

    struct obj_data *od;
    od = obj_data_alloc_no_data(&in_odsc, NULL);
    memcpy(&od->gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));

    if(!od)
        fprintf(stderr, "Obj_data_alloc error\n");


    fprintf(stderr, "Received obj %s \n", obj_desc_sprint(&od->obj_desc));

    //now update the dht
    out.ret = dspaces_SUCCESS;
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);

    obj_put_update_dht(server, od);
    fprintf(stderr, "Finished obj_put_local_update\n");

    //add to the local list for marking as to be drained data
    struct obj_desc_list *odscl;
    odscl = malloc(sizeof(*odscl));
    memcpy(&odscl->odsc, &od->obj_desc, sizeof(obj_descriptor));

    pthread_mutex_lock(&odscmutex);
    list_add_tail(&odscl->odsc_entry, &server->dsg->obj_desc_drain_list);
    pthread_mutex_unlock(&odscmutex);

    //wake up thread to initiate draining
    pthread_mutex_lock(&pmutex);
    cond_num = 1;
    pthread_cond_signal(&pcond);
    pthread_mutex_unlock(&pmutex);

    free(od);

    
}
DEFINE_MARGO_RPC_HANDLER(put_local_rpc)


static void query_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    odsc_gdim_t in;
    odsc_list_t out;
    fprintf(stderr, "received query\n");
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    obj_descriptor *odsc_tab_all;
    obj_descriptor **odsc_tab;
    int *odsc_nums;
    int total_odscs = 0;

    const struct hg_info* info = margo_get_info(handle);
    dspaces_provider_t server = (dspaces_provider_t)margo_registered_data(mid, info->id);

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));

    struct global_dimension od_gdim;
    memcpy(&od_gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));

    fprintf(stderr, "Received query for %s\n",  obj_desc_sprint(&in_odsc));
    //get the dht peers
    pthread_mutex_lock(&sspace_mutex);
    struct sspace* ssd = lookup_sspace(server->dsg, in_odsc.name, &od_gdim);
    pthread_mutex_unlock(&sspace_mutex);
    struct dht_entry *de_tab[ssd->dht->num_entries];
    int peer_num;
    peer_num = ssd_hash(ssd, &(in_odsc.bb), de_tab);

    int self_id_num = -1;

    fprintf(stderr, "peer_num is %d\n", peer_num);
    odsc_tab = malloc(sizeof(obj_descriptor*) * peer_num);
    odsc_nums = malloc(sizeof(int) * peer_num);



    //contact dht peers to get obj_desc_list
    margo_request *serv_req;
    hg_handle_t *hndl;
    hndl = (hg_handle_t*)malloc(sizeof(hg_handle_t)*peer_num);
    serv_req = (margo_request*)malloc(sizeof(margo_request)*peer_num);
     
    for (int i = 0; i < peer_num; ++i)
    {
        odsc_nums[i] = 0;   
        ssg_member_id_t dht_server_id = server->dsg->srv_ids[de_tab[i]->rank];
        fprintf(stderr, "dht servr id %lu\n", dht_server_id);
        ssg_member_id_t self_id = ssg_get_self_id(server->mid);
        fprintf(stderr, "self id %lu\n", self_id);

        if(self_id == dht_server_id){
            //I am one of the dht servers
            self_id_num = i;
            //now get obj_descriptors
            obj_descriptor *podsc[ssd->ent_self->odsc_num];
            int obj_versions[ssd->ent_self->odsc_size];
            int num_odsc = dht_find_entry_all(ssd->ent_self, &in_odsc, podsc);
            fprintf(stderr, "%d odscs found in %lu\n",num_odsc, ssg_get_self_id(server->mid));
            odsc_nums[i] = num_odsc;
            total_odscs = total_odscs + num_odsc;
            if (!num_odsc) {
                continue;
            }
            odsc_tab[i] = malloc(sizeof(obj_descriptor) * num_odsc);
            for (int j = 0; j < num_odsc; j++) {
                obj_descriptor odsc;
                odsc = *podsc[j];
                /* Preserve storage type at the destination. */
                odsc.st = in_odsc.st;
                bbox_intersect(&in_odsc.bb, &odsc.bb, &odsc.bb);
                odsc_tab[i][j] = odsc;
                fprintf(stderr, "%s\n", obj_desc_sprint(&odsc_tab[i][j]));
            }

        }
        else{
            hg_addr_t server_addr;
            server_addr = ssg_get_group_member_addr(server->dsg->gid, dht_server_id);
            hg_handle_t h;
            margo_create(server->mid, server_addr, server->odsc_internal_id, &h);

            margo_request req;
            //forward notification async to all subscribers
            margo_iforward(h, &in, &req); 
            hndl[i] = h;
            serv_req[i] = req;
        }
        

    }
    for (int i = 0; i < peer_num ; ++i){
        if(i == self_id_num)
            continue;
        margo_wait(serv_req[i]);
        //get total number of odscs;
        odsc_list_t dht_resp;
        //dht_resp.odsc_list.size = 0;
        margo_get_output(hndl[i], &dht_resp);
        if(dht_resp.odsc_list.size!=0){
            int num_received = (dht_resp.odsc_list.size)/sizeof(obj_descriptor);
            fprintf(stderr, "received %d odscs from peer %d\n", num_received, i);
            odsc_nums[i] = num_received;
            total_odscs = total_odscs + num_received;
            odsc_tab[i] = malloc(sizeof(obj_descriptor) * num_received);
            memcpy(odsc_tab[i], dht_resp.odsc_list.raw_odsc, dht_resp.odsc_list.size);

            for (int j = 0; j < num_received; ++j)
            {
                obj_descriptor *print_od;
                print_od = (obj_descriptor*)dht_resp.odsc_list.raw_odsc;
                fprintf(stderr, "remote buffer: %s\n", obj_desc_sprint(&print_od[j]));
                //fprintf(stderr, "Received odsc from remote dht: %s\n", obj_desc_sprint(&odsc_tab[i][j]));
            }    

        }
        margo_free_output(hndl[i], &dht_resp);
        margo_destroy(hndl[i]);
    }

    odsc_tab_all = malloc(sizeof(obj_descriptor) * total_odscs);
    int curr_odsc_count = 0;
    for (int i = 0; i < peer_num; ++i){
        for (int j = 0; j < odsc_nums[i]; ++j)
        {
            memcpy(&odsc_tab_all[i+j*odsc_nums[i]], &odsc_tab[i][j], sizeof(obj_descriptor));
            fprintf(stderr, "copied %s\n", obj_desc_sprint(&odsc_tab[i][j]));

        }
        free(odsc_tab[i]);
    }
    free(odsc_nums);

    //debug
    for (int i = 0; i < total_odscs; ++i)
    {
        fprintf(stderr, "Odscs in respose %s\n", obj_desc_sprint(&odsc_tab_all[i]));
    }

    out.odsc_list.size = sizeof(obj_descriptor) * total_odscs;
    out.odsc_list.raw_odsc = (char*)odsc_tab_all;
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    free(odsc_tab_all);
    margo_destroy(handle);

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
        fprintf(stderr,"Error in margo_bulk_create()\n");
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
	}

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, info->addr, in.handle, 0,
            bulk_handle, 0, size);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"Error in margo_bulk_transfer()\n");
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
    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(get_rpc)


static void odsc_internal_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    odsc_gdim_t in;
    odsc_list_t out;
    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info* info = margo_get_info(handle);
    dspaces_provider_t server = (dspaces_provider_t)margo_registered_data(mid, info->id);

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS);

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));

    struct global_dimension od_gdim;
    memcpy(&od_gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));

    
    obj_descriptor odsc, *odsc_tab;
    pthread_mutex_lock(&sspace_mutex);
    struct sspace* ssd = lookup_sspace(server->dsg, in_odsc.name, &od_gdim);
    pthread_mutex_unlock(&sspace_mutex);
    obj_descriptor *podsc[ssd->ent_self->odsc_num];
    int num_odsc;
    num_odsc = dht_find_entry_all(ssd->ent_self, &in_odsc, podsc);
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
        fprintf(stderr, "send odsc: %s\n", obj_desc_sprint(&odsc_tab[i]));
    }
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

    fprintf(stderr, "Received rpc to update obj_dht\n");
    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info* info = margo_get_info(handle);
    dspaces_provider_t server = (dspaces_provider_t)margo_registered_data(mid, info->id);

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS); 

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc_gdim.raw_odsc, sizeof(in_odsc));
    struct global_dimension gdim;
    memcpy(&gdim, in.odsc_gdim.raw_gdim, sizeof(struct global_dimension));

    fprintf(stderr, "received update_rpc %s\n", obj_desc_sprint(&in_odsc));
    pthread_mutex_lock(&sspace_mutex);
    struct sspace* ssd = lookup_sspace(server->dsg, in_odsc.name, &gdim); 
    pthread_mutex_unlock(&sspace_mutex);
    struct dht_entry *de = ssd->ent_self;

    pthread_mutex_lock(&dht_mutex);
    int err = dht_add_entry(de, &in_odsc);
    pthread_mutex_unlock(&dht_mutex);
    fprintf(stderr, "Finished adding in the dht %s\n", obj_desc_sprint(&in_odsc));
    if (err < 0)
        fprintf(stderr, "obj_update_rpc Failed with %d\n", err);
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


