/*
 * Copyright (c) 2020, Rutgers Discovery Informatics Institute, Rutgers University
 *
 * See COPYRIGHT in top-level directory.
 */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <inttypes.h>
#include "ss_data.h"
#include "dspaces.h"
#include "gspace.h"

#define DEBUG_OUT(args...) \
    do { \
        if(client->f_debug) { \
           fprintf(stderr, "Rank %i: %s, line %i (%s): ", client->rank, __FILE__, __LINE__, __func__); \
           fprintf(stderr, args); \
        } \
    }while(0);

#define SUB_HASH_SIZE 16

static enum storage_type st = column_major;

struct dspaces_sub_handle {
    struct dspaces_req *req;
    void *arg;
    int result;
    int status;
    int id;
    dspaces_sub_fn cb;
};

struct sub_list_node {
    struct sub_list_node *next;
    struct dspaces_sub_handle *subh;
    int id;
};

struct dspaces_client {
    margo_instance_id mid;
    hg_id_t put_id;
    hg_id_t put_local_id;
    hg_id_t get_id;
    hg_id_t query_id;
    hg_id_t ss_id;
    hg_id_t drain_id;
    hg_id_t kill_id;
	hg_id_t sub_id;
    hg_id_t notify_id;
	struct dc_gspace *dcg;
    char **server_address;
    int size_sp;
    int rank;
    int local_put_count; // used during finalize
    int f_debug;
    int f_final;
    int listener_init;

    int sub_serial;
    struct sub_list_node *sub_lists[SUB_HASH_SIZE];

    ABT_mutex ls_mutex;
    ABT_mutex drain_mutex;
    ABT_mutex sub_mutex;
    ABT_cond drain_cond;

    ABT_xstream listener_xs;
};

DECLARE_MARGO_RPC_HANDLER(get_rpc);
static void get_rpc(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(drain_rpc);
static void drain_rpc(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(kill_rpc);
static void kill_rpc(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(notify_rpc);
static void notify_rpc(hg_handle_t h);

//round robin fashion
//based on how many clients processes are connected to the server
static hg_return_t get_server_address(dspaces_client_t client, hg_addr_t *server_addr)
{ 
    int peer_id = client->rank % client->size_sp;
 
    return(margo_addr_lookup(client->mid, client->server_address[peer_id], server_addr));
}


static int get_ss_info(dspaces_client_t client) {
    hg_return_t hret;
    hg_handle_t handle;
    ss_information out;
    hg_addr_t server_addr;
    hg_size_t my_addr_size;
    int ret = dspaces_SUCCESS;

    char *my_addr_str = NULL;

    get_server_address(client, &server_addr);

    /* create handle */
    hret = margo_create(client->mid, server_addr, client->ss_id, &handle);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_create() failed\n", __func__);
        return dspaces_ERR_MERCURY;
    }

    hret = margo_forward(handle, NULL);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s):  margo_forward() failed\n", __func__);
        margo_destroy(handle);
        return dspaces_ERR_MERCURY;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_get_output() failed\n", __func__);
        margo_destroy(handle);
        return dspaces_ERR_MERCURY;
    }
    ss_info_hdr ss_data;
    memcpy(&ss_data, out.ss_buf.raw_odsc, sizeof(ss_info_hdr));

    client->dcg->ss_info.num_dims = ss_data.num_dims;
    client->dcg->ss_info.num_space_srv = ss_data.num_space_srv;
    memcpy(&(client->dcg->ss_domain), &(ss_data.ss_domain), sizeof(struct bbox));
    client->dcg->max_versions = ss_data.max_versions;
    client->dcg->hash_version = ss_data.hash_version;
    memcpy(&(client->dcg->default_gdim), &(ss_data.default_gdim), sizeof(struct global_dimension));

    margo_free_output(handle, &out);
    margo_destroy(handle);
    margo_addr_free(client->mid, server_addr);
    return ret;
}

static struct dc_gspace * dcg_alloc(dspaces_client_t client)
{
        struct dc_gspace *dcg_l;
        int i;

        dcg_l = calloc(1, sizeof(*dcg_l));
        if (!dcg_l)
                goto err_out;

        INIT_LIST_HEAD(&dcg_l->locks_list);
        init_gdim_list(&dcg_l->gdim_list);    
        dcg_l->hash_version = ssd_hash_version_v1; // set default hash versio
        return dcg_l;

 err_out:
        fprintf(stderr, "'%s()': failed.\n", __func__);
        return NULL;
}


static int build_address(dspaces_client_t client){
    /* open config file for reading */
    int ret;
    struct stat st;
    char *rd_buf = NULL;
    ssize_t rd_buf_size;
    char *tok;
    void *addr_str_buf = NULL;
    int addr_str_buf_len = 0, num_addrs = 0;
    int wait_time, time = 0;
    int fd;
    char *file_name = "servids.0";
    
    do {
        fd = open(file_name, O_RDONLY);
        if (fd == -1)
        {
            if(errno == ENOENT) {
                DEBUG_OUT("unable to find config file %s after %d seconds, will try again...\n", file_name, time);
            } else {
                fprintf(stderr, "ERROR: could not open config file %s.\n", file_name);
                goto fini;
            }
            wait_time = (rand() % 3) + 1;
            time += wait_time;
            sleep(wait_time);
        }
    }while(fd == -1);

    /* get file size and allocate a buffer to store it */
    ret = fstat(fd, &st);
    if (ret == -1)
    {
        fprintf(stderr, "Error: Unable to stat config file %s for server_address list\n",
            file_name);
        goto fini;
    }
    ret = -1;
    rd_buf = malloc(st.st_size);
    if (rd_buf == NULL) goto fini;

    /* load it all in one fell swoop */
    rd_buf_size = read(fd, rd_buf, st.st_size);
    if (rd_buf_size != st.st_size)
    {
        fprintf(stderr, "Error: Unable to stat config file %s for server_address list\n",
            file_name);
        goto fini;
    }
    rd_buf[rd_buf_size]='\0';

    // strtok the result - each space-delimited address is assumed to be
    // a unique mercury address

    tok = strtok(rd_buf, "\r\n\t ");
    if (tok == NULL) goto fini;

    // build up the address buffer
    addr_str_buf = malloc(rd_buf_size);
    if (addr_str_buf == NULL) goto fini;
    do
    {
        int tok_size = strlen(tok);
        memcpy((char*)addr_str_buf + addr_str_buf_len, tok, tok_size+1);
        addr_str_buf_len += tok_size+1;
        num_addrs++;
        tok = strtok(NULL, "\r\n\t ");
    } while (tok != NULL);
    if (addr_str_buf_len != rd_buf_size)
    {
        // adjust buffer size if our initial guess was wrong
        fprintf(stderr, "Read size and buffer_len are not equal\n");
        void *tmp = realloc(addr_str_buf, addr_str_buf_len);
        if (tmp == NULL) goto fini;
        addr_str_buf = tmp;
    }
    free(rd_buf);
   
    /* set up address string array for group members */
    client->server_address = (char **)addr_str_buf_to_list(addr_str_buf, num_addrs);
    client->size_sp = num_addrs;
    ret = 0;

fini:
    return ret;
}

static int read_conf(dspaces_client_t client, char **listen_addr_str)
{
    int wait_time, time = 0;
    int size;
    FILE *fd;
    fpos_t lstart;
    int i, ret;   
 
    do {
        fd = fopen("conf.ds", "r");
        if(!fd) {
            if(errno == ENOENT) {
                DEBUG_OUT("unable to find config file 'conf.ds' after %d seconds, will try again...\n", time);
            } else {
                fprintf(stderr, "could not open config file 'conf.ds'.\n");
                goto fini;
            }
        }
        wait_time = (rand() % 3) + 1;
        time += wait_time;
        sleep(wait_time);
    }while(!fd);

    fscanf(fd, "%d\n", &client->size_sp);
    client->server_address = malloc(client->size_sp * sizeof(*client->server_address));
    for(i = 0; i < client->size_sp; i++) {
        fgetpos(fd, &lstart);
        fscanf(fd, "%*s%n\n", &size);
        fsetpos(fd, &lstart);
        client->server_address[i] = malloc(size + 1);
        fscanf(fd, "%s\n", client->server_address[i]);
    }
    fgetpos(fd, &lstart);
    fscanf(fd, "%*s%n\n", &size);
    fsetpos(fd, &lstart);
    *listen_addr_str = malloc(size + 1);
    fscanf(fd, "%s\n", *listen_addr_str);

    ret = 0;

fini:
    return ret;
}

int dspaces_init(int rank, dspaces_client_t* c)
{   
    char *listen_addr_str;
    const char *envdebug = getenv("DSPACES_DEBUG"); 
    dspaces_client_t client = (dspaces_client_t)calloc(1, sizeof(*client));
    if(!client) return dspaces_ERR_ALLOCATION;
    int i;

    if(envdebug) {
        client->f_debug = 1;
    }

    client->rank = rank;

    //now do dcg_alloc and store gid
    client->dcg = dcg_alloc(client);

    if(!(client->dcg))
        return dspaces_ERR_ALLOCATION;

    read_conf(client, &listen_addr_str);

    ABT_init(0, NULL);

    client->mid = margo_init(listen_addr_str, MARGO_SERVER_MODE, 0, 0);
    assert(client->mid);

    free(listen_addr_str);

    ABT_mutex_create(&client->ls_mutex);
    ABT_mutex_create(&client->drain_mutex);
    ABT_mutex_create(&client->sub_mutex);
    ABT_cond_create(&client->drain_cond);

    for(i = 0; i < SUB_HASH_SIZE; i++) {
        client->sub_lists[i] = NULL;
    }
    client->sub_serial = 0;

    /* check if RPCs have already been registered */
    hg_bool_t flag;
    hg_id_t id;
    margo_registered_name(client->mid, "put_rpc", &id, &flag);

    if(flag == HG_TRUE) { /* RPCs already registered */
        margo_registered_name(client->mid, "put_rpc",       &client->put_id, &flag);
        margo_registered_name(client->mid, "put_local_rpc", &client->put_local_id, &flag);
        margo_registered_name(client->mid, "get_rpc",       &client->get_id, &flag);
        margo_registered_name(client->mid, "query_rpc",     &client->query_id, &flag);
        margo_registered_name(client->mid, "ss_rpc",        &client->ss_id, &flag);
        margo_registered_name(client->mid, "drain_rpc",     &client->drain_id, &flag);
        margo_registered_name(client->mid, "kill_rpc",      &client->kill_id, &flag);
		margo_registered_name(client->mid, "sub_rpc",		&client->sub_id, &flag);
    	margo_registered_name(client->mid, "notify_rpc",	&client->notify_id, &flag);
	} else {

        client->put_id =
            MARGO_REGISTER(client->mid, "put_rpc", bulk_gdim_t, bulk_out_t, NULL);
        client->put_local_id =
            MARGO_REGISTER(client->mid, "put_local_rpc", odsc_gdim_t, bulk_out_t, NULL);
        client->get_id =
            MARGO_REGISTER(client->mid, "get_rpc", bulk_in_t, bulk_out_t, get_rpc);
        margo_register_data(client->mid, client->get_id, (void *)client, NULL);
        client->query_id =
            MARGO_REGISTER(client->mid, "query_rpc", odsc_gdim_t, odsc_list_t, NULL);
        client->ss_id =
            MARGO_REGISTER(client->mid, "ss_rpc", void, ss_information, NULL);
        client->drain_id =
            MARGO_REGISTER(client->mid, "drain_rpc", bulk_in_t, bulk_out_t, drain_rpc);
        margo_register_data(client->mid, client->drain_id, (void *)client, NULL);
        client->kill_id =
            MARGO_REGISTER(client->mid, "kill_rpc", int32_t, void, kill_rpc);
        margo_registered_disable_response(client->mid, client->kill_id, HG_TRUE);
        margo_register_data(client->mid, client->kill_id, (void *)client, NULL);         
		client->sub_id =
			MARGO_REGISTER(client->mid, "sub_rpc", odsc_gdim_t, void, NULL);
		margo_registered_disable_response(client->mid, client->sub_id, HG_TRUE);
		client->notify_id =
			MARGO_REGISTER(client->mid, "notify_rpc", odsc_list_t, void, notify_rpc);
		margo_register_data(client->mid, client->notify_id, (void *)client, NULL);
		margo_registered_disable_response(client->mid, client->notify_id, HG_TRUE);
			
	}

    get_ss_info(client);
    DEBUG_OUT("Total max versions on the client side is %d\n", client->dcg->max_versions);

    client->dcg->ls = ls_alloc(client->dcg->max_versions);
    client->local_put_count = 0;
    client->f_final = 0;

    *c = client;

    return dspaces_SUCCESS;
}


int dspaces_fini(dspaces_client_t client)
{
    DEBUG_OUT("finalizing.\n");

    do { // watch out for spurious wake
        ABT_mutex_lock(client->drain_mutex);
        client->f_final = 1;

        if(client->local_put_count > 0) {
            DEBUG_OUT("waiting for pending drainage. %d object remain.\n", client->local_put_count);
            ABT_cond_wait(client->drain_cond, client->drain_mutex);
            DEBUG_OUT("received drainage signal.\n");    
        }
        ABT_mutex_unlock(client->drain_mutex);
    } while(client->local_put_count > 0);

    DEBUG_OUT("all objects drained. Finalizing...\n");

    free_gdim_list(&client->dcg->gdim_list);
    free(client->server_address[0]);
    free(client->server_address);
    ls_free(client->dcg->ls);
    free(client->dcg);

    margo_finalize(client->mid);

    if(client->listener_init) {
        ABT_xstream_join(client->listener_xs);
        ABT_xstream_free(&client->listener_xs);
    }

    free(client);

    return dspaces_SUCCESS;
}

void dspaces_define_gdim (dspaces_client_t client, 
    const char *var_name, int ndim, uint64_t *gdim){
    if(ndim > BBOX_MAX_NDIM){
        fprintf(stderr, "ERROR: maximum object dimensionality is  %d\n", BBOX_MAX_NDIM);
    }else{
        update_gdim_list(&(client->dcg->gdim_list), var_name, ndim, gdim);
    }


}

int dspaces_put (dspaces_client_t client,
		const char *var_name,
        unsigned int ver, int elem_size,
        int ndim, uint64_t *lb, uint64_t *ub, 
        void *data)
{
    hg_addr_t server_addr;
    hg_handle_t handle;
	hg_return_t hret;
    int ret = dspaces_SUCCESS;

    obj_descriptor odsc = {
            .version = ver, .owner = {0}, 
            .st = st,
            .size = elem_size,
            .bb = {.num_dims = ndim,}
    };

    memset(odsc.bb.lb.c, 0, sizeof(uint64_t)*BBOX_MAX_NDIM);
    memset(odsc.bb.ub.c, 0, sizeof(uint64_t)*BBOX_MAX_NDIM);

    memcpy(odsc.bb.lb.c, lb, sizeof(uint64_t)*ndim);
    memcpy(odsc.bb.ub.c, ub, sizeof(uint64_t)*ndim);

    strncpy(odsc.name, var_name, sizeof(odsc.name)-1);
    odsc.name[sizeof(odsc.name)-1] = '\0';

    bulk_gdim_t in;
    bulk_out_t out;
    struct global_dimension odsc_gdim;
    set_global_dimension(&(client->dcg->gdim_list), var_name, &(client->dcg->default_gdim),
                         &odsc_gdim);


    in.odsc.size = sizeof(odsc);
    in.odsc.raw_odsc = (char*)(&odsc);
    in.odsc.gdim_size = sizeof(struct global_dimension);
    in.odsc.raw_gdim = (char*)(&odsc_gdim);
    hg_size_t rdma_size = (elem_size)*bbox_volume(&odsc.bb);

    DEBUG_OUT("sending object %s \n", obj_desc_sprint(&odsc));

    hret = margo_bulk_create(client->mid, 1, (void**)&data, &rdma_size,
                            HG_BULK_READ_ONLY, &in.handle);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_bulk_create() failed\n", __func__);
        return dspaces_ERR_MERCURY;
    }
    
    get_server_address(client, &server_addr);
    /* create handle */
    hret = margo_create( client->mid,
            server_addr,
            client->put_id,
            &handle);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_create() failed\n", __func__);
        margo_bulk_free(in.handle);
        return dspaces_ERR_MERCURY;
    }

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_forward() failed\n", __func__);
        margo_bulk_free(in.handle);
        margo_destroy(handle);
        return dspaces_ERR_MERCURY;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_get_output() failed\n", __func__);
        margo_bulk_free(in.handle);
        margo_destroy(handle);
        return dspaces_ERR_MERCURY;
    }

    ret = out.ret;
    margo_free_output(handle, &out);
    margo_bulk_free(in.handle);
    margo_destroy(handle);
    margo_addr_free(client->mid, server_addr);
	return ret;

}

static int get_data(dspaces_client_t client, int num_odscs, obj_descriptor req_obj, obj_descriptor *odsc_tab, void *data)
{
    bulk_in_t *in;
    in = (bulk_in_t*)malloc(sizeof(bulk_in_t)*num_odscs);

    struct obj_data **od;
    od = malloc(num_odscs * sizeof(struct obj_data *));

    margo_request *serv_req;
    hg_handle_t *hndl;
    hndl = (hg_handle_t*)malloc(sizeof(hg_handle_t)*num_odscs);
    serv_req = (margo_request*)malloc(sizeof(margo_request)*num_odscs);

    for (int i = 0; i < num_odscs; ++i)
    {
        od[i] = obj_data_alloc(&odsc_tab[i]);
        in[i].odsc.size = sizeof(obj_descriptor);
        in[i].odsc.raw_odsc = (char*)(&odsc_tab[i]);

        hg_size_t rdma_size = (req_obj.size)*bbox_volume(&odsc_tab[i].bb);

        margo_bulk_create(client->mid, 1, (void**)(&(od[i]->data)), &rdma_size,
                                HG_BULK_WRITE_ONLY, &in[i].handle);

        hg_addr_t server_addr;
        margo_addr_lookup(client->mid, odsc_tab[i].owner, &server_addr);

        hg_handle_t handle;
        margo_create( client->mid,
            server_addr,
            client->get_id,
            &handle);

        margo_request req;
        //forward get requests
        margo_iforward(handle, &in[i], &req); 
        hndl[i] = handle;
        serv_req[i] = req;
        margo_addr_free(client->mid, server_addr);
    }

    struct obj_data *return_od = obj_data_alloc_no_data(&req_obj, data);

    //TODO: rewrite with margo_wait_any()
    for (int i = 0; i < num_odscs; ++i){
        margo_wait(serv_req[i]);
        bulk_out_t resp;
        margo_get_output(hndl[i], &resp);
        margo_free_output(hndl[i], &resp);
        margo_destroy(hndl[i]);
        //copy received data into user return buffer
        ssd_copy(return_od, od[i]);
        obj_data_free(od[i]);
    }
    free(hndl);
    free(serv_req);
    free(in);
    free(return_od);

}

static int dspaces_init_listener(dspaces_client_t client)
{
	
    ABT_pool margo_pool;
    hg_return_t hret;
	int ret = dspaces_SUCCESS;

    hret = margo_get_handler_pool(client->mid, &margo_pool);
    if(hret != HG_SUCCESS || margo_pool == ABT_POOL_NULL) {
    	fprintf(stderr, "DSPACES_ERROR: %s: could not get handler pool (%d).\n", __func__, hret);
    	return(dspaces_ERR_ARGOBOTS);
	}
    client->listener_xs = ABT_XSTREAM_NULL;
    ret = ABT_xstream_create_basic(ABT_SCHED_BASIC_WAIT, 1, &margo_pool, ABT_SCHED_CONFIG_NULL, &client->listener_xs);
	if(ret != ABT_SUCCESS) {
  		char err_str[1000];
   		ABT_error_get_str(ret, err_str, NULL);
    	fprintf(stderr, "DSPACES ERROR: %s: could not launch handler thread: %s\n", __func__, err_str);
    	return(dspaces_ERR_ARGOBOTS);
	}

    client->listener_init = 1;

	return(ret);
}

int dspaces_put_local (dspaces_client_t client,
        const char *var_name,
        unsigned int ver, int elem_size,
        int ndim, uint64_t *lb, uint64_t *ub, 
        void *data)
{
    hg_addr_t server_addr;
    hg_handle_t handle;
    hg_return_t hret;
    int ret = dspaces_SUCCESS;


	ret = dspaces_init_listener(client);
	if(ret != dspaces_SUCCESS) {
		return(ret);
	}
    
	client->local_put_count++;

    obj_descriptor odsc = {
            .version = ver, 
            .st = st,
            .size = elem_size,
            .bb = {.num_dims = ndim,}
    };

    hg_addr_t owner_addr;
    size_t owner_addr_size = 128;

    margo_addr_self(client->mid, &owner_addr);
    margo_addr_to_string(client->mid, odsc.owner, &owner_addr_size, owner_addr);
    margo_addr_free(client->mid, owner_addr);

    memset(odsc.bb.lb.c, 0, sizeof(uint64_t)*BBOX_MAX_NDIM);
    memset(odsc.bb.ub.c, 0, sizeof(uint64_t)*BBOX_MAX_NDIM);

    memcpy(odsc.bb.lb.c, lb, sizeof(uint64_t)*ndim);
    memcpy(odsc.bb.ub.c, ub, sizeof(uint64_t)*ndim);

    strncpy(odsc.name, var_name, sizeof(odsc.name)-1);
    odsc.name[sizeof(odsc.name)-1] = '\0';

    odsc_gdim_t in;
    bulk_out_t out;
    struct obj_data *od;
    od = obj_data_alloc_with_data(&odsc, data);

    set_global_dimension(&(client->dcg->gdim_list), var_name, &(client->dcg->default_gdim),
                         &od->gdim);

    
    ABT_mutex_lock(client->ls_mutex);
    ls_add_obj(client->dcg->ls, od);
    DEBUG_OUT("Added into local_storage\n");
    ABT_mutex_unlock(client->ls_mutex);

    in.odsc_gdim.size = sizeof(odsc);
    in.odsc_gdim.raw_odsc = (char*)(&odsc);
    in.odsc_gdim.gdim_size = sizeof(struct global_dimension);
    in.odsc_gdim.raw_gdim = (char*)(&od->gdim);

    DEBUG_OUT("sending object information %s \n", obj_desc_sprint(&odsc));
    
    get_server_address(client, &server_addr);
    /* create handle */
    hret = margo_create( client->mid,
            server_addr,
            client->put_local_id,
            &handle);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_create() failed\n", __func__);
        return dspaces_ERR_MERCURY;
    }

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_forward() failed\n", __func__);
        margo_destroy(handle);
        return dspaces_ERR_MERCURY;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s):  margo_get_output() failed\n", __func__);
        margo_destroy(handle);
        return dspaces_ERR_MERCURY;
    }

    ret = out.ret;
    margo_free_output(handle, &out);
    margo_destroy(handle);
    margo_addr_free(client->mid, server_addr);

    return ret;

}

int dspaces_get (dspaces_client_t client,
        const char *var_name,
        unsigned int ver, int elem_size,
        int ndim, uint64_t *lb, uint64_t *ub, 
        void *data, int timeout)
{
    hg_addr_t server_addr;
    hg_handle_t handle;
    hg_return_t hret;
    int ret = dspaces_SUCCESS;

    obj_descriptor odsc = {
            .version = ver, .owner = {0}, 
            .st = st,
            .size = elem_size,
            .bb = {.num_dims = ndim,}
    };

    memset(odsc.bb.lb.c, 0, sizeof(uint64_t)*BBOX_MAX_NDIM);
    memset(odsc.bb.ub.c, 0, sizeof(uint64_t)*BBOX_MAX_NDIM);

    memcpy(odsc.bb.lb.c, lb, sizeof(uint64_t)*ndim);
    memcpy(odsc.bb.ub.c, ub, sizeof(uint64_t)*ndim);

    strncpy(odsc.name, var_name, sizeof(odsc.name)-1);
    odsc.name[sizeof(odsc.name)-1] = '\0';

    odsc_gdim_t in;
    odsc_list_t out;

    in.odsc_gdim.size = sizeof(odsc);
    in.odsc_gdim.raw_odsc = (char*)(&odsc);
    in.param = timeout;

    struct global_dimension od_gdim;

    set_global_dimension(&(client->dcg->gdim_list), var_name, &(client->dcg->default_gdim),
                         &od_gdim);

    in.odsc_gdim.gdim_size = sizeof(struct global_dimension);
    in.odsc_gdim.raw_gdim = (char*)(&od_gdim);

    get_server_address(client, &server_addr);

    hret = margo_create( client->mid,
            server_addr,
            client->query_id,
            &handle);
    assert(hret == HG_SUCCESS);
    hret = margo_forward(handle, &in);
    assert(hret == HG_SUCCESS);

    hret = margo_get_output(handle, &out);
    assert(hret == HG_SUCCESS);

    obj_descriptor *odsc_tab;
    int num_odscs = (out.odsc_list.size)/sizeof(obj_descriptor);
    odsc_tab = malloc(out.odsc_list.size);
    memcpy(odsc_tab, out.odsc_list.raw_odsc, out.odsc_list.size);
    margo_free_output(handle, &out);
    margo_destroy(handle);
    
    DEBUG_OUT("Finished query - need to fetch %d objects\n", num_odscs);
    for (int i = 0; i < num_odscs; ++i)
    {
        DEBUG_OUT("%s\n", obj_desc_sprint(&odsc_tab[i]));
    }

    //send request to get the obj_desc
    if(num_odscs!=0)
        get_data(client, num_odscs, odsc, odsc_tab, data);

    margo_addr_free(client->mid, server_addr);

}

static void get_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    bulk_in_t in;
    bulk_out_t out;
    hg_bulk_t bulk_handle;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info* info = margo_get_info(handle);
    dspaces_client_t client = (dspaces_client_t)margo_registered_data(mid, info->id);

    DEBUG_OUT("Received rpc to get data\n");

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS); 

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc.raw_odsc, sizeof(in_odsc));

    DEBUG_OUT("%s\n", obj_desc_sprint(&in_odsc));
     
    struct obj_data *od, *from_obj;

    from_obj = ls_find(client->dcg->ls, &in_odsc);
    if(!from_obj)
        fprintf(stderr, "WARNING: (%s): Object not found in local storage\n", __func__);

    od = obj_data_alloc(&in_odsc);
    if(!od)
        fprintf(stderr, "ERROR: (%s): object allocation failed\n", __func__);

    if(from_obj->data == NULL)
        fprintf(stderr, "ERROR: (%s): object data allocation failed\n", __func__);

    ssd_copy(od, from_obj);
    DEBUG_OUT("After ssd_copy\n");
    
    hg_size_t size = (in_odsc.size)*bbox_volume(&(in_odsc.bb));
    void *buffer = (void*) od->data;

    hret = margo_bulk_create(mid, 1, (void**)&buffer, &size,
                HG_BULK_READ_ONLY, &bulk_handle);

    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s):  margo_bulk_create() failed\n", __func__);
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, info->addr, in.handle, 0,
            bulk_handle, 0, size);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_bulk_transfer() failed\n", __func__);
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

static void drain_rpc(hg_handle_t handle)
{
    hg_return_t hret;
    bulk_in_t in;
    bulk_out_t out;
    hg_bulk_t bulk_handle;

    margo_instance_id mid = margo_hg_handle_get_instance(handle);

    const struct hg_info* info = margo_get_info(handle);
    dspaces_client_t client = (dspaces_client_t)margo_registered_data(mid, info->id);

    DEBUG_OUT("Received rpc to drain data\n");

    hret = margo_get_input(handle, &in);
    assert(hret == HG_SUCCESS); 

    obj_descriptor in_odsc;
    memcpy(&in_odsc, in.odsc.raw_odsc, sizeof(in_odsc));

    DEBUG_OUT("%s\n", obj_desc_sprint(&in_odsc));
     
    struct obj_data *from_obj;

    from_obj = ls_find(client->dcg->ls, &in_odsc);
    if(!from_obj){
        fprintf(stderr, "Object not found in client's local storage.\n Make sure MAX version is set appropriately in dataspaces.conf\n");
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        return; 
    }

    
    hg_size_t size = (in_odsc.size)*bbox_volume(&(in_odsc.bb));
    void *buffer = (void*) from_obj->data;

    hret = margo_bulk_create(mid, 1, (void**)&buffer, &size,
                HG_BULK_READ_ONLY, &bulk_handle);

    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_bulk_create() failed\n", __func__);
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_destroy(handle);
        return;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, info->addr, in.handle, 0,
            bulk_handle, 0, size);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_bulk_transfer() failed\n", __func__);
        out.ret = dspaces_ERR_MERCURY;
        margo_respond(handle, &out);
        margo_free_input(handle, &in);
        margo_bulk_free(bulk_handle);
        margo_destroy(handle);
        return;
    }
    margo_bulk_free(bulk_handle);
     

    out.ret = dspaces_SUCCESS;
    margo_respond(handle, &out);
    margo_free_input(handle, &in);
    margo_destroy(handle);
    //delete object from local storage
    DEBUG_OUT("Finished draining %s\n", obj_desc_sprint(&from_obj->obj_desc)); 
    ABT_mutex_lock(client->ls_mutex);
    ls_try_remove_free(client->dcg->ls, from_obj);
    ABT_mutex_unlock(client->ls_mutex);

    ABT_mutex_lock(client->drain_mutex);
    client->local_put_count--;
    if(client->local_put_count == 0 && client->f_final) {
        DEBUG_OUT("signaling all objects drained.\n");
        ABT_cond_signal(client->drain_cond);
    }
    ABT_mutex_unlock(client->drain_mutex);
    DEBUG_OUT("%d objects left to drain...\n", client->local_put_count);
}
DEFINE_MARGO_RPC_HANDLER(drain_rpc)

static void notify_rpc(hg_handle_t handle)
{
	margo_destroy(handle);	
}
DEFINE_MARGO_RPC_HANDLER(notify_rpc)

static void register_client_sub(dspaces_client_t client, struct dspaces_sub_handle *subh)
{
    int listidx = subh->id % SUB_HASH_SIZE;
    struct sub_list_node **node = &client->sub_lists[listidx];

    while(*node) {
        node = &((*node)->next);
    }

    *node = malloc(sizeof(**node));
    (*node)->next = NULL;
    (*node)->subh = subh;
    (*node)->id = subh->id;
}

struct dspaces_sub_handle *dspaces_sub(dspaces_client_t client,
        const char *var_name,
        unsigned int ver, int elem_size,
        int ndim, uint64_t *lb, uint64_t *ub,
        dspaces_sub_fn sub_cb, void *arg)
{
    hg_addr_t server_addr;
    hg_handle_t handle;
    hg_return_t hret;
    struct dspaces_sub_handle *subh;
    obj_descriptor odsc = {
        .version = ver, .owner = {0},
        .st = st, .size = elem_size,
        .bb = {.num_dims = ndim,}
    };
	odsc_gdim_t in;
    struct global_dimension od_gdim;
    int ret;

	ret = dspaces_init_listener(client);
    if(ret != dspaces_SUCCESS) {
        return(DSPACES_SUB_FAIL);
    }

    subh = malloc(sizeof(*subh));
    
    subh->req = malloc(sizeof(*subh->req));
    subh->req->var_name = strdup(var_name);
    subh->req->ver = ver;
    subh->req->elem_size = elem_size;
    subh->req->ndim = ndim;
    subh->req->lb = malloc(sizeof(*subh->req->lb) * ndim);
    subh->req->ub = malloc(sizeof(*subh->req->ub) * ndim);
    memcpy(subh->req->lb, lb, ndim * sizeof(*lb));
    memcpy(subh->req->ub, ub, ndim * sizeof(*ub));

    subh->arg = arg;
    subh->cb = sub_cb;
    
    ABT_mutex_lock(client->sub_mutex);
    subh->id = client->sub_serial++;
    register_client_sub(client, subh);
    ABT_mutex_unlock(client->sub_mutex);

    
    memset(odsc.bb.lb.c, 0, sizeof(uint64_t)*BBOX_MAX_NDIM);
    memset(odsc.bb.ub.c, 0, sizeof(uint64_t)*BBOX_MAX_NDIM);
    memcpy(odsc.bb.lb.c, lb, sizeof(uint64_t)*ndim);
    memcpy(odsc.bb.ub.c, ub, sizeof(uint64_t)*ndim);
    strncpy(odsc.name, var_name, strlen(var_name) + 1);
    
    in.odsc_gdim.size = sizeof(odsc);
    in.odsc_gdim.raw_odsc = (char *)(&odsc);
    in.param = subh->id;
   
    DEBUG_OUT("registered data subscription for %s with id %d\n", obj_desc_sprint(&odsc), subh->id);
 
    set_global_dimension(&(client->dcg->gdim_list), var_name, &(client->dcg->default_gdim), &od_gdim);
    in.odsc_gdim.gdim_size = sizeof(struct global_dimension);
    in.odsc_gdim.raw_gdim = (char *)(&od_gdim);
   
    get_server_address(client, &server_addr);

    hret = margo_create(client->mid, server_addr, client->sub_id, &handle);
    assert(hret == HG_SUCCESS && "margo_create succeeds");
    hret = margo_forward(handle, &in);
    assert(hret == HG_SUCCESS && "margo_forward succeeds");

    DEBUG_OUT("subscription %d sent.\n", subh->id); 
}

int dspaces_check_sub(dspaces_client_t client, dspaces_sub_t subh, int *result)
{
    
}

static void kill_rpc(hg_handle_t handle)
{
    margo_instance_id mid = margo_hg_handle_get_instance(handle);
    const struct hg_info* info = margo_get_info(handle);
    dspaces_client_t client = (dspaces_client_t)margo_registered_data(mid, info->id);

    DEBUG_OUT("Received kill message.\n");

    ABT_mutex_lock(client->drain_mutex);
    client->local_put_count = 0;
    ABT_cond_signal(client->drain_cond);
    ABT_mutex_unlock(client->drain_mutex);

    margo_destroy(handle);
}
DEFINE_MARGO_RPC_HANDLER(kill_rpc)

void dspaces_kill(dspaces_client_t client)
{
    uint32_t in;
    hg_addr_t server_addr;
    hg_handle_t h;
    hg_return_t hret;

    in = -1;

    DEBUG_OUT("sending kill signal to servers.\n");

    margo_addr_lookup(client->mid, client->server_address[0], &server_addr);
    hret = margo_create(client->mid, server_addr, client->kill_id, &h);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"ERROR: (%s): margo_create() failed\n", __func__);
        margo_addr_free(client->mid, server_addr);
        return;
    }
    margo_forward(h, &in);

    DEBUG_OUT("kill signal sent.\n");

    margo_addr_free(client->mid, server_addr);
    margo_destroy(h);
}
