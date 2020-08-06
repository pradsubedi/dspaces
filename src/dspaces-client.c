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
#include <assert.h>
#include <inttypes.h>
#include "ss_data.h"
#include "dspaces-client.h"
#include "gspace.h"

static enum storage_type st = column_major;

struct dspaces_client {
    margo_instance_id mid;
    hg_id_t put_id;
    hg_id_t get_id;
    hg_id_t query_id;
    hg_id_t ss_id;
    struct dc_gspace *dcg;
    ssg_group_id_t gid;
    
};

//dynamic load balancing based on the number of clients
//round robin fashion
//based on how many clients are currently connected to each server
static hg_addr_t get_server_address(dspaces_client_t client){

    int self_rank = ssg_get_group_self_rank(client->gid);
    
    int peer_id = self_rank % client->dcg->size_sp;
    fprintf(stderr, "Self rank is %d, with peer_id %d\n", self_rank, peer_id);
    hg_addr_t server_addr = ssg_get_group_member_addr(client->gid, client->dcg->srv_ids[peer_id]);
    for (int i = 0; i < client->dcg->size_sp; ++i)
    {
        fprintf(stderr, "server addr %d is %lu \n", i, client->dcg->srv_ids[i]);
    }

    char *my_addr_str = NULL;
    hg_size_t my_addr_size;
    margo_addr_to_string(client->mid, NULL, &my_addr_size, server_addr);
    my_addr_str = malloc(my_addr_size);
    margo_addr_to_string(client->mid, my_addr_str, &my_addr_size, server_addr);
    fprintf(stderr, "server_address being connected is %s\n", my_addr_str);

    return server_addr;

}


static int get_ss_info(dspaces_client_t client){
    hg_return_t hret;
    hg_handle_t handle;
    ss_information out;
    int ret = dspaces_SUCCESS;
    hg_size_t my_addr_size;

    char *my_addr_str = NULL;

    hg_addr_t server_addr = get_server_address(client);
    margo_addr_to_string(client->mid, NULL, &my_addr_size, server_addr);
    my_addr_str = malloc(my_addr_size);
    margo_addr_to_string(client->mid, my_addr_str, &my_addr_size, server_addr);

    fprintf(stderr, "server_address to send ss_info rpc is %s\n", my_addr_str);
    /* create handle */
    hret = margo_create(client->mid, server_addr, client->ss_id, &handle);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"[dspaces] margo_create() failed in ss_get_info()\n");
        return dspaces_ERR_MERCURY;
    }

    hret = margo_forward(handle, NULL);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"[dspaces] margo_forward() failed in ss_get_info() \n");
        margo_destroy(handle);
        return dspaces_ERR_MERCURY;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"[dspaces] margo_get_output() failed in ss_get_info()\n");
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
        dcg_l->hash_version = ssd_hash_version_v1; // set default hash version
        

        //now read from file to update size_sp and srv_ids

        char* file_name = "servids.0";
        int fd = open(file_name, O_RDONLY);
        struct stat st;
        if (fd == -1)
        {
            fprintf(stderr, "Error: Unable to open config file %s for server_address list\n",
                file_name);
            goto err_out;
        }
        /* get file size and allocate a buffer to store it */
        int ret = fstat(fd, &st);
        if (ret == -1)
        {
            fprintf(stderr, "Error: Unable to stat config file %s for server_address list\n",
                file_name);
            goto err_out;
        }
        dcg_l->srv_ids = malloc(st.st_size);
        if (dcg_l->srv_ids == NULL) goto err_out;

        /* load it all in one fell swoop */
        int rd_buf_size = read(fd, dcg_l->srv_ids, st.st_size);
        if (rd_buf_size != st.st_size)
        {
            fprintf(stderr, "Error: Unable to stat config file %s for server_address list\n",
                file_name);
            goto err_out;
        }
        close(fd);
        dcg_l->size_sp = rd_buf_size/sizeof(ssg_member_id_t);
        fprintf(stderr, "Size of server is %d\n", dcg_l->size_sp);


        return dcg_l;
 err_out:
        fprintf(stderr, "'%s()': failed.\n", __func__);
        return NULL;
}

int client_init(char *listen_addr_str, dspaces_client_t* c)
{   
    int ret = ssg_init();
    assert(ret == SSG_SUCCESS);
    
    dspaces_client_t client = (dspaces_client_t)calloc(1, sizeof(*client));
    if(!client) return dspaces_ERR_ALLOCATION;

    client->mid = margo_init(listen_addr_str, MARGO_SERVER_MODE, 1, 0);
    assert(client->mid);
   
    ssg_group_id_t gid;

    //join the server ssg_group
    int num_addrs = 1;
    ret = ssg_group_id_load("dspaces.ssg", &num_addrs, &gid);
    assert(ret == SSG_SUCCESS);

    ret = ssg_group_join(client->mid, gid, NULL, NULL);
    assert(ret == SSG_SUCCESS);

    client->gid = gid;

    /* check if RPCs have already been registered */
    hg_bool_t flag;
    hg_id_t id;
    margo_registered_name(client->mid, "put_rpc", &id, &flag);

    if(flag == HG_TRUE) { /* RPCs already registered */
        margo_registered_name(client->mid, "put_rpc",                   &client->put_id,                   &flag);
        margo_registered_name(client->mid, "get_rpc",                   &client->get_id,                   &flag);
        margo_registered_name(client->mid, "query_rpc",                   &client->query_id,                   &flag);
        margo_registered_name(client->mid, "ss_rpc",                   &client->ss_id,                   &flag);
   
    } else {

        client->put_id =
            MARGO_REGISTER(client->mid, "put_rpc", bulk_gdim_t, bulk_out_t, NULL);
        client->get_id =
            MARGO_REGISTER(client->mid, "get_rpc", bulk_in_t, bulk_out_t, NULL);
        client->query_id =
            MARGO_REGISTER(client->mid, "query_rpc", odsc_gdim_t, odsc_list_t, NULL);
        client->ss_id =
            MARGO_REGISTER(client->mid, "ss_rpc", void, ss_information, NULL);

    }
    //now do dcg_alloc and store gid

    client->dcg = dcg_alloc(client);
    assert(ret == 0);

    if(!(client->dcg))
        return dspaces_ERR_ALLOCATION;

    get_ss_info(client);

    *c = client;

    return dspaces_SUCCESS;
}


int client_finalize(dspaces_client_t client)
{

    int ret = ssg_group_leave(client->gid);
    assert(ret == SSG_SUCCESS);

    free_gdim_list(&client->dcg->gdim_list);
    free(client->dcg->srv_ids);
    free(client->dcg);


    margo_finalize(client->mid);

    free(client);

    ret = ssg_finalize();
    assert(ret == SSG_SUCCESS);

    return dspaces_SUCCESS;
}

void dspaces_define_gdim (dspaces_client_t client, 
    const char *var_name, int ndim, uint64_t *gdim){
    if(ndim > BBOX_MAX_NDIM){
        fprintf(stderr, "The maxium dimentions supported is only %d\n", BBOX_MAX_NDIM);
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
	hg_return_t hret;
    int ret = dspaces_SUCCESS;
    hg_handle_t handle;

    obj_descriptor odsc = {
            .version = ver, .owner = 0, 
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

    fprintf(stderr, "sending object %s \n", obj_desc_sprint(&odsc));

    hret = margo_bulk_create(client->mid, 1, (void**)&data, &rdma_size,
                            HG_BULK_READ_ONLY, &in.handle);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"[dspaces] margo_bulk_create() failed in dspaces_put()\n");
        return dspaces_ERR_MERCURY;
    }
    
    hg_addr_t server_addr = get_server_address(client);
    /* create handle */
    hret = margo_create( client->mid,
            server_addr,
            client->put_id,
            &handle);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"[dspaces] margo_create() failed in dspaces_put()\n");
        margo_bulk_free(in.handle);
        return dspaces_ERR_MERCURY;
    }

    hret = margo_forward(handle, &in);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"[dspaces] margo_forward() failed in dspaces_put() \n");
        margo_bulk_free(in.handle);
        margo_destroy(handle);
        return dspaces_ERR_MERCURY;
    }

    hret = margo_get_output(handle, &out);
    if(hret != HG_SUCCESS) {
        fprintf(stderr,"[dspaces] margo_get_output() failed in dspaces_put()\n");
        margo_bulk_free(in.handle);
        margo_destroy(handle);
        return dspaces_ERR_MERCURY;
    }

    ret = out.ret;
    margo_free_output(handle, &out);
    margo_bulk_free(in.handle);

    margo_destroy(handle);
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
        server_addr = ssg_get_group_member_addr(client->gid, odsc_tab[i].owner);

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
    }

    struct obj_data *return_od = obj_data_alloc_no_data(&req_obj, data);

    for (int i = 0; i < num_odscs; ++i){
        margo_wait(serv_req[i]);
        bulk_out_t resp;
        margo_get_output(hndl[i], &resp);
        margo_free_output(hndl[i], &resp);
        margo_destroy(hndl[i]);

        ssd_copy(return_od, od[i]);
        obj_data_free(od[i]);
    }
    free(hndl);
    free(serv_req);
    free(in);
    free(return_od);

}

int dspaces_get (dspaces_client_t client,
        const char *var_name,
        unsigned int ver, int elem_size,
        int ndim, uint64_t *lb, uint64_t *ub, 
        void *data)
{
    hg_return_t hret;
    int ret = dspaces_SUCCESS;
    hg_handle_t handle;

    obj_descriptor odsc = {
            .version = ver, .owner = 0, 
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

    struct global_dimension od_gdim;

    set_global_dimension(&(client->dcg->gdim_list), var_name, &(client->dcg->default_gdim),
                         &od_gdim);

    in.odsc_gdim.gdim_size = sizeof(struct global_dimension);
    in.odsc_gdim.raw_gdim = (char*)(&od_gdim);

    hg_addr_t server_addr = get_server_address(client);

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


    fprintf(stderr, "Finished query\n");
    for (int i = 0; i < num_odscs; ++i)
    {
        fprintf(stderr, "%s\n", obj_desc_sprint(&odsc_tab[i]));
    }

    //send request to get the obj_desc
    //alloc data for each obj_descriptor



    get_data(client, num_odscs, odsc, odsc_tab, data);



    //obj_assemble

}
