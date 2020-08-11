/*
 * Copyright (c) 2009, NSF Cloud and Autonomic Computing Center, Rutgers University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and
 * the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided with the distribution.
 * - Neither the name of the NSF Cloud and Autonomic Computing Center, Rutgers University, nor the names of its
 * contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

/*
 *  Pradeep Subedi (2020)  RDI2 Rutgers University
 *  pradeep.subedi@rutgers.edu
 */

#ifndef __GSPACE_H_
#define __GSPACE_H_

#include <ssg.h>
#include "ss_data.h"


struct ds_gspace {

        /* Shared space data structure. */
        struct sspace           *ssd;
        /* Local in-memory storage. */
        ss_storage       *ls;
        /* Default global data domain dimension */
        struct global_dimension default_gdim;

        /* List of dynamically added shared space. */ 
        struct list_head        sspace_list;

        /* Pending object descriptors request list. */
        //struct list_head        obj_desc_req_list;

        /* Pending object data request list. */
        //struct list_head        obj_data_req_list;

        /* List of allocated locks. */
        //struct list_head        locks_list;

        int rank;
        int size_sp;
        ssg_group_id_t gid;
        ssg_group_id_t *srv_ids;
        
};

/* Shared space info. */
struct ss_info {
        int                     num_dims;
        int                     num_space_srv;
};

struct dc_gspace {
        struct ss_info          ss_info;
        struct bbox             ss_domain;
        struct global_dimension default_gdim;

        /* List of 'struct dcg_lock' */
        struct list_head        locks_list;
        /* List of 'struct gdim_list_entry' */
        struct list_head        gdim_list;

        enum sspace_hash_version    hash_version;
        int    max_versions; 
        int size_sp;
        ssg_member_id_t *srv_ids;
        //for dimes like client storage
        ss_storage       *ls;
        
};


#endif /* __DS_GSPACE_H_ */
