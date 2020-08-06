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
*  Ciprian Docan (2009)  TASSL Rutgers University
*  docan@cac.rutgers.edu
*/

#ifndef __QUEUE_H_
#define __QUEUE_H_

#include <stdlib.h>

struct queue_node {
        struct queue_node       *next;
        void                    *obj;
};

struct queue {
        int                     num_elem;
        struct queue_node       *head;
        struct queue_node       *tail;
};

static void queue_init(struct queue *q)
{
        q->num_elem = 0;
        q->head = NULL;
        q->tail = NULL;
}

static void queue_enqueue(struct queue *q, void *obj)
{
        struct queue_node *qn;

        qn = malloc(sizeof(struct queue_node));
        qn->obj = obj;
        qn->next = NULL;

        if (q->num_elem == 0)
                q->head = q->tail = qn;
        else {
                q->tail->next = qn;
                q->tail = qn;
        }
        q->num_elem++;
}

static void * queue_dequeue(struct queue *q)
{
        struct queue_node *qn = q->head;
        void *obj;

        if (q->num_elem == 0)
                return NULL;

        obj = qn->obj;
        q->head = qn->next;
        free(qn);

        q->num_elem--;
        if (q->num_elem == 0)
                q->tail = NULL;

        return obj;
}

static inline int queue_is_empty(struct queue *q)
{
        return (q->num_elem == 0);
}

static inline int queue_size(struct queue *q)
{
        return q->num_elem;
}

#endif /* __QUEUE_H_ */
