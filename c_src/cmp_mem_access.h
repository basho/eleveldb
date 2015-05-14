/*
 * Memory buffer access functions for cmp (MessagePack)
 *
 */
#ifndef CMP_MEM_ACCESS_H
#define CMP_MEM_ACCESS_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include "cmp.h"

typedef struct {
    char *buf;
    size_t index;
    size_t size;
} cmp_mem_access_t;

#ifdef __cplusplus
extern "C" {
#endif

// initialize cmp
void cmp_mem_access_init(cmp_ctx_t *cmp, cmp_mem_access_t *m, void *buf, size_t size);

// initialize cmp (read only memory access)
void cmp_mem_access_ro_init(cmp_ctx_t *cmp, cmp_mem_access_t *m, const void *buf, size_t size);

// get current read/write position. this can be used to determine the length of
// the buffer written by MessagePack
size_t cmp_mem_access_get_pos(cmp_mem_access_t *m);

// set current read/write position.
// use cmp_mem_access_get_pos to obtain position.
void cmp_mem_access_set_pos(cmp_mem_access_t *m, size_t pos);

// get a pointer into the buffer at the position pos
void *cmp_mem_access_get_ptr_at_pos(cmp_mem_access_t *m, size_t pos);

// check if the position is inside the buffer
bool cmp_mem_access_pos_is_valid(cmp_mem_access_t *m, size_t pos);

#ifdef __cplusplus
}
#endif

#endif /* CMP_MEM_ACCESS_H */
