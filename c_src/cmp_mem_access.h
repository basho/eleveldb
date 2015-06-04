/*
 * Memory buffer access functions for cmp (MessagePack)
 *
 * https://github.com/Stapelzeiger/cmp_mem_access
 *
 * The MIT License (MIT)

 * Copyright (c) 2015 Patrick Spieler
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
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
