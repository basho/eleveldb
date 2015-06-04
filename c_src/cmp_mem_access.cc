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

#include <string.h>
#include "cmp_mem_access.h"

static bool cmp_mem_reader(struct cmp_ctx_s *ctx, void *data, size_t len)
{
    cmp_mem_access_t *mem = (cmp_mem_access_t*)ctx->buf;
    if (mem->index + len <= mem->size) {
        memcpy(data, &mem->buf[mem->index], len);
        mem->index += len;
        return true;
    } else {
        return false;
    }
}

static size_t cmp_mem_writer(struct cmp_ctx_s *ctx, const void *data, size_t len)
{
    cmp_mem_access_t *mem = (cmp_mem_access_t*)ctx->buf;
    if (mem->index + len <= mem->size) {
        memcpy(&mem->buf[mem->index], data, len);
        mem->index += len;
        return len;
    } else {
        return 0;
    }
}

static size_t cmp_mem_writer_ro(struct cmp_ctx_s *ctx, const void *data, size_t len)
{
    (void)ctx;
    (void)data;
    (void)len;
    return 0;
}

void cmp_mem_access_init(cmp_ctx_t *cmp, cmp_mem_access_t *m, void *buf, size_t size)
{
    m->buf = (char*)buf;
    m->size = size;
    m->index = 0;
    cmp_init(cmp, m, cmp_mem_reader, cmp_mem_writer);
}

void cmp_mem_access_ro_init(cmp_ctx_t *cmp, cmp_mem_access_t *m, const void *buf, size_t size)
{
    m->buf = (char*)buf;
    m->size = size;
    m->index = 0;
    cmp_init(cmp, m, cmp_mem_reader, cmp_mem_writer_ro);
}

size_t cmp_mem_access_get_pos(cmp_mem_access_t *m)
{
    return m->index;
}

void cmp_mem_access_set_pos(cmp_mem_access_t *m, size_t pos)
{
    m->index = pos;
}

void *cmp_mem_access_get_ptr_at_pos(cmp_mem_access_t *m, size_t pos)
{
    return m->buf + pos;
}

bool cmp_mem_access_pos_is_valid(cmp_mem_access_t *m, size_t pos)
{
    if (pos >= m->size) {
        return false;
    } else {
        return true;
    }
}
