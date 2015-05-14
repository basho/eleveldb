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
