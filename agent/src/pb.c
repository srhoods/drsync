#include "pb.h"

#include <stdlib.h>
#include <string.h>

/* ---- encoder ---- */

void pb_init(pb_buf *b)
{
    b->p = NULL;
    b->len = b->cap = 0;
    b->oom = false;
}

void pb_free(pb_buf *b)
{
    free(b->p);
    pb_init(b);
}

static bool ensure(pb_buf *b, size_t extra)
{
    if (b->oom)
        return false;
    if (b->len + extra <= b->cap)
        return true;
    size_t cap = b->cap ? b->cap * 2 : 256;
    while (cap < b->len + extra)
        cap *= 2;
    uint8_t *np = realloc(b->p, cap);
    if (!np) {
        b->oom = true;
        return false;
    }
    b->p = np;
    b->cap = cap;
    return true;
}

void pb_put_varint(pb_buf *b, uint64_t v)
{
    if (!ensure(b, 10))
        return;
    do {
        uint8_t byte = v & 0x7f;
        v >>= 7;
        if (v)
            byte |= 0x80;
        b->p[b->len++] = byte;
    } while (v);
}

void pb_put_tag(pb_buf *b, uint32_t field, int wt)
{
    pb_put_varint(b, ((uint64_t)field << 3) | (uint32_t)wt);
}

void pb_put_u64(pb_buf *b, uint32_t field, uint64_t v)
{
    if (!v)
        return;
    pb_put_tag(b, field, PB_WT_VARINT);
    pb_put_varint(b, v);
}

void pb_put_i64(pb_buf *b, uint32_t field, int64_t v)
{
    pb_put_u64(b, field, (uint64_t)v);
}

void pb_put_bool(pb_buf *b, uint32_t field, bool v)
{
    if (v)
        pb_put_u64(b, field, 1);
}

void pb_append(pb_buf *b, const void *p, size_t n)
{
    if (!n || !ensure(b, n))
        return;
    memcpy(b->p + b->len, p, n);
    b->len += n;
}

void pb_put_bytes(pb_buf *b, uint32_t field, const void *p, size_t n)
{
    if (!n)
        return;
    pb_put_tag(b, field, PB_WT_LEN);
    pb_put_varint(b, n);
    if (!ensure(b, n))
        return;
    memcpy(b->p + b->len, p, n);
    b->len += n;
}

void pb_put_str(pb_buf *b, uint32_t field, const char *s)
{
    if (s && *s)
        pb_put_bytes(b, field, s, strlen(s));
}

void pb_put_msg(pb_buf *b, uint32_t field, const pb_buf *sub)
{
    if (sub->oom) {
        b->oom = true;
        return;
    }
    /* proto3 keeps empty submessages distinguishable from absent; the drsync
     * protocol never relies on that, so empty submessages are omitted. */
    if (!sub->len)
        return;
    pb_put_bytes(b, field, sub->p, sub->len);
}

/* ---- decoder ---- */

bool pb_next(pb_cur *c, uint32_t *field, int *wt)
{
    if (pb_done(c))
        return false;
    uint64_t key = pb_get_varint(c);
    if (c->err)
        return false;
    *field = (uint32_t)(key >> 3);
    *wt = (int)(key & 7);
    if (*field == 0) {
        c->err = true;
        return false;
    }
    return true;
}

uint64_t pb_get_varint(pb_cur *c)
{
    uint64_t v = 0;
    int shift = 0;
    while (c->p < c->end && shift < 64) {
        uint8_t byte = *c->p++;
        v |= (uint64_t)(byte & 0x7f) << shift;
        if (!(byte & 0x80))
            return v;
        shift += 7;
    }
    c->err = true;
    return 0;
}

bool pb_get_len(pb_cur *c, const uint8_t **p, size_t *n)
{
    uint64_t len = pb_get_varint(c);
    if (c->err || len > (uint64_t)(c->end - c->p)) {
        c->err = true;
        return false;
    }
    *p = c->p;
    *n = (size_t)len;
    c->p += len;
    return true;
}

void pb_get_strn(pb_cur *c, char *dst, size_t cap)
{
    const uint8_t *p;
    size_t n;
    dst[0] = '\0';
    if (!pb_get_len(c, &p, &n))
        return;
    if (n >= cap)
        n = cap - 1; /* truncate; callers size buffers to protocol limits */
    memcpy(dst, p, n);
    dst[n] = '\0';
}

void pb_skip(pb_cur *c, int wt)
{
    const uint8_t *p;
    size_t n;
    switch (wt) {
    case PB_WT_VARINT:
        pb_get_varint(c);
        break;
    case PB_WT_I64:
        if (c->end - c->p < 8)
            c->err = true;
        else
            c->p += 8;
        break;
    case PB_WT_LEN:
        pb_get_len(c, &p, &n);
        break;
    case PB_WT_I32:
        if (c->end - c->p < 4)
            c->err = true;
        else
            c->p += 4;
        break;
    default:
        c->err = true;
    }
}
