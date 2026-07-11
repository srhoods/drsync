/* Minimal protobuf wire-format codec (encode + decode) for the drsync agent.
 *
 * Field numbers are pinned to proto/drsync.proto, which remains the contract;
 * this hand-rolled codec exists because the agent has no protobuf-c dependency
 * yet. msgs.c is the only file that knows field numbers. Unknown fields are
 * skipped on decode (forward compatibility). */
#ifndef DRSYNC_PB_H
#define DRSYNC_PB_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* wire types */
#define PB_WT_VARINT 0
#define PB_WT_I64    1
#define PB_WT_LEN    2
#define PB_WT_I32    5

/* ---- encoder: growable buffer ---- */
typedef struct {
    uint8_t *p;
    size_t   len;
    size_t   cap;
    bool     oom;
} pb_buf;

void pb_init(pb_buf *b);
void pb_free(pb_buf *b);
void pb_put_varint(pb_buf *b, uint64_t v);
void pb_put_tag(pb_buf *b, uint32_t field, int wt);
/* proto3 scalar fields: zero values are omitted */
void pb_put_u64(pb_buf *b, uint32_t field, uint64_t v);
void pb_put_i64(pb_buf *b, uint32_t field, int64_t v); /* two's complement, not zigzag */
void pb_put_bool(pb_buf *b, uint32_t field, bool v);
void pb_put_str(pb_buf *b, uint32_t field, const char *s);
void pb_put_bytes(pb_buf *b, uint32_t field, const void *p, size_t n);
/* raw append (no tag/length): for building length-delimited record streams */
void pb_append(pb_buf *b, const void *p, size_t n);
/* embed sub as a length-delimited submessage */
void pb_put_msg(pb_buf *b, uint32_t field, const pb_buf *sub);

/* ---- decoder: cursor ---- */
typedef struct {
    const uint8_t *p;
    const uint8_t *end;
    bool           err;
} pb_cur;

static inline void pb_cur_init(pb_cur *c, const void *p, size_t n)
{
    c->p = (const uint8_t *)p;
    c->end = c->p + n;
    c->err = false;
}
static inline bool pb_done(const pb_cur *c) { return c->err || c->p >= c->end; }

/* Reads the next tag; returns false at end of buffer or on error. */
bool pb_next(pb_cur *c, uint32_t *field, int *wt);
uint64_t pb_get_varint(pb_cur *c);
/* For PB_WT_LEN fields: returns pointer+len into the parent buffer. */
bool pb_get_len(pb_cur *c, const uint8_t **p, size_t *n);
/* Copies a PB_WT_LEN field into dst (NUL-terminated, truncating). */
void pb_get_strn(pb_cur *c, char *dst, size_t cap);
void pb_skip(pb_cur *c, int wt);

#endif
