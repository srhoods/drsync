/* Unit test for walk_fidelity: an unpreservable attribute must be journalled as
 * a JR_FIDELITY_EXCEPTION (not just counted), carrying the attribute name and
 * errno, and must leave errno untouched. Build+run: `make fidelity-test` in
 * agent/. Exits non-zero on failure.
 *
 * jrn_emit leaves a single record in ctx.jrn.raw without flushing (the flush
 * threshold is 1 MiB), so we decode that buffer directly — no socket, no fleet.
 *
 * The agent objects it links (jrn.o, xattr.o) reference a few symbols only used
 * on the flush/error paths this test never takes; they are stubbed below. */
#include "../src/agent.h"
#include "../src/pb.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/xattr.h>

/* ---- stubs for symbols on paths this test does not exercise ---- */
void log_line(const char *level, const char *fmt, ...)
{
    (void)level;
    (void)fmt;
}
void out_push(uint16_t type, pb_buf *b)
{
    (void)type;
    (void)b;
}
void walk_err(struct walk_ctx *ctx, const char *what, const char *path)
{
    (void)ctx;
    (void)what;
    (void)path;
}

static int failures;
#define CHECK(cond, ...)                       \
    do {                                       \
        if (!(cond)) {                         \
            fprintf(stderr, "FAIL: ");         \
            fprintf(stderr, __VA_ARGS__);      \
            fprintf(stderr, "\n");             \
            failures++;                        \
        }                                      \
    } while (0)

int main(void)
{
    struct shard_item it = { .shard_id = 42 };
    struct walk_ctx ctx = { .it = &it };
    jrn_init(&ctx);

    errno = EOPNOTSUPP;
    walk_fidelity(&ctx, "system.nfs4_acl", "projects/a/file");

    /* errno preserved for the caller, and the exception counted. */
    CHECK(errno == EOPNOTSUPP, "walk_fidelity clobbered errno: %d", errno);
    CHECK(ctx.c.fidelity_exceptions == 1, "fidelity_exceptions = %llu, want 1",
          (unsigned long long)ctx.c.fidelity_exceptions);
    CHECK(ctx.jrn.count == 1, "journal record count = %u, want 1", ctx.jrn.count);

    /* Decode the one length-delimited JournalRecord sitting in ctx.jrn.raw. */
    pb_cur outer;
    pb_cur_init(&outer, ctx.jrn.raw.p, ctx.jrn.raw.len);
    const uint8_t *rec;
    size_t reclen;
    CHECK(pb_get_len(&outer, &rec, &reclen), "no framed record in journal buffer");

    int type = 0, err_no = -1;
    char relpath[256] = "", detail[256] = "";
    if (rec) {
        pb_cur c;
        pb_cur_init(&c, rec, reclen);
        uint32_t f;
        int wt;
        while (pb_next(&c, &f, &wt)) {
            const uint8_t *sp;
            size_t sn;
            switch (f) {
            case 1: type = (int)pb_get_varint(&c); break;
            case 2:
                if (pb_get_len(&c, &sp, &sn) && sn < sizeof relpath) {
                    memcpy(relpath, sp, sn);
                    relpath[sn] = '\0';
                }
                break;
            case 8: err_no = (int)pb_get_varint(&c); break;
            case 9:
                if (pb_get_len(&c, &sp, &sn) && sn < sizeof detail) {
                    memcpy(detail, sp, sn);
                    detail[sn] = '\0';
                }
                break;
            default: pb_skip(&c, wt);
            }
        }
    }

    CHECK(type == JR_FIDELITY_EXCEPTION, "record type = %d, want %d (JR_FIDELITY_EXCEPTION)",
          type, JR_FIDELITY_EXCEPTION);
    CHECK(strcmp(relpath, "projects/a/file") == 0, "rel_path = \"%s\", want projects/a/file",
          relpath);
    CHECK(strcmp(detail, "system.nfs4_acl") == 0, "detail = \"%s\", want system.nfs4_acl",
          detail);
    CHECK(err_no == EOPNOTSUPP, "errno field = %d, want %d", err_no, EOPNOTSUPP);

    if (failures) {
        fprintf(stderr, "%d fidelity test(s) failed\n", failures);
        return 1;
    }
    printf("all fidelity tests passed\n");
    return 0;
}
