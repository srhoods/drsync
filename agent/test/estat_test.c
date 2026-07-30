/* Unit test for the dev field on struct estat (agent.h) and its journal
 * encoding: NLINK_DUP records must carry the same (dev, ino) pair `stat(2)`
 * reports, closing the gap where the report's "(dev,ino)" duplication-cost
 * aggregation (ARCHITECTURE.md) previously had only ino to work with.
 * Build+run: `make estat-test` in agent/. Exits non-zero on failure.
 *
 * jrn_emit leaves a single record in ctx.jrn.raw without flushing (same
 * technique as fidelity_test.c) — no socket, no fleet. */
#include "../src/agent.h"
#include "../src/pb.h"

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

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

/* estat_of (the fstatat-fallback conversion path in uring.c) must capture
 * st_dev, not just st_ino — an inode number alone is only unique within one
 * filesystem, and src/dst roots can each span several. */
static void test_estat_of_captures_dev(void)
{
    char tmp[] = "/tmp/drsync-estat-test-XXXXXX";
    CHECK(mkdtemp(tmp) != NULL, "mkdtemp failed");
    char path[PATH_MAX];
    snprintf(path, sizeof path, "%s/f", tmp);
    int fd = open(path, O_CREAT | O_WRONLY, 0644);
    CHECK(fd >= 0, "open failed");
    close(fd);

    struct stat st;
    CHECK(stat(path, &st) == 0, "stat failed");

    struct estat e;
    memset(&e, 0, sizeof e);
    estat_of(&e, &st);

    CHECK(e.dev == (uint64_t)st.st_dev, "estat.dev = %llu, want %llu",
          (unsigned long long)e.dev, (unsigned long long)st.st_dev);
    CHECK(e.ino == (uint64_t)st.st_ino, "estat.ino = %llu, want %llu",
          (unsigned long long)e.ino, (unsigned long long)st.st_ino);

    unlink(path);
    rmdir(tmp);
}

/* Decode field `want_field` of the first nested StatInfo submessage found at
 * top-level field `container_field` inside buf[0..len). Returns the varint
 * value, or 0 if absent (fields are all non-negative counters/ids here). */
static uint64_t decode_nested_u64(const uint8_t *buf, size_t len,
                                   uint32_t container_field, uint32_t want_field)
{
    pb_cur c;
    pb_cur_init(&c, buf, len);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        if (f == container_field && wt == 2) {
            const uint8_t *sp;
            size_t sn;
            if (!pb_get_len(&c, &sp, &sn))
                continue;
            pb_cur sub;
            pb_cur_init(&sub, sp, sn);
            uint32_t sf;
            int swt;
            while (pb_next(&sub, &sf, &swt)) {
                if (sf == want_field)
                    return pb_get_varint(&sub);
                pb_skip(&sub, swt);
            }
            return 0;
        }
        pb_skip(&c, wt);
    }
    return 0;
}

/* End-to-end: jrn_emit's NLINK_DUP record carries the src StatInfo (field 4)
 * with dev (field 9) matching what the walker's stat call saw — the actual
 * consumer of this data is the coordinator's (dev,ino) dup-cost aggregation,
 * so the wire encoding is what matters, not just the in-memory struct. */
static void test_nlink_dup_record_carries_dev(void)
{
    struct shard_item it = { .shard_id = 7 };
    struct walk_ctx ctx = { .it = &it };
    jrn_init(&ctx);

    struct estat ss;
    memset(&ss, 0, sizeof ss);
    ss.mode = S_IFREG | 0644;
    ss.dev = 0x1234;
    ss.ino = 0x5678;
    ss.nlink = 3;
    ss.size = 4096;

    jrn_emit(&ctx, JR_NLINK_DUP, "projects/a/dup", &ss, NULL, 0, NULL);
    CHECK(ctx.jrn.count == 1, "journal record count = %u, want 1", ctx.jrn.count);

    pb_cur outer;
    pb_cur_init(&outer, ctx.jrn.raw.p, ctx.jrn.raw.len);
    const uint8_t *rec;
    size_t reclen;
    CHECK(pb_get_len(&outer, &rec, &reclen), "no framed record in journal buffer");

    uint64_t dev = 0, ino = 0;
    if (rec) {
        dev = decode_nested_u64(rec, reclen, /*src field*/ 4, /*dev field*/ 9);
        ino = decode_nested_u64(rec, reclen, /*src field*/ 4, /*ino field*/ 10);
    }
    CHECK(dev == ss.dev, "wire StatInfo.dev = %llu, want %llu",
          (unsigned long long)dev, (unsigned long long)ss.dev);
    CHECK(ino == ss.ino, "wire StatInfo.ino = %llu, want %llu",
          (unsigned long long)ino, (unsigned long long)ss.ino);
}

int main(void)
{
    test_estat_of_captures_dev();
    test_nlink_dup_record_carries_dev();

    if (failures) {
        fprintf(stderr, "%d estat test(s) failed\n", failures);
        return 1;
    }
    printf("all estat tests passed\n");
    return 0;
}
