/* Unit test for do_linkfix (src/link.c): the core linkat logic behind the
 * LINKFIX phase (docs/DESIGN-hardlinks.md §3.3) — anchor gen re-check,
 * linkat, and the EEXIST retry (already-linked idempotency vs. stale-entry
 * replace). Build+run: `make link-test` in agent/. Exits non-zero on failure.
 *
 * jrn_emit leaves records in ctx.jrn.raw without flushing (same technique as
 * fidelity_test.c) — no socket, no fleet. Real filesystem ops in a tmpdir. */
#include "../src/agent.h"
#include "../src/pb.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* ---- stub for the one symbol on a path this test does not exercise ----
 * out_push/log_line/opts_get/lease_remove come from state.o (linked below):
 * do_linkfix never reaches them (only process_linkfix, which this test never
 * calls, does), so state.c's real implementations link cleanly unused. */
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

static int last_journal_type(struct walk_ctx *ctx)
{
    if (!ctx->jrn.count)
        return -1;
    pb_cur outer;
    pb_cur_init(&outer, ctx->jrn.raw.p, ctx->jrn.raw.len);
    const uint8_t *rec = NULL;
    size_t reclen = 0;
    const uint8_t *last = NULL;
    size_t lastlen = 0;
    while (pb_get_len(&outer, &rec, &reclen)) {
        last = rec;
        lastlen = reclen;
    }
    if (!last)
        return -1;
    pb_cur c;
    pb_cur_init(&c, last, lastlen);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        if (f == 1)
            return (int)pb_get_varint(&c);
        pb_skip(&c, wt);
    }
    return -1;
}

static void write_file(int dfd, const char *name, const char *content)
{
    int fd = openat(dfd, name, O_CREAT | O_WRONLY, 0644);
    CHECK(fd >= 0, "openat %s failed: %s", name, strerror(errno));
    if (fd >= 0) {
        write(fd, content, strlen(content));
        close(fd);
    }
}

/* Happy path: anchor exists with matching gen, member does not exist yet ->
 * linkat succeeds, RES_OK, links_created counted, JR_LINK_CREATED journaled. */
static void test_happy_path(void)
{
    char tmp[] = "/tmp/drsync-link-test-XXXXXX";
    CHECK(mkdtemp(tmp) != NULL, "mkdtemp failed");
    int dst_fd = open(tmp, O_RDONLY | O_DIRECTORY);
    CHECK(dst_fd >= 0, "open dst root failed");

    write_file(dst_fd, "anchor", "hello");
    struct stat ast;
    fstatat(dst_fd, "anchor", &ast, AT_SYMLINK_NOFOLLOW);

    struct shard_item it = { .shard_id = 1 };
    struct walk_ctx ctx = { .it = &it };
    jrn_init(&ctx);

    struct linkfix lf = {
        .anchor_rel = (char *)"anchor", .member_rel = (char *)"member",
        .gen_size = (uint64_t)ast.st_size,
        .gen_mtime_ns = (int64_t)ast.st_mtim.tv_sec * 1000000000 + ast.st_mtim.tv_nsec,
    };
    int status = do_linkfix(&ctx, dst_fd, &lf);
    CHECK(status == RES_OK, "status = %d, want RES_OK", status);
    CHECK(ctx.c.links_created == 1, "links_created = %llu, want 1",
          (unsigned long long)ctx.c.links_created);
    CHECK(last_journal_type(&ctx) == JR_LINK_CREATED, "journal type = %d, want JR_LINK_CREATED",
          last_journal_type(&ctx));

    struct stat mst;
    CHECK(fstatat(dst_fd, "member", &mst, AT_SYMLINK_NOFOLLOW) == 0, "member not created");
    CHECK(mst.st_ino == ast.st_ino, "member ino %llu != anchor ino %llu",
          (unsigned long long)mst.st_ino, (unsigned long long)ast.st_ino);
    CHECK(mst.st_nlink == 2, "nlink = %d, want 2", (int)mst.st_nlink);

    close(dst_fd);
}

/* Anchor drifted since it was copied (size mismatch): abort with
 * RES_SRC_CHANGED, no link created, no destination entry touched. */
static void test_anchor_drift_aborts(void)
{
    char tmp[] = "/tmp/drsync-link-test-XXXXXX";
    CHECK(mkdtemp(tmp) != NULL, "mkdtemp failed");
    int dst_fd = open(tmp, O_RDONLY | O_DIRECTORY);
    CHECK(dst_fd >= 0, "open dst root failed");

    write_file(dst_fd, "anchor", "hello");

    struct shard_item it = { .shard_id = 2 };
    struct walk_ctx ctx = { .it = &it };
    jrn_init(&ctx);

    struct linkfix lf = {
        .anchor_rel = (char *)"anchor", .member_rel = (char *)"member",
        .gen_size = 99999, /* wrong on purpose */
        .gen_mtime_ns = 1,
    };
    int status = do_linkfix(&ctx, dst_fd, &lf);
    CHECK(status == RES_SRC_CHANGED, "status = %d, want RES_SRC_CHANGED", status);
    CHECK(ctx.c.links_created == 0, "links_created = %llu, want 0",
          (unsigned long long)ctx.c.links_created);
    struct stat mst;
    CHECK(fstatat(dst_fd, "member", &mst, AT_SYMLINK_NOFOLLOW) < 0 && errno == ENOENT,
          "member should not exist after aborted link");

    close(dst_fd);
}

/* EEXIST + already the same inode (a prior run of this shard already
 * succeeded): idempotent, reports RES_OK and still counts/journals the link,
 * does not touch the existing correct entry. */
static void test_eexist_already_linked_is_idempotent(void)
{
    char tmp[] = "/tmp/drsync-link-test-XXXXXX";
    CHECK(mkdtemp(tmp) != NULL, "mkdtemp failed");
    int dst_fd = open(tmp, O_RDONLY | O_DIRECTORY);
    CHECK(dst_fd >= 0, "open dst root failed");

    write_file(dst_fd, "anchor", "hello");
    CHECK(linkat(dst_fd, "anchor", dst_fd, "member", 0) == 0, "pre-link setup failed");
    struct stat ast;
    fstatat(dst_fd, "anchor", &ast, AT_SYMLINK_NOFOLLOW);

    struct shard_item it = { .shard_id = 3 };
    struct walk_ctx ctx = { .it = &it };
    jrn_init(&ctx);

    struct linkfix lf = {
        .anchor_rel = (char *)"anchor", .member_rel = (char *)"member",
        .gen_size = (uint64_t)ast.st_size,
        .gen_mtime_ns = (int64_t)ast.st_mtim.tv_sec * 1000000000 + ast.st_mtim.tv_nsec,
    };
    int status = do_linkfix(&ctx, dst_fd, &lf);
    CHECK(status == RES_OK, "status = %d, want RES_OK (idempotent re-run)", status);
    CHECK(ctx.c.links_created == 0, "links_created = %llu, want 0 (already converged, not a new link)",
          (unsigned long long)ctx.c.links_created);
    CHECK(last_journal_type(&ctx) == -1, "journal should stay empty on an idempotent no-op re-run");

    struct stat mst;
    CHECK(fstatat(dst_fd, "member", &mst, AT_SYMLINK_NOFOLLOW) == 0, "member vanished");
    CHECK(mst.st_ino == ast.st_ino, "member ino changed across idempotent re-run");

    close(dst_fd);
}

/* EEXIST + a DIFFERENT inode at the member's name (stale entry from an
 * earlier pass, or a name collision): replaced, ending up correctly linked
 * to the anchor — the walker's "replace on mismatch" pattern. */
static void test_eexist_stale_entry_is_replaced(void)
{
    char tmp[] = "/tmp/drsync-link-test-XXXXXX";
    CHECK(mkdtemp(tmp) != NULL, "mkdtemp failed");
    int dst_fd = open(tmp, O_RDONLY | O_DIRECTORY);
    CHECK(dst_fd >= 0, "open dst root failed");

    write_file(dst_fd, "anchor", "hello");
    write_file(dst_fd, "member", "stale, unrelated content");
    struct stat ast;
    fstatat(dst_fd, "anchor", &ast, AT_SYMLINK_NOFOLLOW);

    struct shard_item it = { .shard_id = 4 };
    struct walk_ctx ctx = { .it = &it };
    jrn_init(&ctx);

    struct linkfix lf = {
        .anchor_rel = (char *)"anchor", .member_rel = (char *)"member",
        .gen_size = (uint64_t)ast.st_size,
        .gen_mtime_ns = (int64_t)ast.st_mtim.tv_sec * 1000000000 + ast.st_mtim.tv_nsec,
    };
    int status = do_linkfix(&ctx, dst_fd, &lf);
    CHECK(status == RES_OK, "status = %d, want RES_OK (stale entry replaced)", status);

    struct stat mst;
    CHECK(fstatat(dst_fd, "member", &mst, AT_SYMLINK_NOFOLLOW) == 0, "member missing after replace");
    CHECK(mst.st_ino == ast.st_ino, "member not linked to anchor after replacing stale entry");

    close(dst_fd);
}

int main(void)
{
    test_happy_path();
    test_anchor_drift_aborts();
    test_eexist_already_linked_is_idempotent();
    test_eexist_stale_entry_is_replaced();

    if (failures) {
        fprintf(stderr, "%d link test(s) failed\n", failures);
        return 1;
    }
    printf("all link tests passed\n");
    return 0;
}
