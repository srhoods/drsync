/* Verify executor (decision D4, docs/DESIGN-agent.md §6).
 * A WI_VERIFY item lists entries this pass copied or meta-fixed; every entry
 * gets a metadata re-check (type/size/mtime/owner/mode/xattrs per job
 * options), and flagged entries are re-read on BOTH sides with XXH3-128
 * compared. This audits the write path end-to-end, including the on-disk
 * bytes the copy engines produced.
 * Mismatch → JR_VERIFY_FAIL + verify_fail counter, and under
 * on_mismatch=recopy the file is re-copied inline (journaled JR_COPIED).
 * Detecting later bit rot needs the full-checksum mode (roadmap); this pass
 * verifies what drsync just wrote. */
#include "agent.h"

#define XXH_INLINE_ALL
#include "xxhash.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define VBUF_SIZE (1 << 20)

static int64_t ts_diff_ns(const struct timespec *a, const struct timespec *b)
{
    int64_t d = ((int64_t)a->tv_sec - b->tv_sec) * 1000000000 +
                (a->tv_nsec - b->tv_nsec);
    return d < 0 ? -d : d;
}

static bool hash_file(struct walk_ctx *ctx, int root_fd, const char *rel,
                      uint8_t *buf, XXH128_hash_t *out)
{
    int fd = open_beneath(root_fd, rel, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) {
        walk_err(ctx, "open for checksum", rel);
        return false;
    }
    static __thread XXH3_state_t *xst;
    if (!xst)
        xst = XXH3_createState();
    XXH3_128bits_reset(xst);
    for (;;) {
        ssize_t r = read(fd, buf, VBUF_SIZE);
        if (r == 0)
            break;
        if (r < 0) {
            if (errno == EINTR)
                continue;
            walk_err(ctx, "read for checksum", rel);
            close(fd);
            return false;
        }
        XXH3_128bits_update(xst, buf, (size_t)r);
    }
    close(fd);
    *out = XXH3_128bits_digest(xst);
    return true;
}

/* returns NULL if the entry verifies clean, else a static reason string */
static const char *check_entry(struct walk_ctx *ctx, const char *rel,
                               bool checksum, uint8_t *buf,
                               struct stat *ss_out, XXH128_hash_t *dhash,
                               bool *have_hash)
{
    const struct job_options *o = &ctx->oe->o;
    struct stat ss, ds;
    *have_hash = false;
    if (fstatat(ctx->oe->src_fd, rel, &ss, AT_SYMLINK_NOFOLLOW) < 0)
        return NULL; /* source vanished since the copy: re-diffed next pass */
    *ss_out = ss;
    if (fstatat(ctx->oe->dst_fd, rel, &ds, AT_SYMLINK_NOFOLLOW) < 0)
        return "destination missing";
    if ((ss.st_mode & S_IFMT) != (ds.st_mode & S_IFMT))
        return "type mismatch";
    if (S_ISREG(ss.st_mode) && ss.st_size != ds.st_size)
        return "size mismatch";
    if (o->meta_times &&
        ts_diff_ns(&ss.st_mtim, &ds.st_mtim) > o->mtime_slop_ns)
        return "mtime mismatch";
    if (o->meta_owner && (ss.st_uid != ds.st_uid || ss.st_gid != ds.st_gid))
        return "owner mismatch";
    if (o->meta_mode && (ss.st_mode & 07777) != (ds.st_mode & 07777))
        return "mode mismatch";
    if (o->meta_xattrs &&
        !xattr_equal_at(ctx, ctx->oe->src_fd, ctx->oe->dst_fd, rel))
        return "xattr mismatch";

    if (checksum && S_ISREG(ss.st_mode)) {
        XXH128_hash_t sh, dh;
        if (!hash_file(ctx, ctx->oe->src_fd, rel, buf, &sh) ||
            !hash_file(ctx, ctx->oe->dst_fd, rel, buf, &dh))
            return "checksum read failed";
        *dhash = dh;
        *have_hash = true;
        if (sh.low64 != dh.low64 || sh.high64 != dh.high64)
            return "checksum mismatch";
    }
    return NULL;
}

static void recopy(struct walk_ctx *ctx, const char *rel, const struct stat *ss)
{
    const char *leaf;
    int spfd = open_parent_beneath(ctx->oe->src_fd, rel, &leaf);
    if (spfd < 0) {
        walk_err(ctx, "open src parent for recopy", rel);
        return;
    }
    const char *dleaf;
    int dpfd = open_parent_beneath(ctx->oe->dst_fd, rel, &dleaf);
    if (dpfd < 0) {
        walk_err(ctx, "open dst parent for recopy", rel);
        close(spfd);
        return;
    }
    struct estat es;
    estat_of(&es, ss);
    /* Recopy replaces a file that failed verification and therefore exists:
     * never direct — the atomic temp+rename must not expose a torn replacement. */
    copy_file_task(ctx, spfd, dpfd, leaf, rel, &es, false);
    close(spfd);
    close(dpfd);
}

void process_verify(const struct shard_item *it)
{
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    static __thread uint8_t *buf;
    if (!buf)
        buf = malloc(VBUF_SIZE);

    struct walk_ctx ctx = { .it = it };
    jrn_init(&ctx);
    int status = RES_OK;
    ctx.oe = opts_get(it->job_id);
    if (!ctx.oe || !buf) {
        snprintf(ctx.err, sizeof ctx.err, "no cached options for job %llu",
                 (unsigned long long)it->job_id);
        status = RES_TRANSIENT;
        goto out;
    }

    for (size_t i = 0; i < it->n_paths; i++) {
        const char *rel = it->paths[i];
        bool want_sum = it->vchecksum && it->vchecksum[i];
        struct stat ss;
        XXH128_hash_t dh = { 0, 0 };
        bool have_hash = false;
        const char *why = check_entry(&ctx, rel, want_sum, buf, &ss, &dh,
                                      &have_hash);
        if (!why) {
            CTR_ADD(ctx.c.verify_ok, 1);
            jrn_emit_hash(&ctx, JR_VERIFY_OK, rel, NULL, NULL,
                          have_hash ? dh.low64 : 0, have_hash ? dh.high64 : 0);
            continue;
        }
        CTR_ADD(ctx.c.verify_fail, 1);
        jrn_emit(&ctx, JR_VERIFY_FAIL, rel, NULL, NULL, 0, why);
        LOGW("shard %llu: verify fail %s: %s",
             (unsigned long long)it->shard_id, rel, why);
        if (ctx.oe->o.verify_on_mismatch != VERIFY_MISMATCH_FAIL &&
            !ctx.oe->o.dry_run && S_ISREG(ss.st_mode))
            recopy(&ctx, rel, &ss); /* default: recopy (D4) */
    }

    jrn_flush(&ctx);
    if (!jrn_wait_acked(&ctx)) {
        snprintf(ctx.err, sizeof ctx.err, "journal ack timeout");
        status = RES_TRANSIENT;
    }
out:
    clock_gettime(CLOCK_MONOTONIC, &t1);
    ctx.c.wall_ms = (uint64_t)((t1.tv_sec - t0.tv_sec) * 1000 +
                               (t1.tv_nsec - t0.tv_nsec) / 1000000);
    pb_buf b;
    pb_init(&b);
    enc_shard_result(&b, it->shard_id, it->lease_id, status, &ctx.c,
                     ctx.err[0] ? ctx.err : NULL);
    out_push(FR_SHARD_RESULT, &b);
    lease_remove(it->lease_id);
    jrn_destroy(&ctx);
}
