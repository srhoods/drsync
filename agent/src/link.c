/* Hardlink-group member linking (docs/DESIGN-hardlinks.md §3.3, LINKFIX phase).
 *
 * A LinkTask names one member of a hardlink group whose anchor — the first
 * member of the group, copied in full when it was first discovered
 * (speculative-anchor design, §3.4) — has been confirmed landed. Rather than
 * copy the member's data again, this just creates the destination directory
 * entry: linkat(anchor, member). Both paths are resolved beneath the
 * destination root under the same containment discipline as the walker
 * (component-wise O_NOFOLLOW, no traversal outside the job's roots).
 *
 * Runs only after DIRFIX, so every directory a member's path could land in
 * already exists — no directory is created here. */
#include "agent.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int64_t st_mtime_ns(const struct stat *st)
{
    return (int64_t)st->st_mtim.tv_sec * 1000000000 + st->st_mtim.tv_nsec;
}

/* Core linkat logic, factored out of process_linkfix so it is unit-testable
 * without the coordinator-protocol machinery (opts_get/journal-ack/lease):
 * given the destination root fd and a LinkTask's fields, re-checks the
 * anchor's gen, then linkat(anchor, member) with the EEXIST retry described
 * below. Journals/counts via ctx exactly as process_linkfix's caller expects;
 * returns the RES_* status. */
int do_linkfix(struct walk_ctx *ctx, int dst_fd, const struct linkfix *lf)
{
    const char *anchor_leaf, *member_leaf;
    int anchor_dfd = open_parent_beneath(dst_fd, lf->anchor_rel, &anchor_leaf);
    if (anchor_dfd < 0) {
        walk_err(ctx, "open anchor parent dir", lf->anchor_rel);
        return RES_ERROR;
    }

    /* Re-check the anchor has not drifted since it was copied — the same
     * drift guard ChunkTask.gen gives cross-host chunk fan-out. A mismatch
     * means the anchor was overwritten or removed since this pass copied it;
     * abort rather than link a stale or missing file into the member's name. */
    struct stat ast;
    if (fstatat(anchor_dfd, anchor_leaf, &ast, AT_SYMLINK_NOFOLLOW) < 0) {
        walk_err(ctx, "stat anchor", lf->anchor_rel);
        close(anchor_dfd);
        return RES_ERROR;
    }
    if ((uint64_t)ast.st_size != lf->gen_size || st_mtime_ns(&ast) != lf->gen_mtime_ns) {
        jrn_emit(ctx, JR_SRC_CHANGED, lf->member_rel, NULL, NULL, 0, NULL);
        close(anchor_dfd);
        return RES_SRC_CHANGED;
    }

    int member_dfd = open_parent_beneath(dst_fd, lf->member_rel, &member_leaf);
    if (member_dfd < 0) {
        walk_err(ctx, "open member parent dir", lf->member_rel);
        close(anchor_dfd);
        return RES_ERROR;
    }

    if (linkat(anchor_dfd, anchor_leaf, member_dfd, member_leaf, 0) < 0) {
        /* A prior run of this shard (lease expiry, retry) already created the
         * link, or a stale destination entry from an earlier pass sits at
         * this name: remove it and retry once, the same "replace" pattern the
         * walker uses for a type mismatch (walker.c remove_dst). */
        if (errno != EEXIST) {
            walk_err(ctx, "linkat", lf->member_rel);
            close(member_dfd);
            close(anchor_dfd);
            return RES_ERROR;
        }
        struct stat existing, anchor_by_stat;
        bool already_linked =
            fstatat(member_dfd, member_leaf, &existing, AT_SYMLINK_NOFOLLOW) == 0 &&
            fstatat(anchor_dfd, anchor_leaf, &anchor_by_stat, AT_SYMLINK_NOFOLLOW) == 0 &&
            existing.st_dev == anchor_by_stat.st_dev &&
            existing.st_ino == anchor_by_stat.st_ino;
        if (!already_linked) {
            if (unlinkat(member_dfd, member_leaf, 0) < 0 && errno != ENOENT) {
                walk_err(ctx, "linkfix-replace-unlink", lf->member_rel);
                close(member_dfd);
                close(anchor_dfd);
                return RES_ERROR;
            }
            if (linkat(anchor_dfd, anchor_leaf, member_dfd, member_leaf, 0) < 0) {
                walk_err(ctx, "linkat (retry)", lf->member_rel);
                close(member_dfd);
                close(anchor_dfd);
                return RES_ERROR;
            }
        } /* else: idempotent — this shard already succeeded once */
    }
    CTR_ADD(ctx->c.links_created, 1);
    jrn_emit(ctx, JR_LINK_CREATED, lf->member_rel, NULL, NULL, 0, lf->anchor_rel);
    close(member_dfd);
    close(anchor_dfd);
    return RES_OK;
}

void process_linkfix(const struct shard_item *it)
{
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    struct walk_ctx ctx = { .it = it };
    jrn_init(&ctx);
    int status = RES_OK;
    ctx.oe = opts_get(it->job_id);
    if (!ctx.oe) {
        snprintf(ctx.err, sizeof ctx.err, "no cached options for job %llu",
                 (unsigned long long)it->job_id);
        status = RES_TRANSIENT;
        goto out;
    }
    if (ctx.oe->o.dry_run)
        goto out; /* nothing is linked in a dry run */

    status = do_linkfix(&ctx, ctx.oe->dst_fd, &it->link);

out:
    jrn_flush(&ctx);
    if (!jrn_wait_acked(&ctx)) { /* ordering invariant: result after journals */
        snprintf(ctx.err, sizeof ctx.err, "journal ack timeout");
        status = RES_TRANSIENT;
    }
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
