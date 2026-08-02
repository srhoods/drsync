/* Hardlink-group member linking (docs/DESIGN-hardlinks.md §3.3, LINKFIX phase).
 *
 * A LinkTaskBatch names many members of hardlink groups whose anchor — the
 * first member of its group, copied in full when it was first discovered
 * (speculative-anchor design, §3.4) — has been confirmed landed. Rather than
 * copy each member's data again, this just creates its destination directory
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

    bool newly_linked = true;
    if (linkat(anchor_dfd, anchor_leaf, member_dfd, member_leaf, 0) < 0) {
        /* A prior run of this shard (lease expiry, retry within this pass, or
         * a fully-converged later pass re-sighting an already-linked group)
         * already created the link, or a stale destination entry from an
         * earlier pass sits at this name: remove it and retry once, the same
         * "replace" pattern the walker uses for a type mismatch
         * (walker.c remove_dst). */
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
        if (already_linked) {
            newly_linked = false; /* converged: nothing to count or journal */
        } else {
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
        }
    }
    if (newly_linked) {
        CTR_ADD(ctx->c.links_created, 1);
        jrn_emit(ctx, JR_LINK_CREATED, lf->member_rel, NULL, NULL, 0, lf->anchor_rel);
    }
    close(member_dfd);
    close(anchor_dfd);
    return RES_OK;
}

/* Runs do_linkfix for every entry in the batch. A single entry's failure
 * (RES_ERROR: walk_err already counted+journaled it; RES_SRC_CHANGED:
 * do_linkfix already journaled JR_SRC_CHANGED) does not abort the rest of the
 * batch — the same soft-fail-and-continue discipline process_dirfix uses for
 * its own per-item loop, and for the same reason: one bad member out of
 * (potentially) linkfixBatchSize should not force every other member in the
 * batch to be requeued and re-attempted, since re-linking an already-linked
 * member is redundant work, not free. The aggregate ShardResult stays RES_OK
 * even when some entries failed — same as process_dirfix, which never
 * inspects a per-item outcome for its own returned status either — since
 * per-entry outcomes are already fully accounted for via ctx.c.errors/
 * links_created and the journal; RES_ERROR/RES_SRC_CHANGED as the *shard's*
 * status is reserved for something preventing the batch from running at all
 * (see process_linkfix). */
static void do_linkfix_batch(struct walk_ctx *ctx, int dst_fd,
                             const struct linkfix *links, size_t n_links)
{
    for (size_t i = 0; i < n_links && !ctx->fatal; i++)
        do_linkfix(ctx, dst_fd, &links[i]);
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

    do_linkfix_batch(&ctx, ctx.oe->dst_fd, it->links, it->n_links);

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
