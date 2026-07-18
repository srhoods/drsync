/* Directory-metadata fix (docs/DESIGN-coordinator.md §2.2 DIRFIX phase).
 *
 * The walker applies a directory's metadata as it finishes the directory, but a
 * directory that fans out — its entries shipped as entry-list shards, or its
 * subtree split to other agents — has files renamed into it AFTER that, on other
 * hosts, which bumps its mtime back off the source value. Only once the whole
 * pass has drained is it safe to set it authoritatively. The coordinator seeds a
 * DirFixBatch per group of directories from the pass journal's DIR_META records;
 * this re-applies each one.
 *
 * It is a diff-then-apply, like the file meta-fix path: a directory already at
 * its target values is left untouched, so a converged pass touches nothing and
 * the destination's dir ctimes are not churned. Fixes are counted for
 * observability (ctx.c.dirs); they are deliberately NOT counted as meta_fixed,
 * which would keep a job from ever converging (the walker re-bumps split dirs
 * every pass). The result is reported as a ShardResult, the same channel the
 * chunk and verify executors use. */
#include "agent.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static int64_t st_mtime_ns(const struct stat *st)
{
    return (int64_t)st->st_mtim.tv_sec * 1000000000 + st->st_mtim.tv_nsec;
}

void process_dirfix(const struct shard_item *it)
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
    const struct job_options *o = &ctx.oe->o;
    if (o->dry_run)
        goto out; /* nothing is applied in a dry run */

    for (size_t i = 0; i < it->n_dirs && !ctx.fatal; i++) {
        const struct dirmeta *dm = &it->dirs[i];
        const char *rel = dm->rel_path[0] ? dm->rel_path : "";
        const char *label = dm->rel_path[0] ? dm->rel_path : "<root>";

        int fd = open_beneath(ctx.oe->dst_fd, rel, O_RDONLY | O_DIRECTORY);
        if (fd < 0) {
            /* Vanished since the walk (a later delete pass, or a racing change):
             * not a fault of this batch, skip it. */
            if (errno != ENOENT)
                walk_err(&ctx, "open dst dir", label);
            continue;
        }
        struct stat st;
        if (fstat(fd, &st) < 0) {
            walk_err(&ctx, "stat dst dir", label);
            close(fd);
            continue;
        }

        /* Diff on owner/mode/mtime only — atime drifts on every read and is not
         * a convergence signal, so it is refreshed when we apply but never the
         * reason to apply. */
        bool fix_owner = o->meta_owner &&
                         ((uint32_t)st.st_uid != dm->uid || (uint32_t)st.st_gid != dm->gid);
        bool fix_mode = o->meta_mode &&
                        (uint32_t)(st.st_mode & 07777) != (dm->mode & 07777);
        bool fix_time = o->meta_times && st_mtime_ns(&st) != dm->mtime_ns;

        if (fix_owner && fchown(fd, dm->uid, dm->gid) < 0)
            walk_err(&ctx, "chown dir", label);
        if (fix_mode && fchmod(fd, dm->mode & 07777) < 0)
            walk_err(&ctx, "chmod dir", label);
        if (fix_time) {
            struct timespec ts[2] = {
                { dm->atime_ns / 1000000000, dm->atime_ns % 1000000000 },
                { dm->mtime_ns / 1000000000, dm->mtime_ns % 1000000000 },
            };
            if (futimens(fd, ts) < 0)
                walk_err(&ctx, "utimens dir", label);
        }
        if (fix_owner || fix_mode || fix_time)
            CTR_ADD(ctx.c.dirs, 1);
        close(fd);
    }

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
