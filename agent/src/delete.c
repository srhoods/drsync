/* Delete-pass executor (decision D5, docs/DESIGN-coordinator.md §2.2).
 * Input: a WI_DELETE item listing destination orphan paths harvested from the
 * previous pass's ORPHAN journal records. Orphan directories were never
 * descended into during the scan, so removal is recursive here — always
 * fd-anchored beneath the destination root (openat2 semantics via the same
 * open_beneath discipline as the walker), never by absolute path.
 * Every removed object is journaled JR_DELETED; dry-run jobs journal
 * JR_WOULD_DELETE and remove nothing.
 *
 * Fan-out (docs/DESIGN-coordinator.md §2.2 DELETE fan-out): a directory whose
 * own entry count exceeds delete_split_threshold is streamed out as new
 * DELETE shards (delete_split_batch names per shard) instead of being
 * unlinked depth-first by this one agent — the delete-pass analogue of
 * walker.c's split_entrylist_stream, but simpler: there is nothing to diff
 * against a destination (the whole subtree is already condemned), so a batch
 * is just names to remove. remove_object() applies this check at EVERY
 * directory the removal touches, not just the top-level path named in the
 * shard's paths[] — rm_dir_contents() routes every entry it finds back
 * through remove_object() rather than recursing directly, so a directory
 * that is individually small still gets caught if it is nested many levels
 * deep under a huge tree of otherwise-small directories (a wide/deep tree
 * where no single directory looks large from its own parent's point of view
 * previously never split at all, and got removed serially by one agent
 * thread — see docs/DESIGN-coordinator.md §2.2 for the incident that
 * surfaced this). */
#include "agent.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* Default names per split-produced DELETE shard when the job doesn't override
 * it (tuning.delete_split_batch). Delete work per name is a plain unlink, not
 * a full stat/diff/copy pipeline, so this can run higher than
 * ENTRYLIST_BATCH_DEFAULT before one shard's batch becomes a bottleneck in
 * its own right. */
#define DELETE_SPLIT_BATCH_DEFAULT 20000

static uint64_t remove_object(struct walk_ctx *ctx, int parentfd,
                              const char *name, const char *rel);

/* depth-first removal of the already-open directory d (fd dirfd(d), path
 * rel) — every entry goes through remove_object, so a subdirectory found
 * at ANY depth that turns out to be pathological is streamed out via
 * stream_delete_split exactly like a top-level orphan, not just the one
 * named directly in the shard's paths[] (see the fan-out comment at the top
 * of this file). Does not remove rel itself or consume d; the caller does
 * both, so remove_object can decide not to (a directory it just handed off
 * to a split must NOT be rmdir'd here — the split's own cleanup shard owns
 * that once every batch it produced has drained). */
static uint64_t rm_dir_contents(struct walk_ctx *ctx, DIR *d, const char *rel)
{
    uint64_t removed = 0;
    struct dirent *de;
    while ((de = readdir(d))) {
        if (de->d_name[0] == '.' &&
            (de->d_name[1] == '\0' ||
             (de->d_name[1] == '.' && de->d_name[2] == '\0')))
            continue;
        char crel[PATH_MAX];
        snprintf(crel, sizeof crel, "%s/%s", rel, de->d_name);
        removed += remove_object(ctx, dirfd(d), de->d_name, crel);
    }
    return removed;
}

/* Fully removes name (file, or directory including every descendant) inside
 * parentfd — depth-first, via rm_dir_contents. Returns removed count. Only
 * reached for a directory once remove_object has already decided it is NOT
 * pathological (under delete_split_threshold), so no further probing here. */
static uint64_t rm_tree(struct walk_ctx *ctx, int parentfd, const char *name,
                        const char *rel, DIR *already_open)
{
    uint64_t removed = 0;
    if (already_open) {
        removed = rm_dir_contents(ctx, already_open, rel);
        closedir(already_open);
        if (unlinkat(parentfd, name, AT_REMOVEDIR) < 0 && errno != ENOENT) {
            walk_err(ctx, "rmdir", rel);
            return removed;
        }
    } else {
        if (unlinkat(parentfd, name, 0) < 0 && errno != ENOENT) {
            walk_err(ctx, "unlink", rel);
            return removed;
        }
    }
    jrn_emit(ctx, JR_DELETED, rel, NULL, NULL, 0, NULL);
    return removed + 1;
}

/* Ships one batch of names still to remove under rel as a DeleteRemainder
 * split. total is 0 for every batch except the last one streamed for rel,
 * which carries the true count the coordinator uses to know when every
 * sibling shard has reported done (store.RecordSplit / CompleteDeleteRemainder
 * — the total is only known once readdir hits EOF, unlike a chunk group's
 * upfront byte-size-derived n_chunks). */
static void flush_delete_split(struct walk_ctx *ctx, const char *rel,
                               char *const *names, size_t n, uint32_t total)
{
    pb_buf b;
    pb_init(&b);
    enc_delete_split(&b, ctx->it->shard_id, ctx->split_seq, rel, names, n, total);
    ship_split(ctx, &b);
}

/* Streams a pathological directory's entries out as DeleteRemainder splits
 * instead of removing them depth-first in this shard. Mirrors walker.c's
 * split_entrylist_stream: names are shipped in batches as readdir yields
 * them (peak memory one batch, not the whole directory), and every batch
 * after the count check has already committed to streaming, so there is no
 * "go back to rm_tree" fallback mid-stream — once a directory is judged
 * pathological, it is fully handed off.
 *
 * The directory rel itself is NOT removed here: it cannot be, until every
 * split-produced child shard has finished emptying it, which this shard has
 * no way to know (they run on other agents, other leases, later). The
 * coordinator seeds a cleanup shard for rel once delete_groups' n_done
 * reaches the n_total this function's final batch reports (store.go
 * CompleteDeleteRemainder). fd is consumed (closed) either way. */
static void stream_delete_split(struct walk_ctx *ctx, const char *rel, int fd)
{
    DIR *d = fdopendir(fd);
    if (!d) {
        close(fd);
        walk_err(ctx, "fdopendir for delete split", rel);
        return;
    }
    size_t batch_max = ctx->oe->o.delete_split_batch
                          ? ctx->oe->o.delete_split_batch
                          : DELETE_SPLIT_BATCH_DEFAULT;
    char **batch = calloc(batch_max, sizeof *batch);
    if (!batch) {
        closedir(d);
        walk_err(ctx, "oom delete split stream", rel);
        return;
    }
    size_t nb = 0;
    uint32_t total = 0;
    struct dirent *de;
    errno = 0;
    while (!ctx->fatal && (de = readdir(d))) {
        if (de->d_name[0] == '.' &&
            (de->d_name[1] == '\0' || (de->d_name[1] == '.' && de->d_name[2] == '\0')))
            continue;
        /* d_name is only valid until the next readdir, and a batch spans
         * many of them, so each name is copied and released once shipped. */
        batch[nb] = strdup(de->d_name);
        if (!batch[nb]) {
            CTR_ADD(ctx->c.errors, 1);
            break;
        }
        nb++;
        total++;
        if (nb >= batch_max) {
            flush_delete_split(ctx, rel, batch, nb, 0);
            for (size_t i = 0; i < nb; i++)
                free(batch[i]);
            nb = 0;
        }
        errno = 0;
    }
    if (errno)
        walk_err(ctx, "read dir for delete split", rel);
    closedir(d); /* also closes fd */
    /* Final batch always ships (even if empty — total must still reach the
     * coordinator so an already-empty pathological directory's cleanup can
     * be seeded), carrying the true total this stream produced. */
    if (!ctx->fatal) {
        flush_delete_split(ctx, rel, batch, nb, total);
    }
    for (size_t i = 0; i < nb; i++)
        free(batch[i]);
    free(batch);
}

/* split rel into (parent dir fd under root, leaf name); -1 on failure */
int open_parent_beneath(int root_fd, const char *rel, const char **leaf)
{
    const char *slash = strrchr(rel, '/');
    if (!slash) {
        *leaf = rel;
        return dup(root_fd);
    }
    char parent[PATH_MAX];
    size_t n = (size_t)(slash - rel);
    if (n >= sizeof parent) {
        errno = ENAMETOOLONG;
        return -1;
    }
    memcpy(parent, rel, n);
    parent[n] = '\0';
    *leaf = slash + 1;
    /* component-wise O_NOFOLLOW walk, same guarantee as the walker */
    int cur = dup(root_fd);
    char *save = NULL;
    for (char *comp = strtok_r(parent, "/", &save); comp;
         comp = strtok_r(NULL, "/", &save)) {
        int next = openat(cur, comp,
                          O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        close(cur);
        if (next < 0)
            return -1;
        cur = next;
    }
    return cur;
}

/* Removes name (file or directory) inside parentfd, fanning out to
 * stream_delete_split instead of recursing if it is a directory over
 * delete_split_threshold. Called both for a shard's top-level orphan paths
 * (process_delete) and for every entry rm_dir_contents finds while
 * descending an already-accepted directory — so a pathologically large
 * subdirectory found at ANY depth is caught, not just one named directly in
 * the shard's paths[] (the gap the single-level-only version of this check
 * left: a tree of many individually-small subdirectories summing to tens of
 * millions of entries never split at all, since no single directory in the
 * chain ever looked large from its own parent's point of view). Returns the
 * number of objects this shard itself removed (0 if it handed the directory
 * off to a split instead — those objects are counted by the split-produced
 * shards that actually remove them). */
static uint64_t remove_object(struct walk_ctx *ctx, int parentfd,
                              const char *name, const char *rel)
{
    struct stat st;
    if (fstatat(parentfd, name, &st, AT_SYMLINK_NOFOLLOW) < 0) {
        if (errno != ENOENT) /* already gone is success */
            walk_err(ctx, "stat for delete", rel);
        return 0;
    }
    if (!S_ISDIR(st.st_mode))
        return rm_tree(ctx, parentfd, name, rel, NULL);

    uint64_t threshold = ctx->oe->o.delete_split_threshold;
    if (!threshold)
        return rm_tree(ctx, parentfd, name, rel, NULL);

    int fd = openat(parentfd, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (fd < 0) {
        walk_err(ctx, "open for delete probe", rel);
        return 0;
    }
    DIR *d = fdopendir(fd);
    if (!d) {
        close(fd);
        walk_err(ctx, "fdopendir for delete probe", rel);
        return 0;
    }
    /* Bounded probe: count up to threshold+1 entries without materialising
     * the whole directory — same idea as walker.c's read_entries_upto,
     * reimplemented here rather than shared, since delete's probe needs no
     * stat placeholders or destination-side bookkeeping, only a count. */
    uint64_t seen = 0;
    struct dirent *de;
    errno = 0;
    while (seen <= threshold && (de = readdir(d))) {
        if (de->d_name[0] == '.' &&
            (de->d_name[1] == '\0' || (de->d_name[1] == '.' && de->d_name[2] == '\0')))
            continue;
        seen++;
    }
    if (errno) {
        walk_err(ctx, "probe dir for delete", rel);
        closedir(d);
        return 0;
    }
    if (seen > threshold) {
        /* rewinddir so the stream starts from the first entry, not wherever
         * the probe's readdir left off. stream_delete_split takes ownership
         * of d's fd (its own fdopendir + closedir) — do not closedir(d)
         * here too, or the fd double-closes. */
        rewinddir(d);
        int dupfd = dup(dirfd(d));
        closedir(d);
        if (dupfd < 0) {
            walk_err(ctx, "dup for delete split", rel);
            return 0;
        }
        stream_delete_split(ctx, rel, dupfd);
        return 0;
    }
    /* Under threshold: reuse this same handle for the real removal —
     * rewinddir resets ITS OWN position (unlike dup(), which shares the
     * original's offset — see this file's history for why that distinction
     * matters), so no second openat is needed on the common case, which now
     * runs at every depth instead of once per top-level orphan. */
    rewinddir(d);
    return rm_tree(ctx, parentfd, name, rel, d);
}

void process_delete(const struct shard_item *it)
{
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    struct walk_ctx ctx = { .it = it, .split_seq = 1 };
    jrn_init(&ctx);
    int status = RES_OK;
    ctx.oe = opts_get(it->job_id);
    if (!ctx.oe) {
        snprintf(ctx.err, sizeof ctx.err, "no cached options for job %llu",
                 (unsigned long long)it->job_id);
        status = RES_TRANSIENT;
        goto out;
    }

    for (size_t i = 0; i < it->n_paths && !ctx.fatal; i++) {
        const char *rel = it->paths[i];
        if (!rel[0] || strstr(rel, "..")) { /* defense in depth */
            walk_err(&ctx, "refusing suspicious delete path", rel);
            continue;
        }
        if (ctx.oe->o.dry_run) {
            jrn_emit(&ctx, JR_WOULD_DELETE, rel, NULL, NULL, 0, NULL);
            CTR_ADD(ctx.c.orphans, 1);
            continue;
        }
        const char *leaf;
        int pfd = open_parent_beneath(ctx.oe->dst_fd, rel, &leaf);
        if (pfd < 0) {
            if (errno != ENOENT) /* parent gone = orphan already gone */
                walk_err(&ctx, "open parent for delete", rel);
            continue;
        }
        /* counters: a delete pass reports removals in the orphans column.
         * remove_object returns 0 (not an undercount) when it fans a
         * pathological directory out instead of removing it directly — those
         * objects are counted by the split-produced shards that actually
         * remove them, same as entry-list fan-out shifts the copy counters
         * onto the children instead of the walker that discovered them. */
        CTR_ADD(ctx.c.orphans, remove_object(&ctx, pfd, leaf, rel));
        close(pfd);
    }

    drain_splits(&ctx); /* every DeleteRemainder acked before the shard result (protocol §4.2) */
    jrn_flush(&ctx);
    if (!jrn_wait_acked(&ctx)) {
        snprintf(ctx.err, sizeof ctx.err, "journal ack timeout");
        status = RES_TRANSIENT;
    }
    if (ctx.fatal && ctx.err[0] == '\0')
        snprintf(ctx.err, sizeof ctx.err, "delete split failed");
    if (ctx.fatal)
        status = RES_TRANSIENT;
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
