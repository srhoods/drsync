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
 * is just names to remove. Only checked at the top level (the orphan path
 * directly named in this shard's paths[]), not recursively at every nested
 * depth — the same scope split_entrylist_stream's own trigger has (it only
 * fires for the directory the walker is currently sitting in). A nested
 * subdirectory that is itself pathological is still removed correctly by
 * whichever shard reaches it, just not further split. */
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

/* depth-first removal of name inside parentfd; returns removed count */
static uint64_t rm_tree(struct walk_ctx *ctx, int parentfd, const char *name,
                        const char *rel)
{
    struct stat st;
    if (fstatat(parentfd, name, &st, AT_SYMLINK_NOFOLLOW) < 0) {
        if (errno != ENOENT) /* already gone is success */
            walk_err(ctx, "stat for delete", rel);
        return 0;
    }
    uint64_t removed = 0;
    if (S_ISDIR(st.st_mode)) {
        int fd = openat(parentfd, name,
                        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (fd < 0) {
            walk_err(ctx, "open for delete", rel);
            return 0;
        }
        DIR *d = fdopendir(fd);
        if (!d) {
            close(fd);
            walk_err(ctx, "fdopendir for delete", rel);
            return 0;
        }
        struct dirent *de;
        while ((de = readdir(d))) {
            if (de->d_name[0] == '.' &&
                (de->d_name[1] == '\0' ||
                 (de->d_name[1] == '.' && de->d_name[2] == '\0')))
                continue;
            char crel[PATH_MAX];
            snprintf(crel, sizeof crel, "%s/%s", rel, de->d_name);
            removed += rm_tree(ctx, dirfd(d), de->d_name, crel);
        }
        closedir(d);
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

/* Removes rel, fanning out to stream_delete_split if it is a directory over
 * delete_split_threshold. Returns the number of objects this shard itself
 * removed (0 if it handed the directory off to a split instead). */
static uint64_t remove_orphan(struct walk_ctx *ctx, int pfd, const char *leaf,
                              const char *rel)
{
    struct stat st;
    if (fstatat(pfd, leaf, &st, AT_SYMLINK_NOFOLLOW) < 0) {
        if (errno != ENOENT)
            walk_err(ctx, "stat for delete", rel);
        return 0;
    }
    uint64_t threshold = ctx->oe->o.delete_split_threshold;
    if (S_ISDIR(st.st_mode) && threshold) {
        /* Bounded probe: open the directory once just to count up to
         * threshold+1 entries and decide pathological without materialising
         * the whole thing — same idea as walker.c's read_entries_upto,
         * reimplemented here rather than shared, since delete's probe needs
         * no stat placeholders or destination-side bookkeeping, only a
         * count. This fd is closed here regardless of outcome: readdir's
         * position is tied to the fd's own open file description, so
         * reusing it for the real stream below would silently resume
         * partway through instead of at the start — a second, independent
         * openat for the pathological case is one extra syscall against
         * what is, by construction, a very large directory. */
        int pfd2 = openat(pfd, leaf, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (pfd2 < 0) {
            walk_err(ctx, "open for delete probe", rel);
            return 0;
        }
        DIR *pd = fdopendir(pfd2);
        if (!pd) {
            close(pfd2);
            walk_err(ctx, "fdopendir for delete probe", rel);
            return 0;
        }
        uint64_t seen = 0;
        struct dirent *de;
        errno = 0;
        while (seen <= threshold && (de = readdir(pd))) {
            if (de->d_name[0] == '.' &&
                (de->d_name[1] == '\0' || (de->d_name[1] == '.' && de->d_name[2] == '\0')))
                continue;
            seen++;
        }
        bool probe_err = errno != 0;
        closedir(pd); /* closes pfd2 too */
        if (probe_err) {
            walk_err(ctx, "probe dir for delete", rel);
            return 0;
        }
        if (seen > threshold) {
            int fd = openat(pfd, leaf, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
            if (fd < 0) {
                walk_err(ctx, "open for delete split", rel);
                return 0;
            }
            stream_delete_split(ctx, rel, fd); /* consumes fd */
            return 0;
        }
        /* Under threshold: fall through to the ordinary recursive removal. */
    }
    return rm_tree(ctx, pfd, leaf, rel);
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
         * remove_orphan returns 0 (not an undercount) when it fans a
         * pathological directory out instead of removing it directly — those
         * objects are counted by the split-produced shards that actually
         * remove them, same as entry-list fan-out shifts the copy counters
         * onto the children instead of the walker that discovered them. */
        CTR_ADD(ctx.c.orphans, remove_orphan(&ctx, pfd, leaf, rel));
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
