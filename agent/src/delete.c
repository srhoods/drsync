/* Delete-pass executor (decision D5, docs/DESIGN-coordinator.md §2.2).
 * Input: a WI_DELETE item listing destination orphan paths harvested from the
 * previous pass's ORPHAN journal records. Orphan directories were never
 * descended into during the scan, so removal is recursive here — always
 * fd-anchored beneath the destination root (openat2 semantics via the same
 * open_beneath discipline as the walker), never by absolute path.
 * Every removed object is journaled JR_DELETED; dry-run jobs journal
 * JR_WOULD_DELETE and remove nothing.
 *
 * Fan-out (docs/DESIGN-coordinator.md §2.2 DELETE fan-out) has TWO
 * independent mechanisms, catching two different pathological shapes:
 *
 * 1. delete_split_threshold / stream_delete_split: a directory whose OWN
 *    entry count exceeds the threshold is streamed out as new DELETE shards
 *    (delete_split_batch names per shard) instead of being unlinked
 *    depth-first by this one agent — the delete-pass analogue of
 *    walker.c's split_entrylist_stream. remove_object() applies this check
 *    at EVERY directory the removal touches, not just the top-level path
 *    named in the shard's paths[] — rm_dir_contents() routes every entry it
 *    finds back through remove_object() rather than recursing directly, so
 *    a directory that is individually small still gets caught if it is
 *    nested many levels deep under a huge tree of otherwise-small
 *    directories. Catches a WIDE directory at any depth.
 *
 * 2. delete_shard_budget / queue_delete_subdir: bounds the total number of
 *    objects ONE shard removes, regardless of tree shape — the delete-pass
 *    analogue of the scan walker's queue_split/shard_budget. Once the
 *    budget (decremented per object removed, threaded through the whole
 *    recursive descent via walk_ctx.budget, same as the walker) runs out,
 *    every not-yet-opened subdirectory rm_dir_contents finds is handed off
 *    as its OWN new top-level DELETE shard (ShardSplit.delete_subdirs)
 *    instead of being recursed into. Catches a tree that is pathological by
 *    aggregate DEPTH/BRANCHING even when no single directory anywhere in it
 *    individually exceeds delete_split_threshold — mechanism 1 alone cannot
 *    see this shape, since it only ever looks at one directory's own
 *    immediate entry count. This was a real production incident: a 77-way
 *    branching orphan tree, each branch several levels deep, with every
 *    individual directory well under any reasonable threshold, took 6 hours
 *    on the last 2 (of what should have been many more) shards. */
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

/* Default work budget (objects removed) for one DELETE shard when the job
 * doesn't override it (tuning.delete_shard_budget) — mirrors shard_budget's
 * own built-in fallback (walker.c), scaled up the same way
 * DELETE_SPLIT_BATCH_DEFAULT is scaled up from ENTRYLIST_BATCH_DEFAULT:
 * delete work per object is a plain unlink, not a full stat/diff/copy. */
#define DELETE_SHARD_BUDGET_DEFAULT 250000

/* How many delete_subdirs accumulate before flush_delete_subdir_splits ships
 * them as one ShardSplit frame — same batching rationale as queue_split's
 * SPLIT_BATCH (walker.c): keeps one frame from growing unbounded when the
 * budget runs out inside a directory with many subdirectory siblings still
 * unopened. */
#define DELETE_SUBDIR_SPLIT_BATCH 256

static uint64_t remove_object(struct walk_ctx *ctx, int parentfd, const char *name,
                              const char *rel, bool top_level, bool *deferred_out);

/* Records rel (a directory this shard processed inline but deliberately did
 * NOT rmdir, because something beneath it was handed off) into
 * ctx->deferred[] — reusing the same growable-array pattern as
 * queue_delete_subdir/ctx->split[], a separate array since both can be
 * live at once (a directory can defer its own rmdir while ALSO still
 * accumulating not-yet-flushed delete_subdirs from a sibling). Flushed into
 * ShardResult.deferred_rmdirs at the very end of process_delete — unlike
 * split[], there is no mid-shard flush: the coordinator only needs this list
 * once, attached to this shard's own final result, not as a streamed batch. */
static void queue_deferred_rmdir(struct walk_ctx *ctx, const char *rel)
{
    if (ctx->n_deferred == ctx->cap_deferred) {
        size_t cap = ctx->cap_deferred ? ctx->cap_deferred * 2 : 64;
        char **nv = realloc(ctx->deferred, cap * sizeof *nv);
        if (!nv) {
            CTR_ADD(ctx->c.errors, 1);
            return;
        }
        ctx->deferred = nv;
        ctx->cap_deferred = cap;
    }
    ctx->deferred[ctx->n_deferred] = strdup(rel);
    if (ctx->deferred[ctx->n_deferred])
        ctx->n_deferred++;
}

/* Accumulates rel (a wholly unopened subdirectory the exhausted budget is
 * handing off) into ctx->split[] — reusing the SAME accumulator fields the
 * walker's queue_split uses, safe because only one of walker.c/delete.c
 * ever runs per shard (they never share a live walk_ctx). Flushed via
 * enc_delete_subdir_split (ShardSplit.delete_subdirs, wire field 8) instead
 * of enc_shard_split (ShardSplit.subdirs, field 3) — a delete_subdirs entry
 * must become a new KindDelete shard, not a KindDir walk/diff shard, so it
 * cannot share the wire field with the walker's own subdirs without the
 * coordinator losing that distinction. */
static void flush_delete_subdir_splits(struct walk_ctx *ctx)
{
    if (!ctx->n_split)
        return;
    pb_buf b;
    pb_init(&b);
    enc_delete_subdir_split(&b, ctx->it->shard_id, ctx->split_seq, ctx->split, ctx->n_split);
    ship_split(ctx, &b);
    for (size_t i = 0; i < ctx->n_split; i++)
        free(ctx->split[i]);
    ctx->n_split = 0;
}

static void queue_delete_subdir(struct walk_ctx *ctx, const char *rel)
{
    if (ctx->n_split == ctx->cap_split) {
        size_t cap = ctx->cap_split ? ctx->cap_split * 2 : DELETE_SUBDIR_SPLIT_BATCH;
        char **nv = realloc(ctx->split, cap * sizeof *nv);
        if (!nv) {
            CTR_ADD(ctx->c.errors, 1);
            return;
        }
        ctx->split = nv;
        ctx->cap_split = cap;
    }
    ctx->split[ctx->n_split] = strdup(rel);
    if (ctx->split[ctx->n_split])
        ctx->n_split++;
    if (ctx->n_split >= DELETE_SUBDIR_SPLIT_BATCH)
        flush_delete_subdir_splits(ctx);
}

/* depth-first removal of the already-open directory d (fd dirfd(d), path
 * rel) — every entry goes through remove_object, so a subdirectory found
 * at ANY depth that turns out to be pathological (WIDE — its own entry
 * count over delete_split_threshold) is streamed out via
 * stream_delete_split exactly like a top-level orphan, not just the one
 * named directly in the shard's paths[] (see the fan-out comment at the top
 * of this file). Does not remove rel itself or consume d; the caller does
 * both, so remove_object can decide not to (a directory it just handed off
 * to a split — either kind — must NOT be rmdir'd here). top_level=false is
 * passed to remove_object for every entry here (never true — top_level only
 * applies to a shard's own paths[], see process_delete): this is what makes
 * the budget check apply to nested recursion but never skip a shard's own
 * assigned top-level orphan path. *deferred_out is set true if ANY entry
 * here was itself deferred or handed off (by either fan-out mechanism) —
 * propagated up so rm_tree knows not to rmdir rel itself yet (see rm_tree's
 * own doc comment for why: something under rel may still be mid-removal on
 * another shard entirely). */
static uint64_t rm_dir_contents(struct walk_ctx *ctx, DIR *d, const char *rel, bool *deferred_out)
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
        removed += remove_object(ctx, dirfd(d), de->d_name, crel, false, deferred_out);
    }
    return removed;
}

/* Fully removes name (file, or directory including every descendant) inside
 * parentfd — depth-first, via rm_dir_contents. Returns removed count. Only
 * reached for a directory once remove_object has already decided it is NOT
 * pathological (under delete_split_threshold), so no further probing here.
 * Decrements ctx->budget by one for this object once removed — threaded
 * through the whole recursive descent (same field, same semantics as the
 * scan walker's shard_budget/queue_split), so remove_object's budget check
 * on the next subdirectory sees work already done deeper in this same
 * shard, not just at this level.
 *
 * If already_open (a directory, not a file) and rm_dir_contents reports any
 * descendant was deferred/handed-off, this directory's OWN rmdir is skipped
 * too (queue_deferred_rmdir records rel instead) and *deferred_out (the
 * CALLER's own flag, one level up) is set — the parent recursing into US
 * must defer ITS rmdir as well, since it cannot know rel is actually empty
 * until every handed-off descendant's own group closes. This is what makes
 * the propagation chain reach all the way up to the shard's own top-level
 * path even though the handoff itself may have happened many levels deeper
 * — see docs/DESIGN-coordinator.md §2.2 for the completion-tracking this
 * feeds coordinator-side (registerPendingChildTx's ancestor walk,
 * ShardResult.deferred_rmdirs). A file is never deferred — only a directory
 * can have a descendant handed off — so deferred_out is left untouched on
 * the non-directory path. */
static uint64_t rm_tree(struct walk_ctx *ctx, int parentfd, const char *name,
                        const char *rel, DIR *already_open, bool *deferred_out)
{
    uint64_t removed = 0;
    if (already_open) {
        bool child_deferred = false;
        removed = rm_dir_contents(ctx, already_open, rel, &child_deferred);
        closedir(already_open);
        if (child_deferred) {
            queue_deferred_rmdir(ctx, rel);
            if (deferred_out)
                *deferred_out = true;
            return removed;
        }
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
    if (ctx->budget > 0)
        ctx->budget--;
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

/* Removes name (file or directory) inside parentfd. Two independent
 * fan-out checks apply to a directory (see the file-level comment for the
 * shapes each catches):
 *   - WIDE: if its own entry count is over delete_split_threshold, streams
 *     it out via stream_delete_split instead of recursing. Sets
 *     *deferred_out — the caller (rm_dir_contents, one level up) must not
 *     let ITS OWN containing directory rmdir until this handoff's group
 *     closes.
 *   - BUDGET: if this shard's ctx->budget is exhausted (top_level=false
 *     only — never a shard's own assigned paths[] entry, see
 *     process_delete/rm_dir_contents), hands it off UNOPENED as a new
 *     top-level DELETE shard via queue_delete_subdir instead of even
 *     probing it. Also sets *deferred_out.
 * Called both for a shard's top-level orphan paths (process_delete,
 * top_level=true, deferred_out=NULL — nothing above a top-level path to
 * propagate to within this shard; see process_delete for how a top-level
 * path's OWN deferred rmdir is recorded) and for every entry
 * rm_dir_contents finds while descending an already-accepted directory
 * (top_level=false, deferred_out always non-NULL there) — so a
 * pathologically WIDE subdirectory found at ANY depth is caught, not just
 * one named directly in the shard's paths[]. Returns the number of objects
 * this shard itself removed (0 if it handed the directory off to either
 * kind of split instead, OR deferred its own rmdir — either way those
 * objects/that directory are accounted for elsewhere: by the split-produced
 * shards that actually remove them, or by the eventual cleanup shard
 * closeDeleteGroupTx seeds once every deferred descendant's group closes). */
static uint64_t remove_object(struct walk_ctx *ctx, int parentfd, const char *name,
                              const char *rel, bool top_level, bool *deferred_out)
{
    struct stat st;
    if (fstatat(parentfd, name, &st, AT_SYMLINK_NOFOLLOW) < 0) {
        if (errno != ENOENT) /* already gone is success */
            walk_err(ctx, "stat for delete", rel);
        return 0;
    }
    if (!S_ISDIR(st.st_mode))
        return rm_tree(ctx, parentfd, name, rel, NULL, deferred_out);

    if (!top_level && ctx->budget <= 0) {
        queue_delete_subdir(ctx, rel);
        if (deferred_out)
            *deferred_out = true;
        return 0;
    }

    uint64_t threshold = ctx->oe->o.delete_split_threshold;
    if (!threshold)
        return rm_tree(ctx, parentfd, name, rel, NULL, deferred_out);

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
        if (deferred_out)
            *deferred_out = true;
        return 0;
    }
    /* Under threshold: reuse this same handle for the real removal —
     * rewinddir resets ITS OWN position (unlike dup(), which shares the
     * original's offset — see this file's history for why that distinction
     * matters), so no second openat is needed on the common case, which now
     * runs at every depth instead of once per top-level orphan. */
    rewinddir(d);
    return rm_tree(ctx, parentfd, name, rel, d, deferred_out);
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
    /* Work budget for this shard (objects removed, decremented in rm_tree) —
     * see the file-level comment's mechanism 2. Applies only to nested
     * recursion (remove_object's top_level=false calls), never to this
     * shard's own paths[] entries below, so a shard is never refused the
     * work it was explicitly granted. */
    ctx.budget = (int64_t)(ctx.oe->o.delete_shard_budget
                               ? ctx.oe->o.delete_shard_budget
                               : DELETE_SHARD_BUDGET_DEFAULT);

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
         * onto the children instead of the walker that discovered them.
         * top_level=true: this is a path this shard was explicitly granted,
         * never itself deferred via the budget check (only nested
         * subdirectories found during rm_dir_contents's descent are). */
        CTR_ADD(ctx.c.orphans, remove_object(&ctx, pfd, leaf, rel, true, NULL));
        close(pfd);
    }

    flush_delete_subdir_splits(&ctx); /* any budget-exhausted hand-offs still batched */
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
                     ctx.err[0] ? ctx.err : NULL, ctx.deferred, ctx.n_deferred);
    out_push(FR_SHARD_RESULT, &b);
    lease_remove(it->lease_id);
    for (size_t i = 0; i < ctx.n_deferred; i++)
        free(ctx.deferred[i]);
    free(ctx.deferred);
    jrn_destroy(&ctx);
}
