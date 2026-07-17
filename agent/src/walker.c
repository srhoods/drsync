/* Shard walker: dual-tree merge walk + diff (docs/DESIGN-agent.md §2).
 * Slice 2: stats are prefetched in batches (io_uring statx, fstatat fallback)
 * and data movement runs on the copy pool; the walker waits per directory so
 * directory metadata still lands after every rename into it (§3.5).
 * Still TODO: explicit stack for very deep trees. */
#include "agent.h"
#include "wire.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#include <linux/openat2.h>

#define SPLIT_BATCH 4096
#define SPLIT_ACK_TIMEOUT_S 120

void walk_err(struct walk_ctx *ctx, const char *what, const char *path)
{
    int e = errno;
    CTR_ADD(ctx->c.errors, 1);
    atomic_fetch_add(&g_stat_errors, 1);
    jrn_emit(ctx, JR_ERROR, path, NULL, NULL, e, what);
    LOGW("shard %llu: %s %s: %s", (unsigned long long)ctx->it->shard_id,
         what, path, strerror(e));
    errno = e;
}

/* openat2(RESOLVE_NO_SYMLINKS|RESOLVE_BENEATH): the design's traversal
 * guarantee in one syscall; ENOSYS falls back to component-wise O_NOFOLLOW. */
int open_beneath(int root_fd, const char *rel, uint64_t flags)
{
    if (!rel[0])
        return openat(root_fd, ".", (int)flags | O_CLOEXEC);
    struct open_how how = {
        .flags = flags | O_CLOEXEC,
        .resolve = RESOLVE_NO_SYMLINKS | RESOLVE_BENEATH,
    };
    long fd = syscall(SYS_openat2, root_fd, rel, &how, sizeof how);
    if (fd >= 0 || errno != ENOSYS)
        return (int)fd;

    char tmp[PATH_MAX];
    if (strlen(rel) >= sizeof tmp) {
        errno = ENAMETOOLONG;
        return -1;
    }
    strcpy(tmp, rel);
    int cur = dup(root_fd);
    char *save = NULL;
    for (char *comp = strtok_r(tmp, "/", &save); comp;
         comp = strtok_r(NULL, "/", &save)) {
        bool last = save == NULL || *save == '\0';
        int next = openat(cur, comp,
                          (last ? (int)flags : O_RDONLY | O_DIRECTORY) |
                              O_NOFOLLOW | O_CLOEXEC);
        close(cur);
        if (next < 0)
            return -1;
        cur = next;
    }
    return cur;
}

/* Open dir rel beneath dst_fd, creating missing components (mode 0700, fixed
 * when the dir itself is walked). Shared by the walker and the chunk executor,
 * which lands a big file's temp in a directory another agent may have created. */
int dst_dir_open(int dst_fd, const char *rel)
{
    int fd = open_beneath(dst_fd, rel, O_RDONLY | O_DIRECTORY);
    if (fd >= 0 || errno != ENOENT)
        return fd;
    char tmp[PATH_MAX];
    if (strlen(rel) >= sizeof tmp) {
        errno = ENAMETOOLONG;
        return -1;
    }
    strcpy(tmp, rel);
    int cur = dup(dst_fd);
    char *save = NULL;
    for (char *comp = strtok_r(tmp, "/", &save); comp;
         comp = strtok_r(NULL, "/", &save)) {
        int next = openat(cur, comp, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (next < 0 && errno == ENOENT) {
            if (mkdirat(cur, comp, 0700) < 0 && errno != EEXIST) {
                close(cur);
                return -1;
            }
            next = openat(cur, comp, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        }
        close(cur);
        if (next < 0)
            return -1;
        cur = next;
    }
    return cur;
}

/* Open dst dir for rel; in a real run, create missing components. */
static int open_dst_dir(const struct walk_ctx *ctx, const char *rel)
{
    if (ctx->oe->o.dry_run)
        return open_beneath(ctx->oe->dst_fd, rel, O_RDONLY | O_DIRECTORY);
    return dst_dir_open(ctx->oe->dst_fd, rel);
}

/* ---- directory entry collection ---- */
struct dent {
    char        *name;
    struct estat st;
    int          st_res; /* 0 ok, else -errno (from the stat prefetch) */
};

static int dent_cmp(const void *a, const void *b)
{
    return strcmp(((const struct dent *)a)->name, ((const struct dent *)b)->name);
}

/* Reads all names of an open dir fd (sorted). On success *dp owns the fd. */
static int read_entries(int fd, DIR **dp, struct dent **out, size_t *n_out)
{
    DIR *d = fdopendir(fd);
    if (!d) {
        close(fd);
        return -1;
    }
    struct dent *v = NULL;
    size_t n = 0, cap = 0;
    errno = 0;
    struct dirent *de;
    while ((de = readdir(d))) {
        if (de->d_name[0] == '.' &&
            (de->d_name[1] == '\0' || (de->d_name[1] == '.' && de->d_name[2] == '\0')))
            continue;
        if (n == cap) {
            cap = cap ? cap * 2 : 64;
            struct dent *nv = realloc(v, cap * sizeof *nv);
            if (!nv)
                goto oom;
            v = nv;
        }
        memset(&v[n], 0, sizeof v[n]);
        v[n].name = strdup(de->d_name);
        if (!v[n].name)
            goto oom;
        v[n].st_res = -EIO;
        n++;
        errno = 0;
    }
    if (errno)
        goto fail;
    qsort(v, n, sizeof *v, dent_cmp);
    *dp = d;
    *out = v;
    *n_out = n;
    return 0;
oom:
    errno = ENOMEM;
fail:;
    int e = errno;
    for (size_t i = 0; i < n; i++)
        free(v[i].name);
    free(v);
    closedir(d);
    errno = e;
    return -1;
}

static void free_entries(struct dent *v, size_t n)
{
    for (size_t i = 0; i < n; i++)
        free(v[i].name);
    free(v);
}

/* Prefetch stats: all src entries + dst entries that also exist in src
 * (dst-only entries are orphans and need no stat). One combined batch keeps
 * the ring full (docs/DESIGN-agent.md §2.2). */
static void prefetch_stats(int sfd, struct dent *sv, size_t sn,
                           int dfd, struct dent *dv, size_t dn)
{
    size_t max = sn + dn;
    struct statx_req *reqs = malloc(max * sizeof *reqs);
    if (!reqs) { /* degraded: everything stays -EIO → counted as errors */
        LOGE("oom stat prefetch (%zu entries)", max);
        return;
    }
    size_t nr = 0;
    for (size_t i = 0; i < sn; i++)
        reqs[nr++] = (struct statx_req){ .dirfd = sfd, .name = sv[i].name,
                                         .out = &sv[i].st };
    if (dfd >= 0) {
        size_t i = 0, j = 0; /* sorted intersection */
        while (i < sn && j < dn) {
            int cmp = strcmp(sv[i].name, dv[j].name);
            if (cmp < 0)
                i++;
            else if (cmp > 0)
                j++;
            else {
                reqs[nr++] = (struct statx_req){ .dirfd = dfd, .name = dv[j].name,
                                                 .out = &dv[j].st };
                i++;
                j++;
            }
        }
    }
    stat_batch(reqs, nr);
    size_t k = 0;
    for (size_t i = 0; i < sn; i++)
        sv[i].st_res = reqs[k++].res;
    if (dfd >= 0) {
        size_t i = 0, j = 0;
        while (i < sn && j < dn) {
            int cmp = strcmp(sv[i].name, dv[j].name);
            if (cmp < 0)
                i++;
            else if (cmp > 0)
                j++;
            else {
                dv[j].st_res = reqs[k++].res;
                i++;
                j++;
            }
        }
    }
    free(reqs);
}

/* ---- helpers ---- */

static int64_t ts_diff_ns(const struct timespec *a, const struct timespec *b)
{
    int64_t d = ((int64_t)a->tv_sec - b->tv_sec) * 1000000000 +
                (a->tv_nsec - b->tv_nsec);
    return d < 0 ? -d : d;
}

static bool times_equal(const struct walk_ctx *ctx, const struct timespec *a,
                        const struct timespec *b)
{
    return ts_diff_ns(a, b) <= ctx->oe->o.mtime_slop_ns;
}

static void apply_meta_dirfd(struct walk_ctx *ctx, int fd, const struct estat *ss,
                             const char *path)
{
    const struct job_options *o = &ctx->oe->o;
    if (o->meta_owner && fchown(fd, ss->uid, ss->gid) < 0)
        walk_err(ctx, "chown dir", path);
    if (o->meta_mode && fchmod(fd, ss->mode & 07777) < 0)
        walk_err(ctx, "chmod dir", path);
    if (o->meta_times) {
        struct timespec ts[2] = { ss->atim, ss->mtim };
        if (futimens(fd, ts) < 0)
            walk_err(ctx, "utimens dir", path);
    }
}

static void remove_dst(struct walk_ctx *ctx, int dfd, const char *name, bool is_dir)
{
    if (unlinkat(dfd, name, is_dir ? AT_REMOVEDIR : 0) < 0 && errno != ENOENT)
        walk_err(ctx, "replace-unlink", name); /* non-empty dir vs file conflict:
                                                * recursive remove TODO(slice3) */
}

static void copy_symlink(struct walk_ctx *ctx, const char *dir_rel, int sfd,
                         int dfd, const char *name, const struct estat *ss,
                         bool dst_is_symlink)
{
    char target[PATH_MAX];
    ssize_t n = readlinkat(sfd, name, target, sizeof target - 1);
    if (n < 0) {
        walk_err(ctx, "readlink", name);
        return;
    }
    target[n] = '\0';

    if (dst_is_symlink) {
        char have[PATH_MAX];
        ssize_t hn = readlinkat(dfd, name, have, sizeof have - 1);
        if (hn >= 0) {
            have[hn] = '\0';
            if (strcmp(have, target) == 0) {
                CTR_ADD(ctx->c.clean, 1);
                return;
            }
        }
        if (!ctx->oe->o.dry_run)
            remove_dst(ctx, dfd, name, false);
    }
    CTR_ADD(ctx->c.symlinks, 1);
    if (ctx->oe->o.dry_run) {
        CTR_ADD(ctx->c.files_copied, 1);
        return;
    }
    if (symlinkat(target, dfd, name) < 0) {
        walk_err(ctx, "symlink", name);
        return;
    }
    if (ctx->oe->o.meta_owner &&
        fchownat(dfd, name, ss->uid, ss->gid, AT_SYMLINK_NOFOLLOW) < 0)
        walk_err(ctx, "lchown", name);
    if (ctx->oe->o.meta_xattrs)
        xattr_copy_at(ctx, sfd, dfd, name, true); /* trusted./security. only:
                                                   * user.* is invalid on links */
    if (ctx->oe->o.meta_times) {
        struct timespec ts[2] = { ss->atim, ss->mtim };
        if (utimensat(dfd, name, ts, AT_SYMLINK_NOFOLLOW) < 0)
            walk_err(ctx, "lutimens", name);
    }
    CTR_ADD(ctx->c.files_copied, 1);
    atomic_fetch_add(&g_stat_files_copied, 1);
    char lrel[PATH_MAX];
    snprintf(lrel, sizeof lrel, "%s%s%s", dir_rel, dir_rel[0] ? "/" : "", name);
    jrn_emit(ctx, JR_COPIED, lrel, ss, NULL, 0, NULL);
}

/* ---- split queue ---- */
/* Names per entry-list shard. Sized to fan a pathological directory out into
 * MANY shards so walkers and agents chew through it in parallel (design §2.3) —
 * a large batch would make one giant shard that only one thread can process. */
#define ENTRYLIST_BATCH 4000

static void handle_orphan(struct walk_ctx *ctx, const char *rel, int dfd,
                          const char *name);

/* Awaits one in-flight split ack (timeout => fatal so the shard re-runs), then
 * unregisters and frees the waiter. */
static void await_split(struct walk_ctx *ctx, struct split_wait *w)
{
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += SPLIT_ACK_TIMEOUT_S;
    if (sem_timedwait(&w->sem, &dl) < 0) {
        snprintf(ctx->err, sizeof ctx->err, "split ack timeout (seq %llu)",
                 (unsigned long long)w->seq);
        ctx->fatal = true;
    }
    split_unregister(w); /* removes from the registry and sem_destroys */
    free(w);
}

/* Ships a prepared ShardSplit frame (subdirs or an entry list) WITHOUT blocking:
 * the ack is awaited later (drain_splits, before the shard result — the ordering
 * invariant of protocol §4.2), so consecutive round-trips overlap instead of
 * serialising. Blocks only for backpressure when SPLIT_WINDOW acks are already
 * outstanding. Consumes seq. */
static void ship_split(struct walk_ctx *ctx, pb_buf *b)
{
    if (ctx->infl_count == SPLIT_WINDOW) {
        struct split_wait *old = ctx->infl[ctx->infl_head];
        ctx->infl_head = (ctx->infl_head + 1) % SPLIT_WINDOW;
        ctx->infl_count--;
        await_split(ctx, old);
    }
    struct split_wait *w = calloc(1, sizeof *w);
    if (!w) {
        CTR_ADD(ctx->c.errors, 1);
        ctx->fatal = true;
        out_push(FR_SHARD_SPLIT, b); /* still recorded (idempotent); just not awaited */
        ctx->split_seq++;
        return;
    }
    w->parent = ctx->it->shard_id;
    w->seq = ctx->split_seq;
    split_register(w);
    out_push(FR_SHARD_SPLIT, b);
    ctx->infl[(ctx->infl_head + ctx->infl_count) % SPLIT_WINDOW] = w;
    ctx->infl_count++;
    ctx->split_seq++;
}

/* Awaits every outstanding split ack. Must run before reporting the shard
 * result so the coordinator has recorded all children first. */
static void drain_splits(struct walk_ctx *ctx)
{
    while (ctx->infl_count > 0) {
        struct split_wait *w = ctx->infl[ctx->infl_head];
        ctx->infl_head = (ctx->infl_head + 1) % SPLIT_WINDOW;
        ctx->infl_count--;
        await_split(ctx, w);
    }
}

static void flush_splits(struct walk_ctx *ctx)
{
    if (!ctx->n_split)
        return;
    pb_buf b;
    pb_init(&b);
    enc_shard_split(&b, ctx->it->shard_id, ctx->split_seq, ctx->split, ctx->n_split);
    ship_split(ctx, &b);

    for (size_t i = 0; i < ctx->n_split; i++)
        free(ctx->split[i]);
    ctx->n_split = 0;
}

/* Ships one batch of source-side names of a pathological directory as an
 * entry-list shard; the coordinator fans it out fleet-wide (§2.3). */
static void flush_entrylist(struct walk_ctx *ctx, const char *dir_rel,
                            char *const *names, size_t n)
{
    pb_buf b;
    pb_init(&b);
    enc_entrylist_split(&b, ctx->it->shard_id, ctx->split_seq, dir_rel, names, n);
    ship_split(ctx, &b);
}

/* A directory whose source entry count exceeds dir_split_threshold: enumerate
 * names only (no per-entry stats here), journal destination-only names as
 * orphans inline, and ship the source-side names as entry-list shards. Each
 * shard then runs the same statx/diff/copy pipeline over its slice. */
static void split_entrylist(struct walk_ctx *ctx, const char *rel,
                            struct dent *sv, size_t sn,
                            struct dent *dv, size_t dn, int dfd)
{
    char **batch = NULL;
    size_t nb = 0, cap = 0;
    size_t i = 0, j = 0;
    while ((i < sn || j < dn) && !ctx->fatal) {
        int cmp = i == sn ? 1 : j == dn ? -1 : strcmp(sv[i].name, dv[j].name);
        if (cmp > 0) { /* destination-only: orphan (D5 report-only) */
            handle_orphan(ctx, rel, dfd, dv[j].name);
            j++;
            continue;
        }
        if (nb == cap) {
            cap = cap ? cap * 2 : 1024;
            char **nv = realloc(batch, cap * sizeof *nv);
            if (!nv) {
                CTR_ADD(ctx->c.errors, 1);
                break;
            }
            batch = nv;
        }
        batch[nb++] = sv[i].name; /* borrowed from sv; freed with the dent array */
        if (cmp == 0)
            j++;
        i++;
        if (nb >= ENTRYLIST_BATCH) {
            flush_entrylist(ctx, rel, batch, nb);
            nb = 0;
        }
    }
    if (nb && !ctx->fatal)
        flush_entrylist(ctx, rel, batch, nb);
    free(batch);
}

static void queue_split(struct walk_ctx *ctx, const char *rel)
{
    if (ctx->n_split == ctx->cap_split) {
        size_t cap = ctx->cap_split ? ctx->cap_split * 2 : 256;
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
    if (ctx->n_split >= SPLIT_BATCH)
        flush_splits(ctx);
}

/* ---- big-file queue (cross-host chunk fan-out) ---- */
/* Big files per ShardSplit.big_files frame. Small: a batch of big files is
 * already a lot of bytes to move, and the coordinator fans each out further. */
#define BIGFILE_BATCH 256

static void flush_bigfiles(struct walk_ctx *ctx)
{
    if (!ctx->n_bigfiles)
        return;
    pb_buf b;
    pb_init(&b);
    enc_bigfile_split(&b, ctx->it->shard_id, ctx->split_seq,
                      ctx->bigfiles, ctx->n_bigfiles);
    ship_split(ctx, &b);
    for (size_t i = 0; i < ctx->n_bigfiles; i++)
        free(ctx->bigfiles[i].rel);
    ctx->n_bigfiles = 0;
}

/* Records a big regular file for the coordinator to chunk across the fleet,
 * instead of copying it on this one agent (cp_submit). */
static void queue_bigfile(struct walk_ctx *ctx, const char *dir_rel,
                          const char *name, const struct estat *ss)
{
    if (ctx->n_bigfiles == ctx->cap_bigfiles) {
        size_t cap = ctx->cap_bigfiles ? ctx->cap_bigfiles * 2 : 64;
        struct bigfile *nv = realloc(ctx->bigfiles, cap * sizeof *nv);
        if (!nv) {
            CTR_ADD(ctx->c.errors, 1);
            return;
        }
        ctx->bigfiles = nv;
        ctx->cap_bigfiles = cap;
    }
    char *rel;
    if (asprintf(&rel, "%s%s%s", dir_rel, dir_rel[0] ? "/" : "", name) < 0) {
        CTR_ADD(ctx->c.errors, 1);
        return;
    }
    ctx->bigfiles[ctx->n_bigfiles].rel = rel;
    ctx->bigfiles[ctx->n_bigfiles].size = ss->size;
    ctx->bigfiles[ctx->n_bigfiles].mtime_ns =
        (int64_t)ss->mtim.tv_sec * 1000000000 + ss->mtim.tv_nsec;
    ctx->n_bigfiles++;
    if (ctx->n_bigfiles >= BIGFILE_BATCH)
        flush_bigfiles(ctx);
}

/* A regular file big enough to copy across the fleet rather than on this agent:
 * at/above chunk_threshold AND larger than one chunk, so it yields ≥2 ranges. */
static bool should_chunk(const struct job_options *o, uint64_t size)
{
    return o->chunk_threshold && size >= o->chunk_threshold &&
           o->chunk_size && size > o->chunk_size;
}

/* ---- the walk ---- */
static void walk_dir(struct walk_ctx *ctx, const char *rel);

/* This shard's entry budget: subdirectories are descended inline while it
 * lasts and pushed back to the coordinator once it runs out. The coordinator's
 * per-shard override wins when present — it knows the fleet size and queue
 * depth, and sends budget 0 to fan a job out (proto WalkOverrides). Absent, the
 * job's tuning applies. */
static int64_t shard_budget(const struct walk_ctx *ctx)
{
    if (ctx->it->ov.have_budget)
        return (int64_t)ctx->it->ov.budget;
    return (int64_t)(ctx->oe->o.shard_budget ? ctx->oe->o.shard_budget : 250000);
}

/* Entry count above which a directory is fanned out as entry-list shards
 * instead of being walked here. Coordinator override wins, as for the budget. */
static uint64_t split_threshold(const struct walk_ctx *ctx)
{
    if (ctx->it->ov.have_split_threshold)
        return ctx->it->ov.split_threshold;
    return ctx->oe->o.dir_split_threshold;
}

static void handle_entry(struct walk_ctx *ctx, struct dpend *dp, const char *rel,
                         int sfd, int dfd, const struct dent *se,
                         const struct dent *de /* NULL if absent in dst */)
{
    const struct job_options *o = &ctx->oe->o;
    const char *name = se->name;
    ctx->c.entries_walked++; /* walker-only counter: plain increment is fine */
    atomic_fetch_add(&g_stat_scanned, 1);
    if (ctx->budget > 0)
        ctx->budget--;

    if (se->st_res != 0) {
        if (se->st_res != -ENOENT) { /* vanished-since-readdir is not an error */
            errno = -se->st_res;
            walk_err(ctx, "stat src", name);
        }
        return;
    }
    const struct estat *ss = &se->st;
    const struct estat *ds = NULL;
    if (de && de->st_res == 0)
        ds = &de->st; /* dst stat failed → treat as absent, recreate */
    bool type_match = ds && ((ss->mode & S_IFMT) == (ds->mode & S_IFMT));

    char child[PATH_MAX];
    switch (ss->mode & S_IFMT) {
    case S_IFDIR:
        CTR_ADD(ctx->c.dirs, 1);
        if (ds && !type_match && !o->dry_run)
            remove_dst(ctx, dfd, name, false);
        if ((!ds || !type_match) && !o->dry_run &&
            mkdirat(dfd, name, 0700) < 0 && errno != EEXIST) {
            walk_err(ctx, "mkdir", name);
            return;
        }
        if (snprintf(child, sizeof child, "%s%s%s", rel, rel[0] ? "/" : "", name) >=
            (int)sizeof child) {
            errno = ENAMETOOLONG;
            walk_err(ctx, "path", name);
            return;
        }
        if (ctx->budget > 0)
            walk_dir(ctx, child); /* TODO: explicit stack for very deep trees */
        else
            queue_split(ctx, child);
        break;

    case S_IFREG: {
        if (ss->nlink > 1) { /* D3: copied independently, cost made visible */
            CTR_ADD(ctx->c.nlink_dup_files, 1);
            CTR_ADD(ctx->c.nlink_dup_bytes, ss->size);
            char nrel[PATH_MAX];
            snprintf(nrel, sizeof nrel, "%s%s%s", rel, rel[0] ? "/" : "", name);
            jrn_emit(ctx, JR_NLINK_DUP, nrel, ss, NULL, 0, NULL);
        }
        bool need = !type_match || ds->size != ss->size ||
                    !times_equal(ctx, &ss->mtim, &ds->mtim);
        if (!need) {
            /* diff predicate steps 5–6: owner/mode, then lazy xattr digest —
             * paid only by files that are otherwise clean (design §2.1) */
            bool fix = (o->meta_owner && (ds->uid != ss->uid || ds->gid != ss->gid)) ||
                       (o->meta_mode && (ds->mode & 07777) != (ss->mode & 07777));
            bool fix_xattrs =
                o->meta_xattrs && !xattr_equal_at(ctx, sfd, dfd, name);
            if (!fix && !fix_xattrs) {
                CTR_ADD(ctx->c.clean, 1);
                return;
            }
            if (o->dry_run) {
                CTR_ADD(ctx->c.meta_fixed, 1);
                return;
            }
            if (fix_xattrs)
                xattr_copy_at(ctx, sfd, dfd, name, false);
            if (o->meta_owner && fchownat(dfd, name, ss->uid, ss->gid, 0) < 0)
                walk_err(ctx, "chown", name);
            if (o->meta_mode && fchmodat(dfd, name, ss->mode & 07777, 0) < 0)
                walk_err(ctx, "chmod", name);
            CTR_ADD(ctx->c.meta_fixed, 1);
            atomic_fetch_add(&g_stat_meta_fixed, 1);
            char frel[PATH_MAX];
            snprintf(frel, sizeof frel, "%s%s%s", rel, rel[0] ? "/" : "", name);
            jrn_emit(ctx, JR_META_FIXED, frel, ss, ds, 0, NULL);
            return;
        }
        if (o->dry_run) {
            CTR_ADD(ctx->c.files_copied, 1);
            CTR_ADD(ctx->c.bytes_copied, ss->size);
            char wrel[PATH_MAX];
            snprintf(wrel, sizeof wrel, "%s%s%s", rel, rel[0] ? "/" : "", name);
            jrn_emit(ctx, JR_WOULD_COPY, wrel, ss, ds, 0, NULL);
            return;
        }
        if (ds && !type_match)
            remove_dst(ctx, dfd, name, S_ISDIR(ds->mode));
        if (should_chunk(o, ss->size)) {
            /* Hand a big file to the coordinator to copy across the fleet
             * instead of on this agent alone (design §2.3). The dst dir exists
             * (opened/created above); chunk tasks land the temp there. */
            queue_bigfile(ctx, rel, name, ss);
            break;
        }
        cp_submit(ctx, dp, sfd, dfd, rel, name, ss); /* async: copy pool */
        break;
    }

    case S_IFLNK:
        if (ds && !type_match && !o->dry_run) {
            remove_dst(ctx, dfd, name, S_ISDIR(ds->mode));
            ds = NULL;
        }
        copy_symlink(ctx, rel, sfd, dfd, name, ss, ds && type_match);
        break;

    default: /* device nodes, FIFOs, sockets */
        CTR_ADD(ctx->c.specials, 1);
        if (!o->meta_specials)
            return;
        if (type_match && ds->rdev_major == ss->rdev_major &&
            ds->rdev_minor == ss->rdev_minor) {
            CTR_ADD(ctx->c.clean, 1);
            return;
        }
        if (o->dry_run) {
            CTR_ADD(ctx->c.files_copied, 1);
            return;
        }
        if (ds)
            remove_dst(ctx, dfd, name, S_ISDIR(ds->mode));
        if (mknodat(dfd, name, ss->mode,
                    makedev(ss->rdev_major, ss->rdev_minor)) < 0) {
            walk_err(ctx, "mknod", name); /* usually EPERM without CAP_MKNOD */
            return;
        }
        if (o->meta_owner &&
            fchownat(dfd, name, ss->uid, ss->gid, AT_SYMLINK_NOFOLLOW) < 0)
            walk_err(ctx, "chown special", name);
        /* mknodat's mode is masked by umask, so restore the exact permission
         * bits; copy xattrs/ACLs; and set times LAST (chmod/chown/setxattr bump
         * ctime, not mtime). Without the utimens the node keeps its creation
         * mtime and every verify pass fails with "mtime mismatch" — and specials
         * are not recopied (recopy is regular-file only), so it never converges. */
        if (o->meta_mode && fchmodat(dfd, name, ss->mode & 07777, 0) < 0)
            walk_err(ctx, "chmod special", name);
        if (o->meta_xattrs)
            xattr_copy_at(ctx, sfd, dfd, name, false);
        if (o->meta_times) {
            struct timespec ts[2] = { ss->atim, ss->mtim };
            if (utimensat(dfd, name, ts, 0) < 0)
                walk_err(ctx, "utimens special", name);
        }
        CTR_ADD(ctx->c.files_copied, 1);
        {
            char sprel[PATH_MAX];
            snprintf(sprel, sizeof sprel, "%s%s%s", rel, rel[0] ? "/" : "", name);
            jrn_emit(ctx, JR_COPIED, sprel, ss, NULL, 0, NULL);
        }
        break;
    }
}

static void handle_orphan(struct walk_ctx *ctx, const char *rel, int dfd,
                          const char *name)
{
    const struct job_options *o = &ctx->oe->o;
    const char *prefix = o->temp_prefix[0] ? o->temp_prefix : ".drsync.tmp.";
    if (strncmp(name, prefix, strlen(prefix)) == 0) {
        /* crash residue from an interrupted copy: always reclaimed (design §3) */
        if (!o->dry_run && unlinkat(dfd, name, 0) < 0 && errno != ENOENT)
            walk_err(ctx, "reclaim temp", name);
        return;
    }
    /* D5: report-only here; the ORPHAN record feeds the explicit delete pass */
    CTR_ADD(ctx->c.orphans, 1);
    char orel[PATH_MAX];
    snprintf(orel, sizeof orel, "%s%s%s", rel, rel[0] ? "/" : "", name);
    jrn_emit(ctx, JR_ORPHAN, orel, NULL, NULL, 0, NULL);
}

static void walk_dir(struct walk_ctx *ctx, const char *rel)
{
    if (ctx->fatal)
        return;
    const struct job_options *o = &ctx->oe->o;

    int sfd_raw = open_beneath(ctx->oe->src_fd, rel, O_RDONLY | O_DIRECTORY);
    if (sfd_raw < 0) {
        if (errno != ENOENT) /* ENOENT: vanished since discovery — fine */
            walk_err(ctx, "open src dir", rel[0] ? rel : "<root>");
        return;
    }
    struct stat sst_raw;
    if (fstat(sfd_raw, &sst_raw) < 0) {
        walk_err(ctx, "stat src dir", rel);
        close(sfd_raw);
        return;
    }
    struct estat sst = {
        .mode = sst_raw.st_mode, .uid = sst_raw.st_uid, .gid = sst_raw.st_gid,
        .atim = sst_raw.st_atim, .mtim = sst_raw.st_mtim,
    };

    DIR *sd = NULL;
    struct dent *sv = NULL;
    size_t sn = 0;
    if (read_entries(sfd_raw, &sd, &sv, &sn) < 0) {
        walk_err(ctx, "read src dir", rel[0] ? rel : "<root>");
        return;
    }
    int sfd = dirfd(sd);

    DIR *dd = NULL;
    struct dent *dv = NULL;
    size_t dn = 0;
    int dfd = -1;
    int dfd_raw = o->dry_run ? open_beneath(ctx->oe->dst_fd, rel, O_RDONLY | O_DIRECTORY)
                             : open_dst_dir(ctx, rel);
    if (dfd_raw >= 0) {
        if (read_entries(dfd_raw, &dd, &dv, &dn) < 0) {
            walk_err(ctx, "read dst dir", rel[0] ? rel : "<root>");
            dd = NULL;
            dn = 0;
        } else {
            dfd = dirfd(dd);
        }
    } else if (!o->dry_run) {
        walk_err(ctx, "open dst dir", rel[0] ? rel : "<root>");
        free_entries(sv, sn);
        closedir(sd);
        return;
    }

    /* Pathological directory: source entry count over the split threshold.
     * Ship the names as entry-list shards for fleet-wide fan-out instead of
     * statting and copying millions of entries in this one shard (§2.3).
     *
     * The directory's OWN metadata must still be applied here — the fanned-out
     * entrylist shards process the dir's ENTRIES, never the dir itself, and
     * DIRFIX is currently a no-op, so without this the split dir's owner/mode/
     * times/xattrs/ACLs never land. Those entrylist shards rename entries into
     * this dir asynchronously, which bumps its mtime after we set it; on a
     * non-final pass the mtime is therefore left dirty and the next pass
     * re-applies it, and by the converging pass nothing is copied so it sticks
     * (dir metadata converges over passes — the same property the non-split
     * path relies on for cross-shard subdirectories). */
    uint64_t split_at = split_threshold(ctx);
    if (split_at && sn > split_at) {
        split_entrylist(ctx, rel, sv, sn, dv, dn, dfd);
        if (dfd >= 0 && !o->dry_run && !ctx->fatal) {
            xattr_copy_fd(ctx, sfd, dfd, rel[0] ? rel : "<root>"); /* incl. ACLs */
            apply_meta_dirfd(ctx, dfd, &sst, rel[0] ? rel : "<root>");
        }
        jrn_emit(ctx, JR_DIR_META, rel, &sst, NULL, 0, NULL);
        free_entries(sv, sn);
        free_entries(dv, dn);
        closedir(sd);
        if (dd)
            closedir(dd);
        return;
    }

    prefetch_stats(sfd, sv, sn, dfd, dv, dn);

    struct dpend dp;
    dpend_init(&dp);

    /* sorted merge (design §2) */
    size_t i = 0, j = 0;
    while ((i < sn || j < dn) && !ctx->fatal) {
        int cmp = i == sn ? 1 : j == dn ? -1 : strcmp(sv[i].name, dv[j].name);
        if (cmp < 0) {
            handle_entry(ctx, &dp, rel, sfd, dfd, &sv[i], NULL);
            i++;
        } else if (cmp > 0) {
            handle_orphan(ctx, rel, dfd, dv[j].name);
            j++;
        } else {
            handle_entry(ctx, &dp, rel, sfd, dfd, &sv[i], &dv[j]);
            i++;
            j++;
        }
    }

    /* Wait for this directory's async copies, then apply its metadata:
     * every rename into the dir has happened, so mtime sticks (§3.5). */
    dpend_wait(&dp);
    dpend_destroy(&dp);
    if (dfd >= 0 && !o->dry_run && !ctx->fatal) {
        xattr_copy_fd(ctx, sfd, dfd, rel[0] ? rel : "<root>"); /* incl. ACLs */
        apply_meta_dirfd(ctx, dfd, &sst, rel[0] ? rel : "<root>");
    }
    jrn_emit(ctx, JR_DIR_META, rel, &sst, NULL, 0, NULL);

    free_entries(sv, sn);
    free_entries(dv, dn);
    closedir(sd);
    if (dd)
        closedir(dd);
}

void process_shard(const struct shard_item *it)
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
    } else {
        ctx.budget = shard_budget(&ctx);
        walk_dir(&ctx, it->rel_path);
        flush_splits(&ctx);
        flush_bigfiles(&ctx);
        drain_splits(&ctx); /* all splits acked before the result (protocol §4.2) */
        jrn_flush(&ctx);
        if (!jrn_wait_acked(&ctx)) { /* same ordering invariant for journals */
            snprintf(ctx.err, sizeof ctx.err, "journal ack timeout");
            ctx.fatal = true;
        }
        if (ctx.fatal)
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
    free(ctx.split);
    free(ctx.bigfiles);
    jrn_destroy(&ctx);
}

/* Entry-list consumer: the supplied names are a source-side slice of a
 * pathological directory (rel). Stat just those names on both sides (no
 * readdir) and run the same per-entry diff/copy pipeline; subdirectory names
 * become dir shards, files are copied. Orphans and this dir's own metadata are
 * owned by the splitting walk, not repeated here. */
static void entrylist_walk(struct walk_ctx *ctx, const char *rel,
                           char *const *names, size_t n_names)
{
    if (ctx->fatal)
        return;
    const struct job_options *o = &ctx->oe->o;

    int sfd = open_beneath(ctx->oe->src_fd, rel, O_RDONLY | O_DIRECTORY);
    if (sfd < 0) {
        if (errno != ENOENT)
            walk_err(ctx, "open src dir", rel[0] ? rel : "<root>");
        return;
    }
    int dfd = o->dry_run ? open_beneath(ctx->oe->dst_fd, rel, O_RDONLY | O_DIRECTORY)
                         : open_dst_dir(ctx, rel);
    if (dfd < 0 && !o->dry_run) {
        walk_err(ctx, "open dst dir", rel[0] ? rel : "<root>");
        close(sfd);
        return;
    }

    struct dent *sv = calloc(n_names, sizeof *sv);
    struct dent *dv = calloc(n_names, sizeof *dv);
    struct statx_req *reqs = malloc((dfd >= 0 ? 2 : 1) * n_names * sizeof *reqs);
    if ((n_names && (!sv || !dv)) || !reqs) {
        walk_err(ctx, "oom entrylist", rel);
        free(sv);
        free(dv);
        free(reqs);
        close(sfd);
        if (dfd >= 0)
            close(dfd);
        return;
    }
    /* stat every name on the source, and the same names on the destination
     * (missing → -ENOENT → treated as absent). Names are borrowed. */
    size_t nr = 0;
    for (size_t i = 0; i < n_names; i++) {
        sv[i].name = names[i];
        sv[i].st_res = -EIO;
        reqs[nr++] = (struct statx_req){ .dirfd = sfd, .name = names[i], .out = &sv[i].st };
    }
    if (dfd >= 0)
        for (size_t i = 0; i < n_names; i++) {
            dv[i].name = names[i];
            dv[i].st_res = -EIO;
            reqs[nr++] = (struct statx_req){ .dirfd = dfd, .name = names[i], .out = &dv[i].st };
        }
    stat_batch(reqs, nr);
    size_t k = 0;
    for (size_t i = 0; i < n_names; i++)
        sv[i].st_res = reqs[k++].res;
    if (dfd >= 0)
        for (size_t i = 0; i < n_names; i++)
            dv[i].st_res = reqs[k++].res;

    struct dpend dp;
    dpend_init(&dp);
    for (size_t i = 0; i < n_names && !ctx->fatal; i++) {
        const struct dent *de = (dfd >= 0 && dv[i].st_res == 0) ? &dv[i] : NULL;
        handle_entry(ctx, &dp, rel, sfd, dfd, &sv[i], de);
    }
    dpend_wait(&dp);
    dpend_destroy(&dp);

    free(sv);
    free(dv);
    free(reqs);
    close(sfd);
    if (dfd >= 0)
        close(dfd);
}

void process_entrylist(const struct shard_item *it)
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
    } else {
        ctx.budget = shard_budget(&ctx);
        entrylist_walk(&ctx, it->rel_path ? it->rel_path : "", it->paths, it->n_paths);
        flush_splits(&ctx);
        flush_bigfiles(&ctx);
        drain_splits(&ctx); /* all splits acked before the result (protocol §4.2) */
        jrn_flush(&ctx);
        if (!jrn_wait_acked(&ctx)) {
            snprintf(ctx.err, sizeof ctx.err, "journal ack timeout");
            ctx.fatal = true;
        }
        if (ctx.fatal)
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
    free(ctx.split);
    free(ctx.bigfiles);
    jrn_destroy(&ctx);
}
