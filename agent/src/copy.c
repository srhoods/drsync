/* Copy pool (docs/DESIGN-agent.md §1/§3, slice 2): dedicated copy threads fed
 * by a bounded queue — walkers keep scanning while data moves, and a full
 * queue blocks the walker (backpressure) instead of growing memory.
 *
 * Copy strategy per file: sparse extent copy (SEEK_DATA/SEEK_HOLE) when the
 * file is sparse, else copy_file_range (server-side copy / reflink when the
 * mount pair supports it), else a byte-copy fallback. The byte-copy fallback
 * uses the io_uring registered-buffer engine (ucopy.c) when io_uring is
 * available — reads of the next block overlap writes of the current one — and a
 * serial read/write loop otherwise. */
#include "agent.h"

#define XXH_INLINE_ALL
#include "xxhash.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define COPY_BUF_SIZE (1 << 20)

/* Intra-file parallelism for huge files: a file at/above chunk_threshold on the
 * byte-copy path is split into contiguous ranges copied concurrently into one
 * pre-fallocated temp (pread/pwrite are position-based, so one fd is shared
 * safely). Bounded so a single giant file can't oversubscribe the copy pool. */
#define CHUNK_RANGE_BYTES  (256ULL << 20)
#define MAX_CHUNK_THREADS  8

/* ---- per-directory pending counters ---- */
void dpend_init(struct dpend *dp)
{
    pthread_mutex_init(&dp->mu, NULL);
    pthread_cond_init(&dp->cv, NULL);
    dp->n = 0;
}

void dpend_add(struct dpend *dp)
{
    pthread_mutex_lock(&dp->mu);
    dp->n++;
    pthread_mutex_unlock(&dp->mu);
}

void dpend_done(struct dpend *dp)
{
    pthread_mutex_lock(&dp->mu);
    if (--dp->n == 0)
        pthread_cond_signal(&dp->cv);
    pthread_mutex_unlock(&dp->mu);
}

void dpend_wait(struct dpend *dp)
{
    pthread_mutex_lock(&dp->mu);
    while (dp->n > 0)
        pthread_cond_wait(&dp->cv, &dp->mu);
    pthread_mutex_unlock(&dp->mu);
}

void dpend_destroy(struct dpend *dp)
{
    pthread_mutex_destroy(&dp->mu);
    pthread_cond_destroy(&dp->cv);
}

/* ---- queue ---- */
struct copy_task {
    struct walk_ctx  *ctx;
    struct dpend     *dp;
    int               sfd, dfd;
    struct estat      ss;
    char              name[NAME_MAX + 1];
    char             *rel; /* job-root-relative path (journal identity) */
    struct copy_task *next;
};

static struct copy_task *cq_head, *cq_tail;
static int               cq_len, cq_cap;
static bool              cq_down;
static pthread_mutex_t   cq_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t    cq_not_empty = PTHREAD_COND_INITIALIZER;
static pthread_cond_t    cq_not_full = PTHREAD_COND_INITIALIZER;
static pthread_t        *cq_threads;
static int               cq_nthreads;

int cp_depth(void)
{
    pthread_mutex_lock(&cq_mu);
    int n = cq_len;
    pthread_mutex_unlock(&cq_mu);
    return n;
}

void cp_submit(struct walk_ctx *ctx, struct dpend *dp, int sfd, int dfd,
               const char *dir_rel, const char *name, const struct estat *ss)
{
    struct copy_task *t = calloc(1, sizeof *t);
    if (!t) {
        walk_err(ctx, "oom copy task", name);
        return;
    }
    t->ctx = ctx;
    t->dp = dp;
    t->sfd = sfd;
    t->dfd = dfd;
    t->ss = *ss;
    snprintf(t->name, sizeof t->name, "%s", name);
    if (asprintf(&t->rel, "%s%s%s", dir_rel, dir_rel[0] ? "/" : "", name) < 0) {
        walk_err(ctx, "oom copy task", name);
        free(t);
        return;
    }

    dpend_add(dp); /* before enqueue: completion may race the walker */
    pthread_mutex_lock(&cq_mu);
    while (cq_len >= cq_cap && !cq_down)
        pthread_cond_wait(&cq_not_full, &cq_mu);
    if (cq_down) {
        pthread_mutex_unlock(&cq_mu);
        free(t->rel);
        free(t);
        dpend_done(dp);
        return;
    }
    if (cq_tail)
        cq_tail->next = t;
    else
        cq_head = t;
    cq_tail = t;
    cq_len++;
    pthread_cond_signal(&cq_not_empty);
    pthread_mutex_unlock(&cq_mu);
}

/* Detach the head task with cq_mu held; caller guarantees cq_head != NULL. */
static struct copy_task *cq_take_locked(void)
{
    struct copy_task *t = cq_head;
    cq_head = t->next;
    if (!cq_head)
        cq_tail = NULL;
    cq_len--;
    pthread_cond_signal(&cq_not_full);
    return t;
}

static struct copy_task *cq_trypop(void)
{
    pthread_mutex_lock(&cq_mu);
    struct copy_task *t = cq_head ? cq_take_locked() : NULL;
    pthread_mutex_unlock(&cq_mu);
    return t;
}

/* Blocks (no timeout) until a task or shutdown. Returns 1/-1 like the others. */
static int cq_pop_block(struct copy_task **out)
{
    pthread_mutex_lock(&cq_mu);
    while (!cq_head && !cq_down)
        pthread_cond_wait(&cq_not_empty, &cq_mu);
    if (cq_head) {
        *out = cq_take_locked();
        pthread_mutex_unlock(&cq_mu);
        return 1;
    }
    pthread_mutex_unlock(&cq_mu);
    return -1;
}

static int cq_pop_timed(struct copy_task **out, int timeout_ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += timeout_ms / 1000;
    ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }
    pthread_mutex_lock(&cq_mu);
    while (!cq_head && !cq_down) {
        if (pthread_cond_timedwait(&cq_not_empty, &cq_mu, &ts) == ETIMEDOUT)
            break;
    }
    if (cq_head) {
        *out = cq_take_locked();
        pthread_mutex_unlock(&cq_mu);
        return 1;
    }
    bool down = cq_down;
    pthread_mutex_unlock(&cq_mu);
    return down ? -1 : 0;
}

static void run_copy_task(struct copy_task *t)
{
    copy_file_task(t->ctx, t->sfd, t->dfd, t->name, t->rel, &t->ss);
    dpend_done(t->dp);
    free(t->rel);
    free(t);
}

bool cp_drain_one(void)
{
    struct copy_task *t = cq_trypop();
    if (!t)
        return false;
    run_copy_task(t);
    return true;
}

static void *copy_thread(void *arg)
{
    bool may_steal = (bool)(intptr_t)arg;
    struct shard_item it;

    if (!may_steal) { /* pure drainer: always available to drain the copy queue */
        for (;;) {
            struct copy_task *t;
            if (cq_pop_block(&t) < 0)
                return NULL;
            run_copy_task(t);
        }
    }

    /* Generalist: prefer copies, but when the copy queue is empty steal a shard
     * and help crawl. The reserved drainer(s) guarantee this thread's own
     * enqueued copies (and everyone's) still drain while it blocks in
     * dpend_wait, so the pool cannot deadlock. */
    for (;;) {
        struct copy_task *t = cq_trypop();
        if (t) {
            run_copy_task(t);
            continue;
        }
        if (wq_trypop(&it)) {
            atomic_fetch_add(&g_steal_shards, 1);
            process_item(&it);
            continue;
        }
        int r = cq_pop_timed(&t, STEAL_POLL_MS);
        if (r < 0)
            return NULL;
        if (r > 0)
            run_copy_task(t);
        /* r == 0: timed out — loop to recheck the shard queue */
    }
}

int cp_init(int threads, int queue_cap, int reserve)
{
    cq_nthreads = threads;
    cq_cap = queue_cap;
    cq_threads = calloc((size_t)threads, sizeof *cq_threads);
    if (!cq_threads)
        return -1;
    for (int i = 0; i < threads; i++) {
        bool may_steal = i >= reserve; /* first `reserve` stay pure drainers */
        if (pthread_create(&cq_threads[i], NULL, copy_thread,
                           (void *)(intptr_t)may_steal) != 0)
            return -1;
    }
    return 0;
}

void cp_shutdown(void)
{
    pthread_mutex_lock(&cq_mu);
    cq_down = true;
    pthread_cond_broadcast(&cq_not_empty);
    pthread_cond_broadcast(&cq_not_full);
    pthread_mutex_unlock(&cq_mu);
    for (int i = 0; i < cq_nthreads; i++)
        pthread_join(cq_threads[i], NULL);
    free(cq_threads);
}

/* ---- the copy itself (runs on copy threads) ---- */

static int64_t ts_diff_ns(const struct timespec *a, const struct timespec *b)
{
    int64_t d = ((int64_t)a->tv_sec - b->tv_sec) * 1000000000 +
                (a->tv_nsec - b->tv_nsec);
    return d < 0 ? -d : d;
}

void apply_meta(struct walk_ctx *ctx, int fd, const struct estat *ss,
                const char *path)
{
    const struct job_options *o = &ctx->oe->o;
    /* xattrs/ACLs are applied by the caller first (needs the src fd);
     * chown before chmod: chown clears setuid/setgid (design §5) */
    if (o->meta_owner && fchown(fd, ss->uid, ss->gid) < 0)
        walk_err(ctx, "chown", path);
    if (o->meta_mode && fchmod(fd, ss->mode & 07777) < 0)
        walk_err(ctx, "chmod", path);
    if (o->meta_times) {
        struct timespec ts[2] = { ss->atim, ss->mtim };
        if (futimens(fd, ts) < 0)
            walk_err(ctx, "utimens", path);
    }
}

/* Sparse copy via SEEK_DATA/SEEK_HOLE (design §4): only data extents are
 * written; ftruncate sets the size so holes stay holes on the destination.
 * Returns 0 done, -1 unsupported (caller falls back to dense), -2 failed
 * (already counted). */
static int copy_sparse(struct walk_ctx *ctx, int in, int out,
                       const struct estat *ss, const char *name,
                       uint8_t *buf, uint64_t *copied, XXH3_state_t *xst)
{
    off_t data = lseek(in, 0, SEEK_DATA);
    if (data < 0 && errno != ENXIO)
        return -1; /* EINVAL: SEEK_DATA unsupported (e.g. NFS < 4.2) */
    if (ftruncate(out, (off_t)ss->size) < 0) {
        walk_err(ctx, "ftruncate", name);
        return -2;
    }
    while (data >= 0 && (uint64_t)data < ss->size) {
        off_t hole = lseek(in, data, SEEK_HOLE);
        if (hole < 0) {
            walk_err(ctx, "seek_hole", name);
            return -2;
        }
        off_t off = data;
        while (off < hole) {
            ssize_t r = pread(in, buf, COPY_BUF_SIZE, off);
            size_t want = (size_t)(hole - off) < COPY_BUF_SIZE
                              ? (size_t)(hole - off) : COPY_BUF_SIZE;
            if (r < 0) {
                if (errno == EINTR)
                    continue;
                walk_err(ctx, "read", name);
                return -2;
            }
            if (r == 0)
                break; /* src shrank; caught by the gen check */
            if ((size_t)r > want)
                r = (ssize_t)want;
            for (ssize_t w0 = 0; w0 < r;) {
                ssize_t w = pwrite(out, buf + w0, (size_t)(r - w0), off + w0);
                if (w < 0) {
                    if (errno == EINTR)
                        continue;
                    walk_err(ctx, "write", name);
                    return -2;
                }
                w0 += w;
            }
            if (xst)
                XXH3_128bits_update(xst, buf, (size_t)r);
            off += r;
            *copied += (uint64_t)r;
        }
        data = lseek(in, hole, SEEK_DATA);
        if (data < 0 && errno != ENXIO) {
            walk_err(ctx, "seek_data", name);
            return -2;
        }
    }
    return 0;
}

/* ---- parallel chunked copy (huge files) ---- */
struct range_arg {
    int    in, out;
    off_t  start, end;
    int    err; /* errno of the first failure in this range, else 0 */
};

static void *range_copy(void *arg)
{
    struct range_arg *r = arg;
    uint8_t *buf = malloc(COPY_BUF_SIZE);
    if (!buf) {
        r->err = ENOMEM;
        return NULL;
    }
    off_t off = r->start;
    while (off < r->end) {
        size_t want = (size_t)(r->end - off);
        if (want > COPY_BUF_SIZE)
            want = COPY_BUF_SIZE;
        ssize_t rd = pread(r->in, buf, want, off);
        if (rd < 0) {
            if (errno == EINTR)
                continue;
            r->err = errno;
            break;
        }
        if (rd == 0)
            break; /* source shrank mid-copy; the gen check aborts the file */
        ssize_t done = 0;
        while (done < rd) {
            ssize_t w = pwrite(r->out, buf + done, (size_t)(rd - done), off + done);
            if (w < 0) {
                if (errno == EINTR)
                    continue;
                r->err = errno;
                goto out;
            }
            done += w;
        }
        off += rd;
    }
out:
    free(buf);
    return NULL;
}

/* Copies [0,size) of in→out with up to MAX_CHUNK_THREADS workers. Returns 0 on
 * success, -1 on failure (errno set, already accounted by the caller's drop). */
static int copy_ranges_parallel(struct walk_ctx *ctx, int in, int out,
                                uint64_t size, const char *name)
{
    /* Reserve space up front: contiguity + early ENOSPC before we fan out.
     * Non-fatal if the fs can't preallocate (tmpfs/NFS may return EOPNOTSUPP). */
    if (fallocate(out, 0, 0, (off_t)size) < 0 &&
        errno != EOPNOTSUPP && errno != ENOSYS) {
        walk_err(ctx, "fallocate", name);
        return -1;
    }
    int nthreads = (int)((size + CHUNK_RANGE_BYTES - 1) / CHUNK_RANGE_BYTES);
    if (nthreads < 1)
        nthreads = 1;
    if (nthreads > MAX_CHUNK_THREADS)
        nthreads = MAX_CHUNK_THREADS;
    uint64_t per = (size + (uint64_t)nthreads - 1) / (uint64_t)nthreads;
    LOGI("chunked copy: %s (%llu bytes, %d ranges)", name,
         (unsigned long long)size, nthreads);

    pthread_t th[MAX_CHUNK_THREADS];
    struct range_arg args[MAX_CHUNK_THREADS] = {0};
    int created = 0;
    int rc = 0;
    for (int i = 0; i < nthreads; i++) {
        off_t s = (off_t)((uint64_t)i * per);
        off_t e = (off_t)((uint64_t)(i + 1) * per);
        if ((uint64_t)e > size)
            e = (off_t)size;
        if (s >= e)
            break;
        args[created] = (struct range_arg){ .in = in, .out = out, .start = s, .end = e };
        if (pthread_create(&th[created], NULL, range_copy, &args[created]) != 0) {
            /* Fall back to copying this range inline, then stop spawning. */
            range_copy(&args[created]);
            created++;
            break;
        }
        created++;
    }
    for (int i = 0; i < created; i++)
        pthread_join(th[i], NULL);
    for (int i = 0; i < created; i++)
        if (args[i].err) {
            errno = args[i].err;
            walk_err(ctx, "chunk copy", name);
            rc = -1;
        }
    return rc;
}

/* ucopy sink: fold each block into the running xxh3 state (design §3). */
static void hash_sink(void *arg, const void *data, size_t n)
{
    XXH3_128bits_update((XXH3_state_t *)arg, data, n);
}

void copy_file_task(struct walk_ctx *ctx, int sfd, int dfd, const char *name,
                    const char *rel, const struct estat *ss)
{
    const struct job_options *o = &ctx->oe->o;

    static __thread uint8_t *buf;
    if (!buf) {
        buf = malloc(COPY_BUF_SIZE);
        if (!buf) {
            walk_err(ctx, "oom copy buffer", name);
            return;
        }
    }

    int in = openat(sfd, name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (in < 0) {
        walk_err(ctx, "open src", name);
        return;
    }
    /* The (job, pass) tag in the name keeps a concurrent walk's orphan sweep
     * from reclaiming this temp as crash residue while we are writing it. */
    char tmp[NAME_MAX + 1];
    unsigned seq = __atomic_fetch_add(&ctx->tmp_seq, 1, __ATOMIC_RELAXED);
    temp_name_fmt(tmp, sizeof tmp,
                  o->temp_prefix[0] ? o->temp_prefix : ".drsync.tmp.",
                  ctx->it->job_id, ctx->it->pass_no, ctx->it->shard_id, seq);
    int out = openat(dfd, tmp, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (out < 0) {
        walk_err(ctx, "create temp", name);
        close(in);
        return;
    }

    /* Engine selection: sparse extent copy → copy_file_range → read/write.
     * The inline source checksum (design §3: "free — folded into the read
     * loop") exists only when bytes flow through our buffers: sparse hashes
     * data extents only, copy_file_range never surfaces the data (hash 0 =
     * absent in the journal). The verify pass re-reads regardless. */
    uint64_t copied = 0;
    bool data_done = false;
    bool hashed = false;
    static __thread XXH3_state_t *xst;
    if (!xst)
        xst = XXH3_createState();
    XXH3_128bits_reset(xst);

    if (o->preserve_sparse && ss->blocks * 512 + 4096 < ss->size) {
        int rc = copy_sparse(ctx, in, out, ss, name, buf, &copied, NULL);
        if (rc == -2)
            goto drop;
        if (rc == 0)
            data_done = true;
        /* rc == -1: no SEEK_DATA on this mount — dense engines below */
    }

    if (!data_done) {
        /* server-side copy / reflink first (unless the job disabled it); falls
         * back to the byte-copy path on the first byte. */
        bool cfr = o->server_side_copy != SSC_OFF;
        while (cfr && copied < ss->size) {
            ssize_t w = copy_file_range(in, NULL, out, NULL,
                                        (size_t)(ss->size - copied), 0);
            if (w > 0) {
                copied += (uint64_t)w;
                continue;
            }
            if (w == 0)
                break; /* src shrank; caught by the gen check below */
            if (copied == 0 &&
                (errno == EXDEV || errno == EINVAL || errno == ENOSYS ||
                 errno == EOPNOTSUPP || errno == EBADF)) {
                if (o->server_side_copy == SSC_REQUIRE) {
                    walk_err(ctx, "server-side copy required but unavailable", name);
                    goto drop;
                }
                cfr = false; /* mount pair can't do it: byte-copy path */
                break;
            }
            if (errno == EINTR)
                continue;
            walk_err(ctx, "copy_file_range", name);
            goto drop;
        }
        if (!cfr && o->chunk_threshold && ss->size >= o->chunk_threshold) {
            /* Huge file: copy its ranges in parallel. No inline hash (ranges
             * complete out of order) — the verify pass re-reads (design §3). */
            if (copy_ranges_parallel(ctx, in, out, ss->size, name) < 0)
                goto drop;
            copied = ss->size;
        } else if (!cfr && ucopy_available()) {
            /* io_uring registered-buffer copy: read of the next block overlaps
             * the write of the current one, and the inline hash is fed in stream
             * order via the sink. */
            int64_t rc = ucopy_run(in, out, ss->size, hash_sink, xst);
            if (rc < 0) {
                errno = (int)-rc;
                walk_err(ctx, "io_uring copy", name);
                goto drop;
            }
            copied = (uint64_t)rc;
            hashed = copied > 0;
        } else if (!cfr) {
            /* Serial fallback when io_uring is unavailable. */
            for (;;) {
                ssize_t r = read(in, buf, COPY_BUF_SIZE);
                if (r == 0)
                    break;
                if (r < 0) {
                    if (errno == EINTR)
                        continue;
                    walk_err(ctx, "read", name);
                    goto drop;
                }
                for (ssize_t off = 0; off < r;) {
                    ssize_t w = write(out, buf + off, (size_t)(r - off));
                    if (w < 0) {
                        if (errno == EINTR)
                            continue;
                        walk_err(ctx, "write", name);
                        goto drop;
                    }
                    off += w;
                }
                XXH3_128bits_update(xst, buf, (size_t)r);
                hashed = true;
                copied += (uint64_t)r;
            }
        }
    }

    /* source changed under us? re-diffed next pass (design §3 step 5) */
    struct stat now;
    if (fstat(in, &now) == 0 &&
        ((uint64_t)now.st_size != ss->size ||
         ts_diff_ns(&now.st_mtim, &ss->mtim) > 0)) {
        LOGW("shard %llu: source changed mid-copy: %s",
             (unsigned long long)ctx->it->shard_id, name);
        CTR_ADD(ctx->c.errors, 1);
        atomic_fetch_add(&g_stat_errors, 1);
        jrn_emit(ctx, JR_SRC_CHANGED, rel, ss, NULL, 0, NULL);
        goto drop;
    }
    /* xattrs + ACLs before closing the src fd (design §5 order: xattrs →
     * ACLs → chown → chmod → times) */
    xattr_copy_fd(ctx, in, out, name);
    close(in);
    in = -1;

    if (o->fsync_per_file && fdatasync(out) < 0) {
        walk_err(ctx, "fsync", name);
        goto drop;
    }
    apply_meta(ctx, out, ss, name);
    if (renameat(dfd, tmp, dfd, name) < 0) {
        walk_err(ctx, "rename", name);
        goto drop;
    }
    close(out);
    CTR_ADD(ctx->c.files_copied, 1);
    CTR_ADD(ctx->c.bytes_copied, copied);
    atomic_fetch_add(&g_stat_files_copied, 1);
    atomic_fetch_add(&g_stat_bytes_copied, copied);
    if (hashed && copied == ss->size) {
        XXH128_hash_t h = XXH3_128bits_digest(xst);
        jrn_emit_hash(ctx, JR_COPIED, rel, ss, NULL, h.low64, h.high64);
    } else {
        jrn_emit(ctx, JR_COPIED, rel, ss, NULL, 0, NULL);
    }
    return;
drop:
    if (in >= 0)
        close(in);
    close(out);
    unlinkat(dfd, tmp, 0);
}
