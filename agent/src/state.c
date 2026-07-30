#include "agent.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <time.h>
#include <unistd.h>

/* GPFS's f_type from statfs(2) (linux/magic.h GPFS_SUPER_MAGIC); same
 * constant tools/fsprobe/fsprobe.c reports as "gpfs". */
#define GPFS_SUPER_MAGIC 0x47504653

/* ---- logging ---- */
void log_line(const char *level, const char *fmt, ...)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    char msg[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof msg, fmt, ap);
    va_end(ap);
    /* single fprintf: atomic enough for line-oriented stderr logging */
    fprintf(stderr, "{\"ts\":%lld.%03ld,\"level\":\"%s\",\"msg\":\"%s\"}\n",
            (long long)ts.tv_sec, ts.tv_nsec / 1000000, level, msg);
}

/* ---- stats ---- */
atomic_ullong g_stat_scanned;
atomic_ullong g_stat_files_copied;
atomic_ullong g_stat_bytes_copied;
atomic_ullong g_stat_meta_fixed;
atomic_ullong g_stat_errors;
atomic_uint   g_inflight;

/* ---- work queue ---- */
struct wq_node {
    struct shard_item it;
    struct wq_node   *next;
};
static struct wq_node *wq_head, *wq_tail;
static int             wq_len;
static bool            wq_down;
static pthread_mutex_t wq_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  wq_cv = PTHREAD_COND_INITIALIZER;

void wq_push(const struct shard_item *it)
{
    struct wq_node *n = calloc(1, sizeof *n);
    if (!n) {
        LOGE("oom queuing shard %llu", (unsigned long long)it->shard_id);
        return;
    }
    n->it = *it;
    pthread_mutex_lock(&wq_mu);
    if (wq_tail)
        wq_tail->next = n;
    else
        wq_head = n;
    wq_tail = n;
    wq_len++;
    pthread_cond_signal(&wq_cv);
    pthread_mutex_unlock(&wq_mu);
}

bool wq_pop(struct shard_item *out)
{
    pthread_mutex_lock(&wq_mu);
    while (!wq_head && !wq_down)
        pthread_cond_wait(&wq_cv, &wq_mu);
    if (!wq_head) {
        pthread_mutex_unlock(&wq_mu);
        return false;
    }
    struct wq_node *n = wq_head;
    wq_head = n->next;
    if (!wq_head)
        wq_tail = NULL;
    wq_len--;
    pthread_mutex_unlock(&wq_mu);
    *out = n->it;
    free(n);
    return true;
}

/* Pop the head node's item with wq_mu held; caller guarantees wq_head != NULL. */
static void wq_take_locked(struct shard_item *out)
{
    struct wq_node *n = wq_head;
    wq_head = n->next;
    if (!wq_head)
        wq_tail = NULL;
    wq_len--;
    *out = n->it;
    free(n);
}

bool wq_trypop(struct shard_item *out)
{
    pthread_mutex_lock(&wq_mu);
    if (!wq_head) {
        pthread_mutex_unlock(&wq_mu);
        return false;
    }
    wq_take_locked(out);
    pthread_mutex_unlock(&wq_mu);
    return true;
}

int wq_pop_timed(struct shard_item *out, int timeout_ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += timeout_ms / 1000;
    ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }
    pthread_mutex_lock(&wq_mu);
    while (!wq_head && !wq_down) {
        if (pthread_cond_timedwait(&wq_cv, &wq_mu, &ts) == ETIMEDOUT)
            break;
    }
    if (wq_head) {
        wq_take_locked(out);
        pthread_mutex_unlock(&wq_mu);
        return 1;
    }
    bool down = wq_down;
    pthread_mutex_unlock(&wq_mu);
    return down ? -1 : 0;
}

int wq_depth(void)
{
    pthread_mutex_lock(&wq_mu);
    int n = wq_len;
    pthread_mutex_unlock(&wq_mu);
    return n;
}

void wq_shutdown(void)
{
    pthread_mutex_lock(&wq_mu);
    wq_down = true;
    pthread_cond_broadcast(&wq_cv);
    pthread_mutex_unlock(&wq_mu);
}

/* Drop every queued shard belonging to job_id (a terminal job), handing each to
 * dispose (which frees it and its lease). Unstarted shards only — one a worker
 * already took is left to finish. Under wq_mu so no worker pops concurrently. */
int wq_drop_job(uint64_t job_id, void (*dispose)(struct shard_item *it))
{
    /* Unlink matches under the lock into a private list, then dispose of them
     * outside it — so wq_mu is never held across dispose and workers popping
     * concurrently never see a half-spliced queue. */
    struct wq_node *dropped = NULL;
    pthread_mutex_lock(&wq_mu);
    struct wq_node **pp = &wq_head;
    wq_tail = NULL;
    while (*pp) {
        struct wq_node *node = *pp;
        if (node->it.job_id == job_id) {
            *pp = node->next;     /* unlink from the queue */
            node->next = dropped; /* stash on the private dropped list */
            dropped = node;
            wq_len--;
        } else {
            wq_tail = node;       /* last survivor is the queue's new tail */
            pp = &node->next;
        }
    }
    pthread_mutex_unlock(&wq_mu);
    int n = 0;
    while (dropped) {
        struct wq_node *next = dropped->next;
        dispose(&dropped->it);
        free(dropped);
        dropped = next;
        n++;
    }
    return n;
}

/* Pop every queued (not-yet-started) shard and hand each to release, which owns
 * disposing of it (report it back + free). Runs under wq_mu so no worker can
 * pop concurrently; a shard a worker already took is running and is left alone.
 * Returns the number released. Used when the agent is told to drain. */
int wq_release_all(void (*release)(struct shard_item *it))
{
    int n = 0;
    pthread_mutex_lock(&wq_mu);
    struct wq_node *node = wq_head;
    wq_head = wq_tail = NULL;
    wq_len = 0;
    pthread_mutex_unlock(&wq_mu);
    while (node) {
        struct wq_node *next = node->next;
        release(&node->it);
        free(node);
        node = next;
        n++;
    }
    return n;
}

/* ---- outbox ---- */
int g_outbox_eventfd = -1;
static struct outmsg  *ob_head, *ob_tail;
static pthread_mutex_t ob_mu = PTHREAD_MUTEX_INITIALIZER;

/* Single-slot priority mailbox for the heartbeat. The bulk outbox above is a
 * strict FIFO with no head-of-line priority: at high worker/copy thread
 * counts the aggregate result/journal volume between one heartbeat interval
 * and the next can be large enough that a heartbeat queued at the tail waits
 * behind all of it before the writer thread's plain FIFO drain ever reaches
 * it — reproducing the same "heartbeat effectively stalled" symptom the
 * writer thread was built to fix, just moved from "blocked write" to "stuck
 * in line". A heartbeat replaces (never queues alongside) whatever's already
 * in the slot: only the newest lease-renewal snapshot is ever useful to send,
 * so coalescing here is correct, not just an optimization. */
static struct outmsg  *pri_msg;
static pthread_mutex_t pri_mu = PTHREAD_MUTEX_INITIALIZER;

int outbox_init(void)
{
    g_outbox_eventfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    return g_outbox_eventfd < 0 ? -1 : 0;
}

void out_push(uint16_t type, pb_buf *b)
{
    struct outmsg *m = calloc(1, sizeof *m);
    if (!m || b->oom) {
        LOGE("oom encoding frame type %u", type);
        free(m);
        pb_free(b);
        return;
    }
    m->type = type;
    m->buf = b->p;
    m->len = b->len;
    clock_gettime(CLOCK_MONOTONIC, &m->queued_at);
    pb_init(b); /* stolen */
    pthread_mutex_lock(&ob_mu);
    if (ob_tail)
        ob_tail->next = m;
    else
        ob_head = m;
    ob_tail = m;
    pthread_mutex_unlock(&ob_mu);
    uint64_t one = 1;
    if (write(g_outbox_eventfd, &one, sizeof one) < 0 && errno != EAGAIN)
        LOGE("outbox eventfd write: %s", strerror(errno));
}

struct outmsg *out_drain(void)
{
    pthread_mutex_lock(&ob_mu);
    struct outmsg *head = ob_head;
    ob_head = ob_tail = NULL;
    pthread_mutex_unlock(&ob_mu);
    return head;
}

void out_push_priority(uint16_t type, pb_buf *b)
{
    struct outmsg *m = calloc(1, sizeof *m);
    if (!m || b->oom) {
        LOGE("oom encoding priority frame type %u", type);
        free(m);
        pb_free(b);
        return;
    }
    m->type = type;
    m->buf = b->p;
    m->len = b->len;
    clock_gettime(CLOCK_MONOTONIC, &m->queued_at);
    pb_init(b); /* stolen */
    pthread_mutex_lock(&pri_mu);
    struct outmsg *old = pri_msg; /* superseded snapshot, if the writer hasn't sent it yet */
    pri_msg = m;
    pthread_mutex_unlock(&pri_mu);
    if (old) {
        free(old->buf);
        free(old);
    }
    uint64_t one = 1;
    if (write(g_outbox_eventfd, &one, sizeof one) < 0 && errno != EAGAIN)
        LOGE("outbox eventfd write: %s", strerror(errno));
}

struct outmsg *out_take_priority(void)
{
    pthread_mutex_lock(&pri_mu);
    struct outmsg *m = pri_msg;
    pri_msg = NULL;
    pthread_mutex_unlock(&pri_mu);
    return m;
}

/* ---- leases ----
 * Slots are index-stable for the life of a lease (never compacted/moved), so
 * the pointer a worker caches in lease_start (tl_lease) stays valid until it
 * releases the lease.
 *
 * Every operation here is O(1), via three intrusive lists threaded through
 * the fixed `leases` array (see struct lease_entry in agent.h):
 *   - free list (free_head/free_next): slots not currently holding a lease.
 *   - active list (active_head/active_next/active_prev): slots currently
 *     holding a lease, doubly-linked so lease_remove can unlink in O(1)
 *     without a scan. This is what lease_snapshot/lease_inflight/
 *     lease_job_held walk — cost proportional to leases *currently held*
 *     (bounded by roughly (workers+copy_threads)*2 in practice), not to how
 *     many leases the process has ever held.
 *   - hash table (hash[] + hash_next chains) keyed by lease_id: what
 *     lease_start/lease_remove use to find a slot in O(1) instead of
 *     scanning for it.
 *
 * An earlier version scanned [0, n_used) — a high-water mark that only ever
 * grew — for every one of these operations. At 30+ walker threads churning
 * many small, fast shards (nothing to copy), that scan's cost climbed over a
 * job's lifetime and was repeated, under one mutex, by every lease_add/
 * lease_start/lease_remove call from every walker thread *and* by
 * send_heartbeat's own two scans every heartbeat interval — contention severe
 * enough to starve the heartbeat with no trace on the network or coordinator
 * side (Send-Q, dispatch timing, store-lock timing all stayed clean). See
 * docs/DESIGN-agent.md §3.5. */
#define MAX_LEASES 8192
#define LEASE_HASH_BUCKETS 16384 /* power of 2, > MAX_LEASES for a low load factor */
static struct lease_entry leases[MAX_LEASES];
static int                free_head = 0;   /* index of first free slot, -1 = table full */
static int                active_head = -1; /* index of first active slot, -1 = none held */
static int                hash[LEASE_HASH_BUCKETS];
static pthread_mutex_t    lease_mu = PTHREAD_MUTEX_INITIALIZER;
static bool                lease_tab_init_done;

static void lease_tab_init_locked(void)
{
    if (lease_tab_init_done)
        return;
    for (int i = 0; i < MAX_LEASES; i++) {
        leases[i].free_next = (i + 1 < MAX_LEASES) ? i + 1 : -1;
        leases[i].active_next = leases[i].active_prev = -1;
        leases[i].hash_next = -1;
    }
    for (int i = 0; i < LEASE_HASH_BUCKETS; i++)
        hash[i] = -1;
    lease_tab_init_done = true;
}

/* Simple multiplicative mix: lease_id is a well-distributed random 63-bit
 * value (coordinator's newLeaseID), so a cheap mix is enough — no adversary
 * chooses lease ids. */
static unsigned hash_bucket(uint64_t id)
{
    uint64_t h = id * 0x9E3779B97F4A7C15ULL;
    return (unsigned)(h >> 48) & (LEASE_HASH_BUCKETS - 1);
}

/* caller holds lease_mu */
static int hash_find_locked(uint64_t id)
{
    for (int i = hash[hash_bucket(id)]; i != -1; i = leases[i].hash_next)
        if (leases[i].lease_id == id)
            return i;
    return -1;
}

/* caller holds lease_mu */
static void hash_insert_locked(int i)
{
    unsigned b = hash_bucket(leases[i].lease_id);
    leases[i].hash_next = hash[b];
    hash[b] = i;
}

/* caller holds lease_mu */
static void hash_remove_locked(int i)
{
    unsigned b = hash_bucket(leases[i].lease_id);
    int *p = &hash[b];
    while (*p != -1 && *p != i)
        p = &leases[*p].hash_next;
    if (*p == i)
        *p = leases[i].hash_next;
}

/* caller holds lease_mu */
static void active_link_locked(int i)
{
    leases[i].active_prev = -1;
    leases[i].active_next = active_head;
    if (active_head != -1)
        leases[active_head].active_prev = i;
    active_head = i;
}

/* caller holds lease_mu */
static void active_unlink_locked(int i)
{
    int prev = leases[i].active_prev, next = leases[i].active_next;
    if (prev != -1)
        leases[prev].active_next = next;
    else
        active_head = next;
    if (next != -1)
        leases[next].active_prev = prev;
    leases[i].active_next = leases[i].active_prev = -1;
}

/* The lease this thread is currently processing (workers run one at a time). */
static __thread struct lease_entry *tl_lease;

void lease_add(const struct shard_item *it)
{
    pthread_mutex_lock(&lease_mu);
    lease_tab_init_locked();
    int i = free_head;
    if (i == -1) {
        pthread_mutex_unlock(&lease_mu);
        LOGE("lease table full (%d); not tracking lease %llu", MAX_LEASES,
             (unsigned long long)it->lease_id);
        return;
    }
    free_head = leases[i].free_next;

    struct lease_entry *e = &leases[i];
    e->lease_id = it->lease_id;
    e->shard_id = it->shard_id;
    e->job_id = it->job_id;
    e->kind = it->kind;
    /* Long paths are truncated: this is a diagnostic label, not an identifier —
     * shard_id is the identifier. */
    snprintf(e->rel_path, sizeof e->rel_path, "%s", it->rel_path ? it->rel_path : "");
    clock_gettime(CLOCK_MONOTONIC, &e->granted_at);
    e->started_at = (struct timespec){ 0 };
    e->running = false;
    atomic_store(&e->entries_done, 0);

    hash_insert_locked(i);
    active_link_locked(i);
    pthread_mutex_unlock(&lease_mu);
}

void lease_remove(uint64_t id)
{
    pthread_mutex_lock(&lease_mu);
    lease_tab_init_locked();
    int i = hash_find_locked(id);
    if (i != -1) {
        hash_remove_locked(i);
        active_unlink_locked(i);
        leases[i].lease_id = 0; /* free_next is set below, overwriting any stale value */
        leases[i].free_next = free_head;
        free_head = i;
    }
    pthread_mutex_unlock(&lease_mu);
}

/* True if any lease for this job is still held (a worker may be using the job's
 * cached root fds). Guards opts_evict from closing fds out from under a shard. */
bool lease_job_held(uint64_t job_id)
{
    bool held = false;
    pthread_mutex_lock(&lease_mu);
    lease_tab_init_locked();
    for (int i = active_head; i != -1; i = leases[i].active_next) {
        if (leases[i].job_id == job_id) {
            held = true;
            break;
        }
    }
    pthread_mutex_unlock(&lease_mu);
    return held;
}

void lease_start(uint64_t id)
{
    pthread_mutex_lock(&lease_mu);
    lease_tab_init_locked();
    tl_lease = NULL;
    int i = hash_find_locked(id);
    if (i != -1) {
        clock_gettime(CLOCK_MONOTONIC, &leases[i].started_at);
        leases[i].running = true;
        tl_lease = &leases[i];
    }
    pthread_mutex_unlock(&lease_mu);
}

void lease_end(void)
{
    tl_lease = NULL;
}

void lease_publish(uint64_t entries_done)
{
    if (tl_lease)
        atomic_store(&tl_lease->entries_done, entries_done);
}

size_t lease_snapshot(uint64_t *dst, size_t cap)
{
    size_t n = 0;
    pthread_mutex_lock(&lease_mu);
    lease_tab_init_locked();
    for (int i = active_head; i != -1 && n < cap; i = leases[i].active_next)
        dst[n++] = leases[i].lease_id;
    pthread_mutex_unlock(&lease_mu);
    return n;
}

static uint32_t ms_since(const struct timespec *t0, const struct timespec *now)
{
    if (t0->tv_sec == 0 && t0->tv_nsec == 0)
        return 0;
    int64_t ms = (int64_t)(now->tv_sec - t0->tv_sec) * 1000 +
                 (now->tv_nsec - t0->tv_nsec) / 1000000;
    return ms > 0 ? (uint32_t)ms : 0;
}

size_t lease_inflight(struct inflight_view *dst, size_t cap)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    size_t n = 0;
    pthread_mutex_lock(&lease_mu);
    lease_tab_init_locked();
    for (int i = active_head; i != -1 && n < cap; i = leases[i].active_next) {
        const struct lease_entry *e = &leases[i];
        dst[n].lease_id = e->lease_id;
        dst[n].shard_id = e->shard_id;
        dst[n].job_id = e->job_id;
        dst[n].kind = e->kind;
        memcpy(dst[n].rel_path, e->rel_path, sizeof dst[n].rel_path);
        dst[n].held_ms = ms_since(&e->granted_at, &now);
        dst[n].running_ms = e->running ? ms_since(&e->started_at, &now) : 0;
        dst[n].running = e->running;
        dst[n].entries_done = atomic_load(&e->entries_done);
        n++;
    }
    pthread_mutex_unlock(&lease_mu);
    return n;
}

/* ---- job options ---- */
#define MAX_JOBS 64
static struct opts_entry opts_tab[MAX_JOBS];
static size_t            n_opts;
static pthread_mutex_t   opts_mu = PTHREAD_MUTEX_INITIALIZER;

static int mkdir_p(const char *path)
{
    char buf[PATH_MAX];
    if (strlen(path) >= sizeof buf) {
        errno = ENAMETOOLONG;
        return -1;
    }
    strcpy(buf, path);
    for (char *p = buf + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(buf, 0755) < 0 && errno != EEXIST)
                return -1;
            *p = '/';
        }
    }
    if (mkdir(buf, 0755) < 0 && errno != EEXIST)
        return -1;
    return 0;
}

int opts_store(const struct job_options *o)
{
    /* Size the statx ring from this job's tuning knob (no-op if unset). */
    uring_set_depth(o->statx_batch);
    pthread_mutex_lock(&opts_mu);
    size_t free_slot = MAX_JOBS; /* first slot freed by opts_evict, if any */
    for (size_t i = 0; i < n_opts; i++) {
        if (opts_tab[i].o.job_id == 0) {
            if (free_slot == MAX_JOBS)
                free_slot = i;
            continue;
        }
        if (opts_tab[i].o.job_id == o->job_id) {
            if (opts_tab[i].o.options_hash != o->options_hash) {
                /* Roots are immutable per design; only tunables change. */
                int sfd = opts_tab[i].src_fd, dfd = opts_tab[i].dst_fd;
                opts_tab[i].o = *o;
                opts_tab[i].src_fd = sfd;
                opts_tab[i].dst_fd = dfd;
                LOGI("job %llu options updated (hash %llx)",
                     (unsigned long long)o->job_id,
                     (unsigned long long)o->options_hash);
            }
            pthread_mutex_unlock(&opts_mu);
            return 0;
        }
    }
    /* Reuse a slot a completed/cancelled job vacated before growing the table. */
    size_t slot = free_slot;
    if (slot == MAX_JOBS) {
        if (n_opts == MAX_JOBS) {
            pthread_mutex_unlock(&opts_mu);
            LOGE("job options table full");
            return -1;
        }
        slot = n_opts;
    }

    int sfd = open(o->src_root, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (sfd < 0) {
        pthread_mutex_unlock(&opts_mu);
        LOGE("open src root %s: %s", o->src_root, strerror(errno));
        return -1;
    }
    if (!o->dry_run && mkdir_p(o->dst_root) < 0) {
        pthread_mutex_unlock(&opts_mu);
        close(sfd);
        LOGE("create dst root %s: %s", o->dst_root, strerror(errno));
        return -1;
    }
    int dfd = open(o->dst_root, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0 && !o->dry_run) {
        pthread_mutex_unlock(&opts_mu);
        close(sfd);
        LOGE("open dst root %s: %s", o->dst_root, strerror(errno));
        return -1;
    }
    /* The io_uring copy engine's WRITE_FIXED silently ignores the submitted
     * length on GPFS, flushing the whole 1 MiB registered buffer regardless
     * of file size (ucopy.c); disable it for this job up front rather than
     * let every file discover the bad write, log a warning, and redo
     * serially (copy.c's size-mismatch fallback). Checked on whichever root
     * is the copy destination for real jobs; both roots for dry-run, where
     * dst_fd is never opened. */
    struct statfs sfs;
    bool gpfs = false;
    if (dfd >= 0 && fstatfs(dfd, &sfs) == 0 && sfs.f_type == GPFS_SUPER_MAGIC)
        gpfs = true;
    if (!gpfs && fstatfs(sfd, &sfs) == 0 && sfs.f_type == GPFS_SUPER_MAGIC)
        gpfs = true;

    opts_tab[slot].o = *o;
    opts_tab[slot].src_fd = sfd;
    opts_tab[slot].dst_fd = dfd;
    opts_tab[slot].no_uring_copy = gpfs;
    if (slot == n_opts)
        n_opts++;
    pthread_mutex_unlock(&opts_mu);
    LOGI("job %llu (%s): %s -> %s%s", (unsigned long long)o->job_id, o->job_name,
         o->src_root, o->dst_root, o->dry_run ? " [dry-run]" : "");
    if (gpfs)
        LOGI("job %llu: GPFS detected, io_uring copy engine disabled for this job",
             (unsigned long long)o->job_id);
    return 0;
}

const struct opts_entry *opts_get(uint64_t job_id)
{
    pthread_mutex_lock(&opts_mu);
    for (size_t i = 0; i < n_opts; i++) {
        if (opts_tab[i].o.job_id == job_id) {
            pthread_mutex_unlock(&opts_mu);
            /* Slots are freed in place (never moved) so this pointer stays valid
             * until the job is evicted, which only happens once the job is
             * terminal and holds no lease — no caller can be using it then. */
            return &opts_tab[i];
        }
    }
    pthread_mutex_unlock(&opts_mu);
    return NULL;
}

/* Release a terminal job's cached options: close the source/destination root
 * fds (the handles lsof shows pinning both mounts) and free the slot for reuse.
 * Called on CMD_CANCEL_JOB, which the coordinator sends for both cancel and
 * completion. Skips the close while a lease for the job is still held — a worker
 * may be mid-shard using the fds — leaving the entry until process exit; the
 * common completed-job case holds no lease and always releases. */
void opts_evict(uint64_t job_id)
{
    if (lease_job_held(job_id)) {
        LOGW("job %llu still has work in flight; deferring options release",
             (unsigned long long)job_id);
        return;
    }
    pthread_mutex_lock(&opts_mu);
    for (size_t i = 0; i < n_opts; i++) {
        if (opts_tab[i].o.job_id != job_id)
            continue;
        if (opts_tab[i].src_fd >= 0)
            close(opts_tab[i].src_fd);
        if (opts_tab[i].dst_fd >= 0)
            close(opts_tab[i].dst_fd);
        memset(&opts_tab[i], 0, sizeof opts_tab[i]); /* job_id 0 = free slot */
        opts_tab[i].src_fd = opts_tab[i].dst_fd = -1;
        pthread_mutex_unlock(&opts_mu);
        LOGI("job %llu options released (root fds closed)",
             (unsigned long long)job_id);
        return;
    }
    pthread_mutex_unlock(&opts_mu);
}

size_t opts_cached(struct cached_opts *dst, size_t cap)
{
    pthread_mutex_lock(&opts_mu);
    size_t n = 0;
    for (size_t i = 0; i < n_opts && n < cap; i++) {
        if (opts_tab[i].o.job_id == 0) /* slot freed by opts_evict */
            continue;
        dst[n].job_id = opts_tab[i].o.job_id;
        dst[n].options_hash = opts_tab[i].o.options_hash;
        n++;
    }
    pthread_mutex_unlock(&opts_mu);
    return n;
}

/* ---- pending splits ---- */
static struct split_wait *split_head;
static pthread_mutex_t    split_mu = PTHREAD_MUTEX_INITIALIZER;

void split_register(struct split_wait *w)
{
    sem_init(&w->sem, 0, 0);
    pthread_mutex_lock(&split_mu);
    w->next = split_head;
    split_head = w;
    pthread_mutex_unlock(&split_mu);
}

bool split_resolve(uint64_t parent, uint64_t seq)
{
    pthread_mutex_lock(&split_mu);
    for (struct split_wait *w = split_head; w; w = w->next) {
        if (w->parent == parent && w->seq == seq) {
            pthread_mutex_unlock(&split_mu);
            sem_post(&w->sem);
            return true;
        }
    }
    pthread_mutex_unlock(&split_mu);
    return false;
}

void split_unregister(struct split_wait *w)
{
    pthread_mutex_lock(&split_mu);
    for (struct split_wait **pp = &split_head; *pp; pp = &(*pp)->next) {
        if (*pp == w) {
            *pp = w->next;
            break;
        }
    }
    pthread_mutex_unlock(&split_mu);
    sem_destroy(&w->sem);
}
