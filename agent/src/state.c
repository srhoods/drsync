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
#include <time.h>
#include <unistd.h>

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

/* ---- leases ----
 * Slots are freed in place (lease_id = 0) rather than compacted, so the pointer
 * a worker caches in lease_start stays valid until it releases the lease. n_used
 * is a high-water mark bounding the scans, not a count. */
#define MAX_LEASES 8192
static struct lease_entry leases[MAX_LEASES];
static size_t             n_used;
static pthread_mutex_t    lease_mu = PTHREAD_MUTEX_INITIALIZER;

/* The lease this thread is currently processing (workers run one at a time). */
static __thread struct lease_entry *tl_lease;

void lease_add(const struct shard_item *it)
{
    pthread_mutex_lock(&lease_mu);
    struct lease_entry *e = NULL;
    for (size_t i = 0; i < n_used; i++) {
        if (leases[i].lease_id == 0) {
            e = &leases[i];
            break;
        }
    }
    if (!e && n_used < MAX_LEASES)
        e = &leases[n_used++];
    if (!e) {
        pthread_mutex_unlock(&lease_mu);
        LOGE("lease table full (%d); not tracking lease %llu", MAX_LEASES,
             (unsigned long long)it->lease_id);
        return;
    }
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
    pthread_mutex_unlock(&lease_mu);
}

void lease_remove(uint64_t id)
{
    pthread_mutex_lock(&lease_mu);
    for (size_t i = 0; i < n_used; i++) {
        if (leases[i].lease_id == id) {
            leases[i].lease_id = 0; /* freed in place; slot may be reused */
            break;
        }
    }
    pthread_mutex_unlock(&lease_mu);
}

/* True if any lease for this job is still held (a worker may be using the job's
 * cached root fds). Guards opts_evict from closing fds out from under a shard. */
bool lease_job_held(uint64_t job_id)
{
    bool held = false;
    pthread_mutex_lock(&lease_mu);
    for (size_t i = 0; i < n_used; i++) {
        if (leases[i].lease_id && leases[i].job_id == job_id) {
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
    tl_lease = NULL;
    for (size_t i = 0; i < n_used; i++) {
        if (leases[i].lease_id == id) {
            clock_gettime(CLOCK_MONOTONIC, &leases[i].started_at);
            leases[i].running = true;
            tl_lease = &leases[i];
            break;
        }
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
    for (size_t i = 0; i < n_used && n < cap; i++) {
        if (leases[i].lease_id)
            dst[n++] = leases[i].lease_id;
    }
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
    for (size_t i = 0; i < n_used && n < cap; i++) {
        const struct lease_entry *e = &leases[i];
        if (!e->lease_id)
            continue;
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
    opts_tab[slot].o = *o;
    opts_tab[slot].src_fd = sfd;
    opts_tab[slot].dst_fd = dfd;
    if (slot == n_opts)
        n_opts++;
    pthread_mutex_unlock(&opts_mu);
    LOGI("job %llu (%s): %s -> %s%s", (unsigned long long)o->job_id, o->job_name,
         o->src_root, o->dst_root, o->dry_run ? " [dry-run]" : "");
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
