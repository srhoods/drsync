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

/* ---- leases ---- */
#define MAX_LEASES 8192
static uint64_t        leases[MAX_LEASES];
static size_t          n_leases;
static pthread_mutex_t lease_mu = PTHREAD_MUTEX_INITIALIZER;

void lease_add(uint64_t id)
{
    pthread_mutex_lock(&lease_mu);
    if (n_leases < MAX_LEASES)
        leases[n_leases++] = id;
    pthread_mutex_unlock(&lease_mu);
}

void lease_remove(uint64_t id)
{
    pthread_mutex_lock(&lease_mu);
    for (size_t i = 0; i < n_leases; i++) {
        if (leases[i] == id) {
            leases[i] = leases[--n_leases];
            break;
        }
    }
    pthread_mutex_unlock(&lease_mu);
}

size_t lease_snapshot(uint64_t *dst, size_t cap)
{
    pthread_mutex_lock(&lease_mu);
    size_t n = n_leases < cap ? n_leases : cap;
    memcpy(dst, leases, n * sizeof *dst);
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
    for (size_t i = 0; i < n_opts; i++) {
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
    if (n_opts == MAX_JOBS) {
        pthread_mutex_unlock(&opts_mu);
        LOGE("job options table full");
        return -1;
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
    opts_tab[n_opts].o = *o;
    opts_tab[n_opts].src_fd = sfd;
    opts_tab[n_opts].dst_fd = dfd;
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
            return &opts_tab[i]; /* entries are never removed or moved */
        }
    }
    pthread_mutex_unlock(&opts_mu);
    return NULL;
}

size_t opts_cached(struct cached_opts *dst, size_t cap)
{
    pthread_mutex_lock(&opts_mu);
    size_t n = n_opts < cap ? n_opts : cap;
    for (size_t i = 0; i < n; i++) {
        dst[i].job_id = opts_tab[i].o.job_id;
        dst[i].options_hash = opts_tab[i].o.options_hash;
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
