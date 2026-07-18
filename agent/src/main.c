/* drsync-agent — data-plane agent.
 * Control thread: socket + heartbeat/stats timers + outbox. Workers: shards.
 * mTLS to the coordinator when -A/-E/-K are given (tls.c); plaintext otherwise.
 * The control connection auto-reconnects with backoff on drop, resuming its
 * in-flight leases while the coordinator holds them for the lease TTL. */
#include "agent.h"
#include "wire.h"
#include "tls.h"

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <netdb.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <unistd.h>

struct agent_cfg {
    char agent_id[128];
    char coordinator[256]; /* host:port */
    char tls_ca[256];
    char tls_cert[256];
    char tls_key[256];
    int  workers;
    int  copy_threads;
    bool no_uring;
};

static struct agent_cfg cfg = { .workers = 4, .copy_threads = 8 };

/* Adaptive cross-pool work-stealing: on by default, -S pins the pools. */
bool g_steal_enabled = true;
atomic_ullong g_steal_shards; /* shards a copy thread crawled */
atomic_ullong g_steal_copies; /* copies a walker drained */

/* control-thread state */
static struct conn g_conn = { .fd = -1 };
static bool     g_pause, g_drain, g_request_pending, g_starved;
static uint64_t g_hb_seq;
static uint32_t g_hb_interval_s = 5;
static uint64_t g_fleet_epoch;    /* from the first HelloAck */
static bool     g_have_epoch;
static volatile sig_atomic_t g_want_exit; /* coordinator or a signal asked us to stop */

static void on_signal(int sig)
{
    (void)sig;
    g_want_exit = 1;
}

void process_item(struct shard_item *it)
{
    atomic_fetch_add(&g_inflight, 1);
    lease_start(it->lease_id); /* binds this thread's slot for lease_publish */
    if (it->kind == WI_DELETE)
        process_delete(it);
    else if (it->kind == WI_VERIFY)
        process_verify(it);
    else if (it->kind == WI_ENTRYLIST)
        process_entrylist(it);
    else if (it->kind == WI_CHUNK)
        process_chunk(it);
    else if (it->kind == WI_PROBE)
        process_probe(it);
    else if (it->kind == WI_DIRFIX)
        process_dirfix(it);
    else
        process_shard(it);
    lease_end(); /* the processors already released the lease itself */
    shard_item_free(it);
    atomic_fetch_sub(&g_inflight, 1);
}

static void *worker_main(void *arg)
{
    (void)arg;
    struct shard_item it;
    if (!g_steal_enabled) { /* fixed pools: plain blocking crawl loop */
        while (wq_pop(&it))
            process_item(&it);
        return NULL;
    }
    /* Adaptive: crawl first, but when no shards are queued help drain the copy
     * backlog instead of idling (safe — copies never wait on anything). */
    for (;;) {
        if (wq_trypop(&it)) {
            process_item(&it);
            continue;
        }
        if (cp_drain_one()) {
            atomic_fetch_add(&g_steal_copies, 1);
            continue;
        }
        int r = wq_pop_timed(&it, STEAL_POLL_MS);
        if (r < 0)
            return NULL;
        if (r > 0)
            process_item(&it);
        /* r == 0: timed out — loop to recheck the copy queue */
    }
}

static uint64_t read_rss(void)
{
    FILE *f = fopen("/proc/self/statm", "r");
    if (!f)
        return 0;
    unsigned long long total = 0, resident = 0;
    int rc = fscanf(f, "%llu %llu", &total, &resident);
    fclose(f);
    return rc == 2 ? resident * (uint64_t)sysconf(_SC_PAGESIZE) : 0;
}

/* Resolves and connects hostport, copying the bare host (for TLS verification)
 * into host_out. Returns a connected fd or -1. */
static int connect_coordinator(const char *hostport, char *host_out, size_t host_cap)
{
    char host[256];
    const char *colon = strrchr(hostport, ':');
    if (!colon || colon == hostport) {
        LOGE("coordinator must be host:port, got %s", hostport);
        return -1;
    }
    size_t hn = (size_t)(colon - hostport);
    if (hn >= sizeof host)
        hn = sizeof host - 1;
    memcpy(host, hostport, hn);
    host[hn] = '\0';
    if (host_out)
        snprintf(host_out, host_cap, "%s", host);

    struct addrinfo hints = { .ai_socktype = SOCK_STREAM }, *res;
    int rc = getaddrinfo(host, colon + 1, &hints, &res);
    if (rc != 0) {
        LOGE("resolve %s: %s", hostport, gai_strerror(rc));
        return -1;
    }
    int fd = -1;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol);
        if (fd < 0)
            continue;
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0)
            break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0)
        LOGE("connect %s: %s", hostport, strerror(errno));
    return fd;
}

static int send_pb(uint16_t type, pb_buf *b)
{
    if (b->oom) {
        pb_free(b);
        errno = ENOMEM;
        return -1;
    }
    int rc = wire_write(&g_conn, type, b->p, b->len);
    pb_free(b);
    return rc;
}

static void maybe_request_work(bool from_timer)
{
    if (g_pause || g_drain || g_request_pending)
        return;
    if (g_starved && !from_timer)
        return; /* empty grant last time; retry only on the 1 Hz tick */
    g_starved = false;
    /* Prefetch window. With work-stealing on, copy threads can also crawl, so
     * size the window to the whole pool — otherwise the extra crawl capacity
     * starves for shards during metadata-heavy phases. */
    int crawlers = cfg.workers + (g_steal_enabled ? cfg.copy_threads : 0);
    int cap = crawlers * 2;
    int avail = cap - wq_depth() - (int)atomic_load(&g_inflight);
    if (avail <= 0)
        return;
    struct cached_opts cached[GRANT_MAX_OPTIONS];
    size_t n = opts_cached(cached, GRANT_MAX_OPTIONS);
    pb_buf b;
    pb_init(&b);
    enc_work_request(&b, (uint32_t)avail, cached, n);
    if (send_pb(FR_WORK_REQUEST, &b) == 0)
        g_request_pending = true;
}

/* In-flight detail is capped well below MAX_LEASES: an agent holds roughly
 * (workers + copy_threads) * 2 leases, so this only ever truncates if something
 * has gone badly wrong — and the renewal list (held) is never truncated. */
#define HB_INFLIGHT_MAX 256

static int send_heartbeat(void)
{
    uint64_t held[1024];
    size_t n = lease_snapshot(held, 1024);
    static struct inflight_view inflight[HB_INFLIGHT_MAX]; /* control thread only */
    size_t n_inflight = lease_inflight(inflight, HB_INFLIGHT_MAX);
    pb_buf b;
    pb_init(&b);
    enc_heartbeat(&b, ++g_hb_seq, held, n, (uint32_t)wq_depth(),
                  (uint32_t)cp_depth(), read_rss(), inflight, n_inflight);
    return send_pb(FR_HEARTBEAT, &b);
}

static int send_stats(void)
{
    struct stats_snapshot s = {
        .entries_scanned = atomic_load(&g_stat_scanned),
        .files_copied = atomic_load(&g_stat_files_copied),
        .bytes_copied = atomic_load(&g_stat_bytes_copied),
        .meta_fixed = atomic_load(&g_stat_meta_fixed),
        .errors = atomic_load(&g_stat_errors),
        .shard_queue_depth = (uint32_t)wq_depth(),
        .copy_queue_depth = (uint32_t)cp_depth(),
        .rss_bytes = read_rss(),
    };
    pb_buf b;
    pb_init(&b);
    enc_stats(&b, &s);
    return send_pb(FR_STATS_REPORT, &b);
}

/* returns -1 to terminate the session */
static int dispatch(uint16_t type, const uint8_t *p, size_t n)
{
    switch (type) {
    case FR_HEARTBEAT_ACK: {
        struct hb_ack a;
        if (!dec_hb_ack(p, n, &a))
            return -1;
        if (a.pause != g_pause)
            LOGI("coordinator set pause=%d", a.pause);
        if (a.drain && !g_drain)
            LOGI("coordinator requested drain");
        g_pause = a.pause;
        g_drain = g_drain || a.drain;
        return 0;
    }
    case FR_WORK_GRANT: {
        g_request_pending = false;
        struct work_grant *g = malloc(sizeof *g);
        if (!g || !dec_work_grant(p, n, g)) {
            LOGE("malformed work grant");
            free(g);
            return -1;
        }
        for (size_t i = 0; i < g->n_options; i++)
            opts_store(&g->options[i]);
        for (size_t i = 0; i < g->n_items; i++) {
            lease_add(&g->items[i]); /* copies rel_path; wq_push takes the original */
            wq_push(&g->items[i]); /* takes rel_path ownership */
        }
        if (g->n_unsupported)
            LOGW("skipped %zu unsupported work items (slice 1 handles dir shards only)",
                 g->n_unsupported);
        if (g->n_items == 0)
            g_starved = true;
        else
            maybe_request_work(false);
        free(g); /* items ownership moved to the queue */
        return 0;
    }
    case FR_SHARD_SPLIT_ACK: {
        uint64_t parent, seq;
        if (!dec_split_ack(p, n, &parent, &seq))
            return -1;
        if (!split_resolve(parent, seq))
            LOGW("split ack with no waiter: parent=%llu seq=%llu",
                 (unsigned long long)parent, (unsigned long long)seq);
        return 0;
    }
    case FR_JOURNAL_ACK: {
        uint64_t acked;
        if (!dec_journal_ack(p, n, &acked))
            return -1;
        jrn_ack_update(acked);
        return 0;
    }
    case FR_CONTROL: {
        uint32_t cmd;
        uint64_t job_id;
        if (!dec_control(p, n, &cmd, &job_id))
            return -1;
        switch (cmd) {
        case CMD_PAUSE:  g_pause = true;  break;
        case CMD_RESUME: g_pause = false; break;
        case CMD_DRAIN:  g_drain = true;  break;
        case CMD_SHUTDOWN:
            LOGI("shutdown requested by coordinator");
            g_want_exit = true;
            return -1;
        default:
            LOGW("unhandled control command %u", cmd);
        }
        return 0;
    }
    case FR_ERROR:
        LOGE("coordinator sent protocol error; closing");
        return -1;
    default:
        LOGW("unexpected frame type %u", type);
        return 0;
    }
}

/* Tear down the current session's transport, leaving worker threads and their
 * in-flight leases untouched (the coordinator holds them for the lease TTL). */
static void session_teardown(void)
{
    if (g_conn.ssl) {
        tls_close(g_conn.ssl);
        g_conn.ssl = NULL;
    }
    if (g_conn.fd >= 0) {
        close(g_conn.fd);
        g_conn.fd = -1;
    }
}

/* Establish one coordinator session: TCP connect, optional TLS handshake, then
 * the HELLO/HELLO_ACK exchange. On success g_conn is live and *ack is filled;
 * returns 0. On failure returns -1 with g_conn torn down. */
static int dial(struct hello_ack *ack)
{
    char host[256];
    int fd = connect_coordinator(cfg.coordinator, host, sizeof host);
    if (fd < 0)
        return -1;
    g_conn.fd = fd;
    g_conn.ssl = NULL;
    if (tls_enabled()) {
        g_conn.ssl = tls_connect(fd, host);
        if (!g_conn.ssl) {
            close(fd);
            g_conn.fd = -1;
            return -1;
        }
    }

    char hn[128] = "unknown";
    gethostname(hn, sizeof hn - 1);
    pb_buf b;
    pb_init(&b);
    enc_hello(&b, cfg.agent_id, hn, AGENT_VERSION,
              (uint32_t)sysconf(_SC_NPROCESSORS_ONLN), 0, g_uring_enabled);
    if (send_pb(FR_HELLO, &b) < 0) {
        LOGE("hello send failed: %s", strerror(errno));
        session_teardown();
        return -1;
    }
    uint16_t ftype;
    uint8_t *payload;
    size_t plen;
    if (wire_read(&g_conn, &ftype, &payload, &plen) < 0 || ftype != FR_HELLO_ACK) {
        LOGE("handshake failed (frame %u, errno %s)", ftype, strerror(errno));
        session_teardown();
        return -1;
    }
    bool ok = dec_hello_ack(payload, plen, ack);
    free(payload);
    if (!ok || !ack->accepted) {
        LOGE("coordinator rejected session: %s",
             ok && ack->reject_reason[0] ? ack->reject_reason : "malformed ack");
        session_teardown();
        return -1;
    }
    return 0;
}

static void default_agent_id(char *dst, size_t cap)
{
    FILE *f = fopen("/etc/machine-id", "r");
    char mid[64] = "";
    if (f) {
        if (!fgets(mid, sizeof mid, f))
            mid[0] = '\0';
        fclose(f);
    }
    mid[strcspn(mid, "\n")] = '\0';
    if (strlen(mid) >= 12) {
        snprintf(dst, cap, "agent-%.12s", mid);
        return;
    }
    char host[128] = "unknown";
    gethostname(host, sizeof host - 1);
    snprintf(dst, cap, "agent-%s", host);
}

/* Raise the open-file soft limit to the hard ceiling. The agent's fd use grows
 * with the pool sizes (io_uring rings, held directory fds, concurrent copies),
 * so high-core hosts can exceed a low default. Doing it in-process is robust:
 * systemd services never read /etc/security/limits.d (that is pam_limits, login
 * sessions only), so the soft limit is whatever LimitNOFILE / DefaultLimitNOFILE
 * gives — but the hard limit is usually high, and we can lift the soft to it. */
static void raise_nofile(void)
{
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) != 0) {
        LOGW("getrlimit(NOFILE): %s", strerror(errno));
        return;
    }
    rlim_t was = rl.rlim_cur;
    if (rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max;
        if (setrlimit(RLIMIT_NOFILE, &rl) != 0) {
            LOGW("setrlimit(NOFILE=%ju): %s", (uintmax_t)rl.rlim_max, strerror(errno));
            rl.rlim_cur = was;
        }
    }
    LOGI("open-file limit soft=%ju hard=%ju%s", (uintmax_t)rl.rlim_cur,
         (uintmax_t)rl.rlim_max, rl.rlim_cur > was ? " (raised)" : "");
    if (rl.rlim_cur < 65536)
        LOGW("open-file soft limit is only %ju — high-core hosts may hit EMFILE; "
             "set LimitNOFILE= in the systemd unit (limits.d does NOT apply to services)",
             (uintmax_t)rl.rlim_cur);
}

int main(int argc, char **argv)
{
    strcpy(cfg.coordinator, "127.0.0.1:7440");
    default_agent_id(cfg.agent_id, sizeof cfg.agent_id);

    int opt;
    while ((opt = getopt(argc, argv, "c:i:w:C:USA:E:K:h")) != -1) {
        switch (opt) {
        case 'c':
            snprintf(cfg.coordinator, sizeof cfg.coordinator, "%s", optarg);
            break;
        case 'i':
            snprintf(cfg.agent_id, sizeof cfg.agent_id, "%s", optarg);
            break;
        case 'A':
            snprintf(cfg.tls_ca, sizeof cfg.tls_ca, "%s", optarg);
            break;
        case 'E':
            snprintf(cfg.tls_cert, sizeof cfg.tls_cert, "%s", optarg);
            break;
        case 'K':
            snprintf(cfg.tls_key, sizeof cfg.tls_key, "%s", optarg);
            break;
        case 'w':
            cfg.workers = atoi(optarg);
            if (cfg.workers < 1 || cfg.workers > 256) {
                fprintf(stderr, "workers must be 1..256\n");
                return 2;
            }
            break;
        case 'C':
            cfg.copy_threads = atoi(optarg);
            if (cfg.copy_threads < 1 || cfg.copy_threads > 256) {
                fprintf(stderr, "copy threads must be 1..256\n");
                return 2;
            }
            break;
        case 'U':
            cfg.no_uring = true; /* A/B benchmarking + emergency escape hatch */
            break;
        case 'S':
            g_steal_enabled = false; /* pin the walker/copy pools to fixed sizes */
            break;
        default:
            fprintf(stderr,
                    "usage: drsync-agent [-c host:port] [-i agent-id] [-w walkers]"
                    " [-C copy-threads] [-U(no io_uring)] [-S(no work-stealing)]\n"
                    "                    [-A ca.crt -E agent.crt -K agent.key (mTLS)]\n");
            return 2;
        }
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, on_signal); /* clean drain on Ctrl-C / systemd stop */
    signal(SIGTERM, on_signal);
    raise_nofile();
    uring_probe(!cfg.no_uring);

    /* mTLS: all three of CA, cert and key together, or none (plaintext dev). */
    if (cfg.tls_ca[0] || cfg.tls_cert[0] || cfg.tls_key[0]) {
        if (!(cfg.tls_ca[0] && cfg.tls_cert[0] && cfg.tls_key[0])) {
            fprintf(stderr, "mTLS needs -A ca.crt, -E cert.crt and -K key.pem together\n");
            return 2;
        }
        if (tls_client_init(cfg.tls_ca, cfg.tls_cert, cfg.tls_key) < 0)
            return 1;
    } else {
        LOGW("connecting WITHOUT TLS — dev mode only");
    }

    /* Worker/copy pools and the outbox live for the whole process: a dropped
     * control connection reconnects underneath them without disturbing
     * in-flight shards or the leases the coordinator is holding for us. */
    if (outbox_init() < 0) {
        LOGE("outbox init failed");
        return 1;
    }
    /* Reserve one copy thread as a pure drainer when stealing is on (and there
     * are >= 2), so shard-stealing copy threads can never starve their own
     * copies. Stealing off (or a lone copy thread) => all pure drainers. */
    int cp_reserve = (g_steal_enabled && cfg.copy_threads >= 2) ? 1 : cfg.copy_threads;
    if (cp_init(cfg.copy_threads, cfg.copy_threads * 8, cp_reserve) < 0) {
        LOGE("copy pool init failed");
        return 1;
    }
    pthread_t *threads = calloc((size_t)cfg.workers, sizeof *threads);
    for (int i = 0; i < cfg.workers; i++)
        pthread_create(&threads[i], NULL, worker_main, NULL);

    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
    struct itimerspec its = { .it_interval = { 1, 0 }, .it_value = { 1, 0 } };
    timerfd_settime(tfd, 0, &its, NULL);

    int exit_code = 0;
    int backoff_ms = 500;
    const int backoff_max_ms = 15000;

    while (!g_want_exit) {
        struct hello_ack ack;
        if (dial(&ack) < 0) {
            struct timespec ts = { backoff_ms / 1000,
                                   (long)(backoff_ms % 1000) * 1000000L };
            nanosleep(&ts, NULL);
            backoff_ms = backoff_ms * 2 < backoff_max_ms ? backoff_ms * 2 : backoff_max_ms;
            continue;
        }
        backoff_ms = 500;
        if (ack.hb_interval_s)
            g_hb_interval_s = ack.hb_interval_s;
        if (!g_have_epoch) {
            g_fleet_epoch = ack.fleet_epoch;
            g_have_epoch = true;
        } else if (ack.fleet_epoch != g_fleet_epoch) {
            /* The coordinator restarted: leases it held for us are gone, so any
             * in-flight results will be rejected as stale and the shards get
             * re-granted. Nothing to unwind locally. */
            LOGW("coordinator restarted (fleet epoch %llu -> %llu); resuming with fresh grants",
                 (unsigned long long)g_fleet_epoch, (unsigned long long)ack.fleet_epoch);
            g_fleet_epoch = ack.fleet_epoch;
        }
        LOGI("connected to %s as %s (hb=%us lease_ttl=%us walkers=%d copy_threads=%d io_uring=%d tls=%d)",
             cfg.coordinator, cfg.agent_id, g_hb_interval_s, ack.lease_ttl_s,
             cfg.workers, cfg.copy_threads, g_uring_enabled, tls_enabled());

        /* Fresh session: clear the outstanding-request gate and re-ask. Held
         * leases are re-declared by the next heartbeat (Heartbeat renews by
         * agent id), so the coordinator keeps our in-flight work. */
        g_request_pending = false;
        g_starved = false;
        maybe_request_work(true);

        struct pollfd pfds[3] = {
            { .fd = g_conn.fd, .events = POLLIN },
            { .fd = tfd, .events = POLLIN },
            { .fd = g_outbox_eventfd, .events = POLLIN },
        };
        unsigned tick = 0;
        bool reconnect = false;

        while (!reconnect && !g_want_exit) {
            if (poll(pfds, 3, -1) < 0) {
                if (errno == EINTR)
                    continue;
                LOGE("poll: %s", strerror(errno));
                exit_code = 1;
                g_want_exit = true;
                break;
            }

            if (pfds[1].revents & POLLIN) { /* 1 Hz tick */
                uint64_t expiries;
                if (read(tfd, &expiries, sizeof expiries) < 0 && errno != EAGAIN) {
                    LOGE("timerfd read: %s", strerror(errno));
                    exit_code = 1;
                    g_want_exit = true;
                    break;
                }
                tick++;
                if (send_stats() < 0) {
                    LOGW("stats send failed (%s); reconnecting", strerror(errno));
                    reconnect = true;
                    break;
                }
                if (tick % g_hb_interval_s == 0 && send_heartbeat() < 0) {
                    LOGW("heartbeat send failed (%s); reconnecting", strerror(errno));
                    reconnect = true;
                    break;
                }
                maybe_request_work(true);
            }

            if (pfds[2].revents & POLLIN) { /* worker outbox */
                uint64_t v;
                while (read(g_outbox_eventfd, &v, sizeof v) > 0)
                    ;
                struct outmsg *m = out_drain();
                while (m) {
                    struct outmsg *next = m->next;
                    /* A frame lost to a mid-write drop is re-derived after the
                     * shard's lease expires and it is redone (at-least-once). */
                    if (!reconnect && wire_write(&g_conn, m->type, m->buf, m->len) < 0) {
                        LOGW("frame write failed (%s); reconnecting", strerror(errno));
                        reconnect = true;
                    }
                    free(m->buf);
                    free(m);
                    m = next;
                }
            }

            if (pfds[0].revents & (POLLIN | POLLHUP | POLLERR)) {
                /* level-triggered: one frame per readiness keeps the loop fair */
                uint16_t t;
                uint8_t *p;
                size_t n;
                if (wire_read(&g_conn, &t, &p, &n) < 0) {
                    if (errno == 0)
                        LOGI("coordinator closed the connection");
                    else
                        LOGW("read failed (%s)", strerror(errno));
                    reconnect = true;
                    break;
                }
                int rc = dispatch(t, p, n);
                free(p);
                if (rc < 0)
                    break; /* CMD_SHUTDOWN set g_want_exit; else end the session */
            }
        }

        session_teardown();
        if (g_want_exit)
            break;
        LOGI("reconnecting to %s (backoff %dms)...", cfg.coordinator, backoff_ms);
        struct timespec ts = { backoff_ms / 1000,
                               (long)(backoff_ms % 1000) * 1000000L };
        nanosleep(&ts, NULL);
        backoff_ms = backoff_ms * 2 < backoff_max_ms ? backoff_ms * 2 : backoff_max_ms;
    }

    LOGI("shutting down (draining %d walkers, %d copy threads)",
         cfg.workers, cfg.copy_threads);
    wq_shutdown();
    for (int i = 0; i < cfg.workers; i++)
        pthread_join(threads[i], NULL);
    cp_shutdown();
    if (g_steal_enabled)
        LOGI("work-stealing: copy threads crawled %llu shards, walkers drained %llu copies",
             (unsigned long long)atomic_load(&g_steal_shards),
             (unsigned long long)atomic_load(&g_steal_copies));
    free(threads);
    session_teardown();
    return exit_code;
}
