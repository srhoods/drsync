/* Shared agent state: work queue, outbox, leases, job-options table, stats.
 * Concurrency model (docs/DESIGN-agent.md §1, slice 1): one control thread
 * owns the socket (sole reader AND writer); worker threads walk shards and
 * communicate outbound frames through the outbox (eventfd-woken). */
#ifndef DRSYNC_AGENT_H
#define DRSYNC_AGENT_H

#include <limits.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/stat.h>
#include <time.h>

#include "msgs.h"
#include "pb.h"

#define AGENT_VERSION "0.1.0-slice5"

/* ---- logging ---- */
void log_line(const char *level, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#define LOGI(...) log_line("info", __VA_ARGS__)
#define LOGW(...) log_line("warn", __VA_ARGS__)
#define LOGE(...) log_line("error", __VA_ARGS__)

/* ---- cumulative stats (agent lifetime; coordinator computes rates) ---- */
extern atomic_ullong g_stat_scanned;
extern atomic_ullong g_stat_files_copied;
extern atomic_ullong g_stat_bytes_copied;
extern atomic_ullong g_stat_meta_fixed;
extern atomic_ullong g_stat_errors;
extern atomic_uint   g_inflight; /* shards being processed right now */

/* ---- work queue (control → workers) ---- */
void wq_push(const struct shard_item *it); /* takes ownership of it->rel_path */
bool wq_pop(struct shard_item *out);       /* blocks; false = shutdown */
int  wq_depth(void);
void wq_shutdown(void);

/* ---- outbox (workers/control → socket, written by control thread) ---- */
extern int g_outbox_eventfd;
struct outmsg {
    uint16_t       type;
    uint8_t       *buf;
    size_t         len;
    struct outmsg *next;
};
/* Steals b's buffer; b is reset. */
void out_push(uint16_t type, pb_buf *b);
struct outmsg *out_drain(void);
int  outbox_init(void);

/* ---- held leases (for heartbeat renewal lists) ---- */
void   lease_add(uint64_t id);
void   lease_remove(uint64_t id);
size_t lease_snapshot(uint64_t *dst, size_t cap);

/* ---- job options table ---- */
struct opts_entry {
    struct job_options o;
    int src_fd; /* O_RDONLY|O_DIRECTORY on src root */
    int dst_fd; /* O_RDONLY|O_DIRECTORY on dst root (created if missing) */
};
int  opts_store(const struct job_options *o);
const struct opts_entry *opts_get(uint64_t job_id);
size_t opts_cached(struct cached_opts *dst, size_t cap);

/* ---- pending shard splits (worker blocks until coordinator ack) ---- */
struct split_wait {
    uint64_t           parent;
    uint64_t           seq;
    sem_t              sem;
    struct split_wait *next;
};
void split_register(struct split_wait *w);
bool split_resolve(uint64_t parent, uint64_t seq); /* posts the waiter */
void split_unregister(struct split_wait *w);

/* ---- unified stat view (statx or fstatat sourced) ---- */
struct estat {
    uint32_t        mode;
    uint32_t        uid, gid;
    uint32_t        nlink;
    uint64_t        ino;
    uint64_t        size;
    uint64_t        blocks; /* 512B blocks; sparseness heuristic */
    struct timespec atim, mtim;
    uint32_t        rdev_major, rdev_minor;
};

/* ---- io_uring statx batching (uring.c) ---- */
/* True when the kernel allows io_uring and supports IORING_OP_STATX; probed
 * once at startup (RHEL disables io_uring by default on some releases). */
extern bool g_uring_enabled;
void uring_probe(bool allow);
/* Set the io_uring ring depth (statx in flight per walker) from a job's
 * tuning.statx_batch. Clamped to a sane range; 0 keeps the default. Takes
 * effect for rings built afterwards. */
void uring_set_depth(unsigned depth);

struct statx_req {
    int           dirfd;
    const char   *name;
    struct estat *out; /* filled on success */
    int           res; /* 0 ok, else -errno */
};
/* Batched stat of n entries: io_uring when available, serial fstatat fallback.
 * Always completes every request (res tells the outcome per entry). */
void stat_batch(struct statx_req *reqs, size_t n);

/* relaxed counter add: shard counters are shared walker ↔ copy pool */
#define CTR_ADD(field, v) __atomic_fetch_add(&(field), (v), __ATOMIC_RELAXED)

/* ---- per-directory pending-copy tracking ----
 * Directory metadata may only be applied once every rename into that
 * directory has happened (design §3.5); the walker waits on this. */
struct dpend {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             n;
};
void dpend_init(struct dpend *dp);
void dpend_add(struct dpend *dp);
void dpend_done(struct dpend *dp);
void dpend_wait(struct dpend *dp);
void dpend_destroy(struct dpend *dp);

/* ---- per-shard journal buffer (jrn.c, docs/DESIGN-coordinator.md §5) ----
 * Walker and copy threads append records; flushes ship zstd-compressed
 * JournalBatch frames via the outbox. The shard result may only be sent
 * after every batch it emitted is acked (ordering invariant, protocol §4.2). */
struct jrn {
    pthread_mutex_t mu;
    pb_buf          raw;     /* varint-delimited JournalRecords, uncompressed */
    uint32_t        count;
    uint64_t        max_seq; /* highest batch seq this shard has sent */
};

/* ---- walk context (shared between walker and copy pool) ---- */
struct walk_ctx {
    const struct opts_entry *oe;
    const struct shard_item *it;
    struct shard_counters    c;      /* fields updated via CTR_ADD */
    struct jrn               jrn;
    int64_t                  budget;
    char                   **split;
    size_t                   n_split, cap_split;
    uint64_t                 split_seq;
    unsigned                 tmp_seq; /* atomic: unique temp names per shard */
    char                     err[256];
    bool                     fatal;
};

void jrn_init(struct walk_ctx *ctx);
/* thread-safe append; auto-flushes at the batch size threshold */
void jrn_emit(struct walk_ctx *ctx, int type, const char *rel_path,
              const struct estat *src, const struct estat *dst,
              int err_no, const char *detail);
/* variant carrying an xxh3-128 checksum (COPIED, VERIFY_*) */
void jrn_emit_hash(struct walk_ctx *ctx, int type, const char *rel_path,
                   const struct estat *src, const struct estat *dst,
                   uint64_t xxh_lo, uint64_t xxh_hi);
void jrn_flush(struct walk_ctx *ctx);
/* true when all this shard's batches are acked; false = timeout (caller
 * must fail the shard so it re-runs — at-least-once journaling) */
bool jrn_wait_acked(struct walk_ctx *ctx);
void jrn_destroy(struct walk_ctx *ctx);
/* control thread: JournalAck received */
void jrn_ack_update(uint64_t acked_seq);
void walk_err(struct walk_ctx *ctx, const char *what, const char *path);
void walk_fidelity(struct walk_ctx *ctx, const char *what, const char *path);

/* ---- xattrs + ACLs (xattr.c, docs/DESIGN-agent.md §5) ----
 * POSIX ACLs travel as system.posix_acl_* xattrs, NFSv4 ACLs as
 * system.nfs4_acl; namespace filtering and the untranslatable policy are
 * applied per job options. Failures to apply count as fidelity exceptions
 * (or errors under policy=fail), never silent drops. */
/* fd-based: regular files (during copy) and directories */
void xattr_copy_fd(struct walk_ctx *ctx, int src_fd, int dst_fd, const char *logname);
/* path-based via /proc/self/fd (no target open); nofollow for symlinks */
void xattr_copy_at(struct walk_ctx *ctx, int sdirfd, int ddirfd, const char *name,
                   bool nofollow);
/* drift check for otherwise-clean files; true = equal (or indeterminable) */
bool xattr_equal_at(struct walk_ctx *ctx, int sdirfd, int ddirfd, const char *name);

/* ---- copy pool (copy.c) ---- */
int  cp_init(int threads, int queue_cap);
void cp_shutdown(void);
int  cp_depth(void);
/* Blocking bounded submit (backpressure). sfd/dfd stay valid until the task
 * completes: the walker's dpend_wait precedes closing them. */
void cp_submit(struct walk_ctx *ctx, struct dpend *dp, int sfd, int dfd,
               const char *dir_rel, const char *name, const struct estat *ss);
/* Synchronous copy used by the copy threads (and dry-run accounting).
 * rel is the job-root-relative path of the file (journal identity). */
void copy_file_task(struct walk_ctx *ctx, int sfd, int dfd, const char *name,
                    const char *rel, const struct estat *ss);

/* ---- walker (walker.c) ---- */
/* Processes one shard end-to-end and enqueues its ShardResult. */
void process_shard(const struct shard_item *it);
/* Processes one entry-list shard (a name slice of a pathological directory). */
void process_entrylist(const struct shard_item *it);

/* ---- shared fs helpers ---- */
/* openat2(RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS) with component-walk fallback
 * (walker.c) — the traversal guarantee for everything under a job root */
int open_beneath(int root_fd, const char *rel, uint64_t flags);
/* opens rel's parent directory beneath root; *leaf points into rel (delete.c) */
int open_parent_beneath(int root_fd, const char *rel, const char **leaf);
/* struct stat → estat (uring.c) */
void estat_of(struct estat *e, const struct stat *st);

/* ---- verify executor (verify.c) ----
 * Metadata re-check (+ xattrs) for every listed entry; sampled entries are
 * re-read on BOTH sides and their XXH3-128 compared. Mismatch → JR_VERIFY_FAIL
 * (+ inline recopy under on_mismatch=recopy). */
void process_verify(const struct shard_item *it);

/* ---- delete pass executor (delete.c) ---- */
/* Removes destination orphans listed in a WI_DELETE item (recursive for
 * orphan directories), journals JR_DELETED per removed object. */
void process_delete(const struct shard_item *it);

#endif
