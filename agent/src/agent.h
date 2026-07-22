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

/* Wire protocol version (proto/drsync.proto §Versioning). Major must match the
 * coordinator exactly — bumping it locks out the whole fleet until every agent
 * is upgraded. Minor is additive: bump it when this agent starts populating a
 * newly added field, so the coordinator can tell "not reported" from "zero".
 * minor 1: Heartbeat.inflight. */
#define PROTO_MAJOR 1
#define PROTO_MINOR 1

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
bool wq_trypop(struct shard_item *out);    /* non-blocking; false = empty/down */
/* Blocks up to timeout_ms for an item. Returns: 1 = got one, 0 = timed out,
 * -1 = shutdown. Used by the adaptive stealing loop so an idle thread rechecks
 * the other pool's queue periodically. */
int  wq_pop_timed(struct shard_item *out, int timeout_ms);
int  wq_depth(void);
void wq_shutdown(void);
/* Drain: release every queued (unstarted) shard, handing each to `release`. */
int  wq_release_all(void (*release)(struct shard_item *it));
/* Terminal job: drop its queued shards, handing each to `dispose`. */
int  wq_drop_job(uint64_t job_id, void (*dispose)(struct shard_item *it));

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

/* ---- held leases (heartbeat renewal + in-flight reporting) ----
 * Registry slots are index-stable for the life of a lease (freed in place, never
 * compacted), so a worker can hold a pointer to its own slot and publish
 * progress into it without re-looking it up. */
#define LEASE_PATH_MAX 256 /* truncated: enough to identify the subtree */

struct lease_entry {
    uint64_t        lease_id; /* 0 = free slot */
    uint64_t        shard_id, job_id;
    int             kind;                    /* WI_* */
    char            rel_path[LEASE_PATH_MAX];
    struct timespec granted_at, started_at;  /* CLOCK_MONOTONIC */
    bool            running;                 /* false = queued, not yet picked up */
    atomic_ullong   entries_done;            /* published by the owning worker */
};

/* Control thread: record a granted lease. Copies what it needs from it, so the
 * caller may hand it->rel_path to the work queue afterwards. */
void lease_add(const struct shard_item *it);
void lease_remove(uint64_t id);
bool lease_job_held(uint64_t job_id);
size_t lease_snapshot(uint64_t *dst, size_t cap);

/* Worker thread: claim/release the lease this thread is processing. lease_start
 * binds the slot to the calling thread so lease_publish is a pointer store. */
void lease_start(uint64_t id);
void lease_end(void);
void lease_publish(uint64_t entries_done);

/* Snapshot of every held lease for the heartbeat. entries_done is resolved to a
 * plain value; ages are computed against now. */
struct inflight_view {
    uint64_t lease_id, shard_id, job_id, entries_done;
    int      kind;
    char     rel_path[LEASE_PATH_MAX];
    uint32_t held_ms, running_ms;
    bool     running;
};
size_t lease_inflight(struct inflight_view *dst, size_t cap);

/* ---- job options table ---- */
struct opts_entry {
    struct job_options o;
    int src_fd; /* O_RDONLY|O_DIRECTORY on src root */
    int dst_fd; /* O_RDONLY|O_DIRECTORY on dst root (created if missing) */
};
int  opts_store(const struct job_options *o);
const struct opts_entry *opts_get(uint64_t job_id);
/* Release a terminal job's cached options + root fds (CMD_CANCEL_JOB). */
void opts_evict(uint64_t job_id);
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

/* ---- io_uring registered-buffer copy engine (ucopy.c) ---- */
/* Called in stream order with each block as it is read, so the caller can fold
 * an inline checksum into the copy for free (design §3). */
typedef void (*ucopy_sink)(void *arg, const void *data, size_t n);
/* True once the calling copy thread has a working copy ring (lazy per-thread);
 * false when io_uring is unavailable — the caller then uses a serial loop. */
bool ucopy_available(void);
/* Disable the io_uring copy engine for this thread after a bad copy result. */
void ucopy_disable(void);
/* Sequentially copy [0,size) from in to out via overlapped READ_FIXED/
 * WRITE_FIXED, feeding each block to sink in order. Returns bytes copied (==
 * size, or less if the source shrank mid-copy — the caller's gen check handles
 * that) or -errno on a read/write error. Only call when ucopy_available(). */
int64_t ucopy_run(int in, int out, uint64_t size, ucopy_sink sink, void *sink_arg);

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
/* Max split/entry-list batches shipped but not yet acked (pipeline depth).
 * Bounds the outbox/coordinator burst while overlapping round-trips. */
#define SPLIT_WINDOW 128

struct walk_ctx {
    const struct opts_entry *oe;
    const struct shard_item *it;
    struct shard_counters    c;      /* fields updated via CTR_ADD */
    struct jrn               jrn;
    int64_t                  budget;
    char                   **split;
    size_t                   n_split, cap_split;
    /* Big files found this shard, shipped as ShardSplit.big_files for the
     * coordinator to fan out into chunk tasks across the fleet. */
    struct bigfile          *bigfiles;
    size_t                   n_bigfiles, cap_bigfiles;
    uint64_t                 split_seq;
    /* In-flight split acks: shipped, awaiting the coordinator's ack. Drained
     * (all awaited) before the shard result — the ordering invariant (§4.2) —
     * but shipped without blocking per-batch so round-trips overlap. */
    struct split_wait       *infl[SPLIT_WINDOW];
    size_t                   infl_head, infl_count;
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
/* For a destination just created by us (a copy temp): skips the destination
 * probe and stale-removal, which a fresh file cannot need. */
void xattr_copy_fd_fresh(struct walk_ctx *ctx, int src_fd, int dst_fd, const char *logname);
/* path-based via /proc/self/fd (no target open); nofollow for symlinks */
void xattr_copy_at(struct walk_ctx *ctx, int sdirfd, int ddirfd, const char *name,
                   bool nofollow);
/* drift check for otherwise-clean files; true = equal (or indeterminable) */
bool xattr_equal_at(struct walk_ctx *ctx, int sdirfd, int ddirfd, const char *name);

/* ---- copy pool (copy.c) ---- */
/* reserve is the number of copy threads that stay pure drainers (never steal a
 * shard); the rest may steal shards to help crawl when the copy queue is empty.
 * A reserve >= 1 guarantees a drainer always exists, so walkers blocked in
 * dpend_wait on their copies can never deadlock the pool. */
int  cp_init(int threads, int queue_cap, int reserve);
void cp_shutdown(void);
int  cp_depth(void);
/* Executes one queued copy task if one is available (non-blocking). Returns
 * true if it ran one. Lets an idle walker help drain the copy backlog. */
bool cp_drain_one(void);

/* Dispatches one shard item to its executor (main.c); runnable from either
 * pool so a copy thread can help crawl. Frees it and maintains g_inflight. */
void process_item(struct shard_item *it);

/* g_steal_enabled toggles the adaptive cross-pool work-stealing (default on;
 * -S disables it and pins the pools to their fixed sizes). */
extern bool g_steal_enabled;

/* An idle stealing thread rechecks the other pool's queue this often (ms). */
#define STEAL_POLL_MS 10

/* Work-stealing telemetry: shards run by copy threads / copies run by walkers.
 * Non-zero confirms the adaptive tuner rebalanced across the pools. */
extern atomic_ullong g_steal_shards;
extern atomic_ullong g_steal_copies;
/* Blocking bounded submit (backpressure). sfd/dfd stay valid until the task
 * completes: the walker's dpend_wait precedes closing them. */
void cp_submit(struct walk_ctx *ctx, struct dpend *dp, int sfd, int dfd,
               const char *dir_rel, const char *name, const struct estat *ss,
               bool direct);
/* Synchronous copy used by the copy threads (and dry-run accounting).
 * rel is the job-root-relative path of the file (journal identity). */
void copy_file_task(struct walk_ctx *ctx, int sfd, int dfd, const char *name,
                    const char *rel, const struct estat *ss, bool direct);
/* Applies owner/mode/times to an open fd (chown before chmod, times last).
 * xattrs/ACLs are the caller's job first (they need the src fd). */
void apply_meta(struct walk_ctx *ctx, int fd, const struct estat *ss,
                const char *path);

/* ---- chunk executor (chunk.c) ----
 * Processes one ChunkTask: a byte range of a big file copied into the shared
 * temp, or the finalize task that fsyncs, applies metadata and renames it in.
 * Cross-host: ranges of one file are granted to different agents. */
void process_chunk(const struct shard_item *it);

/* ---- mount probe (probe.c) ---- */
/* Verifies this agent's src/dst roots are directories and reports a
 * ShardResult; gates pass start (docs/DESIGN-protocol.md §3.1 ProbeTask). */
void process_probe(const struct shard_item *it);

/* ---- dirfix executor (dirfix.c) ----
 * Re-applies directory metadata from a DirFixBatch after a pass has drained, so
 * a directory whose mtime was bumped by cross-shard renames lands on its source
 * value (docs/DESIGN-coordinator.md §2.2 DIRFIX). */
void process_dirfix(const struct shard_item *it);

/* ---- walker (walker.c) ---- */
/* Processes one shard end-to-end and enqueues its ShardResult. */
void process_shard(const struct shard_item *it);
/* Processes one entry-list shard (a name slice of a pathological directory). */
void process_entrylist(const struct shard_item *it);

/* ---- destination temp names (tempname.c) ----
 * "<prefix><job>-<pass>.<shard>.<seq>", hex. The (job, pass) tag marks a temp as
 * this pass's live work: the orphan sweep reclaims prefix-matching destination
 * entries as crash residue, and without the tag it cannot tell residue from a
 * chunk temp whose group is still being assembled by other hosts. */
void temp_name_fmt(char *out, size_t cap, const char *prefix, uint64_t job_id,
                   uint32_t pass_no, uint64_t shard_id, unsigned seq);
/* tail is a temp name with its prefix already stripped. False for anything that
 * does not carry exactly this (job, pass) — including untagged legacy names,
 * which stay reclaimable. */
bool temp_tag_matches(const char *tail, uint64_t job_id, uint32_t pass_no);

/* ---- shared fs helpers ---- */
/* openat2(RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS) with component-walk fallback
 * (walker.c) — the traversal guarantee for everything under a job root */
int open_beneath(int root_fd, const char *rel, uint64_t flags);
/* opens rel's parent directory beneath root; *leaf points into rel (delete.c) */
int open_parent_beneath(int root_fd, const char *rel, const char **leaf);
/* opens dir rel beneath dst_fd, creating missing components (mode 0700, fixed
 * when the dir itself is walked). Returns an fd or -1 (walker.c). */
int dst_dir_open(int dst_fd, const char *rel);
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
