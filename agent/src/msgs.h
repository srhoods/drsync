/* drsync protocol messages — structs + encode/decode against proto/drsync.proto.
 * Field numbers live only in msgs.c. Slice 1 implements the session, work and
 * result messages; journal/verify/chunk payloads follow with later slices. */
#ifndef DRSYNC_MSGS_H
#define DRSYNC_MSGS_H

#include <limits.h>
#include <stdbool.h>
#include <stdint.h>

#include "pb.h"

/* FrameType (must match proto/drsync.proto) */
enum {
    FR_HELLO = 1,
    FR_HELLO_ACK = 2,
    FR_HEARTBEAT = 3,
    FR_HEARTBEAT_ACK = 4,
    FR_WORK_REQUEST = 5,
    FR_WORK_GRANT = 6,
    FR_SHARD_SPLIT = 7,
    FR_SHARD_SPLIT_ACK = 8,
    FR_SHARD_RESULT = 9,
    FR_TASK_RESULT = 10,
    FR_JOURNAL_BATCH = 11,
    FR_JOURNAL_ACK = 12,
    FR_STATS_REPORT = 13,
    FR_CONTROL = 14,
    FR_ERROR = 15,
};

/* ResultStatus */
enum {
    RES_OK = 1,
    RES_ERROR = 2,
    RES_TRANSIENT = 3,
    RES_MOUNT_SICK = 4,
    RES_SRC_CHANGED = 5,
};

/* AclOptions.Untranslatable */
enum {
    ACL_UNTRANS_WARN = 1,
    ACL_UNTRANS_FAIL = 2,
    ACL_UNTRANS_SKIP = 3,
};

/* VerifyOptions.OnMismatch */
enum {
    VERIFY_MISMATCH_RECOPY = 1,
    VERIFY_MISMATCH_FAIL = 2,
};

/* CopyOptions.ServerSideCopy */
enum {
    SSC_AUTO = 1,    /* try copy_file_range, fall back to read/write */
    SSC_OFF = 2,     /* never use copy_file_range (forces the byte-copy path) */
    SSC_REQUIRE = 3, /* copy_file_range must succeed, else error */
};

/* JournalRecord.Type */
enum {
    JR_COPIED = 1,
    JR_META_FIXED = 2,
    JR_SKIPPED_CLEAN = 3,
    JR_ORPHAN = 4,
    JR_DIR_META = 5,
    JR_ERROR = 6,
    JR_FIDELITY_EXCEPTION = 7,
    JR_NLINK_DUP = 8,
    JR_VERIFY_OK = 9,
    JR_VERIFY_FAIL = 10,
    JR_WOULD_COPY = 11,
    JR_WOULD_DELETE = 12,
    JR_DELETED = 13,
    JR_SRC_CHANGED = 14,
};

/* Control.Command */
enum {
    CMD_PAUSE = 1,
    CMD_RESUME = 2,
    CMD_DRAIN = 3,
    CMD_CANCEL_JOB = 4,
    CMD_SHUTDOWN = 5,
    CMD_LOG_LEVEL = 6,
};

struct hello_ack {
    bool     accepted;
    char     reject_reason[256];
    uint32_t proto_major;
    uint32_t hb_interval_s;
    uint32_t lease_ttl_s;
    uint64_t fleet_epoch;
};

struct hb_ack {
    uint64_t seq;
    bool     pause;
    bool     drain;
};

struct job_options {
    uint64_t job_id;
    char     job_name[128];
    char     src_root[PATH_MAX];
    char     dst_root[PATH_MAX];
    /* copy */
    uint64_t chunk_threshold;
    uint64_t chunk_size;
    uint64_t buffer_size;
    bool     preserve_sparse;
    uint32_t server_side_copy; /* SSC_* (0 = unset ⇒ auto) */
    char     temp_prefix[64];
    bool     fsync_per_file;
    /* metadata */
    bool meta_owner, meta_mode, meta_times, meta_xattrs, meta_specials;
    bool acl_posix, acl_nfs4;
    uint32_t acl_untranslatable; /* ACL_UNTRANS_* */
    /* verify */
    uint32_t verify_on_mismatch; /* VERIFY_MISMATCH_* */
    /* tuning */
    uint64_t shard_budget;
    uint64_t dir_split_threshold;
    uint32_t statx_batch; /* target statx in flight per walker ⇒ io_uring ring depth */
    int64_t  mtime_slop_ns;
    bool     dry_run;
    uint64_t options_hash;
};

/* work item kinds the agent executes */
enum {
    WI_SHARD = 0,     /* directory walk shard */
    WI_DELETE = 1,    /* DeleteBatch: remove destination orphans */
    WI_VERIFY = 2,    /* VerifyBatch: metadata + sampled checksum re-check */
    WI_ENTRYLIST = 3, /* EntryListShard: a name slice of a pathological dir */
    WI_CHUNK = 4,     /* ChunkTask: one byte range of a big file, or its finalize */
};

/* Per-shard walk overrides (proto WalkOverrides). The coordinator sends these
 * to fan a job out across the fleet; absent = use the job's own tuning. The
 * have_ flags matter: walk_budget = 0 ("descend nothing, push every subdir
 * back") is a real instruction, not a missing field. */
struct walk_overrides {
    bool     have_budget;
    uint64_t budget;
    bool     have_split_threshold;
    uint64_t split_threshold;
};

/* WI_CHUNK: one byte range of a big file (proto ChunkTask), or (finalize) the
 * terminal task that fsyncs, applies metadata and renames the assembled temp
 * into place. rel_path holds the file; temp_name is the coordinator-assigned
 * shared temp all of the file's chunks write. */
struct chunk_info {
    uint64_t offset, length;
    uint64_t gen_size;      /* abort if the source no longer matches this... */
    int64_t  gen_mtime_ns;  /* ...size/mtime pair (RESULT_SRC_CHANGED) */
    bool     create_temp;   /* this chunk creates + preallocates the temp */
    bool     finalize;      /* fsync + metadata + rename; no byte range */
    char    *temp_name;     /* malloc'd; owner frees */
};

struct shard_item {
    uint64_t lease_id;
    uint32_t lease_ttl_s;
    uint64_t shard_id; /* shard or task id */
    uint64_t job_id;
    uint32_t pass_no;
    int      kind;      /* WI_* */
    char    *rel_path;  /* WI_SHARD: malloc'd; owner frees */
    char   **paths;     /* WI_DELETE/WI_VERIFY: malloc'd array of rel paths */
    unsigned char *vchecksum; /* WI_VERIFY: per-path checksum flag */
    size_t   n_paths;
    struct walk_overrides ov; /* WI_SHARD/WI_ENTRYLIST */
    struct chunk_info chunk;  /* WI_CHUNK */
};
void shard_item_free(struct shard_item *it);

#define GRANT_MAX_ITEMS   64
#define GRANT_MAX_OPTIONS 8

struct work_grant {
    struct shard_item  items[GRANT_MAX_ITEMS];
    size_t             n_items;
    struct job_options options[GRANT_MAX_OPTIONS];
    size_t             n_options;
    size_t             n_unsupported; /* non-Shard work items skipped (slice 1) */
};

struct shard_counters {
    uint64_t entries_walked;
    uint64_t files_copied;
    uint64_t bytes_copied;
    uint64_t meta_fixed;
    uint64_t clean;
    uint64_t orphans;
    uint64_t errors;
    uint64_t dirs;
    uint64_t symlinks;
    uint64_t specials;
    uint64_t nlink_dup_files;
    uint64_t nlink_dup_bytes;
    uint64_t wall_ms;
    uint64_t fidelity_exceptions; /* attribute not preservable: counted, never dropped */
    uint64_t verify_ok;
    uint64_t verify_fail;
};

struct cached_opts {
    uint64_t job_id;
    uint64_t options_hash;
};

struct stats_snapshot {
    uint64_t entries_scanned;
    uint64_t files_copied;
    uint64_t bytes_copied;
    uint64_t meta_fixed;
    uint64_t errors;
    uint32_t shard_queue_depth;
    uint32_t copy_queue_depth;
    uint64_t rss_bytes;
};

/* encode (payload only; framing is wire.c's job) */
void enc_hello(pb_buf *b, const char *agent_id, const char *hostname,
               const char *version, uint32_t cores, uint64_t mem_limit,
               bool io_uring);
void enc_heartbeat(pb_buf *b, uint64_t seq, const uint64_t *leases, size_t n_leases,
                   uint32_t shard_queue_depth, uint32_t copy_queue_depth,
                   uint64_t rss_bytes);
void enc_work_request(pb_buf *b, uint32_t shard_credits,
                      const struct cached_opts *cached, size_t n_cached);
void enc_shard_split(pb_buf *b, uint64_t parent_shard_id, uint64_t seq,
                     char *const *subdirs, size_t n_subdirs);
/* ShardSplit carrying one EntryListShard: a dir_rel plus a batch of names
 * (the source-side slice of a directory over dir_split_threshold). */
void enc_entrylist_split(pb_buf *b, uint64_t parent_shard_id, uint64_t seq,
                         const char *dir_rel, char *const *names, size_t n_names);
/* ShardSplit carrying big files: rel_path + size + mtime_ns each. The
 * coordinator lays them out into ChunkTasks (proto ShardSplit.BigFile). */
struct bigfile {
    char    *rel;
    uint64_t size;
    int64_t  mtime_ns;
};
void enc_bigfile_split(pb_buf *b, uint64_t parent_shard_id, uint64_t seq,
                       const struct bigfile *files, size_t n_files);
void enc_shard_result(pb_buf *b, uint64_t shard_id, uint64_t lease_id, int status,
                      const struct shard_counters *c, const char *error);
void enc_stats(pb_buf *b, const struct stats_snapshot *s);

/* journal (docs/DESIGN-coordinator.md §5): records are varint-length-
 * delimited JournalRecord messages; batches carry them zstd-compressed */
struct estat; /* agent.h */
void enc_journal_record(pb_buf *b, int type, const char *rel_path,
                        const struct estat *src, const struct estat *dst,
                        int err_no, const char *detail,
                        uint64_t xxh_lo, uint64_t xxh_hi);
void enc_journal_batch(pb_buf *b, uint64_t seq, uint64_t job_id, uint32_t pass_no,
                       uint32_t record_count, const void *zstd, size_t zstd_len);
bool dec_journal_ack(const uint8_t *p, size_t n, uint64_t *acked_seq);

/* decode; all return false on malformed payloads */
bool dec_hello_ack(const uint8_t *p, size_t n, struct hello_ack *out);
bool dec_hb_ack(const uint8_t *p, size_t n, struct hb_ack *out);
bool dec_work_grant(const uint8_t *p, size_t n, struct work_grant *out);
bool dec_split_ack(const uint8_t *p, size_t n, uint64_t *parent, uint64_t *seq);
bool dec_control(const uint8_t *p, size_t n, uint32_t *cmd, uint64_t *job_id);
void work_grant_free(struct work_grant *g);

#endif
