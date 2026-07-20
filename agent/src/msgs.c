/* Message encode/decode. Field numbers pinned to proto/drsync.proto — when
 * that file changes, this file must change with it (checked by the protocol
 * conformance test, which round-trips against the Go implementation). */
#include "msgs.h"
#include "agent.h"

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

/* ---------------- encode ---------------- */

void enc_hello(pb_buf *b, const char *agent_id, const char *hostname,
               const char *version, uint32_t cores, uint64_t mem_limit,
               bool io_uring)
{
    pb_put_str(b, 1, agent_id);
    pb_put_str(b, 2, hostname);
    pb_put_u64(b, 3, PROTO_MAJOR);
    pb_put_u64(b, 4, PROTO_MINOR);
    pb_put_str(b, 5, version);
    pb_put_u64(b, 6, cores);
    pb_put_u64(b, 7, mem_limit);
    pb_put_bool(b, 8, io_uring);
}

const char *wi_kind_name(int kind)
{
    switch (kind) {
    case WI_SHARD:     return "dir";
    case WI_DELETE:    return "delete";
    case WI_VERIFY:    return "verify";
    case WI_ENTRYLIST: return "entrylist";
    case WI_CHUNK:     return "chunk";
    case WI_PROBE:     return "probe";
    case WI_DIRFIX:    return "dirfix";
    default:           return "unknown";
    }
}

void enc_heartbeat(pb_buf *b, uint64_t seq, const uint64_t *leases, size_t n_leases,
                   uint32_t shard_queue_depth, uint32_t copy_queue_depth,
                   uint64_t rss_bytes, const struct inflight_view *inflight,
                   size_t n_inflight)
{
    pb_put_u64(b, 1, seq);
    if (n_leases) { /* packed repeated varint */
        pb_buf packed;
        pb_init(&packed);
        for (size_t i = 0; i < n_leases; i++)
            pb_put_varint(&packed, leases[i]);
        pb_put_msg(b, 2, &packed);
        pb_free(&packed);
    }
    pb_put_u64(b, 3, shard_queue_depth);
    pb_put_u64(b, 4, copy_queue_depth);
    pb_put_u64(b, 6, rss_bytes);
    for (size_t i = 0; i < n_inflight; i++) { /* 8: repeated InflightItem */
        const struct inflight_view *v = &inflight[i];
        pb_buf sub;
        pb_init(&sub);
        pb_put_u64(&sub, 1, v->lease_id);
        pb_put_u64(&sub, 2, v->shard_id);
        pb_put_u64(&sub, 3, v->job_id);
        pb_put_str(&sub, 4, wi_kind_name(v->kind));
        pb_put_str(&sub, 5, v->rel_path);
        pb_put_u64(&sub, 6, v->held_ms);
        pb_put_u64(&sub, 7, v->running_ms);
        pb_put_bool(&sub, 8, v->running);
        pb_put_u64(&sub, 9, v->entries_done);
        pb_put_msg(b, 8, &sub);
        pb_free(&sub);
    }
}

void enc_work_request(pb_buf *b, uint32_t shard_credits,
                      const struct cached_opts *cached, size_t n_cached)
{
    pb_put_u64(b, 1, shard_credits);
    for (size_t i = 0; i < n_cached; i++) {
        pb_buf sub;
        pb_init(&sub);
        pb_put_u64(&sub, 1, cached[i].job_id);
        pb_put_u64(&sub, 2, cached[i].options_hash);
        pb_put_msg(b, 3, &sub);
        pb_free(&sub);
    }
}

void enc_shard_split(pb_buf *b, uint64_t parent_shard_id, uint64_t seq,
                     char *const *subdirs, size_t n_subdirs)
{
    pb_put_u64(b, 1, parent_shard_id);
    pb_put_u64(b, 2, seq);
    for (size_t i = 0; i < n_subdirs; i++) {
        pb_buf sub;
        pb_init(&sub);
        pb_put_bytes(&sub, 1, subdirs[i], strlen(subdirs[i]));
        pb_put_msg(b, 3, &sub);
        pb_free(&sub);
    }
}

void enc_entrylist_split(pb_buf *b, uint64_t parent_shard_id, uint64_t seq,
                         const char *dir_rel, char *const *names, size_t n_names)
{
    pb_put_u64(b, 1, parent_shard_id);
    pb_put_u64(b, 2, seq);
    /* one ShardSplit.NewEntryList (field 4): dir_rel (1) + repeated names (2) */
    pb_buf el;
    pb_init(&el);
    pb_put_bytes(&el, 1, dir_rel, strlen(dir_rel));
    for (size_t i = 0; i < n_names; i++)
        pb_put_bytes(&el, 2, names[i], strlen(names[i]));
    pb_put_msg(b, 4, &el);
    pb_free(&el);
}

void enc_bigfile_split(pb_buf *b, uint64_t parent_shard_id, uint64_t seq,
                       const struct bigfile *files, size_t n_files)
{
    pb_put_u64(b, 1, parent_shard_id);
    pb_put_u64(b, 2, seq);
    for (size_t i = 0; i < n_files; i++) {
        /* one ShardSplit.BigFile (field 5): rel_path(1), size(2), mtime_ns(3) */
        pb_buf bf;
        pb_init(&bf);
        pb_put_bytes(&bf, 1, files[i].rel, strlen(files[i].rel));
        pb_put_u64(&bf, 2, files[i].size);
        pb_put_i64(&bf, 3, files[i].mtime_ns);
        pb_put_msg(b, 5, &bf);
        pb_free(&bf);
    }
}

static void enc_counters(pb_buf *b, uint32_t field, const struct shard_counters *c)
{
    pb_buf sub;
    pb_init(&sub);
    pb_put_u64(&sub, 1, c->entries_walked);
    pb_put_u64(&sub, 2, c->files_copied);
    pb_put_u64(&sub, 3, c->bytes_copied);
    pb_put_u64(&sub, 4, c->meta_fixed);
    pb_put_u64(&sub, 5, c->clean);
    pb_put_u64(&sub, 6, c->orphans);
    pb_put_u64(&sub, 7, c->errors);
    pb_put_u64(&sub, 8, c->dirs);
    pb_put_u64(&sub, 9, c->symlinks);
    pb_put_u64(&sub, 10, c->specials);
    pb_put_u64(&sub, 11, c->nlink_dup_files);
    pb_put_u64(&sub, 12, c->nlink_dup_bytes);
    pb_put_u64(&sub, 13, c->wall_ms);
    pb_put_u64(&sub, 14, c->fidelity_exceptions);
    pb_put_u64(&sub, 15, c->verify_ok);
    pb_put_u64(&sub, 16, c->verify_fail);
    pb_put_msg(b, field, &sub);
    pb_free(&sub);
}

void enc_shard_result(pb_buf *b, uint64_t shard_id, uint64_t lease_id, int status,
                      const struct shard_counters *c, const char *error)
{
    pb_put_u64(b, 1, shard_id);
    pb_put_u64(b, 2, lease_id);
    pb_put_u64(b, 3, (uint64_t)status);
    if (c)
        enc_counters(b, 4, c);
    pb_put_str(b, 5, error);
}

void enc_stats(pb_buf *b, const struct stats_snapshot *s)
{
    pb_put_u64(b, 2, s->entries_scanned);
    pb_put_u64(b, 3, s->files_copied);
    pb_put_u64(b, 4, s->bytes_copied);
    pb_put_u64(b, 5, s->meta_fixed);
    pb_put_u64(b, 7, s->errors);
    pb_put_u64(b, 8, s->shard_queue_depth);
    pb_put_u64(b, 9, s->copy_queue_depth);
    pb_put_u64(b, 10, s->rss_bytes);
}

static uint32_t entry_type_of(uint32_t mode)
{
    switch (mode & S_IFMT) {
    case S_IFREG:  return 1;
    case S_IFDIR:  return 2;
    case S_IFLNK:  return 3;
    case S_IFCHR:  return 4;
    case S_IFBLK:  return 5;
    case S_IFIFO:  return 6;
    case S_IFSOCK: return 7;
    default:       return 0;
    }
}

static void enc_stat_info(pb_buf *b, uint32_t field, const struct estat *e)
{
    pb_buf sub;
    pb_init(&sub);
    pb_put_u64(&sub, 1, entry_type_of(e->mode));
    pb_put_u64(&sub, 2, e->mode & 07777);
    pb_put_u64(&sub, 3, e->uid);
    pb_put_u64(&sub, 4, e->gid);
    pb_put_u64(&sub, 5, e->size);
    pb_put_i64(&sub, 6, e->atim.tv_sec * 1000000000 + e->atim.tv_nsec);
    pb_put_i64(&sub, 7, e->mtim.tv_sec * 1000000000 + e->mtim.tv_nsec);
    pb_put_u64(&sub, 10, e->ino);
    pb_put_u64(&sub, 11, e->nlink);
    pb_put_u64(&sub, 12, e->rdev_major);
    pb_put_u64(&sub, 13, e->rdev_minor);
    pb_put_u64(&sub, 14, e->blocks);
    pb_put_msg(b, field, &sub);
    pb_free(&sub);
}

void enc_journal_record(pb_buf *b, int type, const char *rel_path,
                        const struct estat *src, const struct estat *dst,
                        int err_no, const char *detail,
                        uint64_t xxh_lo, uint64_t xxh_hi)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    pb_put_u64(b, 1, (uint64_t)type);
    pb_put_bytes(b, 2, rel_path, strlen(rel_path));
    pb_put_i64(b, 3, ts.tv_sec * 1000000000 + ts.tv_nsec);
    if (src)
        enc_stat_info(b, 4, src);
    if (dst)
        enc_stat_info(b, 5, dst);
    pb_put_u64(b, 6, xxh_lo);
    pb_put_u64(b, 7, xxh_hi);
    pb_put_i64(b, 8, err_no);
    pb_put_str(b, 9, detail);
}

void enc_journal_batch(pb_buf *b, uint64_t seq, uint64_t job_id, uint32_t pass_no,
                       uint32_t record_count, const void *zstd, size_t zstd_len)
{
    pb_put_u64(b, 1, seq);
    pb_put_u64(b, 2, job_id);
    pb_put_u64(b, 3, pass_no);
    pb_put_u64(b, 4, record_count);
    pb_put_bytes(b, 5, zstd, zstd_len);
}

bool dec_journal_ack(const uint8_t *p, size_t n, uint64_t *acked_seq)
{
    *acked_seq = 0;
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        if (f == 1)
            *acked_seq = pb_get_varint(&c);
        else
            pb_skip(&c, wt);
    }
    return !c.err;
}

/* ---------------- decode ---------------- */

bool dec_hello_ack(const uint8_t *p, size_t n, struct hello_ack *out)
{
    memset(out, 0, sizeof(*out));
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: out->accepted = pb_get_varint(&c) != 0; break;
        case 2: pb_get_strn(&c, out->reject_reason, sizeof out->reject_reason); break;
        case 3: out->proto_major = (uint32_t)pb_get_varint(&c); break;
        case 4: out->hb_interval_s = (uint32_t)pb_get_varint(&c); break;
        case 5: out->lease_ttl_s = (uint32_t)pb_get_varint(&c); break;
        case 6: out->fleet_epoch = pb_get_varint(&c); break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

bool dec_hb_ack(const uint8_t *p, size_t n, struct hb_ack *out)
{
    memset(out, 0, sizeof(*out));
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: out->seq = pb_get_varint(&c); break;
        case 2: out->pause = pb_get_varint(&c) != 0; break;
        case 3: out->drain = pb_get_varint(&c) != 0; break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

/* WalkOverrides: proto3 optional, so a field that is present at all is an
 * explicit value — including zero. */
static bool dec_walk_overrides(const uint8_t *p, size_t n, struct walk_overrides *ov)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1:
            ov->budget = pb_get_varint(&c);
            ov->have_budget = true;
            break;
        case 2:
            ov->split_threshold = pb_get_varint(&c);
            ov->have_split_threshold = true;
            break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

static bool dec_shard(const uint8_t *p, size_t n, struct shard_item *it)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: it->shard_id = pb_get_varint(&c); break;
        case 2: it->job_id = pb_get_varint(&c); break;
        case 3: it->pass_no = (uint32_t)pb_get_varint(&c); break;
        case 4: {
            const uint8_t *sp;
            size_t sn;
            if (!pb_get_len(&c, &sp, &sn))
                return false;
            free(it->rel_path);
            it->rel_path = malloc(sn + 1);
            if (!it->rel_path)
                return false;
            memcpy(it->rel_path, sp, sn);
            it->rel_path[sn] = '\0';
            break;
        }
        case 5: {
            const uint8_t *sp;
            size_t sn;
            if (!pb_get_len(&c, &sp, &sn) || !dec_walk_overrides(sp, sn, &it->ov))
                return false;
            break;
        }
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

static bool dec_copy_opts(const uint8_t *p, size_t n, struct job_options *o)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: o->chunk_threshold = pb_get_varint(&c); break;
        case 2: o->chunk_size = pb_get_varint(&c); break;
        case 3: o->buffer_size = pb_get_varint(&c); break;
        case 4: o->preserve_sparse = pb_get_varint(&c) != 0; break;
        case 5: o->server_side_copy = (uint32_t)pb_get_varint(&c); break;
        case 6: pb_get_strn(&c, o->temp_prefix, sizeof o->temp_prefix); break;
        case 7: o->fsync_per_file = pb_get_varint(&c) == 1; /* FSYNC_PER_FILE */ break;
        case 9: o->direct_write = pb_get_varint(&c) != 0; break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

static bool dec_acl_opts(const uint8_t *p, size_t n, struct job_options *o)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: o->acl_posix = pb_get_varint(&c) != 0; break;
        case 2: o->acl_nfs4 = pb_get_varint(&c) != 0; break;
        case 3: o->acl_untranslatable = (uint32_t)pb_get_varint(&c); break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

static bool dec_meta_opts(const uint8_t *p, size_t n, struct job_options *o)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    const uint8_t *sp;
    size_t sn;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: o->meta_owner = pb_get_varint(&c) != 0; break;
        case 2: o->meta_mode = pb_get_varint(&c) != 0; break;
        case 3: o->meta_times = pb_get_varint(&c) != 0; break;
        case 4: o->meta_xattrs = pb_get_varint(&c) != 0; break;
        case 5:
            if (!pb_get_len(&c, &sp, &sn) || !dec_acl_opts(sp, sn, o))
                return false;
            break;
        case 6: o->meta_specials = pb_get_varint(&c) != 0; break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

static bool dec_verify_opts(const uint8_t *p, size_t n, struct job_options *o)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 2: o->verify_on_mismatch = (uint32_t)pb_get_varint(&c); break;
        default: pb_skip(&c, wt); /* sample selection is coordinator-side */
        }
    }
    return !c.err;
}

static bool dec_tuning_opts(const uint8_t *p, size_t n, struct job_options *o)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: o->shard_budget = pb_get_varint(&c); break;
        case 2: o->dir_split_threshold = pb_get_varint(&c); break;
        case 3: o->statx_batch = (uint32_t)pb_get_varint(&c); break;
        case 4: o->mtime_slop_ns = (int64_t)pb_get_varint(&c); break;
        case 6: o->entrylist_batch = (uint32_t)pb_get_varint(&c); break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

static bool dec_filter_rule(const uint8_t *p, size_t n, struct filter_rule *fr)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    fr->exclude = false;
    fr->pattern[0] = '\0';
    const uint8_t *sp;
    size_t sn;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: fr->exclude = pb_get_varint(&c) != 0; break;
        case 2:
            /* Guard against silent truncation: the coordinator bounds the
             * pattern below FILTER_PATTERN_MAX, so an over-length pattern is a
             * corrupt frame or a gross version skew — reject rather than match
             * a truncated glob (which would copy data the operator excluded). */
            if (!pb_get_len(&c, &sp, &sn) || sn >= sizeof fr->pattern)
                return false;
            memcpy(fr->pattern, sp, sn);
            fr->pattern[sn] = '\0';
            break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err && fr->pattern[0];
}

static bool dec_job_options(const uint8_t *p, size_t n, struct job_options *o)
{
    memset(o, 0, sizeof(*o));
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    const uint8_t *sp;
    size_t sn;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: o->job_id = pb_get_varint(&c); break;
        case 2: pb_get_strn(&c, o->job_name, sizeof o->job_name); break;
        case 3: pb_get_strn(&c, o->src_root, sizeof o->src_root); break;
        case 4: pb_get_strn(&c, o->dst_root, sizeof o->dst_root); break;
        case 5:
            /* An over-limit rule count means agent/coordinator version skew:
             * fail loudly rather than silently drop a filter and copy excluded
             * data. The coordinator caps the count at FILTER_MAX_RULES. */
            if (!pb_get_len(&c, &sp, &sn) || o->n_filters >= FILTER_MAX_RULES ||
                !dec_filter_rule(sp, sn, &o->filters[o->n_filters]))
                return false;
            o->n_filters++;
            break;
        case 6:
            if (!pb_get_len(&c, &sp, &sn) || !dec_copy_opts(sp, sn, o))
                return false;
            break;
        case 7:
            if (!pb_get_len(&c, &sp, &sn) || !dec_meta_opts(sp, sn, o))
                return false;
            break;
        case 8:
            if (!pb_get_len(&c, &sp, &sn) || !dec_verify_opts(sp, sn, o))
                return false;
            break;
        case 10:
            if (!pb_get_len(&c, &sp, &sn) || !dec_tuning_opts(sp, sn, o))
                return false;
            break;
        case 11: o->dry_run = pb_get_varint(&c) != 0; break;
        case 12: o->options_hash = pb_get_varint(&c); break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err && o->job_id && o->src_root[0] && o->dst_root[0];
}

void shard_item_free(struct shard_item *it)
{
    free(it->rel_path);
    for (size_t i = 0; i < it->n_paths; i++)
        free(it->paths[i]);
    free(it->paths);
    free(it->vchecksum);
    for (size_t i = 0; i < it->n_dirs; i++)
        free(it->dirs[i].rel_path);
    free(it->dirs);
    free(it->chunk.temp_name);
    it->rel_path = NULL;
    it->paths = NULL;
    it->vchecksum = NULL;
    it->dirs = NULL;
    it->chunk.temp_name = NULL;
    it->n_paths = 0;
    it->n_dirs = 0;
}

/* ProbeTask { task_id = 1, job_id = 2 }. task_id is the probe shard's id; the
 * result is reported as a ShardResult keyed by it, so store it as shard_id. */
static bool dec_probe_task(const uint8_t *p, size_t n, struct shard_item *it)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: it->shard_id = pb_get_varint(&c); break;
        case 2: it->job_id = pb_get_varint(&c); break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err && it->shard_id && it->job_id;
}

static bool dec_delete_batch(const uint8_t *p, size_t n, struct shard_item *it)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    size_t cap = 0;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: it->shard_id = pb_get_varint(&c); break;
        case 2: it->job_id = pb_get_varint(&c); break;
        case 3: it->pass_no = (uint32_t)pb_get_varint(&c); break;
        case 4: {
            const uint8_t *sp;
            size_t sn;
            if (!pb_get_len(&c, &sp, &sn))
                return false;
            if (it->n_paths == cap) {
                cap = cap ? cap * 2 : 64;
                char **nv = realloc(it->paths, cap * sizeof *nv);
                if (!nv)
                    return false;
                it->paths = nv;
            }
            char *s = malloc(sn + 1);
            if (!s)
                return false;
            memcpy(s, sp, sn);
            s[sn] = '\0';
            it->paths[it->n_paths++] = s;
            break;
        }
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

static bool dec_verify_entry(const uint8_t *p, size_t n, struct shard_item *it,
                             size_t *cap)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    char *path = NULL;
    bool checksum = false;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: {
            const uint8_t *sp;
            size_t sn;
            if (!pb_get_len(&c, &sp, &sn))
                goto fail;
            free(path);
            path = malloc(sn + 1);
            if (!path)
                goto fail;
            memcpy(path, sp, sn);
            path[sn] = '\0';
            break;
        }
        case 2: checksum = pb_get_varint(&c) != 0; break;
        default: pb_skip(&c, wt);
        }
    }
    if (c.err || !path)
        goto fail;
    if (it->n_paths == *cap) {
        *cap = *cap ? *cap * 2 : 64;
        char **np = realloc(it->paths, *cap * sizeof *np);
        if (!np)
            goto fail; /* originals stay owned by it (freed by caller) */
        it->paths = np;
        unsigned char *nc = realloc(it->vchecksum, *cap);
        if (!nc)
            goto fail;
        it->vchecksum = nc;
    }
    it->paths[it->n_paths] = path;
    it->vchecksum[it->n_paths] = checksum;
    it->n_paths++;
    return true;
fail:
    free(path);
    return false;
}

/* DirMeta { rel_path=1, uid=2, gid=3, mode=4, atime_ns=5, mtime_ns=6 } → one
 * struct dirmeta appended to it->dirs. */
static bool dec_dir_meta(const uint8_t *p, size_t n, struct shard_item *it,
                         size_t *cap)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    struct dirmeta dm = {0};
    const uint8_t *sp;
    size_t sn;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1:
            if (!pb_get_len(&c, &sp, &sn))
                goto fail;
            free(dm.rel_path);
            dm.rel_path = malloc(sn + 1);
            if (!dm.rel_path)
                goto fail;
            memcpy(dm.rel_path, sp, sn);
            dm.rel_path[sn] = '\0';
            break;
        case 2: dm.uid = (uint32_t)pb_get_varint(&c); break;
        case 3: dm.gid = (uint32_t)pb_get_varint(&c); break;
        case 4: dm.mode = (uint32_t)pb_get_varint(&c); break;
        case 5: dm.atime_ns = (int64_t)pb_get_varint(&c); break;
        case 6: dm.mtime_ns = (int64_t)pb_get_varint(&c); break;
        default: pb_skip(&c, wt);
        }
    }
    if (c.err)
        goto fail;
    if (!dm.rel_path) {
        /* Empty rel_path (the root directory) is omitted on the wire by proto3;
         * treat an absent field as "". */
        dm.rel_path = calloc(1, 1);
        if (!dm.rel_path)
            goto fail;
    }
    if (it->n_dirs == *cap) {
        *cap = *cap ? *cap * 2 : 64;
        struct dirmeta *nd = realloc(it->dirs, *cap * sizeof *nd);
        if (!nd)
            goto fail; /* originals stay owned by it (freed by caller) */
        it->dirs = nd;
    }
    it->dirs[it->n_dirs++] = dm;
    return true;
fail:
    free(dm.rel_path);
    return false;
}

/* DirFixBatch { task_id=1, job_id=2, pass_no=3, dirs=4 (repeated DirMeta) }. */
static bool dec_dirfix_batch(const uint8_t *p, size_t n, struct shard_item *it)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    size_t cap = 0;
    const uint8_t *sp;
    size_t sn;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: it->shard_id = pb_get_varint(&c); break;
        case 2: it->job_id = pb_get_varint(&c); break;
        case 3: it->pass_no = (uint32_t)pb_get_varint(&c); break;
        case 4:
            if (!pb_get_len(&c, &sp, &sn) || !dec_dir_meta(sp, sn, it, &cap))
                return false;
            break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

static bool dec_verify_batch(const uint8_t *p, size_t n, struct shard_item *it)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    size_t cap = 0;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: it->shard_id = pb_get_varint(&c); break;
        case 2: it->job_id = pb_get_varint(&c); break;
        case 3: it->pass_no = (uint32_t)pb_get_varint(&c); break;
        case 4: {
            const uint8_t *sp;
            size_t sn;
            if (!pb_get_len(&c, &sp, &sn) || !dec_verify_entry(sp, sn, it, &cap))
                return false;
            break;
        }
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

/* EntryListShard: dir_rel (field 4) → rel_path, names (field 5, repeated bytes)
 * → paths[]. The names are a source-side slice of a pathological directory. */
static bool dec_entrylist(const uint8_t *p, size_t n, struct shard_item *it)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    size_t cap = 0;
    while (pb_next(&c, &f, &wt)) {
        const uint8_t *sp;
        size_t sn;
        switch (f) {
        case 1: it->shard_id = pb_get_varint(&c); break;
        case 2: it->job_id = pb_get_varint(&c); break;
        case 3: it->pass_no = (uint32_t)pb_get_varint(&c); break;
        case 4:
            if (!pb_get_len(&c, &sp, &sn))
                return false;
            free(it->rel_path);
            it->rel_path = malloc(sn + 1);
            if (!it->rel_path)
                return false;
            memcpy(it->rel_path, sp, sn);
            it->rel_path[sn] = '\0';
            break;
        case 5: {
            if (!pb_get_len(&c, &sp, &sn))
                return false;
            if (it->n_paths == cap) {
                cap = cap ? cap * 2 : 256;
                char **nv = realloc(it->paths, cap * sizeof *nv);
                if (!nv)
                    return false;
                it->paths = nv;
            }
            char *s = malloc(sn + 1);
            if (!s)
                return false;
            memcpy(s, sp, sn);
            s[sn] = '\0';
            it->paths[it->n_paths++] = s;
            break;
        }
        case 6:
            if (!pb_get_len(&c, &sp, &sn) || !dec_walk_overrides(sp, sn, &it->ov))
                return false;
            break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

/* FileGen submessage (ChunkTask.gen): size (1), mtime_ns (2). */
static bool dec_filegen(const uint8_t *p, size_t n, struct chunk_info *ch)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: ch->gen_size = pb_get_varint(&c); break;
        case 2: ch->gen_mtime_ns = (int64_t)pb_get_varint(&c); break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

static bool dec_chunk_task(const uint8_t *p, size_t n, struct shard_item *it)
{
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    const uint8_t *sp;
    size_t sn;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: it->shard_id = pb_get_varint(&c); break;
        case 2: it->job_id = pb_get_varint(&c); break;
        case 3: it->pass_no = (uint32_t)pb_get_varint(&c); break;
        case 4:
            if (!pb_get_len(&c, &sp, &sn))
                return false;
            free(it->rel_path);
            it->rel_path = malloc(sn + 1);
            if (!it->rel_path)
                return false;
            memcpy(it->rel_path, sp, sn);
            it->rel_path[sn] = '\0';
            break;
        case 5: it->chunk.offset = pb_get_varint(&c); break;
        case 6: it->chunk.length = pb_get_varint(&c); break;
        case 7:
            if (!pb_get_len(&c, &sp, &sn) || !dec_filegen(sp, sn, &it->chunk))
                return false;
            break;
        case 8: it->chunk.create_temp = pb_get_varint(&c) != 0; break;
        case 9:
            if (!pb_get_len(&c, &sp, &sn))
                return false;
            free(it->chunk.temp_name);
            it->chunk.temp_name = malloc(sn + 1);
            if (!it->chunk.temp_name)
                return false;
            memcpy(it->chunk.temp_name, sp, sn);
            it->chunk.temp_name[sn] = '\0';
            break;
        case 10: it->chunk.finalize = pb_get_varint(&c) != 0; break;
        case 11: it->chunk.reclaim = pb_get_varint(&c) != 0; break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

static bool dec_work_item(const uint8_t *p, size_t n, struct work_grant *g)
{
    struct shard_item it = {0};
    bool have_item = false, unsupported = false;
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    const uint8_t *sp;
    size_t sn;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: it.lease_id = pb_get_varint(&c); break;
        case 2: it.lease_ttl_s = (uint32_t)pb_get_varint(&c); break;
        case 3:
            if (!pb_get_len(&c, &sp, &sn) || !dec_shard(sp, sn, &it))
                goto fail;
            it.kind = WI_SHARD;
            have_item = true;
            break;
        case 7:
            if (!pb_get_len(&c, &sp, &sn) || !dec_verify_batch(sp, sn, &it))
                goto fail;
            it.kind = WI_VERIFY;
            have_item = true;
            break;
        case 8:
            if (!pb_get_len(&c, &sp, &sn) || !dec_delete_batch(sp, sn, &it))
                goto fail;
            it.kind = WI_DELETE;
            have_item = true;
            break;
        case 4:
            if (!pb_get_len(&c, &sp, &sn) || !dec_entrylist(sp, sn, &it))
                goto fail;
            it.kind = WI_ENTRYLIST;
            have_item = true;
            break;
        case 5:
            if (!pb_get_len(&c, &sp, &sn) || !dec_chunk_task(sp, sn, &it))
                goto fail;
            it.kind = WI_CHUNK;
            have_item = true;
            break;
        case 6:
            if (!pb_get_len(&c, &sp, &sn) || !dec_dirfix_batch(sp, sn, &it))
                goto fail;
            it.kind = WI_DIRFIX;
            have_item = true;
            break;
        case 9:
            if (!pb_get_len(&c, &sp, &sn) || !dec_probe_task(sp, sn, &it))
                goto fail;
            it.kind = WI_PROBE;
            have_item = true;
            break;
        default: pb_skip(&c, wt);
        }
    }
    if (c.err)
        goto fail;
    if (have_item && g->n_items < GRANT_MAX_ITEMS) {
        if ((it.kind == WI_SHARD || it.kind == WI_ENTRYLIST) && !it.rel_path)
            it.rel_path = strdup("");
        g->items[g->n_items++] = it;
    } else {
        if (unsupported)
            g->n_unsupported++;
        shard_item_free(&it);
    }
    return true;
fail:
    shard_item_free(&it);
    return false;
}

bool dec_work_grant(const uint8_t *p, size_t n, struct work_grant *g)
{
    memset(g, 0, sizeof(*g));
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    const uint8_t *sp;
    size_t sn;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1:
            if (!pb_get_len(&c, &sp, &sn) || !dec_work_item(sp, sn, g))
                goto fail;
            break;
        case 2:
            if (!pb_get_len(&c, &sp, &sn))
                goto fail;
            if (g->n_options < GRANT_MAX_OPTIONS) {
                if (!dec_job_options(sp, sn, &g->options[g->n_options]))
                    goto fail;
                g->n_options++;
            }
            break;
        default: pb_skip(&c, wt);
        }
    }
    if (!c.err)
        return true;
fail:
    work_grant_free(g);
    return false;
}

void work_grant_free(struct work_grant *g)
{
    for (size_t i = 0; i < g->n_items; i++)
        shard_item_free(&g->items[i]);
    g->n_items = 0;
}

bool dec_split_ack(const uint8_t *p, size_t n, uint64_t *parent, uint64_t *seq)
{
    *parent = *seq = 0;
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: *parent = pb_get_varint(&c); break;
        case 2: *seq = pb_get_varint(&c); break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}

bool dec_control(const uint8_t *p, size_t n, uint32_t *cmd, uint64_t *job_id)
{
    *cmd = 0;
    *job_id = 0;
    pb_cur c;
    pb_cur_init(&c, p, n);
    uint32_t f;
    int wt;
    while (pb_next(&c, &f, &wt)) {
        switch (f) {
        case 1: *cmd = (uint32_t)pb_get_varint(&c); break;
        case 2: *job_id = pb_get_varint(&c); break;
        default: pb_skip(&c, wt);
        }
    }
    return !c.err;
}
