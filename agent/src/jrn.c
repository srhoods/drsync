/* Per-shard journal buffering → zstd JournalBatch frames (design §5).
 * Records are appended by walker AND copy threads (mutex-protected buffer);
 * batches flush at a size threshold and at shard end. Sequence numbers are
 * agent-global; the control thread feeds JournalAck high-water marks back and
 * a shard's result waits until its highest seq is acked — that ordering plus
 * lease-expiry re-runs gives at-least-once journaling (readers dedup). */
#include "agent.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <zstd.h>

#define JRN_FLUSH_RAW (1 << 20) /* flush when uncompressed buffer hits 1 MiB */
#define JRN_ACK_TIMEOUT_S 120

static atomic_ullong   g_seq;      /* last assigned batch seq */
static uint64_t        g_acked;    /* high-water from JournalAck */
static pthread_mutex_t ack_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  ack_cv = PTHREAD_COND_INITIALIZER;

void jrn_ack_update(uint64_t acked_seq)
{
    pthread_mutex_lock(&ack_mu);
    if (acked_seq > g_acked) {
        g_acked = acked_seq;
        pthread_cond_broadcast(&ack_cv);
    }
    pthread_mutex_unlock(&ack_mu);
}

void jrn_init(struct walk_ctx *ctx)
{
    pthread_mutex_init(&ctx->jrn.mu, NULL);
    pb_init(&ctx->jrn.raw);
    ctx->jrn.count = 0;
    ctx->jrn.max_seq = 0;
}

/* caller holds ctx->jrn.mu */
static void flush_locked(struct walk_ctx *ctx)
{
    struct jrn *j = &ctx->jrn;
    if (!j->count || j->raw.oom)
        goto reset;

    size_t bound = ZSTD_compressBound(j->raw.len);
    void *z = malloc(bound);
    if (!z) {
        LOGE("journal flush oom (%zu records dropped)", (size_t)j->count);
        CTR_ADD(ctx->c.errors, 1);
        goto reset;
    }
    size_t zn = ZSTD_compress(z, bound, j->raw.p, j->raw.len, 1);
    if (ZSTD_isError(zn)) {
        LOGE("journal zstd: %s", ZSTD_getErrorName(zn));
        CTR_ADD(ctx->c.errors, 1);
        free(z);
        goto reset;
    }

    uint64_t seq = atomic_fetch_add(&g_seq, 1) + 1;
    pb_buf b;
    pb_init(&b);
    enc_journal_batch(&b, seq, ctx->it->job_id, ctx->it->pass_no,
                      j->count, z, zn);
    free(z);
    out_push(FR_JOURNAL_BATCH, &b);
    if (seq > j->max_seq)
        j->max_seq = seq;
reset:
    pb_free(&j->raw);
    pb_init(&j->raw);
    j->count = 0;
}

static void jrn_emit2(struct walk_ctx *ctx, int type, const char *rel_path,
                      const struct estat *src, const struct estat *dst,
                      int err_no, const char *detail,
                      uint64_t xxh_lo, uint64_t xxh_hi);

void jrn_emit(struct walk_ctx *ctx, int type, const char *rel_path,
              const struct estat *src, const struct estat *dst,
              int err_no, const char *detail)
{
    jrn_emit2(ctx, type, rel_path, src, dst, err_no, detail, 0, 0);
}

void jrn_emit_hash(struct walk_ctx *ctx, int type, const char *rel_path,
                   const struct estat *src, const struct estat *dst,
                   uint64_t xxh_lo, uint64_t xxh_hi)
{
    jrn_emit2(ctx, type, rel_path, src, dst, 0, NULL, xxh_lo, xxh_hi);
}

static void jrn_emit2(struct walk_ctx *ctx, int type, const char *rel_path,
                      const struct estat *src, const struct estat *dst,
                      int err_no, const char *detail,
                      uint64_t xxh_lo, uint64_t xxh_hi)
{
    pb_buf rec;
    pb_init(&rec);
    enc_journal_record(&rec, type, rel_path, src, dst, err_no, detail,
                       xxh_lo, xxh_hi);
    if (rec.oom) {
        pb_free(&rec);
        return;
    }
    struct jrn *j = &ctx->jrn;
    pthread_mutex_lock(&j->mu);
    pb_put_varint(&j->raw, rec.len); /* length-delimited framing */
    pb_append(&j->raw, rec.p, rec.len);
    j->count++;
    if (j->raw.len >= JRN_FLUSH_RAW)
        flush_locked(ctx);
    pthread_mutex_unlock(&j->mu);
    pb_free(&rec);
}

void jrn_flush(struct walk_ctx *ctx)
{
    pthread_mutex_lock(&ctx->jrn.mu);
    flush_locked(ctx);
    pthread_mutex_unlock(&ctx->jrn.mu);
}

bool jrn_wait_acked(struct walk_ctx *ctx)
{
    uint64_t need = ctx->jrn.max_seq;
    if (!need)
        return true;
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += JRN_ACK_TIMEOUT_S;
    pthread_mutex_lock(&ack_mu);
    while (g_acked < need) {
        if (pthread_cond_timedwait(&ack_cv, &ack_mu, &dl) == ETIMEDOUT) {
            pthread_mutex_unlock(&ack_mu);
            return false;
        }
    }
    pthread_mutex_unlock(&ack_mu);
    return true;
}

void jrn_destroy(struct walk_ctx *ctx)
{
    pb_free(&ctx->jrn.raw);
    pthread_mutex_destroy(&ctx->jrn.mu);
}
