/* Shared ShardSplit outbox machinery: ships a prepared split frame without
 * blocking the caller, tracks in-flight acks in a bounded window (SPLIT_
 * WINDOW), and drains every outstanding ack before a shard reports its
 * result — the ordering invariant protocol doc §4.2 requires (a shard's
 * ShardResult must not be sent until every split it produced is acked).
 *
 * Used by both the directory walker (subdirs / entry-list batches,
 * walker.c) and the delete-pass executor (delete-remainder batches for a
 * pathological orphan directory, delete.c) — the ack/backpressure logic is
 * identical regardless of what kind of work a split carries, so it lives
 * here once instead of twice. */
#include "agent.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/* Awaits one in-flight split ack (timeout => fatal so the shard re-runs), then
 * unregisters and frees the waiter. */
static void await_split(struct walk_ctx *ctx, struct split_wait *w)
{
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += SPLIT_ACK_TIMEOUT_S;
    if (sem_timedwait(&w->sem, &dl) < 0) {
        snprintf(ctx->err, sizeof ctx->err, "split ack timeout (seq %llu)",
                 (unsigned long long)w->seq);
        ctx->fatal = true;
    }
    split_unregister(w); /* removes from the registry and sem_destroys */
    free(w);
}

/* Ships a prepared ShardSplit frame WITHOUT blocking: the ack is awaited
 * later (drain_splits, before the shard result), so consecutive round-trips
 * overlap instead of serialising. Blocks only for backpressure when
 * SPLIT_WINDOW acks are already outstanding. Consumes seq. */
void ship_split(struct walk_ctx *ctx, pb_buf *b)
{
    if (ctx->infl_count == SPLIT_WINDOW) {
        struct split_wait *old = ctx->infl[ctx->infl_head];
        ctx->infl_head = (ctx->infl_head + 1) % SPLIT_WINDOW;
        ctx->infl_count--;
        await_split(ctx, old);
    }
    struct split_wait *w = calloc(1, sizeof *w);
    if (!w) {
        CTR_ADD(ctx->c.errors, 1);
        ctx->fatal = true;
        out_push(FR_SHARD_SPLIT, b); /* still recorded (idempotent); just not awaited */
        ctx->split_seq++;
        return;
    }
    w->parent = ctx->it->shard_id;
    w->seq = ctx->split_seq;
    split_register(w);
    out_push(FR_SHARD_SPLIT, b);
    ctx->infl[(ctx->infl_head + ctx->infl_count) % SPLIT_WINDOW] = w;
    ctx->infl_count++;
    ctx->split_seq++;
}

/* Awaits every outstanding split ack. Must run before reporting the shard
 * result so the coordinator has recorded all children first. */
void drain_splits(struct walk_ctx *ctx)
{
    while (ctx->infl_count > 0) {
        struct split_wait *w = ctx->infl[ctx->infl_head];
        ctx->infl_head = (ctx->infl_head + 1) % SPLIT_WINDOW;
        ctx->infl_count--;
        await_split(ctx, w);
    }
}
