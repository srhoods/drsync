/* Copy-pool sizing policy: how many copy threads must stay pure drainers
 * (never steal a walker shard) under cross-pool work-stealing. Split out from
 * copy.c so this pure arithmetic is unit-testable without linking copy.c's
 * broader dependencies (journal, xattr, ucopy, ...). */
#include "agent.h"

/* A flat reserve of 1 (the original sizing, introduced alongside stealing
 * itself in d5602b5) only guards against deadlock: it guarantees the drain
 * side always makes *some* progress, which is all that commit verified. It
 * does not guard throughput. With stealing on, up to `workers + copy_threads`
 * threads may be crawling directories and pushing into the copy queue at
 * once (see main.c's maybe_request_work, which sizes the shard-prefetch
 * window to the whole pool specifically because stealing lets copy threads
 * crawl too) against just that one drainer. Under real contention a stolen
 * shard's dpend_wait can stall long enough to blow past the coordinator's
 * lease TTL, get requeued, and then complete almost instantly once
 * contention eases — confirmed via fleet A/B: -S (all-drainer, no stealing)
 * held the requeue rate at 0%, and reverting to stealing with the flat
 * reserve reproduced it immediately.
 *
 * 25% of the pool (floored at 1, so deadlock-freedom is never lost) keeps
 * most of the pool stealable — the elastic walker/copy balance the feature
 * exists for — while giving the drain side real throughput instead of a
 * single thread, matching the codebase's existing 25/75 walker/copy design
 * ratio (see ansible/roles/drsync_agent, -w/-C defaults). Stealing off (or a
 * lone copy thread, which can't both steal and be replaced) => every thread
 * is a pure drainer, same as before stealing existed. */
int cp_reserve_for(int copy_threads, bool steal_enabled)
{
    if (!steal_enabled || copy_threads < 2)
        return copy_threads;
    int reserve = copy_threads / 4;
    return reserve < 1 ? 1 : reserve;
}
