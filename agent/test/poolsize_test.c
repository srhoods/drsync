/* Unit test for the copy-pool reserve sizing (src/poolsize.c) — the fix for
 * shards expiring their coordinator lease under work-stealing: too few
 * drainer threads left the copy queue starved under real contention, long
 * enough for dpend_wait to blow past the lease TTL. Build+run: `make test`
 * in agent/. Exits non-zero on any failure. */
#include "../src/agent.h"

#include <stdio.h>

static int failures;

static void check(int copy_threads, bool steal_enabled, int want)
{
    int got = cp_reserve_for(copy_threads, steal_enabled);
    if (got != want) {
        fprintf(stderr, "FAIL: cp_reserve_for(%d, steal=%s) = %d, want %d\n",
                copy_threads, steal_enabled ? "true" : "false", got, want);
        failures++;
    }
}

int main(void)
{
    /* Stealing off: every copy thread is a pure drainer regardless of pool
     * size — unchanged from before work-stealing existed. */
    check(1, false, 1);
    check(2, false, 2);
    check(8, false, 8);
    check(32, false, 32);

    /* A lone copy thread can't both steal and be replaced by a reserved
     * drainer, so it stays a pure drainer even with stealing on. */
    check(1, true, 1);
    check(0, true, 0);

    /* The regression this exists to fix: the default 8-copy-thread pool used
     * to reserve a flat 1 drainer under stealing — 7 of 8 threads could all
     * simultaneously become copy-queue producers via a stolen walker shard's
     * own cp_submit calls, against just one drainer. 25% gives real drain
     * throughput instead. */
    check(8, true, 2);

    /* Small pools: 25% rounds down to 0, which must still floor at 1 — the
     * deadlock-freedom guarantee (a drainer always exists) can never be lost,
     * whatever the throughput target says. */
    check(2, true, 1);
    check(3, true, 1);
    check(4, true, 1);

    /* Larger pools (a real 100 GbE fleet host's -C, per the Ansible 25/75
     * split) scale the reserve with the pool instead of staying flat at 1. */
    check(16, true, 4);
    check(32, true, 8);
    check(100, true, 25);

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("poolsize_test: OK\n");
    return 0;
}
