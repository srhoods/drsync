/* Unit test for the lease table (src/state.c) — the fix for the
 * lease-requeue-under-heavy-walker-load bug: lease_add/lease_start/
 * lease_remove/lease_snapshot/lease_inflight/lease_job_held must all cost
 * O(leases currently held), never O(leases ever held), or a long job with
 * many walker threads churning small shards degrades send_heartbeat's own
 * scans (lease_snapshot, lease_inflight) enough to starve the coordinator's
 * lease renewal. Build+run: `make test` in agent/. Exits non-zero on any
 * failure. */
#include "../src/agent.h"
#include "../src/msgs.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

static int failures;

#define CHECK(cond, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: "); \
        fprintf(stderr, __VA_ARGS__); \
        fprintf(stderr, "\n"); \
        failures++; \
    } \
} while (0)

static struct shard_item mk(uint64_t lease_id, uint64_t shard_id, uint64_t job_id)
{
    struct shard_item it;
    memset(&it, 0, sizeof it);
    it.lease_id = lease_id;
    it.shard_id = shard_id;
    it.job_id = job_id;
    it.kind = 0;
    it.rel_path = NULL;
    return it;
}

#define SNAPSHOT_CAP 2048 /* well above any test's concurrent-lease count */

static bool snapshot_has(uint64_t id)
{
    static uint64_t buf[SNAPSHOT_CAP];
    size_t n = lease_snapshot(buf, SNAPSHOT_CAP);
    for (size_t i = 0; i < n; i++)
        if (buf[i] == id)
            return true;
    return false;
}

/* Basic lifecycle: add -> present in snapshot -> start -> remove -> absent. */
static void test_basic_lifecycle(void)
{
    struct shard_item a = mk(1001, 1, 1);
    lease_add(&a);
    CHECK(snapshot_has(1001), "lease 1001 missing from snapshot after add");

    lease_start(1001);
    lease_publish(42);
    struct inflight_view iv[8];
    size_t n = lease_inflight(iv, 8);
    bool found = false;
    for (size_t i = 0; i < n; i++) {
        if (iv[i].lease_id == 1001) {
            found = true;
            CHECK(iv[i].running, "lease 1001 should show running=true after lease_start");
            CHECK(iv[i].entries_done == 42, "entries_done = %llu, want 42",
                  (unsigned long long)iv[i].entries_done);
        }
    }
    CHECK(found, "lease 1001 missing from lease_inflight after lease_start/publish");
    lease_end();

    lease_remove(1001);
    CHECK(!snapshot_has(1001), "lease 1001 still in snapshot after remove");
}

/* lease_job_held must reflect only currently-active leases for a job, not
 * leases that job ever held. */
static void test_job_held(void)
{
    struct shard_item a = mk(2001, 1, 77);
    lease_add(&a);
    CHECK(lease_job_held(77), "job 77 should be held right after lease_add");
    lease_remove(2001);
    CHECK(!lease_job_held(77), "job 77 should not be held after its only lease is removed");
}

/* The actual regression test: after many add+remove cycles (simulating a long
 * job's shard history — this is what made n_used climb toward MAX_LEASES in
 * the old scan-based implementation), a snapshot must reflect only the
 * leases genuinely held right now, not any accumulated history. This proves
 * the table doesn't leak entries and that held-count tracks active count,
 * not total-ever-added count — the property the O(1) rewrite depends on. */
static void test_long_history_does_not_leak(void)
{
    const int cycles = 20000; /* far more than MAX_LEASES (8192) ever-added */
    for (int i = 0; i < cycles; i++) {
        uint64_t id = 3000000 + (uint64_t)i;
        struct shard_item it = mk(id, (uint64_t)i, 999);
        lease_add(&it);
        lease_remove(id);
    }
    uint64_t buf[16];
    size_t n = lease_snapshot(buf, 16);
    CHECK(n == 0, "snapshot has %zu leases after %d add+remove cycles, want 0", n, cycles);
    CHECK(!lease_job_held(999), "job 999 shows held after every one of its leases was removed");

    /* One lease still held at the end must still be found correctly — proves
     * the hash table and active list are not corrupted by the churn above. */
    struct shard_item last = mk(4000000, 1, 999);
    lease_add(&last);
    CHECK(snapshot_has(4000000), "lease added after heavy churn missing from snapshot");
    lease_remove(4000000);
}

/* Many concurrently-held leases (as a real fleet at -w 34+ would have) must
 * all be found correctly by snapshot/inflight/job_held, and removing one
 * must not disturb the others — the active/hash list bookkeeping must be
 * exact, not just "doesn't crash". */
static void test_many_concurrent_leases(void)
{
    const int n = 500;
    for (int i = 0; i < n; i++) {
        uint64_t id = 5000000 + (uint64_t)i;
        struct shard_item it = mk(id, (uint64_t)i, 55);
        lease_add(&it);
    }
    uint64_t buf[600];
    size_t got = lease_snapshot(buf, 600);
    CHECK(got == (size_t)n, "snapshot returned %zu leases, want %d", got, n);
    for (int i = 0; i < n; i++)
        CHECK(snapshot_has(5000000 + (uint64_t)i), "lease %d missing from snapshot", i);

    /* Remove a middle one; every other lease (including the ones before and
     * after it, and the head/tail of the active list) must still be present. */
    lease_remove(5000000 + 250);
    CHECK(!snapshot_has(5000000 + 250), "removed lease still present");
    for (int i = 0; i < n; i++) {
        if (i == 250)
            continue;
        CHECK(snapshot_has(5000000 + (uint64_t)i),
              "lease %d disappeared after an unrelated lease was removed", i);
    }

    for (int i = 0; i < n; i++) {
        if (i == 250)
            continue;
        lease_remove(5000000 + (uint64_t)i);
    }
    got = lease_snapshot(buf, 600);
    CHECK(got == 0, "snapshot has %zu leases after removing all of them, want 0", got);
}

int main(void)
{
    test_basic_lifecycle();
    test_job_held();
    test_many_concurrent_leases();
    test_long_history_does_not_leak();

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("state_test: OK\n");
    return 0;
}
