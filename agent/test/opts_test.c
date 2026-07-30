/* Unit test for the job options table (src/state.c: opts_store/opts_get) —
 * covers the GPFS-detection flag (no_uring_copy) added to disable the
 * io_uring copy engine per-job (agent.h struct opts_entry), since GPFS's
 * WRITE_FIXED silently ignores the submitted length (ucopy.c). No GPFS mount
 * is available in a unit test, so this checks the plumbing rather than a true
 * positive: the flag defaults false on an ordinary filesystem, and survives a
 * tunables-only options update untouched (opts_store's update branch only
 * overwrites the embedded job_options, not the fd/flag fields alongside it).
 * Build+run: `make test` in agent/. Exits non-zero on any failure. */
#include "../src/agent.h"
#include "../src/msgs.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures;

#define CHECK(cond, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: "); \
        fprintf(stderr, __VA_ARGS__); \
        fprintf(stderr, "\n"); \
        failures++; \
    } \
} while (0)

static struct job_options mk(uint64_t job_id, const char *src, const char *dst,
                             uint64_t options_hash)
{
    struct job_options o;
    memset(&o, 0, sizeof o);
    o.job_id = job_id;
    snprintf(o.src_root, sizeof o.src_root, "%s", src);
    snprintf(o.dst_root, sizeof o.dst_root, "%s", dst);
    o.options_hash = options_hash;
    return o;
}

/* A local/tmpfs test directory is never GPFS: no_uring_copy must default
 * false so ordinary filesystems are unaffected by the detection. */
static void test_non_gpfs_defaults_false(void)
{
    char tmp[] = "/tmp/drsync-opts-test-XXXXXX";
    CHECK(mkdtemp(tmp) != NULL, "mkdtemp failed");

    struct job_options o = mk(9001, tmp, tmp, 111);
    CHECK(opts_store(&o) == 0, "opts_store failed");

    const struct opts_entry *oe = opts_get(9001);
    CHECK(oe != NULL, "opts_get returned NULL after store");
    if (oe)
        CHECK(!oe->no_uring_copy, "no_uring_copy true on a non-GPFS filesystem");

    opts_evict(9001);
    rmdir(tmp);
}

/* opts_store's update branch (same job_id, changed options_hash) only
 * overwrites the embedded job_options struct; no_uring_copy lives alongside
 * it in opts_entry and must be left alone, not reset to false, by an update. */
static void test_flag_survives_tunables_update(void)
{
    char tmp[] = "/tmp/drsync-opts-test-XXXXXX";
    CHECK(mkdtemp(tmp) != NULL, "mkdtemp failed");

    struct job_options o = mk(9002, tmp, tmp, 111);
    CHECK(opts_store(&o) == 0, "opts_store (initial) failed");

    const struct opts_entry *oe = opts_get(9002);
    CHECK(oe != NULL, "opts_get returned NULL after initial store");
    bool before = oe && oe->no_uring_copy;

    struct job_options o2 = mk(9002, tmp, tmp, 222); /* same roots, new hash */
    CHECK(opts_store(&o2) == 0, "opts_store (update) failed");

    oe = opts_get(9002);
    CHECK(oe != NULL, "opts_get returned NULL after update");
    if (oe)
        CHECK(oe->no_uring_copy == before,
              "no_uring_copy changed across a tunables-only update (%d -> %d)",
              before, oe->no_uring_copy);

    opts_evict(9002);
    rmdir(tmp);
}

int main(void)
{
    test_non_gpfs_defaults_false();
    test_flag_survives_tunables_update();

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("opts_test: OK\n");
    return 0;
}
