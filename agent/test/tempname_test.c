/* Unit test for the destination temp-name tag (src/tempname.c) — the thing that
 * stops a walk shard's orphan sweep from reclaiming a chunk temp that other
 * hosts are still writing into. Build+run: `make test` in agent/.
 * Exits non-zero on any failure. */
#include "../src/agent.h"

#include <stdio.h>
#include <string.h>

static int failures;

static void check_fmt(uint64_t job, uint32_t pass, uint64_t shard, unsigned seq,
                      const char *want)
{
    char got[256];
    temp_name_fmt(got, sizeof got, ".drsync.tmp.", job, pass, shard, seq);
    if (strcmp(got, want) != 0) {
        fprintf(stderr, "FAIL: fmt = %s, want %s\n", got, want);
        failures++;
    }
}

static void check_tag(const char *tail, uint64_t job, uint32_t pass, bool want)
{
    bool got = temp_tag_matches(tail, job, pass);
    if (got != want) {
        fprintf(stderr, "FAIL: tag_matches(%-22s, %llu, %u) = %s, want %s\n", tail,
                (unsigned long long)job, pass, got ? "true" : "false",
                want ? "true" : "false");
        failures++;
    }
}

int main(void)
{
    check_fmt(1, 1, 1, 0, ".drsync.tmp.1-1.1.0");
    check_fmt(0x2a, 0x3, 0x1f4, 0xb, ".drsync.tmp.2a-3.1f4.b");
    check_fmt(0, 0, 0, 0, ".drsync.tmp.0-0.0.0");

    /* A name this pass wrote: live, never reclaimed. */
    check_tag("2a-3.1f4.b", 0x2a, 3, true);
    check_tag("0-0.0.0", 0, 0, true);

    /* Same job, other pass — residue from an earlier pass, reclaimable. */
    check_tag("2a-3.1f4.b", 0x2a, 4, false);
    check_tag("2a-3.1f4.b", 0x2a, 2, false);
    /* Other job sharing the destination: not ours to judge live, but the
     * (job, pass) pair differs, so it is reclaimed as residue like before. */
    check_tag("2b-3.1f4.b", 0x2a, 3, false);

    /* Hex, not decimal: "10-3" is job 16, and must not match job 10. */
    check_tag("10-3.1.0", 10, 3, false);
    check_tag("10-3.1.0", 0x10, 3, true);

    /* Untagged legacy names (<shard>.<seq>) stay reclaimable — including the
     * case where the leading field alone would have matched the job. */
    check_tag("2a.b", 0x2a, 3, false);
    check_tag("3.1f4", 3, 1, false);
    /* Malformed / truncated tags: reclaimable, never a crash. */
    check_tag("", 0, 0, false);
    check_tag("-", 0, 0, false);
    check_tag("2a-", 0x2a, 0, false);
    check_tag("2a-3", 0x2a, 3, false);     /* no '.' terminator */
    check_tag("2a-3x.1.0", 0x2a, 3, false);
    check_tag("-3.1.0", 0, 3, false);      /* empty job field */
    check_tag("zz-3.1.0", 0, 3, false);    /* non-hex job */

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("tempname_test: OK\n");
    return 0;
}
