/* Unit test for filter_glob_match — the rsync-like glob matcher.
 * Build+run: `make test` in agent/. Exits non-zero on any failure. */
#include "../src/filter.h"

#include <stdio.h>
#include <string.h>

static int failures;

static void check(const char *pat, const char *path, bool want)
{
    bool got = filter_glob_match(pat, path);
    if (got != want) {
        fprintf(stderr, "FAIL: match(%-18s, %-16s) = %s, want %s\n", pat, path,
                got ? "true" : "false", want ? "true" : "false");
        failures++;
    }
}

int main(void)
{
    /* Documented examples (docs/DESIGN-jobspec.md). */
    check("**/*.tmp", "x.tmp", true);        /* leading star-star matches zero segs */
    check("**/*.tmp", "a/b/c.tmp", true);    /* ...and many segments */
    check("**/*.tmp", "a/b/c.txt", false);
    check("**/.snapshot/**", "a/.snapshot/x", true);
    check("**/.snapshot/**", "a/b/.snapshot/deep/f", true);
    check("**/.snapshot/**", "a/snapshot/x", false); /* name must match exactly */

    /* '*' and '?' do not cross '/'. */
    check("*.tmp", "x.tmp", true);
    check("*.tmp", "a/x.tmp", false); /* anchored at root: '*' stops at '/' */
    check("a/*.log", "a/b.log", true);
    check("a/*.log", "a/b/c.log", false);
    check("a/?.log", "a/b.log", true);
    check("a/?.log", "a/bb.log", false);
    check("a/?.log", "a//.log", false); /* '?' does not match '/' */

    /* '**' crosses '/'. */
    check("a/**", "a/b/c", true);
    check("a/**", "a/b", true);
    check("a/**/z", "a/z", true);       /* interior star-star matches zero segs */
    check("a/**/z", "a/b/c/z", true);
    check("a/**/z", "a/b/c/z/y", false);

    /* Whole-path anchoring: partial matches don't count. */
    check("a", "a/b", false);
    check("a/b", "a", false);
    check("foo", "foobar", false);
    check("foo*", "foobar", true);

    /* '[' is literal (no character classes). */
    check("a[b].log", "a[b].log", true);
    check("a[b].log", "ab.log", false);

    /* Empty-ish edges. */
    check("**", "anything/at/all", true);
    check("*", "top", true);
    check("*", "a/b", false);

    if (failures) {
        fprintf(stderr, "%d filter test(s) failed\n", failures);
        return 1;
    }
    printf("all filter tests passed\n");
    return 0;
}
