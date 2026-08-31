/* Unit test for the copy.on_dest_newer wire decode (msgs.c dec_copy_opts,
 * field 10 of CopyOptions inside JobOptions inside WorkGrant). Covers the
 * mixed-fleet safety property this field was specifically designed for: an
 * old coordinator that never sends the field at all (or a value that
 * decodes to CONFLICT_UNSPECIFIED, proto's own zero default) must still
 * resolve to job_options.on_dest_newer_skip == true — the newer, safer
 * default — not silently fall back to the pre-this-field overwrite-always
 * behavior. Only an explicit CONFLICT_OVERWRITE (2) turns it off.
 *
 * Hand-builds each WorkGrant's wire bytes directly with the same pb_put_*
 * helpers the coordinator's Go encoder ultimately produces equivalent bytes
 * for, so this exercises the real decode path (dec_work_grant ->
 * dec_job_options -> dec_copy_opts) with no socket, no coordinator, no fleet.
 * Build+run: `make on-dest-newer-test` in agent/. Exits non-zero on failure. */
#include "../src/agent.h"
#include "../src/pb.h"

#include <stdio.h>
#include <string.h>

/* ---- stubs for symbols on paths this test does not exercise ---- */
void log_line(const char *level, const char *fmt, ...)
{
    (void)level;
    (void)fmt;
}
void out_push(uint16_t type, pb_buf *b)
{
    (void)type;
    (void)b;
}
void walk_err(struct walk_ctx *ctx, const char *what, const char *path)
{
    (void)ctx;
    (void)what;
    (void)path;
}

static int failures;
#define CHECK(cond, ...)                       \
    do {                                       \
        if (!(cond)) {                         \
            fprintf(stderr, "FAIL: " __VA_ARGS__); \
            fprintf(stderr, "\n");             \
            failures++;                        \
        }                                      \
    } while (0)

/* Builds a minimal WorkGrant containing one JobOptions (job_id=1, minimal
 * valid src/dst roots) whose CopyOptions carries field 10 (on_dest_newer)
 * IF have_conflict_policy is set, to the given enum value. Returns the
 * marshaled WorkGrant bytes in *out (caller frees via pb_free semantics —
 * out itself is a plain pb_buf owned by the caller). */
static void build_grant(pb_buf *out, bool have_conflict_policy, uint64_t conflict_policy)
{
    pb_buf copy;
    pb_init(&copy);
    if (have_conflict_policy)
        pb_put_u64(&copy, 10, conflict_policy);

    pb_buf jo;
    pb_init(&jo);
    pb_put_u64(&jo, 1, 1);            /* job_id */
    pb_put_str(&jo, 3, "/tmp/src");   /* src_root */
    pb_put_str(&jo, 4, "/tmp/dst");   /* dst_root */
    pb_put_msg(&jo, 6, &copy);        /* copy (CopyOptions) */
    pb_free(&copy);

    pb_init(out);
    pb_put_msg(out, 2, &jo);          /* WorkGrant.options[] (repeated field 2) */
    pb_free(&jo);
}

/* An old coordinator that predates this field never sends CopyOptions field
 * 10 at all — the mixed-fleet case on_dest_newer_skip's pre-set-true default
 * (in dec_copy_opts, before the field loop runs) exists for. */
static void test_field_absent_defaults_skip(void)
{
    pb_buf grant;
    build_grant(&grant, false, 0);

    struct work_grant g;
    bool ok = dec_work_grant(grant.p, grant.len, &g);
    pb_free(&grant);

    CHECK(ok, "dec_work_grant failed to decode a grant with no on_dest_newer field");
    CHECK(g.n_options == 1, "n_options = %zu, want 1", g.n_options);
    if (ok && g.n_options == 1)
        CHECK(g.options[0].on_dest_newer_skip,
              "on_dest_newer_skip = false with the field entirely absent from the wire, want true");
    work_grant_free(&g);
}

/* CONFLICT_UNSPECIFIED (0) on the wire — proto's own zero value, should never
 * be sent deliberately, but a misbehaving coordinator sending it explicitly
 * must not be treated as an instruction to overwrite (the destructive
 * choice); it means the same as the field being absent. */
static void test_explicit_unspecified_defaults_skip(void)
{
    pb_buf grant;
    build_grant(&grant, true, 0 /* CONFLICT_UNSPECIFIED */);

    struct work_grant g;
    bool ok = dec_work_grant(grant.p, grant.len, &g);
    pb_free(&grant);

    CHECK(ok, "dec_work_grant failed to decode CONFLICT_UNSPECIFIED");
    if (ok && g.n_options == 1)
        CHECK(g.options[0].on_dest_newer_skip,
              "on_dest_newer_skip = false for explicit CONFLICT_UNSPECIFIED, want true");
    work_grant_free(&g);
}

/* CONFLICT_SKIP_IF_DEST_NEWER (1), the explicit form of the default. */
static void test_explicit_skip(void)
{
    pb_buf grant;
    build_grant(&grant, true, 1 /* CONFLICT_SKIP_IF_DEST_NEWER */);

    struct work_grant g;
    bool ok = dec_work_grant(grant.p, grant.len, &g);
    pb_free(&grant);

    CHECK(ok, "dec_work_grant failed to decode CONFLICT_SKIP_IF_DEST_NEWER");
    if (ok && g.n_options == 1)
        CHECK(g.options[0].on_dest_newer_skip,
              "on_dest_newer_skip = false for explicit CONFLICT_SKIP_IF_DEST_NEWER, want true");
    work_grant_free(&g);
}

/* CONFLICT_OVERWRITE (2) — the only value that turns skip off. */
static void test_explicit_overwrite(void)
{
    pb_buf grant;
    build_grant(&grant, true, 2 /* CONFLICT_OVERWRITE */);

    struct work_grant g;
    bool ok = dec_work_grant(grant.p, grant.len, &g);
    pb_free(&grant);

    CHECK(ok, "dec_work_grant failed to decode CONFLICT_OVERWRITE");
    if (ok && g.n_options == 1)
        CHECK(!g.options[0].on_dest_newer_skip,
              "on_dest_newer_skip = true for explicit CONFLICT_OVERWRITE, want false");
    work_grant_free(&g);
}

int main(void)
{
    test_field_absent_defaults_skip();
    test_explicit_unspecified_defaults_skip();
    test_explicit_skip();
    test_explicit_overwrite();

    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("on_dest_newer_test: OK\n");
    return 0;
}
