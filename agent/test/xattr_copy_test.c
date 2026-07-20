/* Unit test for the file-copy xattr path (src/xattr.c). Guards two things the
 * copy pipeline relies on and that had no coverage before the fresh-destination
 * optimization:
 *
 *   xattr_copy_fd_fresh — used when copying into a just-created temp: copies the
 *       source's xattrs, and (by contract) does not probe the destination,
 *       which is empty by construction. Removing that probe is the GPFS/Weka
 *       win; this asserts the copy is still correct.
 *   xattr_copy_fd — the reconciling path for a pre-existing destination: copies
 *       source xattrs AND removes destination-only ones. Unchanged, tested here
 *       because the fresh path must not be allowed to inherit that behavior by a
 *       future refactor.
 *
 * Uses real temp files and real xattr syscalls — no fleet, no socket. Build:
 * `make xattr-copy-test` in agent/. Exits non-zero on failure.
 */
#include "../src/agent.h"
#include "../src/pb.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/xattr.h>
#include <unistd.h>

/* ---- stubs for symbols on paths this test does not exercise ---- */
void log_line(const char *level, const char *fmt, ...) { (void)level; (void)fmt; }
void out_push(uint16_t type, pb_buf *b) { (void)type; (void)b; }
void walk_err(struct walk_ctx *ctx, const char *what, const char *path)
{
	(void)ctx; (void)what; (void)path;
}

static int failures;
#define CHECK(cond, ...)                                                        \
	do {                                                                   \
		if (!(cond)) {                                                 \
			fprintf(stderr, "FAIL: ");                            \
			fprintf(stderr, __VA_ARGS__);                         \
			fprintf(stderr, "\n");                                \
			failures++;                                           \
		}                                                             \
	} while (0)

/* A temp file, plus a scratch dir removed at exit. */
static char g_dir[] = "/tmp/drsync-xattr-test.XXXXXX";

static int mkfile(const char *name)
{
	char p[256];
	snprintf(p, sizeof p, "%s/%s", g_dir, name);
	int fd = open(p, O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (fd < 0) {
		perror(p);
		exit(2);
	}
	return fd;
}

static bool has_xattr(int fd, const char *name, const char *want)
{
	char buf[128];
	ssize_t n = fgetxattr(fd, name, buf, sizeof buf);
	if (n < 0)
		return false;
	return want == NULL || ((size_t)n == strlen(want) && memcmp(buf, want, (size_t)n) == 0);
}

int main(void)
{
	if (!mkdtemp(g_dir)) {
		perror("mkdtemp");
		return 2;
	}

	struct job_options o = { .meta_xattrs = true, .acl_posix = true, .acl_nfs4 = true };
	struct opts_entry oe = { .o = o };
	struct shard_item it = { .shard_id = 1 };
	struct walk_ctx ctx = { .oe = &oe, .it = &it };
	jrn_init(&ctx);

	/* 1. fresh path copies a source xattr onto a just-created destination. */
	{
		int src = mkfile("src1"), dst = mkfile("dst1");
		CHECK(fsetxattr(src, "user.k", "vvv", 3, 0) == 0, "seed src1 xattr");
		xattr_copy_fd_fresh(&ctx, src, dst, "src1");
		CHECK(has_xattr(dst, "user.k", "vvv"), "fresh: user.k not copied to dst");
		close(src);
		close(dst);
	}

	/* 2. fresh path with a source that has no xattrs leaves the dst clean. */
	{
		int src = mkfile("src2"), dst = mkfile("dst2");
		xattr_copy_fd_fresh(&ctx, src, dst, "src2");
		char buf[64];
		CHECK(flistxattr(dst, buf, sizeof buf) == 0, "fresh: dst gained an xattr from an empty src");
		close(src);
		close(dst);
	}

	/* 3. reconciling path removes a destination-only xattr not present at the
	 *    source — the behavior the fresh path deliberately skips, and which
	 *    must survive for pre-existing destinations. */
	{
		int src = mkfile("src3"), dst = mkfile("dst3");
		CHECK(fsetxattr(src, "user.keep", "1", 1, 0) == 0, "seed src3");
		CHECK(fsetxattr(dst, "user.keep", "old", 3, 0) == 0, "seed dst3 keep");
		CHECK(fsetxattr(dst, "user.stale", "x", 1, 0) == 0, "seed dst3 stale");
		xattr_copy_fd(&ctx, src, dst, "src3");
		CHECK(has_xattr(dst, "user.keep", "1"), "reconcile: user.keep not updated from src");
		CHECK(!has_xattr(dst, "user.stale", NULL), "reconcile: dst-only user.stale not removed");
		close(src);
		close(dst);
	}

	/* 4. meta_xattrs off: nothing is touched, on either path. */
	{
		oe.o.meta_xattrs = false;
		int src = mkfile("src4"), dst = mkfile("dst4");
		CHECK(fsetxattr(src, "user.k", "v", 1, 0) == 0, "seed src4");
		xattr_copy_fd_fresh(&ctx, src, dst, "src4");
		char buf[64];
		CHECK(flistxattr(dst, buf, sizeof buf) == 0, "xattrs-off: dst gained an xattr");
		oe.o.meta_xattrs = true;
		close(src);
		close(dst);
	}

	/* best-effort cleanup */
	char cmd[300];
	snprintf(cmd, sizeof cmd, "rm -rf %s", g_dir);
	if (system(cmd) != 0) { /* ignore */ }

	if (failures) {
		fprintf(stderr, "%d check(s) failed\n", failures);
		return 1;
	}
	printf("xattr_copy_test: OK\n");
	return 0;
}
