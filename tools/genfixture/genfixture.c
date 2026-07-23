/* genfixture — synthetic fidelity-test tree generator for drsync.
 *
 * docs/DESIGN-agent.md §9 calls for "a generator [that] creates every entry
 * type x metadata combination (sparse layouts, 100+ xattrs, both ACL flavors,
 * ns timestamps, suid/sticky, deep names, 255-byte names, symlink targets
 * with newlines...)" to drive the fidelity matrix suite. This is that
 * generator: it builds a source tree exercising every attribute drsync's
 * agent knows how to read, copy, or verify (agent/src/{xattr,walker,verify}.c),
 * so a `drsync sync` of the result is a real fidelity test.
 *
 * It is standalone (libc only, no drsync sources) so it builds anywhere,
 * including a bare filer host:
 *
 *   cc -O2 -o genfixture genfixture.c    (or: make)
 *
 * Entry types produced (one pass, mixed through the tree):
 *   - regular files: random sizes, random content, some sparse (seek past
 *     end, write a tail — matches preserve_sparse's SEEK_DATA/SEEK_HOLE path)
 *   - directories, recursed to the requested depth
 *   - symlinks: ordinary, dangling, and one with a newline in the target
 *   - FIFOs (mkfifo; no privilege needed)
 *   - device nodes (mknod; only attempted when running as root/CAP_MKNOD —
 *     otherwise counted as skipped, same as drsync's own EPERM handling)
 *   - hardlinks to a prior regular file (drsync copies each link independently
 *     per agent/src/walker.c:712 nlink_dup accounting, so this is a real case)
 *
 * Metadata applied per entry, matching struct job_options's toggles
 * (agent/src/msgs.h) and estat fields (agent/src/agent.h):
 *   - owner/group (chown; best-effort, needs privilege to pick non-self ids)
 *   - mode bits including setuid/setgid/sticky
 *   - nanosecond atime/mtime set to non-"now" values (utimensat) so a naive
 *     second-granularity copy would visibly fail verify
 *   - xattrs in user./trusted./security. namespaces (agent/src/xattr.c
 *     XC_COPY), including a directory with 100+ small xattrs
 *   - POSIX ACLs (system.posix_acl_access/default) built directly as the
 *     kernel on-disk blob (struct posix_acl_xattr_{header,entry} from
 *     linux/posix_acl_xattr.h) so this needs no libacl — matches xattr.c's
 *     own comment that the ACL is "a byte-stable kernel blob carried in
 *     system.posix_acl_access/default"
 *   - NFSv4 ACL placeholder xattr (system.nfs4_acl): only meaningful on an
 *     NFSv4 mount, but written as an opaque blob so the copy path (raw xattr
 *     copy) and the untranslatable-policy path (on a non-NFSv4 destination)
 *     both get exercised
 *
 * Never overwrites: every path is created with O_EXCL (or mkdir/symlink/
 * mkfifo/mknod, which are inherently exclusive) and an existing path is
 * skipped with a warning and a counter, so re-running against a directory
 * that already has data extends it without touching what is there.
 *
 * Progress: a status line to stderr roughly once a second (bytes and entries
 * so far, rate, ETA against the requested total size), plus a final summary
 * breaking down what was created and what was skipped.
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/xattr.h>
#include <time.h>
#include <unistd.h>

#include <linux/posix_acl.h>
#include <linux/posix_acl_xattr.h>

/* ------------------------------------------------------------------ time */

static double now_s(void)
{
	struct timespec t;
	clock_gettime(CLOCK_MONOTONIC, &t);
	return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

/* ------------------------------------------------------------------- rng */
/* xoshiro256** — small, fast, no libm/lrand48 state races across the single
 * thread this tool runs on. Seeded from /dev/urandom when available. */
static uint64_t rng_s[4];

static uint64_t rotl(uint64_t x, int k) { return (x << k) | (x >> (64 - k)); }

static uint64_t rng_next(void)
{
	uint64_t r = rotl(rng_s[1] * 5, 7) * 9;
	uint64_t t = rng_s[1] << 17;
	rng_s[2] ^= rng_s[0];
	rng_s[3] ^= rng_s[1];
	rng_s[1] ^= rng_s[2];
	rng_s[0] ^= rng_s[3];
	rng_s[2] ^= t;
	rng_s[3] = rotl(rng_s[3], 45);
	return r;
}

static void rng_seed(uint64_t seed)
{
	/* splitmix64 to spread a possibly-small seed across the state words */
	for (int i = 0; i < 4; i++) {
		seed += 0x9E3779B97F4A7C15ULL;
		uint64_t z = seed;
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
		z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
		rng_s[i] = z ^ (z >> 31);
	}
}

static uint64_t rng_u64(uint64_t bound)
{
	if (bound == 0)
		return 0;
	return rng_next() % bound;
}

/* ------------------------------------------------------------------- log */

static void die(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	fprintf(stderr, "genfixture: ");
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	fprintf(stderr, "\n");
	exit(1);
}

/* ---------------------------------------------------------------- config */

typedef struct {
	const char *root;
	uint64_t total_size;
	int depth;
	uint64_t seed;
	bool no_acl;
	bool no_xattr;
	bool no_special;
} cfg_t;

/* ------------------------------------------------------------- counters */

typedef struct {
	uint64_t bytes;
	uint64_t files;
	uint64_t sparse_files;
	uint64_t dirs;
	uint64_t symlinks;
	uint64_t hardlinks;
	uint64_t fifos;
	uint64_t devices;
	uint64_t sockets;
	uint64_t xattrs_set;
	uint64_t acls_set;
	uint64_t skipped_existing;
	uint64_t errors;
} stats_t;

static stats_t g_stats;
static double g_last_report;
static double g_t0;
static const cfg_t *g_cfg;

static void progress(bool force)
{
	double t = now_s();
	if (!force && t - g_last_report < 1.0)
		return;
	g_last_report = t;
	double el = t - g_t0;
	double pct = g_cfg->total_size ? 100.0 * (double)g_stats.bytes / (double)g_cfg->total_size : 100.0;
	if (pct > 100.0)
		pct = 100.0;
	double rate = el > 0 ? (double)g_stats.bytes / el / 1048576.0 : 0;
	fprintf(stderr,
		"\r[genfixture] %6.1f%%  %8" PRIu64 " files  %6.1f MiB / %6.1f MiB  "
		"%6.1f MiB/s  %5.0fs elapsed",
		pct, g_stats.files + g_stats.dirs + g_stats.symlinks + g_stats.fifos +
			g_stats.devices + g_stats.sockets,
		g_stats.bytes / 1048576.0, g_cfg->total_size / 1048576.0, rate, el);
	fflush(stderr);
}

/* ------------------------------------------------------------- path util */

#define MAXP PATH_MAX

static bool path_join(char *out, size_t cap, const char *dir, const char *name)
{
	int n = snprintf(out, cap, "%s/%s", dir, name);
	return n > 0 && (size_t)n < cap;
}

/* ------------------------------------------------------- attribute setters */

/* Random small ASCII string, letters only, so it is safe as an xattr value
 * that a human might dump. Not NUL-terminated by the caller's need — we
 * NUL-terminate here since some xattr values in the tree are meant to look
 * like text. */
static void rand_letters(char *buf, size_t n)
{
	static const char alpha[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	for (size_t i = 0; i < n; i++)
		buf[i] = alpha[rng_u64(sizeof alpha - 1)];
}

/* Builds a minimal-but-valid POSIX ACL xattr blob: owner/group/other (the
 * mandatory trio) plus one named user and one named group entry, then MASK.
 * Matches struct posix_acl_xattr_{header,entry} (linux/posix_acl_xattr.h) —
 * the exact byte layout the kernel stores under system.posix_acl_access, so
 * no libacl is needed to produce something the kernel (and drsync, which
 * treats it as an opaque blob per agent/src/xattr.c) accepts as-is. */
static size_t build_acl_blob(uint8_t *buf, size_t cap, uint32_t named_uid,
                             uint32_t named_gid, bool with_mask)
{
	size_t n_entries = with_mask ? 5 : 3;
	size_t need = sizeof(struct posix_acl_xattr_header) +
	              n_entries * sizeof(struct posix_acl_xattr_entry);
	if (need > cap)
		return 0;

	struct posix_acl_xattr_header *hdr = (struct posix_acl_xattr_header *)buf;
	hdr->a_version = htole32(POSIX_ACL_XATTR_VERSION);
	struct posix_acl_xattr_entry *e =
		(struct posix_acl_xattr_entry *)(buf + sizeof *hdr);

	/* Kernel requires entries sorted by tag: USER_OBJ, USER, GROUP_OBJ,
	 * [GROUP, MASK in that order if present], OTHER. */
	int i = 0;
	e[i].e_tag = htole16(ACL_USER_OBJ);
	e[i].e_perm = htole16(ACL_READ | ACL_WRITE | ACL_EXECUTE);
	e[i].e_id = htole32((uint32_t)ACL_UNDEFINED_ID);
	i++;

	e[i].e_tag = htole16(ACL_USER);
	e[i].e_perm = htole16(ACL_READ | ACL_WRITE);
	e[i].e_id = htole32(named_uid);
	i++;

	e[i].e_tag = htole16(ACL_GROUP_OBJ);
	e[i].e_perm = htole16(ACL_READ);
	e[i].e_id = htole32((uint32_t)ACL_UNDEFINED_ID);
	i++;

	if (with_mask) {
		e[i].e_tag = htole16(ACL_GROUP);
		e[i].e_perm = htole16(ACL_READ);
		e[i].e_id = htole32(named_gid);
		i++;

		e[i].e_tag = htole16(ACL_MASK);
		e[i].e_perm = htole16(ACL_READ | ACL_WRITE | ACL_EXECUTE);
		e[i].e_id = htole32((uint32_t)ACL_UNDEFINED_ID);
		i++;
	}

	e[i].e_tag = htole16(ACL_OTHER);
	e[i].e_perm = htole16(ACL_READ);
	e[i].e_id = htole32((uint32_t)ACL_UNDEFINED_ID);
	i++;

	return sizeof *hdr + (size_t)i * sizeof *e;
}

/* Sets system.posix_acl_access (and, for directories, a default ACL too —
 * default ACLs only apply to directories). Best-effort: some filesystems
 * (tmpfs without acl mount option, some FUSE backends) reject this, which is
 * exactly the fidelity-exception path drsync itself exercises, so we count
 * and continue rather than treat it as fatal. */
static void apply_acl(const char *path, bool is_dir)
{
	if (g_cfg->no_acl)
		return;
	uint8_t blob[sizeof(struct posix_acl_xattr_header) +
	             5 * sizeof(struct posix_acl_xattr_entry)];
	bool with_mask = (rng_u64(2) == 0);
	size_t n = build_acl_blob(blob, sizeof blob, 1000 + (uint32_t)rng_u64(2000),
	                          1000 + (uint32_t)rng_u64(2000), with_mask);
	if (n == 0)
		return;
	if (lsetxattr(path, "system.posix_acl_access", blob, n, 0) == 0)
		g_stats.acls_set++;

	if (is_dir) {
		size_t dn = build_acl_blob(blob, sizeof blob, 1000 + (uint32_t)rng_u64(2000),
		                          1000 + (uint32_t)rng_u64(2000), false);
		if (dn > 0 && lsetxattr(path, "system.posix_acl_default", blob, dn, 0) == 0)
			g_stats.acls_set++;
	}
}

/* Writes a synthetic system.nfs4_acl blob. On a non-NFSv4 mount this is just
 * opaque bytes under a real xattr name — enough to exercise drsync's raw
 * xattr-copy path and (per acl_untranslatable policy) its skip/fail path on a
 * destination that cannot store system.* xattrs it doesn't recognize. Real
 * NFSv4 ACL XDR encoding is intentionally not reproduced: the value is never
 * interpreted by the kernel off an NFSv4 mount, only copied byte-for-byte. */
static void apply_fake_nfs4_acl(const char *path)
{
	if (g_cfg->no_acl)
		return;
	uint8_t blob[64];
	for (size_t i = 0; i < sizeof blob; i++)
		blob[i] = (uint8_t)rng_u64(256);
	lsetxattr(path, "system.nfs4_acl", blob, sizeof blob, 0);
}

/* user./trusted./security. xattrs — the XC_COPY namespaces in
 * agent/src/xattr.c's classify(). trusted.* and security.* need privilege to
 * set on most kernels/filesystems; failures there are silently skipped
 * (expected under an unprivileged run) rather than counted as errors. */
static void apply_xattrs(const char *path, int count)
{
	if (g_cfg->no_xattr)
		return;
	for (int i = 0; i < count; i++) {
		char name[64], val[128];
		int vlen = 8 + (int)rng_u64(100);
		rand_letters(val, (size_t)vlen);
		snprintf(name, sizeof name, "user.genfixture.attr%03d", i);
		if (lsetxattr(path, name, val, (size_t)vlen, 0) == 0)
			g_stats.xattrs_set++;
	}
	/* one of each privileged namespace, best-effort */
	if (lsetxattr(path, "trusted.genfixture", "t", 1, 0) == 0)
		g_stats.xattrs_set++;
	if (lsetxattr(path, "security.genfixture", "s", 1, 0) == 0)
		g_stats.xattrs_set++;
	/* security.selinux is deliberately NOT touched: xattr.c classifies it
	 * XC_IGNORE (destination-host policy), so setting it here would just be
	 * exercising a path drsync intentionally never copies. */
}

/* Non-"now", non-round nanosecond timestamps: an implementation that copies
 * with second granularity, or that copies mtime but not atime (or vice
 * versa), fails verify against these on the first sync. */
static void apply_times(const char *path, bool nofollow)
{
	struct timespec ts[2];
	time_t base = time(NULL) - (time_t)(3600 + rng_u64(86400 * 30));
	ts[0].tv_sec = base - (time_t)rng_u64(1000);
	ts[0].tv_nsec = (long)rng_u64(999999999);
	ts[1].tv_sec = base;
	ts[1].tv_nsec = (long)rng_u64(999999999);
	int flags = nofollow ? AT_SYMLINK_NOFOLLOW : 0;
	utimensat(AT_FDCWD, path, ts, flags);
}

/* Owner/group: chown to something other than self needs CAP_CHOWN, so this
 * is attempted but not required to succeed — same as drsync's own
 * "journal fidelity exception, keep going" posture for unprivileged agents. */
static void apply_owner(const char *path, bool nofollow)
{
	uint32_t uid = (uint32_t)rng_u64(60000) + 100;
	uint32_t gid = (uint32_t)rng_u64(60000) + 100;
	int flags = nofollow ? AT_SYMLINK_NOFOLLOW : 0;
	fchownat(AT_FDCWD, path, uid, gid, flags);
}

/* --------------------------------------------------------------- writers */

/* Shared outcome handling for the exclusive-create family (open O_EXCL,
 * mkdir, symlink, mkfifo, mknod, link, bind): ok is the syscall's own success
 * test (rc == 0, or fd >= 0 for open). An existing path is a skip, not an
 * error — the "never overwrite" contract — anything else is reported and
 * counted. Returns true only on actual success. */
static bool check_create(bool ok, const char *what, const char *path)
{
	if (ok)
		return true;
	if (errno == EEXIST) {
		g_stats.skipped_existing++;
		return false;
	}
	fprintf(stderr, "\n%s %s: %s\n", what, path, strerror(errno));
	g_stats.errors++;
	return false;
}

/* Fills buf with pseudo-random bytes (content must differ file-to-file so a
 * byte-identity check is meaningful, not just a size check). */
static void fill_random(uint8_t *buf, size_t n)
{
	size_t i = 0;
	while (i + 8 <= n) {
		uint64_t r = rng_next();
		memcpy(buf + i, &r, 8);
		i += 8;
	}
	if (i < n) {
		uint64_t r = rng_next();
		memcpy(buf + i, &r, n - i);
	}
}

/* Writes n freshly-randomized bytes to fd in sizeof(buf)-sized chunks,
 * stopping short on a write error (reported + counted). Returns bytes
 * actually written. Shared by write_regular_file's whole-file and
 * sparse head/tail passes. */
static uint64_t write_loop(int fd, uint8_t *buf, size_t bufcap, uint64_t n,
                           const char *path)
{
	uint64_t written = 0;
	while (written < n) {
		size_t chunk = (n - written) < bufcap ? (size_t)(n - written) : bufcap;
		fill_random(buf, chunk);
		ssize_t w = write(fd, buf, chunk);
		if (w < 0) {
			fprintf(stderr, "\nwrite %s: %s\n", path, strerror(errno));
			g_stats.errors++;
			break;
		}
		written += (uint64_t)w;
	}
	return written;
}

/* Regular file. sparse=true punches a hole in the middle: written as
 * [data][hole via ftruncate/lseek][data], matching the SEEK_DATA/SEEK_HOLE
 * layout preserve_sparse targets (docs/DESIGN-agent.md "16 MiB/4 KiB sparse
 * file arrives content-identical using <1 MiB"). suid/sgid/sticky bits are
 * layered onto the mode when requested — needs no privilege to *set* on a
 * file this process owns, only to have them *honored* on exec (irrelevant
 * here; drsync only needs to preserve the bit). */
static bool write_regular_file(const char *path, uint64_t size, bool sparse,
                               mode_t extra_bits, uint64_t *bytes_out)
{
	int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);
	if (!check_create(fd >= 0, "open", path))
		return false;

	uint64_t written;
	uint8_t buf[65536];
	if (!sparse || size < 8192) {
		written = write_loop(fd, buf, sizeof buf, size, path);
	} else {
		/* head / hole / tail, hole >= half the file so it is unambiguous */
		uint64_t head = size / 4;
		uint64_t hole = size / 2;
		uint64_t tail = size - head - hole;
		write_loop(fd, buf, sizeof buf, head, path);
		if (lseek(fd, (off_t)(head + hole), SEEK_SET) < 0)
			fprintf(stderr, "\nlseek %s: %s\n", path, strerror(errno));
		write_loop(fd, buf, sizeof buf, tail, path);
		if (ftruncate(fd, (off_t)size) < 0)
			fprintf(stderr, "\nftruncate %s: %s\n", path, strerror(errno));
		written = size; /* logical size for progress accounting */
		g_stats.sparse_files++;
	}

	mode_t mode = 0644 | extra_bits;
	if (fchmod(fd, mode) < 0)
		fprintf(stderr, "\nfchmod %s: %s\n", path, strerror(errno));

	apply_xattrs(path, 3 + (int)rng_u64(5));
	apply_acl(path, false);
	apply_fake_nfs4_acl(path);
	apply_owner(path, false);
	apply_times(path, false);

	close(fd);
	g_stats.files++;
	*bytes_out = written;
	return true;
}

static bool make_hardlink(const char *target, const char *linkpath)
{
	if (!check_create(link(target, linkpath) == 0, "link", linkpath))
		return false;
	g_stats.hardlinks++;
	return true;
}

/* Ordinary symlink, or (kind==1) dangling, or (kind==2) a target containing a
 * literal newline — POSIX allows any byte but NUL in a symlink target, and a
 * newline is the classic case that breaks a line-oriented parser, which is
 * exactly why docs/DESIGN-agent.md's own test list calls it out. */
static void make_symlink(const char *linkpath, int kind, uint64_t idx)
{
	char target[512];
	switch (kind) {
	case 1:
		snprintf(target, sizeof target, "/nonexistent/genfixture/dangling-%" PRIu64, idx);
		break;
	case 2:
		snprintf(target, sizeof target, "line-one-%" PRIu64 "\nline-two-embedded-newline", idx);
		break;
	default:
		snprintf(target, sizeof target, "sibling-file-%" PRIu64 ".dat", idx);
	}
	if (!check_create(symlink(target, linkpath) == 0, "symlink", linkpath))
		return;
	g_stats.symlinks++;
	apply_times(linkpath, true);
	/* user.* is invalid on a symlink inode; trusted./security. are valid and
	 * are exactly what agent/src/walker.c's copy_symlink() copies. */
	lsetxattr(linkpath, "trusted.genfixture", "t", 1, 0);
}

static void make_fifo(const char *path)
{
	if (!check_create(mkfifo(path, 0640) == 0, "mkfifo", path))
		return;
	g_stats.fifos++;
	apply_owner(path, false);
	apply_times(path, false);
	apply_xattrs(path, 2);
}

/* Character device node cloned from /dev/null's rdev (a harmless, always-
 * present major/minor pair) so the entry is a real device node without
 * needing to fabricate a device number that might collide with something
 * live. Needs CAP_MKNOD; skipped (counted, not fatal) without it, matching
 * drsync's own "usually EPERM without CAP_MKNOD" handling in walker.c. */
static void make_device(const char *path)
{
	struct stat st;
	if (stat("/dev/null", &st) < 0)
		return;
	if (mknod(path, S_IFCHR | 0600, st.st_rdev) < 0) {
		if (errno == EEXIST)
			g_stats.skipped_existing++;
		/* EPERM: expected when unprivileged; not counted as an error */
		return;
	}
	g_stats.devices++;
	apply_owner(path, false);
	apply_times(path, false);
}

/* AF_UNIX bind is the only portable way to create a socket special file. */
static void make_socket_placeholder(const char *path)
{
	struct sockaddr_un addr;
	int fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0)
		return;
	memset(&addr, 0, sizeof addr);
	addr.sun_family = AF_UNIX;
	size_t plen = strlen(path);
	if (plen >= sizeof addr.sun_path) {
		close(fd);
		return;
	}
	memcpy(addr.sun_path, path, plen);
	if (bind(fd, (struct sockaddr *)&addr, (socklen_t)sizeof addr) < 0) {
		if (errno == EEXIST)
			g_stats.skipped_existing++;
		close(fd);
		return;
	}
	close(fd);
	g_stats.sockets++;
	apply_owner(path, false);
	apply_times(path, false);
}

/* Directory with a "wide xattr" load: 100+ small user.* xattrs, the case
 * docs/DESIGN-agent.md's test list calls out explicitly. Only applied to one
 * directory per level to keep the run's total xattr count sane. */
static void make_wide_xattr_dir(const char *path)
{
	for (int i = 0; i < 120; i++) {
		char name[64], val[32];
		int vlen = 4 + (int)rng_u64(24);
		rand_letters(val, (size_t)vlen);
		snprintf(name, sizeof name, "user.genfixture.wide%03d", i);
		if (lsetxattr(path, name, val, (size_t)vlen, 0) == 0)
			g_stats.xattrs_set++;
	}
}

/* ------------------------------------------------------------- recursion */

/* One entry emitted per call from build_tree's fan-out; sizes are drawn so
 * the running total converges on cfg->total_size without every leaf needing
 * to know the global remaining budget precisely (a bit of overshoot on the
 * last file per directory is fine for a test fixture). */
static uint64_t pick_file_size(uint64_t remaining_budget)
{
	/* Mostly small files (the common case drsync is bottlenecked by), a
	 * periodic larger one, and an occasional multi-MiB file to exercise
	 * chunking. Capped by remaining_budget so a small total_size still
	 * produces a small tree instead of one file eating a huge default. */
	uint64_t choice = rng_u64(100);
	uint64_t want;
	if (choice < 70)
		want = 1 + rng_u64(2048);
	else if (choice < 95)
		want = 2048 + rng_u64(256 * 1024);
	else
		want = (1u << 20) + rng_u64(8u << 20);
	if (remaining_budget > 0 && want > remaining_budget)
		want = remaining_budget;
	if (want == 0)
		want = 1;
	return want;
}

typedef struct {
	char prior_file[MAXP]; /* most recent regular file at this scope, for hardlinks */
	bool have_prior;
} scope_t;

static void build_tree(const cfg_t *c, const char *dir, int depth_remaining,
                       int level, uint64_t *budget)
{
	if (*budget == 0)
		return;

	/* One directory per level gets ACLs + a heavy xattr load, so the fixture
	 * always contains at least one of each without every directory paying
	 * the 120-xattr cost. */
	apply_acl(dir, true);
	apply_fake_nfs4_acl(dir);
	if (level == 0)
		make_wide_xattr_dir(dir);

	scope_t scope = { .have_prior = false };

	/* A handful of regular files at this level. */
	int nfiles = 3 + (int)rng_u64(5);
	for (int i = 0; i < nfiles && *budget > 0; i++) {
		char name[256];
		if (i == 0 && level < 2) {
			/* one 255-byte name per shallow level: the exact
			 * NAME_MAX case docs/DESIGN-agent.md's list calls out.
			 * 255 'a's plus a distinguishing numeric suffix so the
			 * two levels' names don't collide, still 255 bytes total. */
			memset(name, 'a', 255);
			char suffix[8];
			int slen = snprintf(suffix, sizeof suffix, "%03d", level);
			memcpy(name + 255 - slen, suffix, (size_t)slen);
			name[255] = '\0';
		} else {
			snprintf(name, sizeof name, "file-%d-%03d.dat", level, i);
		}
		char path[MAXP];
		if (!path_join(path, sizeof path, dir, name))
			continue;

		bool sparse = (rng_u64(6) == 0);
		mode_t extra = 0;
		int mchoice = (int)rng_u64(20);
		if (mchoice == 0) extra = S_ISUID;
		else if (mchoice == 1) extra = S_ISGID;
		else if (mchoice == 2) extra = S_ISVTX;

		uint64_t sz = pick_file_size(*budget);
		uint64_t wrote = 0;
		if (write_regular_file(path, sz, sparse, extra, &wrote)) {
			*budget -= (wrote > *budget) ? *budget : wrote;
			g_stats.bytes += wrote;
			strncpy(scope.prior_file, path, sizeof scope.prior_file - 1);
			scope.prior_file[sizeof scope.prior_file - 1] = '\0';
			scope.have_prior = true;
			progress(false);
		}
	}

	/* A hardlink to the last regular file created at this level. */
	if (scope.have_prior) {
		char lp[MAXP];
		if (path_join(lp, sizeof lp, dir, "hardlink-to-prior.dat"))
			make_hardlink(scope.prior_file, lp);
	}

	/* Symlinks: ordinary, dangling, newline-target. */
	for (int kind = 0; kind < 3; kind++) {
		char name[64], path[MAXP];
		snprintf(name, sizeof name, "symlink-%s",
		         kind == 0 ? "ok" : kind == 1 ? "dangling" : "newline");
		if (path_join(path, sizeof path, dir, name))
			make_symlink(path, kind, (uint64_t)level * 1000 + (uint64_t)kind);
	}

	/* FIFO: always (no privilege needed). Device + socket: best-effort. */
	{
		char path[MAXP];
		if (path_join(path, sizeof path, dir, "fifo"))
			make_fifo(path);
		if (!c->no_special) {
			if (path_join(path, sizeof path, dir, "chardev"))
				make_device(path);
			if (path_join(path, sizeof path, dir, "socket"))
				make_socket_placeholder(path);
		}
	}

	if (depth_remaining <= 0)
		return;

	/* Two subdirectories per level: enough to make the tree genuinely
	 * recursive without an exponential blow-up in entry count at depth 5+. */
	for (int i = 0; i < 2 && *budget > 0; i++) {
		char name[32], path[MAXP];
		snprintf(name, sizeof name, "dir-%d-%d", level, i);
		if (!path_join(path, sizeof path, dir, name))
			continue;
		if (mkdir(path, 0755) == 0) {
			g_stats.dirs++;
		} else if (errno != EEXIST) {
			/* A real error (not "already there") means this subtree isn't
			 * usable: skip recursing into it, unlike the EEXIST case below
			 * which still descends to add content to what's already there. */
			check_create(false, "mkdir", path);
			continue;
		} else {
			g_stats.skipped_existing++;
		}
		build_tree(c, path, depth_remaining - 1, level + 1, budget);
		/* Directory metadata (mode/owner/times) is set AFTER recursing, since
		 * creating children bumps the directory's own mtime — matching why
		 * drsync applies directory metadata as a DIRFIX pass after the walk
		 * (docs/DESIGN-coordinator.md §2.2), not while descending into it. */
		mode_t dmode = 0750 + (mode_t)(rng_u64(2) * 5); /* 0750 or 0755 */
		chmod(path, dmode);
		apply_owner(path, false);
		apply_times(path, false);
	}
}

/* ----------------------------------------------------------------- usage */

static void usage(const char *argv0)
{
	fprintf(stderr,
"genfixture — synthetic test-tree generator for drsync fidelity testing\n"
"\n"
"usage: %s <directory> <total-size> [depth] [options]\n"
"\n"
"  <directory>    root to create the tree under (created if missing; existing\n"
"                 entries inside it are never overwritten, only added to)\n"
"  <total-size>   approximate total bytes of regular-file content to write;\n"
"                 accepts a bare number or a K/M/G/T suffix (e.g. 500M, 2G)\n"
"  [depth]        directory nesting depth (default 5)\n"
"\n"
"options:\n"
"  --seed N       RNG seed (default: /dev/urandom, or time-based fallback)\n"
"  --no-acl       skip POSIX ACL / nfs4_acl xattrs\n"
"  --no-xattr     skip user./trusted./security. xattrs\n"
"  --no-special   skip device node / socket creation attempts\n"
"  -h, --help     this text\n",
		argv0);
}

static uint64_t parse_size(const char *s)
{
	char *end;
	double v = strtod(s, &end);
	uint64_t mul = 1;
	if (*end) {
		switch (end[0]) {
		case 'k': case 'K': mul = 1024ULL; break;
		case 'm': case 'M': mul = 1024ULL * 1024; break;
		case 'g': case 'G': mul = 1024ULL * 1024 * 1024; break;
		case 't': case 'T': mul = 1024ULL * 1024 * 1024 * 1024; break;
		default: die("bad size suffix in '%s'", s);
		}
	}
	if (v < 0)
		die("size must be non-negative");
	return (uint64_t)(v * (double)mul);
}

static uint64_t random_seed(void)
{
	uint64_t s;
	if (getrandom(&s, sizeof s, 0) == (ssize_t)sizeof s)
		return s;
	return (uint64_t)time(NULL) ^ (uint64_t)getpid();
}

int main(int argc, char **argv)
{
	if (argc < 3 || !strcmp(argv[1], "-h") || !strcmp(argv[1], "--help")) {
		usage(argv[0]);
		return argc < 3 ? 2 : 0;
	}

	cfg_t cfg;
	memset(&cfg, 0, sizeof cfg);
	cfg.root = argv[1];
	cfg.total_size = parse_size(argv[2]);
	cfg.depth = 5;
	cfg.seed = 0;
	bool have_seed = false;

	int next = 3;
	if (next < argc && argv[next][0] != '-') {
		cfg.depth = atoi(argv[next]);
		next++;
	}
	for (int i = next; i < argc; i++) {
		if (!strcmp(argv[i], "--seed") && i + 1 < argc) {
			cfg.seed = strtoull(argv[++i], NULL, 10);
			have_seed = true;
		} else if (!strcmp(argv[i], "--no-acl")) {
			cfg.no_acl = true;
		} else if (!strcmp(argv[i], "--no-xattr")) {
			cfg.no_xattr = true;
		} else if (!strcmp(argv[i], "--no-special")) {
			cfg.no_special = true;
		} else if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
			usage(argv[0]);
			return 0;
		} else {
			die("unknown option: %s", argv[i]);
		}
	}
	if (cfg.depth < 0)
		die("depth must be >= 0");

	if (!have_seed)
		cfg.seed = random_seed();
	rng_seed(cfg.seed);
	g_cfg = &cfg;

	/* mkdir -p the root; an existing root is fine (we only refuse to
	 * overwrite entries, not the root directory itself). */
	{
		char tmp[MAXP];
		size_t len = strlen(cfg.root);
		if (len == 0 || len >= sizeof tmp)
			die("bad directory path");
		strcpy(tmp, cfg.root);
		for (char *p = tmp + 1; *p; p++) {
			if (*p == '/') {
				*p = '\0';
				mkdir(tmp, 0755);
				*p = '/';
			}
		}
		if (mkdir(tmp, 0755) < 0 && errno != EEXIST)
			die("mkdir %s: %s", cfg.root, strerror(errno));
	}

	struct stat rst;
	if (stat(cfg.root, &rst) < 0 || !S_ISDIR(rst.st_mode))
		die("%s is not a directory", cfg.root);

	printf("genfixture: root=%s total-size=%" PRIu64 " depth=%d seed=%" PRIu64
	       " acl=%s xattr=%s special=%s\n",
	       cfg.root, cfg.total_size, cfg.depth, cfg.seed,
	       cfg.no_acl ? "off" : "on", cfg.no_xattr ? "off" : "on",
	       cfg.no_special ? "off" : "on");
	if (cfg.no_special)
		printf("note: device/socket nodes skipped (--no-special)\n");
	else if (geteuid() != 0)
		printf("note: not running as root — device node creation will be "
		       "skipped (EPERM), sockets and fifos still created\n");

	g_t0 = now_s();
	g_last_report = 0;
	/* total-size=0 still gets a 1-byte budget so depth/attribute coverage
	 * (dirs, symlinks, fifos, ACLs, xattrs...) can be exercised with a
	 * near-empty tree, rather than building nothing at all. */
	uint64_t budget = cfg.total_size == 0 ? 1 : cfg.total_size;
	build_tree(&cfg, cfg.root, cfg.depth, 0, &budget);

	progress(true);
	fprintf(stderr, "\n");

	printf("\ngenfixture summary:\n");
	printf("  regular files   : %" PRIu64 " (%" PRIu64 " sparse), %" PRIu64 " bytes\n",
	       g_stats.files, g_stats.sparse_files, g_stats.bytes);
	printf("  directories     : %" PRIu64 "\n", g_stats.dirs);
	printf("  symlinks        : %" PRIu64 "\n", g_stats.symlinks);
	printf("  hardlinks       : %" PRIu64 "\n", g_stats.hardlinks);
	printf("  fifos           : %" PRIu64 "\n", g_stats.fifos);
	printf("  device nodes    : %" PRIu64 "\n", g_stats.devices);
	printf("  sockets         : %" PRIu64 "\n", g_stats.sockets);
	printf("  xattrs set      : %" PRIu64 "\n", g_stats.xattrs_set);
	printf("  acl xattrs set  : %" PRIu64 "\n", g_stats.acls_set);
	printf("  skipped existing: %" PRIu64 "\n", g_stats.skipped_existing);
	printf("  errors          : %" PRIu64 "\n", g_stats.errors);
	printf("  seed            : %" PRIu64 " (pass --seed %" PRIu64 " to reproduce)\n",
	       cfg.seed, cfg.seed);

	return g_stats.errors > 0 ? 1 : 0;
}
