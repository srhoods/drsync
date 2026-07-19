/* fsprobe — filesystem metadata-path profiler for drsync migrations.
 *
 * drsync's throughput on a many-small-files migration is bounded not by
 * bandwidth but by the destination filesystem's per-file metadata cost and by
 * how much of that cost serializes on a single directory. Local XFS answers a
 * file create in microseconds; a clustered filesystem (GPFS, Weka) answers it
 * with a round trip to a metadata server, and creates into one directory may
 * serialize on that directory's lock/token. When they do, adding agents and
 * copy threads buys nothing — the wall is the directory, not the fleet.
 *
 * This tool reproduces drsync's exact per-file write sequence in isolation so
 * you can, on the real source and destination filesystems:
 *
 *   - see the per-operation latency breakdown (which syscall is slow), and
 *   - see whether throughput scales with concurrency or is pinned by one
 *     directory (the single most important question), and
 *   - bisect which operation is responsible by toggling each off.
 *
 * It links nothing from drsync and depends only on libc + pthreads, so it can
 * be built and run directly on a filer host that has no drsync checkout.
 *
 *   cc -O2 -pthread -o fsprobe fsprobe.c
 *
 * The write path mirrors agent/src/copy.c: a small file is copied as
 *   openat(O_CREAT|O_EXCL) -> [ftruncate] -> write -> [setxattr] -> [fdatasync]
 *   -> [fchown] -> [fchmod] -> [futimens] -> [rename] -> close
 * Every bracketed step is a drsync option and a tool flag, so a run with the
 * defaults measures what drsync actually does.
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ timing */

static inline double now_s(void)
{
	struct timespec t;
	clock_gettime(CLOCK_MONOTONIC, &t);
	return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

/* Per-operation latency accumulator. A log-spaced histogram bounds memory
 * regardless of file count while still giving usable tail percentiles; exact
 * min/max/sum sit alongside it. Bucket i covers [BASE*R^i, BASE*R^(i+1)) us. */
#define NB 512
#define BASE_US 1.0
#define RATIO 1.055 /* ~13 buckets per decade → ±5.5% percentile resolution */

typedef struct {
	uint64_t n;
	double sum, min, max;
	uint64_t bucket[NB];
} lat_t;

static void lat_init(lat_t *l)
{
	memset(l, 0, sizeof *l);
	l->min = 1e30;
}

static void lat_add(lat_t *l, double us)
{
	l->n++;
	l->sum += us;
	if (us < l->min)
		l->min = us;
	if (us > l->max)
		l->max = us;
	int b = 0;
	if (us > BASE_US) {
		double v = us / BASE_US;
		/* b = floor(log(v)/log(RATIO)); cheap loop, NB is small */
		while (b < NB - 1 && v >= RATIO) {
			v /= RATIO;
			b++;
		}
	}
	l->bucket[b]++;
}

static void lat_merge(lat_t *dst, const lat_t *src)
{
	dst->n += src->n;
	dst->sum += src->sum;
	if (src->min < dst->min)
		dst->min = src->min;
	if (src->max > dst->max)
		dst->max = src->max;
	for (int i = 0; i < NB; i++)
		dst->bucket[i] += src->bucket[i];
}

/* Lower edge of bucket i, in microseconds. No -lm: NB is small, exponent
 * integer. */
static double bucket_edge_us(int i)
{
	double v = BASE_US;
	for (int k = 0; k < i; k++)
		v *= RATIO;
	return v;
}

static double lat_pct(const lat_t *l, double p)
{
	if (l->n == 0)
		return 0;
	uint64_t want = (uint64_t)(p / 100.0 * (double)l->n);
	uint64_t cum = 0;
	for (int i = 0; i < NB; i++) {
		cum += l->bucket[i];
		if (cum >= want)
			return bucket_edge_us(i);
	}
	return l->max;
}

/* --------------------------------------------------------------- op set */

enum { OP_OPEN, OP_TRUNC, OP_WRITE, OP_XATTR, OP_FSYNC, OP_CHOWN, OP_CHMOD,
       OP_TIMES, OP_RENAME, OP_CLOSE, OP_UNLINK, N_OP };
static const char *OP_NAME[N_OP] = { "open", "ftruncate", "write", "setxattr",
	"fdatasync", "fchown", "fchmod", "futimens", "rename", "close", "unlink" };

/* ---------------------------------------------------------------- config */

typedef struct {
	const char *path;   /* destination directory */
	long files;         /* total files to create */
	long size;          /* bytes per file */
	int threads;        /* concurrent writers */
	int dirs;           /* spread files across this many subdirs */
	/* which drsync steps to perform */
	bool do_trunc, do_xattr, do_fsync, do_chown, do_chmod, do_times, do_rename;
	bool keep;          /* leave files behind (default: unlink at end) */
} cfg_t;

typedef struct {
	const cfg_t *c;
	int tid;
	lat_t lat[N_OP];
	long done;
	long errors;
	int first_errno;
	int first_errop;
} worker_t;

static volatile int g_stop; /* set on first fatal error to abort the run */

#define TIMED(L, EXPR)                                                          \
	do {                                                                   \
		double _t0 = now_s();                                          \
		long _r = (long)(EXPR);                                        \
		lat_add(&(L), (now_s() - _t0) * 1e6);                          \
		if (_r < 0) {                                                  \
			w->errors++;                                          \
			if (!w->first_errno) {                                \
				w->first_errno = errno;                       \
				w->first_errop = &(L) - w->lat;               \
			}                                                     \
		}                                                             \
	} while (0)

/* Copy one file the way agent/src/copy.c does, timing each step. dfd is the
 * (already open) destination subdirectory this file belongs to. */
static void copy_one(worker_t *w, int dfd, long idx, const void *buf)
{
	const cfg_t *c = w->c;
	char tmp[64], fin[64];
	snprintf(fin, sizeof fin, "f%010ld", idx);
	snprintf(tmp, sizeof tmp, ".drsync.tmp.%d.%ld", w->tid, idx);
	const char *first = c->do_rename ? tmp : fin;

	double t0 = now_s();
	int fd = openat(dfd, first, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
	lat_add(&w->lat[OP_OPEN], (now_s() - t0) * 1e6);
	if (fd < 0) {
		w->errors++;
		if (!w->first_errno) { w->first_errno = errno; w->first_errop = OP_OPEN; }
		return;
	}

	/* preserve_sparse path sets the size up front (design §4). */
	if (c->do_trunc)
		TIMED(w->lat[OP_TRUNC], ftruncate(fd, c->size));

	if (c->size > 0) {
		double a = now_s();
		ssize_t r = write(fd, buf, (size_t)c->size);
		lat_add(&w->lat[OP_WRITE], (now_s() - a) * 1e6);
		if (r != c->size) {
			w->errors++;
			if (!w->first_errno) { w->first_errno = errno ? errno : EIO; w->first_errop = OP_WRITE; }
		}
	}

	if (c->do_xattr) {
		/* one small user xattr, as a stand-in for drsync's xattr copy */
		static const char v[] = "fsprobe";
		TIMED(w->lat[OP_XATTR], fsetxattr(fd, "user.fsprobe", v, sizeof v, 0));
	}
	if (c->do_fsync)
		TIMED(w->lat[OP_FSYNC], fdatasync(fd));
	if (c->do_chown)
		/* Own uid/gid: a real SETATTR the kernel actually sends (chown to
		 * -1/-1 is short-circuited), without needing privilege. drsync sends
		 * the source's ids; the RPC cost is the same. */
		TIMED(w->lat[OP_CHOWN], fchown(fd, geteuid(), getegid()));
	if (c->do_chmod)
		TIMED(w->lat[OP_CHMOD], fchmod(fd, 0644));
	if (c->do_times) {
		struct timespec ts[2] = { { 0, 0 }, { 0, 0 } };
		ts[0].tv_sec = ts[1].tv_sec = time(NULL) - 3600;
		TIMED(w->lat[OP_TIMES], futimens(fd, ts));
	}
	if (c->do_rename)
		TIMED(w->lat[OP_RENAME], renameat(dfd, tmp, dfd, fin));

	double z = now_s();
	close(fd);
	lat_add(&w->lat[OP_CLOSE], (now_s() - z) * 1e6);

	w->done++;
}

static void *writer(void *arg)
{
	worker_t *w = arg;
	const cfg_t *c = w->c;
	void *buf = NULL;
	if (c->size > 0) {
		buf = malloc((size_t)c->size);
		memset(buf, 'x', (size_t)c->size);
	}
	/* Each thread opens the destination subdirs it will write into once, the
	 * way drsync holds the walk's dst dir fd rather than re-opening per file. */
	int *dfd = calloc((size_t)c->dirs, sizeof *dfd);
	for (int d = 0; d < c->dirs; d++) {
		char sub[32];
		snprintf(sub, sizeof sub, "d%03d", d);
		dfd[d] = openat(AT_FDCWD, c->path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
		int real = openat(dfd[d], sub, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
		close(dfd[d]);
		dfd[d] = real;
		if (dfd[d] < 0) {
			fprintf(stderr, "open subdir %s/%s: %s\n", c->path, sub, strerror(errno));
			g_stop = 1;
		}
	}
	for (long i = w->tid; i < c->files && !g_stop; i += c->threads)
		copy_one(w, dfd[i % c->dirs], i, buf);
	for (int d = 0; d < c->dirs; d++)
		if (dfd[d] >= 0)
			close(dfd[d]);
	free(dfd);
	free(buf);
	return NULL;
}

/* ---------------------------------------------------------- one write phase */

typedef struct {
	double wall;
	double files_per_s;
	long files, errors;
	int err_errno, err_op;
	lat_t lat[N_OP];
} result_t;

static void make_dirs(const cfg_t *c)
{
	mkdir(c->path, 0755);
	for (int d = 0; d < c->dirs; d++) {
		char sub[512];
		snprintf(sub, sizeof sub, "%s/d%03d", c->path, d);
		mkdir(sub, 0755);
	}
}

static void cleanup_files(const cfg_t *c)
{
	/* Unlink is itself a metadata op on a clustered fs; do it quietly. */
	for (int d = 0; d < c->dirs; d++) {
		char sub[512];
		snprintf(sub, sizeof sub, "%s/d%03d", c->path, d);
		int dfd = openat(AT_FDCWD, sub, O_RDONLY | O_DIRECTORY);
		if (dfd < 0)
			continue;
		for (long i = 0; i < c->files; i++) {
			if (i % c->dirs != d)
				continue;
			char fin[64];
			snprintf(fin, sizeof fin, "f%010ld", i);
			unlinkat(dfd, fin, 0);
		}
		close(dfd);
		rmdir(sub);
	}
}

static result_t run_phase(const cfg_t *c)
{
	result_t res;
	memset(&res, 0, sizeof res);
	for (int i = 0; i < N_OP; i++)
		lat_init(&res.lat[i]);

	make_dirs(c);
	g_stop = 0;
	worker_t *w = calloc((size_t)c->threads, sizeof *w);
	pthread_t *th = calloc((size_t)c->threads, sizeof *th);
	for (int i = 0; i < c->threads; i++) {
		w[i].c = c;
		w[i].tid = i;
		for (int j = 0; j < N_OP; j++)
			lat_init(&w[i].lat[j]);
	}
	double t0 = now_s();
	for (int i = 0; i < c->threads; i++)
		pthread_create(&th[i], NULL, writer, &w[i]);
	for (int i = 0; i < c->threads; i++)
		pthread_join(th[i], NULL);
	res.wall = now_s() - t0;

	for (int i = 0; i < c->threads; i++) {
		res.files += w[i].done;
		res.errors += w[i].errors;
		if (w[i].first_errno && !res.err_errno) {
			res.err_errno = w[i].first_errno;
			res.err_op = w[i].first_errop;
		}
		for (int j = 0; j < N_OP; j++)
			lat_merge(&res.lat[j], &w[i].lat[j]);
	}
	res.files_per_s = res.wall > 0 ? res.files / res.wall : 0;
	free(w);
	free(th);
	if (!c->keep)
		cleanup_files(c);
	return res;
}

/* ------------------------------------------------------- source read bench */

/* Characterize a SOURCE tree the way drsync's scan + copy read it: enumerate
 * one directory, statx every entry, then open+read+close every file. Reports
 * the three rates separately — a slow source shows up as a low statx or read
 * rate, distinct from the destination write cost. Reads the tree left by a
 * prior `write --keep`, or any real directory of files. */
static void read_bench(const char *dir, int threads)
{
	int dfd = openat(AT_FDCWD, dir, O_RDONLY | O_DIRECTORY);
	if (dfd < 0) {
		fprintf(stderr, "open %s: %s\n", dir, strerror(errno));
		return;
	}
	/* enumerate */
	double t0 = now_s();
	char **names = NULL;
	long n = 0, cap = 0;
	DIR *d = fdopendir(dup(dfd));
	struct dirent *de;
	while (d && (de = readdir(d))) {
		if (de->d_name[0] == '.')
			continue;
		if (n == cap) {
			cap = cap ? cap * 2 : 1024;
			names = realloc(names, (size_t)cap * sizeof *names);
		}
		names[n++] = strdup(de->d_name);
	}
	if (d)
		closedir(d);
	double t_enum = now_s() - t0;

	/* statx every entry */
	t0 = now_s();
	long stat_ok = 0;
	for (long i = 0; i < n; i++) {
		struct statx sx;
		if (statx(dfd, names[i], AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS, &sx) == 0)
			stat_ok++;
	}
	double t_stat = now_s() - t0;

	/* read every file fully */
	t0 = now_s();
	long bytes = 0, read_ok = 0;
	char buf[1 << 16];
	for (long i = 0; i < n; i++) {
		int fd = openat(dfd, names[i], O_RDONLY);
		if (fd < 0)
			continue;
		ssize_t r;
		while ((r = read(fd, buf, sizeof buf)) > 0)
			bytes += r;
		close(fd);
		read_ok++;
	}
	double t_read = now_s() - t0;

	(void)threads;
	printf("source read characterization of %s:\n", dir);
	printf("  entries        : %ld\n", n);
	printf("  readdir        : %.0f entries/s  (%.2fs)\n", t_enum > 0 ? n / t_enum : 0, t_enum);
	printf("  statx          : %.0f stat/s     (%.2fs, %ld ok)\n",
	       t_stat > 0 ? n / t_stat : 0, t_stat, stat_ok);
	printf("  open+read+close: %.0f files/s    (%.2fs, %ld ok, %.1f MiB, %.0f MiB/s)\n",
	       t_read > 0 ? n / t_read : 0, t_read, read_ok, bytes / 1048576.0,
	       t_read > 0 ? bytes / 1048576.0 / t_read : 0);
	for (long i = 0; i < n; i++)
		free(names[i]);
	free(names);
	close(dfd);
}

/* ------------------------------------------------------------ diagnostics */

static const char *fs_name(unsigned long t)
{
	switch (t) {
	case 0x58465342: return "xfs";
	case 0x47504653: return "gpfs";
	case 0x6969: return "nfs";
	case 0x65735546: return "fuse (weka/other)";
	case 0xef53: return "ext2/3/4";
	case 0x9123683e: return "btrfs";
	case 0x01021994: return "tmpfs";
	case 0x2fc12fc1: return "zfs";
	default: return "unknown";
	}
}

static void print_fsinfo(const char *path)
{
	struct statfs sf;
	struct statvfs sv;
	printf("filesystem at %s:\n", path);
	if (statfs(path, &sf) == 0)
		printf("  type          : %s (magic 0x%lx)\n", fs_name((unsigned long)sf.f_type),
		       (unsigned long)sf.f_type);
	if (statvfs(path, &sv) == 0) {
		printf("  block size    : %lu\n", sv.f_bsize);
		printf("  free / total  : %.1f / %.1f GiB\n",
		       (double)sv.f_bavail * sv.f_frsize / 1073741824.0,
		       (double)sv.f_blocks * sv.f_frsize / 1073741824.0);
	}
}

/* Print the per-op latency table for a phase. */
static void print_lat(const result_t *r)
{
	printf("  %-11s %8s %9s %9s %9s %9s %9s\n",
	       "op", "count", "mean", "p50", "p90", "p99", "max (ms)");
	for (int i = 0; i < N_OP; i++) {
		const lat_t *l = &r->lat[i];
		if (l->n == 0)
			continue;
		printf("  %-11s %8" PRIu64 " %8.2f  %8.2f  %8.2f  %8.2f  %8.2f\n",
		       OP_NAME[i], l->n, l->sum / l->n / 1000.0,
		       lat_pct(l, 50) / 1000.0, lat_pct(l, 90) / 1000.0,
		       lat_pct(l, 99) / 1000.0, l->max / 1000.0);
	}
}

/* ----------------------------------------------------------------- modes */

static void usage(void)
{
	fprintf(stderr,
"fsprobe — filesystem metadata-path profiler for drsync\n"
"\n"
"  fsprobe info   <dir>\n"
"      Identify the filesystem and capacity at <dir>.\n"
"\n"
"  fsprobe write  <dir> [opts]\n"
"      One write phase mirroring drsync's per-file copy; prints the per-op\n"
"      latency breakdown and aggregate files/s. This is the core measurement.\n"
"\n"
"  fsprobe scale  <dir> [opts]\n"
"      Repeats the write phase at 1,2,4,8,16,32,64 threads into a SINGLE\n"
"      directory. A flat curve means the directory serializes — the finding\n"
"      that explains why adding agents does not help.\n"
"\n"
"  fsprobe ablate <dir> [opts]\n"
"      Repeats the write phase toggling one drsync step at a time, so you can\n"
"      read off which operation dominates on this filesystem.\n"
"\n"
"  fsprobe spread <dir> [opts]\n"
"      Same file count into 1 directory vs <threads> directories, to confirm\n"
"      whether the bottleneck is per-directory.\n"
"\n"
"  fsprobe read   <dir>\n"
"      Characterize a SOURCE tree: readdir, statx, and read rates over an\n"
"      existing directory of files (e.g. one left by `write --keep`).\n"
"\n"
"options:\n"
"  -n N     files            (default 20000)\n"
"  -s N     bytes per file   (default 1100)\n"
"  -t N     threads          (default 8)\n"
"  -d N     spread across N subdirs   (default 1)\n"
"  --no-trunc --no-xattr --fsync --no-chown --no-chmod --no-times --no-rename\n"
"           toggle drsync steps (defaults match drsync: trunc+chown+chmod+\n"
"           times+rename on, xattr on, fsync OFF — NFS/GPFS close already\n"
"           commits; add --fsync to measure per_file mode)\n"
"  --keep   do not delete the files afterwards\n");
}

static void defaults(cfg_t *c)
{
	memset(c, 0, sizeof *c);
	c->files = 20000;
	c->size = 1100;
	c->threads = 8;
	c->dirs = 1;
	c->do_trunc = c->do_xattr = c->do_chown = c->do_chmod = c->do_times = c->do_rename = true;
	c->do_fsync = false;
}

static void parse(cfg_t *c, int argc, char **argv, int from)
{
	for (int i = from; i < argc; i++) {
		char *a = argv[i];
		if (!strcmp(a, "-n")) c->files = atol(argv[++i]);
		else if (!strcmp(a, "-s")) c->size = atol(argv[++i]);
		else if (!strcmp(a, "-t")) c->threads = atoi(argv[++i]);
		else if (!strcmp(a, "-d")) c->dirs = atoi(argv[++i]);
		else if (!strcmp(a, "--no-trunc")) c->do_trunc = false;
		else if (!strcmp(a, "--no-xattr")) c->do_xattr = false;
		else if (!strcmp(a, "--fsync")) c->do_fsync = true;
		else if (!strcmp(a, "--no-chown")) c->do_chown = false;
		else if (!strcmp(a, "--no-chmod")) c->do_chmod = false;
		else if (!strcmp(a, "--no-times")) c->do_times = false;
		else if (!strcmp(a, "--no-rename")) c->do_rename = false;
		else if (!strcmp(a, "--keep")) c->keep = true;
		else { fprintf(stderr, "unknown option: %s\n", a); exit(2); }
		if (c->dirs < 1) c->dirs = 1;
		if (c->threads < 1) c->threads = 1;
	}
}

static void report_phase(const cfg_t *c, const result_t *r, const char *label)
{
	printf("%-18s %8.0f files/s   %ld files in %.2fs   %d thread%s, %d dir%s\n",
	       label, r->files_per_s, r->files, r->wall,
	       c->threads, c->threads == 1 ? "" : "s", c->dirs, c->dirs == 1 ? "" : "s");
	if (r->errors)
		printf("  !! %ld errors — first: %s on %s\n", r->errors,
		       strerror(r->err_errno), OP_NAME[r->err_op]);
}

int main(int argc, char **argv)
{
	if (argc < 3) {
		usage();
		return 2;
	}
	const char *mode = argv[1];
	const char *dir = argv[2];
	cfg_t c;
	defaults(&c);
	c.path = dir;

	if (!strcmp(mode, "info")) {
		print_fsinfo(dir);
		return 0;
	}
	if (!strcmp(mode, "read")) {
		parse(&c, argc, argv, 3);
		print_fsinfo(dir);
		printf("\n");
		read_bench(dir, c.threads);
		return 0;
	}

	parse(&c, argc, argv, 3);
	printf("== fsprobe %s ==\n", mode);
	print_fsinfo(dir);
	printf("sequence per file:  open%s%s -> write ->%s%s%s%s%s%s close\n",
	       c.do_trunc ? " -> ftruncate" : "", "",
	       c.do_xattr ? " setxattr ->" : "", c.do_fsync ? " fdatasync ->" : "",
	       c.do_chown ? " fchown ->" : "", c.do_chmod ? " fchmod ->" : "",
	       c.do_times ? " futimens ->" : "", c.do_rename ? " rename ->" : "");
	printf("\n");

	if (!strcmp(mode, "write")) {
		result_t r = run_phase(&c);
		report_phase(&c, &r, "result");
		printf("\n");
		print_lat(&r);
	} else if (!strcmp(mode, "scale")) {
		printf("thread scaling into a SINGLE directory (flat => the dir serializes):\n\n");
		int steps[] = { 1, 2, 4, 8, 16, 32, 64 };
		c.dirs = 1;
		for (unsigned s = 0; s < sizeof steps / sizeof *steps; s++) {
			c.threads = steps[s];
			result_t r = run_phase(&c);
			char lbl[32];
			snprintf(lbl, sizeof lbl, "threads=%d", c.threads);
			report_phase(&c, &r, lbl);
		}
	} else if (!strcmp(mode, "spread")) {
		printf("one directory vs many (confirms whether the bottleneck is per-directory):\n\n");
		int save = c.threads;
		c.dirs = 1;
		result_t a = run_phase(&c);
		report_phase(&c, &a, "1 directory");
		c.dirs = save;
		result_t b = run_phase(&c);
		char lbl[32];
		snprintf(lbl, sizeof lbl, "%d directories", c.dirs);
		report_phase(&c, &b, lbl);
		if (a.files_per_s > 0)
			printf("\nspreading across %d dirs: %.1fx\n", c.dirs, b.files_per_s / a.files_per_s);
	} else if (!strcmp(mode, "ablate")) {
		printf("which step costs what (each row drops one step from the full drsync sequence):\n\n");
		cfg_t base = c;
		result_t full = run_phase(&base);
		report_phase(&base, &full, "full (drsync)");
		struct { const char *lbl; int op; } toggles[] = {
			{ "  no ftruncate", OP_TRUNC }, { "  no setxattr", OP_XATTR },
			{ "  no fchown", OP_CHOWN }, { "  no fchmod", OP_CHMOD },
			{ "  no futimens", OP_TIMES }, { "  no rename (direct)", OP_RENAME },
		};
		for (unsigned i = 0; i < sizeof toggles / sizeof *toggles; i++) {
			cfg_t t = c;
			switch (toggles[i].op) {
			case OP_TRUNC: t.do_trunc = false; break;
			case OP_XATTR: t.do_xattr = false; break;
			case OP_CHOWN: t.do_chown = false; break;
			case OP_CHMOD: t.do_chmod = false; break;
			case OP_TIMES: t.do_times = false; break;
			case OP_RENAME: t.do_rename = false; break;
			}
			result_t r = run_phase(&t);
			char lbl[48];
			double gain = full.files_per_s > 0 ? r.files_per_s / full.files_per_s : 1.0;
			snprintf(lbl, sizeof lbl, "%s (%.2fx)", toggles[i].lbl, gain);
			report_phase(&t, &r, lbl);
		}
		printf("\n+fdatasync (per_file mode) for comparison:\n");
		cfg_t f = c;
		f.do_fsync = true;
		result_t rf = run_phase(&f);
		report_phase(&f, &rf, "  +fdatasync");
	} else {
		usage();
		return 2;
	}
	return 0;
}
