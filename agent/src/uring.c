/* Raw io_uring statx batching (docs/DESIGN-agent.md §2.2) — no liburing
 * dependency; the ring syscalls and mmaps are driven directly. One ring per
 * walker thread (thread-local), so no locking on the submission path.
 *
 * Batched IORING_OP_STATX overlaps NFS getattr round trips: with the ring
 * kept full the walk is bounded by the NFS slot table, not the RTT. The ring
 * depth defaults to 256 and is set per job from tuning.statx_batch via
 * uring_set_depth (kernel rounds up to a power of two). Falls back to serial
 * fstatat when io_uring is unavailable (RHEL disables it by default on some
 * releases) or IORING_OP_STATX is unsupported. */
#include "agent.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#include <linux/io_uring.h>

#define RING_ENTRIES 256 /* default ring depth (statx_batch default matches) */
#define RING_MIN     1
#define RING_MAX     4096 /* guard against an absurd spec value */

bool g_uring_enabled = false;

/* Desired ring depth (statx in flight per walker thread). Set from the job's
 * tuning.statx_batch via uring_set_depth; read when a thread lazily builds its
 * ring. A change takes effect for rings created afterwards — threads that
 * already hold a ring keep their depth (they are not torn down mid-session). */
static unsigned g_ring_depth = RING_ENTRIES;

void uring_set_depth(unsigned depth)
{
    if (depth == 0)
        return; /* unset ⇒ keep default */
    if (depth < RING_MIN)
        depth = RING_MIN;
    if (depth > RING_MAX)
        depth = RING_MAX;
    unsigned prev = __atomic_exchange_n(&g_ring_depth, depth, __ATOMIC_RELAXED);
    if (prev != depth)
        LOGI("io_uring ring depth set to %u (statx_batch)", depth);
}

struct ring {
    int      fd;
    unsigned sq_entries;
    /* SQ ring */
    unsigned *sq_tail, *sq_mask, *sq_array;
    struct io_uring_sqe *sqes;
    /* CQ ring */
    unsigned *cq_head, *cq_tail, *cq_mask;
    struct io_uring_cqe *cqes;
    void  *sq_ptr, *cq_ptr, *sqe_ptr;
    size_t sq_len, cq_len, sqe_len;
};

static int sys_setup(unsigned entries, struct io_uring_params *p)
{
    return (int)syscall(SYS_io_uring_setup, entries, p);
}

static int sys_enter(int fd, unsigned to_submit, unsigned min_complete)
{
    return (int)syscall(SYS_io_uring_enter, fd, to_submit, min_complete,
                        IORING_ENTER_GETEVENTS, NULL, 0);
}

static void ring_destroy(struct ring *r)
{
    if (r->sqe_ptr && r->sqe_ptr != MAP_FAILED)
        munmap(r->sqe_ptr, r->sqe_len);
    if (r->cq_ptr && r->cq_ptr != MAP_FAILED && r->cq_ptr != r->sq_ptr)
        munmap(r->cq_ptr, r->cq_len);
    if (r->sq_ptr && r->sq_ptr != MAP_FAILED)
        munmap(r->sq_ptr, r->sq_len);
    if (r->fd >= 0)
        close(r->fd);
    memset(r, 0, sizeof *r);
    r->fd = -1;
}

static int ring_init(struct ring *r)
{
    memset(r, 0, sizeof *r);
    struct io_uring_params p;
    memset(&p, 0, sizeof p);
    unsigned depth = __atomic_load_n(&g_ring_depth, __ATOMIC_RELAXED);
    r->fd = sys_setup(depth, &p);
    if (r->fd < 0)
        return -1;

    r->sq_entries = p.sq_entries;
    r->sq_len = p.sq_off.array + p.sq_entries * sizeof(unsigned);
    r->cq_len = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);
    bool single = p.features & IORING_FEAT_SINGLE_MMAP;
    if (single && r->cq_len > r->sq_len)
        r->sq_len = r->cq_len;

    r->sq_ptr = mmap(NULL, r->sq_len, PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_POPULATE, r->fd, IORING_OFF_SQ_RING);
    if (r->sq_ptr == MAP_FAILED)
        goto fail;
    r->cq_ptr = single ? r->sq_ptr
                       : mmap(NULL, r->cq_len, PROT_READ | PROT_WRITE,
                              MAP_SHARED | MAP_POPULATE, r->fd, IORING_OFF_CQ_RING);
    if (r->cq_ptr == MAP_FAILED)
        goto fail;
    r->sqe_len = p.sq_entries * sizeof(struct io_uring_sqe);
    r->sqe_ptr = mmap(NULL, r->sqe_len, PROT_READ | PROT_WRITE,
                      MAP_SHARED | MAP_POPULATE, r->fd, IORING_OFF_SQES);
    if (r->sqe_ptr == MAP_FAILED)
        goto fail;

    uint8_t *sq = r->sq_ptr, *cq = r->cq_ptr;
    r->sq_tail = (unsigned *)(sq + p.sq_off.tail);
    r->sq_mask = (unsigned *)(sq + p.sq_off.ring_mask);
    r->sq_array = (unsigned *)(sq + p.sq_off.array);
    r->sqes = r->sqe_ptr;
    r->cq_head = (unsigned *)(cq + p.cq_off.head);
    r->cq_tail = (unsigned *)(cq + p.cq_off.tail);
    r->cq_mask = (unsigned *)(cq + p.cq_off.ring_mask);
    r->cqes = (struct io_uring_cqe *)(cq + p.cq_off.cqes);
    return 0;
fail:
    ring_destroy(r);
    return -1;
}

static void estat_from_statx(struct estat *e, const struct statx *sx)
{
    e->mode = sx->stx_mode;
    e->uid = sx->stx_uid;
    e->gid = sx->stx_gid;
    e->nlink = sx->stx_nlink;
    e->ino = sx->stx_ino;
    e->size = sx->stx_size;
    e->blocks = sx->stx_blocks;
    e->atim.tv_sec = sx->stx_atime.tv_sec;
    e->atim.tv_nsec = sx->stx_atime.tv_nsec;
    e->mtim.tv_sec = sx->stx_mtime.tv_sec;
    e->mtim.tv_nsec = sx->stx_mtime.tv_nsec;
    e->rdev_major = sx->stx_rdev_major;
    e->rdev_minor = sx->stx_rdev_minor;
}

void estat_of(struct estat *e, const struct stat *st)
{
    e->mode = st->st_mode;
    e->uid = st->st_uid;
    e->gid = st->st_gid;
    e->nlink = (uint32_t)st->st_nlink;
    e->ino = (uint64_t)st->st_ino;
    e->size = (uint64_t)st->st_size;
    e->blocks = (uint64_t)st->st_blocks;
    e->atim = st->st_atim;
    e->mtim = st->st_mtim;
    e->rdev_major = major(st->st_rdev);
    e->rdev_minor = minor(st->st_rdev);
}

/* One statx batch submission + full reap on the given ring. */
static int ring_statx(struct ring *r, struct statx_req *reqs,
                      struct statx *bufs, size_t n)
{
    size_t done = 0;
    while (done < n) {
        unsigned batch = (unsigned)(n - done);
        if (batch > r->sq_entries)
            batch = r->sq_entries;

        unsigned tail = *r->sq_tail;
        for (unsigned k = 0; k < batch; k++) {
            unsigned idx = (tail + k) & *r->sq_mask;
            struct io_uring_sqe *sqe = &r->sqes[idx];
            memset(sqe, 0, sizeof *sqe);
            sqe->opcode = IORING_OP_STATX;
            sqe->fd = reqs[done + k].dirfd;
            sqe->addr = (uint64_t)(uintptr_t)reqs[done + k].name;
            sqe->len = STATX_BASIC_STATS;
            sqe->statx_flags = AT_SYMLINK_NOFOLLOW | AT_STATX_DONT_SYNC;
            sqe->off = (uint64_t)(uintptr_t)&bufs[k];
            sqe->user_data = k;
            r->sq_array[idx] = idx;
        }
        __atomic_store_n(r->sq_tail, tail + batch, __ATOMIC_RELEASE);

        int rc;
        do {
            rc = sys_enter(r->fd, batch, batch);
        } while (rc < 0 && errno == EINTR);
        if (rc < 0)
            return -1;

        unsigned head = *r->cq_head, reaped = 0;
        while (reaped < batch) {
            unsigned ctail = __atomic_load_n(r->cq_tail, __ATOMIC_ACQUIRE);
            while (head != ctail && reaped < batch) {
                const struct io_uring_cqe *cqe = &r->cqes[head & *r->cq_mask];
                struct statx_req *rq = &reqs[done + cqe->user_data];
                if (cqe->res == 0) {
                    rq->res = 0;
                    estat_from_statx(rq->out, &bufs[cqe->user_data]);
                } else {
                    rq->res = cqe->res; /* -errno */
                }
                head++;
                reaped++;
            }
            __atomic_store_n(r->cq_head, head, __ATOMIC_RELEASE);
            if (reaped < batch) {
                do {
                    rc = sys_enter(r->fd, 0, batch - reaped);
                } while (rc < 0 && errno == EINTR);
                if (rc < 0)
                    return -1;
            }
        }
        done += batch;
    }
    return 0;
}

static void stat_serial(struct statx_req *reqs, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        struct stat st;
        if (fstatat(reqs[i].dirfd, reqs[i].name, &st, AT_SYMLINK_NOFOLLOW) == 0) {
            reqs[i].res = 0;
            estat_of(reqs[i].out, &st);
        } else {
            reqs[i].res = -errno;
        }
    }
}

void uring_probe(bool allow)
{
    g_uring_enabled = false;
    if (!allow) {
        LOGI("io_uring disabled by flag; using serial fstatat");
        return;
    }
    struct ring r;
    if (ring_init(&r) < 0) {
        LOGW("io_uring unavailable (%s); falling back to serial fstatat",
             strerror(errno));
        return;
    }
    struct estat e;
    struct statx buf;
    struct statx_req probe = { .dirfd = AT_FDCWD, .name = ".", .out = &e };
    int rc = ring_statx(&r, &probe, &buf, 1);
    ring_destroy(&r);
    if (rc < 0 || probe.res != 0) {
        LOGW("IORING_OP_STATX unsupported (res=%d); falling back to serial fstatat",
             probe.res);
        return;
    }
    g_uring_enabled = true;
    LOGI("io_uring statx batching enabled (default ring depth %u)",
         __atomic_load_n(&g_ring_depth, __ATOMIC_RELAXED));
}

/* thread-local ring, lazily created per walker thread */
static __thread struct ring tls_ring;
static __thread int tls_ring_state; /* 0 unset, 1 ok, -1 failed */

void stat_batch(struct statx_req *reqs, size_t n)
{
    if (!n)
        return;
    for (size_t i = 0; i < n; i++)
        reqs[i].res = -EIO; /* every request gets a definite outcome */

    if (g_uring_enabled && tls_ring_state == 0)
        tls_ring_state = ring_init(&tls_ring) == 0 ? 1 : -1;

    if (g_uring_enabled && tls_ring_state == 1) {
        /* One completion buffer per ring slot; sized to this thread's ring,
         * whose depth was fixed by g_ring_depth at ring_init time. */
        static __thread struct statx *bufs;
        if (!bufs)
            bufs = calloc(tls_ring.sq_entries, sizeof *bufs);
        if (bufs && ring_statx(&tls_ring, reqs, bufs, n) == 0)
            return;
        LOGW("io_uring statx batch failed (%s); this thread now uses fstatat",
             strerror(errno));
        tls_ring_state = -1;
    }
    stat_serial(reqs, n);
}
