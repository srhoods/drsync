/* io_uring registered-buffer copy engine (docs/DESIGN-agent.md §3).
 *
 * The byte-copy fallback (when copy_file_range is unavailable — cross-device or
 * a mount pair without server-side copy/reflink) otherwise serialises read and
 * write: read a block (wait), write it (wait), repeat, so the source read and
 * the destination write never overlap. On a migration the two are on different
 * mounts, so overlapping them roughly halves wall time.
 *
 * This engine keeps a depth-2 ping-pong over two registered ("fixed") buffers:
 * while the current block is being written, the next is being read into the
 * other buffer, both submitted to io_uring at once. Registered buffers skip the
 * per-op page-pin cost. The inline xxh3 hash (design §3 — folded into the copy
 * for free) is preserved: each block is hashed in stream order, which the
 * ping-pong guarantees, via a sink callback so this file stays I/O-only.
 *
 * One ring per copy thread (thread-local), built lazily on first use and
 * self-tested with a READ_FIXED from a memfd. If io_uring is unavailable or the
 * ring cannot be built, ucopy_available() returns false and the caller uses the
 * serial loop. Raw ring plumbing is kept self-contained here rather than shared
 * with the statx ring (uring.c), which has a different depth and no buffers. */
#include "agent.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

#include <linux/io_uring.h>

#define UCOPY_BUF_SIZE (1 << 20) /* one block; matches copy.c COPY_BUF_SIZE */
#define UCOPY_NBUF     2         /* ping-pong: read(next) overlaps write(cur) */
#define UCOPY_DEPTH    4         /* SQ/CQ entries; a read + a write in flight */

#define UD_WRITE 1
#define UD_READ  2

struct ucopy_ring {
    int      fd;
    unsigned sq_entries;
    unsigned *sq_tail, *sq_mask, *sq_array;
    struct io_uring_sqe *sqes;
    unsigned *cq_head, *cq_tail, *cq_mask;
    struct io_uring_cqe *cqes;
    void    *sq_ptr, *cq_ptr, *sqe_ptr;
    size_t   sq_len, cq_len, sqe_len;
    uint8_t *buf[UCOPY_NBUF]; /* registered fixed buffers */
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

static int sys_register(int fd, unsigned op, const void *arg, unsigned nr)
{
    return (int)syscall(SYS_io_uring_register, fd, op, arg, nr);
}

static void ring_free(struct ucopy_ring *r)
{
    for (int i = 0; i < UCOPY_NBUF; i++)
        free(r->buf[i]);
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

static int ring_map(struct ucopy_ring *r)
{
    memset(r, 0, sizeof *r);
    r->fd = -1;
    struct io_uring_params p;
    memset(&p, 0, sizeof p);
    r->fd = sys_setup(UCOPY_DEPTH, &p);
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
    ring_free(r);
    return -1;
}

/* Register the two fixed buffers with the kernel (page-aligned so the pin is
 * cheap). They live for the ring's lifetime. */
static int ring_register_bufs(struct ucopy_ring *r)
{
    struct iovec iov[UCOPY_NBUF];
    for (int i = 0; i < UCOPY_NBUF; i++) {
        r->buf[i] = aligned_alloc(4096, UCOPY_BUF_SIZE);
        if (!r->buf[i])
            return -1;
        iov[i].iov_base = r->buf[i];
        iov[i].iov_len = UCOPY_BUF_SIZE;
    }
    return sys_register(r->fd, IORING_REGISTER_BUFFERS, iov, UCOPY_NBUF);
}

/* Queue one READ_FIXED/WRITE_FIXED into the SQ (does not submit). */
static void prep_rw(struct ucopy_ring *r, int op, int fd, int bufi,
                    uint64_t off, unsigned len, uint64_t user_data)
{
    unsigned tail = *r->sq_tail;
    unsigned idx = tail & *r->sq_mask;
    struct io_uring_sqe *sqe = &r->sqes[idx];
    memset(sqe, 0, sizeof *sqe);
    sqe->opcode = (uint8_t)op;
    sqe->fd = fd;
    sqe->addr = (uint64_t)(uintptr_t)r->buf[bufi];
    sqe->len = len;
    sqe->off = off;
    sqe->buf_index = (uint16_t)bufi;
    sqe->user_data = user_data;
    r->sq_array[idx] = idx;
    __atomic_store_n(r->sq_tail, tail + 1, __ATOMIC_RELEASE);
}

/* Submit the nsub queued ops and reap exactly nsub completions, routing each
 * result to the write- or read-result out-param by user_data. Returns 0, or -1
 * on io_uring_enter error (errno set). Unreaped slots keep their sentinel. */
static int submit_reap(struct ucopy_ring *r, unsigned nsub,
                       ssize_t *wres, ssize_t *rres)
{
    int rc;
    do {
        rc = sys_enter(r->fd, nsub, nsub);
    } while (rc < 0 && errno == EINTR);
    if (rc < 0)
        return -1;

    unsigned head = *r->cq_head, reaped = 0;
    while (reaped < nsub) {
        unsigned ctail = __atomic_load_n(r->cq_tail, __ATOMIC_ACQUIRE);
        while (head != ctail && reaped < nsub) {
            const struct io_uring_cqe *cqe = &r->cqes[head & *r->cq_mask];
            if (cqe->user_data == UD_WRITE)
                *wres = cqe->res;
            else
                *rres = cqe->res;
            head++;
            reaped++;
        }
        __atomic_store_n(r->cq_head, head, __ATOMIC_RELEASE);
        if (reaped < nsub) {
            do {
                rc = sys_enter(r->fd, 0, nsub - reaped);
            } while (rc < 0 && errno == EINTR);
            if (rc < 0)
                return -1;
        }
    }
    return 0;
}

/* Finish a short write synchronously (rare for regular files). */
static int pwrite_all(int fd, const uint8_t *p, size_t n, uint64_t off)
{
    while (n) {
        ssize_t w = pwrite(fd, p, n, (off_t)off);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        p += w;
        off += (uint64_t)w;
        n -= (size_t)w;
    }
    return 0;
}

/* One synchronous READ_FIXED into buf[bufi]; returns bytes or -1 (errno). */
static ssize_t one_read(struct ucopy_ring *r, int fd, int bufi, uint64_t off,
                        unsigned len)
{
    prep_rw(r, IORING_OP_READ_FIXED, fd, bufi, off, len, UD_READ);
    ssize_t wres = 0, rres = -EIO;
    if (submit_reap(r, 1, &wres, &rres) < 0)
        return -1;
    if (rres < 0) {
        errno = (int)-rres;
        return -1;
    }
    return rres;
}

static __thread struct ucopy_ring tls_ring;
static __thread int tls_state; /* 0 unset, 1 ok, -1 failed */

static int ucopy_init(struct ucopy_ring *r)
{
    if (ring_map(r) < 0)
        return -1;
    if (ring_register_bufs(r) < 0) {
        ring_free(r);
        return -1;
    }
    /* Self-test READ_FIXED against a 4 KiB memfd so a copy never discovers
     * mid-file that the op is unsupported (then it would be a hard error, not a
     * fallback). memfd has plain regular-file semantics. */
    int mfd = (int)syscall(SYS_memfd_create, "ucopy-probe", 0u);
    if (mfd < 0 || ftruncate(mfd, 4096) < 0) {
        if (mfd >= 0)
            close(mfd);
        ring_free(r);
        return -1;
    }
    ssize_t n = one_read(r, mfd, 0, 0, 4096);
    close(mfd);
    if (n != 4096) {
        ring_free(r);
        return -1;
    }
    LOGI("io_uring copy engine enabled (per-thread ring, %d x %d KiB fixed buffers)",
         UCOPY_NBUF, UCOPY_BUF_SIZE >> 10);
    return 0;
}

bool ucopy_available(void)
{
    if (!g_uring_enabled)
        return false;
    if (tls_state == 0)
        tls_state = ucopy_init(&tls_ring) == 0 ? 1 : -1;
    return tls_state == 1;
}

/* Permanently stop this thread from using the io_uring copy engine. Called when
 * a copy is found to have produced a wrong-sized destination — the READ_FIXED
 * self-test cannot detect a filesystem whose WRITE_FIXED ignores the submitted
 * length (observed on GPFS: it flushes the whole registered buffer), so the
 * only reliable signal is a bad result at copy time. The caller then falls back
 * to the serial byte copy. */
void ucopy_disable(void)
{
    tls_state = -1;
}

int64_t ucopy_run(int in, int out, uint64_t size, ucopy_sink sink, void *sink_arg)
{
    struct ucopy_ring *r = &tls_ring;
    if (size == 0)
        return 0;

    uint64_t roff = 0, woff = 0; /* next source byte to read / dest byte to write */
    int cur = 0;
    unsigned rn = size - roff < UCOPY_BUF_SIZE ? (unsigned)(size - roff) : UCOPY_BUF_SIZE;
    ssize_t clen = one_read(r, in, cur, roff, rn);
    if (clen < 0)
        return -errno;
    if (clen == 0)
        return 0; /* source shrank to empty; caller's gen check re-diffs it */
    roff += (uint64_t)clen;

    while (clen > 0) {
        int nxt = 1 - cur;
        bool more = roff < size;
        unsigned next_rn = more ? (size - roff < UCOPY_BUF_SIZE
                                       ? (unsigned)(size - roff)
                                       : UCOPY_BUF_SIZE)
                                : 0;

        /* Submit write of the current block and, if any remains, the read of the
         * next block into the other buffer — they overlap. */
        prep_rw(r, IORING_OP_WRITE_FIXED, out, cur, woff, (unsigned)clen, UD_WRITE);
        if (more)
            prep_rw(r, IORING_OP_READ_FIXED, in, nxt, roff, next_rn, UD_READ);

        /* Hash the current block in stream order; the buffer is only read (by the
         * in-flight write and this hash), never written, until it is reused. */
        if (sink)
            sink(sink_arg, r->buf[cur], (size_t)clen);

        ssize_t wres = -EIO, rres = -EIO;
        if (submit_reap(r, more ? 2u : 1u, &wres, &rres) < 0)
            return -errno;

        if (wres < 0)
            return wres; /* -errno */
        if ((uint64_t)wres < (uint64_t)clen &&
            pwrite_all(out, r->buf[cur] + wres, (size_t)clen - (size_t)wres,
                       woff + (uint64_t)wres) < 0)
            return -errno;
        woff += (uint64_t)clen;

        if (more) {
            if (rres < 0)
                return rres;
            cur = nxt;
            clen = rres;
            roff += (uint64_t)rres;
        } else {
            clen = 0;
        }
    }
    return (int64_t)woff;
}
