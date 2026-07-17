/* Cross-host chunk executor (docs/DESIGN-agent.md §2.3, coordinator §4.1).
 *
 * A file big enough to fan out is copied not on the one agent that walked it but
 * as a set of ChunkTasks the coordinator hands to different hosts. Each data
 * chunk copies its byte range into one coordinator-named temp in the file's
 * destination directory; the terminal finalize task fsyncs, applies metadata
 * and renames the temp into place once every range has landed.
 *
 * Every chunk opens the temp with O_CREAT rather than trusting the create_temp
 * chunk to land first: chunks run concurrently on different hosts, and a
 * non-creator must not fail a race to the shared temp. create_temp additionally
 * preallocates, so the ranges write into contiguous, already-reserved space. */
#include "agent.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define CHUNK_BUF (1 << 20)

static int64_t stat_mtime_ns(const struct stat *st)
{
    return (int64_t)st->st_mtim.tv_sec * 1000000000 + st->st_mtim.tv_nsec;
}

/* Splits rel into its directory (copied into dir, "" for a root-level file) and
 * returns a pointer to the basename within rel. */
static const char *split_dir(const char *rel, char *dir, size_t cap)
{
    const char *slash = strrchr(rel, '/');
    if (!slash) {
        dir[0] = '\0';
        return rel;
    }
    size_t n = (size_t)(slash - rel);
    if (n >= cap)
        n = cap - 1;
    memcpy(dir, rel, n);
    dir[n] = '\0';
    return slash + 1;
}

/* Copies [off, off+len) from in to out at the same offset. Returns 0 on
 * success, -1 on error (errno set), -2 if the source was shorter than expected
 * (a mid-flight shrink → treat as source drift). */
static int copy_range(const struct job_options *o, int in, int out,
                      uint64_t off, uint64_t len)
{
    uint64_t done = 0;

    if (o->server_side_copy != SSC_OFF) {
        loff_t io = (loff_t)off, oo = (loff_t)off;
        while (done < len) {
            ssize_t w = copy_file_range(in, &io, out, &oo, (size_t)(len - done), 0);
            if (w > 0) {
                done += (uint64_t)w;
                continue;
            }
            if (w == 0)
                return -2; /* src shorter than its own stat said */
            if (done == 0 && (errno == EXDEV || errno == EINVAL || errno == ENOSYS ||
                              errno == EOPNOTSUPP || errno == EBADF)) {
                if (o->server_side_copy == SSC_REQUIRE)
                    return -1;
                break; /* mount pair can't offload: byte-copy path */
            }
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (done == len)
            return 0;
    }

    uint8_t *buf = malloc(CHUNK_BUF);
    if (!buf) {
        errno = ENOMEM;
        return -1;
    }
    uint64_t pos = off + done, end = off + len;
    int rc = 0;
    while (pos < end) {
        size_t want = (size_t)(end - pos) < CHUNK_BUF ? (size_t)(end - pos) : CHUNK_BUF;
        ssize_t r = pread(in, buf, want, (off_t)pos);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            rc = -1;
            break;
        }
        if (r == 0) {
            rc = -2;
            break;
        }
        for (ssize_t wo = 0; wo < r;) {
            ssize_t w = pwrite(out, buf + wo, (size_t)(r - wo), (off_t)(pos + (uint64_t)wo));
            if (w < 0) {
                if (errno == EINTR)
                    continue;
                rc = -1;
                goto done_copy;
            }
            wo += w;
        }
        pos += (uint64_t)r;
    }
done_copy:
    free(buf);
    return rc;
}

void process_chunk(const struct shard_item *it)
{
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    struct walk_ctx ctx = { .it = it };
    jrn_init(&ctx);
    int status = RES_OK;
    ctx.oe = opts_get(it->job_id);
    if (!ctx.oe) {
        snprintf(ctx.err, sizeof ctx.err, "no cached options for job %llu",
                 (unsigned long long)it->job_id);
        status = RES_TRANSIENT;
        goto out;
    }
    const struct job_options *o = &ctx.oe->o;
    const struct chunk_info *ch = &it->chunk;
    const char *rel = it->rel_path ? it->rel_path : "";

    char dir[PATH_MAX];
    const char *base = split_dir(rel, dir, sizeof dir);

    int in = open_beneath(ctx.oe->src_fd, rel, O_RDONLY);
    if (in < 0) {
        if (errno == ENOENT)
            status = RES_SRC_CHANGED; /* source vanished; re-diff next pass */
        else {
            walk_err(&ctx, "open src", rel);
            status = RES_ERROR;
        }
        goto out;
    }

    /* Gen check: the file the coordinator planned chunks for must still be the
     * file on disk. A mismatch aborts the whole group (design §3 step 5). */
    struct stat st;
    if (fstat(in, &st) < 0) {
        walk_err(&ctx, "stat src", rel);
        status = RES_ERROR;
        goto close_in;
    }
    if ((uint64_t)st.st_size != ch->gen_size || stat_mtime_ns(&st) != ch->gen_mtime_ns) {
        jrn_emit(&ctx, JR_SRC_CHANGED, rel, NULL, NULL, 0, NULL);
        status = RES_SRC_CHANGED;
        goto close_in;
    }

    int dfd = dst_dir_open(ctx.oe->dst_fd, dir);
    if (dfd < 0) {
        walk_err(&ctx, "open dst dir", dir[0] ? dir : "<root>");
        status = RES_ERROR;
        goto close_in;
    }

    if (ch->finalize) {
        struct estat ss;
        estat_of(&ss, &st);
        int out = openat(dfd, ch->temp_name, O_WRONLY | O_CLOEXEC);
        if (out < 0) {
            /* Temp gone. A prior finalize of this shard already renamed it into
             * place; its result was lost (network/crash), so the shard was
             * re-granted. Idempotent when the final file is present and matches
             * the gen — re-accounting the copy so the pass counters land even
             * though this run only confirmed the earlier one. Anything else is
             * a genuine error. */
            struct stat fst;
            if (errno == ENOENT &&
                fstatat(dfd, base, &fst, AT_SYMLINK_NOFOLLOW) == 0 &&
                (uint64_t)fst.st_size == ch->gen_size &&
                stat_mtime_ns(&fst) == ch->gen_mtime_ns) {
                goto finalized;
            }
            walk_err(&ctx, "open temp for finalize", ch->temp_name);
            status = RES_ERROR;
            goto close_dfd;
        }
        /* metadata order (design §5): xattrs/ACLs (need src fd) → fsync →
         * owner/mode → times, then rename. */
        xattr_copy_fd(&ctx, in, out, base);
        if (o->fsync_per_file && fdatasync(out) < 0) {
            walk_err(&ctx, "fsync", rel);
            status = RES_ERROR;
            close(out);
            goto close_dfd;
        }
        apply_meta(&ctx, out, &ss, base);
        if (renameat(dfd, ch->temp_name, dfd, base) < 0) {
            walk_err(&ctx, "rename", rel);
            status = RES_ERROR;
            close(out);
            goto close_dfd;
        }
        close(out);
    finalized:
        /* One accounting per file, at finalize: the data chunks report nothing,
         * so a pass that copied only via chunks still shows a nonzero delta and
         * does not falsely converge. (A re-delivered result is dropped by the
         * coordinator on the lease-id mismatch, so this never double-counts.) */
        CTR_ADD(ctx.c.files_copied, 1);
        CTR_ADD(ctx.c.bytes_copied, ch->gen_size);
        atomic_fetch_add(&g_stat_files_copied, 1);
        atomic_fetch_add(&g_stat_bytes_copied, ch->gen_size);
        jrn_emit(&ctx, JR_COPIED, rel, &ss, NULL, 0, NULL);
    } else {
        int out = openat(dfd, ch->temp_name, O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
        if (out < 0) {
            walk_err(&ctx, "open temp", ch->temp_name);
            status = RES_ERROR;
            goto close_dfd;
        }
        if (ch->create_temp && fallocate(out, 0, 0, (off_t)ch->gen_size) < 0 &&
            errno != EOPNOTSUPP && errno != ENOSYS) {
            walk_err(&ctx, "fallocate temp", ch->temp_name);
            status = RES_ERROR;
            close(out);
            goto close_dfd;
        }
        int rc = copy_range(o, in, out, ch->offset, ch->length);
        close(out);
        if (rc == -2) {
            jrn_emit(&ctx, JR_SRC_CHANGED, rel, NULL, NULL, 0, NULL);
            status = RES_SRC_CHANGED;
        } else if (rc < 0) {
            walk_err(&ctx, "chunk copy", rel);
            status = RES_ERROR;
        }
    }

close_dfd:
    close(dfd);
close_in:
    close(in);
out:
    jrn_flush(&ctx);
    if (!jrn_wait_acked(&ctx)) { /* ordering invariant: result after journals */
        snprintf(ctx.err, sizeof ctx.err, "journal ack timeout");
        status = RES_TRANSIENT;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    ctx.c.wall_ms = (uint64_t)((t1.tv_sec - t0.tv_sec) * 1000 +
                               (t1.tv_nsec - t0.tv_nsec) / 1000000);
    pb_buf b;
    pb_init(&b);
    enc_shard_result(&b, it->shard_id, it->lease_id, status, &ctx.c,
                     ctx.err[0] ? ctx.err : NULL);
    out_push(FR_SHARD_RESULT, &b);
    lease_remove(it->lease_id);
    jrn_destroy(&ctx);
}
