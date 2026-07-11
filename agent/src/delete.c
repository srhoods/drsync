/* Delete-pass executor (decision D5, docs/DESIGN-coordinator.md §2.2).
 * Input: a WI_DELETE item listing destination orphan paths harvested from the
 * previous pass's ORPHAN journal records. Orphan directories were never
 * descended into during the scan, so removal is recursive here — always
 * fd-anchored beneath the destination root (openat2 semantics via the same
 * open_beneath discipline as the walker), never by absolute path.
 * Every removed object is journaled JR_DELETED; dry-run jobs journal
 * JR_WOULD_DELETE and remove nothing. */
#include "agent.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* depth-first removal of name inside parentfd; returns removed count */
static uint64_t rm_tree(struct walk_ctx *ctx, int parentfd, const char *name,
                        const char *rel)
{
    struct stat st;
    if (fstatat(parentfd, name, &st, AT_SYMLINK_NOFOLLOW) < 0) {
        if (errno != ENOENT) /* already gone is success */
            walk_err(ctx, "stat for delete", rel);
        return 0;
    }
    uint64_t removed = 0;
    if (S_ISDIR(st.st_mode)) {
        int fd = openat(parentfd, name,
                        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (fd < 0) {
            walk_err(ctx, "open for delete", rel);
            return 0;
        }
        DIR *d = fdopendir(fd);
        if (!d) {
            close(fd);
            walk_err(ctx, "fdopendir for delete", rel);
            return 0;
        }
        struct dirent *de;
        while ((de = readdir(d))) {
            if (de->d_name[0] == '.' &&
                (de->d_name[1] == '\0' ||
                 (de->d_name[1] == '.' && de->d_name[2] == '\0')))
                continue;
            char crel[PATH_MAX];
            snprintf(crel, sizeof crel, "%s/%s", rel, de->d_name);
            removed += rm_tree(ctx, dirfd(d), de->d_name, crel);
        }
        closedir(d);
        if (unlinkat(parentfd, name, AT_REMOVEDIR) < 0 && errno != ENOENT) {
            walk_err(ctx, "rmdir", rel);
            return removed;
        }
    } else {
        if (unlinkat(parentfd, name, 0) < 0 && errno != ENOENT) {
            walk_err(ctx, "unlink", rel);
            return removed;
        }
    }
    jrn_emit(ctx, JR_DELETED, rel, NULL, NULL, 0, NULL);
    return removed + 1;
}

/* split rel into (parent dir fd under root, leaf name); -1 on failure */
int open_parent_beneath(int root_fd, const char *rel, const char **leaf)
{
    const char *slash = strrchr(rel, '/');
    if (!slash) {
        *leaf = rel;
        return dup(root_fd);
    }
    char parent[PATH_MAX];
    size_t n = (size_t)(slash - rel);
    if (n >= sizeof parent) {
        errno = ENAMETOOLONG;
        return -1;
    }
    memcpy(parent, rel, n);
    parent[n] = '\0';
    *leaf = slash + 1;
    /* component-wise O_NOFOLLOW walk, same guarantee as the walker */
    int cur = dup(root_fd);
    char *save = NULL;
    for (char *comp = strtok_r(parent, "/", &save); comp;
         comp = strtok_r(NULL, "/", &save)) {
        int next = openat(cur, comp,
                          O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        close(cur);
        if (next < 0)
            return -1;
        cur = next;
    }
    return cur;
}

void process_delete(const struct shard_item *it)
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

    for (size_t i = 0; i < it->n_paths; i++) {
        const char *rel = it->paths[i];
        if (!rel[0] || strstr(rel, "..")) { /* defense in depth */
            walk_err(&ctx, "refusing suspicious delete path", rel);
            continue;
        }
        if (ctx.oe->o.dry_run) {
            jrn_emit(&ctx, JR_WOULD_DELETE, rel, NULL, NULL, 0, NULL);
            CTR_ADD(ctx.c.orphans, 1);
            continue;
        }
        const char *leaf;
        int pfd = open_parent_beneath(ctx.oe->dst_fd, rel, &leaf);
        if (pfd < 0) {
            if (errno != ENOENT) /* parent gone = orphan already gone */
                walk_err(&ctx, "open parent for delete", rel);
            continue;
        }
        /* counters: a delete pass reports removals in the orphans column */
        CTR_ADD(ctx.c.orphans, rm_tree(&ctx, pfd, leaf, rel));
        close(pfd);
    }

    jrn_flush(&ctx);
    if (!jrn_wait_acked(&ctx)) {
        snprintf(ctx.err, sizeof ctx.err, "journal ack timeout");
        status = RES_TRANSIENT;
    }
out:
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
