/* Mount probe (docs/DESIGN-protocol.md §3.1 ProbeTask).
 *
 * At pass start the coordinator pins one probe shard to each agent and withholds
 * the root walk shard until every probe reports OK. The probe verifies this
 * agent's source and destination roots are present and are directories, so a
 * missing or misordered mount on ANY host is caught before bulk work runs —
 * not just on whichever agent happened to grab the root shard. The result is a
 * plain ShardResult (RESULT_OK / RESULT_ERROR), the same channel the chunk
 * executor uses; a probe walks nothing and emits no journal records.
 *
 * A directory existing is not proof the volume is mounted: an unmounted network
 * or parallel filesystem leaves its mount point behind as an empty stub on the
 * local root filesystem, and syncing into/out of that stub is silent data loss.
 * When require_mount is set (job spec probe.require_mount, default true) the
 * probe also confirms each root sits on a real mounted filesystem by finding the
 * longest mount point in /proc/self/mountinfo that covers the root: if the only
 * thing covering it is "/", nothing is mounted there and the root is a stub. */
#include "agent.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* Decode mountinfo's octal escapes (\040 space, \011 tab, \012 newline,
 * \134 backslash) in place. Field 5 (the mount point) is the only field we
 * read and the only one the kernel escapes this way. */
static void unescape_octal(char *s)
{
    char *w = s;
    for (const char *r = s; *r;) {
        if (r[0] == '\\' && r[1] >= '0' && r[1] <= '3' && r[2] >= '0' &&
            r[2] <= '7' && r[3] >= '0' && r[3] <= '7') {
            *w++ = (char)(((r[1] - '0') << 6) | ((r[2] - '0') << 3) |
                          (r[3] - '0'));
            r += 4;
        } else {
            *w++ = *r++;
        }
    }
    *w = '\0';
}

/* True if mount point mp is an ancestor of (or equal to) canonical path p.
 * "/" covers every absolute path; otherwise p must equal mp or begin with
 * mp followed by a '/', so /mnt/a does not read as covering /mnt/ab. */
static bool mount_covers(const char *mp, const char *p)
{
    if (mp[0] == '/' && mp[1] == '\0')
        return true;
    size_t n = strlen(mp);
    if (strncmp(p, mp, n) != 0)
        return false;
    return p[n] == '\0' || p[n] == '/';
}

/* Find the longest mount point covering path, writing it to mp_out. Returns
 * false only if /proc/self/mountinfo can't be read (then we can't judge, and
 * the caller treats the mount as live rather than block on a proc quirk). */
static bool covering_mount(const char *path, char *mp_out, size_t cap)
{
    char canon[PATH_MAX];
    const char *p = realpath(path, canon) ? canon : path;

    FILE *f = fopen("/proc/self/mountinfo", "re");
    if (!f)
        return false;

    mp_out[0] = '\0';
    size_t best = 0;
    char *line = NULL;
    size_t linecap = 0;
    ssize_t len;
    while ((len = getline(&line, &linecap, f)) > 0) {
        /* Fields are space-separated; the mount point is the 5th. */
        char *tok = line, *mp = NULL;
        for (int field = 1; field <= 5; field++) {
            tok += strspn(tok, " ");
            char *end = tok + strcspn(tok, " \n");
            if (*end == '\0' && field < 5) /* short/truncated line */
                break;
            if (field == 5) {
                *end = '\0';
                mp = tok;
                break;
            }
            *end = '\0';
            tok = end + 1;
        }
        if (!mp)
            continue;
        unescape_octal(mp);
        size_t n = strlen(mp);
        if (n >= cap)
            continue;
        if (mount_covers(mp, p) && (n > best || mp_out[0] == '\0')) {
            memcpy(mp_out, mp, n + 1);
            best = n;
        }
    }
    free(line);
    fclose(f);
    return true;
}

/* Report whether root is on a live mounted filesystem. On a false return err is
 * filled with a diagnostic. label is "source"/"destination" for the message. */
static bool root_is_live_mount(const char *root, const char *label, char *err,
                               size_t errcap)
{
    char mp[PATH_MAX];
    if (!covering_mount(root, mp, sizeof mp))
        return true; /* couldn't read mountinfo — don't false-park the pass */
    if (mp[0] == '/' && mp[1] == '\0') {
        snprintf(err, errcap,
                 "%s root is not on a mounted filesystem (covered only by \"/\", "
                 "looks like an unmounted volume's stub): %s",
                 label, root);
        return false;
    }
    return true;
}

void process_probe(const struct shard_item *it)
{
    int status = RES_OK;
    char err[PATH_MAX + 224] = ""; /* holds a root path plus a short prefix */

    /* opts_store opened src/dst roots when it cached the job options; if the
     * source mount was missing it failed and never cached them, so a NULL entry
     * is itself a mount fault. */
    const struct opts_entry *oe = opts_get(it->job_id);
    struct stat st;
    if (!oe) {
        status = RES_ERROR;
        snprintf(err, sizeof err,
                 "job %llu options unavailable (source mount missing?)",
                 (unsigned long long)it->job_id);
    } else if (fstat(oe->src_fd, &st) < 0 || !S_ISDIR(st.st_mode)) {
        status = RES_ERROR;
        snprintf(err, sizeof err, "source root is not a directory: %s",
                 oe->o.src_root);
    } else if (!oe->o.dry_run &&
               (oe->dst_fd < 0 || fstat(oe->dst_fd, &st) < 0 ||
                !S_ISDIR(st.st_mode))) {
        status = RES_ERROR;
        snprintf(err, sizeof err, "destination root is not a directory: %s",
                 oe->o.dst_root);
    } else if (oe->o.require_mount) {
        if (!root_is_live_mount(oe->o.src_root, "source", err, sizeof err)) {
            status = RES_ERROR;
        } else if (!oe->o.dry_run &&
                   !root_is_live_mount(oe->o.dst_root, "destination", err,
                                       sizeof err)) {
            status = RES_ERROR;
        }
    }

    if (status != RES_OK)
        LOGW("mount probe failed: %s", err);

    struct shard_counters c;
    memset(&c, 0, sizeof c);
    pb_buf b;
    pb_init(&b);
    enc_shard_result(&b, it->shard_id, it->lease_id, status, &c,
                     err[0] ? err : NULL);
    out_push(FR_SHARD_RESULT, &b);
    lease_remove(it->lease_id);
}
