/* xattr + ACL engine (docs/DESIGN-agent.md §5).
 *
 * All readable namespaces are copied. POSIX ACLs are byte-stable kernel blobs
 * carried in system.posix_acl_access/default; NFSv4 ACLs are the XDR blob in
 * system.nfs4_acl (raw copy between v4 mounts per design §5.1 — the
 * POSIX↔NFSv4 translation table is TODO(slice4), so a cross-flavor pair hits
 * the untranslatable policy). Any attribute that cannot be applied is counted
 * as a fidelity exception (or an error under policy=fail) — never dropped
 * silently.
 *
 * Path-based variants go through /proc/self/fd/<dirfd>/<name>, which avoids
 * opening target files at all (matters for the clean-file drift check: two
 * llistxattr calls instead of two opens + lists) and is the only way to reach
 * symlink xattrs (no *at() syscall family exists for xattrs). */
#include "agent.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/xattr.h>

#define XATTR_LIST_MAX_BUF (64 * 1024)
#define XATTR_VAL_MAX_BUF  (64 * 1024)

void walk_fidelity(struct walk_ctx *ctx, const char *what, const char *path)
{
    CTR_ADD(ctx->c.fidelity_exceptions, 1);
    /* TODO(slice4): JR_FIDELITY_EXCEPTION journal record */
    LOGW("shard %llu: fidelity: %s %s: %s",
         (unsigned long long)ctx->it->shard_id, what, path, strerror(errno));
}

/* ---- attribute classification / policy ---- */
enum xcat {
    XC_COPY,     /* user.*, trusted.*, security.* */
    XC_ACL,      /* system.posix_acl_*, system.nfs4_acl (job ACL options) */
    XC_IGNORE,   /* other system.* (kernel-managed, not portable) */
};

static enum xcat classify(const struct job_options *o, const char *name,
                          bool *acl_enabled)
{
    /* SELinux labels belong to the destination host's policy (restorecon),
     * and copying them unprivileged fails on every pass — which would make
     * the drift check re-fix every file forever. Managed exclusion. */
    if (strcmp(name, "security.selinux") == 0)
        return XC_IGNORE;
    if (strncmp(name, "system.", 7) == 0) {
        if (strcmp(name, "system.posix_acl_access") == 0 ||
            strcmp(name, "system.posix_acl_default") == 0) {
            *acl_enabled = o->acl_posix;
            return XC_ACL;
        }
        if (strcmp(name, "system.nfs4_acl") == 0) {
            *acl_enabled = o->acl_nfs4;
            return XC_ACL;
        }
        return XC_IGNORE;
    }
    return XC_COPY;
}

/* ---- xattr set read (names + values), fd- or path-based ---- */
struct xent {
    const char *name; /* into the names buffer */
    void       *val;  /* malloc'd */
    size_t      len;
};

struct xset {
    char        *names; /* raw listxattr buffer */
    struct xent *v;
    size_t       n;
};

static void xset_free(struct xset *s)
{
    for (size_t i = 0; i < s->n; i++)
        free(s->v[i].val);
    free(s->v);
    free(s->names);
    memset(s, 0, sizeof *s);
}

static int xent_cmp(const void *a, const void *b)
{
    return strcmp(((const struct xent *)a)->name, ((const struct xent *)b)->name);
}

/* target: fd >= 0 → f* calls; else path with l* (nofollow) or plain calls */
struct xtarget {
    int         fd;
    const char *path;
    bool        nofollow;
};

static ssize_t x_list(const struct xtarget *t, char *buf, size_t n)
{
    if (t->fd >= 0)
        return flistxattr(t->fd, buf, n);
    return t->nofollow ? llistxattr(t->path, buf, n) : listxattr(t->path, buf, n);
}

static ssize_t x_get(const struct xtarget *t, const char *name, void *buf, size_t n)
{
    if (t->fd >= 0)
        return fgetxattr(t->fd, name, buf, n);
    return t->nofollow ? lgetxattr(t->path, name, buf, n)
                       : getxattr(t->path, name, buf, n);
}

static int x_set(const struct xtarget *t, const char *name, const void *buf, size_t n)
{
    if (t->fd >= 0)
        return fsetxattr(t->fd, name, buf, n, 0);
    return t->nofollow ? lsetxattr(t->path, name, buf, n, 0)
                       : setxattr(t->path, name, buf, n, 0);
}

static int x_remove(const struct xtarget *t, const char *name)
{
    if (t->fd >= 0)
        return fremovexattr(t->fd, name);
    return t->nofollow ? lremovexattr(t->path, name) : removexattr(t->path, name);
}

/* Reads the full xattr set. Returns 0 (possibly empty set), or -errno.
 * ENOTSUP (fs without xattrs) comes back as an empty set with rc 0. */
static int xset_read(const struct xtarget *t, struct xset *out)
{
    memset(out, 0, sizeof *out);
    ssize_t ln = x_list(t, NULL, 0);
    if (ln < 0)
        return errno == ENOTSUP ? 0 : -errno;
    if (ln == 0)
        return 0;
    if (ln > XATTR_LIST_MAX_BUF)
        ln = XATTR_LIST_MAX_BUF;
    out->names = malloc((size_t)ln);
    if (!out->names)
        return -ENOMEM;
    ln = x_list(t, out->names, (size_t)ln);
    if (ln < 0) {
        int e = errno;
        xset_free(out);
        return e == ENOTSUP ? 0 : -e;
    }

    size_t count = 0;
    for (ssize_t i = 0; i < ln; i += (ssize_t)strlen(out->names + i) + 1)
        count++;
    out->v = calloc(count ? count : 1, sizeof *out->v);
    if (!out->v) {
        xset_free(out);
        return -ENOMEM;
    }
    for (ssize_t i = 0; i < ln; i += (ssize_t)strlen(out->names + i) + 1) {
        const char *name = out->names + i;
        ssize_t vn = x_get(t, name, NULL, 0);
        if (vn < 0)
            continue; /* raced away or unreadable: skip (re-diffed next pass) */
        if (vn > XATTR_VAL_MAX_BUF)
            vn = XATTR_VAL_MAX_BUF;
        void *val = malloc(vn ? (size_t)vn : 1);
        if (!val) {
            xset_free(out);
            return -ENOMEM;
        }
        vn = x_get(t, name, val, (size_t)vn);
        if (vn < 0) {
            free(val);
            continue;
        }
        out->v[out->n].name = name;
        out->v[out->n].val = val;
        out->v[out->n].len = (size_t)vn;
        out->n++;
    }
    qsort(out->v, out->n, sizeof *out->v, xent_cmp);
    return 0;
}

/* ---- copy: src set → dst (set src attrs, remove dst-only attrs) ---- */
static void xcopy(struct walk_ctx *ctx, const struct xtarget *src,
                  const struct xtarget *dst, const char *logname)
{
    const struct job_options *o = &ctx->oe->o;
    if (!o->meta_xattrs)
        return;

    struct xset ss, ds;
    if (xset_read(src, &ss) < 0) {
        walk_err(ctx, "read xattrs", logname);
        return;
    }
    bool have_ds = xset_read(dst, &ds) == 0;

    for (size_t i = 0; i < ss.n; i++) {
        bool acl_enabled = true;
        enum xcat cat = classify(o, ss.v[i].name, &acl_enabled);
        if (cat == XC_IGNORE)
            continue;
        if (cat == XC_ACL) {
            if (!acl_enabled || o->acl_untranslatable == ACL_UNTRANS_SKIP)
                continue;
        }
        if (x_set(dst, ss.v[i].name, ss.v[i].val, ss.v[i].len) < 0) {
            if (cat == XC_ACL && o->acl_untranslatable == ACL_UNTRANS_FAIL)
                walk_err(ctx, ss.v[i].name, logname);
            else
                walk_fidelity(ctx, ss.v[i].name, logname);
        }
    }
    /* dst-only attributes in copied namespaces are stale: remove */
    if (have_ds) {
        for (size_t j = 0; j < ds.n; j++) {
            bool acl_enabled = true;
            if (classify(o, ds.v[j].name, &acl_enabled) == XC_IGNORE)
                continue;
            bool in_src = false;
            for (size_t i = 0; i < ss.n; i++) {
                if (strcmp(ss.v[i].name, ds.v[j].name) == 0) {
                    in_src = true;
                    break;
                }
            }
            if (!in_src && x_remove(dst, ds.v[j].name) < 0 && errno != ENODATA)
                walk_fidelity(ctx, "remove stale xattr", logname);
        }
    }
    xset_free(&ss);
    if (have_ds)
        xset_free(&ds);
}

static void proc_path(char *buf, size_t cap, int dirfd, const char *name)
{
    snprintf(buf, cap, "/proc/self/fd/%d/%s", dirfd, name);
}

void xattr_copy_fd(struct walk_ctx *ctx, int src_fd, int dst_fd, const char *logname)
{
    struct xtarget s = { .fd = src_fd }, d = { .fd = dst_fd };
    xcopy(ctx, &s, &d, logname);
}

void xattr_copy_at(struct walk_ctx *ctx, int sdirfd, int ddirfd, const char *name,
                   bool nofollow)
{
    char sp[PATH_MAX], dp[PATH_MAX];
    proc_path(sp, sizeof sp, sdirfd, name);
    proc_path(dp, sizeof dp, ddirfd, name);
    struct xtarget s = { .fd = -1, .path = sp, .nofollow = nofollow };
    struct xtarget d = { .fd = -1, .path = dp, .nofollow = nofollow };
    xcopy(ctx, &s, &d, name);
}

/* Drift check for otherwise-clean regular files (diff predicate step 6):
 * compares the copyable attribute sets without opening either file. */
bool xattr_equal_at(struct walk_ctx *ctx, int sdirfd, int ddirfd, const char *name)
{
    const struct job_options *o = &ctx->oe->o;
    char sp[PATH_MAX], dp[PATH_MAX];
    proc_path(sp, sizeof sp, sdirfd, name);
    proc_path(dp, sizeof dp, ddirfd, name);
    struct xtarget s = { .fd = -1, .path = sp };
    struct xtarget d = { .fd = -1, .path = dp };

    struct xset ss, ds;
    if (xset_read(&s, &ss) < 0)
        return true; /* indeterminable: don't churn */
    if (xset_read(&d, &ds) < 0) {
        xset_free(&ss);
        return true;
    }
    bool equal = true;
    size_t i = 0, j = 0;
    while (i < ss.n || j < ds.n) {
        bool acl_enabled;
        if (i < ss.n && classify(o, ss.v[i].name, &acl_enabled) == XC_IGNORE) {
            i++;
            continue;
        }
        if (j < ds.n && classify(o, ds.v[j].name, &acl_enabled) == XC_IGNORE) {
            j++;
            continue;
        }
        if (i == ss.n || j == ds.n) {
            equal = false; /* one side has extra copyable attrs */
            break;
        }
        int cmp = strcmp(ss.v[i].name, ds.v[j].name);
        if (cmp != 0 || ss.v[i].len != ds.v[j].len ||
            memcmp(ss.v[i].val, ds.v[j].val, ss.v[i].len) != 0) {
            equal = false;
            break;
        }
        i++;
        j++;
    }
    xset_free(&ss);
    xset_free(&ds);
    return equal;
}
