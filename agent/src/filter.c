/* filter.c — rsync-like glob matcher (see filter.h for semantics).
 *
 * Recursive backtracking. Paths are bounded (PATH_MAX) and patterns are bounded
 * by the coordinator (<= 255 bytes), so recursion depth is bounded and stack use
 * is trivial. No allocation.
 */
#include "filter.h"

bool filter_glob_match(const char *p, const char *s)
{
    while (*p) {
        if (p[0] == '*' && p[1] == '*') {
            /* '**' spans '/'. Collapse a following '/' so that '**\/' also
             * matches zero leading segments (the tail is tried against every
             * position of s, including s itself). */
            p += 2;
            if (*p == '/')
                p++;
            if (*p == '\0')
                return true; /* trailing '**' (or '**\/') matches the rest */
            for (const char *t = s;; t++) {
                if (filter_glob_match(p, t))
                    return true;
                if (*t == '\0')
                    return false;
            }
        } else if (*p == '*') {
            /* single '*' matches zero or more non-'/' characters */
            p++;
            for (const char *t = s;; t++) {
                if (filter_glob_match(p, t))
                    return true;
                if (*t == '\0' || *t == '/')
                    return filter_glob_match(p, t);
            }
        } else if (*p == '?') {
            if (*s == '\0' || *s == '/')
                return false;
            p++;
            s++;
        } else {
            if (*p != *s)
                return false;
            p++;
            s++;
        }
    }
    return *s == '\0';
}
