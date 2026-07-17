/* filter.h - include/exclude glob matching for the walker.
 *
 * Patterns are the resolved JobOptions filter rules (proto FilterRule). The
 * coordinator carries them fully resolved; the agent is the sole enforcement
 * point (nothing matches server-side). Semantics are rsync-like and match the
 * examples in docs/DESIGN-jobspec.md:
 *
 *   - matched against the entry path relative to the job root ("a/b/c.tmp"),
 *     anchored at both ends (the whole path must match);
 *   - '?'  matches one character other than a slash;
 *   - '*'  matches zero or more characters other than a slash;
 *   - a double-star matches zero or more characters including slashes;
 *   - a double-star followed by a slash additionally matches zero leading
 *     path segments, so the pattern for "star-star slash star dot tmp" matches
 *     both "x.tmp" and "a/b/c.tmp" (rsync behaviour).
 *
 * Character classes (square brackets) are not supported - a '[' is a literal.
 */
#ifndef DRSYNC_FILTER_H
#define DRSYNC_FILTER_H

#include <stdbool.h>

/* True if path matches the glob pattern under the semantics above. */
bool filter_glob_match(const char *pattern, const char *path);

#endif /* DRSYNC_FILTER_H */
