/* Destination temp names: one definition of the format, shared by the code that
 * writes them (copy.c) and the code that decides whether one is crash residue
 * (walker.c's orphan sweep). The coordinator writes the same format for chunk
 * temps it names itself — see planBigFiles in coordinator/internal/agentsrv.
 *
 *     <prefix><job>-<pass>.<shard>.<seq>      all fields lowercase hex
 *
 * The leading "<job>-<pass>." tag is the liveness signal. A temp sitting in the
 * destination has no source counterpart, so it reaches the orphan sweep looking
 * exactly like residue from an interrupted copy; a chunked file's temp sits
 * there for the whole multi-host copy. Comparing the tag against the sweeping
 * shard's own (job, pass) is what tells the two apart. */
#include "agent.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

void temp_name_fmt(char *out, size_t cap, const char *prefix, uint64_t job_id,
                   uint32_t pass_no, uint64_t shard_id, unsigned seq)
{
    snprintf(out, cap, "%s%llx-%x.%llx.%x", prefix, (unsigned long long)job_id,
             pass_no, (unsigned long long)shard_id, seq);
}

/* Parses one lowercase-hex field, stopping at term. Deliberately stricter than
 * strtoull, which would also accept leading whitespace, a +/- sign and an "0x"
 * prefix: those parse to a matching (job, pass) for names this code never
 * emits, and a false match means a file is protected from reclaim forever.
 * Returns NULL unless the field is at least one hex digit followed by term. */
static const char *parse_hex(const char *s, char term, uint64_t *out)
{
    uint64_t v = 0;
    const char *p = s;
    for (; *p && *p != term; p++) {
        unsigned d;
        if (*p >= '0' && *p <= '9')
            d = (unsigned)(*p - '0');
        else if (*p >= 'a' && *p <= 'f')
            d = (unsigned)(*p - 'a') + 10;
        else
            return NULL; /* uppercase, sign, space, "0x" — not our format */
        if (v > (UINT64_MAX - d) / 16)
            return NULL; /* overflow: cannot be an id we emitted */
        v = v * 16 + d;
    }
    if (p == s || *p != term)
        return NULL; /* empty field, or ran off the end without term */
    *out = v;
    return p + 1;
}

bool temp_tag_matches(const char *tail, uint64_t job_id, uint32_t pass_no)
{
    uint64_t job, pass;
    const char *p = parse_hex(tail, '-', &job);
    if (!p) /* untagged: pre-upgrade name, or not ours */
        return false;
    if (!parse_hex(p, '.', &pass))
        return false;
    return job == job_id && pass == pass_no;
}
