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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void temp_name_fmt(char *out, size_t cap, const char *prefix, uint64_t job_id,
                   uint32_t pass_no, uint64_t shard_id, unsigned seq)
{
    snprintf(out, cap, "%s%llx-%x.%llx.%x", prefix, (unsigned long long)job_id,
             pass_no, (unsigned long long)shard_id, seq);
}

bool temp_tag_matches(const char *tail, uint64_t job_id, uint32_t pass_no)
{
    char *end;
    unsigned long long job = strtoull(tail, &end, 16);
    if (end == tail || *end != '-')
        return false; /* untagged: pre-upgrade name, or not ours */
    const char *p = end + 1;
    unsigned long long pass = strtoull(p, &end, 16);
    if (end == p || *end != '.')
        return false;
    return job == job_id && pass == pass_no;
}
