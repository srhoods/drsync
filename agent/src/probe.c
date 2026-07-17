/* Mount probe (docs/DESIGN-protocol.md §3.1 ProbeTask).
 *
 * At pass start the coordinator pins one probe shard to each agent and withholds
 * the root walk shard until every probe reports OK. The probe verifies this
 * agent's source and destination roots are present and are directories, so a
 * missing or misordered mount on ANY host is caught before bulk work runs —
 * not just on whichever agent happened to grab the root shard. The result is a
 * plain ShardResult (RESULT_OK / RESULT_ERROR), the same channel the chunk
 * executor uses; a probe walks nothing and emits no journal records. */
#include "agent.h"

#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

void process_probe(const struct shard_item *it)
{
    int status = RES_OK;
    char err[PATH_MAX + 64] = ""; /* holds a root path plus a short prefix */

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
