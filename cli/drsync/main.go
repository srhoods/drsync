// drsync — operator CLI, a thin client of the coordinator REST API
// (docs/DESIGN-jobspec.md §2). Every command maps 1:1 onto an /api/v1
// endpoint; the coordinator owns all state and validation.
//
//	drsync job submit spec.yaml [--dry-run] [--start] [--set path=value]...
//	drsync job list
//	drsync job status [<name>] [--watch] [--all]
//	drsync job start|pause|resume|cancel <name>
//	drsync job purge <name> | --completed|--state S [--older-than DUR]
//	drsync pass trigger <name> [--delete-pass --i-know-this-deletes]
//	drsync agent list | inflight <id> | enable <id> | disable <id>
//	drsync errors <name> [--pass N|all] [--class EACCES] [--path prefix]
//	drsync journal cat <name> [--pass N|all] [--type orphan] [--summary] [--jsonl]
//	drsync report <name> [--json]
//	drsync queue
//	drsync events [--job name]
//
// Connection: --server / DRSYNC_SERVER (default http://127.0.0.1:7441),
// --token / DRSYNC_TOKEN.
package main

import (
	"fmt"
	"os"
)

func usage() {
	fmt.Fprint(os.Stderr, `drsync — operator CLI for the drsync coordinator

USAGE
  drsync <command> [options]

JOBS
  job submit <spec.yaml> [--dry-run] [--start] [--set path=value]...
        Register a job. --dry-run walks/diffs/journals but writes nothing;
        --start runs it immediately; --set overrides a spec field (repeatable),
        e.g. --set spec.tuning.shard_budget=4 --set spec.copy.server_side_copy=off
  job list
        All jobs and their states.
  job status [<name>] [--watch] [--all]
        Per-pass progress. With no name, shows every ACTIVE job (--all includes
        finished ones). --watch follows one job live over the event stream.
  job start|pause|resume|cancel <name>
        Lifecycle control (pause stops new grants; in-flight work finishes).
  job purge <name>
        Delete one FINISHED job (rows + journal) to reclaim coordinator disk.
  job purge --completed [--older-than 168h] [--dry-run]
  job purge --state completed|cancelled|failed|terminal [--older-than 720h]
        Bulk-purge finished jobs; --older-than keeps recently-finished ones;
        --dry-run lists what would be purged without deleting anything.

PASSES
  pass trigger <name>
        Start the next pass (e.g. a final cutover pass, or with schedule:manual).
  pass trigger <name> --delete-pass --i-know-this-deletes
        Run a DELETE pass — removes destination orphans (double-gated).

AGENTS
  agent list
        Connected agents, liveness, and scheduling status.
  agent inflight <id>
        What the agent is working on right now, longest-running first: shard,
        kind, path, how long it has been running and entries walked so far.
        Start here when a job's throughput drops — a shard whose RUNNING time
        climbs while ENTRIES stays put is stuck, not merely slow.
  agent disable <id>
        Stop granting new shards to an agent; it stays connected and finishes
        its in-flight leases (useful to drain a node before maintenance).
  agent enable <id>
        Resume granting new shards to a previously-disabled agent.

INSPECT & AUDIT
  report <name> [--json]
        Migration/cutover summary: per-pass delta, convergence curve, totals.
  queue
        Shard queue depth by state, including PARKED shards needing attention.
  queue retry <shard-id> | --job <name>
        Requeue parked shard(s) for a fresh attempt on any agent (after fixing
        the cause). --job retries every parked shard of a job.
  queue drop <shard-id> | --job <name>
        Permanently discard parked shard(s), accepting the gap and unblocking
        the pass. --job drops every parked shard of a job.
  errors <name> [--pass N|all] [--class EACCES] [--path prefix] [--limit N] [--offset N]
        Browse errors; --class filters by errno name (EACCES, ENOENT, ESTALE, ...).
  journal cat <name> [--pass N|all] [--type T] [--path prefix] [--summary] [--jsonl]
        Page the journal. --type is one of:
          copied  meta_fixed  orphan  deleted  error  fidelity_exception
          nlink_dup  verify_ok  verify_fail  would_copy  would_delete
          src_changed  dir_meta  skipped_clean
        --summary counts records by type instead of listing them (green=nominal,
        yellow=informational, red=failure); honors --pass/--type/--path.
        --jsonl emits raw records (one JSON object per line) for scripting.
  events [--job name]
        Tail the live event stream (state changes, agent up/down, parked-shard
        alerts, 1 Hz stats).

CERTIFICATES (mTLS; local, no server needed)
  ca init [--dir D] [--cn NAME] [--days N]
  ca issue --type server|agent --cn NAME [--dir D] [--dns H]... [--ip A]... [--days N]

HTTP(S) CERTIFICATE (coordinator's WebUI/API listener; local, no server needed)
  cert generate-self-signed [--cn NAME] [--dns H]... [--ip A]... [--out DIR] [--days N]
        Dev/test only. Writes server.crt/server.key; point certs.yaml at them.

CONNECTION (every command except 'ca'/'cert')
  --server URL     coordinator base URL   (or $DRSYNC_SERVER; default http://127.0.0.1:7441)
  --token  TOKEN   API bearer token       (or $DRSYNC_TOKEN)

Run 'drsync <command> -h' for a command's own flags.
`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "job":
		err = cmdJob(os.Args[2:])
	case "pass":
		err = cmdPass(os.Args[2:])
	case "agent":
		err = cmdAgent(os.Args[2:])
	case "errors":
		err = cmdErrors(os.Args[2:])
	case "journal":
		err = cmdJournal(os.Args[2:])
	case "report":
		err = cmdReport(os.Args[2:])
	case "queue":
		err = cmdQueue(os.Args[2:])
	case "events":
		err = cmdEvents(os.Args[2:])
	case "ca":
		err = cmdCA(os.Args[2:])
	case "cert":
		err = cmdCert(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "drsync: unknown command %q\n", os.Args[1])
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "drsync:", err)
		os.Exit(1)
	}
}
