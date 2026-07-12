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
//	drsync agent list
//	drsync errors <name> [--pass N|all] [--class EACCES] [--path prefix]
//	drsync journal cat <name> [--pass N|all] [--type orphan] [--jsonl]
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

INSPECT & AUDIT
  agent list
        Connected agents and liveness.
  report <name> [--json]
        Migration/cutover summary: per-pass delta, convergence curve, totals.
  queue
        Shard queue depth by state, including PARKED shards needing attention.
  errors <name> [--pass N|all] [--class EACCES] [--path prefix] [--limit N] [--offset N]
        Browse errors; --class filters by errno name (EACCES, ENOENT, ESTALE, ...).
  journal cat <name> [--pass N|all] [--type T] [--path prefix] [--jsonl]
        Page the journal. --type is one of:
          copied  meta_fixed  orphan  deleted  error  fidelity_exception
          nlink_dup  verify_ok  verify_fail  would_copy  would_delete
          src_changed  dir_meta  skipped_clean
        --jsonl emits raw records (one JSON object per line) for scripting.
  events [--job name]
        Tail the live event stream (state changes, agent up/down, parked-shard
        alerts, 1 Hz stats).

CERTIFICATES (mTLS; local, no server needed)
  ca init [--dir D] [--cn NAME] [--days N]
  ca issue --type server|agent --cn NAME [--dir D] [--dns H]... [--ip A]... [--days N]

CONNECTION (every command except 'ca')
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
