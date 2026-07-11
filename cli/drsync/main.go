// drsync — operator CLI, a thin client of the coordinator REST API
// (docs/DESIGN-jobspec.md §2). Every command maps 1:1 onto an /api/v1
// endpoint; the coordinator owns all state and validation.
//
//	drsync job submit spec.yaml [--dry-run] [--start] [--set path=value]...
//	drsync job list
//	drsync job status <name> [--watch]
//	drsync job start|pause|resume|cancel <name>
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
	fmt.Fprint(os.Stderr, `usage: drsync <command> [args]

  job submit <spec.yaml> [--dry-run] [--start] [--set path=value]...
  job list
  job status <name> [--watch]
  job start|pause|resume|cancel <name>
  pass trigger <name> [--delete-pass --i-know-this-deletes]
  agent list
  errors <name> [--pass N|all] [--class EACCES] [--path prefix]
  journal cat <name> [--pass N|all] [--type orphan] [--path prefix] [--jsonl]
  report <name> [--json]
  queue
  events [--job name]
  ca init [--dir D] [--cn NAME]
  ca issue --type server|agent --cn NAME [--dir D] [--dns H]... [--ip A]...

connection: --server URL (or DRSYNC_SERVER), --token T (or DRSYNC_TOKEN)
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
