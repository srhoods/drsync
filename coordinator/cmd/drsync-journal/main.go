// drsync-journal — dump/summarize pass journals (precursor of `drsync journal cat`).
//
//	drsync-journal -root /var/lib/drsync/journals -job 1 -pass 1 [-type JR_ORPHAN] [-summary]
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"drsync/coordinator/internal/journal"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

func main() {
	root := flag.String("root", "/var/lib/drsync/journals", "journal root directory")
	job := flag.Int64("job", 0, "job id")
	pass := flag.Int("pass", 0, "pass number")
	typ := flag.String("type", "", "filter by record type (e.g. JR_ORPHAN)")
	summary := flag.Bool("summary", false, "print per-type counts only")
	flag.Parse()
	if *job == 0 || *pass == 0 {
		fmt.Fprintln(os.Stderr, "usage: drsync-journal -root DIR -job N -pass N [-type T] [-summary]")
		os.Exit(2)
	}

	counts := map[string]int{}
	enc := json.NewEncoder(os.Stdout)
	err := journal.ReadRecords(*root, *job, *pass, func(r *drsyncpb.JournalRecord) error {
		t := r.Type.String()
		counts[t]++
		if *summary || (*typ != "" && t != *typ) {
			return nil
		}
		out := map[string]any{"type": t, "rel_path": string(r.RelPath), "ts_ns": r.TsNs}
		if r.Src != nil {
			out["src"] = map[string]any{"mode": fmt.Sprintf("%04o", r.Src.Mode),
				"uid": r.Src.Uid, "gid": r.Src.Gid, "size": r.Src.Size,
				"mtime_ns": r.Src.MtimeNs, "nlink": r.Src.Nlink}
		}
		if r.Xxh3Lo != 0 || r.Xxh3Hi != 0 {
			out["xxh3"] = fmt.Sprintf("%016x%016x", r.Xxh3Hi, r.Xxh3Lo)
		}
		if r.Errno != 0 {
			out["errno"] = r.Errno
		}
		if r.Detail != "" {
			out["detail"] = r.Detail
		}
		return enc.Encode(out)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if *summary {
		for t, n := range counts {
			fmt.Printf("%-24s %d\n", t, n)
		}
	}
}
