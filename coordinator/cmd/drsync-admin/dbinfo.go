package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/rivo/tview"
)

// renderDBInfo renders the "Database" screen: PRAGMA configuration, file
// sizes, and summary counts by state — the "database configuration info"
// requirement, kept separate from per-table row browsing.
func renderDBInfo(db *sql.DB, dbPath string) string {
	out := ""
	line := func(format string, args ...any) { out += fmt.Sprintf(format, args...) + "\n" }

	line("[::b]Database file[::-]  %s", dbPath)
	if fi, err := os.Stat(dbPath); err == nil {
		line("  size: %s", humanBytes(fi.Size()))
	}
	if fi, err := os.Stat(dbPath + "-wal"); err == nil {
		line("  WAL size: %s", humanBytes(fi.Size()))
	}
	line("")

	line("[::b]PRAGMA configuration[::-]")
	for _, p := range []string{"journal_mode", "auto_vacuum", "page_size", "synchronous", "foreign_keys", "busy_timeout"} {
		v, err := pragmaValue(db, p)
		if err != nil {
			v = "?"
		}
		line("  %-16s %s", p, v)
	}
	line("")

	line("[::b]Row counts[::-]")
	for _, t := range []string{"jobs", "passes", "shards", "agents", "shard_counts", "chunk_groups", "splits", "journal_cursors"} {
		n, err := tableRowCount(db, t)
		if err != nil {
			continue
		}
		line("  %-16s %d", t, n)
	}
	line("")

	if n, err := groupCount(db, "jobs", "state"); err == nil {
		line("[::b]Jobs by state[::-]")
		for k, v := range n {
			line("  %-16s %d", k, v)
		}
		line("")
	}
	if n, err := groupCount(db, "agents", "state"); err == nil {
		line("[::b]Agents by state[::-]")
		for k, v := range n {
			line("  %-16s %d", k, v)
		}
	}

	return out
}

func groupCount(db *sql.DB, table, col string) (map[string]int64, error) {
	rows, err := db.Query(fmt.Sprintf(`SELECT %q, COUNT(*) FROM %q GROUP BY %q`, col, table, col))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var k string
		var v int64
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func newDBInfoView(db *sql.DB, dbPath string) *tview.TextView {
	tv := tview.NewTextView().SetDynamicColors(true)
	tv.SetBorder(true).SetTitle(" Database ")
	tv.SetText(renderDBInfo(db, dbPath))
	return tv
}
