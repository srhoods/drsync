package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/coder/websocket/wsjson"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Shared API views (mirror coordinator/internal/api JSON)
// ---------------------------------------------------------------------------

type passView struct {
	PassNo        int    `json:"pass_no"`
	State         string `json:"state"`
	EntriesWalked int64  `json:"entries_walked"`
	FilesCopied   int64  `json:"files_copied"`
	BytesCopied   int64  `json:"bytes_copied"`
	MetaFixed     int64  `json:"meta_fixed"`
	Orphans       int64  `json:"orphans"`
	Errors        int64  `json:"errors"`
	FidelityExc   int64  `json:"fidelity_exceptions"`
	VerifyOK      int64  `json:"verify_ok"`
	VerifyFail    int64  `json:"verify_fail"`
	DurationMs    int64  `json:"duration_ms"`
}

type jobView struct {
	Name   string     `json:"name"`
	State  string     `json:"state"`
	DryRun bool       `json:"dry_run"`
	Passes []passView `json:"passes"`
}

type event struct {
	Type   string         `json:"type"`
	TsMs   int64          `json:"ts_ms"`
	Job    string         `json:"job"`
	PassNo int            `json:"pass_no"`
	Data   map[string]any `json:"data"`
}

// parseFlags parses fs allowing flags and positionals to be interspersed
// (standard flag stops at the first positional). Returns the positionals.
func parseFlags(fs *flag.FlagSet, args []string) []string {
	var pos []string
	for {
		fs.Parse(args)
		rest := fs.Args()
		if len(rest) == 0 {
			return pos
		}
		pos = append(pos, rest[0])
		args = rest[1:]
	}
}

func terminalJob(state string) bool {
	return state == "COMPLETED" || state == "CANCELLED" || state == "FAILED"
}

// ---------------------------------------------------------------------------
// drsync job ...
// ---------------------------------------------------------------------------

func cmdJob(args []string) error {
	if len(args) < 1 {
		usage()
	}
	switch args[0] {
	case "submit":
		return jobSubmit(args[1:])
	case "list":
		return jobList(args[1:])
	case "status":
		return jobStatus(args[1:])
	case "start", "pause", "resume", "cancel":
		return jobAction(args[0], args[1:])
	case "purge":
		return jobPurge(args[1:])
	default:
		return fmt.Errorf("unknown job subcommand %q", args[0])
	}
}

type repeatedFlag []string

func (r *repeatedFlag) String() string     { return strings.Join(*r, ",") }
func (r *repeatedFlag) Set(v string) error { *r = append(*r, v); return nil }

func jobSubmit(args []string) error {
	fs := flag.NewFlagSet("job submit", flag.ExitOnError)
	mk := connFlags(fs)
	dryRun := fs.Bool("dry-run", false, "walk, diff and journal only; execute nothing")
	start := fs.Bool("start", false, "start the job immediately after submitting")
	var sets repeatedFlag
	fs.Var(&sets, "set", "override a spec field, YAML path syntax (repeatable)")
	pos := parseFlags(fs, args)
	if len(pos) != 1 {
		return fmt.Errorf("job submit needs exactly one spec file")
	}
	spec, err := os.ReadFile(pos[0])
	if err != nil {
		return err
	}
	if len(sets) > 0 {
		if spec, err = applySets(spec, sets); err != nil {
			return err
		}
	}
	c := mk()
	path := "/api/v1/jobs"
	if *dryRun {
		path += "?dry_run=true"
	}
	var jv jobView
	if err := c.postRaw(path, spec, &jv); err != nil {
		return err
	}
	fmt.Printf("job %s submitted (state %s%s)\n", jv.Name, jv.State, dryTag(jv.DryRun))
	if *start {
		if err := c.post("/api/v1/jobs/"+url.PathEscape(jv.Name)+"/start", nil, nil); err != nil {
			return err
		}
		fmt.Printf("job %s started\n", jv.Name)
	}
	return nil
}

// postRaw sends the body verbatim — job specs are YAML, which the generic
// JSON-marshaling post would reject.
func (c *client) postRaw(path string, body []byte, out any) error {
	return c.do("POST", path, body, out)
}

// applySets applies --set path.to.field=value overrides onto the YAML spec.
func applySets(spec []byte, sets []string) ([]byte, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(spec, &doc); err != nil {
		return nil, fmt.Errorf("parse spec: %w", err)
	}
	for _, s := range sets {
		key, val, found := strings.Cut(s, "=")
		if !found {
			return nil, fmt.Errorf("--set %q: want path=value", s)
		}
		if err := setPath(doc, strings.Split(key, "."), parseScalar(val)); err != nil {
			return nil, fmt.Errorf("--set %q: %w", s, err)
		}
	}
	return yaml.Marshal(doc)
}

func setPath(node map[string]any, path []string, val any) error {
	if len(path) == 1 {
		node[path[0]] = val
		return nil
	}
	next, ok := node[path[0]]
	if !ok || next == nil {
		child := map[string]any{}
		node[path[0]] = child
		return setPath(child, path[1:], val)
	}
	child, ok := next.(map[string]any)
	if !ok {
		return fmt.Errorf("%s is not a mapping", path[0])
	}
	return setPath(child, path[1:], val)
}

func parseScalar(v string) any {
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	return v
}

func dryTag(dry bool) string {
	if dry {
		return ", dry-run"
	}
	return ""
}

func jobList(args []string) error {
	fs := flag.NewFlagSet("job list", flag.ExitOnError)
	mk := connFlags(fs)
	fs.Parse(args)
	var jobs []jobView
	if err := mk().get("/api/v1/jobs", &jobs); err != nil {
		return err
	}
	tw := newTable()
	fmt.Fprintln(tw, "NAME\tSTATE\tDRY-RUN")
	for _, j := range jobs {
		fmt.Fprintf(tw, "%s\t%s\t%v\n", j.Name, j.State, j.DryRun)
	}
	return tw.Flush()
}

func jobStatus(args []string) error {
	fs := flag.NewFlagSet("job status", flag.ExitOnError)
	mk := connFlags(fs)
	watch := fs.Bool("watch", false, "follow live progress on the event stream")
	all := fs.Bool("all", false, "with no job name, include finished jobs too")
	pos := parseFlags(fs, args)
	if len(pos) > 1 {
		return fmt.Errorf("job status takes at most one job name")
	}
	c := mk()
	// No job name: show every active job (or all with --all). --watch needs a
	// specific job to filter its event stream.
	if len(pos) == 0 {
		if *watch {
			return fmt.Errorf("job status --watch needs a job name")
		}
		return statusAll(c, *all)
	}
	name := pos[0]
	jv, err := printStatus(c, name)
	if err != nil {
		return err
	}
	if !*watch || terminalJob(jv.State) {
		return nil
	}
	return watchJob(c, name)
}

// statusAll prints the status of every active job (RUNNING/PAUSED/READY), or
// every job when all is true.
func statusAll(c *client, all bool) error {
	var jobs []jobView
	if err := c.get("/api/v1/jobs", &jobs); err != nil {
		return err
	}
	shown := 0
	for _, j := range jobs {
		if !all && terminalJob(j.State) {
			continue
		}
		if shown > 0 {
			fmt.Println()
		}
		if _, err := printStatus(c, j.Name); err != nil {
			return err
		}
		shown++
	}
	if shown == 0 {
		if all {
			fmt.Println("no jobs")
		} else {
			fmt.Println("no active jobs (use --all to include finished jobs, or drsync job list)")
		}
	}
	return nil
}

func printStatus(c *client, name string) (*jobView, error) {
	var jv jobView
	if err := c.get("/api/v1/jobs/"+url.PathEscape(name), &jv); err != nil {
		return nil, err
	}
	fmt.Printf("job %s: %s%s\n", jv.Name, jv.State, dryTag(jv.DryRun))
	if len(jv.Passes) == 0 {
		return &jv, nil
	}
	tw := newTable()
	fmt.Fprintln(tw, "PASS\tSTATE\tWALKED\tCOPIED\tBYTES\tMETA\tORPHANS\tVERIFY\tERRORS\tDURATION")
	for _, p := range jv.Passes {
		fmt.Fprintf(tw, "%d\t%s\t%d\t%d\t%s\t%d\t%d\t%s\t%d\t%s\n",
			p.PassNo, p.State, p.EntriesWalked, p.FilesCopied,
			humanBytes(p.BytesCopied), p.MetaFixed, p.Orphans,
			verifyCol(p.VerifyOK, p.VerifyFail), p.Errors, humanMS(p.DurationMs))
	}
	tw.Flush()
	return &jv, nil
}

func verifyCol(ok, fail int64) string {
	if fail > 0 {
		return fmt.Sprintf("%d ok/%d FAIL", ok, fail)
	}
	return fmt.Sprintf("%d ok", ok)
}

// watchJob follows the event stream until the job reaches a terminal state.
func watchJob(c *client, name string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	conn, err := c.dialEvents(ctx)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	for {
		var ev event
		if err := wsjson.Read(ctx, conn, &ev); err != nil {
			if ctx.Err() != nil {
				return nil // interrupted by the operator
			}
			return err
		}
		if ev.Job != name {
			continue
		}
		switch ev.Type {
		case "stats":
			d := ev.Data
			fmt.Printf("pass %d %-9s walked=%d copied=%d (%s) meta=%d orphans=%d verify=%d/%d errs=%d shards=%s\n",
				ev.PassNo, str(d["pass_state"]), i64(d["entries_walked"]), i64(d["files_copied"]),
				humanBytes(i64(d["bytes_copied"])), i64(d["meta_fixed"]), i64(d["orphans"]),
				i64(d["verify_ok"]), i64(d["verify_fail"]), i64(d["errors"]), compactShards(d["shards"]))
		case "pass_state":
			fmt.Printf("pass %d -> %s\n", ev.PassNo, str(ev.Data["state"]))
		case "shard_parked":
			fmt.Printf("SHARD PARKED pass %d %v: %v\n", ev.PassNo,
				ev.Data["rel_path"], ev.Data["error"])
		case "job_state":
			state := str(ev.Data["state"])
			fmt.Printf("job %s -> %s\n", name, state)
			if terminalJob(state) {
				fmt.Println()
				_, err := printStatus(c, name)
				return err
			}
		}
	}
}

func str(v any) string { return fmt.Sprintf("%v", v) }

func i64(v any) int64 {
	f, _ := v.(float64) // JSON numbers decode as float64
	return int64(f)
}

func compactShards(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return "?"
	}
	return fmt.Sprintf("%dq/%dl/%dd", i64(m["QUEUED"]), i64(m["LEASED"]), i64(m["DONE"]))
}

func jobAction(action string, args []string) error {
	fs := flag.NewFlagSet("job "+action, flag.ExitOnError)
	mk := connFlags(fs)
	pos := parseFlags(fs, args)
	if len(pos) != 1 {
		return fmt.Errorf("job %s needs a job name", action)
	}
	name := pos[0]
	if err := mk().post("/api/v1/jobs/"+url.PathEscape(name)+"/"+action, nil, nil); err != nil {
		return err
	}
	fmt.Printf("job %s: %s ok\n", name, action)
	return nil
}

// jobPurge deletes finished jobs and their journals to reclaim coordinator disk.
//
//	drsync job purge <name>                       # one terminal job
//	drsync job purge --completed [--older-than 168h]
//	drsync job purge --state terminal --older-than 720h
func jobPurge(args []string) error {
	fs := flag.NewFlagSet("job purge", flag.ExitOnError)
	mk := connFlags(fs)
	completed := fs.Bool("completed", false, "bulk-purge all COMPLETED jobs")
	state := fs.String("state", "", "bulk-purge jobs in this state: completed|cancelled|failed|terminal")
	olderThan := fs.Duration("older-than", 0, "with bulk purge, only jobs finished longer ago than this (e.g. 168h)")
	dryRun := fs.Bool("dry-run", false, "bulk purge: list which jobs would be purged; delete nothing")
	pos := parseFlags(fs, args)

	// Single named job.
	if len(pos) == 1 {
		if *completed || *state != "" || *olderThan != 0 || *dryRun {
			return fmt.Errorf("give a job name OR bulk flags (--completed/--state/--older-than/--dry-run), not both")
		}
		name := pos[0]
		if err := mk().del("/api/v1/jobs/"+url.PathEscape(name), nil); err != nil {
			return err
		}
		fmt.Printf("job %s purged\n", name)
		return nil
	}
	if len(pos) > 1 {
		return fmt.Errorf("job purge takes at most one job name")
	}

	// Bulk purge.
	sel := *state
	if *completed {
		if sel != "" && sel != "completed" {
			return fmt.Errorf("--completed conflicts with --state %s", sel)
		}
		sel = "completed"
	}
	if sel == "" {
		return fmt.Errorf("specify a job name, or --completed / --state <s> for bulk purge")
	}
	path := "/api/v1/jobs/purge?state=" + url.QueryEscape(sel)
	if *olderThan > 0 {
		path += "&older_than_ms=" + strconv.FormatInt(olderThan.Milliseconds(), 10)
	}
	if *dryRun {
		path += "&dry_run=true"
	}
	var res struct {
		Purged []string `json:"purged"`
		Count  int      `json:"count"`
	}
	if err := mk().post(path, nil, &res); err != nil {
		return err
	}
	if res.Count == 0 {
		fmt.Println("no matching jobs to purge")
		return nil
	}
	verb, tail := "purged", "purged"
	if *dryRun {
		verb, tail = "would purge", "would be purged"
	}
	for _, n := range res.Purged {
		fmt.Printf("%s %s\n", verb, n)
	}
	fmt.Printf("%d job(s) %s\n", res.Count, tail)
	return nil
}

// ---------------------------------------------------------------------------
// drsync pass trigger
// ---------------------------------------------------------------------------

func cmdPass(args []string) error {
	if len(args) < 1 || args[0] != "trigger" {
		return fmt.Errorf("usage: drsync pass trigger <name> [--delete-pass --i-know-this-deletes]")
	}
	fs := flag.NewFlagSet("pass trigger", flag.ExitOnError)
	mk := connFlags(fs)
	del := fs.Bool("delete-pass", false, "run a delete pass (removes destination orphans)")
	ack := fs.Bool("i-know-this-deletes", false, "second gate required with --delete-pass")
	pos := parseFlags(fs, args[1:])
	if len(pos) != 1 {
		return fmt.Errorf("pass trigger needs a job name")
	}
	name := pos[0]
	body := map[string]any{"delete": *del}
	if *del {
		if !*ack {
			// The CLI-side half of the D5 double gate; the API enforces the
			// confirm string regardless of which client speaks to it.
			return fmt.Errorf("--delete-pass requires --i-know-this-deletes")
		}
		body["confirm"] = name
	}
	if err := mk().post("/api/v1/jobs/"+url.PathEscape(name)+"/passes", body, nil); err != nil {
		return err
	}
	kind := "pass"
	if *del {
		kind = "DELETE pass"
	}
	fmt.Printf("job %s: %s triggered\n", name, kind)
	return nil
}

// ---------------------------------------------------------------------------
// drsync agent list
// ---------------------------------------------------------------------------

func cmdAgent(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: drsync agent list | enable <id> | disable <id>")
	}
	switch args[0] {
	case "list":
		return agentList(args[1:])
	case "enable", "disable":
		return agentEnable(args[0], args[1:])
	default:
		return fmt.Errorf("usage: drsync agent list | enable <id> | disable <id>")
	}
}

func agentList(args []string) error {
	fs := flag.NewFlagSet("agent list", flag.ExitOnError)
	mk := connFlags(fs)
	fs.Parse(args)
	var agents []struct {
		ID            string `json:"id"`
		Hostname      string `json:"hostname"`
		Version       string `json:"version"`
		State         string `json:"state"`
		Connected     bool   `json:"connected"`
		Enabled       bool   `json:"enabled"`
		LastHeartbeat int64  `json:"last_heartbeat_ms"`
	}
	if err := mk().get("/api/v1/agents", &agents); err != nil {
		return err
	}
	tw := newTable()
	fmt.Fprintln(tw, "ID\tHOST\tVERSION\tSTATE\tCONNECTED\tSCHED\tLAST-HEARTBEAT")
	for _, a := range agents {
		sched := "enabled"
		if !a.Enabled {
			sched = "DISABLED"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%v\t%s\t%s\n", a.ID, a.Hostname, a.Version,
			a.State, a.Connected, sched, msTime(a.LastHeartbeat))
	}
	return tw.Flush()
}

func agentEnable(action string, args []string) error {
	fs := flag.NewFlagSet("agent "+action, flag.ExitOnError)
	mk := connFlags(fs)
	fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drsync agent %s <id>", action)
	}
	id := fs.Arg(0)
	var out struct {
		Agent   string `json:"agent"`
		Enabled bool   `json:"enabled"`
	}
	if err := mk().post("/api/v1/agents/"+url.PathEscape(id)+"/"+action, nil, &out); err != nil {
		return err
	}
	if out.Enabled {
		fmt.Printf("agent %s enabled (eligible for new shards)\n", out.Agent)
	} else {
		fmt.Printf("agent %s disabled (no new shards; in-flight leases finish)\n", out.Agent)
	}
	return nil
}

// ---------------------------------------------------------------------------
// drsync errors / journal cat
// ---------------------------------------------------------------------------

func journalQuery(fs *flag.FlagSet) (pass, path *string, limit, offset *int) {
	pass = fs.String("pass", "", "pass number, 'latest', or 'all' (default: all passes)")
	path = fs.String("path", "", "filter by rel-path prefix")
	limit = fs.Int("limit", 1000, "page size")
	offset = fs.Int("offset", 0, "page offset")
	return
}

func queryString(pass, path string, limit, offset int, extra url.Values) string {
	q := url.Values{}
	if pass != "" {
		q.Set("pass", pass)
	}
	if path != "" {
		q.Set("path", path)
	}
	q.Set("limit", strconv.Itoa(limit))
	if offset > 0 {
		q.Set("offset", strconv.Itoa(offset))
	}
	for k, vs := range extra {
		for _, v := range vs {
			q.Add(k, v)
		}
	}
	return "?" + q.Encode()
}

func cmdErrors(args []string) error {
	fs := flag.NewFlagSet("errors", flag.ExitOnError)
	mk := connFlags(fs)
	pass, path, limit, offset := journalQuery(fs)
	class := fs.String("class", "", "filter by errno class (e.g. EACCES)")
	pos := parseFlags(fs, args)
	if len(pos) != 1 {
		return fmt.Errorf("errors needs a job name")
	}
	extra := url.Values{}
	if *class != "" {
		extra.Set("class", *class)
	}
	var out struct {
		Count     int              `json:"count"`
		Truncated bool             `json:"truncated"`
		ByClass   map[string]int64 `json:"by_class"`
		Errors    []map[string]any `json:"errors"`
	}
	err := mk().get("/api/v1/jobs/"+url.PathEscape(pos[0])+"/errors"+
		queryString(*pass, *path, *limit, *offset, extra), &out)
	if err != nil {
		return err
	}
	for _, rec := range out.Errors {
		fmt.Printf("pass %v %-20s %-8v %s  %v\n", rec["pass"], rec["type"],
			zeroStr(rec["class"]), rec["rel_path"], zeroStr(rec["detail"]))
	}
	if len(out.ByClass) > 0 {
		fmt.Printf("-- %d error(s)", out.Count)
		for cl, n := range out.ByClass {
			fmt.Printf("  %s:%d", cl, n)
		}
		fmt.Println()
	} else {
		fmt.Println("no errors")
	}
	if out.Truncated {
		fmt.Println("-- page truncated; use --offset/--limit for more")
	}
	return nil
}

func zeroStr(v any) any {
	if v == nil {
		return "-"
	}
	return v
}

func cmdJournal(args []string) error {
	if len(args) < 1 || args[0] != "cat" {
		return fmt.Errorf("usage: drsync journal cat <name> [--pass N] [--type T] [--summary] [--jsonl]")
	}
	fs := flag.NewFlagSet("journal cat", flag.ExitOnError)
	mk := connFlags(fs)
	pass, path, limit, offset := journalQuery(fs)
	typ := fs.String("type", "", "filter by record type: copied, meta_fixed, orphan, deleted, error,\n"+
		"    \tfidelity_exception, nlink_dup, verify_ok, verify_fail, would_copy,\n"+
		"    \twould_delete, src_changed, dir_meta, skipped_clean")
	jsonl := fs.Bool("jsonl", false, "emit raw records as JSON lines")
	summary := fs.Bool("summary", false, "count records by type instead of listing them\n"+
		"    \t(honors --pass/--type/--path; color-coded on a terminal)")
	pos := parseFlags(fs, args[1:])
	if len(pos) != 1 {
		return fmt.Errorf("journal cat needs a job name")
	}
	extra := url.Values{}
	if *typ != "" {
		extra.Set("type", *typ)
	}
	if *summary {
		return journalSummary(mk(), pos[0], *pass, *typ, *path, *limit, *offset, extra, *jsonl)
	}
	var out struct {
		Count     int              `json:"count"`
		Truncated bool             `json:"truncated"`
		Records   []map[string]any `json:"records"`
	}
	err := mk().get("/api/v1/jobs/"+url.PathEscape(pos[0])+"/journal"+
		queryString(*pass, *path, *limit, *offset, extra), &out)
	if err != nil {
		return err
	}
	if *jsonl {
		enc := json.NewEncoder(os.Stdout)
		for _, rec := range out.Records {
			if err := enc.Encode(rec); err != nil {
				return err
			}
		}
	} else {
		for _, rec := range out.Records {
			line := fmt.Sprintf("pass %v %-20s %s", rec["pass"], rec["type"], rec["rel_path"])
			if d := rec["detail"]; d != nil {
				line += fmt.Sprintf("  (%v)", d)
			}
			if h := rec["xxh3"]; h != nil {
				line += fmt.Sprintf("  xxh3=%v", h)
			}
			fmt.Println(line)
		}
	}
	if out.Truncated {
		fmt.Fprintln(os.Stderr, "-- page truncated; use --offset/--limit for more")
	}
	return nil
}

// journalSummary fetches and renders the per-type record histogram.
func journalSummary(c *client, name, pass, typ, path string, limit, offset int, extra url.Values, asJSON bool) error {
	extra.Set("summary", "true")
	var out struct {
		Summary map[string]int64 `json:"summary"`
		Total   int64            `json:"total"`
	}
	if err := c.get("/api/v1/jobs/"+url.PathEscape(name)+"/journal"+
		queryString(pass, path, limit, offset, extra), &out); err != nil {
		return err
	}
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	scope := pass
	if scope == "" {
		scope = "all"
	}
	printJournalSummary(name, scope, out.Summary, out.Total)
	return nil
}

// journalCats lists record types in display order with their category color:
// green = nominal (work done as expected), yellow = informational (dry-run
// intentions, hardlink duplication, orphans, mid-copy source changes), red =
// failures (errors, unpreservable fidelity, checksum mismatches).
var journalCats = []struct{ key, label, color string }{
	{"COPIED", "copied", ansiGreen},
	{"META_FIXED", "meta_fixed", ansiGreen},
	{"DELETED", "deleted", ansiGreen},
	{"DIR_META", "dir_meta", ansiGreen},
	{"SKIPPED_CLEAN", "skipped_clean", ansiGreen},
	{"VERIFY_OK", "verify_ok", ansiGreen},
	{"WOULD_COPY", "would_copy", ansiYellow},
	{"WOULD_DELETE", "would_delete", ansiYellow},
	{"NLINK_DUP", "nlink_dup", ansiYellow},
	{"ORPHAN", "orphan", ansiYellow},
	{"SRC_CHANGED", "src_changed", ansiYellow},
	{"ERROR", "error", ansiRed},
	{"FIDELITY_EXCEPTION", "fidelity_exception", ansiRed},
	{"VERIFY_FAIL", "verify_fail", ansiRed},
}

func printJournalSummary(job, scope string, counts map[string]int64, total int64) {
	fmt.Printf("journal summary: %s (pass %s)\n", job, scope)
	if total == 0 {
		fmt.Println("  (no records)")
		return
	}
	on := colorEnabled()
	cw := len("total")
	seen := map[string]bool{}
	for _, r := range journalCats {
		if c, ok := counts[r.key]; ok {
			seen[r.key] = true
			if w := len(strconv.FormatInt(c, 10)); w > cw {
				cw = w
			}
		}
	}
	// Unknown types (forward-compat): counted, printed uncolored after the rest.
	var extra []string
	for k := range counts {
		if !seen[k] {
			extra = append(extra, k)
			if w := len(strconv.FormatInt(counts[k], 10)); w > cw {
				cw = w
			}
		}
	}
	sort.Strings(extra)

	for _, r := range journalCats {
		if c, ok := counts[r.key]; ok {
			fmt.Println(colorize(on, r.color, fmt.Sprintf("  %-20s %*d", r.label, cw, c)))
		}
	}
	for _, k := range extra {
		fmt.Printf("  %-20s %*d\n", strings.ToLower(k), cw, counts[k])
	}
	fmt.Printf("  %-20s %*d\n", "total", cw, total)
}

// ---------------------------------------------------------------------------
// drsync report / queue / events
// ---------------------------------------------------------------------------

func cmdReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	mk := connFlags(fs)
	asJSON := fs.Bool("json", false, "emit the raw report JSON")
	pos := parseFlags(fs, args)
	if len(pos) != 1 {
		return fmt.Errorf("report needs a job name")
	}
	var rep map[string]any
	if err := mk().get("/api/v1/jobs/"+url.PathEscape(pos[0])+"/report", &rep); err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	fmt.Printf("migration report: %v (state %v%s)\n\n", rep["job"], rep["state"],
		dryTag(rep["dry_run"] == true))
	if passes, ok := rep["passes"].([]any); ok {
		tw := newTable()
		fmt.Fprintln(tw, "PASS\tSTATE\tDURATION\tDELTA-FILES\tDELTA-BYTES\tORPHANS\tVERIFY\tERRORS")
		var totDur, totFiles, totBytes, totVOK, totVFail, totErr int64
		for _, pv := range passes {
			p, _ := pv.(map[string]any)
			totDur += i64(p["duration_ms"])
			totFiles += i64(p["delta_files"])
			totBytes += i64(p["delta_bytes"])
			totVOK += i64(p["verify_ok"])
			totVFail += i64(p["verify_fail"])
			totErr += i64(p["errors"])
			fmt.Fprintf(tw, "%d\t%v\t%s\t%d\t%s\t%d\t%s\t%d\n",
				i64(p["pass_no"]), p["state"], humanMS(i64(p["duration_ms"])),
				i64(p["delta_files"]), humanBytes(i64(p["delta_bytes"])), i64(p["orphans"]),
				verifyCol(i64(p["verify_ok"]), i64(p["verify_fail"])), i64(p["errors"]))
		}
		if len(passes) > 0 {
			// Footer summing the additive per-pass columns. Orphans is a
			// per-scan census (not additive), so it is left as a dash.
			fmt.Fprintf(tw, "TOTAL\t\t%s\t%d\t%s\t%s\t%s\t%d\n",
				humanMS(totDur), totFiles, humanBytes(totBytes), "-",
				verifyCol(totVOK, totVFail), totErr)
		}
		tw.Flush()
	}
	t, _ := rep["totals"].(map[string]any)
	fmt.Printf("\ntotals: %d files / %s copied, %d meta-fixed, %d errors, %d fidelity exceptions\n",
		i64(t["files_copied"]), humanBytes(i64(t["bytes_copied"])), i64(t["meta_fixed"]),
		i64(t["errors"]), i64(t["fidelity_exceptions"]))
	fmt.Printf("verify: %d ok, %d fail\n", i64(t["verify_ok"]), i64(t["verify_fail"]))
	fmt.Printf("converged: %v   orphans remaining: %d   delete pass ran: %v\n",
		rep["converged"], i64(rep["orphans_remaining"]), rep["delete_pass_ran"])
	if n := i64(rep["parked_shard_count"]); n > 0 {
		fmt.Printf("PARKED SHARDS: %d (operator attention required — see drsync queue)\n", n)
	}
	return nil
}

func cmdQueue(args []string) error {
	if len(args) > 0 && (args[0] == "retry" || args[0] == "drop") {
		return queueParked(args[0], args[1:])
	}
	fs := flag.NewFlagSet("queue", flag.ExitOnError)
	mk := connFlags(fs)
	fs.Parse(args)
	var out struct {
		Depth []struct {
			Job    string `json:"job"`
			PassNo int    `json:"pass_no"`
			Kind   string `json:"kind"`
			State  string `json:"state"`
			Count  int64  `json:"count"`
		} `json:"depth"`
		Parked []map[string]any `json:"parked"`
	}
	if err := mk().get("/api/v1/queue", &out); err != nil {
		return err
	}
	if len(out.Depth) == 0 {
		fmt.Println("queue empty (no active passes)")
	} else {
		tw := newTable()
		fmt.Fprintln(tw, "JOB\tPASS\tKIND\tSTATE\tSHARDS")
		for _, d := range out.Depth {
			fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%d\n", d.Job, d.PassNo, d.Kind, d.State, d.Count)
		}
		tw.Flush()
	}
	for _, p := range out.Parked {
		fmt.Printf("PARKED shard %v job %v pass %v %v %v: %v (attempt %v, last agent %v)\n",
			p["shard_id"], p["job"], p["pass_no"], p["kind"], p["rel_path"],
			p["error"], p["attempt"], p["last_agent"])
	}
	if len(out.Parked) > 0 {
		fmt.Println("\nresolve with: drsync queue retry <shard-id> | drsync queue drop <shard-id>" +
			"  (or --job <name> for all of a job's parked shards)")
	}
	return nil
}

// queueParked retries or drops PARKED shards: a single shard by id, or every
// parked shard of a job with --job. Retry requeues for a fresh attempt (any
// agent); drop discards the shard, accepting the gap and unblocking the pass.
func queueParked(action string, args []string) error {
	fs := flag.NewFlagSet("queue "+action, flag.ExitOnError)
	mk := connFlags(fs)
	job := fs.String("job", "", "act on ALL parked shards of this job instead of one shard id")
	fs.Parse(args)
	c := mk()
	past := map[string]string{"retry": "retried", "drop": "dropped"}[action]

	if *job != "" {
		if fs.NArg() != 0 {
			return fmt.Errorf("give a shard id or --job, not both")
		}
		var out struct {
			Job   string `json:"job"`
			Count int64  `json:"count"`
		}
		if err := c.post("/api/v1/jobs/"+url.PathEscape(*job)+"/parked/"+action, nil, &out); err != nil {
			return err
		}
		fmt.Printf("%s %d parked shard(s) for job %s\n", past, out.Count, out.Job)
		return nil
	}

	if fs.NArg() != 1 {
		return fmt.Errorf("usage: drsync queue %s <shard-id> | --job <name>", action)
	}
	id := fs.Arg(0)
	var out struct {
		ShardID int64 `json:"shard_id"`
	}
	if err := c.post("/api/v1/parked/"+url.PathEscape(id)+"/"+action, nil, &out); err != nil {
		return err
	}
	fmt.Printf("%s parked shard %d\n", past, out.ShardID)
	return nil
}

func cmdEvents(args []string) error {
	fs := flag.NewFlagSet("events", flag.ExitOnError)
	mk := connFlags(fs)
	jobFilter := fs.String("job", "", "only events for this job")
	fs.Parse(args)
	c := mk()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	conn, err := c.dialEvents(ctx)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	enc := json.NewEncoder(os.Stdout)
	for {
		var ev json.RawMessage
		if err := wsjson.Read(ctx, conn, &ev); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if *jobFilter != "" {
			var probe struct {
				Job string `json:"job"`
			}
			if json.Unmarshal(ev, &probe) == nil && probe.Job != *jobFilter {
				continue
			}
		}
		if err := enc.Encode(ev); err != nil {
			return err
		}
	}
}
