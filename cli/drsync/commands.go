package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
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
	fmt.Fprintln(tw, "PASS\tSTATE\tWALKED\tCOPIED\tBYTES\tMETA\tORPHANS\tVERIFY\tERRORS")
	for _, p := range jv.Passes {
		fmt.Fprintf(tw, "%d\t%s\t%d\t%d\t%s\t%d\t%d\t%s\t%d\n",
			p.PassNo, p.State, p.EntriesWalked, p.FilesCopied,
			humanBytes(p.BytesCopied), p.MetaFixed, p.Orphans,
			verifyCol(p.VerifyOK, p.VerifyFail), p.Errors)
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
	pos := parseFlags(fs, args)

	// Single named job.
	if len(pos) == 1 {
		if *completed || *state != "" || *olderThan != 0 {
			return fmt.Errorf("give a job name OR bulk flags (--completed/--state/--older-than), not both")
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
	for _, n := range res.Purged {
		fmt.Printf("purged %s\n", n)
	}
	fmt.Printf("%d job(s) purged\n", res.Count)
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
	if len(args) < 1 || args[0] != "list" {
		return fmt.Errorf("usage: drsync agent list")
	}
	fs := flag.NewFlagSet("agent list", flag.ExitOnError)
	mk := connFlags(fs)
	fs.Parse(args[1:])
	var agents []struct {
		ID            string `json:"id"`
		Hostname      string `json:"hostname"`
		Version       string `json:"version"`
		State         string `json:"state"`
		Connected     bool   `json:"connected"`
		LastHeartbeat int64  `json:"last_heartbeat_ms"`
	}
	if err := mk().get("/api/v1/agents", &agents); err != nil {
		return err
	}
	tw := newTable()
	fmt.Fprintln(tw, "ID\tHOST\tVERSION\tSTATE\tCONNECTED\tLAST-HEARTBEAT")
	for _, a := range agents {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%v\t%s\n", a.ID, a.Hostname, a.Version,
			a.State, a.Connected, msTime(a.LastHeartbeat))
	}
	return tw.Flush()
}

// ---------------------------------------------------------------------------
// drsync errors / journal cat
// ---------------------------------------------------------------------------

func journalQuery(fs *flag.FlagSet) (pass, path *string, limit, offset *int) {
	pass = fs.String("pass", "", "pass number, or 'all' (default: latest)")
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
		return fmt.Errorf("usage: drsync journal cat <name> [--pass N] [--type T] [--jsonl]")
	}
	fs := flag.NewFlagSet("journal cat", flag.ExitOnError)
	mk := connFlags(fs)
	pass, path, limit, offset := journalQuery(fs)
	typ := fs.String("type", "", "filter by record type: copied, meta_fixed, orphan, deleted, error,\n"+
		"    \tfidelity_exception, nlink_dup, verify_ok, verify_fail, would_copy,\n"+
		"    \twould_delete, src_changed, dir_meta, skipped_clean")
	jsonl := fs.Bool("jsonl", false, "emit raw records as JSON lines")
	pos := parseFlags(fs, args[1:])
	if len(pos) != 1 {
		return fmt.Errorf("journal cat needs a job name")
	}
	extra := url.Values{}
	if *typ != "" {
		extra.Set("type", *typ)
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
		for _, pv := range passes {
			p, _ := pv.(map[string]any)
			fmt.Fprintf(tw, "%v\t%v\t%s\t%v\t%s\t%v\t%s\t%v\n",
				p["pass_no"], p["state"], humanMS(i64(p["duration_ms"])),
				p["delta_files"], humanBytes(i64(p["delta_bytes"])), p["orphans"],
				verifyCol(i64(p["verify_ok"]), i64(p["verify_fail"])), p["errors"])
		}
		tw.Flush()
	}
	t, _ := rep["totals"].(map[string]any)
	fmt.Printf("\ntotals: %v files / %s copied, %v meta-fixed, %v errors, %v fidelity exceptions\n",
		t["files_copied"], humanBytes(i64(t["bytes_copied"])), t["meta_fixed"],
		t["errors"], t["fidelity_exceptions"])
	fmt.Printf("verify: %v ok, %v fail\n", t["verify_ok"], t["verify_fail"])
	fmt.Printf("converged: %v   orphans remaining: %v   delete pass ran: %v\n",
		rep["converged"], rep["orphans_remaining"], rep["delete_pass_ran"])
	if n := i64(rep["parked_shard_count"]); n > 0 {
		fmt.Printf("PARKED SHARDS: %d (operator attention required — see drsync queue)\n", n)
	}
	return nil
}

func cmdQueue(args []string) error {
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
