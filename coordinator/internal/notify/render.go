package notify

import (
	"fmt"
	"html"
	"strings"
)

// PassReport is the data for a per-pass completion email. It is a plain data
// carrier so the notify package has no dependency on store/passctrl internals.
type PassReport struct {
	Job string
	// Src/Dst are the job's source and destination roots. Recipients often
	// watch several migrations at once, and a job name alone does not say
	// which trees moved.
	Src        string
	Dst        string
	PassNo     int
	IsDelete   bool // a delete pass (orphan reclamation) vs a normal sync pass
	DryRun     bool
	DurationMS int64
	// Sync-pass deltas:
	FilesCopied int64
	BytesCopied int64
	MetaFixed   int64
	Orphans     int64 // orphans observed (sync pass) / removed (delete pass)
	VerifyOK    int64
	VerifyFail  int64
	Errors      int64
	// JobDone / Converged are set when this pass was the job's last.
	JobDone   bool
	Converged bool
}

// JobPass is one row of the per-pass trajectory in a job-summary email.
type JobPass struct {
	PassNo     int
	State      string
	IsDelete   bool
	DurationMS int64
	DeltaFiles int64 // copied + meta-fixed
	DeltaBytes int64
	Orphans    int64
	VerifyOK   int64
	VerifyFail int64
	Errors     int64
}

// JobReport is the data for the end-of-job summary email.
type JobReport struct {
	Job              string
	Src              string
	Dst              string
	State            string
	DryRun           bool
	Passes           []JobPass
	FilesCopied      int64
	BytesCopied      int64
	MetaFixed        int64
	Errors           int64
	FidelityExc      int64
	VerifyOK         int64
	VerifyFail       int64
	Converged        bool
	OrphansRemaining int64
	DeletePassRan    bool
	ParkedShards     int
}

// ParkedShardRow is one shard sidelined at its attempt ceiling, as listed in
// the parked-shards alert email.
type ParkedShardRow struct {
	PassNo    int
	Kind      string
	RelPath   string
	Attempt   int
	Error     string
	LastAgent string
}

// ParkedShardsReport is the data for the "job ended with parked shards"
// alert email — sent whenever a job run ends (converged, hit the pass
// ceiling, or a delete pass finished) with shards still parked, independent
// of whether the job spec opts into the routine on_job_complete summary.
type ParkedShardsReport struct {
	Job    string
	Src    string
	Dst    string
	Shards []ParkedShardRow
}

// Palette — a restrained, professional set reused across both templates.
const (
	colInk    = "#1f2933" // primary text / header band
	colMuted  = "#6b7280" // secondary text
	colLine   = "#e5e7eb" // hairlines
	colPanel  = "#f9fafb" // subtle panel fill
	colPage   = "#f4f5f7" // page background
	colGreen  = "#0f7b4f" // healthy
	colAmber  = "#b45309" // attention
	colRed    = "#b91c1c" // problems
	colAccent = "#2563eb" // brand accent
)

// ---------------------------------------------------------------------------
// Per-pass email
// ---------------------------------------------------------------------------

func renderPass(r PassReport) (subject, htmlBody, textBody string) {
	kind := "pass"
	if r.IsDelete {
		kind = "delete pass"
	}
	statusText, statusColor := passStatus(r)
	subject = fmt.Sprintf("%s — %s %d complete (%s)", r.Job, kind, r.PassNo, statusText)

	rows := [][2]string{}
	if r.IsDelete {
		rows = append(rows,
			[2]string{"Orphans removed", commas(r.Orphans)},
			[2]string{"Errors", commas(r.Errors)},
		)
	} else {
		rows = append(rows,
			[2]string{"Files copied", commas(r.FilesCopied)},
			[2]string{"Bytes copied", humanBytes(r.BytesCopied)},
			[2]string{"Metadata fixed", commas(r.MetaFixed)},
			[2]string{"Orphans observed", commas(r.Orphans)},
			[2]string{"Verify", verifyText(r.VerifyOK, r.VerifyFail)},
			[2]string{"Errors", commas(r.Errors)},
		)
	}
	rows = append(rows, [2]string{"Duration", humanDuration(r.DurationMS)})

	title := fmt.Sprintf("%s %d complete", capitalize(kind), r.PassNo)
	var note string
	if r.JobDone {
		if r.Converged {
			note = "This was the final pass — the job has converged and is now COMPLETED."
		} else {
			note = "This was the final pass — the job is now COMPLETED."
		}
	}

	htmlBody = htmlDoc(r.Job, r.Src, r.Dst, title, statusText, statusColor, r.DryRun, note,
		metricsTable(rows), "")
	textBody = textDoc(r.Job, r.Src, r.Dst, title, statusText, r.DryRun, note, rows, nil)
	return
}

func passStatus(r PassReport) (string, string) {
	switch {
	case r.Errors > 0 || r.VerifyFail > 0:
		return "with errors", colRed
	case r.JobDone && r.Converged:
		return "converged", colGreen
	default:
		return "ok", colGreen
	}
}

// ---------------------------------------------------------------------------
// End-of-job summary email
// ---------------------------------------------------------------------------

func renderJob(r JobReport) (subject, htmlBody, textBody string) {
	statusText, statusColor := jobStatus(r)
	subject = fmt.Sprintf("%s — migration complete (%s)", r.Job, statusText)

	rows := [][2]string{
		{"Passes", commas(int64(len(r.Passes)))},
		{"Files copied", commas(r.FilesCopied)},
		{"Bytes copied", humanBytes(r.BytesCopied)},
		{"Metadata fixed", commas(r.MetaFixed)},
		{"Verify", verifyText(r.VerifyOK, r.VerifyFail)},
		{"Fidelity exceptions", commas(r.FidelityExc)},
		{"Errors", commas(r.Errors)},
		{"Converged", yesNo(r.Converged)},
		{"Orphans remaining", commas(r.OrphansRemaining)},
		{"Delete pass ran", yesNo(r.DeletePassRan)},
	}
	if r.ParkedShards > 0 {
		rows = append(rows, [2]string{"Parked shards", commas(int64(r.ParkedShards)) + " — operator attention required"})
	}

	var note string
	if r.ParkedShards > 0 {
		note = fmt.Sprintf("%d shard(s) are parked and need operator attention (see `drsync queue`).", r.ParkedShards)
	} else if !r.Converged {
		note = "The job stopped without reaching a zero-delta fixpoint (pass ceiling reached)."
	}

	htmlBody = htmlDoc(r.Job, r.Src, r.Dst, "Migration complete", statusText, statusColor, r.DryRun, note,
		metricsTable(rows), passTrajectoryHTML(r.Passes))
	textBody = textDoc(r.Job, r.Src, r.Dst, "Migration complete", statusText, r.DryRun, note, rows, r.Passes)
	return
}

func jobStatus(r JobReport) (string, string) {
	switch {
	case r.ParkedShards > 0:
		return "needs attention", colAmber
	case r.Errors > 0 || r.VerifyFail > 0:
		return "with errors", colRed
	case r.Converged:
		return "converged", colGreen
	default:
		return "completed", colGreen
	}
}

// ---------------------------------------------------------------------------
// Parked-shards alert email
// ---------------------------------------------------------------------------

// renderParkedShards builds the dedicated alert sent whenever a job run ends
// with shards still parked at their attempt ceiling — independent of the
// on_job_complete summary, since a parked shard is an operator action item
// (retry after fixing the cause, or drop it) rather than routine reporting.
func renderParkedShards(r ParkedShardsReport) (subject, htmlBody, textBody string) {
	subject = fmt.Sprintf("%s — %d shard(s) parked, operator attention required", r.Job, len(r.Shards))
	note := fmt.Sprintf(
		"%d shard(s) hit their retry ceiling and are parked. Review with `drsync queue` "+
			"or the WebUI's Queue & shards view, then `drsync queue retry <shard-id>` "+
			"(after fixing the underlying cause) or `drsync queue drop <shard-id>` to "+
			"accept the gap.", len(r.Shards))

	htmlBody = htmlDoc(r.Job, r.Src, r.Dst, "Shards parked — action required", "needs attention", colAmber,
		false, note, "", parkedShardsHTML(r.Shards))
	textBody = parkedShardsText(r.Job, r.Src, r.Dst, note, r.Shards)
	return
}

func parkedShardsHTML(shards []ParkedShardRow) string {
	if len(shards) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<div style="font-size:13px;font-weight:600;color:%s;padding:6px 0 8px 0;">Parked shards</div>`, colInk)
	fmt.Fprintf(&b, `<table role="presentation" width="100%%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid %s;border-radius:8px;overflow:hidden;font-size:12px;">`, colLine)
	headers := []string{"Pass", "Kind", "Path", "Attempts", "Last agent", "Error"}
	fmt.Fprintf(&b, `<tr style="background:%s;">`, colInk)
	for _, h := range headers {
		fmt.Fprintf(&b, `<td align="left" style="padding:8px 12px;color:#ffffff;font-weight:600;">%s</td>`, html.EscapeString(h))
	}
	b.WriteString(`</tr>`)
	for i, sh := range shards {
		bg := "#ffffff"
		if i%2 == 1 {
			bg = colPanel
		}
		cells := []string{
			fmt.Sprintf("%d", sh.PassNo), sh.Kind, sh.RelPath,
			fmt.Sprintf("%d", sh.Attempt), sh.LastAgent, sh.Error,
		}
		fmt.Fprintf(&b, `<tr style="background:%s;">`, bg)
		for ci, v := range cells {
			color := colMuted
			style := ""
			if ci == 2 { // path
				color = colInk
				style = "font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;word-break:break-all;"
			} else if ci == 5 { // error
				color = colRed
			}
			fmt.Fprintf(&b, `<td align="left" style="padding:8px 12px;color:%s;border-top:1px solid %s;%s">%s</td>`,
				color, colLine, style, html.EscapeString(v))
		}
		b.WriteString(`</tr>`)
	}
	b.WriteString(`</table>`)
	return b.String()
}

func parkedShardsText(job, src, dst, note string, shards []ParkedShardRow) string {
	var b strings.Builder
	b.WriteString("drsync — Shards parked, action required\n")
	fmt.Fprintf(&b, "job: %s\n", job)
	if src != "" {
		fmt.Fprintf(&b, "source: %s\n", src)
	}
	if dst != "" {
		fmt.Fprintf(&b, "destination: %s\n", dst)
	}
	b.WriteString(strings.Repeat("-", 44) + "\n")
	if len(shards) > 0 {
		b.WriteString("\nParked shards:\n")
		b.WriteString("  pass  kind        path                                     attempts  last agent    error\n")
		for _, sh := range shards {
			fmt.Fprintf(&b, "  %-4d  %-10s  %-40s  %8d  %-12s  %s\n",
				sh.PassNo, truncate(sh.Kind, 10), truncate(sh.RelPath, 40), sh.Attempt,
				truncate(sh.LastAgent, 12), sh.Error)
		}
	}
	if note != "" {
		fmt.Fprintf(&b, "\n%s\n", note)
	}
	b.WriteString("\n--\nAutomated message from the drsync coordinator.\n")
	return b.String()
}

// ---------------------------------------------------------------------------
// HTML rendering — table-based, inline styles (email-client safe)
// ---------------------------------------------------------------------------

func htmlDoc(job, src, dst, title, statusText, statusColor string, dryRun bool, note, metrics, extra string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<div style="margin:0;padding:24px 0;background:%s;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:%s;">`, colPage, colInk)
	fmt.Fprintf(&b, `<table role="presentation" width="100%%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;"><tr><td align="center">`)
	fmt.Fprintf(&b, `<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="width:600px;max-width:600px;border-collapse:collapse;background:#ffffff;border:1px solid %s;border-radius:10px;overflow:hidden;">`, colLine)

	// Header band.
	fmt.Fprintf(&b, `<tr><td style="background:%s;padding:20px 28px;">`, colInk)
	fmt.Fprintf(&b, `<span style="color:#ffffff;font-size:18px;font-weight:700;letter-spacing:0.3px;">drsync</span>`)
	fmt.Fprintf(&b, `<span style="color:#9aa5b1;font-size:13px;font-weight:500;padding-left:10px;">migration notification</span>`)
	b.WriteString(`</td></tr>`)

	// Title + status pill + job.
	b.WriteString(`<tr><td style="padding:28px 28px 8px 28px;">`)
	fmt.Fprintf(&b, `<div style="font-size:20px;font-weight:700;color:%s;">%s</div>`, colInk, html.EscapeString(title))
	fmt.Fprintf(&b, `<div style="font-size:14px;color:%s;padding-top:4px;">job <span style="font-weight:600;color:%s;">%s</span></div>`,
		colMuted, colInk, html.EscapeString(job))
	// Paths wrap rather than overflow: migration roots are often long, and a
	// clipped path is worse than a wrapped one when the point is to identify
	// which trees moved.
	if src != "" || dst != "" {
		fmt.Fprintf(&b, `<div style="font-size:13px;color:%s;padding-top:8px;line-height:1.6;word-break:break-all;">`, colMuted)
		if src != "" {
			fmt.Fprintf(&b, `<div>source <span style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color:%s;">%s</span></div>`,
				colInk, html.EscapeString(src))
		}
		if dst != "" {
			fmt.Fprintf(&b, `<div>destination <span style="font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color:%s;">%s</span></div>`,
				colInk, html.EscapeString(dst))
		}
		b.WriteString(`</div>`)
	}
	fmt.Fprintf(&b, `<div style="padding-top:14px;"><span style="display:inline-block;background:%s;color:#ffffff;font-size:12px;font-weight:600;padding:5px 12px;border-radius:999px;text-transform:uppercase;letter-spacing:0.4px;">%s</span>`,
		statusColor, html.EscapeString(statusText))
	if dryRun {
		fmt.Fprintf(&b, `<span style="display:inline-block;background:%s;color:#ffffff;font-size:12px;font-weight:600;padding:5px 12px;border-radius:999px;margin-left:8px;text-transform:uppercase;letter-spacing:0.4px;">dry run</span>`, colMuted)
	}
	b.WriteString(`</div></td></tr>`)

	// Metrics.
	fmt.Fprintf(&b, `<tr><td style="padding:16px 28px 8px 28px;">%s</td></tr>`, metrics)

	if extra != "" {
		fmt.Fprintf(&b, `<tr><td style="padding:8px 28px 8px 28px;">%s</td></tr>`, extra)
	}

	if note != "" {
		fmt.Fprintf(&b, `<tr><td style="padding:8px 28px 20px 28px;"><div style="background:%s;border-left:3px solid %s;border-radius:4px;padding:12px 14px;font-size:13px;color:%s;">%s</div></td></tr>`,
			colPanel, colAccent, colInk, html.EscapeString(note))
	}

	// Footer.
	fmt.Fprintf(&b, `<tr><td style="padding:18px 28px;border-top:1px solid %s;">`, colLine)
	fmt.Fprintf(&b, `<div style="font-size:12px;color:%s;">Automated message from the drsync coordinator. You are receiving this because this job's spec lists you as a notification recipient.</div>`, colMuted)
	b.WriteString(`</td></tr>`)

	b.WriteString(`</table></td></tr></table></div>`)
	return b.String()
}

func metricsTable(rows [][2]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<table role="presentation" width="100%%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid %s;border-radius:8px;overflow:hidden;">`, colLine)
	for i, row := range rows {
		bg := "#ffffff"
		if i%2 == 1 {
			bg = colPanel
		}
		fmt.Fprintf(&b, `<tr style="background:%s;">`, bg)
		fmt.Fprintf(&b, `<td style="padding:10px 16px;font-size:13px;color:%s;border-bottom:1px solid %s;">%s</td>`,
			colMuted, colLine, html.EscapeString(row[0]))
		fmt.Fprintf(&b, `<td align="right" style="padding:10px 16px;font-size:13px;font-weight:600;color:%s;border-bottom:1px solid %s;font-variant-numeric:tabular-nums;">%s</td>`,
			colInk, colLine, html.EscapeString(row[1]))
		b.WriteString(`</tr>`)
	}
	b.WriteString(`</table>`)
	return b.String()
}

func passTrajectoryHTML(passes []JobPass) string {
	if len(passes) == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<div style="font-size:13px;font-weight:600;color:%s;padding:6px 0 8px 0;">Pass trajectory</div>`, colInk)
	fmt.Fprintf(&b, `<table role="presentation" width="100%%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid %s;border-radius:8px;overflow:hidden;font-size:12px;">`, colLine)
	headers := []string{"Pass", "State", "Duration", "Δ files", "Δ bytes", "Orphans", "Verify", "Errors"}
	fmt.Fprintf(&b, `<tr style="background:%s;">`, colInk)
	for i, h := range headers {
		align := "left"
		if i >= 2 {
			align = "right"
		}
		fmt.Fprintf(&b, `<td align="%s" style="padding:8px 12px;color:#ffffff;font-weight:600;">%s</td>`, align, html.EscapeString(h))
	}
	b.WriteString(`</tr>`)
	for i, p := range passes {
		bg := "#ffffff"
		if i%2 == 1 {
			bg = colPanel
		}
		label := fmt.Sprintf("%d", p.PassNo)
		if p.IsDelete {
			label += " (del)"
		}
		cells := []struct {
			v     string
			align string
		}{
			{label, "left"},
			{p.State, "left"},
			{humanDuration(p.DurationMS), "right"},
			{commas(p.DeltaFiles), "right"},
			{humanBytes(p.DeltaBytes), "right"},
			{commas(p.Orphans), "right"},
			{verifyText(p.VerifyOK, p.VerifyFail), "right"},
			{commas(p.Errors), "right"},
		}
		fmt.Fprintf(&b, `<tr style="background:%s;">`, bg)
		for _, c := range cells {
			color := colInk
			if c.align == "left" {
				color = colMuted
			}
			fmt.Fprintf(&b, `<td align="%s" style="padding:8px 12px;color:%s;border-top:1px solid %s;font-variant-numeric:tabular-nums;">%s</td>`,
				c.align, color, colLine, html.EscapeString(c.v))
		}
		b.WriteString(`</tr>`)
	}
	b.WriteString(`</table>`)
	return b.String()
}

// ---------------------------------------------------------------------------
// Plain-text rendering (multipart/alternative fallback)
// ---------------------------------------------------------------------------

func textDoc(job, src, dst, title, statusText string, dryRun bool, note string, rows [][2]string, passes []JobPass) string {
	var b strings.Builder
	fmt.Fprintf(&b, "drsync — %s\n", title)
	fmt.Fprintf(&b, "job: %s\n", job)
	if src != "" {
		fmt.Fprintf(&b, "source: %s\n", src)
	}
	if dst != "" {
		fmt.Fprintf(&b, "destination: %s\n", dst)
	}
	fmt.Fprintf(&b, "status: %s\n", statusText)
	if dryRun {
		b.WriteString("mode: DRY RUN (no data modified)\n")
	}
	b.WriteString(strings.Repeat("-", 44) + "\n")
	width := 0
	for _, r := range rows {
		if len(r[0]) > width {
			width = len(r[0])
		}
	}
	for _, r := range rows {
		fmt.Fprintf(&b, "  %-*s  %s\n", width, r[0], r[1])
	}
	if len(passes) > 0 {
		b.WriteString("\nPass trajectory:\n")
		b.WriteString("  pass  state       duration      Δfiles      Δbytes  orphans  verify        errors\n")
		for _, p := range passes {
			label := fmt.Sprintf("%d", p.PassNo)
			if p.IsDelete {
				label += "d"
			}
			fmt.Fprintf(&b, "  %-4s  %-10s  %8s  %8s  %10s  %7s  %-12s  %6s\n",
				label, truncate(p.State, 10), humanDuration(p.DurationMS), commas(p.DeltaFiles), humanBytes(p.DeltaBytes),
				commas(p.Orphans), verifyText(p.VerifyOK, p.VerifyFail), commas(p.Errors))
		}
	}
	if note != "" {
		fmt.Fprintf(&b, "\n%s\n", note)
	}
	b.WriteString("\n--\nAutomated message from the drsync coordinator.\n")
	return b.String()
}

// ---------------------------------------------------------------------------
// Formatting helpers — never scientific notation, always readable integers.
// ---------------------------------------------------------------------------

func commas(n int64) string {
	neg := n < 0
	if neg {
		n = -n
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		if neg {
			return "-" + s
		}
		return s
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func humanDuration(ms int64) string {
	if ms <= 0 {
		return "0s"
	}
	s := ms / 1000
	if s < 60 {
		return fmt.Sprintf("%ds", s)
	}
	m := s / 60
	s %= 60
	if m < 60 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	h := m / 60
	m %= 60
	return fmt.Sprintf("%dh %dm", h, m)
}

func verifyText(ok, fail int64) string {
	if fail > 0 {
		return fmt.Sprintf("%s ok / %s FAIL", commas(ok), commas(fail))
	}
	return fmt.Sprintf("%s ok", commas(ok))
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
