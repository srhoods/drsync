package notify

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "smtp.yaml")
	if err := os.WriteFile(path, []byte("host: smtp.example.com\nfrom: drsync <drsync@example.com>\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 587 { // starttls default port
		t.Errorf("default port = %d, want 587", cfg.Port)
	}
	if cfg.Security != "starttls" {
		t.Errorf("default security = %q, want starttls", cfg.Security)
	}
	if cfg.SubjectPrefix != "[drsync]" {
		t.Errorf("default subject prefix = %q", cfg.SubjectPrefix)
	}
	if cfg.TimeoutSeconds != 30 {
		t.Errorf("default timeout = %d", cfg.TimeoutSeconds)
	}
}

func TestLoadConfigImplicitTLSPort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "smtp.yaml")
	os.WriteFile(path, []byte("host: h\nfrom: a@b.com\nsecurity: tls\n"), 0o600)
	cfg, err := LoadConfig(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 465 {
		t.Errorf("implicit-tls default port = %d, want 465", cfg.Port)
	}
}

func TestLoadConfigMissing(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.yaml")
	// missingOK=true → (nil, nil)
	cfg, err := LoadConfig(missing, true)
	if err != nil || cfg != nil {
		t.Fatalf("missingOK: got cfg=%v err=%v, want nil,nil", cfg, err)
	}
	// missingOK=false → error
	if _, err := LoadConfig(missing, false); err == nil {
		t.Fatal("missing required config should error")
	}
}

func TestLoadConfigRejectsUnknownField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "smtp.yaml")
	os.WriteFile(path, []byte("host: h\nfrom: a@b.com\nbogus: 1\n"), 0o600)
	if _, err := LoadConfig(path, false); err == nil {
		t.Fatal("unknown field should be rejected (KnownFields)")
	}
}

func TestLoadConfigValidation(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct{ name, body string }{
		{"no-host", "from: a@b.com\n"},
		{"no-from", "host: h\n"},
		{"bad-from", "host: h\nfrom: not-an-address\n"},
		{"bad-security", "host: h\nfrom: a@b.com\nsecurity: quantum\n"},
	} {
		path := filepath.Join(dir, tc.name+".yaml")
		os.WriteFile(path, []byte(tc.body), 0o600)
		if _, err := LoadConfig(path, false); err == nil {
			t.Errorf("%s: expected validation error", tc.name)
		}
	}
}

// A nil Sender must be safe to call — callers hold nil when email is disabled.
func TestNilSenderIsInert(t *testing.T) {
	var s *Sender
	if s.Enabled() {
		t.Fatal("nil sender should not be enabled")
	}
	s.PassComplete([]string{"a@b.com"}, PassReport{})       // must not panic
	s.JobComplete([]string{"a@b.com"}, JobReport{})         // must not panic
	s.ParkedShards([]string{"a@b.com"}, ParkedShardsReport{ // must not panic
		Shards: []ParkedShardRow{{PassNo: 1}},
	})
}

func TestBuildMIMEStructure(t *testing.T) {
	msg, err := buildMIME("drsync <d@example.com>", []string{"ops@x.com", "lead@x.com"},
		"[drsync] test subject", "plain body", "<b>html body</b>")
	if err != nil {
		t.Fatal(err)
	}
	s := string(msg)
	for _, want := range []string{
		"From: drsync <d@example.com>",
		"To: ops@x.com, lead@x.com",
		"MIME-Version: 1.0",
		"multipart/alternative; boundary=",
		"Content-Type: text/plain; charset=UTF-8",
		"Content-Type: text/html; charset=UTF-8",
		"Message-ID: <",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("MIME missing %q\n---\n%s", want, s)
		}
	}
	// CRLF line endings are required by SMTP.
	if !strings.Contains(s, "\r\n") {
		t.Error("MIME must use CRLF line endings")
	}
}

func TestRenderPassNoSciNotation(t *testing.T) {
	// A big count that %v on a float64 would render as 1.5e+08.
	subject, htmlBody, textBody := renderPass(PassReport{
		Job: "bigmigrate", PassNo: 2, FilesCopied: 150_000_000,
		BytesCopied: 5 << 40, VerifyOK: 149_999_999, DurationMS: 3_725_000,
	})
	for _, body := range []string{subject, htmlBody, textBody} {
		if strings.ContainsAny(body, "eE+") && strings.Contains(body, "e+") {
			t.Errorf("scientific notation leaked: %q", body)
		}
	}
	if !strings.Contains(textBody, "150,000,000") {
		t.Errorf("expected grouped digits in text body:\n%s", textBody)
	}
	if !strings.Contains(htmlBody, "150,000,000") {
		t.Errorf("expected grouped digits in html body")
	}
	if !strings.Contains(subject, "pass 2 complete") {
		t.Errorf("subject = %q", subject)
	}
}

func TestRenderJobSummary(t *testing.T) {
	subject, htmlBody, textBody := renderJob(JobReport{
		Job: "prod-cutover", State: "COMPLETED", Converged: true,
		Passes: []JobPass{
			{PassNo: 1, State: "COMPLETE", DeltaFiles: 1_200_000, DeltaBytes: 3 << 40, VerifyOK: 12000},
			{PassNo: 2, State: "COMPLETE", DeltaFiles: 0, VerifyOK: 0},
		},
		FilesCopied: 1_200_000, BytesCopied: 3 << 40, VerifyOK: 12000,
	})
	if !strings.Contains(subject, "migration complete") {
		t.Errorf("subject = %q", subject)
	}
	if !strings.Contains(htmlBody, "Pass trajectory") {
		t.Error("job html should include a pass trajectory table")
	}
	if !strings.Contains(textBody, "1,200,000") {
		t.Errorf("expected grouped digits:\n%s", textBody)
	}
	if !strings.Contains(htmlBody, "prod-cutover") {
		t.Error("job name should appear in html")
	}
}

// The pass-trajectory table in the job summary must show each pass's
// duration, not just its deltas — an operator judging whether a slow
// convergence is worth investigating needs to see where the time went.
func TestRenderJobSummaryShowsPassDuration(t *testing.T) {
	_, htmlBody, textBody := renderJob(JobReport{
		Job: "prod-cutover", State: "COMPLETED", Converged: true,
		Passes: []JobPass{
			{PassNo: 1, State: "COMPLETE", DurationMS: 3_725_000, DeltaFiles: 100},
			{PassNo: 2, State: "COMPLETE", DurationMS: 45_000, DeltaFiles: 0},
		},
	})
	if !strings.Contains(htmlBody, "Duration") {
		t.Error("html pass trajectory should have a Duration column header")
	}
	if !strings.Contains(htmlBody, "1h 2m") {
		t.Errorf("html should show pass 1's duration (1h 2m):\n%s", htmlBody)
	}
	if !strings.Contains(htmlBody, "45s") {
		t.Errorf("html should show pass 2's duration (45s):\n%s", htmlBody)
	}
	if !strings.Contains(textBody, "duration") {
		t.Error("text pass trajectory should have a duration column header")
	}
	if !strings.Contains(textBody, "1h 2m") || !strings.Contains(textBody, "45s") {
		t.Errorf("text body missing pass durations:\n%s", textBody)
	}
}

func TestRenderParkedShardsAlert(t *testing.T) {
	subject, htmlBody, textBody := renderParkedShards(ParkedShardsReport{
		Job: "prod-cutover", Src: "/mnt/gpfs/home", Dst: "/mnt/weka/home",
		Shards: []ParkedShardRow{
			{PassNo: 2, Kind: "entrylist", RelPath: "deep/tree/broken", Attempt: 5,
				Error: "EIO", LastAgent: "agent-3"},
			{PassNo: 2, Kind: "chunk", RelPath: "big.bin", Attempt: 5,
				Error: "ESTALE", LastAgent: "agent-1"},
		},
	})
	if !strings.Contains(subject, "2 shard(s) parked") {
		t.Errorf("subject = %q", subject)
	}
	if !strings.Contains(subject, "prod-cutover") {
		t.Errorf("subject should name the job: %q", subject)
	}
	for _, want := range []string{"deep/tree/broken", "big.bin", "EIO", "ESTALE", "agent-3", "agent-1"} {
		if !strings.Contains(htmlBody, want) {
			t.Errorf("html body missing %q", want)
		}
		if !strings.Contains(textBody, want) {
			t.Errorf("text body missing %q", want)
		}
	}
	if !strings.Contains(htmlBody, "/mnt/gpfs/home") || !strings.Contains(htmlBody, "/mnt/weka/home") {
		t.Error("html body should carry the job's source/destination paths")
	}
	if !strings.Contains(htmlBody, "drsync queue") {
		t.Error("body should point the operator at `drsync queue`")
	}
}

// A parked shard's rel_path and error come from the migration tree / agent,
// not the operator, so they must be escaped like every other interpolated
// value reaching the HTML body.
func TestRenderParkedShardsEscapesPath(t *testing.T) {
	_, body, _ := renderParkedShards(ParkedShardsReport{
		Job: "j",
		Shards: []ParkedShardRow{
			{PassNo: 1, Kind: "chunk", RelPath: `<img src=x onerror=alert(1)>`, Error: "EIO"},
		},
	})
	if strings.Contains(body, "<img src=x") {
		t.Error("rel_path reached the HTML body unescaped")
	}
	if !strings.Contains(body, "&lt;img src=x") {
		t.Error("rel_path missing from the body in escaped form")
	}
}

func TestErrorStatusColoring(t *testing.T) {
	_, color := passStatus(PassReport{Errors: 3})
	if color != colRed {
		t.Errorf("errors should color red, got %s", color)
	}
	txt, _ := passStatus(PassReport{JobDone: true, Converged: true})
	if txt != "converged" {
		t.Errorf("converged status text = %q", txt)
	}
}

func TestFormatting(t *testing.T) {
	if got := commas(1_234_567); got != "1,234,567" {
		t.Errorf("commas(1234567) = %q", got)
	}
	if got := commas(-42_000); got != "-42,000" {
		t.Errorf("commas(-42000) = %q", got)
	}
	if got := commas(999); got != "999" {
		t.Errorf("commas(999) = %q", got)
	}
	if got := humanBytes(1536); got != "1.5 KiB" {
		t.Errorf("humanBytes(1536) = %q", got)
	}
	if got := humanDuration(3_725_000); got != "1h 2m" {
		t.Errorf("humanDuration = %q", got)
	}
	if got := humanDuration(45_000); got != "45s" {
		t.Errorf("humanDuration(45s) = %q", got)
	}
}

// Both email types carry the source and destination roots. Recipients often
// watch several migrations at once, where a job name alone does not say which
// trees moved — and for a delete pass, which tree is about to lose files.
func TestRenderCarriesPaths(t *testing.T) {
	const src, dst = "/mnt/gpfs/home", "/mnt/weka/home"

	_, passHTML, passText := renderPass(PassReport{
		Job: "home", Src: src, Dst: dst, PassNo: 2, FilesCopied: 5,
	})
	_, jobHTML, jobText := renderJob(JobReport{
		Job: "home", Src: src, Dst: dst, State: "COMPLETED", Converged: true,
	})

	for _, c := range []struct{ name, body string }{
		{"pass html", passHTML}, {"pass text", passText},
		{"job html", jobHTML}, {"job text", jobText},
	} {
		if !strings.Contains(c.body, src) {
			t.Errorf("%s is missing the source path", c.name)
		}
		if !strings.Contains(c.body, dst) {
			t.Errorf("%s is missing the destination path", c.name)
		}
	}
}

// Paths are operator-supplied and land in an HTML body, so they are escaped
// like every other interpolated value.
func TestRenderEscapesPaths(t *testing.T) {
	_, body, _ := renderPass(PassReport{Job: "j", Src: `/mnt/<img src=x>`, Dst: "/d"})
	if strings.Contains(body, "<img src=x") {
		t.Error("source path reached the HTML body unescaped")
	}
	if !strings.Contains(body, "&lt;img src=x") {
		t.Error("source path missing from the body in escaped form")
	}
}

// A job whose spec somehow carries no paths must render without empty labels
// dangling in the output.
func TestRenderOmitsBlankPaths(t *testing.T) {
	_, body, text := renderPass(PassReport{Job: "j", PassNo: 1})
	if strings.Contains(text, "source:") || strings.Contains(text, "destination:") {
		t.Error("text body prints empty path labels")
	}
	if strings.Contains(body, ">source ") || strings.Contains(body, ">destination ") {
		t.Error("html body prints empty path labels")
	}
}
