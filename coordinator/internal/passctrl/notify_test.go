package passctrl

import (
	"bufio"
	"encoding/binary"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	"drsync/coordinator/internal/journal"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/notify"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// writeMixedJournal seeds (jobID, passNo) with one record per (type, relPath)
// pair — unlike seedverify_test.go's writeJournal (JR_COPIED only), this lets
// a test build a journal with several record types to check the type
// histogram buildJobReport derives from it.
func writeMixedJournal(t *testing.T, root string, jobID int64, passNo int, byType map[drsyncpb.JournalRecord_Type]int) {
	t.Helper()
	w, err := journal.NewWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	var count int
	i := 0
	for typ, n := range byType {
		for j := 0; j < n; j++ {
			rec := &drsyncpb.JournalRecord{Type: typ, RelPath: []byte(strconv.Itoa(i))}
			i++
			b, err := proto.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}
			var hdr [binary.MaxVarintLen64]byte
			hn := binary.PutUvarint(hdr[:], uint64(len(b)))
			raw = append(raw, hdr[:hn]...)
			raw = append(raw, b...)
			count++
		}
	}
	if count > 0 {
		if err := w.Append(&drsyncpb.JournalBatch{
			JobId: uint64(jobID), PassNo: uint32(passNo),
			RecordCount: uint32(count), RecordsZstd: enc.EncodeAll(raw, nil),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// mockSMTP speaks just enough SMTP (no TLS, no auth) to accept messages and
// hand each one's raw DATA back over the returned channel. A local copy
// rather than reusing notify's (package-private, deliver_test.go) so
// passctrl can drive a real notify.Sender end to end without exporting
// test-only plumbing.
func mockSMTP(t *testing.T) (host string, port int, got chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	got = make(chan string, 8)
	go func() {
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveOneSMTPConn(conn, got)
		}
	}()
	h, p, _ := net.SplitHostPort(ln.Addr().String())
	port, _ = strconv.Atoi(p)
	t.Cleanup(func() { ln.Close() })
	return h, port, got
}

func serveOneSMTPConn(conn net.Conn, got chan string) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	write := func(s string) { w.WriteString(s + "\r\n"); w.Flush() }

	write("220 mock ESMTP")
	var body strings.Builder
	inData := false
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		if inData {
			if line == ".\r\n" {
				inData = false
				write("250 OK queued")
				continue
			}
			body.WriteString(line)
			continue
		}
		cmd := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
			write("250 mock")
		case strings.HasPrefix(cmd, "MAIL FROM"), strings.HasPrefix(cmd, "RCPT TO"):
			write("250 OK")
		case strings.HasPrefix(cmd, "DATA"):
			write("354 End data with <CR><LF>.<CR><LF>")
			inData = true
		case strings.HasPrefix(cmd, "QUIT"):
			write("221 Bye")
			got <- body.String()
			return
		default:
			write("250 OK")
		}
	}
}

func setNotifier(t *testing.T, c *Controller) chan string {
	t.Helper()
	return setNotifierWithRollup(t, c, 0)
}

// setNotifierWithRollup is setNotifier with an explicit parked-shard rollup
// window, for tests that exercise the hold-and-merge behavior in
// checkParkedShards. rollupSeconds: 0 means "no rollup" (every new park
// sends immediately), matching every pre-existing test in this file.
func setNotifierWithRollup(t *testing.T, c *Controller, rollupSeconds int) chan string {
	t.Helper()
	host, port, got := mockSMTP(t)
	c.SetNotifier(notify.NewSender(&notify.Config{
		Host: host, Port: port, Security: "none", From: "drsync <d@example.com>", TimeoutSeconds: 2,
		ParkedShardRollupSeconds: rollupSeconds,
	}))
	return got
}

// notifyTestSpec is baseSpec plus notifications.recipients so the wiring
// under test (checkParkedShards) actually fires.
func notifyTestSpec(extra string) []byte {
	return []byte(baseSpec + "  notifications:\n    recipients: [\"ops@example.com\"]\n" + extra)
}

// parkOneShard drives a real shard through insert → lease → park, so the
// fixture matches what production code actually produces rather than an
// SQL-level shortcut.
func parkOneShard(t *testing.T, c *Controller, pass *store.Pass, relPath string) {
	t.Helper()
	ids, err := c.st.InsertShards(pass.ID, 0, []store.NewShard{
		{Kind: model.KindEntryList, RelPath: relPath},
	})
	if err != nil || len(ids) != 1 {
		t.Fatalf("InsertShards: ids=%v err=%v", ids, err)
	}
	leased, err := c.st.LeaseShards("agent-1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var leaseID int64
	for _, sh := range leased {
		if sh.ID == ids[0] {
			leaseID = sh.LeaseID
		}
	}
	if leaseID == 0 {
		t.Fatalf("shard %d was not leased (leased=%v)", ids[0], leased)
	}
	if err := c.st.ParkShard(ids[0], leaseID, "EIO"); err != nil {
		t.Fatal(err)
	}
}

func awaitEmail(t *testing.T, got chan string) string {
	t.Helper()
	select {
	case body := <-got:
		return body
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for an email")
		return ""
	}
}

func assertNoEmail(t *testing.T, got chan string) {
	t.Helper()
	select {
	case body := <-got:
		t.Fatalf("expected no email, got:\n%s", body)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestCheckParkedShardsNoRecipientsIsNoop(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, withConverge("    max: 1\n")) // no notifications block
	got := setNotifier(t, c)

	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	parkOneShard(t, c, pass, "some/path")

	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	assertNoEmail(t, got)
}

func TestCheckParkedShardsSendsWhenShardsParked(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, notifyTestSpec(""))
	got := setNotifier(t, c)

	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	parkOneShard(t, c, pass, "deep/tree/broken")

	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	body := awaitEmail(t, got)
	if !strings.Contains(body, "deep/tree/broken") {
		t.Errorf("email body missing the parked shard's path:\n%s", body)
	}
}

func TestCheckParkedShardsNoneParkedIsNoop(t *testing.T) {
	c := newController(t)
	makeJob(t, c, notifyTestSpec(""))
	got := setNotifier(t, c)

	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	assertNoEmail(t, got)
}

// A shard already alerted on must not be re-emailed on a later tick while it
// stays parked — otherwise a job stuck for hours with N parked shards would
// send a fresh digest every tick forever.
func TestCheckParkedShardsDedupesAcrossTicks(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, notifyTestSpec(""))
	got := setNotifier(t, c)

	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	parkOneShard(t, c, pass, "still/stuck")

	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	awaitEmail(t, got) // first tick: alerted

	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	assertNoEmail(t, got) // second tick: same shard, no repeat
}

// A shard that parks, gets retried (leaves parked state), and later parks
// again is a *new* incident and must be alerted on again — the dedupe map
// must not permanently suppress a shard ID.
func TestCheckParkedShardsReAlertsAfterRetry(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, notifyTestSpec(""))
	got := setNotifier(t, c)

	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := c.st.InsertShards(pass.ID, 0, []store.NewShard{
		{Kind: model.KindEntryList, RelPath: "flaky/shard"},
	})
	if err != nil || len(ids) != 1 {
		t.Fatalf("InsertShards: ids=%v err=%v", ids, err)
	}
	leaseAndPark := func() {
		leased, err := c.st.LeaseShards("agent-1", 10, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		var leaseID int64
		for _, sh := range leased {
			if sh.ID == ids[0] {
				leaseID = sh.LeaseID
			}
		}
		if leaseID == 0 {
			t.Fatalf("shard %d was not leased (leased=%v)", ids[0], leased)
		}
		if err := c.st.ParkShard(ids[0], leaseID, "EIO"); err != nil {
			t.Fatal(err)
		}
	}

	leaseAndPark()
	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	awaitEmail(t, got)

	if err := c.st.RetryParkedShard(ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := c.checkParkedShards(); err != nil { // shard no longer parked
		t.Fatal(err)
	}
	assertNoEmail(t, got)

	leaseAndPark() // parks again — a fresh incident
	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	awaitEmail(t, got)
}

// Several shards parking together (e.g. a mount going unhealthy mid-walk)
// must collapse into one email per job, not one email per shard.
func TestCheckParkedShardsBatchesPerJob(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, notifyTestSpec(""))
	got := setNotifier(t, c)

	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	parkOneShard(t, c, pass, "a/one")
	parkOneShard(t, c, pass, "b/two")
	parkOneShard(t, c, pass, "c/three")

	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	body := awaitEmail(t, got)
	for _, want := range []string{"a/one", "b/two", "c/three"} {
		if !strings.Contains(body, want) {
			t.Errorf("digest missing %q:\n%s", want, body)
		}
	}
	assertNoEmail(t, got) // exactly one email, not three
}

// A burst that trickles in slower than one tick — e.g. a degrading mount
// parking a shard every few seconds over minutes — must not send an email
// per shard: the first park alerts immediately (operators still hear about
// the incident right away), but anything more that parks within the rollup
// window is held and merged into one follow-up, not fired individually.
func TestCheckParkedShardsRollsUpTrickleWithinWindow(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, notifyTestSpec(""))
	got := setNotifierWithRollup(t, c, 1) // 1s window, so the test runs fast

	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}

	parkOneShard(t, c, pass, "first/park")
	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	body := awaitEmail(t, got) // first park in a quiet job: immediate
	if !strings.Contains(body, "first/park") {
		t.Fatalf("first alert missing first/park:\n%s", body)
	}

	// Two more shards park inside the still-open rollup window (well under
	// 1s after the first email) — checkParkedShards runs on each but must
	// NOT email either one individually.
	parkOneShard(t, c, pass, "second/park")
	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	assertNoEmail(t, got)

	parkOneShard(t, c, pass, "third/park")
	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	assertNoEmail(t, got)

	// Window elapses; the next check flushes both held shards together in
	// ONE email, not two.
	time.Sleep(1100 * time.Millisecond)
	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	body = awaitEmail(t, got)
	for _, want := range []string{"second/park", "third/park"} {
		if !strings.Contains(body, want) {
			t.Errorf("rollup email missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "first/park") {
		t.Errorf("rollup email re-sent the already-alerted first/park:\n%s", body)
	}
	assertNoEmail(t, got) // exactly one rollup email, not two
}

// A job that keeps parking shards throughout the rollup window must still
// get an email once the window closes — checkParkedShards must flush a
// job's held batch on the tick the window elapses even if that particular
// tick brings no new park, otherwise a burst that stops right at the edge
// could wait indefinitely for a park that never comes.
func TestCheckParkedShardsFlushesOnWindowElapseWithoutNewPark(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, notifyTestSpec(""))
	got := setNotifierWithRollup(t, c, 1)

	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}

	parkOneShard(t, c, pass, "immediate")
	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	awaitEmail(t, got)

	parkOneShard(t, c, pass, "held")
	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	assertNoEmail(t, got) // held, inside the window

	time.Sleep(1100 * time.Millisecond)
	// No new shard parks on this tick — only the window elapsing should
	// trigger the flush.
	if err := c.checkParkedShards(); err != nil {
		t.Fatal(err)
	}
	body := awaitEmail(t, got)
	if !strings.Contains(body, "held") {
		t.Errorf("window-elapse flush missing the held shard:\n%s", body)
	}
}

func TestJobParkedShardsFiltersByJob(t *testing.T) {
	c := newController(t)
	job1, err := c.st.CreateJob("j1", baseSpec2("j1"), false, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.st.SetJobState(job1.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	job2, err := c.st.CreateJob("j2", baseSpec2("j2"), false, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.st.SetJobState(job2.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}

	p1, err := c.st.CreatePass(job1.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := c.st.CreatePass(job2.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	parkOneShard(t, c, p1, "job1/only")
	parkOneShard(t, c, p2, "job2/only")

	parked, err := c.jobParkedShards(job1.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(parked) != 1 || parked[0].RelPath != "job1/only" {
		t.Fatalf("expected exactly job1's parked shard, got %+v", parked)
	}
}

// baseSpec2 returns a minimal valid spec for a job named name, with a
// destination unique to that name so two jobs from this helper don't trip
// the destination-overlap guard when both are RUNNING at once.
func baseSpec2(name string) []byte {
	s := strings.ReplaceAll(baseSpec, "name: t1", "name: "+name)
	s = strings.ReplaceAll(s, "destination: { path: /dst }", "destination: { path: /dst-"+name+" }")
	return []byte(s)
}

// buildJobReport's JournalSummary must be the type histogram across every
// pass of the job — the same figures `drsync journal cat <name> --summary`
// reports — not just the store-column totals already covered by the rest of
// the report.
// buildJobReport's JournalSummary comes from the store.JournalTypeCounts
// rollup, not a live journal scan — recordJournalTypeCounts is what populates
// that rollup (tested separately below), so this seeds it directly the same
// way that code path would have, and checks buildJobReport reads it back
// correctly summed across passes.
func TestBuildJobReportJournalSummary(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	p1, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := c.st.CreatePass(job.ID, 2, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.st.SetJournalTypeCounts(p1.ID, map[string]int64{"COPIED": 5, "ORPHAN": 2}); err != nil {
		t.Fatal(err)
	}
	if err := c.st.SetJournalTypeCounts(p2.ID, map[string]int64{"COPIED": 3, "ERROR": 1}); err != nil {
		t.Fatal(err)
	}

	rep, err := c.buildJobReport(job)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"COPIED": 8, "ORPHAN": 2, "ERROR": 1}
	for k, v := range want {
		if rep.JournalSummary[k] != v {
			t.Errorf("JournalSummary[%q] = %d, want %d (full: %+v)", k, rep.JournalSummary[k], v, rep.JournalSummary)
		}
	}
	if len(rep.JournalSummary) != len(want) {
		t.Errorf("JournalSummary has extra/missing types: %+v", rep.JournalSummary)
	}
}

// A pass whose journal-type-counts rollup was never written (e.g. a pass that
// never reached COMPLETE) must not fail the whole report — JournalSummary
// just comes back empty.
func TestBuildJobReportNoJournalIsEmptySummary(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	if _, err := c.st.CreatePass(job.ID, 1, model.PassScanning); err != nil {
		t.Fatal(err)
	}
	rep, err := c.buildJobReport(job)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.JournalSummary) != 0 {
		t.Errorf("expected empty JournalSummary for a pass with no rollup written, got %+v", rep.JournalSummary)
	}
}

// recordJournalTypeCounts is what actually scans the journal — once, when a
// pass reaches COMPLETE — and persists the histogram store.JournalTypeCounts
// later reads. This is the one place a real writeMixedJournal fixture belongs
// now: everything downstream (buildJobReport, /report) reads the rollup, not
// the journal.
func TestRecordJournalTypeCountsPersistsRollup(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	writeMixedJournal(t, c.journalRoot, job.ID, pass.PassNo, map[drsyncpb.JournalRecord_Type]int{
		drsyncpb.JournalRecord_JR_COPIED: 5,
		drsyncpb.JournalRecord_JR_ORPHAN: 2,
	})

	c.recordJournalTypeCounts(job, pass)

	got, total, err := c.st.JournalTypeCounts(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got["COPIED"] != 5 || got["ORPHAN"] != 2 || total != 7 {
		t.Errorf("JournalTypeCounts = %+v (total %d), want COPIED:5 ORPHAN:2 (total 7)", got, total)
	}
}

// A scan failure (e.g. a segment read error) must not propagate — the phase
// transition that already committed (SetPassState) must not be undone or
// blocked by a best-effort summary failing to compute.
func TestRecordJournalTypeCountsToleratesMissingJournal(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	// No writeMixedJournal call: this pass's journal directory never existed.
	c.recordJournalTypeCounts(job, pass) // must not panic

	got, total, err := c.st.JournalTypeCounts(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || total != 0 {
		t.Errorf("JournalTypeCounts = %+v (total %d), want empty", got, total)
	}
}
