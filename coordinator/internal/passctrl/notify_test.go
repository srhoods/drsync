package passctrl

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/notify"
	"drsync/coordinator/internal/store"
)

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
	host, port, got := mockSMTP(t)
	c.SetNotifier(notify.NewSender(&notify.Config{
		Host: host, Port: port, Security: "none", From: "drsync <d@example.com>", TimeoutSeconds: 2,
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

func TestJobParkedShardsFiltersByJob(t *testing.T) {
	c := newController(t)
	job1, err := c.st.CreateJob("j1", baseSpec2("j1"), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.st.SetJobState(job1.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	job2, err := c.st.CreateJob("j2", baseSpec2("j2"), false)
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
