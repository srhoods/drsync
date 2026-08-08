package agentsrv

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"drsync/coordinator/internal/journal"
	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/passctrl"
	"drsync/coordinator/internal/scheduler"
	"drsync/coordinator/internal/store"
	"drsync/coordinator/internal/wire"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

const specYAML = `
apiVersion: drsync/v1
kind: Job
metadata:
  name: e2e
spec:
  source: { path: /src }
  destination: { path: /dst }
`

// fakeAgent drives one protocol exchange over a real TCP conn.
type fakeAgent struct {
	t    *testing.T
	conn net.Conn
}

func (a *fakeAgent) send(ft drsyncpb.FrameType, msg proto.Message) {
	a.t.Helper()
	if err := wire.WriteFrame(a.conn, ft, msg); err != nil {
		a.t.Fatal(err)
	}
}

func (a *fakeAgent) recv(want drsyncpb.FrameType, msg proto.Message) {
	a.t.Helper()
	a.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	ft, payload, err := wire.ReadFrame(a.conn)
	if err != nil {
		a.t.Fatal(err)
	}
	if ft != want {
		a.t.Fatalf("frame = %v, want %v", ft, want)
	}
	if err := proto.Unmarshal(payload, msg); err != nil {
		a.t.Fatal(err)
	}
}

// TestAgentSession runs the full happy path: hello, work request granting the
// seeded root shard (with JobOptions), a split, journal batch, shard result.
func TestAgentSession(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	jw, err := journal.NewWriter(filepath.Join(dir, "journals"))
	if err != nil {
		t.Fatal(err)
	}
	defer jw.Close()
	met := metrics.New()
	sched := scheduler.New(st, met, 30*time.Second)
	pc := passctrl.New(st, dir)

	// Seed: job submitted and started (pass 1 with root shard).
	if _, err := st.CreateJob("e2e", []byte(specYAML), false, ""); err != nil {
		t.Fatal(err)
	}
	if err := pc.StartJob("e2e"); err != nil {
		t.Fatal(err)
	}

	srv := New(Config{HeartbeatInterval: 5 * time.Second, LeaseTTL: 30 * time.Second},
		st, sched, jw, met)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go srv.Serve(ln)
	// The journal ack is fsync-gated: the flusher persists batches and then
	// acks. Run it fast so the ack round-trip below stays deterministic.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.RunJournalFlusher(ctx, 10*time.Millisecond)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	a := &fakeAgent{t: t, conn: conn}

	// Handshake.
	a.send(drsyncpb.FrameType_FRAME_HELLO, &drsyncpb.Hello{
		AgentId: "agent-test", Hostname: "testhost", ProtoMajor: 1, AgentVersion: "0.0.1"})
	ack := &drsyncpb.HelloAck{}
	a.recv(drsyncpb.FrameType_FRAME_HELLO_ACK, ack)
	if !ack.Accepted || ack.LeaseTtlS != 30 {
		t.Fatalf("hello ack = %+v", ack)
	}

	// Pull work: expect the root shard plus JobOptions (nothing cached).
	a.send(drsyncpb.FrameType_FRAME_WORK_REQUEST, &drsyncpb.WorkRequest{ShardCredits: 4})
	grant := &drsyncpb.WorkGrant{}
	a.recv(drsyncpb.FrameType_FRAME_WORK_GRANT, grant)
	if len(grant.Items) != 1 {
		t.Fatalf("grant items = %d, want 1", len(grant.Items))
	}
	root := grant.Items[0].GetShard()
	if root == nil || root.RelPath != "" {
		t.Fatalf("granted item = %+v, want root dir shard", grant.Items[0])
	}
	if len(grant.Options) != 1 || grant.Options[0].SrcRoot != "/src" {
		t.Fatalf("options = %+v", grant.Options)
	}
	lease := grant.Items[0].LeaseId

	// Split two subdirectories back.
	a.send(drsyncpb.FrameType_FRAME_SHARD_SPLIT, &drsyncpb.ShardSplit{
		ParentShardId: root.ShardId, Seq: 1,
		Subdirs: []*drsyncpb.ShardSplit_NewShard{
			{RelPath: []byte("projects")}, {RelPath: []byte("home")},
		}})
	splitAck := &drsyncpb.ShardSplitAck{}
	a.recv(drsyncpb.FrameType_FRAME_SHARD_SPLIT_ACK, splitAck)
	if len(splitAck.AssignedShardIds) != 2 {
		t.Fatalf("split ack = %+v", splitAck)
	}

	// Stream a journal batch.
	a.send(drsyncpb.FrameType_FRAME_JOURNAL_BATCH, &drsyncpb.JournalBatch{
		Seq: 1, JobId: root.JobId, PassNo: root.PassNo, RecordCount: 10,
		RecordsZstd: []byte("fake-zstd-payload")})
	jack := &drsyncpb.JournalAck{}
	a.recv(drsyncpb.FrameType_FRAME_JOURNAL_ACK, jack)
	if jack.AckedSeq != 1 {
		t.Fatalf("journal ack = %+v", jack)
	}

	// Complete the root shard.
	a.send(drsyncpb.FrameType_FRAME_SHARD_RESULT, &drsyncpb.ShardResult{
		ShardId: root.ShardId, LeaseId: lease,
		Status:   drsyncpb.ResultStatus_RESULT_OK,
		Counters: &drsyncpb.ShardCounters{EntriesWalked: 100, FilesCopied: 42, BytesCopied: 4096}})

	// Heartbeat round-trip flushes the pipeline so we can assert store state.
	a.send(drsyncpb.FrameType_FRAME_HEARTBEAT, &drsyncpb.Heartbeat{Seq: 9})
	hbAck := &drsyncpb.HeartbeatAck{}
	a.recv(drsyncpb.FrameType_FRAME_HEARTBEAT_ACK, hbAck)
	if hbAck.Seq != 9 {
		t.Fatalf("heartbeat ack = %+v", hbAck)
	}

	// Root shard DONE, two children QUEUED, counters accumulated.
	job, err := st.GetJob("e2e")
	if err != nil {
		t.Fatal(err)
	}
	pass, err := st.ActivePass(job.ID)
	if err != nil || pass == nil {
		t.Fatalf("active pass: %v %v", pass, err)
	}
	counts, err := st.ShardStateCounts(pass.ID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[model.ShardDone] != 1 || counts[model.ShardQueued] != 2 {
		t.Fatalf("shard counts = %+v", counts)
	}
	if pass2, _ := st.LatestPass(job.ID); pass2.FilesCopied != 42 || pass2.BytesCopied != 4096 {
		t.Fatalf("pass counters = %+v", pass2)
	}
}

// newTestServer builds a Server with a seeded, started job (root shard queued).
func newTestServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	jw, err := journal.NewWriter(filepath.Join(dir, "journals"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { jw.Close() })
	met := metrics.New()
	sched := scheduler.New(st, met, 30*time.Second)
	pc := passctrl.New(st, dir)
	if _, err := st.CreateJob("e2e", []byte(specYAML), false, ""); err != nil {
		t.Fatal(err)
	}
	if err := pc.StartJob("e2e"); err != nil {
		t.Fatal(err)
	}
	return New(Config{HeartbeatInterval: 5 * time.Second, LeaseTTL: 30 * time.Second},
		st, sched, jw, met)
}

// TestJournalAckWithheldUntilFlush is the durability regression: a JournalAck
// must not be sent until the batch is fsynced, because the agent releases its
// send buffer and unblocks the shard's result on the ack (agent/src/jrn.c). An
// ack before fsync would lose journal records on a coordinator crash.
func TestJournalAckWithheldUntilFlush(t *testing.T) {
	srv := newTestServer(t)
	coordSide, agentSide := net.Pipe()
	defer agentSide.Close()
	go srv.handle(coordSide)

	a := &fakeAgent{t: t, conn: agentSide}
	a.send(drsyncpb.FrameType_FRAME_HELLO, &drsyncpb.Hello{
		AgentId: "jd", Hostname: "h", ProtoMajor: 1, AgentVersion: "0.0.1"})
	a.recv(drsyncpb.FrameType_FRAME_HELLO_ACK, &drsyncpb.HelloAck{})

	// Persist a batch, then round-trip a heartbeat: the read loop processes
	// frames in order, so once its ack returns the batch has been Append'd.
	a.send(drsyncpb.FrameType_FRAME_JOURNAL_BATCH, &drsyncpb.JournalBatch{
		Seq: 7, JobId: 1, PassNo: 1, RecordCount: 3, RecordsZstd: []byte("x")})
	a.send(drsyncpb.FrameType_FRAME_HEARTBEAT, &drsyncpb.Heartbeat{Seq: 1})
	a.recv(drsyncpb.FrameType_FRAME_HEARTBEAT_ACK, &drsyncpb.HeartbeatAck{})

	// No flusher has run: the ack must not have been sent.
	agentSide.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if ft, _, err := wire.ReadFrame(agentSide); err == nil {
		t.Fatalf("received %v before fsync; ack must be withheld until flush", ft)
	}
	agentSide.SetReadDeadline(time.Time{})

	// After a flush (fsync) the ack is released, at the durable high-water.
	srv.flushAndAck()
	jack := &drsyncpb.JournalAck{}
	a.recv(drsyncpb.FrameType_FRAME_JOURNAL_ACK, jack)
	if jack.AckedSeq != 7 {
		t.Fatalf("acked seq = %d, want 7", jack.AckedSeq)
	}
}

// TestReadLoopNotBlockedByWrites is the end-of-scan deadlock regression. Over an
// unbuffered pipe, an agent that keeps sending frames without reading responses
// used to wedge the coordinator: its read loop blocked writing a reply, so it
// stopped draining the agent and both sides deadlocked (stalling journal-acks
// and heartbeats). With a dedicated writer goroutine the read loop keeps
// consuming, so the agent's writes never block.
func TestReadLoopNotBlockedByWrites(t *testing.T) {
	srv := newTestServer(t)
	coordSide, agentSide := net.Pipe()
	defer agentSide.Close()
	go srv.handle(coordSide)

	a := &fakeAgent{t: t, conn: agentSide}
	a.send(drsyncpb.FrameType_FRAME_HELLO, &drsyncpb.Hello{
		AgentId: "flood", Hostname: "h", ProtoMajor: 1, AgentVersion: "0.0.1"})
	a.recv(drsyncpb.FrameType_FRAME_HELLO_ACK, &drsyncpb.HelloAck{})

	// Flood heartbeats and never read the acks. Each write must complete because
	// the coordinator keeps reading; if the read loop stalled on a reply, the
	// unbuffered pipe would block this write and trip the deadline.
	const n = 300 // < outBuffer, so the writer's backlog never blocks the reader
	for i := 0; i < n; i++ {
		agentSide.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := wire.WriteFrame(agentSide, drsyncpb.FrameType_FRAME_HEARTBEAT,
			&drsyncpb.Heartbeat{Seq: uint64(i)}); err != nil {
			t.Fatalf("heartbeat %d blocked/failed — read loop stalled on a write: %v", i, err)
		}
	}
}

// TestStuckWriterUnwedgesSession is the companion live-incident fix: a peer
// that stops reading but leaves the TCP connection open (not closed/reset —
// e.g. a wedged agent process) used to hang the writer goroutine on its
// blocking wire.WriteFrame call forever. That stalled goroutine then fills
// ac.out (outBuffer frames) and blocks ac.send() in the read loop, wedging
// that agent's session permanently instead of tearing it down so the agent
// can reconnect. A write deadline on every frame bounds this.
func TestStuckWriterUnwedgesSession(t *testing.T) {
	orig := writeDeadline
	writeDeadline = 100 * time.Millisecond
	t.Cleanup(func() { writeDeadline = orig })

	srv := newTestServer(t)
	coordSide, agentSide := net.Pipe()
	defer agentSide.Close()
	go srv.handle(coordSide)

	a := &fakeAgent{t: t, conn: agentSide}
	a.send(drsyncpb.FrameType_FRAME_HELLO, &drsyncpb.Hello{
		AgentId: "stuck", Hostname: "h", ProtoMajor: 1, AgentVersion: "0.0.1"})
	a.recv(drsyncpb.FrameType_FRAME_HELLO_ACK, &drsyncpb.HelloAck{})

	// Trigger a coordinator->agent write (heartbeat ack) but never read it: on
	// the unbuffered pipe the writer goroutine's WriteFrame blocks until
	// something reads, exactly like a peer that stopped draining its socket.
	// Do not read agentSide again below — reading it would itself satisfy the
	// pending write and defeat the point of the test.
	a.send(drsyncpb.FrameType_FRAME_HEARTBEAT, &drsyncpb.Heartbeat{Seq: 1})

	// Wait past the write deadline without ever reading the pending
	// HEARTBEAT_ACK, then probe with a fresh read: if the deadline fired and
	// tore the session down, this returns promptly with EOF/closed-pipe rather
	// than hanging or delivering the stale frame.
	time.Sleep(3 * writeDeadline)
	agentSide.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := agentSide.Read(buf); err == nil {
		t.Fatal("read succeeded after the write deadline elapsed; the coordinator should have closed the session")
	}
}

// TestShardSplitForReapedParentIsAcked is the live-incident regression: an
// agent's outbox can replay a SHARD_SPLIT after a reconnect for a parent shard
// that has since been reaped (docs/DESIGN-coordinator.md §3, PR #57/#58).
// Before this fix, onShardSplit's calls into planBigFiles/planLinkSightings
// and store.RecordSplit all failed with sql.ErrNoRows once the parent's shards
// row was gone, dispatch() treated that as fatal and closed the session — and
// because the outbox replays the same unacked frame on every subsequent
// reconnect, the agent never got past it: a permanent disconnect loop, seen
// live as repeated "dispatch failed: sql: no rows in result set" on the
// coordinator and "coordinator sent protocol error" on the agent. The fix
// treats a missing parent as already-processed and ACKs the split instead of
// erroring the session.
func TestShardSplitForReapedParentIsAcked(t *testing.T) {
	srv := newTestServer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go srv.Serve(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	a := &fakeAgent{t: t, conn: conn}

	a.send(drsyncpb.FrameType_FRAME_HELLO, &drsyncpb.Hello{
		AgentId: "agent-test", Hostname: "testhost", ProtoMajor: 1, AgentVersion: "0.0.1"})
	helloAck := &drsyncpb.HelloAck{}
	a.recv(drsyncpb.FrameType_FRAME_HELLO_ACK, helloAck)
	if !helloAck.Accepted {
		t.Fatalf("hello ack = %+v", helloAck)
	}

	a.send(drsyncpb.FrameType_FRAME_WORK_REQUEST, &drsyncpb.WorkRequest{ShardCredits: 4})
	grant := &drsyncpb.WorkGrant{}
	a.recv(drsyncpb.FrameType_FRAME_WORK_GRANT, grant)
	if len(grant.Items) != 1 {
		t.Fatalf("grant items = %d, want 1", len(grant.Items))
	}
	root := grant.Items[0].GetShard()
	lease := grant.Items[0].LeaseId

	// Complete and then reap the root shard — the same path a live shard takes
	// (eager reap, docs/DESIGN-coordinator.md §3) — so its row is gone by the
	// time the replayed split below arrives, exactly as it would be after a
	// disconnect/reconnect racing the reaper.
	a.send(drsyncpb.FrameType_FRAME_SHARD_RESULT, &drsyncpb.ShardResult{
		ShardId: root.ShardId, LeaseId: lease, Status: drsyncpb.ResultStatus_RESULT_OK,
		Counters: &drsyncpb.ShardCounters{},
	})
	a.send(drsyncpb.FrameType_FRAME_HEARTBEAT, &drsyncpb.Heartbeat{Seq: 0})
	a.recv(drsyncpb.FrameType_FRAME_HEARTBEAT_ACK, &drsyncpb.HeartbeatAck{})

	job, err := srv.st.GetJob("e2e")
	if err != nil {
		t.Fatal(err)
	}
	pass, err := srv.st.ActivePass(job.ID)
	if err != nil || pass == nil {
		t.Fatalf("active pass: %v %v", pass, err)
	}
	if n, err := srv.st.ReapDoneShards(pass.ID, []model.ShardKind{model.KindDir}); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("reaped %d shards, want 1 (root shard id %d)", n, root.ShardId)
	}

	// A split carrying a big file (exercises planBigFiles's ShardJobPass call)
	// plus plain subdirs (exercises store.RecordSplit's own parent lookup).
	a.send(drsyncpb.FrameType_FRAME_SHARD_SPLIT, &drsyncpb.ShardSplit{
		ParentShardId: root.ShardId, Seq: 1,
		Subdirs: []*drsyncpb.ShardSplit_NewShard{{RelPath: []byte("projects")}},
		BigFiles: []*drsyncpb.ShardSplit_BigFile{
			{RelPath: []byte("a/big.bin"), Size: 1 << 20, MtimeNs: 1},
		},
	})

	// Must be ACKed, not answered with FRAME_ERROR (which would tear the
	// session down and leave the agent to replay the exact same frame again).
	splitAck := &drsyncpb.ShardSplitAck{}
	a.recv(drsyncpb.FrameType_FRAME_SHARD_SPLIT_ACK, splitAck)
	if len(splitAck.AssignedShardIds) != 0 {
		t.Fatalf("split ack for reaped parent assigned ids %v, want none", splitAck.AssignedShardIds)
	}

	// The session must still be alive: a heartbeat round-trip proves dispatch
	// did not close the connection after the split.
	a.send(drsyncpb.FrameType_FRAME_HEARTBEAT, &drsyncpb.Heartbeat{Seq: 1})
	hbAck := &drsyncpb.HeartbeatAck{}
	a.recv(drsyncpb.FrameType_FRAME_HEARTBEAT_ACK, hbAck)
}

// TestDeleteRemainderPathsJoinDirRel is the path-joining regression: a
// ShardSplit.DeleteRemainder's Names are bare basenames (agent readdir
// entries, agent/src/delete.c flush_delete_split) — onShardSplit must join
// each one under DirRel before placing it in the split-produced KindDelete
// shard's DeleteBatch.RelPaths, the same full-relative-path shape
// seedDeletePass's own DeleteBatch shards use (passctrl.go). Feeding a bare
// basename straight into DeleteBatch.RelPaths makes the agent try to unlink
// it at the destination root instead of under the orphan directory — a
// silent ENOENT, not an error, so the shard reports success while removing
// nothing. This was caught by local e2e verification (delete_fanout_e2e.sh),
// not by the delete_groups completion-tracking unit tests, since those drive
// store.RecordSplit directly with pre-built NewShards and never exercise
// onShardSplit's own payload construction.
func TestDeleteRemainderPathsJoinDirRel(t *testing.T) {
	srv := newTestServer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go srv.Serve(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	a := &fakeAgent{t: t, conn: conn}

	a.send(drsyncpb.FrameType_FRAME_HELLO, &drsyncpb.Hello{
		AgentId: "agent-test", Hostname: "testhost", ProtoMajor: 1, AgentVersion: "0.0.1"})
	a.recv(drsyncpb.FrameType_FRAME_HELLO_ACK, &drsyncpb.HelloAck{})

	a.send(drsyncpb.FrameType_FRAME_WORK_REQUEST, &drsyncpb.WorkRequest{ShardCredits: 4})
	grant := &drsyncpb.WorkGrant{}
	a.recv(drsyncpb.FrameType_FRAME_WORK_GRANT, grant)
	root := grant.Items[0].GetShard()
	lease := grant.Items[0].LeaseId

	// A pathological orphan directory streamed out as two DeleteRemainder
	// batches — the second (final) one carrying TotalChildren, same shape
	// agent/src/delete.c's stream_delete_split produces.
	a.send(drsyncpb.FrameType_FRAME_SHARD_SPLIT, &drsyncpb.ShardSplit{
		ParentShardId: root.ShardId, Seq: 1,
		DeleteRemainders: []*drsyncpb.ShardSplit_DeleteRemainder{
			{DirRel: []byte("big-orphan-dir"), Names: [][]byte{[]byte("f1.txt"), []byte("f2.txt")}},
		},
	})
	a.recv(drsyncpb.FrameType_FRAME_SHARD_SPLIT_ACK, &drsyncpb.ShardSplitAck{})
	a.send(drsyncpb.FrameType_FRAME_SHARD_SPLIT, &drsyncpb.ShardSplit{
		ParentShardId: root.ShardId, Seq: 2,
		DeleteRemainders: []*drsyncpb.ShardSplit_DeleteRemainder{
			{DirRel: []byte("big-orphan-dir"), Names: [][]byte{[]byte("f3.txt")}, TotalChildren: 2},
		},
	})
	a.recv(drsyncpb.FrameType_FRAME_SHARD_SPLIT_ACK, &drsyncpb.ShardSplitAck{})

	// Finish the root shard so its own credits free up, then pull the
	// split-produced delete shards and inspect their payloads directly.
	a.send(drsyncpb.FrameType_FRAME_SHARD_RESULT, &drsyncpb.ShardResult{
		ShardId: root.ShardId, LeaseId: lease, Status: drsyncpb.ResultStatus_RESULT_OK,
		Counters: &drsyncpb.ShardCounters{},
	})
	a.send(drsyncpb.FrameType_FRAME_WORK_REQUEST, &drsyncpb.WorkRequest{ShardCredits: 4})
	grant2 := &drsyncpb.WorkGrant{}
	a.recv(drsyncpb.FrameType_FRAME_WORK_GRANT, grant2)
	if len(grant2.Items) != 2 {
		t.Fatalf("granted %d delete shards, want 2", len(grant2.Items))
	}

	var gotPaths []string
	for _, item := range grant2.Items {
		batch := item.GetDelete()
		if batch == nil {
			t.Fatalf("granted item = %+v, want a delete work item", item)
		}
		for _, p := range batch.RelPaths {
			gotPaths = append(gotPaths, string(p))
		}
	}
	want := map[string]bool{
		"big-orphan-dir/f1.txt": true, "big-orphan-dir/f2.txt": true, "big-orphan-dir/f3.txt": true,
	}
	if len(gotPaths) != len(want) {
		t.Fatalf("relPaths = %v, want exactly %v", gotPaths, want)
	}
	for _, p := range gotPaths {
		if !want[p] {
			t.Fatalf("relPaths contains %q (bare basename, not joined under dir_rel) — full set: %v", p, gotPaths)
		}
	}
}

// TestChunkTempNamePassTagged is the "open temp for finalize" regression. A
// chunk temp lives in the destination directory, with no source counterpart,
// for the whole multi-host copy — indistinguishable from crash residue to an
// agent's orphan sweep, which unlinks prefix-matching destination orphans. The
// directory can legitimately be re-walked while the group runs (the parent walk
// shard is requeued after a lease lapse or a journal-ack timeout, and
// RecordSplit deliberately keeps the existing group rather than re-fanning it),
// and an untagged temp was then reclaimed out from under the live chunks: the
// finalize failed on ENOENT, or — if the unlink landed mid-group — the
// remaining chunks recreated the temp with O_CREAT and finalize renamed a
// hole-ridden file into place.
//
// The leading "<job>-<pass>." tag is what the agent compares against its own
// (job, pass) to keep the temp. The format is shared with agent/src/tempname.c
// (temp_name_fmt / temp_tag_matches); this pins the coordinator's half.
func TestChunkTempNamePassTagged(t *testing.T) {
	srv := newTestServer(t)

	// The seeded root scan shard stands in for the walk shard that discovers a
	// big file and ships it for fan-out.
	rows, err := srv.st.LeaseShards("agent-1", 1, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("leased %d shards, want the seeded root scan shard", len(rows))
	}
	parent := rows[0]

	_, groups, err := srv.planBigFiles(parent.ID, []*drsyncpb.ShardSplit_BigFile{
		{RelPath: []byte("a/big.bin"), Size: 1 << 30, MtimeNs: 42},
		{RelPath: []byte("a/big2.bin"), Size: 1 << 30, MtimeNs: 43},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("planned %d groups, want 2", len(groups))
	}

	wantTag := fmt.Sprintf("%x-%x.", parent.JobID, parent.PassNo)
	for i, g := range groups {
		want := fmt.Sprintf(".drsync.tmp.%s%x.%x", wantTag, parent.ID, i)
		if g.TempName != want {
			t.Errorf("group %d temp = %q, want %q", i, g.TempName, want)
		}
		// The sweep strips the prefix and compares the tag; an untagged name
		// (the old "<shard>.<index>" form) is what the agent reclaims.
		if !strings.HasPrefix(strings.TrimPrefix(g.TempName, ".drsync.tmp."), wantTag) {
			t.Errorf("group %d temp %q carries no (job, pass) tag — a concurrent "+
				"walk of its directory would reclaim it mid-copy", i, g.TempName)
		}
	}
	// Every chunk of a file must target that one temp, including the finalize
	// task seeded on data-chunk completion.
	if fin := finalizeShard("a/big.bin", groups[0].TempName, nil); fin.Kind != model.KindChunk {
		t.Fatalf("finalize shard kind = %v", fin.Kind)
	}
}
