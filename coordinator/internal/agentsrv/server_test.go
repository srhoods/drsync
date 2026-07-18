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
	if _, err := st.CreateJob("e2e", []byte(specYAML), false); err != nil {
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
	if _, err := st.CreateJob("e2e", []byte(specYAML), false); err != nil {
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
