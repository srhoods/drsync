package agentsrv

import (
	"net"
	"path/filepath"
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
