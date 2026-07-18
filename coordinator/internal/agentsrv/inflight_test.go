package agentsrv

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"drsync/coordinator/internal/journal"
	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/scheduler"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// listenTestServer starts a listening Server with the given config (heartbeat
// and lease TTL filled in). Unlike newTestServer it seeds no job: these tests
// exercise the session and heartbeat only, never work granting.
func listenTestServer(t *testing.T, cfg Config) (*Server, string) {
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
	cfg.HeartbeatInterval = 5 * time.Second
	cfg.LeaseTTL = 30 * time.Second
	srv := New(cfg, st, scheduler.New(st, met, 30*time.Second), jw, met)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go srv.Serve(ln)
	return srv, ln.Addr().String()
}

// dialHello completes a handshake at the given protocol minor and returns the
// connection and the ack.
func dialHello(t *testing.T, addr, agentID string, minor uint32) (*fakeAgent, *drsyncpb.HelloAck) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	a := &fakeAgent{t: t, conn: conn}
	a.send(drsyncpb.FrameType_FRAME_HELLO, &drsyncpb.Hello{
		AgentId: agentID, Hostname: "testhost", ProtoMajor: ProtoMajor,
		ProtoMinor: minor, AgentVersion: "0.0.1"})
	ack := &drsyncpb.HelloAck{}
	a.recv(drsyncpb.FrameType_FRAME_HELLO_ACK, ack)
	return a, ack
}

// waitInflight polls until the agent's snapshot is populated (the heartbeat is
// handled asynchronously by the read loop).
func waitInflight(t *testing.T, srv *Server, agentID string) []*drsyncpb.InflightItem {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		items, _, _, connected := srv.Inflight(agentID)
		if connected && len(items) > 0 {
			return items
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no in-flight snapshot for %s within the deadline", agentID)
	return nil
}

// A heartbeat's in-flight detail must survive to the fleet view intact — this
// is the whole diagnostic surface, so a dropped or mangled field is silent.
func TestHeartbeatInflightReachesFleetView(t *testing.T) {
	srv, addr := listenTestServer(t, Config{})
	a, ack := dialHello(t, addr, "agent-if", MinorInflight)
	if !ack.Accepted {
		t.Fatalf("hello rejected: %s", ack.RejectReason)
	}

	a.send(drsyncpb.FrameType_FRAME_HEARTBEAT, &drsyncpb.Heartbeat{
		Seq: 1,
		Inflight: []*drsyncpb.InflightItem{{
			LeaseId: 77, ShardId: 8821, JobId: 4, Kind: "dir",
			RelPath: "proj/archive/2019", HeldMs: 870000, RunningMs: 869000,
			Running: true, EntriesDone: 2100000,
		}, {
			LeaseId: 78, ShardId: 8830, JobId: 4, Kind: "entrylist",
			RelPath: "proj/archive/2020", HeldMs: 11000, Running: false,
		}},
	})

	items := waitInflight(t, srv, "agent-if")
	if len(items) != 2 {
		t.Fatalf("in-flight items = %d, want 2", len(items))
	}
	got := items[0]
	if got.ShardId != 8821 || got.Kind != "dir" || got.RelPath != "proj/archive/2019" {
		t.Errorf("item[0] identity = %+v", got)
	}
	if !got.Running || got.RunningMs != 869000 || got.EntriesDone != 2100000 {
		t.Errorf("item[0] progress = running=%v running_ms=%d entries=%d",
			got.Running, got.RunningMs, got.EntriesDone)
	}
	// The queued/running split is the signal that separates "over-granted" from
	// "slow", so it must not be flattened.
	if items[1].Running {
		t.Errorf("item[1] should be queued, got running")
	}

	_, _, reports, connected := srv.Inflight("agent-if")
	if !connected || !reports {
		t.Errorf("connected=%v reports=%v, want both true", connected, reports)
	}
}

// An agent too old to report in-flight detail must be distinguishable from one
// that is genuinely holding nothing: both yield an empty list, and only the
// reports flag tells them apart.
func TestInflightUnsupportedForOldAgent(t *testing.T) {
	srv, addr := listenTestServer(t, Config{})
	a, ack := dialHello(t, addr, "agent-old", 0)
	if !ack.Accepted {
		t.Fatalf("minor 0 agent rejected by default config: %s", ack.RejectReason)
	}
	a.send(drsyncpb.FrameType_FRAME_HEARTBEAT, &drsyncpb.Heartbeat{Seq: 1})

	deadline := time.Now().Add(5 * time.Second)
	for {
		items, _, reports, connected := srv.Inflight("agent-old")
		if connected {
			if reports {
				t.Fatal("minor 0 agent reported as supporting in-flight detail")
			}
			if len(items) != 0 {
				t.Fatalf("items = %d, want 0", len(items))
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("agent never registered")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMinAgentMinorGate(t *testing.T) {
	// The floor is opt-in: raising it excludes older agents...
	srv, addr := listenTestServer(t, Config{MinAgentMinor: 1})
	_, ack := dialHello(t, addr, "agent-behind", 0)
	if ack.Accepted {
		t.Error("agent below -min-agent-minor was accepted")
	}
	if ack.RejectReason == "" {
		t.Error("rejection carried no reason; the operator has nothing to act on")
	}
	if _, _, _, connected := srv.Inflight("agent-behind"); connected {
		t.Error("rejected agent left registered as connected")
	}

	// ...while an agent at or above the floor is unaffected.
	if _, ack2 := dialHello(t, addr, "agent-current", 1); !ack2.Accepted {
		t.Errorf("agent at the floor was rejected: %s", ack2.RejectReason)
	}
}

// The default config must not exclude anyone: a version check that silently
// strands a running fleet is worse than the missing telemetry it enforces.
func TestDefaultConfigAcceptsOldMinors(t *testing.T) {
	_, addr := listenTestServer(t, Config{})
	if _, ack := dialHello(t, addr, "agent-legacy", 0); !ack.Accepted {
		t.Errorf("default config rejected a minor 0 agent: %s", ack.RejectReason)
	}
}
