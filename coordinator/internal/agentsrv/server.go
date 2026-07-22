// Package agentsrv terminates agent connections: session handshake, work
// grants, split/result/journal ingestion (docs/DESIGN-protocol.md).
package agentsrv

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"drsync/coordinator/internal/journal"
	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/scheduler"
	"drsync/coordinator/internal/store"
	"drsync/coordinator/internal/wire"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// Wire protocol version (proto/drsync.proto §Versioning).
//
// ProtoMajor must match the agent's exactly: a mismatch means the two sides
// disagree about what a field means, so the session is refused. Bumping it
// locks out every agent until the whole fleet is upgraded — reserve it for
// genuinely incompatible changes.
//
// ProtoMinor is additive. The coordinator accepts any minor and degrades to
// what the agent actually supports; it is recorded per agent so an operator can
// see who is behind, and so "the agent does not report this" is distinguishable
// from "the agent reports zero". Raise MinAgentMinor (the -min-agent-minor
// flag) only to deliberately exclude old agents.
const (
	ProtoMajor = 1
	ProtoMinor = 1

	// MinorInflight is the minor at which agents began reporting per-lease
	// in-flight detail in the heartbeat.
	MinorInflight = 1
)

type Config struct {
	HeartbeatInterval time.Duration
	LeaseTTL          time.Duration
	TLS               *tls.Config // nil = plaintext (dev only; warn loudly)
	FleetEpoch        uint64
	// MinAgentMinor refuses agents below this protocol minor. 0 (the default)
	// accepts every compatible agent — raising it strands work on any agent
	// that cannot reconnect, so it is opt-in.
	MinAgentMinor uint32
}

type Server struct {
	cfg   Config
	st    *store.Store
	sched *scheduler.Scheduler
	jw    *journal.Writer
	met   *metrics.Metrics

	mu     sync.Mutex
	agents map[string]*agentConn
}

type agentConn struct {
	id         string
	hostname   string
	protoMinor uint32
	conn       net.Conn
	wmu        sync.Mutex     // serializes handshake (pre-writer) frames
	out        chan sendFrame // steady-state frames handed to the writer goroutine
	stop       chan struct{}  // closed on teardown to stop the writer
	done       chan struct{}  // closed when the writer has exited
	// drain is set from the API goroutine when an agent is disabled, and read on
	// the read-loop goroutine (heartbeat-ack, work-request), so it is atomic. A
	// draining agent is told to hand back its queued shards and is granted none.
	drain atomic.Bool
	pause bool

	// Journal ack watermarks. onJournalBatch persists a batch and raises
	// jrnPending; the flusher fsyncs and only then acks up to jrnAcked. Gating
	// the ack on fsync is what makes it mean "durable" — the agent releases its
	// send buffer and unblocks the shard's result on the ack, so acking before
	// fsync would lose journal records on a coordinator crash.
	jrnMu      sync.Mutex
	jrnPending uint64
	jrnAcked   uint64

	// Last heartbeat's in-flight snapshot. Sampled state, not history: replaced
	// wholesale each heartbeat, and dropped with the session.
	ifMu       sync.Mutex
	inflight   []*drsyncpb.InflightItem
	inflightAt time.Time
}

// Inflight returns the agent's most recent in-flight snapshot and the time it
// was reported. reports is false when the agent is connected but too old to
// send the detail — the caller must not render that as "idle".
func (s *Server) Inflight(agentID string) (items []*drsyncpb.InflightItem, at time.Time, reports bool, connected bool) {
	s.mu.Lock()
	ac := s.agents[agentID]
	s.mu.Unlock()
	if ac == nil {
		return nil, time.Time{}, false, false
	}
	ac.ifMu.Lock()
	defer ac.ifMu.Unlock()
	return ac.inflight, ac.inflightAt, ac.protoMinor >= MinorInflight, true
}

type sendFrame struct {
	ft  drsyncpb.FrameType
	msg proto.Message
}

// outBuffer bounds the per-agent write queue. Generously sized: with the writer
// goroutine draining independently the read loop never stalls on a write, so
// this only fills if the agent has genuinely stopped reading (then the socket
// write errors and the session is torn down).
const outBuffer = 1024

func New(cfg Config, st *store.Store, sched *scheduler.Scheduler, jw *journal.Writer, met *metrics.Metrics) *Server {
	return &Server{cfg: cfg, st: st, sched: sched, jw: jw, met: met,
		agents: map[string]*agentConn{}}
}

func (s *Server) Serve(ln net.Listener) error {
	if s.cfg.TLS != nil {
		ln = tls.NewListener(ln, s.cfg.TLS)
	} else {
		slog.Warn("agent listener running WITHOUT TLS — dev mode only")
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handle(c)
	}
}

// ConnectedAgents lists live sessions (for the API fleet view).
func (s *Server) ConnectedAgents() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.agents))
	for id := range s.agents {
		out = append(out, id)
	}
	return out
}

// writeSync writes one frame inline. Used only for the HELLO handshake, before
// the writer goroutine starts — no concurrency and no deadlock risk there.
func (ac *agentConn) writeSync(ft drsyncpb.FrameType, msg proto.Message) error {
	ac.wmu.Lock()
	defer ac.wmu.Unlock()
	return wire.WriteFrame(ac.conn, ft, msg)
}

// send queues a frame for the dedicated writer goroutine. The read loop calls
// this to respond, and MUST NOT block on the socket: if it did, a large agent
// write burst (end-of-scan journals/results) plus the coordinator's replies can
// fill both socket buffers so neither side reads — a bidirectional deadlock that
// stalls journal-acks (shard requeues) and heartbeats (lease expiry). Handing
// the write to a separate goroutine keeps the read loop draining the agent.
func (ac *agentConn) send(ft drsyncpb.FrameType, msg proto.Message) error {
	select {
	case ac.out <- sendFrame{ft, msg}:
		return nil
	case <-ac.done:
		return net.ErrClosed
	}
}

func (ac *agentConn) writeLoop() {
	defer close(ac.done)
	for {
		select {
		case f := <-ac.out:
			if err := wire.WriteFrame(ac.conn, f.ft, f.msg); err != nil {
				ac.conn.Close() // unblock the read loop's ReadFrame
				return
			}
		case <-ac.stop:
			return
		}
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()

	// First frame must be HELLO within a short deadline.
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	ft, payload, err := wire.ReadFrame(conn)
	if err != nil || ft != drsyncpb.FrameType_FRAME_HELLO {
		slog.Warn("bad handshake", "remote", remote, "err", err, "frame", ft)
		return
	}
	hello := &drsyncpb.Hello{}
	if err := proto.Unmarshal(payload, hello); err != nil || hello.AgentId == "" {
		slog.Warn("bad hello payload", "remote", remote, "err", err)
		return
	}
	conn.SetReadDeadline(time.Time{})

	ac := &agentConn{
		id: hello.AgentId, hostname: hello.Hostname, protoMinor: hello.ProtoMinor, conn: conn,
		out: make(chan sendFrame, outBuffer), stop: make(chan struct{}), done: make(chan struct{}),
	}
	ack := &drsyncpb.HelloAck{
		Accepted:           true,
		ProtoMajor:         ProtoMajor,
		HeartbeatIntervalS: uint32(s.cfg.HeartbeatInterval / time.Second),
		LeaseTtlS:          uint32(s.cfg.LeaseTTL / time.Second),
		FleetEpoch:         s.cfg.FleetEpoch,
	}
	// Major must match exactly; minor is only checked against an explicit
	// operator floor. Rejecting here is final for this session: the agent
	// retries with backoff, so a misconfigured floor shows up as a fleet that
	// never connects rather than as silent data loss.
	switch {
	case hello.ProtoMajor != ProtoMajor:
		ack.Accepted = false
		ack.RejectReason = fmt.Sprintf("protocol major %d unsupported (want %d)", hello.ProtoMajor, ProtoMajor)
	case hello.ProtoMinor < s.cfg.MinAgentMinor:
		ack.Accepted = false
		ack.RejectReason = fmt.Sprintf("protocol minor %d below the configured minimum %d; upgrade the agent",
			hello.ProtoMinor, s.cfg.MinAgentMinor)
	}
	if !ack.Accepted {
		slog.Warn("agent rejected", "agent", hello.AgentId, "remote", remote,
			"version", hello.AgentVersion, "proto_major", hello.ProtoMajor,
			"proto_minor", hello.ProtoMinor, "reason", ack.RejectReason)
		ac.writeSync(drsyncpb.FrameType_FRAME_HELLO_ACK, ack)
		return
	}

	s.mu.Lock()
	if old := s.agents[ac.id]; old != nil {
		old.conn.Close() // one session per agent id; newest wins
	}
	s.agents[ac.id] = ac
	s.mu.Unlock()

	if err := s.st.UpsertAgent(ac.id, ac.hostname, hello.AgentVersion, hello.ProtoMinor); err != nil {
		slog.Error("agent upsert failed", "agent", ac.id, "err", err)
		return
	}
	if err := ac.writeSync(drsyncpb.FrameType_FRAME_HELLO_ACK, ack); err != nil {
		return
	}
	// Steady state: a dedicated writer drains ac.out so dispatch never blocks on
	// a write (see send()).
	go ac.writeLoop()
	s.met.AgentUp.WithLabelValues(ac.id).Set(1)
	slog.Info("agent connected", "agent", ac.id, "host", ac.hostname,
		"version", hello.AgentVersion, "proto_minor", hello.ProtoMinor)
	if hello.ProtoMinor < ProtoMinor {
		slog.Warn("agent is behind the coordinator's protocol minor; some telemetry will be missing",
			"agent", ac.id, "agent_minor", hello.ProtoMinor, "coordinator_minor", ProtoMinor)
	}

	defer func() {
		close(ac.stop) // stop the writer goroutine
		s.mu.Lock()
		if s.agents[ac.id] == ac {
			delete(s.agents, ac.id)
		}
		s.mu.Unlock()
		s.met.AgentUp.WithLabelValues(ac.id).Set(0)
		s.st.SetAgentState(ac.id, "disconnected")
		slog.Info("agent disconnected", "agent", ac.id)
		// Leases are NOT released here: the agent may reconnect within TTL.
	}()

	for {
		ft, payload, err := wire.ReadFrame(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				slog.Warn("agent read error", "agent", ac.id, "err", err)
			}
			return
		}
		if err := s.dispatch(ac, ft, payload); err != nil {
			slog.Error("dispatch failed; closing session", "agent", ac.id, "frame", ft, "err", err)
			ac.send(drsyncpb.FrameType_FRAME_ERROR,
				&drsyncpb.ProtocolError{Code: 1, Message: err.Error()})
			return
		}
	}
}

func (s *Server) dispatch(ac *agentConn, ft drsyncpb.FrameType, payload []byte) error {
	switch ft {
	case drsyncpb.FrameType_FRAME_HEARTBEAT:
		m := &drsyncpb.Heartbeat{}
		if err := proto.Unmarshal(payload, m); err != nil {
			return err
		}
		return s.onHeartbeat(ac, m)
	case drsyncpb.FrameType_FRAME_WORK_REQUEST:
		m := &drsyncpb.WorkRequest{}
		if err := proto.Unmarshal(payload, m); err != nil {
			return err
		}
		return s.onWorkRequest(ac, m)
	case drsyncpb.FrameType_FRAME_SHARD_SPLIT:
		m := &drsyncpb.ShardSplit{}
		if err := proto.Unmarshal(payload, m); err != nil {
			return err
		}
		return s.onShardSplit(ac, m)
	case drsyncpb.FrameType_FRAME_SHARD_RESULT:
		m := &drsyncpb.ShardResult{}
		if err := proto.Unmarshal(payload, m); err != nil {
			return err
		}
		return s.onShardResult(ac, m)
	case drsyncpb.FrameType_FRAME_TASK_RESULT:
		m := &drsyncpb.TaskResultBatch{}
		if err := proto.Unmarshal(payload, m); err != nil {
			return err
		}
		return s.onTaskResults(ac, m)
	case drsyncpb.FrameType_FRAME_JOURNAL_BATCH:
		m := &drsyncpb.JournalBatch{}
		if err := proto.Unmarshal(payload, m); err != nil {
			return err
		}
		return s.onJournalBatch(ac, m)
	case drsyncpb.FrameType_FRAME_STATS_REPORT:
		m := &drsyncpb.StatsReport{}
		if err := proto.Unmarshal(payload, m); err != nil {
			return err
		}
		s.onStats(ac, m)
		return nil
	case drsyncpb.FrameType_FRAME_ERROR:
		m := &drsyncpb.ProtocolError{}
		proto.Unmarshal(payload, m)
		return fmt.Errorf("agent-side protocol error %d: %s", m.Code, m.Message)
	default:
		return fmt.Errorf("unexpected frame type %v", ft)
	}
}

func (s *Server) onHeartbeat(ac *agentConn, hb *drsyncpb.Heartbeat) error {
	// Latest in-flight snapshot for the fleet view. Kept even when empty: for a
	// minor >= MinorInflight agent, empty genuinely means "holding nothing".
	ac.ifMu.Lock()
	ac.inflight = hb.Inflight
	ac.inflightAt = time.Now()
	ac.ifMu.Unlock()

	// Renew only the leases the agent still holds (per the heartbeat), so a
	// lost grant or a dropped result frame is left to expire and requeue rather
	// than renewed forever (which stalls the pass).
	held := make([]int64, len(hb.HeldLeaseIds))
	for i, id := range hb.HeldLeaseIds {
		held[i] = int64(id)
	}
	if err := s.st.RenewLeasesByID(ac.id, held, s.cfg.LeaseTTL); err != nil {
		return err
	}
	if err := s.st.TouchAgent(ac.id); err != nil {
		return err
	}
	s.met.AgentRSS.WithLabelValues(ac.id).Set(float64(hb.RssBytes))
	return ac.send(drsyncpb.FrameType_FRAME_HEARTBEAT_ACK, &drsyncpb.HeartbeatAck{
		Seq: hb.Seq, Pause: ac.pause, Drain: ac.drain.Load(),
	})
}

// SetDrain tells a live agent whether to drain. On drain the agent stops asking
// for work and hands back the shards still queued (unstarted) on it; running
// shards are left to finish. Returns false if the agent is not connected.
func (s *Server) SetDrain(agentID string, drain bool) bool {
	s.mu.Lock()
	ac := s.agents[agentID]
	s.mu.Unlock()
	if ac == nil {
		return false
	}
	ac.drain.Store(drain)
	// Deliver it now rather than waiting for the next heartbeat-ack (up to a
	// heartbeat interval away), so a drained agent hands its queued shards back
	// promptly. The heartbeat-ack still carries the authoritative flag, which is
	// what clears the drain on re-enable.
	if drain {
		_ = ac.send(drsyncpb.FrameType_FRAME_CONTROL, &drsyncpb.Control{
			Command: drsyncpb.Control_CMD_DRAIN,
		})
	}
	return true
}

// NotifyJobDone tells every connected agent a job has reached a terminal state,
// so each can release that job's cached options and the source/destination root
// directory fds it holds. Without this the agent keeps those fds open until it
// restarts, and lsof shows it pinning both mounts long after the job finished.
// Reuses CMD_CANCEL_JOB: to an agent, "cancelled" and "completed" mean the same
// — stop and forget this job. Best-effort, like every C→A control frame.
func (s *Server) NotifyJobDone(jobID int64) {
	s.mu.Lock()
	acs := make([]*agentConn, 0, len(s.agents))
	for _, ac := range s.agents {
		acs = append(acs, ac)
	}
	s.mu.Unlock()
	for _, ac := range acs {
		_ = ac.send(drsyncpb.FrameType_FRAME_CONTROL, &drsyncpb.Control{
			Command: drsyncpb.Control_CMD_CANCEL_JOB,
			JobId:   uint64(jobID),
		})
	}
}

func (s *Server) onWorkRequest(ac *agentConn, req *drsyncpb.WorkRequest) error {
	if ac.drain.Load() || ac.pause {
		return ac.send(drsyncpb.FrameType_FRAME_WORK_GRANT, &drsyncpb.WorkGrant{})
	}
	grant, err := s.sched.Grant(ac.id, req)
	if err != nil {
		return err
	}
	return ac.send(drsyncpb.FrameType_FRAME_WORK_GRANT, grant)
}

func (s *Server) onShardSplit(ac *agentConn, sp *drsyncpb.ShardSplit) error {
	shards := make([]store.NewShard, 0, len(sp.Subdirs)+len(sp.EntryLists))
	for _, d := range sp.Subdirs {
		shards = append(shards, store.NewShard{Kind: model.KindDir, RelPath: string(d.RelPath)})
	}
	for _, el := range sp.EntryLists {
		payload, err := proto.Marshal(&drsyncpb.EntryListShard{
			DirRel: string(el.DirRel), Names: el.Names})
		if err != nil {
			return err
		}
		shards = append(shards, store.NewShard{
			Kind: model.KindEntryList, RelPath: string(el.DirRel), Payload: payload})
	}

	var groups []store.NewChunkGroup
	if len(sp.BigFiles) > 0 {
		chunkShards, g, err := s.planBigFiles(int64(sp.ParentShardId), sp.BigFiles)
		if err != nil {
			return err
		}
		shards = append(shards, chunkShards...)
		groups = g
	}

	ids, err := s.st.RecordSplit(int64(sp.ParentShardId), sp.Seq, shards, groups)
	if err != nil {
		return err
	}
	ack := &drsyncpb.ShardSplitAck{ParentShardId: sp.ParentShardId, Seq: sp.Seq}
	for _, id := range ids {
		ack.AssignedShardIds = append(ack.AssignedShardIds, uint64(id))
	}
	return ac.send(drsyncpb.FrameType_FRAME_SHARD_SPLIT_ACK, ack)
}

// planBigFiles turns discovered big files into their data-chunk shards plus a
// group per file. Ranges come from the job's copy.chunk_size; the coordinator
// owns the temp name so every chunk (granted to different hosts) writes the
// same destination temp, and chunk 0 alone creates+preallocates it.
func (s *Server) planBigFiles(parentShardID int64, bigs []*drsyncpb.ShardSplit_BigFile) ([]store.NewShard, []store.NewChunkGroup, error) {
	jobID, passNo, err := s.st.ShardJobPass(parentShardID)
	if err != nil {
		return nil, nil, err
	}
	opts, err := s.sched.JobOptions(jobID)
	if err != nil {
		return nil, nil, err
	}
	chunkSize := opts.GetCopy().GetChunkSize()
	if chunkSize == 0 {
		return nil, nil, fmt.Errorf("job %d has chunk_size 0", jobID)
	}
	prefix := opts.GetCopy().GetTempPrefix()
	if prefix == "" {
		prefix = ".drsync.tmp."
	}

	var shards []store.NewShard
	var groups []store.NewChunkGroup
	for i, bf := range bigs {
		rel := string(bf.RelPath)
		nChunks := int((bf.Size + chunkSize - 1) / chunkSize)
		if nChunks < 1 {
			nChunks = 1
		}
		// Temp name is stable across chunk retries and unique per file within
		// the pass: base it on the parent shard and the file's index. The
		// leading "<job>-<pass>." tag is what keeps the temp SAFE while its
		// chunks run: an agent walking this directory reclaims prefix-matching
		// destination orphans, and without the tag it cannot tell a live chunk
		// temp from crash residue — a parent walk shard that is requeued (lease
		// lapse, journal-ack timeout) re-walks the directory while the chunk
		// group it already fanned out is still writing, unlinks the temp, and
		// the finalize fails with "open temp for finalize" (or, if the unlink
		// lands mid-group, later chunks O_CREAT it back and finalize renames a
		// hole-ridden file into place). Agents skip temps carrying their own
		// (job, pass); everything else is residue and is reclaimed.
		temp := fmt.Sprintf("%s%x-%x.%x.%x", prefix, jobID, passNo, parentShardID, i)
		gen := &drsyncpb.FileGen{Size: bf.Size, MtimeNs: bf.MtimeNs}
		for c := 0; c < nChunks; c++ {
			off := uint64(c) * chunkSize
			length := chunkSize
			if off+length > bf.Size {
				length = bf.Size - off
			}
			payload, err := proto.Marshal(&drsyncpb.ChunkTask{
				RelPath: rel, Offset: off, Length: length, Gen: gen,
				CreateTemp: c == 0, TempName: temp})
			if err != nil {
				return nil, nil, err
			}
			shards = append(shards, store.NewShard{
				Kind: model.KindChunk, RelPath: rel, Payload: payload})
		}
		groups = append(groups, store.NewChunkGroup{
			RelPath: rel, TempName: temp, Size: bf.Size, MtimeNs: bf.MtimeNs, NChunks: nChunks})
	}
	return shards, groups, nil
}

// finalizeShard builds the terminal chunk task for a group: no byte range, just
// fsync + metadata + rename of the assembled temp into place. It carries the
// same gen as the data chunks so the finalize aborts (rather than renaming a
// stale temp) if the source drifted while the file was being assembled.
func finalizeShard(rel, temp string, gen *drsyncpb.FileGen) store.NewShard {
	payload, _ := proto.Marshal(&drsyncpb.ChunkTask{
		RelPath: rel, TempName: temp, Finalize: true, Gen: gen})
	return store.NewShard{Kind: model.KindChunk, RelPath: rel, Payload: payload}
}

func (s *Server) onShardResult(ac *agentConn, r *drsyncpb.ShardResult) error {
	shardID, leaseID := int64(r.ShardId), int64(r.LeaseId)
	var err error
	switch r.Status {
	case drsyncpb.ResultStatus_RESULT_OK:
		// One read tells us pass, kind and (for chunks) the ChunkTask, so the
		// chunk group can be maintained in the same completion without a second
		// round trip. It replaces the passOfShard lookup the OK path already did.
		passID, kind, payload, e := s.st.ShardMeta(shardID)
		if e != nil {
			err = e
			break
		}
		// Only successful shards are timed: a shard that failed part-way says
		// nothing about how long the work takes, and would drag the quantiles
		// toward whatever the failure mode's timeout happens to be.
		if ms := r.GetCounters().GetWallMs(); ms > 0 {
			s.met.ShardDuration.WithLabelValues(string(kind)).Observe(float64(ms) / 1000)
		}
		if kind == model.KindChunk {
			err = s.completeChunk(passID, shardID, leaseID, payload, r)
		} else {
			blob, _ := proto.Marshal(r)
			if err = s.st.CompleteShard(shardID, leaseID, blob); err == nil {
				err = s.st.AccumulatePassCounters(passID, r.Counters)
			}
		}
	case drsyncpb.ResultStatus_RESULT_SRC_CHANGED:
		// A chunk saw the source drift under it: abort the whole file's group.
		// The half-written temp is reclaimed as .drsync.tmp residue next walk,
		// and the file is re-diffed next pass. Only chunks emit this status.
		passID, kind, payload, e := s.st.ShardMeta(shardID)
		if e != nil {
			err = e
			break
		}
		if kind == model.KindChunk {
			var ct drsyncpb.ChunkTask
			if err = proto.Unmarshal(payload, &ct); err == nil {
				err = s.st.AbortChunkGroup(shardID, leaseID, passID, ct.RelPath)
			}
		} else {
			err = s.st.RequeueShard(shardID, leaseID, r.Error)
		}
	case drsyncpb.ResultStatus_RESULT_TRANSIENT, drsyncpb.ResultStatus_RESULT_MOUNT_SICK:
		err = s.st.RequeueShard(shardID, leaseID, r.Error)
	case drsyncpb.ResultStatus_RESULT_RELEASED:
		// A draining agent handed back a shard it had queued but never started.
		// Return it to the queue with no error so an active agent picks it up.
		err = s.st.ReleaseShard(shardID, leaseID)
	default:
		err = s.st.ParkShard(shardID, leaseID, r.Error)
		s.met.ShardsParked.Inc()
	}
	if errors.Is(err, store.ErrLeaseMismatch) {
		// Stale result from an expired lease: the shard was (or will be)
		// re-executed elsewhere; idempotency makes dropping this safe.
		slog.Warn("dropping stale shard result", "agent", ac.id, "shard", shardID)
		return nil
	}
	return err
}

// completeChunk finishes a chunk shard and maintains its group. A data chunk's
// completion may seed the finalize shard (done atomically in the store); the
// finalize chunk's completion closes the group. Pass counters are accumulated
// only after a successful, non-duplicate transition, so a re-delivered result
// (ErrLeaseMismatch) neither double-counts nor re-seeds.
func (s *Server) completeChunk(passID, shardID, leaseID int64, payload []byte, r *drsyncpb.ShardResult) error {
	var ct drsyncpb.ChunkTask
	if err := proto.Unmarshal(payload, &ct); err != nil {
		return err
	}
	if ct.Reclaim {
		// Post-drain temp cleanup, not part of the group's assembly: it must not
		// touch n_done (which would re-seed a finalize for a group that has
		// none) or the group's state. Complete it like any ordinary shard.
		blob, _ := proto.Marshal(r)
		if err := s.st.CompleteShard(shardID, leaseID, blob); err != nil {
			return err
		}
	} else if ct.Finalize {
		if err := s.st.CompleteFinalizeChunk(shardID, leaseID, passID, ct.RelPath); err != nil {
			return err
		}
	} else if _, err := s.st.CompleteDataChunk(shardID, leaseID, passID, ct.RelPath,
		finalizeShard(ct.RelPath, ct.TempName, ct.Gen)); err != nil {
		return err
	}
	return s.st.AccumulatePassCounters(passID, r.Counters)
}

func (s *Server) onTaskResults(ac *agentConn, batch *drsyncpb.TaskResultBatch) error {
	for _, r := range batch.Results {
		res := &drsyncpb.ShardResult{ShardId: r.TaskId, LeaseId: r.LeaseId,
			Status: r.Status, Error: r.Error}
		if err := s.onShardResult(ac, res); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) onJournalBatch(ac *agentConn, b *drsyncpb.JournalBatch) error {
	if err := s.jw.Append(b); err != nil {
		return err
	}
	s.met.JournalBatches.Inc()
	// Defer the ack: the batch is written but not yet fsynced. The flusher
	// (RunJournalFlusher) fsyncs and sends the ack, so the agent never releases
	// a journal record the coordinator hasn't durably persisted.
	ac.jrnMu.Lock()
	if b.Seq > ac.jrnPending {
		ac.jrnPending = b.Seq
	}
	ac.jrnMu.Unlock()
	return nil
}

// RunJournalFlusher periodically fsyncs persisted journal batches and then acks
// each agent up to its durable high-water. It replaces an immediate post-write
// ack: the fsync barrier is what lets JournalAck mean "durable" (design §5 and
// agent/src/jrn.c — a shard's result waits on its journal ack). Runs until ctx
// is cancelled, with a final flush+ack so a clean shutdown persists and
// acknowledges the last interval's records.
func (s *Server) RunJournalFlusher(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.flushAndAck()
			return
		case <-t.C:
			s.flushAndAck()
		}
	}
}

func (s *Server) flushAndAck() {
	// Snapshot each agent's pending high-water BEFORE the fsync. Append and
	// Flush share the writer's mutex, so any seq captured here (whose Append has
	// already returned) is on disk once Flush completes — acking up to it is
	// safe. Batches that arrive during the flush are acked on the next tick.
	type ackTarget struct {
		ac  *agentConn
		seq uint64
	}
	s.mu.Lock()
	var targets []ackTarget
	for _, ac := range s.agents {
		ac.jrnMu.Lock()
		if ac.jrnPending > ac.jrnAcked {
			targets = append(targets, ackTarget{ac, ac.jrnPending})
		}
		ac.jrnMu.Unlock()
	}
	s.mu.Unlock()
	if len(targets) == 0 {
		return // nothing new to persist
	}

	if err := s.jw.Flush(); err != nil {
		// Persisting failed: withhold every ack. Agents keep their send buffers
		// and their shards blocked on jrn_wait_acked; a later successful flush
		// acks them. Never ack an un-fsynced batch.
		s.met.JournalFsyncErr.Inc()
		slog.Error("journal flush failed; withholding acks", "err", err)
		return
	}
	for _, t := range targets {
		t.ac.jrnMu.Lock()
		if t.seq > t.ac.jrnAcked {
			t.ac.jrnAcked = t.seq
		}
		t.ac.jrnMu.Unlock()
		// Best-effort: if the agent is gone, its next session resends unacked
		// records (dedup by (shard_id, seq)), so a dropped ack loses nothing.
		_ = t.ac.send(drsyncpb.FrameType_FRAME_JOURNAL_ACK,
			&drsyncpb.JournalAck{AckedSeq: t.seq})
	}
}

func (s *Server) onStats(ac *agentConn, st *drsyncpb.StatsReport) {
	s.met.ScanEntries.WithLabelValues(ac.id).Set(float64(st.EntriesScanned))
	s.met.CopyFiles.WithLabelValues(ac.id).Set(float64(st.FilesCopied))
	s.met.CopyBytes.WithLabelValues(ac.id).Set(float64(st.BytesCopied))
	s.met.AgentRSS.WithLabelValues(ac.id).Set(float64(st.RssBytes))
}
