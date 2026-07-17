// Package scheduler turns queued shards into WorkGrants and sweeps expired
// leases (docs/DESIGN-coordinator.md §4).
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// countsTTL bounds how stale the fan-out/fair-share inputs may be. Grant runs
// on every WorkRequest from every agent, so the counters are cached rather than
// re-queried per grant even though both are cheap read-pool queries. A few
// hundred ms of staleness only ever costs a slightly late switch between spread
// and normal budgets; both states are self-correcting.
const countsTTL = 250 * time.Millisecond

// spreadSplitThreshold is the per-shard dir_split_threshold used while fanning
// out. It matches the agent's ENTRYLIST_BATCH (agent/src/walker.c) so that a
// flat directory splits into whole entry-list shards rather than one shard plus
// a remainder — a directory of this size or less is already a single unit of
// work and gains nothing from being cut up.
const spreadSplitThreshold = 4000

// jobPolicy is a job's resolved agent options plus its coordinator-side
// fan-out policy. Parsed once and cached together: both come from the spec.
type jobPolicy struct {
	opts   *drsyncpb.JobOptions
	spread model.SpreadPolicy
}

type Scheduler struct {
	st       *store.Store
	met      *metrics.Metrics
	LeaseTTL time.Duration

	mu       sync.Mutex
	policies map[int64]*jobPolicy // jobID → resolved spec (cache)

	// Fleet/queue snapshot shared by all agents' grants, refreshed on demand.
	countsAt time.Time
	counts   store.SchedulerCounts
	agents   int64
}

func New(st *store.Store, met *metrics.Metrics, leaseTTL time.Duration) *Scheduler {
	return &Scheduler{st: st, met: met, LeaseTTL: leaseTTL,
		policies: map[int64]*jobPolicy{}}
}

// fleet returns the cached agent count and queue snapshot, refreshing them at
// most every countsTTL. Caller must not hold s.mu.
func (s *Scheduler) fleet() (agents int64, counts store.SchedulerCounts) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.countsAt) < countsTTL {
		return s.agents, s.counts
	}
	// Back off on failure as well as success: Grant runs on every WorkRequest
	// from every agent, so retrying a failing query on each one would hammer an
	// already-sick store from inside the scheduler lock.
	s.countsAt = time.Now()

	n, err := s.st.CountSchedulableAgents()
	if err != nil {
		slog.Error("agent count failed; assuming a single agent", "err", err)
		n = 1
	}
	c, err := s.st.SchedulerCounts()
	if err != nil {
		slog.Error("queue count failed; keeping the last snapshot", "err", err)
		// Zero counts read as "fleet starved", which would spread every shard.
		// Keep the previous snapshot instead and re-read after the TTL.
		s.agents = max(s.agents, 1)
		return s.agents, s.counts
	}
	s.agents, s.counts = n, c
	return n, c
}

// walkOverrides resolves a job's per-shard fan-out overrides, or nil to leave
// the agent on the job's own tuning.
//
// While the fleet holds fewer walk shards than it has capacity for, every
// granted shard is told to descend nothing (walk_budget = 0) and to fan even
// modest directories out (split_threshold). The shard therefore pushes each
// subdirectory straight back as a new shard, which the coordinator hands to
// other agents. Fan-out is exponential and self-limiting: once WalkPending
// reaches the target the overrides stop, budgets revert to the job's
// shard_budget, and agents descend deeply in-process with no further round
// trips — so the steady-state cost at PB scale is unchanged (D7).
//
// Without this a volume smaller than shard_budget (250k entries) never splits
// at all: the root shard walks the whole tree on one thread of one agent.
func walkOverrides(pol *jobPolicy, agents int64, c store.SchedulerCounts) *drsyncpb.WalkOverrides {
	var spread bool
	switch pol.spread.Mode {
	case model.SpreadAlways:
		spread = true
	case model.SpreadOff:
		spread = false
	default: // auto
		spread = c.WalkPending < agents*int64(pol.spread.TargetPerAgent)
	}
	if !spread {
		return nil
	}
	// The override may only fan a job out harder than its own tuning, never
	// softer: an operator who set a lower dir_split_threshold did so to break
	// a pathological directory up, and spreading must not quietly undo that.
	split := uint64(spreadSplitThreshold)
	if t := pol.opts.GetTuning().GetDirSplitThreshold(); t > 0 && t < split {
		split = t
	}
	return &drsyncpb.WalkOverrides{
		WalkBudget:     proto.Uint64(0),
		SplitThreshold: proto.Uint64(split),
	}
}

// fairShare caps a grant so the first agent to poll cannot take the whole
// queue. An agent asks for (workers + copy_threads) * 2 credits — 48 on a
// default host — which is more than the entire queue early in a job, so
// without a cap one agent drains it and the rest of the fleet idles.
//
// The cap only applies while the queue is too shallow to fill every agent's
// request; once it is deep there is enough for everyone and the full request
// is granted, so nothing extra is paid at scale. It never caps below 1 and
// never applies to a single-agent fleet: work must never be stranded QUEUED
// because nobody is allowed to take it — the same property LeaseShards' tier-2
// fallback protects.
func fairShare(credits int, queued, agents int64) int {
	if agents < 2 || credits <= 0 || queued >= agents*int64(credits) {
		return credits
	}
	share := int((queued + agents - 1) / agents) // ceil
	if share < 1 {
		share = 1
	}
	return min(share, credits)
}

// Grant leases up to the requested credits of work to the agent and bundles
// JobOptions for any job whose options_hash the agent does not cache yet.
func (s *Scheduler) Grant(agentID string, req *drsyncpb.WorkRequest) (*drsyncpb.WorkGrant, error) {
	credits := int(req.GetShardCredits()) + int(req.GetTaskCredits())
	agents, counts := s.fleet()
	rows, err := s.st.LeaseShards(agentID, fairShare(credits, counts.Queued, agents), s.LeaseTTL)
	if err != nil {
		return nil, err
	}
	grant := &drsyncpb.WorkGrant{}
	needOpts := map[int64]bool{}
	cached := map[uint64]uint64{} // job_id → hash the agent holds
	for _, c := range req.GetCached() {
		cached[c.JobId] = c.OptionsHash
	}

	for _, row := range rows {
		pol, err := s.jobPolicy(row.JobID)
		if err != nil {
			return nil, err
		}
		item, err := s.buildItem(row, walkOverrides(pol, agents, counts))
		if err != nil {
			// Malformed payload: park, never re-grant a poison row.
			slog.Error("unbuildable shard payload; parking", "shard", row.ID, "err", err)
			_ = s.st.ParkShard(row.ID, row.LeaseID, "unbuildable payload: "+err.Error())
			continue
		}
		grant.Items = append(grant.Items, item)
		needOpts[row.JobID] = true
	}

	for jobID := range needOpts {
		pol, err := s.jobPolicy(jobID)
		if err != nil {
			return nil, err
		}
		if cached[uint64(jobID)] != pol.opts.OptionsHash {
			grant.Options = append(grant.Options, pol.opts)
		}
	}
	if n := len(grant.Items); n > 0 {
		s.met.Grants.Add(float64(n))
	}
	return grant, nil
}

// buildItem renders a leased row as a WorkItem. ov carries the fan-out
// overrides for walk shards; it is nil when the job is not being spread, and
// is ignored by the task kinds, which do not walk.
func (s *Scheduler) buildItem(row *store.ShardRow, ov *drsyncpb.WalkOverrides) (*drsyncpb.WorkItem, error) {
	item := &drsyncpb.WorkItem{
		LeaseId:   uint64(row.LeaseID),
		LeaseTtlS: uint32(s.LeaseTTL / time.Second),
	}
	jobID, passNo := uint64(row.JobID), uint32(row.PassNo)
	switch row.Kind {
	case model.KindDir:
		item.Item = &drsyncpb.WorkItem_Shard{Shard: &drsyncpb.Shard{
			ShardId: uint64(row.ID), JobId: jobID, PassNo: passNo, RelPath: row.RelPath,
			Overrides: ov}}
	case model.KindEntryList:
		m := &drsyncpb.EntryListShard{}
		if err := proto.Unmarshal(row.Payload, m); err != nil {
			return nil, err
		}
		m.ShardId, m.JobId, m.PassNo = uint64(row.ID), jobID, passNo
		m.Overrides = ov
		item.Item = &drsyncpb.WorkItem_EntryList{EntryList: m}
	case model.KindChunk:
		m := &drsyncpb.ChunkTask{}
		if err := proto.Unmarshal(row.Payload, m); err != nil {
			return nil, err
		}
		m.TaskId, m.JobId, m.PassNo = uint64(row.ID), jobID, passNo
		item.Item = &drsyncpb.WorkItem_Chunk{Chunk: m}
	case model.KindDirfix:
		m := &drsyncpb.DirFixBatch{}
		if err := proto.Unmarshal(row.Payload, m); err != nil {
			return nil, err
		}
		m.TaskId, m.JobId, m.PassNo = uint64(row.ID), jobID, passNo
		item.Item = &drsyncpb.WorkItem_Dirfix{Dirfix: m}
	case model.KindVerify:
		m := &drsyncpb.VerifyBatch{}
		if err := proto.Unmarshal(row.Payload, m); err != nil {
			return nil, err
		}
		m.TaskId, m.JobId, m.PassNo = uint64(row.ID), jobID, passNo
		item.Item = &drsyncpb.WorkItem_Verify{Verify: m}
	case model.KindDelete:
		m := &drsyncpb.DeleteBatch{}
		if err := proto.Unmarshal(row.Payload, m); err != nil {
			return nil, err
		}
		m.TaskId, m.JobId, m.PassNo = uint64(row.ID), jobID, passNo
		item.Item = &drsyncpb.WorkItem_Delete{Delete: m}
	case model.KindProbe:
		item.Item = &drsyncpb.WorkItem_Probe{Probe: &drsyncpb.ProbeTask{
			TaskId: uint64(row.ID), JobId: jobID}}
	default:
		return nil, fmt.Errorf("unknown shard kind %q", row.Kind)
	}
	return item, nil
}

// jobPolicy returns the job's resolved agent options and fan-out policy,
// parsing the spec once and caching both.
func (s *Scheduler) jobPolicy(jobID int64) (*jobPolicy, error) {
	s.mu.Lock()
	if p := s.policies[jobID]; p != nil {
		s.mu.Unlock()
		return p, nil
	}
	s.mu.Unlock()

	job, err := s.st.GetJobByID(jobID)
	if err != nil {
		return nil, err
	}
	spec, err := model.ParseSpec(job.SpecYAML)
	if err != nil {
		return nil, fmt.Errorf("job %d spec: %w", jobID, err)
	}
	opts, err := spec.ToJobOptions(uint64(jobID), job.DryRun)
	if err != nil {
		return nil, err
	}
	p := &jobPolicy{opts: opts, spread: spec.SpreadPolicy()}
	s.mu.Lock()
	s.policies[jobID] = p
	s.mu.Unlock()
	return p, nil
}

// InvalidateOptions drops a job's cached spec (job update flow).
func (s *Scheduler) InvalidateOptions(jobID int64) {
	s.mu.Lock()
	delete(s.policies, jobID)
	s.mu.Unlock()
}

// RunSweeper expires leases until ctx is done.
func (s *Scheduler) RunSweeper(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			requeued, parked, err := s.st.ExpireLeases(now)
			if err != nil {
				slog.Error("lease sweep failed", "err", err)
				continue
			}
			if requeued > 0 || parked > 0 {
				slog.Warn("expired leases", "requeued", requeued, "parked", parked)
				s.met.LeaseExpiries.Add(float64(requeued + parked))
				s.met.ShardsParked.Add(float64(parked))
			}
		}
	}
}
