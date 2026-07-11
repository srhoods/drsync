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

type Scheduler struct {
	st       *store.Store
	met      *metrics.Metrics
	LeaseTTL time.Duration

	mu      sync.Mutex
	options map[int64]*drsyncpb.JobOptions // jobID → resolved options (cache)
}

func New(st *store.Store, met *metrics.Metrics, leaseTTL time.Duration) *Scheduler {
	return &Scheduler{st: st, met: met, LeaseTTL: leaseTTL,
		options: map[int64]*drsyncpb.JobOptions{}}
}

// Grant leases up to the requested credits of work to the agent and bundles
// JobOptions for any job whose options_hash the agent does not cache yet.
func (s *Scheduler) Grant(agentID string, req *drsyncpb.WorkRequest) (*drsyncpb.WorkGrant, error) {
	credits := int(req.GetShardCredits()) + int(req.GetTaskCredits())
	rows, err := s.st.LeaseShards(agentID, credits, s.LeaseTTL)
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
		item, err := s.buildItem(row)
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
		opts, err := s.jobOptions(jobID)
		if err != nil {
			return nil, err
		}
		if cached[uint64(jobID)] != opts.OptionsHash {
			grant.Options = append(grant.Options, opts)
		}
	}
	if n := len(grant.Items); n > 0 {
		s.met.Grants.Add(float64(n))
	}
	return grant, nil
}

func (s *Scheduler) buildItem(row *store.ShardRow) (*drsyncpb.WorkItem, error) {
	item := &drsyncpb.WorkItem{
		LeaseId:   uint64(row.LeaseID),
		LeaseTtlS: uint32(s.LeaseTTL / time.Second),
	}
	jobID, passNo := uint64(row.JobID), uint32(row.PassNo)
	switch row.Kind {
	case model.KindDir:
		item.Item = &drsyncpb.WorkItem_Shard{Shard: &drsyncpb.Shard{
			ShardId: uint64(row.ID), JobId: jobID, PassNo: passNo, RelPath: row.RelPath}}
	case model.KindEntryList:
		m := &drsyncpb.EntryListShard{}
		if err := proto.Unmarshal(row.Payload, m); err != nil {
			return nil, err
		}
		m.ShardId, m.JobId, m.PassNo = uint64(row.ID), jobID, passNo
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

func (s *Scheduler) jobOptions(jobID int64) (*drsyncpb.JobOptions, error) {
	s.mu.Lock()
	if o := s.options[jobID]; o != nil {
		s.mu.Unlock()
		return o, nil
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
	s.mu.Lock()
	s.options[jobID] = opts
	s.mu.Unlock()
	return opts, nil
}

// InvalidateOptions drops a job's cached options (job update flow).
func (s *Scheduler) InvalidateOptions(jobID int64) {
	s.mu.Lock()
	delete(s.options, jobID)
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
