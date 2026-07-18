// Package events feeds the /api/v1/events WebSocket (DESIGN-coordinator §6):
// job/pass state changes, agent connect/disconnect, parked-shard alerts, and
// 1 Hz stats frames for running jobs. Consumers today: `drsync job status
// --watch` and `drsync events`; the WebUI console subscribes to the same feed.
//
// Events are derived by diffing store snapshots on a 1 s tick rather than by
// instrumenting every state transition: one producer, inherently consistent
// with what the REST API reports, and no coupling into passctrl/agentsrv.
package events

import (
	"context"
	"strconv"
	"sync"
	"time"

	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
)

type Event struct {
	Type   string         `json:"type"` // job_state|pass_state|stats|agent|shard_parked
	TsMs   int64          `json:"ts_ms"`
	Job    string         `json:"job,omitempty"`
	PassNo int            `json:"pass_no,omitempty"`
	Data   map[string]any `json:"data,omitempty"`
}

// Bus fans events out to subscribers. Slow subscribers lose events rather
// than stall the feed (each event is a self-contained snapshot; the next
// stats frame supersedes a dropped one).
type Bus struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

const subBuffer = 256

func NewBus() *Bus { return &Bus{subs: map[chan Event]struct{}{}} }

func (b *Bus) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, subBuffer)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	cancel := func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
	}
	return ch, cancel
}

func (b *Bus) Publish(e Event) {
	if e.TsMs == 0 {
		e.TsMs = time.Now().UnixMilli()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- e:
		default: // drop for this subscriber; feed must never block
		}
	}
}

// ---------------------------------------------------------------------------

// Poller snapshots the store every interval and publishes the diff.
type Poller struct {
	st  *store.Store
	bus *Bus
	// ConnectedAgents is injected by agentsrv (same hook the API uses).
	ConnectedAgents func() []string

	jobStates  map[string]model.JobState
	passStates map[string]model.PassState // key job/pass_no
	agentsLive map[string]bool
	parkedSeen map[int64]bool
	primed     bool // first snapshot seeds baselines silently
}

func NewPoller(st *store.Store, bus *Bus) *Poller {
	return &Poller{st: st, bus: bus,
		ConnectedAgents: func() []string { return nil },
		jobStates:       map[string]model.JobState{},
		passStates:      map[string]model.PassState{},
		agentsLive:      map[string]bool{},
		parkedSeen:      map[int64]bool{},
	}
}

func (p *Poller) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick()
		}
	}
}

func (p *Poller) tick() {
	p.tickJobs()
	p.tickAgents()
	p.tickParked()
	p.primed = true
}

func terminal(st model.JobState) bool {
	return st == model.JobCompleted || st == model.JobCancelled || st == model.JobFailed
}

func (p *Poller) tickJobs() {
	jobs, err := p.st.ListJobs()
	if err != nil {
		return // transient store error: next tick retries
	}
	for _, j := range jobs {
		prev, known := p.jobStates[j.Name]
		changed := !known || prev != j.State
		p.jobStates[j.Name] = j.State
		if changed && p.primed {
			p.bus.Publish(Event{Type: "job_state", Job: j.Name,
				Data: map[string]any{"state": j.State, "prev": prev}})
		}
		// Terminal jobs whose state did not change this tick are settled:
		// no pass transitions or counter movement left to observe.
		if terminal(j.State) && !changed {
			continue
		}
		p.tickPasses(j)
	}
}

func (p *Poller) tickPasses(j *store.Job) {
	passes, err := p.st.ListPasses(j.ID)
	if err != nil || len(passes) == 0 {
		return
	}
	for _, ps := range passes {
		key := j.Name + "/" + strconv.Itoa(ps.PassNo)
		prev, known := p.passStates[key]
		if ps.State == prev {
			continue
		}
		p.passStates[key] = ps.State
		if p.primed && (known || ps.State != model.PassComplete) {
			p.bus.Publish(Event{Type: "pass_state", Job: j.Name, PassNo: ps.PassNo,
				Data: map[string]any{"state": ps.State, "prev": prev}})
		}
	}
	// One stats frame per tick while the job is live (the --watch heartbeat).
	if j.State == model.JobRunning && p.primed {
		ps := passes[len(passes)-1]
		counts, err := p.st.ShardStateCounts(ps.ID)
		if err != nil {
			return
		}
		queue := map[string]int64{}
		for st, n := range counts {
			queue[string(st)] = n
		}
		p.bus.Publish(Event{Type: "stats", Job: j.Name, PassNo: ps.PassNo,
			Data: map[string]any{
				"pass_state":     ps.State,
				"entries_walked": ps.EntriesWalked,
				"files_copied":   ps.FilesCopied,
				"bytes_copied":   ps.BytesCopied,
				"meta_fixed":     ps.MetaFixed,
				"orphans":        ps.Orphans,
				"errors":         ps.Errors,
				"verify_ok":      ps.VerifyOK,
				"verify_fail":    ps.VerifyFail,
				"shards":         queue,
			}})
	}
}

func (p *Poller) tickAgents() {
	live := map[string]bool{}
	for _, id := range p.ConnectedAgents() {
		live[id] = true
	}
	for id := range live {
		if !p.agentsLive[id] && p.primed {
			p.bus.Publish(Event{Type: "agent",
				Data: map[string]any{"id": id, "connected": true}})
		}
	}
	for id := range p.agentsLive {
		if !live[id] && p.primed {
			p.bus.Publish(Event{Type: "agent",
				Data: map[string]any{"id": id, "connected": false}})
		}
	}
	p.agentsLive = live
}

func (p *Poller) tickParked() {
	parked, err := p.st.ParkedShards()
	if err != nil {
		return
	}
	seen := map[int64]bool{}
	for _, sh := range parked {
		seen[sh.ID] = true
		if !p.parkedSeen[sh.ID] && p.primed {
			p.bus.Publish(Event{Type: "shard_parked", Job: sh.Job, PassNo: sh.PassNo,
				Data: map[string]any{"shard_id": sh.ID, "kind": sh.Kind,
					"rel_path": sh.RelPath, "error": sh.Error, "attempt": sh.Attempt}})
		}
	}
	p.parkedSeen = seen
}
