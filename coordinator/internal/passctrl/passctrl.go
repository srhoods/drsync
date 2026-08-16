// Package passctrl drives job and pass lifecycles: seeding passes, advancing
// pass phases as their shard queues drain, and the convergence decision
// (docs/DESIGN-coordinator.md §2).
package passctrl

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"drsync/coordinator/internal/journal"
	"drsync/coordinator/internal/metrics"
	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/notify"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// deleteBatchSize bounds rel-paths per delete task (frame size + granularity).
const deleteBatchSize = 1000

// verifyBatchSize bounds entries per verify task.
const verifyBatchSize = 500

// dirfixBatchSize bounds directories per DirFixBatch task.
const dirfixBatchSize = 1000

// linkfixBatchSize bounds hardlink-group members per LinkTaskBatch task.
// Before this existed, seedLinkfix inserted one shard (and therefore one
// held lease) per pending member — fine at ordinary hardlink density, but at
// millions of members in a single LINKFIX phase it produced millions of
// shard rows from one InsertShards call (holding the coordinator's single
// write lock for the whole insert, stalling every agent's dispatch/heartbeat
// fleet-wide) and could leave one agent holding more concurrent leases than
// its heartbeat can report in one frame (agent/src/main.c AGENT_MAX_LEASES),
// so leases past that cap were never renewed and expired out from under
// still-running work. Matches dirfixBatchSize/verifyBatchSize in kind, not
// value — a LinkEntry is smaller than a DirMeta/VerifyEntry, so this can run
// higher before approaching WIRE_MAX_FRAME (16 MiB).
const linkfixBatchSize = 2000

type Controller struct {
	st          *store.Store
	journalRoot string
	notifier    *notify.Sender   // nil when email is disabled (no SMTP config)
	met         *metrics.Metrics // nil in tests that don't call SetMetrics
	// NotifyJobDone tells connected agents a job reached a terminal state so they
	// release its cached options and root directory fds. Injected by main; nil in
	// tests. See agentsrv.Server.NotifyJobDone.
	NotifyJobDone func(jobID int64)

	// parkedAlertMu guards parkedAlerted and parkedRollup below.
	parkedAlertMu sync.Mutex
	// parkedAlerted tracks shard IDs already covered by a parked-shard alert —
	// sent, or currently held in a job's pending rollup batch — so the
	// periodic check (checkParkedShards, run from tick) never double-counts a
	// shard. A shard is forgotten once it leaves parked state (retried or
	// dropped) — see checkParkedShards. In-memory only: a coordinator restart
	// re-sends alerts for shards still parked at boot, which is the safe
	// direction to be wrong in (a redundant email, not a silently dropped one).
	parkedAlerted map[int64]bool
	// parkedRollup holds, per job, the shards newly parked since that job's
	// last parked-shard email plus when that email went out — see
	// checkParkedShards for the send-now-vs-hold decision this drives: a
	// job's first newly-parked shard (or the first since parkedRollup last
	// flushed) emails immediately, and any more that park within the
	// notifier's configured rollup window are batched here instead of each
	// firing their own email, then flushed together once the window elapses.
	parkedRollup map[string]*parkedRollupState

	// parkedAutoRetried tracks pass IDs whose parked backlog has already been
	// given one automatic retry round (see advance()'s parked-shard gate) —
	// so a persistently-failing shard that re-parks after the retry blocks
	// the pass for operator attention as before, instead of looping forever.
	// Guarded by parkedAlertMu (same lock as the other parked-tracking maps;
	// a dedicated mutex would only ever be taken alongside it in practice).
	// In-memory only, same "safe direction to be wrong in" reasoning as
	// parkedAlerted: a coordinator restart re-attempts one retry round for a
	// pass that was already retried before the restart — a redundant but
	// harmless extra attempt, not a correctness issue. Forgotten once a pass
	// leaves parked-blocked state (advances or the job ends), so a job that
	// runs again later gets its own fresh retry round.
	parkedAutoRetried map[int64]bool

	// reapCh feeds runReapWorker — see its doc comment for why reaping runs on
	// its own goroutine instead of inline in advance().
	reapCh chan reapRequest
	// reapInflight dedups reapCh: a pass already queued or being worked is not
	// queued again, so a busy eager-SCANNING-reap pass firing every tick while
	// counts[ShardDone] > 0 (advance's own gate, unchanged) cannot pile up
	// redundant requests behind a worker that has not caught up yet.
	reapInflightMu sync.Mutex
	reapInflight   map[reapKey]bool
}

// reapKey identifies one (pass, reap-kind-set) unit of dedup for reapInflight.
// registry is true for a reapLinkRegistry request, false for reapPhase — the
// two touch disjoint tables, so a pass can have one of each in flight without
// colliding on this key.
type reapKey struct {
	passID   int64
	registry bool
}

// reapRequest is one deferred reap, enqueued by advance() and drained by
// runReapWorker.
type reapRequest struct {
	job      *store.Job
	pass     *store.Pass
	kinds    []model.ShardKind
	registry bool // true: reapLinkRegistry(job, pass); false: reapPhase(job, pass, kinds...)
}

// reapChanBuffer bounds reapCh. Generous: with reapInflight deduping repeat
// requests for the same pass, the only way this fills is many distinct passes
// all reaching a reap point faster than the worker drains them, which is far
// beyond any fleet this coordinator targets — sized so a send never blocks
// advance() in practice, with enqueueReap's non-blocking select as the actual
// guarantee.
const reapChanBuffer = 256

type parkedRollupState struct {
	lastSentAt time.Time
	pending    []store.ParkedShard
}

// jobTerminal fires the agent notification for a job that just reached a
// terminal state (best-effort; nil hook in tests is a no-op).
func (c *Controller) jobTerminal(jobID int64) {
	if c.NotifyJobDone != nil {
		c.NotifyJobDone(jobID)
	}
}

func New(st *store.Store, journalRoot string) *Controller {
	return &Controller{st: st, journalRoot: journalRoot,
		parkedAlerted: map[int64]bool{}, parkedRollup: map[string]*parkedRollupState{},
		parkedAutoRetried: map[int64]bool{},
		reapCh:            make(chan reapRequest, reapChanBuffer), reapInflight: map[reapKey]bool{}}
}

// SetNotifier wires an email sender for pass/job completion notifications. A
// nil sender leaves notifications disabled (the Sender methods are no-ops).
func (c *Controller) SetNotifier(n *notify.Sender) { c.notifier = n }

// SetMetrics wires the Prometheus counters reapPhase reports to. Left nil in
// tests that don't call this — reapPhase's nil check makes that a no-op.
func (c *Controller) SetMetrics(m *metrics.Metrics) { c.met = m }

// StartJob transitions READY→RUNNING and seeds pass 1 with the root shard.
func (c *Controller) StartJob(name string) error {
	job, err := c.st.GetJob(name)
	if err != nil {
		return fmt.Errorf("job %q: %w", name, err)
	}
	switch job.State {
	case model.JobReady:
	case model.JobPaused:
		if err := c.destinationFree(job); err != nil {
			return err
		}
		return c.st.SetJobState(job.ID, model.JobRunning)
	default:
		return fmt.Errorf("job %q is %s; expected READY", name, job.State)
	}
	if err := c.destinationFree(job); err != nil {
		return err
	}
	if err := c.st.SetJobState(job.ID, model.JobRunning); err != nil {
		return err
	}
	return c.seedPass(job.ID, 1)
}

// ResumeJob returns a PAUSED job to RUNNING. It exists so resume goes through
// the same destination check as start: while the job was paused another job may
// have taken over its tree, and resuming into that is the corruption this gate
// prevents.
func (c *Controller) ResumeJob(name string) error {
	job, err := c.st.GetJob(name)
	if err != nil {
		return fmt.Errorf("job %q: %w", name, err)
	}
	if job.State != model.JobPaused {
		return fmt.Errorf("job %q is %s; expected PAUSED", name, job.State)
	}
	if err := c.destinationFree(job); err != nil {
		return err
	}
	return c.st.SetJobState(job.ID, model.JobRunning)
}

// destinationFree refuses to start a job whose destination tree is being
// written by another running job. Submit already rejects overlapping
// destinations, so this is a backstop rather than the primary gate — but it is
// the one that holds when submit could not: jobs created before that check
// existed, and two overlapping submits racing (each passes its check before the
// other's row is visible, though CreateJob now closes that window by checking
// under the insert's lock).
//
// Only RUNNING/PAUSED count here. A READY job has not started, so blocking on
// one would deadlock two jobs that each refuse to go first.
func (c *Controller) destinationFree(job *store.Job) error {
	spec, err := model.ParseSpec(job.SpecYAML)
	if err != nil {
		return fmt.Errorf("job %q: %w", job.Name, err)
	}
	return c.st.DestinationConflict(job.Name, spec.Spec.Destination.Path,
		store.JobStatesRunning...)
}

func (c *Controller) seedPass(jobID int64, passNo int) error {
	// Gate the pass on a per-agent mount probe: each currently-schedulable agent
	// gets one probe shard pinned to it (target_agent), and the root walk shard
	// is withheld until every probe reports OK (advance, PassProbing case). This
	// catches a missing or misordered mount on ANY host before bulk work runs —
	// not just on whichever agent happened to grab the root shard.
	agents, err := c.st.SchedulableAgents()
	if err != nil {
		return err
	}
	if len(agents) == 0 {
		// Empty fleet: nothing to probe and nothing to grant the root shard to
		// yet. Fall back to the ungated root shard (a late-joining agent with a
		// bad mount still fails its own work with RESULT_MOUNT_SICK).
		pass, err := c.st.CreatePass(jobID, passNo, model.PassScanning)
		if err != nil {
			return err
		}
		if _, err := c.st.InsertShards(pass.ID, 0,
			[]store.NewShard{{Kind: model.KindDir, RelPath: ""}}); err != nil {
			return err
		}
		slog.Info("pass seeded (no agents to probe)", "job", jobID, "pass", passNo)
		return nil
	}
	pass, err := c.st.CreatePass(jobID, passNo, model.PassProbing)
	if err != nil {
		return err
	}
	probes := make([]store.NewShard, 0, len(agents))
	for _, a := range agents {
		probes = append(probes, store.NewShard{Kind: model.KindProbe, TargetAgent: a})
	}
	if _, err := c.st.InsertShards(pass.ID, 0, probes); err != nil {
		return err
	}
	slog.Info("pass seeded; probing mounts", "job", jobID, "pass", passNo, "agents", len(agents))
	return nil
}

// Run ticks the lifecycle until ctx is done, alongside the dedicated reap
// worker (see runReapWorker) that drains reapCh.
func (c *Controller) Run(ctx context.Context, every time.Duration) {
	go c.runReapWorker(ctx)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.tick(); err != nil {
				slog.Error("passctrl tick failed", "err", err)
			}
		}
	}
}

func (c *Controller) tick() error {
	jobs, err := c.st.ListJobs()
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.State != model.JobRunning {
			continue
		}
		if err := c.advance(job); err != nil {
			pass, perr := c.st.ActivePass(job.ID)
			passNo, passState := -1, "?"
			if perr == nil && pass != nil {
				passNo, passState = pass.PassNo, string(pass.State)
			}
			slog.Error("advance failed", "job", job.Name, "pass", passNo,
				"pass_state", passState, "err", err)
		}
	}
	if err := c.checkParkedShards(); err != nil {
		slog.Error("checkParkedShards failed", "err", err)
	}
	return nil
}

// checkParkedShards alerts on shards that are parked and not yet covered by
// an email, rolled up per job so a burst — whether every shard parks in the
// same tick, or they trickle in one at a time over minutes (e.g. a mount
// degrading mid-walk) — sends one email, not one per shard or one per tick.
// A job's first newly-parked shard since its last alert emails immediately;
// anything more that parks within the notifier's configured rollup window
// (Config.ParkedShardRollupSeconds, default 5 minutes) is held and merged
// into a single follow-up once the window elapses, rather than each firing
// its own email. A shard parking can permanently stall its job (advance()
// will not cross a phase boundary while any of that phase's shards are
// parked), so this tick-driven check is the operator's actual notification
// path — not job completion, which the job may never reach until the parked
// shard is retried or dropped.
func (c *Controller) checkParkedShards() error {
	if c.notifier == nil || !c.notifier.Enabled() {
		return nil
	}
	all, err := c.st.ParkedShards()
	if err != nil {
		return err
	}
	now := time.Now()
	rollup := c.notifier.ParkedShardRollup()

	c.parkedAlertMu.Lock()
	stillParked := make(map[int64]bool, len(all))
	newByJob := map[string][]store.ParkedShard{}
	for _, sh := range all {
		stillParked[sh.ID] = true
		if !c.parkedAlerted[sh.ID] {
			newByJob[sh.Job] = append(newByJob[sh.Job], sh)
			c.parkedAlerted[sh.ID] = true
		}
	}
	// Forget shards that left parked state (retried or dropped) so a shard
	// that parks again later is alerted on again rather than silently
	// suppressed forever by a stale map entry.
	for id := range c.parkedAlerted {
		if !stillParked[id] {
			delete(c.parkedAlerted, id)
		}
	}

	// Decide send-now vs. hold for every job with new parks this tick, and
	// separately flush any job whose hold window has now elapsed even without
	// a new park this tick (otherwise a burst that stops arriving right at
	// the window edge could wait for a park that never comes).
	toSend := map[string][]store.ParkedShard{}
	for jobName, shards := range newByJob {
		st := c.parkedRollup[jobName]
		if st == nil {
			st = &parkedRollupState{}
			c.parkedRollup[jobName] = st
		}
		if st.lastSentAt.IsZero() || now.Sub(st.lastSentAt) >= rollup {
			toSend[jobName] = append(st.pending, shards...)
			st.pending = nil
			st.lastSentAt = now
		} else {
			st.pending = append(st.pending, shards...)
		}
	}
	for jobName, st := range c.parkedRollup {
		if len(st.pending) > 0 && now.Sub(st.lastSentAt) >= rollup {
			toSend[jobName] = append(toSend[jobName], st.pending...)
			st.pending = nil
			st.lastSentAt = now
		}
	}
	c.parkedAlertMu.Unlock()

	for jobName, shards := range toSend {
		job, err := c.st.GetJob(jobName)
		if err != nil {
			slog.Warn("notify: lookup job for parked-shard alert failed", "job", jobName, "err", err)
			continue
		}
		c.emailParkedShards(job, shards)
	}
	return nil
}

// emailParkedShards sends the alert for one job's newly-parked shards.
// notifications.recipients gates it (like every other email); it does not
// require on_job_complete, since a parked shard is an operator action item
// independent of whether the spec opts into routine completion reporting.
func (c *Controller) emailParkedShards(job *store.Job, shards []store.ParkedShard) {
	spec, err := model.ParseSpec(job.SpecYAML)
	if err != nil {
		slog.Warn("notify: parse spec failed", "job", job.Name, "err", err)
		return
	}
	n := spec.Spec.Notifications
	if len(n.Recipients) == 0 {
		return
	}
	rows := make([]notify.ParkedShardRow, len(shards))
	for i, sh := range shards {
		rows[i] = notify.ParkedShardRow{
			PassNo: sh.PassNo, Kind: string(sh.Kind), RelPath: sh.RelPath,
			Attempt: sh.Attempt, Error: sh.Error, LastAgent: sh.LastAgent,
		}
	}
	c.notifier.ParkedShards(n.Recipients, notify.ParkedShardsReport{
		Job: job.Name, Src: spec.Spec.Source.Path, Dst: spec.Spec.Destination.Path,
		Shards: rows,
	})
}

func (c *Controller) advance(job *store.Job) error {
	pass, err := c.st.ActivePass(job.ID)
	if err != nil {
		return err
	}
	if pass == nil {
		return nil // between passes; decideNextPass already ran
	}
	if pass.State == model.PassProbing {
		// Drop probes pinned to agents that left after seeding, so the phase is
		// not stalled by a shard no live agent can lease.
		if n, err := c.st.PruneStaleProbes(pass.ID); err != nil {
			return err
		} else if n > 0 {
			slog.Warn("pruned probes for departed agents", "job", job.Name,
				"pass", pass.PassNo, "pruned", n)
		}
	}
	counts, err := c.st.ShardStateCounts(pass.ID)
	if err != nil {
		return err
	}
	// Eager reap: a DONE probe/dir/entrylist/chunk shard is safe to delete the
	// instant it lands, not just once the whole SCANNING phase drains. The
	// agent's own protocol invariant is what makes this safe (agent/src/
	// walker.c ship_split/await_split, protocol doc §4.2): every ShardSplit
	// for a shard is acked — and therefore durably recorded in the splits
	// table via RecordSplit's (parent_shard_id, seq) idempotency key — BEFORE
	// that shard's own ShardResult is ever sent, so once a shard reaches DONE
	// no further split traffic can reference it as a parent. ExpireLeases
	// only ever touches LEASED rows, so a DONE shard is never requeued either
	// — nothing can make it QUEUED/LEASED again. That is the same safety
	// reapPhase already relies on at the phase-drain point (below); this just
	// stops waiting for the whole phase to finish before applying it, so
	// shards/splits stop growing to their SCANNING-phase peak on a large
	// tree. Runs every tick a SCANNING pass has any DONE row of these kinds —
	// counts is already fetched above for the drain check, so this costs one
	// extra ReapDoneShards call, not an extra query, when there's nothing to
	// reap. reapPhase's own batching/budget/shards_reaped accounting applies
	// unchanged; this is the same call, just gated differently.
	if pass.State == model.PassScanning && counts[model.ShardDone] > 0 {
		c.enqueueReap(job, pass, model.KindProbe, model.KindDir, model.KindEntryList, model.KindChunk)
	}
	inFlight := counts[model.ShardQueued] + counts[model.ShardLeased]
	if inFlight > 0 {
		return nil // phase still draining
	}
	if parked := counts[model.ShardParked]; parked > 0 {
		// Give the backlog one automatic retry round before blocking for an
		// operator: a parked shard exhausted MaxShardAttempts (5) grants, but
		// many park causes are transient at the point they're hit (a mount
		// blip, a brief NFS hiccup) rather than deterministic — by the time
		// the rest of the pass has drained (potentially hours later on a
		// large tree), conditions may well have changed, and RetryParkedByJob
		// resets the attempt counter, so a retried shard gets the same fresh
		// 5-attempt budget as any new shard rather than one bonus try. Only
		// one round: parkedAutoRetried (keyed by pass, not shard — shard IDs
		// change across the retry) stops this from firing every tick forever
		// on a genuinely stuck shard, which would just be an infinite retry
		// loop wearing a different name. If it re-parks after the one round,
		// fall through to the existing block-and-alert behavior below.
		c.parkedAlertMu.Lock()
		alreadyRetried := c.parkedAutoRetried[pass.ID]
		if !alreadyRetried {
			c.parkedAutoRetried[pass.ID] = true
		}
		c.parkedAlertMu.Unlock()
		if !alreadyRetried {
			n, err := c.st.RetryParkedByJob(job.Name)
			if err != nil {
				return fmt.Errorf("auto-retry parked shards: %w", err)
			}
			slog.Info("auto-retried parked shards at end of pass", "job", job.Name,
				"pass", pass.PassNo, "retried", n)
			return nil // re-queued work needs a later tick to grant/drain
		}
		// Do not advance past parked work silently; operator resolves via API.
		slog.Warn("pass blocked on parked shards", "job", job.Name,
			"pass", pass.PassNo, "parked", parked)
		return nil
	}

	switch pass.State {
	case model.PassProbing:
		// Every probe drained without parking (the parked guard above catches a
		// failed mount and blocks here), so all mounts verified. Insert the root
		// walk shard BEFORE flipping to SCANNING — otherwise a tick could see
		// SCANNING with an empty queue and skip the walk straight to DIRFIX.
		if _, err := c.st.InsertShards(pass.ID, 0,
			[]store.NewShard{{Kind: model.KindDir, RelPath: ""}}); err != nil {
			return err
		}
		slog.Info("pass phase: PROBING → SCANNING (mounts verified)",
			"job", job.Name, "pass", pass.PassNo)
		return c.st.SetPassState(pass.ID, model.PassScanning)
	case model.PassScanning:
		// Reclaim the temps of chunk groups that never finalized. Safe exactly
		// here and nowhere earlier: the drain check above proves no chunk shard
		// of this pass is queued or leased, so no name being unlinked can still
		// be in use. The agent sweep cannot do this itself — it skips temps
		// tagged with the pass it is running, which is what stops a re-walk
		// from deleting a group's temp mid-assembly — so without this a job
		// ending on this pass would leave them in the destination for good.
		r, err := c.seedTempReclaim(pass)
		if err != nil {
			return err
		}
		// Seed DIRFIX shards from the pass journal's DIR_META records, THEN flip
		// the phase — inserting first keeps the queue non-empty so a tick can't
		// see DIRFIX drained and skip straight to VERIFY before the fixes run.
		n, err := c.seedDirfix(job, pass)
		if err != nil {
			return err
		}
		slog.Info("pass phase: SCANNING → DIRFIX", "job", job.Name,
			"pass", pass.PassNo, "dirs", n, "temps_reclaimed", r)
		if err := c.st.SetPassState(pass.ID, model.PassDirfix); err != nil {
			return err
		}
		// The eager reap above (this function, before the drain check) already
		// deletes these kinds' DONE rows continuously while SCANNING runs, so by
		// the time a tick proves the phase fully drained there is normally
		// little or nothing left here. Kept as the final catch-all regardless:
		// it is the same call with the same batching/budget, so a trailing
		// remainder the eager pass hasn't caught up to yet (e.g. this tick is
		// the one where inFlight first hits zero) still gets cleared before the
		// phase transition completes, rather than carrying over into DIRFIX.
		c.enqueueReap(job, pass, model.KindProbe, model.KindDir, model.KindEntryList, model.KindChunk)
		return nil
	case model.PassDirfix:
		// Seed LINKFIX before flipping the phase (same reason as every other
		// transition here: an empty queue between the SetPassState and the
		// first shard would let a tick skip straight past a phase with
		// nothing granted yet).
		n, err := c.seedLinkfix(pass)
		if err != nil {
			return err
		}
		slog.Info("pass phase: DIRFIX → LINKFIX", "job", job.Name,
			"pass", pass.PassNo, "link_tasks", n)
		if err := c.st.SetPassState(pass.ID, model.PassLinkfix); err != nil {
			return err
		}
		c.enqueueReap(job, pass, model.KindDirfix)
		// seedLinkfix (above) is the only reader of link_groups/link_members
		// anywhere in the coordinator, and it has now run for this pass — the
		// registry rows it consumed are permanently settled (see
		// store.ReapLinkRegistry's comment for why no later split/sighting
		// traffic for this pass can still arrive). Safe to reap now, not
		// deferred to job purge.
		c.enqueueReapLinkRegistry(job, pass)
		return nil
	case model.PassLinkfix:
		n, err := c.seedVerify(job, pass)
		if err != nil {
			return err
		}
		slog.Info("pass phase: LINKFIX → VERIFY", "job", job.Name,
			"pass", pass.PassNo, "verify_entries", n)
		if err := c.st.SetPassState(pass.ID, model.PassVerify); err != nil {
			return err
		}
		c.enqueueReap(job, pass, model.KindLinkfix)
		return nil
	case model.PassVerify:
		// DELETE phase only on explicit operator trigger (D5); default skips.
		slog.Info("pass complete", "job", job.Name, "pass", pass.PassNo,
			"files_copied", pass.FilesCopied, "bytes_copied", pass.BytesCopied)
		if err := c.st.SetPassState(pass.ID, model.PassComplete); err != nil {
			return err
		}
		c.recordJournalTypeCounts(job, pass)
		c.enqueueReap(job, pass, model.KindVerify)
		jobDone, converged, err := c.decideNextPass(job, pass)
		if err != nil {
			return err
		}
		c.notifyPassComplete(job, pass, false, jobDone, converged)
		if jobDone {
			c.notifyJobComplete(job)
		}
		return nil
	case model.PassDelete:
		slog.Info("delete pass complete", "job", job.Name, "pass", pass.PassNo,
			"removed", pass.Orphans, "errors", pass.Errors)
		if err := c.st.SetPassState(pass.ID, model.PassComplete); err != nil {
			return err
		}
		c.recordJournalTypeCounts(job, pass)
		c.enqueueReap(job, pass, model.KindDelete)
		// A delete pass never auto-seeds another pass: back to COMPLETED,
		// further passes are explicit operator triggers.
		if err := c.st.SetJobState(job.ID, model.JobCompleted); err != nil {
			return err
		}
		c.jobTerminal(job.ID)
		c.notifyPassComplete(job, pass, true, true, false)
		c.notifyJobComplete(job)
		return nil
	}
	return nil
}

// reapPhaseBudget caps the wall time reapPhase spends looping
// store.ReapDoneShards batches for one phase transition. Each batch is its
// own transaction (store.ReapBatchSize rows), so looping just trades a longer
// single tick for draining a phase's DONE backlog fully rather than trickling
// it out — this only needs to bound how long that one tick's remaining jobs
// wait behind it, not correctness: nothing else reads a DONE row, and a
// backlog this call doesn't finish for hitting the cap is picked up in full
// (from the same LIMIT-bounded query, no lost progress) the next time this
// same call site fires — which, for scan-phase kinds, is only ever this one
// phase transition of this one pass, so exceeding the budget on a truly
// enormous backlog leaves a remainder that lasts until the job is purged.
// 30s at ReapBatchSize keeps that a rare, not routine, outcome.
const reapPhaseBudget = 30 * time.Second

// runReapWorker drains reapCh on its own goroutine, one request at a time,
// until ctx is done. Reaping used to run inline inside advance() — but a
// single busy pass's reap loop (ReapDoneShards/ReapLinkRegistry batches,
// back-to-back with no yield between them) can legitimately run for the
// whole reapPhaseBudget, and every batch independently takes store.mu. Inline
// on the tick goroutine, that meant one job's reap could hold up advance()
// for every *other* running job for up to 30 seconds solid (tick() loops jobs
// serially), while simultaneously re-acquiring store.mu in a tight loop the
// entire time — directly competing with every connected agent's
// heartbeat/dispatch call on every single acquisition. A live fleet showed
// this as "long wait for write lock" storms and agents missing their lease
// TTL in unison. Moving the actual work here means advance() only ever
// enqueues (non-blocking, deduped by reapInflight) and returns immediately;
// tick() keeps moving through the job list, and the reap itself still takes
// store.mu the same way — one goroutine at a time now, instead of contending
// with itself across jobs — but never blocks unrelated coordinator work
// upstream of it.
func (c *Controller) runReapWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-c.reapCh:
			if req.registry {
				c.doReapLinkRegistry(req.job, req.pass)
			} else {
				c.doReapPhase(req.job, req.pass, req.kinds)
			}
			c.reapInflightMu.Lock()
			delete(c.reapInflight, reapKey{req.pass.ID, req.registry})
			c.reapInflightMu.Unlock()
		}
	}
}

// enqueueReap defers a reapPhase call to runReapWorker. Best-effort and
// non-blocking by design: advance() must never stall on this, and a reap
// that doesn't get enqueued this tick (channel full, or one already
// in-flight/queued for this pass) is picked up the next time advance() calls
// this for the same still-eligible pass — the eager-SCANNING-reap call site
// does exactly that every 2s tick already; the others are one-shot per phase
// transition but leave any remainder for the next pass's reap at worst.
func (c *Controller) enqueueReap(job *store.Job, pass *store.Pass, kinds ...model.ShardKind) {
	c.enqueue(reapRequest{job: job, pass: pass, kinds: kinds})
}

// enqueueReapLinkRegistry defers a reapLinkRegistry call to runReapWorker.
// See enqueueReap.
func (c *Controller) enqueueReapLinkRegistry(job *store.Job, pass *store.Pass) {
	c.enqueue(reapRequest{job: job, pass: pass, registry: true})
}

func (c *Controller) enqueue(req reapRequest) {
	key := reapKey{req.pass.ID, req.registry}
	c.reapInflightMu.Lock()
	if c.reapInflight[key] {
		c.reapInflightMu.Unlock()
		return
	}
	c.reapInflight[key] = true
	c.reapInflightMu.Unlock()

	select {
	case c.reapCh <- req:
	default:
		// Channel full: extremely unlikely at reapChanBuffer (see its comment),
		// but this must never block advance(). Clear the inflight mark so a
		// later tick's request for this same pass is not dropped forever.
		c.reapInflightMu.Lock()
		delete(c.reapInflight, key)
		c.reapInflightMu.Unlock()
		slog.Warn("reap request dropped: worker queue full", "job", req.job.Name,
			"pass", req.pass.PassNo, "registry", req.registry)
	}
}

// doReapPhase deletes the now-fully-drained phase's DONE shards, looping
// store.ReapDoneShards batches until the phase's backlog is exhausted or
// reapPhaseBudget elapses. Best-effort: a reap failure must not stop the
// phase transition that already committed before this was enqueued. Runs
// only on runReapWorker's goroutine — see enqueueReap.
func (c *Controller) doReapPhase(job *store.Job, pass *store.Pass, kinds []model.ShardKind) {
	deadline := time.Now().Add(reapPhaseBudget)
	var total int64
	for time.Now().Before(deadline) {
		n, err := c.st.ReapDoneShards(pass.ID, kinds)
		if err != nil {
			slog.Warn("shard reap failed", "job", job.Name, "pass", pass.PassNo,
				"kinds", kinds, "reaped_before_error", total, "err", err)
			break
		}
		total += n
		if n < store.ReapBatchSize {
			break // batch came back short: backlog exhausted
		}
	}
	if total > 0 {
		slog.Info("reaped done shards", "job", job.Name, "pass", pass.PassNo,
			"kinds", kinds, "reaped", total)
		if c.met != nil {
			c.met.ShardsReaped.Add(float64(total))
		}
	}
}

// doReapLinkRegistry deletes a pass's now-fully-consumed link_groups/
// link_members rows, looping store.ReapLinkRegistry batches until the
// backlog is exhausted or reapPhaseBudget elapses (same budget, same
// reasoning as doReapPhase — this call site is likewise only ever enqueued
// once per pass, at the DIRFIX->LINKFIX transition, so a remainder left
// behind by hitting the budget on a truly enormous registry is picked up in
// full next time, which for this call site means "not until the job is
// purged"; rare at LinkRegistryReapBatchSize). Best-effort: a reap failure
// must not stop the phase transition that already committed before this was
// enqueued. Runs only on runReapWorker's goroutine — see
// enqueueReapLinkRegistry.
func (c *Controller) doReapLinkRegistry(job *store.Job, pass *store.Pass) {
	deadline := time.Now().Add(reapPhaseBudget)
	var total int64
	for time.Now().Before(deadline) {
		n, err := c.st.ReapLinkRegistry(pass.ID)
		if err != nil {
			slog.Warn("link registry reap failed", "job", job.Name, "pass", pass.PassNo,
				"reaped_before_error", total, "err", err)
			break
		}
		total += n
		if n < store.LinkRegistryReapBatchSize {
			break // batch came back short: backlog exhausted
		}
	}
	if total > 0 {
		slog.Info("reaped link registry", "job", job.Name, "pass", pass.PassNo,
			"link_groups_reaped", total)
		if c.met != nil {
			c.met.LinkGroupsReaped.Add(float64(total))
		}
	}
}

// decideNextPass applies the convergence rule: stop when the pass delta is
// under the configured thresholds or the pass ceiling is reached. It reports
// whether the job reached COMPLETED (jobDone) and, if so, whether it did so by
// converging (vs. hitting the pass ceiling) so the caller can notify.
func (c *Controller) decideNextPass(job *store.Job, done *store.Pass) (jobDone, converged bool, err error) {
	spec, err := model.ParseSpec(job.SpecYAML)
	if err != nil {
		return false, false, err
	}
	cw := spec.Spec.Passes.ConvergeWhen
	// A pass that copied and fixed nothing is a fixpoint: the trees agree and no
	// further pass can change anything (absent source mutation). That is always
	// convergence, independent of converge_when — otherwise a spec without
	// thresholds spins to Passes.Max after the delta already hit zero. Explicit
	// converge_when thresholds only loosen this, letting a job stop earlier while
	// a small nonzero delta remains.
	converged = done.FilesCopied == 0 && done.MetaFixed == 0 && done.BytesCopied == 0
	if cw.DeltaFilesBelow > 0 && uint64(done.FilesCopied+done.MetaFixed) < cw.DeltaFilesBelow {
		converged = true
	}
	if cw.DeltaBytesBelow > 0 && uint64(done.BytesCopied) < uint64(cw.DeltaBytesBelow) {
		converged = true
	}
	if converged || done.PassNo >= spec.Spec.Passes.Max {
		slog.Info("job converged", "job", job.Name, "passes", done.PassNo,
			"last_delta_files", done.FilesCopied, "last_delta_bytes", done.BytesCopied)
		err := c.st.SetJobState(job.ID, model.JobCompleted)
		if err == nil {
			c.jobTerminal(job.ID)
		}
		return true, converged, err
	}
	if spec.Spec.Passes.Schedule == "manual" {
		slog.Info("awaiting manual pass trigger", "job", job.Name, "next_pass", done.PassNo+1)
		return false, false, nil // operator triggers via POST /passes
	}
	return false, false, c.seedPass(job.ID, done.PassNo+1)
}

// seedDirfix builds DirFixBatch shards from this pass's DIR_META journal
// records so directory metadata — knocked off its source value by cross-shard
// renames into a directory during the walk — is re-applied once the pass has
// drained. Like seedVerify it STREAMS the journal into fixed-size batches, so
// memory is O(dirfixBatchSize), never O(directories-walked). Each batch is
// sorted deepest-first (the order the proto documents); a global sort is
// neither feasible at bounded memory nor meaningful, since batches execute
// concurrently on different agents and applying one directory's metadata never
// affects another's.
// seedTempReclaim queues one reclaim chunk task per chunk group of the pass
// that never finalized, unlinking the temp it left in the destination. Returns
// how many were queued.
//
// These are the groups aborted on source drift (DESIGN-coordinator.md §4): no
// finalize is seeded, so the partially assembled temp is left behind. It used
// to be swept by the next walk that reached the directory, but the agent sweep
// now deliberately spares any temp carrying the pass it is running — the rule
// that stops a re-walk from deleting a group's temp while its chunks are still
// writing — so the residue survives its own pass by design, and a job that ends
// on this pass would never collect it.
//
// The caller must have established that the pass's scan phase has drained
// (advance's queued+leased check). That is what makes unlinking these names
// safe rather than a heuristic: nothing is left running that could still be
// writing to one.
func (c *Controller) seedTempReclaim(pass *store.Pass) (int, error) {
	temps, err := c.st.UnfinalizedChunkTemps(pass.ID)
	if err != nil {
		return 0, err
	}
	shards := make([]store.NewShard, 0, len(temps))
	for _, t := range temps {
		payload, err := proto.Marshal(&drsyncpb.ChunkTask{
			RelPath: t.RelPath, TempName: t.TempName, Reclaim: true})
		if err != nil {
			return 0, err
		}
		shards = append(shards, store.NewShard{
			Kind: model.KindChunk, RelPath: t.RelPath, Payload: payload})
	}
	if len(shards) == 0 {
		return 0, nil
	}
	if _, err := c.st.InsertShards(pass.ID, 0, shards); err != nil {
		return 0, err
	}
	return len(shards), nil
}

// seedLinkfix builds LinkTaskBatch shards — up to linkfixBatchSize members
// each — for every hardlink-group member whose group's anchor copy has
// confirmed landed (docs/DESIGN-hardlinks.md §3.3). Called only once the
// DIRFIX phase has drained: LinkSightings arrive on ShardSplit alongside
// every other kind of shard discovery, so a sighting from any still-queued or
// still-leased shard could still add a pending member after this point —
// draining SCANNING (checked long before DIRFIX even starts) already proves
// no such shard remains, which is the same completeness argument
// seedTempReclaim relies on for chunk residue.
//
// A group whose anchor never reached 'copied' contributes no rows — its
// members already have independent copies from the speculative-anchor walk
// (§3.4), so there is nothing to do for them here; that outcome is counted at
// journal time as LINK_FALLBACK, not here.
//
// Batched like seedDirfix/seedVerify (one InsertShards call per batch, not
// one call for every member): at hardlink-heavy scale a single-call,
// one-shard-per-member seed could hold InsertShards' store lock for the
// whole insert and leave one agent holding more concurrently-leased shards
// than its heartbeat can report — see linkfixBatchSize's comment. Requires a
// minor-3+ fleet (Config.MinAgentMinor): unlike DIRFIX/VERIFY batching, there
// is no older unbatched shape seeded as a fallback for pre-batch agents.
func (c *Controller) seedLinkfix(pass *store.Pass) (int, error) {
	members, err := c.st.PendingLinkMembers(pass.ID)
	if err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, nil
	}
	total := 0
	batchEntries := make([]*drsyncpb.LinkEntry, 0, linkfixBatchSize)
	batchKeys := make([]store.LinkMemberKey, 0, linkfixBatchSize)
	flush := func() error {
		if len(batchEntries) == 0 {
			return nil
		}
		payload, err := proto.Marshal(&drsyncpb.LinkTaskBatch{
			JobId:  uint64(pass.JobID),
			PassNo: uint32(pass.PassNo),
			Links:  batchEntries,
		})
		if err != nil {
			return err
		}
		if _, err := c.st.InsertShards(pass.ID, 0,
			[]store.NewShard{{Kind: model.KindLinkfix, Payload: payload}}); err != nil {
			return err
		}
		if err := c.st.MarkLinkMembersQueued(pass.ID, batchKeys); err != nil {
			return err
		}
		total += len(batchEntries)
		batchEntries = batchEntries[:0]
		batchKeys = batchKeys[:0]
		return nil
	}
	for _, m := range members {
		batchEntries = append(batchEntries, &drsyncpb.LinkEntry{
			AnchorRel: m.AnchorRelPath,
			MemberRel: m.RelPath,
			AnchorGen: &drsyncpb.FileGen{Size: m.AnchorSize, MtimeNs: m.AnchorMtimeNs},
		})
		batchKeys = append(batchKeys, store.LinkMemberKey{Dev: m.Dev, Ino: m.Ino, RelPath: m.RelPath})
		if len(batchEntries) >= linkfixBatchSize {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := flush(); err != nil {
		return total, err
	}
	return total, nil
}

func (c *Controller) seedDirfix(job *store.Job, pass *store.Pass) (int, error) {
	total := 0
	batch := make([]*drsyncpb.DirMeta, 0, dirfixBatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		sort.SliceStable(batch, func(i, j int) bool {
			return bytes.Count(batch[i].RelPath, []byte("/")) >
				bytes.Count(batch[j].RelPath, []byte("/"))
		})
		payload, err := proto.Marshal(&drsyncpb.DirFixBatch{Dirs: batch})
		if err != nil {
			return err
		}
		if _, err := c.st.InsertShards(pass.ID, 0,
			[]store.NewShard{{Kind: model.KindDirfix, Payload: payload}}); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	// NOT deduped against a retried shard's duplicate JR_DIR_META (journaling
	// is at-least-once, same mechanism as seedVerify's own doc comment
	// below) — deliberately: this is streamed in O(batch) memory, and a
	// whole-pass "seen" set would reintroduce the O(N) memory the streaming
	// design exists to avoid. A duplicate DirMeta only means the same
	// directory's metadata gets applied twice — wasteful but not incorrect,
	// since dirfix.c's apply is idempotent. journal.Summary (a bounded
	// per-pass histogram, not a per-file streaming consumer) does dedup this
	// class of duplicate; this function cannot afford to the same way.
	err := journal.ReadRecords(c.journalRoot, job.ID, pass.PassNo,
		func(r *drsyncpb.JournalRecord) error {
			if r.Type != drsyncpb.JournalRecord_JR_DIR_META || r.Src == nil {
				return nil
			}
			// r.RelPath is a fresh slice per record (proto.Unmarshal copies bytes
			// fields), so it is safe to retain past this callback.
			batch = append(batch, &drsyncpb.DirMeta{
				RelPath: r.RelPath,
				Uid:     r.Src.Uid,
				Gid:     r.Src.Gid,
				Mode:    r.Src.Mode,
				AtimeNs: r.Src.AtimeNs,
				MtimeNs: r.Src.MtimeNs,
			})
			total++
			if len(batch) >= dirfixBatchSize {
				return flush()
			}
			return nil
		})
	if err != nil {
		return 0, fmt.Errorf("read journal for dirfix: %w", err)
	}
	if err := flush(); err != nil {
		return 0, err
	}
	return total, nil
}

// seedVerify builds VerifyBatch shards from this pass's own journal (D4):
// every COPIED entry gets a metadata re-check; a deterministic sample of them
// (hash(rel_path) — stable across re-runs) is re-read and checksummed on both
// sides. META_FIXED entries get metadata-only verification. The journal is
// complete here because shard results arrive only after their journal batches
// are acked (protocol §4.2), and this runs only once the pass has drained.
//
// It STREAMS the journal into fixed-size batches, inserting each verify shard
// as its batch fills — memory is O(verifyBatchSize), never O(files-copied), so
// it scales to hundred-million-file passes (state is sized by shards, not
// files). Journaling is at-least-once, so a re-run shard may re-emit a path;
// the resulting duplicate verify entry is idempotent (the file is simply
// verified twice), which we accept rather than hold a whole-pass dedup map in
// the heap. verify.mode=off skips the phase entirely.
func (c *Controller) seedVerify(job *store.Job, pass *store.Pass) (int, error) {
	spec, err := model.ParseSpec(job.SpecYAML)
	if err != nil {
		return 0, err
	}
	if spec.Spec.Verify.Mode == "off" {
		return 0, nil // verification disabled by spec
	}
	ppm := uint64(spec.Spec.Verify.Checksum.SampleRate * 1_000_000)

	total := 0
	batch := make([]*drsyncpb.VerifyEntry, 0, verifyBatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		payload, err := proto.Marshal(&drsyncpb.VerifyBatch{Entries: batch})
		if err != nil {
			return err
		}
		if _, err := c.st.InsertShards(pass.ID, 0,
			[]store.NewShard{{Kind: model.KindVerify, Payload: payload}}); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	// Floor: a pass that copies data never verifies zero bytes. With O(1)
	// memory we cannot retroactively flip a bit in an already-flushed batch, so
	// force-checksum the first COPIED entry whenever sampling is on. This is a
	// superset of "checksum one if the sample picked none" — at most one extra
	// file is hashed — and needs no whole-pass state.
	firstCopied := true

	// NOT deduped against a retried shard's duplicate records (journaling is
	// at-least-once — a shard whose lease expires mid-run is requeued and
	// re-runs from scratch, journal.go's own doc comment: "a re-run shard
	// re-emits its records" — confirmed live: a single lease expiry during a
	// 22k-file pass duplicated 2000 files' worth of records). Deliberately:
	// this function is streamed in O(batch) memory (TestSeedVerifyMemoryBounded
	// pins this at 1M files), and a whole-pass "seen" set to dedup would
	// reintroduce the O(N) memory the streaming design exists to avoid. A
	// duplicate VerifyEntry only means the same file gets verified twice —
	// wasteful (double the read/hash cost for a checksum-sampled file) but not
	// incorrect, since check_entry (agent/src/verify.c) is idempotent either
	// way. journal.Summary (the reporting/audit view, not a per-file streaming
	// consumer) does dedup, since a bounded per-pass type histogram can afford
	// a "seen" set that seedVerify's per-file entries cannot.
	err = journal.ReadRecords(c.journalRoot, job.ID, pass.PassNo,
		func(r *drsyncpb.JournalRecord) error {
			var checksum bool
			switch r.Type {
			case drsyncpb.JournalRecord_JR_COPIED:
				h := fnv.New64a()
				h.Write(r.RelPath)
				checksum = h.Sum64()%1_000_000 < ppm
				if firstCopied && ppm > 0 {
					checksum = true
				}
				firstCopied = false
			case drsyncpb.JournalRecord_JR_META_FIXED:
				checksum = false
			default:
				return nil
			}
			// r.RelPath is a fresh slice per record (proto.Unmarshal copies
			// bytes fields), so it is safe to retain past this callback.
			batch = append(batch, &drsyncpb.VerifyEntry{RelPath: r.RelPath, Checksum: checksum})
			total++
			if len(batch) >= verifyBatchSize {
				return flush()
			}
			return nil
		})
	if err != nil {
		return 0, fmt.Errorf("read journal for verify: %w", err)
	}
	if err := flush(); err != nil {
		return 0, err
	}
	return total, nil
}

// notifyPassComplete emails a per-pass report when the job spec opts in. It is
// best-effort: parse/aggregate failures are logged and swallowed so a
// notification never disturbs the lifecycle. Delivery itself is async.
func (c *Controller) notifyPassComplete(job *store.Job, pass *store.Pass, isDelete, jobDone, converged bool) {
	if c.notifier == nil {
		return
	}
	spec, err := model.ParseSpec(job.SpecYAML)
	if err != nil {
		slog.Warn("notify: parse spec failed", "job", job.Name, "err", err)
		return
	}
	n := spec.Spec.Notifications
	if !n.OnPassComplete || len(n.Recipients) == 0 {
		return
	}
	var dur int64
	if pass.Started.Valid { // finished_at was just stamped; now ≈ finished_at
		dur = time.Now().UnixMilli() - pass.Started.Int64
	}
	c.notifier.PassComplete(n.Recipients, notify.PassReport{
		Job: job.Name, Src: spec.Spec.Source.Path, Dst: spec.Spec.Destination.Path,
		PassNo: pass.PassNo, IsDelete: isDelete, DryRun: job.DryRun,
		DurationMS: dur, FilesCopied: pass.FilesCopied, BytesCopied: pass.BytesCopied,
		MetaFixed: pass.MetaFixed, Orphans: pass.Orphans, VerifyOK: pass.VerifyOK,
		VerifyFail: pass.VerifyFail, Errors: pass.Errors,
		JobDone: jobDone, Converged: converged,
	})
}

// recordJournalTypeCounts computes and persists the per-type journal
// histogram for a pass that has just reached COMPLETE. This is the one point
// in the pass lifecycle where a full scan of that pass's journal is safe to
// do inline: every record for the pass is already durably written (that is
// what COMPLETE means), and it happens once per pass rather than once per
// /report request — the store rollup (store.JournalTypeCounts) is what keeps
// the read side (WebUI job selection, the completion email) off the journal
// files entirely. Best-effort: a scan failure here must not block the phase
// transition that already committed; the summary is just missing until the
// next pass (if any) recomputes it.
func (c *Controller) recordJournalTypeCounts(job *store.Job, pass *store.Pass) {
	counts, _, err := journal.Summary(c.journalRoot, job.ID, []int{pass.PassNo})
	if err != nil {
		slog.Warn("journal summary: scan failed", "job", job.Name, "pass", pass.PassNo, "err", err)
		return
	}
	if err := c.st.SetJournalTypeCounts(pass.ID, counts); err != nil {
		slog.Warn("journal summary: persist failed", "job", job.Name, "pass", pass.PassNo, "err", err)
	}
}

// notifyJobComplete emails the end-of-job summary when the spec opts in.
func (c *Controller) notifyJobComplete(job *store.Job) {
	if c.notifier == nil {
		return
	}
	spec, err := model.ParseSpec(job.SpecYAML)
	if err != nil {
		slog.Warn("notify: parse spec failed", "job", job.Name, "err", err)
		return
	}
	n := spec.Spec.Notifications
	if !n.OnJobComplete || len(n.Recipients) == 0 {
		return
	}
	rep, err := c.buildJobReport(job)
	if err != nil {
		slog.Warn("notify: build job report failed", "job", job.Name, "err", err)
		return
	}
	rep.Src, rep.Dst = spec.Spec.Source.Path, spec.Spec.Destination.Path
	c.notifier.JobComplete(n.Recipients, rep)
}

// buildJobReport aggregates the same view the /report endpoint serves (per-pass
// deltas, totals, outstanding orphans and parked shards) for the summary email.
func (c *Controller) buildJobReport(job *store.Job) (notify.JobReport, error) {
	passes, err := c.st.ListPasses(job.ID)
	if err != nil {
		return notify.JobReport{}, err
	}
	rep := notify.JobReport{
		Job: job.Name, State: string(job.State), DryRun: job.DryRun,
		Converged: job.State == model.JobCompleted,
	}
	for _, p := range passes {
		var dur int64
		if p.Finished.Valid && p.Started.Valid {
			dur = p.Finished.Int64 - p.Started.Int64
		}
		rep.Passes = append(rep.Passes, notify.JobPass{
			PassNo: p.PassNo, State: string(p.State),
			IsDelete:   p.State == model.PassDelete || (p.EntriesWalked == 0 && p.Orphans > 0),
			DurationMS: dur,
			DeltaFiles: p.FilesCopied + p.MetaFixed, DeltaBytes: p.BytesCopied,
			Orphans: p.Orphans, VerifyOK: p.VerifyOK, VerifyFail: p.VerifyFail, Errors: p.Errors,
		})
		rep.FilesCopied += p.FilesCopied
		rep.BytesCopied += p.BytesCopied
		rep.MetaFixed += p.MetaFixed
		rep.Errors += p.Errors
		rep.FidelityExc += p.FidelityExceptions
		rep.VerifyOK += p.VerifyOK
		rep.VerifyFail += p.VerifyFail
		rep.LinksCreated += p.LinksCreated
		rep.LinkAnchorRaces += p.LinkAnchorRaces
		rep.LinkFallback += p.LinkFallback
		if p.EntriesWalked > 0 { // scan pass: latest orphan census supersedes
			rep.OrphansRemaining = p.Orphans
		} else if p.Orphans > 0 { // delete pass reclaimed orphans
			rep.DeletePassRan = true
			rep.OrphansRemaining = 0
		}
	}
	parked, err := c.jobParkedShards(job.Name)
	if err != nil {
		return notify.JobReport{}, err
	}
	rep.ParkedShards = len(parked)

	// From the SQLite rollup (store.JournalTypeCounts), not a journal scan:
	// recordJournalTypeCounts already computed this once when each pass
	// completed, so buildJobReport — called synchronously from the pass-complete
	// path itself — never re-reads the journal files it just finished with.
	summary, _, err := c.st.JournalTypeCounts(job.ID)
	if err != nil {
		// A nice-to-have breakdown on top of the store-backed totals above; a
		// read error here should not sink the whole completion email.
		slog.Warn("notify: journal type counts failed", "job", job.Name, "err", err)
	} else {
		rep.JournalSummary = summary
	}
	return rep, nil
}

// jobParkedShards returns the parked shards belonging to job (store.ParkedShards
// lists fleet-wide, so this filters to the one job both the summary and the
// dedicated parked-shards alert email care about).
func (c *Controller) jobParkedShards(jobName string) ([]store.ParkedShard, error) {
	all, err := c.st.ParkedShards()
	if err != nil {
		return nil, err
	}
	var out []store.ParkedShard
	for _, sh := range all {
		if sh.Job == jobName {
			out = append(out, sh)
		}
	}
	return out, nil
}

// TriggerPass starts the next pass manually. Works on RUNNING jobs between
// passes and on COMPLETED jobs (reopened — the cutover flow: converge,
// COMPLETED, then an explicit delete pass).
func (c *Controller) TriggerPass(name string, deletePass bool) error {
	job, err := c.st.GetJob(name)
	if err != nil {
		return err
	}
	if job.State != model.JobRunning && job.State != model.JobCompleted {
		return fmt.Errorf("job %q is %s; expected RUNNING or COMPLETED", name, job.State)
	}
	active, err := c.st.ActivePass(job.ID)
	if err != nil {
		return err
	}
	if active != nil {
		return fmt.Errorf("job %q already has pass %d in state %s", name, active.PassNo, active.State)
	}
	latest, err := c.st.LatestPass(job.ID)
	if err != nil {
		return err
	}
	next := 1
	if latest != nil {
		next = latest.PassNo + 1
	}

	if deletePass {
		if latest == nil {
			return fmt.Errorf("job %q has no completed pass to harvest orphans from", name)
		}
		return c.seedDeletePass(job, latest, next)
	}
	if err := c.st.SetJobState(job.ID, model.JobRunning); err != nil {
		return err
	}
	return c.seedPass(job.ID, next)
}

// seedDeletePass builds DeleteBatch shards from the previous pass's ORPHAN
// journal records — no additional scan (decision D5).
func (c *Controller) seedDeletePass(job *store.Job, latest *store.Pass, passNo int) error {
	orphans, err := journal.Orphans(c.journalRoot, job.ID, latest.PassNo)
	if err != nil {
		return fmt.Errorf("read orphan journal: %w", err)
	}
	if len(orphans) == 0 {
		return fmt.Errorf("pass %d recorded no orphans; nothing to delete", latest.PassNo)
	}
	// Deepest-first so nested orphan paths never race their parents.
	sort.SliceStable(orphans, func(i, j int) bool {
		return strings.Count(orphans[i], "/") > strings.Count(orphans[j], "/")
	})

	pass, err := c.st.CreatePass(job.ID, passNo, model.PassDelete)
	if err != nil {
		return err
	}
	var shards []store.NewShard
	for start := 0; start < len(orphans); start += deleteBatchSize {
		end := start + deleteBatchSize
		if end > len(orphans) {
			end = len(orphans)
		}
		batch := &drsyncpb.DeleteBatch{}
		for _, p := range orphans[start:end] {
			batch.RelPaths = append(batch.RelPaths, []byte(p))
		}
		payload, err := proto.Marshal(batch)
		if err != nil {
			return err
		}
		shards = append(shards, store.NewShard{Kind: model.KindDelete, Payload: payload})
	}
	if _, err := c.st.InsertShards(pass.ID, 0, shards); err != nil {
		return err
	}
	if err := c.st.SetJobState(job.ID, model.JobRunning); err != nil {
		return err
	}
	slog.Info("delete pass seeded", "job", job.Name, "pass", passNo,
		"orphans", len(orphans), "batches", len(shards))
	return nil
}
