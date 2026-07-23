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

type Controller struct {
	st          *store.Store
	journalRoot string
	notifier    *notify.Sender // nil when email is disabled (no SMTP config)
	// NotifyJobDone tells connected agents a job reached a terminal state so they
	// release its cached options and root directory fds. Injected by main; nil in
	// tests. See agentsrv.Server.NotifyJobDone.
	NotifyJobDone func(jobID int64)

	// notifiedParked tracks shard IDs already covered by a parked-shard alert
	// email, so the periodic digest (checkParkedShards, run from tick) never
	// re-notifies the same shard. A shard is forgotten once it leaves parked
	// state (retried or dropped) — see checkParkedShards. In-memory only: a
	// coordinator restart re-sends alerts for shards still parked at boot,
	// which is the safe direction to be wrong in (a redundant email, not a
	// silently dropped one).
	notifiedParkedMu sync.Mutex
	notifiedParked   map[int64]bool
}

// jobTerminal fires the agent notification for a job that just reached a
// terminal state (best-effort; nil hook in tests is a no-op).
func (c *Controller) jobTerminal(jobID int64) {
	if c.NotifyJobDone != nil {
		c.NotifyJobDone(jobID)
	}
}

func New(st *store.Store, journalRoot string) *Controller {
	return &Controller{st: st, journalRoot: journalRoot, notifiedParked: map[int64]bool{}}
}

// SetNotifier wires an email sender for pass/job completion notifications. A
// nil sender leaves notifications disabled (the Sender methods are no-ops).
func (c *Controller) SetNotifier(n *notify.Sender) { c.notifier = n }

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

// Run ticks the lifecycle until ctx is done.
func (c *Controller) Run(ctx context.Context, every time.Duration) {
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
			slog.Error("advance failed", "job", job.Name, "err", err)
		}
	}
	if err := c.checkParkedShards(); err != nil {
		slog.Error("checkParkedShards failed", "err", err)
	}
	return nil
}

// checkParkedShards emails a digest alert for any shard that is parked and
// has not already been notified about, grouped one email per job per tick —
// a burst of shards parking together (e.g. a mount going unhealthy mid-walk)
// becomes one email listing all of them, not one per shard. A shard parking
// can permanently stall its job (advance() will not cross a phase boundary
// while any of that phase's shards are parked), so this tick-driven digest is
// the operator's actual notification path — not job completion, which the
// job may never reach until the parked shard is retried or dropped.
func (c *Controller) checkParkedShards() error {
	if c.notifier == nil || !c.notifier.Enabled() {
		return nil
	}
	all, err := c.st.ParkedShards()
	if err != nil {
		return err
	}
	c.notifiedParkedMu.Lock()
	stillParked := make(map[int64]bool, len(all))
	byJob := map[string][]store.ParkedShard{}
	for _, sh := range all {
		stillParked[sh.ID] = true
		if !c.notifiedParked[sh.ID] {
			byJob[sh.Job] = append(byJob[sh.Job], sh)
		}
	}
	// Forget shards that left parked state (retried or dropped) so a shard
	// that parks again later is alerted on again rather than silently
	// suppressed forever by a stale map entry.
	for id := range c.notifiedParked {
		if !stillParked[id] {
			delete(c.notifiedParked, id)
		}
	}
	for _, shards := range byJob {
		for _, sh := range shards {
			c.notifiedParked[sh.ID] = true
		}
	}
	c.notifiedParkedMu.Unlock()

	for jobName, shards := range byJob {
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
	inFlight := counts[model.ShardQueued] + counts[model.ShardLeased]
	if inFlight > 0 {
		return nil // phase still draining
	}
	if parked := counts[model.ShardParked]; parked > 0 {
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
		return c.st.SetPassState(pass.ID, model.PassDirfix)
	case model.PassDirfix:
		n, err := c.seedVerify(job, pass)
		if err != nil {
			return err
		}
		slog.Info("pass phase: DIRFIX → VERIFY", "job", job.Name,
			"pass", pass.PassNo, "verify_entries", n)
		return c.st.SetPassState(pass.ID, model.PassVerify)
	case model.PassVerify:
		// DELETE phase only on explicit operator trigger (D5); default skips.
		slog.Info("pass complete", "job", job.Name, "pass", pass.PassNo,
			"files_copied", pass.FilesCopied, "bytes_copied", pass.BytesCopied)
		if err := c.st.SetPassState(pass.ID, model.PassComplete); err != nil {
			return err
		}
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
