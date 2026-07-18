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
}

func New(st *store.Store, journalRoot string) *Controller {
	return &Controller{st: st, journalRoot: journalRoot}
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
		return c.st.SetJobState(job.ID, model.JobRunning)
	default:
		return fmt.Errorf("job %q is %s; expected READY", name, job.State)
	}
	if err := c.st.SetJobState(job.ID, model.JobRunning); err != nil {
		return err
	}
	return c.seedPass(job.ID, 1)
}

func (c *Controller) seedPass(jobID int64, passNo int) error {
	pass, err := c.st.CreatePass(jobID, passNo, model.PassScanning)
	if err != nil {
		return err
	}
	// TODO(phase1): a KindProbe task per registered agent gates the pass on
	// mount capability probes before the root shard is granted.
	_, err = c.st.InsertShards(pass.ID, 0, []store.NewShard{
		{Kind: model.KindDir, RelPath: ""},
	})
	if err == nil {
		slog.Info("pass seeded", "job", jobID, "pass", passNo)
	}
	return err
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
	return nil
}

func (c *Controller) advance(job *store.Job) error {
	pass, err := c.st.ActivePass(job.ID)
	if err != nil {
		return err
	}
	if pass == nil {
		return nil // between passes; decideNextPass already ran
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
	case model.PassScanning:
		// Seed DIRFIX shards from the pass journal's DIR_META records, THEN flip
		// the phase — inserting first keeps the queue non-empty so a tick can't
		// see DIRFIX drained and skip straight to VERIFY before the fixes run.
		n, err := c.seedDirfix(job, pass)
		if err != nil {
			return err
		}
		slog.Info("pass phase: SCANNING → DIRFIX", "job", job.Name,
			"pass", pass.PassNo, "dirs", n)
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
		return true, converged, c.st.SetJobState(job.ID, model.JobCompleted)
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
		Job: job.Name, PassNo: pass.PassNo, IsDelete: isDelete, DryRun: job.DryRun,
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
	parked, err := c.st.ParkedShards()
	if err != nil {
		return notify.JobReport{}, err
	}
	for _, sh := range parked {
		if sh.Job == job.Name {
			rep.ParkedShards++
		}
	}
	return rep, nil
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
