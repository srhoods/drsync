package passctrl

import (
	"testing"

	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"

	"path/filepath"
)

const baseSpec = `
apiVersion: drsync/v1
kind: Job
metadata:
  name: t1
spec:
  source: { path: /src }
  destination: { path: /dst }
`

// withConverge appends a converge_when block to the base spec.
func withConverge(block string) []byte {
	return []byte(baseSpec + "  passes:\n" + block)
}

func newController(t *testing.T) *Controller {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return New(st, t.TempDir())
}

// makeJob creates a RUNNING job from spec and returns it.
func makeJob(t *testing.T, c *Controller, spec []byte) *store.Job {
	t.Helper()
	job, err := c.st.CreateJob("t1", spec, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.st.SetJobState(job.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	return job
}

func jobState(t *testing.T, c *Controller, id int64) model.JobState {
	t.Helper()
	j, err := c.st.GetJobByID(id)
	if err != nil {
		t.Fatal(err)
	}
	return j.State
}

// The quirk: with no converge_when, a zero-delta pass (nothing copied or fixed)
// is a fixpoint and must complete the job instead of spinning to Passes.Max.
func TestConvergeZeroDeltaNoThreshold(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))

	done := &store.Pass{ID: 1, JobID: job.ID, PassNo: 1,
		FilesCopied: 0, MetaFixed: 0, BytesCopied: 0}
	if _, _, err := c.decideNextPass(job, done); err != nil {
		t.Fatal(err)
	}
	if got := jobState(t, c, job.ID); got != model.JobCompleted {
		t.Fatalf("zero-delta pass should complete job, got state %v", got)
	}
}

// A nonzero delta below the ceiling and without thresholds must seed another
// pass, not stop early.
func TestNonzeroDeltaSeedsNextPass(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))

	done := &store.Pass{ID: 1, JobID: job.ID, PassNo: 1, FilesCopied: 42}
	if _, _, err := c.decideNextPass(job, done); err != nil {
		t.Fatal(err)
	}
	if got := jobState(t, c, job.ID); got != model.JobRunning {
		t.Fatalf("nonzero-delta pass should keep running, got state %v", got)
	}
	if p, err := c.st.PassByNo(job.ID, 2); err != nil || p == nil {
		t.Fatalf("expected pass 2 seeded: p=%v err=%v", p, err)
	}
}

// A meta-only fix (0 files, 0 bytes, but MetaFixed>0) is a real delta and must
// not be treated as convergence.
func TestMetaOnlyDeltaNotConverged(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))

	done := &store.Pass{ID: 1, JobID: job.ID, PassNo: 1, MetaFixed: 3}
	if _, _, err := c.decideNextPass(job, done); err != nil {
		t.Fatal(err)
	}
	if got := jobState(t, c, job.ID); got != model.JobRunning {
		t.Fatalf("meta-only delta should keep running, got state %v", got)
	}
}

// The pass ceiling still stops a job that never converges.
func TestPassCeilingStops(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, withConverge("    max: 3\n"))

	done := &store.Pass{ID: 1, JobID: job.ID, PassNo: 3, FilesCopied: 99}
	if _, _, err := c.decideNextPass(job, done); err != nil {
		t.Fatal(err)
	}
	if got := jobState(t, c, job.ID); got != model.JobCompleted {
		t.Fatalf("pass at ceiling should complete, got state %v", got)
	}
}

// An explicit converge_when threshold loosens the stop: a small nonzero delta
// under delta_files_below converges early.
func TestThresholdConvergesEarly(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, withConverge("    converge_when:\n      delta_files_below: 10\n"))

	done := &store.Pass{ID: 1, JobID: job.ID, PassNo: 1, FilesCopied: 5}
	if _, _, err := c.decideNextPass(job, done); err != nil {
		t.Fatal(err)
	}
	if got := jobState(t, c, job.ID); got != model.JobCompleted {
		t.Fatalf("delta under threshold should complete, got state %v", got)
	}
}
