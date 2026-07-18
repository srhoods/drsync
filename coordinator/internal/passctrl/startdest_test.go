package passctrl

import (
	"errors"
	"fmt"
	"testing"

	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
)

func destSpec(name, dst string) []byte {
	return []byte(fmt.Sprintf(`apiVersion: drsync/v1
kind: Job
metadata: { name: %s }
spec:
  source: { path: /src }
  destination: { path: %s }
`, name, dst))
}

func makeJobNamed(t *testing.T, c *Controller, name, dst string) *store.Job {
	t.Helper()
	job, err := c.st.CreateJob(name, destSpec(name, dst), false)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

// makeOverlappingJob creates a job whose destination overlaps holder's, and
// leaves holder in state.
//
// CreateJob refuses an overlapping destination outright — that is the
// submit-time gate, checked under the insert's lock — so there is no way to
// produce this pair through the normal path. Parking holder in a terminal state
// across the insert reconstructs what the start-time gate is actually for: rows
// created before that check existed, or two submits that raced past each other.
func makeOverlappingJob(t *testing.T, c *Controller, name, dst string,
	holder *store.Job, state model.JobState) *store.Job {
	t.Helper()
	if err := c.st.SetJobState(holder.ID, model.JobCompleted); err != nil {
		t.Fatal(err)
	}
	job := makeJobNamed(t, c, name, dst)
	if err := c.st.SetJobState(holder.ID, state); err != nil {
		t.Fatal(err)
	}
	return job
}

// TestStartJobRefusesTakenDestination: starting a job whose destination is
// being written by a RUNNING job must fail. Submit blocks this at creation, but
// jobs created before that check existed — or through a racing submit — reach
// start anyway, and two jobs in one tree reclaim each other's in-progress temps.
func TestStartJobRefusesTakenDestination(t *testing.T) {
	c := newController(t)
	first := makeJobNamed(t, c, "first", "/dst/home")
	second := makeOverlappingJob(t, c, "second", "/dst/home/users", first, model.JobRunning)

	err := c.StartJob("second")
	var dc *store.DestinationConflictError
	if !errors.As(err, &dc) {
		t.Fatalf("StartJob err = %v, want DestinationConflictError", err)
	}
	if dc.Other != "first" {
		t.Errorf("conflict names %q, want \"first\"", dc.Other)
	}
	// The refusal must leave the job untouched, not half-started.
	got, err := c.st.GetJobByID(second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != model.JobReady {
		t.Errorf("job state = %s after refused start, want READY", got.State)
	}
	if pass, _ := c.st.ActivePass(second.ID); pass != nil {
		t.Errorf("refused start seeded pass %d", pass.PassNo)
	}
}

// Once the holder reaches a terminal state its tree is free.
func TestStartJobAllowsFreedDestination(t *testing.T) {
	c := newController(t)
	first := makeJobNamed(t, c, "first", "/dst/home")
	second := makeOverlappingJob(t, c, "second", "/dst/home", first, model.JobRunning)

	if err := c.StartJob("second"); err == nil {
		t.Fatal("expected the running job to block the start")
	}
	if err := c.st.SetJobState(first.ID, model.JobCompleted); err != nil {
		t.Fatal(err)
	}
	if err := c.StartJob("second"); err != nil {
		t.Fatalf("start after the holder completed: %v", err)
	}
	if got, _ := c.st.GetJobByID(second.ID); got.State != model.JobRunning {
		t.Errorf("job state = %s, want RUNNING", got.State)
	}
}

// A READY job must not block another READY job from starting: neither has begun
// writing, and blocking on one would deadlock both — each refusing to go first.
func TestStartJobIgnoresOtherReadyJobs(t *testing.T) {
	c := newController(t)
	first := makeJobNamed(t, c, "first", "/dst/home")
	makeOverlappingJob(t, c, "second", "/dst/home", first, model.JobReady)

	if err := c.StartJob("first"); err != nil {
		t.Fatalf("first start blocked by a READY peer: %v", err)
	}
	// ...but now that "first" is RUNNING, "second" is blocked.
	if err := c.StartJob("second"); err == nil {
		t.Fatal("second start succeeded while first is RUNNING")
	}
}

// Resume runs the same gate: another job may have taken the tree while this one
// was paused.
func TestResumeJobRefusesTakenDestination(t *testing.T) {
	c := newController(t)
	paused := makeJobNamed(t, c, "paused", "/dst/home")
	other := makeOverlappingJob(t, c, "other", "/dst/home/sub", paused, model.JobPaused)
	if err := c.st.SetJobState(other.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}

	err := c.ResumeJob("paused")
	var dc *store.DestinationConflictError
	if !errors.As(err, &dc) {
		t.Fatalf("ResumeJob err = %v, want DestinationConflictError", err)
	}
	if got, _ := c.st.GetJobByID(paused.ID); got.State != model.JobPaused {
		t.Errorf("job state = %s after refused resume, want PAUSED", got.State)
	}

	// With the tree free again, resume proceeds.
	if err := c.st.SetJobState(other.ID, model.JobCancelled); err != nil {
		t.Fatal(err)
	}
	if err := c.ResumeJob("paused"); err != nil {
		t.Fatalf("resume after the tree was freed: %v", err)
	}
	if got, _ := c.st.GetJobByID(paused.ID); got.State != model.JobRunning {
		t.Errorf("job state = %s, want RUNNING", got.State)
	}
}
