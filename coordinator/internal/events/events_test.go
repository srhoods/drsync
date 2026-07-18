package events

import (
	"path/filepath"
	"testing"

	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
)

// CreateJob parses the spec (it checks the destination against live jobs), so
// tests need a real one rather than a placeholder blob.
const jobSpecYAML = `
apiVersion: drsync/v1
kind: Job
metadata: { name: evjob }
spec:
  source: { path: /src }
  destination: { path: /dst }
`

func TestBusFanOutAndDrop(t *testing.T) {
	b := NewBus()
	ch, cancel := b.Subscribe()
	defer cancel()

	b.Publish(Event{Type: "job_state", Job: "j"})
	ev := <-ch
	if ev.Type != "job_state" || ev.Job != "j" || ev.TsMs == 0 {
		t.Fatalf("bad event: %+v", ev)
	}

	// A subscriber that never drains must not block the publisher.
	slow, cancelSlow := b.Subscribe()
	defer cancelSlow()
	for i := 0; i < subBuffer+10; i++ {
		b.Publish(Event{Type: "stats"})
	}
	if len(slow) != subBuffer {
		t.Fatalf("slow subscriber buffered %d, want %d", len(slow), subBuffer)
	}
}

func drain(ch <-chan Event) []Event {
	var out []Event
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func TestPollerDiffsStore(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	job, err := st.CreateJob("evjob", []byte(jobSpecYAML), false)
	if err != nil {
		t.Fatal(err)
	}

	bus := NewBus()
	p := NewPoller(st, bus)
	ch, cancel := bus.Subscribe()
	defer cancel()

	p.tick() // priming snapshot: seeds baselines, publishes nothing
	if evs := drain(ch); len(evs) != 0 {
		t.Fatalf("priming tick published %+v", evs)
	}

	if err := st.SetJobState(job.ID, model.JobRunning); err != nil {
		t.Fatal(err)
	}
	pass, err := st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	p.tick()
	var haveJob, havePass, haveStats bool
	for _, ev := range drain(ch) {
		switch ev.Type {
		case "job_state":
			haveJob = ev.Job == "evjob" && ev.Data["state"] == model.JobRunning
		case "pass_state":
			havePass = ev.PassNo == 1 && ev.Data["state"] == model.PassScanning
		case "stats":
			haveStats = true
		}
	}
	if !haveJob || !havePass || !haveStats {
		t.Fatalf("missing events: job=%v pass=%v stats=%v", haveJob, havePass, haveStats)
	}

	// No movement -> only the 1 Hz stats heartbeat for the running job.
	p.tick()
	for _, ev := range drain(ch) {
		if ev.Type != "stats" {
			t.Fatalf("unexpected event on idle tick: %+v", ev)
		}
	}

	if err := st.SetPassState(pass.ID, model.PassComplete); err != nil {
		t.Fatal(err)
	}
	if err := st.SetJobState(job.ID, model.JobCompleted); err != nil {
		t.Fatal(err)
	}
	p.tick()
	var passDone, jobDone bool
	for _, ev := range drain(ch) {
		if ev.Type == "pass_state" && ev.Data["state"] == model.PassComplete {
			passDone = true
		}
		if ev.Type == "job_state" && ev.Data["state"] == model.JobCompleted {
			jobDone = true
		}
	}
	if !passDone || !jobDone {
		t.Fatalf("missing terminal events: pass=%v job=%v", passDone, jobDone)
	}
}
