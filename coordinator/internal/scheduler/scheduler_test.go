package scheduler

import (
	"testing"

	"drsync/coordinator/internal/model"
	"drsync/coordinator/internal/store"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

func policy(mode string, target uint64) *jobPolicy {
	return &jobPolicy{
		opts:   &drsyncpb.JobOptions{Tuning: &drsyncpb.TuningOptions{}},
		spread: model.SpreadPolicy{Mode: mode, TargetPerAgent: target},
	}
}

// Spreading must only ever fan a job out harder than its own tuning. An
// operator who lowered dir_split_threshold to break up a pathological
// directory must keep that: raising it back to the spread default silently
// disables the entry-list path they asked for.
func TestWalkOverridesNeverRaisesSplitThreshold(t *testing.T) {
	tests := []struct {
		name     string
		jobSplit uint64
		want     uint64
	}{
		{"operator set a lower threshold: keep theirs", 25, 25},
		{"operator set a higher threshold: spread wins", 50_000, spreadSplitThreshold},
		{"unset: spread default", 0, spreadSplitThreshold},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pol := policy(model.SpreadAuto, 32)
			pol.opts.Tuning.DirSplitThreshold = tc.jobSplit
			ov := walkOverrides(pol, 4, store.SchedulerCounts{WalkPending: 1})
			if ov == nil {
				t.Fatal("expected spread")
			}
			if got := ov.GetSplitThreshold(); got != tc.want {
				t.Fatalf("split_threshold = %d, want %d", got, tc.want)
			}
		})
	}
}

// The fan-out decision. The bug this guards: a volume smaller than
// shard_budget never splits, so one agent walks the whole tree.
func TestWalkOverrides(t *testing.T) {
	tests := []struct {
		name        string
		mode        string
		target      uint64
		agents      int64
		walkPending int64
		wantSpread  bool
	}{
		{"job start: one root shard, fleet idle", model.SpreadAuto, 32, 4, 1, true},
		{"still starved below target", model.SpreadAuto, 32, 4, 127, true},
		{"target met: stop spreading", model.SpreadAuto, 32, 4, 128, false},
		{"deep queue at scale", model.SpreadAuto, 32, 4, 500_000, false},
		{"single agent still fans out for its own threads", model.SpreadAuto, 32, 1, 1, true},
		{"fleet grew: target rises, spread resumes", model.SpreadAuto, 32, 8, 200, true},
		{"off: never spread, even starved", model.SpreadOff, 32, 4, 1, false},
		{"always: spread even with a deep queue", model.SpreadAlways, 32, 4, 500_000, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ov := walkOverrides(policy(tc.mode, tc.target), tc.agents,
				store.SchedulerCounts{WalkPending: tc.walkPending})
			if got := ov != nil; got != tc.wantSpread {
				t.Fatalf("spread = %v, want %v", got, tc.wantSpread)
			}
			if !tc.wantSpread {
				return
			}
			// Spreading means: descend nothing, push every subdir back. Budget
			// must be an explicit 0 — an absent field means "use job tuning",
			// which is exactly the behaviour being overridden.
			if ov.WalkBudget == nil || *ov.WalkBudget != 0 {
				t.Errorf("walk_budget = %v, want explicit 0", ov.WalkBudget)
			}
			if ov.SplitThreshold == nil || *ov.SplitThreshold != spreadSplitThreshold {
				t.Errorf("split_threshold = %v, want %d", ov.SplitThreshold,
					spreadSplitThreshold)
			}
		})
	}
}

// The fair-share cap. The bug this guards: an agent requests
// (workers+copy_threads)*2 credits, so the first to poll drains the queue and
// the rest of the fleet idles.
func TestFairShare(t *testing.T) {
	tests := []struct {
		name    string
		credits int
		queued  int64
		agents  int64
		want    int
	}{
		{"20 queued across 4 agents: 5 each", 48, 20, 4, 5},
		{"uneven split rounds up, never stranding a shard", 48, 21, 4, 6},
		{"fewer shards than agents: one each", 48, 2, 4, 1},
		{"single agent takes everything", 48, 20, 1, 48},
		{"deep queue: full request, no rationing", 48, 500_000, 4, 48},
		{"exactly enough for everyone: no cap", 48, 192, 4, 48},
		{"verify phase: 100k tasks queued, no throttling", 48, 100_000, 4, 48},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := fairShare(tc.credits, tc.queued, tc.agents); got != tc.want {
				t.Fatalf("fairShare = %d, want %d", got, tc.want)
			}
		})
	}
}

// A grant must never be capped to zero: work would sit QUEUED with nobody
// allowed to take it. Mirrors the "never strand" property of LeaseShards'
// tier-2 fallback.
func TestFairShareNeverStrands(t *testing.T) {
	for _, agents := range []int64{1, 2, 4, 64, 1000} {
		for _, queued := range []int64{0, 1, 2, 7, 100} {
			if got := fairShare(48, queued, agents); got < 1 {
				t.Fatalf("agents=%d queued=%d: fairShare = %d, want >= 1",
					agents, queued, got)
			}
		}
	}
}
