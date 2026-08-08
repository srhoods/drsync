package model

import "testing"

// TestDefaultsAppliedToMinimalSpec locks down ApplyDefaults' resolved values
// for a spec that specifies nothing beyond the required fields, so the
// shipped defaults (and template.yaml / the WebUI's JOB_TEMPLATE, which must
// match) can't drift from what the coordinator actually applies.
func TestDefaultsAppliedToMinimalSpec(t *testing.T) {
	s, err := ParseSpec([]byte(filterBase))
	if err != nil {
		t.Fatal(err)
	}
	sp := &s.Spec

	if sp.Passes.Max != 5 {
		t.Errorf("passes.max = %d, want 5", sp.Passes.Max)
	}
	if sp.Deletes.Mode != "mirror" {
		t.Errorf("deletes.mode = %q, want mirror", sp.Deletes.Mode)
	}
	if sp.Copy.DirectWrite == nil || !*sp.Copy.DirectWrite {
		t.Errorf("copy.direct_write = %v, want true", sp.Copy.DirectWrite)
	}
	if sp.Copy.Fsync != "batched" {
		t.Errorf("copy.fsync = %q, want batched", sp.Copy.Fsync)
	}
	const twentyFourGiB = 24 << 30
	const eightGiB = 8 << 30
	if sp.Copy.ChunkThreshold != twentyFourGiB {
		t.Errorf("copy.chunk_threshold = %d, want %d (24GiB)", sp.Copy.ChunkThreshold, twentyFourGiB)
	}
	if sp.Copy.ChunkSize != eightGiB {
		t.Errorf("copy.chunk_size = %d, want %d (8GiB)", sp.Copy.ChunkSize, eightGiB)
	}
	if sp.Tuning.ShardBudget != 2_000 {
		t.Errorf("tuning.shard_budget = %d, want 2000", sp.Tuning.ShardBudget)
	}
	if sp.Tuning.DeleteSplitThreshold != 200_000 {
		t.Errorf("tuning.delete_split_threshold = %d, want 200000", sp.Tuning.DeleteSplitThreshold)
	}
	if sp.Tuning.DeleteSplitBatch != 20_000 {
		t.Errorf("tuning.delete_split_batch = %d, want 20000", sp.Tuning.DeleteSplitBatch)
	}
	if sp.Metadata.Hardlinks != "preserve" {
		t.Errorf("metadata.hardlinks = %q, want preserve", sp.Metadata.Hardlinks)
	}
	if sp.Metadata.HardlinksMaxGroupScan != 0 {
		t.Errorf("metadata.hardlinks_max_group_scan = %d, want 0 (unlimited)",
			sp.Metadata.HardlinksMaxGroupScan)
	}

	o, err := s.ToJobOptions(1, false)
	if err != nil {
		t.Fatal(err)
	}
	if !o.Copy.DirectWrite {
		t.Error("resolved JobOptions.Copy.DirectWrite = false, want true")
	}
	if o.Copy.ChunkThreshold != twentyFourGiB || o.Copy.ChunkSize != eightGiB {
		t.Errorf("resolved JobOptions chunk sizes = %d/%d, want %d/%d",
			o.Copy.ChunkThreshold, o.Copy.ChunkSize, twentyFourGiB, eightGiB)
	}
	if o.Tuning.DeleteSplitThreshold != 200_000 || o.Tuning.DeleteSplitBatch != 20_000 {
		t.Errorf("resolved JobOptions delete-split tuning = %d/%d, want 200000/20000",
			o.Tuning.DeleteSplitThreshold, o.Tuning.DeleteSplitBatch)
	}
}

// TestDirectWriteExplicitFalseIsRespected guards the *bool switch: an
// operator who explicitly sets direct_write: false must not have it
// silently defaulted back to true.
func TestDirectWriteExplicitFalseIsRespected(t *testing.T) {
	spec := filterBase + "  copy:\n    direct_write: false\n"
	s, err := ParseSpec([]byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	if s.Spec.Copy.DirectWrite == nil || *s.Spec.Copy.DirectWrite {
		t.Errorf("copy.direct_write = %v, want explicit false to stick", s.Spec.Copy.DirectWrite)
	}
}
