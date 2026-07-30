package model

import "testing"

// TestHardlinksPreserveParses confirms the opt-in value round-trips: this is
// coordinator-side-only (docs/DESIGN-hardlinks.md) — it never reaches
// ToJobOptions/MetadataOptions, so there is nothing to assert there, only
// that the spec field itself parses and is validated.
func TestHardlinksPreserveParses(t *testing.T) {
	spec := filterBase + "  metadata:\n    hardlinks: preserve\n"
	s, err := ParseSpec([]byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	if s.Spec.Metadata.Hardlinks != "preserve" {
		t.Errorf("metadata.hardlinks = %q, want preserve", s.Spec.Metadata.Hardlinks)
	}
}

func TestHardlinksRejectUnknownValue(t *testing.T) {
	spec := filterBase + "  metadata:\n    hardlinks: sometimes\n"
	if _, err := ParseSpec([]byte(spec)); err == nil {
		t.Fatal("unknown metadata.hardlinks value should fail validation")
	}
}

func TestHardlinksMaxGroupScanParses(t *testing.T) {
	spec := filterBase + "  metadata:\n    hardlinks: preserve\n    hardlinks_max_group_scan: 500000\n"
	s, err := ParseSpec([]byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	if s.Spec.Metadata.HardlinksMaxGroupScan != 500000 {
		t.Errorf("metadata.hardlinks_max_group_scan = %d, want 500000",
			s.Spec.Metadata.HardlinksMaxGroupScan)
	}
}
