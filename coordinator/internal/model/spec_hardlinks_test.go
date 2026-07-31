package model

import "testing"

// TestHardlinksPreserveParses confirms the explicit value round-trips: this
// is coordinator-side-only (docs/DESIGN-hardlinks.md) — it never reaches
// ToJobOptions/MetadataOptions, so there is nothing to assert there, only
// that the spec field itself parses and is validated. preserve is also the
// D11 default (TestDefaultsAppliedToMinimalSpec), so this pins the explicit
// spelling stays equivalent to leaving it unset.
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

// TestHardlinksReportOptsOut confirms explicitly setting "report" sticks
// (D3 behavior) rather than being defaulted back to "preserve" — the *bool
// pattern used elsewhere in this file doesn't apply to a string field, so
// this is the guard against a boolDefault-style "empty means unset" bug
// silently overriding an explicit opt-out.
func TestHardlinksReportOptsOut(t *testing.T) {
	spec := filterBase + "  metadata:\n    hardlinks: report\n"
	s, err := ParseSpec([]byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	if s.Spec.Metadata.Hardlinks != "report" {
		t.Errorf("metadata.hardlinks = %q, want report (explicit opt-out must stick)", s.Spec.Metadata.Hardlinks)
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
