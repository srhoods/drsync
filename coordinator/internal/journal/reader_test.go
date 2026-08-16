package journal

import (
	"encoding/binary"
	"testing"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	drsyncpb "drsync/proto/gen/drsyncpb"
)

// writeRecords seeds (jobID, passNo) with one batch containing recs, in the
// on-disk format the agent produces (varint-framed records, zstd-compressed
// inside a JournalBatch envelope).
func writeRecords(t *testing.T, root string, jobID int64, passNo int, recs []*drsyncpb.JournalRecord) {
	t.Helper()
	w, err := NewWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	for _, rec := range recs {
		b, err := proto.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		var hdr [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(hdr[:], uint64(len(b)))
		raw = append(raw, hdr[:n]...)
		raw = append(raw, b...)
	}
	if len(recs) > 0 {
		if err := w.Append(&drsyncpb.JournalBatch{
			JobId: uint64(jobID), PassNo: uint32(passNo),
			RecordCount: uint32(len(recs)), RecordsZstd: enc.EncodeAll(raw, nil),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// Summary must aggregate the per-type histogram across every pass given, not
// just the first — this is what distinguishes it from a single ReadRecords
// call and is exactly what the completion email needs (the whole job, not
// one pass).
func TestSummaryAggregatesAcrossPasses(t *testing.T) {
	root := t.TempDir()
	writeRecords(t, root, 1, 1, []*drsyncpb.JournalRecord{
		{Type: drsyncpb.JournalRecord_JR_COPIED, RelPath: []byte("a")},
		{Type: drsyncpb.JournalRecord_JR_COPIED, RelPath: []byte("b")},
		{Type: drsyncpb.JournalRecord_JR_ORPHAN, RelPath: []byte("c")},
	})
	writeRecords(t, root, 1, 2, []*drsyncpb.JournalRecord{
		{Type: drsyncpb.JournalRecord_JR_COPIED, RelPath: []byte("d")},
		{Type: drsyncpb.JournalRecord_JR_ERROR, RelPath: []byte("e")},
	})

	byType, total, err := Summary(root, 1, []int{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
	want := map[string]int64{"COPIED": 3, "ORPHAN": 1, "ERROR": 1}
	for k, v := range want {
		if byType[k] != v {
			t.Errorf("byType[%q] = %d, want %d (full: %+v)", k, byType[k], v, byType)
		}
	}
	if len(byType) != len(want) {
		t.Errorf("byType has extra/missing types: %+v", byType)
	}
}

// A pass number with no journal segments on disk contributes nothing (no
// error) — matches ReadRecords' own behavior for a directory that never got
// written (e.g. a pass that produced zero records).
func TestSummaryMissingPassIsEmpty(t *testing.T) {
	root := t.TempDir()
	byType, total, err := Summary(root, 1, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 || len(byType) != 0 {
		t.Errorf("byType = %+v, total = %d, want empty", byType, total)
	}
}

// TestSummaryDedupsRetriedShard is the at-least-once-journaling regression:
// a shard whose lease expires mid-run is requeued and re-runs from scratch
// (ReadRecords' own doc comment: "a re-run shard re-emits its records"), so
// the SAME (type, rel_path) can appear twice in one pass's journal —
// confirmed live, a single lease expiry during a 22k-file pass alone
// inflated counts by 2000. Without dedup, Summary (and therefore `drsync
// journal cat --summary`, the completion email, and the WebUI's
// journal-summary panel) would overcount by exactly the number of retried
// shards' worth of records, with no way for an operator to tell that from a
// real change in volume.
func TestSummaryDedupsRetriedShard(t *testing.T) {
	root := t.TempDir()
	writeRecords(t, root, 1, 1, []*drsyncpb.JournalRecord{
		{Type: drsyncpb.JournalRecord_JR_META_FIXED, RelPath: []byte("retried")},
		{Type: drsyncpb.JournalRecord_JR_META_FIXED, RelPath: []byte("once")},
		{Type: drsyncpb.JournalRecord_JR_META_FIXED, RelPath: []byte("retried")}, // duplicate: shard re-run
		// A different TYPE on the same path is not a duplicate of the above —
		// dedup keys on (type, rel_path), not rel_path alone, so a file that
		// legitimately appears under two different record types in one pass
		// (not possible from the walker today, but the key shape should not
		// assume that) is still counted for each.
		{Type: drsyncpb.JournalRecord_JR_ERROR, RelPath: []byte("retried")},
	})

	byType, total, err := Summary(root, 1, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Errorf("total = %d, want 3 (2 distinct META_FIXED paths + 1 ERROR, the duplicate META_FIXED dropped)", total)
	}
	if byType["META_FIXED"] != 2 {
		t.Errorf("byType[META_FIXED] = %d, want 2 — a retried shard's duplicate journal record was not deduped", byType["META_FIXED"])
	}
	if byType["ERROR"] != 1 {
		t.Errorf("byType[ERROR] = %d, want 1", byType["ERROR"])
	}
}
