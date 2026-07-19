package passctrl

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	"drsync/coordinator/internal/journal"
	"drsync/coordinator/internal/model"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// writeJournal seeds (jobID, passNo) with one JR_COPIED record per rel path,
// mirroring the on-disk format the agent produces (varint-framed records,
// zstd-compressed inside a JournalBatch envelope).
func writeJournal(t *testing.T, root string, jobID int64, passNo int, rels []string) {
	t.Helper()
	w, err := journal.NewWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	// Emit in bounded batches (like the agent), so the reader decodes bounded
	// blobs rather than one N-sized envelope.
	const perBatch = 10_000
	var raw []byte
	var count int
	appendBatch := func() {
		if count == 0 {
			return
		}
		if err := w.Append(&drsyncpb.JournalBatch{
			JobId: uint64(jobID), PassNo: uint32(passNo),
			RecordCount: uint32(count), RecordsZstd: enc.EncodeAll(raw, nil),
		}); err != nil {
			t.Fatal(err)
		}
		raw = raw[:0]
		count = 0
	}
	for _, rel := range rels {
		rec := &drsyncpb.JournalRecord{Type: drsyncpb.JournalRecord_JR_COPIED, RelPath: []byte(rel)}
		b, err := proto.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		var hdr [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(hdr[:], uint64(len(b)))
		raw = append(raw, hdr[:n]...)
		raw = append(raw, b...)
		if count++; count >= perBatch {
			appendBatch()
		}
	}
	appendBatch()
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// countVerify reads back the verify shards seeded into a pass (by leasing the
// queued shards) and returns the total verify entries and how many are
// checksummed, plus the number of verify shards.
func countVerify(t *testing.T, c *Controller) (entries, checksummed, shards int) {
	t.Helper()
	leased, err := c.st.LeaseShards("verify-reader", 1_000_000, time.Hour, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, sh := range leased {
		if sh.Kind != model.KindVerify {
			continue
		}
		shards++
		vb := &drsyncpb.VerifyBatch{}
		if err := proto.Unmarshal(sh.Payload, vb); err != nil {
			t.Fatal(err)
		}
		for _, e := range vb.Entries {
			entries++
			if e.Checksum {
				checksummed++
			}
		}
	}
	return entries, checksummed, shards
}

// TestSeedVerifyStreamsBatches: N copied files with a 100% sample produce
// ceil(N/verifyBatchSize) verify shards covering every entry, all checksummed.
func TestSeedVerifyStreamsBatches(t *testing.T) {
	c := newController(t)
	spec := []byte(baseSpec + "  verify:\n    checksum:\n      sample_rate: 1.0\n")
	n := verifyBatchSize*2 + 7 // spans three batches
	rels := make([]string, n)
	for i := range rels {
		rels[i] = fmt.Sprintf("dir/file-%06d", i)
	}
	job := makeJob(t, c, spec)
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	writeJournal(t, c.journalRoot, job.ID, pass.PassNo, rels)

	got, err := c.seedVerify(job, pass)
	if err != nil {
		t.Fatal(err)
	}
	if got != n {
		t.Fatalf("seedVerify returned %d entries, want %d", got, n)
	}
	entries, checksummed, shards := countVerify(t, c)
	if entries != n {
		t.Fatalf("verify shards cover %d entries, want %d", entries, n)
	}
	if checksummed != n { // 100% sample
		t.Fatalf("checksummed %d of %d at sample_rate 1.0", checksummed, n)
	}
	wantShards := (n + verifyBatchSize - 1) / verifyBatchSize
	if shards != wantShards {
		t.Fatalf("seeded %d verify shards, want ceil(%d/%d)=%d", shards, n, verifyBatchSize, wantShards)
	}
}

// TestSeedVerifyModeOff seeds nothing when verify.mode=off.
func TestSeedVerifyModeOff(t *testing.T) {
	c := newController(t)
	spec := []byte(baseSpec + "  verify:\n    mode: off\n")
	job := makeJob(t, c, spec)
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	writeJournal(t, c.journalRoot, job.ID, pass.PassNo, []string{"a", "b", "c"})

	got, err := c.seedVerify(job, pass)
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("verify.mode=off seeded %d entries, want 0", got)
	}
	if entries, _, _ := countVerify(t, c); entries != 0 {
		t.Fatalf("verify.mode=off produced %d verify entries", entries)
	}
}

// TestSeedVerifyChecksumFloor: even at a near-zero sample rate, a data-copying
// pass checksums at least one file (never verifies zero bytes).
func TestSeedVerifyChecksumFloor(t *testing.T) {
	c := newController(t)
	// sample_rate 1e-6 → ppm 1; with a handful of files the hash sample almost
	// certainly selects none, so the floor must force one.
	spec := []byte(baseSpec + "  verify:\n    checksum:\n      sample_rate: 0.000001\n")
	job := makeJob(t, c, spec)
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	writeJournal(t, c.journalRoot, job.ID, pass.PassNo, []string{"x", "y", "z"})

	if _, err := c.seedVerify(job, pass); err != nil {
		t.Fatal(err)
	}
	_, checksummed, _ := countVerify(t, c)
	if checksummed < 1 {
		t.Fatalf("checksum floor failed: %d checksummed, want >= 1", checksummed)
	}
}

// TestSeedVerifyMemoryBounded is the scale regression: peak heap during
// seedVerify must stay bounded (O(batch)) independent of files-copied. A
// whole-pass map keyed by every path — the old implementation — would grow
// heap by hundreds of MB at this N; streaming stays in single-digit MB.
func TestSeedVerifyMemoryBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("large-N memory test")
	}
	c := newController(t)
	spec := []byte(baseSpec + "  verify:\n    checksum:\n      sample_rate: 0.01\n")
	job := makeJob(t, c, spec)
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	const n = 1_000_000
	rels := make([]string, n)
	for i := range rels {
		rels[i] = fmt.Sprintf("some/moderately/deep/path/file-%08d.dat", i)
	}
	writeJournal(t, c.journalRoot, job.ID, pass.PassNo, rels)
	rels = nil // drop the generator set so the baseline reflects only seedVerify

	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	// Sample peak HeapInuse from a background goroutine during the call.
	var peak uint64
	done := make(chan struct{})
	go func() {
		var m runtime.MemStats
		for {
			select {
			case <-done:
				return
			default:
				runtime.ReadMemStats(&m) // STW; poll gently to avoid throttling the run
				if m.HeapInuse > peak {
					peak = m.HeapInuse
				}
				time.Sleep(200 * time.Microsecond)
			}
		}
	}()

	got, err := c.seedVerify(job, pass)
	close(done)
	if err != nil {
		t.Fatal(err)
	}
	if got != n {
		t.Fatalf("seedVerify returned %d entries, want %d", got, n)
	}

	const budget = 128 << 20 // 128 MiB — huge margin over streaming's few MB
	grew := int64(peak) - int64(base.HeapInuse)
	t.Logf("peak heap delta during seedVerify of %d files: %d MiB", n, grew>>20)
	if grew > budget {
		t.Fatalf("peak heap grew %d MiB (> %d MiB budget) — not streaming?", grew>>20, budget>>20)
	}
}
