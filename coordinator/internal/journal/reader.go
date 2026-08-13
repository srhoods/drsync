package journal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	drsyncpb "drsync/proto/gen/drsyncpb"
)

// ReadRecords streams every journal record of (jobID, passNo) to fn in
// segment order. Journaling is at-least-once (a re-run shard re-emits its
// records), so consumers must tolerate duplicates.
func ReadRecords(root string, jobID int64, passNo int, fn func(*drsyncpb.JournalRecord) error) error {
	dir := filepath.Join(root, fmt.Sprintf("%d", jobID), fmt.Sprintf("pass-%d", passNo))
	segs, err := filepath.Glob(filepath.Join(dir, "segment-*.drj"))
	if err != nil {
		return err
	}
	sort.Strings(segs)

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer dec.Close()

	for _, seg := range segs {
		if err := readSegment(seg, dec, fn); err != nil {
			return fmt.Errorf("%s: %w", seg, err)
		}
	}
	return nil
}

func readSegment(path string, dec *zstd.Decoder, fn func(*drsyncpb.JournalRecord) error) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for off := 0; off < len(data); {
		if off+4 > len(data) {
			return fmt.Errorf("truncated envelope header at %d", off)
		}
		n := int(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
		if off+n > len(data) {
			// A torn tail write (coordinator crash mid-append) loses at most
			// the final unacked batch; the agent re-ran that shard.
			return nil
		}
		batch := &drsyncpb.JournalBatch{}
		if err := proto.Unmarshal(data[off:off+n], batch); err != nil {
			return fmt.Errorf("bad batch envelope: %w", err)
		}
		off += n

		raw, err := dec.DecodeAll(batch.RecordsZstd, nil)
		if err != nil {
			return fmt.Errorf("zstd: %w", err)
		}
		if err := forEachRecord(raw, fn); err != nil {
			return err
		}
	}
	return nil
}

func forEachRecord(raw []byte, fn func(*drsyncpb.JournalRecord) error) error {
	for off := 0; off < len(raw); {
		l, n := binary.Uvarint(raw[off:])
		if n <= 0 || off+n+int(l) > len(raw) {
			return io.ErrUnexpectedEOF
		}
		off += n
		rec := &drsyncpb.JournalRecord{}
		if err := proto.Unmarshal(raw[off:off+int(l)], rec); err != nil {
			return err
		}
		off += int(l)
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

// Summary returns the per-type record histogram across every pass in passNos
// — the same aggregation `drsync journal cat --summary` and the API's
// ?summary=true query serve, factored out here so callers that need the
// job-wide histogram (the completion email, the WebUI's journal-summary
// panel) don't re-scan the journal with their own copy of the loop.
//
// Deduped per (pass, type, rel_path): journaling is at-least-once (a shard
// whose lease expires mid-run is requeued and re-runs from scratch, see
// ReadRecords' own doc comment and jrn.c — "readers dedup"), so the exact
// same record can appear twice for one shard's worth of work with no
// correctness issue on its own, but it inflates every count here 1:1 with
// however many shards happened to need a retry — confirmed live: a single
// lease expiry during a 22k-file pass alone inflated this by 2000. rel_path
// is not unique enough on its own to dedup by (a legitimately different
// second error on the same path, e.g. two different xattr names each
// failing to apply, would collide) but collapsing that rare case is an
// acceptable trade against the much more common shard-retry duplication
// this exists to fix. Affordable here because Summary holds one small
// per-type histogram (not a per-file batch to seed): passctrl.go's
// seedVerify/seedDirfix hit the exact same duplication but deliberately do
// NOT dedup it, since they stream in O(batch) memory and a whole-pass "seen"
// set would reintroduce the O(N) memory that design exists to avoid — see
// their own doc comments for that tradeoff.
func Summary(root string, jobID int64, passNos []int) (byType map[string]int64, total int64, err error) {
	byType = map[string]int64{}
	for _, pn := range passNos {
		seen := make(map[string]bool)
		err := ReadRecords(root, jobID, pn, func(r *drsyncpb.JournalRecord) error {
			key := r.Type.String() + "\x00" + string(r.RelPath)
			if seen[key] {
				return nil
			}
			seen[key] = true
			byType[strings.TrimPrefix(r.Type.String(), "JR_")]++
			total++
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
	}
	return byType, total, nil
}

// Orphans returns the deduplicated orphan rel-paths recorded in a pass —
// the input for a delete pass (decision D5): no extra scan required.
func Orphans(root string, jobID int64, passNo int) ([]string, error) {
	seen := map[string]struct{}{}
	err := ReadRecords(root, jobID, passNo, func(r *drsyncpb.JournalRecord) error {
		if r.Type == drsyncpb.JournalRecord_JR_ORPHAN {
			seen[string(r.RelPath)] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out, nil
}
