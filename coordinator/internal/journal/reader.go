package journal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

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
