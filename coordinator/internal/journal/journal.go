// Package journal appends per-pass journal batches to segment files.
// Batches arrive zstd-compressed from agents and are stored verbatim inside a
// length-prefixed envelope (docs/DESIGN-coordinator.md §5): the coordinator
// never decompresses on the hot path.
//
// Segment framing: [u32 LE envelope_len][envelope = marshaled JournalBatch]...
package journal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	drsyncpb "drsync/proto/gen/drsyncpb"
)

const segmentMaxBytes = 512 << 20

type Writer struct {
	root string
	mu   sync.Mutex
	segs map[string]*segment // key: "<jobID>/<passNo>"
}

type segment struct {
	f     *os.File
	size  int64
	index int
	dir   string
}

func NewWriter(root string) (*Writer, error) {
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	return &Writer{root: root, segs: map[string]*segment{}}, nil
}

// Append stores one JournalBatch durably. Returns after the OS write; Flush
// (periodic, and on Close) provides the fsync barrier that gates JournalAck
// advancement in the caller.
func (w *Writer) Append(batch *drsyncpb.JournalBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := fmt.Sprintf("%d/%d", batch.JobId, batch.PassNo)
	seg := w.segs[key]
	if seg == nil || seg.size >= segmentMaxBytes {
		next := 0
		if seg != nil {
			next = seg.index + 1
			seg.f.Sync()
			seg.f.Close()
		}
		dir := filepath.Join(w.root, fmt.Sprintf("%d", batch.JobId), fmt.Sprintf("pass-%d", batch.PassNo))
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
		f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("segment-%06d.drj", next)),
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
		if err != nil {
			return err
		}
		st, _ := f.Stat()
		seg = &segment{f: f, size: st.Size(), index: next, dir: dir}
		w.segs[key] = seg
	}

	env, err := proto.Marshal(batch)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(env)))
	if _, err := seg.f.Write(hdr[:]); err != nil {
		return err
	}
	n, err := seg.f.Write(env)
	seg.size += int64(n) + 4
	return err
}

// Flush fsyncs all open segments.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var first error
	for _, seg := range w.segs {
		if err := seg.f.Sync(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// DropJob closes any open segments for a job and removes its journal directory
// on disk (used by job purge). Closing the fds first is what actually reclaims
// the space — an unlinked file whose descriptor is still open is not freed.
func (w *Writer) DropJob(jobID int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	prefix := fmt.Sprintf("%d/", jobID)
	for k, seg := range w.segs {
		if strings.HasPrefix(k, prefix) {
			seg.f.Close()
			delete(w.segs, k)
		}
	}
	return os.RemoveAll(filepath.Join(w.root, fmt.Sprintf("%d", jobID)))
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var first error
	for k, seg := range w.segs {
		seg.f.Sync()
		if err := seg.f.Close(); err != nil && first == nil {
			first = err
		}
		delete(w.segs, k)
	}
	return first
}
