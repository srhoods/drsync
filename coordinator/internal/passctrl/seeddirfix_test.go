package passctrl

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	"drsync/coordinator/internal/journal"
	"drsync/coordinator/internal/model"
	drsyncpb "drsync/proto/gen/drsyncpb"
)

// dirRec is one directory's journalled metadata for the test.
type dirRec struct {
	rel  string
	uid  uint32
	gid  uint32
	mode uint32
	mt   int64
}

// writeDirMetaJournal seeds (jobID, passNo) with one JR_DIR_META record per dir,
// each carrying a StatInfo — the shape the walker emits and seedDirfix consumes.
func writeDirMetaJournal(t *testing.T, root string, jobID int64, passNo int, dirs []dirRec) {
	t.Helper()
	w, err := journal.NewWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	for _, d := range dirs {
		rec := &drsyncpb.JournalRecord{
			Type:    drsyncpb.JournalRecord_JR_DIR_META,
			RelPath: []byte(d.rel),
			Src: &drsyncpb.StatInfo{
				Uid: d.uid, Gid: d.gid, Mode: d.mode, MtimeNs: d.mt, AtimeNs: d.mt,
			},
		}
		b, err := proto.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		var hdr [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(hdr[:], uint64(len(b)))
		raw = append(raw, hdr[:n]...)
		raw = append(raw, b...)
	}
	if err := w.Append(&drsyncpb.JournalBatch{
		JobId: uint64(jobID), PassNo: uint32(passNo),
		RecordCount: uint32(len(dirs)), RecordsZstd: enc.EncodeAll(raw, nil),
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSeedDirfixFromJournal: DIR_META records become DirFixBatch shards carrying
// each directory's metadata, sorted deepest-first within a batch.
func TestSeedDirfixFromJournal(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	// Non-DIR_META records must be ignored; DIR_META (incl. the empty-rel root)
	// carried through with its metadata.
	dirs := []dirRec{
		{rel: "", uid: 0, gid: 0, mode: 0o755, mt: 100},
		{rel: "a", uid: 1, gid: 1, mode: 0o750, mt: 200},
		{rel: "a/b/c", uid: 2, gid: 2, mode: 0o700, mt: 300},
		{rel: "a/b", uid: 3, gid: 3, mode: 0o711, mt: 400},
	}
	writeDirMetaJournal(t, c.journalRoot, job.ID, pass.PassNo, dirs)

	n, err := c.seedDirfix(job, pass)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(dirs) {
		t.Fatalf("seedDirfix returned %d dirs, want %d", n, len(dirs))
	}

	// Read back the seeded dirfix shard(s).
	leased, err := c.st.LeaseShards("dirfix-reader", 1_000_000, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var got []*drsyncpb.DirMeta
	for _, sh := range leased {
		if sh.Kind != model.KindDirfix {
			continue
		}
		fb := &drsyncpb.DirFixBatch{}
		if err := proto.Unmarshal(sh.Payload, fb); err != nil {
			t.Fatal(err)
		}
		got = append(got, fb.Dirs...)
	}
	if len(got) != len(dirs) {
		t.Fatalf("dirfix shards cover %d dirs, want %d", len(got), len(dirs))
	}

	// Metadata preserved (index by rel).
	byRel := map[string]*drsyncpb.DirMeta{}
	for _, d := range got {
		byRel[string(d.RelPath)] = d
	}
	for _, want := range dirs {
		d := byRel[want.rel]
		if d == nil {
			t.Fatalf("dir %q missing from dirfix batch", want.rel)
		}
		if d.Uid != want.uid || d.Gid != want.gid || d.Mode != want.mode || d.MtimeNs != want.mt {
			t.Fatalf("dir %q meta = %+v, want %+v", want.rel, d, want)
		}
	}

	// Within the single batch, deepest-first: depth is non-increasing.
	prev := -1
	for _, d := range got {
		depth := 0
		for _, b := range d.RelPath {
			if b == '/' {
				depth++
			}
		}
		if prev >= 0 && depth > prev {
			t.Fatalf("batch not deepest-first: %q (depth %d) after depth %d",
				d.RelPath, depth, prev)
		}
		prev = depth
	}
}

// TestSeedDirfixEmpty: a pass with no DIR_META records seeds no dirfix shards.
func TestSeedDirfixEmpty(t *testing.T) {
	c := newController(t)
	job := makeJob(t, c, []byte(baseSpec))
	pass, err := c.st.CreatePass(job.ID, 1, model.PassScanning)
	if err != nil {
		t.Fatal(err)
	}
	writeJournal(t, c.journalRoot, job.ID, pass.PassNo, []string{"a/f1", "a/f2"}) // JR_COPIED only
	n, err := c.seedDirfix(job, pass)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("seedDirfix seeded %d dirs from a journal with no DIR_META", n)
	}
}
