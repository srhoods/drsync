package store

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestRecordLinkSightingsFirstIsAnchor: the first sighting of a (dev,ino)
// group becomes its anchor (state 'copied' immediately — the speculative-
// anchor design means there is no separate confirmation step to wait for),
// and PendingLinkMembers must not return it (it needs no LinkTask).
func TestRecordLinkSightingsFirstIsAnchor(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)

	sightings := []NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "a/one", Nlink: 2, Size: 4096, MtimeNs: 111},
	}
	if _, err := s.RecordSplit(shardID, 1, nil, nil, sightings, 0); err != nil {
		t.Fatal(err)
	}

	members, err := s.PendingLinkMembers(passID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 0 {
		t.Fatalf("PendingLinkMembers = %v, want none (anchor needs no LinkTask)", members)
	}
}

// TestRecordLinkSightingsHighBitInode is the production regression: VAST (and
// other filesystems with 64-bit inode allocators) hands out inode numbers
// with the high bit set, which database/sql's default uint64 argument
// converter rejects outright ("values with high bit set are not supported"),
// independent of the SQLite column's declared type. dev/ino must survive a
// round trip through link_groups/link_members at the full uint64 range,
// including math.MaxUint64 itself.
func TestRecordLinkSightingsHighBitInode(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)

	const hugeIno uint64 = 1<<63 + 12345 // high bit set, VAST-scale
	const maxIno uint64 = 1<<64 - 1      // math.MaxUint64
	first := []NewLinkSighting{
		{Dev: 1, Ino: hugeIno, RelPath: "a/one", Nlink: 2, Size: 4096, MtimeNs: 111},
	}
	if _, err := s.RecordSplit(shardID, 1, nil, nil, first, 0); err != nil {
		t.Fatalf("RecordSplit with a high-bit inode: %v", err)
	}
	second := []NewLinkSighting{
		{Dev: 1, Ino: hugeIno, RelPath: "b/two", Nlink: 2, Size: 4096, MtimeNs: 111},
	}
	if _, err := s.RecordSplit(shardID, 2, nil, nil, second, 0); err != nil {
		t.Fatalf("RecordSplit (second sighting) with a high-bit inode: %v", err)
	}
	// A second, independent group at the absolute max value.
	maxGroup := []NewLinkSighting{
		{Dev: maxIno, Ino: maxIno, RelPath: "c/max", Nlink: 2, Size: 8192, MtimeNs: 222},
	}
	if _, err := s.RecordSplit(shardID, 3, nil, nil, maxGroup, 0); err != nil {
		t.Fatalf("RecordSplit with dev=ino=MaxUint64: %v", err)
	}

	members, err := s.PendingLinkMembers(passID)
	if err != nil {
		t.Fatalf("PendingLinkMembers after a high-bit inode: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("PendingLinkMembers = %v, want 1 (b/two, pending against the huge-inode anchor)", members)
	}
	m := members[0]
	if m.RelPath != "b/two" || m.AnchorRelPath != "a/one" || m.AnchorSize != 4096 {
		t.Errorf("member = %+v, want rel_path=b/two anchor=a/one size=4096", m)
	}

	// dev/ino round-trip exactly, not just "some value" — verified via the raw
	// row rather than a Go-side accessor, since dev/ino are never SELECTed
	// back into a struct anywhere in the production code path. Filtered by
	// dev=1 to disambiguate from the second (dev=ino=MaxUint64) group also in
	// this pass.
	var rawIno int64
	if err := s.rdb.QueryRow(`SELECT ino FROM link_groups
		WHERE pass_id = ? AND dev = 1`, passID).Scan(&rawIno); err != nil {
		t.Fatalf("reading back the huge-inode group row: %v", err)
	}
	if gotIno := uint64(rawIno); gotIno != hugeIno {
		t.Errorf("group ino round-tripped as %d, want %d", gotIno, hugeIno)
	}

	// The MaxUint64 group (dev=ino=MaxUint64) round-trips too — the absolute
	// edge of the type, not just "a large value".
	var rawMaxDev, rawMaxIno int64
	if err := s.rdb.QueryRow(`SELECT dev, ino FROM link_groups
		WHERE pass_id = ? AND dev != 1`, passID).Scan(&rawMaxDev, &rawMaxIno); err != nil {
		t.Fatalf("reading back the MaxUint64 group row: %v", err)
	}
	if uint64(rawMaxDev) != maxIno || uint64(rawMaxIno) != maxIno {
		t.Errorf("MaxUint64 group round-tripped as (%d,%d), want (%d,%d)",
			uint64(rawMaxDev), uint64(rawMaxIno), maxIno, maxIno)
	}
}

// TestRecordLinkSightingsSecondIsPendingMember: a later sighting of the same
// group is a pending member, joined with the anchor's path and gen — exactly
// what seedLinkfix needs to build a LinkTask.
func TestRecordLinkSightingsSecondIsPendingMember(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)

	first := []NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "a/one", Nlink: 2, Size: 4096, MtimeNs: 111},
	}
	if _, err := s.RecordSplit(shardID, 1, nil, nil, first, 0); err != nil {
		t.Fatal(err)
	}
	second := []NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "b/two", Nlink: 2, Size: 4096, MtimeNs: 111},
	}
	if _, err := s.RecordSplit(shardID, 2, nil, nil, second, 0); err != nil {
		t.Fatal(err)
	}

	members, err := s.PendingLinkMembers(passID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 {
		t.Fatalf("PendingLinkMembers = %v, want 1 pending member", members)
	}
	m := members[0]
	if m.RelPath != "b/two" || m.AnchorRelPath != "a/one" ||
		m.AnchorSize != 4096 || m.AnchorMtimeNs != 111 {
		t.Errorf("member = %+v, want rel_path=b/two anchor=a/one size=4096 mtime=111", m)
	}
}

// TestRecordLinkSightingsIdempotent: a retransmitted ShardSplit (same
// parent+seq) must not double-record the sighting or its member row —
// RecordSplit's own retransmit short-circuit (return the cached assigned_ids)
// means recordLinkSightingsTx never even runs twice for the same split, but
// this pins that behavior specifically for the link-sighting path too.
func TestRecordLinkSightingsIdempotent(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)

	sightings := []NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "a/one", Nlink: 2, Size: 4096, MtimeNs: 111},
	}
	if _, err := s.RecordSplit(shardID, 1, nil, nil, sightings, 0); err != nil {
		t.Fatal(err)
	}
	// Same (parent, seq): RecordSplit returns the cached result without
	// re-running recordLinkSightingsTx.
	if _, err := s.RecordSplit(shardID, 1, nil, nil, sightings, 0); err != nil {
		t.Fatal(err)
	}

	var seen uint64
	if err := s.rdb.QueryRow(`SELECT members_seen FROM link_groups
		WHERE pass_id = ? AND dev = 1 AND ino = 100`, passID).Scan(&seen); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Errorf("members_seen = %d, want 1 (retransmit must not double-count)", seen)
	}
}

// TestRecordLinkSightingsMaxGroupScanFallback: once a group's member count
// exceeds the job's cap, it flips to anchor_state 'fallback' and stops
// producing PendingLinkMembers rows — its members keep their independent
// copies (docs/DESIGN-hardlinks.md §3.6).
func TestRecordLinkSightingsMaxGroupScanFallback(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)

	if _, err := s.RecordSplit(shardID, 1, nil, nil, []NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "a/one", Nlink: 3, Size: 10, MtimeNs: 1},
	}, 2); err != nil { // maxGroupScan=2
		t.Fatal(err)
	}
	if _, err := s.RecordSplit(shardID, 2, nil, nil, []NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "b/two", Nlink: 3, Size: 10, MtimeNs: 1},
	}, 2); err != nil {
		t.Fatal(err)
	}
	// Third sighting pushes members_seen to 3, over the cap of 2.
	if _, err := s.RecordSplit(shardID, 3, nil, nil, []NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "c/three", Nlink: 3, Size: 10, MtimeNs: 1},
	}, 2); err != nil {
		t.Fatal(err)
	}
	// Fourth sighting of the SAME already-fallen-back group: must not
	// increment link_fallback again (it counts groups, not sightings).
	if _, err := s.RecordSplit(shardID, 4, nil, nil, []NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "d/four", Nlink: 3, Size: 10, MtimeNs: 1},
	}, 2); err != nil {
		t.Fatal(err)
	}

	var anchorState string
	if err := s.rdb.QueryRow(`SELECT anchor_state FROM link_groups
		WHERE pass_id = ? AND dev = 1 AND ino = 100`, passID).Scan(&anchorState); err != nil {
		t.Fatal(err)
	}
	if anchorState != "fallback" {
		t.Errorf("anchor_state = %q, want fallback", anchorState)
	}
	members, err := s.PendingLinkMembers(passID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 0 {
		t.Fatalf("PendingLinkMembers = %v, want none once the group fell back", members)
	}

	var linkFallback int64
	if err := s.rdb.QueryRow(`SELECT link_fallback FROM passes WHERE id = ?`, passID).
		Scan(&linkFallback); err != nil {
		t.Fatal(err)
	}
	if linkFallback != 1 {
		t.Errorf("passes.link_fallback = %d, want 1 (one group fell back, four sightings)",
			linkFallback)
	}
}

// TestMarkLinkMembersQueuedIsScopedToPending: only 'pending' rows flip to
// 'queued' — an already-'anchor' row is untouched, so a defensive re-run of
// seedLinkfix's own bookkeeping can't corrupt a completed member's state.
func TestMarkLinkMembersQueuedIsScopedToPending(t *testing.T) {
	s := openTest(t)
	_, passID, shardID := seed(t, s)

	if _, err := s.RecordSplit(shardID, 1, nil, nil, []NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "a/one", Nlink: 2, Size: 10, MtimeNs: 1},
	}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordSplit(shardID, 2, nil, nil, []NewLinkSighting{
		{Dev: 1, Ino: 100, RelPath: "b/two", Nlink: 2, Size: 10, MtimeNs: 1},
	}, 0); err != nil {
		t.Fatal(err)
	}

	if err := s.MarkLinkMembersQueued(passID, []LinkMemberKey{
		{Dev: 1, Ino: 100, RelPath: "a/one"},
		{Dev: 1, Ino: 100, RelPath: "b/two"},
	}); err != nil {
		t.Fatal(err)
	}

	var anchorState, memberState string
	if err := s.rdb.QueryRow(`SELECT state FROM link_members
		WHERE pass_id = ? AND rel_path = 'a/one'`, passID).Scan(&anchorState); err != nil {
		t.Fatal(err)
	}
	if anchorState != "anchor" {
		t.Errorf("anchor member state = %q, want anchor (untouched by MarkLinkMembersQueued)", anchorState)
	}
	if err := s.rdb.QueryRow(`SELECT state FROM link_members
		WHERE pass_id = ? AND rel_path = 'b/two'`, passID).Scan(&memberState); err != nil {
		t.Fatal(err)
	}
	if memberState != "queued" {
		t.Errorf("pending member state = %q, want queued", memberState)
	}
}

// TestMarkLinkMembersQueuedStaysFastAtScale is the regression pin for the
// production incident this fixed: a link_members table with millions of
// unrelated pending rows for the same pass_id made a single
// MarkLinkMembersQueued call (then an UPDATE ... WHERE rel_path IN (...)
// list) take ~1.3s, because rel_path has no supporting index outside the
// tail of the (pass_id, dev, ino, rel_path) primary key — SQLite could only
// narrow to pass_id, then scan every one of that pass's rows checking the
// IN-list. Run once per LinkTaskBatch flush (~1250 times for a 2.5M-member
// pass), that held the coordinator's single write lock for a cumulative
// ~20+ minutes, stalling every agent's heartbeat renewal fleet-wide.
//
// Seeds link_members directly (bypassing RecordSplit/ShardSplit — not what
// this test is about) since the real bug only manifests at a row count
// RecordSplit's per-sighting path would take far too long to build in a
// unit test. Measures the pre-fix IN-list query directly, in the same
// process against the same table, as a same-hardware baseline, then asserts
// MarkLinkMembersQueued beats it by a wide margin — a relative comparison,
// not an absolute wall-clock bound, so it isn't sensitive to how fast or
// slow the machine running the test happens to be (an earlier version of
// this test used a fixed 200ms cutoff and flaked on slower CI hardware,
// having measured ~10ms locally).
func TestMarkLinkMembersQueuedStaysFastAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test; skipped with -short")
	}
	s := openTest(t)
	_, passID, _ := seed(t, s)

	// The baseline (pre-fix) query scales linearly with totalRows — it scans
	// every row of the pass checking the IN-list — while the fix stays flat
	// (a real seek doesn't care how big the table is). Measured directly at
	// this file's row counts: ~0.6x at 100K, ~1.4x at 500K, ~12.7x at 2.5M —
	// the gap only becomes a reliable signal at real production scale, which
	// is exactly why 500K (this test's first version) flaked: the two costs
	// hadn't diverged enough yet to leave any real margin.
	const totalRows = 2_500_000
	const batchSize = 2000
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO link_members (pass_id, dev, ino, rel_path, state)
		VALUES (?, ?, ?, ?, 'pending')`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < totalRows; i++ {
		if _, err := stmt.Exec(passID, 1, i, fmt.Sprintf("member/%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Two disjoint batches from the middle of the table (a scan-based plan
	// pays roughly the same regardless of position; a seek-based plan should
	// too, which is exactly what this test distinguishes): one for the
	// baseline (the exact pre-fix UPDATE ... IN (...) shape, run directly so
	// this test measures it on this same table on this same machine, not a
	// number from a different incident's hardware), one for the real
	// MarkLinkMembersQueued call under test. Both are real writes so the
	// comparison is apples to apples — an earlier version of this test used
	// a read-only SELECT as the "baseline", which turned out not to cost the
	// same as the write the incident actually measured.
	keys := make([]LinkMemberKey, batchSize)
	baselinePaths := make([]any, 0, batchSize+1)
	baselinePaths = append(baselinePaths, passID)
	for i := 0; i < batchSize; i++ {
		n := totalRows/2 + i
		keys[i] = LinkMemberKey{Dev: 1, Ino: uint64(n), RelPath: fmt.Sprintf("member/%d", n)}
		bn := totalRows/4 + i
		baselinePaths = append(baselinePaths, fmt.Sprintf("member/%d", bn))
	}

	ph := strings.TrimSuffix(strings.Repeat("?,", batchSize), ",")
	baselineStart := time.Now()
	res, err := s.db.Exec(`UPDATE link_members SET state = 'queued'
		WHERE pass_id = ? AND state = 'pending' AND rel_path IN (`+ph+`)`, baselinePaths...)
	if err != nil {
		t.Fatal(err)
	}
	baseline := time.Since(baselineStart)
	if n, _ := res.RowsAffected(); n != batchSize {
		t.Fatalf("baseline IN-list update affected %d rows, want %d", n, batchSize)
	}

	start := time.Now()
	if err := s.MarkLinkMembersQueued(passID, keys); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	t.Logf("baseline IN-list update: %v; MarkLinkMembersQueued (PK-seek): %v", baseline, elapsed)
	// Measured ~12.7x at this exact row count/batch size (file-based WAL db,
	// matching production's Open()). Require 3x: comfortable margin below
	// the measured gap so ordinary hardware/scheduling noise can't flake
	// this, while still failing hard if the fix regresses back to a
	// scan-based plan (which would put the ratio near 1x, not comfortably
	// above 3x).
	if elapsed*3 > baseline {
		t.Errorf("MarkLinkMembersQueued(%d keys) against %d rows took %v, "+
			"want well under the %v IN-list baseline (3x margin) — "+
			"looks like a scan-based plan regressed back in", batchSize, totalRows, elapsed, baseline)
	}

	var queued int
	if err := s.rdb.QueryRow(`SELECT COUNT(*) FROM link_members
		WHERE pass_id = ? AND state = 'queued'`, passID).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	// Two disjoint batches were queued: the baseline's IN-list update and the
	// real MarkLinkMembersQueued call under test.
	if want := 2 * batchSize; queued != want {
		t.Errorf("queued count = %d, want %d", queued, want)
	}
}
