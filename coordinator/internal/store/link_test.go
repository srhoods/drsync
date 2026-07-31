package store

import "testing"

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

	if err := s.MarkLinkMembersQueued(passID, []string{"a/one", "b/two"}); err != nil {
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
