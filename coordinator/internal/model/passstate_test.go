package model

import "testing"

// EffectiveState closes the reporting gap where advance() seeds a phase's
// shards before flipping passes.state to that phase: reporting must show the
// phase the live queue proves, not the (possibly one-tick-stale) stored state.
func TestEffectiveStateAdvancesOnLiveShardKind(t *testing.T) {
	cases := []struct {
		name   string
		stored PassState
		kinds  map[ShardKind]bool
		want   PassState
	}{
		{"no shards yet, reports stored state", PassScanning, nil, PassScanning},
		{"dirfix shards queued while stored state still SCANNING", PassScanning,
			map[ShardKind]bool{KindDirfix: true}, PassDirfix},
		{"verify shards queued while stored state still DIRFIX", PassDirfix,
			map[ShardKind]bool{KindVerify: true}, PassVerify},
		{"leftover dirfix shards don't regress a later stored VERIFY", PassVerify,
			map[ShardKind]bool{KindDirfix: true}, PassVerify},
		{"scan-phase kinds (dir/entrylist/chunk) carry no phase signal", PassScanning,
			map[ShardKind]bool{KindDir: true, KindEntryList: true, KindChunk: true}, PassScanning},
		{"present-but-false map entries are ignored", PassScanning,
			map[ShardKind]bool{KindDirfix: false}, PassScanning},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.stored.EffectiveState(c.kinds); got != c.want {
				t.Errorf("EffectiveState(%s, %v) = %s, want %s", c.stored, c.kinds, got, c.want)
			}
		})
	}
}
