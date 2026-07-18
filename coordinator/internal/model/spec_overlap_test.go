package model

import "testing"

// TestPathsOverlap: containment on whole components, not string prefixes. This
// backs both the source/destination disjointness rule and the cross-job
// destination check, where a false negative lets two jobs write into one tree
// and reclaim each other's in-progress temps.
func TestPathsOverlap(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"/dst", "/dst", true},
		{"/dst", "/dst/", true},     // trailing slash is not a difference
		{"/dst/", "/dst", true},     // ...in either direction
		{"/dst//", "/dst", true},    // nor is a doubled one
		{"/dst", "/dst/home", true}, // b nested under a
		{"/dst/home", "/dst", true}, // a nested under b
		{"/dst/a/b/c", "/dst/a", true},
		{"/", "/anything", true}, // root contains everything

		{"/dst/a", "/dst/ab", false}, // sibling, NOT a nesting: the bug a raw
		{"/dst/ab", "/dst/a", false}, // strings.HasPrefix test would introduce
		{"/dsta", "/dst", false},     // same, one component up
		{"/src", "/dst", false},      // plainly disjoint
		{"/dst/a", "/dst/b", false},  // siblings
		{"/a/b", "/b/a", false},      // same components, different order
	}
	for _, c := range cases {
		if got := PathsOverlap(c.a, c.b); got != c.want {
			t.Errorf("PathsOverlap(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
		// The relation is symmetric; callers rely on passing either order.
		if got := PathsOverlap(c.b, c.a); got != c.want {
			t.Errorf("PathsOverlap(%q, %q) = %v, want %v (asymmetric)", c.b, c.a, got, c.want)
		}
	}
}
