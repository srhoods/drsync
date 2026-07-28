package main

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestAutoRefresherDefaultsToOff(t *testing.T) {
	var r autoRefresher
	if got := r.Interval(); got != 0 {
		t.Fatalf("new autoRefresher interval = %v, want 0 (on-demand)", got)
	}
}

func TestAutoRefresherClampsBelowMinimum(t *testing.T) {
	var r autoRefresher
	defer r.Stop()
	got := r.Set(1*time.Second, func() {})
	if got != minRefreshInterval {
		t.Fatalf("Set(1s) = %v, want clamped to minRefreshInterval (%v)", got, minRefreshInterval)
	}
	if r.Interval() != minRefreshInterval {
		t.Fatalf("Interval() = %v after clamp, want %v", r.Interval(), minRefreshInterval)
	}
}

func TestAutoRefresherZeroOrNegativeTurnsOff(t *testing.T) {
	var r autoRefresher
	r.Set(10*time.Second, func() {})
	if r.Interval() == 0 {
		t.Fatal("expected non-zero interval after Set(10s)")
	}
	got := r.Set(0, nil)
	if got != 0 || r.Interval() != 0 {
		t.Fatalf("Set(0) should turn refresh off, got interval=%v", r.Interval())
	}

	r.Set(10*time.Second, func() {})
	got = r.Set(-5*time.Second, nil)
	if got != 0 || r.Interval() != 0 {
		t.Fatalf("Set(negative) should turn refresh off, got interval=%v", r.Interval())
	}
}

func TestAutoRefresherTicksAtSetInterval(t *testing.T) {
	var r autoRefresher
	defer r.Stop()
	var ticks int64
	// Use exactly the floor so the test doesn't need its own separate
	// "is clamping too aggressive" tolerance — this is the fastest legal rate.
	r.Set(minRefreshInterval, func() { atomic.AddInt64(&ticks, 1) })

	deadline := time.After(minRefreshInterval*2 + 2*time.Second)
	for atomic.LoadInt64(&ticks) < 1 {
		select {
		case <-deadline:
			t.Fatal("no tick observed within 2 refresh intervals + slack")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestAutoRefresherStopHaltsTicking(t *testing.T) {
	var r autoRefresher
	var ticks int64
	r.Set(minRefreshInterval, func() { atomic.AddInt64(&ticks, 1) })
	r.Stop()
	if r.Interval() != 0 {
		t.Fatalf("Interval() after Stop = %v, want 0", r.Interval())
	}

	before := atomic.LoadInt64(&ticks)
	time.Sleep(minRefreshInterval + 1*time.Second)
	after := atomic.LoadInt64(&ticks)
	if after != before {
		t.Fatalf("ticks increased from %d to %d after Stop — ticker goroutine kept running", before, after)
	}
}

func TestAutoRefresherStopIsIdempotent(t *testing.T) {
	var r autoRefresher
	r.Stop() // never started
	r.Set(minRefreshInterval, func() {})
	r.Stop()
	r.Stop() // stopping twice must not panic (double close of the stop channel)
}

func TestAutoRefresherSetReplacesPreviousTicker(t *testing.T) {
	var r autoRefresher
	defer r.Stop()
	var slowTicks, fastTicks int64
	r.Set(minRefreshInterval, func() { atomic.AddInt64(&slowTicks, 1) })
	// Re-Set before the first ticker ever fires: only the second callback
	// should ever run, proving the first goroutine was actually stopped
	// rather than left running alongside the new one.
	r.Set(minRefreshInterval, func() { atomic.AddInt64(&fastTicks, 1) })

	time.Sleep(minRefreshInterval + 2*time.Second)
	if atomic.LoadInt64(&slowTicks) != 0 {
		t.Fatalf("first ticker's callback fired %d times after being replaced", slowTicks)
	}
	if atomic.LoadInt64(&fastTicks) == 0 {
		t.Fatal("replacement ticker never fired")
	}
}

func TestRefreshLabel(t *testing.T) {
	if got := refreshLabel(0); got != "on-demand" {
		t.Errorf("refreshLabel(0) = %q, want on-demand", got)
	}
	if got := refreshLabel(5 * time.Second); got != "5s" {
		t.Errorf("refreshLabel(5s) = %q, want 5s", got)
	}
}
