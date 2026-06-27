package pool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStartupProbeSlot_BoundsGlobalConcurrency proves the process-wide startup
// probe semaphore caps total concurrent probes across all pool instances. This
// is the fix for the hybrid/multi-port freeze: thousands of single-member pools
// each launch a startup probe, and the per-pool worker limit cannot bound them
// (each pool has one member), so without a global cap thousands of dials fire
// at once and exhaust file descriptors.
func TestStartupProbeSlot_BoundsGlobalConcurrency(t *testing.T) {
	// Reset the lazily-initialized semaphore for a deterministic test.
	startupProbeSemOnce = sync.Once{}
	startupProbeSem = nil
	SetStartupProbeLimit(8) // clamps to a known small ceiling

	const goroutines = 200
	var inFlight, maxInFlight int32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			release := acquireStartupProbeSlot()
			defer release()
			cur := atomic.AddInt32(&inFlight, 1)
			for {
				m := atomic.LoadInt32(&maxInFlight)
				if cur <= m || atomic.CompareAndSwapInt32(&maxInFlight, m, cur) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond) // hold the slot briefly
			atomic.AddInt32(&inFlight, -1)
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&maxInFlight); got > 8 {
		t.Fatalf("global startup-probe concurrency exceeded cap: peak=%d, want <= 8", got)
	}
}

// TestSetStartupProbeLimit_Clamps verifies the limit is bounded to a safe range.
func TestSetStartupProbeLimit_Clamps(t *testing.T) {
	cases := []struct {
		in   int
		want int32
	}{
		{0, 8}, {3, 8}, {64, 64}, {1000, 256},
	}
	for _, c := range cases {
		SetStartupProbeLimit(c.in)
		if got := atomic.LoadInt32(&startupProbeLimit); got != c.want {
			t.Errorf("SetStartupProbeLimit(%d) -> %d, want %d", c.in, got, c.want)
		}
	}
}
