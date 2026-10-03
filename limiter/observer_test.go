package limiter

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestObserver_LimiterDecisions(t *testing.T) {
	var c Counters
	l := mustLimiter(t, Config{Rate: 1, Capacity: 5, Clock: newFakeClock(), Observer: &c})

	l.Allow()    // allowed, 1 token
	l.AllowN(3)  // allowed, 3 tokens
	l.AllowN(3)  // rejected, 3 tokens (only 1 left)
	l.AllowN(99) // rejected: exceeds capacity, still a valid request
	l.AllowN(0)  // invalid: not reported
	l.AllowN(-2) // invalid: not reported

	got := c.Snapshot()
	want := CountersSnapshot{AllowedRequests: 2, AllowedTokens: 4, RejectedRequests: 2, RejectedTokens: 102}
	if got != want {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
}

func TestObserver_Wait(t *testing.T) {
	var c Counters
	l := mustLimiter(t, Config{Rate: 1000, Capacity: 1, Observer: &c})
	l.Allow()
	if err := l.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = l.Wait(ctx)

	got := c.Snapshot()
	if got.Waits != 2 || got.WaitErrors != 1 {
		t.Fatalf("waits=%d errors=%d, want 2 and 1", got.Waits, got.WaitErrors)
	}
	// Allow + successful Wait are allowed; the cancelled Wait is rejected.
	if got.AllowedRequests != 2 || got.RejectedRequests != 1 {
		t.Fatalf("allowed=%d rejected=%d, want 2 and 1", got.AllowedRequests, got.RejectedRequests)
	}
	if got.WaitTotal <= 0 {
		t.Fatal("wait duration was not recorded")
	}
}

// With a global limiter involved a request still produces exactly one
// notification, and the per-user buckets stay silent.
func TestObserver_PerUserReportsOncePerRequest(t *testing.T) {
	var c, gc Counters
	clk := newFakeClock()
	g := mustLimiter(t, Config{Rate: 1, Capacity: 100, Clock: clk, Observer: &gc})
	p := mustPerUser(t, PerUserConfig{
		Limit:    Config{Rate: 1, Capacity: 2, Clock: clk, Observer: &gc}, // ignored by design
		Global:   g,
		Observer: &c,
	})
	for i := 0; i < 5; i++ {
		p.Allow("a")
	}
	got := c.Snapshot()
	if got.AllowedRequests != 2 || got.RejectedRequests != 3 {
		t.Fatalf("allowed=%d rejected=%d, want 2 and 3", got.AllowedRequests, got.RejectedRequests)
	}
	// The global limiter is charged through takeTwo, which does not notify its
	// own observer; the PerUser observer is the single source of truth.
	if s := gc.Snapshot(); s != (CountersSnapshot{}) {
		t.Fatalf("inner observers were notified: %+v", s)
	}
}

func TestCounters_ConcurrentUse(t *testing.T) {
	var c Counters
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				c.Allowed(1)
				c.Rejected(2)
				c.Waited(time.Microsecond, nil)
			}
		}()
	}
	wg.Wait()
	s := c.Snapshot()
	if s.AllowedRequests != 8000 || s.RejectedTokens != 16000 || s.Waits != 8000 || s.WaitTotal != 8000*time.Microsecond {
		t.Fatalf("snapshot = %+v", s)
	}
}
