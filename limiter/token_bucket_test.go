package limiter

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestNewLimiter_ValidatesConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"valid", Config{Rate: 10, Capacity: 100}, true},
		{"fractional rate", Config{Rate: 0.5, Capacity: 1}, true},
		{"capacity exactly one", Config{Rate: 1, Capacity: 1}, true},
		{"zero rate", Config{Rate: 0, Capacity: 10}, false},
		{"negative rate", Config{Rate: -1, Capacity: 10}, false},
		{"NaN rate", Config{Rate: math.NaN(), Capacity: 10}, false},
		{"infinite rate", Config{Rate: math.Inf(1), Capacity: 10}, false},
		{"zero capacity", Config{Rate: 1, Capacity: 0}, false},
		{"negative capacity", Config{Rate: 1, Capacity: -5}, false},
		{"capacity below one token", Config{Rate: 1, Capacity: 0.5}, false},
		{"NaN capacity", Config{Rate: 1, Capacity: math.NaN()}, false},
		{"infinite capacity", Config{Rate: 1, Capacity: math.Inf(1)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := NewLimiter(tt.cfg)
			if tt.ok {
				if err != nil || l == nil {
					t.Fatalf("want success, got limiter=%v err=%v", l, err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("want ErrInvalidConfig, got %v", err)
			}
			if l != nil {
				t.Fatal("limiter must be nil on error")
			}
		})
	}
}

func TestLimiter_StartsFull(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1, Capacity: 5, Clock: newFakeClock()})
	if got := l.Tokens(); got != 5 {
		t.Fatalf("initial tokens = %v, want 5", got)
	}
	if got := drain(l); got != 5 {
		t.Fatalf("initial burst = %d, want 5", got)
	}
}

func TestLimiter_AllowConsumesOneToken(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1, Capacity: 73, Clock: newFakeClock()})
	if !l.Allow() {
		t.Fatal("Allow on a full bucket failed")
	}
	if got := l.Tokens(); got != 72 {
		t.Fatalf("tokens after one request = %v, want 72", got)
	}
}

func TestLimiter_EmptyBucketRejects(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1, Capacity: 2, Clock: newFakeClock()})
	drain(l)
	if l.Allow() {
		t.Fatal("empty bucket admitted a request")
	}
	if got := l.Tokens(); got != 0 {
		t.Fatalf("a rejected request changed tokens to %v", got)
	}
}

func TestLimiter_RefillRestoresTokens(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Rate: 10, Capacity: 10, Clock: clk})
	drain(l)

	clk.Advance(100 * time.Millisecond) // 10/s * 0.1s = exactly 1 token
	if got := l.Tokens(); got != 1 {
		t.Fatalf("tokens after 100ms = %v, want 1", got)
	}
	if !l.Allow() {
		t.Fatal("request after refill rejected")
	}
	if l.Allow() {
		t.Fatal("second request admitted with only one token refilled")
	}
}

func TestLimiter_FractionalTokensAccumulate(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Rate: 10, Capacity: 10, Clock: clk})
	drain(l)

	clk.Advance(50 * time.Millisecond)
	if l.Allow() {
		t.Fatal("half a token must not admit a request")
	}
	clk.Advance(50 * time.Millisecond)
	if !l.Allow() {
		t.Fatal("two half tokens must add up to a request")
	}
}

func TestLimiter_RefillNeverExceedsCapacity(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Rate: 1000, Capacity: 7, Clock: clk})
	drain(l)

	for _, idle := range []time.Duration{time.Second, time.Hour, 24 * 365 * time.Hour, time.Duration(math.MaxInt64)} {
		clk.Advance(idle)
		if got := l.Tokens(); got != 7 {
			t.Fatalf("after idling %v tokens = %v, want capacity 7", idle, got)
		}
	}
}

func TestLimiter_HugeRateDoesNotOverflow(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Rate: math.MaxFloat64, Capacity: 3, Clock: clk})
	drain(l)
	clk.Advance(time.Hour)
	if got := l.Tokens(); got != 3 {
		t.Fatalf("tokens = %v, want 3", got)
	}
}

func TestLimiter_BurstThenSustained(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Rate: 10, Capacity: 100, Clock: clk})

	if got := drain(l); got != 100 {
		t.Fatalf("burst admitted %d requests, want 100", got)
	}
	if l.Allow() {
		t.Fatal("request beyond the burst was admitted")
	}

	// After the burst, throughput is bound by the refill rate: one second of
	// idleness buys exactly 10 requests, not 100.
	clk.Advance(time.Second)
	if got := drain(l); got != 10 {
		t.Fatalf("admitted %d requests after 1s, want 10", got)
	}

	// Steady arrivals at exactly the refill rate are all admitted.
	for i := 0; i < 50; i++ {
		clk.Advance(100 * time.Millisecond)
		if !l.Allow() {
			t.Fatalf("steady request %d rejected", i)
		}
	}
}

func TestLimiter_BurstRegrowsOnlyToCapacity(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Rate: 10, Capacity: 100, Clock: clk})
	drain(l)
	clk.Advance(time.Minute) // would be 600 tokens without the cap
	if got := drain(l); got != 100 {
		t.Fatalf("burst after long idle = %d, want 100", got)
	}
}

func TestLimiter_AllowN(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Rate: 1, Capacity: 10, Clock: clk})

	if !l.AllowN(4) || !l.AllowN(4) {
		t.Fatal("two requests of 4 from a bucket of 10 should succeed")
	}
	if l.AllowN(3) {
		t.Fatal("request of 3 admitted with only 2 tokens left")
	}
	if got := l.Tokens(); got != 2 {
		t.Fatalf("a rejected AllowN consumed tokens: have %v, want 2", got)
	}
	if !l.AllowN(2) {
		t.Fatal("exact remaining tokens should be admitted")
	}
	if got := l.Tokens(); got != 0 {
		t.Fatalf("tokens = %v, want 0", got)
	}
}

func TestLimiter_AllowN_ExactCapacity(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1, Capacity: 10, Clock: newFakeClock()})
	if !l.AllowN(10) {
		t.Fatal("consuming exactly the capacity should succeed")
	}
	if l.Allow() {
		t.Fatal("bucket should be empty")
	}
}

func TestLimiter_AllowN_InvalidN(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1, Capacity: 10, Clock: newFakeClock()})
	for _, n := range []int{0, -1, math.MinInt, 11, math.MaxInt} {
		if l.AllowN(n) {
			t.Fatalf("AllowN(%d) = true, want false", n)
		}
	}
	if got := l.Tokens(); got != 10 {
		t.Fatalf("invalid requests changed tokens to %v", got)
	}
}

func TestLimiter_TakeRetryAfter(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Rate: 10, Capacity: 10, Clock: clk})
	drain(l)

	if d := l.Take(1); d.Allowed || d.RetryAfter != 100*time.Millisecond {
		t.Fatalf("Take(1) = %+v, want rejected with RetryAfter 100ms", d)
	}
	if d := l.Take(5); d.Allowed || d.RetryAfter != 500*time.Millisecond {
		t.Fatalf("Take(5) = %+v, want rejected with RetryAfter 500ms", d)
	}

	clk.Advance(30 * time.Millisecond) // 0.3 tokens: 0.7 short for one token
	if d := l.Take(1); d.Allowed || d.RetryAfter != 70*time.Millisecond {
		t.Fatalf("Take(1) = %+v, want RetryAfter 70ms", d)
	}

	// Waiting exactly RetryAfter must be enough.
	clk.Advance(70 * time.Millisecond)
	if d := l.Take(1); !d.Allowed || d.RetryAfter != 0 {
		t.Fatalf("Take(1) after RetryAfter = %+v, want allowed", d)
	}
}

func TestLimiter_TakeRetryAfterIsZeroWhenImpossible(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1, Capacity: 3, Clock: newFakeClock()})
	if d := l.Take(4); d.Allowed || d.RetryAfter != 0 {
		t.Fatalf("Take(4) on capacity 3 = %+v, want rejected with no estimate", d)
	}
}

func TestLimiter_RetryAfterSaturates(t *testing.T) {
	l := mustLimiter(t, Config{Rate: math.SmallestNonzeroFloat64, Capacity: 1, Clock: newFakeClock()})
	l.Allow()
	if d := l.Take(1); d.Allowed || d.RetryAfter != time.Duration(math.MaxInt64) {
		t.Fatalf("Take = %+v, want saturated RetryAfter", d)
	}
}

func TestLimiter_ClockGoingBackwards(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Rate: 10, Capacity: 10, Clock: clk})
	drain(l)

	clk.Advance(-time.Hour) // e.g. a non-monotonic clock stepped backwards
	if l.Allow() {
		t.Fatal("a backwards clock step must not mint tokens")
	}
	if got := l.Tokens(); got != 0 {
		t.Fatalf("tokens = %v after backwards step, want 0", got)
	}

	// Time is counted from the re-anchored timestamp, so the bucket does not
	// stay frozen for an hour.
	clk.Advance(200 * time.Millisecond)
	if got := l.Tokens(); got != 2 {
		t.Fatalf("tokens = %v after 200ms, want 2", got)
	}
}

func TestLimiter_FrozenClockNeverRefills(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1e9, Capacity: 5, Clock: newFakeClock()})
	drain(l)
	for i := 0; i < 1000; i++ {
		if l.Allow() {
			t.Fatal("tokens appeared without time passing")
		}
	}
}

func TestLimiter_DefaultClockRefills(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1000, Capacity: 1})
	if !l.Allow() {
		t.Fatal("first request rejected")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !l.Allow() {
		if time.Now().After(deadline) {
			t.Fatal("system clock limiter never refilled")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestDocumentedWalkthrough replays, step for step, the worked example in
// docs/token-bucket.md (rate 5 tokens/s, capacity 10). If this test changes,
// that document must change with it.
func TestDocumentedWalkthrough(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Rate: 5, Capacity: 10, Clock: clk})

	step := func(advance time.Duration, n int, wantAllowed bool, wantTokens float64) {
		t.Helper()
		clk.Advance(advance)
		if got := l.AllowN(n); got != wantAllowed {
			t.Fatalf("after +%v, AllowN(%d) = %v, want %v", advance, n, got, wantAllowed)
		}
		if got := l.Tokens(); math.Abs(got-wantTokens) > 1e-9 {
			t.Fatalf("after +%v, tokens = %v, want %v", advance, got, wantTokens)
		}
	}

	step(0, 10, true, 0)                      // t=0.0s  burst of 10 at once
	step(0, 1, false, 0)                      // t=0.0s  11th request refused
	step(200*time.Millisecond, 1, true, 0)    // t=0.2s  1.0 token earned
	step(100*time.Millisecond, 1, false, 0.5) // t=0.3s  only 0.5 tokens
	step(100*time.Millisecond, 1, true, 0)    // t=0.4s  0.5 + 0.5 = 1.0
	step(time.Second, 3, true, 2)             // t=1.4s  5 earned, spend 3
	step(0, 3, false, 2)                      // t=1.4s  short by 1 token
	if d := l.Take(3); d.RetryAfter != 200*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want 200ms (1 token short at 5/s)", d.RetryAfter)
	}
	step(10*time.Second, 1, true, 9) // t=11.4s 50 earned, capped at 10, spend 1
}
