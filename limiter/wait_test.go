package limiter

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests use the real clock with fast refill rates so waits are a few
// milliseconds. Assertions on elapsed time only use bounds that are guaranteed
// (timers never fire early), never upper bounds tight enough to flake on a
// loaded CI machine.

func TestWait_ImmediateWhenTokenAvailable(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1, Capacity: 1})
	start := time.Now()
	if err := l.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Wait blocked despite an available token")
	}
	if l.Allow() {
		t.Fatal("Wait did not consume the token")
	}
}

func TestWait_BlocksUntilRefill(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 100, Capacity: 1}) // one token per 10ms
	if !l.Allow() {
		t.Fatal("setup: first token")
	}
	start := time.Now()
	if err := l.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Fatalf("Wait returned after %v, before a token could exist", elapsed)
	}
}

func TestWaitN(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1000, Capacity: 10})
	if !l.AllowN(10) {
		t.Fatal("setup")
	}
	start := time.Now()
	if err := l.WaitN(context.Background(), 5); err != nil { // 5 tokens = 5ms
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 3*time.Millisecond {
		t.Fatalf("WaitN(5) returned after %v", elapsed)
	}
}

func TestWait_InvalidArguments(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1, Capacity: 3})
	for _, n := range []int{0, -4} {
		if err := l.WaitN(context.Background(), n); !errors.Is(err, ErrInvalidTokens) {
			t.Fatalf("WaitN(%d) = %v, want ErrInvalidTokens", n, err)
		}
	}
	if err := l.WaitN(context.Background(), 4); !errors.Is(err, ErrExceedsCapacity) {
		t.Fatalf("WaitN(4) = %v, want ErrExceedsCapacity", err)
	}
}

func TestWait_AlreadyCancelledConsumesNothing(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 1, Capacity: 5})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait = %v, want context.Canceled", err)
	}
	if got := l.Tokens(); got < 5 {
		t.Fatalf("cancelled Wait consumed tokens: %v left", got)
	}
}

func TestWait_CancelledWhileWaiting(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 0.01, Capacity: 1}) // next token in 100s
	l.Allow()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Wait(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after cancellation")
	}
}

func TestWait_DeadlineShorterThanWaitFailsFast(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 0.01, Capacity: 1})
	l.Allow()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	err := l.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait = %v, want DeadlineExceeded", err)
	}
	// The wait needed 100s but the deadline is 10s away; the call must not
	// have slept toward the deadline.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Wait slept %v instead of failing fast", elapsed)
	}
}

func TestWait_DeadlineExpiresWhileWaiting(t *testing.T) {
	// A waiter that could succeed within its deadline loses the race to a
	// competing consumer and must then give up at the deadline.
	l := mustLimiter(t, Config{Rate: 100, Capacity: 1})
	l.Allow()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // thief: takes every token the instant it appears
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				l.Allow()
			}
		}
	}()
	err := l.Wait(ctx)
	close(stop)
	wg.Wait()
	// The thief may occasionally lose the race; only an error, if any, is
	// constrained.
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait = %v, want nil or DeadlineExceeded", err)
	}
}

func TestWait_NoGoroutineLeak(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 0.01, Capacity: 1})
	l.Allow()
	before := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			_ = l.Wait(ctx) // fails fast: needs 100s, has 20ms
		}()
	}
	wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	var wg2 sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg2.Add(1)
		go func() { defer wg2.Done(); _ = l.Wait(ctx) }()
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
	wg2.Wait()

	// Give the runtime a moment to retire exiting goroutines.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines: %d before, %d after; Wait leaked", before, after)
	}
}

func TestWait_ManyWaitersRespectRate(t *testing.T) {
	const waiters = 50
	start := time.Now()
	l := mustLimiter(t, Config{Rate: 1000, Capacity: 1})

	var ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if l.Wait(ctx) == nil {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := ok.Load(); got != waiters {
		t.Fatalf("%d of %d waiters succeeded", got, waiters)
	}
	// Supply is capacity + rate*t, so 50 tokens cannot exist before
	// (50-1)/1000 s = 49ms. Allow slack for float rounding only.
	if elapsed := time.Since(start); elapsed < 48*time.Millisecond {
		t.Fatalf("%d waiters finished in %v, faster than the rate permits", waiters, elapsed)
	}
}
