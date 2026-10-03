package limiter

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// With a frozen clock no tokens are ever refilled, so the number of successful
// requests is exactly determined: it must equal the initial capacity. Any
// double spend (two goroutines consuming the same token) makes the count
// exceed it; any lost update makes it fall short.
func TestConcurrent_AllowFrozenClockAdmitsExactlyCapacity(t *testing.T) {
	const (
		capacity   = 1000
		goroutines = 64
		attempts   = 100 // 6400 attempts for 1000 tokens
	)
	l := mustLimiter(t, Config{Rate: 1, Capacity: capacity, Clock: newFakeClock()})

	var ok atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < attempts; i++ {
				if l.Allow() {
					ok.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := ok.Load(); got != capacity {
		t.Fatalf("admitted %d requests, want exactly %d", got, capacity)
	}
	if got := l.Tokens(); got != 0 {
		t.Fatalf("tokens = %v, want 0", got)
	}
}

// Mixed request sizes: conservation of tokens. Whatever was granted plus
// whatever remains must equal the starting budget, and nothing may go
// negative.
func TestConcurrent_AllowNConservesTokens(t *testing.T) {
	const capacity = 5000
	l := mustLimiter(t, Config{Rate: 1, Capacity: capacity, Clock: newFakeClock()})

	var granted atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 500; i++ {
				n := 1 + rng.Intn(20)
				if l.AllowN(n) {
					granted.Add(int64(n))
				}
			}
		}(int64(g))
	}
	wg.Wait()

	remaining := l.Tokens()
	if remaining < 0 {
		t.Fatalf("negative tokens: %v", remaining)
	}
	if got := float64(granted.Load()) + remaining; got != capacity {
		t.Fatalf("granted %d + remaining %v = %v, want %d", granted.Load(), remaining, got, capacity)
	}
}

// With a real clock the exact count is not deterministic, but it is bounded:
// total supply over the run is capacity plus what the rate could produce in
// the measured window.
func TestConcurrent_AllowRealClockStaysWithinBudget(t *testing.T) {
	const (
		rate     = 2000.0
		capacity = 50.0
	)
	begin := time.Now()
	l := mustLimiter(t, Config{Rate: rate, Capacity: capacity})

	var ok atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if l.Allow() {
						ok.Add(1)
					}
				}
			}
		}()
	}
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()
	elapsed := time.Since(begin).Seconds()

	budget := capacity + rate*elapsed
	if got := float64(ok.Load()); got > budget+1e-6 {
		t.Fatalf("admitted %v requests, budget for %.3fs is %v", got, elapsed, budget)
	}
	// Liveness: the limiter must not be stingier than half the budget.
	if got := float64(ok.Load()); got < budget/2 {
		t.Fatalf("admitted only %v of a budget of %v", got, budget)
	}
}

// Readers of Tokens and Take running together exercise the refill path from
// many goroutines; the race detector is the main assertion here.
func TestConcurrent_MixedOperationsAreRaceFree(t *testing.T) {
	l := mustLimiter(t, Config{Rate: 5000, Capacity: 20})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				l.Allow()
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				if tok := l.Tokens(); tok < 0 || tok > 20 {
					t.Errorf("tokens out of range: %v", tok)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				l.Take(3)
			}
		}()
	}
	wg.Wait()
}
