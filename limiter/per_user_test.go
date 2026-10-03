package limiter

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewPerUser_ValidatesConfig(t *testing.T) {
	good := Config{Rate: 1, Capacity: 1}
	for name, cfg := range map[string]PerUserConfig{
		"bad limit":    {Limit: Config{Rate: 0, Capacity: 1}},
		"negative max": {Limit: good, MaxUsers: -1},
	} {
		if _, err := NewPerUser(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: err = %v, want ErrInvalidConfig", name, err)
		}
	}
	if _, err := NewPerUser(PerUserConfig{Limit: good}); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestPerUser_UsersAreIsolated(t *testing.T) {
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 3, Clock: newFakeClock()}})

	for i := 0; i < 3; i++ {
		if !p.Allow("A") {
			t.Fatalf("A request %d rejected", i)
		}
	}
	if p.Allow("A") {
		t.Fatal("A exceeded its bucket")
	}
	// A is exhausted; B must be entirely unaffected.
	for i := 0; i < 3; i++ {
		if !p.Allow("B") {
			t.Fatalf("B request %d rejected because of A", i)
		}
	}
	if p.Len() != 2 {
		t.Fatalf("Len = %d, want 2", p.Len())
	}
}

func TestPerUser_RefillIsPerUser(t *testing.T) {
	clk := newFakeClock()
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 10, Capacity: 1, Clock: clk}})
	p.Allow("A")
	clk.Advance(50 * time.Millisecond)
	p.Allow("B") // B created (full) halfway through A's refill
	clk.Advance(50 * time.Millisecond)
	if !p.Allow("A") {
		t.Fatal("A should have refilled after 100ms")
	}
	if p.Allow("B") {
		t.Fatal("B has been idle only 50ms, but is not yet empty-then-refilled")
	}
}

func TestPerUser_InvalidRequestsCreateNoState(t *testing.T) {
	g := mustLimiter(t, Config{Rate: 1, Capacity: 2, Clock: newFakeClock()})
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 5, Clock: newFakeClock()}, Global: g})
	for _, n := range []int{0, -1, 6, 3} { // 3 fits the user bucket but not the global one
		if p.AllowN("x", n) {
			t.Fatalf("AllowN(%d) = true", n)
		}
	}
	if p.Len() != 0 {
		t.Fatalf("Len = %d; impossible requests must not allocate buckets", p.Len())
	}
}

func TestPerUser_GlobalLimitAppliesAcrossUsers(t *testing.T) {
	clk := newFakeClock()
	g := mustLimiter(t, Config{Rate: 1, Capacity: 5, Clock: clk})
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 3, Clock: clk}, Global: g})

	if !(p.Allow("A") && p.Allow("A") && p.Allow("A")) {
		t.Fatal("A should get its 3 tokens")
	}
	if p.Allow("A") {
		t.Fatal("A exceeded its per-user limit")
	}
	if !p.Allow("B") || !p.Allow("B") {
		t.Fatal("B should get 2 tokens while the global limiter has 2 left")
	}
	// The global bucket (5) is now empty. C has a full personal bucket but the
	// global limit binds.
	if p.Allow("C") {
		t.Fatal("C admitted although the global limiter is empty")
	}
}

// The central correctness property of layered limiting: a request rejected by
// one layer must not consume from the other.
func TestPerUser_NoTokenLeakageBetweenLayers(t *testing.T) {
	t.Run("user rejects, global untouched", func(t *testing.T) {
		clk := newFakeClock()
		g := mustLimiter(t, Config{Rate: 1, Capacity: 10, Clock: clk})
		p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 2, Clock: clk}, Global: g})

		ok := 0
		for i := 0; i < 100; i++ { // one abusive user
			if p.Allow("abuser") {
				ok++
			}
		}
		if ok != 2 {
			t.Fatalf("abuser admitted %d times, want 2", ok)
		}
		if got := g.Tokens(); got != 8 {
			t.Fatalf("global tokens = %v, want 8: rejected requests leaked global tokens", got)
		}
	})

	t.Run("global rejects, user untouched", func(t *testing.T) {
		clk := newFakeClock()
		g := mustLimiter(t, Config{Rate: 1, Capacity: 1, Clock: clk})
		p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 5, Clock: clk}, Global: g})

		if !p.Allow("A") {
			t.Fatal("first request should pass")
		}
		for i := 0; i < 10; i++ {
			if p.Allow("A") {
				t.Fatal("global limiter should be empty")
			}
		}
		if got := p.buckets["A"].Tokens(); got != 4 {
			t.Fatalf("A's tokens = %v, want 4: globally rejected requests leaked user tokens", got)
		}
	})
}

func TestPerUser_TakeRetryAfterIsMaxOfLayers(t *testing.T) {
	clk := newFakeClock()
	g := mustLimiter(t, Config{Rate: 1, Capacity: 1, Clock: clk})                                   // 1s per token
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 10, Capacity: 1, Clock: clk}, Global: g}) // 100ms per token
	if !p.Allow("A") {
		t.Fatal("setup")
	}
	// Both layers are now empty; the slower (global) layer decides.
	if d := p.Take("A", 1); d.Allowed || d.RetryAfter != time.Second {
		t.Fatalf("Take = %+v, want RetryAfter 1s", d)
	}
}

// Many users, many goroutines, frozen clock: the outcome is exactly
// determined, so any leakage or double spend shows up as a wrong total.
func TestPerUser_ConcurrentGlobalAndUserAccounting(t *testing.T) {
	const (
		users      = 50
		userCap    = 10
		globalCap  = 100
		perUserTry = 40
	)
	clk := newFakeClock()
	g := mustLimiter(t, Config{Rate: 1, Capacity: globalCap, Clock: clk})
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: userCap, Clock: clk}, Global: g})

	var ok atomic.Int64
	var wg sync.WaitGroup
	for u := 0; u < users; u++ {
		key := fmt.Sprintf("user-%d", u)
		for g := 0; g < 4; g++ { // several goroutines share each user
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perUserTry/4; i++ {
					if p.Allow(key) {
						ok.Add(1)
					}
				}
			}()
		}
	}
	wg.Wait()

	if got := ok.Load(); got != globalCap {
		t.Fatalf("admitted %d, want exactly the global capacity %d", got, globalCap)
	}
	if got := g.Tokens(); got != 0 {
		t.Fatalf("global tokens = %v, want 0", got)
	}
	var spent float64
	for _, l := range p.buckets {
		spent += userCap - l.Tokens()
		if l.Tokens() < 0 {
			t.Fatal("negative user tokens")
		}
	}
	if spent != globalCap {
		t.Fatalf("users spent %v tokens in total, global spent %d: layers disagree", spent, globalCap)
	}
}

func TestPerUser_CleanupRemovesOnlyFullBuckets(t *testing.T) {
	clk := newFakeClock()
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 10, Clock: clk}})
	p.AllowN("heavy", 10) // empty: needs 10s to refill
	p.AllowN("light", 1)  // needs 1s to refill

	if got := p.Cleanup(); got != 0 {
		t.Fatalf("Cleanup removed %d buckets immediately, want 0", got)
	}
	clk.Advance(2 * time.Second)
	if got := p.Cleanup(); got != 1 || p.Len() != 1 {
		t.Fatalf("after 2s: removed %d, Len %d; want 1 and 1 (light gone, heavy kept)", got, p.Len())
	}
	if _, ok := p.buckets["heavy"]; !ok {
		t.Fatal("heavy's partially refilled bucket was evicted")
	}
	clk.Advance(10 * time.Second)
	if got := p.Cleanup(); got != 1 || p.Len() != 0 {
		t.Fatalf("after 12s: removed %d, Len %d; want 1 and 0", got, p.Len())
	}
}

// Eviction must be invisible: a limiter that is swept aggressively must make
// exactly the same decisions as one that never is, including with a global
// limiter involved.
func TestPerUser_CleanupIsLossless(t *testing.T) {
	for seed := int64(0); seed < 50; seed++ {
		rng := rand.New(rand.NewSource(seed))
		clk := newFakeClock()
		mk := func() *PerUser {
			g := mustLimiter(t, Config{Rate: 20, Capacity: 30, Clock: clk})
			return mustPerUser(t, PerUserConfig{Limit: Config{Rate: 3, Capacity: 5, Clock: clk}, Global: g})
		}
		swept, plain := mk(), mk()

		for i := 0; i < 2000; i++ {
			clk.Advance(time.Duration(rng.Int63n(int64(400 * time.Millisecond))))
			key := fmt.Sprintf("u%d", rng.Intn(12))
			n := 1 + rng.Intn(3)
			a, b := swept.AllowN(key, n), plain.AllowN(key, n)
			if a != b {
				t.Fatalf("seed %d step %d: swept=%v plain=%v; eviction changed a decision", seed, i, a, b)
			}
			if rng.Intn(3) == 0 {
				swept.Cleanup()
			}
		}
	}
}

func TestPerUser_MaxUsers(t *testing.T) {
	clk := newFakeClock()
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 10, Capacity: 2, Clock: clk}, MaxUsers: 2})

	if !p.Allow("A") || !p.Allow("B") {
		t.Fatal("first two users should be admitted")
	}
	if p.Allow("C") {
		t.Fatal("third user admitted beyond MaxUsers")
	}
	if d := p.Take("C", 1); d.Allowed || d.RetryAfter != 0 {
		t.Fatalf("Take for a refused new key = %+v, want rejected with no retry estimate", d)
	}
	if !p.Allow("A") {
		t.Fatal("existing users must keep working when the table is full")
	}
	if p.Len() != 2 {
		t.Fatalf("Len = %d, want 2", p.Len())
	}

	// A and B refill fully within 200ms, but the on-demand sweep is rate
	// limited to once per second, so C is still refused at 500ms...
	clk.Advance(500 * time.Millisecond)
	if p.Allow("C") {
		t.Fatal("sweep cooldown not honored")
	}
	// ...and admitted once the cooldown has passed and idle buckets are swept.
	clk.Advance(600 * time.Millisecond)
	if !p.Allow("C") {
		t.Fatal("C should be admitted after idle buckets were swept")
	}
	if p.Len() != 1 {
		t.Fatalf("Len = %d, want 1 after sweep", p.Len())
	}
}

func TestPerUser_RunCleanup(t *testing.T) {
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1000, Capacity: 2}})
	p.AllowN("a", 2)
	p.AllowN("b", 2)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.RunCleanup(ctx, 2*time.Millisecond) }()

	deadline := time.Now().Add(5 * time.Second)
	for p.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("RunCleanup never removed idle buckets (Len=%d)", p.Len())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunCleanup returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunCleanup did not stop after cancellation")
	}
}

func TestPerUser_RunCleanupRejectsBadInterval(t *testing.T) {
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 1}})
	for _, d := range []time.Duration{0, -time.Second} {
		if err := p.RunCleanup(context.Background(), d); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("RunCleanup(%v) = %v, want ErrInvalidConfig", d, err)
		}
	}
}

// Requests, key creation, sweeps and Len from many goroutines at once. The
// race detector and the invariants are the assertions.
func TestPerUser_ConcurrentRequestsWithCleanup(t *testing.T) {
	g := mustLimiter(t, Config{Rate: 1e6, Capacity: 1e6})
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 20000, Capacity: 5}, Global: g, MaxUsers: 200})

	ctx, cancel := context.WithCancel(context.Background())
	var cleaner sync.WaitGroup
	cleaner.Add(1)
	go func() { defer cleaner.Done(); _ = p.RunCleanup(ctx, time.Millisecond) }()

	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 3000; i++ {
				p.Allow(fmt.Sprintf("user-%d", rng.Intn(500)))
				if i%500 == 0 {
					if n := p.Len(); n > 200 {
						t.Errorf("Len = %d exceeds MaxUsers", n)
					}
				}
			}
		}(int64(w))
	}
	wg.Wait()
	cancel()
	cleaner.Wait()

	p.mu.RLock()
	defer p.mu.RUnlock()
	for k, l := range p.buckets {
		if tok := l.Tokens(); tok < 0 || tok > 5 {
			t.Fatalf("user %s tokens out of range: %v", k, tok)
		}
	}
}

// Many goroutines make the very first request for the same key at the same
// instant. They all miss under the read lock and then queue for the write lock;
// only the first may create the bucket. If the write path did not re-check the
// table after the upgrade, each would install its own fresh bucket, silently
// discarding the tokens already spent, and more than capacity requests would
// succeed. With a frozen clock the correct answer is exactly capacity.
//
// The interleaving is forced rather than hoped for: the test holds the table's
// write lock while the goroutines start, so they all block in RLock with the
// table still empty, and are released together.
func TestPerUser_ConcurrentFirstRequestsShareOneBucket(t *testing.T) {
	const (
		capacity   = 5
		goroutines = 32
		rounds     = 50
	)
	for round := 0; round < rounds; round++ {
		p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: capacity, Clock: newFakeClock()}})

		var ok, started atomic.Int64
		var wg sync.WaitGroup
		p.mu.Lock() // readers will queue behind this
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				started.Add(1)
				if p.Allow("brand-new-key") {
					ok.Add(1)
				}
			}()
		}
		for started.Load() < goroutines {
			runtime.Gosched()
		}
		time.Sleep(2 * time.Millisecond) // let them park in RLock; too short only weakens the test, never breaks it
		p.mu.Unlock()
		wg.Wait()

		if got := ok.Load(); got != capacity {
			t.Fatalf("round %d: %d requests admitted for one key, want exactly %d (duplicate bucket created?)", round, got, capacity)
		}
		if p.Len() != 1 {
			t.Fatalf("round %d: Len = %d, want 1", round, p.Len())
		}
	}
}
