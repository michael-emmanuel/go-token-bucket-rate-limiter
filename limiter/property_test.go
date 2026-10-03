package limiter

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"testing"
	"time"
)

// TestProperty_MatchesExactModel drives a Limiter with random operations and
// compares every decision with an exact rational-arithmetic model of the
// mathematical token bucket:
//
//	tokens = min(capacity, tokens + elapsed_seconds * rate)
//
// The float64 implementation may differ from the model by rounding error, so a
// disagreement is only a failure when the model's answer is decided by more
// than epsilon. Inside that band either answer is acceptable and the model
// adopts the implementation's answer, keeping the two aligned.
//
// It also checks the invariant 0 <= tokens <= capacity after every step and
// that the float state stays within epsilon of the model.
func TestProperty_MatchesExactModel(t *testing.T) {
	const (
		seeds = 300
		steps = 400
	)
	epsilon := big.NewRat(1, 1_000_000)
	negEpsilon := new(big.Rat).Neg(epsilon)

	for seed := int64(0); seed < seeds; seed++ {
		rng := rand.New(rand.NewSource(seed))
		rate := int64(1 + rng.Intn(50))
		capacity := int64(1 + rng.Intn(60))

		clk := newFakeClock()
		l := mustLimiter(t, Config{Rate: float64(rate), Capacity: float64(capacity), Clock: clk})

		model := big.NewRat(capacity, 1)
		capRat := big.NewRat(capacity, 1)
		ctx := func(step int) string {
			return fmt.Sprintf("seed=%d step=%d rate=%d capacity=%d", seed, step, rate, capacity)
		}

		for step := 0; step < steps; step++ {
			// Advance time: mostly small gaps, sometimes none, occasionally
			// a very long idle period.
			var d time.Duration
			switch r := rng.Intn(10); {
			case r < 2:
				d = 0
			case r < 9:
				d = time.Duration(rng.Int63n(int64(1500 * time.Millisecond)))
			default:
				d = time.Duration(rng.Int63n(int64(48 * time.Hour)))
			}
			clk.Advance(d)
			earned := big.NewRat(d.Nanoseconds()*rate, int64(time.Second))
			model.Add(model, earned)
			if model.Cmp(capRat) > 0 {
				model.Set(capRat)
			}

			n := int64(1 + rng.Intn(int(capacity)+2)) // sometimes above capacity
			got := l.AllowN(int(n))

			need := big.NewRat(n, 1)
			margin := new(big.Rat).Sub(model, need) // model tokens - n
			switch {
			case n > capacity:
				if got {
					t.Fatalf("%s: admitted n=%d above capacity", ctx(step), n)
				}
			case margin.Cmp(epsilon) > 0 && !got:
				t.Fatalf("%s: rejected n=%d but model has %v tokens", ctx(step), n, model.FloatString(9))
			case margin.Cmp(negEpsilon) < 0 && got:
				t.Fatalf("%s: admitted n=%d but model has only %v tokens", ctx(step), n, model.FloatString(9))
			}
			if got && n <= capacity {
				model.Sub(model, need)
				if model.Sign() < 0 { // borderline admit: absorb rounding
					model.SetInt64(0)
				}
			}

			// Invariants on the implementation's own state (single goroutine,
			// so reading the fields directly is safe).
			if l.tokens < 0 || l.tokens > l.capacity {
				t.Fatalf("%s: invariant 0 <= tokens <= capacity broken: %v", ctx(step), l.tokens)
			}
			// Compare effective tokens. Tokens applies pending refill; the raw
			// field can legitimately be stale after a request that was
			// rejected up front (n > capacity) because refill is lazy.
			m, _ := model.Float64()
			if eff := l.Tokens(); math.Abs(eff-m) > 1e-6 {
				t.Fatalf("%s: state drifted: limiter %v, model %v", ctx(step), eff, m)
			}
		}
	}
}

// Whatever the sequence, total granted tokens can never exceed the initial
// capacity plus what the rate could have produced over the elapsed time.
func TestProperty_NeverExceedsSupply(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		rng := rand.New(rand.NewSource(seed))
		rate := 0.5 + rng.Float64()*100
		capacity := float64(1 + rng.Intn(100))
		clk := newFakeClock()
		l := mustLimiter(t, Config{Rate: rate, Capacity: capacity, Clock: clk})

		var granted, elapsed float64
		for i := 0; i < 500; i++ {
			d := time.Duration(rng.Int63n(int64(500 * time.Millisecond)))
			clk.Advance(d)
			elapsed += d.Seconds()
			n := 1 + rng.Intn(int(capacity))
			if l.AllowN(n) {
				granted += float64(n)
			}
			if supply := capacity + rate*elapsed; granted > supply+1e-6 {
				t.Fatalf("seed %d: granted %v exceeds supply %v", seed, granted, supply)
			}
			if tok := l.tokens; tok < 0 || tok > capacity {
				t.Fatalf("seed %d: tokens %v outside [0, %v]", seed, tok, capacity)
			}
		}
	}
}

// Waiting exactly the advertised RetryAfter must always be enough. This
// exercises the ceil() in retryAfterLocked across arbitrary rates, where
// rounding down would leave the bucket a hair short.
func TestProperty_RetryAfterIsSufficient(t *testing.T) {
	for seed := int64(0); seed < 500; seed++ {
		rng := rand.New(rand.NewSource(seed))
		rate := 0.1 + rng.Float64()*1000
		capacity := float64(1 + rng.Intn(50))
		clk := newFakeClock()
		l := mustLimiter(t, Config{Rate: rate, Capacity: capacity, Clock: clk})
		drain(l)
		clk.Advance(time.Duration(rng.Int63n(int64(100 * time.Millisecond))))

		n := 1 + rng.Intn(int(capacity))
		d := l.Take(n)
		if d.Allowed {
			continue
		}
		clk.Advance(d.RetryAfter)
		if !l.AllowN(n) {
			t.Fatalf("seed %d rate %v: waiting RetryAfter=%v was not enough for n=%d (tokens %v)",
				seed, rate, d.RetryAfter, n, l.tokens)
		}
	}
}
