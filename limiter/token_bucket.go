package limiter

import (
	"math"
	"sync"
	"time"
)

// Decision is the outcome of Take.
type Decision struct {
	// Allowed reports whether the tokens were consumed.
	Allowed bool

	// RetryAfter is, when Allowed is false, the earliest delay after which the
	// same request could succeed if no other request consumed tokens in the
	// meantime. It is a lower bound, not a reservation: another caller may take
	// the tokens first.
	//
	// RetryAfter is zero when Allowed is true and also when no useful estimate
	// exists (the request asks for more tokens than the bucket can ever hold,
	// or a PerUser table is full).
	RetryAfter time.Duration
}

// Limiter is a token bucket. It is safe for concurrent use by multiple
// goroutines. Create one with NewLimiter; the zero value is not usable.
//
// # Representation
//
// The number of tokens is a float64 so that partial tokens accrue between
// calls: at 10 tokens/second, 50ms of idleness earns 0.5 tokens, and a second
// 50ms later completes the token. Integer tokens would either lose that
// fraction (under-admitting at low rates) or need a fixed sub-token
// resolution. The cost is rounding error, which is small (relative error
// around 1e-16 per operation) and always resolved conservatively: a bucket
// that is short by a rounding error rejects and reports a RetryAfter of a few
// more nanoseconds. See docs/token-bucket.md for the full discussion.
//
// # Critical section
//
// Every operation performs refill, check, consume and timestamp update as one
// step under mu. Reading the clock is part of that step: reading it before
// taking the lock would let a goroutine apply a stale timestamp after a
// later one had already been applied.
type Limiter struct {
	rate     float64 // tokens per second; immutable after construction
	capacity float64 // maximum tokens; immutable after construction
	clock    Clock   // immutable after construction
	observer Observer

	mu     sync.Mutex // guards tokens and last
	tokens float64    // invariant: 0 <= tokens <= capacity
	last   time.Time  // time of the most recent refill
}

// NewLimiter returns a Limiter that starts with a full bucket. It returns an
// error wrapping ErrInvalidConfig if cfg is invalid.
func NewLimiter(cfg Config) (*Limiter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newLimiter(cfg), nil
}

// newLimiter builds a limiter from an already validated configuration.
func newLimiter(cfg Config) *Limiter {
	clock := cfg.Clock
	if clock == nil {
		clock = systemClock{}
	}
	return &Limiter{
		rate:     cfg.Rate,
		capacity: cfg.Capacity,
		clock:    clock,
		observer: cfg.Observer,
		tokens:   cfg.Capacity,
		last:     clock.Now(),
	}
}

// Allow reports whether one token was available and consumes it if so. It
// never blocks.
func (l *Limiter) Allow() bool { return l.Take(1).Allowed }

// AllowN reports whether n tokens were available and consumes them if so. The
// operation is all-or-nothing: if fewer than n tokens are available none are
// consumed. It returns false, without consuming anything, if n is less than 1
// or greater than the bucket capacity. It never blocks.
func (l *Limiter) AllowN(n int) bool { return l.Take(n).Allowed }

// Take is like AllowN but also reports, for a rejected request, how long the
// caller should wait before retrying. It is what HTTP handlers use to build a
// Retry-After header.
func (l *Limiter) Take(n int) Decision {
	d := l.take(n)
	notify(l.observer, n, d.Allowed)
	return d
}

// Tokens returns the number of tokens currently available, after applying any
// pending refill. It is a snapshot: the value can be stale by the time the
// caller uses it, so it is meant for metrics and debugging, never for
// deciding whether to send a request (use Allow for that).
func (l *Limiter) Tokens() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refillLocked(l.clock.Now())
	return l.tokens
}

// take is Take without observer notification.
func (l *Limiter) take(n int) Decision {
	if n < 1 || float64(n) > l.capacity {
		return Decision{}
	}
	need := float64(n)

	l.mu.Lock()
	defer l.mu.Unlock() // deferred so a panicking Clock cannot leave mu held
	l.refillLocked(l.clock.Now())
	if l.tokens >= need {
		l.tokens -= need
		return Decision{Allowed: true}
	}
	return Decision{RetryAfter: l.retryAfterLocked(need)}
}

// takeTwo consumes n tokens from both a and b, or from neither. It is the
// building block for global-plus-per-user limiting.
//
// Both mutexes are held while the decision is made, so no other goroutine can
// observe or modify either bucket between the availability check and the
// consumption. This is what prevents token leakage: a request rejected
// because b is empty never consumes from a, and vice versa.
//
// Lock ordering: a is always locked before b. Callers must always pass the
// shared (global) limiter as a and the narrower (per-user) limiter as b. No
// code path locks two per-user limiters at once, so the ordering is total and
// deadlock-free.
func takeTwo(a, b *Limiter, n int) Decision {
	if n < 1 || float64(n) > a.capacity || float64(n) > b.capacity {
		return Decision{}
	}
	need := float64(n)

	a.mu.Lock()
	defer a.mu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()

	a.refillLocked(a.clock.Now())
	b.refillLocked(b.clock.Now())

	if a.tokens >= need && b.tokens >= need {
		a.tokens -= need
		b.tokens -= need
		return Decision{Allowed: true}
	}
	// The request can succeed only once both buckets can cover it, so the
	// retry hint is the larger of the two individual waits.
	var wait time.Duration
	if a.tokens < need {
		wait = a.retryAfterLocked(need)
	}
	if b.tokens < need {
		if w := b.retryAfterLocked(need); w > wait {
			wait = w
		}
	}
	return Decision{RetryAfter: wait}
}

// full reports whether the bucket is at capacity after applying pending
// refill. A full bucket is indistinguishable from a freshly created one,
// which is what makes evicting it from a PerUser table lossless.
func (l *Limiter) full() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refillLocked(l.clock.Now())
	return l.tokens >= l.capacity
}

// refillLocked credits the tokens earned since the last refill. The caller
// must hold l.mu.
func (l *Limiter) refillLocked(now time.Time) {
	elapsed := now.Sub(l.last)
	if elapsed <= 0 {
		if elapsed < 0 {
			// The clock went backwards (possible only with a custom Clock, or
			// a time value stripped of its monotonic reading). Credit nothing
			// and re-anchor, so that time counts from here instead of the
			// bucket freezing until the old timestamp is reached again.
			l.last = now
		}
		return
	}
	// Multiply before dividing: for integral rates and whole-millisecond
	// durations the product is an exact integer, so results such as
	// "100ms at 10/s is exactly 1 token" carry no rounding error.
	earned := float64(elapsed) * l.rate / float64(time.Second)
	// Clamping here is what maintains tokens <= capacity, including after
	// very long idle periods. If earned overflows to +Inf, Min still yields
	// capacity.
	l.tokens = math.Min(l.capacity, l.tokens+earned)
	l.last = now
}

// retryAfterLocked returns how long until the bucket holds need tokens,
// assuming no other consumers. The caller must hold l.mu and must know that
// l.tokens < need.
func (l *Limiter) retryAfterLocked(need float64) time.Duration {
	nanos := math.Ceil((need - l.tokens) / l.rate * float64(time.Second))
	if nanos >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	// Round up, and never report zero for a rejected request: a zero wait
	// would make a retry loop spin.
	if nanos < 1 {
		nanos = 1
	}
	return time.Duration(nanos)
}
