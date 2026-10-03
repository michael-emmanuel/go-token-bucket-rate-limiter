package limiter

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// sweepCooldown bounds how often a full PerUser table may trigger an
// on-demand sweep. Without it, an attacker who keeps the table full would make
// every new-key request pay for an O(n) scan under the write lock.
const sweepCooldown = time.Second

// PerUserConfig configures a PerUser limiter.
type PerUserConfig struct {
	// Limit configures each key's bucket. Limit.Observer is ignored; set
	// Observer below so a request is reported once even when a Global limiter
	// is involved.
	Limit Config

	// Global, if non-nil, is a limiter shared by all keys. A request must
	// obtain tokens from both the key's bucket and Global; the two are charged
	// atomically (all or nothing). Global may be shared by several PerUser
	// values. Give it the same Clock as Limit.
	Global *Limiter

	// MaxUsers bounds the number of tracked keys. Zero means unbounded. When
	// the table is full, idle buckets are swept (at most once per second); if
	// that frees no space, requests for new keys are rejected. See the type
	// documentation for why rejection was chosen over eviction.
	MaxUsers int

	// Observer receives one notification per request. Nil disables it.
	Observer Observer
}

// Validate reports whether the configuration is usable.
func (c PerUserConfig) Validate() error {
	if err := c.Limit.Validate(); err != nil {
		return err
	}
	if c.MaxUsers < 0 {
		return fmt.Errorf("%w: MaxUsers must not be negative, got %d", ErrInvalidConfig, c.MaxUsers)
	}
	return nil
}

// PerUser rate limits many independent keys (user IDs, API keys, addresses),
// each with its own token bucket, optionally combined with a shared global
// limiter. It is safe for concurrent use. Create one with NewPerUser.
//
// # Concurrency
//
// The key table is guarded by an RWMutex. The common path (existing key) takes
// only the read lock, so different keys proceed in parallel and contend only
// on their own bucket's mutex. Creating a key, and sweeping, take the write
// lock. The read lock is held for the duration of the bucket operation, which
// is a handful of arithmetic instructions; that is what lets a sweep remove a
// bucket without racing an in-flight request (see below).
//
// Lock order is table lock, then global limiter, then key limiter.
//
// # Memory lifecycle
//
// A bucket that has refilled to capacity behaves exactly like a bucket that
// was never created, so it can be deleted without changing any future
// decision. Cleanup deletes precisely those buckets. This is a lossless
// eviction: unlike a fixed idle timeout, it can never hand a user tokens they
// had spent. It also means the retention time of a key is the time to refill
// from its current level, at most Capacity/Rate seconds after its last
// request.
//
// PerUser starts no goroutines. The owner decides when cleanup runs: call
// Cleanup from an existing loop, or run RunCleanup in a goroutine the owner
// controls with a context.
//
// Because retention is bounded by Capacity/Rate, memory is bounded by the
// number of distinct keys seen in that window. Set MaxUsers to turn that into
// a hard cap. When the cap is reached, new keys are rejected rather than
// evicting existing ones: eviction of an active key would reset its budget, so
// an attacker rotating through keys could use eviction to erase other users'
// limits, while rejection fails closed for new keys only. The trade-off is
// that a key-flooding attacker can lock new legitimate users out until
// buckets refill; see docs/architecture.md.
type PerUser struct {
	limit    Config // Observer cleared; buckets are silent
	global   *Limiter
	max      int
	observer Observer
	clock    Clock

	mu        sync.RWMutex // guards buckets and lastSweep
	buckets   map[string]*Limiter
	lastSweep time.Time
}

// NewPerUser returns a PerUser limiter. It returns an error wrapping
// ErrInvalidConfig if cfg is invalid.
func NewPerUser(cfg PerUserConfig) (*PerUser, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	limit := cfg.Limit
	limit.Observer = nil
	if limit.Clock == nil {
		limit.Clock = systemClock{}
	}
	return &PerUser{
		limit:    limit,
		global:   cfg.Global,
		max:      cfg.MaxUsers,
		observer: cfg.Observer,
		clock:    limit.Clock,
		buckets:  make(map[string]*Limiter),
	}, nil
}

// Allow reports whether key may make one request now, consuming a token from
// key's bucket (and from the global limiter, if configured) if so.
func (p *PerUser) Allow(key string) bool { return p.Take(key, 1).Allowed }

// AllowN is like Allow for a request costing n tokens. It returns false,
// without consuming anything or creating state for key, if n is less than 1 or
// exceeds the capacity of either bucket.
func (p *PerUser) AllowN(key string, n int) bool { return p.Take(key, n).Allowed }

// Take is like AllowN but also reports a retry hint for rejected requests. See
// Decision. RetryAfter is zero when the table is full and key is new.
func (p *PerUser) Take(key string, n int) Decision {
	d := p.take(key, n)
	notify(p.observer, n, d.Allowed)
	return d
}

// Len returns the number of keys currently tracked. It is intended for an
// "active users" gauge.
func (p *PerUser) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.buckets)
}

// Cleanup removes every key whose bucket has refilled to capacity and returns
// the number removed. Removal never changes the outcome of any later request.
// The cost is proportional to the number of tracked keys and the table is
// write-locked while it runs, so on very large tables run it at a modest
// interval (see docs/architecture.md for sharding as the next step).
func (p *PerUser) Cleanup() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastSweep = p.clock.Now()
	return p.sweepLocked()
}

// RunCleanup calls Cleanup every interval until ctx is done, then returns
// ctx.Err(). It blocks, so run it in a goroutine the caller owns:
//
//	go func() { _ = p.RunCleanup(ctx, time.Minute) }()
//
// The ticker is stopped on return. It returns an error wrapping
// ErrInvalidConfig, without blocking, if interval is not positive.
func (p *PerUser) RunCleanup(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("%w: cleanup interval must be positive, got %v", ErrInvalidConfig, interval)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			p.Cleanup()
		}
	}
}

func (p *PerUser) take(key string, n int) Decision {
	// Reject requests that can never succeed before touching the table, so
	// they cannot create state for a key.
	if n < 1 || float64(n) > p.limit.Capacity || (p.global != nil && float64(n) > p.global.capacity) {
		return Decision{}
	}

	p.mu.RLock()
	if l, ok := p.buckets[key]; ok {
		d := p.chargeLocked(l, n)
		p.mu.RUnlock()
		return d
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	// Another goroutine may have created the bucket between RUnlock and Lock.
	l, ok := p.buckets[key]
	if !ok {
		if p.max > 0 && len(p.buckets) >= p.max {
			p.sweepIfDueLocked()
			if len(p.buckets) >= p.max {
				return Decision{}
			}
		}
		l = newLimiter(p.limit)
		p.buckets[key] = l
	}
	return p.chargeLocked(l, n)
}

// chargeLocked charges n tokens to l (and the global limiter). The caller
// must hold p.mu in either mode; that hold is what keeps l in the table for
// the duration of the operation.
func (p *PerUser) chargeLocked(l *Limiter, n int) Decision {
	if p.global != nil {
		return takeTwo(p.global, l, n)
	}
	return l.take(n)
}

func (p *PerUser) sweepIfDueLocked() {
	now := p.clock.Now()
	if now.Sub(p.lastSweep) < sweepCooldown {
		return
	}
	p.lastSweep = now
	p.sweepLocked()
}

func (p *PerUser) sweepLocked() int {
	removed := 0
	for key, l := range p.buckets {
		if l.full() {
			delete(p.buckets, key)
			removed++
		}
	}
	return removed
}
