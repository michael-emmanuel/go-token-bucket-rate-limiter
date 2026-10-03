package limiter

import (
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced Clock. Tests that use it run in zero real
// time and are fully deterministic.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func mustLimiter(t testing.TB, cfg Config) *Limiter {
	t.Helper()
	l, err := NewLimiter(cfg)
	if err != nil {
		t.Fatalf("NewLimiter(%+v): %v", cfg, err)
	}
	return l
}

func mustPerUser(t testing.TB, cfg PerUserConfig) *PerUser {
	t.Helper()
	p, err := NewPerUser(cfg)
	if err != nil {
		t.Fatalf("NewPerUser: %v", err)
	}
	return p
}

// drain consumes single tokens until the limiter rejects, returning how many
// it took. Only use with a frozen fake clock.
func drain(l *Limiter) int {
	n := 0
	for l.Allow() {
		n++
	}
	return n
}
