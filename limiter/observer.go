package limiter

import (
	"sync/atomic"
	"time"
)

// Observer receives the outcome of limiter decisions. It lets an application
// export metrics without the limiter depending on a metrics library.
//
// Contract:
//   - Methods are called after the limiter's locks have been released, from
//     the goroutine that made the request, so implementations must be safe for
//     concurrent use and should be cheap.
//   - Each Allow/AllowN/Take/Wait call produces exactly one Allowed or
//     Rejected notification. Calls with n < 1 are programming errors and
//     produce no notification.
//   - Observers deliberately do not receive the key of a per-user request.
//     Per-key labels create unbounded metric cardinality; see
//     docs/observability.md.
type Observer interface {
	// Allowed reports that a request for n tokens succeeded.
	Allowed(n int)
	// Rejected reports that a request for n tokens was denied, or that a Wait
	// ended without obtaining tokens.
	Rejected(n int)
	// Waited reports how long a Wait or WaitN call blocked and its result
	// (nil on success).
	Waited(d time.Duration, err error)
}

func notify(o Observer, n int, allowed bool) {
	if o == nil || n < 1 {
		return
	}
	if allowed {
		o.Allowed(n)
	} else {
		o.Rejected(n)
	}
}

// Counters is a minimal Observer that keeps atomic counters. It is useful in
// tests and examples, and as a template for adapters to real metrics systems.
// The zero value is ready to use.
type Counters struct {
	allowedRequests  atomic.Uint64
	rejectedRequests atomic.Uint64
	allowedTokens    atomic.Uint64
	rejectedTokens   atomic.Uint64
	waits            atomic.Uint64
	waitErrors       atomic.Uint64
	waitNanos        atomic.Uint64
}

// CountersSnapshot is a point-in-time copy of a Counters value. Fields are
// read individually, so a snapshot taken during traffic is not a single atomic
// cut across all fields.
type CountersSnapshot struct {
	AllowedRequests  uint64        // calls that obtained their tokens
	RejectedRequests uint64        // calls that were denied or whose Wait failed
	AllowedTokens    uint64        // tokens consumed by allowed calls
	RejectedTokens   uint64        // tokens requested by rejected calls
	Waits            uint64        // completed Wait/WaitN calls, successful or not
	WaitErrors       uint64        // Wait/WaitN calls that returned an error
	WaitTotal        time.Duration // total time spent blocked in Wait/WaitN
}

// Allowed implements Observer.
func (c *Counters) Allowed(n int) {
	c.allowedRequests.Add(1)
	c.allowedTokens.Add(uint64(n))
}

// Rejected implements Observer.
func (c *Counters) Rejected(n int) {
	c.rejectedRequests.Add(1)
	c.rejectedTokens.Add(uint64(n))
}

// Waited implements Observer.
func (c *Counters) Waited(d time.Duration, err error) {
	c.waits.Add(1)
	if err != nil {
		c.waitErrors.Add(1)
	}
	if d > 0 {
		c.waitNanos.Add(uint64(d))
	}
}

// Snapshot returns the current counter values.
func (c *Counters) Snapshot() CountersSnapshot {
	return CountersSnapshot{
		AllowedRequests:  c.allowedRequests.Load(),
		RejectedRequests: c.rejectedRequests.Load(),
		AllowedTokens:    c.allowedTokens.Load(),
		RejectedTokens:   c.rejectedTokens.Load(),
		Waits:            c.waits.Load(),
		WaitErrors:       c.waitErrors.Load(),
		WaitTotal:        time.Duration(c.waitNanos.Load()),
	}
}
