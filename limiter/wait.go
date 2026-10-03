package limiter

import (
	"context"
	"fmt"
	"time"
)

// Wait blocks until one token is available, consumes it, and returns nil. It
// is WaitN(ctx, 1).
func (l *Limiter) Wait(ctx context.Context) error { return l.WaitN(ctx, 1) }

// WaitN blocks until n tokens are available, consumes them, and returns nil.
//
// It returns early with an error, consuming nothing, when:
//   - n is less than 1 (ErrInvalidTokens) or greater than the bucket capacity
//     (ErrExceedsCapacity);
//   - ctx is already done (ctx.Err());
//   - ctx is done while waiting (ctx.Err());
//   - the required wait already exceeds ctx's deadline. The error then wraps
//     context.DeadlineExceeded, and the call returns immediately instead of
//     sleeping until the deadline for a request that cannot succeed.
//
// # Strategy
//
// WaitN loops: try to take the tokens; if that fails, sleep for the computed
// shortfall (or until ctx ends) and try again. It does not reserve tokens
// ahead of time. Reservation would require the bucket to go negative, which
// breaks the 0 <= tokens invariant and needs a refund path on cancellation.
// The price of retrying is that waiters are not served in FIFO order: a
// concurrent Allow can take tokens a sleeping waiter was waiting for, in
// which case the waiter recomputes and sleeps again. Under sustained overload
// this can starve an unlucky waiter, so bound waits with a context deadline.
//
// # Resources
//
// Each call uses at most one time.Timer, stopped on every return path, and
// starts no goroutines, so cancelled calls leave nothing running. Sleeping
// uses real timers even if a custom Clock is configured.
func (l *Limiter) WaitN(ctx context.Context, n int) error {
	if n < 1 {
		return ErrInvalidTokens
	}
	if float64(n) > l.capacity {
		return ErrExceedsCapacity
	}
	start := time.Now()
	err := l.wait(ctx, n)
	notify(l.observer, n, err == nil)
	if l.observer != nil {
		l.observer.Waited(time.Since(start), err)
	}
	return err
}

func (l *Limiter) wait(ctx context.Context, n int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		d := l.take(n)
		if d.Allowed {
			return nil
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < d.RetryAfter {
			return fmt.Errorf("limiter: required wait of %v exceeds context deadline: %w", d.RetryAfter, context.DeadlineExceeded)
		}
		if timer == nil {
			timer = time.NewTimer(d.RetryAfter)
		} else {
			// Safe: the previous firing was received from timer.C below.
			timer.Reset(d.RetryAfter)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}
