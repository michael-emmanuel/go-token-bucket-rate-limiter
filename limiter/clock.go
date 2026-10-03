package limiter

import "time"

// Clock is the source of time for a Limiter. Only Now is needed because the
// bucket derives everything from elapsed time.
//
// The default clock is the system clock. time.Now carries a monotonic clock
// reading, and Time.Sub uses it, so wall-clock adjustments (NTP steps, manual
// changes) do not affect refill. Custom clocks exist mainly so tests can
// advance time deterministically; see docs/testing.md.
//
// Now is called while the bucket's lock is held (see Limiter), so it must be
// fast, must not block, and must not call back into the limiter.
//
// Wait always sleeps using real timers; the Clock does not control sleeping.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
