package limiter

import (
	"fmt"
	"math"
)

// Config describes one token bucket.
type Config struct {
	// Rate is the sustained refill rate in tokens per second. It must be a
	// finite number greater than zero. Fractional rates are allowed (0.5 means
	// one token every two seconds).
	Rate float64

	// Capacity is the maximum number of tokens the bucket can hold, and
	// therefore the largest burst. The bucket starts full. It must be a finite
	// number of at least 1, because the smallest request consumes one token; a
	// smaller capacity would reject every request forever.
	Capacity float64

	// Clock supplies the current time. Nil selects the system clock.
	Clock Clock

	// Observer receives allow/reject/wait notifications. Nil disables
	// observation. See Observer for the calling contract.
	Observer Observer
}

// Validate reports whether the configuration is usable. NewLimiter and
// NewPerUser call it, so configuration errors surface at construction time
// instead of as surprising behavior under load.
func (c Config) Validate() error {
	// Written as !(x > 0) so that NaN is rejected as well.
	if !(c.Rate > 0) || math.IsInf(c.Rate, 0) {
		return fmt.Errorf("%w: Rate must be a finite number greater than 0, got %v", ErrInvalidConfig, c.Rate)
	}
	if !(c.Capacity >= 1) || math.IsInf(c.Capacity, 0) {
		return fmt.Errorf("%w: Capacity must be a finite number of at least 1, got %v", ErrInvalidConfig, c.Capacity)
	}
	return nil
}
