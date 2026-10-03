package limiter

import "errors"

var (
	// ErrInvalidConfig is returned (wrapped, with details) when a
	// configuration value is rejected at construction time. Use errors.Is.
	ErrInvalidConfig = errors.New("limiter: invalid configuration")

	// ErrInvalidTokens is returned by WaitN when n is less than 1.
	ErrInvalidTokens = errors.New("limiter: token count must be at least 1")

	// ErrExceedsCapacity is returned by WaitN when n is larger than the bucket
	// capacity. Such a request can never be satisfied, so waiting would block
	// until the context ends for no benefit.
	ErrExceedsCapacity = errors.New("limiter: requested tokens exceed bucket capacity")
)
