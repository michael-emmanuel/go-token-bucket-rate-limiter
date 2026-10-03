package limiter

import (
	"math"
	"net/http"
	"strconv"
	"time"
)

// KeyFunc extracts the identity to rate limit from a request. It returns
// ok=false when the request carries no usable identity. See keys.go for
// ready-made implementations and docs/architecture.md for the trust issues
// each one has.
type KeyFunc func(r *http.Request) (key string, ok bool)

// Middleware returns HTTP middleware that charges one token per request to the
// key returned by keyFn:
//
//   - allowed: the request is passed to the next handler;
//   - over limit: the response is 429 Too Many Requests, with a Retry-After
//     header (whole seconds, rounded up) when the limiter can estimate one;
//   - no identity: the response is 400 Bad Request, and no limiter state is
//     created. Combine key functions with FirstKey to fall back to a coarser
//     identity instead.
//
// Place the middleware after authentication if keys derive from credentials.
// Otherwise an unauthenticated client can invent unlimited identities.
//
// Middleware panics if p or keyFn is nil. This is a wiring error that should
// fail at startup, as http.Handle does for a nil handler.
func Middleware(p *PerUser, keyFn KeyFunc) func(http.Handler) http.Handler {
	if p == nil || keyFn == nil {
		panic("limiter: Middleware requires a non-nil PerUser and KeyFunc")
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, ok := keyFn(r)
			if !ok {
				http.Error(w, "unable to identify client", http.StatusBadRequest)
				return
			}
			d := p.Take(key, 1)
			if !d.Allowed {
				if secs := retryAfterSeconds(d.RetryAfter); secs > 0 {
					w.Header().Set("Retry-After", strconv.Itoa(secs))
				}
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// retryAfterSeconds converts a delay to the integer delta-seconds form that
// the Retry-After header requires, rounding up so that a client that obeys the
// header does not retry too early. Zero means "no estimate".
func retryAfterSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	secs := math.Ceil(d.Seconds())
	if secs > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(secs)
}
