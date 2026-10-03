package limiter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
}

func do(h http.Handler, remoteAddr string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = remoteAddr
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestMiddleware_AllowsThenReturns429(t *testing.T) {
	clk := newFakeClock()
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 2, Clock: clk}})
	h := Middleware(p, KeyFromRemoteAddr())(okHandler())

	for i := 0; i < 2; i++ {
		if rec := do(h, "192.0.2.1:5000", nil); rec.Code != http.StatusOK {
			t.Fatalf("request %d: status %d, want 200", i, rec.Code)
		}
	}
	rec := do(h, "192.0.2.1:5000", nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want \"1\"", got)
	}

	clk.Advance(time.Second)
	if rec := do(h, "192.0.2.1:5000", nil); rec.Code != http.StatusOK {
		t.Fatalf("after waiting Retry-After: status %d, want 200", rec.Code)
	}
}

func TestMiddleware_RetryAfterRoundsUp(t *testing.T) {
	clk := newFakeClock()
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 0.25, Capacity: 1, Clock: clk}}) // 4s per token
	h := Middleware(p, KeyFromRemoteAddr())(okHandler())
	do(h, "192.0.2.1:1", nil)
	clk.Advance(1500 * time.Millisecond) // 2.5s still to wait
	rec := do(h, "192.0.2.1:1", nil)
	if got, _ := strconv.Atoi(rec.Header().Get("Retry-After")); got != 3 {
		t.Fatalf("Retry-After = %q, want 3 (2.5s rounded up)", rec.Header().Get("Retry-After"))
	}
}

func TestMiddleware_ClientsAreIsolated(t *testing.T) {
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 1, Clock: newFakeClock()}})
	h := Middleware(p, KeyFromHeader("X-API-Key", "key:"))(okHandler())

	if rec := do(h, "192.0.2.1:1", map[string]string{"X-API-Key": "alice"}); rec.Code != 200 {
		t.Fatalf("alice: %d", rec.Code)
	}
	if rec := do(h, "192.0.2.1:1", map[string]string{"X-API-Key": "alice"}); rec.Code != 429 {
		t.Fatalf("alice second request: %d, want 429", rec.Code)
	}
	if rec := do(h, "192.0.2.1:1", map[string]string{"X-API-Key": "bob"}); rec.Code != 200 {
		t.Fatalf("bob should be unaffected by alice: %d", rec.Code)
	}
}

func TestMiddleware_NoIdentityIs400AndCreatesNoState(t *testing.T) {
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 1, Clock: newFakeClock()}})
	h := Middleware(p, KeyFromHeader("X-API-Key", "key:"))(okHandler())
	if rec := do(h, "192.0.2.1:1", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
	if p.Len() != 0 {
		t.Fatal("unidentifiable request created limiter state")
	}
}

func TestMiddleware_DeniedRequestDoesNotReachHandler(t *testing.T) {
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 1, Clock: newFakeClock()}})
	calls := 0
	h := Middleware(p, KeyFromRemoteAddr())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	for i := 0; i < 5; i++ {
		do(h, "192.0.2.1:1", nil)
	}
	if calls != 1 {
		t.Fatalf("handler called %d times, want 1", calls)
	}
}

func TestMiddleware_TableFullIs429WithoutRetryAfter(t *testing.T) {
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 1, Clock: newFakeClock()}, MaxUsers: 1})
	h := Middleware(p, KeyFromRemoteAddr())(okHandler())
	do(h, "192.0.2.1:1", nil)
	rec := do(h, "192.0.2.2:1", nil)
	if rec.Code != 429 || rec.Header().Get("Retry-After") != "" {
		t.Fatalf("status %d Retry-After %q; want 429 with no Retry-After", rec.Code, rec.Header().Get("Retry-After"))
	}
}

func TestMiddleware_PanicsOnNilArguments(t *testing.T) {
	p := mustPerUser(t, PerUserConfig{Limit: Config{Rate: 1, Capacity: 1}})
	for name, fn := range map[string]func(){
		"nil limiter": func() { Middleware(nil, KeyFromRemoteAddr()) },
		"nil keyfunc": func() { Middleware(p, nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: expected panic", name)
				}
			}()
			fn()
		}()
	}
}

func TestRetryAfterSeconds(t *testing.T) {
	for _, tt := range []struct {
		d    time.Duration
		want int
	}{
		{0, 0}, {-time.Second, 0}, {1, 1}, {time.Second, 1}, {time.Second + 1, 2},
		{2500 * time.Millisecond, 3}, {time.Duration(1<<63 - 1), 2147483647},
	} {
		if got := retryAfterSeconds(tt.d); got != tt.want {
			t.Errorf("retryAfterSeconds(%v) = %d, want %d", tt.d, got, tt.want)
		}
	}
}

type ctxKey struct{}

func TestKeyFunctions(t *testing.T) {
	req := func(remote string, hdr map[string][]string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = remote
		for k, vs := range hdr {
			for _, v := range vs {
				r.Header.Add(k, v)
			}
		}
		return r
	}
	long := make([]byte, 300)
	for i := range long {
		long[i] = 'a'
	}

	tests := []struct {
		name    string
		fn      KeyFunc
		r       *http.Request
		wantKey string
		wantOK  bool
	}{
		{"remote ipv4 with port", KeyFromRemoteAddr(), req("192.0.2.7:4321", nil), "ip:192.0.2.7", true},
		{"remote ipv4-mapped ipv6", KeyFromRemoteAddr(), req("[::ffff:192.0.2.7]:80", nil), "ip:192.0.2.7", true},
		{"remote ipv6 truncated to /64", KeyFromRemoteAddr(), req("[2001:db8:1:2:aaaa:bbbb:cccc:dddd]:80", nil), "ip:2001:db8:1:2::/64", true},
		{"two hosts in one /64 share a key", KeyFromRemoteAddr(), req("[2001:db8:1:2:1::9]:80", nil), "ip:2001:db8:1:2::/64", true},
		{"remote ipv6 zone stripped", KeyFromRemoteAddr(), req("[fe80::1%eth0]:80", nil), "ip:fe80::/64", true},
		{"remote garbage", KeyFromRemoteAddr(), req("not-an-address", nil), "", false},
		{"remote without port", KeyFromRemoteAddr(), req("192.0.2.7", nil), "ip:192.0.2.7", true},

		{"header present", KeyFromHeader("X-API-Key", "key:"), req("x", map[string][]string{"X-Api-Key": {" abc "}}), "key:abc", true},
		{"header missing", KeyFromHeader("X-API-Key", "key:"), req("x", nil), "", false},
		{"header too long", KeyFromHeader("X-API-Key", "key:"), req("x", map[string][]string{"X-Api-Key": {string(long)}}), "", false},

		// Client forges the left side; only the rightmost trusted entry counts.
		{"xff one proxy ignores forged entries", KeyFromForwardedFor(1),
			req("10.0.0.1:1", map[string][]string{"X-Forwarded-For": {"6.6.6.6, 198.51.100.4"}}), "ip:198.51.100.4", true},
		{"xff two proxies", KeyFromForwardedFor(2),
			req("10.0.0.1:1", map[string][]string{"X-Forwarded-For": {"198.51.100.4, 203.0.113.9"}}), "ip:198.51.100.4", true},
		{"xff split across header lines", KeyFromForwardedFor(1),
			req("10.0.0.1:1", map[string][]string{"X-Forwarded-For": {"6.6.6.6", "198.51.100.4"}}), "ip:198.51.100.4", true},
		{"xff too few entries", KeyFromForwardedFor(2),
			req("10.0.0.1:1", map[string][]string{"X-Forwarded-For": {"198.51.100.4"}}), "", false},
		{"xff absent", KeyFromForwardedFor(1), req("10.0.0.1:1", nil), "", false},
		{"xff garbage entry", KeyFromForwardedFor(1),
			req("10.0.0.1:1", map[string][]string{"X-Forwarded-For": {"evil"}}), "", false},

		{"first key falls through", FirstKey(KeyFromHeader("X-API-Key", "key:"), KeyFromRemoteAddr()), req("192.0.2.7:1", nil), "ip:192.0.2.7", true},
		{"first key prefers earlier", FirstKey(KeyFromHeader("X-API-Key", "key:"), KeyFromRemoteAddr()),
			req("192.0.2.7:1", map[string][]string{"X-Api-Key": {"k"}}), "key:k", true},
		{"first key none", FirstKey(KeyFromHeader("X-API-Key", "key:")), req("x", nil), "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, ok := tt.fn(tt.r)
			if key != tt.wantKey || ok != tt.wantOK {
				t.Fatalf("got (%q, %v), want (%q, %v)", key, ok, tt.wantKey, tt.wantOK)
			}
		})
	}
}

func TestKeyFromContextValue(t *testing.T) {
	fn := KeyFromContextValue(ctxKey{}, "user:")
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if _, ok := fn(r); ok {
		t.Fatal("missing context value should not identify")
	}
	r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, "u-42"))
	if key, ok := fn(r); !ok || key != "user:u-42" {
		t.Fatalf("got (%q, %v)", key, ok)
	}
	r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, 42)) // wrong type
	if _, ok := fn(r); ok {
		t.Fatal("non-string context value should not identify")
	}
}

func TestKeyFromForwardedFor_PanicsOnZero(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	KeyFromForwardedFor(0)
}
