package limiter

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// maxKeyLength bounds identities taken from request data. Keys are stored in
// the PerUser table, so an unbounded key would let a client spend server
// memory at the rate of its header size.
const maxKeyLength = 256

// KeyFromContextValue returns a KeyFunc for authenticated identities: it reads
// a string stored in the request context under ctxKey (typically by an
// authentication middleware) and returns prefix plus that string. This is the
// preferred identity when one is available.
func KeyFromContextValue(ctxKey any, prefix string) KeyFunc {
	return func(r *http.Request) (string, bool) {
		v, ok := r.Context().Value(ctxKey).(string)
		return boundedKey(prefix, v, ok)
	}
}

// KeyFromHeader returns a KeyFunc that uses the value of a request header,
// such as an API key header, prefixed with prefix. Prefixes keep identity
// namespaces apart ("key:" versus "ip:"). The header value is used as
// presented: validate it (authenticate it) before this middleware runs, or
// clients can mint unlimited identities.
func KeyFromHeader(header, prefix string) KeyFunc {
	return func(r *http.Request) (string, bool) {
		v := strings.TrimSpace(r.Header.Get(header))
		return boundedKey(prefix, v, true)
	}
}

// KeyFromRemoteAddr returns a KeyFunc that identifies clients by the peer
// address of the TCP connection (Request.RemoteAddr). It ignores all headers,
// so it cannot be spoofed by the client, but behind a reverse proxy it sees
// only the proxy's address. IPv4-mapped IPv6 addresses are unmapped and IPv6
// addresses are truncated to their /64 prefix, because a single subscriber is
// usually assigned a whole /64 and could otherwise obtain 2^64 identities.
// Keys look like "ip:192.0.2.7" or "ip:2001:db8::/64".
func KeyFromRemoteAddr() KeyFunc {
	return func(r *http.Request) (string, bool) { return ipKey(r.RemoteAddr) }
}

// KeyFromForwardedFor returns a KeyFunc for services behind trustedProxies
// reverse proxies that they operate, each appending the address of its peer to
// X-Forwarded-For. It selects the entry trustedProxies positions from the
// right of the header: entries further left were supplied by the client and
// can be forged, so they are never used.
//
// With one trusted proxy the last entry is used; with a CDN in front of a load
// balancer, trustedProxies is 2, and so on. Requests with fewer entries than
// trustedProxies (for example, ones that bypassed the proxy) yield ok=false.
//
// Only use this if every request really passes through the stated number of
// proxies. It panics if trustedProxies is less than 1, since that would make
// every request unidentifiable.
func KeyFromForwardedFor(trustedProxies int) KeyFunc {
	if trustedProxies < 1 {
		panic("limiter: KeyFromForwardedFor requires trustedProxies >= 1")
	}
	return func(r *http.Request) (string, bool) {
		var entries []string
		for _, line := range r.Header.Values("X-Forwarded-For") {
			for _, e := range strings.Split(line, ",") {
				if e = strings.TrimSpace(e); e != "" {
					entries = append(entries, e)
				}
			}
		}
		if len(entries) < trustedProxies {
			return "", false
		}
		return ipKey(entries[len(entries)-trustedProxies])
	}
}

// FirstKey returns a KeyFunc that tries each function in order and returns the
// first identity found. Typical use: authenticated user, then API key, then
// address.
func FirstKey(fns ...KeyFunc) KeyFunc {
	return func(r *http.Request) (string, bool) {
		for _, fn := range fns {
			if key, ok := fn(r); ok {
				return key, true
			}
		}
		return "", false
	}
}

func boundedKey(prefix, v string, ok bool) (string, bool) {
	if !ok || v == "" || len(prefix)+len(v) > maxKeyLength {
		return "", false
	}
	return prefix + v, true
}

func ipKey(hostport string) (string, bool) {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return "", false
	}
	addr = addr.WithZone("").Unmap()
	if addr.Is6() {
		prefix, err := addr.Prefix(64)
		if err != nil {
			return "", false
		}
		return "ip:" + prefix.String(), true
	}
	return "ip:" + addr.String(), true
}
